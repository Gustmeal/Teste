# app/routes/custo_oportunidade_routes.py
"""
Rotas do módulo Custo de Oportunidade.

Pipeline:
  1. Baixa CSV "ConsolidatedTradesDerivatives" da B3.
  2. Filtra DI1 e mapeia preços médios por ANO_MES.
  3. CONSTRÓI uma SÉRIE MENSAL COMPLETA de 105 meses consecutivos,
     começando em (DT_ATUALIZACAO + 1 mês).
     - Meses presentes no CSV → usa contrato e preço real.
     - Meses AUSENTES no CSV → cria contrato virtual (DI1+letraCME+ano)
       sem preço, será interpolado.
  4. Aplica interpolação linear nos vazios (regras início/meio/fim EMGEA).
  5. Insere as 105 linhas no banco.
  6. Recalcula TAXA_MEDIA APENAS do pregão atual (foto histórica).
  7. Aplica horizonte retroativo na base inteira (limpa eventuais sobras
     além de 105 meses em pregões antigos).

Compatível com Python 3.9 e 3.12.
"""
from flask import Blueprint, render_template, request, jsonify, make_response
from flask_login import login_required, current_user
from app import db
from app.models.custo_oportunidade import CustoOportunidade
from app.utils.audit import registrar_log
from datetime import datetime, date, timedelta
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from sqlalchemy import text
import os
import tempfile
import requests
import urllib3

# Desativa warning de SSL (firewall corporativo da EMGEA intercepta certificados)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

custo_oportunidade_bp = Blueprint(
    'custo_oportunidade',
    __name__,
    url_prefix='/custo-oportunidade'
)

# Quantidade de meses na série completa (regra de negócio EMGEA)
HORIZONTE_MESES = 105

# Código de mês dos contratos futuros (padrão CME/B3)
MES_CONTRATO = {
    'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
    'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12,
}


@custo_oportunidade_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


# =========================================================================
# PÁGINA PRINCIPAL
# =========================================================================
@custo_oportunidade_bp.route('/')
@login_required
def index():
    """
    Página principal. Aceita ?dt_atualizacao=YYYY-MM-DD pra filtrar.
    Anti-cache: força re-leitura do banco e desativa cache do navegador.

    Lógica do dropdown de datas:
      - Lista todas as DT_ATUALIZACAO distintas de FIN_TB001.
      - Para cada uma, busca em FIN_TB003 a flag REUNIAO (toggle COPOM).
      - Monta uma lista de tuplas (data, reuniao_bool) para o template
        renderizar "(Marcado)" ao lado quando REUNIAO=True.
    """
    from app.models.custo_oportunidade_media import CustoOportunidadeMedia

    db.session.expire_all()

    # 1. Parse do filtro
    dt_filtro_str = (request.args.get('dt_atualizacao') or '').strip()
    dt_filtro = None
    if dt_filtro_str:
        try:
            dt_filtro = datetime.strptime(dt_filtro_str, '%Y-%m-%d').date()
        except ValueError:
            dt_filtro = None

    # 2. Lista de DT_ATUALIZACAO distintas (de FIN_TB001) — ordem decrescente
    datas_disponiveis = CustoOportunidade.listar_datas_atualizacao_distintas()

    # 3. Buscar status REUNIAO de cada data em FIN_TB003 (1 query única)
    # Resultado: dict {date: bool}
    medias = CustoOportunidadeMedia.query.all()
    mapa_reuniao = {
        m.DT_ATUALIZACAO: bool(m.REUNIAO)
        for m in medias
        if m.DT_ATUALIZACAO is not None
    }

    # 4. Montar lista de tuplas (data, reuniao_bool) na mesma ordem
    # de datas_disponiveis (mais recente primeiro)
    datas_com_status = [
        (dt, mapa_reuniao.get(dt, False))
        for dt in datas_disponiveis
    ]

    # 5. Determinar qual data mostrar
    if dt_filtro and dt_filtro in datas_disponiveis:
        dt_exibida = dt_filtro
    else:
        dt_exibida = datas_disponiveis[0] if datas_disponiveis else None

    # 6. Buscar registros e ordenar por ANO_MES
    registros = []
    media_pregao = None
    if dt_exibida:
        registros_brutos = CustoOportunidade.listar_por_data_atualizacao(dt_exibida)
        registros = sorted(
            registros_brutos,
            key=lambda r: (r.ANO_MES or '999999')
        )
        media_pregao = CustoOportunidadeMedia.obter_por_data(dt_exibida)

    # 7. Histórico das últimas 12 médias
    historico_medias = CustoOportunidadeMedia.query.order_by(
        CustoOportunidadeMedia.DT_ATUALIZACAO.desc()
    ).limit(12).all()

    # 8. Estatísticas
    total_geral = CustoOportunidade.query.count()
    pregoes_distintos = len(datas_disponiveis)
    dt_mais_recente = datas_disponiveis[0] if datas_disponiveis else None

    response = make_response(render_template(
        'custo_oportunidade/index.html',
        registros=registros,
        datas_disponiveis=datas_disponiveis,    # mantido p/ compatibilidade
        datas_com_status=datas_com_status,      # NOVO: lista de (data, reuniao)
        dt_exibida=dt_exibida,
        dt_mais_recente=dt_mais_recente,
        total_geral=total_geral,
        pregoes_distintos=pregoes_distintos,
        media_pregao=media_pregao,
        historico_medias=historico_medias,
    ))
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


# =========================================================================
# EXECUTAR BOT
# =========================================================================
@custo_oportunidade_bp.route('/executar-bot', methods=['POST'])
@login_required
def executar_bot():
    """Pipeline completa do bot."""
    try:
        # 1. Baixar CSV
        caminho_csv, dt_atualizacao = _baixar_csv_b3()

        if not caminho_csv or not os.path.exists(caminho_csv):
            return jsonify({
                'success': False,
                'message': 'Não foi possível baixar o CSV da B3. '
                           'Verifique a conexão ou tente novamente.'
            }), 500

        # 2. Processar (gera série completa de 105 meses) e inserir
        inseridos, ignorados, interpolados, virtuais = _processar_csv(
            caminho_csv, dt_atualizacao
        )

        # 3. Recalcular TAXA_MEDIA APENAS do pregão atual
        try:
            linhas_taxa_atualizadas = _recalcular_taxa_media(dt_atualizacao)
            print(f'[CustoOportunidade] TAXA_MEDIA recalculada (pregão '
                  f'{dt_atualizacao}) - {linhas_taxa_atualizadas} linha(s)')
        except Exception as e_calc:
            print(f'[CustoOportunidade] FALHA TAXA_MEDIA: {e_calc}')
            linhas_taxa_atualizadas = 0

        # 4. Calcular TAXA_MEDIA_MENSAL APENAS do pregão atual
        # Fórmula: ((1 + TAXA_MEDIA/100)^(1/12) - 1) * 100
        try:
            linhas_mensal = _calcular_taxa_media_mensal(dt_atualizacao)
            print(f'[CustoOportunidade] TAXA_MEDIA_MENSAL calculada (pregão '
                  f'{dt_atualizacao}) - {linhas_mensal} linha(s)')
        except Exception as e_mensal:
            print(f'[CustoOportunidade] FALHA TAXA_MEDIA_MENSAL: {e_mensal}')
            linhas_mensal = 0

        # 5. Calcular AVG(TAXA_MEDIA_MENSAL) e gravar 1 linha em FIN_TB003
        try:
            media_mensal_pregao = _calcular_e_salvar_media_mensal_pregao(dt_atualizacao)
            if media_mensal_pregao is not None:
                print(f'[CustoOportunidade] MEDIA_MENSAL gravada em FIN_TB003: '
                      f'{media_mensal_pregao} (pregão {dt_atualizacao})')
        except Exception as e_media:
            print(f'[CustoOportunidade] FALHA MEDIA_MENSAL: {e_media}')
            media_mensal_pregao = None

        # 6. Limpar pregões antigos que tenham sobras além de 105 meses
        try:
            linhas_excluidas = _aplicar_horizonte_meses_global()
            print(f'[CustoOportunidade] Horizonte aplicado retroativamente - '
                  f'{linhas_excluidas} linha(s) excluída(s)')
        except Exception as e_horiz:
            print(f'[CustoOportunidade] FALHA horizonte: {e_horiz}')
            linhas_excluidas = 0

        # 7. Auditoria
        registrar_log(
            acao='carga',
            entidade='custo_oportunidade',
            entidade_id=None,
            descricao=(
                f'Carga de Custo de Oportunidade - '
                f'DT_ATUALIZACAO: {dt_atualizacao.strftime("%d/%m/%Y")}'
            ),
            dados_novos={
                'arquivo': os.path.basename(caminho_csv),
                'registros_inseridos': inseridos,
                'registros_virtuais': virtuais,
                'registros_ignorados': ignorados,
                'registros_interpolados': interpolados,
                'taxa_media_atualizadas': linhas_taxa_atualizadas,
                'taxa_media_mensal_calculadas': linhas_mensal,
                'media_mensal_pregao': (
                    str(media_mensal_pregao) if media_mensal_pregao is not None else None
                ),
                'linhas_excluidas_horizonte': linhas_excluidas,
                'dt_atualizacao': dt_atualizacao.strftime('%Y-%m-%d')
            }
        )

        # 8. Limpar arquivo temp
        try:
            os.remove(caminho_csv)
        except Exception:
            pass

        return jsonify({
            'success': True,
            'message': (
                f'Carga realizada! {inseridos} linha(s) na série de {HORIZONTE_MESES} meses '
                f'({virtuais} virtual(is), {interpolados} interpolado(s)). '
                f'TAXA_MEDIA: {linhas_taxa_atualizadas} | '
                f'TAXA_MEDIA_MENSAL: {linhas_mensal} | '
                f'MEDIA_MENSAL do pregão: '
                f'{media_mensal_pregao if media_mensal_pregao is not None else "—"}. '
                f'Pregão: {dt_atualizacao.strftime("%d/%m/%Y")}.'
            ),
            'inseridos': inseridos,
            'virtuais': virtuais,
            'ignorados': ignorados,
            'interpolados': interpolados,
            'taxa_media_atualizadas': linhas_taxa_atualizadas,
            'taxa_media_mensal_calculadas': linhas_mensal,
            'media_mensal_pregao': (
                str(media_mensal_pregao) if media_mensal_pregao is not None else None
            ),
            'linhas_excluidas_horizonte': linhas_excluidas,
            'dt_atualizacao': dt_atualizacao.strftime('%d/%m/%Y')
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Erro ao executar bot: {str(e)}'
        }), 500

# =========================================================================
# TOGGLE da flag REUNIAO COPOM
# =========================================================================
@custo_oportunidade_bp.route('/marcar-reuniao', methods=['POST'])
@login_required
def marcar_reuniao():
    """
    Alterna (toggle) a flag REUNIAO COPOM de um pregão específico.

    Recebe (JSON ou form):
      dt_atualizacao : 'YYYY-MM-DD' (obrigatório)

    Retorna JSON com o novo estado (true/false) e mensagem.
    """
    from app.models.custo_oportunidade_media import CustoOportunidadeMedia

    try:
        # Aceita JSON ou form-encoded
        dados = request.get_json(silent=True) or request.form
        dt_str = (dados.get('dt_atualizacao') or '').strip()

        if not dt_str:
            return jsonify({
                'success': False,
                'message': 'Parâmetro dt_atualizacao é obrigatório.'
            }), 400

        try:
            dt = datetime.strptime(dt_str, '%Y-%m-%d').date()
        except ValueError:
            return jsonify({
                'success': False,
                'message': f'Data inválida: {dt_str}. Use formato YYYY-MM-DD.'
            }), 400

        registro = CustoOportunidadeMedia.obter_por_data(dt)
        if not registro:
            return jsonify({
                'success': False,
                'message': (
                    f'Pregão {dt.strftime("%d/%m/%Y")} não encontrado em '
                    f'FIN_TB003. Rode o bot primeiro.'
                )
            }), 404

        # Toggle
        valor_anterior = bool(registro.REUNIAO)
        registro.REUNIAO = not valor_anterior
        db.session.commit()

        # Auditoria
        # IMPORTANTE: o parâmetro correto é "dados_antigos" (não "dados_anteriores")
        registrar_log(
            acao='editar',
            entidade='custo_oportunidade_media',
            entidade_id=str(dt),
            descricao=(
                f'Toggle REUNIAO COPOM em FIN_TB003 - '
                f'pregão {dt.strftime("%d/%m/%Y")}'
            ),
            dados_antigos={'REUNIAO': valor_anterior},
            dados_novos={'REUNIAO': bool(registro.REUNIAO)}
        )

        novo_estado = bool(registro.REUNIAO)
        return jsonify({
            'success': True,
            'novo_estado': novo_estado,
            'message': (
                f'Reunião COPOM {"marcada" if novo_estado else "desmarcada"} '
                f'para o pregão de {dt.strftime("%d/%m/%Y")}.'
            )
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Erro ao alternar flag REUNIAO: {str(e)}'
        }), 500

# =========================================================================
# DOWNLOAD DO CSV
# =========================================================================
def _baixar_csv_b3():
    """
    Baixa CSV de Negócios Consolidados via POST na API da B3. Tenta do
    dia atual para trás até 10 dias. Retorna (caminho, dt_atualizacao).
    """
    url = 'https://arquivos.b3.com.br/bdi/table/export/csv?lang=pt-BR'

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'text/csv, */*',
        'Origin': 'https://arquivos.b3.com.br',
        'Referer': 'https://arquivos.b3.com.br/bdi/tabelas?lang=pt-BR',
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        ),
    }

    hoje = datetime.now().date()

    for dias_atras in range(0, 10):
        tentativa = hoje - timedelta(days=dias_atras)
        data_str = tentativa.strftime('%Y-%m-%d')

        payload = {
            'Name': 'ConsolidatedTradesDerivatives',
            'ClientId': '',
            'Date': data_str,
            'Filters': {},
            'FinalDate': data_str,
        }

        try:
            response = requests.post(
                url, json=payload, headers=headers,
                verify=False, timeout=60
            )
        except requests.RequestException as e:
            print(f'[CustoOportunidade] Erro de rede em {data_str}: {e}')
            continue

        if response.status_code != 200:
            print(f'[CustoOportunidade] {data_str}: HTTP {response.status_code} - pulando')
            continue

        conteudo = response.content
        if len(conteudo) < 2000:
            continue
        if b'Instrumento financeiro' not in conteudo:
            continue
        if b'\nDI1' not in conteudo and b'\rDI1' not in conteudo:
            print(f'[CustoOportunidade] {data_str}: sem DI1 (não consolidado) - pulando')
            continue

        caminho = os.path.join(
            tempfile.gettempdir(),
            f'b3_negocios_{data_str}_{datetime.now().strftime("%H%M%S")}.csv'
        )
        with open(caminho, 'wb') as f:
            f.write(conteudo)

        print(f'[CustoOportunidade] CSV VÁLIDO: {caminho} '
              f'({len(conteudo)} bytes, pregão de {data_str})')

        return caminho, tentativa

    return None, None


# =========================================================================
# PROCESSAMENTO DO CSV — CONSTRÓI SÉRIE COMPLETA DE 105 MESES
# =========================================================================
def _processar_csv(caminho_csv, dt_atualizacao):
    """
    Lê CSV → mapeia DI1 → constrói série mensal completa de 105 meses
    a partir de (DT_ATUALIZACAO + 1 mês) → interpola vazios → insere.

    Retorna (inseridos, ignorados, interpolados, virtuais).
    """
    import pandas as pd
    from app.models.mes_instrumento import MesInstrumento

    # 1. Ler CSV
    df = None
    ultimo_erro = None
    tentativas = [
        {'sep': ';', 'encoding': 'utf-8-sig'},
        {'sep': ';', 'encoding': 'latin-1'},
        {'sep': ',', 'encoding': 'utf-8-sig'},
    ]
    for cfg in tentativas:
        try:
            df_tmp = pd.read_csv(
                caminho_csv,
                dtype=str,
                skiprows=[0, 1],
                engine='python',
                on_bad_lines='skip',
                **cfg
            )
            if 'Instrumento financeiro' in df_tmp.columns:
                df = df_tmp
                print(f'[CustoOportunidade] CSV lido: {len(df)} linhas, '
                      f'sep="{cfg["sep"]}", enc="{cfg["encoding"]}"')
                break
        except Exception as e:
            ultimo_erro = e

    if df is None:
        raise Exception(f'Não foi possível ler o CSV. Último erro: {ultimo_erro}')

    if 'Preço médio' not in df.columns:
        raise Exception(
            "Coluna 'Preço médio' não encontrada. "
            f"Colunas: {list(df.columns)}"
        )

    # 2. Mapas de meses (letra→número e número→letra)
    mapa_letra_para_nr = MesInstrumento.carregar_mapa()
    mapa_nr_para_letra = MesInstrumento.carregar_mapa_invertido()

    if not mapa_letra_para_nr or not mapa_nr_para_letra:
        raise Exception(
            'Tabela BDG.FIN_TB002_MESES_INSTRUMENTO está vazia ou incompleta.'
        )

    # 3. Indexar contratos DI1 do CSV por ANO_MES (int YYYYMM)
    # ano_mes_int → {'inst': 'DI1K26', 'preco': Decimal('14.62') ou None}
    df = df[df['Instrumento financeiro'].notna()]
    df['Instrumento financeiro'] = df['Instrumento financeiro'].astype(str).str.strip()
    di1 = df[df['Instrumento financeiro'].str.startswith('DI1', na=False)]

    contratos_csv = {}
    for _, row in di1.iterrows():
        inst = str(row['Instrumento financeiro']).strip()
        ordinal = _vencto_para_ordinal(inst)
        if ordinal is None:
            continue
        contratos_csv[ordinal] = {
            'inst': inst,
            'preco': _converter_preco_br(row['Preço médio']),
        }

    print(f'[CustoOportunidade] Contratos DI1 no CSV: {len(contratos_csv)}')

    # 4. Definir mês inicial e final da série de 105 meses
    # Início = DT_ATUALIZACAO + 1 mês
    ano_mes_inicio = _somar_meses_em_ano_mes(
        dt_atualizacao.year * 100 + dt_atualizacao.month,
        1
    )
    # Final = início + (HORIZONTE_MESES - 1) → total de 105 meses inclusive
    ano_mes_fim = _somar_meses_em_ano_mes(ano_mes_inicio, HORIZONTE_MESES - 1)

    print(f'[CustoOportunidade] Série de {HORIZONTE_MESES} meses: '
          f'{ano_mes_inicio} até {ano_mes_fim}')

    # 5. Construir série completa: 105 entradas consecutivas
    # Cada entrada: {'ano_mes_int': N, 'inst': str, 'preco': Decimal|None,
    #                'virtual': bool}
    serie = []
    cursor = ano_mes_inicio
    for _ in range(HORIZONTE_MESES):
        if cursor in contratos_csv:
            # Mês existe no CSV — usa dados reais
            real = contratos_csv[cursor]
            serie.append({
                'ano_mes_int': cursor,
                'inst': real['inst'],
                'preco': real['preco'],   # pode ser None se '-' no CSV
                'virtual': False,
            })
        else:
            # Mês ausente no CSV — cria contrato virtual
            inst_virtual = _construir_inst_virtual(cursor, mapa_nr_para_letra)
            serie.append({
                'ano_mes_int': cursor,
                'inst': inst_virtual,
                'preco': None,            # será interpolado
                'virtual': True,
            })
        # Próximo mês
        cursor = _somar_meses_em_ano_mes(cursor, 1)

    qtd_virtuais_iniciais = sum(1 for s in serie if s['virtual'])
    qtd_vazios_iniciais = sum(1 for s in serie if s['preco'] is None)
    print(f'[CustoOportunidade] Série montada: {len(serie)} linhas, '
          f'{qtd_virtuais_iniciais} virtual(is), '
          f'{qtd_vazios_iniciais} sem preço (a interpolar)')

    # 6. Adaptar série pra estrutura esperada por _interpolar_precos
    # (que espera lista de dicts com 'inst' e 'preco')
    interpolados = _interpolar_precos(serie)

    # 7. Inserir no banco
    dt_carga = datetime.now().date()
    inseridos = 0
    ignorados = 0
    virtuais_inseridos = 0

    for entry in serie:
        if entry['preco'] is None:
            # Caso extremo: nem real nem interpolável (CSV vazio ou
            # com 1 só preço). Não conseguimos calcular — ignora.
            print(f'[CustoOportunidade] Sem preço final pra '
                  f'{entry["inst"]} - ignorado')
            ignorados += 1
            continue

        ano_mes_int = entry['ano_mes_int']
        ano_mes_str = f'{ano_mes_int:06d}'
        # COD_MES_ANO = sufixo após "DI1" (ex: 'K26')
        cod_mes_ano = entry['inst'][3:] if len(entry['inst']) > 3 else None

        ja_existe = CustoOportunidade.query.filter_by(
            DT_ATUALIZACAO=dt_atualizacao,
            INST_FINANC=entry['inst']
        ).first()

        if ja_existe:
            ignorados += 1
            continue

        novo = CustoOportunidade(
            DT_CARGA=dt_carga,
            DT_ATUALIZACAO=dt_atualizacao,
            INST_FINANC=entry['inst'],
            VR_PRECO_MEDIA=entry['preco'],
            ANO_MES=ano_mes_str,
            COD_MES_ANO=cod_mes_ano,
            TAXA_MEDIA=None,
        )
        db.session.add(novo)
        inseridos += 1
        if entry['virtual']:
            virtuais_inseridos += 1

    db.session.commit()
    print(f'[CustoOportunidade] Inseridos: {inseridos} (sendo {virtuais_inseridos} '
          f'virtuais), Ignorados: {ignorados}, Interpolados: {interpolados}')

    return inseridos, ignorados, interpolados, virtuais_inseridos


# =========================================================================
# CONSTRUÇÃO DE INST_FINANC VIRTUAL
# =========================================================================
def _construir_inst_virtual(ano_mes_int, mapa_nr_para_letra):
    """
    Constrói INST_FINANC para um mês ausente no CSV.
    ano_mes_int = 202607 → 'DI1N26'  (N = julho, 26 = 2026)

    Regra:
      - Letra do mês: lookup em FIN_TB002_MESES_INSTRUMENTO
      - Ano: 2 últimos dígitos do ano (sempre 20XX)
    """
    ano = ano_mes_int // 100   # 2026
    mes = ano_mes_int % 100    # 7
    letra = mapa_nr_para_letra.get(mes)
    if not letra:
        # fallback de segurança (nunca deve acontecer se a tabela está OK)
        letra = list(MES_CONTRATO.keys())[mes - 1] if 1 <= mes <= 12 else '?'
    ano_2dig = f'{ano % 100:02d}'  # 26
    return f'DI1{letra}{ano_2dig}'


# =========================================================================
# RECÁLCULO DA TAXA_MEDIA (somente pregão atual)
# =========================================================================
def _recalcular_taxa_media(dt_atualizacao):
    """
    Calcula TAXA_MEDIA SOMENTE para os registros do pregão atual.
    Pregões anteriores não são tocados (foto histórica).
    """
    sql = text("""
        WITH UltimasDatas AS (
            SELECT DISTINCT TOP 12 [DT_ATUALIZACAO]
            FROM [BDG].[FIN_TB001_CUSTO_OPORTUNIDADE]
            ORDER BY [DT_ATUALIZACAO] DESC
        ),
        MediaPorAnoMes AS (
            SELECT
                CO.ANO_MES,
                AVG(CO.[VR_PRECO_MEDIA]) AS MEDIA
            FROM [BDG].[FIN_TB001_CUSTO_OPORTUNIDADE] CO
            INNER JOIN UltimasDatas SEM
                ON CO.[DT_ATUALIZACAO] = SEM.[DT_ATUALIZACAO]
            GROUP BY CO.ANO_MES
        )
        UPDATE CO
        SET CO.TAXA_MEDIA = M.MEDIA
        FROM [BDG].[FIN_TB001_CUSTO_OPORTUNIDADE] CO
        INNER JOIN MediaPorAnoMes M ON CO.ANO_MES = M.ANO_MES
        WHERE CO.DT_ATUALIZACAO = :dt_atualizacao;
    """)
    result = db.session.execute(sql, {'dt_atualizacao': dt_atualizacao})
    db.session.commit()
    return result.rowcount or 0


# =========================================================================
# HORIZONTE GLOBAL (limpa sobras retroativas)
# =========================================================================
def _aplicar_horizonte_meses_global():
    """
    Limpa contratos da tabela inteira que estejam além do horizonte de
    105 meses calculado individualmente para cada pregão (= primeiro
    ANO_MES daquele pregão + 104 meses).

    Idempotente: rodar várias vezes não muda o resultado depois de limpo.
    """
    sql = text("""
        ;WITH MinPorPregao AS (
            SELECT
                DT_ATUALIZACAO,
                MIN(CAST(ANO_MES AS INT)) AS ANO_MES_MIN
            FROM [BDG].[FIN_TB001_CUSTO_OPORTUNIDADE]
            WHERE ANO_MES IS NOT NULL
              AND LEN(ANO_MES) = 6
            GROUP BY DT_ATUALIZACAO
        ),
        LimitePorPregao AS (
            SELECT
                DT_ATUALIZACAO,
                ANO_MES_MIN,
                /* total_meses_zero_based + (HORIZONTE - 1) */
                ((ANO_MES_MIN / 100) * 12 + (ANO_MES_MIN % 100 - 1) + 104) AS TOTAL_NOVO
            FROM MinPorPregao
        ),
        LimiteFinal AS (
            SELECT
                DT_ATUALIZACAO,
                ((TOTAL_NOVO / 12) * 100 + (TOTAL_NOVO % 12 + 1)) AS LIMITE_ANO_MES
            FROM LimitePorPregao
        )
        DELETE CO
        FROM [BDG].[FIN_TB001_CUSTO_OPORTUNIDADE] CO
        INNER JOIN LimiteFinal LF
            ON CO.DT_ATUALIZACAO = LF.DT_ATUALIZACAO
        WHERE CAST(CO.ANO_MES AS INT) > LF.LIMITE_ANO_MES;
    """)
    result = db.session.execute(sql)
    db.session.commit()
    return result.rowcount or 0


# =========================================================================
# INTERPOLAÇÃO LINEAR (regras EMGEA: início, meio, fim)
# =========================================================================
def _interpolar_precos(registros):
    """
    Aplica interpolação linear nos registros com 'preco' = None.

    Regras EMGEA:
      INÍCIO : valor = V1 + k*(V1 - V2)         [V1=1º válido, V2=2º]
      MEIO   : valor = V_antes + i*(V_depois - V_antes)/(N+1)
      FIM    : valor = V_último + k*(V_último - V_penúltimo)

    Modifica in-place. Retorna quantidade interpolada.
    """
    n = len(registros)
    if n == 0:
        return 0

    idx_validos = [i for i, r in enumerate(registros) if r['preco'] is not None]

    if len(idx_validos) < 2:
        return 0

    interpolados = 0
    primeiro_valido = idx_validos[0]
    ultimo_valido = idx_validos[-1]

    # Início
    if primeiro_valido > 0:
        v1 = registros[idx_validos[0]]['preco']
        v2 = registros[idx_validos[1]]['preco']
        tendencia = v1 - v2
        for k, pos in enumerate(range(primeiro_valido - 1, -1, -1), start=1):
            valor = (v1 + Decimal(k) * tendencia).quantize(
                Decimal('0.0001'), rounding=ROUND_HALF_UP
            )
            registros[pos]['preco'] = valor
            interpolados += 1
            print(f'[CustoOportunidade] INTERP. INÍCIO: '
                  f'{registros[pos]["inst"]} = {valor}')

    # Meio
    for a, b in zip(idx_validos, idx_validos[1:]):
        gap = b - a - 1
        if gap <= 0:
            continue
        v_antes = registros[a]['preco']
        v_depois = registros[b]['preco']
        passo = (v_depois - v_antes) / Decimal(gap + 1)
        for i, pos in enumerate(range(a + 1, b), start=1):
            valor = (v_antes + Decimal(i) * passo).quantize(
                Decimal('0.0001'), rounding=ROUND_HALF_UP
            )
            registros[pos]['preco'] = valor
            interpolados += 1
            print(f'[CustoOportunidade] INTERP. MEIO: '
                  f'{registros[pos]["inst"]} = {valor}')

    # Fim
    if ultimo_valido < n - 1:
        v_ult = registros[idx_validos[-1]]['preco']
        v_pen = registros[idx_validos[-2]]['preco']
        tendencia = v_ult - v_pen
        for k, pos in enumerate(range(ultimo_valido + 1, n), start=1):
            valor = (v_ult + Decimal(k) * tendencia).quantize(
                Decimal('0.0001'), rounding=ROUND_HALF_UP
            )
            registros[pos]['preco'] = valor
            interpolados += 1
            print(f'[CustoOportunidade] INTERP. FIM: '
                  f'{registros[pos]["inst"]} = {valor}')

    return interpolados


# =========================================================================
# HELPERS
# =========================================================================
def _vencto_para_ordinal(inst_financ):
    """'DI1F27' → 202701 (int YYYYMM). None se inválido."""
    if not inst_financ or not inst_financ.startswith('DI1'):
        return None
    sufixo = inst_financ[3:]
    if len(sufixo) < 2:
        return None
    letra = sufixo[0].upper()
    ano_str = sufixo[1:]
    mes = MES_CONTRATO.get(letra)
    if mes is None:
        return None
    try:
        ano = int(ano_str) + 2000
    except ValueError:
        return None
    return ano * 100 + mes


def _somar_meses_em_ano_mes(ano_mes_int, qtd_meses):
    """
    Soma `qtd_meses` ao YYYYMM (int).
    Ex: _somar_meses_em_ano_mes(202605, 1) → 202606
        _somar_meses_em_ano_mes(202612, 1) → 202701
        _somar_meses_em_ano_mes(202605, 104) → 203501
    """
    if not ano_mes_int:
        return None
    ano = ano_mes_int // 100
    mes = ano_mes_int % 100
    if mes < 1 or mes > 12:
        return None
    total_meses = ano * 12 + (mes - 1) + qtd_meses
    novo_ano = total_meses // 12
    novo_mes = (total_meses % 12) + 1
    return novo_ano * 100 + novo_mes


def _converter_preco_br(valor):
    """Converte '29,3045' → Decimal('29.3045') com 4 casas. None se inválido."""
    if valor is None:
        return None
    texto = str(valor).strip()
    if not texto or texto in ('-', 'nan', 'NaN', 'None'):
        return None
    normalizado = texto.replace('.', '').replace(',', '.')
    try:
        return Decimal(normalizado).quantize(
            Decimal('0.0001'), rounding=ROUND_HALF_UP
        )
    except (InvalidOperation, ValueError):
        return None

def _calcular_taxa_media_mensal(dt_atualizacao):
    """
    Calcula e grava TAXA_MEDIA_MENSAL APENAS para os registros do pregão
    atual. Os pregões anteriores não são tocados (foto histórica).

    Fórmula:
      TAXA_MEDIA_MENSAL = ((1 + TAXA_MEDIA/100)^(1/12) - 1) * 100

    Conceito (matemática financeira):
      Se TAXA_MEDIA é uma taxa anual (% a.a.), TAXA_MEDIA_MENSAL é a
      taxa equivalente mensal (% a.m.) com capitalização composta.

    Retorna a quantidade de linhas atualizadas.
    """
    sql = text("""
        UPDATE [BDG].[FIN_TB001_CUSTO_OPORTUNIDADE]
        SET [TAXA_MEDIA_MENSAL] = (
            POWER((1 + ([TAXA_MEDIA] / 100.0)), (1.0 / 12.0)) - 1
        ) * 100
        WHERE [DT_ATUALIZACAO] = :dt_atualizacao
          AND [TAXA_MEDIA] IS NOT NULL;
    """)
    result = db.session.execute(sql, {'dt_atualizacao': dt_atualizacao})
    db.session.commit()
    return result.rowcount or 0


def _calcular_e_salvar_media_mensal_pregao(dt_atualizacao):
    """
    Calcula AVG(TAXA_MEDIA_MENSAL) do pregão atual e grava (ou atualiza)
    1 linha em FIN_TB003_CUSTO_OPORTUNIDADE_MEDIAS, junto com o
    CUSTO_DE_OPURTUNIDADE (taxa anualizada equivalente):

      MEDIA_MENSAL          = AVG(TAXA_MEDIA_MENSAL)
      CUSTO_DE_OPURTUNIDADE = ((1 + MEDIA_MENSAL/100)^12 - 1) * 100

    A flag REUNIAO NÃO é tocada (preserva valor anterior se já existia,
    ou usa o default 0 se for inserção nova).

    Retorna o valor da MEDIA_MENSAL gravada (Decimal) ou None se não
    houve registros válidos para calcular.
    """
    # 1. Calcular AVG(TAXA_MEDIA_MENSAL)
    sql_avg = text("""
        SELECT AVG([TAXA_MEDIA_MENSAL]) AS MEDIA
        FROM [BDG].[FIN_TB001_CUSTO_OPORTUNIDADE]
        WHERE [DT_ATUALIZACAO] = :dt_atualizacao
          AND [TAXA_MEDIA_MENSAL] IS NOT NULL;
    """)
    row = db.session.execute(
        sql_avg, {'dt_atualizacao': dt_atualizacao}
    ).fetchone()

    media = row[0] if row else None
    if media is None:
        return None

    # 2. MERGE em FIN_TB003 (upsert por DT_ATUALIZACAO)
    # A flag REUNIAO NÃO é alterada no UPDATE (preserva o valor que estava lá).
    # No INSERT, fica com o default da coluna (= 0).
    sql_merge = text("""
        MERGE [BDG].[FIN_TB003_CUSTO_OPORTUNIDADE_MEDIAS] AS dest
        USING (SELECT
                  :dt_atualizacao AS DT_ATUALIZACAO,
                  :media AS MEDIA_MENSAL,
                  ((POWER(1 + (:media / 100.0), 12) - 1) * 100) AS CUSTO
              ) AS src
            ON dest.DT_ATUALIZACAO = src.DT_ATUALIZACAO
        WHEN MATCHED THEN
            UPDATE SET
                dest.MEDIA_MENSAL          = src.MEDIA_MENSAL,
                dest.CUSTO_DE_OPURTUNIDADE = src.CUSTO
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (DT_ATUALIZACAO, MEDIA_MENSAL, CUSTO_DE_OPURTUNIDADE)
            VALUES (src.DT_ATUALIZACAO, src.MEDIA_MENSAL, src.CUSTO);
    """)
    db.session.execute(sql_merge, {
        'dt_atualizacao': dt_atualizacao,
        'media': media
    })
    db.session.commit()
    return media
# =========================================================================
# GERAR NOTA TÉCNICA (PDF)
# =========================================================================
@custo_oportunidade_bp.route('/nota-tecnica', methods=['GET'])
@login_required
def gerar_nota_tecnica():
    """
    Gera o PDF da Nota Técnica de Custo de Oportunidade.

    Query string:
      dt_atualizacao : YYYY-MM-DD - data do pregão de referência (atual)

    Lógica:
      1. Busca os 105 registros do pregão atual (FIN_TB001).
      2. Identifica o ÚLTIMO pregão marcado como COPOM (REUNIAO=1 em FIN_TB003).
      3. Busca os registros desse pregão COPOM em FIN_TB001.
      4. Pareia por ANO_MES e gera o PDF.

    Bloqueia (HTTP 400) se:
      - Não há nenhum pregão marcado como COPOM (Opção B confirmada).
      - dt_atualizacao não tem registros em FIN_TB001.
    """
    from flask import send_file, flash, redirect, url_for
    from app.models.custo_oportunidade_media import CustoOportunidadeMedia
    from app.utils.nota_tecnica_co_pdf import NotaTecnicaCustoOportunidadePDF

    # 1. Parse e validação do parâmetro
    dt_str = (request.args.get('dt_atualizacao') or '').strip()
    if not dt_str:
        flash('Selecione um pregão antes de gerar a Nota Técnica.', 'warning')
        return redirect(url_for('custo_oportunidade.index'))

    try:
        dt_pregao_atual = datetime.strptime(dt_str, '%Y-%m-%d').date()
    except ValueError:
        flash('Data inválida.', 'danger')
        return redirect(url_for('custo_oportunidade.index'))

    # 2. Buscar registros do pregão atual
    registros_atuais = CustoOportunidade.listar_por_data_atualizacao(dt_pregao_atual)
    if not registros_atuais:
        flash(
            f'Nenhum registro encontrado para o pregão de '
            f'{dt_pregao_atual.strftime("%d/%m/%Y")}.',
            'warning'
        )
        return redirect(url_for(
            'custo_oportunidade.index',
            dt_atualizacao=dt_str
        ))

    # Ordenar por ANO_MES (mesma ordem da tela)
    registros_atuais = sorted(
        registros_atuais,
        key=lambda r: (r.ANO_MES or '999999')
    )

    # 3. Identificar o ÚLTIMO pregão COPOM (REUNIAO=1) em FIN_TB003
    ultimo_copom = CustoOportunidadeMedia.query.filter_by(
        REUNIAO=True
    ).order_by(
        CustoOportunidadeMedia.DT_ATUALIZACAO.desc()
    ).first()

    # Opção B: bloqueia se não houver COPOM marcado
    if not ultimo_copom:
        flash(
            'Não é possível gerar a Nota Técnica: nenhum pregão foi marcado '
            'como Reunião COPOM ainda. Marque pelo menos um pregão antes.',
            'warning'
        )
        return redirect(url_for(
            'custo_oportunidade.index',
            dt_atualizacao=dt_str
        ))

    dt_pregao_copom = ultimo_copom.DT_ATUALIZACAO

    # 4. Buscar registros do pregão COPOM em FIN_TB001
    registros_copom = CustoOportunidade.listar_por_data_atualizacao(dt_pregao_copom)
    if not registros_copom:
        flash(
            f'O pregão COPOM marcado ({dt_pregao_copom.strftime("%d/%m/%Y")}) '
            f'não possui registros em FIN_TB001. Verifique a base.',
            'warning'
        )
        return redirect(url_for(
            'custo_oportunidade.index',
            dt_atualizacao=dt_str
        ))

    # 5. Gerar o PDF
    try:
        gerador = NotaTecnicaCustoOportunidadePDF()
        buffer = gerador.gerar(
            dt_pregao_atual=dt_pregao_atual,
            registros_atuais=registros_atuais,
            dt_pregao_copom=dt_pregao_copom,
            registros_copom=registros_copom,
        )
    except Exception as e:
        flash(f'Erro ao gerar PDF: {str(e)}', 'danger')
        return redirect(url_for(
            'custo_oportunidade.index',
            dt_atualizacao=dt_str
        ))

    # 6. Auditoria
    registrar_log(
        acao='gerar',
        entidade='custo_oportunidade_nota_tecnica',
        entidade_id=str(dt_pregao_atual),
        descricao=(
            f'Geração de Nota Técnica - pregão atual: '
            f'{dt_pregao_atual.strftime("%d/%m/%Y")}, '
            f'COPOM referência: {dt_pregao_copom.strftime("%d/%m/%Y")}'
        )
    )

    # 7. Enviar arquivo
    nome_arquivo = (
        f'Nota_Tecnica_Custo_Oportunidade_'
        f'{dt_pregao_atual.strftime("%d%m%Y")}.pdf'
    )

    return send_file(
        buffer,
        as_attachment=True,
        download_name=nome_arquivo,
        mimetype='application/pdf'
    )