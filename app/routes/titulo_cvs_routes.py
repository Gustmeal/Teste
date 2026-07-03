# app/routes/titulo_cvs_routes.py
"""
Rotas do módulo Título CVS.

Primeira parte (esta versão):
  - Upload de planilha Excel no padrão:
        AAAA-MM-DD_Planilhas Resumo CVS - Contrato NNN.xlsx
  - Extração de DT_ATUALIZACAO e NU_CONTRATO do nome do arquivo.
  - Leitura da primeira aba (ignora a linha TOTAL e a segunda tabela).
  - Inserção em BDG.FIN_TB006_RESUMO_CVS com MERGE/UPSERT por
    (DT_ATUALIZACAO, NU_CONTRATO, ATIVO).
  - DT_CARGA recebe a data do dia da carga.

Compatível com Python 3.9 e 3.12.
"""
from flask import (
    Blueprint, render_template, request, jsonify, make_response
)
from flask_login import login_required
from app import db
from app.models.titulo_cvs import ResumoCVS, OrigemDestinoCVS, ExtratoCVS
from app.utils.audit import registrar_log
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from sqlalchemy import text
import os
import re
import tempfile
from types import SimpleNamespace
from app.models.titulo_cvs import (
    ResumoCVS, OrigemDestinoCVS, ExtratoCVS, PosicaoEstoqueCVS,
    RecebimentoCVS,
)


titulo_cvs_bp = Blueprint(
    'titulo_cvs',
    __name__,
    url_prefix='/titulo-cvs'
)


# Regex do nome do arquivo:
# Aceita "_" OU espaço como separador. Ex:
#   2026-03-12_Planilhas Resumo CVS - Contrato 574.xlsx
#   2026-03-12_Planilhas_Resumo_CVS_-_Contrato_574.xlsx
REGEX_NOME_ARQUIVO = re.compile(
    r'^(?P<data>\d{4}-\d{2}-\d{2})[_ ]+Planilhas[_ ]+Resumo[_ ]+CVS'
    r'[_ ]+-[_ ]+Contrato[_ ]+(?P<contrato>\d+)\.(?:xlsx|xlsm)$',
    re.IGNORECASE
)


@titulo_cvs_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


# =========================================================================
# PÁGINA PRINCIPAL
# =========================================================================
@titulo_cvs_bp.route('/')
@login_required
def index():
    """
    Página principal do módulo Título CVS.
    Lista os registros já cadastrados em BDG.FIN_TB006_RESUMO_CVS.
    """
    db.session.expire_all()

    # Filtro opcional por DT_ATUALIZACAO
    dt_filtro_str = (request.args.get('dt_atualizacao') or '').strip()
    dt_filtro = None
    if dt_filtro_str:
        try:
            dt_filtro = datetime.strptime(dt_filtro_str, '%Y-%m-%d').date()
        except ValueError:
            dt_filtro = None

    # Datas disponíveis para o dropdown
    datas_disponiveis = ResumoCVS.listar_datas_atualizacao_distintas()

    # Se não passou filtro, usa a mais recente
    if not dt_filtro and datas_disponiveis:
        dt_filtro = datas_disponiveis[0]

    # Registros para exibir
    registros = []
    if dt_filtro:
        registros = ResumoCVS.listar_por_data_atualizacao(dt_filtro)

        # Estatísticas
        total_registros = ResumoCVS.contar_registros()
        total_contratos = ResumoCVS.contar_contratos_distintos()
        total_cargas = len(datas_disponiveis)

        # Última DT_ATUALIZACAO dos Índices do Dia 1 (FIN_TB015)
        sql_ultima_dt_indice = text("""
            SELECT MAX([DT_ATUALIZACAO])
            FROM [BDG].[FIN_TB015_INDICES_DIA_1]
        """)
        ultima_dt_indice = db.session.execute(sql_ultima_dt_indice).scalar()

        response = make_response(render_template(
            'titulo_cvs/index.html',
            registros=registros,
            datas_disponiveis=datas_disponiveis,
            dt_filtro=dt_filtro,
            total_registros=total_registros,
            total_contratos=total_contratos,
            total_cargas=total_cargas,
            ultima_dt_indice=ultima_dt_indice,
        ))
    response.headers['Cache-Control'] = (
        'no-store, no-cache, must-revalidate, max-age=0'
    )
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


# =========================================================================
# UPLOAD DE PLANILHA EXCEL
# =========================================================================
@titulo_cvs_bp.route('/upload', methods=['POST'])
@login_required
def upload_excel():
    """
    Recebe a planilha Excel, extrai DT_ATUALIZACAO e NU_CONTRATO do
    nome do arquivo, lê a primeira tabela e insere em FIN_TB007_RESUMO_CVS.
    """
    # 1. Validar envio
    if 'arquivo' not in request.files:
        return jsonify({
            'success': False,
            'message': 'Nenhum arquivo enviado.'
        }), 400

    arquivo = request.files['arquivo']
    nome_arquivo = (arquivo.filename or '').strip()

    if not nome_arquivo:
        return jsonify({
            'success': False,
            'message': 'Nome do arquivo está vazio.'
        }), 400

    if not nome_arquivo.lower().endswith(('.xlsx', '.xlsm')):
        return jsonify({
            'success': False,
            'message': 'Arquivo deve ser .xlsx ou .xlsm.'
        }), 400

    # 2. Extrair DT_ATUALIZACAO e NU_CONTRATO do nome
    match = REGEX_NOME_ARQUIVO.match(nome_arquivo)
    if not match:
        return jsonify({
            'success': False,
            'message': (
                'Nome do arquivo fora do padrão esperado. '
                'Use: AAAA-MM-DD_Planilhas Resumo CVS - Contrato NNN.xlsx'
            )
        }), 400

    try:
        dt_atualizacao = datetime.strptime(
            match.group('data'), '%Y-%m-%d'
        ).date()
    except ValueError:
        return jsonify({
            'success': False,
            'message': f'Data inválida no nome: {match.group("data")}.'
        }), 400

    try:
        nu_contrato = int(match.group('contrato'))
    except ValueError:
        return jsonify({
            'success': False,
            'message': (
                f'Número de contrato inválido no nome: '
                f'{match.group("contrato")}.'
            )
        }), 400

    # 3. Salvar temporariamente
    caminho_tmp = os.path.join(
        tempfile.gettempdir(),
        f'titulo_cvs_{datetime.now().strftime("%Y%m%d_%H%M%S")}_{nome_arquivo}'
    )
    try:
        arquivo.save(caminho_tmp)
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Falha ao salvar arquivo temporário: {e}'
        }), 500

    try:
        # 4. Processar planilha
        inseridos, atualizados, ignorados = _processar_excel(
            caminho_tmp, dt_atualizacao, nu_contrato
        )

        # 5. Auditoria
        registrar_log(
            acao='carga',
            entidade='titulo_cvs',
            entidade_id=None,
            descricao=(
                f'Upload de Resumo CVS - Contrato: {nu_contrato} - '
                f'DT_ATUALIZACAO: {dt_atualizacao.strftime("%d/%m/%Y")} - '
                f"EVENTO: E"
            ),
            dados_novos={
                'arquivo': nome_arquivo,
                'nu_contrato': nu_contrato,
                'dt_atualizacao': dt_atualizacao.strftime('%Y-%m-%d'),
                'evento': 'E',
                'registros_inseridos': inseridos,
                'registros_atualizados': atualizados,
                'registros_ignorados': ignorados,
            }
        )

        return jsonify({
            'success': True,
            'message': (
                f'Planilha carregada com sucesso! '
                f'{inseridos} inserido(s), '
                f'{atualizados} atualizado(s), '
                f'{ignorados} ignorado(s). '
                f'Contrato: {nu_contrato} - '
                f'Data: {dt_atualizacao.strftime("%d/%m/%Y")} - '
                f'EVENTO: E.'
            ),
            'nu_contrato': nu_contrato,
            'dt_atualizacao': dt_atualizacao.strftime('%d/%m/%Y'),
            'evento': 'E',
            'inseridos': inseridos,
            'atualizados': atualizados,
            'ignorados': ignorados,
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Erro ao processar planilha: {str(e)}'
        }), 500

    finally:
        # 6. Limpar arquivo temporário
        try:
            if os.path.exists(caminho_tmp):
                os.remove(caminho_tmp)
        except Exception:
            pass

# =========================================================================
# PROCESSAMENTO DO EXCEL
# =========================================================================
def _processar_excel(caminho_arquivo, dt_atualizacao, nu_contrato):
    """
    Lê APENAS a primeira tabela da primeira aba do Excel e faz UPSERT
    em BDG.FIN_TB007_RESUMO_CVS.

    Como o Excel tem uma segunda tabela abaixo da primeira (com ATIVO,
    Data VNA, VNA, Pu Juros, VNA Total), a leitura é interrompida
    no primeiro "buraco":
      - linha TOTAL  → para
      - linha com ATIVO vazio/NaN → para (separador entre as tabelas)

    Assim a segunda tabela nunca é processada.

    Estrutura esperada da PRIMEIRA tabela (linha 2 do Excel é o cabeçalho):
      Col A: Ativo
      Col B: Quantidade (Contrato)
      Col C: VNA em ...
      Col D: Financeiro em ...
      Col E: PU Retroativo de Juros em ...
      Col F: Financeiro Juros Vencidos em ... (R$)
      Col G: PU Retroativo de Principal em ...
      Col H: Financeiro Principal Vencido em ... (R$)
      Col I: Financeiro Vencido a Pagar em ... (R$)
      Col J: Financeiro Total (R$)

    Retorna (inseridos, atualizados, ignorados).
    """
    import pandas as pd

    # Lê a primeira aba; header na 2ª linha do Excel (índice 1)
    df = pd.read_excel(
        caminho_arquivo,
        sheet_name=0,
        header=1,
        engine='openpyxl'
    )

    if df.empty:
        raise Exception('Planilha está vazia.')

    # Pega só as 10 primeiras colunas
    df = df.iloc[:, :10]

    # Renomeia para nomes internos
    colunas_internas = [
        'ATIVO',
        'QTDE',
        'VNA',
        'FINANCEIRO',
        'PU_RETROATIVO_JUROS',
        'FINANCEIRO_JUROS',
        'PU_RETROATIVO_PRINC',
        'FINANCEIRO_PRINC',
        'FINANCEIRO_VENC_PAGAR',
        'TOTAL',
    ]
    if df.shape[1] < len(colunas_internas):
        raise Exception(
            f'Planilha tem apenas {df.shape[1]} colunas; '
            f'são esperadas {len(colunas_internas)}.'
        )
    df.columns = colunas_internas

    # Corte da primeira tabela
    linhas_validas = []
    for _, row in df.iterrows():
        ativo_raw = row.get('ATIVO')

        if pd.isna(ativo_raw):
            break

        ativo = str(ativo_raw).strip()
        if not ativo:
            break

        if ativo.upper() == 'TOTAL':
            break

        linhas_validas.append(row)

    if not linhas_validas:
        raise Exception(
            'Nenhuma linha válida encontrada na primeira tabela do Excel.'
        )

    df = pd.DataFrame(linhas_validas)

    dt_carga = datetime.now().date()
    inseridos = 0
    atualizados = 0
    ignorados = 0

    for _, row in df.iterrows():
        ativo = str(row['ATIVO']).strip()
        if not ativo:
            ignorados += 1
            continue

        qtde = _to_int(row.get('QTDE'))
        vna = _to_decimal(row.get('VNA'), casas=8)
        financeiro = _to_decimal(row.get('FINANCEIRO'), casas=2)
        pu_juros = _to_decimal(row.get('PU_RETROATIVO_JUROS'), casas=10)
        fin_juros = _to_decimal(row.get('FINANCEIRO_JUROS'), casas=2)
        pu_princ = _to_decimal(row.get('PU_RETROATIVO_PRINC'), casas=10)
        fin_princ = _to_decimal(row.get('FINANCEIRO_PRINC'), casas=2)
        fin_venc = _to_decimal(row.get('FINANCEIRO_VENC_PAGAR'), casas=2)
        total = _to_decimal(row.get('TOTAL'), casas=2)

        existente = ResumoCVS.query.filter_by(
            DT_ATUALIZACAO=dt_atualizacao,
            NU_CONTRATO=nu_contrato,
            ATIVO=ativo
        ).first()

        if existente:
            existente.EVENTO = 'E'
            existente.DT_CARGA = dt_carga
            existente.QTDE = qtde
            existente.VNA = vna
            existente.FINANCEIRO = financeiro
            existente.PU_RETROATIVO_JUROS = pu_juros
            existente.FINANCEIRO_JUROS = fin_juros
            existente.PU_RETROATIVO_PRINC = pu_princ
            existente.FINANCEIRO_PRINC = fin_princ
            existente.FINANCEIRO_VENC_PAGAR = fin_venc
            existente.TOTAL = total
            atualizados += 1
        else:
            novo = ResumoCVS(
                DT_CARGA=dt_carga,
                DT_ATUALIZACAO=dt_atualizacao,
                NU_CONTRATO=nu_contrato,
                ATIVO=ativo,
                EVENTO='E',
                QTDE=qtde,
                VNA=vna,
                FINANCEIRO=financeiro,
                PU_RETROATIVO_JUROS=pu_juros,
                FINANCEIRO_JUROS=fin_juros,
                PU_RETROATIVO_PRINC=pu_princ,
                FINANCEIRO_PRINC=fin_princ,
                FINANCEIRO_VENC_PAGAR=fin_venc,
                TOTAL=total,
            )
            db.session.add(novo)
            inseridos += 1

    db.session.commit()
    return inseridos, atualizados, ignorados


# =========================================================================
# HELPERS DE CONVERSÃO
# =========================================================================
def _to_decimal(valor, casas=2):
    """
    Converte para Decimal com `casas` casas decimais. Retorna None
    se vazio/NaN/inválido.
    """
    if valor is None:
        return None
    try:
        import math
        if isinstance(valor, float) and math.isnan(valor):
            return None
    except Exception:
        pass

    texto = str(valor).strip()
    if not texto or texto.lower() in ('nan', 'none', '-'):
        return None

    # Normaliza padrão brasileiro (se vier "1.234,56")
    if ',' in texto and '.' in texto:
        # Tem ambos: assume formato brasileiro (1.234,56)
        normalizado = texto.replace('.', '').replace(',', '.')
    elif ',' in texto:
        # Só vírgula: assume decimal brasileiro
        normalizado = texto.replace(',', '.')
    else:
        normalizado = texto

    try:
        formato = Decimal('1.' + ('0' * casas)) if casas > 0 else Decimal('1')
        return Decimal(normalizado).quantize(
            formato, rounding=ROUND_HALF_UP
        )
    except (InvalidOperation, ValueError):
        return None


def _to_int(valor):
    """Converte para int. Retorna None se inválido/NaN."""
    if valor is None:
        return None
    try:
        import math
        if isinstance(valor, float) and math.isnan(valor):
            return None
    except Exception:
        pass

    try:
        return int(float(str(valor).strip()))
    except (ValueError, TypeError):
        return None


# =========================================================================
# ORIGEM / DESTINO — HELPER PRIVADO
# =========================================================================
def _contar_origem_destino_completo(dt_atualizacao):
    """
    Conta total / pendentes / preenchidos usando o LEFT JOIN entre
    FIN_TB007_RESUMO_CVS (fonte da verdade) e FIN_TB008_ORIGEM_DESTINO_CVS
    (onde fica o ORIGEM_DESTINO).

    A contagem é sempre baseada na FIN_TB007 para refletir todos os
    ativos que precisam ter ORIGEM_DESTINO preenchido.

    Retorna (total, pendentes, preenchidos).
    """
    sql = text("""
        SELECT 
            COUNT(*) AS total,
            SUM(CASE WHEN B.[ORIGEM_DESTINO] IS NULL 
                       OR B.[ORIGEM_DESTINO] = ''
                     THEN 1 ELSE 0 END) AS pendentes
        FROM (
            SELECT DISTINCT
                   A.[DT_ATUALIZACAO],
                   A.[EVENTO],
                   SUBSTRING(A.[ATIVO], 4, 1) AS ATIVO
            FROM [BDG].[FIN_TB007_RESUMO_CVS] A
            WHERE A.[DT_ATUALIZACAO] = :dt
        ) X
        LEFT JOIN [BDG].[FIN_TB008_ORIGEM_DESTINO_CVS] B
          ON X.[DT_ATUALIZACAO] = B.[DT_ATUALIZACAO]
         AND X.[EVENTO]         = B.[EVENTO]
         AND X.[ATIVO]          = B.[ATIVO];
    """)
    row = db.session.execute(sql, {'dt': dt_atualizacao}).fetchone()
    if not row:
        return 0, 0, 0
    total = int(row[0] or 0)
    pendentes = int(row[1] or 0)
    preenchidos = total - pendentes
    return total, pendentes, preenchidos

# =========================================================================
# ORIGEM / DESTINO — PÁGINA PRINCIPAL
# =========================================================================
@titulo_cvs_bp.route('/origem-destino')
@login_required
def origem_destino_index():
    """
    Página de preenchimento de ORIGEM_DESTINO.

    Lista os ativos vindos da FIN_TB007_RESUMO_CVS (fonte da verdade)
    fazendo LEFT JOIN com FIN_TB008_ORIGEM_DESTINO_CVS para saber
    quais já foram preenchidos.
    """
    db.session.expire_all()

    # 1. Datas disponíveis COM contagem de pendências/total.
    #    Ordenadas: primeiro as que têm mais pendências (coisa a fazer),
    #    depois as concluídas. Base sempre a FIN_TB007 (fonte da verdade).
    sql_datas = text("""
        SELECT 
            X.[DT_ATUALIZACAO],
            COUNT(*) AS total,
            SUM(CASE WHEN B.[ORIGEM_DESTINO] IS NULL 
                       OR B.[ORIGEM_DESTINO] = ''
                     THEN 1 ELSE 0 END) AS pendentes
        FROM (
            SELECT DISTINCT
                   A.[DT_ATUALIZACAO],
                   A.[EVENTO],
                   SUBSTRING(A.[ATIVO], 4, 1) AS ATIVO
            FROM [BDG].[FIN_TB007_RESUMO_CVS] A
        ) X
        LEFT JOIN [BDG].[FIN_TB008_ORIGEM_DESTINO_CVS] B
          ON X.[DT_ATUALIZACAO] = B.[DT_ATUALIZACAO]
         AND X.[EVENTO]         = B.[EVENTO]
         AND X.[ATIVO]          = B.[ATIVO]
        GROUP BY X.[DT_ATUALIZACAO]
        ORDER BY pendentes DESC, X.[DT_ATUALIZACAO] DESC;
    """)
    rows_datas = db.session.execute(sql_datas).fetchall()

    datas_com_pendencia = []   # SimpleNamespace(.data, .total, .pendentes, .preenchidos)
    datas_completas = []
    datas_disponiveis = []     # lista simples de dates (compatibilidade / default)

    for r in rows_datas:
        dt_at = r[0]
        total_d = int(r[1] or 0)
        pend_d = int(r[2] or 0)
        pre_d = total_d - pend_d

        if dt_at is None:
            continue

        info = SimpleNamespace(
            data=dt_at,
            total=total_d,
            pendentes=pend_d,
            preenchidos=pre_d,
        )
        datas_disponiveis.append(dt_at)

        if pend_d > 0:
            datas_com_pendencia.append(info)
        else:
            datas_completas.append(info)

    # 2. Filtro de data
    dt_filtro_str = (request.args.get('dt_atualizacao') or '').strip()
    dt_filtro = None
    if dt_filtro_str:
        try:
            dt_filtro = datetime.strptime(dt_filtro_str, '%Y-%m-%d').date()
        except ValueError:
            dt_filtro = None

    # Default: primeira data da lista ordenada (a com mais pendências)
    if not dt_filtro and datas_disponiveis:
        dt_filtro = datas_disponiveis[0]

    # 3. Query principal com LEFT JOIN
    pendentes = []
    preenchidos = []
    if dt_filtro:
        sql_join = text("""
            SELECT DISTINCT
                   A.[DT_ATUALIZACAO],
                   A.[EVENTO],
                   SUBSTRING(A.[ATIVO], 4, 1) AS ATIVO,
                   B.[ORIGEM_DESTINO]
            FROM [BDG].[FIN_TB007_RESUMO_CVS] A
            LEFT JOIN [BDG].[FIN_TB008_ORIGEM_DESTINO_CVS] B
              ON A.[DT_ATUALIZACAO] = B.[DT_ATUALIZACAO]
             AND A.[EVENTO]         = B.[EVENTO]
             AND SUBSTRING(A.[ATIVO], 4, 1) = B.[ATIVO]
            WHERE A.[DT_ATUALIZACAO] = :dt
            ORDER BY ATIVO, A.[EVENTO];
        """)
        rows = db.session.execute(sql_join, {'dt': dt_filtro}).fetchall()

        for r in rows:
            item = SimpleNamespace(
                DT_ATUALIZACAO=r[0],
                EVENTO=r[1],
                ATIVO=r[2],
                ORIGEM_DESTINO=r[3],
            )
            if r[3] and str(r[3]).strip():
                preenchidos.append(item)
            else:
                pendentes.append(item)

    # 4. Estatísticas
    total = len(pendentes) + len(preenchidos)
    qtd_pendentes = len(pendentes)
    qtd_preenchidos = len(preenchidos)
    progresso_pct = (
        round((qtd_preenchidos / total) * 100, 1) if total > 0 else 0
    )

    response = make_response(render_template(
        'titulo_cvs/origem_destino.html',
        datas_disponiveis=datas_disponiveis,
        datas_com_pendencia=datas_com_pendencia,
        datas_completas=datas_completas,
        dt_filtro=dt_filtro,
        pendentes=pendentes,
        preenchidos=preenchidos,
        total=total,
        qtd_pendentes=qtd_pendentes,
        qtd_preenchidos=qtd_preenchidos,
        progresso_pct=progresso_pct,
    ))
    response.headers['Cache-Control'] = (
        'no-store, no-cache, must-revalidate, max-age=0'
    )
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


# =========================================================================
# ORIGEM / DESTINO — SALVAR UM ÚNICO REGISTRO (AJAX)
# =========================================================================
@titulo_cvs_bp.route('/origem-destino/salvar', methods=['POST'])
@login_required
def origem_destino_salvar():
    """
    Salva o ORIGEM_DESTINO de UM registro na FIN_TB008.

    UPSERT: se já existe registro na (DT_ATUALIZACAO, ATIVO, EVENTO),
    faz UPDATE. Se não existe, faz INSERT com DT_CARGA = hoje.

    Recebe JSON:
      {
        "dt_atualizacao": "YYYY-MM-DD",
        "ativo": "A",         (1 caractere - vem do SUBSTRING)
        "evento": "E",        (1 caractere)
        "origem_destino": "texto livre até 150 chars"
      }
    """
    try:
        dados = request.get_json(silent=True) or {}

        dt_str = (dados.get('dt_atualizacao') or '').strip()
        ativo = (dados.get('ativo') or '').strip()
        evento = (dados.get('evento') or '').strip()
        origem_destino = (dados.get('origem_destino') or '').strip()

        # Validação
        if not dt_str or not ativo or not evento:
            return jsonify({
                'success': False,
                'message': (
                    'Parâmetros obrigatórios: dt_atualizacao, ativo, evento.'
                )
            }), 400

        if not origem_destino:
            return jsonify({
                'success': False,
                'message': 'O campo Origem/Destino não pode ficar vazio.'
            }), 400

        if len(origem_destino) > 150:
            return jsonify({
                'success': False,
                'message': 'Origem/Destino excede 150 caracteres.'
            }), 400

        try:
            dt_atualizacao = datetime.strptime(dt_str, '%Y-%m-%d').date()
        except ValueError:
            return jsonify({
                'success': False,
                'message': f'Data inválida: {dt_str}.'
            }), 400

        # UPSERT
        registro = OrigemDestinoCVS.obter(dt_atualizacao, ativo, evento)

        if registro:
            # UPDATE
            valor_antigo = registro.ORIGEM_DESTINO
            registro.ORIGEM_DESTINO = origem_destino
            acao_log = 'editar'
        else:
            # INSERT
            valor_antigo = None
            novo = OrigemDestinoCVS(
                DT_CARGA=datetime.now().date(),
                DT_ATUALIZACAO=dt_atualizacao,
                ATIVO=ativo,
                EVENTO=evento,
                ORIGEM_DESTINO=origem_destino,
            )
            db.session.add(novo)
            acao_log = 'criar'

        db.session.commit()

        # Auditoria
        registrar_log(
            acao=acao_log,
            entidade='origem_destino_cvs',
            entidade_id=f'{ativo}/{evento}/{dt_str}',
            descricao=(
                f'{acao_log.capitalize()} ORIGEM_DESTINO - '
                f'ATIVO: {ativo}, EVENTO: {evento}, '
                f'DT_ATUALIZACAO: {dt_atualizacao.strftime("%d/%m/%Y")}'
            ),
            dados_antigos={'ORIGEM_DESTINO': valor_antigo},
            dados_novos={'ORIGEM_DESTINO': origem_destino}
        )

        # Progresso atualizado (usando o JOIN, baseado na FIN_TB007)
        total, qtd_pend, qtd_pre = _contar_origem_destino_completo(
            dt_atualizacao
        )
        progresso_pct = (
            round((qtd_pre / total) * 100, 1) if total > 0 else 0
        )

        return jsonify({
            'success': True,
            'message': (
                f'Origem/Destino salvo para {ativo} ({evento}).'
            ),
            'item': {
                'dt_atualizacao': dt_atualizacao.strftime('%Y-%m-%d'),
                'ativo': ativo,
                'evento': evento,
                'origem_destino': origem_destino,
            },
            'progresso': {
                'total': total,
                'pendentes': qtd_pend,
                'preenchidos': qtd_pre,
                'percentual': progresso_pct,
            }
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Erro ao salvar Origem/Destino: {str(e)}'
        }), 500



# =========================================================================
# ORIGEM / DESTINO — SALVAR LOTE (AJAX)
# =========================================================================
@titulo_cvs_bp.route('/origem-destino/salvar-lote', methods=['POST'])
@login_required
def origem_destino_salvar_lote():
    """
    Salva o ORIGEM_DESTINO de VÁRIOS registros de uma só vez.

    Cada item faz UPSERT individual na FIN_TB008.

    Recebe JSON:
      {
        "itens": [
          {
            "dt_atualizacao": "YYYY-MM-DD",
            "ativo": "A",
            "evento": "E",
            "origem_destino": "texto"
          },
          ...
        ]
      }
    """
    try:
        dados = request.get_json(silent=True) or {}
        itens = dados.get('itens') or []

        if not isinstance(itens, list) or len(itens) == 0:
            return jsonify({
                'success': False,
                'message': 'Nenhum item enviado para salvar.'
            }), 400

        inseridos = 0
        atualizados = 0
        ignorados = 0
        dt_atualizacao_ref = None

        for item in itens:
            dt_str = (item.get('dt_atualizacao') or '').strip()
            ativo = (item.get('ativo') or '').strip()
            evento = (item.get('evento') or '').strip()
            origem_destino = (item.get('origem_destino') or '').strip()

            if not dt_str or not ativo or not evento or not origem_destino:
                ignorados += 1
                continue

            if len(origem_destino) > 150:
                ignorados += 1
                continue

            try:
                dt_atualizacao = datetime.strptime(dt_str, '%Y-%m-%d').date()
            except ValueError:
                ignorados += 1
                continue

            dt_atualizacao_ref = dt_atualizacao

            # UPSERT
            registro = OrigemDestinoCVS.obter(
                dt_atualizacao, ativo, evento
            )
            if registro:
                registro.ORIGEM_DESTINO = origem_destino
                atualizados += 1
            else:
                novo = OrigemDestinoCVS(
                    DT_CARGA=datetime.now().date(),
                    DT_ATUALIZACAO=dt_atualizacao,
                    ATIVO=ativo,
                    EVENTO=evento,
                    ORIGEM_DESTINO=origem_destino,
                )
                db.session.add(novo)
                inseridos += 1

        db.session.commit()

        # Auditoria
        registrar_log(
            acao='editar_lote',
            entidade='origem_destino_cvs',
            entidade_id=None,
            descricao=(
                f'Preenchimento em lote de ORIGEM_DESTINO - '
                f'{inseridos} inserido(s), '
                f'{atualizados} atualizado(s)'
            ),
            dados_novos={
                'inseridos': inseridos,
                'atualizados': atualizados,
                'ignorados': ignorados,
            }
        )

        # Progresso
        progresso = {}
        if dt_atualizacao_ref:
            total, qtd_pend, qtd_pre = _contar_origem_destino_completo(
                dt_atualizacao_ref
            )
            progresso = {
                'total': total,
                'pendentes': qtd_pend,
                'preenchidos': qtd_pre,
                'percentual': (
                    round((qtd_pre / total) * 100, 1) if total > 0 else 0
                ),
            }

        return jsonify({
            'success': True,
            'message': (
                f'{inseridos + atualizados} registro(s) salvo(s) com sucesso '
                f'({inseridos} novo(s), {atualizados} atualizado(s)). '
                f'{ignorados} ignorado(s).'
            ),
            'inseridos': inseridos,
            'atualizados': atualizados,
            'ignorados': ignorados,
            'progresso': progresso,
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Erro ao salvar em lote: {str(e)}'
        }), 500


# =========================================================================
# ORIGEM / DESTINO — LIMPAR (voltar para pendente)
# =========================================================================
@titulo_cvs_bp.route('/origem-destino/limpar', methods=['POST'])
@login_required
def origem_destino_limpar():
    """
    DELETA o registro da FIN_TB008. Como a query principal usa
    LEFT JOIN com a FIN_TB007, ao deletar da FIN_TB008 o ativo
    volta naturalmente para a lista de pendentes (porque
    B.ORIGEM_DESTINO vira NULL no JOIN).

    Recebe JSON:
      {
        "dt_atualizacao": "YYYY-MM-DD",
        "ativo": "A",
        "evento": "E"
      }
    """
    try:
        dados = request.get_json(silent=True) or {}

        dt_str = (dados.get('dt_atualizacao') or '').strip()
        ativo = (dados.get('ativo') or '').strip()
        evento = (dados.get('evento') or '').strip()

        if not dt_str or not ativo or not evento:
            return jsonify({
                'success': False,
                'message': (
                    'Parâmetros obrigatórios: dt_atualizacao, ativo, evento.'
                )
            }), 400

        try:
            dt_atualizacao = datetime.strptime(dt_str, '%Y-%m-%d').date()
        except ValueError:
            return jsonify({
                'success': False,
                'message': f'Data inválida: {dt_str}.'
            }), 400

        registro = OrigemDestinoCVS.obter(dt_atualizacao, ativo, evento)
        if not registro:
            return jsonify({
                'success': False,
                'message': 'Registro não encontrado para limpar.'
            }), 404

        valor_antigo = registro.ORIGEM_DESTINO
        db.session.delete(registro)
        db.session.commit()

        registrar_log(
            acao='excluir',
            entidade='origem_destino_cvs',
            entidade_id=f'{ativo}/{evento}/{dt_str}',
            descricao=(
                f'Limpeza de ORIGEM_DESTINO (DELETE em FIN_TB008) - '
                f'ATIVO: {ativo}, EVENTO: {evento}'
            ),
            dados_antigos={'ORIGEM_DESTINO': valor_antigo},
            dados_novos=None
        )

        # Progresso (recalculado via JOIN com FIN_TB007)
        total, qtd_pend, qtd_pre = _contar_origem_destino_completo(
            dt_atualizacao
        )
        progresso_pct = (
            round((qtd_pre / total) * 100, 1) if total > 0 else 0
        )

        return jsonify({
            'success': True,
            'message': (
                f'Origem/Destino limpo para {ativo} ({evento}). '
                f'Voltou para pendentes.'
            ),
            'item': {
                'dt_atualizacao': dt_atualizacao.strftime('%Y-%m-%d'),
                'ativo': ativo,
                'evento': evento,
            },
            'progresso': {
                'total': total,
                'pendentes': qtd_pend,
                'preenchidos': qtd_pre,
                'percentual': progresso_pct,
            }
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Erro ao limpar Origem/Destino: {str(e)}'
        }), 500
# =========================================================================
# EXTRATO — HELPERS PRIVADOS
# =========================================================================
def _proximo_mes_dia_1(data_qualquer):
    """Retorna o primeiro dia do mês seguinte à data informada."""
    if data_qualquer.month == 12:
        return data_qualquer.replace(
            year=data_qualquer.year + 1, month=1, day=1
        )
    return data_qualquer.replace(month=data_qualquer.month + 1, day=1)


def _ultimo_dia_mes(data_qualquer):
    """Retorna o último dia do mês da data informada."""
    from calendar import monthrange
    ultimo = monthrange(data_qualquer.year, data_qualquer.month)[1]
    return data_qualquer.replace(day=ultimo)

def _primeiro_dia_util_mes(dt_qualquer_no_mes):
    """
    Retorna o primeiro dia útil do mês de dt_qualquer_no_mes,
    consultando BDG.PAR_TB020_CALENDARIO.

    Critério: menor [DIA] no mesmo (ano, mês) com [DIA_UTIL] = 1.
    Se nenhum dia útil for encontrado (cenário improvável), retorna
    o próprio dia 1 do mês como fallback.
    """
    primeiro_do_mes = dt_qualquer_no_mes.replace(day=1)
    ultimo_do_mes = _ultimo_dia_mes(dt_qualquer_no_mes)

    sql = text("""
        SELECT TOP 1 [DIA]
        FROM [BDG].[PAR_TB020_CALENDARIO]
        WHERE [DIA] BETWEEN :dt_ini AND :dt_fim
          AND [DIA_UTIL] = 1
        ORDER BY [DIA] ASC;
    """)
    row = db.session.execute(
        sql,
        {'dt_ini': primeiro_do_mes, 'dt_fim': ultimo_do_mes}
    ).fetchone()

    if row and row[0] is not None:
        valor = row[0]
        # Pode vir como date ou datetime — normaliza para date
        if hasattr(valor, 'date'):
            return valor.date()
        return valor

    # Fallback: dia 1 (caso o calendário esteja vazio no mês)
    return primeiro_do_mes

def _proxima_ordem_no_mes(primeiro_dia_mes, ultimo_dia_mes):
    """Retorna MAX(ORDEM) + 1 dentro do intervalo de datas."""
    from sqlalchemy import func
    max_ordem = db.session.query(
        func.max(ExtratoCVS.ORDEM)
    ).filter(
        ExtratoCVS.DT_MOVIMENTACAO >= primeiro_dia_mes,
        ExtratoCVS.DT_MOVIMENTACAO <= ultimo_dia_mes,
    ).scalar()
    return (int(max_ordem) + 1) if max_ordem is not None else 1


def _gerar_historico_estorno(historico_original):
    """
    Substitui 'Provisão' (ou 'Provisao' sem acento) do início por
    'Estorno', mantendo o resto do texto intacto.
    """
    if not historico_original:
        return historico_original
    match = re.match(
        r'^Provis[ãa]o(.*)$',
        historico_original,
        flags=re.IGNORECASE
    )
    if match:
        return 'Estorno' + match.group(1)
    return historico_original

def _proximo_nu_linha_extrato():
    """
    Retorna MAX(NU_LINHA) + 1 da tabela FIN_TB013_EXTRATO_CVS.
    Como NU_LINHA é NOT NULL e não é gerada automaticamente,
    precisamos calcular manualmente para cada INSERT.
    """
    from sqlalchemy import func
    max_nu = db.session.query(
        func.max(ExtratoCVS.NU_LINHA)
    ).scalar()
    return (int(max_nu) + 1) if max_nu is not None else 1

# =========================================================================
# EXTRATO — PÁGINA PRINCIPAL
# =========================================================================
# =========================================================================
# EXTRATO — PÁGINA PRINCIPAL
# =========================================================================
@titulo_cvs_bp.route('/extrato')
@login_required
def extrato_index():
    """
    Página principal do Extrato CVS.
    Mostra status, ações disponíveis e a lista de movimentações
    filtradas por mês.
    """
    db.session.expire_all()

    # 1. Último mês disponível na tabela
    ultima_data = ExtratoCVS.obter_ultima_data_movimentacao()
    primeiro_dia_ultimo_mes = None
    ultimo_dia_ultimo_mes = None
    proximo_mes_destino = None

    if ultima_data:
        primeiro_dia_ultimo_mes = ultima_data.replace(day=1)
        ultimo_dia_ultimo_mes = _ultimo_dia_mes(ultima_data)
        proximo_mes_destino = _proximo_mes_dia_1(ultima_data)

    # 2. Contagens gerais
    total_movimentacoes = ExtratoCVS.query.count()
    qtd_provisoes_ultimo_mes = 0
    qtd_movimentacoes_destino = 0

    if primeiro_dia_ultimo_mes:
        qtd_provisoes_ultimo_mes = ExtratoCVS.query.filter(
            ExtratoCVS.DT_MOVIMENTACAO >= primeiro_dia_ultimo_mes,
            ExtratoCVS.DT_MOVIMENTACAO <= ultimo_dia_ultimo_mes,
            db.or_(
                ExtratoCVS.HISTORICO.like('Provisão%'),
                ExtratoCVS.HISTORICO.like('Provisao%')
            )
        ).count()

        ultimo_dia_destino = _ultimo_dia_mes(proximo_mes_destino)
        qtd_movimentacoes_destino = ExtratoCVS.query.filter(
            ExtratoCVS.DT_MOVIMENTACAO >= proximo_mes_destino,
            ExtratoCVS.DT_MOVIMENTACAO <= ultimo_dia_destino
        ).count()

    # 3. Quantidade na FIN_TB014_INCORPORACOES_MES_CVS
    qtd_incorporacoes = db.session.execute(
        text("SELECT COUNT(*) FROM [BDG].[FIN_TB014_INCORPORACOES_MES_CVS]")
    ).scalar() or 0

    # 4. AJUSTE pendente na FIN_VW011 para o último mês com dados
    #    (usado pelo botão de "Ajustar Saldo")
    ajuste_pendente = None
    if primeiro_dia_ultimo_mes:
        ano_mes_ajuste = int(primeiro_dia_ultimo_mes.strftime('%Y%m'))
        row_ajuste = db.session.execute(
            text("""
                SELECT [AJUSTE]
                FROM [BDG].[FIN_VW011_AJUSTE_SALDO_PROV_JUROS_CVS]
                WHERE [ANO_MES] = :ano_mes;
            """),
            {'ano_mes': ano_mes_ajuste}
        ).fetchone()
        if row_ajuste and row_ajuste[0] is not None:
            ajuste_pendente = Decimal(str(row_ajuste[0]))

    # 5. Filtro de mês para listagem
    mes_filtro_str = (request.args.get('mes') or '').strip()
    mes_filtro = None
    if mes_filtro_str:
        try:
            mes_filtro = datetime.strptime(
                mes_filtro_str, '%Y-%m-%d'
            ).date()
        except ValueError:
            mes_filtro = None

    if not mes_filtro and ultima_data:
        mes_filtro = primeiro_dia_ultimo_mes

    # 6. Listar meses distintos
    meses_disponiveis = ExtratoCVS.listar_meses_distintos()

    # 7. Buscar movimentações do mês filtrado
    movimentacoes = []
    if mes_filtro:
        movimentacoes = ExtratoCVS.listar_por_mes(
            mes_filtro,
            _ultimo_dia_mes(mes_filtro)
        )

    response = make_response(render_template(
        'titulo_cvs/extrato.html',
        movimentacoes=movimentacoes,
        meses_disponiveis=meses_disponiveis,
        mes_filtro=mes_filtro,
        ultima_data=ultima_data,
        primeiro_dia_ultimo_mes=primeiro_dia_ultimo_mes,
        proximo_mes_destino=proximo_mes_destino,
        total_movimentacoes=total_movimentacoes,
        qtd_provisoes_ultimo_mes=qtd_provisoes_ultimo_mes,
        qtd_movimentacoes_destino=qtd_movimentacoes_destino,
        qtd_incorporacoes=qtd_incorporacoes,
        ajuste_pendente=ajuste_pendente,
    ))
    response.headers['Cache-Control'] = (
        'no-store, no-cache, must-revalidate, max-age=0'
    )
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

# =========================================================================
# EXTRATO — PROCESSAR PROVISÕES (gera próximo mês completo)
# =========================================================================
@titulo_cvs_bp.route('/extrato/processar-provisoes', methods=['POST'])
@login_required
def extrato_processar_provisoes():
    """
    Processa o próximo mês do extrato em 7 etapas, dentro de uma
    única transação (se algo falhar, rollback total):

    ETAPA 1 — Estornos.
    ETAPA 2 — Incorporações (FIN_TB014).
    ETAPA 3 — Recebimentos IR (FIN_VW006).
    ETAPA 4 — Entrada Títulos Pro Rata (FIN_VW007).
    ETAPA 5 — Provisão ATM/Juros (FIN_VW008) — cada linha vira 2.
    ETAPA 6 — Provisão Pro Rata (FIN_VW010) — cada linha vira 2.
    ETAPA 7 — Recálculo de VR_SALDO.

    O AJUSTE de Saldo de Provisão de Juros (FIN_VW011) NÃO é aplicado
    aqui — ele fica na rota separada /extrato/ajustar-saldo, acionada
    pelo botão "Ajustar Saldo" na tela do extrato.

    NU_LINHA é IDENTITY (gerado pelo banco).
    """
    try:
        # =================================================================
        # 1. Resolver mês origem e mês destino
        # =================================================================
        ultima_data = ExtratoCVS.obter_ultima_data_movimentacao()
        if not ultima_data:
            return jsonify({
                'success': False,
                'message': (
                    'A tabela FIN_TB013_EXTRATO_CVS está vazia. '
                    'Nada para processar.'
                )
            }), 400

        primeiro_dia_ultimo_mes = ultima_data.replace(day=1)
        ultimo_dia_ultimo_mes = _ultimo_dia_mes(ultima_data)
        dt_movimentacao_nova = _proximo_mes_dia_1(ultima_data)
        ultimo_dia_destino = _ultimo_dia_mes(dt_movimentacao_nova)

        # =================================================================
        # 2. Já tem coisa no mês destino? Aborta.
        # =================================================================
        ja_existe = ExtratoCVS.query.filter(
            ExtratoCVS.DT_MOVIMENTACAO >= dt_movimentacao_nova,
            ExtratoCVS.DT_MOVIMENTACAO <= ultimo_dia_destino
        ).count()
        if ja_existe > 0:
            return jsonify({
                'success': False,
                'message': (
                    f'Já existem {ja_existe} movimentação(ões) em '
                    f'{dt_movimentacao_nova.strftime("%m/%Y")}. '
                    f'Apague-as primeiro para reprocessar.'
                )
            }), 400

        # =================================================================
        # 3. Provisões do último mês
        # =================================================================
        provisoes = ExtratoCVS.query.filter(
            ExtratoCVS.DT_MOVIMENTACAO >= primeiro_dia_ultimo_mes,
            ExtratoCVS.DT_MOVIMENTACAO <= ultimo_dia_ultimo_mes,
            db.or_(
                ExtratoCVS.HISTORICO.like('Provisão%'),
                ExtratoCVS.HISTORICO.like('Provisao%')
            )
        ).order_by(ExtratoCVS.ORDEM.asc()).all()

        provisoes_para_processar = [p for p in provisoes if p.HISTORICO]
        provisoes_ja_estornadas = 0  # mantido por compatibilidade

        # =================================================================
        # 4. Incorporações (FIN_TB014) — filtradas pelo mês/ano destino
        # =================================================================
        sql_incorporacoes = text("""
            SELECT 
                [TIPO],
                [HISTORICO],
                [PERIODO_DE],
                [PERIODO_ATE],
                [VR_MOVIMENTACAO]
            FROM [BDG].[FIN_TB014_INCORPORACOES_MES_CVS]
            WHERE YEAR([DT_MOVIMENTACAO])  = YEAR(:dt_destino)
              AND MONTH([DT_MOVIMENTACAO]) = MONTH(:dt_destino)
            ORDER BY [DT_MOVIMENTACAO], [TIPO];
        """)
        incorporacoes = db.session.execute(
            sql_incorporacoes, {'dt_destino': dt_movimentacao_nova}
        ).fetchall()

        # =================================================================
        # 5. Recebimentos IR (FIN_VW006)
        # =================================================================
        sql_recebimentos_ir = text("""
            SELECT
                [DT_PREV_RECEBIMENTO],
                [TIPO],
                [HISTORICO],
                [PERIODO_DE],
                [PERIODO_ATE],
                [MOVIMENTACAO]
            FROM [BDDASHBOARDBI].[BDG].[FIN_VW006_RECEBIMENTO_IR_CVS]
            WHERE YEAR([DT_PREV_RECEBIMENTO])  = YEAR(:dt_destino)
              AND MONTH([DT_PREV_RECEBIMENTO]) = MONTH(:dt_destino)
            ORDER BY [DT_PREV_RECEBIMENTO], [TIPO];
        """)
        recebimentos_ir = db.session.execute(
            sql_recebimentos_ir, {'dt_destino': dt_movimentacao_nova}
        ).fetchall()

        # =================================================================
        # 6. Entrada Títulos Pro Rata (FIN_VW007)
        # =================================================================
        sql_entrada_pro_rata = text("""
            SELECT
                [DT_ATUALIZACAO],
                [TIPO],
                [HISTORICO],
                [PU_ATU_PRO],
                [VR_JR_PRORATA],
                [QTDE_TITULOS]
            FROM [BDG].[FIN_VW007_ENTRADA_TITULOS_PRORATA_CVS]
            WHERE YEAR([DT_ATUALIZACAO])  = YEAR(:dt_destino)
              AND MONTH([DT_ATUALIZACAO]) = MONTH(:dt_destino)
            ORDER BY [DT_ATUALIZACAO], [TIPO];
        """)
        entradas_pro_rata = db.session.execute(
            sql_entrada_pro_rata, {'dt_destino': dt_movimentacao_nova}
        ).fetchall()

        # =================================================================
        # 7. Provisão ATM/Juros (FIN_VW008) — cada linha vira 2 no extrato
        # =================================================================
        sql_provisao_atm_juros = text("""
            SELECT
                [DT_ATUALIZACAO],
                [PERIODO_DE],
                [PERIODO_ATE],
                [TIPO],
                [HISTORICO1],
                [MOVIMENTACAO1],
                [HISTORICO2],
                [MOVIMENTACAO2]
            FROM [BDG].[FIN_VW008_PROVISAO_ATM_JUROS_CVS]
            WHERE YEAR([DT_ATUALIZACAO])  = YEAR(:dt_destino)
              AND MONTH([DT_ATUALIZACAO]) = MONTH(:dt_destino)
            ORDER BY [DT_ATUALIZACAO], [TIPO];
        """)
        provisoes_atm_juros = db.session.execute(
            sql_provisao_atm_juros, {'dt_destino': dt_movimentacao_nova}
        ).fetchall()

        # =================================================================
        # 8. Provisão Pro Rata (FIN_VW010) — cada linha vira 2 no extrato
        # =================================================================
        sql_provisao_pro_rata = text("""
            SELECT
                [DT_ATUALIZACAO],
                [PERIODO_DE],
                [PERIODO_ATE],
                [TIPO],
                [HISTORICO],
                [MOVIMENTACAO],
                [HISTORICO2],
                [MOVIMENTACAO2]
            FROM [BDG].[FIN_VW010_PROVISAO_PRORATA_CVS]
            WHERE YEAR([DT_ATUALIZACAO])  = YEAR(:dt_destino)
              AND MONTH([DT_ATUALIZACAO]) = MONTH(:dt_destino)
            ORDER BY [DT_ATUALIZACAO], [TIPO];
        """)
        provisoes_pro_rata = db.session.execute(
            sql_provisao_pro_rata, {'dt_destino': dt_movimentacao_nova}
        ).fetchall()

        # =================================================================
        # 9. Validar que há algo a fazer
        # =================================================================
        if (not provisoes_para_processar
                and not incorporacoes
                and not recebimentos_ir
                and not entradas_pro_rata
                and not provisoes_atm_juros
                and not provisoes_pro_rata):
            return jsonify({
                'success': False,
                'message': (
                    'Nenhum dado encontrado para processar em '
                    f'{dt_movimentacao_nova.strftime("%m/%Y")}. '
                    '(Provisões, Incorporações, Recebimentos IR, Entrada '
                    'Pro Rata, Provisão ATM/Juros e Provisão Pro Rata '
                    'estão todas vazias.)'
                )
            }), 400

        # =================================================================
        # 10. ORDEM inicial e validação de limite smallint
        # =================================================================
        proxima_ordem = _proxima_ordem_no_mes(
            dt_movimentacao_nova, ultimo_dia_destino
        )
        total_a_inserir = (
            len(provisoes_para_processar)
            + len(incorporacoes)
            + len(recebimentos_ir)
            + len(entradas_pro_rata)
            + (len(provisoes_atm_juros) * 2)
            + (len(provisoes_pro_rata) * 2)
        )
        ordem_final_estimada = proxima_ordem + total_a_inserir
        if ordem_final_estimada > 32767:
            return jsonify({
                'success': False,
                'message': (
                    f'ORDEM final estimada ({ordem_final_estimada}) '
                    f'ultrapassa o limite de smallint (32767).'
                )
            }), 400

        dt_carga_hoje = datetime.now().date()
        inseridas_estorno = 0
        inseridas_incorporacao = 0
        inseridas_recebimento = 0
        inseridas_entrada_pro_rata = 0
        inseridas_provisao_atm_juros = 0
        inseridas_provisao_pro_rata = 0

        # =================================================================
        # ETAPA 1 — INSERIR ESTORNOS
        # DT_MOVIMENTACAO = primeiro dia útil do mês destino, conforme
        # BDG.PAR_TB020_CALENDARIO (coluna DIA_UTIL = 1).
        # =================================================================
        dt_estorno = _primeiro_dia_util_mes(dt_movimentacao_nova)
        for prov in provisoes_para_processar:
            historico_estorno = _gerar_historico_estorno(prov.HISTORICO)
            valor_estorno = (
                -prov.VR_MOVIMENTACAO
                if prov.VR_MOVIMENTACAO is not None
                else None
            )
            db.session.add(ExtratoCVS(
                DT_CARGA=dt_carga_hoje,
                DT_MOVIMENTACAO=dt_estorno,
                TIPO=prov.TIPO,
                ORDEM=proxima_ordem,
                HISTORICO=historico_estorno,
                PERIODO_DE=prov.PERIODO_DE,
                PERIODO_ATE=prov.PERIODO_ATE,
                VR_MOVIMENTACAO=valor_estorno,
                VR_SALDO=Decimal('0'),
            ))
            proxima_ordem += 1
            inseridas_estorno += 1

        # =================================================================
        # ETAPA 2 — INSERIR INCORPORAÇÕES
        # =================================================================
        for inc in incorporacoes:
            db.session.add(ExtratoCVS(
                DT_CARGA=dt_carga_hoje,
                DT_MOVIMENTACAO=dt_movimentacao_nova,
                TIPO=inc.TIPO,
                ORDEM=proxima_ordem,
                HISTORICO=inc.HISTORICO,
                PERIODO_DE=inc.PERIODO_DE,
                PERIODO_ATE=inc.PERIODO_ATE,
                VR_MOVIMENTACAO=inc.VR_MOVIMENTACAO,
                VR_SALDO=Decimal('0'),
            ))
            proxima_ordem += 1
            inseridas_incorporacao += 1

        # =================================================================
        # ETAPA 3 — INSERIR RECEBIMENTOS IR
        # =================================================================
        for rec in recebimentos_ir:
            db.session.add(ExtratoCVS(
                DT_CARGA=dt_carga_hoje,
                DT_MOVIMENTACAO=rec.DT_PREV_RECEBIMENTO,
                TIPO=rec.TIPO,
                ORDEM=proxima_ordem,
                HISTORICO=rec.HISTORICO,
                PERIODO_DE=rec.PERIODO_DE,
                PERIODO_ATE=rec.PERIODO_ATE,
                VR_MOVIMENTACAO=rec.MOVIMENTACAO,
                VR_SALDO=Decimal('0'),
            ))
            proxima_ordem += 1
            inseridas_recebimento += 1

        # =================================================================
        # ETAPA 4 — INSERIR ENTRADA TÍTULOS PRO RATA (FIN_VW007)
        # =================================================================
        for ent in entradas_pro_rata:
            vr_movimentacao_entrada = None
            if (ent.PU_ATU_PRO is not None
                    and ent.VR_JR_PRORATA is not None
                    and ent.QTDE_TITULOS is not None):
                pu = Decimal(str(ent.PU_ATU_PRO))
                jr = Decimal(str(ent.VR_JR_PRORATA))
                qtd = Decimal(str(ent.QTDE_TITULOS))
                vr_movimentacao_entrada = (pu + jr) * qtd

            db.session.add(ExtratoCVS(
                DT_CARGA=dt_carga_hoje,
                DT_MOVIMENTACAO=ent.DT_ATUALIZACAO,
                TIPO=ent.TIPO,
                ORDEM=proxima_ordem,
                HISTORICO=ent.HISTORICO,
                PERIODO_DE=ent.DT_ATUALIZACAO,
                PERIODO_ATE=ent.DT_ATUALIZACAO,
                VR_MOVIMENTACAO=vr_movimentacao_entrada,
                VR_SALDO=Decimal('0'),
            ))
            proxima_ordem += 1
            inseridas_entrada_pro_rata += 1

        # =================================================================
        # ETAPA 5 — INSERIR PROVISÃO ATM/JUROS (FIN_VW008)
        # =================================================================
        for prv in provisoes_atm_juros:
            db.session.add(ExtratoCVS(
                DT_CARGA=dt_carga_hoje,
                DT_MOVIMENTACAO=prv.DT_ATUALIZACAO,
                TIPO=prv.TIPO,
                ORDEM=proxima_ordem,
                HISTORICO=prv.HISTORICO1,
                PERIODO_DE=prv.PERIODO_DE,
                PERIODO_ATE=prv.PERIODO_ATE,
                VR_MOVIMENTACAO=prv.MOVIMENTACAO1,
                VR_SALDO=Decimal('0'),
            ))
            proxima_ordem += 1
            inseridas_provisao_atm_juros += 1

            db.session.add(ExtratoCVS(
                DT_CARGA=dt_carga_hoje,
                DT_MOVIMENTACAO=prv.DT_ATUALIZACAO,
                TIPO=prv.TIPO,
                ORDEM=proxima_ordem,
                HISTORICO=prv.HISTORICO2,
                PERIODO_DE=prv.PERIODO_DE,
                PERIODO_ATE=prv.PERIODO_ATE,
                VR_MOVIMENTACAO=prv.MOVIMENTACAO2,
                VR_SALDO=Decimal('0'),
            ))
            proxima_ordem += 1
            inseridas_provisao_atm_juros += 1

        # =================================================================
        # ETAPA 6 — INSERIR PROVISÃO PRO RATA (FIN_VW010)
        # =================================================================
        for prv in provisoes_pro_rata:
            db.session.add(ExtratoCVS(
                DT_CARGA=dt_carga_hoje,
                DT_MOVIMENTACAO=prv.DT_ATUALIZACAO,
                TIPO=prv.TIPO,
                ORDEM=proxima_ordem,
                HISTORICO=prv.HISTORICO,
                PERIODO_DE=prv.PERIODO_DE,
                PERIODO_ATE=prv.PERIODO_ATE,
                VR_MOVIMENTACAO=prv.MOVIMENTACAO,
                VR_SALDO=Decimal('0'),
            ))
            proxima_ordem += 1
            inseridas_provisao_pro_rata += 1

            db.session.add(ExtratoCVS(
                DT_CARGA=dt_carga_hoje,
                DT_MOVIMENTACAO=prv.DT_ATUALIZACAO,
                TIPO=prv.TIPO,
                ORDEM=proxima_ordem,
                HISTORICO=prv.HISTORICO2,
                PERIODO_DE=prv.PERIODO_DE,
                PERIODO_ATE=prv.PERIODO_ATE,
                VR_MOVIMENTACAO=prv.MOVIMENTACAO2,
                VR_SALDO=Decimal('0'),
            ))
            proxima_ordem += 1
            inseridas_provisao_pro_rata += 1

        db.session.flush()

        # =================================================================
        # ETAPA 7 — RECALCULAR VR_SALDO
        # =================================================================
        sql_saldo_inicial = text("""
            SELECT TOP 1 [VR_SALDO]
            FROM [BDG].[FIN_TB013_EXTRATO_CVS]
            WHERE [DT_MOVIMENTACAO] < :dt_destino
              AND [VR_SALDO] IS NOT NULL
            ORDER BY [DT_MOVIMENTACAO] DESC, [ORDEM] DESC;
        """)
        row_saldo = db.session.execute(
            sql_saldo_inicial, {'dt_destino': dt_movimentacao_nova}
        ).fetchone()
        saldo_anterior = (
            Decimal(str(row_saldo[0]))
            if row_saldo and row_saldo[0] is not None
            else Decimal('0')
        )
        saldo_inicial_referencia = saldo_anterior

        linhas_destino = ExtratoCVS.query.filter(
            ExtratoCVS.DT_MOVIMENTACAO >= dt_movimentacao_nova,
            ExtratoCVS.DT_MOVIMENTACAO <= ultimo_dia_destino
        ).order_by(
            ExtratoCVS.DT_MOVIMENTACAO.asc(),
            ExtratoCVS.ORDEM.asc()
        ).all()

        saldos_recalculados = 0
        for linha in linhas_destino:
            movimentacao = (
                Decimal(str(linha.VR_MOVIMENTACAO))
                if linha.VR_MOVIMENTACAO is not None
                else Decimal('0')
            )
            novo_saldo = saldo_anterior + movimentacao
            linha.VR_SALDO = novo_saldo
            saldo_anterior = novo_saldo
            saldos_recalculados += 1

        # Commit final (toda a transação)
        db.session.commit()

        # =================================================================
        # 11. Auditoria
        # =================================================================
        registrar_log(
            acao='carga',
            entidade='extrato_cvs',
            entidade_id=dt_movimentacao_nova.strftime('%Y-%m-%d'),
            descricao=(
                f'Processamento completo do mês '
                f'{dt_movimentacao_nova.strftime("%m/%Y")}: '
                f'{inseridas_estorno} estorno(s), '
                f'{inseridas_incorporacao} incorporação(ões), '
                f'{inseridas_recebimento} recebimento(s) IR, '
                f'{inseridas_entrada_pro_rata} entrada(s) pro rata, '
                f'{inseridas_provisao_atm_juros} provisão(ões) ATM/Juros, '
                f'{inseridas_provisao_pro_rata} provisão(ões) pro rata, '
                f'{saldos_recalculados} saldo(s) recalculado(s).'
            ),
            dados_novos={
                'mes_origem': primeiro_dia_ultimo_mes.strftime('%Y-%m'),
                'mes_destino': dt_movimentacao_nova.strftime('%Y-%m'),
                'provisoes_consideradas': len(provisoes),
                'provisoes_ja_estornadas': provisoes_ja_estornadas,
                'estornos_inseridos': inseridas_estorno,
                'incorporacoes_inseridas': inseridas_incorporacao,
                'recebimentos_ir_inseridos': inseridas_recebimento,
                'entrada_pro_rata_inseridas': inseridas_entrada_pro_rata,
                'provisao_atm_juros_inseridas': inseridas_provisao_atm_juros,
                'provisao_pro_rata_inseridas': inseridas_provisao_pro_rata,
                'saldos_recalculados': saldos_recalculados,
                'saldo_inicial': str(saldo_inicial_referencia),
                'saldo_final': str(saldo_anterior),
            }
        )

        return jsonify({
            'success': True,
            'message': (
                f'Processamento concluído em '
                f'{dt_movimentacao_nova.strftime("%d/%m/%Y")}: '
                f'{inseridas_estorno} estorno(s), '
                f'{inseridas_incorporacao} incorporação(ões), '
                f'{inseridas_recebimento} recebimento(s) IR, '
                f'{inseridas_entrada_pro_rata} entrada(s) pro rata, '
                f'{inseridas_provisao_atm_juros} provisão(ões) ATM/Juros, '
                f'{inseridas_provisao_pro_rata} provisão(ões) pro rata, '
                f'{saldos_recalculados} saldo(s) recalculado(s).'
            ),
            'mes_origem': primeiro_dia_ultimo_mes.strftime('%Y-%m'),
            'mes_destino': dt_movimentacao_nova.strftime('%Y-%m'),
            'estornos_inseridos': inseridas_estorno,
            'incorporacoes_inseridas': inseridas_incorporacao,
            'recebimentos_ir_inseridos': inseridas_recebimento,
            'entrada_pro_rata_inseridas': inseridas_entrada_pro_rata,
            'provisao_atm_juros_inseridas': inseridas_provisao_atm_juros,
            'provisao_pro_rata_inseridas': inseridas_provisao_pro_rata,
            'saldos_recalculados': saldos_recalculados,
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Erro ao processar mês: {str(e)}'
        }), 500

# =========================================================================
# EXTRATO — AJUSTAR SALDO (aplica AJUSTE da FIN_VW011 no último mês)
# =========================================================================
@titulo_cvs_bp.route('/extrato/ajustar-saldo', methods=['POST'])
@login_required
def extrato_ajustar_saldo():
    """
    Aplica o AJUSTE de Saldo de Provisão de Juros no mês do
    último registro do extrato.

    Etapas (dentro de uma única transação):

    ETAPA A — Descobrir o mês alvo:
        Mês do ajuste = primeiro dia do mês da MAX(DT_MOVIMENTACAO).

    ETAPA B — Ler AJUSTE da view FIN_VW011_AJUSTE_SALDO_PROV_JUROS_CVS
        WHERE ANO_MES = YYYYMM do mês alvo.
        Se não existir registro, ou AJUSTE = 0, aborta com mensagem.

    ETAPA C — UPDATE na linha de:
            HISTORICO LIKE 'Provisão Juros Pro Rata%'
                (ou 'Provisao Juros Pro Rata%' sem acento)
            TIPO = 'A'
        do mês alvo, somando AJUSTE em VR_MOVIMENTACAO e VR_SALDO.
        Considera a linha de MAIOR NU_LINHA (caso haja mais de uma).

    ETAPA D — Recalcula em CADEIA o VR_SALDO de todas as linhas
        POSTERIORES (NU_LINHA maior) do mesmo mês alvo:
            novo_saldo = saldo_anterior + VR_MOVIMENTACAO
        Garantindo coerência matemática completa.

    Se AJUSTE for zero ou não existir na view, nada é feito.
    """
    try:
        # =================================================================
        # A. Determinar o mês do ajuste (último mês com dados)
        # =================================================================
        ultima_data = ExtratoCVS.obter_ultima_data_movimentacao()
        if not ultima_data:
            return jsonify({
                'success': False,
                'message': (
                    'A tabela FIN_TB013_EXTRATO_CVS está vazia. '
                    'Nada para ajustar.'
                )
            }), 400

        primeiro_dia_mes = ultima_data.replace(day=1)
        ultimo_dia_mes = _ultimo_dia_mes(ultima_data)
        ano_mes = int(primeiro_dia_mes.strftime('%Y%m'))

        # =================================================================
        # B. Ler AJUSTE da view FIN_VW011
        # =================================================================
        sql_ajuste = text("""
            SELECT [AJUSTE]
            FROM [BDG].[FIN_VW011_AJUSTE_SALDO_PROV_JUROS_CVS]
            WHERE [ANO_MES] = :ano_mes;
        """)
        row_ajuste = db.session.execute(
            sql_ajuste, {'ano_mes': ano_mes}
        ).fetchone()

        if not row_ajuste or row_ajuste[0] is None:
            return jsonify({
                'success': False,
                'message': (
                    f'Nenhum registro encontrado na view '
                    f'FIN_VW011_AJUSTE_SALDO_PROV_JUROS_CVS para '
                    f'{primeiro_dia_mes.strftime("%m/%Y")}.'
                )
            }), 400

        ajuste = Decimal(str(row_ajuste[0]))

        if ajuste == 0:
            return jsonify({
                'success': False,
                'message': (
                    f'O AJUSTE de {primeiro_dia_mes.strftime("%m/%Y")} '
                    f'já está zerado. Nada a ajustar.'
                )
            }), 400

        # =================================================================
        # C.1. Encontrar NU_LINHA da linha alvo
        #      HISTORICO LIKE 'Provisão Juros Pro Rata%' e TIPO = 'A'
        # =================================================================
        sql_nu_linha_alvo = text("""
            SELECT MAX([NU_LINHA])
            FROM [BDG].[FIN_TB013_EXTRATO_CVS]
            WHERE YEAR([DT_MOVIMENTACAO])  = YEAR(:dt_mes)
              AND MONTH([DT_MOVIMENTACAO]) = MONTH(:dt_mes)
              AND LTRIM(RTRIM([TIPO])) = 'A'
              AND (
                  [HISTORICO] LIKE N'Provisão Juros Pro Rata%'
                  OR [HISTORICO] LIKE N'Provisao Juros Pro Rata%'
              );
        """)
        row_nu = db.session.execute(
            sql_nu_linha_alvo, {'dt_mes': primeiro_dia_mes}
        ).fetchone()
        nu_linha_alvo = row_nu[0] if row_nu else None

        if nu_linha_alvo is None:
            return jsonify({
                'success': False,
                'message': (
                    f'Nenhuma linha "Provisão Juros Pro Rata" com TIPO A '
                    f'encontrada em {primeiro_dia_mes.strftime("%m/%Y")}.'
                )
            }), 400

        # =================================================================
        # C.2. UPDATE somando AJUSTE em VR_MOVIMENTACAO e VR_SALDO
        # =================================================================
        sql_update_alvo = text("""
            UPDATE [BDG].[FIN_TB013_EXTRATO_CVS]
            SET [VR_MOVIMENTACAO] = [VR_MOVIMENTACAO] + :ajuste,
                [VR_SALDO]        = [VR_SALDO]        + :ajuste
            WHERE [NU_LINHA] = :nu_linha;
        """)
        db.session.execute(
            sql_update_alvo,
            {'ajuste': ajuste, 'nu_linha': nu_linha_alvo}
        )

        # =================================================================
        # C.3. Ler o novo VR_SALDO da linha alvo (ponto de partida
        #      para o recálculo em cadeia)
        # =================================================================
        sql_saldo_alvo = text("""
            SELECT [VR_SALDO]
            FROM [BDG].[FIN_TB013_EXTRATO_CVS]
            WHERE [NU_LINHA] = :nu_linha;
        """)
        row_saldo = db.session.execute(
            sql_saldo_alvo, {'nu_linha': nu_linha_alvo}
        ).fetchone()
        saldo_acumulado = (
            Decimal(str(row_saldo[0]))
            if row_saldo and row_saldo[0] is not None
            else Decimal('0')
        )
        saldo_apos_ajuste = saldo_acumulado

        # =================================================================
        # D. Recalcular em CADEIA o saldo das linhas POSTERIORES
        # =================================================================
        sql_posteriores = text("""
            SELECT [NU_LINHA], [VR_MOVIMENTACAO]
            FROM [BDG].[FIN_TB013_EXTRATO_CVS]
            WHERE YEAR([DT_MOVIMENTACAO])  = YEAR(:dt_mes)
              AND MONTH([DT_MOVIMENTACAO]) = MONTH(:dt_mes)
              AND [NU_LINHA] > :nu_linha_alvo
            ORDER BY [NU_LINHA] ASC;
        """)
        rows_posteriores = db.session.execute(
            sql_posteriores,
            {
                'dt_mes': primeiro_dia_mes,
                'nu_linha_alvo': nu_linha_alvo,
            }
        ).fetchall()

        sql_update_saldo = text("""
            UPDATE [BDG].[FIN_TB013_EXTRATO_CVS]
            SET [VR_SALDO] = :vr_saldo
            WHERE [NU_LINHA] = :nu_linha;
        """)

        qtd_recalculadas = 0
        for r in rows_posteriores:
            nu = r[0]
            mov = r[1]
            mov_decimal = (
                Decimal(str(mov)) if mov is not None else Decimal('0')
            )
            saldo_acumulado = saldo_acumulado + mov_decimal
            db.session.execute(
                sql_update_saldo,
                {'vr_saldo': saldo_acumulado, 'nu_linha': nu}
            )
            qtd_recalculadas += 1

        # =================================================================
        # Commit final da transação
        # =================================================================
        db.session.commit()

        # =================================================================
        # Auditoria
        # =================================================================
        registrar_log(
            acao='ajustar',
            entidade='extrato_cvs',
            entidade_id=primeiro_dia_mes.strftime('%Y-%m-%d'),
            descricao=(
                f'Ajuste de saldo aplicado em '
                f'{primeiro_dia_mes.strftime("%m/%Y")}: '
                f'AJUSTE = {ajuste}. '
                f'{qtd_recalculadas} saldo(s) posterior(es) recalculado(s). '
                f'Saldo final: {saldo_acumulado}.'
            ),
            dados_novos={
                'mes_ajuste': primeiro_dia_mes.strftime('%Y-%m'),
                'ano_mes': ano_mes,
                'nu_linha_alvo': int(nu_linha_alvo),
                'ajuste_aplicado': str(ajuste),
                'saldo_apos_ajuste_na_linha_alvo': str(saldo_apos_ajuste),
                'saldos_recalculados': qtd_recalculadas,
                'saldo_final': str(saldo_acumulado),
            }
        )

        return jsonify({
            'success': True,
            'message': (
                f'Ajuste aplicado com sucesso em '
                f'{primeiro_dia_mes.strftime("%m/%Y")}. '
                f'Valor: {ajuste}. '
                f'{qtd_recalculadas} saldo(s) posterior(es) recalculado(s). '
                f'Saldo final: {saldo_acumulado}.'
            ),
            'mes_ajuste': primeiro_dia_mes.strftime('%Y-%m'),
            'ajuste_aplicado': str(ajuste),
            'saldos_recalculados': qtd_recalculadas,
            'saldo_final': str(saldo_acumulado),
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Erro ao ajustar saldo: {str(e)}'
        }), 500

# =========================================================================
# EXTRATO — ÍNDICES DO DIA 1 (calcula próximo mês na FIN_TB015)
# =========================================================================
@titulo_cvs_bp.route('/extrato/indices-dia-1', methods=['POST'])
@login_required
def extrato_indices_dia_1():
    """
    Calcula a próxima linha (próximo mês, dia 1) da tabela
    BDG.FIN_TB015_INDICES_DIA_1, com base na última DT_ATUALIZACAO.

    Para cada TIPO existente na última data, gera uma nova linha
    na data do mês seguinte (dia 1), com os seguintes cálculos:

      fator         = 1 + (numIndicadorEconomico / 100)
      PU_ATUALIZADO = PU_CARECA_anterior * fator
      JUROS         = PU_ATUALIZADO * 0.005
      AMORTIZACAO   = PU_ATUALIZADO / (1 - sequencia * 0.004608) * 0.004608
      PU_CARECA     = PU_ATUALIZADO - AMORTIZACAO
      VR_GANHO_VNA  = ((PU_CARECA_atual - PU_ATUALIZADO_anterior)
                       / (1 - 0.004608 * SEQ_FIN_CVS_anterior))
                      * 0.004608

    Onde:
      - numIndicadorEconomico vem de
        [DBPRDINDICADORECONOMICO].[dbo].[tblIndicadorEconomico]
        com idTipoIndicadorEconomico=2, chDTFinal=YYYYMMDD da nova data
        e chDTInicio terminando em '01'.
        IMPORTANTE: o valor vem armazenado como percentual (ex: 0,42
        significa 0,42% ao mês), por isso convertemos em fator
        multiplicativo dividindo por 100 e somando 1.
      - sequencia vem da view BDG.FIN_VW003_SEQUENCIAL_DT_TITULOS_CVS,
        coluna SEQ_FIN_CVS, onde DIA = última data (mês anterior à nova).
      - PU_ATUALIZADO_anterior é da linha base (mês anterior).
    """
    try:
        # =================================================================
        # 1. Última DT_ATUALIZACAO da FIN_TB015
        # =================================================================
        sql_ultima = text("""
            SELECT MAX([DT_ATUALIZACAO])
            FROM [BDG].[FIN_TB015_INDICES_DIA_1];
        """)
        ultima_data = db.session.execute(sql_ultima).scalar()

        if not ultima_data:
            return jsonify({
                'success': False,
                'message': (
                    'A tabela FIN_TB015_INDICES_DIA_1 está vazia. '
                    'Nada para calcular.'
                )
            }), 400

        # =================================================================
        # 2. Nova data = dia 1 do mês seguinte
        # =================================================================
        nova_data = _proximo_mes_dia_1(ultima_data)
        ch_dt_final = nova_data.strftime('%Y%m%d')  # ex: '20260701'

        # =================================================================
        # 3. Já existe registro para a nova data? Aborta.
        # =================================================================
        sql_ja_existe = text("""
            SELECT COUNT(*)
            FROM [BDG].[FIN_TB015_INDICES_DIA_1]
            WHERE [DT_ATUALIZACAO] = :nova_data;
        """)
        ja_existe = db.session.execute(
            sql_ja_existe, {'nova_data': nova_data}
        ).scalar() or 0
        if ja_existe > 0:
            return jsonify({
                'success': False,
                'message': (
                    f'Já existem {ja_existe} registro(s) em '
                    f'{nova_data.strftime("%d/%m/%Y")}. '
                    f'Apague-os primeiro para reprocessar.'
                )
            }), 400

        # =================================================================
        # 4. Buscar linhas da última data (uma por TIPO)
        #    Agora trazendo também PU_ATUALIZADO (necessário para o
        #    cálculo do VR_GANHO_VNA).
        # =================================================================
        sql_linhas_anteriores = text("""
            SELECT [TIPO], [PU_CARECA], [PU_ATUALIZADO]
            FROM [BDG].[FIN_TB015_INDICES_DIA_1]
            WHERE [DT_ATUALIZACAO] = :ultima_data
            ORDER BY [TIPO];
        """)
        linhas_anteriores = db.session.execute(
            sql_linhas_anteriores, {'ultima_data': ultima_data}
        ).fetchall()

        if not linhas_anteriores:
            return jsonify({
                'success': False,
                'message': (
                    f'Nenhuma linha encontrada em '
                    f'{ultima_data.strftime("%d/%m/%Y")}.'
                )
            }), 400

        # =================================================================
        # 5. Buscar índice econômico para a NOVA data
        # =================================================================
        sql_indice = text("""
            SELECT TOP 1 [numIndicadorEconomico]
            FROM [DBPRDINDICADORECONOMICO].[dbo].[tblIndicadorEconomico]
            WHERE [idTipoIndicadorEconomico] = 2
              AND [chDTFinal] = :ch_dt_final
              AND RIGHT([chDTInicio], 2) = '01';
        """)
        row_indice = db.session.execute(
            sql_indice, {'ch_dt_final': ch_dt_final}
        ).fetchone()

        if not row_indice or row_indice[0] is None:
            return jsonify({
                'success': False,
                'message': (
                    f'Índice econômico não encontrado para '
                    f'chDTFinal={ch_dt_final} '
                    f'(idTipoIndicadorEconomico=2, '
                    f'chDTInicio terminando em 01).'
                )
            }), 400

        # Valor cru do índice (vem como percentual: ex 0,42 = 0,42%)
        indice_percentual = Decimal(str(row_indice[0]))

        # Converte em fator multiplicativo: (1 + percentual/100)
        fator_indice = Decimal('1') + (indice_percentual / Decimal('100'))

        # =================================================================
        # 6. Buscar sequência da view (DIA = última data)
        # =================================================================
        sql_sequencia = text("""
            SELECT TOP 1 [SEQ_FIN_CVS]
            FROM [BDG].[FIN_VW003_SEQUENCIAL_DT_TITULOS_CVS]
            WHERE [DIA] = :ultima_data;
        """)
        row_seq = db.session.execute(
            sql_sequencia, {'ultima_data': ultima_data}
        ).fetchone()

        if not row_seq or row_seq[0] is None:
            return jsonify({
                'success': False,
                'message': (
                    f'SEQ_FIN_CVS não encontrado em '
                    f'FIN_VW003_SEQUENCIAL_DT_TITULOS_CVS para '
                    f'DIA={ultima_data.strftime("%d/%m/%Y")}.'
                )
            }), 400

        sequencia = Decimal(str(row_seq[0]))

        # Proteção contra divisão por zero
        fator_amortizacao = Decimal('0.004608')
        denominador = Decimal('1') - (sequencia * fator_amortizacao)
        if denominador == 0:
            return jsonify({
                'success': False,
                'message': (
                    f'Denominador da amortização é zero '
                    f'(sequencia={sequencia}). Cálculo abortado.'
                )
            }), 400

        # =================================================================
        # 7. Para cada TIPO, calcular e inserir nova linha
        # =================================================================
        sql_insert = text("""
            INSERT INTO [BDG].[FIN_TB015_INDICES_DIA_1]
                ([DT_ATUALIZACAO], [TIPO], [PU_ATUALIZADO],
                 [JUROS], [AMORTIZACAO], [PU_CARECA], [VR_GANHO_VNA])
            VALUES
                (:dt, :tipo, :pu_atual, :juros, :amort,
                 :pu_careca, :vr_ganho);
        """)

        fator_juros = Decimal('0.005')
        inseridas = 0
        detalhes = []

        for linha in linhas_anteriores:
            tipo = linha[0]
            pu_careca_anterior = (
                Decimal(str(linha[1])) if linha[1] is not None
                else Decimal('0')
            )
            pu_atualizado_anterior = (
                Decimal(str(linha[2])) if linha[2] is not None
                else Decimal('0')
            )

            # PU_ATUALIZADO = PU_CARECA_anterior * fator
            # (fator = 1 + percentual/100, ex: 0,42% -> 1,0042)
            pu_atualizado = pu_careca_anterior * fator_indice

            # JUROS = PU_ATUALIZADO * 0.005
            juros = pu_atualizado * fator_juros

            # AMORTIZACAO = PU_ATUALIZADO / (1 - sequencia*0.004608) * 0.004608
            amortizacao = (pu_atualizado / denominador) * fator_amortizacao

            # PU_CARECA = PU_ATUALIZADO - AMORTIZACAO
            pu_careca = pu_atualizado - amortizacao

            # =============================================================
            # NOVO: VR_GANHO_VNA
            #   A = PU_CARECA atual - PU_ATUALIZADO anterior
            #   B = 1 - 0.004608 * SEQ_FIN_CVS anterior  (= denominador)
            #   C = A / B
            #   VR_GANHO_VNA = C * 0.004608
            # =============================================================
            a_ganho = pu_atualizado - pu_careca_anterior
            b_ganho = denominador  # mesmo valor já calculado
            c_ganho = a_ganho / b_ganho
            vr_ganho_vna = c_ganho * fator_amortizacao

            db.session.execute(sql_insert, {
                'dt': nova_data,
                'tipo': tipo,
                'pu_atual': pu_atualizado,
                'juros': juros,
                'amort': amortizacao,
                'pu_careca': pu_careca,
                'vr_ganho': vr_ganho_vna,
            })

            inseridas += 1
            detalhes.append({
                'tipo': tipo,
                'pu_careca_anterior': str(pu_careca_anterior),
                'pu_atualizado_anterior': str(pu_atualizado_anterior),
                'pu_atualizado': str(pu_atualizado),
                'juros': str(juros),
                'amortizacao': str(amortizacao),
                'pu_careca': str(pu_careca),
                'vr_ganho_vna': str(vr_ganho_vna),
            })

        db.session.commit()

        # =================================================================
        # 8. Auditoria
        # =================================================================
        registrar_log(
            acao='carga',
            entidade='indices_dia_1_cvs',
            entidade_id=nova_data.strftime('%Y-%m-%d'),
            descricao=(
                f'Cálculo de Índices do Dia 1 para '
                f'{nova_data.strftime("%d/%m/%Y")} '
                f'(base: {ultima_data.strftime("%d/%m/%Y")})'
            ),
            dados_novos={
                'ultima_data': ultima_data.strftime('%Y-%m-%d'),
                'nova_data': nova_data.strftime('%Y-%m-%d'),
                'ch_dt_final': ch_dt_final,
                'indice_percentual': str(indice_percentual),
                'fator_indice': str(fator_indice),
                'sequencia': str(sequencia),
                'inseridas': inseridas,
                'detalhes': detalhes,
            }
        )

        return jsonify({
            'success': True,
            'message': (
                f'{inseridas} índice(s) inserido(s) com sucesso para '
                f'{nova_data.strftime("%d/%m/%Y")}. '
                f'(índice: {indice_percentual}% / '
                f'fator: {fator_indice})'
            ),
            'nova_data': nova_data.strftime('%Y-%m-%d'),
            'inseridas': inseridas,
            'indice_percentual': str(indice_percentual),
            'fator_indice': str(fator_indice),
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Erro ao calcular índices: {str(e)}'
        }), 500
# =========================================================================
# ESTOQUE — PÁGINA PRINCIPAL
# =========================================================================
@titulo_cvs_bp.route('/estoque')
@login_required
def estoque_index():
    """
    Página principal do Estoque CVS.

    Lista todas as posições agrupadas por DT_POSICAO, mostrando
    cada uma com uma linha TOTAL + uma linha por TIPO (A, B).
    """
    db.session.expire_all()

    # Buscar todas as posições ordenadas
    todas = PosicaoEstoqueCVS.listar_todos_ordenados()

    # Agrupar por DT_POSICAO (mantendo ordem ASC)
    # estrutura: lista de {dt, tipos: {'A': obj, 'B': obj, ...}, total_qtde, total_vr}
    from collections import OrderedDict
    grupos = OrderedDict()
    for p in todas:
        if p.DT_POSICAO not in grupos:
            grupos[p.DT_POSICAO] = {
                'dt': p.DT_POSICAO,
                'tipos': {},
                'total_vr': Decimal('0'),
            }
        grupos[p.DT_POSICAO]['tipos'][p.TIPO] = p
        if p.VR_TOTAL is not None:
            grupos[p.DT_POSICAO]['total_vr'] += p.VR_TOTAL

    # Lista final pro template (em ordem ASC pela DT_POSICAO)
    grupos_lista = list(grupos.values())

    # Última DT_POSICAO para info do header
    ultima_dt = PosicaoEstoqueCVS.obter_ultima_dt_posicao()
    total_grupos = len(grupos_lista)

    response = make_response(render_template(
        'titulo_cvs/estoque.html',
        grupos=grupos_lista,
        ultima_dt=ultima_dt,
        total_grupos=total_grupos,
    ))
    response.headers['Cache-Control'] = (
        'no-store, no-cache, must-revalidate, max-age=0'
    )
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


# =========================================================================
# ESTOQUE — ATUALIZAR (dia útil a dia útil até hoje)
# =========================================================================
@titulo_cvs_bp.route('/estoque/atualizar', methods=['POST'])
@login_required
def estoque_atualizar():
    """
    Atualiza o estoque calculando UMA NOVA POSIÇÃO para CADA DIA ÚTIL
    entre a última DT_POSICAO existente e hoje (inclusive).

    Lógica:
      1. Pega MAX(DT_POSICAO) da FIN_TB010 → ultima_dt
      2. Consulta BDG.PAR_TB020_CALENDARIO buscando todos os
         [DIA] tais que:
             ultima_dt < DIA <= hoje
             E DIA_UTIL = 1
      3. Calcula QTDE por TIPO uma única vez (FIN_TB007).
      4. Para cada dia útil encontrado:
            a) Busca VR_PU (PU_PRORATA) por TIPO na
               FIN_VW004_ATUALIZACAO_PRORATA_CVS
               WHERE DT_FIM_PRORATA_CVS = dia.
            b) INSERT em FIN_TB010 (DT_POSICAO=dia, TIPO, QTDE, VR_PU,
               VR_TOTAL = QTDE × VR_PU). DT_CARGA = hoje.
            c) Se a view não tem dados para algum TIPO nessa data,
               grava VR_PU/VR_TOTAL como NULL (mantém o registro).

    Exemplo:
      ultima_dt = 12/04/2026, hoje = 30/06/2026
        → Para cada dia útil entre 13/04 e 30/06, gera registros.
      Se entre essas datas há 55 dias úteis, serão 55 novas
      DT_POSICAO inseridas.

    Tudo em uma única transação (rollback automático em caso de erro).
    """
    try:
        hoje = datetime.now().date()

        # =================================================================
        # 1. Última DT_POSICAO
        # =================================================================
        ultima_dt = PosicaoEstoqueCVS.obter_ultima_dt_posicao()
        if not ultima_dt:
            return jsonify({
                'success': False,
                'message': (
                    'A tabela FIN_TB010_POSICAO_MENSAL_ESTOQUE_CVS está '
                    'vazia. É necessário pelo menos um registro inicial '
                    'para servir de referência.'
                )
            }), 400

        if ultima_dt >= hoje:
            return jsonify({
                'success': False,
                'message': (
                    f'A última DT_POSICAO ({ultima_dt.strftime("%d/%m/%Y")}) '
                    f'já está em dia (>= hoje, '
                    f'{hoje.strftime("%d/%m/%Y")}). Nada a processar.'
                )
            }), 400

        # =================================================================
        # 2. Dias úteis entre (ultima_dt, hoje] no calendário
        # =================================================================
        sql_dias_uteis = text("""
            SELECT [DIA]
            FROM [BDG].[PAR_TB020_CALENDARIO]
            WHERE [DIA] >  :ultima_dt
              AND [DIA] <= :hoje
              AND [DIA_UTIL] = 1
            ORDER BY [DIA] ASC;
        """)
        rows_dias = db.session.execute(
            sql_dias_uteis,
            {'ultima_dt': ultima_dt, 'hoje': hoje}
        ).fetchall()

        dias_uteis = []
        for r in rows_dias:
            valor = r[0]
            if valor is None:
                continue
            # Normaliza para date (caso venha datetime)
            if hasattr(valor, 'date'):
                dias_uteis.append(valor.date())
            else:
                dias_uteis.append(valor)

        if not dias_uteis:
            return jsonify({
                'success': False,
                'message': (
                    f'Nenhum dia útil encontrado entre '
                    f'{ultima_dt.strftime("%d/%m/%Y")} e '
                    f'{hoje.strftime("%d/%m/%Y")} no calendário.'
                )
            }), 400

        # =================================================================
        # 3. QTDE por TIPO da FIN_TB007 (carregada uma única vez)
        # =================================================================
        sql_qtde = text("""
            SELECT 
                SUBSTRING([ATIVO], 4, 1) AS TIPO,
                SUM([QTDE])              AS QTDE
            FROM [BDG].[FIN_TB007_RESUMO_CVS]
            WHERE [DT_ATUALIZACAO] >= '20240716'
            GROUP BY SUBSTRING([ATIVO], 4, 1)
            ORDER BY TIPO ASC;
        """)
        qtdes_rows = db.session.execute(sql_qtde).fetchall()
        if not qtdes_rows:
            return jsonify({
                'success': False,
                'message': (
                    'Nenhum dado encontrado em FIN_TB007_RESUMO_CVS '
                    'para calcular as quantidades.'
                )
            }), 400

        qtdes_por_tipo = {}
        for r in qtdes_rows:
            if r[0] is not None:
                qtdes_por_tipo[r[0]] = int(r[1]) if r[1] is not None else 0

        # =================================================================
        # 4. INSERT em cadeia — um conjunto de linhas por dia útil
        # =================================================================
        sql_pu = text("""
            SELECT [TIPO], [PU_PRORATA]
            FROM [BDG].[FIN_VW004_ATUALIZACAO_PRORATA_CVS]
            WHERE [DT_FIM_PRORATA_CVS] = :dia;
        """)

        sql_insert = text("""
            INSERT INTO [BDG].[FIN_TB010_POSICAO_MENSAL_ESTOQUE_CVS]
                ([DT_CARGA], [DT_POSICAO], [TIPO],
                 [QTDE], [VR_PU], [VR_TOTAL])
            VALUES
                (:dt_carga, :dt_pos, :tipo,
                 :qtde, :vr_pu, :vr_total);
        """)

        sql_ja_existe = text("""
            SELECT COUNT(*)
            FROM [BDG].[FIN_TB010_POSICAO_MENSAL_ESTOQUE_CVS]
            WHERE [DT_POSICAO] = :dia;
        """)

        total_dias_processados = 0
        total_linhas_inseridas = 0
        total_dias_pulados = 0
        detalhes_por_dia = []

        for dia in dias_uteis:
            # Pula se já existir registro para este dia (evita duplicação)
            ja_existe = db.session.execute(
                sql_ja_existe, {'dia': dia}
            ).scalar() or 0
            if ja_existe > 0:
                total_dias_pulados += 1
                continue

            # PU_PRORATA por TIPO da view nesse dia
            pus_rows = db.session.execute(sql_pu, {'dia': dia}).fetchall()
            pu_por_tipo = {}
            for r in pus_rows:
                if r[0] is not None and r[1] is not None:
                    pu_por_tipo[r[0]] = Decimal(str(r[1]))

            linhas_dia = 0
            for tipo, qtde in qtdes_por_tipo.items():
                vr_pu_decimal = pu_por_tipo.get(tipo)
                vr_total = (
                    Decimal(str(qtde)) * vr_pu_decimal
                    if vr_pu_decimal is not None
                    else None
                )
                db.session.execute(sql_insert, {
                    'dt_carga': hoje,
                    'dt_pos': dia,
                    'tipo': tipo,
                    'qtde': qtde,
                    'vr_pu': vr_pu_decimal,
                    'vr_total': vr_total,
                })
                linhas_dia += 1
                total_linhas_inseridas += 1

            total_dias_processados += 1
            detalhes_por_dia.append({
                'dia': dia.strftime('%Y-%m-%d'),
                'linhas_inseridas': linhas_dia,
                'tipos_sem_pu': [
                    tipo for tipo in qtdes_por_tipo
                    if tipo not in pu_por_tipo
                ],
            })

        db.session.commit()

        # =================================================================
        # 5. Auditoria
        # =================================================================
        nova_ultima_dt = (
            detalhes_por_dia[-1]['dia']
            if detalhes_por_dia
            else ultima_dt.strftime('%Y-%m-%d')
        )

        registrar_log(
            acao='carga',
            entidade='posicao_estoque_cvs',
            entidade_id=nova_ultima_dt,
            descricao=(
                f'Atualização de Estoque — varredura de dias úteis. '
                f'Base: {ultima_dt.strftime("%d/%m/%Y")} → '
                f'Hoje: {hoje.strftime("%d/%m/%Y")}. '
                f'Dias úteis processados: {total_dias_processados}. '
                f'Linhas inseridas: {total_linhas_inseridas}. '
                f'Dias pulados (já existentes): {total_dias_pulados}.'
            ),
            dados_novos={
                'ultima_dt_base': ultima_dt.strftime('%Y-%m-%d'),
                'hoje': hoje.strftime('%Y-%m-%d'),
                'dias_uteis_encontrados': len(dias_uteis),
                'dias_processados': total_dias_processados,
                'dias_pulados': total_dias_pulados,
                'linhas_inseridas': total_linhas_inseridas,
                'nova_ultima_dt': nova_ultima_dt,
                'detalhes': detalhes_por_dia,
            }
        )

        if total_dias_processados == 0:
            return jsonify({
                'success': False,
                'message': (
                    f'Nenhum dia útil novo foi processado entre '
                    f'{ultima_dt.strftime("%d/%m/%Y")} e '
                    f'{hoje.strftime("%d/%m/%Y")} '
                    f'(todos já tinham registro).'
                )
            }), 400

        return jsonify({
            'success': True,
            'message': (
                f'Estoque atualizado! '
                f'{total_dias_processados} dia(s) útil(eis) processado(s) '
                f'entre {ultima_dt.strftime("%d/%m/%Y")} e '
                f'{hoje.strftime("%d/%m/%Y")}. '
                f'Total de {total_linhas_inseridas} linha(s) inserida(s).'
                + (f' {total_dias_pulados} dia(s) pulado(s) por já existirem.'
                   if total_dias_pulados else '')
            ),
            'ultima_dt_base': ultima_dt.strftime('%Y-%m-%d'),
            'hoje': hoje.strftime('%Y-%m-%d'),
            'dias_processados': total_dias_processados,
            'dias_pulados': total_dias_pulados,
            'linhas_inseridas': total_linhas_inseridas,
            'nova_ultima_dt': nova_ultima_dt,
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Erro ao atualizar estoque: {str(e)}'
        }), 500

# =========================================================================
# RECEBIMENTO — PÁGINA PRINCIPAL (lista + formulário de cadastro)
# =========================================================================
@titulo_cvs_bp.route('/recebimento')
@login_required
def recebimento_index():
    """
    Página de cadastro de Recebimentos CVS.

    Mostra um formulário para adicionar novo registro e a lista
    dos recebimentos já cadastrados (mais recentes primeiro).
    """
    db.session.expire_all()

    recebimentos = RecebimentoCVS.listar_todos()
    total = len(recebimentos)

    response = make_response(render_template(
        'titulo_cvs/recebimento.html',
        recebimentos=recebimentos,
        total=total,
    ))
    response.headers['Cache-Control'] = (
        'no-store, no-cache, must-revalidate, max-age=0'
    )
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


# =========================================================================
# RECEBIMENTO — SALVAR (POST AJAX)
# =========================================================================
@titulo_cvs_bp.route('/recebimento/salvar', methods=['POST'])
@login_required
def recebimento_salvar():
    """
    Salva um novo Recebimento na tabela FIN_TB016_RECEBIMENTO.

    Recebe JSON:
      {
        "tipo": "A",                    (1 caractere)
        "dt_atualizacao": "YYYY-MM-DD",
        "historico": "texto até 30 chars",
        "vr_entrada": "1234.5678"       (decimal, opcional)
      }
    """
    try:
        dados = request.get_json(silent=True) or {}

        tipo = (dados.get('tipo') or '').strip().upper()
        dt_str = (dados.get('dt_atualizacao') or '').strip()
        historico = (dados.get('historico') or '').strip()
        vr_entrada_str = (dados.get('vr_entrada') or '').strip()

        # ----- Validações -----
        if not tipo or len(tipo) != 1:
            return jsonify({
                'success': False,
                'message': 'TIPO deve ter exatamente 1 caractere.'
            }), 400

        if not dt_str:
            return jsonify({
                'success': False,
                'message': 'DT_ATUALIZACAO é obrigatória.'
            }), 400

        try:
            dt_atualizacao = datetime.strptime(dt_str, '%Y-%m-%d').date()
        except ValueError:
            return jsonify({
                'success': False,
                'message': f'Data inválida: {dt_str}.'
            }), 400

        if not historico:
            return jsonify({
                'success': False,
                'message': 'HISTORICO é obrigatório.'
            }), 400

        if len(historico) > 30:
            return jsonify({
                'success': False,
                'message': 'HISTORICO excede 30 caracteres.'
            }), 400

        # VR_ENTRADA é opcional, mas se vier precisa ser número válido
        vr_entrada = None
        if vr_entrada_str:
            try:
                # aceita ',' e '.' como separador decimal
                vr_limpo = vr_entrada_str.replace(',', '.')
                vr_entrada = Decimal(vr_limpo)
            except (InvalidOperation, ValueError):
                return jsonify({
                    'success': False,
                    'message': f'VR_ENTRADA inválido: {vr_entrada_str}.'
                }), 400

        # ----- Já existe? -----
        existente = RecebimentoCVS.obter(tipo, dt_atualizacao, historico)
        if existente:
            return jsonify({
                'success': False,
                'message': (
                    f'Já existe registro para TIPO={tipo}, '
                    f'DT_ATUALIZACAO={dt_atualizacao.strftime("%d/%m/%Y")} '
                    f'e HISTORICO="{historico}".'
                )
            }), 400

        # ----- INSERT -----
        novo = RecebimentoCVS(
            TIPO=tipo,
            DT_ATUALIZACAO=dt_atualizacao,
            HISTORICO=historico,
            VR_ENTRADA=vr_entrada,
        )
        db.session.add(novo)
        db.session.commit()

        # ----- Auditoria -----
        registrar_log(
            acao='criar',
            entidade='recebimento_cvs',
            entidade_id=f'{tipo}/{dt_str}/{historico}',
            descricao=(
                f'Cadastro de Recebimento - TIPO: {tipo}, '
                f'DT: {dt_atualizacao.strftime("%d/%m/%Y")}, '
                f'HIST: {historico}'
            ),
            dados_novos={
                'TIPO': tipo,
                'DT_ATUALIZACAO': dt_str,
                'HISTORICO': historico,
                'VR_ENTRADA': str(vr_entrada) if vr_entrada else None,
            }
        )

        return jsonify({
            'success': True,
            'message': (
                f'Recebimento cadastrado: {tipo} / '
                f'{dt_atualizacao.strftime("%d/%m/%Y")} / {historico}.'
            ),
            'item': {
                'tipo': tipo,
                'dt_atualizacao': dt_str,
                'historico': historico,
                'vr_entrada': str(vr_entrada) if vr_entrada else None,
            }
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Erro ao salvar: {str(e)}'
        }), 500


# =========================================================================
# RECEBIMENTO — EXCLUIR (POST AJAX)
# =========================================================================
@titulo_cvs_bp.route('/recebimento/excluir', methods=['POST'])
@login_required
def recebimento_excluir():
    """
    Exclui um Recebimento pela PK composta.

    Recebe JSON:
      { "tipo": "A", "dt_atualizacao": "YYYY-MM-DD", "historico": "..." }
    """
    try:
        dados = request.get_json(silent=True) or {}

        tipo = (dados.get('tipo') or '').strip().upper()
        dt_str = (dados.get('dt_atualizacao') or '').strip()
        historico = (dados.get('historico') or '').strip()

        if not tipo or not dt_str or not historico:
            return jsonify({
                'success': False,
                'message': 'Parâmetros obrigatórios: tipo, dt_atualizacao, historico.'
            }), 400

        try:
            dt_atualizacao = datetime.strptime(dt_str, '%Y-%m-%d').date()
        except ValueError:
            return jsonify({
                'success': False,
                'message': f'Data inválida: {dt_str}.'
            }), 400

        registro = RecebimentoCVS.obter(tipo, dt_atualizacao, historico)
        if not registro:
            return jsonify({
                'success': False,
                'message': 'Registro não encontrado.'
            }), 404

        valor_antigo = {
            'TIPO': registro.TIPO,
            'DT_ATUALIZACAO': dt_str,
            'HISTORICO': registro.HISTORICO,
            'VR_ENTRADA': str(registro.VR_ENTRADA) if registro.VR_ENTRADA else None,
        }

        db.session.delete(registro)
        db.session.commit()

        registrar_log(
            acao='excluir',
            entidade='recebimento_cvs',
            entidade_id=f'{tipo}/{dt_str}/{historico}',
            descricao=(
                f'Exclusão de Recebimento - TIPO: {tipo}, '
                f'DT: {dt_atualizacao.strftime("%d/%m/%Y")}, '
                f'HIST: {historico}'
            ),
            dados_antigos=valor_antigo,
            dados_novos=None
        )

        return jsonify({
            'success': True,
            'message': f'Recebimento excluído com sucesso.'
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Erro ao excluir: {str(e)}'
        }), 500