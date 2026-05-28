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
from app.models.titulo_cvs import ResumoCVS, OrigemDestinoCVS
from app.utils.audit import registrar_log
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from sqlalchemy import text
import os
import re
import tempfile
from types import SimpleNamespace

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

    response = make_response(render_template(
        'titulo_cvs/index.html',
        registros=registros,
        datas_disponiveis=datas_disponiveis,
        dt_filtro=dt_filtro,
        total_registros=total_registros,
        total_contratos=total_contratos,
        total_cargas=total_cargas,
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
