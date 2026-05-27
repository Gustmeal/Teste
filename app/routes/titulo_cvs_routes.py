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
from app.models.titulo_cvs import ResumoCVS
from app.utils.audit import registrar_log
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from sqlalchemy import text
import os
import re
import tempfile

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