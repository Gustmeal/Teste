import os
import re
import tempfile
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

from flask import (
    Blueprint, render_template, request, jsonify
)
from flask_login import login_required

from app import db
from app.models.boletim_financeiro import BoletimFinanceiro
from app.utils.audit import registrar_log

boletim_financeiro_bp = Blueprint(
    'boletim_financeiro', __name__, url_prefix='/boletim-financeiro'
)

# =========================================================================
# CONSTANTES DE PARSE
# =========================================================================
# Tons de verde usados como TOTAL/subtotal no Boletim (não capturar).
_VERDES_CONHECIDOS = {'FF339966', 'FFCCFFCC'}

# Ordem fixa dos meses -> nº do mês (JANEIRO = 1 ... DEZEMBRO = 12).
_MESES_ORDEM = [
    'JANEIRO', 'FEVEREIRO', 'MARÇO', 'ABRIL', 'MAIO', 'JUNHO',
    'JULHO', 'AGOSTO', 'SETEMBRO', 'OUTUBRO', 'NOVEMBRO', 'DEZEMBRO',
]

# Rótulos de total conhecidos (dupla checagem: além da cor, pelo nome).
_TOTAIS_NOME = {
    'SALDO FINAL DE CAIXA',
    'DISPONIBILIDADE TOTAL',
    'DISPONÍVEL INICIAL',
    'BANCOS - CONTAS CORRENTES',
    'APLICAÇÕES FINANCEIRAS',
    'BLOQUEIOS JUDICIAIS - FUNDO DE INVESTIMENTOS',
}

_MARCADOR_INICIO = 'SALDO FINAL DE CAIXA'
_MARCADOR_FIM = 'NATUREZAS MOVIMENTADAS'


# =========================================================================
# HELPERS DE PARSE
# =========================================================================
def _texto_limpo(valor):
    """Normaliza espaços e faz trim. Retorna '' se None."""
    if valor is None:
        return ''
    return re.sub(r'\s+', ' ', str(valor)).strip()


def _eh_celula_verde(cell):
    """
    True se a célula tem preenchimento sólido em tom de verde.
    Checa os códigos conhecidos e, como reforço, uma heurística de
    'canal verde dominante' para pegar qualquer outro tom de verde.
    """
    fill = cell.fill
    if fill is None or fill.patternType != 'solid':
        return False

    rgb = getattr(fill.fgColor, 'rgb', None)
    if not isinstance(rgb, str):
        return False

    if rgb.upper() in _VERDES_CONHECIDOS:
        return True

    hexc = rgb[-6:]
    try:
        r = int(hexc[0:2], 16)
        g = int(hexc[2:4], 16)
        b = int(hexc[4:6], 16)
    except ValueError:
        return False
    return g > r and g > b and g >= 128


def _to_decimal_2(valor):
    """Converte para Decimal(18,2). Trata None/NaN/vazio como 0.00."""
    if valor is None or valor == '':
        return Decimal('0.00')
    try:
        import math
        if isinstance(valor, float) and math.isnan(valor):
            return Decimal('0.00')
    except Exception:
        pass
    try:
        return Decimal(str(valor)).quantize(
            Decimal('0.01'), rounding=ROUND_HALF_UP
        )
    except (InvalidOperation, ValueError):
        return Decimal('0.00')


# =========================================================================
# PROCESSAMENTO DO EXCEL DO BOLETIM (ETAPA 1)
# =========================================================================
def _processar_excel_boletim(caminho_arquivo):
    """
    Lê o Excel do Boletim Financeiro e grava em BDG.FIN_TB020_BOLETIM_FINANCEIRO.

    Regras:
      - ANO vem do texto 'ANO: AAAA' (parte superior, não capturada como natureza).
      - Captura a partir da linha SEGUINTE a ' SALDO FINAL DE CAIXA'.
      - Ignora linhas VERDES (totais/subtotais) por cor E por nome.
      - Para no rodapé 'NATUREZAS MOVIMENTADAS...'.
      - MES_EXECUCAO = 'AAAAMM' (JANEIRO -> 01 ... DEZEMBRO -> 12).
      - Grava só os meses ATÉ o último mês com dados (não grava meses futuros
        zerados). Para gravar sempre os 12 meses, veja o comentário abaixo.

    Retorna dict com resumo do processamento.
    """
    from openpyxl import load_workbook

    wb = load_workbook(caminho_arquivo, data_only=True)
    ws = wb.active

    # -----------------------------------------------------------------
    # 1. ANO (ex.: 'ANO: 2026' -> '2026')
    # -----------------------------------------------------------------
    ano = None
    for r in range(1, 9):
        for c in range(1, ws.max_column + 1):
            texto = _texto_limpo(ws.cell(r, c).value)
            m = re.search(r'ANO[:\s]*?(\d{4})', texto, re.IGNORECASE)
            if m:
                ano = m.group(1)
                break
        if ano:
            break
    if not ano:
        raise Exception("Não foi possível localizar o ANO (ex.: 'ANO: 2026') na planilha.")

    # -----------------------------------------------------------------
    # 2. Cabeçalho -> coluna da NATUREZA e mapa mês -> coluna
    # -----------------------------------------------------------------
    col_natureza = None
    header_row = None
    mapa_mes = {}
    for r in range(1, 15):
        rotulos = {
            _texto_limpo(ws.cell(r, c).value).upper(): c
            for c in range(1, ws.max_column + 1)
        }
        if 'NATUREZA' in rotulos and 'JANEIRO' in rotulos:
            header_row = r
            col_natureza = rotulos['NATUREZA']
            for i, mes in enumerate(_MESES_ORDEM, start=1):
                if mes in rotulos:
                    mapa_mes[i] = rotulos[mes]
            break
    if header_row is None or not mapa_mes:
        raise Exception("Cabeçalho (NATUREZA / meses) não encontrado na planilha.")

    # -----------------------------------------------------------------
    # 3. Marcador de início (SALDO FINAL DE CAIXA)
    # -----------------------------------------------------------------
    marcador = None
    for r in range(header_row + 1, ws.max_row + 1):
        if _texto_limpo(ws.cell(r, col_natureza).value).upper() == _MARCADOR_INICIO:
            marcador = r
            break
    if marcador is None:
        raise Exception("Marcador ' SALDO FINAL DE CAIXA' não encontrado na planilha.")

    # -----------------------------------------------------------------
    # 4. Captura das naturezas (ignorando verdes/totais)
    # -----------------------------------------------------------------
    capturadas = []  # lista de (linha_excel, nome_natureza)
    for r in range(marcador + 1, ws.max_row + 1):
        celula = ws.cell(r, col_natureza)
        nome = _texto_limpo(celula.value)
        if not nome:
            continue
        if nome.upper().startswith(_MARCADOR_FIM):
            break
        if _eh_celula_verde(celula) or nome.upper() in _TOTAIS_NOME:
            continue
        capturadas.append((r, nome))

    if not capturadas:
        raise Exception("Nenhuma natureza capturada abaixo de 'SALDO FINAL DE CAIXA'.")

    # -----------------------------------------------------------------
    # 5. Último mês com dados (para não gravar meses futuros zerados)
    #    >>> Para gravar SEMPRE os 12 meses, troque a linha abaixo por:
    #        ultimo_mes = max(mapa_mes.keys())
    # -----------------------------------------------------------------
    ultimo_mes = 0
    for mes, col in mapa_mes.items():
        for (r, _nome) in capturadas:
            val = ws.cell(r, col).value or 0
            if val not in (0, None):
                ultimo_mes = max(ultimo_mes, mes)
                break
    if ultimo_mes == 0:
        ultimo_mes = max(mapa_mes.keys())

    meses_do_arquivo = [f"{ano}{m:02d}" for m in range(1, ultimo_mes + 1)]

    # -----------------------------------------------------------------
    # 6. Idempotência: apaga os meses do arquivo antes de reinserir
    # -----------------------------------------------------------------
    apagados = BoletimFinanceiro.query.filter(
        BoletimFinanceiro.MES_EXECUCAO.in_(meses_do_arquivo)
    ).delete(synchronize_session=False)
    db.session.flush()  # garante que o MAX(NU_LINHA) já reflita a exclusão

    # -----------------------------------------------------------------
    # 7. Monta e insere os registros
    # -----------------------------------------------------------------
    proximo_nu = BoletimFinanceiro.obter_proximo_nu_linha()
    registros = []
    for (r, nome) in capturadas:
        for mes in range(1, ultimo_mes + 1):
            col = mapa_mes[mes]
            vr = _to_decimal_2(ws.cell(r, col).value)
            registros.append(BoletimFinanceiro(
                NU_LINHA=proximo_nu,
                NATUREZA=nome,
                MES_EXECUCAO=f"{ano}{mes:02d}",
                VR_EXECUTADO=vr,
            ))
            proximo_nu += 1

    db.session.bulk_save_objects(registros)
    db.session.commit()

    return {
        'ano': ano,
        'naturezas': len(capturadas),
        'meses': ultimo_mes,
        'apagados': int(apagados or 0),
        'inseridos': len(registros),
    }


# =========================================================================
# PÁGINA PRINCIPAL
# =========================================================================
@boletim_financeiro_bp.route('/')
@login_required
def index():
    """Página principal do Boletim Financeiro (upload da ETAPA 1)."""
    total_registros = BoletimFinanceiro.query.count()
    return render_template(
        'boletim_financeiro/index.html',
        total_registros=total_registros,
    )


# =========================================================================
# UPLOAD DO EXCEL DO BOLETIM (ETAPA 1)
# =========================================================================
@boletim_financeiro_bp.route('/upload', methods=['POST'])
@login_required
def upload():
    """Recebe o Excel do Boletim e grava em FIN_TB020_BOLETIM_FINANCEIRO."""
    if 'arquivo' not in request.files:
        return jsonify({'success': False, 'message': 'Nenhum arquivo enviado.'}), 400

    arquivo = request.files['arquivo']
    nome_arquivo = (arquivo.filename or '').strip()

    if not nome_arquivo:
        return jsonify({'success': False, 'message': 'Nome do arquivo está vazio.'}), 400

    if not nome_arquivo.lower().endswith(('.xlsx', '.xlsm')):
        return jsonify({'success': False, 'message': 'Arquivo deve ser .xlsx ou .xlsm.'}), 400

    caminho_tmp = os.path.join(
        tempfile.gettempdir(),
        f'boletim_upload_{datetime.now().strftime("%Y%m%d_%H%M%S")}_{nome_arquivo}'
    )
    try:
        arquivo.save(caminho_tmp)
    except Exception as e:
        return jsonify({'success': False, 'message': f'Falha ao salvar arquivo temporário: {e}'}), 500

    try:
        resumo = _processar_excel_boletim(caminho_tmp)

        registrar_log(
            acao='carga',
            entidade='boletim_financeiro',
            entidade_id=None,
            descricao=f'Upload do Boletim Financeiro ({nome_arquivo})',
            dados_novos={'arquivo': nome_arquivo, **resumo},
        )

        return jsonify({
            'success': True,
            'message': (
                f'Boletim {resumo["ano"]} carregado. '
                f'{resumo["naturezas"]} natureza(s) × {resumo["meses"]} mês(es) = '
                f'{resumo["inseridos"]} registro(s) inserido(s) '
                f'({resumo["apagados"]} substituído(s)).'
            ),
            **resumo,
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': f'Erro ao processar planilha: {str(e)}'}), 500

    finally:
        try:
            if os.path.exists(caminho_tmp):
                os.remove(caminho_tmp)
        except Exception:
            pass