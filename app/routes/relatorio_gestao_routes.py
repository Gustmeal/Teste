from os import abort

from flask import Blueprint, render_template, jsonify
from flask_login import login_required
from sqlalchemy import text

from app import db
from app.models.relatorio_gestao_item import RelatorioGestaoItem
from app.utils.relatorio_gestao import partes_posicao, renderizar_pagina
from app.utils.relatorio_gestao_textos import SUMARIO_EXECUTIVO
import re
from decimal import Decimal
from app.models.relatorio_resultado_financeiro import RelatorioResultadoFinanceiro
from app.utils.relatorio_gestao import partes_posicao, renderizar_pagina, montar_consideracoes
from app.models.relatorio_consideracoes_item import RelatorioConsideracoesItem
from app.models.quadro_rentabilidade import QuadroRentabilidade
from app.utils.relatorio_gestao import (
    partes_posicao, renderizar_pagina, montar_consideracoes, _fmt_br, preencher_fragmento
)
from flask_login import current_user


from app.utils.relatorio_gestao import (
    partes_posicao, renderizar_pagina, montar_consideracoes, montar_sumario,
    _fmt_br, preencher_fragmento
)


relatorio_gestao_bp = Blueprint(
    'relatorio_gestao', __name__, url_prefix='/relatorio-gestao'
)

# Valor de PAGINA gravado na FIN_TB023 (confirmado no seu SELECT: 'SUMARIO').
PAGINA_SUMARIO = 'SUMARIO'

_MESES_LBL = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun',
              'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']


_MESES_ABREV = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun',
                'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']

_MESES_NOME = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
               'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']

FUNDO_BB_EXCLUSIVO = 'BB RF Exclusivo'

FUNDO_CAIXA_XXI = 'CAIXA RF Exclusivo XXI'

FUNDO_FAE2 = 'Extramercado FAE 2'

FUNDO_COMP_BB = 'BB Exclusivo Extramercado Emgea'

FUNDO_COMP_XXI = 'CAIXA Extramercado Exclusivo XXI'

FUNDO_COMP_FAE2 = 'BB Extramercado FAE 2'

# Nomes amigáveis dos fundos para o quadro comparativo do Sumário
_FUNDO_DISPLAY = {
    'BB RF Exclusivo': 'BB Exclusivo Extramercado Emgea',
    'CAIXA RF Exclusivo XXI': 'Caixa Econômica Exclusivo XXI',
    'Extramercado FAE 2': 'BB Extramercado FAE 2',
}

def _hierarquia(nat):
    """
    Extrai nível/numero/nome a partir do código no início da NATUREZA.
    Aceita o separador do nome como espaço OU ponto:
      '02.01 Carteira de Créditos Comerciais' -> nivel=1, numero='01', nome='...'
      '03.02 Tributos/Encargos'               -> nivel=1, numero='02', nome='...'
      '02.06.01 Monetização CVS'              -> nivel=2, numero='6.1', nome='...'
    Sem código (Saldo Inicial, Ingressos, Saídas, Saldo Final,
    Resultado Financeiro...) -> nivel=0.
    """
    nat = (nat or '').strip()
    m = re.match(r'^(\d{1,2}(?:\.\d{1,2})+)[.\s]+(.*)$', nat)
    if not m:
        return {'nivel': 0, 'numero': '', 'nome': nat}
    segs = m.group(1).split('.')
    # nível 1 = "NN.NN" (2 segmentos); nível 2 = "NN.NN.NN" (3 segmentos)...
    nivel = len(segs) - 1
    if nivel == 1:
        numero = segs[1]                     # mantém o "01", "02" como no relatório
    else:
        numero = '.'.join(str(int(s)) for s in segs[1:])  # 6.1, 6.2...
    return {'nivel': nivel, 'numero': numero, 'nome': m.group(2).strip()}

def _rotulo_mes_ano(ano, mes):
    return f"{_MESES_ABREV[mes - 1]}/{ano}" if 1 <= mes <= 12 else str(ano)


def _mes_anterior(ano, mes):
    return (ano - 1, 12) if mes <= 1 else (ano, mes - 1)


def _fmt_cell(v, tipo):
    """Formata a célula no padrão BR. Retorna {'txt', 'neg'}."""
    if v is None:
        return {'txt': '-', 'neg': False}
    d = Decimal(str(v))
    neg = d < 0
    inteiro, _, dec = f"{abs(d):.2f}".partition('.')
    inteiro = re.sub(r'(?<=\d)(?=(?:\d{3})+$)', '.', inteiro)
    txt = ('-' if neg else '') + f"{inteiro},{dec}" + ('%' if tipo == 'perc' else '')
    return {'txt': txt, 'neg': neg}

def _dados_grafico_ingressos(mes_limite=12):
    """
    Lê a FIN_VW013 e devolve {labels, datasets} no formato do Chart.js.

    Agora filtra os resultados para usar sempre o maior ANO retornado pela view,
    ou seja, só serão exibidas as séries do ano mais recente.
    """
    sql = text("""
        SELECT ANO, ITEM,
               [JAN] AS M1, [FEV] AS M2, [MAR] AS M3, [ABR] AS M4,
               [MAI] AS M5, [JUN] AS M6, [JUL] AS M7, [AGO] AS M8,
               [SET] AS M9, [OUT] AS M10, [NOV] AS M11, [DEZ] AS M12
        FROM [BDG].[FIN_VW013_GRAFICO_INGRESSOS_SUMARIO_RG]
        ORDER BY ANO, ITEM
    """)
    rows = db.session.execute(sql).fetchall()

    n = max(1, min(12, int(mes_limite or 12)))
    labels = _MESES_LBL[:n]

    # Recolhe todos os anos e os registros brutos
    anos = set()
    brutos = []
    for r in rows:
        # r[0] é ANO; convertemos para int quando possível para comparação correta
        try:
            ano = int(r[0])
        except (TypeError, ValueError):
            ano = r[0]
        item = (r[1] or '').strip()
        anos.add(ano)
        valores = []
        for i in range(n):
            v = r[2 + i]
            valores.append(float(v) if v is not None else 0.0)
        brutos.append({'ano': ano, 'item': item, 'data': valores})

    # Se houver anos, filtra para manter apenas o maior ano
    if anos:
        try:
            max_ano = max(int(a) for a in anos)
        except Exception:
            # fallback: compara diretamente
            max_ano = max(anos)
        brutos = [b for b in brutos if b['ano'] == max_ano]

    # Monta datasets apenas com os registros filtrados (maior ano)
    datasets = []
    for b in brutos:
        if b['item']:
            rotulo = b['item']
        else:
            rotulo = str(b['ano'])
        datasets.append({'label': rotulo, 'data': b['data']})

    return {'labels': labels, 'datasets': datasets}

def _dados_grafico_saidas(ano=None, mes_limite=12):
    """
    Lê a FIN_VW014 (Saídas) do maior ANO disponível e devolve {labels, datasets}
    no formato do Chart.js. Mesma estrutura da VW013.
    - [SET] entre colchetes por ser palavra reservada.
    """
    if ano is None:
        ano = db.session.execute(text(
            "SELECT MAX(ANO) FROM [BDG].[FIN_VW014_GRAFICO_SAIDAS_SUMARIO_RG]"
        )).scalar()

    sql = text("""
        SELECT ANO, ITEM,
               [JAN] AS M1, [FEV] AS M2, [MAR] AS M3, [ABR] AS M4,
               [MAI] AS M5, [JUN] AS M6, [JUL] AS M7, [AGO] AS M8,
               [SET] AS M9, [OUT] AS M10, [NOV] AS M11, [DEZ] AS M12
        FROM [BDG].[FIN_VW014_GRAFICO_SAIDAS_SUMARIO_RG]
        WHERE ANO = :ano
        ORDER BY ITEM
    """)
    rows = db.session.execute(sql, {'ano': ano}).fetchall()

    n = max(1, min(12, int(mes_limite or 12)))
    labels = _MESES_LBL[:n]

    datasets = []
    for r in rows:
        item = (r[1] or '').strip()
        valores = []
        for i in range(n):
            v = r[2 + i]
            valores.append(float(v) if v is not None else 0.0)
        datasets.append({'label': item or str(r[0]), 'data': valores})

    return {'labels': labels, 'datasets': datasets}

def _dados_grafico_view(view, mes_limite=12, ano=None):
    """Leitor genérico de view (ANO, ITEM, JAN..DEZ) do maior ANO.
    Devolve {labels, datasets, ano} para o Chart.js. [SET] entre colchetes."""
    if ano is None:
        ano = db.session.execute(text(
            f"SELECT MAX(ANO) FROM [BDG].[{view}]"
        )).scalar()

    sql = text(f"""
        SELECT ANO, ITEM,
               [JAN] AS M1, [FEV] AS M2, [MAR] AS M3, [ABR] AS M4,
               [MAI] AS M5, [JUN] AS M6, [JUL] AS M7, [AGO] AS M8,
               [SET] AS M9, [OUT] AS M10, [NOV] AS M11, [DEZ] AS M12
        FROM [BDG].[{view}]
        WHERE ANO = :ano
        ORDER BY ITEM
    """)
    rows = db.session.execute(sql, {'ano': ano}).fetchall()

    n = max(1, min(12, int(mes_limite or 12)))
    labels = _MESES_LBL[:n]
    datasets = []
    for r in rows:
        item = (r[1] or '').strip()
        valores = [float(r[2 + i]) if r[2 + i] is not None else 0.0 for i in range(n)]
        datasets.append({'label': item or str(r[0]), 'data': valores})
    return {'labels': labels, 'datasets': datasets, 'ano': ano}

def _fmt_pct_rent(v):
    """Valor já em percentual -> 'x,xx%'. Só formata em BR e adiciona '%'.
    Ex.: 1.24 -> '1,24%' ; 103.12 -> '103,12%'. '-' se None."""
    if v is None:
        return '-'
    d = Decimal(str(v)).quantize(Decimal('0.01'))
    return f"{d:.2f}".replace('.', ',') + '%'


def _mes_abrev_de_anomes(anomes):
    """'202605' -> 'Maio' ; '202512' -> 'Dez/2025' (mostra ano se != referência)."""
    s = str(anomes or '')
    if len(s) < 6:
        return s
    return _MESES_ABREV[int(s[4:6]) - 1] + '/' + s[:4]

def _montar_texto_bloqueios(registros):
    """Monta o texto dos bloqueios (FIN_VW025) trocando '...' pelo VR em módulo,
    na ordem do ID. Mesma mecânica das Considerações."""
    partes = []
    for r in registros:
        partes.append(preencher_fragmento(getattr(r, 'TEXTO', None),
                                           getattr(r, 'VR', None)))
    texto = ' '.join(p for p in partes if p)
    texto = re.sub(r'\s+', ' ', texto).strip()
    return re.sub(r'\s+([,.;:)%])', r'\1', texto)

def _dados_view_itens(view, mes_limite=12):
    """Leitor genérico (ANO, ITEM, JAN..DEZ) do maior ANO -> {labels, datasets}."""
    ano = db.session.execute(text(f"SELECT MAX(ANO) FROM [BDG].[{view}]")).scalar()
    rows = []
    if ano is not None:
        rows = db.session.execute(text(f"""
            SELECT ITEM, [JAN] M1,[FEV] M2,[MAR] M3,[ABR] M4,[MAI] M5,[JUN] M6,
                   [JUL] M7,[AGO] M8,[SET] M9,[OUT] M10,[NOV] M11,[DEZ] M12
            FROM [BDG].[{view}] WHERE ANO = :a ORDER BY ITEM
        """), {'a': ano}).fetchall()
    n = max(1, min(12, int(mes_limite or 12)))
    labels = ['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez'][:n]
    datasets = []
    for r in rows:
        datasets.append({'label': (r[0] or '').strip(),
                         'data': [float(r[1+i]) if r[1+i] is not None else 0.0 for i in range(n)]})
    return {'labels': labels, 'datasets': datasets}


def _dados_rentabilidade(fundo):
    cols = ['PERF_MES', 'IRF_M1', 'TMS', 'IRF_M1_COMP_MENSAL', 'TMS_COMP_MENSAL',
            'IRF_M1_COMP_ANUAL', 'TMS_COMP_ANUAL']
    linhas, labels, irf, tms = [], [], [], []
    for r in QuadroRentabilidade.carregar_por_fundo(fundo):
        linhas.append({'mes': _mes_abrev_de_anomes(r.ANO_MES),
                       'cells': [_fmt_pct_rent(getattr(r, c)) for c in cols]})
        if r.IRF_M1_COMP_ANUAL is not None or r.TMS_COMP_ANUAL is not None:
            labels.append(_mes_abrev_de_anomes(r.ANO_MES))
            irf.append(float(r.IRF_M1_COMP_ANUAL) if r.IRF_M1_COMP_ANUAL is not None else 0.0)
            tms.append(float(r.TMS_COMP_ANUAL) if r.TMS_COMP_ANUAL is not None else 0.0)
    return {'linhas': linhas, 'grafico': {'labels': labels,
            'datasets': [{'label': 'IRF-M 1', 'data': irf}, {'label': 'TMS', 'data': tms}]}}


def _dados_composicao(dsc_fundo):
    ano = db.session.execute(text(
        "SELECT MAX(LEFT(ANO_MES,4)) FROM [BDG].[FIN_VW027_COMPOSICAO_FI] WHERE DSC_FUNDO = :f"
    ), {'f': dsc_fundo}).scalar()
    rows = []
    if ano is not None:
        rows = db.session.execute(text("""
            SELECT ANO_MES, LFT, PC_LFT, [NTN-F], PC_NTN, OC, PC_OC, LTN, PC_LTN, TOTAL
            FROM [BDG].[FIN_VW027_COMPOSICAO_FI]
            WHERE DSC_FUNDO = :f AND LEFT(ANO_MES,4) = :a ORDER BY ANO_MES
        """), {'f': dsc_fundo, 'a': ano}).fetchall()
    vol = lambda v: _fmt_br(Decimal(str(v)), 2) if v is not None else '-'
    pc = lambda v: (_fmt_br(Decimal(str(v)), 2) + '%') if v is not None else '-'
    linhas, labels, g = [], [], {'lft': [], 'ntn': [], 'oc': [], 'ltn': []}
    for r in rows:
        linhas.append({'mes': _mes_abrev_de_anomes(r[0]),
                       'lft': vol(r[1]), 'pc_lft': pc(r[2]), 'ntn': vol(r[3]), 'pc_ntn': pc(r[4]),
                       'oc': vol(r[5]), 'pc_oc': pc(r[6]), 'ltn': vol(r[7]), 'pc_ltn': pc(r[8]),
                       'total': vol(r[9])})
        if r[9] is not None:
            labels.append(_mes_abrev_de_anomes(r[0]))
            g['lft'].append(float(r[1] or 0)); g['ntn'].append(float(r[3] or 0))
            g['oc'].append(float(r[5] or 0)); g['ltn'].append(float(r[7] or 0))
    return {'linhas': linhas, 'grafico': {'labels': labels, 'datasets': [
        {'label': 'LFT', 'data': g['lft']}, {'label': 'NTN-F', 'data': g['ntn']},
        {'label': 'OC', 'data': g['oc']}, {'label': 'LTN', 'data': g['ltn']}]}}


def _dados_disponibilidades():
    ano = db.session.execute(text(
        "SELECT MAX(ANO) FROM [BDG].[FIN_VW023_DISPONIBILIDADES_CONTAS_EMGEA]")).scalar()
    rows = []
    if ano is not None:
        rows = db.session.execute(text("""
            SELECT MES, CT_CORRENTES, BB_EXCLUSIVO, CX_EXCLUSIVO, FAE_2, TOTAL
            FROM [BDG].[FIN_VW023_DISPONIBILIDADES_CONTAS_EMGEA]
            WHERE ANO = :a ORDER BY MES_EXECUCAO
        """), {'a': ano}).fetchall()
    f = lambda v: _fmt_br(Decimal(str(v)), 2) if v is not None else '-'
    linhas, labels, g = [], [], {'cc': [], 'bb': [], 'cx': [], 'fae': []}
    for r in rows:
        linhas.append({'mes': r[0] or '', 'cc': f(r[1]), 'bb': f(r[2]),
                       'cx': f(r[3]), 'fae': f(r[4]), 'total': f(r[5])})
        if r[5] is not None:
            labels.append(r[0] or '')
            g['cc'].append(float(r[1] or 0)); g['bb'].append(float(r[2] or 0))
            g['cx'].append(float(r[3] or 0)); g['fae'].append(float(r[4] or 0))
    reg = db.session.execute(text("""
        SELECT ID, TEXTO, VR FROM [BDG].[FIN_VW025_DISPONIBILIDADES_BLOQUEIOS_JUDICIAIS_TEXTO]
        ORDER BY ID""")).fetchall()

    class _F:
        def __init__(self, t, v): self.TEXTO = t; self.VR = v
    texto = _montar_texto_bloqueios([_F(r[1], r[2]) for r in reg])
    grafico = {'labels': labels, 'datasets': [
        {'label': 'Contas Correntes', 'data': g['cc']}, {'label': 'BB Exclusivo', 'data': g['bb']},
        {'label': 'Caixa Exclusivo XXI', 'data': g['cx']}, {'label': 'BB FAE 2', 'data': g['fae']}]}
    return {'linhas': linhas, 'texto': texto, 'grafico': grafico}


def _dados_titulos():
    dt = db.session.execute(text("SELECT MAX(DT_POSICAO) FROM [BDG].[FIN_VW026_TITULOS_CUSTODIADOS_RG]")).scalar()
    linhas, total = [], Decimal('0')
    if dt is not None:
        for r in db.session.execute(text("""
                SELECT TIPO, QTDE, VR_TOTAL FROM [BDG].[FIN_VW026_TITULOS_CUSTODIADOS_RG]
                WHERE DT_POSICAO = :d ORDER BY TIPO"""), {'d': dt}).fetchall():
            vr = Decimal(str(r[2])) if r[2] is not None else Decimal('0')
            total += vr
            linhas.append({'tipo': r[0] or '',
                           'qtde': _fmt_br(Decimal(str(r[1])), 0) if r[1] is not None else '-',
                           'valor': _fmt_br(vr, 2)})
    return {'linhas': linhas, 'total': _fmt_br(total, 2),
            'data': dt.strftime('%d/%m/%Y') if dt else '—'}


def _dados_resultado_financeiro(ano_int, mes_num):
    if not ano_int:
        return None
    ano_ant = ano_int - 1
    a_ano, a_mes = _mes_anterior(ano_int, mes_num)
    _, _, mes_ref_cap = partes_posicao(f"{ano_int}{mes_num:02d}")
    mrn, man = _MESES_NOME[mes_num - 1], _MESES_NOME[a_mes - 1]
    grupos = [
        {'titulo': str(ano_int), 'cols': [
            {'sub': man, 'attr': 'VR_MES_ANTERIOR', 'tipo': 'moeda'},
            {'sub': mrn, 'attr': 'VR_MES_ATUAL', 'tipo': 'moeda'},
            {'sub': 'Acumulado', 'attr': 'VR_ACUMUL_ATE_MES', 'tipo': 'moeda'}]},
        {'titulo': str(ano_ant), 'cols': [
            {'sub': mrn, 'attr': 'VR_ANO_ANTERIOR', 'tipo': 'moeda'},
            {'sub': 'Acumulado', 'attr': 'VR_ACUMUL_ATE_MES_ANO_ANT', 'tipo': 'moeda'}]},
        {'titulo': f'∆ {ano_int} x {ano_ant}', 'cols': [
            {'sub': '∆ % Mês', 'attr': 'VARIACAO_ANUAL_PERC', 'tipo': 'perc'},
            {'sub': '∆ % Acum.', 'attr': 'VARIACAO_ANUAL_ACUML_PERC', 'tipo': 'perc'}]},
        {'titulo': f'∆ {ano_int} x {ano_ant}', 'cols': [
            {'sub': 'Mês', 'attr': 'VARIACAO_ANUAL', 'tipo': 'moeda'},
            {'sub': 'Acumulado', 'attr': 'VARIACAO_ANUAL_ACUML', 'tipo': 'moeda'}]},
        {'titulo': f'{mrn} x {man}', 'cols': [
            {'sub': '∆ %', 'attr': 'VARIACAO_MENSAL_PERC', 'tipo': 'perc'},
            {'sub': 'atual X anterior', 'attr': 'VARIACAO_MENSAL', 'tipo': 'moeda'}]},
    ]
    flat = []
    for gr in grupos:
        for i, c in enumerate(gr['cols']):
            flat.append({'attr': c['attr'], 'tipo': c['tipo'], 'sep': (i == 0)})

    # Fonte: FIN_VW031_RELATORIO_GESTAO_RESULTADO_FINANCEIRO (mesmas colunas da FIN_TB024)
    registros = db.session.execute(text("""
        SELECT NU_LINHA, NATUREZA,
               VR_MES_ANTERIOR, VR_MES_ATUAL, VR_ACUMUL_ATE_MES,
               VR_ANO_ANTERIOR, VR_ACUMUL_ATE_MES_ANO_ANT,
               VARIACAO_ANUAL_PERC, VARIACAO_ANUAL_ACUML_PERC,
               VARIACAO_ANUAL, VARIACAO_ANUAL_ACUML,
               VARIACAO_MENSAL_PERC, VARIACAO_MENSAL
        FROM [BDG].[FIN_VW031_RELATORIO_GESTAO_RESULTADO_FINANCEIRO]
        WHERE ANO = :ano AND MES = :mes
        ORDER BY NU_LINHA
    """), {'ano': ano_int, 'mes': mes_num}).fetchall()

    linhas = []
    for row in registros:
        h = _hierarquia((row.NATUREZA or '').strip())
        cells = []
        for col in flat:
            cell = _fmt_cell(getattr(row, col['attr']), col['tipo']); cell['sep'] = col['sep']
            cells.append(cell)
        linhas.append({'nivel': h['nivel'], 'numero': h['numero'], 'nome': h['nome'], 'cells': cells})

    # Split em Ingressos / Saídas para paginar sem cortar no PDF
    idx = None
    for i, l in enumerate(linhas):
        nome_up = l['nome'].strip().upper()
        if l['nivel'] == 0 and (nome_up.startswith('SAÍDA') or nome_up.startswith('SAIDA')):
            idx = i
            break
    bloco1 = linhas[:idx] if idx is not None else linhas
    bloco2 = linhas[idx:] if idx is not None else []

    return {'grupos': grupos, 'linhas': linhas, 'mes_ref_cap': mes_ref_cap,
            'bloco1': bloco1, 'bloco2': bloco2}

def _dados_quadro_comparativo():
    """Quadro Rentabilidade Acumulada dos Fundos (FIN_VW030) — já vem em %."""
    ano = db.session.execute(text(
        "SELECT MAX(ANO) FROM [BDG].[FIN_VW030_QUADRO_COMPARATIVO_SUMARIO]")).scalar()
    linhas = []
    if ano is not None:
        rows = db.session.execute(text("""
            SELECT FUNDO, PERC, IRF_M1, TMS
            FROM [BDG].[FIN_VW030_QUADRO_COMPARATIVO_SUMARIO]
            WHERE ANO = :a ORDER BY FUNDO
        """), {'a': ano}).fetchall()
        pc = lambda v: (_fmt_br(Decimal(str(v)), 2) + '%') if v is not None else '-'
        for r in rows:
            raw = (r[0] or '').strip()
            linhas.append({'fundo': _FUNDO_DISPLAY.get(raw, raw),
                           'perc': pc(r[1]), 'irf': pc(r[2]), 'tms': pc(r[3])})
    return {'ano': ano, 'linhas': linhas}


@relatorio_gestao_bp.route('/sumario-executivo')
@login_required
def sumario_executivo():
    """Sumário Executivo — frases vindas da FIN_TB023_RG_SUMARIO (mesma
    mecânica das Considerações), com os gráficos e o quadro mantidos."""
    posicao = db.session.execute(text(
        "SELECT MAX(POSICAO) FROM [BDG].[FIN_TB023_RG_SUMARIO]"
    )).scalar()

    if posicao:
        ano_ref, _mes, mes_ref_cap = partes_posicao(posicao)
        sem_dados = False
        try:
            mes_num = int(str(posicao)[4:6])
        except (ValueError, TypeError):
            mes_num = 12
        registros = db.session.execute(text("""
            SELECT ID, SUBITEM, VR, TEXTO
            FROM [BDG].[FIN_TB023_RG_SUMARIO]
            WHERE POSICAO = :p AND ID <> 34
            ORDER BY ID
        """), {'p': posicao}).fetchall()
        blocos = montar_sumario(registros)
    else:
        ano_ref, mes_ref_cap = '—', '—'
        sem_dados = True
        mes_num = 12
        blocos = []

    try:
        grafico_ingressos = _dados_grafico_ingressos(mes_limite=mes_num)
    except Exception as e:
        grafico_ingressos = {'labels': [], 'datasets': [], 'erro': str(e)}

    try:
        grafico_saidas = _dados_grafico_saidas(mes_limite=mes_num)
    except Exception as e:
        grafico_saidas = {'labels': [], 'datasets': [], 'erro': str(e)}

    return render_template(
        'relatorio_gestao/sumario_executivo.html',
        blocos=blocos,
        mes_ref_cap=mes_ref_cap,
        ano_ref=ano_ref,
        sem_dados=sem_dados,
        grafico_ingressos=grafico_ingressos,
        grafico_saidas=grafico_saidas,
        quadro=_dados_quadro_comparativo(),
    )


@relatorio_gestao_bp.route('/resultado-financeiro')
@login_required
def resultado_financeiro():
    """Página 2 do relatório — tabela Resultado Financeiro (FIN_VW031)."""
    # Mês de referência: o mais recente que EXISTE na própria view.
    ref = db.session.execute(text("""
        SELECT TOP 1 ANO, MES
        FROM [BDG].[FIN_VW031_RELATORIO_GESTAO_RESULTADO_FINANCEIRO]
        ORDER BY ANO DESC, MES DESC
    """)).fetchone()

    ano_ref = mes_num = None
    if ref:
        ano_ref, mes_num = int(ref[0]), int(ref[1])
    else:
        # Plano B: competência do Sumário
        posicao = RelatorioGestaoItem.obter_posicao_referencia(PAGINA_SUMARIO)
        if posicao and str(posicao)[:6].isdigit():
            ano_ref = int(str(posicao)[:4])
            mes_num = int(str(posicao)[4:6])

    dados = _dados_resultado_financeiro(ano_ref, mes_num)

    if not dados:
        return render_template('relatorio_gestao/resultado_financeiro.html',
                               sem_dados=True, grupos=[], linhas=[],
                               mes_ref_cap='—', ano_ref='—')

    return render_template(
        'relatorio_gestao/resultado_financeiro.html',
        sem_dados=(len(dados['linhas']) == 0),
        grupos=dados['grupos'],
        linhas=dados['linhas'],
        mes_ref_cap=dados['mes_ref_cap'],
        ano_ref=ano_ref,
    )

@relatorio_gestao_bp.route('/consideracoes')
@login_required
def consideracoes():
    """Página 3 do relatório — Considerações (FIN_TB025) + gráficos VW015/VW016."""
    posicao = RelatorioConsideracoesItem.obter_posicao_referencia()
    vazio = {'labels': [], 'datasets': []}
    if not posicao:
        return render_template('relatorio_gestao/consideracoes.html',
                               sem_dados=True, secoes=[], mes_ref_cap='—', ano_ref='—',
                               grafico_ingressos_consid=vazio, grafico_saidas_consid=vazio)

    ano_ref, _mes, mes_ref_cap = partes_posicao(posicao)
    try:
        mes_num = int(str(posicao)[4:6])
    except (ValueError, TypeError):
        mes_num = 12

    registros = RelatorioConsideracoesItem.carregar(posicao)
    secoes = montar_consideracoes(registros)

    try:
        g_ing = _dados_grafico_view('FIN_VW015_GRAFICO_INGRESSOS_CONSIDERACOES_RG', mes_limite=mes_num)
        g_ing['titulo'] = f'Evolução dos Ingressos Operacionais em {g_ing.get("ano", ano_ref)}'
    except Exception as e:
        g_ing = {'labels': [], 'datasets': [], 'erro': str(e)}

    try:
        g_sai = _dados_grafico_view('FIN_VW016_GRAFICO_SAIDAS_CONSIDERACOES_RG', mes_limite=mes_num)
        g_sai['titulo'] = f'Evolução dos Desembolsos Operacionais em {g_sai.get("ano", ano_ref)}'
    except Exception as e:
        g_sai = {'labels': [], 'datasets': [], 'erro': str(e)}

    return render_template('relatorio_gestao/consideracoes.html',
                           sem_dados=False, secoes=secoes,
                           mes_ref_cap=mes_ref_cap, ano_ref=ano_ref,
                           grafico_ingressos_consid=g_ing,
                           grafico_saidas_consid=g_sai)


@relatorio_gestao_bp.route('/rentabilidade-bb-exclusivo')
@login_required
def rentabilidade_bb_exclusivo():
    """Página 'Rentabilidade BB Exc' — tabela + gráfico (FIN_TB031)."""
    linhas_db = QuadroRentabilidade.carregar_por_fundo(FUNDO_BB_EXCLUSIVO)

    # Colunas da tabela (na ordem do Excel, SEM as acumuladas): (attr)
    cols = [
        'PERF_MES', 'IRF_M1', 'TMS',
        'IRF_M1_COMP_MENSAL', 'TMS_COMP_MENSAL',
        'IRF_M1_COMP_ANUAL', 'TMS_COMP_ANUAL',
    ]

    linhas = []
    graf_labels, graf_irf_anual, graf_tms_anual = [], [], []
    for r in linhas_db:
        linhas.append({
            'mes': _mes_abrev_de_anomes(r.ANO_MES),
            'cells': [_fmt_pct_rent(getattr(r, c)) for c in cols],
        })
        # Gráfico: Comparativo Rentabilidade Acumulada no Ano (IRF-M 1 e TMS)
        if r.IRF_M1_COMP_ANUAL is not None or r.TMS_COMP_ANUAL is not None:
            graf_labels.append(_mes_abrev_de_anomes(r.ANO_MES))
            graf_irf_anual.append(float(r.IRF_M1_COMP_ANUAL) if r.IRF_M1_COMP_ANUAL is not None else 0.0)
            graf_tms_anual.append(float(r.TMS_COMP_ANUAL) if r.TMS_COMP_ANUAL is not None else 0.0)

    grafico = {
        'labels': graf_labels,
        'datasets': [
            {'label': 'IRF-M 1', 'data': graf_irf_anual},
            {'label': 'TMS', 'data': graf_tms_anual},
        ],
    }

    return render_template(
        'relatorio_gestao/rentabilidade_bb_exclusivo.html',
        linhas=linhas, grafico=grafico, sem_dados=(len(linhas) == 0),
    )

@relatorio_gestao_bp.route('/disponibilidades')
@login_required
def disponibilidades():
    """Página Disponibilidades — tabela (VW023) + gráfico + texto de bloqueios (VW025)."""
    # ----- Tabela mensal por conta -----
    ano = db.session.execute(text(
        "SELECT MAX(ANO) FROM [BDG].[FIN_VW023_DISPONIBILIDADES_CONTAS_EMGEA]"
    )).scalar()

    linhas, graf_labels, g_cc, g_bb, g_cx, g_fae, g_total = [], [], [], [], [], [], []
    if ano is not None:
        rows = db.session.execute(text("""
            SELECT MES, MES_EXECUCAO, CT_CORRENTES, BB_EXCLUSIVO, CX_EXCLUSIVO, FAE_2, TOTAL
            FROM [BDG].[FIN_VW023_DISPONIBILIDADES_CONTAS_EMGEA]
            WHERE ANO = :ano
            ORDER BY MES_EXECUCAO
        """), {'ano': ano}).fetchall()

        def _f(v):
            if v is None:
                return '-'
            return _fmt_br(Decimal(str(v)), 2)

        for r in rows:
            linhas.append({
                'mes': (r[0] or ''),
                'cc': _f(r[2]), 'bb': _f(r[3]), 'cx': _f(r[4]),
                'fae': _f(r[5]), 'total': _f(r[6]),
            })
            if r[6] is not None:  # gráfico só meses com Total
                graf_labels.append(r[0] or '')
                g_cc.append(float(r[2]) if r[2] is not None else 0.0)
                g_bb.append(float(r[3]) if r[3] is not None else 0.0)
                g_cx.append(float(r[4]) if r[4] is not None else 0.0)
                g_fae.append(float(r[5]) if r[5] is not None else 0.0)
                g_total.append(float(r[6]) if r[6] is not None else 0.0)

    grafico = {
        'labels': graf_labels,
        'datasets': [
            {'label': 'Contas Correntes', 'data': g_cc},
            {'label': 'BB Exclusivo', 'data': g_bb},
            {'label': 'Caixa Exclusivo XXI', 'data': g_cx},
            {'label': 'BB FAE 2', 'data': g_fae},
        ],
    }

    # ----- Texto dos bloqueios judiciais (VW025) -----
    reg_texto = db.session.execute(text("""
        SELECT ID, TEXTO, VR
        FROM [BDG].[FIN_VW025_DISPONIBILIDADES_BLOQUEIOS_JUDICIAIS_TEXTO]
        ORDER BY ID
    """)).fetchall()

    class _Frag:
        def __init__(self, texto, vr):
            self.TEXTO = texto
            self.VR = vr
    texto_bloqueios = _montar_texto_bloqueios([_Frag(r[1], r[2]) for r in reg_texto])

    return render_template(
        'relatorio_gestao/disponibilidades.html',
        linhas=linhas, grafico=grafico, texto_bloqueios=texto_bloqueios,
        sem_dados=(len(linhas) == 0),
    )

@relatorio_gestao_bp.route('/rentabilidade-xxi')
@login_required
def rentabilidade_xxi():
    """Página 'Rentabilidade XXI' — mesma estrutura do BB, filtro CAIXA XXI."""
    linhas_db = QuadroRentabilidade.carregar_por_fundo(FUNDO_CAIXA_XXI)

    cols = [
        'PERF_MES', 'IRF_M1', 'TMS',
        'IRF_M1_COMP_MENSAL', 'TMS_COMP_MENSAL',
        'IRF_M1_COMP_ANUAL', 'TMS_COMP_ANUAL',
    ]

    linhas = []
    graf_labels, graf_irf_anual, graf_tms_anual = [], [], []
    for r in linhas_db:
        linhas.append({
            'mes': _mes_abrev_de_anomes(r.ANO_MES),
            'cells': [_fmt_pct_rent(getattr(r, c)) for c in cols],
        })
        if r.IRF_M1_COMP_ANUAL is not None or r.TMS_COMP_ANUAL is not None:
            graf_labels.append(_mes_abrev_de_anomes(r.ANO_MES))
            graf_irf_anual.append(float(r.IRF_M1_COMP_ANUAL) if r.IRF_M1_COMP_ANUAL is not None else 0.0)
            graf_tms_anual.append(float(r.TMS_COMP_ANUAL) if r.TMS_COMP_ANUAL is not None else 0.0)

    grafico = {
        'labels': graf_labels,
        'datasets': [
            {'label': 'IRF-M 1', 'data': graf_irf_anual},
            {'label': 'TMS', 'data': graf_tms_anual},
        ],
    }

    return render_template(
        'relatorio_gestao/rentabilidade_xxi.html',
        linhas=linhas, grafico=grafico, sem_dados=(len(linhas) == 0),
    )

@relatorio_gestao_bp.route('/rentabilidade-fae2')
@login_required
def rentabilidade_fae2():
    """Página 'Rentabilidade FAE 2' — mesma estrutura, filtro Extramercado FAE 2."""
    linhas_db = QuadroRentabilidade.carregar_por_fundo(FUNDO_FAE2)

    cols = [
        'PERF_MES', 'IRF_M1', 'TMS',
        'IRF_M1_COMP_MENSAL', 'TMS_COMP_MENSAL',
        'IRF_M1_COMP_ANUAL', 'TMS_COMP_ANUAL',
    ]

    linhas = []
    graf_labels, graf_irf_anual, graf_tms_anual = [], [], []
    for r in linhas_db:
        linhas.append({
            'mes': _mes_abrev_de_anomes(r.ANO_MES),
            'cells': [_fmt_pct_rent(getattr(r, c)) for c in cols],
        })
        if r.IRF_M1_COMP_ANUAL is not None or r.TMS_COMP_ANUAL is not None:
            graf_labels.append(_mes_abrev_de_anomes(r.ANO_MES))
            graf_irf_anual.append(float(r.IRF_M1_COMP_ANUAL) if r.IRF_M1_COMP_ANUAL is not None else 0.0)
            graf_tms_anual.append(float(r.TMS_COMP_ANUAL) if r.TMS_COMP_ANUAL is not None else 0.0)

    grafico = {
        'labels': graf_labels,
        'datasets': [
            {'label': 'IRF-M 1', 'data': graf_irf_anual},
            {'label': 'TMS', 'data': graf_tms_anual},
        ],
    }

    return render_template(
        'relatorio_gestao/rentabilidade_fae2.html',
        linhas=linhas, grafico=grafico, sem_dados=(len(linhas) == 0),
    )

def _pagina_composicao(dsc_fundo, titulo, voltar_endpoint, descer_endpoint):
    """Composição de um fundo (FIN_VW027) — tabela (Volume+% por ativo) + gráfico."""
    ano = db.session.execute(text(
        "SELECT MAX(LEFT(ANO_MES,4)) FROM [BDG].[FIN_VW027_COMPOSICAO_FI] WHERE DSC_FUNDO = :f"
    ), {'f': dsc_fundo}).scalar()

    rows = []
    if ano is not None:
        rows = db.session.execute(text("""
            SELECT ANO_MES, LFT, PC_LFT, [NTN-F], PC_NTN, OC, PC_OC, LTN, PC_LTN, TOTAL
            FROM [BDG].[FIN_VW027_COMPOSICAO_FI]
            WHERE DSC_FUNDO = :f AND LEFT(ANO_MES,4) = :ano
            ORDER BY ANO_MES
        """), {'f': dsc_fundo, 'ano': ano}).fetchall()

    def _vol(v):
        return _fmt_br(Decimal(str(v)), 2) if v is not None else '-'

    def _pc(v):
        if v is None:
            return '-'
        return _fmt_br(Decimal(str(v)), 2) + '%'

    linhas = []
    graf_labels, g_lft, g_ntn, g_oc, g_ltn = [], [], [], [], []
    for r in rows:
        linhas.append({
            'mes': _mes_abrev_de_anomes(r[0]),
            'lft': _vol(r[1]), 'pc_lft': _pc(r[2]),
            'ntn': _vol(r[3]), 'pc_ntn': _pc(r[4]),
            'oc': _vol(r[5]), 'pc_oc': _pc(r[6]),
            'ltn': _vol(r[7]), 'pc_ltn': _pc(r[8]),
            'total': _vol(r[9]),
        })
        if r[9] is not None:  # gráfico só meses com Total
            graf_labels.append(_mes_abrev_de_anomes(r[0]))
            g_lft.append(float(r[1]) if r[1] is not None else 0.0)
            g_ntn.append(float(r[3]) if r[3] is not None else 0.0)
            g_oc.append(float(r[5]) if r[5] is not None else 0.0)
            g_ltn.append(float(r[7]) if r[7] is not None else 0.0)

    grafico = {
        'labels': graf_labels,
        'datasets': [
            {'label': 'LFT', 'data': g_lft},
            {'label': 'NTN-F', 'data': g_ntn},
            {'label': 'OC', 'data': g_oc},
            {'label': 'LTN', 'data': g_ltn},
        ],
    }

    return render_template(
        'relatorio_gestao/composicao_bb.html',
        titulo=titulo, linhas=linhas, grafico=grafico, sem_dados=(len(linhas) == 0),
        voltar_endpoint=voltar_endpoint, descer_endpoint=descer_endpoint,
    )


@relatorio_gestao_bp.route('/composicao-xxi')
@login_required
def composicao_xxi():
    """Página 'Composição XXI' — tabela (Volume+% por ativo) + gráfico (FIN_VW027)."""
    dsc_fundo = FUNDO_COMP_XXI

    ano = db.session.execute(text(
        "SELECT MAX(LEFT(ANO_MES,4)) FROM [BDG].[FIN_VW027_COMPOSICAO_FI] WHERE DSC_FUNDO = :f"
    ), {'f': dsc_fundo}).scalar()

    rows = []
    if ano is not None:
        rows = db.session.execute(text("""
            SELECT ANO_MES, LFT, PC_LFT, [NTN-F], PC_NTN, OC, PC_OC, LTN, PC_LTN, TOTAL
            FROM [BDG].[FIN_VW027_COMPOSICAO_FI]
            WHERE DSC_FUNDO = :f AND LEFT(ANO_MES,4) = :ano
            ORDER BY ANO_MES
        """), {'f': dsc_fundo, 'ano': ano}).fetchall()

    def _vol(v):
        return _fmt_br(Decimal(str(v)), 2) if v is not None else '-'

    def _pc(v):
        # PC_* já vem em percentual: só formata e coloca o '%'
        return (_fmt_br(Decimal(str(v)), 2) + '%') if v is not None else '-'

    linhas = []
    graf_labels, g_lft, g_ntn, g_oc, g_ltn = [], [], [], [], []
    for r in rows:
        linhas.append({
            'mes': _mes_abrev_de_anomes(r[0]),
            'lft': _vol(r[1]), 'pc_lft': _pc(r[2]),
            'ntn': _vol(r[3]), 'pc_ntn': _pc(r[4]),
            'oc': _vol(r[5]), 'pc_oc': _pc(r[6]),
            'ltn': _vol(r[7]), 'pc_ltn': _pc(r[8]),
            'total': _vol(r[9]),
        })
        if r[9] is not None:  # gráfico só meses com Total
            graf_labels.append(_mes_abrev_de_anomes(r[0]))
            g_lft.append(float(r[1]) if r[1] is not None else 0.0)
            g_ntn.append(float(r[3]) if r[3] is not None else 0.0)
            g_oc.append(float(r[5]) if r[5] is not None else 0.0)
            g_ltn.append(float(r[7]) if r[7] is not None else 0.0)

    grafico = {
        'labels': graf_labels,
        'datasets': [
            {'label': 'LFT', 'data': g_lft},
            {'label': 'NTN-F', 'data': g_ntn},
            {'label': 'OC', 'data': g_oc},
            {'label': 'LTN', 'data': g_ltn},
        ],
    }

    return render_template(
        'relatorio_gestao/composicao_xxi.html',
        linhas=linhas, grafico=grafico, sem_dados=(len(linhas) == 0),
    )

@relatorio_gestao_bp.route('/composicao-bb')
@login_required
def composicao_bb():
    return _pagina_composicao(
        FUNDO_COMP_BB, 'BB Exclusivo Extramercado Emgea',
        voltar_endpoint='relatorio_gestao.rentabilidade_bb_exclusivo',
        descer_endpoint='relatorio_gestao.rentabilidade_xxi',
    )


@relatorio_gestao_bp.route('/composicao-fae2')
@login_required
def composicao_fae2():
    """Página 'Composição FAE 2' — tabela (Volume+% por ativo) + gráfico (FIN_VW027)."""
    dsc_fundo = FUNDO_COMP_FAE2

    ano = db.session.execute(text(
        "SELECT MAX(LEFT(ANO_MES,4)) FROM [BDG].[FIN_VW027_COMPOSICAO_FI] WHERE DSC_FUNDO = :f"
    ), {'f': dsc_fundo}).scalar()

    rows = []
    if ano is not None:
        rows = db.session.execute(text("""
            SELECT ANO_MES, LFT, PC_LFT, [NTN-F], PC_NTN, OC, PC_OC, LTN, PC_LTN, TOTAL
            FROM [BDG].[FIN_VW027_COMPOSICAO_FI]
            WHERE DSC_FUNDO = :f AND LEFT(ANO_MES,4) = :ano
            ORDER BY ANO_MES
        """), {'f': dsc_fundo, 'ano': ano}).fetchall()

    def _vol(v):
        return _fmt_br(Decimal(str(v)), 2) if v is not None else '-'

    def _pc(v):
        # PC_* já vem em percentual: só formata e coloca o '%'
        return (_fmt_br(Decimal(str(v)), 2) + '%') if v is not None else '-'

    linhas = []
    graf_labels, g_lft, g_ntn, g_oc, g_ltn = [], [], [], [], []
    for r in rows:
        linhas.append({
            'mes': _mes_abrev_de_anomes(r[0]),
            'lft': _vol(r[1]), 'pc_lft': _pc(r[2]),
            'ntn': _vol(r[3]), 'pc_ntn': _pc(r[4]),
            'oc': _vol(r[5]), 'pc_oc': _pc(r[6]),
            'ltn': _vol(r[7]), 'pc_ltn': _pc(r[8]),
            'total': _vol(r[9]),
        })
        if r[9] is not None:  # gráfico só meses com Total
            graf_labels.append(_mes_abrev_de_anomes(r[0]))
            g_lft.append(float(r[1]) if r[1] is not None else 0.0)
            g_ntn.append(float(r[3]) if r[3] is not None else 0.0)
            g_oc.append(float(r[5]) if r[5] is not None else 0.0)
            g_ltn.append(float(r[7]) if r[7] is not None else 0.0)

    grafico = {
        'labels': graf_labels,
        'datasets': [
            {'label': 'LFT', 'data': g_lft},
            {'label': 'NTN-F', 'data': g_ntn},
            {'label': 'OC', 'data': g_oc},
            {'label': 'LTN', 'data': g_ltn},
        ],
    }

    return render_template(
        'relatorio_gestao/composicao_fae2.html',
        linhas=linhas, grafico=grafico, sem_dados=(len(linhas) == 0),
    )

@relatorio_gestao_bp.route('/titulos-consolidados-bb')
@login_required
def titulos_consolidados_bb():
    """Página 'Títulos Consolidados BB' — estoque de Títulos CVS (FIN_VW026)."""
    dt_pos = db.session.execute(text(
        "SELECT MAX(DT_POSICAO) FROM [BDG].[FIN_VW026_TITULOS_CUSTODIADOS_RG]"
    )).scalar()

    linhas, total = [], Decimal('0')
    if dt_pos is not None:
        rows = db.session.execute(text("""
            SELECT TIPO, QTDE, VR_TOTAL
            FROM [BDG].[FIN_VW026_TITULOS_CUSTODIADOS_RG]
            WHERE DT_POSICAO = :dt
            ORDER BY TIPO
        """), {'dt': dt_pos}).fetchall()

        for r in rows:
            vr = Decimal(str(r[2])) if r[2] is not None else Decimal('0')
            total += vr
            linhas.append({
                'tipo': (r[0] or ''),
                'qtde': _fmt_br(Decimal(str(r[1])), 0) if r[1] is not None else '-',
                'valor': _fmt_br(vr, 2),
            })

    return render_template(
        'relatorio_gestao/titulos_consolidados_bb.html',
        linhas=linhas,
        total=_fmt_br(total, 2),
        data_posicao=dt_pos.strftime('%d/%m/%Y') if dt_pos else '—',
        sem_dados=(len(linhas) == 0),
    )

@relatorio_gestao_bp.route('/completo')
@login_required
def relatorio_completo():
    """Relatório inteiro em uma página, otimizado para impressão em PDF."""
    # Referência do relatório = mês mais recente da FIN_VW031 (mesma da tela).
    # Fallback: competência do Sumário.
    ref_rf = db.session.execute(text("""
        SELECT TOP 1 ANO, MES
        FROM [BDG].[FIN_VW031_RELATORIO_GESTAO_RESULTADO_FINANCEIRO]
        ORDER BY ANO DESC, MES DESC
    """)).fetchone()

    posicao = RelatorioGestaoItem.obter_posicao_referencia(PAGINA_SUMARIO)

    if ref_rf:
        ano_int, mes_num = int(ref_rf[0]), int(ref_rf[1])
        ano_ref, mes_ref, mes_ref_cap = partes_posicao(f"{ano_int}{mes_num:02d}")
    elif posicao and str(posicao)[:6].isdigit():
        ano_ref, mes_ref, mes_ref_cap = partes_posicao(posicao)
        ano_int, mes_num = int(str(posicao)[:4]), int(str(posicao)[4:6])
    else:
        ano_ref, mes_ref, mes_ref_cap = '—', '—', '—'
        ano_int, mes_num = None, 12

    # Mapa do Sumário (frases) continua vindo da competência do Sumário
    mapa = (RelatorioGestaoItem.carregar_mapa_id_vr(PAGINA_SUMARIO, posicao)
            if posicao and str(posicao)[:6].isdigit() else {})

    sumario_itens = renderizar_pagina(SUMARIO_EXECUTIVO, mapa, mes_ref, mes_ref_cap, ano_ref)

    # Resultado Financeiro: mesmo mês da referência (FIN_VW031)
    resultado = _dados_resultado_financeiro(ano_int, mes_num)

    pos_c = RelatorioConsideracoesItem.obter_posicao_referencia()
    consideracoes = montar_consideracoes(RelatorioConsideracoesItem.carregar(pos_c)) if pos_c else []
    disp = _dados_disponibilidades()
    rent_bb, rent_xxi, rent_fae2 = (_dados_rentabilidade(FUNDO_BB_EXCLUSIVO),
                                    _dados_rentabilidade(FUNDO_CAIXA_XXI),
                                    _dados_rentabilidade(FUNDO_FAE2))
    comp_bb, comp_xxi, comp_fae2 = (_dados_composicao(FUNDO_COMP_BB),
                                    _dados_composicao(FUNDO_COMP_XXI),
                                    _dados_composicao(FUNDO_COMP_FAE2))
    titulos = _dados_titulos()

    graficos = {
        'g_sum_ing':  {'horizontal': True,  'stacked': False, 'percent': False, 'zero_min': True,     'dados': _dados_grafico_ingressos(mes_limite=mes_num)},
        'g_sum_sai':  {'horizontal': True,  'stacked': False, 'percent': False, 'abs_negativo': True, 'dados': _dados_grafico_saidas(mes_limite=mes_num)},
        'g_con_ing':  {'horizontal': False, 'stacked': True,  'percent': False, 'dados': _dados_view_itens('FIN_VW015_GRAFICO_INGRESSOS_CONSIDERACOES_RG', mes_num)},
        'g_con_sai':  {'horizontal': False, 'stacked': True,  'percent': False, 'dados': _dados_view_itens('FIN_VW016_GRAFICO_SAIDAS_CONSIDERACOES_RG', mes_num)},
        'g_disp':     {'horizontal': False, 'stacked': True,  'percent': False, 'dados': disp['grafico']},
        'g_rent_bb':  {'horizontal': False, 'stacked': False, 'percent': True,  'dados': rent_bb['grafico']},
        'g_comp_bb':  {'horizontal': False, 'stacked': True,  'percent': False, 'dados': comp_bb['grafico']},
        'g_rent_xxi': {'horizontal': False, 'stacked': False, 'percent': True,  'dados': rent_xxi['grafico']},
        'g_comp_xxi': {'horizontal': False, 'stacked': True,  'percent': False, 'dados': comp_xxi['grafico']},
        'g_rent_fae2':{'horizontal': False, 'stacked': False, 'percent': True,  'dados': rent_fae2['grafico']},
        'g_comp_fae2':{'horizontal': False, 'stacked': True,  'percent': False, 'dados': comp_fae2['grafico']},
    }

    return render_template(
        'relatorio_gestao/relatorio_completo.html',
        mes_ref_cap=mes_ref_cap, ano_ref=ano_ref,
        sumario_itens=sumario_itens, resultado=resultado, consideracoes=consideracoes,
        disp=disp, rent_bb=rent_bb, rent_xxi=rent_xxi, rent_fae2=rent_fae2,
        comp_bb=comp_bb, comp_xxi=comp_xxi, comp_fae2=comp_fae2,
        titulos=titulos, graficos=graficos, quadro=_dados_quadro_comparativo())


@relatorio_gestao_bp.route('/teste-conferencia-saldo')
@login_required
def teste_conferencia_saldo():
    """[TEMPORÁRIO - TESTE] Confere Boletim (NU_LINHA=61) x soma FIN_TB021,
    no mês mais recente. Visível só para admin/moderador."""

    try:
        row = db.session.execute(text("""
            SELECT TOP 1
                   BF.MES_EXECUCAO,
                   BF.VR_EXECUTADO,
                   SD.SD_CONTAS,
                   BF.VR_EXECUTADO - SD.SD_CONTAS AS DIFERENCA
            FROM [BDG].[FIN_TB020_BOLETIM_FINANCEIRO] BF
            INNER JOIN (
                SELECT [MES_EXECUCAO], SUM(VR_EXECUTADO) AS SD_CONTAS
                FROM [BDG].[FIN_TB021_SALDO_CONTAS_BF]
                GROUP BY [MES_EXECUCAO]
            ) SD ON BF.[MES_EXECUCAO] = SD.[MES_EXECUCAO]
            WHERE BF.NU_LINHA = 61
            ORDER BY BF.MES_EXECUCAO DESC
        """)).fetchone()

        if not row:
            return jsonify({'success': True, 'encontrado': False,
                            'message': 'Nenhuma competência com dados nas duas tabelas.'})

        mes = str(row[0] or '')
        mes_fmt = f"{mes[4:6]}/{mes[:4]}" if len(mes) >= 6 else mes
        vr = Decimal(str(row[1] or 0))
        sd = Decimal(str(row[2] or 0))
        dif = Decimal(str(row[3] or 0))

        return jsonify({
            'success': True, 'encontrado': True,
            'mes': mes_fmt,
            'vr_executado': _fmt_br(vr, 2),
            'sd_contas': _fmt_br(sd, 2),
            'diferenca': _fmt_br(dif, 2),
            'bate': (abs(dif) < Decimal('0.005')),
        })
    except Exception as e:
        return jsonify({'success': False, 'message': f'Erro: {str(e)}'}), 500


@relatorio_gestao_bp.route('/teste-siscor-boletim')
@login_required
def teste_siscor_boletim():
    """[AUDITORIA] Boletim (NU_LINHA 2 e 21) x Execução Orçamentária SISCOR, por competência."""
    try:
        rows = db.session.execute(text("""
            SELECT BOL.[MES_EXECUCAO],
                   VR_BOLETIM,
                   VR_SISCOR,
                   VR_BOLETIM - VR_SISCOR AS DIFERENCA
            FROM (
                SELECT [MES_EXECUCAO], SUM(VR_EXECUTADO) AS VR_BOLETIM
                FROM [BDG].[FIN_TB020_BOLETIM_FINANCEIRO]
                WHERE NU_LINHA IN (2, 21)
                GROUP BY [MES_EXECUCAO]
            ) BOL
            INNER JOIN (
                SELECT [DT_EXECUCAO_ORCAMENTO], SUM(VR_EXECUCAO_ORCAMENTO) AS VR_SISCOR
                FROM [BDG].[COR_TB001_EXECUCAO_ORCAMENTARIA_SISCOR]
                GROUP BY [DT_EXECUCAO_ORCAMENTO]
            ) SIS ON BOL.[MES_EXECUCAO] = SIS.[DT_EXECUCAO_ORCAMENTO]
            ORDER BY BOL.[MES_EXECUCAO] DESC
        """)).fetchall()

        if not rows:
            return jsonify({'success': True, 'encontrado': False,
                            'message': 'Nenhuma competência com dados nas duas fontes.'})

        linhas = []
        for r in rows:
            mes = str(r[0] or '')
            mes_fmt = f"{mes[4:6]}/{mes[:4]}" if len(mes) >= 6 else mes
            dif = Decimal(str(r[3] or 0))
            linhas.append({
                'mes': mes_fmt,
                'boletim': _fmt_br(Decimal(str(r[1] or 0)), 2),
                'siscor': _fmt_br(Decimal(str(r[2] or 0)), 2),
                'diferenca': _fmt_br(dif, 2),
                'bate': (abs(dif) < Decimal('0.005')),
            })
        return jsonify({'success': True, 'encontrado': True, 'linhas': linhas})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Erro: {str(e)}'}), 500


@relatorio_gestao_bp.route('/tabelas-dados')
@login_required
def tabelas_dados():
    """Documentação das tabelas/views (restrita a admin/moderador)."""
    if current_user.perfil not in ['admin', 'moderador']:
        abort(403)
    return render_template('relatorio_gestao/tabelas_dados.html')

@relatorio_gestao_bp.route('/auditoria')
@login_required
def auditoria():
    """Página com os testes/verificações do Boletim (liberada para todos)."""
    return render_template('relatorio_gestao/auditoria.html')