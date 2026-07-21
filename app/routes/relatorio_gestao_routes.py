from flask import Blueprint, render_template
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


def _hierarquia(nat):
    """
    Extrai nível/numero/nome a partir do código no início da NATUREZA.
    Ex.: '02.06.01.Monetização CVS' -> nivel=2, numero='6.1', nome='Monetização CVS'.
    Sem código (Saldo Inicial, Ingressos, Saídas) -> nivel=0.
    """
    m = re.match(r'^(\d{1,2}(?:\.\d{1,2})+)\.\s*(.*)$', nat or '')
    if not m:
        return {'nivel': 0, 'numero': '', 'nome': (nat or '').strip()}
    segs = m.group(1).split('.')
    numero = '.'.join(str(int(s)) for s in segs[1:])  # remove 1º segmento e zeros
    return {'nivel': len(segs) - 1, 'numero': numero, 'nome': m.group(2).strip()}

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

@relatorio_gestao_bp.route('/sumario-executivo')
@login_required
def sumario_executivo():
    """Sumário Executivo com valores, mês de referência e gráfico automáticos."""
    posicao = RelatorioGestaoItem.obter_posicao_referencia(PAGINA_SUMARIO)

    if posicao:
        ano_ref, mes_ref, mes_ref_cap = partes_posicao(posicao)
        mapa = RelatorioGestaoItem.carregar_mapa_id_vr(PAGINA_SUMARIO, posicao)
        sem_dados = False
        try:
            mes_num = int(str(posicao)[4:6])
        except (ValueError, TypeError):
            mes_num = 12
    else:
        ano_ref, mes_ref, mes_ref_cap = '—', '—', '—'
        mapa = {}
        sem_dados = True
        mes_num = 12

    itens = renderizar_pagina(SUMARIO_EXECUTIVO, mapa, mes_ref, mes_ref_cap, ano_ref)

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
        itens=itens,
        mes_ref_cap=mes_ref_cap,
        ano_ref=ano_ref,
        sem_dados=sem_dados,
        grafico_ingressos=grafico_ingressos,
        grafico_saidas=grafico_saidas,
    )

@relatorio_gestao_bp.route('/resultado-financeiro')
@login_required
def resultado_financeiro():
    """Página 2 do relatório — tabela Resultado Financeiro (FIN_TB024)."""
    posicao = RelatorioGestaoItem.obter_posicao_referencia(PAGINA_SUMARIO)
    ano_ref = mes_num = None
    if posicao and str(posicao)[:6].isdigit():
        ano_ref = int(str(posicao)[:4])
        mes_num = int(str(posicao)[4:6])
    else:
        comp = RelatorioResultadoFinanceiro.obter_ano_mes_referencia()
        if comp:
            ano_ref, mes_num = int(comp[0]), int(comp[1])

    if not ano_ref:
        return render_template('relatorio_gestao/resultado_financeiro.html',
                               sem_dados=True, grupos=[], linhas=[],
                               mes_ref_cap='—', ano_ref='—')

    ano_ant = ano_ref - 1
    a_ano, a_mes = _mes_anterior(ano_ref, mes_num)
    _, _, mes_ref_cap = partes_posicao(f"{ano_ref}{mes_num:02d}")

    mes_ref_nome = _MESES_NOME[mes_num - 1]
    mes_ant_nome = _MESES_NOME[a_mes - 1]

    # Cabeçalho em 2 níveis (grupo -> colunas), igual ao Excel.
    grupos = [
        {'titulo': str(ano_ref), 'cols': [
            {'sub': mes_ant_nome, 'attr': 'VR_MES_ANTERIOR', 'tipo': 'moeda'},
            {'sub': mes_ref_nome, 'attr': 'VR_MES_ATUAL', 'tipo': 'moeda'},
            {'sub': 'Acumulado', 'attr': 'VR_ACUMUL_ATE_MES', 'tipo': 'moeda'},
        ]},
        {'titulo': str(ano_ant), 'cols': [
            {'sub': mes_ref_nome, 'attr': 'VR_ANO_ANTERIOR', 'tipo': 'moeda'},
            {'sub': 'Acumulado', 'attr': 'VR_ACUMUL_ATE_MES_ANO_ANT', 'tipo': 'moeda'},
        ]},
        {'titulo': f'∆ {ano_ref} x {ano_ant}', 'cols': [
            {'sub': '∆ % Mês', 'attr': 'VARIACAO_ANUAL_PERC', 'tipo': 'perc'},
            {'sub': '∆ % Acumulada', 'attr': 'VARIACAO_ANUAL_ACUML_PERC', 'tipo': 'perc'},
        ]},
        {'titulo': f'∆ {ano_ref} x {ano_ant}', 'cols': [
            {'sub': 'Mês', 'attr': 'VARIACAO_ANUAL', 'tipo': 'moeda'},
            {'sub': 'Acumulado', 'attr': 'VARIACAO_ANUAL_ACUML', 'tipo': 'moeda'},
        ]},
        {'titulo': f'{mes_ref_nome} {ano_ref} x {_MESES_ABREV[a_mes-1]} {a_ano}', 'cols': [
            {'sub': '∆ %', 'attr': 'VARIACAO_MENSAL_PERC', 'tipo': 'perc'},
            {'sub': 'atual X anterior', 'attr': 'VARIACAO_MENSAL', 'tipo': 'moeda'},
        ]},
    ]

    # Ordem plana das colunas + marca de início de grupo (para o separador).
    colunas_flat = []
    for g in grupos:
        for i, c in enumerate(g['cols']):
            colunas_flat.append({'attr': c['attr'], 'tipo': c['tipo'], 'sep': (i == 0)})

    registros = RelatorioResultadoFinanceiro.query.filter_by(
        ANO=ano_ref, MES=mes_num
    ).order_by(RelatorioResultadoFinanceiro.NU_LINHA).all()

    linhas = []
    for row in registros:
        h = _hierarquia((row.NATUREZA or '').strip())
        cells = []
        for col in colunas_flat:
            cell = _fmt_cell(getattr(row, col['attr']), col['tipo'])
            cell['sep'] = col['sep']
            cells.append(cell)
        linhas.append({
            'nivel': h['nivel'],
            'numero': h['numero'],
            'nome': h['nome'],
            'cells': cells,
        })

    return render_template(
        'relatorio_gestao/resultado_financeiro.html',
        sem_dados=False,
        grupos=grupos,
        linhas=linhas,
        mes_ref_cap=mes_ref_cap,
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