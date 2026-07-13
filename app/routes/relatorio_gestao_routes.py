from flask import Blueprint, render_template
from flask_login import login_required
from sqlalchemy import text

from app import db
from app.models.relatorio_gestao_item import RelatorioGestaoItem
from app.utils.relatorio_gestao import partes_posicao, renderizar_pagina
from app.utils.relatorio_gestao_textos import SUMARIO_EXECUTIVO

relatorio_gestao_bp = Blueprint(
    'relatorio_gestao', __name__, url_prefix='/relatorio-gestao'
)

# Valor de PAGINA gravado na FIN_TB023 (confirmado no seu SELECT: 'SUMARIO').
PAGINA_SUMARIO = 'SUMARIO'

_MESES_LBL = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun',
              'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']


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

    return render_template(
        'relatorio_gestao/sumario_executivo.html',
        itens=itens,
        mes_ref_cap=mes_ref_cap,
        ano_ref=ano_ref,
        sem_dados=sem_dados,
        grafico_ingressos=grafico_ingressos,
    )