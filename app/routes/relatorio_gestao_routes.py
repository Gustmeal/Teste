from flask import Blueprint, render_template
from flask_login import login_required

from app.models.relatorio_gestao_item import RelatorioGestaoItem
from app.utils.relatorio_gestao import partes_posicao, renderizar_pagina
from app.utils.relatorio_gestao_textos import SUMARIO_EXECUTIVO

relatorio_gestao_bp = Blueprint(
    'relatorio_gestao', __name__, url_prefix='/relatorio-gestao'
)

# Valor de PAGINA usado na FIN_TB023 para esta página.
PAGINA_SUMARIO = 'SUMARIO'


@relatorio_gestao_bp.route('/sumario-executivo')
@login_required
def sumario_executivo():
    """Sumário Executivo com valores e mês de referência automáticos."""
    posicao = RelatorioGestaoItem.obter_posicao_referencia(PAGINA_SUMARIO)

    if posicao:
        ano_ref, mes_ref, mes_ref_cap = partes_posicao(posicao)
        mapa = RelatorioGestaoItem.carregar_mapa_id_vr(PAGINA_SUMARIO, posicao)
        sem_dados = False
    else:
        ano_ref, mes_ref, mes_ref_cap = '—', '—', '—'
        mapa = {}
        sem_dados = True

    itens = renderizar_pagina(SUMARIO_EXECUTIVO, mapa, mes_ref, mes_ref_cap, ano_ref)

    return render_template(
        'relatorio_gestao/sumario_executivo.html',
        itens=itens,
        mes_ref_cap=mes_ref_cap,
        ano_ref=ano_ref,
        sem_dados=sem_dados,
    )