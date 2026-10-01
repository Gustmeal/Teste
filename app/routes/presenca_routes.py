# -*- coding: utf-8 -*-
"""
Rotas do painel de usuários online do Portal GEINC.
Compatível com Python 3.9 e 3.12.
"""

import traceback

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required, current_user

from app.auth.utils import admin_or_moderador_required
from app.utils import presenca as presenca_util

presenca_bp = Blueprint('presenca', __name__, url_prefix='/presenca')


@presenca_bp.app_context_processor
def injetar_config_presenca():
    """Disponibiliza o intervalo do sinal para o base.html (vale para todas as páginas)."""
    return {'presenca_config': {'intervalo_sinal_seg': presenca_util.INTERVALO_SINAL_SEG}}


@presenca_bp.route('/sinal', methods=['POST'])
def registrar_sinal():
    # Sem @login_required para não devolver redirect (302) ao fetch silencioso
    if not current_user.is_authenticated:
        return jsonify({'success': False}), 401

    dados = request.get_json(silent=True, force=True) or {}
    ok = presenca_util.registrar_sinal(
        usuario_id=current_user.id,
        usuario_nome=current_user.nome,
        dados=dados,
        ip=request.remote_addr,
        script_root=request.script_root,
    )
    return jsonify({'success': ok}), (200 if ok else 400)


@presenca_bp.route('/saida', methods=['POST'])
def registrar_saida():
    if not current_user.is_authenticated:
        return ('', 204)
    dados = request.get_json(silent=True, force=True) or {}
    presenca_util.registrar_saida(current_user.id, dados)
    return ('', 204)


@presenca_bp.route('/')
@login_required
@admin_or_moderador_required
def painel():
    return render_template('presenca/painel.html')


@presenca_bp.route('/dados')
@login_required
@admin_or_moderador_required
def dados_painel():
    try:
        return jsonify(presenca_util.consultar_painel(current_user.id))
    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False,
                        'message': 'Erro ao consultar usuários online: {}'.format(e)}), 500