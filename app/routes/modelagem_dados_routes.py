# -*- coding: utf-8 -*-
"""
app/routes/modelagem_dados_routes.py

Modelagem de Dados do Portal GEINC (somente admin e moderador).

  GET  /modelagem-dados/                -> tela interativa (casca HTML, sem dados fixos)
  GET  /modelagem-dados/dados           -> modelo em JSON (cache no servidor)
  GET  /modelagem-dados/dados?atualizar=1 -> força nova leitura do código e do banco
  POST /modelagem-dados/exportar-sql    -> script DDL (SQL Server) das tabelas escolhidas

Compatível com Python 3.9 e 3.12.
"""

import logging
from datetime import datetime
from functools import wraps

from flask import Blueprint, Response, flash, jsonify, redirect, render_template, request, url_for
from flask_login import current_user, login_required

from app.utils.modelagem_dados import gerar_ddl, obter_modelagem

logger = logging.getLogger(__name__)

modelagem_dados_bp = Blueprint('modelagem_dados', __name__, url_prefix='/modelagem-dados')

PERFIS_PERMITIDOS = ('admin', 'moderador')


def admin_ou_moderador_required(f):
    """Libera só admin e moderador. Tela: volta para o início com aviso.
    Chamadas JSON: responde 403."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated or current_user.perfil not in PERFIS_PERMITIDOS:
            if request.endpoint == 'modelagem_dados.index':
                flash('Acesso restrito a administradores e moderadores.', 'danger')
                return redirect(url_for('main.geinc_index'))
            return jsonify({'erro': 'Acesso restrito a administradores e moderadores.'}), 403
        return f(*args, **kwargs)
    return wrapper


@modelagem_dados_bp.route('/')
@login_required
@admin_ou_moderador_required
def index():
    """Só a casca da tela. Os dados chegam por /dados, depois que a página abre,
    para a navegação não ficar esperando a montagem do modelo."""
    return render_template('modelagem_dados/index.html')


@modelagem_dados_bp.route('/dados')
@login_required
@admin_ou_moderador_required
def dados():
    forcar = request.args.get('atualizar') == '1'
    try:
        modelo = obter_modelagem(forcar=forcar)
    except Exception as erro:
        logger.exception('Modelagem de dados: erro ao montar o modelo')
        return jsonify({'erro': 'Não foi possível montar a modelagem: {}'.format(erro)}), 500
    resposta = jsonify(modelo)
    resposta.headers['Cache-Control'] = 'no-store'
    return resposta


@modelagem_dados_bp.route('/exportar-sql', methods=['POST'])
@login_required
@admin_ou_moderador_required
def exportar_sql():
    corpo = request.get_json(silent=True) or {}
    tabelas = corpo.get('tabelas') or []
    if not isinstance(tabelas, list) or not tabelas:
        return jsonify({'erro': 'Nenhuma tabela informada para exportar.'}), 400

    usuario = getattr(current_user, 'nome', None) or getattr(current_user, 'email', None)
    try:
        script, qtd = gerar_ddl([str(t) for t in tabelas], usuario=usuario)
    except Exception as erro:
        logger.exception('Modelagem de dados: erro ao gerar o script SQL')
        return jsonify({'erro': 'Não foi possível gerar o script: {}'.format(erro)}), 500

    if not script:
        return jsonify({'erro': 'As tabelas informadas não fazem parte da modelagem atual.'}), 400

    nome_arquivo = 'modelagem_{}_{}tabelas.sql'.format(datetime.now().strftime('%Y%m%d_%H%M'), qtd)
    return Response(
        script.encode('utf-8-sig'),
        mimetype='application/sql',
        headers={
            'Content-Disposition': 'attachment; filename="{}"'.format(nome_arquivo),
            'Cache-Control': 'no-store',
        },
    )
