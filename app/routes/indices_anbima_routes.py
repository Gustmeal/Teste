from datetime import datetime

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required

from app import db
from app.models.indices_anbima import IndiceAnbima
from app.utils.anbima_importador import importar_indice_anbima, INDICE_PADRAO
from app.utils.audit import registrar_log

indices_anbima_bp = Blueprint(
    'indices_anbima', __name__, url_prefix='/indices-anbima'
)


@indices_anbima_bp.route('/')
@login_required
def index():
    ultimos = IndiceAnbima.query.order_by(IndiceAnbima.DIA.desc()).limit(15).all()
    return render_template('indices_anbima/index.html',
                           ultimos=ultimos, indice=INDICE_PADRAO)


@indices_anbima_bp.route('/importar', methods=['POST'])
@login_required
def importar():
    """Captura da ANBIMA a data informada e grava na FIN_TB030."""
    data_str = (request.form.get('data') or '').strip()
    if not data_str:
        return jsonify({'success': False, 'message': 'Informe a data.'}), 400
    try:
        data_ref = datetime.strptime(data_str, '%Y-%m-%d').date()
    except ValueError:
        return jsonify({'success': False, 'message': 'Data inválida.'}), 400

    try:
        dados = importar_indice_anbima(data_ref)

        registro = IndiceAnbima.query.filter_by(DIA=data_ref).first()
        acao = 'atualizacao' if registro else 'inclusao'
        if not registro:
            registro = IndiceAnbima(DIA=data_ref)
            db.session.add(registro)
        for k, val in dados.items():
            setattr(registro, k, val)
        db.session.commit()

        registrar_log(
            acao=acao, entidade='indices_anbima', entidade_id=None,
            descricao=f'Índices ANBIMA {data_ref.strftime("%d/%m/%Y")} ({INDICE_PADRAO})',
            dados_novos={k: str(v) for k, v in dados.items()},
        )

        return jsonify({
            'success': True,
            'message': (f'Importado {data_ref.strftime("%d/%m/%Y")}: '
                        f'diária {dados["VARIACAO_DIARIA_PERC"]}%, '
                        f'mês {dados["VARIACAO_MENSAL_PERC"]}%, '
                        f'ano {dados["VARIACAO_ANUAL_PERC"]}%.'),
        })
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)}), 500