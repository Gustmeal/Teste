from flask import Blueprint, jsonify
from app.models.empresa_participante import EmpresaParticipante
from app import db
from flask_login import login_required

api_bp = Blueprint('api', __name__, url_prefix='/api')


@api_bp.route('/empresas')
@login_required
def lista_empresas():
    """API para obter lista de empresas em formato JSON"""
    try:
        # Usar uma consulta distinta para obter empresas únicas
        empresas = db.session.query(
            EmpresaParticipante.ID_EMPRESA,
            EmpresaParticipante.NO_EMPRESA,
            EmpresaParticipante.NO_EMPRESA_ABREVIADA
        ).filter(
            EmpresaParticipante.DELETED_AT == None
        ).distinct(EmpresaParticipante.ID_EMPRESA).all()

        return jsonify({
            'success': True,
            'empresas': [
                {
                    'id_empresa': empresa[0],
                    'nome': empresa[1],
                    'nome_abreviado': empresa[2]
                }
                for empresa in empresas
            ]
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        })