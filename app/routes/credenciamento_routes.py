from flask import Blueprint, render_template, redirect, url_for
from flask_login import login_required, current_user  # Adicionando current_user aqui
from datetime import datetime

credenciamento_bp = Blueprint('credenciamento', __name__, url_prefix='/credenciamento')

@credenciamento_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}

@credenciamento_bp.route('/')
@login_required
def index():
    # Contar editais ativos
    from app.models.edital import Edital
    editais = Edital.query.filter(Edital.DELETED_AT == None).count()

    # Contar períodos ativos
    from app.models.periodo import PeriodoAvaliacao
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).count()

    # Contar critérios de distribuição ativos
    from app.models.criterio_distribuicao import CriterioDistribuicao
    criterios = CriterioDistribuicao.query.filter(CriterioDistribuicao.DELETED_AT == None).count()

    # Contar metas ativas
    from app.models.meta_avaliacao import MetaAvaliacao
    metas = MetaAvaliacao.query.filter(MetaAvaliacao.DELETED_AT == None).count()

    # Contar usuários ativos (apenas para administradores)
    usuarios = 0
    if current_user.perfil == 'admin':
        from app.models.usuario import Usuario
        usuarios = Usuario.query.filter(Usuario.DELETED_AT == None, Usuario.ATIVO == True).count()

    return render_template('credenciamento/index.html', editais=editais, periodos=periodos, criterios=criterios, metas=metas, usuarios=usuarios)