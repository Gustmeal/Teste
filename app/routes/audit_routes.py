from flask import Blueprint, render_template, request
from app.models.audit_log import AuditLog
from flask_login import login_required
from app.auth.utils import admin_required
from datetime import datetime, timedelta

audit_bp = Blueprint('audit', __name__)


@audit_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@audit_bp.route('/auditoria')
@login_required
@admin_required
def index():
    # Obter parâmetros de filtro
    entidade = request.args.get('entidade', '')
    acao = request.args.get('acao', '')
    usuario_id = request.args.get('usuario_id', '')
    data_inicio = request.args.get('data_inicio', '')
    data_fim = request.args.get('data_fim', '')

    # Consulta base
    query = AuditLog.query

    # Aplicar filtros
    if entidade:
        query = query.filter(AuditLog.ENTIDADE == entidade)
    if acao:
        query = query.filter(AuditLog.ACAO == acao)
    if usuario_id:
        query = query.filter(AuditLog.USUARIO_ID == usuario_id)
    if data_inicio:
        try:
            data_inicio = datetime.strptime(data_inicio, '%Y-%m-%d')
            query = query.filter(AuditLog.DATA >= data_inicio)
        except:
            pass
    if data_fim:
        try:
            data_fim = datetime.strptime(data_fim, '%Y-%m-%d')
            # Adiciona um dia para incluir no último dia
            data_fim = data_fim + timedelta(days=1)
            query = query.filter(AuditLog.DATA <= data_fim)
        except:
            pass

    # Ordenar por data decrescente (mais recentes primeiro)
    logs = query.order_by(AuditLog.DATA.desc()).all()

    # Buscar usuários para o filtro
    from app.models.usuario import Usuario
    usuarios = Usuario.query.filter(Usuario.DELETED_AT == None).all()

    return render_template('auditoria/index.html',
                           logs=logs,
                           usuarios=usuarios,
                           entidade=entidade,
                           acao=acao,
                           usuario_id=usuario_id,
                           data_inicio=data_inicio,
                           data_fim=data_fim)


@audit_bp.route('/auditoria/<int:id>')
@login_required
@admin_required
def detalhes(id):
    log = AuditLog.query.get_or_404(id)
    return render_template('auditoria/detalhes.html', log=log)