from flask import Blueprint, render_template, redirect, url_for, request, flash
from flask_login import login_required, current_user
from app import db
from app.models.feedback import Feedback
from app.models.usuario import Usuario
from app.utils.audit import registrar_log
from datetime import datetime

feedback_bp = Blueprint('feedback', __name__)


@feedback_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@feedback_bp.route('/feedback')
@login_required
def index():
    # Para usuários comuns, mostrar apenas seu próprio feedback
    if current_user.perfil == 'usuario':
        feedbacks = Feedback.query.filter_by(USUARIO_ID=current_user.id).order_by(Feedback.CREATED_AT.desc()).all()
    # Para admins e moderadores, mostrar todos os feedbacks
    else:
        feedbacks = Feedback.query.order_by(Feedback.CREATED_AT.desc()).all()

    return render_template('feedback/index.html', feedbacks=feedbacks)


@feedback_bp.route('/feedback/novo', methods=['GET', 'POST'])
@login_required
def novo_feedback():
    if request.method == 'POST':
        try:
            titulo = request.form['titulo']
            mensagem = request.form['mensagem']

            feedback = Feedback(
                USUARIO_ID=current_user.id,
                TITULO=titulo,
                MENSAGEM=mensagem
            )

            db.session.add(feedback)
            db.session.commit()

            # Registrar log de auditoria
            dados_novos = {
                'titulo': titulo,
                'mensagem': mensagem
            }
            registrar_log(
                acao='criar',
                entidade='feedback',
                entidade_id=feedback.ID,
                descricao=f'Novo feedback: {titulo}',
                dados_novos=dados_novos
            )

            flash('Feedback enviado com sucesso!', 'success')
            return redirect(url_for('feedback.index'))
        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao enviar feedback: {str(e)}', 'danger')

    return render_template('feedback/form_feedback.html')


@feedback_bp.route('/feedback/visualizar/<int:id>')
@login_required
def visualizar_feedback(id):
    feedback = Feedback.query.get_or_404(id)

    # Verificar permissões
    if current_user.perfil not in ['admin', 'moderador'] and feedback.USUARIO_ID != current_user.id:
        flash('Você não tem permissão para visualizar este feedback.', 'danger')
        return redirect(url_for('feedback.index'))

    # Marcar como lido se for admin/moderador
    if current_user.perfil in ['admin', 'moderador'] and not feedback.LIDO:
        feedback.LIDO = True
        db.session.commit()

    return render_template('feedback/visualizar_feedback.html', feedback=feedback)


@feedback_bp.route('/feedback/responder/<int:id>', methods=['GET', 'POST'])
@login_required
def responder_feedback(id):
    # Apenas admins e moderadores podem responder
    if current_user.perfil not in ['admin', 'moderador']:
        flash('Você não tem permissão para responder feedbacks.', 'danger')
        return redirect(url_for('feedback.index'))

    feedback = Feedback.query.get_or_404(id)

    if request.method == 'POST':
        try:
            resposta = request.form['resposta']

            feedback.RESPOSTA = resposta
            feedback.RESPONDIDO = True
            feedback.RESPONDIDO_POR = current_user.id
            feedback.RESPONDIDO_AT = datetime.utcnow()

            db.session.commit()

            # Registrar log de auditoria
            dados_novos = {
                'resposta': resposta,
                'respondido_por': current_user.id,
                'respondido_at': feedback.RESPONDIDO_AT.strftime('%Y-%m-%d %H:%M:%S')
            }
            registrar_log(
                acao='editar',
                entidade='feedback',
                entidade_id=feedback.ID,
                descricao=f'Resposta ao feedback: {feedback.TITULO}',
                dados_novos=dados_novos
            )

            flash('Resposta enviada com sucesso!', 'success')
            return redirect(url_for('feedback.visualizar_feedback', id=feedback.ID))
        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao responder feedback: {str(e)}', 'danger')

    return render_template('feedback/responder_feedback.html', feedback=feedback)