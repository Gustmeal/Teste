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
    # Obter o filtro de sistema se existir
    sistema_filtro = request.args.get('sistema', '')

    # Lista de sistemas disponíveis para o filtro
    sistemas_disponiveis = [
        {'id': 'credenciamento', 'nome': 'Sistema de Credenciamento'},
        {'id': 'dashboards', 'nome': 'Dashboards Power BI'}
        # Adicione novos sistemas aqui conforme necessário
    ]

    # Para usuários comuns, mostrar apenas seu próprio feedback
    if current_user.perfil == 'usuario':
        feedbacks = Feedback.query.filter_by(USUARIO_ID=current_user.id).order_by(Feedback.CREATED_AT.desc()).all()
    # Para admins e moderadores, mostrar todos os feedbacks
    else:
        feedbacks = Feedback.query.order_by(Feedback.CREATED_AT.desc()).all()

    # Como a coluna SISTEMA não existe no banco, não podemos filtrar por ela
    # O filtro de sistema está temporariamente desabilitado

    # Adicionar nome do sistema para cada feedback (valor padrão)
    for feedback in feedbacks:
        # Definir um valor padrão para o sistema
        feedback.SISTEMA_NOME = 'Sistema de Credenciamento'

    return render_template('feedback/index.html',
                           feedbacks=feedbacks,
                           todos_sistemas=sistemas_disponiveis,
                           sistema_filtro=sistema_filtro)


@feedback_bp.route('/feedback/novo', methods=['GET', 'POST'])
@login_required
def novo_feedback():
    if request.method == 'POST':
        try:
            titulo = request.form['titulo']
            mensagem = request.form['mensagem']
            # Ignoramos o campo sistema por enquanto, pois ele não existe na tabela
            # sistema = request.form['sistema']

            feedback = Feedback(
                USUARIO_ID=current_user.id,
                TITULO=titulo,
                MENSAGEM=mensagem
                # Não incluir o campo SISTEMA até que a coluna seja adicionada ao banco
            )

            db.session.add(feedback)
            db.session.commit()

            # Registrar log de auditoria
            dados_novos = {
                'titulo': titulo,
                'mensagem': mensagem
                # Não incluir sistema nos dados de auditoria
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

    # Como a coluna SISTEMA não existe, definimos um valor padrão
    feedback.SISTEMA_NOME = 'Sistema de Credenciamento'

    return render_template('feedback/visualizar_feedback.html', feedback=feedback)


@feedback_bp.route('/feedback/responder/<int:id>', methods=['GET', 'POST'])
@login_required
def responder_feedback(id):
    # Apenas admins e moderadores podem responder
    if current_user.perfil not in ['admin', 'moderador']:
        flash('Você não tem permissão para responder feedbacks.', 'danger')
        return redirect(url_for('feedback.index'))

    feedback = Feedback.query.get_or_404(id)

    # Como a coluna SISTEMA não existe, definimos um valor padrão
    feedback.SISTEMA_NOME = 'Sistema de Credenciamento'

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