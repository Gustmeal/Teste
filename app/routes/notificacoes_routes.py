# app/routes/notificacoes_routes.py
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from app import db
from app.models.notificacao import Notificacao, NotificacaoVisualizacao
from app.models.usuario import Usuario
from app.utils.audit import registrar_log
from datetime import datetime

notificacoes_bp = Blueprint('notificacoes', __name__, url_prefix='/notificacoes')


def admin_ou_moderador_required(f):
    """Decorator para restringir acesso a admin e moderador"""
    from functools import wraps
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if current_user.perfil not in ['admin', 'moderador']:
            flash('Acesso negado. Apenas administradores e moderadores.', 'danger')
            return redirect(url_for('main.index'))
        return f(*args, **kwargs)

    return decorated_function


@notificacoes_bp.route('/')
@login_required
@admin_ou_moderador_required
def index():
    """Lista todas as notificações (apenas admin/moderador)"""
    notificacoes = Notificacao.query.filter(
        Notificacao.DELETED_AT.is_(None)
    ).order_by(Notificacao.CREATED_AT.desc()).all()

    # Contar visualizações para cada notificação
    total_usuarios = Usuario.query.filter_by(ATIVO=True, DELETED_AT=None).count()

    for notif in notificacoes:
        notif.total_visualizacoes = len(notif.visualizacoes)
        notif.percentual_visualizacao = (notif.total_visualizacoes / total_usuarios * 100) if total_usuarios > 0 else 0

    return render_template('notificacoes/index.html', notificacoes=notificacoes)


@notificacoes_bp.route('/nova', methods=['GET', 'POST'])
@login_required
@admin_ou_moderador_required
def nova():
    """Criar nova notificação"""
    if request.method == 'POST':
        try:
            titulo = request.form.get('titulo')
            mensagem = request.form.get('mensagem')
            tipo = request.form.get('tipo', 'modal')
            prioridade = request.form.get('prioridade', 'normal')
            dt_inicio = request.form.get('dt_inicio')
            dt_fim = request.form.get('dt_fim')

            # Validações
            if not titulo or not mensagem:
                flash('Título e mensagem são obrigatórios!', 'danger')
                return redirect(url_for('notificacoes.nova'))

            # Converter datas
            dt_inicio_obj = datetime.strptime(dt_inicio, '%Y-%m-%dT%H:%M') if dt_inicio else None
            dt_fim_obj = datetime.strptime(dt_fim, '%Y-%m-%dT%H:%M') if dt_fim else None

            # DEBUG: Verificar current_user
            print(f"DEBUG - Tipo de current_user: {type(current_user)}")
            print(f"DEBUG - Atributos de current_user: {dir(current_user)}")
            print(f"DEBUG - current_user.id: {current_user.id}")

            # Pegar o ID do usuário
            usuario_id = current_user.id
            print(f"DEBUG - usuario_id: {usuario_id}")

            # Criar notificação - USANDO APENAS VALORES SIMPLES
            notificacao = Notificacao(
                TITULO=titulo,
                MENSAGEM=mensagem,
                TIPO=tipo,
                PRIORIDADE=prioridade,
                CRIADO_POR=usuario_id,  # Passar apenas o INTEGER
                DT_INICIO=dt_inicio_obj,
                DT_FIM=dt_fim_obj,
                ATIVO=True
            )

            print(f"DEBUG - Notificacao criada: {notificacao}")

            db.session.add(notificacao)

            print("DEBUG - Antes do commit")
            db.session.commit()
            print("DEBUG - Depois do commit")

            # Log de auditoria
            registrar_log(
                acao='criar',
                entidade='notificacao',
                entidade_id=notificacao.ID,
                descricao=f'Notificação criada: {titulo}'
            )

            flash('Notificação criada com sucesso!', 'success')
            return redirect(url_for('notificacoes.index'))

        except Exception as e:
            import traceback
            print("DEBUG - ERRO COMPLETO:")
            print(traceback.format_exc())
            db.session.rollback()
            flash(f'Erro ao criar notificação: {str(e)}', 'danger')

    return render_template('notificacoes/form.html', notificacao=None)


@notificacoes_bp.route('/editar/<int:id>', methods=['GET', 'POST'])
@login_required
@admin_ou_moderador_required
def editar(id):
    """Editar notificação existente"""
    notificacao = Notificacao.query.get_or_404(id)

    if request.method == 'POST':
        try:
            notificacao.TITULO = request.form.get('titulo')
            notificacao.MENSAGEM = request.form.get('mensagem')
            notificacao.TIPO = request.form.get('tipo', 'modal')
            notificacao.PRIORIDADE = request.form.get('prioridade', 'normal')

            dt_inicio = request.form.get('dt_inicio')
            dt_fim = request.form.get('dt_fim')

            notificacao.DT_INICIO = datetime.strptime(dt_inicio, '%Y-%m-%dT%H:%M') if dt_inicio else None
            notificacao.DT_FIM = datetime.strptime(dt_fim, '%Y-%m-%dT%H:%M') if dt_fim else None
            notificacao.UPDATED_AT = datetime.utcnow()

            db.session.commit()

            # Log
            registrar_log(
                acao='atualizar',
                entidade='notificacao',
                entidade_id=notificacao.ID,
                descricao=f'Notificação atualizada: {notificacao.TITULO}'
            )

            flash('Notificação atualizada com sucesso!', 'success')
            return redirect(url_for('notificacoes.index'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao atualizar: {str(e)}', 'danger')

    return render_template('notificacoes/form.html', notificacao=notificacao)


@notificacoes_bp.route('/alternar-status/<int:id>', methods=['POST'])
@login_required
@admin_ou_moderador_required
def alternar_status(id):
    """Ativa ou desativa uma notificação"""
    try:
        notificacao = Notificacao.query.get_or_404(id)
        notificacao.ATIVO = not notificacao.ATIVO
        notificacao.UPDATED_AT = datetime.utcnow()

        db.session.commit()

        status = 'ativada' if notificacao.ATIVO else 'desativada'
        flash(f'Notificação {status} com sucesso!', 'success')

        # Log
        registrar_log(
            acao='atualizar',
            entidade='notificacao',
            entidade_id=notificacao.ID,
            descricao=f'Notificação {status}: {notificacao.TITULO}'
        )

    except Exception as e:
        db.session.rollback()
        flash(f'Erro: {str(e)}', 'danger')

    return redirect(url_for('notificacoes.index'))


@notificacoes_bp.route('/excluir/<int:id>', methods=['POST'])
@login_required
@admin_ou_moderador_required
def excluir(id):
    """Soft delete de notificação"""
    try:
        notificacao = Notificacao.query.get_or_404(id)
        notificacao.DELETED_AT = datetime.utcnow()

        db.session.commit()

        flash('Notificação excluída com sucesso!', 'warning')

        # Log
        registrar_log(
            acao='excluir',
            entidade='notificacao',
            entidade_id=notificacao.ID,
            descricao=f'Notificação excluída: {notificacao.TITULO}'
        )

    except Exception as e:
        db.session.rollback()
        flash(f'Erro: {str(e)}', 'danger')

    return redirect(url_for('notificacoes.index'))


@notificacoes_bp.route('/marcar-visualizada/<int:id>', methods=['POST'])
@login_required
def marcar_visualizada(id):
    """Marca notificação como visualizada pelo usuário atual"""
    try:
        notificacao = Notificacao.query.get_or_404(id)
        usuario_id = current_user.id

        print(f"DEBUG - Marcando notificacao {id} como lida para usuario {usuario_id}")

        # Verificar se já foi visualizada
        ja_visualizada = NotificacaoVisualizacao.query.filter_by(
            NOTIFICACAO_ID=id,
            USUARIO_ID=usuario_id
        ).first()

        if ja_visualizada:
            print(f"DEBUG - Já estava marcada como visualizada")
            return jsonify({'success': True, 'message': 'Já visualizada'})

        # Criar novo registro de visualização
        visualizacao = NotificacaoVisualizacao(
            NOTIFICACAO_ID=id,
            USUARIO_ID=usuario_id
        )
        db.session.add(visualizacao)
        db.session.commit()

        print(f"DEBUG - Visualização salva com sucesso!")

        return jsonify({'success': True, 'message': 'Marcada como lida'})

    except Exception as e:
        import traceback
        print("ERRO ao marcar como visualizada:")
        print(traceback.format_exc())
        db.session.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500


@notificacoes_bp.route('/obter-nao-lidas', methods=['GET'])
@login_required
def obter_nao_lidas():
    """API para buscar notificações não lidas do usuário atual"""
    try:
        # USAR current_user.id MINÚSCULO
        usuario_id = current_user.id
        print(f"DEBUG - Buscando notificações para usuario_id: {usuario_id}")

        notificacoes = Notificacao.obter_ativas_nao_visualizadas(usuario_id)
        print(f"DEBUG - Encontradas {len(notificacoes)} notificações")

        resultado = []
        for notif in notificacoes:
            resultado.append({
                'id': notif.ID,
                'titulo': notif.TITULO,
                'mensagem': notif.MENSAGEM,
                'tipo': notif.TIPO,
                'prioridade': notif.PRIORIDADE
            })

        print(f"DEBUG - Retornando: {resultado}")
        return jsonify({'notificacoes': resultado})

    except Exception as e:
        import traceback
        print("ERRO em obter_nao_lidas:")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500


@notificacoes_bp.route('/visualizacoes/<int:id>')
@login_required
@admin_ou_moderador_required
def visualizacoes(id):
    """Lista quem visualizou a notificação"""
    notificacao = Notificacao.query.get_or_404(id)

    visualizacoes = NotificacaoVisualizacao.query.filter_by(
        NOTIFICACAO_ID=id
    ).order_by(NotificacaoVisualizacao.VISUALIZADO_AT.desc()).all()

    return render_template('notificacoes/visualizacoes.html',
                           notificacao=notificacao,
                           visualizacoes=visualizacoes)