from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify
from flask_login import login_required, current_user
from app import db
from app.models.mensagem import Mensagem
from app.models.usuario import Usuario
from datetime import datetime

chat_bp = Blueprint('chat', __name__)


@chat_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@chat_bp.route('/chat')
@login_required
def index():
    # Obter todos os usuários admins para chat
    admins = Usuario.query.filter_by(PERFIL='admin', DELETED_AT=None).all()

    # Obter conversas recentes
    conversas = db.session.query(
        db.func.max(Mensagem.CREATED_AT).label('ultima_data'),
        Mensagem.REMETENTE_ID,
        Mensagem.DESTINATARIO_ID
    ).filter(
        ((Mensagem.REMETENTE_ID == current_user.id) |
         (Mensagem.DESTINATARIO_ID == current_user.id))
    ).group_by(
        Mensagem.REMETENTE_ID,
        Mensagem.DESTINATARIO_ID
    ).order_by(
        db.desc('ultima_data')
    ).all()

    # Processar conversas para exibição
    conversas_exibicao = []
    usuarios_ids = set()

    for conversa in conversas:
        outro_usuario_id = conversa.DESTINATARIO_ID if conversa.REMETENTE_ID == current_user.id else conversa.REMETENTE_ID
        if outro_usuario_id not in usuarios_ids:
            usuarios_ids.add(outro_usuario_id)
            usuario = Usuario.query.get(outro_usuario_id)
            if usuario:
                # Contar mensagens não lidas
                nao_lidas = Mensagem.query.filter_by(
                    REMETENTE_ID=outro_usuario_id,
                    DESTINATARIO_ID=current_user.id,
                    LIDO=False
                ).count()

                conversas_exibicao.append({
                    'usuario': usuario,
                    'ultima_data': conversa.ultima_data,
                    'nao_lidas': nao_lidas
                })

    return render_template('chat/index.html', admins=admins, conversas=conversas_exibicao)


@chat_bp.route('/chat/<int:usuario_id>')
@login_required
def conversa(usuario_id):
    # Verificar se o usuário existe
    usuario = Usuario.query.get_or_404(usuario_id)

    # Para usuários comuns, apenas permitir conversa com admins
    if current_user.perfil == 'usuario' and usuario.PERFIL != 'admin':
        flash('Você só pode iniciar conversas com administradores.', 'danger')
        return redirect(url_for('chat.index'))

    # Obter mensagens da conversa
    mensagens = Mensagem.query.filter(
        ((Mensagem.REMETENTE_ID == current_user.id) & (Mensagem.DESTINATARIO_ID == usuario_id)) |
        ((Mensagem.REMETENTE_ID == usuario_id) & (Mensagem.DESTINATARIO_ID == current_user.id))
    ).order_by(Mensagem.CREATED_AT).all()

    # Marcar mensagens como lidas
    for mensagem in mensagens:
        if mensagem.DESTINATARIO_ID == current_user.id and not mensagem.LIDO:
            mensagem.LIDO = True
            mensagem.LIDO_AT = datetime.utcnow()

    db.session.commit()

    return render_template('chat/conversa.html', usuario=usuario, mensagens=mensagens)


@chat_bp.route('/chat/enviar', methods=['POST'])
@login_required
def enviar_mensagem():
    try:
        destinatario_id = int(request.form['destinatario_id'])
        conteudo = request.form['conteudo']

        # Verificar se o destinatário existe
        destinatario = Usuario.query.get_or_404(destinatario_id)

        # Para usuários comuns, apenas permitir mensagens para admins
        if current_user.perfil == 'usuario' and destinatario.PERFIL != 'admin':
            return jsonify({'success': False, 'message': 'Você só pode enviar mensagens para administradores.'})

        mensagem = Mensagem(
            REMETENTE_ID=current_user.id,
            DESTINATARIO_ID=destinatario_id,
            CONTEUDO=conteudo
        )

        db.session.add(mensagem)
        db.session.commit()

        # Formatar mensagem para resposta AJAX
        mensagem_formatada = {
            'id': mensagem.ID,
            'remetente_id': mensagem.REMETENTE_ID,
            'destinatario_id': mensagem.DESTINATARIO_ID,
            'conteudo': mensagem.CONTEUDO,
            'created_at': mensagem.CREATED_AT.strftime('%d/%m/%Y %H:%M'),
            'is_mine': True  # Sempre será verdadeiro para mensagens recém-enviadas
        }

        return jsonify({
            'success': True,
            'message': 'Mensagem enviada com sucesso!',
            'mensagem': mensagem_formatada
        })
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': f'Erro ao enviar mensagem: {str(e)}'})


@chat_bp.route('/chat/novas/<int:usuario_id>', methods=['GET'])
@login_required
def verificar_novas_mensagens(usuario_id):
    try:
        # Verificar novas mensagens desde o último id
        ultimo_id = request.args.get('ultimo_id', 0, type=int)

        novas_mensagens = Mensagem.query.filter(
            Mensagem.ID > ultimo_id,
            Mensagem.REMETENTE_ID == usuario_id,
            Mensagem.DESTINATARIO_ID == current_user.id
        ).order_by(Mensagem.CREATED_AT).all()

        # Marcar como lidas
        for mensagem in novas_mensagens:
            mensagem.LIDO = True
            mensagem.LIDO_AT = datetime.utcnow()

        db.session.commit()

        # Formatar mensagens para resposta AJAX
        mensagens_formatadas = []
        for mensagem in novas_mensagens:
            mensagens_formatadas.append({
                'id': mensagem.ID,
                'remetente_id': mensagem.REMETENTE_ID,
                'destinatario_id': mensagem.DESTINATARIO_ID,
                'conteudo': mensagem.CONTEUDO,
                'created_at': mensagem.CREATED_AT.strftime('%d/%m/%Y %H:%M'),
                'is_mine': False
            })

        return jsonify({
            'success': True,
            'mensagens': mensagens_formatadas
        })
    except Exception as e:
        return jsonify({'success': False, 'message': f'Erro ao verificar novas mensagens: {str(e)}'})