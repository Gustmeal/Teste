from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from app import db
from app.models.email_programado import EmailProgramado, ConfiguracaoEmail
from app.models.depositos_judiciais import Area
from app.models.usuario import Usuario
from app.utils.audit import registrar_log
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from cryptography.fernet import Fernet
import os

email_programado_bp = Blueprint('email_programado', __name__, url_prefix='/emails-programados')

# Chave para criptografia (deve estar em variável de ambiente)
CRYPTO_KEY = os.environ.get('CRYPTO_KEY', Fernet.generate_key())
cipher_suite = Fernet(CRYPTO_KEY)


def tem_permissao_email():
    """Verifica se o usuário tem permissão para enviar emails"""
    return current_user.perfil in ['admin', 'moderador']


@email_programado_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@email_programado_bp.route('/')
@login_required
def index():
    """Lista de emails programados"""
    if not tem_permissao_email():
        flash('Você não tem permissão para acessar esta funcionalidade.', 'warning')
        return redirect(url_for('main.geinc_index'))

    # Admin e moderadores veem todos os emails
    if current_user.perfil in ['admin', 'moderador']:
        emails = EmailProgramado.query.order_by(EmailProgramado.DT_PROGRAMADA.desc()).all()
    else:
        # Usuários normais veriam apenas os seus (mas não têm acesso)
        emails = EmailProgramado.query.filter_by(CRIADO_POR=current_user.id).order_by(
            EmailProgramado.DT_PROGRAMADA.desc()
        ).all()

    # Verificar se existe configuração global
    config_global = ConfiguracaoEmail.query.filter_by(USUARIO_ID=1).first()  # ID 1 = config global

    return render_template('email_programado/index.html',
                           emails=emails,
                           tem_config_global=config_global is not None,
                           is_admin=current_user.perfil in ['admin', 'moderador'])


@email_programado_bp.route('/novo', methods=['GET', 'POST'])
@login_required
def novo():
    """Criar novo email programado"""
    if not tem_permissao_email():
        flash('Você não tem permissão para enviar emails.', 'warning')
        return redirect(url_for('main.geinc_index'))

    if request.method == 'POST':
        try:
            # Admin usa configuração global, outros usam sua própria
            if current_user.perfil in ['admin', 'moderador']:
                config = ConfiguracaoEmail.query.filter_by(USUARIO_ID=1).first()  # Config global
                if not config:
                    flash('Configure as credenciais globais de email antes de enviar mensagens.', 'warning')
                    return redirect(url_for('email_programado.configuracoes'))
            else:
                config = ConfiguracaoEmail.query.filter_by(USUARIO_ID=current_user.id).first()
                if not config:
                    flash('Configure suas credenciais de email antes de enviar mensagens.', 'warning')
                    return redirect(url_for('email_programado.configuracoes'))

            # Criar novo email programado
            email = EmailProgramado()
            email.REMETENTE_EMAIL = config.EMAIL_REMETENTE
            email.ASSUNTO = request.form.get('assunto')
            email.CORPO_EMAIL = request.form.get('corpo_email')
            email.DT_PROGRAMADA = datetime.strptime(
                f"{request.form.get('data_envio')} {request.form.get('hora_envio')}",
                '%Y-%m-%d %H:%M'
            )
            email.CRIADO_POR = current_user.id

            # Definir destinatários
            tipo_destinatario = request.form.get('tipo_destinatario')
            if tipo_destinatario == 'individual':
                email.DESTINATARIO_TIPO = 'individual'
                email.DESTINATARIO_EMAIL = request.form.get('destinatario_email')
            elif tipo_destinatario == 'area':
                email.DESTINATARIO_TIPO = 'area'
                email.ID_AREA_DESTINO = int(request.form.get('id_area'))
                # Buscar emails de todos os usuários da área
                usuarios_area = Usuario.query.filter_by(
                    ID_AREA=email.ID_AREA_DESTINO,
                    ATIVO=True
                ).all()
                emails_list = [u.EMAIL for u in usuarios_area if u.EMAIL]
                email.DESTINATARIO_EMAIL = ','.join(emails_list)
            else:  # todos
                email.DESTINATARIO_TIPO = 'todos'
                # Buscar todos os usuários ativos
                todos_usuarios = Usuario.query.filter_by(ATIVO=True).all()
                emails_list = [u.EMAIL for u in todos_usuarios if u.EMAIL]
                email.DESTINATARIO_EMAIL = ','.join(emails_list)

            # Se a data programada for agora ou passada, enviar imediatamente
            if email.DT_PROGRAMADA <= datetime.now():
                enviar_email_agora(email, config)
            else:
                email.STATUS = 'pendente'

            db.session.add(email)
            db.session.commit()

            # Registrar log
            registrar_log(
                'email_programado',
                'create',
                f'Email programado criado para {email.DT_PROGRAMADA}',
                {'id': email.ID}
            )

            flash('Email programado com sucesso!', 'success')
            return redirect(url_for('email_programado.index'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao programar email: {str(e)}', 'danger')
            return redirect(url_for('email_programado.novo'))

    # GET - Buscar áreas para o dropdown
    areas = Area.query.order_by(Area.NO_AREA).all()

    # Verificar configuração
    if current_user.perfil in ['admin', 'moderador']:
        config = ConfiguracaoEmail.query.filter_by(USUARIO_ID=1).first()
    else:
        config = ConfiguracaoEmail.query.filter_by(USUARIO_ID=current_user.id).first()

    return render_template('email_programado/novo.html',
                           areas=areas,
                           tem_config=config is not None,
                           is_admin=current_user.perfil in ['admin', 'moderador'],
                           datetime=datetime)  # ADICIONAR ESTA LINHA


@email_programado_bp.route('/configuracoes', methods=['GET', 'POST'])
@login_required
def configuracoes():
    """Configurações de email do usuário"""
    if not tem_permissao_email():
        flash('Você não tem permissão para acessar esta funcionalidade.', 'warning')
        return redirect(url_for('main.geinc_index'))

    # Admin configura o email global (ID=1)
    if current_user.perfil in ['admin', 'moderador']:
        config = ConfiguracaoEmail.query.filter_by(USUARIO_ID=1).first()
        usuario_config_id = 1
    else:
        config = ConfiguracaoEmail.query.filter_by(USUARIO_ID=current_user.id).first()
        usuario_config_id = current_user.id

    if request.method == 'POST':
        try:
            if not config:
                config = ConfiguracaoEmail()
                config.USUARIO_ID = usuario_config_id

            config.EMAIL_REMETENTE = request.form.get('email_remetente')
            config.SERVIDOR_SMTP = request.form.get('servidor_smtp')
            config.PORTA_SMTP = int(request.form.get('porta_smtp'))
            config.USUARIO_SMTP = request.form.get('usuario_smtp')

            # Criptografar senha
            senha = request.form.get('senha_smtp')
            if senha:  # Só atualiza se uma nova senha foi fornecida
                config.SENHA_SMTP = cipher_suite.encrypt(senha.encode()).decode()

            config.USA_TLS = request.form.get('usa_tls') == 'on'

            db.session.add(config)
            db.session.commit()

            if current_user.perfil in ['admin', 'moderador']:
                flash('Configurações globais de email salvas com sucesso!', 'success')
            else:
                flash('Configurações salvas com sucesso!', 'success')

            return redirect(url_for('email_programado.index'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao salvar configurações: {str(e)}', 'danger')

    return render_template('email_programado/configuracoes.html',
                           config=config,
                           is_admin=current_user.perfil in ['admin', 'moderador'])


@email_programado_bp.route('/cancelar/<int:id>', methods=['POST'])
@login_required
def cancelar(id):
    """Cancelar email pendente"""
    if not tem_permissao_email():
        flash('Você não tem permissão para esta ação.', 'warning')
        return redirect(url_for('main.geinc_index'))

    email = EmailProgramado.query.get_or_404(id)

    # Admin pode cancelar qualquer email, outros apenas os seus
    if current_user.perfil not in ['admin', 'moderador'] and email.CRIADO_POR != current_user.id:
        flash('Você não tem permissão para cancelar este email.', 'warning')
        return redirect(url_for('email_programado.index'))

    if email.STATUS == 'pendente':
        email.STATUS = 'cancelado'
        db.session.commit()

        registrar_log(
            'email_programado',
            'cancel',
            f'Email cancelado - ID: {id}',
            {'id': id}
        )

        flash('Email cancelado com sucesso!', 'success')
    else:
        flash('Apenas emails pendentes podem ser cancelados.', 'warning')

    return redirect(url_for('email_programado.index'))


@email_programado_bp.route('/api/usuarios-area/<int:id_area>')
@login_required
def api_usuarios_area(id_area):
    """API para retornar usuários de uma área"""
    if not tem_permissao_email():
        return jsonify({'error': 'Sem permissão'}), 403

    usuarios = Usuario.query.filter_by(ID_AREA=id_area, ATIVO=True).all()
    return jsonify([{
        'id': u.ID,
        'nome': u.NOME,
        'email': u.EMAIL
    } for u in usuarios if u.EMAIL])


@email_programado_bp.route('/api/todos-usuarios')
@login_required
def api_todos_usuarios():
    """API para contar todos os usuários com email"""
    if not tem_permissao_email():
        return jsonify({'error': 'Sem permissão'}), 403

    count = Usuario.query.filter(
        Usuario.ATIVO == True,
        Usuario.EMAIL.isnot(None),
        Usuario.EMAIL != ''
    ).count()

    return jsonify({'count': count})


def enviar_email_agora(email_obj, config):
    """Função para enviar email imediatamente"""
    try:
        # Descriptografar senha
        senha = cipher_suite.decrypt(config.SENHA_SMTP.encode()).decode()

        # Configurar servidor SMTP
        if config.USA_TLS:
            server = smtplib.SMTP(config.SERVIDOR_SMTP, config.PORTA_SMTP)
            server.starttls()
        else:
            server = smtplib.SMTP_SSL(config.SERVIDOR_SMTP, config.PORTA_SMTP)

        server.login(config.USUARIO_SMTP, senha)

        # Criar mensagem
        msg = MIMEMultipart()
        msg['From'] = config.EMAIL_REMETENTE
        msg['Subject'] = email_obj.ASSUNTO

        # Adicionar assinatura corporativa
        corpo_completo = f"""
        {email_obj.CORPO_EMAIL}
        <br><br>
        <hr>
        <small style="color: #666;">
        Este email foi enviado através do Portal GEINC - EMGEA<br>
        Por favor, não responda a este email.
        </small>
        """

        msg.attach(MIMEText(corpo_completo, 'html'))

        # Enviar para cada destinatário
        destinatarios = email_obj.DESTINATARIO_EMAIL.split(',')
        for dest in destinatarios:
            if dest.strip():
                msg['To'] = dest.strip()
                server.send_message(msg)
                del msg['To']

        server.quit()

        # Atualizar status
        email_obj.STATUS = 'enviado'
        email_obj.DT_ENVIO = datetime.now()
        db.session.commit()

    except Exception as e:
        email_obj.STATUS = 'erro'
        email_obj.ERRO_MSG = str(e)
        db.session.commit()
        raise


# Função para ser executada pelo scheduler
def processar_emails_pendentes():
    """Processa emails pendentes que já passaram da data programada"""
    emails_pendentes = EmailProgramado.query.filter(
        EmailProgramado.STATUS == 'pendente',
        EmailProgramado.DT_PROGRAMADA <= datetime.now()
    ).all()

    for email in emails_pendentes:
        # Usar config global para emails de admin/moderador
        if email.usuario and email.usuario.perfil in ['admin', 'moderador']:
            config = ConfiguracaoEmail.query.filter_by(USUARIO_ID=1).first()
        else:
            config = ConfiguracaoEmail.query.filter_by(USUARIO_ID=email.CRIADO_POR).first()

        if config:
            try:
                enviar_email_agora(email, config)
            except Exception as e:
                print(f"Erro ao enviar email {email.ID}: {str(e)}")