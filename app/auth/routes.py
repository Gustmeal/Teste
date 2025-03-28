from flask import Blueprint, render_template, redirect, url_for, request, flash
from flask_login import login_user, logout_user, login_required, current_user
from app.models.usuario import Usuario
from app import db
from datetime import datetime
from app.auth.utils import UserLogin, admin_required, admin_or_moderador_required

auth_bp = Blueprint('auth', __name__)


@auth_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('main.geinc_index'))

    if request.method == 'POST':
        email = request.form.get('email')
        senha = request.form.get('senha')

        usuario = Usuario.query.filter_by(EMAIL=email, DELETED_AT=None).first()

        if usuario and usuario.verificar_senha(senha) and usuario.is_active():
            user_login = UserLogin(usuario.ID, usuario.EMAIL, usuario.NOME, usuario.PERFIL)
            login_user(user_login)

            next_page = request.args.get('next')
            return redirect(next_page or url_for('main.geinc_index'))
        else:
            flash('Credenciais inválidas. Por favor, tente novamente.', 'danger')

    return render_template('auth/login.html')


@auth_bp.route('/registrar', methods=['GET', 'POST'])
def registrar():
    if current_user.is_authenticated:
        return redirect(url_for('main.geinc_index'))

    if request.method == 'POST':
        try:
            email = request.form['email']

            # Verificar se é um email institucional
            if not email.endswith('@emgea.gov.br'):
                flash('Por favor, utilize seu email institucional (@emgea.gov.br).', 'danger')
                return render_template('auth/registrar.html')

            # Verificar se o e-mail já existe
            usuario_existente = Usuario.query.filter_by(EMAIL=email).first()
            if usuario_existente:
                flash('Este e-mail já está cadastrado.', 'danger')
                return render_template('auth/registrar.html')

            novo_usuario = Usuario(
                NOME=request.form['nome'],
                EMAIL=email,
                PERFIL='usuario'  # Por padrão, novos registros são usuários comuns
            )
            novo_usuario.set_senha(request.form['senha'])

            db.session.add(novo_usuario)
            db.session.commit()

            # Fazer login automático após o registro
            user_login = UserLogin(novo_usuario.ID, novo_usuario.EMAIL, novo_usuario.NOME, novo_usuario.PERFIL)
            login_user(user_login)

            flash('Conta criada com sucesso!', 'success')
            return redirect(url_for('main.geinc_index'))
        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao criar conta: {str(e)}', 'danger')

    return render_template('auth/registrar.html')


@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    flash('Você saiu do sistema.', 'info')
    return redirect(url_for('auth.login'))


@auth_bp.route('/usuarios')
@login_required
@admin_or_moderador_required
def lista_usuarios():
    usuarios = Usuario.query.filter(Usuario.DELETED_AT == None).all()
    return render_template('auth/lista_usuarios.html', usuarios=usuarios)


@auth_bp.route('/usuarios/novo', methods=['GET', 'POST'])
@login_required
@admin_or_moderador_required
def novo_usuario():
    if request.method == 'POST':
        try:
            email = request.form['email']

            # Verificar se é um email institucional
            if not email.endswith('@emgea.gov.br'):
                flash('Por favor, utilize um email institucional (@emgea.gov.br).', 'danger')
                return render_template('auth/form_usuario.html')

            # Verificar se o e-mail já existe
            usuario_existente = Usuario.query.filter_by(EMAIL=email).first()
            if usuario_existente:
                flash('Este e-mail já está cadastrado.', 'danger')
                return render_template('auth/form_usuario.html')

            # Restrição: Apenas administradores podem criar outros administradores
            if request.form['perfil'] == 'admin' and current_user.perfil != 'admin':
                flash('Apenas administradores podem criar contas de administrador.', 'danger')
                return render_template('auth/form_usuario.html')

            novo_usuario = Usuario(
                NOME=request.form['nome'],
                EMAIL=email,
                PERFIL=request.form['perfil']
            )
            novo_usuario.set_senha(request.form['senha'])

            db.session.add(novo_usuario)
            db.session.commit()
            flash('Usuário cadastrado com sucesso!', 'success')
            return redirect(url_for('auth.lista_usuarios'))
        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')

    return render_template('auth/form_usuario.html')


@auth_bp.route('/usuarios/editar/<int:id>', methods=['GET', 'POST'])
@login_required
@admin_or_moderador_required
def editar_usuario(id):
    usuario = Usuario.query.get_or_404(id)

    # Apenas admins podem editar outros admins
    if usuario.PERFIL == 'admin' and current_user.perfil != 'admin':
        flash('Apenas administradores podem editar contas de administrador.', 'danger')
        return redirect(url_for('auth.lista_usuarios'))

    if request.method == 'POST':
        try:
            usuario.NOME = request.form['nome']

            # Restrição: Apenas administradores podem promover para admin
            novo_perfil = request.form['perfil']
            if novo_perfil == 'admin' and current_user.perfil != 'admin':
                flash('Apenas administradores podem promover usuários a administradores.', 'danger')
                return render_template('auth/form_usuario.html', usuario=usuario)

            usuario.PERFIL = novo_perfil

            # Verifica se a senha foi alterada
            nova_senha = request.form.get('senha')
            if nova_senha and nova_senha.strip():
                usuario.set_senha(nova_senha)

            db.session.commit()
            flash('Usuário atualizado!', 'success')
            return redirect(url_for('auth.lista_usuarios'))
        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')

    return render_template('auth/form_usuario.html', usuario=usuario)


@auth_bp.route('/usuarios/excluir/<int:id>')
@login_required
@admin_or_moderador_required
def excluir_usuario(id):
    try:
        usuario = Usuario.query.get_or_404(id)

        # Apenas admins podem excluir outros admins
        if usuario.PERFIL == 'admin' and current_user.perfil != 'admin':
            flash('Apenas administradores podem excluir contas de administrador.', 'danger')
            return redirect(url_for('auth.lista_usuarios'))

        # Impedir exclusão do próprio usuário
        if usuario.ID == current_user.id:
            flash('Você não pode excluir sua própria conta.', 'danger')
            return redirect(url_for('auth.lista_usuarios'))

        usuario.DELETED_AT = datetime.utcnow()
        db.session.commit()
        flash('Usuário removido!', 'warning')
    except Exception as e:
        db.session.rollback()
        flash(f'Erro: {str(e)}', 'danger')
    return redirect(url_for('auth.lista_usuarios'))


@auth_bp.route('/perfil', methods=['GET', 'POST'])
@login_required
def perfil():
    usuario = Usuario.query.get(current_user.id)

    if request.method == 'POST':
        try:
            usuario.NOME = request.form['nome']

            # Verificar senha atual antes de alterar a senha
            senha_atual = request.form.get('senha_atual')
            nova_senha = request.form.get('nova_senha')

            if senha_atual and nova_senha:
                if usuario.verificar_senha(senha_atual):
                    usuario.set_senha(nova_senha)
                    flash('Senha alterada com sucesso!', 'success')
                else:
                    flash('Senha atual incorreta.', 'danger')
                    return render_template('auth/perfil.html', usuario=usuario)

            db.session.commit()
            flash('Perfil atualizado!', 'success')
        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')

    return render_template('auth/perfil.html', usuario=usuario)