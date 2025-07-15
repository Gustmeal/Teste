from flask import Blueprint, render_template, redirect, url_for, request, flash, session
from flask_login import login_user, logout_user, login_required, current_user
from app import db
from app.models.usuario import Usuario
from app.models.permissao_sistema import PermissaoSistema
from app.auth.utils import UserLogin, admin_required, admin_or_moderador_required
from app.utils.audit import registrar_log
from datetime import datetime

auth_bp = Blueprint('auth', __name__)


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('main.geinc_index'))

    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()
        senha = request.form.get('senha', '')

        usuario = Usuario.query.filter_by(EMAIL=email).first()

        if usuario and usuario.verificar_senha(senha):
            if usuario.is_active():
                # Criar objeto UserLogin
                user_login = UserLogin(usuario.ID, usuario.EMAIL, usuario.NOME, usuario.PERFIL)

                # Carregar dados do empregado se existir
                if usuario.empregado:
                    user_login.empregado = {
                        'area': usuario.empregado.sgSuperintendencia,
                        'cargo': usuario.empregado.dsCargo
                    }

                login_user(user_login)

                # Registrar log de login
                registrar_log(
                    acao='login',
                    entidade='sistema',
                    entidade_id=usuario.ID,
                    descricao=f'Login realizado: {email}'
                )

                flash(f'Bem-vindo, {usuario.NOME}!', 'success')
                next_page = request.args.get('next')
                return redirect(next_page) if next_page else redirect(url_for('main.geinc_index'))
            else:
                flash('Conta inativa. Entre em contato com o administrador.', 'danger')
        else:
            flash('E-mail ou senha incorretos. Por favor, tente novamente.', 'danger')

    return render_template('auth/login.html')


# Substituir a rota de registro por primeiro acesso
@auth_bp.route('/primeiro-acesso', methods=['GET', 'POST'])
def primeiro_acesso():
    if current_user.is_authenticated:
        return redirect(url_for('main.geinc_index'))

    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()

        # Validar email contra base de empregados
        valido, resultado = Usuario.validar_email_empregado(email)

        if valido:
            empregado = resultado
            # Armazenar temporariamente para a próxima etapa
            session['primeiro_acesso_email'] = email
            session['primeiro_acesso_pk'] = empregado.pkPessoa
            session['primeiro_acesso_nome'] = empregado.nmPessoa

            return redirect(url_for('auth.definir_senha'))
        else:
            flash(resultado, 'danger')

    return render_template('auth/primeiro_acesso.html')


@auth_bp.route('/definir-senha', methods=['GET', 'POST'])
def definir_senha():
    # Verificar se veio da primeira etapa
    if 'primeiro_acesso_email' not in session:
        return redirect(url_for('auth.primeiro_acesso'))

    email = session.get('primeiro_acesso_email')
    pk_pessoa = session.get('primeiro_acesso_pk')
    nome = session.get('primeiro_acesso_nome')

    if request.method == 'POST':
        senha = request.form.get('senha', '')
        confirmar_senha = request.form.get('confirmar_senha', '')

        if len(senha) < 8:
            flash('A senha deve ter pelo menos 8 caracteres.', 'danger')
        elif senha != confirmar_senha:
            flash('As senhas não coincidem.', 'danger')
        else:
            try:
                # Criar usuário
                novo_usuario = Usuario(
                    NOME=nome,
                    EMAIL=email,
                    FK_PESSOA=pk_pessoa,
                    PERFIL='usuario'
                )
                novo_usuario.set_senha(senha)

                db.session.add(novo_usuario)
                db.session.commit()

                # Criar permissões padrão em DEV
                PermissaoSistema.criar_permissoes_padrao(novo_usuario.ID)

                # Registrar log
                registrar_log(
                    acao='criar',
                    entidade='usuario',
                    entidade_id=novo_usuario.ID,
                    descricao=f'Primeiro acesso criado para: {email}'
                )

                # Limpar sessão
                session.pop('primeiro_acesso_email', None)
                session.pop('primeiro_acesso_pk', None)
                session.pop('primeiro_acesso_nome', None)

                flash('Conta criada com sucesso! Faça login para continuar.', 'success')
                return redirect(url_for('auth.login'))

            except Exception as e:
                db.session.rollback()
                flash(f'Erro ao criar conta: {str(e)}', 'danger')

    return render_template('auth/definir_senha.html', email=email, nome=nome)


@auth_bp.route('/logout')
@login_required
def logout():
    # Registrar log de logout
    registrar_log(
        acao='logout',
        entidade='sistema',
        entidade_id=current_user.id,
        descricao=f'Logout realizado: {current_user.email}'
    )

    logout_user()
    flash('Você saiu do sistema.', 'info')
    return redirect(url_for('auth.login'))


@auth_bp.route('/usuarios')
@login_required
@admin_or_moderador_required
def lista_usuarios():
    usuarios = Usuario.query.filter(Usuario.DELETED_AT == None).all()
    return render_template('auth/lista_usuarios.html', usuarios=usuarios)


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


@auth_bp.route('/usuarios/<int:id>/permissoes', methods=['GET', 'POST'])
@login_required
@admin_or_moderador_required
def gerenciar_permissoes(id):
    usuario = Usuario.query.get_or_404(id)

    # Não permitir editar permissões de admins e moderadores
    if usuario.PERFIL in ['admin', 'moderador']:
        flash('Administradores e moderadores têm acesso total aos sistemas.', 'info')
        return redirect(url_for('auth.lista_usuarios'))

    if request.method == 'POST':
        try:
            # Processar cada sistema
            for sistema in PermissaoSistema.SISTEMAS_DISPONIVEIS.keys():
                # Verificar se o checkbox foi marcado
                tem_acesso = request.form.get(f'sistema_{sistema}') == 'on'

                # Buscar permissão existente em DEV
                permissao = PermissaoSistema.query.filter_by(
                    USUARIO_ID=usuario.ID,
                    SISTEMA=sistema
                ).first()

                if permissao:
                    # Atualizar permissão existente
                    if permissao.DELETED_AT:
                        # Reativar se estava deletada
                        permissao.DELETED_AT = None
                    permissao.TEM_ACESSO = tem_acesso
                    permissao.UPDATED_AT = datetime.utcnow()
                else:
                    # Criar nova permissão
                    permissao = PermissaoSistema(
                        USUARIO_ID=usuario.ID,
                        SISTEMA=sistema,
                        TEM_ACESSO=tem_acesso
                    )
                    db.session.add(permissao)

            db.session.commit()

            # Registrar log
            registrar_log(
                acao='atualizar',
                entidade='permissoes',
                entidade_id=usuario.ID,
                descricao=f'Permissões de sistema atualizadas para: {usuario.EMAIL}'
            )

            flash('Permissões atualizadas com sucesso!', 'success')
            return redirect(url_for('auth.lista_usuarios'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao atualizar permissões: {str(e)}', 'danger')

    # Buscar permissões atuais em DEV
    permissoes = {}
    for sistema in PermissaoSistema.SISTEMAS_DISPONIVEIS.keys():
        permissao = PermissaoSistema.query.filter_by(
            USUARIO_ID=usuario.ID,
            SISTEMA=sistema,
            DELETED_AT=None
        ).first()
        permissoes[sistema] = permissao.TEM_ACESSO if permissao else True

    return render_template('auth/gerenciar_permissoes.html',
                           usuario=usuario,
                           sistemas=PermissaoSistema.SISTEMAS_DISPONIVEIS,
                           permissoes=permissoes)


@auth_bp.route('/api/verificar-acesso/<sistema>')
@login_required
def verificar_acesso_api(sistema):
    tem_acesso = PermissaoSistema.verificar_acesso(current_user.id, sistema)
    return {'tem_acesso': tem_acesso}


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