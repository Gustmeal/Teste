from functools import wraps
from flask import session, redirect, url_for, flash, request
from flask_login import LoginManager, UserMixin, current_user

login_manager = LoginManager()

class UserLogin(UserMixin):
    def __init__(self, user_id, email, nome, perfil):
        self.id = user_id
        self.email = email
        self.nome = nome
        self.perfil = perfil

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or current_user.perfil != 'admin':
            flash('Acesso restrito. Você precisa ser um administrador.', 'danger')
            return redirect(url_for('auth.login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

def admin_or_moderador_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or (current_user.perfil != 'admin' and current_user.perfil != 'moderador'):
            flash('Acesso restrito. Você precisa ser um administrador ou moderador.', 'danger')
            return redirect(url_for('auth.login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

def init_login_manager(app):
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message = 'Por favor, faça login para acessar esta página.'
    login_manager.login_message_category = 'info'

    from app.models.usuario import Usuario

    @login_manager.user_loader
    def load_user(user_id):
        user = Usuario.query.get(int(user_id))
        if user and user.is_active():
            return UserLogin(user.ID, user.EMAIL, user.NOME, user.PERFIL)
        return None