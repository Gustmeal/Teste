from functools import wraps
from flask import flash, redirect, url_for
from flask_login import current_user
from app.models.permissao_sistema import PermissaoSistema


def sistema_requerido(sistema):
    """Decorador para verificar se o usuário tem acesso a um sistema específico"""

    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                flash('Por favor, faça login para acessar esta página.', 'info')
                return redirect(url_for('auth.login'))

            # Verificar se o usuário tem acesso ao sistema
            if not PermissaoSistema.verificar_acesso(current_user.id, sistema):
                flash(
                    'Você não tem acesso para esse sistema. Entre em contato com o Administrador para solicitar o acesso.',
                    'warning')
                return redirect(url_for('main.geinc_index'))

            return f(*args, **kwargs)

        return decorated_function

    return decorator