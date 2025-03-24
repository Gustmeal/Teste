from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from config import Config

db = SQLAlchemy()


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    db.init_app(app)

    # Adicionar filtros personalizados
    from app.utils.formatters import format_currency, format_number

    @app.template_filter('br_currency')
    def br_currency_filter(value):
        return format_currency(value)

    @app.template_filter('br_number')
    def br_number_filter(value):
        return format_number(value)

    # Inicializar Flask-Login
    from app.auth.utils import init_login_manager
    init_login_manager(app)

    with app.app_context():
        db.create_all()

    # Registrar blueprints
    from app.routes.edital_routes import edital_bp
    app.register_blueprint(edital_bp)

    from app.routes.periodo_routes import periodo_bp
    app.register_blueprint(periodo_bp)

    from app.auth.routes import auth_bp
    app.register_blueprint(auth_bp)

    from app.routes.audit_routes import audit_bp
    app.register_blueprint(audit_bp)

    from app.routes.empresa_routes import empresa_bp
    app.register_blueprint(empresa_bp)

    # Registrar blueprint para metas de avaliação
    from app.routes.meta_routes import meta_bp
    app.register_blueprint(meta_bp)

    # Registrar blueprint para limites de distribuição
    from app.routes.limite_routes import limite_bp
    app.register_blueprint(limite_bp)

    # Registrar blueprint para feedback
    from app.routes.feedback_routes import feedback_bp
    app.register_blueprint(feedback_bp)

    # Registrar blueprint para chat
    from app.routes.chat_routes import chat_bp
    app.register_blueprint(chat_bp)

    # Redirecionar a rota raiz para o login se o usuário não estiver autenticado
    @app.route('/')
    def index():
        from flask import redirect, url_for
        from flask_login import current_user

        if not current_user.is_authenticated:
            return redirect(url_for('auth.login'))

        return redirect(url_for('edital.index'))

    return app