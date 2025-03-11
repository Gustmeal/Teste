from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from config import Config

db = SQLAlchemy()


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    db.init_app(app)

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

    # Redirecionar a rota raiz para o login se o usuário não estiver autenticado
    @app.route('/')
    def index():
        from flask import redirect, url_for
        from flask_login import current_user

        if not current_user.is_authenticated:
            return redirect(url_for('auth.login'))

        return redirect(url_for('edital.index'))

    return app