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
        # Importar todos os modelos antes de criar as tabelas
        from app.models.usuario import Usuario
        from app.models.edital import Edital
        from app.models.periodo import PeriodoAvaliacao
        from app.models.empresa_participante import EmpresaParticipante
        from app.models.empresa_responsavel import EmpresaResponsavel
        from app.models.criterio_selecao import CriterioSelecao
        from app.models.limite_distribuicao import LimiteDistribuicao
        from app.models.meta_avaliacao import MetaAvaliacao
        from app.models.audit_log import AuditLog
        from app.models.feedback import Feedback
        from app.models.mensagem import Mensagem

        db.create_all()

    # Registrar blueprint para a página principal do GEINC
    from app.routes.main_routes import main_bp
    app.register_blueprint(main_bp)

    # Registrar o blueprint principal do credenciamento
    from app.routes.credenciamento_routes import credenciamento_bp
    app.register_blueprint(credenciamento_bp)

    # Registrar os outros blueprints com prefixo
    from app.routes.edital_routes import edital_bp
    app.register_blueprint(edital_bp)

    from app.routes.periodo_routes import periodo_bp
    app.register_blueprint(periodo_bp)

    from app.routes.meta_routes import meta_bp
    app.register_blueprint(meta_bp)

    from app.routes.limite_routes import limite_bp
    app.register_blueprint(limite_bp)

    from app.routes.empresa_routes import empresa_bp
    app.register_blueprint(empresa_bp)

    from app.auth.routes import auth_bp
    app.register_blueprint(auth_bp)

    from app.routes.audit_routes import audit_bp
    app.register_blueprint(audit_bp)

    from app.routes.feedback_routes import feedback_bp
    app.register_blueprint(feedback_bp)

    from app.routes.chat_routes import chat_bp
    app.register_blueprint(chat_bp)

    # Registrar o novo blueprint de exportação
    from app.routes.export_routes import export_bp
    app.register_blueprint(export_bp)

    # Definir rota raiz para redirecionar para o portal GEINC
    @app.route('/')
    def index():
        from flask import redirect, url_for
        from flask_login import current_user

        if not current_user.is_authenticated:
            return redirect(url_for('auth.login'))

        return redirect(url_for('main.geinc_index'))

    return app