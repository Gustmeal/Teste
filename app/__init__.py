from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from config import Config

db = SQLAlchemy()


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    db.init_app(app)

    with app.app_context():
        db.create_all()

    from app.routes.edital_routes import edital_bp
    app.register_blueprint(edital_bp)

    from app.routes.periodo_routes import periodo_bp
    app.register_blueprint(periodo_bp)

    return app