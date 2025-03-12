from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from app import db


class Usuario(db.Model):
    __tablename__ = 'DCA_TB000_USUARIOS'
    __table_args__ = {'schema': 'DEV'}  # Especifica o esquema

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    NOME = db.Column(db.String(100), nullable=False)
    EMAIL = db.Column(db.String(100), unique=True, nullable=False)
    SENHA_HASH = db.Column(db.String(255), nullable=False)
    ATIVO = db.Column(db.Boolean, default=True)
    PERFIL = db.Column(db.String(20), default='usuario')  # 'admin', 'usuario'
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    def set_senha(self, senha):
        self.SENHA_HASH = generate_password_hash(senha)

    def verificar_senha(self, senha):
        return check_password_hash(self.SENHA_HASH, senha)

    def is_active(self):
        return self.ATIVO and self.DELETED_AT is None

    def is_admin(self):
        return self.PERFIL == 'admin'

    @staticmethod
    def validar_email(email):
        """Valida se o email é institucional e não está em uso."""
        # Verificar formato
        if not email.endswith('@emgea.gov.br'):
            return False, "Por favor, utilize seu email institucional (@emgea.gov.br)."

        # Verificar se já existe
        usuario_existente = Usuario.query.filter_by(EMAIL=email).first()
        if usuario_existente:
            return False, "Este e-mail já está cadastrado."

        return True, ""

    def __repr__(self):
        return f'<Usuario {self.EMAIL}>'