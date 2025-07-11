from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from app import db
from sqlalchemy import text


class Empregado(db.Model):
    """Modelo para acessar a tabela de empregados diretamente do BDG"""
    __tablename__ = 'PES_TB001_EMPREGADOS'
    __table_args__ = {'schema': 'BDG', 'extend_existing': True}

    # Como não tem PK definida, usamos pkPessoa com primary_key=True para SQLAlchemy
    pkPessoa = db.Column(db.Integer, primary_key=True)
    nmPessoa = db.Column(db.String(250), nullable=False)
    fkStatus = db.Column(db.Integer, nullable=False)
    fkSetor = db.Column(db.Integer)
    sgSetor = db.Column(db.String(10))
    sgSuperintendencia = db.Column(db.String(10))
    sgDiretoria = db.Column(db.String(10))
    fkCargo = db.Column(db.Integer)
    dsCargo = db.Column(db.String(100))
    pkEmpregadoCedido = db.Column(db.Integer)
    dsLogon = db.Column(db.String(9))
    dtInclusao = db.Column(db.Date)
    dsEnderecoEletronico = db.Column(db.String(100))
    DT_REFERENCIA = db.Column(db.Date)

    @property
    def esta_ativo(self):
        """Verifica se o empregado está ativo (assumindo que fkStatus = 1 é ativo)"""
        return self.fkStatus == 1

    @staticmethod
    def buscar_por_email(email):
        """Busca empregado pelo email - tratando possíveis duplicados"""
        # Como pode haver duplicados, pegar o mais recente
        return Empregado.query.filter_by(dsEnderecoEletronico=email) \
            .order_by(Empregado.DT_REFERENCIA.desc()) \
            .first()


class Usuario(db.Model):
    __tablename__ = 'APK_TB002_USUARIOS'
    __table_args__ = {'schema': 'BDG'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    NOME = db.Column(db.String(100), nullable=False)
    EMAIL = db.Column(db.String(100), unique=True, nullable=False)
    SENHA_HASH = db.Column(db.String(255), nullable=False)
    ATIVO = db.Column(db.Boolean, default=True)
    PERFIL = db.Column(db.String(20), default='usuario')
    FK_PESSOA = db.Column(db.Integer)  # Nova coluna para vincular com empregado
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

    def is_moderador(self):
        return self.PERFIL == 'moderador'

    @property
    def empregado(self):
        """Retorna os dados do empregado vinculado"""
        if self.FK_PESSOA:
            return Empregado.query.filter_by(pkPessoa=self.FK_PESSOA).first()
        return None

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

    @staticmethod
    def validar_email_empregado(email):
        """Valida se o email existe na base de empregados e se pode criar usuário"""
        # Verificar formato
        if not email.endswith('@emgea.gov.br'):
            return False, "Por favor, utilize seu email institucional (@emgea.gov.br)."

        # Verificar se existe na base de empregados
        empregado = Empregado.buscar_por_email(email)
        if not empregado:
            return False, "Email não encontrado na base de empregados. Entre em contato com o RH."

        # Verificar se empregado está ativo
        if not empregado.esta_ativo:
            return False, "Empregado inativo. Entre em contato com o RH."

        # Verificar se já tem usuário criado
        usuario_existente = Usuario.query.filter_by(EMAIL=email).first()
        if usuario_existente:
            return False, "Usuário já cadastrado. Use a opção de login."

        return True, empregado

    def __repr__(self):
        return f'<Usuario {self.EMAIL}>'