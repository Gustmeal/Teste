from app import db
from datetime import datetime


class EmailProgramado(db.Model):
    """Modelo para emails programados"""
    __tablename__ = 'APK_TB010_EMAILS_PROGRAMADOS'
    __table_args__ = {'schema': 'DEV'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    REMETENTE_EMAIL = db.Column(db.String(100), nullable=False)
    DESTINATARIO_EMAIL = db.Column(db.Text)  # Pode ser múltiplos separados por vírgula
    DESTINATARIO_TIPO = db.Column(db.String(20))  # 'individual', 'area' ou 'todos'
    ID_AREA_DESTINO = db.Column(db.Integer, db.ForeignKey('dbo.DPJ_TB001_AREA.ID_AREA'))
    ASSUNTO = db.Column(db.String(200), nullable=False)
    CORPO_EMAIL = db.Column(db.Text, nullable=False)
    DT_PROGRAMADA = db.Column(db.DateTime, nullable=False)
    DT_ENVIO = db.Column(db.DateTime)  # Quando foi efetivamente enviado
    STATUS = db.Column(db.String(20), default='pendente')  # pendente, enviado, erro, cancelado
    ERRO_MSG = db.Column(db.Text)  # Mensagem de erro se houver
    CRIADO_POR = db.Column(db.Integer, db.ForeignKey('BDG.APK_TB002_USUARIOS.ID'))
    DT_CRIACAO = db.Column(db.DateTime, default=datetime.utcnow)

    # Relacionamentos
    area = db.relationship('Area', backref='emails_programados', foreign_keys=[ID_AREA_DESTINO])
    usuario = db.relationship('Usuario', backref='emails_criados')


class ConfiguracaoEmail(db.Model):
    """Configurações de email do usuário"""
    __tablename__ = 'APK_TB011_CONFIG_EMAIL'
    __table_args__ = {'schema': 'DEV'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    USUARIO_ID = db.Column(db.Integer, db.ForeignKey('BDG.APK_TB002_USUARIOS.ID'), unique=True)
    EMAIL_REMETENTE = db.Column(db.String(100))
    SERVIDOR_SMTP = db.Column(db.String(100))
    PORTA_SMTP = db.Column(db.Integer)
    USUARIO_SMTP = db.Column(db.String(100))
    SENHA_SMTP = db.Column(db.String(200))  # Deve ser criptografada
    USA_TLS = db.Column(db.Boolean, default=True)

    usuario = db.relationship('Usuario', backref='config_email', uselist=False)