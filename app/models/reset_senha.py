from datetime import datetime
from app import db


class ResetSenha(db.Model):
    """Modelo para solicitações de reset de senha"""
    __tablename__ = 'APK_TB009_RESET_SENHA'
    __table_args__ = {'schema': 'BDG'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    USUARIO_ID = db.Column(db.Integer, db.ForeignKey('BDG.APK_TB002_USUARIOS.ID'), nullable=False)
    STATUS = db.Column(db.String(20), default='PENDENTE')  # PENDENTE, APROVADO, RECUSADO
    MOTIVO = db.Column(db.String(500))
    SOLICITADO_AT = db.Column(db.DateTime, default=datetime.utcnow)
    APROVADO_POR = db.Column(db.Integer, db.ForeignKey('BDG.APK_TB002_USUARIOS.ID'))
    APROVADO_AT = db.Column(db.DateTime)
    NOVA_SENHA_TEMPORARIA = db.Column(db.String(50))
    DELETED_AT = db.Column(db.DateTime)

    # Relacionamentos
    usuario = db.relationship('Usuario', foreign_keys=[USUARIO_ID], backref='solicitacoes_reset')
    aprovador = db.relationship('Usuario', foreign_keys=[APROVADO_POR])

    def __repr__(self):
        return f'<ResetSenha {self.ID} - Usuario: {self.USUARIO_ID} - Status: {self.STATUS}>'