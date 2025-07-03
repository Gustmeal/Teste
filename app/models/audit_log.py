from datetime import datetime
from app import db


class AuditLog(db.Model):
    __tablename__ = 'DCA_TB_AUDIT_LOG'
    __table_args__ = {'schema': 'DEV'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    USUARIO_ID = db.Column(db.Integer, db.ForeignKey('DEV.DCA_TB000_USUARIOS.ID'), nullable=False)
    USUARIO_NOME = db.Column(db.String(100), nullable=False)
    ACAO = db.Column(db.String(50), nullable=False)
    ENTIDADE = db.Column(db.String(50), nullable=False)
    ENTIDADE_ID = db.Column(db.Integer)
    DESCRICAO = db.Column(db.String(255), nullable=False)
    DATA = db.Column(db.DateTime, default=datetime.utcnow)
    IP = db.Column(db.String(50))
    DADOS_ANTIGOS = db.Column(db.Text)
    DADOS_NOVOS = db.Column(db.Text)

    # Novos campos para controle de reversão
    REVERTIDO = db.Column(db.Boolean, default=False)
    REVERTIDO_POR = db.Column(db.Integer, db.ForeignKey('DEV.DCA_TB000_USUARIOS.ID'))
    REVERTIDO_EM = db.Column(db.DateTime)
    LOG_REVERSAO_ID = db.Column(db.Integer, db.ForeignKey('DEV.DCA_TB_AUDIT_LOG.ID'))

    # Relacionamentos
    usuario = db.relationship('Usuario', foreign_keys=[USUARIO_ID], backref='logs')
    revertido_por_usuario = db.relationship('Usuario', foreign_keys=[REVERTIDO_POR])
    log_reversao = db.relationship('AuditLog', remote_side=[ID], backref='log_original')

    def __repr__(self):
        return f'<AuditLog {self.ID} - {self.ACAO} {self.ENTIDADE}>'