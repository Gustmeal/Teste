from datetime import datetime
from app import db


class AuditLog(db.Model):
    __tablename__ = 'DCA_TB_AUDIT_LOG'
    __table_args__ = {'schema': 'DEV'}  # Especifica o esquema

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    USUARIO_ID = db.Column(db.Integer, db.ForeignKey('DEV.DCA_TB000_USUARIOS.ID'), nullable=False)
    USUARIO_NOME = db.Column(db.String(100), nullable=False)
    ACAO = db.Column(db.String(50), nullable=False)  # 'criar', 'editar', 'excluir'
    ENTIDADE = db.Column(db.String(50), nullable=False)  # 'edital', 'periodo', etc.
    ENTIDADE_ID = db.Column(db.Integer)
    DESCRICAO = db.Column(db.String(255), nullable=False)
    DATA = db.Column(db.DateTime, default=datetime.utcnow)
    IP = db.Column(db.String(50))
    DADOS_ANTIGOS = db.Column(db.Text)
    DADOS_NOVOS = db.Column(db.Text)

    usuario = db.relationship('Usuario', backref='logs')

    def __repr__(self):
        return f'<AuditLog {self.ID} - {self.ACAO} {self.ENTIDADE}>'