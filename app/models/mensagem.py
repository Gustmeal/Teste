from datetime import datetime
from app import db

class Mensagem(db.Model):
    __tablename__ = 'APK_TB003_MENSAGENS'
    __table_args__ = {'schema': 'BDG'}  # Mesmo esquema das outras tabelas

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    REMETENTE_ID = db.Column(db.Integer, db.ForeignKey('BDG.APK_TB002_USUARIOS.ID'), nullable=False)
    DESTINATARIO_ID = db.Column(db.Integer, db.ForeignKey('BDG.APK_TB002_USUARIOS.ID'), nullable=False)
    CONTEUDO = db.Column(db.Text, nullable=False)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    LIDO = db.Column(db.Boolean, default=False)
    LIDO_AT = db.Column(db.DateTime)

    # Relacionamentos
    remetente = db.relationship('Usuario', foreign_keys=[REMETENTE_ID], backref='mensagens_enviadas')
    destinatario = db.relationship('Usuario', foreign_keys=[DESTINATARIO_ID], backref='mensagens_recebidas')

    def __repr__(self):
        return f'<Mensagem {self.ID} - De: {self.REMETENTE_ID} Para: {self.DESTINATARIO_ID}>'