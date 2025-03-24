from datetime import datetime
from app import db

class Feedback(db.Model):
    __tablename__ = 'DCA_TB010_FEEDBACK'
    __table_args__ = {'schema': 'DEV'}  # Mesmo esquema das outras tabelas

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    USUARIO_ID = db.Column(db.Integer, db.ForeignKey('DEV.DCA_TB000_USUARIOS.ID'), nullable=False)
    TITULO = db.Column(db.String(100), nullable=False)
    MENSAGEM = db.Column(db.Text, nullable=False)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    LIDO = db.Column(db.Boolean, default=False)
    RESPONDIDO = db.Column(db.Boolean, default=False)
    RESPOSTA = db.Column(db.Text)
    RESPONDIDO_POR = db.Column(db.Integer, db.ForeignKey('DEV.DCA_TB000_USUARIOS.ID'))
    RESPONDIDO_AT = db.Column(db.DateTime)

    # Relacionamentos
    usuario = db.relationship('Usuario', foreign_keys=[USUARIO_ID], backref='feedbacks')
    respondido_por_usuario = db.relationship('Usuario', foreign_keys=[RESPONDIDO_POR])

    def __repr__(self):
        return f'<Feedback {self.ID} - {self.TITULO}>'