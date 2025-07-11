from datetime import datetime
from app import db


class PeriodoAvaliacao(db.Model):
    __tablename__ = 'DCA_TB001_PERIODO_AVALIACAO'
    __table_args__ = {'schema': 'BDG'}  # Especifica o esquema

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ID_PERIODO = db.Column(db.Integer, nullable=False)  # Campo adicionado
    ID_EDITAL = db.Column(db.Integer, db.ForeignKey('BDG.DCA_TB008_EDITAIS.ID'), nullable=False)
    DT_INICIO = db.Column(db.DateTime, nullable=False)
    DT_FIM = db.Column(db.DateTime, nullable=False)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    edital = db.relationship('Edital', backref='periodos')

    def __repr__(self):
        return f'<Período {self.ID}>'