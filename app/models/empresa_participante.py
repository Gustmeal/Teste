from datetime import datetime
from app import db


class EmpresaParticipante(db.Model):
    __tablename__ = 'DCA_TB002_EMPRESAS_PARTICIPANTES'
    __table_args__ = {'schema': 'DEV'}  # Especifica o esquema

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ID_EDITAL = db.Column(db.Integer, nullable=False)
    ID_PERIODO = db.Column(db.Integer, db.ForeignKey('DEV.DCA_TB001_PERIODO_AVALIACAO.ID'), nullable=False)
    ID_EMPRESA = db.Column(db.Integer, nullable=False)
    NO_EMPRESA = db.Column(db.String(100), nullable=True)
    NO_EMPRESA_ABREVIADA = db.Column(db.String(30), nullable=True)
    DS_CONDICAO = db.Column(db.String(50), nullable=True)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    periodo = db.relationship('PeriodoAvaliacao', backref='empresas')

    def __repr__(self):
        return f'<EmpresaParticipante {self.NO_EMPRESA}>'