# app/models/meta_avaliacao.py
from datetime import datetime
from app import db


class MetaAvaliacao(db.Model):
    __tablename__ = 'DCA_TB009_META_AVALIACAO'
    __table_args__ = {'schema': 'DEV'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ID_EDITAL = db.Column(db.Integer, nullable=False)
    ID_PERIODO = db.Column(db.Integer, nullable=False)
    ID_EMPRESA = db.Column(db.Integer, nullable=False)
    ANO_MES_COMPETENCIA = db.Column(db.String(7), nullable=False)  # Formato: YYYY-MM
    VR_META_ARRECADACAO = db.Column(db.Numeric(18, 2), nullable=True)
    VR_META_ACIONAMENTO = db.Column(db.Integer, nullable=True)
    QTDE_META_LIQUIDACAO = db.Column(db.Integer, nullable=True)
    QTDE_META_BONIFICACAO = db.Column(db.Numeric(18, 2), nullable=True)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    def __repr__(self):
        return f'<MetaAvaliacao {self.ID} - Competência: {self.ANO_MES_COMPETENCIA}>'


class MetaSemestral(db.Model):
    __tablename__ = 'DCA_TB010_META_SEMESTRAL'
    __table_args__ = {'schema': 'DEV'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ID_EDITAL = db.Column(db.Integer, nullable=False)
    ID_PERIODO = db.Column(db.Integer, nullable=False)
    ID_EMPRESA = db.Column(db.Integer, nullable=False)
    NO_ABREVIADO_EMPRESA = db.Column(db.String(30), nullable=True)
    VR_SALDO_DEVEDOR = db.Column(db.Numeric(18, 2), nullable=True)
    PERC_SD_EMPRESA = db.Column(db.Numeric(5, 2), nullable=True)
    VR_META_TOTAL = db.Column(db.Numeric(18, 2), nullable=True)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    def __repr__(self):
        return f'<MetaSemestral {self.ID} - Empresa: {self.NO_ABREVIADO_EMPRESA}>'