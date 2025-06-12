from datetime import datetime
from app import db


class MetasPercentuaisDistribuicao(db.Model):
    __tablename__ = 'DCA_TB015_METAS_PERCENTUAIS_DISTRIBUICAO'
    __table_args__ = {'schema': 'DEV'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    DT_REFERENCIA = db.Column(db.Date, nullable=False)
    ID_EDITAL = db.Column(db.Integer, nullable=False)
    ID_PERIODO = db.Column(db.Integer, nullable=False)
    ID_EMPRESA = db.Column(db.Integer, nullable=False)
    VR_SALDO_DEVEDOR_DISTRIBUIDO = db.Column(db.Numeric(18, 2))
    PERCENTUAL_SALDO_DEVEDOR = db.Column(db.Numeric(12, 8))
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)


class Metas(db.Model):
    __tablename__ = 'DCA_TB013_METAS'
    __table_args__ = {'schema': 'DEV'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    DT_REFERENCIA = db.Column(db.Date, nullable=False)
    ID_EDITAL = db.Column(db.Integer, nullable=False)
    ID_PERIODO = db.Column(db.Integer, nullable=False)
    COMPETENCIA = db.Column(db.String(7), nullable=False)  # YYYY-MM
    VR_MENSAL_SISCOR = db.Column(db.Numeric(18, 2))
    QTDE_DIAS_UTEIS_MES = db.Column(db.Integer)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)


class MetasPeriodoAvaliativo(db.Model):
    __tablename__ = 'DCA_TB014_METAS_PERIODO_AVALIATIVO'
    __table_args__ = {'schema': 'DEV'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    DT_REFERENCIA = db.Column(db.Date, nullable=False)
    ID_EDITAL = db.Column(db.Integer, nullable=False)
    ID_PERIODO = db.Column(db.Integer, nullable=False)
    VR_GLOBAL_SISCOR = db.Column(db.Numeric(18, 2))
    QTDE_DIAS_UTEIS_PERIODO = db.Column(db.Integer)
    INDICE_INCREMENTO_META = db.Column(db.Numeric(5, 2))
    VR_META_A_DISTRIBUIR = db.Column(db.Numeric(18, 2))
    VR_POR_DIA_UTIL = db.Column(db.Numeric(18, 2))
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)