from app import db
from datetime import datetime


class DepositosSufin(db.Model):
    """Modelo para a tabela de Depósitos Judiciais SUFIN"""
    __tablename__ = 'DPJ_TB004_DEPOSITOS_SUFIN'
    __table_args__ = {'schema': 'DEV'}

    NU_LINHA = db.Column(db.Integer, primary_key=True, autoincrement=False)
    LANCAMENTO_RM = db.Column(db.String(15))
    DT_LANCAMENTO_DJ = db.Column(db.Date)
    VR_RATEIO = db.Column(db.Numeric(18, 2))
    MEMO_SUFIN = db.Column(db.String(15))
    DT_MEMO = db.Column(db.Date)
    ID_IDENTIFICADO = db.Column(db.Boolean)
    DT_IDENTIFICACAO = db.Column(db.Date)
    ID_AREA = db.Column(db.Integer, db.ForeignKey('dbo.DPJ_TB001_AREA.ID_AREA'))
    ID_AREA_2 = db.Column(db.Integer)
    ID_CENTRO = db.Column(db.Integer, db.ForeignKey('dbo.DPJ_TB002_CENTRO_RESULTADO.ID_CENTRO'))
    ID_AJUSTE_RM = db.Column(db.Boolean)
    DT_AJUSTE_RM = db.Column(db.Date)
    NU_CONTRATO = db.Column(db.BigInteger)
    NU_CONTRATO_2 = db.Column(db.BigInteger)
    EVENTO_CONTABIL_ANTERIOR = db.Column(db.Integer)
    EVENTO_CONTABIL_ATUAL = db.Column(db.Integer)
    OBS = db.Column(db.String(70))
    IC_APROPRIADO = db.Column(db.Boolean)
    DT_SISCOR = db.Column(db.Date)
    IC_INCLUIDO_ACERTO = db.Column(db.Boolean)

    # Relacionamentos
    area = db.relationship('Area', backref='depositos', foreign_keys=[ID_AREA])
    centro = db.relationship('CentroResultado', backref='depositos', foreign_keys=[ID_CENTRO])


class Area(db.Model):
    """Modelo para a tabela de Áreas"""
    __tablename__ = 'DPJ_TB001_AREA'
    __table_args__ = {'schema': 'dbo'}

    ID_AREA = db.Column(db.Integer, primary_key=True)
    NO_AREA = db.Column(db.String(100))


class CentroResultado(db.Model):
    """Modelo para a tabela de Centro de Resultado"""
    __tablename__ = 'DPJ_TB002_CENTRO_RESULTADO'
    __table_args__ = {'schema': 'dbo'}

    ID_CENTRO = db.Column(db.Integer, primary_key=True)
    NO_CENTRO_RESULTADO = db.Column(db.String(100))
    NO_CARTEIRA = db.Column(db.String(100))


class ProcessosJudiciais(db.Model):
    """Modelo para a tabela de Processos Judiciais"""
    __tablename__ = 'DPJ_TB006_PROCESSOS_JUDICIAIS'
    __table_args__ = {'schema': 'dbo'}

    NU_LINHA = db.Column(db.Integer, primary_key=True, autoincrement=False)
    NR_PROCESSO = db.Column(db.String(100))