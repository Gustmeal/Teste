from datetime import datetime
from app import db


class IndicadorFormula(db.Model):
    """Modelo para a tabela de fórmulas de indicadores"""
    __tablename__ = 'IND_TB002_INDICADORES_FORMULA_CALCULO'
    __table_args__ = {'schema': 'BDG'}

    # Como não tem PK definida, vamos usar uma chave composta
    DT_REFERENCIA = db.Column(db.Date, primary_key=True)
    INDICADOR = db.Column(db.String(50), primary_key=True)
    VARIAVEL = db.Column(db.Integer, primary_key=True)
    NO_VARIAVEL = db.Column(db.String(255))
    FONTE = db.Column(db.String(255))
    VR_VARIAVEL = db.Column(db.Numeric(18, 2))
    RESPONSAVEL_INCLUSAO = db.Column(db.String(100))

    def __repr__(self):
        return f'<IndicadorFormula {self.INDICADOR} - {self.VARIAVEL}>'


class CodigoIndicador(db.Model):
    """Modelo para a tabela de códigos de indicadores"""
    __tablename__ = 'IND_TB003_COD_INDICADORES'
    __table_args__ = {'schema': 'BDG'}

    CO_INDICADOR = db.Column(db.Integer, primary_key=True)
    SG_INDICADOR = db.Column(db.String(50))
    DSC_INDICADOR = db.Column(db.String(255))
    QTDE_VARIAVEIS = db.Column(db.Integer)

    def __repr__(self):
        return f'<CodigoIndicador {self.SG_INDICADOR}>'


class VariavelIndicador(db.Model):
    """Modelo para a tabela de variáveis dos indicadores"""
    __tablename__ = 'IND_TB004_VARIAVEIS'
    __table_args__ = {'schema': 'BDG'}

    CO_INDICADOR = db.Column(db.Integer, primary_key=True)
    VARIAVEL = db.Column(db.Integer, primary_key=True)
    NO_VARIAVEL = db.Column(db.String(255))
    FONTE = db.Column(db.String(255))

    def __repr__(self):
        return f'<VariavelIndicador {self.CO_INDICADOR} - {self.VARIAVEL}>'