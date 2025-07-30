from datetime import datetime
from app import db
from sqlalchemy import Column, Integer, String, Numeric, Date, Text


class DemonstrativoSucorMeses(db.Model):
    """Modelo para a tabela de demonstrativos SUCOR por mês"""
    __tablename__ = 'COR_DEM_TB005_DEMONSTRATIVOS_MESES'
    __table_args__ = {'schema': 'BDG'}

    CO_DEMONSTRATIVO = Column(Integer, primary_key=True)
    NO_DEMONSTRATIVO = Column(String(200))
    ORDEM = Column(Integer)
    GRUPO = Column(String(200))
    ANO = Column(Integer)
    JAN = Column(Numeric(18, 2))
    FEV = Column(Numeric(18, 2))
    MAR = Column(Numeric(18, 2))
    ABR = Column(Numeric(18, 2))
    MAI = Column(Numeric(18, 2))
    JUN = Column(Numeric(18, 2))
    JUL = Column(Numeric(18, 2))
    AGO = Column(Numeric(18, 2))
    SET = Column(Numeric(18, 2))
    OUT = Column(Numeric(18, 2))
    NOV = Column(Numeric(18, 2))
    DEZ = Column(Numeric(18, 2))

    def get_valor_mes(self, mes):
        """Retorna o valor do mês especificado"""
        meses_map = {
            1: self.JAN, 2: self.FEV, 3: self.MAR, 4: self.ABR,
            5: self.MAI, 6: self.JUN, 7: self.JUL, 8: self.AGO,
            9: self.SET, 10: self.OUT, 11: self.NOV, 12: self.DEZ
        }
        return meses_map.get(mes, 0) or 0

    def __repr__(self):
        return f'<DemonstrativoSucorMeses {self.NO_DEMONSTRATIVO} - {self.ANO}>'


class JustificativaSucor(db.Model):
    """Modelo para a tabela de justificativas SUCOR"""
    __tablename__ = 'COR_DEM_TB006_JUSTIFICATIVAS'
    __table_args__ = {'schema': 'BDG'}

    DT_REFERENCIA = Column(Date, primary_key=True)
    CO_DEMONSTRATIVO = Column(Integer, primary_key=True)
    ORDEM = Column(Integer, primary_key=True)
    DESCRICAO = Column(Text)
    VARIACAO_DEM = Column(Numeric(18, 2))

    def __repr__(self):
        return f'<JustificativaSucor {self.CO_DEMONSTRATIVO} - {self.ORDEM}>'