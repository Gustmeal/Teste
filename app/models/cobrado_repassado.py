from datetime import date
from app import db
from sqlalchemy import BigInteger, Integer, String, Date, Text


class PenRelacionaVlrRepassado(db.Model):
    """Modelo para a tabela PEN_TB011_RELACIONA_VLR_REPASSADO"""
    __tablename__ = 'PEN_TB011_RELACIONA_VLR_REPASSADO'
    __table_args__ = (
        {'schema': 'BDG'}
    )
    ID_PENDENCIA = db.Column(db.Integer, primary_key=True, nullable=False)
    ID_ARREC_EXT_SISTEMA = db.Column(db.BigInteger, nullable=True)
    OBS = db.Column(db.Text, nullable=True)
    NO_RSPONSAVEL = db.Column(db.String, nullable=True)
    DT_ANALISE = db.Column(db.Date, nullable=True)

    def __repr__(self):
        return f'<PenRelacionaVlrRepassado Pendencia: {self.ID_PENDENCIA} - Arrec: {self.ID_ARREC_EXT_SISTEMA}>'