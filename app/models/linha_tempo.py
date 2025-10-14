from datetime import datetime
from app import db


class LinhaTempo(db.Model):
    """Modelo para a tabela COM_TB076_LINHA_DO_TEMPO"""
    __tablename__ = 'COM_TB076_LINHA_DO_TEMPO'
    __table_args__ = {'schema': 'BDG'}

    ANO = db.Column(db.Integer, primary_key=True, nullable=False)
    ITEM = db.Column(db.Integer, primary_key=True, nullable=False)
    DSC_EVENTO = db.Column(db.String(300), nullable=False)

    def __repr__(self):
        return f'<LinhaTempo {self.ANO} - Item {self.ITEM}>'

    @staticmethod
    def obter_proximo_item(ano):
        """
        Retorna o próximo número de ITEM para um determinado ano.
        Se não houver eventos no ano, retorna 1.
        """
        ultimo_item = db.session.query(db.func.max(LinhaTempo.ITEM)) \
            .filter(LinhaTempo.ANO == ano) \
            .scalar()

        return (ultimo_item + 1) if ultimo_item else 1