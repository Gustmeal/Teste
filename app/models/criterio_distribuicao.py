from datetime import datetime
from app import db


class CriterioDistribuicao(db.Model):
    __tablename__ = 'DCA_TB004_CRITERIO_SELECAO'
    __table_args__ = {'schema': 'DEV'}  # Especifica o esquema

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    COD = db.Column(db.Integer, nullable=False)
    DS_CRITERIO_SELECAO = db.Column(db.String(100), nullable=False)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    def __repr__(self):
        return f'<CriterioDistribuicao {self.COD} - {self.DS_CRITERIO_SELECAO}>'