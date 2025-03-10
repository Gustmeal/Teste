from datetime import datetime
from app import db


class Edital(db.Model):
    __tablename__ = 'DCA_TB008_EDITAIS'
    __table_args__ = {'schema': 'DEV'}  # Especifica o esquema

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    NU_EDITAL = db.Column(db.Integer, unique=True, nullable=False)
    ANO = db.Column(db.Integer, nullable=False)
    DESCRICAO = db.Column(db.String(100), nullable=False)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    def __repr__(self):
        return f'<Edital {self.NU_EDITAL}>'