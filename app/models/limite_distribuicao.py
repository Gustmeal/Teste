from datetime import datetime
from app import db


class LimiteDistribuicao(db.Model):
    __tablename__ = 'DCA_TB003_LIMITES_DISTRIBUICAO'
    __table_args__ = {'schema': 'BDG'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ID_EDITAL = db.Column(db.Integer, db.ForeignKey('BDG.DCA_TB008_EDITAIS.ID'), nullable=False)
    ID_PERIODO = db.Column(db.Integer, db.ForeignKey('BDG.DCA_TB001_PERIODO_AVALIACAO.ID'), nullable=False)
    ID_EMPRESA = db.Column(db.Integer, nullable=False)
    COD_CRITERIO_SELECAO = db.Column(db.Integer, nullable=False)
    QTDE_MAXIMA = db.Column(db.Integer, nullable=True)
    VALOR_MAXIMO = db.Column(db.Numeric(18, 2), nullable=True)
    PERCENTUAL_FINAL = db.Column(db.Numeric(5, 2), nullable=True)
    DT_APURACAO = db.Column(db.Date, nullable=True)  # <-- Adiciona aqui
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    edital = db.relationship('Edital', backref='limites_distribuicao')
    periodo = db.relationship('PeriodoAvaliacao', backref='limites_distribuicao')

    empresa_nome = None
    empresa_nome_abreviado = None

    def __repr__(self):
        return f'<LimiteDistribuicao {self.ID}>'
