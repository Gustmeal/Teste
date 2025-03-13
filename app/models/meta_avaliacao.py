# app/models/meta_avaliacao.py (atualizado)
from datetime import datetime
from app import db


class MetaAvaliacao(db.Model):
    __tablename__ = 'DCA_TB009_META_AVALIACAO'
    __table_args__ = {'schema': 'DEV'}  # Especifica o esquema

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ID_EDITAL = db.Column(db.Integer, db.ForeignKey('DEV.DCA_TB008_EDITAIS.ID'), nullable=False)
    ID_PERIODO = db.Column(db.Integer, db.ForeignKey('DEV.DCA_TB001_PERIODO_AVALIACAO.ID'), nullable=False)
    ID_EMPRESA = db.Column(db.Integer, nullable=False)  # Referência para o ID_EMPRESA, não o ID
    COMPETENCIA = db.Column(db.String(7), nullable=False)
    META_ARRECADACAO = db.Column(db.Numeric(18, 2), nullable=True)
    META_ACIONAMENTO = db.Column(db.Numeric(18, 2), nullable=True)
    META_LIQUIDACAO = db.Column(db.Numeric(18, 2), nullable=True)
    META_BONIFICACAO = db.Column(db.Numeric(18, 2), nullable=True)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    # Relacionamentos
    edital = db.relationship('Edital', backref='metas_avaliacao')
    periodo = db.relationship('PeriodoAvaliacao', backref='metas_avaliacao')
    # Removido relacionamento direto com a Empresa porque estamos usando ID_EMPRESA

    # Atributos adicionais para uso temporário
    empresa_nome = None
    empresa_nome_abreviado = None

    def __repr__(self):
        return f'<MetaAvaliacao {self.ID} - Competência: {self.COMPETENCIA}>'