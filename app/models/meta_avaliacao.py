# app/models/meta_avaliacao.py
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

    @classmethod
    def calcular_metas_periodo(cls, edital_id, periodo_id):
        """Calcula metas para todas as empresas de um período"""
        from app.utils.meta_calculator import MetaCalculator

        calculator = MetaCalculator(edital_id, periodo_id)
        return calculator.calcular_todas_metas()

    def to_dict(self):
        """Converte a meta para dicionário"""
        return {
            'id': self.ID,
            'edital_id': self.ID_EDITAL,
            'periodo_id': self.ID_PERIODO,
            'empresa_id': self.ID_EMPRESA,
            'competencia': self.COMPETENCIA,
            'meta_arrecadacao': float(self.META_ARRECADACAO) if self.META_ARRECADACAO else 0,
            'meta_acionamento': float(self.META_ACIONAMENTO) if self.META_ACIONAMENTO else 0,
            'meta_liquidacao': int(self.META_LIQUIDACAO) if self.META_LIQUIDACAO else 0,
            'meta_bonificacao': float(self.META_BONIFICACAO) if self.META_BONIFICACAO else 0
        }

    def __repr__(self):
        return f'<MetaAvaliacao {self.ID} - Competência: {self.COMPETENCIA}>'