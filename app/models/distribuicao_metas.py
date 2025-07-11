# app/models/distribuicao_metas.py
from datetime import datetime
from app import db


class DistribuicaoSumario(db.Model):
    """Tabela resumo da distribuição - uma linha por empresa"""
    __tablename__ = 'DCA_TB017_DISTRIBUICAO_SUMARIO'
    __table_args__ = {'schema': 'DEV'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ID_EDITAL = db.Column(db.Integer, nullable=False)
    ID_PERIODO = db.Column(db.Integer, nullable=False)
    DT_REFERENCIA = db.Column(db.Date, nullable=False)
    ID_EMPRESA = db.Column(db.Integer, nullable=False)
    NO_EMPRESA_ABREVIADA = db.Column(db.String(100))
    VR_SALDO_DEVEDOR_DISTRIBUIDO = db.Column(db.Numeric(18, 2))
    PERCENTUAL_SALDO_DEVEDOR = db.Column(db.Numeric(18, 8))
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    # Relacionamento com os detalhes mensais
    detalhes_mensais = db.relationship('DistribuicaoMensal', backref='sumario', lazy='dynamic')

    def __repr__(self):
        return f'<DistribuicaoSumario {self.ID} - Empresa: {self.NO_EMPRESA_ABREVIADA}>'


class DistribuicaoMensal(db.Model):
    """Tabela de detalhes mensais da distribuição"""
    __tablename__ = 'DCA_TB019_DISTRIBUICAO_MENSAL'
    __table_args__ = {'schema': 'DEV'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ID_DISTRIBUICAO_SUMARIO = db.Column(db.Integer, db.ForeignKey('BDG.DCA_TB017_DISTRIBUICAO_SUMARIO.ID'), nullable=False)
    MES_COMPETENCIA = db.Column(db.String(7), nullable=False)  # Formato YYYY-MM
    VR_META_MES = db.Column(db.Numeric(18, 2))
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    def __repr__(self):
        return f'<DistribuicaoMensal {self.ID} - Competência: {self.MES_COMPETENCIA}>'