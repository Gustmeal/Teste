from datetime import datetime
from app import db


class ContratoDistribuivel(db.Model):
    __tablename__ = 'DCA_TB006_DISTRIBUIVEIS'
    __table_args__ = {'schema': 'DEV'}  # Especifica o esquema

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    FkContratoSISCTR = db.Column(db.BigInteger, nullable=False)
    NR_CPF_CNPJ = db.Column(db.BigInteger, nullable=True)
    VR_SD_DEVEDOR = db.Column(db.Numeric(18, 2), nullable=True)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    def __repr__(self):
        return f'<ContratoDistribuivel {self.FkContratoSISCTR}>'


class ContratoArrastavel(db.Model):
    __tablename__ = 'DCA_TB007_ARRASTAVEIS'
    __table_args__ = {'schema': 'DEV'}  # Especifica o esquema

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    FkContratoSISCTR = db.Column(db.BigInteger, nullable=False)
    NR_CPF_CNPJ = db.Column(db.BigInteger, nullable=True)
    VR_SD_DEVEDOR = db.Column(db.Numeric(18, 2), nullable=True)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    def __repr__(self):
        return f'<ContratoArrastavel {self.FkContratoSISCTR}>'


class Distribuicao(db.Model):
    __tablename__ = 'DCA_TB005_DISTRIBUICAO'
    __table_args__ = {'schema': 'DEV'}  # Especifica o esquema

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    DT_REFERENCIA = db.Column(db.DateTime, nullable=False)
    ID_EDITAL = db.Column(db.Integer, db.ForeignKey('DEV.DCA_TB008_EDITAIS.ID'), nullable=False)
    ID_PERIODO = db.Column(db.Integer, db.ForeignKey('DEV.DCA_TB001_PERIODO_AVALIACAO.ID'), nullable=False)
    FkContratoSISCTR = db.Column(db.BigInteger, nullable=False)
    COD_CRITERIO_SELECAO = db.Column(db.Integer, nullable=False)
    COD_EMPRESA_COBRANCA = db.Column(db.Integer, nullable=False)
    NR_CPF_CNPJ = db.Column(db.BigInteger, nullable=True)
    VR_SD_DEVEDOR = db.Column(db.Numeric(18, 2), nullable=True)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    # Relacionamentos
    edital = db.relationship('Edital', backref='distribuicoes')
    periodo = db.relationship('PeriodoAvaliacao', backref='distribuicoes')

    def __repr__(self):
        return f'<Distribuicao {self.ID} - Contrato: {self.FkContratoSISCTR} - Empresa: {self.COD_EMPRESA_COBRANCA}>'