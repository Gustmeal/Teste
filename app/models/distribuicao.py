from datetime import datetime
from app import db
from sqlalchemy import BigInteger, Integer, Numeric, String, Date, DateTime, Text, ForeignKey
from sqlalchemy.orm import relationship


class Distribuiveis(db.Model):
    __tablename__ = 'DCA_TB006_DISTRIBUIVEIS'
    __table_args__ = {'schema': 'DEV'}

    ID = db.Column(Integer, primary_key=True, autoincrement=True)
    FkContratoSISCTR = db.Column(BigInteger, nullable=False, index=True)
    NR_CPF_CNPJ = db.Column(BigInteger, nullable=False, index=True)
    VR_SD_DEVEDOR = db.Column(Numeric(18, 2), nullable=True)
    CREATED_AT = db.Column(DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(DateTime)

    def __repr__(self):
        return f'<Distribuivel {self.ID}: Contrato {self.FkContratoSISCTR}>'


class Arrastaveis(db.Model):
    __tablename__ = 'DCA_TB007_ARRASTAVEIS'
    __table_args__ = {'schema': 'DEV'}

    ID = db.Column(Integer, primary_key=True, autoincrement=True)
    FkContratoSISCTR = db.Column(BigInteger, nullable=False, index=True)
    NR_CPF_CNPJ = db.Column(BigInteger, nullable=False, index=True)
    VR_SD_DEVEDOR = db.Column(Numeric(18, 2), nullable=True)
    CREATED_AT = db.Column(DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(DateTime)

    def __repr__(self):
        return f'<Arrastavel {self.ID}: Contrato {self.FkContratoSISCTR}>'


class Distribuicao(db.Model):
    __tablename__ = 'DCA_TB005_DISTRIBUICAO'
    __table_args__ = {'schema': 'DEV'}

    ID = db.Column(Integer, primary_key=True, autoincrement=True)
    DT_REFERENCIA = db.Column(Date, nullable=False)
    ID_EDITAL = db.Column(Integer, ForeignKey('BDG.DCA_TB008_EDITAIS.ID'), nullable=False)
    ID_PERIODO = db.Column(Integer, ForeignKey('BDG.DCA_TB001_PERIODO_AVALIACAO.ID'), nullable=False)
    fkContratoSISCTR = db.Column(BigInteger, nullable=False, index=True)
    COD_EMPRESA_COBRANCA = db.Column(Integer, nullable=False)
    COD_CRITERIO_SELECAO = db.Column(Integer, ForeignKey('DEV.DCA_TB004_CRITERIO_SELECAO.COD'), nullable=False)
    NR_CPF_CNPJ = db.Column(BigInteger, nullable=True, index=True)
    VR_SD_DEVEDOR = db.Column(Numeric(18, 2), nullable=True)
    CREATED_AT = db.Column(DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(DateTime)

    edital = relationship('Edital', backref='distribuicoes')
    periodo = relationship('PeriodoAvaliacao', backref='distribuicoes')
    criterio = relationship('CriterioSelecao', backref='distribuicoes',
                            primaryjoin="and_(Distribuicao.COD_CRITERIO_SELECAO==CriterioSelecao.COD, "
                                        "CriterioSelecao.DELETED_AT==None)")

    def __repr__(self):
        return f'<Distribuicao {self.ID}: Contrato {self.fkContratoSISCTR} - Empresa {self.COD_EMPRESA_COBRANCA}>'


class EmpresaCobrancaAtual(db.Model):
    """Modelo para a tabela COM_TB011_EMPRESA_COBRANCA_ATUAL no esquema BDG"""
    __tablename__ = 'COM_TB011_EMPRESA_COBRANCA_ATUAL'
    __table_args__ = {'schema': 'BDG'}

    # Esta tabela pode não ter uma chave primária explícita no banco
    # Usar um identificador composto
    DT_REFERENCIA = db.Column(Date, primary_key=True)
    fkContratoSISCTR = db.Column(BigInteger, primary_key=True)
    COD_EMPRESA_COBRANCA = db.Column(Integer, nullable=False)
    DT_PERIODO_INICIO = db.Column(Date, nullable=True)

    def __repr__(self):
        return f'<EmpresaCobrancaAtual: Contrato {self.fkContratoSISCTR} - Empresa {self.COD_EMPRESA_COBRANCA}>'


class AcordoLiquidadoVigente(db.Model):
    """Modelo para a tabela COM_TB009_ACORDOS_LIQUIDADOS_VIGENTES no esquema BDG"""
    __tablename__ = 'COM_TB009_ACORDOS_LIQUIDADOS_VIGENTES'
    __table_args__ = {'schema': 'BDG'}

    # Esta tabela pode não ter uma chave primária explícita no banco
    DT_REFERENCIA = db.Column(Date, primary_key=True)
    fkContratoSISCTR = db.Column(BigInteger, primary_key=True)
    pkCredito = db.Column(BigInteger, nullable=True)
    pkAcordo = db.Column(BigInteger, nullable=True)
    dtAcordo = db.Column(Date, nullable=True)
    VR_SD_DT_ACORDO = db.Column(Numeric(18, 2), nullable=True)
    VR_ACORDO = db.Column(Numeric(18, 2), nullable=True)
    dtAcordoVencimento = db.Column(Date, nullable=True)
    VR_DESCONTO = db.Column(Numeric(18, 2), nullable=True)
    VR_PAGO_ACORDO = db.Column(Numeric(18, 2), nullable=True)
    fkEstadoAcordo = db.Column(Integer, nullable=True)  # 1 = vigente
    QTDE_PARCELAS_AJUST = db.Column(Integer, nullable=True)
    DT_LIQUIDACAO = db.Column(Date, nullable=True)
    DT_ULTIMO_PAGAMENTO = db.Column(Date, nullable=True)
    VR_HONORARIO = db.Column(Numeric(18, 2), nullable=True)
    VR_DESPESA = db.Column(Numeric(18, 2), nullable=True)
    VR_SD_ATUAL = db.Column(Numeric(18, 2), nullable=True)
    qtAcordoParcela = db.Column(Integer, nullable=True)
    QTDE_PARC_PAGAS = db.Column(Integer, nullable=True)
    IND_VIA_PORTAL = db.Column(String(1), nullable=True)
    VR_PAGO_TOTAL = db.Column(Numeric(18, 2), nullable=True)
    ID_VENCIDO = db.Column(Integer, nullable=True)

    def __repr__(self):
        return f'<AcordoLiquidadoVigente: Contrato {self.fkContratoSISCTR} - Acordo {self.pkAcordo}>'


class PercentualEmpresa(db.Model):
    """Modelo para a tabela DCA_TB001_PERCENTUAL_EMPRESA"""
    __tablename__ = 'DCA_TB001_PERCENTUAL_EMPRESA'
    __table_args__ = {'schema': 'DEV'}

    ID = db.Column(Integer, primary_key=True, autoincrement=True)
    ID_EDITAL = db.Column(Integer, ForeignKey('BDG.DCA_TB008_EDITAIS.ID'), nullable=False)
    ID_PERIODO = db.Column(Integer, ForeignKey('BDG.DCA_TB001_PERIODO_AVALIACAO.ID'), nullable=False)
    COD_EMPRESA_COBRANCA = db.Column(Integer, nullable=False, index=True)
    PERCENTUAL = db.Column(Numeric(5, 2), nullable=False)
    CREATED_AT = db.Column(DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(DateTime)

    edital = relationship('Edital', backref='percentuais_empresa')
    periodo = relationship('PeriodoAvaliacao', backref='percentuais_empresa')

    def __repr__(self):
        return f'<PercentualEmpresa: Empresa {self.COD_EMPRESA_COBRANCA} - {self.PERCENTUAL}%>'


class Contrato(db.Model):
    """Modelo para a tabela COM_TB001_CONTRATO"""
    __tablename__ = 'COM_TB001_CONTRATO'
    __table_args__ = {'schema': 'BDG'}

    fkContratoSISCTR = db.Column(BigInteger, primary_key=True)
    NR_CPF_CNPJ = db.Column(BigInteger, nullable=False, index=True)
    # Adicione outros campos conforme necessário

    def __repr__(self):
        return f'<Contrato {self.fkContratoSISCTR} - CPF/CNPJ {self.NR_CPF_CNPJ}>'


class SituacaoContrato(db.Model):
    """Modelo para a tabela COM_TB007_SITUACAO_CONTRATOS"""
    __tablename__ = 'COM_TB007_SITUACAO_CONTRATOS'
    __table_args__ = {'schema': 'BDG'}

    fkContratoSISCTR = db.Column(BigInteger, primary_key=True)
    fkSituacaoCredito = db.Column(Integer, nullable=True, index=True)  # 1 = ativo
    VR_SD_DEVEDOR = db.Column(Numeric(18, 2), nullable=True)
    # Adicione outros campos conforme necessário

    def __repr__(self):
        return f'<SituacaoContrato {self.fkContratoSISCTR} - Situação {self.fkSituacaoCredito}>'


class SuspensoDecisaoJudicial(db.Model):
    """Modelo para a tabela COM_TB013_SUSPENSO_DECISAO_JUDICIAL"""
    __tablename__ = 'COM_TB013_SUSPENSO_DECISAO_JUDICIAL'
    __table_args__ = {'schema': 'BDG'}

    fkContratoSISCTR = db.Column(BigInteger, primary_key=True)
    # Adicione outros campos conforme necessário

    def __repr__(self):
        return f'<SuspensoDecisaoJudicial {self.fkContratoSISCTR}>'