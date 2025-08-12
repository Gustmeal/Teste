from datetime import datetime
from app import db
from sqlalchemy import BigInteger, Integer, Numeric, String, Date, DateTime, Text, Boolean


class PenDetalhamento(db.Model):
    """Modelo para a tabela PEN_TB004_DETALHAMENTO"""
    __tablename__ = 'PEN_TB004_DETALHAMENTO'
    __table_args__ = {'schema': 'BDG'}

    ID_DETALHAMENTO = db.Column(db.Integer, primary_key=True)
    NU_CONTRATO = db.Column(db.Numeric(23, 0), nullable=False, index=True)  # CORRIGIDO: decimal
    ID_CARTEIRA = db.Column(db.Integer, nullable=True)
    ID_OCORRENCIA = db.Column(db.Integer, nullable=True)
    NU_PROCESSO = db.Column(db.Text, nullable=True)
    VR_FALHA = db.Column(db.Numeric(18, 2), nullable=True)
    ID_STATUS = db.Column(db.Integer, nullable=True)
    NU_OFICIO = db.Column(db.Integer, nullable=True)
    IC_CONDENACAO = db.Column(db.Boolean, nullable=True)
    IC_INDICIO_DUPLIC = db.Column(db.Boolean, nullable=True)
    IC_EXCLUIR = db.Column(db.Boolean, nullable=True)
    VR_REAL_FALHA = db.Column(db.Numeric(18, 2), nullable=True)
    DT_PAGTO = db.Column(db.Date, nullable=True)
    DT_ACERTO_PENDENCIA = db.Column(db.Date, nullable=True)
    Data_Ultima_Atualizacao = db.Column(db.Date, nullable=True)
    OBSERVACAO_DT_PGTO = db.Column(db.String(300), nullable=True)
    DT_INICIO_ATUALIZACAO = db.Column(db.Date, nullable=True)

    def __repr__(self):
        return f'<PenDetalhamento {self.ID_DETALHAMENTO} - Contrato: {self.NU_CONTRATO}>'


class AexAnalitico(db.Model):
    """Modelo para a tabela AEX_TB002_ANALITICO"""
    __tablename__ = 'AEX_TB002_ANALITICO'
    __table_args__ = {'schema': 'BDG'}

    ID = db.Column(db.BigInteger, primary_key=True)
    DT_REPASSE = db.Column(db.Date, nullable=True)
    DT_REMESSA = db.Column(db.Date, nullable=True)
    CO_UNIDADE_MOVIMENTO = db.Column(db.Integer, nullable=True)
    DT_EVENTO = db.Column(db.Date, nullable=True)
    DT_RESGATE = db.Column(db.Date, nullable=True)
    NU_CONTRATO = db.Column(db.String(30), nullable=True, index=True)  # CORRIGIDO: varchar
    CO_TIPO_PEDIDO = db.Column(db.Integer, nullable=True)
    CO_NCPD = db.Column(db.String(7), nullable=True)
    VALOR = db.Column(db.Numeric(18, 2), nullable=True)
    CO_ORIGEM_MOVIMENTO = db.Column(db.Integer, nullable=True)  # tinyint
    CO_REPASSE = db.Column(db.Integer, nullable=True)
    CO_DAMP = db.Column(db.String(10), nullable=True)
    NU_MOVIMENTO = db.Column(db.Integer, nullable=True)
    CO_TIPO_PROCESSO = db.Column(db.Integer, nullable=True)  # tinyint
    DS_TIPO_PROCESSO = db.Column(db.String(100), nullable=True)
    CO_TIPO_DESPESA = db.Column(db.Integer, nullable=True)  # smallint
    DS_TIPO_DESPESA = db.Column(db.String(100), nullable=True)
    CODIGO = db.Column(db.String(10), nullable=True)
    CO_UNIDADE = db.Column(db.Integer, nullable=True)  # smallint
    SG_UNIDADE = db.Column(db.String(10), nullable=True)
    CO_TIPO_REGISTRO = db.Column(db.String(1), nullable=True)
    DS_JUSTIFICATIVA = db.Column(db.Text, nullable=True)  # nvarchar
    NO_ARQUIVO = db.Column(db.String(30), nullable=True)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime, nullable=True)
    fkContratoSISCTR = db.Column(db.BigInteger, nullable=True)
    CARTEIRA = db.Column(db.String(50), nullable=True)
    VR_APROPRIADO = db.Column(db.Numeric(18, 2), nullable=True)
    OBSERVACAO = db.Column(db.String(50), nullable=True)

    def __repr__(self):
        return f'<AexAnalitico {self.ID} - Contrato: {self.NU_CONTRATO}>'


class PenRelacionaVlrRetido(db.Model):
    """Modelo para a tabela PEN_TB010_RELACIONA_VLR_RETIDO"""
    __tablename__ = 'PEN_TB010_RELACIONA_VLR_RETIDO'
    __table_args__ = {'schema': 'BDG'}

    ID_PENDENCIA = db.Column(db.Integer, primary_key=True, nullable=True)  # Permitir NULL
    ID_ARREC_EXT_SISTEMA = db.Column(db.BigInteger, nullable=True)
    OBS = db.Column(db.Text, nullable=True)
    NO_RSPONSAVEL = db.Column(db.String(100), nullable=True)
    DT_ANALISE = db.Column(db.DateTime, nullable=True)

    def __repr__(self):
        return f'<PenRelacionaVlrRetido Pendencia: {self.ID_PENDENCIA} - Arrec: {self.ID_ARREC_EXT_SISTEMA}>'


class PenCarteiras(db.Model):
    """Modelo para a tabela PEN_TB007_CARTEIRAS"""
    __tablename__ = 'PEN_TB007_CARTEIRAS'
    __table_args__ = {'schema': 'BDG'}

    ID_CARTEIRA = db.Column(db.Integer, primary_key=True)
    DSC_CARTEIRA = db.Column(db.String(255), nullable=True)

    def __repr__(self):
        return f'<PenCarteiras {self.ID_CARTEIRA} - {self.DSC_CARTEIRA}>'


class PenOcorrencias(db.Model):
    """Modelo para a tabela PEN_TB001_OCORRENCIAS"""
    __tablename__ = 'PEN_TB001_OCORRENCIAS'
    __table_args__ = {'schema': 'BDG'}

    ID_OCORRENCIA = db.Column(db.Integer, primary_key=True)
    DSC_OCORRENCIA = db.Column(db.String(255), nullable=True)

    def __repr__(self):
        return f'<PenOcorrencias {self.ID_OCORRENCIA} - {self.DSC_OCORRENCIA}>'


class AexConsolidado(db.Model):
    """Modelo para a tabela AEX_TB006_CONSOLIDADO"""
    __tablename__ = 'AEX_TB006_CONSOLIDADO'
    __table_args__ = {'schema': 'BDG'}

    ANO_REPASSE = db.Column(db.Integer, primary_key=True)
    CARTEIRA = db.Column(db.String(100), primary_key=True)
    DSC_TIPO_PEDIDO = db.Column(db.String(255), nullable=True)
    OBSERVACAO = db.Column(db.Text, nullable=True)
    VR_RETIDO_CAIXA = db.Column(db.Numeric(18, 2), nullable=True)
    QTDE_REGISTROS = db.Column(db.Integer, nullable=True)
    QTDE_CONTRATO = db.Column(db.Integer, nullable=True)
    VALOR = db.Column(db.Numeric(18, 2), nullable=True)

    def __repr__(self):
        return f'<AexConsolidado {self.ANO_REPASSE} - {self.CARTEIRA}>'

class PenStatusOcorrencia(db.Model):
    """Modelo para a tabela PEN_TB003_STATUS_OCORRENCIA"""
    __tablename__ = 'PEN_TB003_STATUS_OCORRENCIA'
    __table_args__ = {'schema': 'BDG'}

    ID_STATUS = db.Column(db.Integer, primary_key=True)
    DSC_STATUS = db.Column(db.String(255), nullable=True)

    def __repr__(self):
        return f'<PenStatusOcorrencia {self.ID_STATUS} - {self.DSC_STATUS}>'


from datetime import datetime


class PenOficios(db.Model):
    """Modelo para a tabela PEN_TB002_OFICIOS"""
    __tablename__ = 'PEN_TB002_OFICIOS'
    __table_args__ = {'schema': 'BDG'}

    NU_OFICIO = db.Column(db.Integer, primary_key=True)
    DT_OFICIO = db.Column(db.String(8), nullable=True)  # Vem como string YYYYMMDD
    VIGENCIA_CTR_CAIXA = db.Column(db.String(50), nullable=True)

    @property
    def dt_oficio_formatada(self):
        """Converte a string YYYYMMDD para objeto date"""
        if self.DT_OFICIO and len(self.DT_OFICIO) == 8:
            try:
                return datetime.strptime(self.DT_OFICIO, '%Y%m%d').date()
            except:
                return None
        return None

    def __repr__(self):
        return f'<PenOficios {self.NU_OFICIO} - {self.DT_OFICIO}>'