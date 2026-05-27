# app/models/titulo_cvs.py
from app import db


class ResumoCVS(db.Model):
    """
    Modelo para a tabela BDG.FIN_TB006_RESUMO_CVS.

    Tabela de Resumo de Títulos CVS (Crédito Securitizado).
    Os dados vêm de planilhas Excel enviadas pelo usuário, cujo nome
    segue o padrão:
        AAAA-MM-DD_Planilhas Resumo CVS - Contrato NNN.xlsx

    Onde:
      - AAAA-MM-DD     → DT_ATUALIZACAO (extraída do nome do arquivo)
      - NNN            → NU_CONTRATO    (extraído do nome do arquivo)

    Chave primária composta:
      (DT_ATUALIZACAO, NU_CONTRATO, ATIVO)

    Motivo: cada planilha pode trazer mais de uma linha de ativo
    (ex: CVSA970101 e CVSB970101) para o mesmo contrato e mesma data.
    """
    __tablename__ = 'FIN_TB007_RESUMO_CVS'
    __table_args__ = {'schema': 'BDG'}

    DT_ATUALIZACAO = db.Column(db.Date, primary_key=True, nullable=False)
    NU_CONTRATO = db.Column(db.BigInteger, primary_key=True, nullable=False)
    ATIVO = db.Column(db.String(20), primary_key=True, nullable=False)

    DT_CARGA = db.Column(db.Date, nullable=False)
    QTDE = db.Column(db.Integer, nullable=True)
    VNA = db.Column(db.Numeric(18, 8), nullable=True)
    FINANCEIRO = db.Column(db.Numeric(18, 2), nullable=True)
    PU_RETROATIVO_JUROS = db.Column(db.Numeric(18, 10), nullable=True)
    FINANCEIRO_JUROS = db.Column(db.Numeric(18, 2), nullable=True)
    PU_RETROATIVO_PRINC = db.Column(db.Numeric(18, 10), nullable=True)
    FINANCEIRO_PRINC = db.Column(db.Numeric(18, 2), nullable=True)
    FINANCEIRO_VENC_PAGAR = db.Column(db.Numeric(18, 2), nullable=True)
    TOTAL = db.Column(db.Numeric(18, 2), nullable=True)

    EVENTO = db.Column(db.String(1), nullable=False, default='E')

    def __repr__(self):
        return (f'<ResumoCVS Contrato {self.NU_CONTRATO} - '
                f'{self.ATIVO} - {self.DT_ATUALIZACAO}>')

    @staticmethod
    def listar_todos():
        """Lista todos os registros ordenados por DT_ATUALIZACAO desc."""
        return ResumoCVS.query.order_by(
            ResumoCVS.DT_ATUALIZACAO.desc(),
            ResumoCVS.NU_CONTRATO.asc(),
            ResumoCVS.ATIVO.asc()
        ).all()

    @staticmethod
    def listar_por_data_atualizacao(dt_atualizacao):
        """Lista registros de uma DT_ATUALIZACAO específica."""
        return ResumoCVS.query.filter_by(
            DT_ATUALIZACAO=dt_atualizacao
        ).order_by(
            ResumoCVS.NU_CONTRATO.asc(),
            ResumoCVS.ATIVO.asc()
        ).all()

    @staticmethod
    def listar_datas_atualizacao_distintas():
        """Lista todas as DT_ATUALIZACAO distintas (desc)."""
        rows = db.session.query(
            ResumoCVS.DT_ATUALIZACAO
        ).distinct().order_by(
            ResumoCVS.DT_ATUALIZACAO.desc()
        ).all()
        return [r[0] for r in rows if r[0] is not None]

    @staticmethod
    def contar_registros():
        """Total de registros na tabela."""
        return ResumoCVS.query.count()

    @staticmethod
    def contar_contratos_distintos():
        """Total de contratos distintos na tabela."""
        return db.session.query(
            ResumoCVS.NU_CONTRATO
        ).distinct().count()