# app/models/titulo_cvs.py
from app import db


class ResumoCVS(db.Model):
    """
    Modelo para a tabela BDG.FIN_TB007_RESUMO_CVS.

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


# =========================================================================
# Origem / Destino dos ativos CVS
# =========================================================================
class OrigemDestinoCVS(db.Model):
    """
    Modelo para a tabela BDG.FIN_TB008_ORIGEM_DESTINO_CVS.

    Estrutura real (conforme SSMS):
      - DT_CARGA        date         NOT NULL
      - DT_ATUALIZACAO  date         NOT NULL    *chave (PK)
      - ATIVO           varchar(1)   NOT NULL    *chave (PK)
      - ORIGEM_DESTINO  varchar(150) NULL
      - EVENTO          char(1)      NOT NULL    *chave (PK)

    Esta tabela é pré-populada por um processo externo (SQL Job ou
    similar) com os dados de DT_CARGA, DT_ATUALIZACAO, ATIVO e EVENTO.
    A coluna ORIGEM_DESTINO normalmente vem NULL e precisa ser
    preenchida manualmente pelo usuário através do portal.

    Chave primária composta:
      (DT_ATUALIZACAO, ATIVO, EVENTO)
    """
    __tablename__ = 'FIN_TB008_ORIGEM_DESTINO_CVS'
    __table_args__ = {'schema': 'BDG'}

    DT_ATUALIZACAO = db.Column(db.Date, primary_key=True, nullable=False)
    ATIVO = db.Column(db.String(1), primary_key=True, nullable=False)
    EVENTO = db.Column(db.String(1), primary_key=True, nullable=False)

    DT_CARGA = db.Column(db.Date, nullable=False)
    ORIGEM_DESTINO = db.Column(db.String(150), nullable=True)

    def __repr__(self):
        return (f'<OrigemDestinoCVS {self.ATIVO}/{self.EVENTO} - '
                f'{self.DT_ATUALIZACAO} - {self.ORIGEM_DESTINO}>')

    # -------------------------------------------------------------------
    # CONSULTAS
    # -------------------------------------------------------------------
    @staticmethod
    def listar_datas_atualizacao_distintas():
        """Lista todas as DT_ATUALIZACAO distintas (desc)."""
        rows = db.session.query(
            OrigemDestinoCVS.DT_ATUALIZACAO
        ).distinct().order_by(
            OrigemDestinoCVS.DT_ATUALIZACAO.desc()
        ).all()
        return [r[0] for r in rows if r[0] is not None]

    @staticmethod
    def listar_por_data(dt_atualizacao):
        """Lista todos os registros de uma DT_ATUALIZACAO específica."""
        return OrigemDestinoCVS.query.filter_by(
            DT_ATUALIZACAO=dt_atualizacao
        ).order_by(
            OrigemDestinoCVS.ATIVO.asc(),
            OrigemDestinoCVS.EVENTO.asc()
        ).all()

    @staticmethod
    def listar_pendentes(dt_atualizacao):
        """Lista registros sem ORIGEM_DESTINO preenchido."""
        return OrigemDestinoCVS.query.filter(
            OrigemDestinoCVS.DT_ATUALIZACAO == dt_atualizacao,
            db.or_(
                OrigemDestinoCVS.ORIGEM_DESTINO.is_(None),
                OrigemDestinoCVS.ORIGEM_DESTINO == ''
            )
        ).order_by(
            OrigemDestinoCVS.ATIVO.asc(),
            OrigemDestinoCVS.EVENTO.asc()
        ).all()

    @staticmethod
    def listar_preenchidos(dt_atualizacao):
        """Lista registros COM ORIGEM_DESTINO preenchido."""
        return OrigemDestinoCVS.query.filter(
            OrigemDestinoCVS.DT_ATUALIZACAO == dt_atualizacao,
            OrigemDestinoCVS.ORIGEM_DESTINO.isnot(None),
            OrigemDestinoCVS.ORIGEM_DESTINO != ''
        ).order_by(
            OrigemDestinoCVS.ATIVO.asc(),
            OrigemDestinoCVS.EVENTO.asc()
        ).all()

    @staticmethod
    def obter(dt_atualizacao, ativo, evento):
        """Obtém um registro específico pela PK composta."""
        return OrigemDestinoCVS.query.filter_by(
            DT_ATUALIZACAO=dt_atualizacao,
            ATIVO=ativo,
            EVENTO=evento
        ).first()

    @staticmethod
    def contar_por_data(dt_atualizacao):
        """
        Retorna (total, pendentes, preenchidos) para uma DT_ATUALIZACAO.
        """
        total = OrigemDestinoCVS.query.filter_by(
            DT_ATUALIZACAO=dt_atualizacao
        ).count()

        pendentes = OrigemDestinoCVS.query.filter(
            OrigemDestinoCVS.DT_ATUALIZACAO == dt_atualizacao,
            db.or_(
                OrigemDestinoCVS.ORIGEM_DESTINO.is_(None),
                OrigemDestinoCVS.ORIGEM_DESTINO == ''
            )
        ).count()

        preenchidos = total - pendentes
        return total, pendentes, preenchidos

# =========================================================================
# Extrato CVS
# =========================================================================
class ExtratoCVS(db.Model):
    """
    Modelo para a tabela BDG.FIN_TB013_EXTRATO_CVS.

    Estrutura real (conforme SSMS):
      - NU_LINHA         int          NOT NULL   IDENTITY (gerado pelo banco)
      - DT_CARGA         date         NOT NULL
      - DT_MOVIMENTACAO  date         NOT NULL   *chave (PK)
      - TIPO             varchar(3)   NULL
      - ORDEM            smallint     NOT NULL   *chave (PK)
      - HISTORICO        varchar(150) NULL
      - PERIODO_DE       date         NULL
      - PERIODO_ATE      date         NULL
      - VR_MOVIMENTACAO  decimal(18,2) NULL
      - VR_SALDO         decimal(18,2) NULL

    Chave primária composta: (DT_MOVIMENTACAO, ORDEM)

    OBS: NU_LINHA é IDENTITY no SQL Server e propositalmente NÃO está
    mapeada no modelo. Assim o SQLAlchemy nunca envia valor para essa
    coluna no INSERT, e o banco gera automaticamente.
    """
    __tablename__ = 'FIN_TB013_EXTRATO_CVS'
    __table_args__ = {'schema': 'BDG'}

    DT_MOVIMENTACAO = db.Column(db.Date, primary_key=True, nullable=False)
    ORDEM = db.Column(db.SmallInteger, primary_key=True, nullable=False)

    DT_CARGA = db.Column(db.Date, nullable=False)
    TIPO = db.Column(db.String(3), nullable=True)
    HISTORICO = db.Column(db.String(150), nullable=True)
    PERIODO_DE = db.Column(db.Date, nullable=True)
    PERIODO_ATE = db.Column(db.Date, nullable=True)
    VR_MOVIMENTACAO = db.Column(db.Numeric(18, 2), nullable=True)
    VR_SALDO = db.Column(db.Numeric(18, 2), nullable=True)

    def __repr__(self):
        return (f'<ExtratoCVS {self.DT_MOVIMENTACAO} - '
                f'ORDEM {self.ORDEM} - {self.HISTORICO}>')

    @staticmethod
    def obter_ultima_data_movimentacao():
        """Retorna a MAX(DT_MOVIMENTACAO) da tabela. None se vazia."""
        from sqlalchemy import func
        return db.session.query(
            func.max(ExtratoCVS.DT_MOVIMENTACAO)
        ).scalar()

    @staticmethod
    def listar_por_mes(primeiro_dia_mes, ultimo_dia_mes):
        """Lista todas as movimentações de um intervalo de datas."""
        return ExtratoCVS.query.filter(
            ExtratoCVS.DT_MOVIMENTACAO >= primeiro_dia_mes,
            ExtratoCVS.DT_MOVIMENTACAO <= ultimo_dia_mes
        ).order_by(
            ExtratoCVS.DT_MOVIMENTACAO.asc(),
            ExtratoCVS.ORDEM.asc()
        ).all()

    @staticmethod
    def listar_meses_distintos():
        """
        Lista os primeiros dias de cada mês com movimentação,
        em ordem decrescente. Usa SQL nativo do SQL Server
        (DATEFROMPARTS) para evitar incompatibilidade com
        SELECT DISTINCT + ORDER BY.
        """
        from sqlalchemy import text
        sql = text("""
            SELECT DISTINCT 
                DATEFROMPARTS(
                    YEAR([DT_MOVIMENTACAO]),
                    MONTH([DT_MOVIMENTACAO]),
                    1
                ) AS PRIMEIRO_DIA_MES
            FROM [BDG].[FIN_TB013_EXTRATO_CVS]
            ORDER BY PRIMEIRO_DIA_MES DESC;
        """)
        rows = db.session.execute(sql).fetchall()
        return [r[0] for r in rows if r[0] is not None]

# =========================================================================
# Posição Mensal de Estoque CVS
# =========================================================================
class PosicaoEstoqueCVS(db.Model):
    """
    Modelo para a tabela BDG.FIN_TB010_POSICAO_MENSAL_ESTOQUE_CVS.

    Estrutura real (conforme SSMS):
      - DT_CARGA    date          NOT NULL
      - DT_POSICAO  date          NOT NULL    *chave (PK)
      - TIPO        varchar(1)    NOT NULL    *chave (PK)
      - QTDE        int           NULL
      - VR_PU       decimal(18,2) NULL
      - VR_TOTAL    decimal(18,2) NULL

    Chave primária composta: (DT_POSICAO, TIPO)
    """
    __tablename__ = 'FIN_TB010_POSICAO_MENSAL_ESTOQUE_CVS'
    __table_args__ = {'schema': 'BDG'}

    DT_POSICAO = db.Column(db.Date, primary_key=True, nullable=False)
    TIPO = db.Column(db.String(1), primary_key=True, nullable=False)

    DT_CARGA = db.Column(db.Date, nullable=False)
    QTDE = db.Column(db.Integer, nullable=True)
    VR_PU = db.Column(db.Numeric(18, 2), nullable=True)
    VR_TOTAL = db.Column(db.Numeric(18, 2), nullable=True)

    def __repr__(self):
        return (f'<PosicaoEstoqueCVS {self.DT_POSICAO} - '
                f'{self.TIPO} - QTDE {self.QTDE}>')

    @staticmethod
    def listar_todos_ordenados():
        """
        Lista todas as posições ordenadas por DT_POSICAO (ASC) e TIPO (ASC).
        """
        return PosicaoEstoqueCVS.query.order_by(
            PosicaoEstoqueCVS.DT_POSICAO.asc(),
            PosicaoEstoqueCVS.TIPO.asc()
        ).all()

    @staticmethod
    def obter_ultima_dt_posicao():
        """Retorna a MAX(DT_POSICAO) ou None se vazia."""
        from sqlalchemy import func
        return db.session.query(
            func.max(PosicaoEstoqueCVS.DT_POSICAO)
        ).scalar()