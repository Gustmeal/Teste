# app/models/custo_oportunidade.py
from datetime import datetime
from app import db


class CustoOportunidade(db.Model):
    """
    Modelo para a tabela BDG.FIN_TB001_CUSTO_OPORTUNIDADE.

    Estrutura real (conforme SSMS):
      - DT_CARGA            date           NOT NULL
      - DT_ATUALIZACAO      date           NOT NULL    *chave (PK)
      - INST_FINANC         varchar(13)    NOT NULL    *chave (PK)
      - VR_PRECO_MEDIA      decimal(18,4)  NULL
      - ANO_MES             varchar(8)     NULL
      - COD_MES_ANO         varchar(3)     NULL
      - TAXA_MEDIA          decimal(18,4)  NULL
      - TAXA_MEDIA_MENSAL   decimal(18,4)  NULL
        (taxa equivalente mensal: ((1 + TAXA_MEDIA/100)^(1/12) - 1) * 100)
    """
    __tablename__ = 'FIN_TB001_CUSTO_OPORTUNIDADE'
    __table_args__ = {'schema': 'BDG'}

    DT_ATUALIZACAO = db.Column(db.Date, primary_key=True, nullable=False)
    INST_FINANC = db.Column(db.String(13), primary_key=True, nullable=False)

    DT_CARGA = db.Column(db.Date, nullable=False)
    VR_PRECO_MEDIA = db.Column(db.Numeric(18, 4), nullable=True)
    ANO_MES = db.Column(db.String(8), nullable=True)
    COD_MES_ANO = db.Column(db.String(3), nullable=True)
    TAXA_MEDIA = db.Column(db.Numeric(18, 4), nullable=True)
    TAXA_MEDIA_MENSAL = db.Column(db.Numeric(18, 4), nullable=True)

    def __repr__(self):
        return f'<CustoOportunidade {self.INST_FINANC} - {self.DT_ATUALIZACAO}>'

    @staticmethod
    def listar_por_data_atualizacao(dt_atualizacao):
        """Lista registros de uma determinada data de pregão"""
        return CustoOportunidade.query.filter_by(
            DT_ATUALIZACAO=dt_atualizacao
        ).order_by(CustoOportunidade.INST_FINANC.asc()).all()

    @staticmethod
    def listar_datas_atualizacao_distintas():
        """
        Retorna todas as DT_ATUALIZACAO distintas presentes na tabela,
        em ordem decrescente. Usado para popular o dropdown de filtro.
        """
        rows = db.session.query(
            CustoOportunidade.DT_ATUALIZACAO
        ).distinct().order_by(
            CustoOportunidade.DT_ATUALIZACAO.desc()
        ).all()
        return [r[0] for r in rows if r[0] is not None]

    @staticmethod
    def obter_dt_atualizacao_mais_recente():
        """Retorna a DT_ATUALIZACAO mais recente da tabela (ou None)"""
        return db.session.query(
            db.func.max(CustoOportunidade.DT_ATUALIZACAO)
        ).scalar()