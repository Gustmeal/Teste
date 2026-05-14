# app/models/custo_oportunidade_media.py
from app import db


class CustoOportunidadeMedia(db.Model):
    """
    Modelo para a tabela BDG.FIN_TB003_CUSTO_OPORTUNIDADE_MEDIAS.

    1 linha por pregão (DT_ATUALIZACAO é PK simples).

    Estrutura:
      - DT_ATUALIZACAO        date           NOT NULL    *chave (PK)
      - MEDIA_MENSAL          decimal(18,4)  NULL
        (= AVG(TAXA_MEDIA_MENSAL) de todos os contratos do pregão)
      - CUSTO_DE_OPURTUNIDADE decimal(18,4)  NULL
        (= ((1 + MEDIA_MENSAL/100)^12 - 1) * 100 — taxa anual equivalente)
      - REUNIAO               bit            NOT NULL DEFAULT 0
        (flag COPOM: 1 = pregão coincidiu com reunião do COPOM)
      - TX_SELIC              decimal(18,4)  NULL
        (Taxa Selic do pregão, preenchida manualmente pelo usuário)
    """
    __tablename__ = 'FIN_TB003_CUSTO_OPORTUNIDADE_MEDIAS'
    __table_args__ = {'schema': 'BDG'}

    DT_ATUALIZACAO = db.Column(db.Date, primary_key=True, nullable=False)
    MEDIA_MENSAL = db.Column(db.Numeric(18, 4), nullable=True)
    CUSTO_DE_OPURTUNIDADE = db.Column(db.Numeric(18, 4), nullable=True)
    REUNIAO = db.Column(db.Boolean, nullable=False, default=False)
    TX_SELIC = db.Column(db.Numeric(18, 2), nullable=True)

    def __repr__(self):
        return (f'<CustoOportunidadeMedia {self.DT_ATUALIZACAO} '
                f'MEDIA={self.MEDIA_MENSAL} CO={self.CUSTO_DE_OPURTUNIDADE} '
                f'COPOM={self.REUNIAO} SELIC={self.TX_SELIC}>')

    @staticmethod
    def obter_por_data(dt_atualizacao):
        """Busca o registro de um pregão específico"""
        return CustoOportunidadeMedia.query.filter_by(
            DT_ATUALIZACAO=dt_atualizacao
        ).first()