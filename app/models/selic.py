# app/models/selic.py
from app import db


class Selic(db.Model):
    """
    Modelo para a tabela BDG.FIN_TB005_SELIC.

    Armazena a série histórica mensal da Selic conforme planilha
    fornecida pela área financeira. A tabela é totalmente sobrescrita
    a cada upload (TRUNCATE + INSERT).

    Estrutura:
      - COGEP     date           NOT NULL   *chave (PK) - 1º dia do mês de referência
      - COPOM     decimal(18,4)  NULL       Meta Selic decidida pelo COPOM (% a.a.)
      - SELIC_AM  decimal(18,4)  NULL       Selic mensal (% a.m.)
      - SELIC_AA  decimal(18,4)  NULL       Selic anual (% a.a.)
    """
    __tablename__ = 'FIN_TB005_SELIC'
    __table_args__ = {'schema': 'BDG'}

    COGEP = db.Column(db.Date, primary_key=True, nullable=False)
    COPOM = db.Column(db.Numeric(18, 4), nullable=True)
    SELIC_AM = db.Column(db.Numeric(18, 4), nullable=True)
    SELIC_AA = db.Column(db.Numeric(18, 4), nullable=True)

    def __repr__(self):
        return (f'<Selic {self.COGEP} COPOM={self.COPOM} '
                f'AM={self.SELIC_AM} AA={self.SELIC_AA}>')