from app import db


class BoletimFinanceiro(db.Model):
    """
    Tabela BDG.FIN_TB020_BOLETIM_FINANCEIRO.

    Estrutura:
      - NU_LINHA      int            *chave  (vem da FIN_TB019 pela NATUREZA)
      - MES_EXECUCAO  varchar(6)     *chave  (competência 'AAAAMM', ex.: '202603')
      - NATUREZA      varchar        natureza (rótulo) capturada do Boletim
      - VR_EXECUTADO  decimal(18,2)  valor executado da natureza no mês

    Chave primária COMPOSTA: (NU_LINHA, MES_EXECUCAO) — pois o mesmo NU_LINHA
    se repete uma vez por mês.

    OBS.: assumi a PK como (NU_LINHA, MES_EXECUCAO). Confirme no SSMS; se for
    outra, me avise que ajusto o mapeamento.
    """
    __tablename__ = 'FIN_TB021_SALDO_CONTAS_BF'
    __table_args__ = {'schema': 'BDG'}

    NU_LINHA = db.Column(db.Integer, primary_key=True,
                         autoincrement=False, nullable=False)
    MES_EXECUCAO = db.Column(db.String(6), primary_key=True, nullable=False)
    NATUREZA = db.Column(db.String(255), nullable=True)
    VR_EXECUTADO = db.Column(db.Numeric(18, 2), nullable=True)

    def __repr__(self):
        return (f'<BoletimFinanceiro L{self.NU_LINHA} '
                f'{self.MES_EXECUCAO} {self.NATUREZA}>')