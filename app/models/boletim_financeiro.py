from app import db


class BoletimFinanceiro(db.Model):
    """
    Tabela BDG.FIN_TB020_BOLETIM_FINANCEIRO.

    Estrutura:
      - NU_LINHA      int            *chave (sequencial, atribuído pela aplicação)
      - NATUREZA      varchar        natureza (rótulo) capturada do Boletim
      - MES_EXECUCAO  varchar(6)     competência no formato 'AAAAMM' (ex.: '202603')
      - VR_EXECUTADO  decimal(18,2)  valor executado da natureza no mês

    OBS.: NU_LINHA aqui NÃO é IDENTITY — a ordem é controlada pela aplicação
    (MAX+1), conforme a regra "deve ir seguindo a ordem". Se no banco real ela
    for IDENTITY, me avise que eu troco para o padrão de INSERT via text() sem
    mapear a coluna (igual à FIN_TB013).
    """
    __tablename__ = 'FIN_TB021_SALDO_CONTAS_BF'
    __table_args__ = {'schema': 'BDG'}

    NU_LINHA = db.Column(db.Integer, primary_key=True,
                         autoincrement=False, nullable=False)
    NATUREZA = db.Column(db.String(255), nullable=True)
    MES_EXECUCAO = db.Column(db.String(6), nullable=True)
    VR_EXECUTADO = db.Column(db.Numeric(18, 2), nullable=True)

    def __repr__(self):
        return (f'<BoletimFinanceiro L{self.NU_LINHA} '
                f'{self.MES_EXECUCAO} {self.NATUREZA}>')

    @staticmethod
    def obter_proximo_nu_linha():
        """Retorna MAX(NU_LINHA)+1. Retorna 1 se a tabela estiver vazia."""
        from sqlalchemy import func
        max_nu = db.session.query(func.max(BoletimFinanceiro.NU_LINHA)).scalar()
        return (int(max_nu) + 1) if max_nu is not None else 1