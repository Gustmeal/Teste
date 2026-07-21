from app import db


class RelatorioResultadoFinanceiro(db.Model):
    """
    Tabela BDG.FIN_TB024_RELATORIO_GESTAO_RESULTADO_FINANCEIRO.
    Chave composta: (ANO, MES, NU_LINHA). Tabela somente leitura nesta página.
    """
    __tablename__ = 'FIN_TB024_RELATORIO_GESTAO_RESULTADO_FINANCEIRO'
    __table_args__ = {'schema': 'BDG'}

    ANO = db.Column(db.Integer, primary_key=True, nullable=False)
    MES = db.Column(db.Integer, primary_key=True, nullable=False)
    NU_LINHA = db.Column(db.Integer, primary_key=True, nullable=False)
    NATUREZA = db.Column(db.String(100), nullable=True)
    VR_MES_ANTERIOR = db.Column(db.Numeric(18, 2), nullable=True)
    VR_MES_ATUAL = db.Column(db.Numeric(18, 2), nullable=True)
    VR_ACUMUL_ATE_MES = db.Column(db.Numeric(18, 2), nullable=True)
    VR_ANO_ANTERIOR = db.Column(db.Numeric(18, 2), nullable=True)
    VR_ACUMUL_ATE_MES_ANO_ANT = db.Column(db.Numeric(18, 2), nullable=True)
    VARIACAO_ANUAL_PERC = db.Column(db.Numeric(18, 2), nullable=True)
    VARIACAO_ANUAL_ACUML_PERC = db.Column(db.Numeric(18, 2), nullable=True)
    VARIACAO_ANUAL = db.Column(db.Numeric(18, 2), nullable=True)
    VARIACAO_ANUAL_ACUML = db.Column(db.Numeric(18, 2), nullable=True)
    VARIACAO_MENSAL_PERC = db.Column(db.Numeric(18, 2), nullable=True)
    VARIACAO_MENSAL = db.Column(db.Numeric(18, 2), nullable=True)

    @staticmethod
    def obter_ano_mes_referencia():
        """Fallback: (ANO, MES) mais recente da própria tabela. None se vazia."""
        return db.session.query(
            RelatorioResultadoFinanceiro.ANO, RelatorioResultadoFinanceiro.MES
        ).order_by(
            RelatorioResultadoFinanceiro.ANO.desc(),
            RelatorioResultadoFinanceiro.MES.desc()
        ).first()