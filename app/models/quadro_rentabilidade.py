from app import db


class QuadroRentabilidade(db.Model):
    """BDG.FIN_TB031 — Quadros de rentabilidade dos fundos.
    *chave composta: (FUNDO, ANO_MES)."""
    __tablename__ = 'FIN_TB031_QUADROS_RETABILIDADE'
    __table_args__ = {'schema': 'BDG', 'extend_existing': True}

    FUNDO = db.Column(db.String(30), primary_key=True, nullable=False)
    ANO_MES = db.Column(db.String(6), primary_key=True, nullable=False)
    PERF_MES = db.Column(db.Numeric(16, 2), nullable=False)
    ACUMULADA = db.Column(db.Numeric(16, 2))
    IRF_M1 = db.Column(db.Numeric(16, 2))
    IRF_M1_ACUMUL = db.Column(db.Numeric(16, 2), nullable=False)
    TMS = db.Column(db.Numeric(16, 2))
    TMS_ACUMUL = db.Column(db.Numeric(16, 2))
    IRF_M1_COMP_MENSAL = db.Column(db.Numeric(16, 2))
    TMS_COMP_MENSAL = db.Column(db.Numeric(16, 2))
    IRF_M1_COMP_ANUAL = db.Column(db.Numeric(16, 2))
    TMS_COMP_ANUAL = db.Column(db.Numeric(16, 2))

    @staticmethod
    def carregar_por_fundo(fundo):
        """Todas as linhas do fundo, ordenadas por ANO_MES (competência)."""
        return QuadroRentabilidade.query.filter_by(FUNDO=fundo)\
            .order_by(QuadroRentabilidade.ANO_MES).all()