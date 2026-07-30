from app import db


class IndiceAnbima(db.Model):
    """BDG.FIN_TB030 — Índices ANBIMA (IMA). *chave: DIA."""
    __tablename__ = 'FIN_TB030_INDICES_AMBIMA'
    __table_args__ = {'schema': 'BDG', 'extend_existing': True}

    DIA = db.Column(db.Date, primary_key=True, nullable=False)
    VARIACAO_DIARIA_PERC = db.Column(db.Numeric(18, 8))
    VARIACAO_MENSAL_PERC = db.Column(db.Numeric(18, 8))
    VARIACAO_ANUAL_PERC = db.Column(db.Numeric(18, 8))
    VARIACAO_12MESES_PERC = db.Column(db.Numeric(18, 8))
    VARIACAO_24MESES_PERC = db.Column(db.Numeric(18, 8))

    @staticmethod
    def existe(dia):
        return db.session.query(
            IndiceAnbima.query.filter_by(DIA=dia).exists()
        ).scalar()