from app import db


class RelatorioConsideracoesItem(db.Model):
    """
    Tabela BDG.FIN_TB025_RELATORIO_GESTAO_CONSIDERACOES.
    Cada linha é um fragmento de texto (TEXTO) com um '...' que recebe o VR.
    Os fragmentos se juntam por (ITEM, SUBITEM), na ordem do ID.
    """
    __tablename__ = 'FIN_TB025_RELATORIO_GESTAO_CONSIDERACOES'
    __table_args__ = {'schema': 'BDG'}

    ID = db.Column(db.Integer, primary_key=True, nullable=False)
    POSICAO = db.Column(db.String(6), nullable=True)
    ITEM = db.Column(db.String(50), nullable=True)
    SUBITEM = db.Column(db.String(120), nullable=True)
    ID_VR = db.Column(db.Integer, nullable=True)
    VR = db.Column(db.Numeric(18, 2), nullable=True)
    TEXTO = db.Column(db.String(500), nullable=True)

    @staticmethod
    def obter_posicao_referencia():
        """MAX(POSICAO) — competência mais recente. None se vazia."""
        from sqlalchemy import func
        return db.session.query(
            func.max(RelatorioConsideracoesItem.POSICAO)
        ).scalar()

    @staticmethod
    def carregar(posicao):
        """Registros da competência, na ordem do ID."""
        return RelatorioConsideracoesItem.query.filter_by(
            POSICAO=posicao
        ).order_by(RelatorioConsideracoesItem.ID).all()