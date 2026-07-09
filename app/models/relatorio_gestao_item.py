from app import db


class RelatorioGestaoItem(db.Model):
    """
    Tabela BDG.FIN_TB023_RELATORIO_GESTAO_ITENS.

    Guarda os valores que substituem os números dos textos do Relatório de
    Gestão. A substituição é por CONTAGEM: o n-ésimo valor do texto (ordem de
    leitura, incluindo os entre parênteses) é trocado pelo VR cujo ID == n.

    Colunas (conforme SSMS):
      - PAGINA   varchar(20)   *chave  (página, ex.: 'SUMARIO_EXECUTIVO')
      - POSICAO  varchar(6)    *chave  (competência de referência 'AAAAMM', ex.: '202605')
      - ID       int           *chave  (nº de contagem do valor no texto: 1..N)
      - VR       decimal(18,2)         (valor que entra no lugar do número contado)
      - OBS      varchar(100)          (uso da área — ignorado nesta automação)

    Chave primária composta: (PAGINA, POSICAO, ID).
    """
    __tablename__ = 'FIN_TB023_RELATORIO_GESTAO_ITENS'
    __table_args__ = {'schema': 'BDG'}

    PAGINA = db.Column(db.String(20), primary_key=True, nullable=False)
    POSICAO = db.Column(db.String(6), primary_key=True, nullable=False)
    ID = db.Column(db.Integer, primary_key=True, nullable=False)
    VR = db.Column(db.Numeric(18, 2), nullable=True)
    OBS = db.Column(db.String(100), nullable=False)

    def __repr__(self):
        return (f'<RelatorioGestaoItem {self.PAGINA}/{self.POSICAO} '
                f'ID={self.ID} VR={self.VR}>')

    @staticmethod
    def obter_posicao_referencia(pagina):
        """MAX(POSICAO) da página (competência mais recente). None se vazia."""
        from sqlalchemy import func
        return db.session.query(
            func.max(RelatorioGestaoItem.POSICAO)
        ).filter(RelatorioGestaoItem.PAGINA == pagina).scalar()

    @staticmethod
    def carregar_mapa_id_vr(pagina, posicao):
        """Retorna {ID(int) -> VR(Decimal)} da página/competência informada."""
        registros = RelatorioGestaoItem.query.filter(
            RelatorioGestaoItem.PAGINA == pagina,
            RelatorioGestaoItem.POSICAO == posicao,
        ).all()
        mapa = {}
        for r in registros:
            if r.ID is not None and r.VR is not None:
                mapa[int(r.ID)] = r.VR
        return mapa