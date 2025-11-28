from app import db
from sqlalchemy import text


class VisaoEconomicaSiscor(db.Model):
    """Tabela de visão econômica do SISCOR"""
    __tablename__ = 'COR_TB004_VISAO_ECONOMICA_SISCOR'
    __table_args__ = {'schema': 'BDG'}

    ID_CENTRO_RESULTADO = db.Column(db.Integer, nullable=True)
    ID_ITEM = db.Column(db.Integer, primary_key=True, nullable=False)
    DT_PREVISAO_ORCAMENTO = db.Column(db.DateTime, nullable=True)
    ID_FASE_ORCAMENTARIA = db.Column(db.Integer, nullable=True)
    DSC_ITEM_ORCAMENTO = db.Column(db.String(255), nullable=True)
    VR_PREVISAO_ORCAMENTO = db.Column(db.Numeric(18, 2), nullable=True)
    DT_REFERENCIA = db.Column(db.DateTime, nullable=True)
    UNIDADE = db.Column(db.String(50), nullable=True)
    ID_NATUREZA = db.Column(db.Integer, nullable=True)
    ID_VISAO = db.Column(db.Integer, nullable=True)
    COD_RUBRICA = db.Column(db.String(50), nullable=True)

    def __repr__(self):
        return f'<VisaoEconomicaSiscor {self.ID_ITEM} - {self.DSC_ITEM_ORCAMENTO}>'

    @staticmethod
    def obter_itens_distintos():
        """Retorna lista de ID_ITEM distintos ordenados"""
        try:
            itens = db.session.query(
                VisaoEconomicaSiscor.ID_ITEM,
                VisaoEconomicaSiscor.DSC_ITEM_ORCAMENTO
            ).distinct().order_by(
                VisaoEconomicaSiscor.ID_ITEM.asc()
            ).all()
            return itens
        except:
            return []


class VinculoDreItem(db.Model):
    """Tabela de vínculo entre DRE e Item Orçamentário"""
    __tablename__ = 'COR_TB024_VINCULO_DRE_ITEM'
    __table_args__ = {'schema': 'BDG'}

    # NIVEL3_ITEM é a chave primária (PK) - SEM autoincrement
    NIVEL3_ITEM = db.Column(db.Integer, primary_key=True, nullable=False, autoincrement=False)

    # ORDEM pode se repetir (não é chave primária)
    ORDEM = db.Column(db.Integer, nullable=True)

    NIVEL2 = db.Column(db.String(255), nullable=True)
    NIVEL1 = db.Column(db.String(255), nullable=True)
    RECEITA_LIQUIDA = db.Column(db.Integer, nullable=True)
    LUCRO_BRUTO = db.Column(db.Integer, nullable=True)
    DESPESAS_ADMINISTRATIVAS = db.Column(db.Integer, nullable=True)
    RECEITAS_DESPESAS = db.Column(db.Integer, nullable=True)
    LUCRO_ANTES_RESULT_FINANC = db.Column(db.Integer, nullable=True)
    RESULT_ANTES_TRIBUTOS = db.Column(db.Integer, nullable=True)
    LUCRO_LIQ_PERIODO = db.Column(db.Integer, nullable=True)
    RESULT_DEPOIS_JCP = db.Column(db.Integer, nullable=True)
    SINAL = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return f'<VinculoDreItem {self.NIVEL3_ITEM} - {self.NIVEL2}>'

    @staticmethod
    def obter_nivel2_distintos():
        """Retorna lista de NIVEL2 distintos ordenados"""
        try:
            niveis = db.session.query(
                VinculoDreItem.NIVEL2
            ).distinct().filter(
                VinculoDreItem.NIVEL2.isnot(None)
            ).order_by(VinculoDreItem.NIVEL2.asc()).all()
            return [nivel[0] for nivel in niveis if nivel[0]]
        except:
            return []

    @staticmethod
    def obter_nivel1_distintos():
        """Retorna lista de NIVEL1 distintos ordenados"""
        try:
            niveis = db.session.query(
                VinculoDreItem.NIVEL1
            ).distinct().filter(
                VinculoDreItem.NIVEL1.isnot(None)
            ).order_by(VinculoDreItem.NIVEL1.asc()).all()
            return [nivel[0] for nivel in niveis if nivel[0]]
        except:
            return []

    @staticmethod
    def obter_ordem_por_nivel2(nivel2):
        """Retorna a ordem correta baseada no NIVEL2 selecionado"""
        try:
            resultado = db.session.query(
                VinculoDreItem.ORDEM
            ).filter(
                VinculoDreItem.NIVEL2 == nivel2
            ).first()
            return resultado[0] if resultado else None
        except:
            return None