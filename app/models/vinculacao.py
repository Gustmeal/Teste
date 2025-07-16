from datetime import datetime
from app import db
from sqlalchemy import text


class ItemContaSucor(db.Model):
    __tablename__ = 'COR_TB008_PDG_ITEM_CONTA_SUCOR'
    __table_args__ = {'schema': 'BDG'}

    # Chave primária composta por ID_ITEM, CODIGO e ANO.
    ID_ITEM = db.Column(db.Integer, primary_key=True, autoincrement=False)
    CODIGO = db.Column(db.String(20), primary_key=True, nullable=False)
    ANO = db.Column(db.Integer, primary_key=True, nullable=False)

    # Colunas que não fazem parte da chave
    DSC_ARQUIVO = db.Column(db.String(255), nullable=True)
    ARQUIVO7 = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return f'<ItemContaSucor {self.ID_ITEM} - {self.CODIGO} - {self.ANO}>'


class DescricaoItensSiscor(db.Model):
    __tablename__ = 'COR_TB003_DESCRICAO_ITENS_SISCOR'
    __table_args__ = {'schema': 'BDG'}

    ID_ITEM = db.Column(db.Integer, primary_key=True)
    DSC_ITEM_ORCAMENTO = db.Column(db.String(255), nullable=True)

    def __repr__(self):
        return f'<DescricaoItensSiscor {self.ID_ITEM} - {self.DSC_ITEM_ORCAMENTO}>'

    @staticmethod
    def obter_itens_ordenados():
        """
        Retorna lista de itens ordenados crescente por ID_ITEM
        """
        try:
            itens = DescricaoItensSiscor.query.filter(
                DescricaoItensSiscor.DSC_ITEM_ORCAMENTO.isnot(None)
            ).order_by(DescricaoItensSiscor.ID_ITEM.asc()).all()
            return itens
        except:
            return []


class CodigoContabilVinculacao:
    """
    Classe helper para buscar dados dos códigos contábeis para vinculação
    """

    @staticmethod
    def obter_codigos_ordenados():
        """
        Retorna códigos contábeis ordenados para seleção
        """
        try:
            from app.models.codigo_contabil import CodigoContabil
            codigos = CodigoContabil.query.order_by(
                CodigoContabil.CODIGO.asc()
            ).all()
            return codigos
        except:
            return []

    @staticmethod
    def obter_arquivos_distinct():
        """
        Retorna lista distinct de NO_QUEBRA para DSC_ARQUIVO
        """
        try:
            from app.models.codigo_contabil import CodigoContabil
            arquivos = db.session.query(CodigoContabil.NO_QUEBRA).distinct().filter(
                CodigoContabil.NO_QUEBRA.isnot(None)
            ).order_by(CodigoContabil.NO_QUEBRA.asc()).all()
            return [arquivo[0] for arquivo in arquivos if arquivo[0]]
        except:
            return []