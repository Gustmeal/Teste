from datetime import datetime
from app import db


class CodigoContabil(db.Model):
    __tablename__ = 'COR_TB010_PDG_CODIGOS_CONTABEIS'
    __table_args__ = {'schema': 'DEV'}

    # Usar chave primária composta CODIGO + ANO
    CODIGO = db.Column(db.String(20), primary_key=True)  # Ex: 1.100.000.000
    ANO = db.Column(db.Integer, primary_key=True)

    # Campos da tabela real
    DSC_CODIGO = db.Column(db.String(255), nullable=False)  # Ex: RECEITAS DE CAPITAL
    NU_ORDEM = db.Column(db.Integer, nullable=True)  # NULL por enquanto
    NO_QUEBRA = db.Column(db.String(100), nullable=True)
    COD_RUBRICA = db.Column(db.BigInteger, nullable=False)  # CODIGO sem pontos
    IND_TOTALIZACAO = db.Column(db.Integer, nullable=True)  # 1 ou NULL

    def __repr__(self):
        return f'<CodigoContabil {self.CODIGO} - {self.DSC_CODIGO}>'

    @staticmethod
    def gerar_cod_rubrica(codigo_com_pontos):
        """
        Remove os pontos do código para gerar COD_RUBRICA
        Ex: '1.100.000.000' -> 1100000000
        """
        return int(codigo_com_pontos.replace('.', '')) if codigo_com_pontos else 0

    @staticmethod
    def obter_quebras_disponiveis():
        """
        Retorna lista distinct de NO_QUEBRA disponíveis na tabela
        """
        try:
            quebras = db.session.query(CodigoContabil.NO_QUEBRA).distinct().filter(
                CodigoContabil.NO_QUEBRA.isnot(None)
            ).all()
            return [quebra[0] for quebra in quebras if quebra[0]]
        except:
            return []