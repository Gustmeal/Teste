from datetime import datetime
from app import db
from sqlalchemy import BigInteger, Integer, Numeric, DateTime


class NaoDistribuiveis(db.Model):
    """
    Modelo para a tabela DCA_TB020_NÃO_DISTRIBUIVEIS.

    Armazena contratos que foram excluídos do cálculo de distribuição por:
    - Valor contratado menor que R$ 1.000,00
    - Produto CCFácil ou CCFácil Rot
    - Contratos Serasa (opcional, selecionado pelo usuário)
    """
    __tablename__ = 'DCA_TB020_NÃO_DISTRIBUIVEIS'
    __table_args__ = {'schema': 'BDG'}

    ID = db.Column(Integer, primary_key=True, autoincrement=True)
    FkContratoSISCTR = db.Column(BigInteger, nullable=False, index=True)
    NR_CPF_CNPJ = db.Column(BigInteger, nullable=False, index=True)
    VR_SD_DEVEDOR = db.Column(Numeric(18, 2), nullable=True)
    CREATED_AT = db.Column(DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(DateTime)

    def __repr__(self):
        return f'<NaoDistribuivel {self.ID}: Contrato {self.FkContratoSISCTR}>'

    @staticmethod
    def limpar_tabela():
        """Limpa todos os registros da tabela (TRUNCATE)"""
        from sqlalchemy import text
        try:
            db.session.execute(text("TRUNCATE TABLE [BDG].[DCA_TB020_NÃO_DISTRIBUIVEIS]"))
            db.session.commit()
            return True
        except Exception as e:
            db.session.rollback()
            print(f"Erro ao limpar tabela NÃO_DISTRIBUIVEIS: {str(e)}")
            return False

    @staticmethod
    def contar_registros():
        """Conta os registros ativos na tabela"""
        return NaoDistribuiveis.query.filter(
            NaoDistribuiveis.DELETED_AT == None
        ).count()

    @staticmethod
    def obter_total_saldo():
        """Obtém o total do saldo devedor dos contratos não distribuíveis"""
        from sqlalchemy import func
        resultado = db.session.query(
            func.sum(NaoDistribuiveis.VR_SD_DEVEDOR)
        ).filter(
            NaoDistribuiveis.DELETED_AT == None
        ).scalar()
        return float(resultado) if resultado else 0.0