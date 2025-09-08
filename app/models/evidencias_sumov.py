from app import db
from sqlalchemy import Column, Integer, Numeric, String, Date
from datetime import datetime, date


class EvidenciasSumov(db.Model):
    """Modelo para a tabela de Evidências SUMOV"""
    __tablename__ = 'MOV_TB029_EXPLICACAO_EVIDENCIAS'
    __table_args__ = {'schema': 'BDG'}

    ID = Column(Integer, primary_key=True, autoincrement=True)
    NR_CONTRATO = Column(Numeric(23, 0), nullable=True)  # DECIMAL(23,0)
    MESANO = Column(Date, nullable=True)
    VALOR = Column(Numeric(18, 2), nullable=True)
    DESCRICAO = Column(String(150), nullable=True)

    def __repr__(self):
        return f'<EvidenciasSumov {self.ID} - {self.NR_CONTRATO}>'

    @staticmethod
    def listar_todas():
        """Lista todas as evidências do banco"""
        return EvidenciasSumov.query.order_by(
            EvidenciasSumov.MESANO.desc(),
            EvidenciasSumov.ID.desc()
        ).all()

    @staticmethod
    def buscar_por_id(id):
        """Busca uma evidência por ID"""
        return EvidenciasSumov.query.filter_by(ID=id).first()

    def formatar_mesano(self):
        """Formata MESANO de date para MM/YYYY"""
        if self.MESANO:
            return self.MESANO.strftime('%m/%Y')
        return ''

    def get_mesano_string(self):
        """Retorna MESANO como string YYYYMM"""
        if self.MESANO:
            return self.MESANO.strftime('%Y%m')
        return ''

    def get_nr_contrato_formatado(self):
        """Retorna o número do contrato formatado como string"""
        if self.NR_CONTRATO:
            # Converte para int para remover decimais e depois para string
            return str(int(self.NR_CONTRATO))
        return ''