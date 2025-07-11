from app import db
from datetime import datetime


class RelacaoImovelContratoParcelamento(db.Model):
    """Modelo para a tabela MOV_TB026_RELACAO_IMOVEL_CONTRATO_PARCELAMENTO"""
    __tablename__ = 'MOV_TB026_RELACAO_IMOVEL_CONTRATO_PARCELAMENTO'
    __table_args__ = {'schema': 'BDG'}

    # Chave primária composta
    NR_CONTRATO = db.Column(db.String(23), primary_key=True, nullable=False)
    NR_IMOVEL = db.Column(db.String(23), primary_key=True, nullable=False)

    # Campos de auditoria (apenas datas)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    def __repr__(self):
        return f'<RelacaoImovelContrato: Contrato {self.NR_CONTRATO} - Imóvel {self.NR_IMOVEL}>'

    @staticmethod
    def verificar_vinculacao_existente(nr_contrato, nr_imovel):
        """Verifica se já existe vinculação entre contrato e imóvel"""
        return RelacaoImovelContratoParcelamento.query.filter_by(
            NR_CONTRATO=nr_contrato,
            NR_IMOVEL=nr_imovel,
            DELETED_AT=None
        ).first() is not None

    @staticmethod
    def listar_vinculacoes_ativas():
        """Lista todas as vinculações ativas (não deletadas)"""
        return RelacaoImovelContratoParcelamento.query.filter_by(
            DELETED_AT=None
        ).order_by(
            RelacaoImovelContratoParcelamento.CREATED_AT.desc()
        ).all()