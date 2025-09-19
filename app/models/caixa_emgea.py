from datetime import datetime
from app import db
from sqlalchemy import BigInteger, Integer, Numeric, String, Date, DateTime, Text, SmallInteger


class CaixaEmgea(db.Model):
    """
    Modelo para a tabela PEN_TB012_CAIXA_EMGEA
    Controle de caixas EMGEA relacionadas a pendências e retenções
    """
    __tablename__ = 'PEN_TB012_CAIXA_EMGEA'
    __table_args__ = {'schema': 'DEV'}

    # Campos da tabela - TODOS OPCIONAIS exceto ID
    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ID_OCORRENCIA = db.Column(db.Integer, nullable=True, index=True)
    NU_OFICIO = db.Column(db.Integer, nullable=True, index=True)
    DT_OFICIO = db.Column(db.Integer, nullable=True)
    EMITENTE = db.Column(db.String(5), nullable=True)
    ID_STATUS = db.Column(db.Integer, nullable=True, index=True)
    NU_CONTRATO = db.Column(db.Numeric(23, 0), nullable=True, index=True)
    NU_PROCESSO = db.Column(db.Text, nullable=True)
    VALOR = db.Column(db.Numeric(18, 2), nullable=True, index=True)
    VR_REAL = db.Column(db.Numeric(18, 2), nullable=True)
    DT_PAGTO = db.Column(db.Date, nullable=True)
    ID_OBSERVACAO = db.Column(db.SmallInteger, nullable=True)
    ID_ESPECIFICACAO = db.Column(db.SmallInteger, nullable=True)
    ID_CARTEIRA = db.Column(db.Integer, nullable=True)
    NR_TICKET = db.Column(db.Integer, nullable=True)
    DSC_DOCUMENTO = db.Column(db.String(100), nullable=True)
    VR_ISS = db.Column(db.Numeric(18, 2), nullable=True)

    # Campos de auditoria e usuário
    USUARIO_CRIACAO = db.Column(db.String(100), nullable=True)
    USUARIO_ALTERACAO = db.Column(db.String(100), nullable=True)
    USUARIO_EXCLUSAO = db.Column(db.String(100), nullable=True)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime, nullable=True)

    def __repr__(self):
        return f'<CaixaEmgea {self.ID} - Ofício: {self.NU_OFICIO} - Contrato: {self.NU_CONTRATO}>'

    @staticmethod
    def verificar_duplicidade(nu_contrato, valor, id_excluir=None):
        """Verifica se existe duplicidade de contrato e valor"""
        query = CaixaEmgea.query.filter(
            CaixaEmgea.NU_CONTRATO == nu_contrato,
            CaixaEmgea.VALOR == valor,
            CaixaEmgea.DELETED_AT.is_(None)
        )
        if id_excluir:
            query = query.filter(CaixaEmgea.ID != id_excluir)
        return query.first() is not None

    @staticmethod
    def obter_todos_ativos():
        """Retorna todos os registros ativos"""
        return CaixaEmgea.query.filter(
            CaixaEmgea.DELETED_AT.is_(None)
        ).order_by(CaixaEmgea.CREATED_AT.desc()).all()

    def soft_delete(self, usuario):
        """Realiza soft delete do registro"""
        self.DELETED_AT = datetime.utcnow()
        self.USUARIO_EXCLUSAO = usuario
        db.session.commit()

    def formatar_dt_oficio(self):
        """Formata DT_OFICIO de INT para string legível"""
        if self.DT_OFICIO:
            dt_str = str(self.DT_OFICIO)
            if len(dt_str) == 8:
                return f"{dt_str[6:8]}/{dt_str[4:6]}/{dt_str[0:4]}"
        return ""