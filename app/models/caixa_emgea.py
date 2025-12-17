from datetime import datetime
from app import db
from sqlalchemy import BigInteger, Integer, Numeric, String, Date, DateTime, Text, SmallInteger, func


class CaixaEmgea(db.Model):
    """
    Modelo para a tabela PEN_TB013_TABELA_PRINCIPAL
    Controle de caixas EMGEA relacionadas a pendências e retenções

    ATUALIZADO: Migrado de PEN_TB012_CAIXA_EMGEA para PEN_TB013_TABELA_PRINCIPAL
    Schema alterado de DEV para BDG

    IMPORTANTE: Esta tabela é compartilhada com PenDetalhamento.
    ID_DETALHAMENTO é gerenciado manualmente (não autoincrement).
    """
    __tablename__ = 'PEN_TB013_TABELA_PRINCIPAL'
    __table_args__ = {'schema': 'BDG', 'extend_existing': True}

    # Campos da tabela - ID_DETALHAMENTO SEM AUTOINCREMENT
    ID_DETALHAMENTO = db.Column(db.Integer, primary_key=True, autoincrement=False)
    NU_CONTRATO = db.Column(db.Numeric(23, 0), nullable=True, index=True)
    ID_CARTEIRA = db.Column(db.Integer, nullable=True)
    ID_OCORRENCIA = db.Column(db.Integer, nullable=True, index=True)
    VR_FALHA = db.Column(db.Numeric(18, 2), nullable=True, index=True)
    ID_STATUS = db.Column(db.Integer, nullable=True, index=True)
    NU_OFICIO = db.Column(db.Integer, nullable=True, index=True)
    IC_CONDENACAO = db.Column(db.Boolean, nullable=True)
    INDICIO_DUPLIC = db.Column(db.Boolean, nullable=True)
    DT_PAGTO = db.Column(db.Date, nullable=True)
    ID_ACAO = db.Column(db.Integer, nullable=True)
    DT_DOCUMENTO = db.Column(db.Date, nullable=True)
    DEVEDOR = db.Column(db.String(50), nullable=True)
    DT_INICIO_ATUALIZACAO = db.Column(db.Date, nullable=True)
    DT_ATUALIZACAO = db.Column(db.Date, nullable=True)
    NR_PROCESSO = db.Column(db.Text, nullable=True)
    VR_REAL = db.Column(db.Numeric(18, 2), nullable=True)
    ID_OBSERVACAO = db.Column(db.SmallInteger, nullable=True)
    ID_ESPECIFICACAO = db.Column(db.SmallInteger, nullable=True)
    NR_TICKET = db.Column(db.Integer, nullable=True)
    DSC_DOCUMENTO = db.Column(db.String(100), nullable=True)
    VR_ISS = db.Column(db.Numeric(18, 2), nullable=True)

    # NOVAS COLUNAS ADICIONADAS
    NU_MEMORANDO = db.Column(db.String(20), nullable=True, index=True)
    NR_PROCESSO_SEI = db.Column(db.String(50), nullable=True, index=True)

    # Campos de auditoria e usuário
    USUARIO_CRIACAO = db.Column(db.String(100), nullable=True)
    USUARIO_ALTERACAO = db.Column(db.String(100), nullable=True)
    USUARIO_EXCLUSAO = db.Column(db.String(100), nullable=True)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime, nullable=True)

    def __repr__(self):
        return f'<CaixaEmgea {self.ID_DETALHAMENTO} - Contrato: {self.NU_CONTRATO}>'

    @staticmethod
    def obter_proximo_id():
        """Obtém o próximo ID_DETALHAMENTO disponível"""
        resultado = db.session.query(func.max(CaixaEmgea.ID_DETALHAMENTO)).first()
        proximo_id = (resultado[0] or 0) + 1
        return proximo_id

    @staticmethod
    def verificar_duplicidade(nu_contrato, vr_falha, excluir_id=None):
        """
        Verifica se existe registro com mesmo NU_CONTRATO e VR_FALHA
        excluir_id: ID a ser ignorado na verificação (para edição)
        """
        query = CaixaEmgea.query.filter(
            CaixaEmgea.NU_CONTRATO == nu_contrato,
            CaixaEmgea.VR_FALHA == vr_falha,
            CaixaEmgea.DELETED_AT.is_(None)
        )

        if excluir_id:
            query = query.filter(CaixaEmgea.ID_DETALHAMENTO != excluir_id)

        return query.first() is not None

    @staticmethod
    def obter_todos_ativos():
        """Retorna todos os registros não excluídos"""
        return CaixaEmgea.query.filter(CaixaEmgea.DELETED_AT.is_(None)).order_by(
            CaixaEmgea.ID_DETALHAMENTO.desc()
        ).all()

    @staticmethod
    def obter_devedores_distintos():
        """Retorna lista de devedores distintos"""
        resultado = db.session.query(CaixaEmgea.DEVEDOR).distinct().filter(
            CaixaEmgea.DEVEDOR.isnot(None),
            CaixaEmgea.DELETED_AT.is_(None)
        ).all()
        return [r[0] for r in resultado]

    def soft_delete(self, usuario):
        """Marca registro como excluído"""
        self.DELETED_AT = datetime.utcnow()
        self.USUARIO_EXCLUSAO = usuario
        db.session.commit()

    def formatar_dt_documento(self):
        """Formata DT_DOCUMENTO para string legível"""
        if self.DT_DOCUMENTO:
            return self.DT_DOCUMENTO.strftime('%d/%m/%Y')
        return ""

    def formatar_dt_pagto(self):
        """Formata DT_PAGTO para string legível"""
        if self.DT_PAGTO:
            return self.DT_PAGTO.strftime('%d/%m/%Y')
        return ""

    def formatar_dt_atualizacao(self):
        """Formata DT_ATUALIZACAO para string legível"""
        if self.DT_ATUALIZACAO:
            return self.DT_ATUALIZACAO.strftime('%d/%m/%Y')
        return ""

    def formatar_dt_inicio_atualizacao(self):
        """Formata DT_INICIO_ATUALIZACAO para string legível"""
        if self.DT_INICIO_ATUALIZACAO:
            return self.DT_INICIO_ATUALIZACAO.strftime('%d/%m/%Y')
        return ""


from datetime import datetime
from app import db
from sqlalchemy import Integer, String, DateTime, Text


class PenEspecificacaoFalha(db.Model):
    """Modelo para a tabela PEN_TB006_ESPECIFICACAO_FALHA"""
    __tablename__ = 'PEN_TB006_ESPECIFICACAO_FALHA'
    __table_args__ = {'schema': 'BDG'}

    ID_ESPECIFICACAO = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ID_DETALHAMENTO = db.Column(db.Integer, nullable=True)
    DSC_ESPECIFICACAO = db.Column(db.String(255), nullable=True)
    ULTIMA_ATUALIZACAO = db.Column(db.DateTime, nullable=True)

    def __repr__(self):
        return f'<PenEspecificacaoFalha {self.ID_ESPECIFICACAO} - {self.DSC_ESPECIFICACAO}>'

    @staticmethod
    def criar_especificacao(descricao, id_detalhamento=None):
        """
        Cria uma nova especificação e retorna o ID gerado
        """
        nova_especificacao = PenEspecificacaoFalha()
        nova_especificacao.DSC_ESPECIFICACAO = descricao
        nova_especificacao.ID_DETALHAMENTO = id_detalhamento
        nova_especificacao.ULTIMA_ATUALIZACAO = datetime.utcnow()

        db.session.add(nova_especificacao)
        db.session.flush()  # Força a geração do ID sem fazer commit

        return nova_especificacao.ID_ESPECIFICACAO