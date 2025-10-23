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

    # Campos de auditoria e usuário
    USUARIO_CRIACAO = db.Column(db.String(100), nullable=True)
    USUARIO_ALTERACAO = db.Column(db.String(100), nullable=True)
    USUARIO_EXCLUSAO = db.Column(db.String(100), nullable=True)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime, nullable=True)

    def __repr__(self):
        return f'<CaixaEmgea {self.ID_DETALHAMENTO} - Ofício: {self.NU_OFICIO} - Contrato: {self.NU_CONTRATO}>'

    @staticmethod
    def obter_proximo_id():
        """
        Busca o último ID_DETALHAMENTO e retorna o próximo (último + 1)
        LÓGICA: Simula autoincrement manualmente
        """
        ultimo_id = db.session.query(func.max(CaixaEmgea.ID_DETALHAMENTO)).scalar()
        return (ultimo_id + 1) if ultimo_id else 1

    @staticmethod
    def verificar_duplicidade(nu_contrato, vr_falha, id_excluir=None):
        """
        Verifica se existe duplicidade de contrato e valor de falha
        ATUALIZADO: Mudou de VALOR para VR_FALHA
        """
        query = CaixaEmgea.query.filter(
            CaixaEmgea.NU_CONTRATO == nu_contrato,
            CaixaEmgea.VR_FALHA == vr_falha,
            CaixaEmgea.DELETED_AT.is_(None)
        )
        if id_excluir:
            query = query.filter(CaixaEmgea.ID_DETALHAMENTO != id_excluir)
        return query.first() is not None

    @staticmethod
    def obter_todos_ativos():
        """
        Retorna todos os registros ativos
        Mantém a mesma lógica, apenas mudou o campo de ordenação
        """
        return CaixaEmgea.query.filter(
            CaixaEmgea.DELETED_AT.is_(None)
        ).order_by(CaixaEmgea.CREATED_AT.desc()).all()

    @staticmethod
    def obter_devedores_distintos():
        """
        Retorna lista de devedores distintos (CAIXA e EMGEA)
        """
        devedores = db.session.query(CaixaEmgea.DEVEDOR).distinct().filter(
            CaixaEmgea.DEVEDOR.isnot(None),
            CaixaEmgea.DELETED_AT.is_(None)
        ).all()
        return [d[0] for d in devedores if d[0]]

    def soft_delete(self, usuario):
        """
        Realiza soft delete do registro
        Mantém a mesma lógica
        """
        self.DELETED_AT = datetime.utcnow()
        self.USUARIO_EXCLUSAO = usuario
        db.session.commit()

    def formatar_dt_documento(self):
        """Formata DT_DOCUMENTO de DATE para string legível"""
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