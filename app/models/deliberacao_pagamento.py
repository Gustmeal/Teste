# app/models/deliberacao_pagamento.py
from app import db
from datetime import datetime
from decimal import Decimal


class DeliberacaoPagamento(db.Model):
    """Modelo para Deliberação de Pagamento seguindo estrutura do Word"""
    __tablename__ = 'MOV_TB033_DELIBERACAO_PAGAMENTO'
    __table_args__ = {'schema': 'BDG'}

    # CHAVE PRIMÁRIA
    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    NU_CONTRATO = db.Column(db.String(50), nullable=False, unique=True)

    # ==== QUADRO RESUMO - IDENTIFICAÇÃO ====
    COLABORADOR_ANALISOU = db.Column(db.String(200), nullable=True)
    DT_ANALISE = db.Column(db.Date, nullable=True)
    MATRICULA_CAIXA_EMGEA = db.Column(db.String(100), nullable=True)
    DT_ARREMATACAO_AQUISICAO = db.Column(db.Date, nullable=True)
    DT_ENTRADA_ESTOQUE = db.Column(db.Date, nullable=True)

    # ==== COBRANÇA E DÍVIDA ====
    PERIODO_COBRANCA = db.Column(db.String(200), nullable=True)

    # Valor da dívida cobrada pelo condomínio (primeiro valor)
    VR_DIVIDA_CONDOMINIO_1 = db.Column(db.Numeric(18, 2), nullable=True)
    INDICE_DIVIDA_CONDOMINIO_1 = db.Column(db.String(50), nullable=True)
    PERC_HONORARIOS_CONDOMINIO_1 = db.Column(db.Numeric(5, 2), nullable=True)
    OBS_DIVIDA_CONDOMINIO_1 = db.Column(db.String(200), nullable=True)

    # Valor da dívida cobrada pelo condomínio (segundo valor)
    VR_DIVIDA_CONDOMINIO_2 = db.Column(db.Numeric(18, 2), nullable=True)
    OBS_DIVIDA_CONDOMINIO_2 = db.Column(db.String(200), nullable=True)

    PERIODO_PRESCRITO = db.Column(db.Text, nullable=True)

    # ==== ROTEIRO PARA ANÁLISE ====
    VR_DEBITO_EXCLUIDO_PRESCRITAS = db.Column(db.Numeric(18, 2), nullable=True)

    # Valor do débito calculado pela Emgea
    VR_DEBITO_CALCULADO_EMGEA = db.Column(db.Numeric(18, 2), nullable=True)
    INDICE_DEBITO_EMGEA = db.Column(db.String(50), nullable=True)
    PERC_HONORARIOS_EMGEA = db.Column(db.Numeric(5, 2), nullable=True)
    VR_HONORARIOS_EMGEA = db.Column(db.Numeric(18, 2), nullable=True)
    DT_CALCULO_EMGEA = db.Column(db.Date, nullable=True)

    # Valor de avaliação
    VR_AVALIACAO = db.Column(db.Numeric(18, 2), nullable=True)
    DT_LAUDO = db.Column(db.Date, nullable=True)

    # Status do imóvel
    STATUS_IMOVEL = db.Column(db.String(100), nullable=True)
    VR_VENDA = db.Column(db.Numeric(18, 2), nullable=True)
    DT_VENDA = db.Column(db.Date, nullable=True)
    VR_ESCRITURA = db.Column(db.Numeric(18, 2), nullable=True)
    NOME_COMPRADOR = db.Column(db.String(300), nullable=True)  # ADICIONADO
    DT_REGISTRO = db.Column(db.Date, nullable=True)  # ADICIONADO

    # Inadimplência condominial
    POSSUI_INADIMPLENCIA_CONDOMINIAL = db.Column(db.Boolean, nullable=True)
    VR_INADIMPLENCIA_CONDOMINIAL = db.Column(db.Numeric(18, 2), nullable=True)
    PERIODO_INADIMPLENCIA = db.Column(db.String(200), nullable=True)

    # ==== AÇÃO JUDICIAL ====
    POSSUI_ACAO_JUDICIAL = db.Column(db.Boolean, nullable=True)
    NR_PROCESSO_JUDICIAL = db.Column(db.String(200), nullable=True)
    POLO_ACAO = db.Column(db.String(100), nullable=True)
    PARTE_CONTRARIA = db.Column(db.String(300), nullable=True)
    POSSUI_DEPOSITOS_JUDICIAIS = db.Column(db.Boolean, nullable=True)
    VR_DEPOSITOS_JUDICIAIS = db.Column(db.Numeric(18, 2), nullable=True)
    DT_DEPOSITOS_JUDICIAIS = db.Column(db.Date, nullable=True)
    OBS_DEPOSITOS_JUDICIAIS = db.Column(db.Text, nullable=True)
    POSSUI_BLOQUEIOS_JUDICIAIS = db.Column(db.Boolean, nullable=True)
    OBS_BLOQUEIOS_JUDICIAIS = db.Column(db.Text, nullable=True)
    DECISAO_FAVORAVEL_EMGEA = db.Column(db.Boolean, nullable=True)
    OBS_DECISAO_JUDICIAL = db.Column(db.Text, nullable=True)
    POSSUI_OUTRAS_ACOES = db.Column(db.Boolean, nullable=True)
    NR_OUTRAS_ACOES = db.Column(db.String(500), nullable=True)
    PODE_ACORDO_EXTRAJUDICIAL = db.Column(db.Boolean, nullable=True)
    OBS_ACORDO_EXTRAJUDICIAL = db.Column(db.Text, nullable=True)

    # ==== DÉBITOS E PENALIDADES ====
    VR_DEMAIS_DEBITOS_SISDEX = db.Column(db.Numeric(18, 2), nullable=True)
    VR_DEMAIS_DEBITOS_SISGEA = db.Column(db.Numeric(18, 2), nullable=True)
    VR_DEMAIS_DEBITOS_TOTAL = db.Column(db.Numeric(18, 2), nullable=True)

    # Penalidade de ANS - CAIXA (até 3 contratos)
    PENALIDADE_ANS_CONTRATO_1 = db.Column(db.String(100), nullable=True)
    PENALIDADE_ANS_VALOR_1 = db.Column(db.Numeric(18, 2), nullable=True)
    PENALIDADE_ANS_CALCULO_1 = db.Column(db.String(100), nullable=True)

    PENALIDADE_ANS_CONTRATO_2 = db.Column(db.String(100), nullable=True)
    PENALIDADE_ANS_VALOR_2 = db.Column(db.Numeric(18, 2), nullable=True)
    PENALIDADE_ANS_CALCULO_2 = db.Column(db.String(100), nullable=True)

    PENALIDADE_ANS_CONTRATO_3 = db.Column(db.String(100), nullable=True)
    PENALIDADE_ANS_VALOR_3 = db.Column(db.Numeric(18, 2), nullable=True)
    PENALIDADE_ANS_CALCULO_3 = db.Column(db.String(100), nullable=True)

    PENALIDADE_ANS_TOTAL = db.Column(db.Numeric(18, 2), nullable=True)

    # Prejuízo Financeiro - CAIXA
    VR_DESEMBOLSADO_CAIXA = db.Column(db.Numeric(18, 2), nullable=True)
    VR_COBRADO_CONDOMINIO = db.Column(db.Numeric(18, 2), nullable=True)
    VR_PREJUIZO_FINANCEIRO = db.Column(db.Numeric(18, 2), nullable=True)
    OBS_PREJUIZO_FINANCEIRO = db.Column(db.Text, nullable=True)

    # ==== CONSIDERAÇÕES ====
    CONSIDERACOES_ANALISTA_GEADI = db.Column(db.Text, nullable=True)
    CONSIDERACOES_GESTOR_GEADI = db.Column(db.Text, nullable=True)

    # ==== CONTROLE ====
    STATUS_DOCUMENTO = db.Column(db.String(50), default='RASCUNHO')

    # ==== AUDITORIA ====
    USUARIO_CRIACAO = db.Column(db.String(200), nullable=True)
    USUARIO_ATUALIZACAO = db.Column(db.String(200), nullable=True)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime, nullable=True)

    # ==== PARTE FINAL DO FORMULÁRIO ====
    GRAVAME_MATRICULA = db.Column(db.Text, nullable=True)
    ACOES_NEGOCIAIS_ADMINISTRATIVAS = db.Column(db.Text, nullable=True)
    NR_PROCESSOS_JUDICIAIS = db.Column(db.Text, nullable=True)
    VARA_PROCESSO = db.Column(db.Text, nullable=True)
    FASE_PROCESSO = db.Column(db.Text, nullable=True)
    RELATORIO_ASSESSORIA_JURIDICA = db.Column(db.Text, nullable=True)
    VR_DEBITOS_SISDEX = db.Column(db.Numeric(18, 2), nullable=True)
    VR_DEBITOS_SISGEA = db.Column(db.Numeric(18, 2), nullable=True)
    VR_DEBITOS_TOTAL = db.Column(db.Numeric(18, 2), nullable=True)
    PENALIDADE_ANS_CAIXA = db.Column(db.Text, nullable=True)
    PREJUIZO_FINANCEIRO_CAIXA = db.Column(db.Text, nullable=True)
    CONSIDERACOES_GESTOR_SUMOV = db.Column(db.Text, nullable=True)  # ← ADICIONE ESTA LINHA

    # Informações de Venda
    TIPO_PAGAMENTO_VENDA = db.Column(db.String(20), nullable=True)  # ← ADICIONE ESTA LINHA

    def __repr__(self):
        return f'<DeliberacaoPagamento {self.NU_CONTRATO}>'

    @staticmethod
    def buscar_por_contrato(nu_contrato):
        """Busca uma deliberação por número de contrato"""
        return DeliberacaoPagamento.query.filter_by(
            NU_CONTRATO=nu_contrato,
            DELETED_AT=None
        ).first()

    def salvar(self):
        """Salva ou atualiza a deliberação no banco"""
        try:
            db.session.add(self)
            db.session.commit()
            return True
        except Exception as e:
            db.session.rollback()
            print(f"Erro ao salvar deliberação: {str(e)}")
            return False