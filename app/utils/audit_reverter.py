import json
from flask import request
from flask_login import current_user
from app import db
from app.models.audit_log import AuditLog
from app.models.edital import Edital
from app.models.periodo import PeriodoAvaliacao
from app.models.empresa_participante import EmpresaParticipante
from app.models.meta_avaliacao import MetaAvaliacao
from app.models.usuario import Usuario
from app.utils.audit import registrar_log
from datetime import datetime
from sqlalchemy import text


class AuditReverter:
    """Classe responsável por reverter ações registradas nos logs de auditoria"""

    # Mapeamento de entidades para modelos
    ENTIDADE_MODELO = {
        'edital': Edital,
        'periodo': PeriodoAvaliacao,
        'empresa': EmpresaParticipante,
        'meta': MetaAvaliacao,
        'usuario': Usuario
    }

    @staticmethod
    def pode_reverter(log):
        """Verifica se uma ação pode ser revertida"""
        # Não pode reverter se já foi revertido
        if log.REVERTIDO:
            return False, "Esta ação já foi revertida"

        # Verificar se há ações dependentes posteriores
        if log.ACAO == 'excluir':
            # Pode reverter exclusão sempre (vai recriar o registro)
            return True, None

        if log.ACAO == 'criar':
            # Verificar se o registro ainda existe
            modelo = AuditReverter.ENTIDADE_MODELO.get(log.ENTIDADE)
            if modelo:
                registro = modelo.query.get(log.ENTIDADE_ID)
                if not registro:
                    return False, "O registro já foi excluído"

                # Verificar se há edições posteriores
                edicoes_posteriores = AuditLog.query.filter(
                    AuditLog.ENTIDADE == log.ENTIDADE,
                    AuditLog.ENTIDADE_ID == log.ENTIDADE_ID,
                    AuditLog.ACAO == 'editar',
                    AuditLog.DATA > log.DATA,
                    AuditLog.REVERTIDO == False
                ).count()

                if edicoes_posteriores > 0:
                    return False, "Existem edições posteriores que devem ser revertidas primeiro"

        if log.ACAO == 'editar':
            # Verificar se o registro ainda existe
            modelo = AuditReverter.ENTIDADE_MODELO.get(log.ENTIDADE)
            if modelo:
                registro = modelo.query.get(log.ENTIDADE_ID)
                if not registro:
                    return False, "O registro foi excluído"

        return True, None

    @staticmethod
    def reverter_acao(log_id):
        """Reverte uma ação específica"""
        try:
            log = AuditLog.query.get(log_id)
            if not log:
                return False, "Log não encontrado"

            # Verificar se pode reverter
            pode, motivo = AuditReverter.pode_reverter(log)
            if not pode:
                return False, motivo

            # Executar reversão baseada no tipo de ação
            if log.ACAO == 'criar':
                sucesso, mensagem = AuditReverter._reverter_criacao(log)
            elif log.ACAO == 'editar':
                sucesso, mensagem = AuditReverter._reverter_edicao(log)
            elif log.ACAO == 'excluir':
                sucesso, mensagem = AuditReverter._reverter_exclusao(log)
            else:
                return False, "Tipo de ação não suportada para reversão"

            if sucesso:
                # Marcar log como revertido
                log.REVERTIDO = True
                log.REVERTIDO_POR = current_user.id
                log.REVERTIDO_EM = datetime.utcnow()

                # Registrar a reversão
                descricao_reversao = f"Revertida ação: {log.DESCRICAO}"
                novo_log = registrar_log(
                    acao='reverter',
                    entidade=log.ENTIDADE,
                    entidade_id=log.ENTIDADE_ID,
                    descricao=descricao_reversao,
                    dados_antigos=log.DADOS_NOVOS,  # O que estava novo volta a ser antigo
                    dados_novos=log.DADOS_ANTIGOS  # O que era antigo volta a ser novo
                )

                # Associar os logs
                if novo_log:
                    log.LOG_REVERSAO_ID = novo_log.ID

                db.session.commit()
                return True, mensagem
            else:
                db.session.rollback()
                return False, mensagem

        except Exception as e:
            db.session.rollback()
            return False, f"Erro ao reverter: {str(e)}"

    @staticmethod
    def _reverter_criacao(log):
        """Reverte uma ação de criação (deleta o registro)"""
        try:
            modelo = AuditReverter.ENTIDADE_MODELO.get(log.ENTIDADE)
            if not modelo:
                return False, "Modelo não encontrado"

            registro = modelo.query.get(log.ENTIDADE_ID)
            if not registro:
                return False, "Registro não encontrado"

            # Soft delete se a tabela tiver DELETED_AT
            if hasattr(registro, 'DELETED_AT'):
                registro.DELETED_AT = datetime.utcnow()
            else:
                db.session.delete(registro)

            return True, "Criação revertida com sucesso"

        except Exception as e:
            return False, f"Erro ao reverter criação: {str(e)}"

    @staticmethod
    def _reverter_edicao(log):
        """Reverte uma ação de edição (restaura dados anteriores)"""
        try:
            if not log.DADOS_ANTIGOS:
                return False, "Não há dados anteriores para restaurar"

            modelo = AuditReverter.ENTIDADE_MODELO.get(log.ENTIDADE)
            if not modelo:
                return False, "Modelo não encontrado"

            registro = modelo.query.get(log.ENTIDADE_ID)
            if not registro:
                return False, "Registro não encontrado"

            # Restaurar dados anteriores
            dados_antigos = json.loads(log.DADOS_ANTIGOS)
            for campo, valor in dados_antigos.items():
                if hasattr(registro, campo):
                    # Tratar campos de data
                    if 'DATA' in campo or 'DT_' in campo:
                        if valor:
                            valor = datetime.fromisoformat(valor.replace('Z', '+00:00'))
                    setattr(registro, campo, valor)

            # Atualizar timestamp de modificação
            if hasattr(registro, 'UPDATED_AT'):
                registro.UPDATED_AT = datetime.utcnow()

            return True, "Edição revertida com sucesso"

        except Exception as e:
            return False, f"Erro ao reverter edição: {str(e)}"

    @staticmethod
    def _reverter_exclusao(log):
        """Reverte uma ação de exclusão (restaura o registro)"""
        try:
            if not log.DADOS_ANTIGOS:
                return False, "Não há dados para restaurar"

            modelo = AuditReverter.ENTIDADE_MODELO.get(log.ENTIDADE)
            if not modelo:
                return False, "Modelo não encontrado"

            # Verificar se é soft delete
            registro_existente = db.session.query(modelo).filter(
                modelo.ID == log.ENTIDADE_ID
            ).first()

            if registro_existente and hasattr(registro_existente, 'DELETED_AT'):
                # Reverter soft delete
                registro_existente.DELETED_AT = None
                if hasattr(registro_existente, 'UPDATED_AT'):
                    registro_existente.UPDATED_AT = datetime.utcnow()
            else:
                # Recriar registro
                dados_antigos = json.loads(log.DADOS_ANTIGOS)
                novo_registro = modelo()

                for campo, valor in dados_antigos.items():
                    if hasattr(novo_registro, campo):
                        # Tratar campos de data
                        if 'DATA' in campo or 'DT_' in campo:
                            if valor:
                                valor = datetime.fromisoformat(valor.replace('Z', '+00:00'))
                        setattr(novo_registro, campo, valor)

                # Garantir que o ID seja o mesmo
                if 'ID' in dados_antigos:
                    novo_registro.ID = dados_antigos['ID']

                # Resetar timestamps
                if hasattr(novo_registro, 'CREATED_AT'):
                    novo_registro.CREATED_AT = datetime.utcnow()
                if hasattr(novo_registro, 'DELETED_AT'):
                    novo_registro.DELETED_AT = None

                db.session.add(novo_registro)

            return True, "Exclusão revertida com sucesso"

        except Exception as e:
            return False, f"Erro ao reverter exclusão: {str(e)}"