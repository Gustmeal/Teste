import json
from flask import request
from flask_login import current_user
from app import db
from app.models.audit_log import AuditLog


def registrar_log(acao, entidade, entidade_id, descricao, dados_antigos=None, dados_novos=None):
    """
    Registra uma ação de auditoria no sistema.

    Args:
        acao: Tipo de ação ('criar', 'editar', 'excluir')
        entidade: Tipo de entidade ('edital', 'periodo', etc)
        entidade_id: ID da entidade
        descricao: Descrição da ação
        dados_antigos: Dados antes da modificação (opcional)
        dados_novos: Dados após a modificação (opcional)
    """
    try:
        # Converter dados para JSON se fornecidos
        dados_antigos_json = json.dumps(dados_antigos) if dados_antigos else None
        dados_novos_json = json.dumps(dados_novos) if dados_novos else None

        # Obter endereço IP
        ip = request.remote_addr

        # Criar log
        log = AuditLog(
            USUARIO_ID=current_user.id,
            USUARIO_NOME=current_user.nome,
            ACAO=acao,
            ENTIDADE=entidade,
            ENTIDADE_ID=entidade_id,
            DESCRICAO=descricao,
            IP=ip,
            DADOS_ANTIGOS=dados_antigos_json,
            DADOS_NOVOS=dados_novos_json
        )

        db.session.add(log)
        db.session.commit()
    except Exception as e:
        print(f"Erro ao registrar log: {str(e)}")
        # Não lançar exceção para não interromper a operação principal