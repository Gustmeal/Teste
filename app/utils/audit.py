from app import db
from app.models.audit_log import AuditLog
from flask_login import current_user
from flask import request
import json
from datetime import datetime
from sqlalchemy import text


def registrar_log(acao, entidade, entidade_id, descricao, dados_antigos=None, dados_novos=None):
    """
    Registra log de auditoria com dados do empregado

    Args:
        acao: Tipo de ação (criar, editar, excluir, login, logout, etc)
        entidade: Nome da entidade afetada
        entidade_id: ID da entidade
        descricao: Descrição da ação
        dados_antigos: Dados antes da alteração (dict)
        dados_novos: Dados após a alteração (dict)
    """
    try:
        # Dados básicos do usuário
        usuario_id = current_user.id if current_user.is_authenticated else None
        usuario_nome = current_user.nome if current_user.is_authenticated else 'Sistema'

        # Buscar dados adicionais do empregado se autenticado
        area = None
        cargo = None

        if current_user.is_authenticated and hasattr(current_user, 'empregado'):
            empregado = current_user.empregado
            if empregado:
                area = empregado.sgSuperintendencia
                cargo = empregado.dsCargo

        # Criar log na estrutura existente
        log = AuditLog(
            USUARIO_ID=usuario_id,
            USUARIO_NOME=usuario_nome,
            ACAO=acao,
            ENTIDADE=entidade,
            ENTIDADE_ID=entidade_id,
            DESCRICAO=descricao,
            DATA=datetime.utcnow(),
            IP=request.remote_addr if request else None,
            DADOS_ANTIGOS=json.dumps(dados_antigos, ensure_ascii=False) if dados_antigos else None,
            DADOS_NOVOS=json.dumps(dados_novos, ensure_ascii=False) if dados_novos else None
        )

        # Adicionar área e cargo na descrição se disponível
        if area or cargo:
            info_adicional = []
            if area:
                info_adicional.append(f"Área: {area}")
            if cargo:
                info_adicional.append(f"Cargo: {cargo}")
            log.DESCRICAO = f"{descricao} | {' | '.join(info_adicional)}"

        db.session.add(log)
        db.session.commit()

        return True
    except Exception as e:
        print(f"Erro ao registrar log: {str(e)}")
        db.session.rollback()
        return False