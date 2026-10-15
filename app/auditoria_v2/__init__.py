# -*- coding: utf-8 -*-
"""
app/auditoria_v2
Auditoria V2 do Portal GEINC - captura AUTOMÁTICA de tudo, sem alterar os módulos.

Camadas:
    captura_http.py  -> cada ação do usuário (tela, rota, parâmetros, erro, download, login...)
    captura_banco.py -> cada gravação no banco (ORM e SQL bruto), com antes/depois campo a campo
    gravador.py      -> grava em lote, em segundo plano (o usuário nunca espera)
    config.py        -> parâmetros e nomes de aplicativos vindos do banco

Uso (já feito no create_app):
    from app.auditoria_v2 import iniciar_auditoria_v2
    iniciar_auditoria_v2(app)

Registrar um evento de negócio manualmente (opcional, nunca obrigatório):
    from app.auditoria_v2 import registrar_evento
    registrar_evento('APROVACAO', 'Deliberação aprovada', entidade='deliberacao', entidade_id=10)

Compatível com Python 3.9 e 3.12.
"""

import os
import traceback

from app.auditoria_v2.util import agora, cortar, para_json


def iniciar_auditoria_v2(app):
    """Liga a Auditoria V2. Nunca impede o portal de subir: qualquer falha só vai para o log."""
    from app import db
    from app.auditoria_v2 import captura_banco, captura_http, config
    from app.auditoria_v2.gravador import gravador

    try:
        with app.app_context():
            engine = db.engine
            config.definir_engine(engine)
            try:
                config.semear(engine, sorted(app.blueprints.keys()))
            except Exception:
                _log('Auditoria V2: falha ao semear configuração: ' + traceback.format_exc())
            pasta_logs = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
                os.path.abspath(__file__)))), 'logs')
            gravador.iniciar(engine, pasta_logs)
            captura_banco.instalar(engine)
            captura_http.instalar(app)
    except Exception:
        _log('Auditoria V2: falha ao iniciar: ' + traceback.format_exc())


def registrar_evento(tipo, descricao, entidade=None, entidade_id=None, detalhes=None, nivel='INFO'):
    """Registra um evento de negócio ligado à ação atual do usuário. Nunca lança exceção."""
    try:
        from app.auditoria_v2 import contexto
        pacote = contexto.novo_pacote()
        pacote['eventos'].append({
            'ID_CORRELACAO': contexto.correlacao_atual(), 'DT_EVENTO': agora(),
            'TIPO': cortar(str(tipo).upper(), 40), 'NIVEL': nivel,
            'ENTIDADE': cortar(entidade, 100),
            'ENTIDADE_ID': cortar(entidade_id, 100) if entidade_id is not None else None,
            'DESCRICAO': cortar(descricao, 4000), 'DETALHES': para_json(detalhes),
        })
        contexto.entregar(pacote)
        return True
    except Exception:
        _log('Auditoria V2: falha em registrar_evento: ' + traceback.format_exc())
        return False


def _log(mensagem):
    try:
        from app.utils.log_seguro import log_erro
        log_erro(mensagem)
    except Exception:
        pass
