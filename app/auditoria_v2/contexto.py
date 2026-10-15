# -*- coding: utf-8 -*-
"""
app/auditoria_v2/contexto.py
Liga cada item capturado (comando SQL, alteração, evento) à ação do usuário que o originou.

  - Dentro de uma requisição: os itens ficam em g._aud e são enviados juntos no final
    da requisição, já com o usuário, o aplicativo e o ID de correlação preenchidos.
  - Fora de requisição (rotinas agendadas, threads): os itens vão direto para a fila,
    identificados como SISTEMA + nome da thread.

Compatível com Python 3.9 e 3.12.
"""

import threading

from flask import g, has_request_context

from app.auditoria_v2 import config
from app.auditoria_v2.gravador import gravador, pacote_vazio
from app.auditoria_v2.util import novo_codigo

_tl = threading.local()


# ----------------------------------------------------------------------
# Controle por thread
# ----------------------------------------------------------------------
def em_flush():
    return getattr(_tl, 'em_flush', 0) > 0


def entrar_flush():
    _tl.em_flush = getattr(_tl, 'em_flush', 0) + 1


def sair_flush(zerar=False):
    if zerar:
        _tl.em_flush = 0
    else:
        _tl.em_flush = max(0, getattr(_tl, 'em_flush', 0) - 1)


def correlacao_sem_requisicao():
    """Fora de requisição, cada transação ganha um ID próprio."""
    correl = getattr(_tl, 'correlacao', None)
    if not correl:
        correl = novo_codigo()
        _tl.correlacao = correl
    return correl


def encerrar_correlacao_sem_requisicao():
    _tl.correlacao = None


# ----------------------------------------------------------------------
# Requisição atual
# ----------------------------------------------------------------------
def aud_requisicao():
    """Dados de auditoria da requisição atual, ou None fora de requisição."""
    if not has_request_context():
        return None
    try:
        return g.get('_aud')
    except Exception:
        return None


def requisicao_ignorada():
    aud = aud_requisicao()
    return aud is not None and aud.get('ignorar')


def correlacao_atual():
    aud = aud_requisicao()
    if aud and aud.get('id'):
        return aud['id']
    return correlacao_sem_requisicao()


def usuario_logado():
    """
    Lê o usuário logado SEM disparar consulta ao banco (usa o que o Flask-Login
    já carregou em g._login_user). Retorna dict ou None.
    """
    if not has_request_context():
        return None
    try:
        usuario = g.get('_login_user')
    except Exception:
        return None
    if usuario is None or not getattr(usuario, 'is_authenticated', False):
        return None
    empregado = getattr(usuario, 'empregado', None)
    if not isinstance(empregado, dict):
        empregado = {}
    return {
        'id': getattr(usuario, 'id', None),
        'nome': getattr(usuario, 'nome', None),
        'email': getattr(usuario, 'email', None),
        'perfil': getattr(usuario, 'perfil', None),
        'area': empregado.get('area'),
        'cargo': empregado.get('cargo'),
    }


def usuario_sistema():
    return {'id': None, 'nome': 'SISTEMA ({})'.format(threading.current_thread().name),
            'email': None, 'perfil': None, 'area': None, 'cargo': None}


# ----------------------------------------------------------------------
# Envio
# ----------------------------------------------------------------------
def preencher(pacote, usuario, blueprint, nome_aplicativo):
    for nome in ('comandos', 'alteracoes', 'campos', 'eventos'):
        for item in pacote.get(nome) or []:
            if item.get('USUARIO_ID') is None and item.get('USUARIO_NOME') is None:
                item['USUARIO_ID'] = usuario.get('id')
                item['USUARIO_NOME'] = usuario.get('nome')
            if nome != 'campos':
                item.setdefault('BLUEPRINT', blueprint)
                if item.get('BLUEPRINT') is None:
                    item['BLUEPRINT'] = blueprint
                if item.get('APLICATIVO') is None:
                    item['APLICATIVO'] = nome_aplicativo
    return pacote


def entregar(pacote):
    """
    Entrega itens capturados: guarda na requisição atual (para enviar no final)
    ou, fora de requisição / requisição já finalizada, envia direto para a fila.
    """
    if not pacote:
        return
    aud = aud_requisicao()
    if aud is not None and aud.get('ignorar'):
        return
    if aud is not None and not aud.get('finalizado'):
        for nome, itens in pacote.items():
            if itens:
                aud['itens'][nome].extend(itens)
        return
    if aud is not None:
        usuario = aud.get('usuario') or usuario_logado() or usuario_sistema()
        blueprint = aud.get('blueprint')
        nome_app = aud.get('aplicativo')
    else:
        usuario = usuario_sistema()
        blueprint = None
        nome_app = 'Rotina do sistema'
    gravador.enfileirar(preencher(pacote, usuario, blueprint, nome_app))


def novo_pacote():
    return pacote_vazio()


def captura_ligada():
    try:
        return config.ativo()
    except Exception:
        return True
