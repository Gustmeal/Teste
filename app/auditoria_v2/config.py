# -*- coding: utf-8 -*-
"""
app/auditoria_v2/config.py
Parâmetros da Auditoria V2 lidos do banco (AUD_TB007_CONFIG e AUD_TB006_APLICATIVO),
com cache em memória para não consultar o banco a cada requisição.

Compatível com Python 3.9 e 3.12.
"""

import threading
import time

from sqlalchemy import text

from app.auditoria_v2.util import MARCA_SQL, agora

TB_CONFIG = 'BDG.AUD_TB007_CONFIG'
TB_APLICATIVO = 'BDG.AUD_TB006_APLICATIVO'

# Valores iniciais: gravados no banco na primeira execução. Depois disso,
# vale SEMPRE o que estiver no banco (editável pela tela de configurações).
CONFIG_INICIAL = [
    ('ATIVO', '1',
     'Liga (1) ou desliga (0) toda a captura da Auditoria V2.'),
    ('ENDPOINTS_IGNORADOS',
     'static,presenca.registrar_sinal,presenca.registrar_saida,presenca.dados_painel,'
     'chat.verificar_novas_mensagens,notificacoes.obter_nao_lidas',
     'Rotas (endpoints) que NÃO são auditadas, separadas por vírgula. Use só para chamadas '
     'automáticas de alta frequência (polling), que não são ações do usuário.'),
    ('ENDPOINT_LOGIN', 'auth.login',
     'Rota de login, usada para detectar tentativas de login com falha.'),
    ('CAMPOS_SENSIVEIS', 'senha,password,senha_hash,token,csrf_token,secret',
     'Trechos de nomes de campos cujo valor é gravado como *** (nunca em claro).'),
    ('MAX_LINHAS_DETALHE', '500',
     'Máximo de linhas com valores antes/depois gravados por comando SQL. Acima disso '
     'grava o comando, os parâmetros e a quantidade de linhas afetadas.'),
    ('MAX_OBJETOS_ORM', '2000',
     'Máximo de registros detalhados por gravação via models (ORM).'),
    ('MAX_TAMANHO_VALOR', '4000',
     'Tamanho máximo (caracteres) de cada valor gravado.'),
    ('REGISTRAR_ACESSOS', '1',
     'Grava (1) ou não (0) as navegações/consultas sem alteração de dados.'),
    ('DIAS_RETENCAO', '0',
     'Dias de histórico mantidos pela rotina de limpeza (0 = guardar para sempre).'),
]

_TTL_SEGUNDOS = 60

_lock = threading.Lock()
_cache = {'config': {}, 'apps': {}, 'carregado_em': 0.0}
_engine = {'obj': None}


def definir_engine(engine):
    _engine['obj'] = engine


def _carregar():
    engine = _engine['obj']
    if engine is None:
        return
    config = {}
    apps = {}
    with engine.connect() as conn:
        for chave, valor in conn.execute(text(MARCA_SQL + ' SELECT CHAVE, VALOR FROM ' + TB_CONFIG)):
            config[chave] = valor if valor is not None else ''
        sql_apps = MARCA_SQL + ' SELECT BLUEPRINT, NO_APLICATIVO, CATEGORIA, IN_AUDITAR FROM ' + TB_APLICATIVO
        for bp, nome, categoria, auditar in conn.execute(text(sql_apps)):
            apps[bp] = {'nome': nome, 'categoria': categoria, 'auditar': bool(auditar)}
    _cache['config'] = config
    _cache['apps'] = apps


def _garantir_cache():
    if time.time() - _cache['carregado_em'] < _TTL_SEGUNDOS:
        return
    with _lock:
        if time.time() - _cache['carregado_em'] < _TTL_SEGUNDOS:
            return
        try:
            _carregar()
        except Exception:
            pass
        # Mesmo com falha, só tenta de novo depois do TTL (não martela o banco)
        _cache['carregado_em'] = time.time()


def invalidar_cache():
    _cache['carregado_em'] = 0.0


def obter(chave):
    _garantir_cache()
    valor = _cache['config'].get(chave)
    if valor is None:
        for c, v, _d in CONFIG_INICIAL:
            if c == chave:
                return v
    return valor


def obter_int(chave, padrao):
    try:
        return int(str(obter(chave)).strip())
    except Exception:
        return padrao


def obter_lista(chave):
    valor = obter(chave) or ''
    return [p.strip() for p in valor.split(',') if p.strip()]


def ativo():
    return str(obter('ATIVO')).strip() == '1'


def aplicativo(blueprint):
    """Retorna {'nome', 'categoria', 'auditar'} do blueprint (padrão se não cadastrado)."""
    _garantir_cache()
    if not blueprint:
        return {'nome': 'Portal (geral)', 'categoria': None, 'auditar': True}
    info = _cache['apps'].get(blueprint)
    if info:
        return info
    return {'nome': nome_padrao_blueprint(blueprint), 'categoria': None, 'auditar': True}


def nome_padrao_blueprint(blueprint):
    return str(blueprint).replace('_bp', '').replace('_', ' ').strip().title()


def semear(engine, blueprints):
    """
    Grava no banco os parâmetros iniciais e cada blueprint registrado que ainda
    não existir em AUD_TB006_APLICATIVO. Nunca sobrescreve o que já está lá.
    """
    with engine.begin() as conn:
        existentes = set(r[0] for r in conn.execute(text(MARCA_SQL + ' SELECT CHAVE FROM ' + TB_CONFIG)))
        for chave, valor, descricao in CONFIG_INICIAL:
            if chave not in existentes:
                conn.execute(
                    text(MARCA_SQL + ' INSERT INTO ' + TB_CONFIG +
                         ' (CHAVE, VALOR, DESCRICAO, UPDATED_AT, UPDATED_POR) VALUES (:c, :v, :d, :dt, :u)'),
                    {'c': chave, 'v': valor, 'd': descricao, 'dt': agora(), 'u': 'SISTEMA'}
                )
        apps = set(r[0] for r in conn.execute(text(MARCA_SQL + ' SELECT BLUEPRINT FROM ' + TB_APLICATIVO)))
        for bp in blueprints:
            if bp not in apps:
                conn.execute(
                    text(MARCA_SQL + ' INSERT INTO ' + TB_APLICATIVO +
                         ' (BLUEPRINT, NO_APLICATIVO, CATEGORIA, IN_AUDITAR, CREATED_AT) '
                         'VALUES (:b, :n, NULL, 1, :dt)'),
                    {'b': bp, 'n': nome_padrao_blueprint(bp), 'dt': agora()}
                )
    invalidar_cache()
