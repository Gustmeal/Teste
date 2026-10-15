# -*- coding: utf-8 -*-
"""
app/auditoria_v2/util.py
Funções utilitárias da Auditoria V2: geração de códigos, serialização segura
de valores e mascaramento de campos sensíveis.

Compatível com Python 3.9 e 3.12.
"""

import json
import uuid
from datetime import datetime, date, time
from decimal import Decimal

# Prefixo colocado em TODO comando SQL da própria auditoria.
# A captura de banco ignora qualquer comando que comece com ele (evita auditar a auditoria).
MARCA_SQL = '/*AUDV2*/'

# Usado quando a configuração do banco ainda não foi carregada
CAMPOS_SENSIVEIS_PADRAO = ('senha', 'password', 'senha_hash', 'token', 'csrf_token', 'secret')


def novo_codigo():
    """Código único de 32 caracteres (UUID4 em hexadecimal)."""
    return uuid.uuid4().hex


def agora():
    """Data/hora local do servidor (horário de Brasília no servidor do portal)."""
    return datetime.now()


def valor_para_texto(valor, limite=4000):
    """Converte qualquer valor vindo do banco/formulário em texto legível e limitado."""
    if valor is None:
        return None
    try:
        if isinstance(valor, (bytes, bytearray, memoryview)):
            return '<binario {} bytes>'.format(len(bytes(valor)))
        if isinstance(valor, datetime):
            texto = valor.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(valor, date):
            texto = valor.strftime('%Y-%m-%d')
        elif isinstance(valor, time):
            texto = valor.strftime('%H:%M:%S')
        elif isinstance(valor, Decimal):
            texto = format(valor, 'f')
        elif isinstance(valor, bool):
            texto = '1' if valor else '0'
        elif isinstance(valor, float):
            texto = repr(valor)
        else:
            texto = str(valor)
    except Exception:
        texto = '<valor nao serializavel>'
    if limite and len(texto) > limite:
        texto = texto[:limite - 15] + '...[cortado]'
    return texto


def _normalizar(obj, limite_valor):
    if isinstance(obj, dict):
        return dict((str(k), _normalizar(v, limite_valor)) for k, v in obj.items())
    if isinstance(obj, (list, tuple, set)):
        return [_normalizar(v, limite_valor) for v in obj]
    if obj is None or isinstance(obj, (int, str)) and not isinstance(obj, bool):
        if isinstance(obj, str) and limite_valor and len(obj) > limite_valor:
            return obj[:limite_valor - 15] + '...[cortado]'
        return obj
    return valor_para_texto(obj, limite_valor)


def para_json(obj, limite_valor=4000, limite_total=200000):
    """Serializa para JSON sem nunca lançar exceção."""
    if obj is None:
        return None
    try:
        texto = json.dumps(_normalizar(obj, limite_valor), ensure_ascii=False, default=str)
    except Exception:
        texto = json.dumps({'erro': 'nao serializavel'})
    if limite_total and len(texto) > limite_total:
        texto = texto[:limite_total - 20] + '...[cortado]'
    return texto


def eh_sensivel(nome, campos_sensiveis=None):
    """Indica se o nome de um campo/coluna/parâmetro deve ter o valor mascarado."""
    if not nome:
        return False
    nome_min = str(nome).lower()
    for item in (campos_sensiveis or CAMPOS_SENSIVEIS_PADRAO):
        item = item.strip().lower()
        if item and item in nome_min:
            return True
    return False


def mascarar_dict(dados, campos_sensiveis=None):
    """Retorna cópia do dicionário com os valores sensíveis trocados por ***."""
    if not dados:
        return dados
    saida = {}
    for chave, valor in dados.items():
        saida[chave] = '***' if eh_sensivel(chave, campos_sensiveis) and valor not in (None, '') else valor
    return saida


def cortar(texto, limite):
    if texto is None:
        return None
    texto = str(texto)
    if len(texto) > limite:
        return texto[:limite - 15] + '...[cortado]'
    return texto
