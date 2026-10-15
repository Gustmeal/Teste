# -*- coding: utf-8 -*-
"""
app/auditoria_v2/captura_banco.py
Captura AUTOMÁTICA de tudo que é gravado no banco, sem precisar alterar os módulos.

Duas portas de entrada são interceptadas:

1) ORM (models + db.session.add/commit)
   Evento after_flush da Session: para cada objeto incluído, alterado ou excluído,
   o SQLAlchemy informa o valor ANTIGO e o NOVO de cada coluna. Nada é consultado
   a mais no banco.

2) Qualquer comando SQL (text(), pandas, bulk_save_objects, query.update()...)
   Eventos before/after_cursor_execute do Engine: todo INSERT/UPDATE/DELETE/MERGE/EXEC
   que passa pelo SQLAlchemy é registrado com o comando, os parâmetros, as linhas
   afetadas e a duração. Para UPDATE/DELETE simples, é feita uma "foto" das linhas
   ANTES (SELECT com o mesmo WHERE) e DEPOIS (pela chave primária), dentro da mesma
   transação, gerando o antes/depois campo a campo.
   Para INSERT, os valores são lidos dos próprios parâmetros do comando.

Só é gravado o que foi CONFIRMADO (commit). Se a transação sofrer rollback, os
registros alterados são descartados e o comando fica marcado como DESFEITA.

Compatível com Python 3.9 e 3.12.
"""

import re
import time
import traceback

from sqlalchemy import event, inspect as sa_inspect, select
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import TextClause

from app.auditoria_v2 import config, contexto
from app.auditoria_v2.util import (MARCA_SQL, agora, cortar, eh_sensivel, novo_codigo,
                                   para_json, valor_para_texto)

# Tabelas que nunca geram detalhe de alteração (a própria auditoria e o log legado)
_TABELAS_EXCLUIDAS = ('AUD_TB00', 'APK_TB001_AUDIT_LOG')

_CHAVE_PEND = 'aud_v2_pendentes'

_PALAVRAS_DML = ('INSERT', 'UPDATE', 'DELETE', 'MERGE')

_RE_INICIO = re.compile(r'^\s*(?:(?:/\*.*?\*/)|(?:--[^\n]*\n)|\s)*', re.S)
_RE_PRIMEIRA = re.compile(r'^\s*([A-Za-z_]+)')
_NOME = r'(\[[^\]]+\]|[\w#@$]+)'
_RE_TABELA_NOME = r'(?P<tab>{0}(?:\s*\.\s*{0}){{0,3}})'.format(_NOME)
_RE_TOP = r'(?:TOP\s*\(\s*\d+\s*\)\s*(?:PERCENT\s+)?)?'

_RE_TABELA = {
    'INSERT': re.compile(r'^\s*INSERT\s+' + _RE_TOP + r'(?:INTO\s+)?' + _RE_TABELA_NOME, re.I),
    'UPDATE': re.compile(r'^\s*UPDATE\s+' + _RE_TOP + _RE_TABELA_NOME, re.I),
    'DELETE': re.compile(r'^\s*DELETE\s+' + _RE_TOP + r'(?:FROM\s+)?' + _RE_TABELA_NOME, re.I),
    'MERGE': re.compile(r'^\s*MERGE\s+' + _RE_TOP + r'(?:INTO\s+)?' + _RE_TABELA_NOME, re.I),
    'TRUNCATE': re.compile(r'^\s*TRUNCATE\s+TABLE\s+' + _RE_TABELA_NOME, re.I),
    'PROCEDURE': re.compile(r'^\s*EXEC(?:UTE)?\s+(?:@\w+\s*=\s*)?' + _RE_TABELA_NOME, re.I),
}

_RE_UPDATE = re.compile(
    r'^\s*UPDATE\s+' + _RE_TOP + _RE_TABELA_NOME +
    r'(?:\s+(?:AS\s+)?(?P<alias>(?!SET\b)[A-Za-z_]\w*))?\s+SET\s', re.I)
_RE_DELETE = re.compile(
    r'^\s*DELETE\s+' + _RE_TOP + r'(?:FROM\s+)?' + _RE_TABELA_NOME +
    r'(?:\s+(?:AS\s+)?(?P<alias>(?!WHERE\b|FROM\b|OUTPUT\b|OPTION\b)[A-Za-z_]\w*))?', re.I)
_RE_INSERT = re.compile(
    r'^\s*INSERT\s+(?:INTO\s+)?' + _RE_TABELA_NOME + r'\s*\((?P<cols>[^()]*)\)\s*', re.I)

# Palavras que, no nível principal da cláusula WHERE, indicam outro comando no mesmo lote.
# Nesses casos a foto antes/depois NÃO é tirada (segurança: a foto é sempre só um SELECT).
_PALAVRAS_PROIBIDAS_WHERE = ('INSERT', 'UPDATE', 'DELETE', 'MERGE', 'EXEC', 'EXECUTE', 'DROP',
                             'CREATE', 'ALTER', 'TRUNCATE', 'DECLARE', 'SET', 'BEGIN', 'IF',
                             'SELECT', 'OUTPUT', 'FROM', 'WITH', 'GRANT', 'REVOKE', 'DENY',
                             'BACKUP', 'RESTORE', 'USE', 'GO', 'WAITFOR', 'COMMIT', 'ROLLBACK')

_cache_pk = {}


# ======================================================================
# Análise de texto SQL (respeitando aspas, colchetes, comentários e parênteses)
# ======================================================================
def _sem_comentario_inicial(sql):
    return _RE_INICIO.sub('', sql, count=1)


def _varrer(sql):
    """
    Percorre o SQL devolvendo (posição, caractere, profundidade) apenas para trechos
    que estão FORA de strings, colchetes e comentários.
    """
    i = 0
    n = len(sql)
    prof = 0
    while i < n:
        c = sql[i]
        if c == "'":
            i += 1
            while i < n:
                if sql[i] == "'":
                    if i + 1 < n and sql[i + 1] == "'":
                        i += 2
                        continue
                    break
                i += 1
            i += 1
            continue
        if c == '[':
            fim = sql.find(']', i)
            i = n if fim < 0 else fim + 1
            continue
        if c == '-' and sql[i:i + 2] == '--':
            fim = sql.find('\n', i)
            i = n if fim < 0 else fim + 1
            continue
        if c == '/' and sql[i:i + 2] == '/*':
            fim = sql.find('*/', i + 2)
            i = n if fim < 0 else fim + 2
            continue
        if c == '(':
            prof += 1
        elif c == ')':
            prof -= 1
        yield i, c, prof
        i += 1


def _contar_marcadores(trecho):
    return sum(1 for _i, c, _p in _varrer(trecho) if c == '?')


def _palavras_topo(sql):
    """Lista (posição, PALAVRA) das palavras que estão no nível principal (fora de parênteses)."""
    resultado = []
    atual = []
    inicio = None
    prof_inicio = 0
    anterior = -2
    for i, c, prof in _varrer(sql):
        eh_letra = c.isalnum() or c == '_'
        if eh_letra and atual and i == anterior + 1:
            atual.append(c)
        else:
            if atual and prof_inicio == 0:
                resultado.append((inicio, ''.join(atual).upper()))
            atual = []
            if eh_letra:
                atual = [c]
                inicio = i
                prof_inicio = prof
        anterior = i
    if atual and prof_inicio == 0:
        resultado.append((inicio, ''.join(atual).upper()))
    return resultado


def _tem_ponto_virgula_topo(sql):
    texto = sql.rstrip().rstrip(';')
    return any(c == ';' for _i, c, _p in _varrer(texto))


def _dividir_virgulas_topo(trecho):
    partes = []
    inicio = 0
    for i, c, prof in _varrer(trecho):
        if c == ',' and prof == 0:
            partes.append(trecho[inicio:i])
            inicio = i + 1
    partes.append(trecho[inicio:])
    return [p.strip() for p in partes]


def _grupos_values(trecho):
    """Separa 'VALUES (a, b), (c, d)' em ['a, b', 'c, d']."""
    grupos = []
    inicio = None
    for i, c, prof in _varrer(trecho):
        if c == '(' and prof == 1:
            inicio = i + 1
        elif c == ')' and prof == 0 and inicio is not None:
            grupos.append(trecho[inicio:i])
            inicio = None
    return grupos


def _limpar_nome(nome):
    partes = re.split(r'\s*\.\s*', nome.strip())
    return '.'.join(p.strip().strip('[]') for p in partes)


def _nome_registro(nome):
    """
    Nome gravado na auditoria: sempre ESQUEMA.TABELA (sem o nome do banco), para que
    'BDDASHBOARDBI.BDG.X', '[BDG].[X]' e o nome usado pelo model caiam no mesmo histórico.
    """
    if not nome:
        return nome
    partes = [p for p in _limpar_nome(nome).split('.') if p]
    return '.'.join(partes[-2:]) if len(partes) >= 2 else '.'.join(partes)


def _tabela_excluida(tabela):
    if not tabela:
        return False
    base = tabela.split('.')[-1].upper()
    if base.startswith('#') or base.startswith('@'):
        return True
    for prefixo in _TABELAS_EXCLUIDAS:
        if base.startswith(prefixo):
            return True
    return False


def classificar(sql):
    """Retorna (OPERACAO, TABELA) ou (None, None) para leituras."""
    corpo = _sem_comentario_inicial(sql)
    m = _RE_PRIMEIRA.match(corpo)
    if not m:
        return None, None
    palavra = m.group(1).upper()
    if palavra in ('SELECT', 'SET', 'DECLARE', 'USE', 'PRINT', 'WAITFOR', 'RAISERROR', 'THROW'):
        # Lotes que começam com SET/DECLARE mas escrevem em tabela
        if palavra in ('SET', 'DECLARE'):
            for _pos, p in _palavras_topo(corpo):
                if p in _PALAVRAS_DML:
                    return 'LOTE', None
        return None, None
    if palavra == 'WITH':
        for pos, p in _palavras_topo(corpo):
            if p in _PALAVRAS_DML:
                operacao = p
                m2 = _RE_TABELA[operacao].match(corpo[pos:])
                return operacao, (_nome_registro(m2.group('tab')) if m2 else None)
        return None, None
    if palavra in ('EXEC', 'EXECUTE'):
        palavra = 'PROCEDURE'
    if palavra in _RE_TABELA:
        m2 = _RE_TABELA[palavra].match(corpo)
        return palavra, (_nome_registro(m2.group('tab')) if m2 else None)
    if palavra in ('CREATE', 'ALTER', 'DROP'):
        return 'DDL', None
    if palavra in ('BEGIN', 'IF', 'WHILE'):
        for _pos, p in _palavras_topo(corpo):
            if p in _PALAVRAS_DML:
                return 'LOTE', None
        return None, None
    return None, None


# ======================================================================
# Fotos antes / depois (UPDATE e DELETE)
# ======================================================================
def _plano_foto(sql, operacao):
    """
    Monta o SELECT que fotografa as linhas atingidas por um UPDATE/DELETE.
    Retorna dict {tabela, alias, where, inicio_param, qtd_param} ou None quando o
    comando não é simples o bastante para ser fotografado com segurança.
    """
    corpo = _sem_comentario_inicial(sql).rstrip().rstrip(';')
    if _tem_ponto_virgula_topo(corpo):
        return None
    m = (_RE_UPDATE if operacao == 'UPDATE' else _RE_DELETE).match(corpo)
    if not m:
        return None
    tabela = m.group('tab')
    alias = m.group('alias')
    resto_inicio = m.end()
    resto = corpo[resto_inicio:]
    palavras = _palavras_topo(resto)
    nomes = [p for _pos, p in palavras]
    if 'OUTPUT' in nomes or 'FROM' in nomes:
        return None
    pos_where = None
    for pos, p in palavras:
        if p == 'WHERE':
            pos_where = pos
            break
    if operacao == 'DELETE' and pos_where is None and resto.strip():
        # Sobrou algo que não é WHERE (ex.: OPTION) - não arrisca
        return None
    if pos_where is None:
        where = ''
        abs_where = len(corpo)
    else:
        where = resto[pos_where + len('WHERE'):]
        abs_where = resto_inicio + pos_where + len('WHERE')
        for pos, p in _palavras_topo(where):
            if p in _PALAVRAS_PROIBIDAS_WHERE or p == 'OPTION':
                return None
    return {
        'tabela': tabela,
        'alias': alias,
        'where': where.strip(),
        'inicio_param': _contar_marcadores(corpo[:abs_where]),
        'qtd_param': _contar_marcadores(where),
    }


def _executar_select(dbapi_conn, sql, params):
    cursor = dbapi_conn.cursor()
    try:
        cursor.execute(sql, params) if params else cursor.execute(sql)
        colunas = [d[0] for d in (cursor.description or [])]
        linhas = cursor.fetchall()
        return colunas, [list(l) for l in linhas]
    finally:
        try:
            cursor.close()
        except Exception:
            pass


def _chave_primaria(dbapi_conn, tabela):
    chave = tabela.upper()
    if chave in _cache_pk:
        return _cache_pk[chave]
    colunas = []
    try:
        _cols, linhas = _executar_select(
            dbapi_conn,
            "SELECT c.name FROM sys.indexes i "
            "JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
            "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
            "WHERE i.is_primary_key = 1 AND i.object_id = OBJECT_ID(?) ORDER BY ic.key_ordinal",
            (tabela,))
        colunas = [l[0] for l in linhas]
    except Exception:
        colunas = []
    _cache_pk[chave] = colunas
    return colunas


def _tirar_foto(dbapi_conn, plano, params, limite):
    origem = plano['tabela'] + ((' AS ' + plano['alias']) if plano['alias'] else '')
    sql = 'SELECT TOP ({}) * FROM {}'.format(int(limite) + 1, origem)
    if plano['where']:
        sql += ' WHERE ' + plano['where']
    p = tuple(params[plano['inicio_param']:plano['inicio_param'] + plano['qtd_param']])
    colunas, linhas = _executar_select(dbapi_conn, sql, p)
    truncado = len(linhas) > limite
    return colunas, linhas[:limite], truncado


def _foto_depois_por_pk(dbapi_conn, plano, colunas, linhas, pk):
    idx = [colunas.index(c) for c in pk]
    resultado_cols = None
    resultado = []
    por_lote = max(1, 900 // max(1, len(pk)))
    for inicio in range(0, len(linhas), por_lote):
        parte = linhas[inicio:inicio + por_lote]
        condicoes = []
        params = []
        for linha in parte:
            condicoes.append('(' + ' AND '.join('[{}] = ?'.format(c) for c in pk) + ')')
            params.extend(linha[i] for i in idx)
        sql = 'SELECT * FROM {} WHERE {}'.format(plano['tabela'], ' OR '.join(condicoes))
        cols, lins = _executar_select(dbapi_conn, sql, tuple(params))
        resultado_cols = cols
        resultado.extend(lins)
    return resultado_cols or colunas, resultado


# ======================================================================
# Montagem dos itens de auditoria
# ======================================================================
def _limites():
    return (config.obter_int('MAX_LINHAS_DETALHE', 500),
            config.obter_int('MAX_TAMANHO_VALOR', 4000),
            config.obter_lista('CAMPOS_SENSIVEIS'))


def _linha_dict(colunas, linha, limite_valor, sensiveis):
    dados = {}
    for col, valor in zip(colunas, linha):
        if eh_sensivel(col, sensiveis) and valor not in (None, ''):
            dados[col] = '***'
        else:
            dados[col] = valor_para_texto(valor, limite_valor)
    return dados


def _texto_chave(dados, pk, colunas):
    if pk and all(c in dados for c in pk):
        return cortar('|'.join('{}={}'.format(c, dados.get(c)) for c in pk), 500)
    usar = colunas[:3]
    return cortar('(sem PK) ' + '|'.join('{}={}'.format(c, dados.get(c)) for c in usar), 500)


def _nova_alteracao(base, origem, operacao, tabela, chave, antes, depois, cod_comando=None):
    cod = novo_codigo()
    alterados = None
    campos = []
    if operacao == 'UPDATE' and antes is not None and depois is not None:
        nomes = [c for c in depois.keys() if antes.get(c) != depois.get(c)]
        alterados = cortar(', '.join(nomes), 4000)
        for c in nomes:
            campos.append({
                'COD_ALTERACAO': cod, 'ID_CORRELACAO': base['correl'], 'DT_ALTERACAO': base['dt'],
                'TABELA': tabela, 'CHAVE_REGISTRO': chave, 'CAMPO': c[:150],
                'VALOR_ANTES': cortar(antes.get(c), 4000), 'VALOR_DEPOIS': cortar(depois.get(c), 4000),
            })
    alteracao = {
        'COD_ALTERACAO': cod, 'ID_CORRELACAO': base['correl'], 'COD_COMANDO': cod_comando,
        'DT_ALTERACAO': base['dt'], 'ORIGEM': origem, 'OPERACAO': operacao, 'TABELA': tabela,
        'CHAVE_REGISTRO': chave, 'CAMPOS_ALTERADOS': alterados,
        'DADOS_ANTES': para_json(antes), 'DADOS_DEPOIS': para_json(depois),
    }
    return alteracao, campos


def _detalhar_update_delete(dbapi_conn, item, conjuntos):
    """Tira a foto ANTES para cada conjunto de parâmetros (até 50)."""
    limite, _lv, _s = _limites()
    fotos = []
    restante = limite
    for params in conjuntos[:50]:
        if restante <= 0:
            item['parcial'] = True
            break
        colunas, linhas, truncado = _tirar_foto(dbapi_conn, item['plano'], params, restante)
        if truncado:
            item['parcial'] = True
        restante -= len(linhas)
        fotos.append((params, colunas, linhas))
    if len(conjuntos) > 50:
        item['parcial'] = True
    item['fotos_antes'] = fotos


def _concluir_update_delete(dbapi_conn, item, base):
    _limite, limite_valor, sensiveis = _limites()
    plano = item['plano']
    pk = _chave_primaria(dbapi_conn, _limpar_nome(plano['tabela']))
    tabela = _nome_registro(plano['tabela'])
    alteracoes = []
    campos = []
    for params, colunas, linhas in item.get('fotos_antes') or []:
        if not linhas:
            continue
        antes = [_linha_dict(colunas, l, limite_valor, sensiveis) for l in linhas]
        if item['operacao'] == 'DELETE':
            for a in antes:
                alt, _c = _nova_alteracao(base, 'SQL', 'DELETE', tabela, _texto_chave(a, pk, colunas),
                                          a, None, item['cod'])
                alteracoes.append(alt)
            continue
        # UPDATE: foto depois
        if pk and all(c in colunas for c in pk):
            cols_d, lins_d = _foto_depois_por_pk(dbapi_conn, plano, colunas, linhas, pk)
        else:
            cols_d, lins_d, _t = _tirar_foto(dbapi_conn, plano, params, len(linhas))
        depois = [_linha_dict(cols_d, l, limite_valor, sensiveis) for l in lins_d]
        if pk and all(c in colunas for c in pk):
            mapa = dict((tuple(d.get(c) for c in pk), d) for d in depois)
            pares = [(a, mapa.get(tuple(a.get(c) for c in pk))) for a in antes]
        else:
            pares = [(a, depois[i] if i < len(depois) else None) for i, a in enumerate(antes)]
        for a, d in pares:
            if d is not None and a == d:
                continue  # linha atingida pelo WHERE mas sem mudança real
            alt, cps = _nova_alteracao(base, 'SQL', 'UPDATE', tabela, _texto_chave(a, pk, colunas),
                                       a, d, item['cod'])
            alteracoes.append(alt)
            campos.extend(cps)
    return alteracoes, campos


def _detalhar_insert(sql, conjuntos, item, base):
    """Lê os valores incluídos direto dos parâmetros do INSERT ... VALUES."""
    limite, limite_valor, sensiveis = _limites()
    corpo = _sem_comentario_inicial(sql).rstrip().rstrip(';')
    if _tem_ponto_virgula_topo(corpo):
        return [], False
    m = _RE_INSERT.match(corpo)
    if not m:
        return [], False
    resto = corpo[m.end():]
    palavras = _palavras_topo(resto)
    pos_values = None
    for pos, p in palavras:
        if p == 'VALUES':
            pos_values = pos
            break
        if p in ('SELECT', 'EXEC', 'EXECUTE', 'DEFAULT'):
            return [], False
    if pos_values is None:
        return [], False
    tabela = _nome_registro(m.group('tab'))
    colunas = [_limpar_nome(c) for c in _dividir_virgulas_topo(m.group('cols'))]
    prefixo_params = _contar_marcadores(corpo[:m.end() + pos_values])
    grupos = _grupos_values(resto[pos_values + len('VALUES'):])
    alteracoes = []
    parcial = False
    for params in conjuntos:
        idx = prefixo_params
        for grupo in grupos:
            if len(alteracoes) >= limite:
                return alteracoes, True
            tokens = _dividir_virgulas_topo(grupo)
            dados = {}
            for col, tok in zip(colunas, tokens):
                qtd = _contar_marcadores(tok)
                valores = list(params[idx:idx + qtd]) if params else []
                idx += qtd
                if eh_sensivel(col, sensiveis):
                    dados[col] = '***'
                elif tok == '?' and valores:
                    dados[col] = valor_para_texto(valores[0], limite_valor)
                elif qtd:
                    dados[col] = cortar('{} {}'.format(tok, [valor_para_texto(v, 200) for v in valores]),
                                        limite_valor)
                else:
                    dados[col] = cortar(tok, limite_valor)
            chave = _texto_chave(dados, None, colunas).replace('(sem PK) ', '(novo) ')
            alt, _c = _nova_alteracao(base, 'SQL', 'INSERT', tabela, chave, None, dados, item['cod'])
            alteracoes.append(alt)
    return alteracoes, parcial


def _parametros_texto(sql, parameters, executemany, sensiveis, limite_valor):
    if not parameters:
        return None, 1
    mascarar = any(s.strip() and s.strip().lower() in sql.lower() for s in sensiveis)
    conjuntos = list(parameters) if executemany else [parameters]
    amostra = conjuntos[:5]
    if isinstance(amostra[0], dict):
        saida = [dict((k, '***' if (mascarar or eh_sensivel(k, sensiveis)) else valor_para_texto(v, limite_valor))
                      for k, v in c.items()) for c in amostra]
    else:
        saida = [['***' if mascarar else valor_para_texto(v, limite_valor) for v in c] for c in amostra]
    texto = para_json(saida if executemany else saida[0], limite_valor, 60000)
    if executemany and len(conjuntos) > 5:
        texto = cortar(texto + ' ... (+{} conjuntos)'.format(len(conjuntos) - 5), 65000)
    return texto, len(conjuntos)


# ======================================================================
# Pendências por conexão (só viram auditoria após o COMMIT)
# ======================================================================
def _pendentes(conn):
    try:
        info = conn.info
    except Exception:
        return None
    lista = info.get(_CHAVE_PEND)
    if lista is None:
        lista = []
        info[_CHAVE_PEND] = lista
    return lista


def _base():
    return {'correl': contexto.correlacao_atual(), 'dt': agora()}


def _finalizar_pendentes(info, situacao):
    lista = info.get(_CHAVE_PEND) if info is not None else None
    if not lista:
        return
    info[_CHAVE_PEND] = []
    pacote = contexto.novo_pacote()
    for comando, alteracoes, campos in lista:
        comando['SITUACAO'] = situacao
        pacote['comandos'].append(comando)
        if situacao == 'CONFIRMADA':
            pacote['alteracoes'].extend(alteracoes)
            pacote['campos'].extend(campos)
    contexto.entregar(pacote)


# ======================================================================
# Eventos do Engine
# ======================================================================
def _antes_execucao(conn, cursor, statement, parameters, context, executemany):
    if statement.startswith(MARCA_SQL):
        return
    aud = contexto.aud_requisicao()
    if aud is not None:
        if aud.get('ignorar'):
            return
        aud['consultas'] = aud.get('consultas', 0) + 1
    try:
        context._aud_t0 = time.perf_counter()
        context._aud_item = None
    except Exception:
        pass
    if contexto.em_flush():
        return  # detalhado pelo evento de ORM
    try:
        operacao, tabela = classificar(statement)
        if operacao is None or _tabela_excluida(tabela):
            return
        if not contexto.captura_ligada():
            return
        _limite, limite_valor, sensiveis = _limites()
        base = _base()
        parametros, qtd = _parametros_texto(statement, parameters, executemany, sensiveis, limite_valor)
        # text() / SQL escrito à mão = SQL; construções do SQLAlchemy fora do flush
        # (bulk_save_objects, query.update(), pandas) = ORM_LOTE
        compilado = getattr(context, 'compiled', None)
        origem = 'SQL'
        if compilado is not None and not isinstance(getattr(compilado, 'statement', None), TextClause):
            origem = 'ORM_LOTE'
        item = {
            'cod': novo_codigo(), 'operacao': operacao, 'tabela': tabela, 'base': base,
            'comando': {
                'COD_COMANDO': None, 'ID_CORRELACAO': base['correl'], 'DT_EXECUCAO': base['dt'],
                'ORIGEM': origem, 'OPERACAO': operacao, 'TABELA': cortar(tabela, 300),
                'COMANDO': cortar(statement, 20000), 'PARAMETROS': parametros, 'QTDE_LOTES': qtd,
                'DETALHAMENTO': 'SEM_DETALHE',
            },
            'conjuntos': (list(parameters) if executemany else [parameters]) if parameters else [()],
            'parcial': False,
        }
        item['comando']['COD_COMANDO'] = item['cod']
        if operacao in ('UPDATE', 'DELETE'):
            plano = _plano_foto(statement, operacao)
            if plano is not None and not isinstance(item['conjuntos'][0], dict):
                item['plano'] = plano
                try:
                    _detalhar_update_delete(conn.connection, item, item['conjuntos'])
                except Exception as erro:
                    item['plano'] = None
                    item['falha'] = repr(erro)
        context._aud_item = item
    except Exception:
        contexto_log('captura antes', traceback.format_exc())


def _depois_execucao(conn, cursor, statement, parameters, context, executemany):
    if statement.startswith(MARCA_SQL):
        return
    t0 = getattr(context, '_aud_t0', None)
    duracao = (time.perf_counter() - t0) * 1000.0 if t0 else 0.0
    aud = contexto.aud_requisicao()
    if aud is not None and not aud.get('ignorar'):
        aud['tempo_banco'] = aud.get('tempo_banco', 0.0) + duracao
    item = getattr(context, '_aud_item', None)
    if not item:
        return
    context._aud_item = None
    try:
        comando = item['comando']
        comando['DURACAO_MS'] = int(duracao)
        try:
            comando['LINHAS_AFETADAS'] = cursor.rowcount if cursor.rowcount is not None else None
        except Exception:
            comando['LINHAS_AFETADAS'] = None
        alteracoes, campos = [], []
        if item.get('plano'):
            if cursor.description is None:
                try:
                    alteracoes, campos = _concluir_update_delete(conn.connection, item, item['base'])
                    comando['DETALHAMENTO'] = 'PARCIAL' if item.get('parcial') else 'COMPLETO'
                except Exception as erro:
                    comando['DETALHAMENTO'] = 'FALHOU'
                    comando['ERRO'] = cortar('Detalhe antes/depois: ' + repr(erro), 4000)
        elif item.get('falha'):
            comando['DETALHAMENTO'] = 'FALHOU'
            comando['ERRO'] = cortar('Detalhe antes/depois: ' + item['falha'], 4000)
        elif item['operacao'] == 'INSERT' and not isinstance(item['conjuntos'][0], dict):
            alteracoes, parcial = _detalhar_insert(statement, item['conjuntos'], item, item['base'])
            if alteracoes:
                comando['DETALHAMENTO'] = 'PARCIAL' if parcial else 'COMPLETO'
        pend = _pendentes(conn)
        if pend is not None:
            pend.append((comando, alteracoes, campos))
    except Exception:
        contexto_log('captura depois', traceback.format_exc())


def _erro_execucao(contexto_erro):
    try:
        statement = contexto_erro.statement or ''
        if statement.startswith(MARCA_SQL):
            return
        operacao, tabela = classificar(statement)
        if operacao is None or _tabela_excluida(tabela) or contexto.requisicao_ignorada():
            return
        base = _base()
        _l, limite_valor, sensiveis = _limites()
        parametros, qtd = _parametros_texto(statement, contexto_erro.parameters,
                                            isinstance(contexto_erro.parameters, list) and
                                            bool(contexto_erro.parameters) and
                                            isinstance(contexto_erro.parameters[0], (list, tuple, dict)),
                                            sensiveis, limite_valor)
        pacote = contexto.novo_pacote()
        pacote['comandos'].append({
            'COD_COMANDO': novo_codigo(), 'ID_CORRELACAO': base['correl'], 'DT_EXECUCAO': base['dt'],
            'ORIGEM': 'SQL', 'OPERACAO': operacao, 'TABELA': cortar(tabela, 300),
            'COMANDO': cortar(statement, 20000), 'PARAMETROS': parametros, 'QTDE_LOTES': qtd,
            'SITUACAO': 'ERRO', 'DETALHAMENTO': 'SEM_DETALHE',
            'ERRO': cortar(repr(contexto_erro.original_exception), 4000),
        })
        contexto.entregar(pacote)
    except Exception:
        pass


def _ao_commit(conn):
    try:
        _finalizar_pendentes(conn.info, 'CONFIRMADA')
    except Exception:
        contexto_log('commit', traceback.format_exc())
    if contexto.aud_requisicao() is None:
        contexto.encerrar_correlacao_sem_requisicao()


def _ao_rollback(conn):
    try:
        _finalizar_pendentes(conn.info, 'DESFEITA')
    except Exception:
        pass
    if contexto.aud_requisicao() is None:
        contexto.encerrar_correlacao_sem_requisicao()


def _ao_devolver_ao_pool(dbapi_connection, connection_record):
    # Conexão devolvida sem commit = rollback automático do pool
    try:
        if connection_record is not None:
            _finalizar_pendentes(connection_record.info, 'DESFEITA')
    except Exception:
        pass


# ======================================================================
# Eventos do ORM (Session)
# ======================================================================
def _antes_flush(session, flush_context, instances):
    contexto.entrar_flush()
    if contexto.requisicao_ignorada():
        return
    try:
        _carregar_valores_antigos(session)
    except Exception:
        contexto_log('valores antigos ORM', traceback.format_exc())


def _carregar_valores_antigos(session):
    """
    Quando o objeto foi alterado depois de um commit (atributos expirados), o SQLAlchemy
    não conhece o valor antigo. Nesse caso, busca só as colunas alteradas pela chave
    primária, ANTES da gravação (uma consulta leve por objeto, no máximo 200 por flush).
    """
    antigos = session.info.setdefault('aud_v2_antigos', {})
    contador = 0
    for obj in list(session.dirty):
        if contador >= 200:
            break
        estado = sa_inspect(obj)
        if estado.key is None:
            continue
        mapper = estado.mapper
        faltando = []
        for prop in mapper.column_attrs:
            hist = estado.attrs[prop.key].history
            if hist.added and not hist.deleted:
                faltando.append(prop)
        if not faltando:
            continue
        tabela = mapper.persist_selectable
        try:
            pk_cols = list(mapper.primary_key)
            valores_pk = list(estado.key[1])
        except Exception:
            continue
        if len(pk_cols) != len(valores_pk):
            continue
        consulta = select(*[p.columns[0] for p in faltando]).select_from(tabela)
        for col, val in zip(pk_cols, valores_pk):
            consulta = consulta.where(col == val)
        linha = session.connection().execute(consulta).first()
        contador += 1
        if linha is None:
            continue
        antigos[id(obj)] = dict((p.key, v) for p, v in zip(faltando, linha))


def _depois_flush_exec(session, flush_context):
    contexto.sair_flush()


def _valores_objeto(estado, mapper, limite_valor, sensiveis):
    dados = {}
    for prop in mapper.column_attrs:
        if prop.key not in estado.dict:
            continue
        nome = prop.columns[0].name
        valor = estado.dict.get(prop.key)
        dados[nome] = '***' if eh_sensivel(nome, sensiveis) and valor not in (None, '') \
            else valor_para_texto(valor, limite_valor)
    return dados


def _chave_objeto(mapper, obj):
    try:
        valores = mapper.primary_key_from_instance(obj)
        nomes = [c.name for c in mapper.primary_key]
        return cortar('|'.join('{}={}'.format(n, valor_para_texto(v, 100)) for n, v in zip(nomes, valores)), 500)
    except Exception:
        return None


def _depois_flush(session, flush_context):
    if contexto.requisicao_ignorada():
        return
    try:
        if not contexto.captura_ligada():
            return
        limite_obj = config.obter_int('MAX_OBJETOS_ORM', 2000)
        _l, limite_valor, sensiveis = _limites()
        base = _base()
        pend = session.info.setdefault(_CHAVE_PEND, [])
        antigos = session.info.pop('aud_v2_antigos', None) or {}
        contagem_resumo = {}
        total = 0

        def registrar(obj, operacao):
            estado = sa_inspect(obj)
            mapper = estado.mapper
            tabela = _nome_registro(mapper.persist_selectable.fullname
                                    if hasattr(mapper.persist_selectable, 'fullname')
                                    else str(mapper.persist_selectable))
            if _tabela_excluida(tabela):
                return
            if total >= limite_obj:
                chave_r = (tabela, operacao)
                contagem_resumo[chave_r] = contagem_resumo.get(chave_r, 0) + 1
                return
            antes = depois = None
            if operacao == 'INSERT':
                depois = _valores_objeto(estado, mapper, limite_valor, sensiveis)
            elif operacao == 'DELETE':
                antes = _valores_objeto(estado, mapper, limite_valor, sensiveis)
            else:
                antes, depois = {}, {}
                carregados = antigos.get(id(obj), {})
                for prop in mapper.column_attrs:
                    hist = estado.attrs[prop.key].history
                    if not hist.has_changes():
                        continue
                    nome = prop.columns[0].name
                    if hist.deleted:
                        velho = hist.deleted[0]
                    elif prop.key in carregados:
                        velho = carregados[prop.key]
                    else:
                        velho = '<nao carregado>'
                    novo = hist.added[0] if hist.added else None
                    if eh_sensivel(nome, sensiveis):
                        velho, novo = '***', '***'
                    antes[nome] = valor_para_texto(velho, limite_valor)
                    depois[nome] = valor_para_texto(novo, limite_valor)
                if not depois:
                    return
            alt, campos = _nova_alteracao(base, 'ORM', operacao, tabela, _chave_objeto(mapper, obj), antes, depois)
            pend.append((alt, campos))

        for obj in list(session.new):
            registrar(obj, 'INSERT')
            total += 1
        for obj in list(session.dirty):
            if session.is_modified(obj, include_collections=False):
                registrar(obj, 'UPDATE')
                total += 1
        for obj in list(session.deleted):
            registrar(obj, 'DELETE')
            total += 1

        for (tabela, operacao), qtd in contagem_resumo.items():
            alt, _c = _nova_alteracao(base, 'ORM', operacao, tabela, '(resumo)', None,
                                      {'registros_nao_detalhados': qtd,
                                       'motivo': 'acima de MAX_OBJETOS_ORM'})
            pend.append((alt, []))
    except Exception:
        contexto_log('flush ORM', traceback.format_exc())


def _sessao_commit(session):
    lista = session.info.get(_CHAVE_PEND)
    if lista:
        session.info[_CHAVE_PEND] = []
        pacote = contexto.novo_pacote()
        for alt, campos in lista:
            pacote['alteracoes'].append(alt)
            pacote['campos'].extend(campos)
        contexto.entregar(pacote)
    contexto.sair_flush(zerar=True)
    if contexto.aud_requisicao() is None:
        contexto.encerrar_correlacao_sem_requisicao()


def _sessao_rollback(session, *args):
    session.info[_CHAVE_PEND] = []
    session.info.pop('aud_v2_antigos', None)
    contexto.sair_flush(zerar=True)


# ======================================================================
def contexto_log(onde, detalhe):
    try:
        from app.utils.log_seguro import log_erro
        log_erro('Auditoria V2 ({}): {}'.format(onde, detalhe))
    except Exception:
        pass


def instalar(engine):
    """Liga todos os ganchos no engine e na Session (uma única vez)."""
    if getattr(engine, '_aud_v2_instalado', False):
        return
    event.listen(engine, 'before_cursor_execute', _antes_execucao)
    event.listen(engine, 'after_cursor_execute', _depois_execucao)
    event.listen(engine, 'handle_error', _erro_execucao)
    event.listen(engine, 'commit', _ao_commit)
    event.listen(engine, 'rollback', _ao_rollback)
    event.listen(engine.pool, 'checkin', _ao_devolver_ao_pool)

    event.listen(Session, 'before_flush', _antes_flush)
    event.listen(Session, 'after_flush', _depois_flush)
    event.listen(Session, 'after_flush_postexec', _depois_flush_exec)
    event.listen(Session, 'after_commit', _sessao_commit)
    event.listen(Session, 'after_rollback', _sessao_rollback)
    event.listen(Session, 'after_soft_rollback', _sessao_rollback)
    engine._aud_v2_instalado = True
