# -*- coding: utf-8 -*-
"""
app/utils/modelagem_dados.py

Serviço da Modelagem de Dados do Portal GEINC.

Monta o modelo de dados do portal (tabelas, colunas, chaves e ligações)
cruzando três fontes, sem nenhum dado fixo:

  1) Models SQLAlchemy (app/models)  -> tabelas mapeadas, colunas, PK e FK do ORM
                                       (arquivos que o portal não importa são lidos
                                       só como texto, sem importar)
  2) Rotas (app/routes e *routes.py) -> models importados por cada rota (uso via ORM)
                                       e tabelas citadas no SQL escrito à mão
  3) SQL Server                      -> estrutura REAL, consultada SÓ para as
                                       tabelas encontradas nos passos 1 e 2

Para não pesar o portal:
  - nada roda no carregamento normal das páginas; só quando a tela é aberta;
  - o resultado fica em cache na memória (TTL) e é reaproveitado por todos;
  - as consultas ao banco são de catálogo (sys.*), filtradas por object_id,
    em poucos comandos (sem varrer o banco inteiro e sem ler dados das tabelas);
  - o script SQL (DDL) só é montado quando alguém clica em exportar.

Compatível com Python 3.9 e 3.12.
"""

import ast
import logging
import os
import pkgutil
import re
import threading
import time
from datetime import datetime

from sqlalchemy import bindparam, text

from app import db

logger = logging.getLogger(__name__)

# Tempo que o modelo montado fica em cache (segundos)
TTL_SEGUNDOS = 30 * 60

# Tamanho máximo de cada lista IN enviada ao SQL Server (limite do driver é 2100)
TAMANHO_LOTE = 900

_RE_NOME_SEGURO = re.compile(r'^[A-Za-z0-9_]+$')

_lock = threading.Lock()
_cache = {'publico': None, 'interno': None, 'gerado_em': 0.0}


# =====================================================================
# Utilitários
# =====================================================================

def _q(nome):
    """Coloca o identificador entre colchetes (padrão SQL Server)."""
    return '[' + str(nome).replace(']', ']]') + ']'


def _lotes(valores, tamanho=TAMANHO_LOTE):
    valores = list(valores)
    for i in range(0, len(valores), tamanho):
        yield valores[i:i + tamanho]


def _executar_em_lotes(conn, sql, nome_param, valores, extras=None):
    """Executa um SELECT com IN expandido, quebrando a lista em lotes."""
    linhas = []
    if not valores:
        return linhas
    comando = text(sql).bindparams(bindparam(nome_param, expanding=True))
    for lote in _lotes(valores):
        params = {nome_param: lote}
        if extras:
            params.update(extras)
        linhas.extend(conn.execute(comando, params).mappings().all())
    return linhas


def _chave(banco, schema, nome, banco_atual):
    """Chave única da tabela (maiúscula). Banco atual não entra na chave."""
    schema = (schema or '').upper()
    nome = (nome or '').upper()
    if banco and banco.upper() != (banco_atual or '').upper():
        return '{}.{}.{}'.format(banco.upper(), schema, nome)
    return '{}.{}'.format(schema, nome)


def _tipo_sql(tipo, max_len, precisao, escala):
    """Monta o tipo legível/DDL a partir de sys.columns."""
    t = (tipo or '').lower()
    if t in ('varchar', 'char', 'varbinary', 'binary'):
        return '{}({})'.format(t.upper(), 'MAX' if max_len == -1 else max_len)
    if t in ('nvarchar', 'nchar'):
        return '{}({})'.format(t.upper(), 'MAX' if max_len == -1 else int(max_len) // 2)
    if t in ('decimal', 'numeric'):
        return '{}({},{})'.format(t.upper(), precisao, escala)
    if t in ('datetime2', 'time', 'datetimeoffset'):
        return '{}({})'.format(t.upper(), escala)
    if t == 'float' and precisao and int(precisao) != 53:
        return 'FLOAT({})'.format(precisao)
    return t.upper()


def _nome_amigavel_blueprint(bp):
    return str(bp).replace('_bp', '').replace('_', ' ').strip().title()


def _info_aplicativo(bp):
    """Nome e categoria do aplicativo vindos do banco (AUD_TB006_APLICATIVO,
    via cache da Auditoria V2). Se não houver cadastro, usa o nome do blueprint."""
    try:
        from app.auditoria_v2 import config as aud_config
        info = aud_config.aplicativo(bp) or {}
        return info.get('nome') or _nome_amigavel_blueprint(bp), info.get('categoria')
    except Exception:
        return _nome_amigavel_blueprint(bp), None


# =====================================================================
# 1) Models SQLAlchemy
# =====================================================================

def _models_nao_carregados():
    """Arquivos de app/models que o portal NÃO importou (nenhuma rota usa).
    Eles são lidos só como texto (ast), sem importar: importar um model que o
    portal não carrega poderia registrar classes/tabelas duplicadas no
    SQLAlchemy e afetar o resto do sistema.
    Retorna [(schema ou None, tabela, 'arquivo.py:Classe'), ...]."""
    import sys
    import app.models as pacote
    achados = []
    for info in pkgutil.iter_modules(pacote.__path__):
        if 'app.models.' + info.name in sys.modules:
            continue
        caminho = os.path.join(pacote.__path__[0], info.name + '.py')
        try:
            with open(caminho, 'r', encoding='utf-8') as arq:
                arvore = ast.parse(arq.read())
        except Exception:
            continue
        for no in arvore.body:
            if not isinstance(no, ast.ClassDef):
                continue
            tabela, schema = None, None
            for item in no.body:
                if not isinstance(item, ast.Assign) or len(item.targets) != 1:
                    continue
                alvo = getattr(item.targets[0], 'id', None)
                if alvo == '__tablename__' and isinstance(item.value, ast.Constant):
                    tabela = item.value.value
                elif alvo == '__table_args__':
                    dicts = [item.value] if isinstance(item.value, ast.Dict) else [
                        e for e in getattr(item.value, 'elts', []) if isinstance(e, ast.Dict)]
                    for d in dicts:
                        for k, v in zip(d.keys, d.values):
                            if isinstance(k, ast.Constant) and k.value == 'schema' and isinstance(v, ast.Constant):
                                schema = v.value
            if isinstance(tabela, str):
                achados.append((schema, tabela, '{}.py:{}'.format(info.name, no.name)))
    return achados


def _mapa_models():
    """Retorna:
      classes: {'NomeClasse': [Table, ...]}   (pode haver nomes repetidos)
      por_modulo: {'app.models.x': [NomeClasse, ...]}
      tabela_classes: {id(Table): ['arquivo.py:Classe', ...]}
      tabelas: [Table, ...] (todas do metadata, mapeadas ou não)
    """
    classes = {}
    por_modulo = {}
    tabela_classes = {}

    for mapper in db.Model.registry.mappers:
        cls = mapper.class_
        tabela = getattr(mapper, 'local_table', None)
        if tabela is None or not hasattr(tabela, 'columns'):
            continue
        classes.setdefault(cls.__name__, []).append(tabela)
        por_modulo.setdefault(cls.__module__, []).append(cls.__name__)
        arquivo = cls.__module__.replace('app.models.', '') + '.py'
        tabela_classes.setdefault(id(tabela), []).append('{}:{}'.format(arquivo, cls.__name__))

    tabelas = []
    vistos = set()
    metadatas = getattr(db, 'metadatas', None) or {None: db.metadata}
    for metadata in metadatas.values():
        for tabela in metadata.tables.values():
            if id(tabela) not in vistos:
                vistos.add(id(tabela))
                tabelas.append(tabela)

    return classes, por_modulo, tabela_classes, tabelas


# =====================================================================
# 2) Rotas (varredura com ast: só strings e imports, ignora comentários)
# =====================================================================

def _arquivos_rotas():
    raiz_app = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    arquivos = []
    for pasta, subpastas, nomes in os.walk(raiz_app):
        subpastas[:] = [s for s in subpastas if s not in ('__pycache__', 'static', 'templates')]
        for nome in nomes:
            if not nome.endswith('.py'):
                continue
            if os.path.basename(pasta) == 'routes' or nome.endswith('routes.py'):
                if nome != '__init__.py':
                    arquivos.append(os.path.join(pasta, nome))
    raiz_projeto = os.path.dirname(raiz_app)
    return sorted(arquivos), raiz_projeto


def _strings_do_codigo(arvore):
    """Todas as strings literais do arquivo (inclui pedaços de f-strings)."""
    for no in ast.walk(arvore):
        if isinstance(no, ast.Constant) and isinstance(no.value, str):
            yield no.value


def _blueprints_do_arquivo(arvore):
    """Nomes dos blueprints criados no arquivo: Blueprint('nome', ...)."""
    nomes = []
    for no in ast.walk(arvore):
        if isinstance(no, ast.Call):
            func = no.func
            nome_func = getattr(func, 'id', None) or getattr(func, 'attr', None)
            if nome_func == 'Blueprint' and no.args:
                primeiro = no.args[0]
                if isinstance(primeiro, ast.Constant) and isinstance(primeiro.value, str):
                    nomes.append(primeiro.value)
    return nomes


def _imports_de_models(arvore):
    """Retorna {'modulo_models': set(nomes importados)} dos imports de app.models."""
    achados = {}
    for no in ast.walk(arvore):
        if isinstance(no, ast.ImportFrom):
            modulo = no.module or ''
            partes = modulo.split('.')
            if 'models' not in partes:
                continue
            idx = partes.index('models')
            sub = '.'.join(partes[idx + 1:])
            alvo = 'app.models' + ('.' + sub if sub else '')
            for alias in no.names:
                achados.setdefault(alvo, set()).add(alias.name)
    return achados


def _regex_tabelas(schemas):
    alternativas = '|'.join(sorted({re.escape(s) for s in schemas if s}, key=len, reverse=True))
    if not alternativas:
        return None
    return re.compile(
        r'(?<![\w\]\.])(?:\[?([A-Za-z_]\w*)\]?\s*\.\s*)?\[?(' + alternativas + r')\]?\s*\.\s*\[?([A-Za-z_]\w*)\]?',
        re.IGNORECASE
    )


# =====================================================================
# 3) Leitura da estrutura real no SQL Server
# =====================================================================

def _ler_estrutura_banco(conn, prefixo, nomes):
    """Lê do catálogo do SQL Server a estrutura das tabelas/views informadas.
    prefixo: '' para o banco atual ou '[OUTRO_BANCO].' para outro banco.
    nomes: lista de nomes de objetos (sem schema).
    Retorna dict com objetos, colunas, chaves, índices, FKs e dependências."""
    sql_obj = """
        SELECT o.object_id AS OBJECT_ID, s.name AS SCHEMA_NOME, o.name AS NOME, o.type AS TIPO
        FROM {p}sys.objects o
        JOIN {p}sys.schemas s ON s.schema_id = o.schema_id
        WHERE o.type IN ('U', 'V', 'P', 'PC', 'FN', 'IF', 'TF', 'FS', 'FT', 'SN') AND o.name IN :nomes
    """.format(p=prefixo)
    todos = _executar_em_lotes(conn, sql_obj, 'nomes', nomes)
    # Procedures, funções e sinônimos citados nas rotas não são tabelas: ficam de fora do diagrama
    rotinas = [o for o in todos if (o['TIPO'] or '').strip() not in ('U', 'V')]
    objetos = [o for o in todos if (o['TIPO'] or '').strip() in ('U', 'V')]
    ids = [int(o['OBJECT_ID']) for o in objetos]
    if not ids:
        return {'objetos': [], 'rotinas': rotinas, 'colunas': [], 'chaves': [], 'indices': [], 'fks': [],
                'deps': []}

    sql_col = """
        SELECT c.object_id AS OBJECT_ID, c.column_id AS COLUMN_ID, c.name AS NOME,
               t.name AS TIPO, c.max_length AS MAX_LEN, c.precision AS PRECISAO, c.scale AS ESCALA,
               c.is_nullable AS NULO, c.is_identity AS IDENTIDADE,
               CAST(ic.seed_value AS BIGINT) AS SEMENTE, CAST(ic.increment_value AS BIGINT) AS INCREMENTO,
               dc.name AS NOME_PADRAO, dc.definition AS PADRAO, cc.definition AS CALCULADA
        FROM {p}sys.columns c
        JOIN {p}sys.types t ON t.user_type_id = c.user_type_id
        LEFT JOIN {p}sys.identity_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        LEFT JOIN {p}sys.default_constraints dc ON dc.object_id = c.default_object_id
        LEFT JOIN {p}sys.computed_columns cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
        WHERE c.object_id IN :ids
    """.format(p=prefixo)
    colunas = _executar_em_lotes(conn, sql_col, 'ids', ids)

    sql_chaves = """
        SELECT kc.parent_object_id AS OBJECT_ID, kc.name AS NOME, kc.type AS TIPO,
               i.type_desc AS TIPO_INDICE, ic.key_ordinal AS ORDEM, ic.is_descending_key AS DESC_,
               col.name AS COLUNA
        FROM {p}sys.key_constraints kc
        JOIN {p}sys.indexes i ON i.object_id = kc.parent_object_id AND i.index_id = kc.unique_index_id
        JOIN {p}sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN {p}sys.columns col ON col.object_id = ic.object_id AND col.column_id = ic.column_id
        WHERE kc.parent_object_id IN :ids AND ic.key_ordinal > 0
    """.format(p=prefixo)
    chaves = _executar_em_lotes(conn, sql_chaves, 'ids', ids)

    sql_ind = """
        SELECT i.object_id AS OBJECT_ID, i.name AS NOME, i.type_desc AS TIPO_INDICE, i.is_unique AS UNICO,
               i.filter_definition AS FILTRO, ic.key_ordinal AS ORDEM, ic.index_column_id AS ORDEM_COL,
               ic.is_included_column AS INCLUIDA, ic.is_descending_key AS DESC_, col.name AS COLUNA
        FROM {p}sys.indexes i
        JOIN {p}sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN {p}sys.columns col ON col.object_id = ic.object_id AND col.column_id = ic.column_id
        WHERE i.object_id IN :ids
          AND i.type > 0 AND i.is_primary_key = 0 AND i.is_unique_constraint = 0
          AND i.is_hypothetical = 0 AND i.name IS NOT NULL
    """.format(p=prefixo)
    indices = _executar_em_lotes(conn, sql_ind, 'ids', ids)

    sql_fk = """
        SELECT fk.object_id AS FK_ID, fk.name AS NOME,
               ps.name AS DE_SCHEMA, po.name AS DE_TABELA, pc.name AS DE_COLUNA,
               rs.name AS PARA_SCHEMA, ro.name AS PARA_TABELA, rc.name AS PARA_COLUNA,
               fk.delete_referential_action_desc AS AO_EXCLUIR,
               fk.update_referential_action_desc AS AO_ATUALIZAR,
               fk.is_disabled AS DESATIVADA, fkc.constraint_column_id AS ORDEM
        FROM {p}sys.foreign_keys fk
        JOIN {p}sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
        JOIN {p}sys.objects po ON po.object_id = fk.parent_object_id
        JOIN {p}sys.schemas ps ON ps.schema_id = po.schema_id
        JOIN {p}sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
        JOIN {p}sys.objects ro ON ro.object_id = fk.referenced_object_id
        JOIN {p}sys.schemas rs ON rs.schema_id = ro.schema_id
        JOIN {p}sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
        WHERE fk.{campo} IN :ids
    """
    fks = {}
    for campo in ('parent_object_id', 'referenced_object_id'):
        for linha in _executar_em_lotes(conn, sql_fk.format(p=prefixo, campo=campo), 'ids', ids):
            fks[(int(linha['FK_ID']), int(linha['ORDEM']))] = linha

    ids_views = [int(o['OBJECT_ID']) for o in objetos if (o['TIPO'] or '').strip() == 'V']
    deps = []
    if ids_views:
        sql_dep = """
            SELECT DISTINCT d.referencing_id AS OBJECT_ID,
                   d.referenced_database_name AS BANCO, d.referenced_schema_name AS SCHEMA_NOME,
                   d.referenced_entity_name AS NOME
            FROM {p}sys.sql_expression_dependencies d
            WHERE d.referencing_id IN :ids AND d.referenced_entity_name IS NOT NULL
        """.format(p=prefixo)
        deps = _executar_em_lotes(conn, sql_dep, 'ids', ids_views)

    return {
        'objetos': objetos, 'rotinas': rotinas, 'colunas': colunas, 'chaves': chaves,
        'indices': indices, 'fks': list(fks.values()), 'deps': deps,
    }


def _info_banco(conn):
    """Nome do banco atual, schema padrão do usuário e schemas existentes."""
    info = conn.execute(text('SELECT DB_NAME() AS BANCO, SCHEMA_NAME() AS SCHEMA_PADRAO')).mappings().first()
    schemas = [r[0] for r in conn.execute(text(
        "SELECT name FROM sys.schemas "
        "WHERE schema_id < 16384 AND name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest')"
    ))]
    return info['BANCO'], info['SCHEMA_PADRAO'] or 'dbo', schemas


def _definicoes_views(ids):
    """Texto (CREATE VIEW ...) das views do banco atual, por object_id."""
    definicoes = {}
    with db.engine.connect() as conn:
        for r in _executar_em_lotes(
                conn, 'SELECT object_id AS OBJECT_ID, definition AS DEFINICAO FROM sys.sql_modules '
                      'WHERE object_id IN :ids', 'ids', ids):
            definicoes[int(r['OBJECT_ID'])] = r['DEFINICAO']
    return definicoes


# =====================================================================
# Montagem do modelo
# =====================================================================

def _registro(tabelas, chave, banco, schema, nome):
    if chave not in tabelas:
        tabelas[chave] = {
            'chave': chave, 'banco': banco, 'schema': schema, 'nome': nome,
            'origens': set(), 'models': set(), 'modulos': set(), 'sa': None,
        }
    return tabelas[chave]


def _construir():
    inicio = time.time()
    avisos = []
    engine = db.engine

    with engine.connect() as conn:
        banco_atual, schema_padrao, schemas_banco = _info_banco(conn)

    # ---------- 1) Models ----------
    # Só o que o portal já carregou (nada é importado a mais)
    classes, por_modulo, tabela_classes, tabelas_sa = _mapa_models()

    tabelas = {}
    for t in tabelas_sa:
        schema = t.schema or schema_padrao
        chave = _chave(None, schema, t.name, banco_atual)
        reg = _registro(tabelas, chave, None, schema, t.name)
        reg['origens'].add('MODEL')
        reg['models'].update(tabela_classes.get(id(t), []))
        reg['sa'] = t

    # Models que existem em app/models mas o portal não importa (lidos como texto)
    for schema, nome, origem in _models_nao_carregados():
        schema = schema or schema_padrao
        chave = _chave(None, schema, nome, banco_atual)
        reg = _registro(tabelas, chave, None, schema, nome)
        reg['origens'].add('MODEL')
        reg['models'].add(origem + ' (não carregado)')

    # ---------- 2) Rotas ----------
    schemas_busca = set(schemas_banco) | {(t.schema or schema_padrao) for t in tabelas_sa}
    regex = _regex_tabelas(schemas_busca)
    arquivos, raiz_projeto = _arquivos_rotas()
    modulos = {}

    for caminho in arquivos:
        relativo = os.path.relpath(caminho, raiz_projeto).replace('\\', '/')
        try:
            with open(caminho, 'r', encoding='utf-8') as arq:
                arvore = ast.parse(arq.read(), filename=relativo)
        except Exception as erro:
            avisos.append('Não foi possível ler {}: {}'.format(relativo, erro))
            continue

        bps = _blueprints_do_arquivo(arvore)
        id_modulo = bps[0] if bps else os.path.splitext(os.path.basename(caminho))[0]
        mod = modulos.setdefault(id_modulo, {'id': id_modulo, 'arquivos': set(), 'tabelas': set()})
        mod['arquivos'].add(relativo)

        # Uso via ORM: classes importadas de app.models
        for modulo_models, nomes in _imports_de_models(arvore).items():
            for nome in nomes:
                alvos = []
                if nome in classes:
                    alvos = classes[nome]
                elif modulo_models == 'app.models':
                    # "from app.models import arquivo" -> todas as classes do arquivo
                    for nome_cls in por_modulo.get('app.models.' + nome, []):
                        alvos.extend(classes.get(nome_cls, []))
                for t in alvos:
                    chave = _chave(None, t.schema or schema_padrao, t.name, banco_atual)
                    reg = tabelas.get(chave)
                    if reg is not None:
                        reg['origens'].add('ROTA_ORM')
                        reg['modulos'].add(id_modulo)
                        mod['tabelas'].add(chave)

        # Uso via SQL escrito à mão: BANCO.SCHEMA.TABELA ou SCHEMA.TABELA
        if regex is not None:
            for s in _strings_do_codigo(arvore):
                for m in regex.finditer(s):
                    banco, schema, nome = m.group(1), m.group(2), m.group(3)
                    if banco and not _RE_NOME_SEGURO.match(banco):
                        continue
                    chave = _chave(banco, schema, nome, banco_atual)
                    externo = bool(banco) and banco.upper() != banco_atual.upper()
                    reg = _registro(tabelas, chave, banco if externo else None, schema, nome)
                    reg['origens'].add('ROTA_SQL')
                    reg['modulos'].add(id_modulo)
                    mod['tabelas'].add(chave)

    # ---------- 3) Estrutura real no banco (só das tabelas encontradas) ----------
    por_banco = {}
    for reg in tabelas.values():
        por_banco.setdefault(reg['banco'] or '', set()).add(reg['nome'].upper())

    estrutura = {}  # chave -> dados lidos do banco
    rotinas = set()  # procedures/funções citadas nas rotas (não entram no diagrama)
    fks_banco = []
    deps_views = []
    with engine.connect() as conn:
        for banco, nomes in por_banco.items():
            if banco and not _RE_NOME_SEGURO.match(banco):
                continue
            prefixo = (_q(banco) + '.') if banco else ''
            try:
                dados = _ler_estrutura_banco(conn, prefixo, sorted(nomes))
            except Exception as erro:
                logger.warning('Modelagem: falha ao ler estrutura (%s): %s', banco or banco_atual, erro)
                avisos.append('Não foi possível ler a estrutura do banco {}: sem acesso ou banco indisponível.'
                              .format(banco or banco_atual))
                continue

            for r in dados.get('rotinas', []):
                rotinas.add(_chave(banco or None, r['SCHEMA_NOME'], r['NOME'], banco_atual))

            por_id = {}
            for o in dados['objetos']:
                chave = _chave(banco or None, o['SCHEMA_NOME'], o['NOME'], banco_atual)
                if chave not in tabelas:
                    continue  # mesmo nome em outro schema: não foi citado
                por_id[int(o['OBJECT_ID'])] = chave
                estrutura[chave] = {
                    'object_id': int(o['OBJECT_ID']), 'banco': banco or None,
                    'schema': o['SCHEMA_NOME'], 'nome': o['NOME'],
                    'tipo': 'VIEW' if (o['TIPO'] or '').strip() == 'V' else 'TABELA',
                    'colunas': [], 'pk': None, 'uniques': {}, 'indices': {},
                }

            for c in sorted(dados['colunas'], key=lambda x: (x['OBJECT_ID'], x['COLUMN_ID'])):
                chave = por_id.get(int(c['OBJECT_ID']))
                if chave:
                    estrutura[chave]['colunas'].append(c)

            for k in sorted(dados['chaves'], key=lambda x: (x['OBJECT_ID'], x['NOME'], x['ORDEM'])):
                chave = por_id.get(int(k['OBJECT_ID']))
                if not chave:
                    continue
                e = estrutura[chave]
                tipo = (k['TIPO'] or '').strip()
                if tipo == 'PK':
                    if e['pk'] is None:
                        e['pk'] = {'nome': k['NOME'], 'tipo_indice': k['TIPO_INDICE'], 'colunas': []}
                    e['pk']['colunas'].append((k['COLUNA'], bool(k['DESC_'])))
                else:
                    u = e['uniques'].setdefault(k['NOME'], {'nome': k['NOME'], 'tipo_indice': k['TIPO_INDICE'],
                                                            'colunas': []})
                    u['colunas'].append((k['COLUNA'], bool(k['DESC_'])))

            for i in sorted(dados['indices'], key=lambda x: (x['OBJECT_ID'], x['NOME'], x['INCLUIDA'],
                                                             x['ORDEM'], x['ORDEM_COL'])):
                chave = por_id.get(int(i['OBJECT_ID']))
                if not chave:
                    continue
                ind = estrutura[chave]['indices'].setdefault(i['NOME'], {
                    'nome': i['NOME'], 'tipo_indice': i['TIPO_INDICE'], 'unico': bool(i['UNICO']),
                    'filtro': i['FILTRO'], 'colunas': [], 'incluidas': []})
                if i['INCLUIDA']:
                    ind['incluidas'].append(i['COLUNA'])
                else:
                    ind['colunas'].append((i['COLUNA'], bool(i['DESC_'])))

            agrupadas = {}
            for f in sorted(dados['fks'], key=lambda x: (x['FK_ID'], x['ORDEM'])):
                g = agrupadas.setdefault(int(f['FK_ID']), {
                    'nome': f['NOME'],
                    'de': _chave(banco or None, f['DE_SCHEMA'], f['DE_TABELA'], banco_atual),
                    'para': _chave(banco or None, f['PARA_SCHEMA'], f['PARA_TABELA'], banco_atual),
                    'para_exibicao': '{}.{}'.format(f['PARA_SCHEMA'], f['PARA_TABELA']),
                    'de_exibicao': '{}.{}'.format(f['DE_SCHEMA'], f['DE_TABELA']),
                    'de_colunas': [], 'para_colunas': [],
                    'ao_excluir': f['AO_EXCLUIR'], 'ao_atualizar': f['AO_ATUALIZAR'],
                    'desativada': bool(f['DESATIVADA']),
                })
                g['de_colunas'].append(f['DE_COLUNA'])
                g['para_colunas'].append(f['PARA_COLUNA'])
            fks_banco.extend(agrupadas.values())

            for d in dados['deps']:
                origem = por_id.get(int(d['OBJECT_ID']))
                if not origem:
                    continue
                banco_ref = d['BANCO'] or banco or None
                schema_ref = d['SCHEMA_NOME'] or schema_padrao
                deps_views.append({
                    'de': origem,
                    'para': _chave(banco_ref, schema_ref, d['NOME'], banco_atual),
                    'para_exibicao': '.'.join([p for p in (d['BANCO'], schema_ref, d['NOME']) if p]),
                })

    # ---------- 4) Montagem final ----------
    for chave in list(tabelas.keys()):
        if chave in rotinas and chave not in estrutura and 'MODEL' not in tabelas[chave]['origens']:
            del tabelas[chave]
            for m in modulos.values():
                m['tabelas'].discard(chave)
    publico_tabelas = []
    interno_tabelas = {}

    for chave, reg in sorted(tabelas.items()):
        e = estrutura.get(chave)
        sa = reg['sa']
        colunas = []
        pk_cols = []
        fonte = None
        tipo = 'NAO_LOCALIZADA'

        if e:
            fonte = 'BANCO'
            tipo = e['tipo']
            pk_cols = [c for c, _d in (e['pk']['colunas'] if e['pk'] else [])]
            for c in e['colunas']:
                colunas.append({
                    'nome': c['NOME'],
                    'tipo': _tipo_sql(c['TIPO'], c['MAX_LEN'], c['PRECISAO'], c['ESCALA']),
                    'nulo': bool(c['NULO']),
                    'pk': c['NOME'] in pk_cols,
                    'identidade': bool(c['IDENTIDADE']),
                    'padrao': c['PADRAO'],
                    'calculada': c['CALCULADA'],
                })
        elif sa is not None:
            fonte = 'MODEL'
            tipo = 'TABELA'
            pk_cols = [c.name for c in sa.primary_key.columns]
            for c in sa.columns:
                try:
                    from sqlalchemy.dialects import mssql
                    tipo_col = c.type.compile(dialect=mssql.dialect())
                except Exception:
                    tipo_col = str(c.type)
                colunas.append({
                    'nome': c.name, 'tipo': str(tipo_col).upper(), 'nulo': bool(c.nullable),
                    'pk': c.name in pk_cols, 'identidade': False, 'padrao': None, 'calculada': None,
                })

        nome_exibicao = '{}.{}'.format(e['schema'] if e else reg['schema'], e['nome'] if e else reg['nome'])
        if reg['banco']:
            nome_exibicao = '{}.{}'.format(reg['banco'], nome_exibicao)

        publico_tabelas.append({
            'id': chave,
            'exibicao': nome_exibicao,
            'banco': reg['banco'],
            'schema': e['schema'] if e else reg['schema'],
            'nome': e['nome'] if e else reg['nome'],
            'tipo': tipo,
            'fonte_estrutura': fonte,
            'origens': sorted(reg['origens']),
            'models': sorted(reg['models']),
            'modulos': sorted(reg['modulos']),
            'colunas': colunas,
            'indices': [
                {'nome': i['nome'], 'unico': i['unico'], 'colunas': [c for c, _d in i['colunas']],
                 'incluidas': i['incluidas']}
                for i in (e['indices'].values() if e else [])
            ],
            'uniques': [
                {'nome': u['nome'], 'colunas': [c for c, _d in u['colunas']]}
                for u in (e['uniques'].values() if e else [])
            ],
            'refs_fora': [],
        })
        interno_tabelas[chave] = {'estrutura': e, 'sa': sa, 'exibicao': nome_exibicao}

    tabelas_por_id = {t['id']: t for t in publico_tabelas}
    ligacoes = []
    assinaturas = set()

    def _assinatura(de, cols_de, para, cols_para):
        return (de, tuple(c.upper() for c in cols_de), para, tuple(c.upper() for c in cols_para))

    # FKs reais do banco
    for f in fks_banco:
        if f['de'] not in tabelas_por_id:
            continue  # FK de uma tabela fora do escopo apontando para uma do escopo
        if f['para'] not in tabelas_por_id:
            tabelas_por_id[f['de']]['refs_fora'].append({
                'nome': f['nome'], 'colunas': f['de_colunas'], 'para': f['para_exibicao'],
                'para_colunas': f['para_colunas']})
            continue
        ass = _assinatura(f['de'], f['de_colunas'], f['para'], f['para_colunas'])
        if ass in assinaturas:
            continue
        assinaturas.add(ass)
        ligacoes.append({
            'id': 'fk:' + f['nome'], 'tipo': 'FK_BANCO', 'nome': f['nome'],
            'de': f['de'], 'de_colunas': f['de_colunas'],
            'para': f['para'], 'para_colunas': f['para_colunas'],
            'ao_excluir': f['ao_excluir'], 'ao_atualizar': f['ao_atualizar'],
            'desativada': f['desativada'], 'tambem_no_model': False,
        })

    # FKs declaradas nos models
    indice_lig = {(l['de'], tuple(c.upper() for c in l['de_colunas']), l['para'],
                   tuple(c.upper() for c in l['para_colunas'])): l for l in ligacoes}
    for t in tabelas_sa:
        de = _chave(None, t.schema or schema_padrao, t.name, banco_atual)
        if de not in tabelas_por_id:
            continue
        restricoes = {}
        for fk in t.foreign_keys:
            partes = fk.target_fullname.split('.')
            if len(partes) < 2:
                continue
            col_alvo = partes[-1]
            tab_alvo = partes[-2]
            schema_alvo = partes[-3] if len(partes) >= 3 else schema_padrao
            para = _chave(None, schema_alvo, tab_alvo, banco_atual)
            nome_restr = getattr(fk.constraint, 'name', None)
            grupo = restricoes.setdefault((id(fk.constraint), para), {
                'nome': nome_restr, 'para': para, 'de_colunas': [], 'para_colunas': [],
                'para_exibicao': '{}.{}'.format(schema_alvo, tab_alvo)})
            grupo['de_colunas'].append(fk.parent.name)
            grupo['para_colunas'].append(col_alvo)
        for g in restricoes.values():
            ass = _assinatura(de, g['de_colunas'], g['para'], g['para_colunas'])
            if ass in indice_lig:
                indice_lig[ass]['tambem_no_model'] = True
                continue
            if g['para'] not in tabelas_por_id:
                tabelas_por_id[de]['refs_fora'].append({
                    'nome': g['nome'] or '(model)', 'colunas': g['de_colunas'],
                    'para': g['para_exibicao'], 'para_colunas': g['para_colunas']})
                continue
            if ass in assinaturas:
                continue
            assinaturas.add(ass)
            ligacoes.append({
                'id': 'model:{}:{}:{}'.format(de, '+'.join(g['de_colunas']), g['para']),
                'tipo': 'FK_MODEL', 'nome': g['nome'],
                'de': de, 'de_colunas': g['de_colunas'],
                'para': g['para'], 'para_colunas': g['para_colunas'],
                'ao_excluir': None, 'ao_atualizar': None, 'desativada': False, 'tambem_no_model': True,
            })

    # Dependências de views (a view lê a tabela)
    vistos_dep = set()
    for d in deps_views:
        if d['de'] in tabelas_por_id and d['para'] in tabelas_por_id and d['de'] != d['para']:
            par = (d['de'], d['para'])
            if par in vistos_dep:
                continue
            vistos_dep.add(par)
            ligacoes.append({
                'id': 'view:{}>{}'.format(d['de'], d['para']), 'tipo': 'VIEW', 'nome': None,
                'de': d['de'], 'de_colunas': [], 'para': d['para'], 'para_colunas': [],
                'ao_excluir': None, 'ao_atualizar': None, 'desativada': False, 'tambem_no_model': False,
            })
    leituras_view = {}
    for d in deps_views:
        leituras_view.setdefault(d['de'], set()).add(d['para_exibicao'])
    for chave, lidas in leituras_view.items():
        if chave in tabelas_por_id:
            tabelas_por_id[chave]['view_le'] = sorted(lidas)

    # Ligações inferidas: coluna com o mesmo nome da PK simples de outra tabela, quando:
    #   - o nome da PK é de identificador (ID_xxx ou xxx_ID), não um número de negócio
    #     como NU_CONTRATO, que aparece em muitas tabelas sem ser chave delas; e
    #   - esse nome de PK é exclusivo de uma única tabela (evita ambiguidade).
    donos_pk = {}
    for t in publico_tabelas:
        if t['tipo'] != 'TABELA':
            continue
        pks = [c['nome'] for c in t['colunas'] if c['pk']]
        if len(pks) == 1:
            donos_pk.setdefault(pks[0].upper(), []).append(t['id'])
    colunas_com_fk = set()
    for l in ligacoes:
        if l['tipo'] in ('FK_BANCO', 'FK_MODEL'):
            for c in l['de_colunas']:
                colunas_com_fk.add((l['de'], c.upper()))
    for t in publico_tabelas:
        if t['tipo'] != 'TABELA':
            continue
        for c in t['colunas']:
            nome = c['nome'].upper()
            donos = donos_pk.get(nome)
            if not donos or len(donos) != 1 or not (nome.startswith('ID_') or nome.endswith('_ID')):
                continue
            alvo = donos[0]
            if alvo == t['id'] or c['identidade'] or (t['id'], nome) in colunas_com_fk:
                continue
            pk_alvo = [x['nome'] for x in tabelas_por_id[alvo]['colunas'] if x['pk']][0]
            ass = _assinatura(t['id'], [c['nome']], alvo, [pk_alvo])
            if ass in assinaturas:
                continue
            assinaturas.add(ass)
            ligacoes.append({
                'id': 'inf:{}.{}>{}'.format(t['id'], c['nome'], alvo), 'tipo': 'INFERIDA', 'nome': None,
                'de': t['id'], 'de_colunas': [c['nome']], 'para': alvo, 'para_colunas': [pk_alvo],
                'ao_excluir': None, 'ao_atualizar': None, 'desativada': False, 'tambem_no_model': False,
            })

    # Marca nas colunas quais participam de ligação (para o desenho e filtros)
    for l in ligacoes:
        if l['tipo'] == 'VIEW':
            continue
        for c in tabelas_por_id[l['de']]['colunas']:
            if c['nome'].upper() in [x.upper() for x in l['de_colunas']]:
                if l['tipo'] == 'INFERIDA':
                    c['fk_inferida'] = True
                else:
                    c['fk'] = True
    for t in publico_tabelas:
        for r in t['refs_fora']:
            for c in t['colunas']:
                if c['nome'].upper() in [x.upper() for x in r['colunas']]:
                    c['fk'] = True

    # Módulos (nome e categoria do cadastro de aplicativos da Auditoria V2)
    lista_modulos = []
    for m in modulos.values():
        if not m['tabelas']:
            continue
        nome, categoria = _info_aplicativo(m['id'])
        lista_modulos.append({
            'id': m['id'], 'nome': nome, 'categoria': categoria,
            'arquivos': sorted(m['arquivos']), 'qtd_tabelas': len(m['tabelas']),
        })
    lista_modulos.sort(key=lambda x: (x['nome'] or '').upper())

    contagem_tipos = {}
    for l in ligacoes:
        contagem_tipos[l['tipo']] = contagem_tipos.get(l['tipo'], 0) + 1

    resumo = {
        'tabelas': sum(1 for t in publico_tabelas if t['tipo'] == 'TABELA'),
        'views': sum(1 for t in publico_tabelas if t['tipo'] == 'VIEW'),
        'nao_localizadas': sum(1 for t in publico_tabelas if t['tipo'] == 'NAO_LOCALIZADA'),
        'so_model': sum(1 for t in publico_tabelas if t['fonte_estrutura'] == 'MODEL'),
        'colunas': sum(len(t['colunas']) for t in publico_tabelas),
        'ligacoes': contagem_tipos,
        'modulos': len(lista_modulos),
        'rotinas_ignoradas': len(rotinas),
        'arquivos_rotas': len(arquivos),
        'tempo_ms': int((time.time() - inicio) * 1000),
    }

    publico = {
        'banco': banco_atual,
        'gerado_em': datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
        'resumo': resumo,
        'modulos': lista_modulos,
        'tabelas': publico_tabelas,
        'ligacoes': ligacoes,
        'avisos': avisos,
    }
    interno = {
        'banco': banco_atual,
        'schema_padrao': schema_padrao,
        'tabelas': interno_tabelas,
        'fks_banco': fks_banco,
        'ligacoes': ligacoes,
        'publico_por_id': tabelas_por_id,
    }
    return publico, interno


def obter_modelagem(forcar=False):
    """Retorna o modelo (parte pública, em JSON). Usa o cache enquanto válido."""
    agora = time.time()
    if not forcar and _cache['publico'] is not None and agora - _cache['gerado_em'] < TTL_SEGUNDOS:
        return _cache['publico']
    with _lock:
        # Outro usuário pode ter montado enquanto este esperava o lock
        if not forcar and _cache['publico'] is not None and time.time() - _cache['gerado_em'] < TTL_SEGUNDOS:
            return _cache['publico']
        publico, interno = _construir()
        _cache['publico'] = publico
        _cache['interno'] = interno
        _cache['gerado_em'] = time.time()
        return publico


def _obter_interno():
    if _cache['interno'] is None or time.time() - _cache['gerado_em'] >= TTL_SEGUNDOS:
        obter_modelagem()
    return _cache['interno']


# =====================================================================
# Exportação SQL (DDL para SQL Server)
# =====================================================================

def _ordem_dependencia(chaves, ligacoes):
    """Ordena as tabelas para que as referenciadas venham antes de quem as
    referencia (ciclos ficam no fim, em ordem alfabética)."""
    conjunto = set(chaves)
    depende = {c: set() for c in chaves}
    for l in ligacoes:
        if l['tipo'] == 'FK_BANCO' and l['de'] in conjunto and l['para'] in conjunto and l['de'] != l['para']:
            depende[l['de']].add(l['para'])
    ordem = []
    restantes = set(chaves)
    while restantes:
        prontas = sorted(c for c in restantes if not (depende[c] & restantes))
        if not prontas:
            ordem.extend(sorted(restantes))
            break
        ordem.extend(prontas)
        restantes -= set(prontas)
    return ordem


def _ddl_tabela_banco(e):
    nome = '{}.{}'.format(_q(e['schema']), _q(e['nome']))
    linhas = []
    for c in e['colunas']:
        if c['CALCULADA']:
            linhas.append('    {} AS {}'.format(_q(c['NOME']), c['CALCULADA']))
            continue
        partes = ['    {} {}'.format(_q(c['NOME']), _tipo_sql(c['TIPO'], c['MAX_LEN'], c['PRECISAO'], c['ESCALA']))]
        if c['IDENTIDADE']:
            partes.append('IDENTITY({},{})'.format(c['SEMENTE'] if c['SEMENTE'] is not None else 1,
                                                   c['INCREMENTO'] if c['INCREMENTO'] is not None else 1))
        partes.append('NULL' if c['NULO'] else 'NOT NULL')
        if c['PADRAO']:
            if c['NOME_PADRAO']:
                partes.append('CONSTRAINT {} DEFAULT {}'.format(_q(c['NOME_PADRAO']), c['PADRAO']))
            else:
                partes.append('DEFAULT {}'.format(c['PADRAO']))
        linhas.append(' '.join(partes))

    if e['pk']:
        cols = ', '.join('{}{}'.format(_q(c), ' DESC' if d else '') for c, d in e['pk']['colunas'])
        agrup = 'CLUSTERED' if e['pk']['tipo_indice'] == 'CLUSTERED' else 'NONCLUSTERED'
        linhas.append('    CONSTRAINT {} PRIMARY KEY {} ({})'.format(_q(e['pk']['nome']), agrup, cols))
    for u in e['uniques'].values():
        cols = ', '.join('{}{}'.format(_q(c), ' DESC' if d else '') for c, d in u['colunas'])
        agrup = 'CLUSTERED' if u['tipo_indice'] == 'CLUSTERED' else 'NONCLUSTERED'
        linhas.append('    CONSTRAINT {} UNIQUE {} ({})'.format(_q(u['nome']), agrup, cols))

    texto = [
        "IF OBJECT_ID(N'{}', N'U') IS NULL".format(nome.replace("'", "''")),
        'BEGIN',
        '  CREATE TABLE {} ('.format(nome),
        ',\n'.join('  ' + l for l in linhas),
        '  );',
        'END',
        'GO',
    ]
    return '\n'.join(texto)


def _ddl_indices(e):
    nome_tab = '{}.{}'.format(_q(e['schema']), _q(e['nome']))
    blocos = []
    for i in e['indices'].values():
        tipo = 'CLUSTERED' if i['tipo_indice'] == 'CLUSTERED' else 'NONCLUSTERED'
        if 'COLUMNSTORE' in (i['tipo_indice'] or ''):
            blocos.append('-- Índice {} ({}) não exportado: tipo columnstore.'.format(i['nome'], i['tipo_indice']))
            continue
        cols = ', '.join('{}{}'.format(_q(c), ' DESC' if d else '') for c, d in i['colunas'])
        cmd = 'CREATE {}{} INDEX {} ON {} ({})'.format('UNIQUE ' if i['unico'] else '', tipo, _q(i['nome']),
                                                       nome_tab, cols)
        if i['incluidas']:
            cmd += ' INCLUDE ({})'.format(', '.join(_q(c) for c in i['incluidas']))
        if i['filtro']:
            cmd += ' WHERE {}'.format(i['filtro'])
        blocos.append(
            "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{}' AND object_id = OBJECT_ID(N'{}'))\n"
            "    {};\nGO".format(i['nome'].replace("'", "''"), nome_tab.replace("'", "''"), cmd))
    return blocos


def _ddl_model(sa):
    from sqlalchemy.dialects import mssql
    from sqlalchemy.schema import CreateTable
    try:
        sql = str(CreateTable(sa).compile(dialect=mssql.dialect()))
    except Exception:
        sql = str(CreateTable(sa, include_foreign_key_constraints=[]).compile(dialect=mssql.dialect()))
    nome = '{}.{}'.format(_q(sa.schema), _q(sa.name)) if sa.schema else _q(sa.name)
    corpo = '\n'.join('  ' + linha for linha in sql.strip().splitlines())
    return "IF OBJECT_ID(N'{}', N'U') IS NULL\nBEGIN\n{};\nEND\nGO".format(nome.replace("'", "''"), corpo)


def gerar_ddl(chaves, usuario=None):
    """Gera o script DDL (SQL Server) das tabelas informadas.
    Ordem: schemas -> tabelas (por dependência) -> índices -> FKs -> views.
    O script é idempotente: só cria o que ainda não existe."""
    interno = _obter_interno()
    validas = [c for c in dict.fromkeys(chaves or []) if c in interno['tabelas']]
    if not validas:
        return None, 0

    do_banco = [c for c in validas if interno['tabelas'][c]['estrutura'] is not None
                and interno['tabelas'][c]['estrutura']['banco'] is None]
    tabelas = [c for c in do_banco if interno['tabelas'][c]['estrutura']['tipo'] == 'TABELA']
    views = [c for c in do_banco if interno['tabelas'][c]['estrutura']['tipo'] == 'VIEW']
    so_model = [c for c in validas if interno['tabelas'][c]['estrutura'] is None
                and interno['tabelas'][c]['sa'] is not None]
    ignoradas = [c for c in validas if c not in do_banco and c not in so_model]

    saida = [
        '/* =====================================================================',
        '   Portal GEINC - Modelagem de Dados',
        '   Banco de origem : {}'.format(interno['banco']),
        '   Gerado em       : {}'.format(datetime.now().strftime('%d/%m/%Y %H:%M:%S')),
        '   Gerado por      : {}'.format(usuario or '-'),
        '   Conteúdo        : {} tabela(s), {} view(s), {} só no model'.format(len(tabelas), len(views),
                                                                                len(so_model)),
        '   Script idempotente: só cria o que ainda não existe. Não copia dados.',
        '   ===================================================================== */',
        '',
        'SET ANSI_NULLS ON;',
        'SET QUOTED_IDENTIFIER ON;',
        'GO',
        '',
    ]

    schemas = sorted({interno['tabelas'][c]['estrutura']['schema'] for c in do_banco})
    if schemas:
        saida.append('-- ===== Schemas =====')
        for s in schemas:
            saida.append("IF SCHEMA_ID(N'{0}') IS NULL EXEC(N'CREATE SCHEMA {1}');".format(
                s.replace("'", "''"), _q(s).replace("'", "''")))
        saida.extend(['GO', ''])

    if tabelas:
        saida.append('-- ===== Tabelas =====')
        for c in _ordem_dependencia(tabelas, interno['ligacoes']):
            e = interno['tabelas'][c]['estrutura']
            saida.append('-- {}'.format(interno['tabelas'][c]['exibicao']))
            saida.append(_ddl_tabela_banco(e))
            saida.append('')

    blocos_ind = []
    for c in tabelas:
        blocos_ind.extend(_ddl_indices(interno['tabelas'][c]['estrutura']))
    if blocos_ind:
        saida.append('-- ===== Índices =====')
        saida.extend(blocos_ind)
        saida.append('')

    conjunto = set(tabelas)
    fks = [f for f in interno['fks_banco'] if f['de'] in conjunto]
    if fks:
        saida.append('-- ===== Chaves estrangeiras =====')
        vistos = set()
        for f in sorted(fks, key=lambda x: x['nome']):
            if f['nome'] in vistos:
                continue
            vistos.add(f['nome'])
            e_de = interno['tabelas'][f['de']]['estrutura']
            nome_de = '{}.{}'.format(_q(e_de['schema']), _q(e_de['nome']))
            sch_para, tab_para = f['para_exibicao'].split('.', 1)
            nome_para = '{}.{}'.format(_q(sch_para), _q(tab_para))
            cmd = 'ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({})'.format(
                nome_de, _q(f['nome']), ', '.join(_q(x) for x in f['de_colunas']),
                nome_para, ', '.join(_q(x) for x in f['para_colunas']))
            if f['ao_excluir'] and f['ao_excluir'] != 'NO_ACTION':
                cmd += ' ON DELETE {}'.format(f['ao_excluir'].replace('_', ' '))
            if f['ao_atualizar'] and f['ao_atualizar'] != 'NO_ACTION':
                cmd += ' ON UPDATE {}'.format(f['ao_atualizar'].replace('_', ' '))
            prefixo = ''
            if f['para'] not in conjunto:
                prefixo = '-- Referencia tabela fora deste script (crie-a antes ou remova o comentário):\n-- '
            saida.append(prefixo + "IF OBJECT_ID(N'{}', N'F') IS NULL {};".format(
                '{}.{}'.format(_q(e_de['schema']), _q(f['nome'])).replace("'", "''"), cmd))
            if f['desativada']:
                saida.append(('-- ' if prefixo else '') +
                             'ALTER TABLE {} NOCHECK CONSTRAINT {};'.format(nome_de, _q(f['nome'])))
        saida.extend(['GO', ''])

    # Ligações que existem só no model ou foram inferidas: vão como comentário
    extras = [l for l in interno['ligacoes'] if l['tipo'] in ('FK_MODEL', 'INFERIDA') and l['de'] in set(validas)]
    if extras:
        saida.append('-- ===== Ligações não declaradas no banco (apenas referência) =====')
        for l in extras:
            origem = 'declarada só no model' if l['tipo'] == 'FK_MODEL' else 'inferida pelo nome da coluna'
            saida.append('-- {} ({}) -> {} ({})  [{}]'.format(
                l['de'], ', '.join(l['de_colunas']), l['para'], ', '.join(l['para_colunas']), origem))
        saida.append('')

    if so_model:
        saida.append('-- ===== Tabelas que existem só no model (não localizadas no banco) =====')
        for c in sorted(so_model):
            saida.append('-- {}'.format(interno['tabelas'][c]['exibicao']))
            saida.append(_ddl_model(interno['tabelas'][c]['sa']))
            saida.append('')

    if views:
        definicoes = _definicoes_views([interno['tabelas'][c]['estrutura']['object_id'] for c in views])
        saida.append('-- ===== Views =====')
        for c in sorted(views):
            e = interno['tabelas'][c]['estrutura']
            definicao = definicoes.get(e['object_id'])
            nome = '{}.{}'.format(_q(e['schema']), _q(e['nome']))
            if not definicao:
                saida.append('-- View {}: definição indisponível (criptografada ou sem permissão).'.format(nome))
                continue
            saida.append('-- {}'.format(interno['tabelas'][c]['exibicao']))
            saida.append("IF OBJECT_ID(N'{}', N'V') IS NULL EXEC(N'{}');".format(
                nome.replace("'", "''"), definicao.strip().replace("'", "''")))
            saida.extend(['GO', ''])

    if ignoradas:
        saida.append('-- ===== Não exportadas =====')
        for c in sorted(ignoradas):
            pub = interno['publico_por_id'].get(c, {})
            motivo = 'outro banco' if pub.get('banco') else 'não localizada no banco nem nos models'
            saida.append('-- {} ({})'.format(pub.get('exibicao', c), motivo))
        saida.append('')

    return '\n'.join(saida), len(validas)
