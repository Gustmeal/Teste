"""
Utilitário: app/utils/ans_auditoria.py
Auditoria própria do módulo ANS Glosas.

Regra de ouro: NADA aqui faz UPDATE ou DELETE. Tudo é gravado como
linha NOVA (append-only):

    MOV_TB058_ANS_AUDITORIA_EVENTO    -> 1 linha por ação do usuário
    MOV_TB059_ANS_AUDITORIA_ALTERACAO -> 1 linha por campo alterado
    MOV_TB060_ANS_AUDITORIA_VERSAO    -> 1 linha por versão completa do registro

Como funciona:
    1) Antes da operação, tira uma "foto" (SELECT *) das linhas que podem mudar.
    2) A operação original roda normalmente (model AnsApuracao, sem alteração).
    3) Depois, tira outra foto do MESMO escopo.
    4) Compara as fotos: cada campo diferente vira uma linha na TB059
       e cada registro alterado ganha uma nova versão na TB060.

Compatível com Python 3.9 e 3.12.
"""

import json
import time
import uuid
import traceback
from datetime import datetime, date
from decimal import Decimal
from functools import wraps

from flask import request, jsonify, flash, redirect, url_for
from flask_login import current_user
from sqlalchemy import text, bindparam

from app import db


# =====================================================================
# TABELAS
# =====================================================================

PREFIXO = 'BDDASHBOARDBI.BDG.'
TB_EVENTO = PREFIXO + 'MOV_TB058_ANS_AUDITORIA_EVENTO'
TB_ALTERACAO = PREFIXO + 'MOV_TB059_ANS_AUDITORIA_ALTERACAO'
TB_VERSAO = PREFIXO + 'MOV_TB060_ANS_AUDITORIA_VERSAO'

# Tabelas do módulo ANS que são monitoradas
TB045 = 'MOV_TB045_ANS_APURACAO'
TB046 = 'MOV_TB046_ANS_CONCLUSAO'
TB048 = 'MOV_TB048_ANS_JUSTIFICATIVA_PRESTADORA'
TB049 = 'MOV_TB049_ANS_PENALIDADES_CONCLUIDAS'
TB053 = 'MOV_TB053_DT_APLICACAO_GLOSA'

# Chave natural de cada tabela (identifica a "mesma linha" antes e depois)
CHAVES_TABELAS = {
    TB045: ('DT_APURACAO', 'nrOcorrencia'),
    TB046: ('DT_APURACAO',),
    TB048: ('DT_APURACAO', 'nrOcorrencia'),
    TB049: ('DT_APURACAO', 'GRUPO', 'PENALIDADE'),
    TB053: ('DT_APURACAO',),
}
TABELAS_COM_GRUPO = (TB045, TB049)
TABELAS_COM_OCORRENCIA = (TB045, TB048)
ORDEM_CAPTURA = (TB045, TB048, TB049, TB046, TB053)   # TB045 primeiro: descobre os grupos

NOMES_TABELAS = {
    TB045: 'Apuração (TB045)',
    TB046: 'Conclusão (TB046)',
    TB048: 'Justificativa Prestadora (TB048)',
    TB049: 'Penalidades Concluídas (TB049)',
    TB053: 'Data de Aplicação (TB053)',
}

# Campos 0/1 que viram "Sim/Não" na auditoria
CAMPOS_SIM_NAO = ('ADVERTENCIA', 'REINCIDENCIA', 'REITERACAO', 'JUST_ACEITA', 'NO_PRAZO')

# SQL Server aceita no máximo 2100 parâmetros por comando
LOTE_IN = 900

PERFIS_AUDITORIA = ('admin', 'moderador')


# =====================================================================
# CATÁLOGO DE AÇÕES / TIPOS (rótulos enviados ao front pelo backend)
# =====================================================================

ACOES = {
    'ACESSO_PAGINA':           {'rotulo': 'Acesso à ANS Glosas',              'origem': 'CONSULTA', 'cor': 'bg-light text-dark border', 'icone': 'fa-eye'},
    'ANALISE_INDIVIDUAL':      {'rotulo': 'Análise individual (Sim/Não)',     'origem': 'MANUAL',   'cor': 'bg-primary',               'icone': 'fa-check'},
    'ANALISE_LOTE':            {'rotulo': 'Análise em lote',                  'origem': 'MANUAL',   'cor': 'bg-primary',               'icone': 'fa-layer-group'},
    'ADVERTENCIA':             {'rotulo': 'Processamento de advertência',     'origem': 'REGRA',    'cor': 'bg-danger',                'icone': 'fa-gavel'},
    'REINCIDENCIA':            {'rotulo': 'Processamento de reincidência',    'origem': 'REGRA',    'cor': 'bg-warning text-dark',     'icone': 'fa-redo'},
    'REITERACAO':              {'rotulo': 'Processamento de reiteração',      'origem': 'REGRA',    'cor': 'bg-dark',                  'icone': 'fa-exclamation-circle'},
    'EDICAO_MANUAL':           {'rotulo': 'Edição manual de penalidade',      'origem': 'MANUAL',   'cor': 'bg-info text-dark',        'icone': 'fa-pen'},
    'JUST_PRESTADORA':         {'rotulo': 'Retorno da prestadora',            'origem': 'MANUAL',   'cor': 'bg-secondary',             'icone': 'fa-comment-dots'},
    'DT_APLICACAO':            {'rotulo': 'Data de aplicação da glosa',       'origem': 'MANUAL',   'cor': 'bg-secondary',             'icone': 'fa-calendar-check'},
    'CONCLUSAO':               {'rotulo': 'Conclusão da apuração',            'origem': 'MANUAL',   'cor': 'bg-success',               'icone': 'fa-lock'},
    'ACESSO_AUDITORIA':        {'rotulo': 'Acesso à auditoria',               'origem': 'CONSULTA', 'cor': 'bg-light text-dark border', 'icone': 'fa-user-shield'},
    'EXPORTACAO_AUDITORIA':    {'rotulo': 'Exportação da auditoria',          'origem': 'CONSULTA', 'cor': 'bg-light text-dark border', 'icone': 'fa-file-excel'},
    'ACESSO_NEGADO_AUDITORIA': {'rotulo': 'Acesso negado à auditoria',        'origem': 'SISTEMA',  'cor': 'bg-danger',                'icone': 'fa-ban'},
    'ACESSO_PREVIAS':          {'rotulo': 'Consulta às prévias',              'origem': 'CONSULTA', 'cor': 'bg-light text-dark border', 'icone': 'fa-eye'},
    'EXPORTACAO_PREVIAS':      {'rotulo': 'Exportação das prévias',           'origem': 'CONSULTA', 'cor': 'bg-light text-dark border', 'icone': 'fa-file-excel'},
}

ORIGENS = {
    'MANUAL':   {'rotulo': 'Manual (usuário decidiu)',   'cor': 'bg-info text-dark'},
    'REGRA':    {'rotulo': 'Regra (processamento)',       'cor': 'bg-secondary'},
    'CONSULTA': {'rotulo': 'Consulta',                    'cor': 'bg-light text-dark border'},
    'SISTEMA':  {'rotulo': 'Sistema',                     'cor': 'bg-dark'},
}

TIPOS_ALTERACAO = {
    'NAO_PARA_SIM':   {'rotulo': 'Não → Sim',        'cor': 'bg-primary'},
    'SIM_PARA_NAO':   {'rotulo': 'Sim → Não',        'cor': 'bg-warning text-dark'},
    'VAZIO_PARA_SIM': {'rotulo': 'Vazio → Sim',      'cor': 'bg-info text-dark'},
    'VAZIO_PARA_NAO': {'rotulo': 'Vazio → Não',      'cor': 'bg-secondary'},
    'SIM_PARA_VAZIO': {'rotulo': 'Sim → Vazio',      'cor': 'bg-dark'},
    'NAO_PARA_VAZIO': {'rotulo': 'Não → Vazio',      'cor': 'bg-dark'},
    'INCLUSAO':       {'rotulo': 'Inclusão',         'cor': 'bg-success'},
    'EXCLUSAO':       {'rotulo': 'Exclusão',         'cor': 'bg-danger'},
    'PREENCHIMENTO':  {'rotulo': 'Preenchimento',    'cor': 'bg-info text-dark'},
    'LIMPEZA':        {'rotulo': 'Limpeza',          'cor': 'bg-dark'},
    'ALTERACAO':      {'rotulo': 'Alteração de valor', 'cor': 'bg-secondary'},
}

TIPOS_VERSAO = {
    'ESTADO_INICIAL': {'rotulo': 'Estado inicial', 'cor': 'bg-light text-dark border'},
    'INCLUSAO':       {'rotulo': 'Inclusão',       'cor': 'bg-success'},
    'ALTERACAO':      {'rotulo': 'Alteração',      'cor': 'bg-primary'},
    'EXCLUSAO':       {'rotulo': 'Exclusão',       'cor': 'bg-danger'},
}


# =====================================================================
# HELPERS DE CONVERSÃO
# =====================================================================

def _normalizar(valor):
    """Deixa o valor comparável e serializável (datetime ANTES de date: é subclasse)."""
    if valor is None:
        return None
    if isinstance(valor, bool):
        return int(valor)
    if isinstance(valor, datetime):
        return valor.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(valor, date):
        return valor.strftime('%Y-%m-%d')
    if isinstance(valor, Decimal):
        return format(valor, 'f')
    if isinstance(valor, (bytes, bytearray)):
        return bytes(valor).hex()
    if isinstance(valor, str):
        return valor.rstrip()
    return valor


def _linha_para_dict(row):
    """Row do SQLAlchemy (1.3, 1.4 ou 2.0) -> dict normalizado."""
    mapa = row._mapping if hasattr(row, '_mapping') else row
    return {str(k): _normalizar(mapa[k]) for k in mapa.keys()}


def _texto(valor, limite=4000):
    if valor is None:
        return None
    return str(valor)[:limite]


def _json(obj):
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        return None


def _converter_int(valor):
    try:
        if valor is None or valor == '':
            return None
        return int(valor)
    except (TypeError, ValueError):
        return None


def converter_data(valor):
    """Aceita date, datetime ou string (%Y-%m-%d, %Y%m%d, %d/%m/%Y). Retorna date ou None."""
    if valor is None or valor == '':
        return None
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor
    texto_valor = str(valor).strip()[:10]
    for formato in ('%Y-%m-%d', '%Y%m%d', '%d/%m/%Y'):
        try:
            return datetime.strptime(texto_valor, formato).date()
        except ValueError:
            continue
    return None


def _montar_chave(tabela, dados):
    return '|'.join(str(dados.get(coluna)) for coluna in CHAVES_TABELAS[tabela])


def _dados_usuario():
    try:
        if current_user and current_user.is_authenticated:
            return {
                'id': _converter_int(getattr(current_user, 'id', None)),
                'nome': _texto(getattr(current_user, 'nome', None), 200),
                'email': _texto(getattr(current_user, 'email', None), 200),
                'perfil': _texto(getattr(current_user, 'perfil', None), 30),
            }
    except Exception:
        pass
    return {'id': None, 'nome': 'Sistema', 'email': None, 'perfil': None}


def _dados_requisicao():
    try:
        ip = request.headers.get('X-Forwarded-For', '') or request.remote_addr or ''
        ip = ip.split(',')[0].strip()
        return {
            'rota': _texto(request.path, 300),
            'metodo': _texto(request.method, 10),
            'ip': _texto(ip, 60),
            'user_agent': _texto(request.headers.get('User-Agent'), 500),
        }
    except RuntimeError:
        # Fora de contexto de requisição (ex.: script/rotina)
        return {'rota': None, 'metodo': None, 'ip': None, 'user_agent': None}


# =====================================================================
# CAPTURA DE ESTADO (FOTO DAS TABELAS)
# =====================================================================

def _executar_select(tabela, filtros, params, coluna_in=None, lista_in=None):
    """SELECT * com filtros fixos + IN opcional quebrado em lotes (limite de parâmetros)."""
    base = 'SELECT * FROM ' + PREFIXO + tabela + ' WHERE ' + ' AND '.join(filtros)
    if not coluna_in:
        return db.session.execute(text(base), params).fetchall()

    resultado = []
    for inicio in range(0, len(lista_in), LOTE_IN):
        pedaco = lista_in[inicio:inicio + LOTE_IN]
        sql = text(base + ' AND ' + coluna_in + ' IN :lista_in').bindparams(
            bindparam('lista_in', expanding=True)
        )
        parametros = dict(params)
        parametros['lista_in'] = pedaco
        resultado.extend(db.session.execute(sql, parametros).fetchall())
    return resultado


def _capturar_estado(dt_apuracao, tabelas, grupo=None, nrs=None, grupos_forcados=None):
    """
    Retorna ({(tabela, chave): dict_da_linha}, grupos_encontrados).
    Escopo:
        - sempre filtra por DT_APURACAO
        - grupo informado -> filtra TB045/TB049 pelo grupo
        - nrs informados  -> filtra TB045/TB048 pelas ocorrências e
                             TB049 pelos grupos dessas ocorrências
    """
    estado = {}
    grupos_encontrados = set()

    for tabela in ORDEM_CAPTURA:
        if tabela not in tabelas:
            continue

        filtros = ['DT_APURACAO = :dt']
        params = {'dt': dt_apuracao}
        coluna_in = None
        lista_in = None

        if tabela in TABELAS_COM_GRUPO and grupo is not None:
            filtros.append('GRUPO = :grupo')
            params['grupo'] = grupo
        elif tabela == TB049 and nrs:
            grupos_alvo = grupos_forcados or grupos_encontrados
            if not grupos_alvo:
                continue
            coluna_in = 'GRUPO'
            lista_in = sorted(grupos_alvo)

        if tabela in TABELAS_COM_OCORRENCIA and nrs:
            coluna_in = 'nrOcorrencia'
            lista_in = sorted(set(nrs))

        for linha in _executar_select(tabela, filtros, params, coluna_in, lista_in):
            dados = _linha_para_dict(linha)
            estado[(tabela, _montar_chave(tabela, dados))] = dados
            if tabela == TB045 and dados.get('GRUPO') is not None:
                grupos_encontrados.add(dados['GRUPO'])

    return estado, grupos_encontrados


# =====================================================================
# COMPARAÇÃO ANTES x DEPOIS
# =====================================================================

def _rotulo_sim_nao(valor):
    if valor is None:
        return 'VAZIO'
    return 'SIM' if str(valor) == '1' else 'NAO'


def _classificar(campo, anterior, novo, operacao):
    if operacao == 'INSERT':
        return 'INCLUSAO'
    if operacao == 'DELETE':
        return 'EXCLUSAO'
    if campo in CAMPOS_SIM_NAO:
        return '{}_PARA_{}'.format(_rotulo_sim_nao(anterior), _rotulo_sim_nao(novo))
    if anterior is None:
        return 'PREENCHIMENTO'
    if novo is None:
        return 'LIMPEZA'
    return 'ALTERACAO'


def _comparar(antes, depois):
    alteracoes = []
    versoes = []

    for chave_composta in sorted(set(antes) | set(depois), key=lambda c: (c[0], c[1])):
        tabela, chave_registro = chave_composta
        linha_antes = antes.get(chave_composta)
        linha_depois = depois.get(chave_composta)

        if linha_antes == linha_depois:
            continue

        if linha_antes is None:
            operacao = 'INSERT'
        elif linha_depois is None:
            operacao = 'DELETE'
        else:
            operacao = 'UPDATE'

        referencia = linha_depois if linha_depois is not None else linha_antes
        campos = sorted(set((linha_antes or {}).keys()) | set((linha_depois or {}).keys()))

        for campo in campos:
            anterior = linha_antes.get(campo) if linha_antes else None
            novo = linha_depois.get(campo) if linha_depois else None
            if operacao == 'UPDATE' and anterior == novo:
                continue
            if operacao == 'INSERT' and novo is None:
                continue
            if operacao == 'DELETE' and anterior is None:
                continue
            alteracoes.append({
                'tabela': tabela,
                'chave': chave_registro,
                'dt_apuracao': converter_data(referencia.get('DT_APURACAO')),
                'grupo': _converter_int(referencia.get('GRUPO')),
                'nr': _converter_int(referencia.get('nrOcorrencia')),
                'operacao': operacao,
                'campo': campo[:80],
                'anterior': _texto(anterior),
                'novo': _texto(novo),
                'tipo': _classificar(campo, anterior, novo, operacao),
            })

        versoes.append({
            'tabela': tabela,
            'chave': chave_registro,
            'operacao': operacao,
            'antes': linha_antes,
            'depois': linha_depois,
            'dt_apuracao': converter_data(referencia.get('DT_APURACAO')),
            'grupo': _converter_int(referencia.get('GRUPO')),
            'nr': _converter_int(referencia.get('nrOcorrencia')),
        })

    return alteracoes, versoes


# =====================================================================
# GRAVAÇÃO (SOMENTE INSERT)
# =====================================================================

def _inserir_evento(dados):
    sql = text("""
        INSERT INTO """ + TB_EVENTO + """
            (CD_EVENTO, DT_EVENTO, USUARIO_ID, USUARIO_NOME, USUARIO_EMAIL, USUARIO_PERFIL,
             ACAO, ORIGEM, DT_APURACAO, GRUPO, nrOcorrencia, SUCESSO, MENSAGEM,
             QTDE_REGISTROS_AFETADOS, QTDE_CAMPOS_ALTERADOS, PARAMETROS_JSON,
             ROTA, METODO_HTTP, IP_ORIGEM, USER_AGENT, DURACAO_MS)
        VALUES
            (:cd, SYSDATETIME(), :uid, :unome, :uemail, :uperfil,
             :acao, :origem, :dt, :grupo, :nr, :sucesso, :msg,
             :qreg, :qcampos, :params,
             :rota, :metodo, :ip, :ua, :duracao)
    """)
    db.session.execute(sql, dados)
    # Busca o ID pelo GUID (evita depender de OUTPUT/SCOPE_IDENTITY com triggers)
    return db.session.execute(
        text('SELECT ID_EVENTO FROM ' + TB_EVENTO + ' WHERE CD_EVENTO = :cd'),
        {'cd': dados['cd']}
    ).scalar()


def _inserir_alteracoes(id_evento, usuario, acao, origem, alteracoes):
    sql = text("""
        INSERT INTO """ + TB_ALTERACAO + """
            (ID_EVENTO, DT_ALTERACAO, USUARIO_ID, USUARIO_NOME, ACAO, ORIGEM,
             TABELA, CHAVE_REGISTRO, DT_APURACAO, GRUPO, nrOcorrencia,
             TIPO_OPERACAO, CAMPO, VALOR_ANTERIOR, VALOR_NOVO, TIPO_ALTERACAO)
        VALUES
            (:ev, SYSDATETIME(), :uid, :unome, :acao, :origem,
             :tabela, :chave, :dt, :grupo, :nr,
             :op, :campo, :anterior, :novo, :tipo)
    """)
    linhas = [{
        'ev': id_evento, 'uid': usuario['id'], 'unome': usuario['nome'],
        'acao': acao, 'origem': origem,
        'tabela': a['tabela'], 'chave': a['chave'][:200], 'dt': a['dt_apuracao'],
        'grupo': a['grupo'], 'nr': a['nr'], 'op': a['operacao'], 'campo': a['campo'],
        'anterior': a['anterior'], 'novo': a['novo'], 'tipo': a['tipo'],
    } for a in alteracoes]
    for inicio in range(0, len(linhas), 500):
        db.session.execute(sql, linhas[inicio:inicio + 500])


def _ultimas_versoes(tabela, chaves):
    resultado = {}
    lista = sorted(set(chaves))
    for inicio in range(0, len(lista), LOTE_IN):
        sql = text(
            'SELECT CHAVE_REGISTRO, MAX(NU_VERSAO) AS ULTIMA FROM ' + TB_VERSAO +
            ' WHERE TABELA = :tabela AND CHAVE_REGISTRO IN :chaves GROUP BY CHAVE_REGISTRO'
        ).bindparams(bindparam('chaves', expanding=True))
        for row in db.session.execute(sql, {'tabela': tabela, 'chaves': lista[inicio:inicio + LOTE_IN]}).fetchall():
            resultado[row[0]] = row[1] or 0
    return resultado


def _inserir_versoes(id_evento, usuario, acao, versoes):
    """
    Nunca atualiza versão antiga. Se o registro ainda não tem histórico,
    grava primeiro o ESTADO_INICIAL (foto de antes) e depois a nova versão.
    """
    sql = text("""
        INSERT INTO """ + TB_VERSAO + """
            (ID_EVENTO, DT_VERSAO, USUARIO_ID, USUARIO_NOME, ACAO, TABELA, CHAVE_REGISTRO,
             DT_APURACAO, GRUPO, nrOcorrencia, NU_VERSAO, TIPO_VERSAO, DADOS_JSON)
        VALUES
            (:ev, SYSDATETIME(), :uid, :unome, :acao, :tabela, :chave,
             :dt, :grupo, :nr, :versao, :tipo, :dados)
    """)
    tipo_por_operacao = {'INSERT': 'INCLUSAO', 'UPDATE': 'ALTERACAO', 'DELETE': 'EXCLUSAO'}

    por_tabela = {}
    for v in versoes:
        por_tabela.setdefault(v['tabela'], []).append(v)

    linhas = []
    for tabela, itens in por_tabela.items():
        ultimas = _ultimas_versoes(tabela, [i['chave'][:200] for i in itens])
        for v in itens:
            chave = v['chave'][:200]
            base = {
                'ev': id_evento, 'uid': usuario['id'], 'unome': usuario['nome'], 'acao': acao,
                'tabela': tabela, 'chave': chave, 'dt': v['dt_apuracao'],
                'grupo': v['grupo'], 'nr': v['nr'],
            }
            ultima = ultimas.get(chave, 0)
            if ultima == 0 and v['antes'] is not None:
                ultima += 1
                linha = dict(base)
                linha.update({'versao': ultima, 'tipo': 'ESTADO_INICIAL', 'dados': _json(v['antes'])})
                linhas.append(linha)
            ultima += 1
            linha = dict(base)
            linha.update({
                'versao': ultima,
                'tipo': tipo_por_operacao[v['operacao']],
                'dados': _json(v['depois'] if v['depois'] is not None else v['antes']),
            })
            linhas.append(linha)

    for inicio in range(0, len(linhas), 500):
        db.session.execute(sql, linhas[inicio:inicio + 500])


# =====================================================================
# CLASSE PRINCIPAL
# =====================================================================

class AuditoriaAns(object):
    """
    Uso nas rotas:
        aud = AuditoriaAns('EDICAO_MANUAL', dt_apuracao=dt, nrs=[nr], tabelas=[TB045, TB049], parametros=dados)
        ok, msg = AnsApuracao.editar_campo_individual(...)
        aud.finalizar(ok, msg)

    A falha da auditoria NUNCA derruba a operação do usuário (só imprime o traceback).
    """

    def __init__(self, acao, dt_apuracao=None, grupo=None, nrs=None, tabelas=None, parametros=None):
        self.acao = acao
        self.config = ACOES.get(acao, {'origem': 'SISTEMA'})
        self.dt_apuracao = dt_apuracao
        self.dt_evento = converter_data(dt_apuracao)
        self.grupo = _converter_int(grupo)
        self.nrs = [n for n in (_converter_int(x) for x in (nrs or [])) if n is not None]
        self.tabelas = tuple(tabelas or ())
        self.parametros = parametros
        self.inicio = time.time()
        self.usuario = _dados_usuario()
        self.requisicao = _dados_requisicao()
        self.antes = {}
        self.grupos = set()
        self.captura_ok = False
        self.finalizado = False

        if self.dt_evento and self.tabelas:
            try:
                self.antes, self.grupos = _capturar_estado(
                    self.dt_apuracao, self.tabelas, self.grupo, self.nrs or None
                )
                self.captura_ok = True
            except Exception:
                db.session.rollback()
                traceback.print_exc()

    def finalizar(self, sucesso, mensagem=None):
        if self.finalizado:
            return None
        self.finalizado = True
        try:
            alteracoes, versoes = [], []
            if self.captura_ok:
                depois, grupos_depois = _capturar_estado(
                    self.dt_apuracao, self.tabelas, self.grupo, self.nrs or None,
                    grupos_forcados=self.grupos or None
                )
                alteracoes, versoes = _comparar(self.antes, depois)
                self.grupos = self.grupos | grupos_depois

            grupo_evento = self.grupo
            if grupo_evento is None and len(self.grupos) == 1:
                grupo_evento = list(self.grupos)[0]

            id_evento = _inserir_evento({
                'cd': str(uuid.uuid4()),
                'uid': self.usuario['id'],
                'unome': self.usuario['nome'],
                'uemail': self.usuario['email'],
                'uperfil': self.usuario['perfil'],
                'acao': self.acao[:50],
                'origem': self.config.get('origem', 'SISTEMA'),
                'dt': self.dt_evento,
                'grupo': grupo_evento,
                'nr': self.nrs[0] if len(self.nrs) == 1 else None,
                'sucesso': 1 if sucesso else 0,
                'msg': _texto(mensagem, 2000),
                'qreg': len(versoes),
                'qcampos': len(alteracoes),
                'params': _json(self.parametros) if self.parametros is not None else None,
                'rota': self.requisicao['rota'],
                'metodo': self.requisicao['metodo'],
                'ip': self.requisicao['ip'],
                'ua': self.requisicao['user_agent'],
                'duracao': int((time.time() - self.inicio) * 1000),
            })

            if alteracoes:
                _inserir_alteracoes(id_evento, self.usuario, self.acao, self.config.get('origem', 'SISTEMA'), alteracoes)
            if versoes:
                _inserir_versoes(id_evento, self.usuario, self.acao, versoes)

            db.session.commit()
            return id_evento
        except Exception:
            db.session.rollback()
            print('[AUDITORIA ANS] Falha ao gravar auditoria da ação {}:'.format(self.acao))
            traceback.print_exc()
            return None


def registrar_evento_simples(acao, dt_apuracao=None, sucesso=True, mensagem=None,
                             parametros=None, grupo=None, nr_ocorrencia=None):
    """Evento sem comparação de dados (acessos, validações recusadas, exportações)."""
    auditoria = AuditoriaAns(
        acao,
        dt_apuracao=dt_apuracao,
        grupo=grupo,
        nrs=[nr_ocorrencia] if nr_ocorrencia is not None else None,
        tabelas=None,
        parametros=parametros,
    )
    return auditoria.finalizar(sucesso, mensagem)


# =====================================================================
# CONTROLE DE ACESSO (admin / moderador)
# =====================================================================

def pode_ver_auditoria():
    try:
        return bool(current_user.is_authenticated and
                    (getattr(current_user, 'perfil', '') or '').lower() in PERFIS_AUDITORIA)
    except Exception:
        return False


def auditoria_ans_required(f):
    """Usar SEMPRE abaixo de @login_required. Tentativa negada também fica auditada."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not pode_ver_auditoria():
            perfil = (getattr(current_user, 'perfil', '') or 'desconhecido')
            registrar_evento_simples(
                'ACESSO_NEGADO_AUDITORIA',
                dt_apuracao=request.args.get('dt_apuracao') or None,
                sucesso=False,
                mensagem='Tentativa de acesso à auditoria sem perfil admin/moderador (perfil: {}).'.format(perfil),
                parametros={'rota': request.path, 'args': request.args.to_dict()},
            )
            if request.headers.get('X-Silent-Request') == 'true':
                return jsonify({'success': False,
                                'message': 'Acesso restrito a administradores e moderadores.'}), 403
            flash('Acesso restrito: somente administradores e moderadores podem acessar a Auditoria da ANS Glosas.',
                  'danger')
            return redirect(url_for('sumov.ans_glosas'))
        return f(*args, **kwargs)
    return wrapper