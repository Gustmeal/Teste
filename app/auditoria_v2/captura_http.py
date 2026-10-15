# -*- coding: utf-8 -*-
"""
app/auditoria_v2/captura_http.py
Captura AUTOMÁTICA de toda ação do usuário no portal (cada requisição HTTP):
quem, quando, em qual aplicativo, qual tela/rota, com quais parâmetros, quanto
tempo levou, se deu erro, se baixou arquivo, mensagens mostradas na tela, login,
logout e tentativas de login com falha.

Funciona por ganchos do Flask registrados uma única vez no create_app(); nenhuma
rota precisa ser alterada.

Compatível com Python 3.9 e 3.12.
"""

import re
import time
import traceback

from flask import g, request, got_request_exception, message_flashed
from flask_login import user_logged_in, user_logged_out, user_unauthorized

from app.auditoria_v2 import config, contexto
from app.auditoria_v2.gravador import gravador
from app.auditoria_v2.util import agora, cortar, eh_sensivel, novo_codigo, para_json

_MIMES_DOWNLOAD = ('application/vnd', 'application/pdf', 'application/zip', 'application/octet-stream',
                   'application/msword', 'text/csv', 'application/x-zip', 'application/x-7z')
_RE_NOME_ARQUIVO = re.compile(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', re.I)


# ======================================================================
# Utilitários
# ======================================================================
def _mascarar(obj, sensiveis, nivel=0):
    if nivel > 6:
        return '...'
    if isinstance(obj, dict):
        return dict((k, '***' if eh_sensivel(k, sensiveis) and obj[k] not in (None, '')
                     else _mascarar(obj[k], sensiveis, nivel + 1)) for k in obj)
    if isinstance(obj, list):
        return [_mascarar(v, sensiveis, nivel + 1) for v in obj[:200]]
    return obj


def _multidict(md, sensiveis):
    saida = {}
    for chave in md.keys():
        valores = md.getlist(chave)
        if eh_sensivel(chave, sensiveis):
            saida[chave] = '***'
        else:
            saida[chave] = valores[0] if len(valores) == 1 else valores
    return saida


def _usuario_de_objeto(usuario):
    if usuario is None:
        return None
    empregado = getattr(usuario, 'empregado', None)
    if not isinstance(empregado, dict):
        empregado = {}
    return {
        'id': getattr(usuario, 'id', None), 'nome': getattr(usuario, 'nome', None),
        'email': getattr(usuario, 'email', None), 'perfil': getattr(usuario, 'perfil', None),
        'area': empregado.get('area'), 'cargo': empregado.get('cargo'),
    }


def _ip():
    encaminhado = request.headers.get('X-Forwarded-For', '')
    if encaminhado:
        return encaminhado.split(',')[0].strip()[:60]
    return (request.remote_addr or '')[:60]


def _evento(aud, tipo, nivel, descricao, detalhes=None, entidade=None, entidade_id=None):
    aud['itens']['eventos'].append({
        'ID_CORRELACAO': aud['id'], 'DT_EVENTO': agora(), 'TIPO': tipo, 'NIVEL': nivel,
        'IP': _ip(), 'ENTIDADE': entidade, 'ENTIDADE_ID': entidade_id,
        'DESCRICAO': cortar(descricao, 4000), 'DETALHES': para_json(detalhes),
    })


# ======================================================================
# Ganchos
# ======================================================================
def _inicio_requisicao():
    contexto.sair_flush(zerar=True)
    try:
        endpoint = request.endpoint or ''
        if endpoint == 'static' or request.path.startswith('/static/') or not contexto.captura_ligada():
            g._aud = {'ignorar': True}
            return
        if endpoint in config.obter_lista('ENDPOINTS_IGNORADOS'):
            g._aud = {'ignorar': True}
            return
        info_app = config.aplicativo(request.blueprint)
        if not info_app.get('auditar', True):
            g._aud = {'ignorar': True}
            return
        g._aud = {
            'id': novo_codigo(), 'ignorar': False, 'finalizado': False,
            't0': time.perf_counter(), 'dt': agora(),
            'consultas': 0, 'tempo_banco': 0.0,
            'blueprint': request.blueprint, 'aplicativo': info_app.get('nome'),
            'itens': contexto.novo_pacote(), 'mensagens': [], 'erro': None,
            'usuario': None, 'login_ok': False, 'logout': False, 'resposta': {},
        }
    except Exception:
        g._aud = {'ignorar': True}


def _depois_requisicao(resposta):
    aud = contexto.aud_requisicao()
    if not aud or aud.get('ignorar'):
        return resposta
    try:
        disposicao = resposta.headers.get('Content-Disposition', '') or ''
        aud['resposta'] = {
            'status': resposta.status_code,
            'mimetype': resposta.mimetype or '',
            'disposicao': disposicao,
            'tamanho': resposta.content_length,
        }
        resposta.headers['X-Auditoria-Id'] = aud['id']
    except Exception:
        pass
    return resposta


def _ao_excecao(remetente, exception, **extra):
    aud = contexto.aud_requisicao()
    if aud and not aud.get('ignorar'):
        aud['erro'] = cortar(traceback.format_exc(), 60000)


def _ao_flash(remetente, message, category, **extra):
    aud = contexto.aud_requisicao()
    if aud and not aud.get('ignorar'):
        aud['mensagens'].append({'categoria': category, 'mensagem': cortar(message, 1000)})


def _ao_login(remetente, user, **extra):
    aud = contexto.aud_requisicao()
    if aud and not aud.get('ignorar'):
        aud['login_ok'] = True
        aud['usuario'] = _usuario_de_objeto(user)
        _evento(aud, 'LOGIN', 'INFO', 'Login realizado com sucesso')


def _ao_logout(remetente, user, **extra):
    aud = contexto.aud_requisicao()
    if aud and not aud.get('ignorar'):
        aud['logout'] = True
        aud['usuario'] = _usuario_de_objeto(user)
        _evento(aud, 'LOGOUT', 'INFO', 'Logout realizado')


def _ao_nao_autorizado(remetente, **extra):
    aud = contexto.aud_requisicao()
    if aud and not aud.get('ignorar'):
        _evento(aud, 'ACESSO_SEM_LOGIN', 'INFO',
                'Tentativa de abrir página protegida sem estar logado (ou sessão expirada)',
                {'url': request.url})


def _classificar(aud, metodo, status, download, ajax, qtde_escrita):
    mensagens_negadas = [m for m in aud['mensagens']
                         if m.get('categoria') in ('danger', 'warning', 'error')
                         and ('acesso' in (m.get('mensagem') or '').lower()
                              or 'permiss' in (m.get('mensagem') or '').lower())]
    if aud.get('login_ok'):
        return 'LOGIN'
    if aud.get('logout'):
        return 'LOGOUT'
    if aud.get('login_falho'):
        return 'LOGIN_FALHO'
    if aud.get('erro') or (status or 0) >= 500:
        return 'ERRO'
    if status in (401, 403) or mensagens_negadas:
        return 'ACESSO_NEGADO'
    if download:
        return 'EXPORTACAO'
    if qtde_escrita:
        return 'ALTERACAO'
    if metodo not in ('GET', 'HEAD', 'OPTIONS'):
        return 'SUBMISSAO'
    if ajax:
        return 'CONSULTA'
    return 'ACESSO'


def _fim_requisicao(excecao=None):
    aud = contexto.aud_requisicao()
    if not aud or aud.get('ignorar') or aud.get('finalizado'):
        return
    try:
        if excecao is not None and not aud.get('erro'):
            aud['erro'] = cortar(''.join(traceback.format_exception(
                type(excecao), excecao, excecao.__traceback__)), 60000)
        sensiveis = config.obter_lista('CAMPOS_SENSIVEIS')
        resposta = aud.get('resposta') or {}
        status = resposta.get('status') or (500 if aud.get('erro') else None)
        mimetype = resposta.get('mimetype') or ''
        disposicao = resposta.get('disposicao') or ''
        download = 'attachment' in disposicao.lower() or mimetype.startswith(_MIMES_DOWNLOAD)
        arquivo = None
        if download:
            m = _RE_NOME_ARQUIVO.search(disposicao)
            arquivo = cortar(m.group(1) if m else '(arquivo sem nome) ' + mimetype, 400)
        ajax = (request.headers.get('X-Requested-With') == 'XMLHttpRequest'
                or bool(request.headers.get('X-Silent-Request'))
                or 'application/json' in (request.headers.get('Accept') or '')
                or mimetype == 'application/json')

        # Login com falha: POST na rota de login sem login_user()
        if request.method == 'POST' and request.endpoint == config.obter('ENDPOINT_LOGIN') \
                and not aud.get('login_ok'):
            aud['login_falho'] = True
            email = (request.form.get('email') or request.form.get('usuario') or '').strip()
            _evento(aud, 'LOGIN_FALHO', 'ALERTA', 'Tentativa de login sem sucesso: ' + cortar(email, 150),
                    {'email_informado': email, 'mensagens': aud['mensagens']})

        if aud.get('erro'):
            _evento(aud, 'ERRO_APLICACAO', 'CRITICO', 'Erro na rota ' + (request.endpoint or request.path),
                    {'erro': cortar(aud['erro'], 8000)})

        itens = aud['itens']
        confirmados = [c for c in itens['comandos'] if c.get('SITUACAO') == 'CONFIRMADA']
        qtde_escrita = len(confirmados) + len([a for a in itens['alteracoes'] if a.get('ORIGEM') == 'ORM'])
        qtde_registros = len(itens['alteracoes'])
        tipo = _classificar(aud, request.method, status, download, ajax, qtde_escrita or qtde_registros)

        sem_itens = not any(itens[n] for n in itens)
        if tipo in ('ACESSO', 'CONSULTA') and sem_itens and str(config.obter('REGISTRAR_ACESSOS')).strip() == '0':
            aud['finalizado'] = True
            return

        parametros = {}
        if request.view_args:
            parametros['rota'] = _mascarar(dict(request.view_args), sensiveis)
        if request.args:
            parametros['url'] = _multidict(request.args, sensiveis)
        if request.method != 'GET':
            if request.form:
                parametros['formulario'] = _multidict(request.form, sensiveis)
            if request.is_json:
                parametros['json'] = _mascarar(request.get_json(silent=True), sensiveis)
            if request.files:
                parametros['arquivos'] = [{'campo': k, 'nome': f.filename, 'tipo': f.mimetype}
                                          for k, f in request.files.items(multi=True)]

        usuario = aud.get('usuario') or contexto.usuario_logado() or {}
        if not usuario.get('nome'):
            usuario = dict(usuario)
            usuario['nome'] = 'Anônimo (não logado)'

        requisicao = {
            'ID_CORRELACAO': aud['id'], 'DT_INICIO': aud['dt'],
            'DURACAO_MS': int((time.perf_counter() - aud['t0']) * 1000),
            'USUARIO_ID': usuario.get('id'), 'USUARIO_NOME': cortar(usuario.get('nome'), 150),
            'USUARIO_EMAIL': cortar(usuario.get('email'), 150), 'USUARIO_PERFIL': cortar(usuario.get('perfil'), 30),
            'USUARIO_AREA': cortar(usuario.get('area'), 30), 'USUARIO_CARGO': cortar(usuario.get('cargo'), 150),
            'IP': _ip(), 'NAVEGADOR': cortar(request.headers.get('User-Agent'), 500),
            'METODO': request.method, 'URL': cortar(request.full_path.rstrip('?'), 2000),
            'PAGINA_ORIGEM': cortar(request.referrer, 2000), 'ROTA': cortar(request.endpoint, 200),
            'BLUEPRINT': aud.get('blueprint'), 'APLICATIVO': aud.get('aplicativo'),
            'TIPO_ACAO': tipo, 'STATUS_HTTP': status, 'AJAX': bool(ajax),
            'PARAMETROS': para_json(parametros) if parametros else None,
            'ARQUIVO_DOWNLOAD': arquivo, 'TAMANHO_RESPOSTA': resposta.get('tamanho'),
            'QTDE_CONSULTAS_BANCO': aud.get('consultas', 0),
            'TEMPO_BANCO_MS': int(aud.get('tempo_banco', 0.0)),
            'QTDE_COMANDOS_ESCRITA': len(itens['comandos']),
            'QTDE_REGISTROS_ALTERADOS': qtde_registros,
            'MENSAGENS': para_json(aud['mensagens']) if aud['mensagens'] else None,
            'ERRO': aud.get('erro'),
        }
        aud['finalizado'] = True
        aud['usuario'] = usuario
        itens['requisicoes'].append(requisicao)
        for evento in itens['eventos']:
            evento.setdefault('IP', requisicao['IP'])
        gravador.enfileirar(contexto.preencher(itens, usuario, aud.get('blueprint'), aud.get('aplicativo')))
    except Exception:
        aud['finalizado'] = True
        try:
            from app.utils.log_seguro import log_erro
            log_erro('Auditoria V2 (fim requisição): ' + traceback.format_exc())
        except Exception:
            pass


def instalar(app):
    """Registra os ganchos HTTP. O gancho de início roda ANTES de qualquer outro."""
    app.before_request_funcs.setdefault(None, []).insert(0, _inicio_requisicao)
    app.after_request(_depois_requisicao)
    app.teardown_request(_fim_requisicao)
    got_request_exception.connect(_ao_excecao, app)
    message_flashed.connect(_ao_flash, app)
    user_logged_in.connect(_ao_login, app)
    user_logged_out.connect(_ao_logout, app)
    user_unauthorized.connect(_ao_nao_autorizado, app)
