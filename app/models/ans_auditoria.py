"""
Model: app/models/ans_auditoria.py
Consultas da Auditoria do módulo ANS Glosas (somente leitura).
Tabelas: MOV_TB058 (eventos), MOV_TB059 (alterações), MOV_TB060 (versões).
Compatível com Python 3.9 e 3.12.
"""

import json
import re
from datetime import datetime, date

from sqlalchemy import text

from app import db
from app.utils.ans_auditoria import (
    TB_EVENTO, TB_ALTERACAO, TB_VERSAO, TB045, TB048,
    ACOES, ORIGENS, TIPOS_ALTERACAO, TIPOS_VERSAO, NOMES_TABELAS, CAMPOS_SIM_NAO,
    converter_data,
)

RE_DATA_ISO = re.compile(r'^\d{4}-\d{2}-\d{2}$')
RE_DATA_HORA_ISO = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')

LIMITE_EXPORTACAO = 100000


def _dict(row):
    mapa = row._mapping if hasattr(row, '_mapping') else row
    return {str(k): mapa[k] for k in mapa.keys()}


def _fmt_data_hora(valor):
    if valor is None:
        return ''
    if isinstance(valor, datetime):
        return valor.strftime('%d/%m/%Y %H:%M:%S')
    if isinstance(valor, date):
        return valor.strftime('%d/%m/%Y')
    return str(valor)


def _fmt_data(valor):
    if valor is None:
        return ''
    if isinstance(valor, (datetime, date)):
        return valor.strftime('%d/%m/%Y')
    return str(valor)


def formatar_valor(campo, valor):
    """Valor gravado como texto na auditoria -> texto amigável."""
    if valor is None:
        return '(vazio)'
    texto_valor = str(valor)
    if campo in CAMPOS_SIM_NAO:
        if texto_valor == '1':
            return 'Sim'
        if texto_valor == '0':
            return 'Não'
    if RE_DATA_ISO.match(texto_valor):
        return '{}/{}/{}'.format(texto_valor[8:10], texto_valor[5:7], texto_valor[0:4])
    if RE_DATA_HORA_ISO.match(texto_valor):
        return '{}/{}/{} {}'.format(texto_valor[8:10], texto_valor[5:7], texto_valor[0:4], texto_valor[11:])
    return texto_valor


def _int(valor):
    try:
        if valor is None or str(valor).strip() == '':
            return None
        return int(valor)
    except (TypeError, ValueError):
        return None


class AnsAuditoria(object):

    POR_PAGINA = 50

    # ==================================================================
    # FILTROS
    # ==================================================================

    @staticmethod
    def ler_filtros(args):
        """Lê e sanitiza os filtros da querystring."""
        origem = (args.get('origem') or '').strip().upper()
        sucesso = (args.get('sucesso') or '').strip()
        return {
            'dt_apuracao': converter_data(args.get('dt_apuracao')),
            'dt_inicio': converter_data(args.get('dt_inicio')),
            'dt_fim': converter_data(args.get('dt_fim')),
            'usuario_id': _int(args.get('usuario_id')),
            'acao': (args.get('acao') or '').strip()[:50] or None,
            'grupo': _int(args.get('grupo')),
            'nr_ocorrencia': _int(args.get('nr_ocorrencia')),
            'tipo_alteracao': (args.get('tipo_alteracao') or '').strip()[:30] or None,
            'campo': (args.get('campo') or '').strip()[:80] or None,
            'origem': origem[:20] or None,
            'sucesso': sucesso if sucesso in ('0', '1') else None,
        }

    @staticmethod
    def filtros_para_url(filtros):
        """Filtros preenchidos -> dict de strings (usado em links e na exportação)."""
        saida = {}
        for chave, valor in filtros.items():
            if valor is None or valor == '':
                continue
            if isinstance(valor, (datetime, date)):
                saida[chave] = valor.strftime('%Y-%m-%d')
            else:
                saida[chave] = str(valor)
        return saida

    @staticmethod
    def _where_eventos(f):
        condicoes = ['1=1']
        params = {}
        if f.get('dt_apuracao'):
            condicoes.append('E.DT_APURACAO = :dt_apuracao')
            params['dt_apuracao'] = f['dt_apuracao']
        if f.get('dt_inicio'):
            condicoes.append('E.DT_EVENTO >= :dt_inicio')
            params['dt_inicio'] = f['dt_inicio']
        if f.get('dt_fim'):
            condicoes.append('E.DT_EVENTO < DATEADD(DAY, 1, CAST(:dt_fim AS DATE))')
            params['dt_fim'] = f['dt_fim']
        if f.get('usuario_id') is not None:
            condicoes.append('E.USUARIO_ID = :usuario_id')
            params['usuario_id'] = f['usuario_id']
        if f.get('acao'):
            condicoes.append('E.ACAO = :acao')
            params['acao'] = f['acao']
        if f.get('origem'):
            condicoes.append('E.ORIGEM = :origem')
            params['origem'] = f['origem']
        if f.get('sucesso') is not None:
            condicoes.append('E.SUCESSO = :sucesso')
            params['sucesso'] = int(f['sucesso'])
        # Grupo/ocorrência: vale o próprio evento OU qualquer alteração dele (lote/processamento)
        if f.get('grupo') is not None:
            condicoes.append('(E.GRUPO = :grupo OR EXISTS (SELECT 1 FROM ' + TB_ALTERACAO +
                             ' A1 WHERE A1.ID_EVENTO = E.ID_EVENTO AND A1.GRUPO = :grupo))')
            params['grupo'] = f['grupo']
        if f.get('nr_ocorrencia') is not None:
            condicoes.append('(E.nrOcorrencia = :nr OR EXISTS (SELECT 1 FROM ' + TB_ALTERACAO +
                             ' A2 WHERE A2.ID_EVENTO = E.ID_EVENTO AND A2.nrOcorrencia = :nr))')
            params['nr'] = f['nr_ocorrencia']
        if f.get('tipo_alteracao'):
            condicoes.append('EXISTS (SELECT 1 FROM ' + TB_ALTERACAO +
                             ' A3 WHERE A3.ID_EVENTO = E.ID_EVENTO AND A3.TIPO_ALTERACAO = :tipo)')
            params['tipo'] = f['tipo_alteracao']
        if f.get('campo'):
            condicoes.append('EXISTS (SELECT 1 FROM ' + TB_ALTERACAO +
                             ' A4 WHERE A4.ID_EVENTO = E.ID_EVENTO AND A4.CAMPO = :campo)')
            params['campo'] = f['campo']
        return ' AND '.join(condicoes), params

    @staticmethod
    def _where_alteracoes(f):
        condicoes = ['1=1']
        params = {}
        if f.get('dt_apuracao'):
            condicoes.append('A.DT_APURACAO = :dt_apuracao')
            params['dt_apuracao'] = f['dt_apuracao']
        if f.get('dt_inicio'):
            condicoes.append('A.DT_ALTERACAO >= :dt_inicio')
            params['dt_inicio'] = f['dt_inicio']
        if f.get('dt_fim'):
            condicoes.append('A.DT_ALTERACAO < DATEADD(DAY, 1, CAST(:dt_fim AS DATE))')
            params['dt_fim'] = f['dt_fim']
        if f.get('usuario_id') is not None:
            condicoes.append('A.USUARIO_ID = :usuario_id')
            params['usuario_id'] = f['usuario_id']
        if f.get('acao'):
            condicoes.append('A.ACAO = :acao')
            params['acao'] = f['acao']
        if f.get('origem'):
            condicoes.append('A.ORIGEM = :origem')
            params['origem'] = f['origem']
        if f.get('grupo') is not None:
            condicoes.append('A.GRUPO = :grupo')
            params['grupo'] = f['grupo']
        if f.get('nr_ocorrencia') is not None:
            condicoes.append('A.nrOcorrencia = :nr')
            params['nr'] = f['nr_ocorrencia']
        if f.get('tipo_alteracao'):
            condicoes.append('A.TIPO_ALTERACAO = :tipo')
            params['tipo'] = f['tipo_alteracao']
        if f.get('campo'):
            condicoes.append('A.CAMPO = :campo')
            params['campo'] = f['campo']
        if f.get('sucesso') == '0':
            # Evento com falha não gera alteração
            condicoes.append('1=0')
        return ' AND '.join(condicoes), params

    # ==================================================================
    # PAGINAÇÃO
    # ==================================================================

    @staticmethod
    def _paginacao(total, pagina, por_pagina):
        total_paginas = max(1, (total + por_pagina - 1) // por_pagina)
        pagina = min(max(1, pagina), total_paginas)
        inicio = max(1, pagina - 3)
        fim = min(total_paginas, pagina + 3)
        return {
            'pagina': pagina,
            'total': total,
            'total_paginas': total_paginas,
            'tem_anterior': pagina > 1,
            'tem_proxima': pagina < total_paginas,
            'paginas': list(range(inicio, fim + 1)),
            'offset': (pagina - 1) * por_pagina,
        }

    # ==================================================================
    # FORMATAÇÃO DE LINHAS
    # ==================================================================

    @staticmethod
    def _formatar_evento(r):
        d = _dict(r)
        acao = ACOES.get(d.get('ACAO'), {})
        origem = ORIGENS.get(d.get('ORIGEM'), {})
        return {
            'id_evento': d.get('ID_EVENTO'),
            'dt_evento': _fmt_data_hora(d.get('DT_EVENTO')),
            'usuario_id': d.get('USUARIO_ID'),
            'usuario_nome': d.get('USUARIO_NOME') or 'Sistema',
            'usuario_email': d.get('USUARIO_EMAIL') or '',
            'usuario_perfil': d.get('USUARIO_PERFIL') or '',
            'acao': d.get('ACAO'),
            'acao_rotulo': acao.get('rotulo', d.get('ACAO')),
            'acao_cor': acao.get('cor', 'bg-secondary'),
            'acao_icone': acao.get('icone', 'fa-circle'),
            'origem': d.get('ORIGEM'),
            'origem_rotulo': origem.get('rotulo', d.get('ORIGEM')),
            'origem_cor': origem.get('cor', 'bg-secondary'),
            'dt_apuracao': _fmt_data(d.get('DT_APURACAO')),
            'dt_apuracao_iso': d.get('DT_APURACAO').strftime('%Y-%m-%d') if d.get('DT_APURACAO') else '',
            'grupo': d.get('GRUPO'),
            'nr_ocorrencia': d.get('nrOcorrencia'),
            'sucesso': bool(d.get('SUCESSO')),
            'mensagem': d.get('MENSAGEM') or '',
            'qtde_registros': d.get('QTDE_REGISTROS_AFETADOS') or 0,
            'qtde_campos': d.get('QTDE_CAMPOS_ALTERADOS') or 0,
            'parametros_json': d.get('PARAMETROS_JSON'),
            'rota': d.get('ROTA') or '',
            'metodo': d.get('METODO_HTTP') or '',
            'ip': d.get('IP_ORIGEM') or '',
            'user_agent': d.get('USER_AGENT') or '',
            'duracao_ms': d.get('DURACAO_MS'),
        }

    @staticmethod
    def _formatar_alteracao(r):
        d = _dict(r)
        acao = ACOES.get(d.get('ACAO'), {})
        tipo = TIPOS_ALTERACAO.get(d.get('TIPO_ALTERACAO'), {})
        origem = ORIGENS.get(d.get('ORIGEM'), {})
        campo = d.get('CAMPO')
        return {
            'id_alteracao': d.get('ID_ALTERACAO'),
            'id_evento': d.get('ID_EVENTO'),
            'dt_alteracao': _fmt_data_hora(d.get('DT_ALTERACAO')),
            'usuario_nome': d.get('USUARIO_NOME') or 'Sistema',
            'acao': d.get('ACAO'),
            'acao_rotulo': acao.get('rotulo', d.get('ACAO')),
            'acao_cor': acao.get('cor', 'bg-secondary'),
            'origem': d.get('ORIGEM'),
            'origem_rotulo': origem.get('rotulo', d.get('ORIGEM')),
            'origem_cor': origem.get('cor', 'bg-secondary'),
            'tabela': d.get('TABELA'),
            'tabela_rotulo': NOMES_TABELAS.get(d.get('TABELA'), d.get('TABELA')),
            'dt_apuracao': _fmt_data(d.get('DT_APURACAO')),
            'grupo': d.get('GRUPO'),
            'nr_ocorrencia': d.get('nrOcorrencia'),
            'operacao': d.get('TIPO_OPERACAO'),
            'campo': campo,
            'valor_anterior': formatar_valor(campo, d.get('VALOR_ANTERIOR')),
            'valor_novo': formatar_valor(campo, d.get('VALOR_NOVO')),
            'tipo': d.get('TIPO_ALTERACAO'),
            'tipo_rotulo': tipo.get('rotulo', d.get('TIPO_ALTERACAO')),
            'tipo_cor': tipo.get('cor', 'bg-secondary'),
        }

    # ==================================================================
    # LISTAGENS
    # ==================================================================

    @staticmethod
    def listar_eventos(filtros, pagina=1, por_pagina=None):
        por_pagina = por_pagina or AnsAuditoria.POR_PAGINA
        where, params = AnsAuditoria._where_eventos(filtros)
        total = db.session.execute(
            text('SELECT COUNT(*) FROM ' + TB_EVENTO + ' E WHERE ' + where), params
        ).scalar() or 0
        pag = AnsAuditoria._paginacao(total, pagina, por_pagina)
        p = dict(params)
        p.update({'off': pag['offset'], 'lim': por_pagina})
        rows = db.session.execute(text(
            'SELECT E.* FROM ' + TB_EVENTO + ' E WHERE ' + where +
            ' ORDER BY E.DT_EVENTO DESC, E.ID_EVENTO DESC OFFSET :off ROWS FETCH NEXT :lim ROWS ONLY'
        ), p).fetchall()
        return [AnsAuditoria._formatar_evento(r) for r in rows], pag

    @staticmethod
    def listar_alteracoes(filtros, pagina=1, por_pagina=None):
        por_pagina = por_pagina or AnsAuditoria.POR_PAGINA
        where, params = AnsAuditoria._where_alteracoes(filtros)
        total = db.session.execute(
            text('SELECT COUNT(*) FROM ' + TB_ALTERACAO + ' A WHERE ' + where), params
        ).scalar() or 0
        pag = AnsAuditoria._paginacao(total, pagina, por_pagina)
        p = dict(params)
        p.update({'off': pag['offset'], 'lim': por_pagina})
        rows = db.session.execute(text(
            'SELECT A.* FROM ' + TB_ALTERACAO + ' A WHERE ' + where +
            ' ORDER BY A.DT_ALTERACAO DESC, A.ID_ALTERACAO DESC OFFSET :off ROWS FETCH NEXT :lim ROWS ONLY'
        ), p).fetchall()
        return [AnsAuditoria._formatar_alteracao(r) for r in rows], pag

    # ==================================================================
    # ESTATÍSTICAS
    # ==================================================================

    @staticmethod
    def estatisticas(filtros):
        where_e, params_e = AnsAuditoria._where_eventos(filtros)
        ev = db.session.execute(text("""
            SELECT COUNT(*) AS TOTAL,
                   SUM(CASE WHEN E.SUCESSO = 0 THEN 1 ELSE 0 END) AS FALHAS,
                   SUM(CASE WHEN E.ORIGEM = 'CONSULTA' THEN 1 ELSE 0 END) AS CONSULTAS,
                   COUNT(DISTINCT E.USUARIO_ID) AS USUARIOS,
                   MAX(E.DT_EVENTO) AS ULTIMO
            FROM """ + TB_EVENTO + " E WHERE " + where_e), params_e).fetchone()

        where_a, params_a = AnsAuditoria._where_alteracoes(filtros)
        alt = db.session.execute(text("""
            SELECT COUNT(*) AS TOTAL,
                   SUM(CASE WHEN A.ORIGEM = 'MANUAL' THEN 1 ELSE 0 END) AS MANUAIS,
                   SUM(CASE WHEN A.TIPO_ALTERACAO = 'SIM_PARA_NAO' THEN 1 ELSE 0 END) AS SIM_NAO,
                   SUM(CASE WHEN A.TIPO_ALTERACAO = 'NAO_PARA_SIM' THEN 1 ELSE 0 END) AS NAO_SIM,
                   COUNT(DISTINCT A.nrOcorrencia) AS OCORRENCIAS
            FROM """ + TB_ALTERACAO + " A WHERE " + where_a), params_a).fetchone()

        return {
            'total_eventos': (ev[0] if ev else 0) or 0,
            'falhas': (ev[1] if ev else 0) or 0,
            'consultas': (ev[2] if ev else 0) or 0,
            'usuarios': (ev[3] if ev else 0) or 0,
            'ultimo_evento': _fmt_data_hora(ev[4]) if ev and ev[4] else '',
            'total_alteracoes': (alt[0] if alt else 0) or 0,
            'manuais': (alt[1] if alt else 0) or 0,
            'sim_para_nao': (alt[2] if alt else 0) or 0,
            'nao_para_sim': (alt[3] if alt else 0) or 0,
            'ocorrencias': (alt[4] if alt else 0) or 0,
        }

    @staticmethod
    def resumo_por_usuario(filtros):
        where_e, params_e = AnsAuditoria._where_eventos(filtros)
        where_a, params_a = AnsAuditoria._where_alteracoes(filtros)
        params = dict(params_e)
        params.update(params_a)   # mesmos nomes/valores nos dois WHERE
        rows = db.session.execute(text("""
            SELECT EV.USUARIO_ID, EV.USUARIO_NOME, EV.USUARIO_PERFIL, EV.EVENTOS, EV.FALHAS,
                   EV.PRIMEIRO, EV.ULTIMO,
                   ISNULL(AL.CAMPOS, 0) AS CAMPOS, ISNULL(AL.MANUAIS, 0) AS MANUAIS,
                   ISNULL(AL.SIM_NAO, 0) AS SIM_NAO, ISNULL(AL.NAO_SIM, 0) AS NAO_SIM,
                   ISNULL(AL.ADV, 0) AS ADV, ISNULL(AL.REINC, 0) AS REINC, ISNULL(AL.REIT, 0) AS REIT
            FROM (
                SELECT E.USUARIO_ID, MAX(E.USUARIO_NOME) AS USUARIO_NOME, MAX(E.USUARIO_PERFIL) AS USUARIO_PERFIL,
                       COUNT(*) AS EVENTOS, SUM(CASE WHEN E.SUCESSO = 0 THEN 1 ELSE 0 END) AS FALHAS,
                       MIN(E.DT_EVENTO) AS PRIMEIRO, MAX(E.DT_EVENTO) AS ULTIMO
                FROM """ + TB_EVENTO + " E WHERE " + where_e + """
                GROUP BY E.USUARIO_ID
            ) EV
            LEFT JOIN (
                SELECT A.USUARIO_ID, COUNT(*) AS CAMPOS,
                       SUM(CASE WHEN A.ORIGEM = 'MANUAL' THEN 1 ELSE 0 END) AS MANUAIS,
                       SUM(CASE WHEN A.TIPO_ALTERACAO = 'SIM_PARA_NAO' THEN 1 ELSE 0 END) AS SIM_NAO,
                       SUM(CASE WHEN A.TIPO_ALTERACAO = 'NAO_PARA_SIM' THEN 1 ELSE 0 END) AS NAO_SIM,
                       SUM(CASE WHEN A.CAMPO = 'ADVERTENCIA' THEN 1 ELSE 0 END) AS ADV,
                       SUM(CASE WHEN A.CAMPO = 'REINCIDENCIA' THEN 1 ELSE 0 END) AS REINC,
                       SUM(CASE WHEN A.CAMPO = 'REITERACAO' THEN 1 ELSE 0 END) AS REIT
                FROM """ + TB_ALTERACAO + " A WHERE " + where_a + """
                GROUP BY A.USUARIO_ID
            ) AL ON (AL.USUARIO_ID = EV.USUARIO_ID OR (AL.USUARIO_ID IS NULL AND EV.USUARIO_ID IS NULL))
            ORDER BY EV.EVENTOS DESC
        """), params).fetchall()

        resultado = []
        for r in rows:
            d = _dict(r)
            resultado.append({
                'usuario_id': d['USUARIO_ID'],
                'usuario_nome': d['USUARIO_NOME'] or 'Sistema',
                'usuario_perfil': d['USUARIO_PERFIL'] or '',
                'eventos': d['EVENTOS'] or 0,
                'falhas': d['FALHAS'] or 0,
                'primeiro': _fmt_data_hora(d['PRIMEIRO']),
                'ultimo': _fmt_data_hora(d['ULTIMO']),
                'campos': d['CAMPOS'] or 0,
                'manuais': d['MANUAIS'] or 0,
                'sim_para_nao': d['SIM_NAO'] or 0,
                'nao_para_sim': d['NAO_SIM'] or 0,
                'adv': d['ADV'] or 0,
                'reinc': d['REINC'] or 0,
                'reit': d['REIT'] or 0,
            })
        return resultado

    # ==================================================================
    # OPÇÕES DOS FILTROS (tudo do banco)
    # ==================================================================

    @staticmethod
    def opcoes_filtro():
        datas = [r[0] for r in db.session.execute(text(
            'SELECT DISTINCT DT_APURACAO FROM ' + TB_EVENTO +
            ' WHERE DT_APURACAO IS NOT NULL ORDER BY DT_APURACAO DESC'
        )).fetchall()]

        usuarios = [{'id': r[0], 'nome': r[1]} for r in db.session.execute(text(
            'SELECT USUARIO_ID, MAX(USUARIO_NOME) AS NOME FROM ' + TB_EVENTO +
            ' WHERE USUARIO_ID IS NOT NULL GROUP BY USUARIO_ID ORDER BY MAX(USUARIO_NOME)'
        )).fetchall()]

        acoes = []
        for r in db.session.execute(text(
                'SELECT DISTINCT ACAO FROM ' + TB_EVENTO + ' ORDER BY ACAO')).fetchall():
            acoes.append({'valor': r[0], 'rotulo': ACOES.get(r[0], {}).get('rotulo', r[0])})

        origens = []
        for r in db.session.execute(text(
                'SELECT DISTINCT ORIGEM FROM ' + TB_EVENTO + ' ORDER BY ORIGEM')).fetchall():
            origens.append({'valor': r[0], 'rotulo': ORIGENS.get(r[0], {}).get('rotulo', r[0])})

        grupos = [r[0] for r in db.session.execute(text(
            'SELECT GRUPO FROM ' + TB_EVENTO + ' WHERE GRUPO IS NOT NULL '
            'UNION SELECT GRUPO FROM ' + TB_ALTERACAO + ' WHERE GRUPO IS NOT NULL ORDER BY GRUPO'
        )).fetchall()]

        tipos = []
        for r in db.session.execute(text(
                'SELECT DISTINCT TIPO_ALTERACAO FROM ' + TB_ALTERACAO + ' ORDER BY TIPO_ALTERACAO')).fetchall():
            tipos.append({'valor': r[0], 'rotulo': TIPOS_ALTERACAO.get(r[0], {}).get('rotulo', r[0])})

        campos = [r[0] for r in db.session.execute(text(
            'SELECT DISTINCT CAMPO FROM ' + TB_ALTERACAO + ' ORDER BY CAMPO'
        )).fetchall()]

        return {
            'datas': [{'valor': d.strftime('%Y-%m-%d'), 'rotulo': d.strftime('%d/%m/%Y')} for d in datas if d],
            'usuarios': usuarios,
            'acoes': acoes,
            'origens': origens,
            'grupos': grupos,
            'tipos': tipos,
            'campos': campos,
        }

    # ==================================================================
    # DETALHE DO EVENTO
    # ==================================================================

    @staticmethod
    def obter_evento(id_evento):
        row = db.session.execute(
            text('SELECT * FROM ' + TB_EVENTO + ' WHERE ID_EVENTO = :id'), {'id': id_evento}
        ).fetchone()
        if not row:
            return None
        evento = AnsAuditoria._formatar_evento(row)

        try:
            evento['parametros'] = json.loads(evento['parametros_json']) if evento['parametros_json'] else None
        except (ValueError, TypeError):
            evento['parametros'] = evento['parametros_json']
        evento.pop('parametros_json', None)

        alteracoes = db.session.execute(text(
            'SELECT * FROM ' + TB_ALTERACAO + ' WHERE ID_EVENTO = :id ORDER BY TABELA, nrOcorrencia, CAMPO'
        ), {'id': id_evento}).fetchall()
        evento['alteracoes'] = [AnsAuditoria._formatar_alteracao(r) for r in alteracoes]

        versoes = db.session.execute(text(
            'SELECT ID_VERSAO, TABELA, CHAVE_REGISTRO, NU_VERSAO, TIPO_VERSAO FROM ' + TB_VERSAO +
            ' WHERE ID_EVENTO = :id ORDER BY TABELA, CHAVE_REGISTRO, NU_VERSAO'
        ), {'id': id_evento}).fetchall()
        evento['versoes'] = [{
            'id_versao': v[0],
            'tabela_rotulo': NOMES_TABELAS.get(v[1], v[1]),
            'chave': v[2],
            'nu_versao': v[3],
            'tipo_rotulo': TIPOS_VERSAO.get(v[4], {}).get('rotulo', v[4]),
            'tipo_cor': TIPOS_VERSAO.get(v[4], {}).get('cor', 'bg-secondary'),
        } for v in versoes]
        return evento

    # ==================================================================
    # HISTÓRICO DE VERSÕES DE UMA OCORRÊNCIA
    # ==================================================================

    @staticmethod
    def historico_versoes(dt_apuracao, nr_ocorrencia):
        rows = db.session.execute(text("""
            SELECT ID_VERSAO, ID_EVENTO, DT_VERSAO, USUARIO_NOME, ACAO, TABELA, CHAVE_REGISTRO,
                   NU_VERSAO, TIPO_VERSAO, DADOS_JSON
            FROM """ + TB_VERSAO + """
            WHERE DT_APURACAO = :dt AND nrOcorrencia = :nr AND TABELA IN (:t45, :t48)
            ORDER BY TABELA, NU_VERSAO
        """), {'dt': dt_apuracao, 'nr': nr_ocorrencia, 't45': TB045, 't48': TB048}).fetchall()

        por_tabela = {}
        for r in rows:
            d = _dict(r)
            try:
                dados = json.loads(d['DADOS_JSON']) if d['DADOS_JSON'] else {}
            except (ValueError, TypeError):
                dados = {}
            por_tabela.setdefault(d['TABELA'], []).append({
                'id_versao': d['ID_VERSAO'],
                'id_evento': d['ID_EVENTO'],
                'dt_versao': _fmt_data_hora(d['DT_VERSAO']),
                'usuario_nome': d['USUARIO_NOME'] or 'Sistema',
                'acao_rotulo': ACOES.get(d['ACAO'], {}).get('rotulo', d['ACAO']),
                'nu_versao': d['NU_VERSAO'],
                'tipo': d['TIPO_VERSAO'],
                'tipo_rotulo': TIPOS_VERSAO.get(d['TIPO_VERSAO'], {}).get('rotulo', d['TIPO_VERSAO']),
                'tipo_cor': TIPOS_VERSAO.get(d['TIPO_VERSAO'], {}).get('cor', 'bg-secondary'),
                'dados_brutos': dados,
            })

        resultado = []
        for tabela, versoes in por_tabela.items():
            anterior = None
            colunas = []
            for v in versoes:
                for c in v['dados_brutos'].keys():
                    if c not in colunas:
                        colunas.append(c)
            for v in versoes:
                bruto = v.pop('dados_brutos')
                v['campos'] = []
                for c in colunas:
                    valor = bruto.get(c)
                    mudou = anterior is not None and anterior.get(c) != valor
                    v['campos'].append({'campo': c, 'valor': formatar_valor(c, valor), 'mudou': mudou})
                anterior = bruto
            resultado.append({
                'tabela': tabela,
                'tabela_rotulo': NOMES_TABELAS.get(tabela, tabela),
                'versoes': versoes,
            })
        return resultado

    # ==================================================================
    # EXPORTAÇÃO
    # ==================================================================

    @staticmethod
    def exportar(filtros):
        where_e, params_e = AnsAuditoria._where_eventos(filtros)
        p_e = dict(params_e)
        p_e['lim'] = LIMITE_EXPORTACAO
        eventos = db.session.execute(text(
            'SELECT TOP (:lim) E.* FROM ' + TB_EVENTO + ' E WHERE ' + where_e +
            ' ORDER BY E.DT_EVENTO DESC, E.ID_EVENTO DESC'
        ), p_e).fetchall()

        where_a, params_a = AnsAuditoria._where_alteracoes(filtros)
        p_a = dict(params_a)
        p_a['lim'] = LIMITE_EXPORTACAO
        alteracoes = db.session.execute(text(
            'SELECT TOP (:lim) A.* FROM ' + TB_ALTERACAO + ' A WHERE ' + where_a +
            ' ORDER BY A.DT_ALTERACAO DESC, A.ID_ALTERACAO DESC'
        ), p_a).fetchall()

        return ([AnsAuditoria._formatar_evento(r) for r in eventos],
                [AnsAuditoria._formatar_alteracao(r) for r in alteracoes])