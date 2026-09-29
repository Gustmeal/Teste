"""
Model: app/models/ans_previas.py
Prévias ANS Glosas — somente leitura da tabela
BDDASHBOARDBI.BDG.MOV_TB059_PREVIAS_ANS_GLOSA_FATURAMENTO
Compatível com Python 3.9 e 3.12.
"""

from datetime import datetime, date
from decimal import Decimal

from sqlalchemy import text

from app import db

TB_PREVIAS = 'BDDASHBOARDBI.BDG.MOV_TB059_PREVIAS_ANS_GLOSA_FATURAMENTO'

# Ordem das colunas exatamente como na tabela
COLUNAS = (
    'DT_APURACAO', 'nrOcorrencia', 'NR_CONTRATO', 'itemServico', 'GRUPO', 'NO_GRUPO',
    'PRAZO_GRUPO', 'DT_ABERTURA', 'QTDE_DIAS', 'DT_EFETIVACAO', 'DT_ANDAMENTO',
    'DT_JUSTIFICATIVA', 'DT_DEFERIDO', 'NO_PRAZO', 'ADVERTENCIA', 'DT_ADVERTENCIA',
    'REINCIDENCIA', 'DT_REINCIDENCIA', 'REITERACAO', 'DT_REITERACAO',
    'DSC_JUSTIFICATIVA', 'PREVIA', 'DT_JUSTIFICATIVA_ULT',
)

# Cabeçalhos exibidos na tela / Excel (rótulo, coluna)
CABECALHOS = (
    ('Apuração', 'DT_APURACAO'), ('Ocorrência', 'nrOcorrencia'), ('Contrato', 'NR_CONTRATO'),
    ('Item de serviço', 'itemServico'), ('Grupo', 'GRUPO'), ('Nome do grupo', 'NO_GRUPO'),
    ('Prazo do grupo', 'PRAZO_GRUPO'), ('Abertura', 'DT_ABERTURA'), ('Qtde dias', 'QTDE_DIAS'),
    ('Efetivação', 'DT_EFETIVACAO'), ('Andamento', 'DT_ANDAMENTO'),
    ('Justificativa', 'DT_JUSTIFICATIVA'), ('Deferido', 'DT_DEFERIDO'), ('No prazo', 'NO_PRAZO'),
    ('Advertência', 'ADVERTENCIA'), ('Dt advertência', 'DT_ADVERTENCIA'),
    ('Reincidência', 'REINCIDENCIA'), ('Dt reincidência', 'DT_REINCIDENCIA'),
    ('Reiteração', 'REITERACAO'), ('Dt reiteração', 'DT_REITERACAO'),
    ('Descrição da justificativa', 'DSC_JUSTIFICATIVA'), ('Prévia', 'PREVIA'),
    ('Última justificativa', 'DT_JUSTIFICATIVA_ULT'),
)

CAMPOS_SIM_NAO = ('NO_PRAZO', 'ADVERTENCIA', 'REINCIDENCIA', 'REITERACAO')

SELECT_COLUNAS = ', '.join('[{}]'.format(c) for c in COLUNAS)


def _mapa(row):
    return row._mapping if hasattr(row, '_mapping') else row


def _formatar(coluna, valor):
    """Valor do banco -> texto para exibição."""
    if valor is None:
        return ''
    if coluna in CAMPOS_SIM_NAO:
        if str(valor) == '1':
            return 'Sim'
        if str(valor) == '0':
            return 'Não'
    if isinstance(valor, datetime):
        return valor.strftime('%d/%m/%Y')
    if isinstance(valor, date):
        return valor.strftime('%d/%m/%Y')
    if isinstance(valor, Decimal):
        return str(int(valor)) if valor == valor.to_integral_value() else format(valor, 'f')
    return str(valor).strip()


def _int(valor):
    try:
        if valor is None or str(valor).strip() == '':
            return None
        return int(valor)
    except (TypeError, ValueError):
        return None


class AnsPrevias(object):

    # ==================================================================
    # DATAS / OPÇÕES DE FILTRO
    # ==================================================================

    @staticmethod
    def obter_datas():
        rows = db.session.execute(text(
            'SELECT DISTINCT DT_APURACAO FROM ' + TB_PREVIAS +
            ' WHERE DT_APURACAO IS NOT NULL ORDER BY DT_APURACAO DESC'
        )).fetchall()
        return [r[0] for r in rows if r[0]]

    @staticmethod
    def obter_opcoes(dt_apuracao):
        grupos = db.session.execute(text(
            'SELECT GRUPO, MAX(NO_GRUPO) AS NO_GRUPO, COUNT(*) AS QTDE FROM ' + TB_PREVIAS +
            ' WHERE DT_APURACAO = :dt AND GRUPO IS NOT NULL GROUP BY GRUPO ORDER BY GRUPO'
        ), {'dt': dt_apuracao}).fetchall()

        previas = db.session.execute(text(
            'SELECT UPPER(LTRIM(RTRIM(CAST(PREVIA AS VARCHAR(50))))) AS PREVIA, COUNT(*) AS QTDE FROM ' + TB_PREVIAS +
            ' WHERE DT_APURACAO = :dt GROUP BY UPPER(LTRIM(RTRIM(CAST(PREVIA AS VARCHAR(50))))) ORDER BY 1'
        ), {'dt': dt_apuracao}).fetchall()

        return {
            'grupos': [{'valor': g[0], 'rotulo': 'Grupo {} - {}'.format(g[0], g[1]) if g[1] else 'Grupo {}'.format(g[0]),
                        'qtde': g[2]} for g in grupos],
            'previas': [{'valor': '' if p[0] is None else str(p[0]),
                         'rotulo': '(vazio)' if p[0] is None else _formatar('PREVIA', p[0]),
                         'qtde': p[1]} for p in previas],
        }

    # ==================================================================
    # FILTROS
    # ==================================================================

    @staticmethod
    def ler_filtros(args):
        return {
            'grupo': _int(args.get('grupo')),
            'previa': (args.get('previa') or '').strip()[:50] or None,
            'nr_ocorrencia': _int(args.get('nr_ocorrencia')),
            'nr_contrato': (args.get('nr_contrato') or '').strip()[:50] or None,
            'no_prazo': (args.get('no_prazo') or '').strip() if (args.get('no_prazo') or '').strip() in ('0', '1') else None,
        }

    @staticmethod
    def _where(dt_apuracao, f):
        condicoes = ['DT_APURACAO = :dt']
        params = {'dt': dt_apuracao}
        if f.get('grupo') is not None:
            condicoes.append('GRUPO = :grupo')
            params['grupo'] = f['grupo']
        if f.get('previa'):
            condicoes.append('UPPER(LTRIM(RTRIM(CAST(PREVIA AS VARCHAR(50))))) = :previa')
            params['previa'] = f['previa'].strip().upper()
        if f.get('nr_ocorrencia') is not None:
            condicoes.append('nrOcorrencia = :nr')
            params['nr'] = f['nr_ocorrencia']
        if f.get('nr_contrato'):
            condicoes.append('CAST(NR_CONTRATO AS VARCHAR(50)) LIKE :contrato')
            params['contrato'] = '%' + f['nr_contrato'] + '%'
        if f.get('no_prazo') is not None:
            condicoes.append('NO_PRAZO = :no_prazo')
            params['no_prazo'] = int(f['no_prazo'])
        return ' AND '.join(condicoes), params

    # ==================================================================
    # LISTAGEM (todas as colunas da TB059)
    # ==================================================================

    @staticmethod
    def listar(dt_apuracao, filtros):
        where, params = AnsPrevias._where(dt_apuracao, filtros)
        rows = db.session.execute(text(
            'SELECT ' + SELECT_COLUNAS + ' FROM ' + TB_PREVIAS + ' WHERE ' + where +
            ' ORDER BY GRUPO, nrOcorrencia'
        ), params).fetchall()

        registros = []
        for r in rows:
            m = _mapa(r)
            registros.append({c: _formatar(c, m[c]) for c in COLUNAS})
        return registros

    # ==================================================================
    # RESUMO (cards e quadro por grupo) — contado pela coluna PREVIA
    # Os valores (ADVERTIR, REINCIDIR, REITERAR...) vêm do banco:
    # cada valor distinto vira um card e uma coluna do quadro por grupo.
    # ==================================================================

    CORES_PREVIA = ('#e74a3b', '#f6c23e', '#5a5c69', '#36b9cc', '#4e73df', '#1cc88a')
    CHAVE_SEM_PREVIA = '__SEM_PREVIA__'

    @staticmethod
    def resumo(dt_apuracao, filtros):
        where, params = AnsPrevias._where(dt_apuracao, filtros)

        geral = db.session.execute(text(
            'SELECT COUNT(*) AS TOTAL, COUNT(DISTINCT GRUPO) AS GRUPOS FROM ' + TB_PREVIAS + ' WHERE ' + where
        ), params).fetchone()

        # Contagem por GRUPO x PREVIA (PREVIA normalizada: sem espaços e em maiúsculas)
        linhas = db.session.execute(text("""
            SELECT GRUPO,
                   MAX(NO_GRUPO) AS NO_GRUPO,
                   MAX(PRAZO_GRUPO) AS PRAZO,
                   UPPER(LTRIM(RTRIM(CAST(PREVIA AS VARCHAR(50))))) AS PREVIA,
                   COUNT(*) AS QTDE
            FROM """ + TB_PREVIAS + " WHERE " + where + """
            GROUP BY GRUPO, UPPER(LTRIM(RTRIM(CAST(PREVIA AS VARCHAR(50)))))
            ORDER BY GRUPO
        """), params).fetchall()

        totais_previa = {}
        grupos = {}
        for r in linhas:
            m = _mapa(r)
            chave = m['PREVIA'] or AnsPrevias.CHAVE_SEM_PREVIA
            qtde = m['QTDE'] or 0
            totais_previa[chave] = totais_previa.get(chave, 0) + qtde

            g = grupos.setdefault(m['GRUPO'], {
                'grupo': m['GRUPO'],
                'no_grupo': m['NO_GRUPO'] or '',
                'prazo': _formatar('PRAZO_GRUPO', m['PRAZO']),
                'total': 0,
                'previas': {},
            })
            g['total'] += qtde
            g['previas'][chave] = g['previas'].get(chave, 0) + qtde
            if not g['no_grupo'] and m['NO_GRUPO']:
                g['no_grupo'] = m['NO_GRUPO']

        # Colunas/cards: valores reais da PREVIA em ordem alfabética; "Sem prévia" por último
        chaves = sorted(k for k in totais_previa if k != AnsPrevias.CHAVE_SEM_PREVIA)
        if AnsPrevias.CHAVE_SEM_PREVIA in totais_previa:
            chaves.append(AnsPrevias.CHAVE_SEM_PREVIA)

        colunas_previa = []
        for i, chave in enumerate(chaves):
            sem = chave == AnsPrevias.CHAVE_SEM_PREVIA
            colunas_previa.append({
                'chave': chave,
                'rotulo': 'Sem prévia' if sem else chave,
                'qtde': totais_previa[chave],
                'cor': '#858796' if sem else AnsPrevias.CORES_PREVIA[i % len(AnsPrevias.CORES_PREVIA)],
            })

        m_geral = _mapa(geral) if geral else None
        return {
            'total': (m_geral['TOTAL'] if m_geral else 0) or 0,
            'grupos': (m_geral['GRUPOS'] if m_geral else 0) or 0,
            'colunas_previa': colunas_previa,
            'por_grupo': [grupos[k] for k in sorted(grupos, key=lambda x: (x is None, x))],
        }