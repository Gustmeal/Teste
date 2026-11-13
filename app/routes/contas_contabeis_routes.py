# app/routes/contas_contabeis_routes.py
"""
Rotas do módulo FCO (vinculação de contas e SISGEA x Balancete).
(Chave de permissão/blueprint mantidas como 'contas_contabeis'.)

1. Vinculação de contas  -> BDG.COR_TB031_FCO_CONTAS_CONTABEIS
   - Lista todas as contas (CO_CONTA, TIPO_PESSOA, ID_LINHA) com a descrição
     da linha vinda de BDG.COR_TB030_FCO_ITENS (LEFT JOIN).
   - Vincular: escolhe a conta (dropdown com pesquisa) e a linha
     (dropdown montado a partir da COR_TB030) e grava o ID_LINHA na COR_TB031.
   - Nova conta: INSERT de uma nova linha na COR_TB031.

2. SISGEA x Balancete
   - Prévia atual   -> BDG.COR_VW004_FCO_VALORES_MES (só traz o mês mais recente)
   - Histórico      -> BDG.COR_TB032_FCO_DIFERENCAS  (todos os meses já gravados)
   - "Retirar de prévia": só grava se TODAS as linhas da view tiverem
     ID_PREVIA = 0. Copia a view para a COR_TB032 (sem o ID_PREVIA),
     calculando VR_DIFERENCA = VR_BALANCETE - VR_SISGEA.
   - Tabela de análise (igual à aba "Análise-SISGEA X BALANCETE" da planilha),
     montada por DSC_LINHA, mostrando só o TOTAL (SISGEA / Balancete /
     Diferença). O filtro de tipo de pessoa aceita vários tipos ao mesmo
     tempo (caixas de seleção); o total soma apenas os tipos marcados.

Todas as consultas usam text() parametrizado.
Compatível com Python 3.9 e 3.12.
"""
from flask import Blueprint, render_template, request, jsonify, send_file
from flask_login import login_required
from app import db
from app.auth.decorators import sistema_requerido
from app.utils.audit import registrar_log
from datetime import datetime, date
from decimal import Decimal, ROUND_HALF_UP
from sqlalchemy import text
from io import BytesIO
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter


contas_contabeis_bp = Blueprint(
    'contas_contabeis',
    __name__,
    url_prefix='/contas-contabeis'
)

SISTEMA = 'contas_contabeis'

TB_ITENS = '[BDG].[COR_TB030_FCO_ITENS]'
TB_CONTAS = '[BDG].[COR_TB031_FCO_CONTAS_CONTABEIS]'
TB_DIFERENCAS = '[BDG].[COR_TB032_FCO_DIFERENCAS]'
VW_VALORES_MES = '[BDG].[COR_VW004_FCO_VALORES_MES]'

# Limites das colunas (para validar antes de gravar)
TAM_CO_CONTA = 15       # varchar(15)
TAM_TIPO_PESSOA = 2     # varchar(2)
INT_MIN, INT_MAX = -2147483648, 2147483647   # VR_DIFERENCA é int

FONTE_PREVIA = 'previa'
FONTE_HISTORICO = 'historico'


# =========================================================================
# AUXILIARES
# =========================================================================

def _para_date(valor):
    """Converte o que vier do banco (date/datetime/str) para date."""
    if valor is None:
        return None
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor
    texto = str(valor).strip()[:10]
    for formato in ('%Y-%m-%d', '%Y%m%d', '%d/%m/%Y'):
        try:
            return datetime.strptime(texto, formato).date()
        except ValueError:
            continue
    return None


def _dec(valor):
    """Converte para Decimal (None continua None)."""
    if valor is None:
        return None
    if isinstance(valor, Decimal):
        return valor
    try:
        return Decimal(str(valor))
    except Exception:
        return None


def _fmt_br(valor, casas=2):
    """Formata número no padrão brasileiro: 1.234.567,89"""
    if valor is None:
        return '-'
    try:
        numero = float(valor)
    except (TypeError, ValueError):
        return str(valor)
    texto = '{:,.{c}f}'.format(numero, c=casas)
    return texto.replace(',', 'X').replace('.', ',').replace('X', '.')


def _calcular_diferenca_int(vr_balancete, vr_sisgea):
    """
    VR_DIFERENCA (int) = VR_BALANCETE - VR_SISGEA, arredondado (ROUND_HALF_UP).
    Se passar do limite de um int do SQL Server, grava NULL.
    """
    bal = _dec(vr_balancete) or Decimal('0')
    sis = _dec(vr_sisgea) or Decimal('0')
    dif = (bal - sis).quantize(Decimal('1'), rounding=ROUND_HALF_UP)
    dif_int = int(dif)
    if dif_int < INT_MIN or dif_int > INT_MAX:
        return None
    return dif_int


def _json_erro(mensagem, status=400, **extra):
    corpo = {'sucesso': False, 'mensagem': mensagem}
    corpo.update(extra)
    return jsonify(corpo), status


# ---------------------------- Consultas: vinculação ----------------------

def _listar_itens():
    """Linhas do FCO (COR_TB030) para o dropdown."""
    sql = text(f"""
        SELECT [ID_LINHA], [DSC_LINHA]
        FROM {TB_ITENS}
        ORDER BY [ID_LINHA]
    """)
    return [
        {'id_linha': int(r['ID_LINHA']), 'dsc_linha': (r['DSC_LINHA'] or '').strip()}
        for r in db.session.execute(sql).mappings().all()
    ]


def _listar_contas():
    """Contas da COR_TB031 com a descrição da linha vinculada."""
    sql = text(f"""
        SELECT C.[CO_CONTA], C.[TIPO_PESSOA], C.[ID_LINHA], I.[DSC_LINHA]
        FROM {TB_CONTAS} C
        LEFT JOIN {TB_ITENS} I ON I.[ID_LINHA] = C.[ID_LINHA]
        ORDER BY C.[CO_CONTA]
    """)
    contas = []
    for r in db.session.execute(sql).mappings().all():
        contas.append({
            'co_conta': (r['CO_CONTA'] or '').strip(),
            'tipo_pessoa': (r['TIPO_PESSOA'] or '').strip(),
            'id_linha': int(r['ID_LINHA']) if r['ID_LINHA'] is not None else None,
            'dsc_linha': (r['DSC_LINHA'] or '').strip(),
        })
    return contas


def _listar_tipos_pessoa_existentes():
    """Tipos de pessoa já usados no banco (sugestões do campo da nova conta)."""
    sql = text(f"""
        SELECT DISTINCT LTRIM(RTRIM([TIPO_PESSOA])) AS TIPO_PESSOA
        FROM {TB_CONTAS}
        WHERE [TIPO_PESSOA] IS NOT NULL AND LTRIM(RTRIM([TIPO_PESSOA])) <> ''
        UNION
        SELECT DISTINCT LTRIM(RTRIM([TIPO_PESSOA]))
        FROM {VW_VALORES_MES}
        WHERE [TIPO_PESSOA] IS NOT NULL AND LTRIM(RTRIM([TIPO_PESSOA])) <> ''
    """)
    try:
        return sorted(set(r[0] for r in db.session.execute(sql).all() if r[0]))
    except Exception:
        db.session.rollback()
        return []


def _item_existe(id_linha):
    sql = text(f"SELECT COUNT(1) FROM {TB_ITENS} WHERE [ID_LINHA] = :id")
    return (db.session.execute(sql, {'id': id_linha}).scalar() or 0) > 0


def _buscar_conta(co_conta):
    sql = text(f"""
        SELECT [CO_CONTA], [TIPO_PESSOA], [ID_LINHA]
        FROM {TB_CONTAS}
        WHERE [CO_CONTA] = :c
    """)
    return db.session.execute(sql, {'c': co_conta}).mappings().first()


def _ler_id_linha(valor):
    """
    Lê o ID_LINHA vindo da tela.
    Retorna (ok, id_linha). Vazio = sem vínculo (None). tinyint: 0 a 255.
    """
    if valor is None or str(valor).strip() == '':
        return True, None
    try:
        id_linha = int(str(valor).strip())
    except ValueError:
        return False, None
    if id_linha < 0 or id_linha > 255:
        return False, None
    return True, id_linha


# ---------------------------- Consultas: balancete -----------------------

def _status_previa():
    """Situação da view: datas, total de linhas e quantas ainda estão em prévia."""
    sql = text(f"""
        SELECT
            COUNT(1) AS QTD_TOTAL,
            SUM(CASE WHEN [ID_PREVIA] = 0 THEN 0 ELSE 1 END) AS QTD_PREVIA,
            MIN([DT_REFERENCIA]) AS DT_MIN,
            MAX([DT_REFERENCIA]) AS DT_MAX,
            SUM(CASE WHEN [DT_REFERENCIA] IS NULL THEN 1 ELSE 0 END) AS QTD_SEM_DATA
        FROM {VW_VALORES_MES}
    """)
    r = db.session.execute(sql).mappings().first()
    qtd_total = int(r['QTD_TOTAL'] or 0) if r else 0
    qtd_previa = int(r['QTD_PREVIA'] or 0) if r else 0
    dt_min = _para_date(r['DT_MIN']) if r else None
    dt_max = _para_date(r['DT_MAX']) if r else None

    ja_gravado = False
    if dt_max:
        sql_existe = text(f"""
            SELECT COUNT(1) FROM {TB_DIFERENCAS}
            WHERE CONVERT(varchar(8), [DT_REFERENCIA], 112) = :dt
        """)
        ja_gravado = (db.session.execute(sql_existe, {'dt': dt_max.strftime('%Y%m%d')}).scalar() or 0) > 0

    return {
        'qtd_total': qtd_total,
        'qtd_previa': qtd_previa,
        'qtd_liberada': qtd_total - qtd_previa,
        'dt_min': dt_min,
        'dt_max': dt_max,
        'qtd_sem_data': int(r['QTD_SEM_DATA'] or 0) if r else 0,
        'liberado': qtd_total > 0 and qtd_previa == 0,
        'ja_gravado': ja_gravado,
    }


def _datas_historico():
    sql = text(f"""
        SELECT DISTINCT [DT_REFERENCIA]
        FROM {TB_DIFERENCAS}
        ORDER BY [DT_REFERENCIA] DESC
    """)
    datas = []
    for r in db.session.execute(sql).all():
        d = _para_date(r[0])
        if d:
            datas.append(d)
    return datas


def _buscar_linhas_balancete(fonte, dt_ref=None, id_linha=None, tipos=None):
    """
    Busca as linhas da view (prévia) ou da COR_TB032 (histórico), já com os
    filtros aplicados. Devolve lista de dicts com valores Decimal.
    """
    filtros = []
    params = {}
    if id_linha is not None:
        filtros.append('[ID_LINHA] = :id_linha')
        params['id_linha'] = id_linha
    if tipos:
        # Um parâmetro por tipo marcado: IN (:tipo0, :tipo1, ...)
        nomes = []
        for i, t in enumerate(tipos):
            nomes.append(':tipo{}'.format(i))
            params['tipo{}'.format(i)] = t
        filtros.append('LTRIM(RTRIM([TIPO_PESSOA])) IN ({})'.format(', '.join(nomes)))

    if fonte == FONTE_HISTORICO:
        if not dt_ref:
            return []
        filtros.append('CONVERT(varchar(8), [DT_REFERENCIA], 112) = :dt')
        params['dt'] = dt_ref.strftime('%Y%m%d')
        where = ('WHERE ' + ' AND '.join(filtros)) if filtros else ''
        sql = text(f"""
            SELECT [DT_REFERENCIA], [TIPO_PESSOA], [ID_LINHA], [DSC_LINHA],
                   [VR_SISGEA], [VR_BALANCETE], [VR_DIFERENCA]
            FROM {TB_DIFERENCAS}
            {where}
            ORDER BY [ID_LINHA], [TIPO_PESSOA]
        """)
    else:
        where = ('WHERE ' + ' AND '.join(filtros)) if filtros else ''
        sql = text(f"""
            SELECT [DT_REFERENCIA], [TIPO_PESSOA], [ID_LINHA], [DSC_LINHA],
                   [VR_SISGEA], [VR_BALANCETE], [ID_PREVIA]
            FROM {VW_VALORES_MES}
            {where}
            ORDER BY [ID_LINHA], [TIPO_PESSOA]
        """)

    linhas = []
    for r in db.session.execute(sql, params).mappings().all():
        sisgea = _dec(r['VR_SISGEA'])
        balancete = _dec(r['VR_BALANCETE'])
        if fonte == FONTE_HISTORICO and r['VR_DIFERENCA'] is not None:
            # Histórico: usa a diferença que foi gravada na tabela
            diferenca = Decimal(int(r['VR_DIFERENCA']))
        else:
            # Prévia: diferença calculada aqui (Balancete - SISGEA)
            diferenca = (balancete or Decimal('0')) - (sisgea or Decimal('0'))
        linhas.append({
            'dt_referencia': _para_date(r['DT_REFERENCIA']),
            'tipo_pessoa': (r['TIPO_PESSOA'] or '').strip(),
            'id_linha': int(r['ID_LINHA']),
            'dsc_linha': (r['DSC_LINHA'] or '').strip(),
            'vr_sisgea': sisgea,
            'vr_balancete': balancete,
            'vr_diferenca': diferenca,
        })
    return linhas


def _opcoes_filtro(fonte, dt_ref=None):
    """Linhas (ID/DSC) e tipos de pessoa existentes na fonte escolhida."""
    if fonte == FONTE_HISTORICO:
        if not dt_ref:
            return [], []
        origem = TB_DIFERENCAS
        where = 'WHERE CONVERT(varchar(8), [DT_REFERENCIA], 112) = :dt'
        params = {'dt': dt_ref.strftime('%Y%m%d')}
    else:
        origem = VW_VALORES_MES
        where = ''
        params = {}

    sql_linhas = text(f"""
        SELECT [ID_LINHA], MAX([DSC_LINHA]) AS DSC_LINHA
        FROM {origem} {where}
        GROUP BY [ID_LINHA]
        ORDER BY [ID_LINHA]
    """)
    sql_tipos = text(f"""
        SELECT DISTINCT LTRIM(RTRIM([TIPO_PESSOA])) AS TIPO_PESSOA
        FROM {origem} {where}
        ORDER BY 1
    """)
    linhas = [
        {'id_linha': int(r['ID_LINHA']), 'dsc_linha': (r['DSC_LINHA'] or '').strip()}
        for r in db.session.execute(sql_linhas, params).mappings().all()
    ]
    tipos = [r[0] for r in db.session.execute(sql_tipos, params).all() if r[0]]
    return linhas, tipos


def _montar_tabela(linhas_db):
    """
    Monta a tabela: uma linha por DSC_LINHA (ordem do ID_LINHA) com o TOTAL
    de SISGEA / Balancete / Diferença somando os tipos de pessoa que vieram
    do banco (já filtrados). Todos os rótulos vêm dos dados.
    """
    def _zerado():
        return {'sisgea': Decimal('0'), 'balancete': Decimal('0'), 'diferenca': Decimal('0')}

    por_linha = {}
    ordem = []
    totais = _zerado()
    for l in linhas_db:
        chave = l['id_linha']
        if chave not in por_linha:
            por_linha[chave] = {'id_linha': l['id_linha'], 'dsc_linha': l['dsc_linha'], 'total': _zerado()}
            ordem.append(chave)
        for destino in (por_linha[chave]['total'], totais):
            destino['sisgea'] += l['vr_sisgea'] or Decimal('0')
            destino['balancete'] += l['vr_balancete'] or Decimal('0')
            destino['diferenca'] += l['vr_diferenca'] or Decimal('0')

    return {
        'linhas': [por_linha[c] for c in ordem],
        'totais': totais,
        'tipos_na_tabela': sorted(set(l['tipo_pessoa'] for l in linhas_db)),
    }


def _ler_filtros_balancete():
    """Lê os filtros da querystring e resolve a data padrão do histórico."""
    fonte = request.args.get('fonte', FONTE_PREVIA)
    if fonte not in (FONTE_PREVIA, FONTE_HISTORICO):
        fonte = FONTE_PREVIA

    datas = _datas_historico()
    dt_ref = None
    if fonte == FONTE_HISTORICO:
        dt_ref = _para_date(request.args.get('dt')) if request.args.get('dt') else None
        if dt_ref not in datas:
            dt_ref = datas[0] if datas else None

    ok, id_linha = _ler_id_linha(request.args.get('linha'))
    if not ok:
        id_linha = None

    # Vários tipos de pessoa (caixas de seleção): ?tipo=PF&tipo=PJ
    tipos = []
    for t in request.args.getlist('tipo'):
        t = (t or '').strip()[:TAM_TIPO_PESSOA]
        if t and t not in tipos:
            tipos.append(t)
    return fonte, datas, dt_ref, id_linha, tipos


# =========================================================================
# PÁGINA INICIAL
# =========================================================================

@contas_contabeis_bp.route('/')
@login_required
@sistema_requerido(SISTEMA)
def index():
    """Tela inicial com os atalhos para Vinculação e Balancete."""
    db.session.expire_all()
    resumo = {'qtd_contas': 0, 'qtd_sem_vinculo': 0, 'status': None}
    try:
        sql = text(f"""
            SELECT COUNT(1) AS QTD,
                   SUM(CASE WHEN [ID_LINHA] IS NULL THEN 1 ELSE 0 END) AS SEM_VINCULO
            FROM {TB_CONTAS}
        """)
        r = db.session.execute(sql).mappings().first()
        resumo['qtd_contas'] = int(r['QTD'] or 0) if r else 0
        resumo['qtd_sem_vinculo'] = int(r['SEM_VINCULO'] or 0) if r else 0
        resumo['status'] = _status_previa()
    except Exception as e:
        db.session.rollback()
        print(f"[contas_contabeis] Erro ao montar resumo: {e}")

    return render_template('contas_contabeis/index.html', resumo=resumo)


# =========================================================================
# VINCULAÇÃO (COR_TB031 x COR_TB030)
# =========================================================================

@contas_contabeis_bp.route('/vinculacao')
@login_required
@sistema_requerido(SISTEMA)
def vinculacao():
    db.session.expire_all()
    contas = _listar_contas()
    itens = _listar_itens()
    qtd_sem_vinculo = sum(1 for c in contas if c['id_linha'] is None)
    return render_template(
        'contas_contabeis/vinculacao.html',
        contas=contas,
        itens=itens,
        tipos_pessoa=_listar_tipos_pessoa_existentes(),
        qtd_contas=len(contas),
        qtd_sem_vinculo=qtd_sem_vinculo,
        qtd_vinculadas=len(contas) - qtd_sem_vinculo,
        tam_co_conta=TAM_CO_CONTA,
        tam_tipo_pessoa=TAM_TIPO_PESSOA,
    )


@contas_contabeis_bp.route('/vinculacao/salvar', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def vinculacao_salvar():
    """Grava o ID_LINHA (COR_TB030) na conta escolhida da COR_TB031."""
    dados = request.get_json(silent=True) or {}
    co_conta = str(dados.get('co_conta') or '').strip()
    ok, id_linha = _ler_id_linha(dados.get('id_linha'))

    if not co_conta:
        return _json_erro('Selecione a conta contábil.')
    if not ok:
        return _json_erro('Linha do FCO inválida.')

    try:
        conta = _buscar_conta(co_conta)
        if not conta:
            return _json_erro('Conta {} não encontrada na tabela de contas.'.format(co_conta), 404)
        if id_linha is not None and not _item_existe(id_linha):
            return _json_erro('A linha {} não existe na tabela de itens do FCO.'.format(id_linha), 404)

        id_anterior = int(conta['ID_LINHA']) if conta['ID_LINHA'] is not None else None
        if id_anterior == id_linha:
            return jsonify({'sucesso': True, 'mensagem': 'A conta já estava vinculada a essa linha.'})

        db.session.execute(text(f"""
            UPDATE {TB_CONTAS}
            SET [ID_LINHA] = :id_linha
            WHERE [CO_CONTA] = :c
        """), {'id_linha': id_linha, 'c': co_conta})
        db.session.commit()

        registrar_log(
            acao='editar',
            entidade='contas_contabeis_vinculacao',
            entidade_id=co_conta,
            descricao='Vinculação da conta {} alterada de {} para {}'.format(
                co_conta,
                id_anterior if id_anterior is not None else 'sem vínculo',
                id_linha if id_linha is not None else 'sem vínculo'),
            dados_antigos={'CO_CONTA': co_conta, 'ID_LINHA': id_anterior},
            dados_novos={'CO_CONTA': co_conta, 'ID_LINHA': id_linha},
        )
        return jsonify({'sucesso': True, 'mensagem': 'Vinculação da conta {} salva com sucesso.'.format(co_conta)})
    except Exception as e:
        db.session.rollback()
        return _json_erro('Erro ao salvar a vinculação: {}'.format(e), 500)


@contas_contabeis_bp.route('/vinculacao/nova', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def vinculacao_nova():
    """Cria uma nova linha na COR_TB031 (CO_CONTA, TIPO_PESSOA, ID_LINHA)."""
    dados = request.get_json(silent=True) or {}
    co_conta = str(dados.get('co_conta') or '').strip()
    tipo_pessoa = str(dados.get('tipo_pessoa') or '').strip().upper()
    ok, id_linha = _ler_id_linha(dados.get('id_linha'))

    if not co_conta:
        return _json_erro('Informe o código da conta contábil.')
    if len(co_conta) > TAM_CO_CONTA:
        return _json_erro('A conta contábil aceita no máximo {} caracteres.'.format(TAM_CO_CONTA))
    if len(tipo_pessoa) > TAM_TIPO_PESSOA:
        return _json_erro('O tipo de pessoa aceita no máximo {} caracteres.'.format(TAM_TIPO_PESSOA))
    if not ok:
        return _json_erro('Linha do FCO inválida.')

    try:
        if _buscar_conta(co_conta):
            return _json_erro('A conta {} já existe. Use a vinculação para alterar a linha.'.format(co_conta), 409)
        if id_linha is not None and not _item_existe(id_linha):
            return _json_erro('A linha {} não existe na tabela de itens do FCO.'.format(id_linha), 404)

        db.session.execute(text(f"""
            INSERT INTO {TB_CONTAS} ([CO_CONTA], [TIPO_PESSOA], [ID_LINHA])
            VALUES (:c, :t, :id_linha)
        """), {'c': co_conta, 't': tipo_pessoa or None, 'id_linha': id_linha})
        db.session.commit()

        registrar_log(
            acao='criar',
            entidade='contas_contabeis_vinculacao',
            entidade_id=co_conta,
            descricao='Nova conta contábil {} cadastrada'.format(co_conta),
            dados_novos={'CO_CONTA': co_conta, 'TIPO_PESSOA': tipo_pessoa or None, 'ID_LINHA': id_linha},
        )
        return jsonify({'sucesso': True, 'mensagem': 'Conta {} cadastrada com sucesso.'.format(co_conta)})
    except Exception as e:
        db.session.rollback()
        return _json_erro('Erro ao cadastrar a conta: {}'.format(e), 500)


# =========================================================================
# BALANCETE (COR_VW004 / COR_TB032)
# =========================================================================

@contas_contabeis_bp.route('/balancete')
@login_required
@sistema_requerido(SISTEMA)
def balancete():
    db.session.expire_all()
    fonte, datas, dt_ref, id_linha, tipos = _ler_filtros_balancete()

    status = _status_previa()
    opcoes_linhas, opcoes_tipos = _opcoes_filtro(fonte, dt_ref)
    # Ignora tipos que não existem na origem escolhida (ex.: trocou a data)
    tipos = [t for t in tipos if t in opcoes_tipos]
    linhas_db = _buscar_linhas_balancete(fonte, dt_ref, id_linha, tipos)
    tabela = _montar_tabela(linhas_db)

    # Data exibida no título da tabela
    if fonte == FONTE_HISTORICO:
        dt_tabela = dt_ref
    else:
        datas_view = sorted(set(l['dt_referencia'] for l in linhas_db if l['dt_referencia']))
        dt_tabela = datas_view[-1] if datas_view else status['dt_max']

    linha_escolhida = next((o for o in opcoes_linhas if o['id_linha'] == id_linha), None)

    return render_template(
        'contas_contabeis/balancete.html',
        status=status,
        fonte=fonte,
        datas_historico=datas,
        dt_ref=dt_ref,
        dt_tabela=dt_tabela,
        id_linha=id_linha,
        tipos=tipos,
        linha_escolhida=linha_escolhida,
        opcoes_linhas=opcoes_linhas,
        opcoes_tipos=opcoes_tipos,
        tabela=tabela,
        fmt=_fmt_br,
        FONTE_PREVIA=FONTE_PREVIA,
        FONTE_HISTORICO=FONTE_HISTORICO,
    )


@contas_contabeis_bp.route('/balancete/retirar-previa', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def balancete_retirar_previa():
    """
    Copia a COR_VW004 para a COR_TB032 somente se TODAS as linhas estiverem
    com ID_PREVIA = 0. Se a data já existir na COR_TB032, pede confirmação
    para substituir.
    """
    dados = request.get_json(silent=True) or {}
    substituir = bool(dados.get('substituir'))

    try:
        sql = text(f"""
            SELECT [DT_REFERENCIA], [TIPO_PESSOA], [ID_LINHA], [DSC_LINHA],
                   [VR_SISGEA], [VR_BALANCETE], [ID_PREVIA]
            FROM {VW_VALORES_MES}
        """)
        registros = db.session.execute(sql).mappings().all()

        if not registros:
            return _json_erro('A view do balancete não retornou nenhuma linha.')

        # 1) Todas as linhas precisam estar com ID_PREVIA = 0
        em_previa = [r for r in registros if r['ID_PREVIA'] is None or int(r['ID_PREVIA']) != 0]
        if em_previa:
            return _json_erro(
                'O balancete ainda está em prévia: {} de {} linha(s) com ID_PREVIA diferente de 0. '
                'Nada foi gravado.'.format(len(em_previa), len(registros)),
                422
            )

        # 2) Validações das chaves da COR_TB032 (DT_REFERENCIA, TIPO_PESSOA, ID_LINHA)
        sem_data = [r for r in registros if _para_date(r['DT_REFERENCIA']) is None]
        if sem_data:
            return _json_erro('Existem {} linha(s) sem DT_REFERENCIA na view. Nada foi gravado.'.format(len(sem_data)), 422)

        chaves = set()
        for r in registros:
            chave = (_para_date(r['DT_REFERENCIA']), (r['TIPO_PESSOA'] or '').strip(), int(r['ID_LINHA']))
            if chave in chaves:
                return _json_erro(
                    'A view tem linha repetida para {} / {} / linha {}. Nada foi gravado.'.format(
                        chave[0].strftime('%d/%m/%Y'), chave[1], chave[2]), 422)
            chaves.add(chave)

        datas = sorted(set(c[0] for c in chaves))
        datas_txt = [d.strftime('%d/%m/%Y') for d in datas]

        # 3) Data já gravada? Pede confirmação para substituir
        sql_existe = text(f"""
            SELECT COUNT(1) FROM {TB_DIFERENCAS}
            WHERE CONVERT(varchar(8), [DT_REFERENCIA], 112) = :dt
        """)
        datas_existentes = [d for d in datas
                            if (db.session.execute(sql_existe, {'dt': d.strftime('%Y%m%d')}).scalar() or 0) > 0]
        if datas_existentes and not substituir:
            return jsonify({
                'sucesso': False,
                'precisa_confirmar': True,
                'mensagem': 'A data de referência {} já foi retirada de prévia antes. '
                            'Deseja substituir os valores gravados pelos valores atuais da view?'.format(
                                ', '.join(d.strftime('%d/%m/%Y') for d in datas_existentes)),
            }), 409

        # 4) Grava tudo numa única transação
        qtd_apagada = 0
        for d in datas_existentes:
            res = db.session.execute(text(f"""
                DELETE FROM {TB_DIFERENCAS}
                WHERE CONVERT(varchar(8), [DT_REFERENCIA], 112) = :dt
            """), {'dt': d.strftime('%Y%m%d')})
            qtd_apagada += res.rowcount or 0

        sql_insert = text(f"""
            INSERT INTO {TB_DIFERENCAS}
                ([DT_REFERENCIA], [TIPO_PESSOA], [ID_LINHA], [DSC_LINHA],
                 [VR_SISGEA], [VR_BALANCETE], [VR_DIFERENCA])
            VALUES (:dt, :tipo, :id_linha, :dsc, :sisgea, :balancete, :diferenca)
        """)
        for r in registros:
            db.session.execute(sql_insert, {
                'dt': _para_date(r['DT_REFERENCIA']),
                'tipo': (r['TIPO_PESSOA'] or '').strip(),
                'id_linha': int(r['ID_LINHA']),
                'dsc': (r['DSC_LINHA'] or '').strip(),
                'sisgea': _dec(r['VR_SISGEA']),
                'balancete': _dec(r['VR_BALANCETE']) if r['VR_BALANCETE'] is not None else Decimal('0'),
                'diferenca': _calcular_diferenca_int(r['VR_BALANCETE'], r['VR_SISGEA']),
            })
        db.session.commit()

        registrar_log(
            acao='criar',
            entidade='contas_contabeis_balancete',
            entidade_id=','.join(d.strftime('%Y-%m-%d') for d in datas),
            descricao='Balancete retirado de prévia ({}): {} linha(s) gravada(s){}'.format(
                ', '.join(datas_txt), len(registros),
                ' substituindo {} linha(s)'.format(qtd_apagada) if qtd_apagada else ''),
            dados_novos={'datas': datas_txt, 'qtd_linhas': len(registros), 'substituidas': qtd_apagada},
        )
        return jsonify({
            'sucesso': True,
            'mensagem': 'Balancete de {} retirado de prévia: {} linha(s) gravada(s).'.format(
                ', '.join(datas_txt), len(registros)),
            'dt_referencia': datas[-1].isoformat(),
        })
    except Exception as e:
        db.session.rollback()
        return _json_erro('Erro ao retirar o balancete de prévia: {}'.format(e), 500)


@contas_contabeis_bp.route('/balancete/exportar')
@login_required
@sistema_requerido(SISTEMA)
def balancete_exportar():
    """Exporta para Excel a mesma tabela que está na tela (com os filtros)."""
    fonte, _datas, dt_ref, id_linha, tipos = _ler_filtros_balancete()
    linhas_db = _buscar_linhas_balancete(fonte, dt_ref, id_linha, tipos)
    tabela = _montar_tabela(linhas_db)

    if fonte == FONTE_HISTORICO:
        dt_tabela = dt_ref
        titulo_fonte = 'Histórico'
    else:
        datas_view = sorted(set(l['dt_referencia'] for l in linhas_db if l['dt_referencia']))
        dt_tabela = datas_view[-1] if datas_view else None
        titulo_fonte = 'Prévia atual'

    wb = Workbook()
    ws = wb.active
    ws.title = 'SISGEA x Balancete'

    azul = PatternFill('solid', fgColor='224ABE')
    cinza = PatternFill('solid', fgColor='F2F2F2')
    branco_negrito = Font(bold=True, color='FFFFFF')
    negrito = Font(bold=True)
    fina = Side(style='thin', color='D9D9D9')
    borda = Border(left=fina, right=fina, top=fina, bottom=fina)
    centro = Alignment(horizontal='center', vertical='center', wrap_text=True)
    formato = '#,##0.00;[Red]-#,##0.00'

    titulo = 'Análise SISGEA x Balancete - {}{}'.format(
        titulo_fonte, ' - ' + dt_tabela.strftime('%d/%m/%Y') if dt_tabela else '')
    ws.cell(row=1, column=1, value=titulo).font = Font(bold=True, size=13)

    filtro_tipos = ', '.join(tipos) if tipos else 'Todos os tipos de pessoa'
    ws.cell(row=2, column=1, value='Tipos de pessoa: {}'.format(filtro_tipos)).font = Font(italic=True, color='666666')

    cabecalho = ['Linha', 'Descrição', 'SISGEA', 'Balancete', 'Diferença']
    for c, nome in enumerate(cabecalho, start=1):
        cel = ws.cell(row=4, column=c, value=nome)
        cel.fill = azul
        cel.font = branco_negrito
        cel.alignment = centro
        cel.border = borda
    ultima_col = len(cabecalho)

    linha_excel = 5
    for item in tabela['linhas']:
        v = item['total']
        valores = [item['id_linha'], item['dsc_linha'],
                   float(v['sisgea']), float(v['balancete']), float(v['diferenca'])]
        for c, valor in enumerate(valores, start=1):
            cel = ws.cell(row=linha_excel, column=c, value=valor)
            cel.border = borda
            if c >= 3:
                cel.number_format = formato
        linha_excel += 1

    ws.cell(row=linha_excel, column=1, value='TOTAL')
    ws.merge_cells(start_row=linha_excel, start_column=1, end_row=linha_excel, end_column=2)
    t = tabela['totais']
    for c, valor in enumerate([float(t['sisgea']), float(t['balancete']), float(t['diferenca'])], start=3):
        ws.cell(row=linha_excel, column=c, value=valor)
    for c in range(1, ultima_col + 1):
        cel = ws.cell(row=linha_excel, column=c)
        cel.fill = cinza
        cel.font = negrito
        cel.border = borda
        if c >= 3:
            cel.number_format = formato

    ws.column_dimensions['A'].width = 8
    ws.column_dimensions['B'].width = 48
    for c in range(3, ultima_col + 1):
        ws.column_dimensions[get_column_letter(c)].width = 19
    ws.freeze_panes = 'A5'

    saida = BytesIO()
    wb.save(saida)
    saida.seek(0)

    nome = 'Analise_SISGEA_x_Balancete_{}_{}.xlsx'.format(
        titulo_fonte.replace(' ', '_').replace('é', 'e').replace('ó', 'o'),
        dt_tabela.strftime('%Y%m%d') if dt_tabela else 'sem_data')
    registrar_log(
        acao='exportar',
        entidade='contas_contabeis_balancete',
        entidade_id=dt_tabela.isoformat() if dt_tabela else '',
        descricao='Exportação da análise SISGEA x Balancete ({}; tipos: {})'.format(titulo_fonte, filtro_tipos),
    )
    return send_file(
        saida,
        as_attachment=True,
        download_name=nome,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )
