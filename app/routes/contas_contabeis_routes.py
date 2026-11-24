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

1b. Vinculação SISGEA X FCO -> BDG.COR_TB037_FCO_LINHAS_SISGEA
   - Mesmo funcionamento da vinculação das contas: escolhe a linha do SISGEA
     (ID_FCO_LINHA / DSC_FCO_LINHA) e grava o ID_LINHA da COR_TB030.
   - pkLinhaContabil não aparece na tela; é usado só como chave do UPDATE.
   - Nova linha: INSERT na COR_TB037 (pk por IDENTITY ou MAX + 1).

1c. Vinculação FCO X BALANCETE -> BDG.COR_TB041_FCO_VINC_ITEM_LINHA_CONTA
   - Cada vínculo = agrupamento (COR_TB034) + item (COR_TB036) + linha
     (COR_TB037) + conta contábil. Os nomes vêm das tabelas de parâmetro.
   - A tabela não tem chave: a edição localiza o registro pelos 4 valores
     originais.

Regra para contas novas (SISGEA X BALANCETE e FCO X BALANCETE): a conta só
pode ser incluída se existir no CO_CONTA da BDG.COR_TB012_BALANCETE.

2. SISGEA x Balancete
   - Prévia atual   -> BDG.COR_VW004_FCO_VALORES_MES (só traz o mês mais recente)
   - Histórico      -> BDG.COR_TB032_FCO_DIFERENCAS  (todos os meses já gravados)
   - "Guardar FCO": só grava se TODAS as linhas da view tiverem
     ID_PREVIA = 0. Copia a view para a COR_TB032 (sem o ID_PREVIA),
     calculando VR_DIFERENCA = VR_BALANCETE - VR_SISGEA.
   - Tabela de análise (igual à aba "Análise-SISGEA X BALANCETE" da planilha),
     montada por DSC_LINHA, mostrando só o TOTAL (SISGEA / Balancete /
     Diferença). O filtro de tipo de pessoa aceita vários tipos ao mesmo
     tempo (caixas de seleção); o total soma apenas os tipos marcados.

3. Relatórios SISGEA (montagem do FCO, como a aba "CONCILIAÇÃO FCO X SISGEA")
   - FCO Prévia       -> BDG.COR_VW007_FCO_MONTAGEM_SISGEA_PREVIA
   - FCO Histórico     -> BDG.COR_VW008_FCO_MONTAGEM_SISGEA_HISTORICO
     (escolha da data de referência; a consulta já filtra pela data)
   - Tela interativa: agrupamento > item contábil > linhas, filtros de
     agrupamento e item (vários ao mesmo tempo), busca, ocultar zerados,
     visão agrupada ou tabela única com ordenação; exportação para Excel
     com os mesmos filtros.

4. Auditoria
   - SD OPER ZERO                   -> BDG.COR_VW009_FCO_AUDITORIA_SD_OPER_ZERO
   - Baixa Não Marcada (Evolução)   -> BDG.COR_VW010_FCO_AUDITORIA_BAIXA_NAO_MARCADA_EVOLUCAO
   - Filtros no back end (WHERE parametrizado), paginação no SQL Server
     (OFFSET/FETCH), ordenação por coluna, resumo por agrupamento e
     exportação para Excel com os mesmos filtros. NO_AGRUPAMENTO é filtro
     nos dois relatórios.

Todas as consultas usam text() parametrizado.
Compatível com Python 3.9 e 3.12.
"""
from flask import Blueprint, render_template, request, jsonify, send_file, g, abort
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
TB_LINHAS_SISGEA = '[BDG].[COR_TB037_FCO_LINHAS_SISGEA]'
TB_LINHAS_SISGEA_OBJ = 'BDG.COR_TB037_FCO_LINHAS_SISGEA'
TB_BALANCETE = '[BDG].[COR_TB012_BALANCETE]'
TB_AGRUP_CONTABIL = '[BDG].[COR_TB034_FCO_AGRUPAMENTO_CONTABIL]'
TB_ITENS_SISGEA = '[BDG].[COR_TB036_FCO_ITENS_SISGEA]'
TB_VINC_ITEM_LINHA_CONTA = '[BDG].[COR_TB041_FCO_VINC_ITEM_LINHA_CONTA]'
VW_VALORES_MES = '[BDG].[COR_VW004_FCO_VALORES_MES]'
VW_SISGEA_PREVIA = '[BDG].[COR_VW007_FCO_MONTAGEM_SISGEA_PREVIA]'
VW_SISGEA_HISTORICO = '[BDG].[COR_VW008_FCO_MONTAGEM_SISGEA_HISTORICO]'

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
    """
    Tipos de pessoa já usados (sugestões do campo da nova conta).
    Lê só tabelas (COR_TB031 + COR_TB032): a view é pesada e não precisa
    ser executada para montar sugestões.
    """
    sql = text(f"""
        SELECT DISTINCT LTRIM(RTRIM([TIPO_PESSOA])) AS TIPO_PESSOA
        FROM {TB_CONTAS}
        WHERE [TIPO_PESSOA] IS NOT NULL AND LTRIM(RTRIM([TIPO_PESSOA])) <> ''
        UNION
        SELECT DISTINCT LTRIM(RTRIM([TIPO_PESSOA]))
        FROM {TB_DIFERENCAS}
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


def _conta_existe_no_balancete(co_conta):
    """A conta precisa existir no CO_CONTA da COR_TB012_BALANCETE (registros não excluídos)."""
    sql = text(f"""
        SELECT TOP 1 1 FROM {TB_BALANCETE}
        WHERE LTRIM(RTRIM([CO_CONTA])) = :c AND [DELETED_AT] IS NULL
    """)
    return db.session.execute(sql, {'c': str(co_conta).strip()}).first() is not None


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
#
# DESEMPENHO: a COR_VW004 é uma view calculada (pesada). Antes cada tela a
# executava 4 vezes (status, linhas do filtro, tipos do filtro e tabela).
# Agora ela é lida UMA única vez por requisição (_ler_view, guardada em g) e
# status, opções de filtro e tabela são montados em Python a partir dessas
# linhas (poucas: linhas do FCO x tipos de pessoa). O histórico (COR_TB032)
# é filtrado direto pela data, com o parâmetro do tipo date, sem CONVERT na
# coluna (assim o SQL Server usa a chave primária).

def _normalizar_linha(r, historico=False):
    sisgea = _dec(r['VR_SISGEA'])
    balancete = _dec(r['VR_BALANCETE'])
    if historico and r.get('VR_DIFERENCA') is not None:
        # Histórico: usa a diferença que foi gravada na tabela
        diferenca = Decimal(int(r['VR_DIFERENCA']))
    else:
        # Prévia: diferença calculada aqui (Balancete - SISGEA)
        diferenca = (balancete or Decimal('0')) - (sisgea or Decimal('0'))
    return {
        'dt_referencia': _para_date(r['DT_REFERENCIA']),
        'tipo_pessoa': (r['TIPO_PESSOA'] or '').strip(),
        'id_linha': int(r['ID_LINHA']),
        'dsc_linha': (r['DSC_LINHA'] or '').strip(),
        'vr_sisgea': sisgea,
        'vr_balancete': balancete,
        'vr_diferenca': diferenca,
        'id_previa': r.get('ID_PREVIA'),
    }


def _ler_view():
    """Executa a COR_VW004 uma única vez por requisição (cache em flask.g)."""
    if getattr(g, '_cc_view', None) is not None:
        return g._cc_view
    sql = text(f"""
        SELECT [DT_REFERENCIA], [TIPO_PESSOA], [ID_LINHA], [DSC_LINHA],
               [VR_SISGEA], [VR_BALANCETE], [ID_PREVIA]
        FROM {VW_VALORES_MES}
    """)
    linhas = [_normalizar_linha(dict(r)) for r in db.session.execute(sql).mappings().all()]
    linhas.sort(key=lambda l: (l['id_linha'], l['tipo_pessoa']))
    g._cc_view = linhas
    return linhas


def _ler_historico(dt_ref):
    """Linhas da COR_TB032 de uma data (filtro direto na chave)."""
    if not dt_ref:
        return []
    sql = text(f"""
        SELECT [DT_REFERENCIA], [TIPO_PESSOA], [ID_LINHA], [DSC_LINHA],
               [VR_SISGEA], [VR_BALANCETE], [VR_DIFERENCA]
        FROM {TB_DIFERENCAS}
        WHERE [DT_REFERENCIA] = :dt
        ORDER BY [ID_LINHA], [TIPO_PESSOA]
    """)
    return [_normalizar_linha(dict(r), historico=True)
            for r in db.session.execute(sql, {'dt': dt_ref}).mappings().all()]


def _data_ja_guardada(dt_ref):
    if not dt_ref:
        return False
    sql = text(f"SELECT TOP 1 1 FROM {TB_DIFERENCAS} WHERE [DT_REFERENCIA] = :dt")
    return db.session.execute(sql, {'dt': dt_ref}).first() is not None


def _status_previa():
    """Situação da prévia (view): datas, total de linhas e quantas ainda estão em prévia."""
    registros = _ler_view()
    qtd_total = len(registros)
    qtd_previa = sum(1 for r in registros if r['id_previa'] is None or int(r['id_previa']) != 0)
    datas = sorted(set(r['dt_referencia'] for r in registros if r['dt_referencia']))
    dt_min = datas[0] if datas else None
    dt_max = datas[-1] if datas else None
    return {
        'qtd_total': qtd_total,
        'qtd_previa': qtd_previa,
        'qtd_liberada': qtd_total - qtd_previa,
        'dt_min': dt_min,
        'dt_max': dt_max,
        'qtd_sem_data': sum(1 for r in registros if r['dt_referencia'] is None),
        'liberado': qtd_total > 0 and qtd_previa == 0,
        'ja_gravado': _data_ja_guardada(dt_max),
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


def _linhas_da_fonte(fonte, dt_ref=None):
    """Todas as linhas da origem escolhida, sem filtro (view ou histórico)."""
    if fonte == FONTE_HISTORICO:
        return _ler_historico(dt_ref)
    return _ler_view()


def _opcoes_filtro(linhas_fonte):
    """Linhas (ID/DSC) e tipos de pessoa existentes na origem, montados em Python."""
    por_id = {}
    for l in linhas_fonte:
        if l['id_linha'] not in por_id or (not por_id[l['id_linha']] and l['dsc_linha']):
            por_id[l['id_linha']] = l['dsc_linha']
    linhas = [{'id_linha': k, 'dsc_linha': por_id[k]} for k in sorted(por_id)]
    tipos = sorted(set(l['tipo_pessoa'] for l in linhas_fonte if l['tipo_pessoa']))
    return linhas, tipos


def _filtrar_linhas(linhas_fonte, id_linha=None, tipos=None):
    return [
        l for l in linhas_fonte
        if (id_linha is None or l['id_linha'] == id_linha)
        and (not tipos or l['tipo_pessoa'] in tipos)
    ]


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
    """
    Tela inicial com os atalhos: Vinculações, Conciliações, Relatórios FCO e Auditoria.
    Não executa a view do balancete aqui (fica na página de Conciliações),
    para a tela inicial abrir rápido.
    """
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
    except Exception as e:
        db.session.rollback()
        print(f"[contas_contabeis] Erro ao montar resumo: {e}")

    return render_template('contas_contabeis/index.html', resumo=resumo)


@contas_contabeis_bp.route('/conciliacoes')
@login_required
@sistema_requerido(SISTEMA)
def conciliacoes():
    """Página que reúne as conciliações (por enquanto: SISGEA X BALANCETE)."""
    db.session.expire_all()
    resumo = {'status': None}
    try:
        resumo['status'] = _status_previa()
    except Exception as e:
        db.session.rollback()
        print(f"[contas_contabeis] Erro ao ler a situação da prévia: {e}")
    return render_template('contas_contabeis/conciliacoes.html', resumo=resumo)


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
        if not _conta_existe_no_balancete(co_conta):
            return _json_erro('A conta {} não existe no balancete (COR_TB012). '
                              'Só é possível incluir contas que constam no balancete.'.format(co_conta), 422)
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
# VINCULAÇÕES (página com os botões de vinculação)
# =========================================================================

@contas_contabeis_bp.route('/vinculacoes')
@login_required
@sistema_requerido(SISTEMA)
def vinculacoes():
    """Página que reúne as vinculações: SISGEA X BALANCETE (COR_TB031), SISGEA X FCO (COR_TB037)
    e FCO X BALANCETE (COR_TB041)."""
    db.session.expire_all()
    resumo = {'contas': {'qtd': 0, 'sem': 0}, 'fco': {'qtd': 0, 'sem': 0}, 'fco_bal': {'qtd': 0, 'sem': 0}}
    for chave, tabela in (('contas', TB_CONTAS), ('fco', TB_LINHAS_SISGEA)):
        try:
            r = db.session.execute(text(f"""
                SELECT COUNT(1) AS QTD,
                       SUM(CASE WHEN [ID_LINHA] IS NULL THEN 1 ELSE 0 END) AS SEM_VINCULO
                FROM {tabela}
            """)).mappings().first()
            resumo[chave]['qtd'] = int(r['QTD'] or 0) if r else 0
            resumo[chave]['sem'] = int(r['SEM_VINCULO'] or 0) if r else 0
        except Exception as e:
            db.session.rollback()
            print(f"[contas_contabeis] Erro ao contar {tabela}: {e}")
    try:
        resumo['fco_bal']['qtd'] = int(db.session.execute(
            text(f"SELECT COUNT(1) FROM {TB_VINC_ITEM_LINHA_CONTA}")).scalar() or 0)
    except Exception as e:
        db.session.rollback()
        print(f"[contas_contabeis] Erro ao contar {TB_VINC_ITEM_LINHA_CONTA}: {e}")
    return render_template('contas_contabeis/vinculacoes.html', resumo=resumo)


# =========================================================================
# VINCULAÇÃO SISGEA X FCO (COR_TB037 x COR_TB030)
# =========================================================================

def _metadados_tb037():
    """
    Tipo e tamanho das colunas da COR_TB037 lidos do próprio banco
    (INFORMATION_SCHEMA), para validar e limitar os campos sem fixar nada
    no código. Também diz se o pkLinhaContabil é IDENTITY.
    """
    sql = text("""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'BDG' AND TABLE_NAME = 'COR_TB037_FCO_LINHAS_SISGEA'
    """)
    colunas = {}
    for r in db.session.execute(sql).mappings().all():
        tamanho = r['CHARACTER_MAXIMUM_LENGTH']
        colunas[r['COLUMN_NAME'].upper()] = {
            'tipo': (r['DATA_TYPE'] or '').lower(),
            'tamanho': int(tamanho) if tamanho is not None and int(tamanho) > 0 else None,
        }
    identidade = db.session.execute(text(
        "SELECT COLUMNPROPERTY(OBJECT_ID(:obj), 'pkLinhaContabil', 'IsIdentity')"
    ), {'obj': TB_LINHAS_SISGEA_OBJ}).scalar()
    return colunas, bool(identidade)


TIPOS_INTEIROS = ('tinyint', 'smallint', 'int', 'bigint')
TIPOS_NUMERICOS = TIPOS_INTEIROS + ('decimal', 'numeric', 'float', 'real', 'money', 'smallmoney')


def _validar_campo_tb037(valor, meta, rotulo, obrigatorio=True):
    """Valida um valor de acordo com o tipo da coluna. Retorna (ok, valor_convertido, mensagem)."""
    texto = str(valor if valor is not None else '').strip()
    if not texto:
        if obrigatorio:
            return False, None, 'Informe o campo {}.'.format(rotulo)
        return True, None, ''
    tipo = (meta or {}).get('tipo', '')
    if tipo in TIPOS_INTEIROS:
        try:
            return True, int(texto), ''
        except ValueError:
            return False, None, 'O campo {} aceita apenas números inteiros.'.format(rotulo)
    if tipo in TIPOS_NUMERICOS:
        try:
            return True, Decimal(texto.replace(',', '.')), ''
        except Exception:
            return False, None, 'O campo {} aceita apenas números.'.format(rotulo)
    tamanho = (meta or {}).get('tamanho')
    if tamanho and len(texto) > tamanho:
        return False, None, 'O campo {} aceita no máximo {} caracteres.'.format(rotulo, tamanho)
    return True, texto, ''


def _listar_linhas_sisgea():
    """Linhas da COR_TB037 com a descrição da linha do FCO (COR_TB030)."""
    sql = text(f"""
        SELECT L.[pkLinhaContabil], L.[ID_FCO_LINHA], L.[DSC_FCO_LINHA], L.[ID_LINHA], I.[DSC_LINHA]
        FROM {TB_LINHAS_SISGEA} L
        LEFT JOIN {TB_ITENS} I ON I.[ID_LINHA] = L.[ID_LINHA]
    """)
    linhas = []
    for r in db.session.execute(sql).mappings().all():
        linhas.append({
            'pk': _txt(r['pkLinhaContabil']),
            'id_fco_linha': _txt(r['ID_FCO_LINHA']),
            'dsc_fco_linha': _txt(r['DSC_FCO_LINHA']),
            'id_linha': int(r['ID_LINHA']) if r['ID_LINHA'] is not None else None,
            'dsc_linha': _txt(r['DSC_LINHA']),
        })
    linhas.sort(key=lambda l: (_chave_ordem(l['id_fco_linha']), l['dsc_fco_linha'], _chave_ordem(l['pk'])))
    return linhas


def _buscar_linha_sisgea(pk):
    sql = text(f"""
        SELECT [pkLinhaContabil], [ID_FCO_LINHA], [DSC_FCO_LINHA], [ID_LINHA]
        FROM {TB_LINHAS_SISGEA}
        WHERE [pkLinhaContabil] = :pk
    """)
    return db.session.execute(sql, {'pk': pk}).mappings().first()


@contas_contabeis_bp.route('/vinculacao-fco')
@login_required
@sistema_requerido(SISTEMA)
def vinculacao_fco():
    db.session.expire_all()
    linhas = _listar_linhas_sisgea()
    itens = _listar_itens()
    try:
        meta, _identidade = _metadados_tb037()
    except Exception:
        db.session.rollback()
        meta = {}
    qtd_sem_vinculo = sum(1 for l in linhas if l['id_linha'] is None)

    def _limite(coluna):
        m = meta.get(coluna) or {}
        return {'numero': m.get('tipo') in TIPOS_NUMERICOS, 'tamanho': m.get('tamanho')}

    return render_template(
        'contas_contabeis/vinculacao_fco.html',
        linhas=linhas,
        itens=itens,
        qtd_linhas=len(linhas),
        qtd_sem_vinculo=qtd_sem_vinculo,
        qtd_vinculadas=len(linhas) - qtd_sem_vinculo,
        campo_id=_limite('ID_FCO_LINHA'),
        campo_dsc=_limite('DSC_FCO_LINHA'),
    )


@contas_contabeis_bp.route('/vinculacao-fco/salvar', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def vinculacao_fco_salvar():
    """Grava o ID_LINHA (COR_TB030) na linha do SISGEA escolhida (COR_TB037)."""
    dados = request.get_json(silent=True) or {}
    pk = str(dados.get('pk') or '').strip()
    ok, id_linha = _ler_id_linha(dados.get('id_linha'))

    if not pk:
        return _json_erro('Selecione a linha do SISGEA.')
    if not ok:
        return _json_erro('Linha do FCO inválida.')

    try:
        linha = _buscar_linha_sisgea(pk)
        if not linha:
            return _json_erro('Linha do SISGEA não encontrada.', 404)
        if id_linha is not None and not _item_existe(id_linha):
            return _json_erro('A linha {} não existe na tabela de itens do FCO.'.format(id_linha), 404)

        rotulo = '{} - {}'.format(_txt(linha['ID_FCO_LINHA']), _txt(linha['DSC_FCO_LINHA']))
        id_anterior = int(linha['ID_LINHA']) if linha['ID_LINHA'] is not None else None
        if id_anterior == id_linha:
            return jsonify({'sucesso': True, 'mensagem': 'A linha {} já estava vinculada a essa linha do FCO.'.format(rotulo)})

        db.session.execute(text(f"""
            UPDATE {TB_LINHAS_SISGEA}
            SET [ID_LINHA] = :id_linha
            WHERE [pkLinhaContabil] = :pk
        """), {'id_linha': id_linha, 'pk': pk})
        db.session.commit()

        registrar_log(
            acao='editar',
            entidade='contas_contabeis_vinculacao_fco',
            entidade_id=pk,
            descricao='Vinculação SISGEA X FCO da linha {} alterada de {} para {}'.format(
                rotulo,
                id_anterior if id_anterior is not None else 'sem vínculo',
                id_linha if id_linha is not None else 'sem vínculo'),
            dados_antigos={'pkLinhaContabil': pk, 'ID_LINHA': id_anterior},
            dados_novos={'pkLinhaContabil': pk, 'ID_LINHA': id_linha},
        )
        return jsonify({'sucesso': True, 'mensagem': 'Vinculação da linha {} salva com sucesso.'.format(rotulo)})
    except Exception as e:
        db.session.rollback()
        return _json_erro('Erro ao salvar a vinculação: {}'.format(e), 500)


@contas_contabeis_bp.route('/vinculacao-fco/nova', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def vinculacao_fco_nova():
    """Cria uma nova linha na COR_TB037 (ID_FCO_LINHA, DSC_FCO_LINHA, ID_LINHA)."""
    dados = request.get_json(silent=True) or {}
    ok_vinc, id_linha = _ler_id_linha(dados.get('id_linha'))
    if not ok_vinc:
        return _json_erro('Linha do FCO inválida.')

    try:
        meta, identidade = _metadados_tb037()
        ok, id_fco, msg = _validar_campo_tb037(dados.get('id_fco_linha'), meta.get('ID_FCO_LINHA'), 'ID da linha do SISGEA')
        if not ok:
            return _json_erro(msg)
        ok, dsc_fco, msg = _validar_campo_tb037(dados.get('dsc_fco_linha'), meta.get('DSC_FCO_LINHA'), 'descrição da linha do SISGEA')
        if not ok:
            return _json_erro(msg)
        if isinstance(dsc_fco, str):
            dsc_fco = dsc_fco.upper()

        if id_linha is not None and not _item_existe(id_linha):
            return _json_erro('A linha {} não existe na tabela de itens do FCO.'.format(id_linha), 404)

        # Mesma linha (ID + descrição) já cadastrada?
        existe = db.session.execute(text(f"""
            SELECT TOP 1 1 FROM {TB_LINHAS_SISGEA}
            WHERE [ID_FCO_LINHA] = :id AND LTRIM(RTRIM([DSC_FCO_LINHA])) = :dsc
        """), {'id': id_fco, 'dsc': dsc_fco}).first()
        if existe:
            return _json_erro('A linha {} - {} já existe. Use a vinculação para alterar a linha do FCO.'.format(id_fco, dsc_fco), 409)

        if identidade:
            db.session.execute(text(f"""
                INSERT INTO {TB_LINHAS_SISGEA} ([ID_FCO_LINHA], [DSC_FCO_LINHA], [ID_LINHA])
                VALUES (:id, :dsc, :id_linha)
            """), {'id': id_fco, 'dsc': dsc_fco, 'id_linha': id_linha})
        else:
            # pk sem IDENTITY: próximo número calculado na mesma instrução (com bloqueio)
            db.session.execute(text(f"""
                INSERT INTO {TB_LINHAS_SISGEA} ([pkLinhaContabil], [ID_FCO_LINHA], [DSC_FCO_LINHA], [ID_LINHA])
                SELECT ISNULL(MAX([pkLinhaContabil]), 0) + 1, :id, :dsc, :id_linha
                FROM {TB_LINHAS_SISGEA} WITH (UPDLOCK, HOLDLOCK)
            """), {'id': id_fco, 'dsc': dsc_fco, 'id_linha': id_linha})
        db.session.commit()

        registrar_log(
            acao='criar',
            entidade='contas_contabeis_vinculacao_fco',
            entidade_id='{}'.format(id_fco),
            descricao='Nova linha SISGEA X FCO cadastrada: {} - {}'.format(id_fco, dsc_fco),
            dados_novos={'ID_FCO_LINHA': id_fco, 'DSC_FCO_LINHA': dsc_fco, 'ID_LINHA': id_linha},
        )
        return jsonify({'sucesso': True, 'mensagem': 'Linha {} - {} cadastrada com sucesso.'.format(id_fco, dsc_fco)})
    except Exception as e:
        db.session.rollback()
        return _json_erro('Erro ao cadastrar a linha: {}'.format(e), 500)


# =========================================================================
# VINCULAÇÃO FCO X BALANCETE (COR_TB041 + parâmetros COR_TB034/036/037)
# =========================================================================

# A Vinculação FCO X BALANCETE trabalha SOMENTE com este item do FCO (ID_FCO da COR_TB036):
# a lista mostra só ele, a inclusão vem travada nele e o back end bloqueia qualquer outro.
ITEM_FCO_PERMITIDO = '80'

# Colunas da COR_TB041 -> (rótulo na tela, chave usada no JSON)
COLS_TB041 = [
    ('COD_AGRUP_SISGEA', 'agrupamento', 'agrup'),
    ('ID_FCO_ITEM', 'item do FCO', 'item'),
    ('ID_FCO_LINHA', 'linha do FCO', 'linha'),
    ('CT_CONTABIL', 'conta contábil', 'conta'),
]


def _metadados_tabela(nome_tabela):
    """Tipo e tamanho das colunas de uma tabela do schema BDG (INFORMATION_SCHEMA)."""
    sql = text("""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'BDG' AND TABLE_NAME = :t
    """)
    colunas = {}
    for r in db.session.execute(sql, {'t': nome_tabela}).mappings().all():
        tamanho = r['CHARACTER_MAXIMUM_LENGTH']
        colunas[r['COLUMN_NAME'].upper()] = {
            'tipo': (r['DATA_TYPE'] or '').lower(),
            'tamanho': int(tamanho) if tamanho is not None and int(tamanho) > 0 else None,
        }
    return colunas


def _param_agrupamentos():
    """COR_TB034: um item por COD_AGRUP_SISGEA, com a descrição do SISCONT."""
    sql = text(f"""
        SELECT [COD_AGRUP_SISGEA], MIN([DSC_AgrupamentoSISCONT]) AS DSC
        FROM {TB_AGRUP_CONTABIL}
        WHERE [COD_AGRUP_SISGEA] IS NOT NULL
        GROUP BY [COD_AGRUP_SISGEA]
    """)
    lista = [{'cod': _txt(r['COD_AGRUP_SISGEA']), 'dsc': _txt(r['DSC'])}
             for r in db.session.execute(sql).mappings().all()]
    return sorted(lista, key=lambda x: _chave_ordem(x['cod']))


def _param_itens_sisgea(somente=None):
    """COR_TB036: itens do FCO (ID_FCO / DSC_FCO). 'somente' limita a um item."""
    filtro = 'AND [ID_FCO] = :item' if somente is not None else ''
    sql = text(f"""
        SELECT [ID_FCO], MIN([DSC_FCO]) AS DSC
        FROM {TB_ITENS_SISGEA}
        WHERE [ID_FCO] IS NOT NULL {filtro}
        GROUP BY [ID_FCO]
    """)
    params = {'item': somente} if somente is not None else {}
    lista = [{'cod': _txt(r['ID_FCO']), 'dsc': _txt(r['DSC'])}
             for r in db.session.execute(sql, params).mappings().all()]
    return sorted(lista, key=lambda x: _chave_ordem(x['cod']))


def _param_linhas_sisgea():
    """COR_TB037: linhas do FCO (ID_FCO_LINHA / DSC_FCO_LINHA)."""
    sql = text(f"""
        SELECT [ID_FCO_LINHA], MIN([DSC_FCO_LINHA]) AS DSC
        FROM {TB_LINHAS_SISGEA}
        WHERE [ID_FCO_LINHA] IS NOT NULL
        GROUP BY [ID_FCO_LINHA]
    """)
    lista = [{'cod': _txt(r['ID_FCO_LINHA']), 'dsc': _txt(r['DSC'])}
             for r in db.session.execute(sql).mappings().all()]
    return sorted(lista, key=lambda x: _chave_ordem(x['cod']))


def _listar_vinc_fco_balancete():
    """
    COR_TB041 (somente o item ITEM_FCO_PERMITIDO) com os nomes dos parâmetros
    (OUTER APPLY evita duplicar linhas).
    """
    sql = text(f"""
        SELECT V.[COD_AGRUP_SISGEA], V.[ID_FCO_ITEM], V.[ID_FCO_LINHA], V.[CT_CONTABIL],
               A.DSC AS DSC_AGRUP, I.DSC AS DSC_ITEM, L.DSC AS DSC_LINHA
        FROM {TB_VINC_ITEM_LINHA_CONTA} V
        OUTER APPLY (SELECT TOP 1 [DSC_AgrupamentoSISCONT] AS DSC FROM {TB_AGRUP_CONTABIL}
                     WHERE [COD_AGRUP_SISGEA] = V.[COD_AGRUP_SISGEA]) A
        OUTER APPLY (SELECT TOP 1 [DSC_FCO] AS DSC FROM {TB_ITENS_SISGEA}
                     WHERE [ID_FCO] = V.[ID_FCO_ITEM]) I
        OUTER APPLY (SELECT TOP 1 [DSC_FCO_LINHA] AS DSC FROM {TB_LINHAS_SISGEA}
                     WHERE [ID_FCO_LINHA] = V.[ID_FCO_LINHA]) L
        WHERE V.[ID_FCO_ITEM] = :item
    """)
    lista = []
    for r in db.session.execute(sql, {'item': ITEM_FCO_PERMITIDO}).mappings().all():
        lista.append({
            'agrup': _txt(r['COD_AGRUP_SISGEA']), 'dsc_agrup': _txt(r['DSC_AGRUP']),
            'item': _txt(r['ID_FCO_ITEM']), 'dsc_item': _txt(r['DSC_ITEM']),
            'linha': _txt(r['ID_FCO_LINHA']), 'dsc_linha': _txt(r['DSC_LINHA']),
            'conta': _txt(r['CT_CONTABIL']),
        })
    # Contas do vínculo que existem no balancete (uma consulta só para todas)
    contas_ok = set()
    if lista:
        sql_bal = text(f"""
            SELECT DISTINCT LTRIM(RTRIM(B.[CO_CONTA])) AS CO_CONTA
            FROM {TB_BALANCETE} B
            WHERE B.[DELETED_AT] IS NULL
              AND LTRIM(RTRIM(B.[CO_CONTA])) IN (
                  SELECT LTRIM(RTRIM([CT_CONTABIL])) FROM {TB_VINC_ITEM_LINHA_CONTA}
                  WHERE [ID_FCO_ITEM] = :item AND [CT_CONTABIL] IS NOT NULL)
        """)
        try:
            contas_ok = set(_txt(r[0]) for r in db.session.execute(sql_bal, {'item': ITEM_FCO_PERMITIDO}).all())
        except Exception as e:
            db.session.rollback()
            print(f"[contas_contabeis] Erro ao conferir contas no balancete: {e}")
            contas_ok = None

    # Situação: "vinculada" = linha com conta contábil preenchida; "sem" = conta vazia.
    # Alertas (não mudam a situação): parâmetro não encontrado ou conta fora do balancete.
    for v in lista:
        alertas = []
        if not v['dsc_agrup']:
            alertas.append('agrupamento não encontrado')
        if not v['dsc_item']:
            alertas.append('item não encontrado')
        if not v['dsc_linha']:
            alertas.append('linha não encontrada')
        if v['conta'] and contas_ok is not None and v['conta'] not in contas_ok:
            alertas.append('conta fora do balancete')
        v['pendencias'] = alertas
        v['situacao'] = 'vinculada' if v['conta'] else 'sem'

    lista.sort(key=lambda x: (_chave_ordem(x['agrup']), _chave_ordem(x['item']),
                              _chave_ordem(x['linha']), x['conta']))
    return lista


def _ler_valores_tb041(dados, prefixo='', conta_obrigatoria=True):
    """
    Lê e valida os 4 campos (com o tipo da coluna vindo do banco).
    conta_obrigatoria=False: usado no vínculo original, que pode estar sem conta.
    Retorna (valores, mensagem_erro).
    """
    meta = _metadados_tabela('COR_TB041_FCO_VINC_ITEM_LINHA_CONTA')
    valores = {}
    for coluna, rotulo, chave in COLS_TB041:
        obrigatorio = conta_obrigatoria or coluna != 'CT_CONTABIL'
        ok, valor, msg = _validar_campo_tb037(dados.get(prefixo + chave), meta.get(coluna), rotulo, obrigatorio)
        if not ok:
            return None, msg
        valores[coluna] = valor
    return valores, None


def _where_tb041(valores, prefixo='o_'):
    """
    WHERE pelos 4 valores (a tabela não tem chave primária).
    Valor vazio (None) localiza a coluna nula ou em branco: é o caso da linha sem conta.
    """
    partes = []
    for c, _r, _k in COLS_TB041:
        if valores.get(c) is None:
            partes.append("([{c}] IS NULL OR LTRIM(RTRIM(CAST([{c}] AS varchar(100)))) = '')".format(c=c))
        else:
            partes.append('[{c}] = :{p}{c}'.format(c=c, p=prefixo))
    return ' AND '.join(partes)


def _validar_parametros_tb041(v):
    """Confere se cada valor existe na sua tabela de parâmetro e a conta no balancete."""
    checagens = [
        (TB_AGRUP_CONTABIL, 'COD_AGRUP_SISGEA', v['COD_AGRUP_SISGEA'], 'O agrupamento {} não existe na COR_TB034.'),
        (TB_ITENS_SISGEA, 'ID_FCO', v['ID_FCO_ITEM'], 'O item {} não existe na COR_TB036.'),
        (TB_LINHAS_SISGEA, 'ID_FCO_LINHA', v['ID_FCO_LINHA'], 'A linha {} não existe na COR_TB037.'),
    ]
    for tabela, coluna, valor, msg in checagens:
        existe = db.session.execute(text(
            'SELECT TOP 1 1 FROM {} WHERE [{}] = :v'.format(tabela, coluna)), {'v': valor}).first()
        if not existe:
            return msg.format(valor)
    if not _conta_existe_no_balancete(v['CT_CONTABIL']):
        return ('A conta {} não existe no balancete (COR_TB012). '
                'Só é possível vincular contas que constam no balancete.'.format(v['CT_CONTABIL']))
    return None


@contas_contabeis_bp.route('/vinculacao-fco-balancete')
@login_required
@sistema_requerido(SISTEMA)
def vinculacao_fco_balancete():
    db.session.expire_all()
    vinculos = _listar_vinc_fco_balancete()
    try:
        meta = _metadados_tabela('COR_TB041_FCO_VINC_ITEM_LINHA_CONTA')
    except Exception:
        db.session.rollback()
        meta = {}
    tam_conta = (meta.get('CT_CONTABIL') or {}).get('tamanho')
    return render_template(
        'contas_contabeis/vinculacao_fco_balancete.html',
        vinculos=vinculos,
        agrupamentos=_param_agrupamentos(),
        itens=_param_itens_sisgea(somente=ITEM_FCO_PERMITIDO),
        linhas=_param_linhas_sisgea(),
        qtd_vinculos=len(vinculos),
        qtd_agrup=len(set(v['agrup'] for v in vinculos)),
        qtd_contas=len(set(v['conta'] for v in vinculos if v['conta'])),
        qtd_sem_vinculo=sum(1 for v in vinculos if v['situacao'] == 'sem'),
        qtd_vinculadas=sum(1 for v in vinculos if v['situacao'] == 'vinculada'),
        tam_conta=tam_conta,
        item_permitido=ITEM_FCO_PERMITIDO,
    )


@contas_contabeis_bp.route('/vinculacao-fco-balancete/contas')
@login_required
@sistema_requerido(SISTEMA)
def vinculacao_fco_balancete_contas():
    """Busca de contas do balancete (COR_TB012) para o dropdown com pesquisa."""
    termo = (request.args.get('q') or '').strip()
    params = {}
    filtro = ''
    if termo:
        filtro = 'AND LTRIM(RTRIM([CO_CONTA])) LIKE :q'
        params['q'] = '%' + termo.replace('[', '[[]').replace('%', '[%]').replace('_', '[_]') + '%'
    sql = text(f"""
        SELECT DISTINCT TOP 50 LTRIM(RTRIM([CO_CONTA])) AS CO_CONTA
        FROM {TB_BALANCETE}
        WHERE [DELETED_AT] IS NULL AND [CO_CONTA] IS NOT NULL {filtro}
        ORDER BY 1
    """)
    try:
        contas = [r[0] for r in db.session.execute(sql, params).all() if r[0]]
    except Exception as e:
        db.session.rollback()
        return jsonify({'results': [], 'erro': str(e)}), 500
    return jsonify({'results': [{'id': c, 'text': c} for c in contas]})


@contas_contabeis_bp.route('/verificar-conta')
@login_required
@sistema_requerido(SISTEMA)
def verificar_conta():
    """
    Verificação na hora (enquanto digita) se a conta existe no CO_CONTA da COR_TB012.
    Usada nos formulários de conta nova. A gravação confere de novo no back end.
    """
    conta = (request.args.get('c') or '').strip()
    if not conta:
        return jsonify({'existe': False, 'conta': ''})
    try:
        return jsonify({'existe': _conta_existe_no_balancete(conta), 'conta': conta})
    except Exception as e:
        db.session.rollback()
        return jsonify({'existe': None, 'conta': conta, 'erro': str(e)}), 500


@contas_contabeis_bp.route('/vinculacao-fco-balancete/salvar', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def vinculacao_fco_balancete_salvar():
    """
    Inclui um vínculo novo ou altera um existente na COR_TB041.
    Alteração: o front manda os 4 valores originais (o_agrup, o_item, o_linha, o_conta).
    """
    dados = request.get_json(silent=True) or {}
    editando = bool(dados.get('o_agrup') or dados.get('o_conta'))

    try:
        novos, msg = _ler_valores_tb041(dados)
        if msg:
            return _json_erro(msg)
        if isinstance(novos['CT_CONTABIL'], str):
            novos['CT_CONTABIL'] = novos['CT_CONTABIL'].strip()

        # Bloqueio: esta vinculação só aceita o item ITEM_FCO_PERMITIDO
        if _txt(novos['ID_FCO_ITEM']) != ITEM_FCO_PERMITIDO:
            return _json_erro('Nesta vinculação só é permitido o item {} do FCO.'.format(ITEM_FCO_PERMITIDO), 422)

        msg = _validar_parametros_tb041(novos)
        if msg:
            return _json_erro(msg, 422)

        antigos = None
        if editando:
            antigos, msg = _ler_valores_tb041(dados, prefixo='o_', conta_obrigatoria=False)
            if msg:
                return _json_erro('Vínculo original inválido: ' + msg)
            if _txt(antigos['ID_FCO_ITEM']) != ITEM_FCO_PERMITIDO:
                return _json_erro('Nesta vinculação só é permitido alterar vínculos do item {}.'.format(ITEM_FCO_PERMITIDO), 422)

        # Já existe exatamente esse vínculo?
        params_novos = {'o_' + c: v for c, v in novos.items()}
        ja_existe = db.session.execute(text(
            'SELECT TOP 1 1 FROM {} WHERE {}'.format(TB_VINC_ITEM_LINHA_CONTA, _where_tb041(novos))), params_novos).first()
        if ja_existe and (not editando or antigos != novos):
            return _json_erro('Esse vínculo já existe (agrupamento {}, item {}, linha {}, conta {}).'.format(
                novos['COD_AGRUP_SISGEA'], novos['ID_FCO_ITEM'], novos['ID_FCO_LINHA'], novos['CT_CONTABIL']), 409)
        if editando and antigos == novos:
            return jsonify({'sucesso': True, 'mensagem': 'Nada foi alterado.'})

        if editando:
            params = {'n_' + c: v for c, v in novos.items()}
            params.update({'o_' + c: v for c, v in antigos.items() if v is not None})
            set_sql = ', '.join('[{c}] = :n_{c}'.format(c=c) for c, _r, _k in COLS_TB041)
            res = db.session.execute(text('UPDATE {} SET {} WHERE {}'.format(
                TB_VINC_ITEM_LINHA_CONTA, set_sql, _where_tb041(antigos))), params)
            if not res.rowcount:
                db.session.rollback()
                return _json_erro('O vínculo original não foi encontrado (pode ter sido alterado por outra pessoa).', 404)
            acao, descricao = 'editar', 'Vínculo FCO X BALANCETE alterado'
        else:
            colunas = ', '.join('[{}]'.format(c) for c, _r, _k in COLS_TB041)
            valores = ', '.join(':n_{}'.format(c) for c, _r, _k in COLS_TB041)
            db.session.execute(text('INSERT INTO {} ({}) VALUES ({})'.format(
                TB_VINC_ITEM_LINHA_CONTA, colunas, valores)), {'n_' + c: v for c, v in novos.items()})
            acao, descricao = 'criar', 'Novo vínculo FCO X BALANCETE'
        db.session.commit()

        def _json(d):
            return {k: (str(v) if v is not None else None) for k, v in d.items()} if d else None

        registrar_log(
            acao=acao,
            entidade='contas_contabeis_vinculacao_fco_balancete',
            entidade_id='{}|{}|{}|{}'.format(novos['COD_AGRUP_SISGEA'], novos['ID_FCO_ITEM'],
                                             novos['ID_FCO_LINHA'], novos['CT_CONTABIL']),
            descricao='{}: agrupamento {}, item {}, linha {}, conta {}'.format(
                descricao, novos['COD_AGRUP_SISGEA'], novos['ID_FCO_ITEM'], novos['ID_FCO_LINHA'], novos['CT_CONTABIL']),
            dados_antigos=_json(antigos),
            dados_novos=_json(novos),
        )
        return jsonify({'sucesso': True, 'mensagem': 'Vínculo {} com sucesso.'.format(
            'alterado' if editando else 'incluído')})
    except Exception as e:
        db.session.rollback()
        return _json_erro('Erro ao salvar o vínculo: {}'.format(e), 500)


# =========================================================================
# BALANCETE (COR_VW004 / COR_TB032)
# =========================================================================

@contas_contabeis_bp.route('/balancete')
@login_required
@sistema_requerido(SISTEMA)
def balancete():
    db.session.expire_all()
    fonte, datas, dt_ref, id_linha, tipos = _ler_filtros_balancete()

    status = _status_previa()                  # 1ª (e única) leitura da view
    linhas_fonte = _linhas_da_fonte(fonte, dt_ref)
    opcoes_linhas, opcoes_tipos = _opcoes_filtro(linhas_fonte)
    # Ignora tipos que não existem na origem escolhida (ex.: trocou a data)
    tipos = [t for t in tipos if t in opcoes_tipos]
    linhas_db = _filtrar_linhas(linhas_fonte, id_linha, tipos)
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


@contas_contabeis_bp.route('/balancete/guardar-fco', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def balancete_guardar_fco():
    """
    Guardar FCO: copia a COR_VW004 para a COR_TB032 somente se TODAS as linhas
    estiverem com ID_PREVIA = 0. Se a data já existir na COR_TB032, pede
    confirmação para substituir.
    """
    dados = request.get_json(silent=True) or {}
    substituir = bool(dados.get('substituir'))

    try:
        registros = _ler_view()   # uma única execução da view

        if not registros:
            return _json_erro('A view do FCO não retornou nenhuma linha.')

        # 1) Todas as linhas precisam estar com ID_PREVIA = 0
        em_previa = [r for r in registros if r['id_previa'] is None or int(r['id_previa']) != 0]
        if em_previa:
            return _json_erro(
                'O FCO ainda está em prévia: {} de {} linha(s) com ID_PREVIA diferente de 0. '
                'Nada foi guardado.'.format(len(em_previa), len(registros)),
                422
            )

        # 2) Validações das chaves da COR_TB032 (DT_REFERENCIA, TIPO_PESSOA, ID_LINHA)
        sem_data = [r for r in registros if r['dt_referencia'] is None]
        if sem_data:
            return _json_erro('Existem {} linha(s) sem DT_REFERENCIA na view. Nada foi guardado.'.format(len(sem_data)), 422)

        chaves = set()
        for r in registros:
            chave = (r['dt_referencia'], r['tipo_pessoa'], r['id_linha'])
            if chave in chaves:
                return _json_erro(
                    'A view tem linha repetida para {} / {} / linha {}. Nada foi guardado.'.format(
                        chave[0].strftime('%d/%m/%Y'), chave[1], chave[2]), 422)
            chaves.add(chave)

        datas = sorted(set(c[0] for c in chaves))
        datas_txt = [d.strftime('%d/%m/%Y') for d in datas]

        # 3) Data já guardada? Pede confirmação para substituir
        datas_existentes = [d for d in datas if _data_ja_guardada(d)]
        if datas_existentes and not substituir:
            return jsonify({
                'sucesso': False,
                'precisa_confirmar': True,
                'mensagem': 'O FCO de {} já foi guardado antes. '
                            'Deseja substituir os valores guardados pelos valores atuais da view?'.format(
                                ', '.join(d.strftime('%d/%m/%Y') for d in datas_existentes)),
            }), 409

        # 4) Grava tudo numa única transação
        qtd_apagada = 0
        for d in datas_existentes:
            res = db.session.execute(text(f"""
                DELETE FROM {TB_DIFERENCAS}
                WHERE [DT_REFERENCIA] = :dt
            """), {'dt': d})
            qtd_apagada += res.rowcount or 0

        sql_insert = text(f"""
            INSERT INTO {TB_DIFERENCAS}
                ([DT_REFERENCIA], [TIPO_PESSOA], [ID_LINHA], [DSC_LINHA],
                 [VR_SISGEA], [VR_BALANCETE], [VR_DIFERENCA])
            VALUES (:dt, :tipo, :id_linha, :dsc, :sisgea, :balancete, :diferenca)
        """)
        # Uma lista de parâmetros = executemany (um único envio ao banco)
        lote = [{
            'dt': r['dt_referencia'],
            'tipo': r['tipo_pessoa'],
            'id_linha': r['id_linha'],
            'dsc': r['dsc_linha'],
            'sisgea': r['vr_sisgea'],
            'balancete': r['vr_balancete'] if r['vr_balancete'] is not None else Decimal('0'),
            'diferenca': _calcular_diferenca_int(r['vr_balancete'], r['vr_sisgea']),
        } for r in registros]
        db.session.execute(sql_insert, lote)
        db.session.commit()

        registrar_log(
            acao='criar',
            entidade='contas_contabeis_balancete',
            entidade_id=','.join(d.strftime('%Y-%m-%d') for d in datas),
            descricao='FCO guardado ({}): {} linha(s) gravada(s){}'.format(
                ', '.join(datas_txt), len(registros),
                ' substituindo {} linha(s)'.format(qtd_apagada) if qtd_apagada else ''),
            dados_novos={'datas': datas_txt, 'qtd_linhas': len(registros), 'substituidas': qtd_apagada},
        )
        return jsonify({
            'sucesso': True,
            'mensagem': 'FCO de {} guardado: {} linha(s) gravada(s).'.format(
                ', '.join(datas_txt), len(registros)),
            'dt_referencia': datas[-1].isoformat(),
        })
    except Exception as e:
        db.session.rollback()
        return _json_erro('Erro ao guardar o FCO: {}'.format(e), 500)


@contas_contabeis_bp.route('/balancete/exportar')
@login_required
@sistema_requerido(SISTEMA)
def balancete_exportar():
    """Exporta para Excel a mesma tabela que está na tela (com os filtros)."""
    fonte, _datas, dt_ref, id_linha, tipos = _ler_filtros_balancete()
    linhas_db = _filtrar_linhas(_linhas_da_fonte(fonte, dt_ref), id_linha, tipos)
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



# =========================================================================
# RELATÓRIOS SISGEA (COR_VW007 prévia / COR_VW008 histórico)
# =========================================================================

@contas_contabeis_bp.route('/relatorios')
@login_required
@sistema_requerido(SISTEMA)
def relatorios():
    """Página que reúne os relatórios: FCO Prévia (COR_VW007) e FCO Histórico (COR_VW008)."""
    return render_template('contas_contabeis/relatorios.html')


SISGEA_FONTES = {
    'previa': {
        'view': VW_SISGEA_PREVIA,
        'titulo': 'FCO Prévia',
        'subtitulo': 'Montagem do FCO a partir do SISGEA - mês em prévia',
        'arquivo': 'FCO_Previa',
    },
    'historico': {
        'view': VW_SISGEA_HISTORICO,
        'titulo': 'FCO Histórico',
        'subtitulo': 'Montagem do FCO a partir do SISGEA - meses anteriores',
        'arquivo': 'FCO_Historico',
    },
}

SISGEA_COLUNAS = """
    [DT_REFERENCIA], [CD_AGRUPAMENTO], [ID_ITEM_CONTABIL], [DSC_ITEM_CONTABIL],
    [ID_LINHA_CONTABIL], [DSC_LINHA_CONTABIL], [COD_EVENTO],
    [SL_DEB], [VR_DEBITO], [SL_CRED], [VR_CREDITO]
"""


def _txt(valor):
    """Texto limpo (None vira '')."""
    if valor is None:
        return ''
    if isinstance(valor, float) and valor.is_integer():
        valor = int(valor)
    if isinstance(valor, Decimal) and valor == valor.to_integral_value():
        valor = int(valor)
    return str(valor).strip()


def _chave_ordem(valor):
    """Ordena códigos numéricos como número e o resto como texto."""
    texto = _txt(valor)
    try:
        return (0, float(texto), texto)
    except ValueError:
        return (1, 0.0, texto)


def _normalizar_sisgea(r):
    return {
        'dt': _para_date(r['DT_REFERENCIA']),
        'agrup': _txt(r['CD_AGRUPAMENTO']),
        'id_item': _txt(r['ID_ITEM_CONTABIL']),
        'dsc_item': _txt(r['DSC_ITEM_CONTABIL']),
        'id_linha': _txt(r['ID_LINHA_CONTABIL']),
        'dsc_linha': _txt(r['DSC_LINHA_CONTABIL']),
        'evento': _txt(r['COD_EVENTO']),
        'sl_deb': _txt(r['SL_DEB']),
        'vr_deb': _dec(r['VR_DEBITO']),
        'sl_cred': _txt(r['SL_CRED']),
        'vr_cred': _dec(r['VR_CREDITO']),
    }


def _num_excel(texto):
    """No Excel, códigos só com dígitos vão como número (ex.: linha 0, SL 2)."""
    if texto and texto.isdigit() and not (len(texto) > 1 and texto.startswith('0')):
        return int(texto)
    return texto or None


def _ordenar_sisgea(linhas):
    linhas.sort(key=lambda l: (_chave_ordem(l['agrup']), _chave_ordem(l['id_item']), _chave_ordem(l['id_linha'])))
    return linhas


def _sisgea_datas(fonte):
    """Datas de referência disponíveis na view do histórico (mais recente primeiro)."""
    sql = text(f"""
        SELECT DISTINCT [DT_REFERENCIA]
        FROM {SISGEA_FONTES[fonte]['view']}
        WHERE [DT_REFERENCIA] IS NOT NULL
        ORDER BY [DT_REFERENCIA] DESC
    """)
    datas = []
    for r in db.session.execute(sql).all():
        d = _para_date(r[0])
        if d and d not in datas:
            datas.append(d)
    return datas


def _sisgea_carregar(fonte, dt_pedida=None):
    """
    Lê a view escolhida e devolve (linhas, datas, dt_escolhida).
    - Prévia: a view traz só o mês em prévia, então é lida inteira UMA vez e as
      datas são tiradas das próprias linhas.
    - Histórico: busca primeiro as datas e depois só a data escolhida
      (filtro na consulta, para não trazer todos os meses).
    """
    view = SISGEA_FONTES[fonte]['view']
    if fonte == 'historico':
        datas = _sisgea_datas(fonte)
        dt_escolhida = dt_pedida if dt_pedida in datas else (datas[0] if datas else None)
        if not dt_escolhida:
            return [], datas, None
        sql = text(f"SELECT {SISGEA_COLUNAS} FROM {view} WHERE [DT_REFERENCIA] = :dt")
        linhas = [_normalizar_sisgea(r) for r in db.session.execute(sql, {'dt': dt_escolhida}).mappings().all()]
        return _ordenar_sisgea(linhas), datas, dt_escolhida

    sql = text(f"SELECT {SISGEA_COLUNAS} FROM {view}")
    todas = [_normalizar_sisgea(r) for r in db.session.execute(sql).mappings().all()]
    datas = sorted(set(l['dt'] for l in todas if l['dt']), reverse=True)
    dt_escolhida = dt_pedida if dt_pedida in datas else (datas[0] if datas else None)
    if len(datas) > 1:
        linhas = [l for l in todas if l['dt'] == dt_escolhida]
    else:
        linhas = todas
    return _ordenar_sisgea(linhas), datas, dt_escolhida


def _sisgea_filtrar(linhas, agrups=None, itens=None, busca='', ocultar_zerados=False):
    """Mesmos filtros da tela, usados na exportação para Excel."""
    busca = (busca or '').strip().lower()
    resultado = []
    for l in linhas:
        if agrups and l['agrup'] not in agrups:
            continue
        if itens and l['id_item'] not in itens:
            continue
        if ocultar_zerados and not (l['vr_deb'] or 0) and not (l['vr_cred'] or 0):
            continue
        if busca:
            alvo = ' '.join([l['agrup'], l['id_item'], l['dsc_item'], l['id_linha'],
                             l['dsc_linha'], l['evento']]).lower()
            if busca not in alvo:
                continue
        resultado.append(l)
    return resultado


@contas_contabeis_bp.route('/sisgea/<fonte>')
@login_required
@sistema_requerido(SISTEMA)
def sisgea(fonte):
    if fonte not in SISGEA_FONTES:
        abort(404)
    cfg = SISGEA_FONTES[fonte]
    dt_pedida = _para_date(request.args.get('dt')) if request.args.get('dt') else None

    erro = None
    try:
        linhas, datas, dt_escolhida = _sisgea_carregar(fonte, dt_pedida)
    except Exception as e:
        db.session.rollback()
        linhas, datas, dt_escolhida = [], [], None
        erro = 'Erro ao consultar a view {}: {}'.format(cfg['view'], e)

    # Dados enviados ao front em JSON (o JS monta a tela e os filtros)
    dados = [{
        'a': l['agrup'], 'i': l['id_item'], 'di': l['dsc_item'],
        'l': l['id_linha'], 'dl': l['dsc_linha'], 'ev': l['evento'],
        'sd': l['sl_deb'], 'vd': float(l['vr_deb']) if l['vr_deb'] is not None else None,
        'sc': l['sl_cred'], 'vc': float(l['vr_cred']) if l['vr_cred'] is not None else None,
    } for l in linhas]

    return render_template(
        'contas_contabeis/sisgea.html',
        fonte=fonte,
        cfg=cfg,
        outra_fonte='historico' if fonte == 'previa' else 'previa',
        outra_cfg=SISGEA_FONTES['historico' if fonte == 'previa' else 'previa'],
        datas=datas,
        dt_escolhida=dt_escolhida,
        dados=dados,
        erro=erro,
    )


@contas_contabeis_bp.route('/sisgea/<fonte>/exportar')
@login_required
@sistema_requerido(SISTEMA)
def sisgea_exportar(fonte):
    """Excel com os filtros da tela: aba 'Relatório' (montagem) e aba 'Dados' (tabela com filtro)."""
    if fonte not in SISGEA_FONTES:
        abort(404)
    cfg = SISGEA_FONTES[fonte]
    dt_pedida = _para_date(request.args.get('dt')) if request.args.get('dt') else None
    agrups = [a for a in request.args.getlist('agrup') if a]
    itens = [i for i in request.args.getlist('item') if i]
    busca = request.args.get('busca', '')
    ocultar_zerados = request.args.get('zerados') == '1'

    linhas, _datas, dt_escolhida = _sisgea_carregar(fonte, dt_pedida)
    linhas = _sisgea_filtrar(linhas, agrups, itens, busca, ocultar_zerados)

    wb = Workbook()
    azul = PatternFill('solid', fgColor='224ABE')
    azul_claro = PatternFill('solid', fgColor='DCE3F7')
    cinza = PatternFill('solid', fgColor='F2F2F2')
    branco_negrito = Font(bold=True, color='FFFFFF')
    negrito = Font(bold=True)
    fina = Side(style='thin', color='D9D9D9')
    borda = Border(left=fina, right=fina, top=fina, bottom=fina)
    centro = Alignment(horizontal='center', vertical='center')
    formato = '#,##0.00;[Red]-#,##0.00'

    filtros_txt = []
    filtros_txt.append('Agrupamento: ' + (', '.join(agrups) if agrups else 'Todos'))
    filtros_txt.append('Item contábil: ' + (', '.join(itens) if itens else 'Todos'))
    if busca.strip():
        filtros_txt.append('Busca: "{}"'.format(busca.strip()))
    if ocultar_zerados:
        filtros_txt.append('Linhas zeradas ocultas')

    # ---------------- Aba 1: Relatório (montagem como no SISGEA) ----------------
    ws = wb.active
    ws.title = 'Relatório'
    ws.cell(row=1, column=1, value='Fechamento Contábil Operacional - FCO - {}'.format(cfg['titulo'])).font = Font(bold=True, size=13)
    ws.cell(row=2, column=1, value='Data de referência: {}'.format(
        dt_escolhida.strftime('%d/%m/%Y') if dt_escolhida else '-'))
    ws.cell(row=3, column=1, value=' | '.join(filtros_txt)).font = Font(italic=True, color='666666')
    ws.cell(row=4, column=1, value='Gerado em {}'.format(datetime.now().strftime('%d/%m/%Y %H:%M:%S'))).font = Font(italic=True, color='666666')

    cab = ['Linha', 'Descrição', 'Evento', 'SL', 'Débito', 'SL', 'Crédito']
    linha_excel = 6
    agrup_atual = None
    item_atual = None
    for l in linhas:
        if l['agrup'] != agrup_atual:
            agrup_atual = l['agrup']
            item_atual = None
            if linha_excel > 6:
                linha_excel += 1
            ws.cell(row=linha_excel, column=1, value='Agrupamento {}'.format(agrup_atual))
            ws.merge_cells(start_row=linha_excel, start_column=1, end_row=linha_excel, end_column=len(cab))
            for c in range(1, len(cab) + 1):
                ws.cell(row=linha_excel, column=c).fill = azul
                ws.cell(row=linha_excel, column=c).font = branco_negrito
            linha_excel += 1
        if l['id_item'] != item_atual:
            item_atual = l['id_item']
            ws.cell(row=linha_excel, column=1, value='{} - {}'.format(l['id_item'], l['dsc_item']))
            ws.merge_cells(start_row=linha_excel, start_column=1, end_row=linha_excel, end_column=2)
            for c, nome in enumerate(cab[2:], start=3):
                ws.cell(row=linha_excel, column=c, value=nome).alignment = centro
            for c in range(1, len(cab) + 1):
                cel = ws.cell(row=linha_excel, column=c)
                cel.fill = azul_claro
                cel.font = negrito
                cel.border = borda
            linha_excel += 1
        valores = [_num_excel(l['id_linha']), l['dsc_linha'], l['evento'], _num_excel(l['sl_deb']),
                   float(l['vr_deb']) if l['vr_deb'] is not None else None,
                   _num_excel(l['sl_cred']),
                   float(l['vr_cred']) if l['vr_cred'] is not None else None]
        for c, v in enumerate(valores, start=1):
            cel = ws.cell(row=linha_excel, column=c, value=v)
            cel.border = borda
            if c in (5, 7):
                cel.number_format = formato
            if c in (1, 3, 4, 6):
                cel.alignment = centro
        linha_excel += 1

    if not linhas:
        ws.cell(row=6, column=1, value='Nenhum dado para os filtros escolhidos.')

    for col, largura in zip('ABCDEFG', (8, 46, 12, 6, 18, 6, 18)):
        ws.column_dimensions[col].width = largura

    # ---------------- Aba 2: Dados (tabela plana com autofiltro) ----------------
    wd = wb.create_sheet('Dados')
    cab_dados = ['DT_REFERENCIA', 'CD_AGRUPAMENTO', 'ID_ITEM_CONTABIL', 'DSC_ITEM_CONTABIL',
                 'ID_LINHA_CONTABIL', 'DSC_LINHA_CONTABIL', 'COD_EVENTO',
                 'SL_DEB', 'VR_DEBITO', 'SL_CRED', 'VR_CREDITO']
    for c, nome in enumerate(cab_dados, start=1):
        cel = wd.cell(row=1, column=c, value=nome)
        cel.fill = azul
        cel.font = branco_negrito
        cel.alignment = centro
    for n, l in enumerate(linhas, start=2):
        valores = [l['dt'], _num_excel(l['agrup']), _num_excel(l['id_item']), l['dsc_item'],
                   _num_excel(l['id_linha']), l['dsc_linha'], l['evento'], _num_excel(l['sl_deb']),
                   float(l['vr_deb']) if l['vr_deb'] is not None else None,
                   _num_excel(l['sl_cred']),
                   float(l['vr_cred']) if l['vr_cred'] is not None else None]
        for c, v in enumerate(valores, start=1):
            cel = wd.cell(row=n, column=c, value=v)
            if c == 1 and v:
                cel.number_format = 'DD/MM/YYYY'
            if c in (9, 11):
                cel.number_format = formato
    if linhas:
        wd.auto_filter.ref = 'A1:{}{}'.format(get_column_letter(len(cab_dados)), len(linhas) + 1)
    wd.freeze_panes = 'A2'
    for c, largura in enumerate((13, 16, 16, 40, 16, 40, 12, 8, 18, 8, 18), start=1):
        wd.column_dimensions[get_column_letter(c)].width = largura

    saida = BytesIO()
    wb.save(saida)
    saida.seek(0)

    nome = '{}_{}.xlsx'.format(cfg['arquivo'], dt_escolhida.strftime('%Y%m%d') if dt_escolhida else 'sem_data')
    registrar_log(
        acao='exportar',
        entidade='contas_contabeis_sisgea',
        entidade_id=dt_escolhida.isoformat() if dt_escolhida else '',
        descricao='Exportação {} ({} linha(s); {})'.format(cfg['titulo'], len(linhas), ' | '.join(filtros_txt)),
    )
    return send_file(
        saida,
        as_attachment=True,
        download_name=nome,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )



# =========================================================================
# AUDITORIA (COR_VW009 / COR_VW010)
# =========================================================================
#
# Cada relatório é descrito por uma configuração (view, colunas e filtros).
# A mesma rota e o mesmo template atendem os dois.
#   tipo da coluna: 'data' | 'inteiro' | 'texto' | 'bit' | 'valor' (R$, 2 casas)
#   colunas_valor: colunas somadas no resumo (total geral e por agrupamento)
#   filtro:
#     'multi'  -> caixas de seleção com os valores que existem na view
#     'faixa'  -> data de / até
#     'contem' -> texto digitado (busca parcial)

AUDITORIAS = {
    'sd-oper-zero': {
        'view': '[BDG].[COR_VW009_FCO_AUDITORIA_SD_OPER_ZERO]',
        'titulo': 'SD OPER ZERO',
        'subtitulo': 'Contratos com saldo devedor operacional zerado',
        'icone': 'fa-equals',
        'cor': '#e74a3b',
        'arquivo': 'Auditoria_SD_Oper_Zero',
        'colunas': [
            ('DT_REFERENCIA', 'Data de referência', 'data'),
            ('fkContratoSISCTR', 'Contrato SISCTR', 'inteiro'),
            ('COD_AGRUPAMENTO', 'Cód. agrupamento', 'inteiro'),
            ('NO_AGRUPAMENTO', 'Agrupamento', 'texto'),
            ('VR_FCO', 'Valor FCO', 'valor'),
        ],
        'colunas_valor': ['VR_FCO'],
        'filtros': [
            ('NO_AGRUPAMENTO', 'multi'),
            ('COD_AGRUPAMENTO', 'multi'),
            ('DT_REFERENCIA', 'multi'),
            ('fkContratoSISCTR', 'contem'),
        ],
        'ordem_padrao': ('NO_AGRUPAMENTO', 'fkContratoSISCTR'),
    },
    'baixa-nao-marcada': {
        'view': '[BDG].[COR_VW010_FCO_AUDITORIA_BAIXA_NAO_MARCADA_EVOLUCAO]',
        'titulo': 'Baixa Não Marcada (Evolução)',
        'subtitulo': 'Contratos com baixa a prejuízo não marcada na evolução',
        'icone': 'fa-flag',
        'cor': '#f6c23e',
        'arquivo': 'Auditoria_Baixa_Nao_Marcada_Evolucao',
        'colunas': [
            ('fkContratoSISCTR', 'Contrato SISCTR', 'inteiro'),
            ('BT_CTR_BAIXADO_PREJ', 'Baixado a prejuízo', 'bit'),
            ('DT_BAIXA_PREJUIZO', 'Data da baixa a prejuízo', 'data'),
            ('COD_AGRUPAMENTO', 'Cód. agrupamento', 'inteiro'),
            ('NO_AGRUPAMENTO', 'Agrupamento', 'texto'),
            ('VR_DIV_TOTAL', 'Dívida total', 'valor'),
            ('VR_FCO', 'Valor FCO', 'valor'),
        ],
        'colunas_valor': ['VR_DIV_TOTAL', 'VR_FCO'],
        'filtros': [
            ('NO_AGRUPAMENTO', 'multi'),
            ('COD_AGRUPAMENTO', 'multi'),
            ('BT_CTR_BAIXADO_PREJ', 'multi'),
            ('DT_BAIXA_PREJUIZO', 'faixa'),
            ('fkContratoSISCTR', 'contem'),
        ],
        'ordem_padrao': ('NO_AGRUPAMENTO', 'fkContratoSISCTR'),
    },
}

AUD_NULO = '__nulo__'                 # opção "(vazio)" nos filtros
AUD_TAMANHOS_PAGINA = (50, 100, 250, 500)


def _aud_cfg(relatorio):
    cfg = AUDITORIAS.get(relatorio)
    if not cfg:
        abort(404)
    return cfg


def _aud_tipo(cfg, coluna):
    for c, _rot, tipo in cfg['colunas']:
        if c == coluna:
            return tipo
    return 'texto'


def _aud_rotulo(cfg, coluna):
    for c, rot, _tipo in cfg['colunas']:
        if c == coluna:
            return rot
    return coluna


def _aud_valor_tela(valor, tipo):
    """Texto exibido na tela/Excel para um valor da view."""
    if valor is None:
        return '(vazio)'
    if tipo == 'data':
        d = _para_date(valor)
        return d.strftime('%d/%m/%Y') if d else _txt(valor)
    if tipo == 'bit':
        return 'Sim' if bool(valor) else 'Não'
    if tipo == 'valor':
        return _fmt_br(valor, 2)
    return _txt(valor)


def _aud_valor_chave(valor, tipo):
    """Valor usado no filtro (querystring) para uma opção."""
    if valor is None:
        return AUD_NULO
    if tipo == 'data':
        d = _para_date(valor)
        return d.isoformat() if d else _txt(valor)
    if tipo == 'bit':
        return '1' if bool(valor) else '0'
    return _txt(valor)


def _aud_converter(texto, tipo):
    """Converte o valor vindo da tela para o tipo da coluna (None = inválido)."""
    if tipo == 'data':
        return _para_date(texto)
    if tipo in ('inteiro', 'bit'):
        try:
            return int(str(texto).strip())
        except ValueError:
            return None
    return str(texto)


def _aud_ler_filtros(cfg, args):
    """Lê os filtros da querystring já validados."""
    filtros = {}
    for coluna, tipo_filtro in cfg['filtros']:
        tipo = _aud_tipo(cfg, coluna)
        if tipo_filtro == 'multi':
            valores = []
            for v in args.getlist(coluna):
                v = (v or '').strip()
                if v and v not in valores:
                    valores.append(v)
            filtros[coluna] = valores
        elif tipo_filtro == 'faixa':
            de = _para_date(args.get(coluna + '_de')) if args.get(coluna + '_de') else None
            ate = _para_date(args.get(coluna + '_ate')) if args.get(coluna + '_ate') else None
            filtros[coluna] = {'de': de, 'ate': ate}
        else:
            filtros[coluna] = (args.get(coluna) or '').strip()[:30]
    return filtros


def _aud_where(cfg, filtros):
    """Monta o WHERE parametrizado a partir dos filtros."""
    partes = []
    params = {}
    n = 0
    for coluna, tipo_filtro in cfg['filtros']:
        tipo = _aud_tipo(cfg, coluna)
        valor = filtros.get(coluna)
        if tipo_filtro == 'multi' and valor:
            condicoes = []
            nomes = []
            for v in valor:
                if v == AUD_NULO:
                    condicoes.append('[{}] IS NULL'.format(coluna))
                    continue
                convertido = _aud_converter(v, tipo)
                if convertido is None:
                    continue
                nome = 'f{}'.format(n)
                n += 1
                params[nome] = convertido
                nomes.append(':' + nome)
            if nomes:
                if tipo == 'texto':
                    condicoes.append('LTRIM(RTRIM([{}])) IN ({})'.format(coluna, ', '.join(nomes)))
                else:
                    condicoes.append('[{}] IN ({})'.format(coluna, ', '.join(nomes)))
            if condicoes:
                partes.append('(' + ' OR '.join(condicoes) + ')')
        elif tipo_filtro == 'faixa' and valor:
            if valor.get('de'):
                params['f{}'.format(n)] = valor['de']
                partes.append('[{}] >= :f{}'.format(coluna, n))
                n += 1
            if valor.get('ate'):
                params['f{}'.format(n)] = valor['ate']
                partes.append('[{}] <= :f{}'.format(coluna, n))
                n += 1
        elif tipo_filtro == 'contem' and valor:
            texto = valor.replace('[', '[[]').replace('%', '[%]').replace('_', '[_]')
            params['f{}'.format(n)] = '%' + texto + '%'
            partes.append('CAST([{}] AS varchar(40)) LIKE :f{}'.format(coluna, n))
            n += 1
    return ('WHERE ' + ' AND '.join(partes)) if partes else '', params


def _aud_opcoes(cfg):
    """Valores existentes na view para cada filtro de caixas de seleção (com quantidade)."""
    multis = [c for c, t in cfg['filtros'] if t == 'multi']
    if not multis:
        return {}
    cols = ', '.join('[{}]'.format(c) for c in multis)
    sql = text('SELECT {cols}, COUNT(1) AS QTD FROM {view} GROUP BY {cols}'.format(cols=cols, view=cfg['view']))
    contagem = {c: {} for c in multis}
    for r in db.session.execute(sql).mappings().all():
        for c in multis:
            tipo = _aud_tipo(cfg, c)
            chave = _aud_valor_chave(r[c], tipo)
            atual = contagem[c].get(chave)
            if not atual:
                contagem[c][chave] = {'valor': chave, 'rotulo': _aud_valor_tela(r[c], tipo),
                                      'qtd': 0, 'ordem': r[c]}
            contagem[c][chave]['qtd'] += int(r['QTD'] or 0)

    opcoes = {}
    for c in multis:
        tipo = _aud_tipo(cfg, c)
        lista = list(contagem[c].values())
        if tipo == 'data':
            # datas mais recentes primeiro; (vazio) no fim
            lista.sort(key=lambda o: (o['ordem'] is None,
                                      -(_para_date(o['ordem']).toordinal() if _para_date(o['ordem']) else 0)))
        else:
            lista.sort(key=lambda o: (o['ordem'] is None, _chave_ordem(o['ordem'])))
        opcoes[c] = [{'valor': o['valor'], 'rotulo': o['rotulo'], 'qtd': o['qtd']} for o in lista]
    return opcoes


def _aud_resumo(cfg, where, params):
    """Quantidade (e soma das colunas_valor, se houver) por agrupamento, já com os filtros."""
    cols_valor = cfg.get('colunas_valor') or []
    soma = ''.join(', SUM([{c}]) AS [SOMA_{c}]'.format(c=c) for c in cols_valor)
    sql = text("""
        SELECT [COD_AGRUPAMENTO], [NO_AGRUPAMENTO], COUNT(1) AS QTD{soma}
        FROM {view} {where}
        GROUP BY [COD_AGRUPAMENTO], [NO_AGRUPAMENTO]
    """.format(soma=soma, view=cfg['view'], where=where))
    resumo = [{'cod': _txt(r['COD_AGRUPAMENTO']), 'nome': _txt(r['NO_AGRUPAMENTO']),
               'chave_nome': _aud_valor_chave(r['NO_AGRUPAMENTO'], 'texto'), 'qtd': int(r['QTD'] or 0),
               'valores': [{'rotulo': _aud_rotulo(cfg, c), 'valor': _dec(r['SOMA_' + c]) or Decimal('0')}
                           for c in cols_valor]}
              for r in db.session.execute(sql, params).mappings().all()]
    resumo.sort(key=lambda x: (-x['qtd'], x['nome']))
    return resumo


def _aud_ordem(cfg, args):
    colunas = [c for c, _r, _t in cfg['colunas']]
    col = args.get('ordem')
    direcao = 'desc' if args.get('dir') == 'desc' else 'asc'
    if col not in colunas:
        col = None
    if col:
        extra = [c for c in cfg['ordem_padrao'] if c != col]
        partes = ['[{}] {}'.format(col, direcao.upper())] + ['[{}]'.format(c) for c in extra]
    else:
        partes = ['[{}]'.format(c) for c in cfg['ordem_padrao']]
    return col, direcao, ', '.join(partes)


def _aud_linhas(cfg, where, params, order_by, offset=None, limite=None):
    cols = ', '.join('[{}]'.format(c) for c, _r, _t in cfg['colunas'])
    sql = 'SELECT {cols} FROM {view} {where} ORDER BY {ordem}'.format(
        cols=cols, view=cfg['view'], where=where, ordem=order_by)
    p = dict(params)
    if limite is not None:
        sql += ' OFFSET :_off ROWS FETCH NEXT :_lim ROWS ONLY'
        p['_off'] = offset or 0
        p['_lim'] = limite
    return db.session.execute(text(sql), p).mappings().all()


@contas_contabeis_bp.route('/auditoria')
@login_required
@sistema_requerido(SISTEMA)
def auditoria():
    """Página com os relatórios de auditoria do FCO."""
    relatorios = [{'chave': k, 'titulo': v['titulo'], 'subtitulo': v['subtitulo'],
                   'icone': v['icone'], 'cor': v['cor']} for k, v in AUDITORIAS.items()]
    return render_template('contas_contabeis/auditoria.html', relatorios=relatorios)


@contas_contabeis_bp.route('/auditoria/<relatorio>')
@login_required
@sistema_requerido(SISTEMA)
def auditoria_relatorio(relatorio):
    cfg = _aud_cfg(relatorio)
    filtros = _aud_ler_filtros(cfg, request.args)
    where, params = _aud_where(cfg, filtros)
    ordem_col, ordem_dir, order_by = _aud_ordem(cfg, request.args)

    try:
        tam_pagina = int(request.args.get('tam', 100))
    except ValueError:
        tam_pagina = 100
    if tam_pagina not in AUD_TAMANHOS_PAGINA:
        tam_pagina = 100
    try:
        pagina = max(1, int(request.args.get('pag', 1)))
    except ValueError:
        pagina = 1

    erro = None
    opcoes, resumo, linhas, total = {}, [], [], 0
    totais_valor = []
    try:
        opcoes = _aud_opcoes(cfg)
        resumo = _aud_resumo(cfg, where, params)
        total = sum(r['qtd'] for r in resumo)
        # Total geral de cada coluna de valor (soma dos agrupamentos)
        for i, c in enumerate(cfg.get('colunas_valor') or []):
            totais_valor.append({'rotulo': _aud_rotulo(cfg, c),
                                 'valor': sum((r['valores'][i]['valor'] for r in resumo), Decimal('0'))})
        total_paginas = max(1, (total + tam_pagina - 1) // tam_pagina)
        pagina = min(pagina, total_paginas)
        registros = _aud_linhas(cfg, where, params, order_by, (pagina - 1) * tam_pagina, tam_pagina)
        for r in registros:
            linhas.append([{'texto': _aud_valor_tela(r[c], t), 'tipo': t, 'nulo': r[c] is None,
                            'negativo': t == 'valor' and r[c] is not None and _dec(r[c]) < 0}
                           for c, _rot, t in cfg['colunas']])
    except Exception as e:
        db.session.rollback()
        erro = 'Erro ao consultar a view: {}'.format(e)
    total_paginas = max(1, (total + tam_pagina - 1) // tam_pagina)

    # Descrição dos filtros para os campos da tela
    filtros_tela = []
    for coluna, tipo_filtro in cfg['filtros']:
        filtros_tela.append({
            'coluna': coluna, 'rotulo': _aud_rotulo(cfg, coluna), 'tipo_filtro': tipo_filtro,
            'opcoes': opcoes.get(coluna, []), 'valor': filtros.get(coluna),
        })
    filtros_ativos = sum(1 for f in filtros_tela if (
        (f['tipo_filtro'] == 'multi' and f['valor']) or
        (f['tipo_filtro'] == 'faixa' and (f['valor']['de'] or f['valor']['ate'])) or
        (f['tipo_filtro'] == 'contem' and f['valor'])))

    # Querystring atual (sem paginação/ordem) para montar links
    base_args = []
    for k in request.args.keys():
        if k in ('pag', 'ordem', 'dir'):
            continue
        for v in request.args.getlist(k):
            base_args.append((k, v))

    return render_template(
        'contas_contabeis/auditoria_relatorio.html',
        relatorio=relatorio, cfg=cfg, colunas=cfg['colunas'],
        filtros=filtros_tela, filtros_ativos=filtros_ativos,
        resumo=resumo, linhas=linhas, total=total,
        totais_valor=totais_valor,
        fmt=_fmt_br,
        pagina=pagina, total_paginas=total_paginas, tam_pagina=tam_pagina,
        tamanhos=AUD_TAMANHOS_PAGINA, ordem_col=ordem_col, ordem_dir=ordem_dir,
        base_args=base_args, outros=[(k, v['titulo']) for k, v in AUDITORIAS.items() if k != relatorio],
        nulo=AUD_NULO, erro=erro,
    )


@contas_contabeis_bp.route('/auditoria/<relatorio>/exportar')
@login_required
@sistema_requerido(SISTEMA)
def auditoria_exportar(relatorio):
    """Excel com todas as linhas dos filtros da tela (aba Dados + aba Resumo por agrupamento)."""
    cfg = _aud_cfg(relatorio)
    filtros = _aud_ler_filtros(cfg, request.args)
    where, params = _aud_where(cfg, filtros)
    _c, _d, order_by = _aud_ordem(cfg, request.args)

    registros = _aud_linhas(cfg, where, params, order_by)
    resumo = _aud_resumo(cfg, where, params)

    azul = PatternFill('solid', fgColor='224ABE')
    cinza = PatternFill('solid', fgColor='F2F2F2')
    branco_negrito = Font(bold=True, color='FFFFFF')
    negrito = Font(bold=True)
    centro = Alignment(horizontal='center', vertical='center')

    # Texto dos filtros usados
    desc_filtros = []
    for coluna, tipo_filtro in cfg['filtros']:
        tipo = _aud_tipo(cfg, coluna)
        v = filtros.get(coluna)
        rot = _aud_rotulo(cfg, coluna)
        if tipo_filtro == 'multi' and v:
            textos = ['(vazio)' if x == AUD_NULO else _aud_valor_tela(_aud_converter(x, tipo), tipo) for x in v]
            desc_filtros.append('{}: {}'.format(rot, ', '.join(textos)))
        elif tipo_filtro == 'faixa' and v and (v['de'] or v['ate']):
            desc_filtros.append('{}: {} a {}'.format(
                rot, v['de'].strftime('%d/%m/%Y') if v['de'] else '...',
                v['ate'].strftime('%d/%m/%Y') if v['ate'] else '...'))
        elif tipo_filtro == 'contem' and v:
            desc_filtros.append('{} contém "{}"'.format(rot, v))
    texto_filtros = ' | '.join(desc_filtros) if desc_filtros else 'Sem filtros'

    wb = Workbook()
    ws = wb.active
    ws.title = 'Dados'
    ws.cell(row=1, column=1, value='Auditoria FCO - {}'.format(cfg['titulo'])).font = Font(bold=True, size=13)
    ws.cell(row=2, column=1, value=texto_filtros).font = Font(italic=True, color='666666')
    ws.cell(row=3, column=1, value='Gerado em {} · {} registro(s)'.format(
        datetime.now().strftime('%d/%m/%Y %H:%M:%S'), len(registros))).font = Font(italic=True, color='666666')

    linha_cab = 5
    for i, (c, rot, _t) in enumerate(cfg['colunas'], start=1):
        cel = ws.cell(row=linha_cab, column=i, value=rot)
        cel.fill = azul
        cel.font = branco_negrito
        cel.alignment = centro
    for n, r in enumerate(registros, start=linha_cab + 1):
        for i, (c, _rot, t) in enumerate(cfg['colunas'], start=1):
            v = r[c]
            if t == 'data':
                v = _para_date(v)
            elif t == 'bit':
                v = None if v is None else ('Sim' if bool(v) else 'Não')
            elif t == 'inteiro' and v is not None:
                v = int(v)
            elif t == 'valor' and v is not None:
                v = float(v)
            cel = ws.cell(row=n, column=i, value=v)
            if t == 'valor':
                cel.number_format = '#,##0.00;[Red]-#,##0.00'
            if t == 'data' and v:
                cel.number_format = 'DD/MM/YYYY'
            if t in ('data', 'bit', 'inteiro'):
                cel.alignment = Alignment(horizontal='center')
    ultima = linha_cab + len(registros)
    ws.auto_filter.ref = 'A{}:{}{}'.format(linha_cab, get_column_letter(len(cfg['colunas'])), max(ultima, linha_cab))
    ws.freeze_panes = 'A{}'.format(linha_cab + 1)
    for i in range(1, len(cfg['colunas']) + 1):
        ws.column_dimensions[get_column_letter(i)].width = 24

    # Linha de total da coluna de valor no fim da aba Dados
    cols_valor = cfg.get('colunas_valor') or []
    if cols_valor and registros:
        nomes_colunas = [c for c, _r, _t in cfg['colunas']]
        lin_tot = ultima + 1
        ws.cell(row=lin_tot, column=1, value='TOTAL').font = negrito
        for c in cols_valor:
            cel = ws.cell(row=lin_tot, column=nomes_colunas.index(c) + 1,
                          value=float(sum((_dec(r[c]) or Decimal('0')) for r in registros)))
            cel.number_format = '#,##0.00;[Red]-#,##0.00'
            cel.font = negrito
        for i in range(1, len(cfg['colunas']) + 1):
            ws.cell(row=lin_tot, column=i).fill = cinza

    wr = wb.create_sheet('Resumo por agrupamento')
    cab_resumo = ['Cód. agrupamento', 'Agrupamento', 'Quantidade']
    cab_resumo += [_aud_rotulo(cfg, c) for c in cols_valor]
    for i, nome in enumerate(cab_resumo, start=1):
        cel = wr.cell(row=1, column=i, value=nome)
        cel.fill = azul
        cel.font = branco_negrito
        cel.alignment = centro
    for n, r in enumerate(resumo, start=2):
        wr.cell(row=n, column=1, value=_num_excel(r['cod']))
        wr.cell(row=n, column=2, value=r['nome'])
        wr.cell(row=n, column=3, value=r['qtd'])
        for i, v in enumerate(r['valores'], start=4):
            wr.cell(row=n, column=i, value=float(v['valor'])).number_format = '#,##0.00;[Red]-#,##0.00'
    tot = len(resumo) + 2
    wr.cell(row=tot, column=1, value='TOTAL')
    wr.cell(row=tot, column=3, value=sum(r['qtd'] for r in resumo))
    for i in range(len(cols_valor)):
        wr.cell(row=tot, column=4 + i, value=float(sum((r['valores'][i]['valor'] for r in resumo), Decimal('0')))).number_format = '#,##0.00;[Red]-#,##0.00'
    for i in range(1, len(cab_resumo) + 1):
        wr.cell(row=tot, column=i).fill = cinza
        wr.cell(row=tot, column=i).font = negrito
    for col, larg in zip('ABCDEF', (18, 30, 14, 20, 20, 20)):
        wr.column_dimensions[col].width = larg

    saida = BytesIO()
    wb.save(saida)
    saida.seek(0)
    registrar_log(
        acao='exportar',
        entidade='contas_contabeis_auditoria',
        entidade_id=relatorio,
        descricao='Exportação auditoria {} ({} registro(s); {})'.format(cfg['titulo'], len(registros), texto_filtros),
    )
    return send_file(
        saida,
        as_attachment=True,
        download_name='{}_{}.xlsx'.format(cfg['arquivo'], datetime.now().strftime('%Y%m%d_%H%M')),
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )
