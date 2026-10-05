# app/utils/analise_financeira_pf_calculo.py
"""
Motor de cálculo do Fluxo de Caixa Simulado - Análise Financeira PF.

Reproduz a aba "Fluxo 1" da planilha da SUFIN, célula a célula:

  J  = PZ_EXECUCAO_MESES                       (mês da consolidação; Excel J7)
  L  = J + PZ_PERMANENCIA_ESTOQUE_MESES         (mês da venda;        Excel L7)
  VA = VR_LAUDO_AVALIACAO do contrato           (Excel O13)

  Para cada NU_MES n = 0 .. L  (Excel colunas A..I):
    ANO_MES             = data de referência + n meses (1º dia do mês)
    VR_DESP_MANUT       (col. C)
        n = J + 12      -> -VA * PC_DESP_MANUT_INU_PRI_ANO      (L4)
        n = J + 24      ->  0  (Memoria C42 = 0%, sem coluna na tabela) (L5)
        n = J + 36      ->  0  (Memoria C42 = 0%, sem coluna na tabela) (L6)
        n = L           -> -VA * PC_DESP_MANUT_INU_6_MESES       (L7)
        (a ordem dos testes é a mesma do SE() aninhado do Excel)
    VR_CUSTO_MANUT      (col. D)
        n <= J          -> -VR_CUSTO_CIPF
        n >  J          -> -(VR_TARIFA_ADM_IMOVEIS + VR_CUSTO_INU)
    VR_DESP_CONSOL_PROP (col. E)  n = J -> -VR_DESP_MEDIA_EXEC_JUD
    VR_DEB_PROPTERREM   (col. F)  n = J -> -VR_DEBITOS_PROPTERREM do contrato
    VR_VENDA            (col. G)  n = L -> VA * (1 - PC_DESP_DESCONTO_VENDA)
    VR_TOTAL            (col. H)  soma de C..G
    VR_PRESENTE         (col. I)  VR_TOTAL / (1 + taxa) ^ (n / 12)
        taxa = CUSTO_MEDIO / 100 da view FIN_VW036 no ANO_MES da linha;
        mês sem taxa na view repete a taxa do mês anterior (IFERROR do Excel).

  VPL do contrato = soma de VR_PRESENTE (Excel I7).

Valores nominais em Decimal; o desconto (expoente fracionário) em float,
como o Excel. Cada valor gravado é arredondado em 2 casas (ROUND_HALF_UP).

Compatível com Python 3.9 e 3.12.
"""
from app import db
from sqlalchemy import text
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from io import BytesIO
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter


TB_PARAMETROS = '[BDG].[FIN_TB034_ANALISE_FINANCEIRA_PF_PARAMETROS]'
TB_CONTRATOS = '[BDG].[FIN_TB035_ANALISE_FINANCEIRA_PF_CONTRATOS]'
TB_FLUXOS = '[BDG].[FIN_TB036_ANALISE_FINANCEIRA_PF_FLUXOS]'
VW_CUSTO_MEDIO = '[BDG].[FIN_VW036_ANALISE_FINANCEIRA_PF_CUSTO_MEDIO]'

CENTAVO = Decimal('0.01')

MESES_ABREV = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun',
               'jul', 'ago', 'set', 'out', 'nov', 'dez']

# Colunas de valor do fluxo, na ordem da planilha (C..I)
COLUNAS_FLUXO = [
    ('VR_DESP_MANUT', 'Despesas de Manutenção'),
    ('VR_CUSTO_MANUT', 'Custo Manutenção'),
    ('VR_DESP_CONSOL_PROP', 'Despesas de Consolidação de Propriedade'),
    ('VR_DEB_PROPTERREM', 'Débitos Propter Rem'),
    ('VR_VENDA', 'Venda Imóvel Executado'),
    ('VR_TOTAL', 'TOTAL'),
    ('VR_PRESENTE', 'Valor Presente'),
]


class ErroCalculo(Exception):
    """Erro de regra de negócio no cálculo (mensagem vai para a tela)."""
    pass


# =========================================================================
# UTILITÁRIOS
# =========================================================================
def _dec(valor):
    if valor is None:
        return Decimal('0')
    return Decimal(str(valor))


def _r2(valor):
    return _dec(valor).quantize(CENTAVO, rounding=ROUND_HALF_UP)


def _para_date(valor):
    if valor is None:
        return None
    if isinstance(valor, datetime):
        return valor.date()
    return valor


def somar_meses(data_base, meses):
    """Equivalente ao EDATE do Excel, sempre no 1º dia do mês."""
    total = data_base.year * 12 + (data_base.month - 1) + meses
    return date(total // 12, total % 12 + 1, 1)


def chave_ano_mes(data_ref):
    """date(2026, 9, 1) -> 202609"""
    return data_ref.year * 100 + data_ref.month


def mes_ano_texto(data_ref):
    """date(2026, 9, 1) -> 'set/2026' (mesmo formato da planilha)."""
    if not data_ref:
        return ''
    return f'{MESES_ABREV[data_ref.month - 1]}/{data_ref.year}'


def fmt_br(valor, casas=2):
    if valor is None:
        return ''
    fmt = '{:,.' + str(casas) + 'f}'
    return fmt.format(float(valor)).replace(',', 'X').replace('.', ',').replace('X', '.')


def formatar_contrato(nu_contrato):
    """101600101095 -> 1-0160-0101-095"""
    if nu_contrato is None:
        return ''
    txt = str(nu_contrato).strip()
    if txt.endswith('.0'):
        txt = txt[:-2]
    if txt.isdigit() and len(txt) <= 12:
        t = txt.zfill(12)
        return f'{t[0]}-{t[1:5]}-{t[5:9]}-{t[9:12]}'
    return txt


def contrato_texto(nu_contrato):
    """Decimal('101600101095') -> '101600101095'"""
    txt = str(nu_contrato).strip()
    return txt[:-2] if txt.endswith('.0') else txt


# =========================================================================
# LEITURA DAS FONTES
# =========================================================================
def obter_parametros_na_data(data_ref):
    """
    Linha de FIN_TB034 em vigor na data informada:
      DT_INI_VIGENCIA <= data  E  (DT_FIM_VIGENCIA IS NULL OU DT_FIM_VIGENCIA >= data)
    Assim, no dia de uma edição ainda vale a versão antiga (que termina hoje);
    a nova só entra no dia seguinte, como definido na regra de vigência.
    """
    row = db.session.execute(text(f"""
        SELECT TOP 1
               [DT_INI_VIGENCIA], [DT_FIM_VIGENCIA],
               [VR_CUSTO_CIPF], [VR_CUSTO_INU], [VR_TARIFA_ADM_IMOVEIS],
               [PC_DESP_MANUT_INU_PRI_ANO], [PC_DESP_MANUT_INU_6_MESES],
               [PC_DESP_DESCONTO_VENDA], [VR_DESP_MEDIA_EXEC_JUD],
               [PZ_EXECUCAO_ANOS], [PZ_PERMANENCIA_ESTOQUE_MESES],
               [PZ_EXECUCAO_MESES]
        FROM {TB_PARAMETROS}
        WHERE [DT_INI_VIGENCIA] <= :data_ref
          AND ([DT_FIM_VIGENCIA] IS NULL OR [DT_FIM_VIGENCIA] >= :data_ref)
        ORDER BY [DT_INI_VIGENCIA] DESC
    """), {'data_ref': data_ref}).mappings().first()
    return dict(row) if row else None


def obter_taxas_custo_medio():
    """
    Lê a view FIN_VW036 e devolve {AAAAMM (int): CUSTO_MEDIO (Decimal, % a.a.)}.
    Aceita ANO_MES vindo como número, texto ou data.
    """
    rows = db.session.execute(text(f"""
        SELECT [ANO_MES], [CUSTO_MEDIO]
        FROM {VW_CUSTO_MEDIO}
    """)).fetchall()

    taxas = {}
    for ano_mes, custo in rows:
        if ano_mes is None or custo is None:
            continue
        if isinstance(ano_mes, (date, datetime)):
            chave = ano_mes.year * 100 + ano_mes.month
        else:
            txt = str(ano_mes).strip().replace('-', '').replace('/', '')
            if txt.endswith('.0'):
                txt = txt[:-2]
            if not txt.isdigit() or len(txt) < 6:
                continue
            chave = int(txt[:6])
        taxas[chave] = _dec(custo)
    return taxas


def listar_contratos_cadastrados():
    rows = db.session.execute(text(f"""
        SELECT [NU_CONTRATO], [NO_MUTUARIO],
               [VR_DEBITOS_PROPTERREM], [VR_LAUDO_AVALIACAO]
        FROM {TB_CONTRATOS}
        ORDER BY [NO_MUTUARIO], [NU_CONTRATO]
    """)).mappings().all()
    return [dict(r) for r in rows]


def obter_contratos(lista_nu):
    """Busca os contratos escolhidos na FIN_TB035 (lista de int)."""
    if not lista_nu:
        return []
    params = {}
    marcadores = []
    for i, nu in enumerate(lista_nu):
        params[f'nu{i}'] = nu
        marcadores.append(f':nu{i}')
    rows = db.session.execute(text(f"""
        SELECT [NU_CONTRATO], [NO_MUTUARIO],
               [VR_DEBITOS_PROPTERREM], [VR_LAUDO_AVALIACAO]
        FROM {TB_CONTRATOS}
        WHERE [NU_CONTRATO] IN ({', '.join(marcadores)})
        ORDER BY [NO_MUTUARIO], [NU_CONTRATO]
    """), params).mappings().all()
    return [dict(r) for r in rows]


# =========================================================================
# CÁLCULO (função pura: não acessa banco)
# =========================================================================
def prazos_do_fluxo(parametros):
    """Retorna (J, L): mês da consolidação e mês da venda."""
    meses_exec = parametros.get('PZ_EXECUCAO_MESES')
    if meses_exec is None:
        meses_exec = int(parametros['PZ_EXECUCAO_ANOS']) * 12
    j = int(meses_exec)
    l = j + int(parametros['PZ_PERMANENCIA_ESTOQUE_MESES'])
    return j, l


def calcular_fluxo(contrato, parametros, data_referencia, taxas):
    """
    Monta o fluxo mês a mês de UM contrato.

    contrato:        dict com VR_DEBITOS_PROPTERREM e VR_LAUDO_AVALIACAO
    parametros:      dict da FIN_TB034
    data_referencia: date (1º dia do mês) = mês 0
    taxas:           {AAAAMM: CUSTO_MEDIO em % a.a.}

    Retorna (linhas, vpl). Cada linha já vem arredondada em 2 casas e com a
    taxa usada (TAXA_AA, em % a.a.) para exibição/exportação.
    """
    j, l = prazos_do_fluxo(parametros)

    va = _dec(contrato.get('VR_LAUDO_AVALIACAO'))
    debitos = _dec(contrato.get('VR_DEBITOS_PROPTERREM'))

    custo_cipf = _dec(parametros['VR_CUSTO_CIPF'])
    custo_inu = _dec(parametros['VR_CUSTO_INU'])
    tarifa_adm = _dec(parametros['VR_TARIFA_ADM_IMOVEIS'])
    pc_pri_ano = _dec(parametros['PC_DESP_MANUT_INU_PRI_ANO'])
    pc_6_meses = _dec(parametros['PC_DESP_MANUT_INU_6_MESES'])
    pc_desconto = _dec(parametros['PC_DESP_DESCONTO_VENDA'])
    desp_exec = _dec(parametros['VR_DESP_MEDIA_EXEC_JUD'])

    # Marcos anuais de manutenção (Excel L4, L5, L6)
    marco_1 = j + 12
    marco_2 = j + 24
    marco_3 = j + 36

    linhas = []
    vpl = Decimal('0')
    taxa_anterior = None

    for n in range(0, l + 1):
        mes = somar_meses(data_referencia, n)
        taxa = taxas.get(chave_ano_mes(mes))
        if taxa is None:
            if taxa_anterior is None:
                raise ErroCalculo(
                    f'Não há custo médio na FIN_VW036 para {mes_ano_texto(mes)} '
                    f'(mês 0 da data de referência).'
                )
            taxa = taxa_anterior
        taxa_anterior = taxa

        # Col. C - Despesas de manutenção (% do VA)
        if n == marco_1:
            desp_manut = -(va * pc_pri_ano)
        elif n == marco_2 or n == marco_3:
            desp_manut = Decimal('0')
        elif n == l:
            desp_manut = -(va * pc_6_meses)
        else:
            desp_manut = Decimal('0')

        # Col. D - Custo de manutenção
        if n <= j:
            custo_manut = -custo_cipf
        else:
            custo_manut = -(tarifa_adm + custo_inu)

        # Col. E e F - Consolidação e propter rem no mês J
        desp_consol = -desp_exec if n == j else Decimal('0')
        deb_propter = -debitos if n == j else Decimal('0')

        # Col. G - Venda no mês L
        venda = va * (Decimal('1') - pc_desconto) if n == l else Decimal('0')

        # Col. H - Total
        total = desp_manut + custo_manut + desp_consol + deb_propter + venda

        # Col. I - Valor presente
        fator = (1.0 + float(taxa) / 100.0) ** (n / 12.0)
        presente = Decimal(repr(float(total) / fator))

        linha = {
            'NU_MES': n,
            'ANO_MES': mes,
            'TAXA_AA': taxa,
            'VR_DESP_MANUT': _r2(desp_manut),
            'VR_CUSTO_MANUT': _r2(custo_manut),
            'VR_DESP_CONSOL_PROP': _r2(desp_consol),
            'VR_DEB_PROPTERREM': _r2(deb_propter),
            'VR_VENDA': _r2(venda),
            'VR_TOTAL': _r2(total),
            'VR_PRESENTE': _r2(presente),
        }
        vpl += linha['VR_PRESENTE']
        linhas.append(linha)

    return linhas, vpl


# =========================================================================
# GRAVAÇÃO
# =========================================================================
def gravar_fluxo(dt_calculo, nu_contrato, linhas):
    """
    Regrava o fluxo do contrato na data de cálculo:
    apaga o que existir para (DT_CALCULO, NU_CONTRATO) e insere as linhas.
    """
    db.session.execute(text(f"""
        DELETE FROM {TB_FLUXOS}
        WHERE [DT_CALCULO] = :dt_calculo
          AND [NU_CONTRATO] = :nu
    """), {'dt_calculo': dt_calculo, 'nu': nu_contrato})

    registros = [{
        'dt_calculo': dt_calculo,
        'nu': nu_contrato,
        'nu_mes': ln['NU_MES'],
        'ano_mes': ln['ANO_MES'],
        'desp_manut': ln['VR_DESP_MANUT'],
        'custo_manut': ln['VR_CUSTO_MANUT'],
        'desp_consol': ln['VR_DESP_CONSOL_PROP'],
        'deb_propter': ln['VR_DEB_PROPTERREM'],
        'venda': ln['VR_VENDA'],
        'total': ln['VR_TOTAL'],
        'presente': ln['VR_PRESENTE'],
    } for ln in linhas]

    db.session.execute(text(f"""
        INSERT INTO {TB_FLUXOS}
            ([DT_CALCULO], [NU_CONTRATO], [NU_MES], [ANO_MES],
             [VR_DESP_MANUT], [VR_CUSTO_MANUT], [VR_DESP_CONSOL_PROP],
             [VR_DEB_PROPTERREM], [VR_VENDA], [VR_TOTAL], [VR_PRESENTE])
        VALUES
            (:dt_calculo, :nu, :nu_mes, :ano_mes,
             :desp_manut, :custo_manut, :desp_consol,
             :deb_propter, :venda, :total, :presente)
    """), registros)


# =========================================================================
# CONSULTA DOS RESULTADOS GRAVADOS
# =========================================================================
def listar_datas_calculo():
    rows = db.session.execute(text(f"""
        SELECT [DT_CALCULO], COUNT(DISTINCT [NU_CONTRATO]) AS QTD
        FROM {TB_FLUXOS}
        GROUP BY [DT_CALCULO]
        ORDER BY [DT_CALCULO] DESC
    """)).fetchall()
    return [{'dt_calculo': _para_date(r[0]), 'qtd': int(r[1])} for r in rows]


def listar_resumo_calculo(dt_calculo, nu_contrato=None):
    """
    Um registro por contrato: data de referência (ANO_MES do mês 0),
    meses do fluxo, VPL (soma de VR_PRESENTE) e dados da FIN_TB035.
    """
    filtro = ''
    params = {'dt_calculo': dt_calculo}
    if nu_contrato is not None:
        filtro = 'AND f.[NU_CONTRATO] = :nu'
        params['nu'] = nu_contrato

    rows = db.session.execute(text(f"""
        SELECT f.[NU_CONTRATO],
               MAX(c.[NO_MUTUARIO])          AS NO_MUTUARIO,
               MAX(c.[VR_LAUDO_AVALIACAO])   AS VR_LAUDO_AVALIACAO,
               -SUM(f.[VR_DEB_PROPTERREM])   AS VR_DEBITOS_PROPTERREM,
               MIN(f.[ANO_MES])              AS DT_REFERENCIA,
               MAX(f.[NU_MES])               AS ULTIMO_MES,
               SUM(f.[VR_PRESENTE])          AS VPL
        FROM {TB_FLUXOS} f
        LEFT JOIN {TB_CONTRATOS} c ON c.[NU_CONTRATO] = f.[NU_CONTRATO]
        WHERE f.[DT_CALCULO] = :dt_calculo
          {filtro}
        GROUP BY f.[NU_CONTRATO]
        ORDER BY MAX(c.[NO_MUTUARIO]), f.[NU_CONTRATO]
    """), params).mappings().all()

    resumo = []
    for r in rows:
        vpl = _dec(r['VPL'])
        nu = contrato_texto(r['NU_CONTRATO'])
        resumo.append({
            'nu_contrato': nu,
            'nu_contrato_fmt': formatar_contrato(nu),
            'no_mutuario': r['NO_MUTUARIO'] or '(contrato não está mais cadastrado)',
            'vr_laudo': r['VR_LAUDO_AVALIACAO'],
            'vr_debitos': r['VR_DEBITOS_PROPTERREM'],
            'dt_referencia': _para_date(r['DT_REFERENCIA']),
            'ultimo_mes': int(r['ULTIMO_MES']),
            'vpl': vpl,
            'positivo': vpl >= 0,
        })
    return resumo


def obter_fluxo_gravado(dt_calculo, nu_contrato, taxas=None):
    """Linhas gravadas do contrato + taxa da view (para exibição)."""
    rows = db.session.execute(text(f"""
        SELECT [NU_MES], [ANO_MES],
               [VR_DESP_MANUT], [VR_CUSTO_MANUT], [VR_DESP_CONSOL_PROP],
               [VR_DEB_PROPTERREM], [VR_VENDA], [VR_TOTAL], [VR_PRESENTE]
        FROM {TB_FLUXOS}
        WHERE [DT_CALCULO] = :dt_calculo
          AND [NU_CONTRATO] = :nu
        ORDER BY [NU_MES]
    """), {'dt_calculo': dt_calculo, 'nu': nu_contrato}).mappings().all()

    if taxas is None:
        taxas = obter_taxas_custo_medio()

    linhas = []
    taxa_anterior = None
    for r in rows:
        ano_mes = _para_date(r['ANO_MES'])
        taxa = taxas.get(chave_ano_mes(ano_mes)) if ano_mes else None
        if taxa is None:
            taxa = taxa_anterior
        taxa_anterior = taxa
        linha = dict(r)
        linha['ANO_MES'] = ano_mes
        linha['TAXA_AA'] = taxa
        linhas.append(linha)
    return linhas


# =========================================================================
# EXPORTAÇÃO EXCEL
# =========================================================================
_AZUL = '224ABE'
_CINZA = 'F8F9FC'
_FMT_VALOR = '#,##0.00;[Red]-#,##0.00'
_FMT_TAXA = '0.0000'


def _estilo_cabecalho(cel):
    cel.font = Font(bold=True, color='FFFFFF')
    cel.fill = PatternFill('solid', fgColor=_AZUL)
    cel.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
    cel.border = Border(bottom=Side(style='thin', color='B7C3E8'))


def _titulo(ws, linha, texto):
    cel = ws.cell(row=linha, column=1, value=texto)
    cel.font = Font(bold=True, size=13, color=_AZUL)


def _rotulo_valor(ws, linha, rotulo, valor, formato=None):
    c1 = ws.cell(row=linha, column=1, value=rotulo)
    c1.font = Font(bold=True)
    c2 = ws.cell(row=linha, column=2, value=valor)
    if formato:
        c2.number_format = formato
    return c2


def gerar_excel_calculo(dt_calculo, resumo, parametros, fluxos):
    """
    resumo:     lista de listar_resumo_calculo()
    parametros: dict da FIN_TB034 em vigor na DT_CALCULO (ou None)
    fluxos:     {nu_contrato (str): linhas de obter_fluxo_gravado()}
    Retorna BytesIO com o .xlsx.
    """
    wb = Workbook()

    # ---------------- Aba Resumo ----------------
    ws = wb.active
    ws.title = 'Resumo'
    _titulo(ws, 1, 'Análise Financeira PF - Resumo do cálculo')
    _rotulo_valor(ws, 2, 'Data do cálculo', dt_calculo, 'DD/MM/YYYY')

    cab = ['Contrato', 'Nome', 'Débitos Propter Rem', 'Laudo de Avaliação',
           'Data de Referência', 'Meses do fluxo', 'VPL', 'Resultado']
    larguras = [18, 40, 20, 20, 18, 15, 20, 14]
    for i, (t, w) in enumerate(zip(cab, larguras), start=1):
        _estilo_cabecalho(ws.cell(row=4, column=i, value=t))
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.row_dimensions[4].height = 30

    linha = 5
    for r in resumo:
        ws.cell(row=linha, column=1, value=r['nu_contrato_fmt'])
        ws.cell(row=linha, column=2, value=r['no_mutuario'])
        ws.cell(row=linha, column=3, value=float(r['vr_debitos'] or 0)).number_format = _FMT_VALOR
        ws.cell(row=linha, column=4,
                value=float(r['vr_laudo']) if r['vr_laudo'] is not None else None).number_format = _FMT_VALOR
        ws.cell(row=linha, column=5, value=r['dt_referencia']).number_format = 'MM/YYYY'
        ws.cell(row=linha, column=6, value=r['ultimo_mes'] + 1)
        ws.cell(row=linha, column=7, value=float(r['vpl'])).number_format = _FMT_VALOR
        res = ws.cell(row=linha, column=8, value='POSITIVO' if r['positivo'] else 'NEGATIVO')
        res.font = Font(bold=True, color='047857' if r['positivo'] else 'B91C1C')
        linha += 1
    ws.freeze_panes = 'A5'

    # ---------------- Aba Parâmetros ----------------
    wp = wb.create_sheet('Parâmetros')
    wp.column_dimensions['A'].width = 70
    wp.column_dimensions['B'].width = 20
    _titulo(wp, 1, 'Parâmetros em vigor na data do cálculo')
    if parametros:
        itens = [
            ('Início da vigência', _para_date(parametros['DT_INI_VIGENCIA']), 'DD/MM/YYYY'),
            ('Fim da vigência', _para_date(parametros['DT_FIM_VIGENCIA']), 'DD/MM/YYYY'),
            ('Custo EMGEA Créditos Imobiliários PF (por contrato)', parametros['VR_CUSTO_CIPF'], _FMT_VALOR),
            ('Custo EMGEA Imóveis Não de Uso (por contrato)', parametros['VR_CUSTO_INU'], _FMT_VALOR),
            ('Tarifa de Administração de Imóveis Não de Uso (por contrato)',
             parametros['VR_TARIFA_ADM_IMOVEIS'], _FMT_VALOR),
            ('Despesa de manutenção INU - primeiro ano', parametros['PC_DESP_MANUT_INU_PRI_ANO'], '0.00%'),
            ('Despesa de manutenção INU - seis meses subsequentes',
             parametros['PC_DESP_MANUT_INU_6_MESES'], '0.00%'),
            ('Desconto na venda do imóvel sobre o valor de avaliação',
             parametros['PC_DESP_DESCONTO_VENDA'], '0.00%'),
            ('Despesa média de execução extrajudicial (por contrato)',
             parametros['VR_DESP_MEDIA_EXEC_JUD'], _FMT_VALOR),
            ('Prazo para a execução (anos)', parametros['PZ_EXECUCAO_ANOS'], '0'),
            ('Prazo de permanência em estoque (meses)', parametros['PZ_PERMANENCIA_ESTOQUE_MESES'], '0'),
            ('Prazo para a execução (meses)', parametros['PZ_EXECUCAO_MESES'], '0'),
        ]
        for i, (rot, val, fmt) in enumerate(itens, start=3):
            if isinstance(val, Decimal):
                val = float(val)
            _rotulo_valor(wp, i, rot, val if val is not None else 'Vigente', fmt)
    else:
        wp.cell(row=3, column=1, value='Nenhuma vigência encontrada para a data do cálculo.')

    # ---------------- Uma aba de fluxo por contrato ----------------
    cab_fluxo = ['Mês', 'Ano/Mês'] + [t for _, t in COLUNAS_FLUXO[:-1]] + \
                ['Custo Médio (% a.a.)', 'Valor Presente']
    larg_fluxo = [8, 12, 20, 18, 22, 20, 20, 18, 16, 20]

    nomes_usados = set()
    for r in resumo:
        nome_aba = f"Fluxo {r['nu_contrato']}"[:31]
        base, k = nome_aba, 2
        while nome_aba in nomes_usados:
            nome_aba = f'{base[:28]}_{k}'
            k += 1
        nomes_usados.add(nome_aba)

        wf = wb.create_sheet(nome_aba)
        _titulo(wf, 1, f"Fluxo de Caixa Simulado - {r['no_mutuario']}")
        _rotulo_valor(wf, 2, 'Contrato', r['nu_contrato_fmt'])
        _rotulo_valor(wf, 3, 'Laudo de Avaliação',
                      float(r['vr_laudo']) if r['vr_laudo'] is not None else None, _FMT_VALOR)
        _rotulo_valor(wf, 4, 'Data de Referência (mês 0)', r['dt_referencia'], 'MM/YYYY')
        c_vpl = _rotulo_valor(wf, 5, 'VP', float(r['vpl']), _FMT_VALOR)
        c_vpl.font = Font(bold=True, color='047857' if r['positivo'] else 'B91C1C')

        for i, (t, w) in enumerate(zip(cab_fluxo, larg_fluxo), start=1):
            _estilo_cabecalho(wf.cell(row=7, column=i, value=t))
            wf.column_dimensions[get_column_letter(i)].width = w
        wf.row_dimensions[7].height = 45

        lin = 8
        for ln in fluxos.get(r['nu_contrato'], []):
            wf.cell(row=lin, column=1, value=int(ln['NU_MES']))
            wf.cell(row=lin, column=2, value=ln['ANO_MES']).number_format = 'MMM/YYYY'
            col = 3
            for chave, _ in COLUNAS_FLUXO[:-1]:
                wf.cell(row=lin, column=col, value=float(ln[chave])).number_format = _FMT_VALOR
                col += 1
            wf.cell(row=lin, column=col,
                    value=float(ln['TAXA_AA']) if ln.get('TAXA_AA') is not None else None
                    ).number_format = _FMT_TAXA
            wf.cell(row=lin, column=col + 1, value=float(ln['VR_PRESENTE'])).number_format = _FMT_VALOR
            if lin % 2 == 0:
                for c in range(1, len(cab_fluxo) + 1):
                    wf.cell(row=lin, column=c).fill = PatternFill('solid', fgColor=_CINZA)
            lin += 1

        # Linha de totais
        tot = wf.cell(row=lin, column=1, value='TOTAL')
        tot.font = Font(bold=True)
        for c in list(range(3, 3 + len(COLUNAS_FLUXO) - 1)) + [len(cab_fluxo)]:
            letra = get_column_letter(c)
            cel = wf.cell(row=lin, column=c, value=f'=SUM({letra}8:{letra}{lin - 1})')
            cel.number_format = _FMT_VALOR
            cel.font = Font(bold=True)
        wf.freeze_panes = 'C8'

    saida = BytesIO()
    wb.save(saida)
    saida.seek(0)
    return saida