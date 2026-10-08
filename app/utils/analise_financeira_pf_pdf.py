# app/utils/analise_financeira_pf_pdf.py
"""
PDFs da Análise Financeira PF (ReportLab).

  gerar_pdf_fluxo(resumo, fluxos)
      Uma página (A4 paisagem) por fluxo de contrato e, quando o contrato tem
      simulação (VPL negativo), a página da Meta logo depois — o mesmo
      conteúdo das abas do Excel do fluxo.

  gerar_pdf_resumo(blocos)
      A4 retrato, um bloco por contrato com o cabeçalho do memorando
      (Memorando / Destinatário / Assunto) e as seções com os valores,
      no mesmo layout da tela de Resumo.

Os dados chegam prontos das mesmas funções usadas no Excel e na tela
(listar_resumo_calculo, obter_fluxo_gravado, montar_fluxo_meta,
montar_resumo), então PDF, Excel e tela mostram sempre os mesmos números.

Compatível com Python 3.9 e 3.12.
"""
from decimal import Decimal
from io import BytesIO
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.enums import TA_RIGHT
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (KeepTogether, PageBreak, Paragraph, SimpleDocTemplate,
                                Spacer, Table, TableStyle)

from app.utils import analise_financeira_pf_calculo as calc

AZUL = colors.HexColor('#224ABE')
AZUL_CLARO = colors.HexColor('#EEF2FF')
CINZA_FUNDO = colors.HexColor('#F8F9FC')
CINZA_LINHA = colors.HexColor('#E3E6F0')
CINZA_TEXTO = colors.HexColor('#5A5C69')
AMARELO = colors.HexColor('#FFF8E1')
AMARELO_FORTE = colors.HexColor('#FFF59D')
VERDE = colors.HexColor('#047857')
VERMELHO = colors.HexColor('#B91C1C')

_TITULO = ParagraphStyle('titulo', fontName='Helvetica-Bold', fontSize=13, textColor=AZUL, spaceAfter=4)
_SUB = ParagraphStyle('sub', fontName='Helvetica', fontSize=8.5, textColor=CINZA_TEXTO)
_ROT = ParagraphStyle('rot', fontName='Helvetica', fontSize=7, textColor=CINZA_TEXTO, leading=9)
_VAL = ParagraphStyle('val', fontName='Helvetica-Bold', fontSize=9.5, leading=12)
_TXT = ParagraphStyle('txt', fontName='Helvetica', fontSize=9, leading=12)
_TXT_DIR = ParagraphStyle('txtdir', parent=_TXT, alignment=TA_RIGHT, fontName='Helvetica-Bold')
_SECAO = ParagraphStyle('secao', fontName='Helvetica-Bold', fontSize=10, textColor=AZUL)
_FAIXA = ParagraphStyle('faixa', fontName='Helvetica-Bold', fontSize=10.5, textColor=colors.white)


def _br(valor, casas=2):
    if valor is None:
        return ''
    fmt = '{:,.' + str(casas) + 'f}'
    return fmt.format(float(valor)).replace(',', 'X').replace('.', ',').replace('X', '.')


def _p(texto, estilo, cor=None):
    txt = escape(str(texto if texto is not None else ''))
    if cor is not None:
        txt = f'<font color="{cor.hexval().replace("0x", "#")}">{txt}</font>'
    return Paragraph(txt, estilo)


def _rodape(canvas, doc):
    canvas.saveState()
    canvas.setFont('Helvetica', 7)
    canvas.setFillColor(CINZA_TEXTO)
    largura, _ = doc.pagesize
    canvas.drawString(doc.leftMargin, 0.8 * cm, 'Portal GEINC · Análise Financeira PF')
    canvas.drawRightString(largura - doc.rightMargin, 0.8 * cm, f'Página {doc.page}')
    canvas.restoreState()


# =========================================================================
# FLUXO
# =========================================================================
def _cartoes(itens, largura_total):
    """Linha de 'cartões' rótulo/valor (Laudo, Débitos, Referência, VP...)."""
    celulas = []
    for rotulo, valor, cor in itens:
        celulas.append([_p(rotulo.upper(), _ROT), _p(valor, _VAL, cor)])
    larg = largura_total / len(celulas)
    t = Table([celulas], colWidths=[larg] * len(celulas))
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), CINZA_FUNDO),
        ('BOX', (0, 0), (-1, -1), 0.5, CINZA_LINHA),
        ('LINEAFTER', (0, 0), (-2, -1), 0.5, CINZA_LINHA),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
    ]))
    return t


def _tabela_fluxo(linhas, mes_simulado=None):
    """Tabela mês a mês no layout da planilha, com linha de TOTAL."""
    cab = ['Mês', 'Ano/Mês', 'Despesas de\nManutenção', 'Custo\nManutenção',
           'Desp. Consolidação\nde Propriedade', 'Débitos\nPropter Rem', 'Venda Imóvel\nExecutado',
           'Total', 'Custo Médio\n(% a.a.)', 'Valor\nPresente']
    chaves = ['VR_DESP_MANUT', 'VR_CUSTO_MANUT', 'VR_DESP_CONSOL_PROP',
              'VR_DEB_PROPTERREM', 'VR_VENDA', 'VR_TOTAL']
    dados = [cab]
    estilos = []
    totais = {k: Decimal('0') for k in chaves + ['VR_PRESENTE']}

    for i, ln in enumerate(linhas, start=1):
        vals = {k: Decimal(str(ln[k])) for k in chaves + ['VR_PRESENTE']}
        for k in totais:
            totais[k] += vals[k]
        linha = [str(int(ln['NU_MES'])), calc.mes_ano_texto(ln['ANO_MES'])]
        linha += [_br(vals[k]) for k in chaves]
        linha += [_br(ln['TAXA_AA'], 4) if ln.get('TAXA_AA') is not None else '-', _br(vals['VR_PRESENTE'])]
        dados.append(linha)

        evento = any(vals[k] != 0 for k in ('VR_DESP_MANUT', 'VR_DESP_CONSOL_PROP',
                                            'VR_DEB_PROPTERREM', 'VR_VENDA'))
        if evento:
            estilos.append(('BACKGROUND', (0, i), (-1, i), AMARELO))
            estilos.append(('FONTNAME', (0, i), (-1, i), 'Helvetica-Bold'))
        elif i % 2 == 0:
            estilos.append(('BACKGROUND', (0, i), (-1, i), CINZA_FUNDO))
        if mes_simulado is not None and int(ln['NU_MES']) == mes_simulado:
            for col in (5, 7, 9):   # débitos, total e VP simulados
                estilos.append(('BOX', (col, i), (col, i), 1.2, AZUL))
        for col, k in enumerate(chaves + [None, 'VR_PRESENTE'], start=2):
            if k and vals[k] < 0:
                estilos.append(('TEXTCOLOR', (col, i), (col, i), VERMELHO))

    n = len(dados)
    dados.append(['TOTAL', ''] + [_br(totais[k]) for k in chaves] + ['', _br(totais['VR_PRESENTE'])])
    estilos += [
        ('BACKGROUND', (0, n), (-1, n), AZUL_CLARO),
        ('FONTNAME', (0, n), (-1, n), 'Helvetica-Bold'),
        ('LINEABOVE', (0, n), (-1, n), 1.2, AZUL),
        ('TEXTCOLOR', (9, n), (9, n), VERDE if totais['VR_PRESENTE'] >= 0 else VERMELHO),
    ]

    larguras = [1.1, 1.8, 2.9, 2.5, 3.2, 2.9, 2.9, 2.9, 2.2, 3.0]
    t = Table(dados, colWidths=[w * cm for w in larguras], repeatRows=1)
    t.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BACKGROUND', (0, 0), (-1, 0), AZUL),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTSIZE', (0, 0), (-1, 0), 6.8),
        ('FONTSIZE', (0, 1), (-1, -1), 6.6),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, -1), 1.2),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 1.2),
        ('LINEBELOW', (0, 1), (-1, -2), 0.25, CINZA_LINHA),
    ] + estilos))
    return t


def _pagina_fluxo(historia, titulo, subtitulo, cartoes, linhas, largura, mes_simulado=None):
    historia.append(_p(titulo, _TITULO))
    historia.append(_p(subtitulo, _SUB))
    historia.append(Spacer(1, 6))
    historia.append(_cartoes(cartoes, largura))
    historia.append(Spacer(1, 8))
    historia.append(_tabela_fluxo(linhas, mes_simulado))


def gerar_pdf_fluxo(resumo, fluxos):
    """
    resumo: lista de listar_resumo_calculo()
    fluxos: {(DT_CALCULO, nu_contrato str): linhas de obter_fluxo_gravado()}
    Retorna BytesIO com o PDF.
    """
    saida = BytesIO()
    pagina = landscape(A4)
    doc = SimpleDocTemplate(saida, pagesize=pagina, leftMargin=1.2 * cm, rightMargin=1.2 * cm,
                            topMargin=1.0 * cm, bottomMargin=1.3 * cm,
                            title='Análise Financeira PF - Fluxo de Caixa')
    largura = pagina[0] - doc.leftMargin - doc.rightMargin
    historia = []

    for r in resumo:
        linhas = fluxos.get((r['dt_calculo'], r['nu_contrato']), [])
        info = calc.SITUACOES.get(r['situacao'])
        cor_vpl = VERDE if r['positivo'] else VERMELHO
        sub = f"Contrato {r['nu_contrato_fmt']} · calculado em {r['dt_calculo'].strftime('%d/%m/%Y')}"
        if r.get('memo'):
            sub += f" · {r['memo']}"

        if historia:
            historia.append(PageBreak())
        _pagina_fluxo(
            historia, f"Fluxo de Caixa Simulado - {r['no_mutuario']}", sub,
            [
                ('Laudo de Avaliação', 'R$ ' + _br(r['vr_laudo']), None),
                ('Débitos Propter Rem', 'R$ ' + _br(r['vr_debitos'] or 0), None),
                ('Referência (mês 0)', calc.mes_ano_texto(r['dt_referencia']), None),
                ('VP', 'R$ ' + _br(r['vpl']), cor_vpl),
                ('Situação', info['rotulo'] if info else '-',
                 colors.HexColor(info['cor']) if info else None),
            ],
            linhas, largura,
        )

        meta = calc.montar_fluxo_meta(linhas, r['simulacao'])
        if meta:
            sim = r['simulacao']
            vpl_meta = sum((Decimal(str(ln['VR_PRESENTE'])) for ln in meta), Decimal('0'))
            historia.append(PageBreak())
            _pagina_fluxo(
                historia, f"Fluxo de Caixa Simulado - Meta - {r['no_mutuario']}", sub,
                [
                    ('Débitos Propter Rem atuais', 'R$ ' + _br(r['vr_debitos'] or 0), None),
                    ('Débitos Propter Rem simulados', 'R$ ' + _br(sim['VR_DEB_PROPTERREM_SIMULADO']), None),
                    ('Redução necessária', 'R$ ' + _br(sim['VR_REDUCAO_PROPTERREM']), None),
                    ('VP da Meta', 'R$ ' + _br(vpl_meta), VERDE if vpl_meta >= 0 else VERMELHO),
                ],
                meta, largura, int(sim['NU_MES_CONSOLIDACAO']),
            )

    doc.build(historia, onFirstPage=_rodape, onLaterPages=_rodape)
    saida.seek(0)
    return saida


# =========================================================================
# RESUMO
# =========================================================================
def gerar_pdf_resumo(blocos):
    """
    blocos: [{'nu_contrato', 'no_mutuario', 'resumo': nota.montar_resumo()}]
    Retorna BytesIO com o PDF.
    """
    saida = BytesIO()
    doc = SimpleDocTemplate(saida, pagesize=A4, leftMargin=1.8 * cm, rightMargin=1.8 * cm,
                            topMargin=1.5 * cm, bottomMargin=1.5 * cm,
                            title='Análise Financeira PF - Resumo')
    largura = A4[0] - doc.leftMargin - doc.rightMargin
    historia = [_p('Análise Financeira PF - Resumo', _TITULO), Spacer(1, 8)]

    for bloco in blocos:
        res = bloco['resumo']
        partes = []

        faixa = Table([[_p(f"{bloco['no_mutuario']}  ·  Contrato {bloco['nu_contrato']}", _FAIXA)]],
                      colWidths=[largura])
        faixa.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), AZUL),
            ('TOPPADDING', (0, 0), (-1, -1), 6), ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        partes.append(faixa)

        if not res['cabecalho'] and not res['secoes']:
            partes += [Spacer(1, 6), _p('Resumo ainda não disponível na FIN_VW038.', _SUB)]
        else:
            # Cabeçalho do memorando
            if res['cabecalho']:
                dados = [[_p(c['rotulo'].upper(), _ROT), _p(c['texto'], _TXT)] for c in res['cabecalho']]
                t = Table(dados, colWidths=[3 * cm, largura - 3 * cm])
                est = [
                    ('BACKGROUND', (0, 0), (-1, -1), CINZA_FUNDO),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    ('LINEBELOW', (0, 0), (-1, -2), 0.4, CINZA_LINHA),
                    ('TOPPADDING', (0, 0), (-1, -1), 5), ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                ]
                for i, c in enumerate(res['cabecalho']):
                    if c['pendente']:
                        est.append(('BACKGROUND', (1, i), (1, i), AMARELO_FORTE))
                t.setStyle(TableStyle(est))
                partes += [Spacer(1, 6), t]

            # Seções
            for secao in res['secoes']:
                dados = [[_p(secao['titulo'], _SECAO), '']]
                est = [
                    ('SPAN', (0, 0), (1, 0)),
                    ('BACKGROUND', (0, 0), (-1, 0), AZUL_CLARO),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('TOPPADDING', (0, 0), (-1, -1), 6), ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                    ('BOX', (0, 0), (-1, -1), 0.5, CINZA_LINHA),
                ]
                for i, item in enumerate(secao['itens'], start=1):
                    cor = None
                    if item['valor']:
                        cor = VERMELHO if item['negativo'] else (VERDE if item['numero'] is not None else None)
                    dados.append([_p(item['rotulo'], _TXT), _p(item['valor'] or '', _TXT_DIR, cor)])
                    est.append(('LINEBELOW', (0, i), (-1, i), 0.4, CINZA_LINHA))
                    if item['pendente']:
                        est.append(('BACKGROUND', (0, i), (-1, i), AMARELO_FORTE))
                t = Table(dados, colWidths=[largura - 4.5 * cm, 4.5 * cm])
                t.setStyle(TableStyle(est))
                partes += [Spacer(1, 8), t]

        historia.append(KeepTogether(partes))
        historia.append(Spacer(1, 18))

    doc.build(historia, onFirstPage=_rodape, onLaterPages=_rodape)
    saida.seek(0)
    return saida


# =========================================================================
# NOTA TÉCNICA EM PDF
# =========================================================================
# Fonte: Calibri do Windows (igual ao Word). Se não existir no servidor,
# tenta a Carlito (mesmas medidas da Calibri) e, por último, Helvetica.
_FONTES_CALIBRI = [
    (r'C:\Windows\Fonts\calibri.ttf', r'C:\Windows\Fonts\calibrib.ttf'),
    ('/usr/share/fonts/truetype/crosextra/Carlito-Regular.ttf',
     '/usr/share/fonts/truetype/crosextra/Carlito-Bold.ttf'),
]
_FONTE_NOTA = None


def _registrar_fonte_nota():
    """Registra a fonte da nota uma vez e devolve (normal, negrito, tamanho)."""
    global _FONTE_NOTA
    if _FONTE_NOTA:
        return _FONTE_NOTA
    import os
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.lib.fonts import addMapping

    for normal, negrito in _FONTES_CALIBRI:
        if os.path.exists(normal) and os.path.exists(negrito):
            try:
                pdfmetrics.registerFont(TTFont('NotaCalibri', normal))
                pdfmetrics.registerFont(TTFont('NotaCalibri-Bold', negrito))
                addMapping('NotaCalibri', 0, 0, 'NotaCalibri')
                addMapping('NotaCalibri', 1, 0, 'NotaCalibri-Bold')
                addMapping('NotaCalibri', 0, 1, 'NotaCalibri')
                addMapping('NotaCalibri', 1, 1, 'NotaCalibri-Bold')
                _FONTE_NOTA = ('NotaCalibri', 'NotaCalibri-Bold', 13.5)
                return _FONTE_NOTA
            except Exception:
                continue
    _FONTE_NOTA = ('Helvetica', 'Helvetica-Bold', 12)   # Helvetica é mais larga: corpo menor
    return _FONTE_NOTA


def _markup_nota(texto, negrito=False, negrito_palavras=()):
    """
    Converte o texto do parágrafo em marcação do ReportLab:
      - trechos pendentes ('...', 'xxx', 'xx/xx/xxxx') com fundo amarelo;
      - palavras de negrito_palavras em negrito;
      - quebra de linha -> <br/>.
    """
    import re
    from app.utils.analise_financeira_pf_nota import _PENDENTE

    texto = texto.replace('\u2011', '-')   # hífen inseparável não existe em toda fonte
    padrao_neg = None
    if negrito_palavras:
        padrao_neg = re.compile(r'\b(' + '|'.join(re.escape(p) for p in negrito_palavras) + r')\b')

    def _trecho(t):
        t = escape(t)
        if padrao_neg:
            t = padrao_neg.sub(r'<b>\1</b>', t)
        return t

    linhas = []
    for linha in texto.split('\n'):
        partes = []
        pos = 0
        for m in _PENDENTE.finditer(linha):
            partes.append(_trecho(linha[pos:m.start()]))
            partes.append(f'<font backColor="#FFF59D">{escape(m.group(0))}</font>')
            pos = m.end()
        partes.append(_trecho(linha[pos:]))
        linhas.append(''.join(partes))
    corpo = '<br/>'.join(linhas)
    return f'<b>{corpo}</b>' if negrito else corpo


def gerar_pdf_nota(paragrafos, assinante_nome=None, assinante_cargo=None):
    """
    Nota técnica em PDF com o MESMO conteúdo e a mesma organização do Word
    (paragrafos = nota.montar_paragrafos(...)): A4, margens do modelo,
    'Assunto:' em negrito, marcadores, itens numerados justificados,
    linhas em branco entre blocos e assinatura do usuário no final.
    """
    from reportlab.lib.enums import TA_JUSTIFY, TA_LEFT

    fonte, fonte_negrito, tamanho = _registrar_fonte_nota()
    entrelinha = tamanho * 1.22

    base = ParagraphStyle('nota', fontName=fonte, fontSize=tamanho, leading=entrelinha,
                          alignment=TA_LEFT, textColor=colors.black)
    justificado = ParagraphStyle('nota_just', parent=base, alignment=TA_JUSTIFY)
    marcador = ParagraphStyle('nota_marc', parent=justificado,
                              leftIndent=1.905 * cm, bulletIndent=1.27 * cm, bulletFontName=fonte)

    saida = BytesIO()
    doc = SimpleDocTemplate(saida, pagesize=A4,
                            leftMargin=3.0 * cm, rightMargin=3.0 * cm,
                            topMargin=2.5 * cm, bottomMargin=2.5 * cm,
                            title='Nota Técnica - Análise Financeira PF')
    historia = []

    def _branco():
        historia.append(Spacer(1, entrelinha))

    total = len(paragrafos)
    for i, par in enumerate(paragrafos):
        tipo, texto = par['tipo'], par['texto']
        proximo = paragrafos[i + 1] if i + 1 < total else None

        if tipo == 'marcador':
            historia.append(Paragraph(_markup_nota(texto), marcador, bulletText='\u2022'))
        elif tipo == 'assunto':
            rotulo, _, resto = texto.partition(':')
            historia.append(Paragraph(escape(rotulo) + ':\u00a0' + _markup_nota(resto.strip(), negrito=True), base))
        elif tipo == 'numerado':
            destaque = ('positivo', 'negativo') if texto.rstrip().endswith(':') else ()
            historia.append(Paragraph(_markup_nota(texto, negrito_palavras=destaque), justificado))
        elif tipo == 'nome':
            historia.append(Paragraph(_markup_nota(texto, negrito=True), base))
        else:
            historia.append(Paragraph(_markup_nota(texto), base))

        # Mesmas regras de linha em branco do Word
        if proximo is None:
            continue
        if tipo == 'marcador' and proximo['tipo'] == 'marcador':
            continue
        if tipo == 'nome':
            continue
        _branco()

    if assinante_nome:
        _branco()
        assinatura = f'<b>{escape(assinante_nome.upper())}</b>'
        if assinante_cargo:
            assinatura += '<br/>' + escape(assinante_cargo)
        historia.append(Paragraph(assinatura, base))

    doc.build(historia)
    saida.seek(0)
    return saida