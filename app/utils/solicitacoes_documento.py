# app/utils/solicitacoes_documento.py
"""
Termo de Entrega e Homologação do aplicativo "Solicitações à Área".

Como funciona
-------------
1. montar_blocos(dados) transforma os dados da solicitação (vindos do banco
   pelas rotas) numa lista de BLOCOS neutros:
       ('titulo', texto, subtitulo)
       ('secao', texto)
       ('campos', [(rotulo, valor), ...])        -> tabela rótulo/valor
       ('paragrafo', texto)
       ('checklist', [(concluido, texto), ...])
       ('tabela', cabecalho, linhas, larguras_relativas)
       ('declaracao', decisao)                   -> ATENDE / NÃO ATENDE
       ('assinaturas', [(papel, nome, cargo), ...])
       ('nota', texto)
2. gerar_pdf(blocos, rodape) e gerar_docx(blocos, rodape) desenham OS MESMOS
   blocos — assim o PDF (para assinar digitalmente, ex.: assinador gov.br
   ou SEI) e o Word (editável) têm sempre o mesmo conteúdo.
3. codigo_verificacao(dados) gera um código a partir do conteúdo do termo
   (SHA-256). Se qualquer informação do termo mudar, o código muda: serve
   para conferir se a via assinada corresponde ao que está no portal.
4. dados['modelo'] = True gera o MODELO em branco (campos com linhas para
   preencher), usado no botão "Modelo do termo".

Compatível com Python 3.9 e 3.12.
"""
import hashlib
import json
from datetime import date, datetime
from io import BytesIO
from xml.sax.saxutils import escape

from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm, Pt, RGBColor

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (KeepTogether, Paragraph, SimpleDocTemplate, Spacer,
                                Table, TableStyle)

BRANCO = '____________________________'

# Cores (PDF e Word)
_HEX_AZUL = '224ABE'
_HEX_AZUL_CLARO = 'EEF2FF'
_HEX_CINZA = '5A5C69'
_HEX_LINHA = 'D1D5E0'
_HEX_FUNDO = 'F8F9FC'
_HEX_VERDE = '047857'
_HEX_VERMELHO = 'B91C1C'


# =========================================================================
# FORMATAÇÃO
# =========================================================================
def _fmt_data(valor, com_hora=False):
    if valor is None or valor == '':
        return ''
    if isinstance(valor, datetime):
        return valor.strftime('%d/%m/%Y %H:%M' if com_hora else '%d/%m/%Y')
    if isinstance(valor, date):
        return valor.strftime('%d/%m/%Y')
    return str(valor)


def _txt(valor, modelo=False):
    """Texto do campo; no modelo em branco devolve a linha para preencher."""
    if modelo:
        return BRANCO
    if valor is None:
        return '—'
    texto = str(valor).strip()
    return texto if texto else '—'


def codigo_verificacao(dados):
    """
    Código de verificação do termo: SHA-256 do conteúdo relevante.
    Retorna (hash_completo_64, codigo_exibicao 'XXXX-XXXX-XXXX-XXXX').
    """
    sol = dados.get('sol') or {}
    base = {
        'protocolo': sol.get('NU_PROTOCOLO'),
        'titulo': sol.get('TITULO'),
        'descricao': sol.get('DESCRICAO'),
        'solicitante': sol.get('ID_USUARIO_SOLICITANTE'),
        'resumo': sol.get('RESUMO_ENTREGA'),
        'orientacoes': sol.get('ORIENTACOES_USO'),
        'link': sol.get('LINK_ACESSO'),
        'entrega': _fmt_data(sol.get('DT_ENTREGA'), True),
        'homologacao': sol.get('ST_HOMOLOGACAO_SOLICITANTE'),
        'parecer': sol.get('PARECER_SOLICITANTE'),
        'dt_homologacao': _fmt_data(sol.get('DT_HOMOLOGACAO_SOLICITANTE'), True),
        'executores': [e.get('nome') for e in dados.get('executores') or []],
        'outros_solicitantes': [e.get('nome') for e in dados.get('outros_solicitantes') or []],
        'itens': [[i.get('descricao'), bool(i.get('concluido'))] for i in dados.get('itens') or []],
    }
    bruto = json.dumps(base, ensure_ascii=False, sort_keys=True, default=str).encode('utf-8')
    hash_hex = hashlib.sha256(bruto).hexdigest()
    curto = hash_hex[:16].upper()
    return hash_hex, '-'.join(curto[i:i + 4] for i in range(0, 16, 4))


# =========================================================================
# CONTEÚDO (blocos neutros)
# =========================================================================
def montar_blocos(dados):
    modelo = bool(dados.get('modelo'))
    sol = dados.get('sol') or {}
    rot = dados.get('rotulos') or {}
    blocos = []

    blocos.append((
        'titulo',
        'TERMO DE ENTREGA E HOMOLOGAÇÃO',
        '{} · {}'.format(dados.get('orgao') or '', dados.get('area_executora') or '').strip(' ·')
    ))

    # 1. Identificação
    blocos.append(('secao', '1. Identificação da solicitação'))
    blocos.append(('campos', [
        ('Protocolo', _txt(sol.get('NU_PROTOCOLO'), modelo)),
        ('Título', _txt(sol.get('TITULO'), modelo)),
        ('Tipo de entrega', _txt(rot.get('tipo'), modelo)),
        ('Categoria', _txt(rot.get('categoria'), modelo)),
        ('Aplicativo / painel', _txt(sol.get('NO_SISTEMA'), modelo)),
        ('Referência (SEI / memorando)', _txt(sol.get('NU_REFERENCIA'), modelo)),
        ('Data da solicitação', _txt(_fmt_data(sol.get('DT_SOLICITACAO')), modelo)),
        ('Canal de origem', _txt(rot.get('canal'), modelo)),
        ('Prioridade', _txt(rot.get('prioridade'), modelo)),
    ]))

    # 2. Solicitante
    area = ' / '.join([x for x in [sol.get('SG_DIRETORIA'), sol.get('SG_SUPERINTENDENCIA'),
                                   sol.get('SG_SETOR')] if x])
    ciencia = sol.get('DT_CIENCIA_SOLICITANTE')
    outros = [] if modelo else (dados.get('outros_solicitantes') or [])
    blocos.append(('secao', '2. Solicitante' + ('s' if outros else '')))
    blocos.append(('campos', [
        ('Nome', _txt(sol.get('NO_SOLICITANTE'), modelo)),
        ('Cargo', _txt(sol.get('DS_CARGO_SOLICITANTE'), modelo)),
        ('Área (Diretoria / Superintendência / Setor)', _txt(area, modelo)),
        ('E-mail', _txt(sol.get('EMAIL_SOLICITANTE'), modelo)),
        ('Ciência do pedido registrada no portal',
         _txt(_fmt_data(ciencia, True) if ciencia else 'Não registrada', modelo)),
    ]))
    if outros:
        blocos.append(('paragrafo', 'Também solicitaram:'))
        blocos.append(('tabela', ['Nome', 'Cargo', 'Área'],
                       [[o.get('nome') or '', o.get('cargo') or '—', o.get('area') or '—'] for o in outros],
                       [2.4, 2, 2]))

    # 3. O que foi pedido
    blocos.append(('secao', '3. Descrição do que foi solicitado'))
    blocos.append(('paragrafo', _txt(sol.get('DESCRICAO'), modelo) if not modelo
                   else BRANCO * 3))

    # 4. Equipe
    blocos.append(('secao', '4. Equipe responsável pela execução'))
    executores = dados.get('executores') or []
    if modelo or not executores:
        linhas = [[BRANCO, BRANCO]] if modelo else [['Nenhum executor informado', '']]
    else:
        linhas = [[e.get('nome') or '', e.get('funcao') or '—'] for e in executores]
    blocos.append(('tabela', ['Nome', 'Função na entrega'], linhas, [3, 2]))

    # 5. Escopo
    blocos.append(('secao', '5. Escopo entregue'))
    itens = dados.get('itens') or []
    if modelo:
        blocos.append(('checklist', [(False, BRANCO * 2), (False, BRANCO * 2), (False, BRANCO * 2)]))
    elif itens:
        blocos.append(('checklist', [(bool(i.get('concluido')), i.get('descricao') or '') for i in itens]))
    else:
        blocos.append(('paragrafo', 'Não foram cadastrados itens de escopo para esta solicitação.'))

    # 6. Resumo do que foi feito
    blocos.append(('secao', '6. Resumo do que foi realizado'))
    blocos.append(('paragrafo', _txt(sol.get('RESUMO_ENTREGA'), modelo) if not modelo
                   else BRANCO * 4))

    # 7. Orientações
    blocos.append(('secao', '7. Orientações de uso e acesso'))
    blocos.append(('campos', [
        ('Endereço / caminho de acesso', _txt(sol.get('LINK_ACESSO'), modelo)),
    ]))
    if modelo:
        blocos.append(('paragrafo', BRANCO * 3))
    elif sol.get('ORIENTACOES_USO'):
        blocos.append(('paragrafo', sol.get('ORIENTACOES_USO')))

    # 8. Prazos
    ind = dados.get('indicadores') or {}
    blocos.append(('secao', '8. Prazos'))
    blocos.append(('campos', [
        ('Prazo acordado', _txt(_fmt_data(sol.get('DT_PRAZO_ACORDADO') or sol.get('DT_PRAZO_DESEJADO')), modelo)),
        ('Início da execução', _txt(_fmt_data(sol.get('DT_INICIO')), modelo)),
        ('Entrega para homologação', _txt(_fmt_data(sol.get('DT_ENTREGA'), True), modelo)),
        ('Dias corridos até a entrega', _txt(ind.get('dias_ate_entrega'), modelo)),
        ('Situação do prazo', _txt(ind.get('situacao_prazo'), modelo)),
    ]))

    # 9. Andamento (somente registros públicos)
    linha_tempo = dados.get('linha_tempo') or []
    blocos.append(('secao', '9. Histórico de andamento'))
    if modelo:
        blocos.append(('tabela', ['Data', 'Registro', 'Por'], [[BRANCO[:10], BRANCO, BRANCO[:14]]] * 3, [1.3, 4.7, 1.8]))
    elif linha_tempo:
        linhas = [[_fmt_data(r.get('data'), True), r.get('texto') or '', r.get('usuario') or '']
                  for r in linha_tempo]
        blocos.append(('tabela', ['Data', 'Registro', 'Por'], linhas, [1.3, 4.7, 1.8]))
    else:
        blocos.append(('paragrafo', 'Sem registros de andamento.'))

    # 10. Homologação
    blocos.append(('secao', '10. Homologação pelo solicitante'))
    decisao = None if modelo else sol.get('ST_HOMOLOGACAO_SOLICITANTE')
    blocos.append(('declaracao', decisao))
    if modelo:
        blocos.append(('campos', [('Parecer do solicitante', BRANCO * 2),
                                  ('Data do parecer', BRANCO)]))
    elif decisao:
        blocos.append(('campos', [
            ('Parecer do solicitante', _txt(sol.get('PARECER_SOLICITANTE'))),
            ('Registrado no portal em', _fmt_data(sol.get('DT_HOMOLOGACAO_SOLICITANTE'), True)),
        ]))
    else:
        blocos.append(('nota', 'A decisão ainda não foi registrada no portal. Marque a opção '
                               'correspondente antes de assinar ou registre pelo botão "Homologar".'))

    # Assinaturas
    gerador = dados.get('gerado_por') or {}
    assinantes = [('Solicitante', '' if modelo else (sol.get('NO_SOLICITANTE') or ''),
                   '' if modelo else (sol.get('DS_CARGO_SOLICITANTE') or ''))]
    assinantes += [('Solicitante', o.get('nome') or '', o.get('cargo') or '') for o in outros]
    assinantes.append(('Responsável pela entrega',
                       '' if modelo else (dados.get('responsavel_nome') or gerador.get('nome') or ''),
                       '' if modelo else (dados.get('responsavel_cargo') or gerador.get('cargo') or '')))
    # Duas assinaturas por linha, para caber na página
    for i in range(0, len(assinantes), 2):
        blocos.append(('assinaturas', assinantes[i:i + 2], i == 0))
    blocos.append(('nota', 'Este termo pode ser assinado eletronicamente (assinador gov.br, SEI ou '
                           'certificado digital ICP-Brasil). Após assinado, anexe a via assinada no '
                           'portal, na aba Homologação da solicitação.'))
    return blocos


def texto_rodape(dados):
    if dados.get('modelo'):
        return 'Portal GEINC · Modelo de Termo de Entrega e Homologação'
    sol = dados.get('sol') or {}
    gerador = dados.get('gerado_por') or {}
    return 'Portal GEINC · {} · Gerado em {} por {} · Código de verificação {}'.format(
        sol.get('NU_PROTOCOLO') or '',
        _fmt_data(dados.get('gerado_em') or datetime.now(), True),
        gerador.get('nome') or '',
        dados.get('codigo') or '',
    )


# =========================================================================
# PDF
# =========================================================================
def _c(hex6):
    return colors.HexColor('#' + hex6)


_E_TIT = ParagraphStyle('sol_tit', fontName='Helvetica-Bold', fontSize=15, textColor=_c(_HEX_AZUL),
                        alignment=TA_CENTER, leading=19)
_E_SUBTIT = ParagraphStyle('sol_sub', fontName='Helvetica', fontSize=9, textColor=_c(_HEX_CINZA),
                           alignment=TA_CENTER, leading=12)
_E_SECAO = ParagraphStyle('sol_secao', fontName='Helvetica-Bold', fontSize=10.5, textColor=colors.white,
                          leading=13)
_E_ROT = ParagraphStyle('sol_rot', fontName='Helvetica-Bold', fontSize=8.5, textColor=_c(_HEX_CINZA), leading=11)
_E_VAL = ParagraphStyle('sol_val', fontName='Helvetica', fontSize=9.5, leading=12.5)
_E_TXT = ParagraphStyle('sol_txt', fontName='Helvetica', fontSize=10, leading=14, alignment=TA_JUSTIFY)
_E_CEL = ParagraphStyle('sol_cel', fontName='Helvetica', fontSize=8.5, leading=11)
_E_CAB = ParagraphStyle('sol_cab', fontName='Helvetica-Bold', fontSize=8.5, leading=11, textColor=_c(_HEX_CINZA))
_E_NOTA = ParagraphStyle('sol_nota', fontName='Helvetica-Oblique', fontSize=8, leading=10.5,
                         textColor=_c(_HEX_CINZA))
_E_ASS = ParagraphStyle('sol_ass', fontName='Helvetica', fontSize=9, leading=12, alignment=TA_CENTER)


def _p(texto, estilo):
    txt = escape(str(texto if texto is not None else '')).replace('\n', '<br/>')
    return Paragraph(txt, estilo)


def gerar_pdf(blocos, rodape):
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, leftMargin=2 * cm, rightMargin=2 * cm,
                            topMargin=1.8 * cm, bottomMargin=1.8 * cm,
                            title='Termo de Entrega e Homologação', author='Portal GEINC')
    largura = A4[0] - 4 * cm
    hist = []

    pendente = []  # cabeçalho de seção aguardando o bloco seguinte (não fica sozinho no pé da página)
    for bloco in blocos:
        tipo = bloco[0]
        atual = []
        if tipo == 'titulo':
            atual.append(_p(bloco[1], _E_TIT))
            if bloco[2]:
                atual.append(_p(bloco[2], _E_SUBTIT))
            atual.append(Spacer(1, 0.45 * cm))

        elif tipo == 'secao':
            t = Table([[_p(bloco[1], _E_SECAO)]], colWidths=[largura])
            t.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, -1), _c(_HEX_AZUL)),
                ('LEFTPADDING', (0, 0), (-1, -1), 8),
                ('TOPPADDING', (0, 0), (-1, -1), 4),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ]))
            atual.append(Spacer(1, 0.25 * cm))
            atual.append(t)
            atual.append(Spacer(1, 0.15 * cm))

        elif tipo == 'campos':
            dados_t = [[_p(r, _E_ROT), _p(v, _E_VAL)] for r, v in bloco[1]]
            t = Table(dados_t, colWidths=[largura * 0.36, largura * 0.64])
            t.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('BACKGROUND', (0, 0), (0, -1), _c(_HEX_FUNDO)),
                ('LINEBELOW', (0, 0), (-1, -1), 0.4, _c(_HEX_LINHA)),
                ('TOPPADDING', (0, 0), (-1, -1), 3),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ]))
            atual.append(t)

        elif tipo == 'paragrafo':
            atual.append(_p(bloco[1], _E_TXT))

        elif tipo == 'checklist':
            linhas = []
            for feito, texto in bloco[1]:
                marca = '<font color="#{}"><b>[X]</b></font>'.format(_HEX_VERDE) if feito else '<b>[  ]</b>'
                linhas.append([Paragraph(marca, _E_CEL), _p(texto, _E_CEL)])
            t = Table(linhas, colWidths=[1.1 * cm, largura - 1.1 * cm])
            t.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LINEBELOW', (0, 0), (-1, -1), 0.3, _c(_HEX_LINHA)),
            ]))
            atual.append(t)

        elif tipo == 'tabela':
            cab, linhas, pesos = bloco[1], bloco[2], bloco[3]
            total = float(sum(pesos))
            larguras = [largura * p / total for p in pesos]
            dados_t = [[_p(c, _E_CAB) for c in cab]] + [[_p(c, _E_CEL) for c in l] for l in linhas]
            t = Table(dados_t, colWidths=larguras, repeatRows=1)
            t.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('BACKGROUND', (0, 0), (-1, 0), _c(_HEX_AZUL_CLARO)),
                ('LINEBELOW', (0, 0), (-1, -1), 0.3, _c(_HEX_LINHA)),
                ('TOPPADDING', (0, 0), (-1, -1), 3),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ]))
            atual.append(t)

        elif tipo == 'declaracao':
            decisao = bloco[1]
            m_sim = '[X]' if decisao == 'APROVADA' else '[  ]'
            m_nao = '[X]' if decisao == 'RECUSADA' else '[  ]'
            texto = ('Declaro que recebi e verifiquei a entrega descrita neste termo e que ela:<br/><br/>'
                     '<b>{}</b>&nbsp; ATENDE ao que foi solicitado (homologada)<br/>'
                     '<b>{}</b>&nbsp; NÃO ATENDE ao que foi solicitado (não homologada)').format(m_sim, m_nao)
            t = Table([[Paragraph(texto, _E_TXT)]], colWidths=[largura])
            t.setStyle(TableStyle([
                ('BOX', (0, 0), (-1, -1), 0.8, _c(_HEX_AZUL)),
                ('BACKGROUND', (0, 0), (-1, -1), _c(_HEX_AZUL_CLARO)),
                ('LEFTPADDING', (0, 0), (-1, -1), 10),
                ('TOPPADDING', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ]))
            atual.append(Spacer(1, 0.1 * cm))
            atual.append(t)
            atual.append(Spacer(1, 0.1 * cm))

        elif tipo == 'assinaturas':
            celulas = []
            for papel, nome, cargo in bloco[1]:
                txt = '_____________________________________<br/><b>{}</b><br/>{}<br/><font color="#{}">{}</font>'.format(
                    escape(nome or ' '), escape(cargo or ' '), _HEX_CINZA, escape(papel))
                celulas.append(Paragraph(txt, _E_ASS))
            primeira = bloco[2] if len(bloco) > 2 else True
            while len(celulas) < 2:
                celulas.append('')
            t = Table([celulas], colWidths=[largura / 2.0] * 2)
            t.setStyle(TableStyle([('TOPPADDING', (0, 0), (-1, -1), 36), ('VALIGN', (0, 0), (-1, -1), 'TOP')]))
            itens = [Spacer(1, 0.4 * cm), _p('Brasília/DF, ____ de ______________ de ______.', _E_ASS), t] if primeira else [t]
            atual.append(KeepTogether(itens))

        elif tipo == 'nota':
            atual.append(Spacer(1, 0.15 * cm))
            atual.append(_p(bloco[1], _E_NOTA))

        if tipo == 'secao':
            pendente = atual
        elif pendente:
            hist.append(KeepTogether(pendente + atual))
            pendente = []
        else:
            hist.extend(atual)
    hist.extend(pendente)

    def _rodape(canvas, d):
        canvas.saveState()
        canvas.setFont('Helvetica', 7)
        canvas.setFillColor(_c(_HEX_CINZA))
        canvas.drawString(d.leftMargin, 0.9 * cm, rodape[:150])
        canvas.drawRightString(A4[0] - d.rightMargin, 0.9 * cm, 'Página {}'.format(d.page))
        canvas.restoreState()

    doc.build(hist, onFirstPage=_rodape, onLaterPages=_rodape)
    return buffer.getvalue()


# =========================================================================
# WORD
# =========================================================================
def _sombrear(celula, hex6):
    tc_pr = celula._tc.get_or_add_tcPr()
    shd = OxmlElement('w:shd')
    shd.set(qn('w:val'), 'clear')
    shd.set(qn('w:color'), 'auto')
    shd.set(qn('w:fill'), hex6)
    tc_pr.append(shd)


def _run(paragrafo, texto, negrito=False, tamanho=10, cor=None, italico=False):
    r = paragrafo.add_run(texto)
    r.bold = negrito
    r.italic = italico
    r.font.size = Pt(tamanho)
    r.font.name = 'Calibri'
    if cor:
        r.font.color.rgb = RGBColor.from_string(cor)
    return r


def _espaco(paragrafo, antes=0, depois=2):
    paragrafo.paragraph_format.space_before = Pt(antes)
    paragrafo.paragraph_format.space_after = Pt(depois)


def _separador(doc, tamanho=4):
    """Parágrafo mínimo entre tabelas (o Word junta tabelas coladas)."""
    p = doc.add_paragraph()
    _espaco(p, 0, 0)
    p.paragraph_format.line_spacing = Pt(tamanho)
    r = p.add_run(' ')
    r.font.size = Pt(tamanho)
    return p


def _larguras(tabela, larguras_cm):
    tabela.autofit = False
    for i, largura in enumerate(larguras_cm):
        tabela.columns[i].width = Cm(largura)
        for cel in tabela.columns[i].cells:
            cel.width = Cm(largura)


def _campo_pagina(paragrafo):
    """Insere o campo PAGE (número da página) no rodapé."""
    run = paragrafo.add_run()
    for tipo, texto in (('begin', None), (None, 'PAGE'), ('end', None)):
        if tipo:
            el = OxmlElement('w:fldChar')
            el.set(qn('w:fldCharType'), tipo)
        else:
            el = OxmlElement('w:instrText')
            el.set(qn('xml:space'), 'preserve')
            el.text = texto
        run._r.append(el)
    run.font.size = Pt(7)


def gerar_docx(blocos, rodape):
    doc = Document()
    sec = doc.sections[0]
    sec.page_height, sec.page_width = Cm(29.7), Cm(21.0)
    sec.left_margin = sec.right_margin = Cm(2)
    sec.top_margin = sec.bottom_margin = Cm(1.8)

    estilo = doc.styles['Normal']
    estilo.font.name = 'Calibri'
    estilo.font.size = Pt(10)

    rp = sec.footer.paragraphs[0]
    _run(rp, rodape + ' · Página ', tamanho=7, cor=_HEX_CINZA)
    _campo_pagina(rp)

    for bloco in blocos:
        tipo = bloco[0]
        if tipo == 'titulo':
            p = doc.add_paragraph()
            p.alignment = WD_ALIGN_PARAGRAPH.CENTER
            _run(p, bloco[1], negrito=True, tamanho=16, cor=_HEX_AZUL)
            _espaco(p, 0, 0)
            if bloco[2]:
                p = doc.add_paragraph()
                p.alignment = WD_ALIGN_PARAGRAPH.CENTER
                _run(p, bloco[2], tamanho=9, cor=_HEX_CINZA)
                _espaco(p, 0, 10)

        elif tipo == 'secao':
            t = doc.add_table(rows=1, cols=1)
            t.alignment = WD_TABLE_ALIGNMENT.CENTER
            _larguras(t, [17.0])
            cel = t.rows[0].cells[0]
            _sombrear(cel, _HEX_AZUL)
            p = cel.paragraphs[0]
            _espaco(p, 2, 2)
            _run(p, bloco[1], negrito=True, tamanho=10.5, cor='FFFFFF')
            _separador(doc)

        elif tipo == 'campos':
            t = doc.add_table(rows=0, cols=2)
            t.style = 'Table Grid'
            for rotulo, valor in bloco[1]:
                cels = t.add_row().cells
                _sombrear(cels[0], _HEX_FUNDO)
                _run(cels[0].paragraphs[0], rotulo, negrito=True, tamanho=9, cor=_HEX_CINZA)
                _run(cels[1].paragraphs[0], str(valor), tamanho=9.5)
            _larguras(t, [6.1, 10.9])
            _separador(doc, 6)

        elif tipo == 'paragrafo':
            for trecho in str(bloco[1]).split('\n'):
                p = doc.add_paragraph()
                p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
                _run(p, trecho, tamanho=10.5)
                _espaco(p, 0, 3)

        elif tipo == 'checklist':
            for feito, texto in bloco[1]:
                p = doc.add_paragraph()
                _run(p, '☑ ' if feito else '☐ ', negrito=True, tamanho=11,
                     cor=_HEX_VERDE if feito else _HEX_CINZA)
                _run(p, texto, tamanho=10)
                _espaco(p, 0, 1)

        elif tipo == 'tabela':
            cab, linhas, pesos = bloco[1], bloco[2], bloco[3]
            total = float(sum(pesos))
            t = doc.add_table(rows=1, cols=len(cab))
            t.style = 'Table Grid'
            for i, c in enumerate(cab):
                cel = t.rows[0].cells[i]
                _sombrear(cel, _HEX_AZUL_CLARO)
                _run(cel.paragraphs[0], c, negrito=True, tamanho=9, cor=_HEX_CINZA)
            for linha in linhas:
                cels = t.add_row().cells
                for i, valor in enumerate(linha):
                    _run(cels[i].paragraphs[0], str(valor), tamanho=9)
            _larguras(t, [17.0 * p / total for p in pesos])
            _separador(doc, 6)

        elif tipo == 'declaracao':
            decisao = bloco[1]
            t = doc.add_table(rows=1, cols=1)
            t.style = 'Table Grid'
            cel = t.rows[0].cells[0]
            _sombrear(cel, _HEX_AZUL_CLARO)
            p = cel.paragraphs[0]
            _run(p, 'Declaro que recebi e verifiquei a entrega descrita neste termo e que ela:', tamanho=10.5)
            p = cel.add_paragraph()
            _run(p, '☒ ' if decisao == 'APROVADA' else '☐ ', negrito=True, tamanho=12)
            _run(p, 'ATENDE ao que foi solicitado (homologada)', tamanho=10.5, negrito=True)
            p = cel.add_paragraph()
            _run(p, '☒ ' if decisao == 'RECUSADA' else '☐ ', negrito=True, tamanho=12)
            _run(p, 'NÃO ATENDE ao que foi solicitado (não homologada)', tamanho=10.5, negrito=True)
            _separador(doc, 6)

        elif tipo == 'assinaturas':
            if len(bloco) <= 2 or bloco[2]:
                p = doc.add_paragraph()
                p.alignment = WD_ALIGN_PARAGRAPH.CENTER
                _espaco(p, 14, 26)
                _run(p, 'Brasília/DF, ____ de ______________ de ______.', tamanho=10)
            else:
                _espaco(doc.add_paragraph(), 18, 0)
            t = doc.add_table(rows=1, cols=2)
            for i, (papel, nome, cargo) in enumerate(bloco[1]):
                cel = t.rows[0].cells[i]
                p = cel.paragraphs[0]
                p.alignment = WD_ALIGN_PARAGRAPH.CENTER
                _run(p, '_________________________________', tamanho=10)
                for texto, neg, cor in ((nome, True, None), (cargo, False, None), (papel, False, _HEX_CINZA)):
                    q = cel.add_paragraph()
                    q.alignment = WD_ALIGN_PARAGRAPH.CENTER
                    _espaco(q, 0, 0)
                    _run(q, texto or ' ', negrito=neg, tamanho=9.5, cor=cor)

        elif tipo == 'nota':
            p = doc.add_paragraph()
            _espaco(p, 4, 2)
            _run(p, bloco[1], italico=True, tamanho=8, cor=_HEX_CINZA)

    buffer = BytesIO()
    doc.save(buffer)
    return buffer.getvalue()
