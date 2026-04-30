# app/utils/nota_tecnica_co_pdf.py
"""
Gerador de Nota Técnica (PDF) do Custo de Oportunidade EMGEA.

Tabela com 5 colunas:
  1. Pontos na Curva                 (numeração 1..105)
  2. Data                            (ex: mai/2026)
  3. Curva de Juros - Média 12 sem (a.a.) - DD.M.AAAA   (TAXA_MEDIA do pregão atual)
  4. Taxa Futura Mensal - Média 12 sem (a.m.)           (TAXA_MEDIA_MENSAL do pregão atual)
  5. Curva de Juros - Média 12 sem (a.a.) - Aprovada na Direx
                                                        (TAXA_MEDIA do último pregão COPOM)
"""
from io import BytesIO
from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
)


# Meses abreviados em PT-BR (formato 'mai/2026')
MESES_ABREV_PT = {
    1: 'jan', 2: 'fev', 3: 'mar', 4: 'abr', 5: 'mai', 6: 'jun',
    7: 'jul', 8: 'ago', 9: 'set', 10: 'out', 11: 'nov', 12: 'dez',
}


def _ano_mes_para_extenso(ano_mes_str):
    """
    Converte '202605' → 'mai/2026'.
    Retorna '-' se inválido.
    """
    if not ano_mes_str or len(ano_mes_str) != 6 or not ano_mes_str.isdigit():
        return '-'
    ano = int(ano_mes_str[:4])
    mes = int(ano_mes_str[4:])
    if mes < 1 or mes > 12:
        return '-'
    return f'{MESES_ABREV_PT[mes]}/{ano}'


def _formatar_numero_br(valor, casas=4):
    """
    Formata Decimal/float em padrão brasileiro: 1.234,5678
    Retorna '—' se valor for None.
    """
    if valor is None:
        return '—'
    try:
        return (f'{{:,.{casas}f}}'.format(float(valor))
                .replace(',', 'X').replace('.', ',').replace('X', '.'))
    except (ValueError, TypeError):
        return '—'


def _data_para_titulo_coluna(dt):
    """
    Converte date(2026, 4, 22) → '22.4.2026' (formato pedido na nota).
    """
    if not dt:
        return ''
    return f'{dt.day}.{dt.month}.{dt.year}'


class NotaTecnicaCustoOportunidadePDF:
    """Gera PDF da Nota Técnica de Custo de Oportunidade."""

    def __init__(self):
        self.page_width, self.page_height = landscape(A4)
        self.margin = 12 * mm

    # -----------------------------------------------------------------
    # API pública
    # -----------------------------------------------------------------
    def gerar(self, dt_pregao_atual, registros_atuais,
              dt_pregao_copom, registros_copom):
        """
        Gera o PDF e retorna um BytesIO pronto para envio via send_file.

        Parâmetros:
          dt_pregao_atual    : date  -> data do pregão filtrado (cabeçalho col. 3)
          registros_atuais   : list  -> 105 CustoOportunidade do pregão atual
          dt_pregao_copom    : date  -> data do último pregão marcado COPOM
          registros_copom    : list  -> CustoOportunidade do pregão COPOM
        """
        buffer = BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=landscape(A4),
            leftMargin=self.margin,
            rightMargin=self.margin,
            topMargin=self.margin,
            bottomMargin=self.margin + 8 * mm,  # espaço pro rodapé
            title='Nota Técnica - Custo de Oportunidade',
            author='Portal GEINC - EMGEA',
        )

        elementos = []
        elementos.extend(self._cabecalho(dt_pregao_atual, dt_pregao_copom))
        elementos.append(Spacer(1, 4 * mm))
        elementos.append(self._tabela(
            dt_pregao_atual, registros_atuais,
            dt_pregao_copom, registros_copom
        ))

        doc.build(
            elementos,
            onFirstPage=self._desenhar_rodape,
            onLaterPages=self._desenhar_rodape,
        )
        buffer.seek(0)
        return buffer

    # -----------------------------------------------------------------
    # Cabeçalho do documento
    # -----------------------------------------------------------------
    def _cabecalho(self, dt_pregao_atual, dt_pregao_copom):
        styles = getSampleStyleSheet()

        style_titulo = ParagraphStyle(
            'TituloNota',
            parent=styles['Title'],
            fontSize=16,
            textColor=colors.HexColor('#1e3a8a'),
            alignment=TA_CENTER,
            fontName='Helvetica-Bold',
            spaceAfter=2 * mm,
        )

        style_subtitulo = ParagraphStyle(
            'SubtituloNota',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.HexColor('#475569'),
            alignment=TA_CENTER,
            spaceAfter=4 * mm,
        )

        style_meta = ParagraphStyle(
            'MetaNota',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.HexColor('#475569'),
            alignment=TA_CENTER,
            spaceAfter=1 * mm,
        )

        elementos = [
            Paragraph('Nota Técnica - Custo de Oportunidade EMGEA',
                      style_titulo),
            Paragraph('Pontos da curva do custo de oportunidade Emgea',
                      style_subtitulo),
            Paragraph(
                f'Pregão de referência: <b>{dt_pregao_atual.strftime("%d/%m/%Y")}</b> '
                f'&nbsp;&nbsp;|&nbsp;&nbsp; '
                f'Última aprovação na Direx (COPOM): '
                f'<b>{dt_pregao_copom.strftime("%d/%m/%Y")}</b>',
                style_meta,
            ),
        ]
        return elementos

    # -----------------------------------------------------------------
    # Tabela principal (cabeçalho + linhas + estilo)
    # -----------------------------------------------------------------
    def _tabela(self, dt_pregao_atual, registros_atuais,
                dt_pregao_copom, registros_copom):
        # Indexa COPOM por ANO_MES (lookup O(1) durante o pareamento)
        mapa_copom_por_ano_mes = {
            r.ANO_MES: r for r in registros_copom if r.ANO_MES
        }

        # ---- Header das colunas ----
        styles = getSampleStyleSheet()
        style_header = ParagraphStyle(
            'TableHeader',
            parent=styles['Normal'],
            fontSize=8,
            textColor=colors.white,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold',
            leading=10,
        )

        # As colunas 3, 4 e 5 têm subtítulos extensos — usar Paragraph
        # garante quebra de linha automática dentro da célula
        h1 = Paragraph('Pontos<br/>na Curva', style_header)
        h2 = Paragraph('Data', style_header)
        h3 = Paragraph(
            f'Curva de Juros<br/>'
            f'Média das Últimas 12 semanas (a.a.)<br/>'
            f'<font size="7">{_data_para_titulo_coluna(dt_pregao_atual)}</font>',
            style_header,
        )
        h4 = Paragraph(
            'Taxa Futura Mensal<br/>'
            'Média das Últimas 12 semanas (a.m.)',
            style_header,
        )
        h5 = Paragraph(
            'Curva de Juros<br/>'
            'Média das Últimas 12 semanas (a.a.)<br/>'
            '<font size="7">Aprovada na Direx</font>',
            style_header,
        )

        dados = [[h1, h2, h3, h4, h5]]

        # ---- Linhas da tabela ----
        # Iterar sobre o pregão ATUAL (Opção A: 105 linhas do atual,
        # COPOM aparece quando o ANO_MES casa)
        for idx, reg in enumerate(registros_atuais, start=1):
            data_str = _ano_mes_para_extenso(reg.ANO_MES)

            taxa_atual = _formatar_numero_br(reg.TAXA_MEDIA, casas=4)
            taxa_atual_mensal = _formatar_numero_br(reg.TAXA_MEDIA_MENSAL, casas=4)

            # Procurar o mesmo ANO_MES no pregão COPOM
            reg_copom = mapa_copom_por_ano_mes.get(reg.ANO_MES)
            if reg_copom and reg_copom.TAXA_MEDIA is not None:
                taxa_copom = _formatar_numero_br(reg_copom.TAXA_MEDIA, casas=4)
            else:
                taxa_copom = '—'

            dados.append([
                str(idx),
                data_str,
                taxa_atual,
                taxa_atual_mensal,
                taxa_copom,
            ])

        # ---- Larguras das colunas (somam ~273mm = A4 paisagem - margens) ----
        col_widths = [
            22 * mm,   # # Pontos
            28 * mm,   # Data
            70 * mm,   # Curva Juros (a.a.) atual
            70 * mm,   # Taxa Futura (a.m.)
            70 * mm,   # Curva Juros (a.a.) Direx
        ]

        # ---- Estilo da tabela ----
        cor_header = colors.HexColor('#1e3a8a')   # azul EMGEA
        cor_zebra1 = colors.HexColor('#f8fafc')
        cor_zebra2 = colors.white
        cor_grid = colors.HexColor('#cbd5e1')
        cor_borda = colors.HexColor('#1e3a8a')

        estilo = TableStyle([
            # Cabeçalho
            ('BACKGROUND', (0, 0), (-1, 0), cor_header),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('VALIGN', (0, 0), (-1, 0), 'MIDDLE'),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('TOPPADDING', (0, 0), (-1, 0), 6),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 6),

            # Corpo - alinhamento
            ('ALIGN', (0, 1), (0, -1), 'CENTER'),    # # Pontos
            ('ALIGN', (1, 1), (1, -1), 'CENTER'),    # Data
            ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),    # Números → direita

            # Corpo - tipografia
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 8.5),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.HexColor('#0f172a')),
            ('VALIGN', (0, 1), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 1), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 4),

            # Coluna # Pontos em destaque
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
            ('TEXTCOLOR', (0, 1), (0, -1), colors.HexColor('#475569')),

            # Zebra (alternar cor das linhas)
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [cor_zebra2, cor_zebra1]),

            # Bordas
            ('GRID', (0, 0), (-1, -1), 0.4, cor_grid),
            ('BOX', (0, 0), (-1, -1), 1, cor_borda),
            ('LINEBELOW', (0, 0), (-1, 0), 1.2, cor_borda),
        ])

        tabela = Table(dados, colWidths=col_widths, repeatRows=1)
        tabela.setStyle(estilo)
        return tabela

    # -----------------------------------------------------------------
    # Rodapé (chamado em cada página)
    # -----------------------------------------------------------------
    def _desenhar_rodape(self, canvas_, doc):
        canvas_.saveState()
        canvas_.setFont('Helvetica', 8)
        canvas_.setFillColor(colors.HexColor('#64748b'))

        agora = datetime.now().strftime('%d/%m/%Y %H:%M')
        rodape_esq = f'Gerado em: {agora}'
        rodape_dir = f'Página {doc.page}'
        rodape_centro = 'Portal GEINC · EMGEA · Custo de Oportunidade'

        y = 8 * mm
        canvas_.drawString(self.margin, y, rodape_esq)
        canvas_.drawCentredString(self.page_width / 2.0, y, rodape_centro)
        canvas_.drawRightString(self.page_width - self.margin, y, rodape_dir)

        # Linha fina acima do rodapé
        canvas_.setStrokeColor(colors.HexColor('#cbd5e1'))
        canvas_.setLineWidth(0.4)
        canvas_.line(
            self.margin, y + 4 * mm,
            self.page_width - self.margin, y + 4 * mm
        )
        canvas_.restoreState()