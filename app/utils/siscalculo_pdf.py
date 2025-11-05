from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image, PageBreak
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from datetime import datetime
from decimal import Decimal
import os


class SiscalculoPDF:
    """Classe para gerar PDF dos resultados do SISCalculo"""

    def __init__(self, logo_path=None):
        """
        Inicializa o gerador de PDF

        Args:
            logo_path: Caminho para a logo da Emgea (opcional)
        """
        self.logo_path = logo_path
        self.page_width, self.page_height = A4
        self.margin = 15 * mm

    def _criar_cabecalho(self, nome_condominio, endereco_imovel, data_adjudicacao,
                         valor_proposta, data_analise):
        """Cria o cabeçalho do PDF com logo e informações"""
        elementos = []

        # Logo (se existir)
        if self.logo_path and os.path.exists(self.logo_path):
            try:
                logo = Image(self.logo_path, width=60 * mm, height=15 * mm, kind='proportional')
                elementos.append(logo)
                elementos.append(Spacer(1, 5 * mm))
            except Exception as e:
                print(f"Erro ao carregar logo: {e}")

        # Estilos
        styles = getSampleStyleSheet()

        # Estilo para departamentos
        style_dept = ParagraphStyle(
            'Department',
            parent=styles['Normal'],
            fontSize=8,
            textColor=colors.HexColor('#333333'),
            alignment=TA_LEFT
        )

        # Estilo para data
        style_data = ParagraphStyle(
            'Data',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.HexColor('#333333'),
            alignment=TA_RIGHT
        )

        # Departamentos e Data
        dept_text = "Superintendência de Pessoas Físicas - SUPEF<br/>Gerência de Canais - GECAN"
        data_text = f"Brasília, {datetime.now().strftime('%d de %B de %Y')}"

        # Tabela para alinhar departamento e data
        data_table = Table(
            [[Paragraph(dept_text, style_dept), Paragraph(data_text, style_data)]],
            colWidths=[self.page_width - 2 * self.margin - 60 * mm, 60 * mm]
        )
        data_table.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
        ]))

        elementos.append(data_table)
        elementos.append(Spacer(1, 5 * mm))

        # Título
        style_title = ParagraphStyle(
            'Title',
            parent=styles['Normal'],
            fontSize=12,
            textColor=colors.black,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold',
            spaceAfter=3 * mm
        )

        elementos.append(Paragraph("<b>Proposta Negocial EMGEA</b>", style_title))

        # Subtítulo
        style_subtitle = ParagraphStyle(
            'Subtitle',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.black,
            alignment=TA_CENTER,
            spaceAfter=5 * mm
        )

        subtitle_text = "Quitação de todos os débitos exigíveis, mediante aceitação dos encargos, inclusive honorários, conforme critérios no rodapé - JUDICIAL"
        elementos.append(Paragraph(subtitle_text, style_subtitle))

        # Informações do processo
        style_info = ParagraphStyle(
            'Info',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.black,
            alignment=TA_LEFT,
            spaceAfter=1 * mm
        )

        elementos.append(Paragraph(f"<b>Reclamante:</b> {nome_condominio or 'N/A'}", style_info))
        elementos.append(Paragraph(f"<b>Endereço do Imóvel:</b> {endereco_imovel or 'N/A'}", style_info))

        # Tabela com Data Adjudicação, Valor e Data Análise
        info_data = [
            ["Data Adjudicação:", data_adjudicacao or "N/A",
             f"Valor da Proposta R$ {self._formatar_moeda(valor_proposta)}",
             "Data da Análise:", data_analise or datetime.now().strftime('%d/%m/%Y')]
        ]

        info_table = Table(info_data, colWidths=[30 * mm, 25 * mm, 50 * mm, 30 * mm, 25 * mm])
        info_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('FONTNAME', (0, 0), (0, 0), 'Helvetica-Bold'),
            ('FONTNAME', (2, 0), (2, 0), 'Helvetica-Bold'),
            ('FONTNAME', (3, 0), (3, 0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2 * mm),
            ('TOPPADDING', (0, 0), (-1, -1), 2 * mm),
        ]))

        elementos.append(Spacer(1, 3 * mm))
        elementos.append(info_table)
        elementos.append(Spacer(1, 5 * mm))

        return elementos

    def _criar_tabela_parcelas(self, parcelas):
        """Cria a tabela com as parcelas"""
        from decimal import Decimal

        # Cabeçalho da tabela
        data = [[
            'Data de\nVencimento',
            'Tempo de\nAtraso em\nMeses',
            'Valor da\nCota',
            'Percentual\nde\nAtualização',
            'Atualização\nMonetária',
            'Juros',
            'Multa',
            'Desconto',
            'Soma'
        ]]

        # Adicionar parcelas
        total_cota = Decimal('0')
        total_atm = Decimal('0')
        total_juros = Decimal('0')
        total_multa = Decimal('0')
        total_desconto = Decimal('0')
        total_soma = Decimal('0')

        for p in parcelas:
            # Converter valores para Decimal se necessário
            vr_cota = Decimal(str(p['VR_COTA'])) if not isinstance(p['VR_COTA'], Decimal) else p['VR_COTA']
            atm = Decimal(str(p['ATM'])) if not isinstance(p['ATM'], Decimal) else p['ATM']
            juros = Decimal(str(p['VR_JUROS'])) if not isinstance(p['VR_JUROS'], Decimal) else p['VR_JUROS']
            multa = Decimal(str(p['VR_MULTA'])) if not isinstance(p['VR_MULTA'], Decimal) else p['VR_MULTA']
            desconto = Decimal(str(p['VR_DESCONTO'])) if not isinstance(p['VR_DESCONTO'], Decimal) else p['VR_DESCONTO']
            vr_total = Decimal(str(p['VR_TOTAL'])) if not isinstance(p['VR_TOTAL'], Decimal) else p['VR_TOTAL']

            total_cota += vr_cota
            total_atm += atm
            total_juros += juros
            total_multa += multa
            total_desconto += desconto
            total_soma += vr_total

            data.append([
                p['DT_VENCIMENTO'].strftime('%d/%m/%Y'),
                str(p['TEMPO_ATRASO']),
                self._formatar_moeda(p['VR_COTA']),
                self._formatar_percentual(p['PERC_ATUALIZACAO']),
                self._formatar_moeda(p['ATM']),
                self._formatar_moeda(p['VR_JUROS']),
                self._formatar_moeda(p['VR_MULTA']),
                self._formatar_moeda(p['VR_DESCONTO']),
                self._formatar_moeda(p['VR_TOTAL'])
            ])

        # Linha de soma
        data.append([
            'Soma',
            '',
            self._formatar_moeda(total_cota),
            '',
            self._formatar_moeda(total_atm),
            self._formatar_moeda(total_juros),
            self._formatar_moeda(total_multa),
            self._formatar_moeda(total_desconto),
            self._formatar_moeda(total_soma)
        ])

        # Criar tabela
        col_widths = [20 * mm, 18 * mm, 20 * mm, 20 * mm, 20 * mm, 18 * mm, 18 * mm, 18 * mm, 20 * mm]
        table = Table(data, colWidths=col_widths, repeatRows=1)

        # Estilo da tabela
        style = TableStyle([
            # Cabeçalho
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4a5568')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('VALIGN', (0, 0), (-1, 0), 'MIDDLE'),

            # Corpo da tabela
            ('FONTNAME', (0, 1), (-1, -2), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -2), 8),
            ('ALIGN', (0, 1), (1, -2), 'CENTER'),
            ('ALIGN', (2, 1), (-1, -2), 'RIGHT'),

            # Linha de soma
            ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#e2e8f0')),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, -1), (-1, -1), 9),
            ('ALIGN', (0, -1), (0, -1), 'LEFT'),
            ('ALIGN', (1, -1), (-1, -1), 'RIGHT'),

            # Bordas
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('LINEABOVE', (0, -1), (-1, -1), 1.5, colors.black),

            # Padding
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ('LEFTPADDING', (0, 0), (-1, -1), 3),
            ('RIGHTPADDING', (0, 0), (-1, -1), 3),
        ])

        table.setStyle(style)

        return table, total_soma

    def _criar_totais(self, total_soma, perc_honorarios):
        """Cria a seção de honorários e total"""
        elementos = []

        # Converter para Decimal para evitar erro de tipo
        from decimal import Decimal
        if not isinstance(total_soma, Decimal):
            total_soma = Decimal(str(total_soma))
        if not isinstance(perc_honorarios, Decimal):
            perc_honorarios = Decimal(str(perc_honorarios))

        honorarios = total_soma * (perc_honorarios / Decimal('100'))
        total_final = total_soma + honorarios

        # Espaçamento
        elementos.append(Spacer(1, 3 * mm))

        # Tabela de totais
        data_totais = [
            ['', '', f'Honorários Advocatícios: {perc_honorarios:.0f}%', self._formatar_moeda(honorarios)],
            ['', '', 'TOTAL:', self._formatar_moeda(total_final)]
        ]

        table_totais = Table(data_totais, colWidths=[40 * mm, 40 * mm, 50 * mm, 30 * mm])
        table_totais.setStyle(TableStyle([
            ('FONTNAME', (2, 0), (2, 0), 'Helvetica'),
            ('FONTNAME', (2, 1), (2, 1), 'Helvetica-Bold'),
            ('FONTSIZE', (2, 0), (-1, -1), 10),
            ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
            ('FONTNAME', (3, 0), (3, 0), 'Helvetica'),
            ('FONTNAME', (3, 1), (3, 1), 'Helvetica-Bold'),
        ]))

        elementos.append(table_totais)

        return elementos

    def _criar_rodape(self):
        """Cria o rodapé com observações e critérios"""
        elementos = []

        elementos.append(Spacer(1, 10 * mm))

        styles = getSampleStyleSheet()
        style_obs = ParagraphStyle(
            'Observacao',
            parent=styles['Normal'],
            fontSize=8,
            textColor=colors.black,
            alignment=TA_LEFT,
            spaceAfter=2 * mm
        )

        # Observação
        obs_text = "<b>Observação:</b><br/>O desconto de R$ 0,00 corresponde aos valores nominais e encargos de cotas prescritas, ou seja, vencidas e não cobradas há mais de 60 meses."
        elementos.append(Paragraph(obs_text, style_obs))

        elementos.append(Spacer(1, 3 * mm))

        # Linha separadora
        from reportlab.platypus import HRFlowable
        elementos.append(HRFlowable(width="100%", thickness=0.5, color=colors.black))

        elementos.append(Spacer(1, 2 * mm))

        # Critérios
        criterios = [
            "Atualização Monetária: Pelo IGP-M, acumulado a partir do mês de vencimento.",
            "Juros: Simples de 1% ao mês, calculados sobre o valor da cota atualizada.",
            "Multa: 10% para vencimentos até 10.01.2003; 2% a partir de 11.01.2003, ambas sobre o valor da cota atualizada.",
            "Honorários: 10% sobre o total dos débitos"
        ]

        for crit in criterios:
            elementos.append(Paragraph(crit, style_obs))

        # Rodapé da página
        elementos.append(Spacer(1, 5 * mm))

        style_footer = ParagraphStyle(
            'Footer',
            parent=styles['Normal'],
            fontSize=8,
            textColor=colors.HexColor('#666666'),
            alignment=TA_CENTER
        )

        elementos.append(Paragraph("Página 1 de 1", style_footer))

        return elementos

    def _formatar_moeda(self, valor):
        """Formata valor como moeda brasileira"""
        if valor is None:
            valor = 0

        # Converter Decimal para float para formatação
        from decimal import Decimal
        if isinstance(valor, Decimal):
            valor_float = float(valor)
        else:
            valor_float = float(valor)

        # Formatar com sinal negativo se necessário
        if valor_float < 0:
            return f"-{abs(valor_float):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        else:
            return f"{valor_float:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

    def _formatar_percentual(self, valor):
        """Formata valor como percentual"""
        if valor is None:
            return "0,0000%"

        # Converter Decimal para float para formatação
        from decimal import Decimal
        if isinstance(valor, Decimal):
            valor_float = float(valor) * 100
        else:
            valor_float = float(valor) * 100

        if valor_float < 0:
            return f"{valor_float:.4f}%".replace('.', ',')
        else:
            return f"{valor_float:.4f}%".replace('.', ',')

    def gerar_pdf(self, output_path, dados):
        """
        Gera o PDF completo

        Args:
            output_path: Caminho onde o PDF será salvo
            dados: Dicionário com os dados do cálculo
                {
                    'nome_condominio': str,
                    'endereco_imovel': str,
                    'data_adjudicacao': str,
                    'valor_proposta': Decimal,
                    'data_analise': str,
                    'parcelas': [list de dict com dados das parcelas],
                    'perc_honorarios': Decimal
                }
        """
        # Criar documento
        doc = SimpleDocTemplate(
            output_path,
            pagesize=A4,
            leftMargin=self.margin,
            rightMargin=self.margin,
            topMargin=self.margin,
            bottomMargin=self.margin
        )

        # Elementos do PDF
        elementos = []

        # Cabeçalho
        elementos.extend(self._criar_cabecalho(
            dados.get('nome_condominio', ''),
            dados.get('endereco_imovel', ''),
            dados.get('data_adjudicacao', ''),
            dados.get('valor_proposta', 0),
            dados.get('data_analise', '')
        ))

        # Tabela de parcelas
        table, total_soma = self._criar_tabela_parcelas(dados.get('parcelas', []))
        elementos.append(table)

        # Totais
        elementos.extend(self._criar_totais(
            total_soma,
            dados.get('perc_honorarios', 10)
        ))

        # Rodapé
        elementos.extend(self._criar_rodape())

        # Gerar PDF
        doc.build(elementos)

        return output_path


# Função auxiliar para uso direto
def gerar_pdf_siscalculo(output_path, parcelas, nome_condominio='', endereco_imovel='',
                         data_adjudicacao='', valor_proposta=0, data_analise='',
                         perc_honorarios=10, logo_path=None):
    """
    Função auxiliar para gerar PDF do SISCalculo

    Args:
        output_path: Caminho onde o PDF será salvo
        parcelas: Lista de dicionários com os dados das parcelas
        nome_condominio: Nome do condomínio/reclamante
        endereco_imovel: Endereço do imóvel
        data_adjudicacao: Data de adjudicação
        valor_proposta: Valor total da proposta
        data_analise: Data da análise
        perc_honorarios: Percentual de honorários
        logo_path: Caminho para a logo

    Returns:
        Caminho do PDF gerado
    """
    pdf_generator = SiscalculoPDF(logo_path=logo_path)

    dados = {
        'nome_condominio': nome_condominio,
        'endereco_imovel': endereco_imovel,
        'data_adjudicacao': data_adjudicacao,
        'valor_proposta': valor_proposta,
        'data_analise': data_analise,
        'parcelas': parcelas,
        'perc_honorarios': perc_honorarios
    }

    return pdf_generator.gerar_pdf(output_path, dados)


