# utils/siscalculo_pdf.py
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from datetime import datetime
from decimal import Decimal
import os


class SiscalculoPDF:
    """Classe para gerar PDF completo dos resultados do SISCalculo"""

    def __init__(self, logo_path=None):
        self.logo_path = logo_path
        self.page_width, self.page_height = A4
        self.margin = 15 * mm

    def _criar_cabecalho(self, nome_condominio, endereco_imovel, imovel,
                         data_atualizacao, indice_nome, periodo_prescricao=''):
        """Cria o cabeçalho completo do PDF - IGUAL AO HTML"""
        elementos = []

        # Logo
        if self.logo_path and os.path.exists(self.logo_path):
            try:
                logo = Image(self.logo_path, width=50 * mm, height=12 * mm, kind='proportional')
                elementos.append(logo)
                elementos.append(Spacer(1, 3 * mm))
            except:
                pass

        styles = getSampleStyleSheet()

        # Título
        style_title = ParagraphStyle(
            'Title',
            parent=styles['Normal'],
            fontSize=16,
            textColor=colors.HexColor('#198754'),  # ✅ Verde (text-success)
            alignment=TA_LEFT,
            fontName='Helvetica-Bold',
            spaceAfter=2 * mm
        )
        elementos.append(Paragraph("Proposta Negocial EMGEA", style_title))

        # Subtítulo
        style_subtitle = ParagraphStyle(
            'Subtitle',
            parent=styles['Normal'],
            fontSize=8,
            textColor=colors.HexColor('#6c757d'),  # ✅ text-muted
            alignment=TA_LEFT,
            spaceAfter=4 * mm
        )
        subtitle = "Quitação de todos os débitos exigíveis, mediante aceitação dos encargos, inclusive honorários, conforme critérios no rodapé - JUDICIAL"
        elementos.append(Paragraph(subtitle, style_subtitle))

        # ✅ INFORMAÇÕES DO IMÓVEL - IGUAL AO HTML
        style_info = ParagraphStyle(
            'Info',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.black,
            alignment=TA_LEFT,
            spaceAfter=1 * mm
        )

        # Reclamante
        if nome_condominio:
            elementos.append(Paragraph(f"<b>Reclamante:</b> {nome_condominio}", style_info))

        # Endereço do Imóvel
        if endereco_imovel:
            elementos.append(Paragraph(f"<b>Endereço do Imóvel:</b> {endereco_imovel}", style_info))

        # Linha com Imóvel | Data | Índice
        info_linha = f"<b>Imóvel:</b> {imovel} | <b>Data de Atualização:</b> {data_atualizacao}"
        if indice_nome:
            info_linha += f" | <b>Índice:</b> {indice_nome}"
        elementos.append(Paragraph(info_linha, style_info))

        # ✅ Alert de Período de Prescrição
        if periodo_prescricao:
            style_alert = ParagraphStyle(
                'Alert',
                parent=styles['Normal'],
                fontSize=9,
                textColor=colors.HexColor('#856404'),  # Amarelo escuro
                alignment=TA_LEFT,
                spaceAfter=2 * mm,
                leftIndent=5 * mm,
                backColor=colors.HexColor('#fff3cd')  # Fundo amarelo claro
            )
            elementos.append(Spacer(1, 2 * mm))
            elementos.append(Paragraph(
                f"<b>⚠️ Atenção:</b> Foi aplicado filtro de prescrição no período <b>{periodo_prescricao}</b>. "
                f"Parcelas fora deste período foram excluídas do cálculo.",
                style_alert
            ))

        elementos.append(Spacer(1, 5 * mm))

        return elementos

    def _criar_tabela_parcelas(self, parcelas):
        """Cria a tabela com as parcelas"""
        from decimal import Decimal

        # Cabeçalho
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

        # Totais
        total_cota = Decimal('0')
        total_atm = Decimal('0')
        total_juros = Decimal('0')
        total_multa = Decimal('0')
        total_desconto = Decimal('0')
        total_soma = Decimal('0')

        # Adicionar parcelas
        for p in parcelas:
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
                self._formatar_moeda(vr_cota),
                self._formatar_percentual(p['PERC_ATUALIZACAO']),
                self._formatar_moeda(atm),
                self._formatar_moeda(juros),
                self._formatar_moeda(multa),
                self._formatar_moeda(desconto),
                self._formatar_moeda(vr_total)
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
        col_widths = [20 * mm, 15 * mm, 20 * mm, 18 * mm, 20 * mm, 18 * mm, 18 * mm, 18 * mm, 20 * mm]
        table = Table(data, colWidths=col_widths, repeatRows=1)

        # Estilo
        style = TableStyle([
            # Cabeçalho
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4a5568')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 7),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('VALIGN', (0, 0), (-1, 0), 'MIDDLE'),

            # Corpo
            ('FONTNAME', (0, 1), (-1, -2), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -2), 8),
            ('ALIGN', (0, 1), (1, -2), 'CENTER'),
            ('ALIGN', (2, 1), (-1, -2), 'RIGHT'),

            # Linha de soma
            ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#e2e8f0')),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, -1), (-1, -1), 9),
            ('ALIGN', (0, -1), (0, -1), 'CENTER'),
            ('ALIGN', (1, -1), (-1, -1), 'RIGHT'),

            # Bordas
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('LINEABOVE', (0, -1), (-1, -1), 1.5, colors.black),

            # Padding
            ('TOPPADDING', (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
            ('LEFTPADDING', (0, 0), (-1, -1), 2),
            ('RIGHTPADDING', (0, 0), (-1, -1), 2),
        ])

        table.setStyle(style)

        return table, total_soma

    def _criar_totais(self, total_soma, perc_honorarios):
        """Cria a seção de honorários e total"""
        elementos = []
        from decimal import Decimal

        if not isinstance(total_soma, Decimal):
            total_soma = Decimal(str(total_soma))
        if not isinstance(perc_honorarios, Decimal):
            perc_honorarios = Decimal(str(perc_honorarios))

        honorarios = total_soma * (perc_honorarios / Decimal('100'))
        total_final = total_soma + honorarios

        elementos.append(Spacer(1, 3 * mm))

        # Tabela de totais com 2 linhas
        data_totais = [
            [f'Honorários Advocatícios: {float(perc_honorarios):.2f}%', self._formatar_moeda(honorarios)],
            ['TOTAL:', self._formatar_moeda(total_final)]
        ]

        table_totais = Table(data_totais, colWidths=[140 * mm, 30 * mm])
        table_totais.setStyle(TableStyle([
            # Linha de Honorários (azul claro)
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#bee3f8')),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('ALIGN', (0, 0), (0, 0), 'RIGHT'),
            ('ALIGN', (1, 0), (1, 0), 'RIGHT'),

            # Linha de Total (verde claro)
            ('BACKGROUND', (0, 1), (-1, 1), colors.HexColor('#c6f6d5')),
            ('FONTNAME', (0, 1), (-1, 1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 1), (-1, 1), 12),
            ('ALIGN', (0, 1), (0, 1), 'RIGHT'),
            ('ALIGN', (1, 1), (1, 1), 'RIGHT'),

            # Bordas
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ('LINEABOVE', (0, 1), (-1, 1), 1.5, colors.black),

            # Padding
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('LEFTPADDING', (0, 0), (-1, -1), 5),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
        ]))

        elementos.append(table_totais)
        elementos.append(Spacer(1, 5 * mm))

        return elementos

    def _criar_informacoes_calculo(self, indice_nome, totais):
        """Cria a seção de informações complementares"""
        elementos = []
        styles = getSampleStyleSheet()

        # Título
        style_subtitle = ParagraphStyle(
            'Subtitle',
            parent=styles['Normal'],
            fontSize=11,
            fontName='Helvetica-Bold',
            spaceAfter=2 * mm
        )
        elementos.append(Paragraph("Informações do Cálculo", style_subtitle))

        # Estilo para informações
        style_info = ParagraphStyle(
            'Info',
            parent=styles['Normal'],
            fontSize=8,
            spaceAfter=1 * mm
        )

        # Critérios Utilizados
        elementos.append(Paragraph("<b>Critérios Utilizados:</b>", style_info))
        criterios = [
            f"<b>Correção Monetária:</b> {indice_nome} (Juros Compostos)",
            "<b>Valor Atualizado:</b> Valor da Cota × Fator Acumulado dos Índices",
            "<b>ATM:</b> Diferença entre Valor Atualizado e Valor Original (pode ser negativa em caso de deflação)",
            "<b>Juros de Mora:</b> Valor Atualizado × 1% × Meses de Atraso (Juros Simples)",
            "<b>Multa:</b> Valor Atualizado × 2% (após 10/01/2003) ou 10% (antes)",
            f"<b>Honorários:</b> {totais.get('perc_honorarios', 10):.2f}% sobre o total"
        ]

        for criterio in criterios:
            elementos.append(Paragraph(f"• {criterio}", style_info))

        elementos.append(Spacer(1, 3 * mm))

        # Resumo
        elementos.append(Paragraph("<b>Resumo:</b>", style_info))
        resumo = [
            f"<b>Total de Parcelas:</b> {totais.get('quantidade', 0)}",
            f"<b>Valor Original:</b> R$ {self._formatar_moeda(totais.get('vr_cota', 0))}",
            f"<b>Total de Encargos:</b> R$ {self._formatar_moeda(totais.get('atm', 0) + totais.get('juros', 0) + totais.get('multa', 0))}",
            f"<b>Total sem Honorários:</b> R$ {self._formatar_moeda(totais.get('total_geral', 0))}",
            f"<b>Honorários ({totais.get('perc_honorarios', 10):.2f}%):</b> R$ {self._formatar_moeda(totais.get('honorarios', 0))}",
            f"<b>Total com Honorários:</b> R$ {self._formatar_moeda(totais.get('total_final', 0))}"
        ]

        for item in resumo:
            elementos.append(Paragraph(f"• {item}", style_info))

        return elementos

    def _formatar_moeda(self, valor):
        """Formata valor como moeda brasileira"""
        if valor is None:
            return "R$ 0,00"

        from decimal import Decimal
        if isinstance(valor, Decimal):
            valor_float = float(valor)
        else:
            valor_float = float(valor)

        if valor_float < 0:
            return f"-R$ {abs(valor_float):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        else:
            return f"R$ {valor_float:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

    def _formatar_percentual(self, valor):
        """Formata valor como percentual"""
        if valor is None:
            return "0,0000%"

        from decimal import Decimal
        if isinstance(valor, Decimal):
            valor_float = float(valor) * 100
        else:
            valor_float = float(valor) * 100

        return f"{valor_float:.4f}%".replace('.', ',')

    def gerar_pdf(self, output_path, dados):
        """Gera o PDF completo com todos os dados"""
        doc = SimpleDocTemplate(
            output_path,
            pagesize=A4,
            leftMargin=self.margin,
            rightMargin=self.margin,
            topMargin=self.margin,
            bottomMargin=self.margin
        )

        elementos = []

        # Cabeçalho
        elementos.extend(self._criar_cabecalho(
            dados.get('nome_condominio', ''),
            dados.get('endereco_imovel', ''),
            dados.get('imovel', ''),
            dados.get('data_atualizacao', ''),
            dados.get('indice_nome', ''),
            dados.get('periodo_prescricao', '')
        ))

        # Tabela de parcelas
        table, total_soma = self._criar_tabela_parcelas(dados.get('parcelas', []))
        elementos.append(table)

        # Totais
        elementos.extend(self._criar_totais(
            total_soma,
            dados.get('perc_honorarios', 10)
        ))

        # Informações do Cálculo
        elementos.extend(self._criar_informacoes_calculo(
            dados.get('indice_nome', 'N/A'),
            dados.get('totais', {})
        ))

        # Gerar PDF
        doc.build(elementos)

        return output_path


def gerar_pdf_siscalculo(output_path, parcelas, totais, nome_condominio='',
                         endereco_imovel='', imovel='', data_atualizacao='',
                         indice_nome='', perc_honorarios=10, periodo_prescricao='',
                         logo_path=None):
    """Função auxiliar para gerar PDF completo"""
    pdf_generator = SiscalculoPDF(logo_path=logo_path)

    dados = {
        'nome_condominio': nome_condominio,
        'endereco_imovel': endereco_imovel,
        'imovel': imovel,
        'data_atualizacao': data_atualizacao,
        'indice_nome': indice_nome,
        'parcelas': parcelas,
        'perc_honorarios': perc_honorarios,
        'periodo_prescricao': periodo_prescricao,
        'totais': totais
    }

    return pdf_generator.gerar_pdf(output_path, dados)