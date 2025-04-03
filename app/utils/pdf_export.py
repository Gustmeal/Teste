from fpdf import FPDF
import tempfile
from datetime import datetime

class PDF(FPDF):
    def header(self):
        # Logo (opcional)
        # self.image('logo.png', 10, 8, 33)

        # Fonte
        self.set_font('Arial', 'B', 15)

        # Título
        self.cell(0, 10, self.titulo, 0, 1, 'C')

        # Data de geração
        self.set_font('Arial', '', 10)
        self.cell(0, 10, f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}", 0, 1, 'R')

        # Linha
        self.line(10, 30, self.w - 10, 30)
        self.ln(10)

    def footer(self):
        # Posição a 1.5 cm do final
        self.set_y(-15)

        # Fonte
        self.set_font('Arial', 'I', 8)

        # Número da página
        self.cell(0, 10, f'Página {self.page_no()}/{{nb}}', 0, 0, 'C')

def export_to_pdf(dados, colunas, titulo, filepath):
    """
    Exporta dados para um arquivo PDF

    Args:
        dados: Lista de dicionários com os dados a serem exportados
        colunas: Lista de nomes das colunas na ordem desejada
        titulo: Título do relatório
        filepath: Caminho completo onde o arquivo será salvo
    """
    # Determinar a orientação com base no número de colunas
    orientation = 'P' if len(colunas) <= 6 else 'L'

    # Criar PDF
    pdf = PDF(orientation=orientation)
    pdf.titulo = titulo
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.alias_nb_pages()

    # Definir fonte
    pdf.set_font('Arial', 'B', 10)

    # Calcular largura das colunas
    page_width = pdf.w - 20  # Margens de 10 de cada lado
    col_width = page_width / len(colunas)

    # Cabeçalho da tabela
    pdf.set_fill_color(91, 82, 229)  # Cor de fundo: roxo primário
    pdf.set_text_color(255, 255, 255)  # Texto branco

    for col in colunas:
        pdf.cell(col_width, 10, col, 1, 0, 'C', 1)
    pdf.ln()

    # Dados
    pdf.set_font('Arial', '', 9)
    pdf.set_text_color(0, 0, 0)  # Texto preto

    # Alternar cores das linhas
    alternate = False

    for row in dados:
        # Alternar cor de fundo
        if alternate:
            pdf.set_fill_color(240, 240, 250)  # Cinza bem claro roxeado
        else:
            pdf.set_fill_color(255, 255, 255)  # Branco
        alternate = not alternate

        # Altura da linha
        line_height = 7

        # Verificar se precisa de uma nova página antes de cada linha
        if pdf.y + line_height > pdf.page_break_trigger:
            pdf.add_page(orientation)
            pdf.set_font('Arial', 'B', 10)
            pdf.set_fill_color(91, 82, 229)
            pdf.set_text_color(255, 255, 255)

            for col in colunas:
                pdf.cell(col_width, 10, col, 1, 0, 'C', 1)
            pdf.ln()

            pdf.set_font('Arial', '', 9)
            pdf.set_text_color(0, 0, 0)

        # Imprimir dados
        for col in colunas:
            value = row.get(col, '')

            # Formatar datas
            if isinstance(value, datetime):
                if value.hour == 0 and value.minute == 0 and value.second == 0:
                    value = value.strftime('%d/%m/%Y')
                else:
                    value = value.strftime('%d/%m/%Y %H:%M')

            # Formatar números
            elif isinstance(value, float) and not('ID' in col):
                value = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

            # Converter para string
            if value is None:
                value = ''
            elif not isinstance(value, str):
                value = str(value)

            # Alinhar diferente com base no tipo de coluna
            align = 'L'
            if 'ID' in col or 'Número' in col:
                align = 'C'
            elif any(term in col for term in ['Valor', 'Meta', 'Percentual', 'Quantidade']):
                align = 'R'

            pdf.cell(col_width, line_height, value, 1, 0, align, 1)

        pdf.ln()

    pdf.output(filepath)
    return filepath