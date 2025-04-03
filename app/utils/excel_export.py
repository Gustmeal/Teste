import xlsxwriter
from datetime import datetime


def export_to_excel(dados, colunas, titulo, filepath):
    """
    Exporta dados para um arquivo Excel

    Args:
        dados: Lista de dicionários com os dados a serem exportados
        colunas: Lista de nomes das colunas na ordem desejada
        titulo: Título do relatório
        filepath: Caminho completo onde o arquivo será salvo
    """
    # Criar um novo arquivo Excel
    workbook = xlsxwriter.Workbook(filepath)
    worksheet = workbook.add_worksheet()

    # Definir formatos
    header_format = workbook.add_format({
        'bold': True,
        'font_color': 'white',
        'bg_color': '#5b52e5',
        'border': 1,
        'align': 'center',
        'valign': 'vcenter'
    })

    date_format = workbook.add_format({
        'num_format': 'dd/mm/yyyy',
        'border': 1
    })

    datetime_format = workbook.add_format({
        'num_format': 'dd/mm/yyyy hh:mm',
        'border': 1
    })

    number_format = workbook.add_format({
        'num_format': '#,##0.00',
        'border': 1
    })

    integer_format = workbook.add_format({
        'num_format': '#,##0',
        'border': 1
    })

    text_format = workbook.add_format({
        'border': 1
    })

    # Adicionar título
    worksheet.merge_range(0, 0, 0, len(colunas) - 1, titulo, header_format)

    # Adicionar data de geração
    worksheet.merge_range(1, 0, 1, len(colunas) - 1,
                          f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}", text_format)

    # Escrever cabeçalho das colunas
    for col_num, col_name in enumerate(colunas):
        worksheet.write(3, col_num, col_name, header_format)
        worksheet.set_column(col_num, col_num, 15)  # Ajustar largura da coluna

    # Escrever dados
    for row_num, row_data in enumerate(dados):
        for col_num, col_name in enumerate(colunas):
            value = row_data.get(col_name, '')

            # Aplicar formato apropriado com base no tipo de dado
            if isinstance(value, datetime):
                if value.hour == 0 and value.minute == 0 and value.second == 0:
                    worksheet.write(row_num + 4, col_num, value, date_format)
                else:
                    worksheet.write(row_num + 4, col_num, value, datetime_format)
            elif isinstance(value, (int, float)) and 'ID' in col_name:
                worksheet.write(row_num + 4, col_num, value, integer_format)
            elif isinstance(value, float):
                worksheet.write(row_num + 4, col_num, value, number_format)
            else:
                worksheet.write(row_num + 4, col_num, value, text_format)

    # Ajustar largura das colunas automaticamente
    for col_num, _ in enumerate(colunas):
        worksheet.set_column(col_num, col_num, 15)

    # Adicionar filtros
    worksheet.autofilter(3, 0, 3 + len(dados), len(colunas) - 1)

    # Congelar painel para manter cabeçalhos visíveis
    worksheet.freeze_panes(4, 0)

    workbook.close()
    return filepath