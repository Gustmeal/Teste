from flask import Blueprint, render_template, request, flash, send_file, redirect, url_for, jsonify
from flask_login import login_required, current_user
from datetime import datetime
import os
import io
from functools import wraps
import openpyxl
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
import pandas as pd
import tempfile

seguro_caixa_bp = Blueprint('seguro_caixa', __name__, url_prefix='/seguro-caixa')


# Decorator para verificar se é admin ou moderador
def admin_or_moderator_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if current_user.perfil not in ['admin', 'moderador']:
            flash('Acesso negado. Apenas administradores e moderadores podem acessar esta funcionalidade.', 'danger')
            return redirect(url_for('main.index'))
        return f(*args, **kwargs)

    return decorated_function


@seguro_caixa_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@seguro_caixa_bp.route('/')
@login_required
@admin_or_moderator_required
def index():
    """Página principal do processador de Seguro Caixa"""
    return render_template('seguro_caixa/index.html')


@seguro_caixa_bp.route('/processar', methods=['POST'])
@login_required
@admin_or_moderator_required
def processar_arquivo():
    """Processa o arquivo TXT simulando importação de dados do Excel"""
    try:
        if 'arquivo' not in request.files:
            return jsonify({'erro': 'Nenhum arquivo foi enviado'}), 400

        arquivo = request.files['arquivo']
        competencia = request.form.get('competencia')

        if arquivo.filename == '':
            return jsonify({'erro': 'Nenhum arquivo selecionado'}), 400

        if not competencia:
            return jsonify({'erro': 'Competência não foi informada'}), 400

        # Converter competência para datetime
        dt_competencia = datetime.strptime(competencia, '%Y-%m-%d')

        # Ler o conteúdo do arquivo TXT
        conteudo = arquivo.read().decode('utf-8', errors='ignore')

        # Processar usando pandas para simular importação do Excel
        excel_buffer = processar_txt_como_excel(conteudo, dt_competencia)

        # Nome do arquivo de saída
        nome_arquivo = f"Premio{dt_competencia.strftime('%B%Y')}.xlsx"

        return send_file(
            excel_buffer,
            as_attachment=True,
            download_name=nome_arquivo,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        return jsonify({'erro': f'Erro ao processar arquivo: {str(e)}'}), 500


def processar_txt_como_excel(conteudo_txt, dt_competencia):
    """
    Processa o TXT como se fosse uma importação de dados do Excel
    """

    # Criar um DataFrame vazio com as colunas esperadas
    colunas_saida = [
        'COD_SUBEST', 'COD_PRODUTO', 'NUM_CONTRATO', 'NOME_TITULAR',
        'PREMIO_MIP', 'IOF_MIP', 'PRM_MIP_ATRASO', 'IOF_MIP_ATRASO',
        'PREMIO_DFI', 'IOF_DFI', 'PRM_DFI_ATRASO', 'IOF_DFI_ATRASO',
        'REMUNER_MES', 'Ref', 'Obs'
    ]

    # Processar linhas do TXT
    dados_processados = []
    linhas = conteudo_txt.strip().split('\n')

    # Dicionário para acumular valores por contrato
    contratos = {}

    for linha in linhas:
        if linha.strip():
            campos = linha.split(';')

            if len(campos) >= 16:
                try:
                    # Extrair campos conforme estrutura do TXT
                    num_contrato = campos[0].strip()
                    cod_produto = campos[3].strip()
                    cod_subest = campos[4].strip()
                    tipo = campos[5].strip()  # 1=MIP, 2=DFI
                    valor_premio = float(campos[10].strip().replace(',', '.'))
                    valor_atraso = float(campos[11].strip().replace(',', '.'))

                    # Criar chave única para o contrato
                    if num_contrato not in contratos:
                        contratos[num_contrato] = {
                            'COD_SUBEST': cod_subest,
                            'COD_PRODUTO': cod_produto,
                            'NUM_CONTRATO': num_contrato,
                            'NOME_TITULAR': '',  # Será preenchido depois
                            'PREMIO_MIP': 0.0,
                            'IOF_MIP': 0.0,
                            'PRM_MIP_ATRASO': 0.0,
                            'IOF_MIP_ATRASO': 0.0,
                            'PREMIO_DFI': 0.0,
                            'IOF_DFI': 0.0,
                            'PRM_DFI_ATRASO': 0.0,
                            'IOF_DFI_ATRASO': 0.0,
                            'REMUNER_MES': 0.0,
                            'Ref': dt_competencia.strftime('%d/%m/%Y'),
                            'Obs': ''
                        }

                    # Taxa IOF
                    taxa_iof = 0.0038

                    # Acumular valores por tipo
                    if tipo == '1':  # MIP
                        contratos[num_contrato]['PREMIO_MIP'] += valor_premio
                        contratos[num_contrato]['IOF_MIP'] += round(valor_premio * taxa_iof, 2)
                        contratos[num_contrato]['PRM_MIP_ATRASO'] += valor_atraso
                        contratos[num_contrato]['IOF_MIP_ATRASO'] += round(valor_atraso * taxa_iof, 2)
                    elif tipo == '2':  # DFI
                        contratos[num_contrato]['PREMIO_DFI'] += valor_premio
                        contratos[num_contrato]['IOF_DFI'] += round(valor_premio * taxa_iof, 2)
                        contratos[num_contrato]['PRM_DFI_ATRASO'] += valor_atraso
                        contratos[num_contrato]['IOF_DFI_ATRASO'] += round(valor_atraso * taxa_iof, 2)

                except Exception as e:
                    print(f"Erro na linha: {e}")
                    continue

    # Calcular REMUNER_MES
    for contrato in contratos.values():
        contrato['REMUNER_MES'] = round(
            contrato['PREMIO_MIP'] + contrato['PRM_MIP_ATRASO'] +
            contrato['PREMIO_DFI'] + contrato['PRM_DFI_ATRASO'], 2
        )

    # Converter para DataFrame
    df = pd.DataFrame(list(contratos.values()))

    # Criar Excel
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Premios', index=False)

        # Formatação
        workbook = writer.book
        worksheet = writer.sheets['Premios']

        # Formato para valores monetários
        money_format = workbook.add_format({'num_format': '#,##0.00'})

        # Formato para cabeçalho
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#D9E1F2',
            'border': 1,
            'align': 'center',
            'valign': 'vcenter'
        })

        # Aplicar formatação
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)

        # Formatar colunas de valores (E até M)
        for col in range(4, 13):  # Colunas E até M
            worksheet.set_column(col, col, 15, money_format)

        # Largura das colunas
        worksheet.set_column('A:A', 12)  # COD_SUBEST
        worksheet.set_column('B:B', 15)  # COD_PRODUTO
        worksheet.set_column('C:C', 18)  # NUM_CONTRATO
        worksheet.set_column('D:D', 40)  # NOME_TITULAR
        worksheet.set_column('N:N', 12)  # Ref
        worksheet.set_column('O:O', 20)  # Obs

        # Adicionar totais
        if len(df) > 0:
            row_total = len(df) + 1
            worksheet.write(row_total, 3, 'TOTAL:', header_format)

            # Fórmulas de soma
            for col in range(4, 13):
                col_letter = chr(65 + col)  # E, F, G, etc.
                formula = f'=SUM({col_letter}2:{col_letter}{row_total})'
                worksheet.write_formula(row_total, col, formula, money_format)

    output.seek(0)
    return output