# routes/siscalculo_routes.py
from flask import Blueprint, render_template, request, flash, redirect, url_for, jsonify, send_file
from flask_login import login_required, current_user
from app import db
from app.models.siscalculo import (
    ParamIndicesEconomicos,
    SiscalculoDados,
    SiscalculoCalculos,
    IndicadorEconomico
)
from app.utils.siscalculo_calc import CalculadorSiscalculo
from app.utils.audit import registrar_log
from datetime import datetime, date
from decimal import Decimal
import pandas as pd
import io
import os

siscalculo_bp = Blueprint('siscalculo', __name__, url_prefix='/sumov/siscalculo')


@siscalculo_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@siscalculo_bp.route('/')
@login_required
def index():
    """Página principal do SISCalculo"""
    # Buscar índices permitidos
    indices = ParamIndicesEconomicos.obter_indices_permitidos()

    # Data padrão (hoje)
    data_atual = date.today()

    # Buscar histórico de cálculos
    historico = db.session.query(
        SiscalculoCalculos.DT_ATUALIZACAO,
        db.func.count(SiscalculoCalculos.ID).label('qtd_registros'),
        db.func.sum(SiscalculoCalculos.VR_TOTAL).label('valor_total')
    ).group_by(
        SiscalculoCalculos.DT_ATUALIZACAO
    ).order_by(
        SiscalculoCalculos.DT_ATUALIZACAO.desc()
    ).limit(10).all()

    return render_template('sumov/siscalculo/index.html',
                           indices=indices,
                           data_atual=data_atual,
                           historico=historico)


@siscalculo_bp.route('/processar', methods=['POST'])
@login_required
def processar():
    """Processa o upload do Excel e realiza os cálculos"""
    try:
        # Validar arquivo
        if 'arquivo_excel' not in request.files:
            flash('Nenhum arquivo foi enviado.', 'danger')
            return redirect(url_for('siscalculo.index'))

        arquivo = request.files['arquivo_excel']
        if arquivo.filename == '':
            flash('Nenhum arquivo selecionado.', 'danger')
            return redirect(url_for('siscalculo.index'))

        # Capturar parâmetros
        dt_atualizacao = request.form.get('dt_atualizacao')
        id_indice = int(request.form.get('id_indice'))

        # Converter data
        dt_atualizacao = datetime.strptime(dt_atualizacao, '%Y-%m-%d').date()

        # Ler arquivo Excel
        try:
            df = pd.read_excel(arquivo, skiprows=1)  # Pular primeira linha se for cabeçalho

            # Mapear colunas esperadas
            # Esperamos: IMÓVEL | DATA_VENCIMENTO | VALOR
            if len(df.columns) < 3:
                flash('O arquivo deve ter pelo menos 3 colunas: Imóvel, Data Vencimento e Valor', 'danger')
                return redirect(url_for('siscalculo.index'))

            # Renomear colunas para padronizar
            df.columns = ['IMOVEL', 'DT_VENCIMENTO', 'VR_COTA'] + list(df.columns[3:])

        except Exception as e:
            flash(f'Erro ao ler arquivo Excel: {str(e)}', 'danger')
            return redirect(url_for('siscalculo.index'))

        # Limpar dados anteriores para esta data
        SiscalculoDados.limpar_dados_temporarios(dt_atualizacao)

        # Inserir dados na tabela temporária
        registros_inseridos = 0
        for idx, row in df.iterrows():
            try:
                # Converter data do vencimento
                dt_venc = pd.to_datetime(row['DT_VENCIMENTO'])
                if pd.isna(dt_venc):
                    continue

                # Criar registro
                novo_dado = SiscalculoDados(
                    IMOVEL=str(row['IMOVEL']) if not pd.isna(row['IMOVEL']) else None,
                    DT_VENCIMENTO=dt_venc.date(),
                    VR_COTA=Decimal(str(row['VR_COTA'])),
                    DT_ATUALIZACAO=dt_atualizacao,
                    USUARIO_CARGA=current_user.nome
                )
                db.session.add(novo_dado)
                registros_inseridos += 1

            except Exception as e:
                print(f"Erro na linha {idx}: {e}")
                continue

        db.session.commit()

        if registros_inseridos == 0:
            flash('Nenhum registro válido foi encontrado no arquivo.', 'warning')
            return redirect(url_for('siscalculo.index'))

        # Executar cálculos
        calculador = CalculadorSiscalculo(dt_atualizacao, id_indice, current_user.nome)
        resultado = calculador.executar_calculos()

        if resultado['sucesso']:
            flash(f'Processamento concluído! {registros_inseridos} registros importados e calculados.', 'success')

            # Registrar no log
            registrar_log(
                acao='processar_siscalculo',
                entidade='siscalculo',
                entidade_id=None,
                descricao=f'Processamento SISCalculo - {registros_inseridos} registros',
                dados_novos={
                    'dt_atualizacao': str(dt_atualizacao),
                    'id_indice': id_indice,
                    'registros': registros_inseridos,
                    'valor_total': float(resultado.get('valor_total', 0))
                }
            )

            # Redirecionar para visualização dos resultados
            return redirect(url_for('siscalculo.resultados',
                                    dt_atualizacao=dt_atualizacao,
                                    id_indice=id_indice))
        else:
            flash(f'Erro no processamento: {resultado.get("erro", "Erro desconhecido")}', 'danger')
            return redirect(url_for('siscalculo.index'))

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao processar arquivo: {str(e)}', 'danger')
        return redirect(url_for('siscalculo.index'))


@siscalculo_bp.route('/resultados')
@login_required
def resultados():
    """Visualiza os resultados dos cálculos"""
    dt_atualizacao = request.args.get('dt_atualizacao')
    id_indice = request.args.get('id_indice')

    if not dt_atualizacao:
        flash('Data de atualização não informada.', 'danger')
        return redirect(url_for('siscalculo.index'))

    # Converter string para data
    try:
        dt_atualizacao = datetime.strptime(dt_atualizacao, '%Y-%m-%d').date()
    except:
        dt_atualizacao = date.today()

    # Buscar cálculos
    query = SiscalculoCalculos.query.filter_by(DT_ATUALIZACAO=dt_atualizacao)

    if id_indice:
        query = query.filter_by(ID_INDICE_ECONOMICO=int(id_indice))

    calculos = query.order_by(
        SiscalculoCalculos.ID_INDICE_ECONOMICO,
        SiscalculoCalculos.DT_VENCIMENTO
    ).all()

    # Totalizar por índice
    totais = db.session.query(
        SiscalculoCalculos.ID_INDICE_ECONOMICO,
        ParamIndicesEconomicos.DSC_INDICE_ECONOMICO,
        db.func.sum(SiscalculoCalculos.VR_COTA).label('total_cotas'),
        db.func.sum(SiscalculoCalculos.ATM).label('total_atualizacao'),
        db.func.sum(SiscalculoCalculos.VR_JUROS).label('total_juros'),
        db.func.sum(SiscalculoCalculos.VR_MULTA).label('total_multa'),
        db.func.sum(SiscalculoCalculos.VR_TOTAL).label('total_geral')
    ).join(
        ParamIndicesEconomicos,
        SiscalculoCalculos.ID_INDICE_ECONOMICO == ParamIndicesEconomicos.ID_INDICE_ECONOMICO
    ).filter(
        SiscalculoCalculos.DT_ATUALIZACAO == dt_atualizacao
    ).group_by(
        SiscalculoCalculos.ID_INDICE_ECONOMICO,
        ParamIndicesEconomicos.DSC_INDICE_ECONOMICO
    ).all()

    # Calcular honorários (10% sobre o total)
    for total in totais:
        total.honorarios = total.total_geral * Decimal('0.10')
        total.total_final = total.total_geral + total.honorarios

    return render_template('sumov/siscalculo/resultados.html',
                           calculos=calculos,
                           totais=totais,
                           dt_atualizacao=dt_atualizacao)


@siscalculo_bp.route('/exportar_resultados')
@login_required
def exportar_resultados():
    """Exporta os resultados para Excel"""
    dt_atualizacao = request.args.get('dt_atualizacao')
    id_indice = request.args.get('id_indice')

    if not dt_atualizacao:
        flash('Data de atualização não informada.', 'danger')
        return redirect(url_for('siscalculo.index'))

    try:
        # Converter data
        dt_atualizacao = datetime.strptime(dt_atualizacao, '%Y-%m-%d').date()

        # Buscar dados
        query = SiscalculoCalculos.query.filter_by(DT_ATUALIZACAO=dt_atualizacao)

        if id_indice:
            query = query.filter_by(ID_INDICE_ECONOMICO=int(id_indice))

        calculos = query.all()

        if not calculos:
            flash('Nenhum resultado encontrado para exportar.', 'warning')
            return redirect(url_for('siscalculo.resultados',
                                    dt_atualizacao=dt_atualizacao))

        # Criar DataFrame
        dados = []
        for calc in calculos:
            # Buscar nome do índice
            indice = ParamIndicesEconomicos.query.get(calc.ID_INDICE_ECONOMICO)

            dados.append({
                'Imóvel': calc.IMOVEL or '',
                'Vencimento': calc.DT_VENCIMENTO.strftime('%d/%m/%Y'),
                'Valor Original': float(calc.VR_COTA),
                'Meses Atraso': calc.TEMPO_ATRASO,
                '% Atualização': float(calc.PERC_ATUALIZACAO) * 100 if calc.PERC_ATUALIZACAO else 0,
                'Atualização': float(calc.ATM) if calc.ATM else 0,
                'Juros': float(calc.VR_JUROS) if calc.VR_JUROS else 0,
                'Multa': float(calc.VR_MULTA) if calc.VR_MULTA else 0,
                'Desconto': float(calc.VR_DESCONTO) if calc.VR_DESCONTO else 0,
                'Total': float(calc.VR_TOTAL) if calc.VR_TOTAL else 0,
                'Índice': indice.DSC_INDICE_ECONOMICO if indice else str(calc.ID_INDICE_ECONOMICO)
            })

        df = pd.DataFrame(dados)

        # Criar arquivo Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # Aba de detalhamento
            df.to_excel(writer, sheet_name='Detalhamento', index=False)

            # Criar aba de resumo
            resumo = df.groupby('Índice').agg({
                'Valor Original': 'sum',
                'Atualização': 'sum',
                'Juros': 'sum',
                'Multa': 'sum',
                'Desconto': 'sum',
                'Total': 'sum'
            }).round(2)

            # Adicionar honorários (10%)
            resumo['Honorários'] = (resumo['Total'] * 0.10).round(2)
            resumo['Total Final'] = resumo['Total'] + resumo['Honorários']

            resumo.to_excel(writer, sheet_name='Resumo')

            # Formatar planilha
            workbook = writer.book
            worksheet_det = writer.sheets['Detalhamento']
            worksheet_res = writer.sheets['Resumo']

            # Formato moeda
            money_fmt = workbook.add_format({'num_format': 'R$ #,##0.00'})
            perc_fmt = workbook.add_format({'num_format': '0.00%'})

            # Aplicar formatação
            worksheet_det.set_column('C:C', 15, money_fmt)  # Valor Original
            worksheet_det.set_column('E:E', 12, perc_fmt)  # % Atualização
            worksheet_det.set_column('F:J', 15, money_fmt)  # Valores monetários

            worksheet_res.set_column('B:I', 15, money_fmt)  # Todos os valores

        output.seek(0)

        # Nome do arquivo
        nome_arquivo = f'siscalculo_{dt_atualizacao.strftime("%Y%m%d")}.xlsx'

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=nome_arquivo
        )

    except Exception as e:
        flash(f'Erro ao exportar resultados: {str(e)}', 'danger')
        return redirect(url_for('siscalculo.index'))


@siscalculo_bp.route('/comparar_indices')
@login_required
def comparar_indices():
    """Compara resultados entre diferentes índices"""
    dt_atualizacao = request.args.get('dt_atualizacao')

    if not dt_atualizacao:
        flash('Data de atualização não informada.', 'danger')
        return redirect(url_for('siscalculo.index'))

    try:
        dt_atualizacao = datetime.strptime(dt_atualizacao, '%Y-%m-%d').date()

        # Buscar todos os índices calculados para esta data
        comparacao = db.session.query(
            SiscalculoCalculos.ID_INDICE_ECONOMICO,
            ParamIndicesEconomicos.DSC_INDICE_ECONOMICO,
            db.func.sum(SiscalculoCalculos.VR_COTA).label('total_cotas'),
            db.func.sum(SiscalculoCalculos.ATM).label('total_atualizacao'),
            db.func.sum(SiscalculoCalculos.VR_JUROS).label('total_juros'),
            db.func.sum(SiscalculoCalculos.VR_MULTA).label('total_multa'),
            db.func.sum(SiscalculoCalculos.VR_TOTAL).label('total_geral')
        ).join(
            ParamIndicesEconomicos,
            SiscalculoCalculos.ID_INDICE_ECONOMICO == ParamIndicesEconomicos.ID_INDICE_ECONOMICO
        ).filter(
            SiscalculoCalculos.DT_ATUALIZACAO == dt_atualizacao
        ).group_by(
            SiscalculoCalculos.ID_INDICE_ECONOMICO,
            ParamIndicesEconomicos.DSC_INDICE_ECONOMICO
        ).order_by(
            db.func.sum(SiscalculoCalculos.VR_TOTAL).desc()
        ).all()

        # Identificar melhor e pior índice
        if comparacao:
            melhor_indice = comparacao[0]  # Primeiro (maior valor)
            pior_indice = comparacao[-1]  # Último (menor valor)
        else:
            melhor_indice = None
            pior_indice = None

        return render_template('sumov/siscalculo/comparacao.html',
                               comparacao=comparacao,
                               melhor_indice=melhor_indice,
                               pior_indice=pior_indice,
                               dt_atualizacao=dt_atualizacao)

    except Exception as e:
        flash(f'Erro ao comparar índices: {str(e)}', 'danger')
        return redirect(url_for('siscalculo.index'))