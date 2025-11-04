
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
def inject_datetime():
    """Injeta datetime e ano atual no contexto de todos os templates do SISCalculo"""
    return {
        'datetime': datetime,
        'current_year': datetime.utcnow().year
    }


@siscalculo_bp.route('/')
@login_required
def index():
    """Página principal do SISCalculo"""
    # Buscar índices permitidos
    indices = ParamIndicesEconomicos.obter_indices_permitidos()

    # Data padrão (hoje)
    data_atual = date.today()

    # Buscar histórico de cálculos - SEM USAR ID que não existe na tabela
    historico = db.session.query(
        SiscalculoCalculos.DT_ATUALIZACAO,
        db.func.count(SiscalculoCalculos.DT_VENCIMENTO).label('qtd_registros'),
        db.func.sum(SiscalculoCalculos.VR_TOTAL).label('valor_total')
    ).group_by(
        SiscalculoCalculos.DT_ATUALIZACAO
    ).order_by(
        SiscalculoCalculos.DT_ATUALIZACAO.desc()
    ).limit(10).all()

    # Últimas vinculações (se necessário - caso contrário, remover)
    ultimas_vinculacoes = []  # Vazio por enquanto, já que não temos vinculações no SISCalculo

    return render_template('sumov/siscalculo/index.html',
                           indices=indices,
                           data_atual=data_atual,
                           historico=historico)


@siscalculo_bp.route('/processar', methods=['POST'])
@login_required
def processar():
    """Processa o upload do Excel e realiza os cálculos"""
    print("\n" + "=" * 80)
    print("ROTA /PROCESSAR CHAMADA")
    print("=" * 80)

    try:
        print("\n[DEBUG 1] Validando arquivo recebido...")

        # Validar arquivo
        if 'arquivo_excel' not in request.files:
            print("[ERRO 1] Nenhum arquivo foi enviado no request.files")
            flash('Nenhum arquivo foi enviado.', 'danger')
            return redirect(url_for('siscalculo.index'))

        arquivo = request.files['arquivo_excel']
        print(f"[DEBUG 1] Arquivo recebido: {arquivo.filename}")

        if arquivo.filename == '':
            print("[ERRO 1] Nome do arquivo está vazio")
            flash('Nenhum arquivo selecionado.', 'danger')
            return redirect(url_for('siscalculo.index'))

        # Capturar parâmetros
        print("\n[DEBUG 2] Capturando parâmetros do formulário...")
        dt_atualizacao = request.form.get('dt_atualizacao')
        id_indice = request.form.get('id_indice')
        perc_honorarios = request.form.get('perc_honorarios')

        print(f"[DEBUG 2] dt_atualizacao (raw): {dt_atualizacao}")
        print(f"[DEBUG 2] id_indice (raw): {id_indice}")
        print(f"[DEBUG 2] perc_honorarios (raw): {perc_honorarios}")

        if not dt_atualizacao or not id_indice or not perc_honorarios:
            print("[ERRO 2] Parâmetros obrigatórios não informados")
            flash('Data de atualização, índice e honorários são obrigatórios.', 'danger')
            return redirect(url_for('siscalculo.index'))

        # Converter parâmetros
        print("\n[DEBUG 3] Convertendo parâmetros...")
        try:
            dt_atualizacao = datetime.strptime(dt_atualizacao, '%Y-%m-%d').date()
            id_indice = int(id_indice)
            perc_honorarios = Decimal(perc_honorarios)
            print(f"[DEBUG 3] dt_atualizacao (convertida): {dt_atualizacao}")
            print(f"[DEBUG 3] id_indice (convertido): {id_indice}")
            print(f"[DEBUG 3] perc_honorarios (convertido): {perc_honorarios}%")
        except Exception as e:
            print(f"[ERRO 3] Erro ao converter parâmetros: {e}")
            flash(f'Erro nos parâmetros: {str(e)}', 'danger')
            return redirect(url_for('siscalculo.index'))

        # Ler arquivo Excel - NOVO FORMATO
        print("\n[DEBUG 4] Iniciando leitura do arquivo Excel - NOVO FORMATO...")
        print(f"[DEBUG 4] Nome do arquivo: {arquivo.filename}")
        print(f"[DEBUG 4] Content-Type: {arquivo.content_type}")

        try:
            print("[DEBUG 4.1] Lendo Excel com pandas...")

            # Ler célula B1 (Número do Imóvel)
            df_imovel = pd.read_excel(arquivo, header=None, nrows=1)
            numero_imovel = str(int(df_imovel.iloc[0, 1]))  # Célula B1
            print(f"[DEBUG 4.1] Número do Imóvel: {numero_imovel}")

            # Ler os dados a partir da linha 3 (linha 4 no Excel, índice 3 no pandas)
            df = pd.read_excel(arquivo, header=2)  # Linha 3 é o cabeçalho (índice 2)
            print(f"[DEBUG 4.1] Excel lido! Shape: {df.shape}")
            print(f"[DEBUG 4.1] Colunas encontradas: {list(df.columns)}")

            # Validar se tem pelo menos 2 colunas
            if len(df.columns) < 2:
                print(f"[ERRO 4.1] Arquivo deve ter 2 colunas")
                flash('O arquivo deve ter 2 colunas: Data Vencimento e Valor Cota', 'danger')
                return redirect(url_for('siscalculo.index'))

            # Renomear colunas para padronizar
            print("[DEBUG 4.2] Renomeando colunas...")
            df.columns = ['DT_VENCIMENTO', 'VR_COTA'] + list(df.columns[2:])
            print(f"[DEBUG 4.2] Colunas após renomear: {list(df.columns[:2])}")

            # Remover linhas vazias
            df = df.dropna(subset=['DT_VENCIMENTO', 'VR_COTA'], how='all')

            # Mostrar primeiras linhas
            print("\n[DEBUG 4.3] Primeiras 3 linhas do DataFrame:")
            print(df[['DT_VENCIMENTO', 'VR_COTA']].head(3).to_string())

        except Exception as e:
            print(f"[ERRO 4] Erro ao ler arquivo Excel: {str(e)}")
            import traceback
            traceback.print_exc()
            flash(f'Erro ao ler arquivo Excel: {str(e)}', 'danger')
            return redirect(url_for('siscalculo.index'))

        # Limpar dados anteriores
        print("\n[DEBUG 5] Limpando dados temporários anteriores...")
        try:
            registros_deletados = SiscalculoDados.query.filter_by(
                DT_ATUALIZACAO=dt_atualizacao,
                IMOVEL=numero_imovel
            ).delete()
            db.session.commit()
            print(f"[DEBUG 5] Registros deletados: {registros_deletados}")
        except Exception as e:
            print(f"[ERRO 5] Erro ao limpar dados temporários: {str(e)}")
            db.session.rollback()

        # Inserir dados na tabela temporária
        print("\n[DEBUG 6] Inserindo dados na tabela MOV_TB030_SISCALCULO_DADOS...")
        print(f"[DEBUG 6] Imóvel: {numero_imovel}")
        registros_inseridos = 0
        erros_insercao = 0

        for idx, row in df.iterrows():
            try:
                if idx < 3 or idx % 50 == 0:
                    print(f"\n[DEBUG 6.{idx}] Processando linha {idx + 1}/{len(df)}:")
                    print(f"  DT_VENCIMENTO: {row['DT_VENCIMENTO']}")
                    print(f"  VR_COTA: {row['VR_COTA']}")

                # Converter data do vencimento - FORMATO BRASILEIRO (DD/MM/AAAA)
                dt_venc = pd.to_datetime(row['DT_VENCIMENTO'], format='%d/%m/%Y', dayfirst=True)

                if idx < 3:
                    print(f"  [DEBUG] Data convertida: {dt_venc.date()}")

                if pd.isna(dt_venc):
                    if idx < 3:
                        print(f"  [AVISO] Data de vencimento inválida, pulando linha")
                    continue

                # Validar valor
                valor = row['VR_COTA']
                if pd.isna(valor) or valor <= 0:
                    if idx < 3:
                        print(f"  [AVISO] Valor inválido ({valor}), pulando linha")
                    continue

                # Criar registro
                if idx < 3:
                    print(f"  [DEBUG] Criando objeto SiscalculoDados...")

                novo_dado = SiscalculoDados(
                    IMOVEL=numero_imovel,
                    DT_VENCIMENTO=dt_venc.date(),
                    VR_COTA=Decimal(str(valor)),
                    DT_ATUALIZACAO=dt_atualizacao
                )

                db.session.add(novo_dado)
                registros_inseridos += 1

                if idx < 3:
                    print(f"  [OK] Registro adicionado à sessão")

            except Exception as e:
                print(f"  [ERRO] Erro na linha {idx}: {str(e)}")
                import traceback
                traceback.print_exc()
                erros_insercao += 1
                continue

        print(f"\n[DEBUG 6] Resumo da inserção:")
        print(f"  Imóvel: {numero_imovel}")
        print(f"  Total de linhas no Excel: {len(df)}")
        print(f"  Registros inseridos: {registros_inseridos}")
        print(f"  Erros de inserção: {erros_insercao}")

        # Commit dos dados importados
        print("\n[DEBUG 7] Realizando commit dos dados importados...")
        try:
            db.session.commit()
            print("[DEBUG 7] Commit realizado com sucesso!")
        except Exception as e:
            print(f"[ERRO 7] Erro no commit: {str(e)}")
            import traceback
            traceback.print_exc()
            db.session.rollback()
            flash(f'Erro ao salvar dados: {str(e)}', 'danger')
            return redirect(url_for('siscalculo.index'))

        if registros_inseridos == 0:
            print("[ERRO 7] Nenhum registro válido foi inserido!")
            flash('Nenhum registro válido foi encontrado no arquivo.', 'warning')
            return redirect(url_for('siscalculo.index'))

        # Executar cálculos COM PERCENTUAL DE HONORÁRIOS
        print("\n[DEBUG 8] Iniciando execução dos cálculos...")
        print(f"[DEBUG 8] Criando CalculadorSiscalculo...")
        print(f"  - dt_atualizacao: {dt_atualizacao}")
        print(f"  - id_indice: {id_indice}")
        print(f"  - perc_honorarios: {perc_honorarios}%")
        print(f"  - usuario: {current_user.nome}")

        calculador = CalculadorSiscalculo(dt_atualizacao, id_indice, current_user.nome, perc_honorarios)
        print("[DEBUG 8] CalculadorSiscalculo criado!")

        print("[DEBUG 8] Chamando executar_calculos()...")
        resultado = calculador.executar_calculos()
        print(f"[DEBUG 8] executar_calculos() retornou: {resultado}")

        if resultado['sucesso']:
            print("[DEBUG 9] Processamento concluído com sucesso!")
            flash(f'Processamento concluído! {registros_inseridos} parcelas importadas e calculadas.', 'success')

            # Registrar no log
            try:
                registrar_log(
                    acao='processar_siscalculo',
                    entidade='siscalculo',
                    entidade_id=None,
                    descricao=f'Processamento SISCalculo - Imóvel {numero_imovel} - {registros_inseridos} parcelas - Honorários {perc_honorarios}%',
                    dados_novos={
                        'dt_atualizacao': str(dt_atualizacao),
                        'id_indice': id_indice,
                        'imovel': numero_imovel,
                        'parcelas': registros_inseridos,
                        'perc_honorarios': float(perc_honorarios),
                        'valor_total': float(resultado.get('valor_total', 0))
                    }
                )
            except Exception as e:
                print(f"[AVISO] Erro ao registrar log (não crítico): {str(e)}")

            # Redirecionar para visualização dos resultados
            print(f"[DEBUG 10] Redirecionando para resultados...")
            return redirect(url_for('siscalculo.resultados',
                                    dt_atualizacao=dt_atualizacao,
                                    id_indice=id_indice,
                                    imovel=numero_imovel,
                                    perc_honorarios=perc_honorarios))
        else:
            print(f"[ERRO 9] Erro no processamento: {resultado.get('erro', 'Erro desconhecido')}")
            flash(f'Erro no processamento: {resultado.get("erro", "Erro desconhecido")}', 'danger')
            return redirect(url_for('siscalculo.index'))

    except Exception as e:
        print("\n" + "=" * 80)
        print("[ERRO CRÍTICO] Exceção não tratada na rota /processar:")
        print(f"Erro: {str(e)}")
        import traceback
        traceback.print_exc()
        print("=" * 80)

        db.session.rollback()
        flash(f'Erro ao processar arquivo: {str(e)}', 'danger')
        return redirect(url_for('siscalculo.index'))


@siscalculo_bp.route('/resultados')
@login_required
def resultados():
    """Visualiza os resultados dos cálculos"""
    dt_atualizacao = request.args.get('dt_atualizacao')
    id_indice = request.args.get('id_indice')
    imovel = request.args.get('imovel')
    perc_honorarios = request.args.get('perc_honorarios', '10.00')

    if not dt_atualizacao:
        flash('Data de atualização não informada.', 'danger')
        return redirect(url_for('siscalculo.index'))

    try:
        # Converter data
        dt_atualizacao_filtro = datetime.strptime(dt_atualizacao, '%Y-%m-%d').date()
        perc_honorarios = Decimal(perc_honorarios)

        print(f"\n[DEBUG RESULTADOS] Buscando resultados:")
        print(f"  - dt_atualizacao: {dt_atualizacao_filtro}")
        print(f"  - id_indice: {id_indice}")
        print(f"  - imovel: {imovel}")
        print(f"  - perc_honorarios: {perc_honorarios}")

        # Buscar parcelas calculadas - PERMITIR BUSCA APENAS POR DATA
        query = SiscalculoCalculos.query.filter_by(DT_ATUALIZACAO=dt_atualizacao_filtro)

        # Aplicar filtros opcionais se fornecidos
        if id_indice:
            query = query.filter_by(ID_INDICE_ECONOMICO=int(id_indice))

        if imovel:
            query = query.filter_by(IMOVEL=imovel)

        parcelas = query.order_by(SiscalculoCalculos.DT_VENCIMENTO).all()

        print(f"[DEBUG RESULTADOS] Total de parcelas encontradas: {len(parcelas)}")

        if not parcelas:
            flash('Nenhum resultado encontrado para esta data.', 'warning')
            return redirect(url_for('siscalculo.index'))

        # Se não veio imóvel nos parâmetros, pegar da primeira parcela
        if not imovel and parcelas:
            imovel = parcelas[0].IMOVEL
            print(f"[DEBUG RESULTADOS] Imóvel identificado: {imovel}")

        # Se não veio id_indice nos parâmetros, pegar da primeira parcela
        if not id_indice and parcelas:
            id_indice = parcelas[0].ID_INDICE_ECONOMICO
            print(f"[DEBUG RESULTADOS] Índice identificado: {id_indice}")

        # Calcular totais gerais
        total_parcelas = len(parcelas)
        total_vr_cota = sum(p.VR_COTA for p in parcelas)
        total_atm = sum(p.ATM or Decimal('0') for p in parcelas)
        total_juros = sum(p.VR_JUROS or Decimal('0') for p in parcelas)
        total_multa = sum(p.VR_MULTA or Decimal('0') for p in parcelas)
        total_geral = sum(p.VR_TOTAL or Decimal('0') for p in parcelas)

        # Honorários COM PERCENTUAL PERSONALIZADO
        honorarios = total_geral * (perc_honorarios / Decimal('100'))
        total_final = total_geral + honorarios

        totais = {
            'quantidade': total_parcelas,
            'vr_cota': float(total_vr_cota),
            'atm': float(total_atm),
            'juros': float(total_juros),
            'multa': float(total_multa),
            'total_geral': float(total_geral),
            'honorarios': float(honorarios),
            'total_final': float(total_final),
            'perc_honorarios': float(perc_honorarios)
        }

        # Buscar nome do índice
        indice = None
        if id_indice:
            indice = ParamIndicesEconomicos.query.get(int(id_indice))
            print(
                f"[DEBUG RESULTADOS] Índice encontrado: {indice.DSC_INDICE_ECONOMICO if indice else 'Não encontrado'}")

        return render_template('sumov/siscalculo/resultados.html',
                               parcelas=parcelas,
                               totais=totais,
                               dt_atualizacao=dt_atualizacao_filtro,
                               imovel=imovel,
                               indice=indice)

    except Exception as e:
        print(f"[ERRO] Erro ao buscar resultados: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Erro ao buscar resultados: {str(e)}', 'danger')
        return redirect(url_for('siscalculo.index'))


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