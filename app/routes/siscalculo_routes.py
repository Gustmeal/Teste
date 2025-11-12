
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
from sqlalchemy import text
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

    # ✅ Capturar filtro de número de contrato da URL
    filtro_contrato = request.args.get('filtro_contrato', '').strip()

    # ✅ MODIFICADO: Buscar histórico agrupando também por ID_INDICE_ECONOMICO
    query = db.session.query(
        SiscalculoCalculos.IMOVEL,
        SiscalculoCalculos.DT_ATUALIZACAO,
        SiscalculoCalculos.ID_INDICE_ECONOMICO,  # ✅ ADICIONAR
        ParamIndicesEconomicos.DSC_INDICE_ECONOMICO,  # ✅ ADICIONAR para mostrar nome
        db.func.count(SiscalculoCalculos.DT_VENCIMENTO).label('qtd_registros'),
        db.func.sum(SiscalculoCalculos.VR_TOTAL).label('valor_total')
    ).join(
        ParamIndicesEconomicos,
        SiscalculoCalculos.ID_INDICE_ECONOMICO == ParamIndicesEconomicos.ID_INDICE_ECONOMICO
    )

    # ✅ Aplicar filtro por número de contrato, se fornecido
    if filtro_contrato:
        query = query.filter(SiscalculoCalculos.IMOVEL.like(f'%{filtro_contrato}%'))

    historico = query.group_by(
        SiscalculoCalculos.IMOVEL,
        SiscalculoCalculos.DT_ATUALIZACAO,
        SiscalculoCalculos.ID_INDICE_ECONOMICO,  # ✅ ADICIONAR ao GROUP BY
        ParamIndicesEconomicos.DSC_INDICE_ECONOMICO  # ✅ ADICIONAR ao GROUP BY
    ).order_by(
        SiscalculoCalculos.DT_ATUALIZACAO.desc(),
        SiscalculoCalculos.IMOVEL
    ).all()

    return render_template('sumov/siscalculo/index.html',
                           indices=indices,
                           data_atual=data_atual,
                           historico=historico,
                           filtro_contrato=filtro_contrato)


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

        # ✅ Capturar MÊS/ANO de prescrição
        aplicar_prescricao = request.form.get('aplicar_prescricao') == 'on'
        mes_ano_prescricao_inicio = request.form.get('mes_ano_prescricao_inicio')
        mes_ano_prescricao_fim = request.form.get('mes_ano_prescricao_fim')

        print(f"[DEBUG 2] dt_atualizacao (raw): {dt_atualizacao}")
        print(f"[DEBUG 2] id_indice (raw): {id_indice}")
        print(f"[DEBUG 2] perc_honorarios (raw): {perc_honorarios}")
        print(f"[DEBUG 2] ✅ aplicar_prescricao: {aplicar_prescricao}")
        if aplicar_prescricao:
            print(f"[DEBUG 2] ✅ mes_ano_prescricao_inicio: {mes_ano_prescricao_inicio}")
            print(f"[DEBUG 2] ✅ mes_ano_prescricao_fim: {mes_ano_prescricao_fim}")

        if not dt_atualizacao or not id_indice or not perc_honorarios:
            print("[ERRO 2] Parâmetros obrigatórios não informados")
            flash('Data de atualização, índice e honorários são obrigatórios.', 'danger')
            return redirect(url_for('siscalculo.index'))

        # ✅ Validar período de prescrição se aplicado
        if aplicar_prescricao:
            if not mes_ano_prescricao_inicio or not mes_ano_prescricao_fim:
                print("[ERRO 2] Período de prescrição incompleto")
                flash('Informe o período completo da prescrição (mês/ano início e fim).', 'danger')
                return redirect(url_for('siscalculo.index'))

        # Converter parâmetros
        print("\n[DEBUG 3] Convertendo parâmetros...")
        try:
            dt_atualizacao = datetime.strptime(dt_atualizacao, '%Y-%m-%d').date()
            id_indice = int(id_indice)
            perc_honorarios = Decimal(perc_honorarios)

            # ✅ Converter mês/ano de prescrição
            if aplicar_prescricao:
                ano_inicio, mes_inicio = map(int, mes_ano_prescricao_inicio.split('-'))
                ano_fim, mes_fim = map(int, mes_ano_prescricao_fim.split('-'))
                periodo_prescricao = f"{mes_inicio:02d}/{ano_inicio} - {mes_fim:02d}/{ano_fim}"
                print(f"[DEBUG 3] ✅ Período de prescrição: {periodo_prescricao}")
            else:
                ano_inicio = mes_inicio = ano_fim = mes_fim = None
                periodo_prescricao = None

            print(f"[DEBUG 3] dt_atualizacao (convertida): {dt_atualizacao}")
            print(f"[DEBUG 3] id_indice (convertido): {id_indice}")
            print(f"[DEBUG 3] perc_honorarios (convertido): {perc_honorarios}%")
        except Exception as e:
            print(f"[ERRO 3] Erro ao converter parâmetros: {e}")
            flash(f'Erro nos parâmetros: {str(e)}', 'danger')
            return redirect(url_for('siscalculo.index'))

        # Ler arquivo Excel
        print("\n[DEBUG 4] Iniciando leitura do arquivo Excel...")
        print(f"[DEBUG 4] Nome do arquivo: {arquivo.filename}")

        try:
            print("[DEBUG 4.1] Lendo Excel com pandas...")

            # Ler célula B1 (Número do Imóvel)
            df_imovel = pd.read_excel(arquivo, header=None, nrows=1)
            numero_imovel = str(int(df_imovel.iloc[0, 1]))
            print(f"[DEBUG 4.1] Número do Imóvel: {numero_imovel}")

            # Ler célula B2 (Nome do Condomínio)
            df_condominio = pd.read_excel(arquivo, header=None, nrows=2)
            nome_condominio = str(df_condominio.iloc[1, 1]) if pd.notna(df_condominio.iloc[1, 1]) else ''
            print(f"[DEBUG 4.1] Nome do Condomínio: {nome_condominio}")

            # Ler os dados a partir da linha 3
            df = pd.read_excel(arquivo, header=2)
            print(f"[DEBUG 4.1] Excel lido com sucesso!")
            print(f"[DEBUG 4.1] Total de linhas: {len(df)}")

        except Exception as e:
            print(f"[ERRO 4] Erro ao ler Excel: {str(e)}")
            import traceback
            traceback.print_exc()
            flash(f'Erro ao ler arquivo Excel: {str(e)}', 'danger')
            return redirect(url_for('siscalculo.index'))

        # ✅ CORREÇÃO PRINCIPAL: Limpar dados anteriores por IMOVEL + DT_ATUALIZACAO + ID_INDICE
        print(f"\n[DEBUG 5] Limpando dados anteriores do imóvel {numero_imovel} com dt_atualizacao {dt_atualizacao} e índice {id_indice}...")
        try:
            # ✅ Limpar SiscalculoDados (dados temporários) - APENAS para este imóvel e data
            deletados_dados = SiscalculoDados.query.filter_by(
                IMOVEL=numero_imovel,
                DT_ATUALIZACAO=dt_atualizacao
            ).delete()

            # ✅ Limpar SiscalculoCalculos (resultados anteriores) - FILTRANDO POR ÍNDICE
            deletados_calculos = SiscalculoCalculos.query.filter_by(
                IMOVEL=numero_imovel,
                DT_ATUALIZACAO=dt_atualizacao,
                ID_INDICE_ECONOMICO=id_indice
            ).delete()

            db.session.commit()
            print(f"[DEBUG 5] ✅ {deletados_dados} registros deletados em SiscalculoDados")
            print(f"[DEBUG 5] ✅ {deletados_calculos} registros deletados em SiscalculoCalculos (índice {id_indice})")
        except Exception as e:
            print(f"[ERRO 5] Erro ao limpar dados: {str(e)}")
            db.session.rollback()

        # Importar dados
        print(f"\n[DEBUG 6] Importando dados das parcelas...")
        registros_inseridos = 0
        registros_excluidos_prescricao = 0
        erros_insercao = 0

        for idx, row in df.iterrows():
            try:
                if idx < 3:
                    print(f"  [DEBUG {idx}] Processando linha {idx + 1}/{len(df)}:")
                    print(f"  DATA VENCIMENTO: {row['DATA VENCIMENTO']}")
                    print(f"  VALOR COTA: {row['VALOR COTA']}")

                # Converter data do vencimento
                dt_venc = pd.to_datetime(row['DATA VENCIMENTO'])

                if pd.isna(dt_venc):
                    if idx < 3:
                        print(f"  [AVISO] Data inválida, pulando linha")
                    continue

                # Validar valor
                valor = row['VALOR COTA']
                if pd.isna(valor) or valor <= 0:
                    if idx < 3:
                        print(f"  [AVISO] Valor inválido ({valor}), pulando linha")
                    continue

                # ✅ Verificar prescrição por MÊS/ANO
                if aplicar_prescricao:
                    data_vencimento = dt_venc.date()
                    ano_venc = data_vencimento.year
                    mes_venc = data_vencimento.month

                    chave_venc = ano_venc * 100 + mes_venc
                    chave_inicio = ano_inicio * 100 + mes_inicio
                    chave_fim = ano_fim * 100 + mes_fim

                    if chave_inicio <= chave_venc <= chave_fim:
                        if idx < 3 or registros_excluidos_prescricao < 5:
                            print(f"  [PRESCRIÇÃO] Data {data_vencimento} ({mes_venc:02d}/{ano_venc}) - EXCLUÍDA")
                        registros_excluidos_prescricao += 1
                        continue

                # Criar registro
                novo_dado = SiscalculoDados(
                    IMOVEL=numero_imovel,
                    NOME_CONDOMINIO=nome_condominio,
                    DT_VENCIMENTO=dt_venc.date(),
                    VR_COTA=Decimal(str(valor)),
                    DT_ATUALIZACAO=dt_atualizacao
                )

                db.session.add(novo_dado)
                registros_inseridos += 1

                if idx < 3:
                    print(f"  [OK] Registro adicionado")

            except Exception as e:
                print(f"  [ERRO] Erro na linha {idx}: {str(e)}")
                erros_insercao += 1
                continue

        print(f"\n[DEBUG 7] Resumo da inserção:")
        print(f"  Imóvel: {numero_imovel}")
        print(f"  Condomínio: {nome_condominio}")
        print(f"  Total linhas Excel: {len(df)}")
        if aplicar_prescricao:
            print(f"  ✅ Período prescrição: {periodo_prescricao}")
        print(f"  ✅ EXCLUÍDOS (prescritos): {registros_excluidos_prescricao}")
        print(f"  ✅ INSERIDOS (válidos): {registros_inseridos}")
        print(f"  Erros: {erros_insercao}")

        # Commit dos dados importados
        print("\n[DEBUG 8] Realizando commit...")
        try:
            db.session.commit()
            print("[DEBUG 8] ✅ Commit realizado!")
        except Exception as e:
            print(f"[ERRO 8] Erro no commit: {str(e)}")
            import traceback
            traceback.print_exc()
            db.session.rollback()
            flash(f'Erro ao salvar dados: {str(e)}', 'danger')
            return redirect(url_for('siscalculo.index'))

        if registros_inseridos == 0:
            if registros_excluidos_prescricao > 0:
                flash(
                    f'Atenção: Todos os {registros_excluidos_prescricao} registros estavam prescritos e foram excluídos.',
                    'warning')
            else:
                flash('Nenhum registro válido encontrado.', 'warning')
            return redirect(url_for('siscalculo.index'))

        # Mensagem sobre prescrição
        if registros_excluidos_prescricao > 0:
            flash(f'✅ {registros_excluidos_prescricao} registro(s) excluídos por prescrição ({periodo_prescricao}).', 'info')

        # Executar cálculos
        print("\n[DEBUG 9] Executando cálculos...")
        try:
            calculador = CalculadorSiscalculo(
                dt_atualizacao=dt_atualizacao,
                id_indice=id_indice,
                usuario=current_user.nome,
                perc_honorarios=perc_honorarios
            )

            resultado = calculador.executar_calculos()

            if not resultado['sucesso']:
                print(f"[ERRO 9] Erro: {resultado.get('erro')}")
                flash(f"Erro ao executar cálculos: {resultado.get('erro')}", 'danger')
                return redirect(url_for('siscalculo.index'))

            print(f"[DEBUG 9] ✅ Cálculos executados!")
            print(f"[DEBUG 9] Total: R$ {resultado.get('total_processado', 0)}")

            # Log de auditoria
            registrar_log(
                acao='processar_siscalculo',
                entidade='siscalculo',
                entidade_id=numero_imovel,
                descricao=f'Processamento SISCalculo - Imóvel {numero_imovel}',
                dados_novos={
                    'imovel': numero_imovel,
                    'dt_atualizacao': str(dt_atualizacao),
                    'id_indice': id_indice,
                    'registros_inseridos': registros_inseridos,
                    'registros_prescritos': registros_excluidos_prescricao,
                    'periodo_prescricao': periodo_prescricao or 'Não aplicado',
                    'total_processado': str(resultado.get('total_processado', 0))
                }
            )

            # Mensagem de sucesso
            mensagem_sucesso = f'Processamento concluído com sucesso! '
            mensagem_sucesso += f'{registros_inseridos} parcela(s) processada(s). '
            if registros_excluidos_prescricao > 0:
                mensagem_sucesso += f'{registros_excluidos_prescricao} parcela(s) excluída(s) por prescrição ({periodo_prescricao}). '
            mensagem_sucesso += f'Total: R$ {resultado.get("total_processado", 0):,.2f}'

            flash(mensagem_sucesso, 'success')

            # ✅ CORREÇÃO: Passar período de prescrição na URL
            return redirect(url_for('siscalculo.resultados',
                                    dt_atualizacao=dt_atualizacao.strftime('%Y-%m-%d'),
                                    imovel=numero_imovel,
                                    id_indice=id_indice,
                                    perc_honorarios=float(perc_honorarios),
                                    periodo_prescricao=periodo_prescricao or ''))

        except Exception as e:
            print(f"[ERRO 9] Erro nos cálculos: {str(e)}")
            import traceback
            traceback.print_exc()
            flash(f'Erro ao executar cálculos: {str(e)}', 'danger')
            return redirect(url_for('siscalculo.index'))

    except Exception as e:
        print(f"[ERRO GERAL] {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Erro no processamento: {str(e)}', 'danger')
        return redirect(url_for('siscalculo.index'))

@siscalculo_bp.route('/resultados')
@login_required
def resultados():
    """Exibe os resultados dos cálculos"""
    try:
        print("\n" + "=" * 80)
        print("ROTA /RESULTADOS CHAMADA")
        print("=" * 80)

        # Capturar parâmetros
        dt_atualizacao_str = request.args.get('dt_atualizacao')
        id_indice = request.args.get('id_indice')
        imovel = request.args.get('imovel')
        perc_honorarios = request.args.get('perc_honorarios', '10.00')
        periodo_prescricao = request.args.get('periodo_prescricao', '')  # ✅ NOVO

        print(f"[DEBUG RESULTADOS] Parâmetros recebidos:")
        print(f"  dt_atualizacao: {dt_atualizacao_str}")
        print(f"  id_indice: {id_indice}")
        print(f"  imovel: {imovel}")
        print(f"  perc_honorarios: {perc_honorarios}")
        print(f"  periodo_prescricao: {periodo_prescricao}")  # ✅ NOVO

        if not dt_atualizacao_str:
            flash('Data de atualização não informada.', 'warning')
            return redirect(url_for('siscalculo.index'))

        # Converter data
        dt_atualizacao_filtro = datetime.strptime(dt_atualizacao_str, '%Y-%m-%d').date()
        perc_honorarios = Decimal(perc_honorarios)

        # Buscar parcelas calculadas
        print(f"\n[DEBUG RESULTADOS] Buscando parcelas calculadas...")

        if imovel and id_indice:
            # Filtro completo
            parcelas = SiscalculoCalculos.query.filter_by(
                DT_ATUALIZACAO=dt_atualizacao_filtro,
                ID_INDICE_ECONOMICO=int(id_indice),
                IMOVEL=imovel
            ).order_by(SiscalculoCalculos.DT_VENCIMENTO).all()
        elif id_indice:
            # Apenas por índice e data
            parcelas = SiscalculoCalculos.query.filter_by(
                DT_ATUALIZACAO=dt_atualizacao_filtro,
                ID_INDICE_ECONOMICO=int(id_indice)
            ).order_by(SiscalculoCalculos.DT_VENCIMENTO).all()
        else:
            # Apenas por data
            parcelas = SiscalculoCalculos.query.filter_by(
                DT_ATUALIZACAO=dt_atualizacao_filtro
            ).order_by(SiscalculoCalculos.DT_VENCIMENTO).all()

        print(f"[DEBUG RESULTADOS] Total de parcelas encontradas: {len(parcelas)}")

        if not parcelas:
            flash('Nenhum cálculo encontrado.', 'warning')
            return redirect(url_for('siscalculo.index'))

        # Se não veio imóvel nos parâmetros, pegar da primeira parcela
        if not imovel and parcelas:
            imovel = parcelas[0].IMOVEL
            print(f"[DEBUG RESULTADOS] Imóvel identificado: {imovel}")

        # Buscar nome do condomínio
        nome_condominio = ''
        if imovel:
            dado = SiscalculoDados.query.filter_by(IMOVEL=imovel).first()
            nome_condominio = dado.NOME_CONDOMINIO if dado and dado.NOME_CONDOMINIO else ''
            print(f"[DEBUG RESULTADOS] Nome do Condomínio: {nome_condominio}")

        # Buscar endereço do imóvel na tabela SIFOB_ATUAL
        endereco_imovel = ''
        if imovel:
            try:
                print(f"\n[DEBUG RESULTADOS] Buscando endereço do imóvel {imovel}...")

                sql_endereco = text("""
                    SELECT TOP 1
                        [ABR_LOGR_IMO],
                        [LOGR_IMO],
                        [NU_IMO],
                        [COMPL_IMO],
                        [BAIR_IMO],
                        [CIDADE_IMO],
                        [UF_IMO]
                    FROM [EMGEA_MENSAL].[dbo].[SIFOB_ATUAL]
                    WHERE CTR = :contrato
                """)

                resultado_endereco = db.session.execute(
                    sql_endereco,
                    {'contrato': imovel}
                ).fetchone()

                if resultado_endereco:
                    # Montar endereço formatado
                    abr_logr = resultado_endereco[0] or ''
                    logr = resultado_endereco[1] or ''
                    nu = resultado_endereco[2] or ''
                    compl = resultado_endereco[3] or ''
                    bairro = resultado_endereco[4] or ''
                    cidade = resultado_endereco[5] or ''
                    uf = resultado_endereco[6] or ''

                    partes_endereco = []

                    # Parte 1: Logradouro com número
                    if abr_logr or logr:
                        logradouro = f"{abr_logr} {logr}".strip()
                        if nu:
                            logradouro += f" nº {nu}"
                        if compl:
                            logradouro += f" {compl}"
                        partes_endereco.append(logradouro)

                    # Parte 2: Bairro
                    if bairro:
                        partes_endereco.append(bairro)

                    # Parte 3: Cidade - UF
                    if cidade and uf:
                        partes_endereco.append(f"{cidade} - {uf}")
                    elif cidade:
                        partes_endereco.append(cidade)

                    endereco_imovel = ', '.join(partes_endereco)
                    print(f"[DEBUG RESULTADOS] Endereço encontrado: {endereco_imovel}")
                else:
                    print(f"[DEBUG RESULTADOS] Nenhum endereço encontrado para o imóvel {imovel}")

            except Exception as e:
                print(f"[ERRO] Erro ao buscar endereço: {str(e)}")
                import traceback
                traceback.print_exc()

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

        print(f"\n[DEBUG RESULTADOS] Totais calculados:")
        print(f"  Quantidade de parcelas: {total_parcelas}")
        print(f"  Total VR_COTA: R$ {total_vr_cota:,.2f}")
        print(f"  Total ATM: R$ {total_atm:,.2f}")
        print(f"  Total Juros: R$ {total_juros:,.2f}")
        print(f"  Total Multa: R$ {total_multa:,.2f}")
        print(f"  Total Geral: R$ {total_geral:,.2f}")
        print(f"  Honorários ({perc_honorarios}%): R$ {honorarios:,.2f}")
        print(f"  Total Final: R$ {total_final:,.2f}")

        # Buscar nome do índice
        indice = None
        if id_indice:
            indice = ParamIndicesEconomicos.query.get(int(id_indice))
            print(f"[DEBUG RESULTADOS] Índice encontrado: {indice.DSC_INDICE_ECONOMICO if indice else 'Não encontrado'}")

        print("=" * 80)
        print("RENDERIZANDO TEMPLATE DE RESULTADOS")
        print("=" * 80)

        return render_template('sumov/siscalculo/resultados.html',
                               parcelas=parcelas,
                               totais=totais,
                               dt_atualizacao=dt_atualizacao_filtro,
                               imovel=imovel,
                               nome_condominio=nome_condominio,
                               endereco_imovel=endereco_imovel,
                               indice=indice,
                               periodo_prescricao=periodo_prescricao)  # ✅ NOVO

    except Exception as e:
        print("\n" + "=" * 80)
        print("[ERRO CRÍTICO NA ROTA /RESULTADOS]")
        print(f"Erro: {str(e)}")
        import traceback
        traceback.print_exc()
        print("=" * 80)

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


@siscalculo_bp.route('/exportar_pdf')
@login_required
def exportar_pdf():
    """Exporta os resultados para PDF - VERSÃO COMPLETA"""
    dt_atualizacao = request.args.get('dt_atualizacao')
    id_indice = request.args.get('id_indice')
    imovel = request.args.get('imovel')
    perc_honorarios = request.args.get('perc_honorarios', '10')
    periodo_prescricao = request.args.get('periodo_prescricao', '')  # ✅ NOVO

    if not dt_atualizacao:
        flash('Data de atualização não informada.', 'danger')
        return redirect(url_for('siscalculo.index'))

    try:
        # Converter parâmetros
        dt_atualizacao_filtro = datetime.strptime(dt_atualizacao, '%Y-%m-%d').date()
        perc_honorarios = Decimal(perc_honorarios)

        # Buscar parcelas calculadas
        if imovel and id_indice:
            parcelas = SiscalculoCalculos.query.filter_by(
                DT_ATUALIZACAO=dt_atualizacao_filtro,
                ID_INDICE_ECONOMICO=int(id_indice),
                IMOVEL=imovel
            ).order_by(SiscalculoCalculos.DT_VENCIMENTO).all()
        elif id_indice:
            parcelas = SiscalculoCalculos.query.filter_by(
                DT_ATUALIZACAO=dt_atualizacao_filtro,
                ID_INDICE_ECONOMICO=int(id_indice)
            ).order_by(SiscalculoCalculos.DT_VENCIMENTO).all()
        else:
            parcelas = SiscalculoCalculos.query.filter_by(
                DT_ATUALIZACAO=dt_atualizacao_filtro
            ).order_by(SiscalculoCalculos.DT_VENCIMENTO).all()

        if not parcelas:
            flash('Nenhum cálculo encontrado.', 'warning')
            return redirect(url_for('siscalculo.index'))

        # Se não veio imóvel nos parâmetros, pegar da primeira parcela
        if not imovel and parcelas:
            imovel = parcelas[0].IMOVEL

        # Buscar informações complementares
        nome_condominio = ''
        endereco_imovel = ''
        indice_nome = ''

        if imovel:
            # Buscar nome do condomínio
            dado = SiscalculoDados.query.filter_by(IMOVEL=imovel).first()
            nome_condominio = dado.NOME_CONDOMINIO if dado and dado.NOME_CONDOMINIO else ''

            # Buscar endereço do imóvel
            try:
                sql_endereco = text("""
                    SELECT TOP 1
                        [ABR_LOGR_IMO],
                        [LOGR_IMO],
                        [NU_IMO],
                        [COMPL_IMO],
                        [BAIR_IMO],
                        [CIDADE_IMO],
                        [UF_IMO]
                    FROM [EMGEA_MENSAL].[dbo].[SIFOB_ATUAL]
                    WHERE CTR = :contrato
                """)

                resultado = db.session.execute(sql_endereco, {'contrato': imovel}).fetchone()

                if resultado:
                    partes_endereco = []
                    if resultado[0] and resultado[1]:
                        partes_endereco.append(f"{resultado[0]} {resultado[1]}")
                    if resultado[2]:
                        partes_endereco.append(f" nº {resultado[2]}")
                    if resultado[3]:
                        partes_endereco.append(f" {resultado[3]}")
                    if resultado[4]:
                        partes_endereco.append(f", {resultado[4]}")
                    if resultado[5]:
                        partes_endereco.append(f", {resultado[5]}")
                    if resultado[6]:
                        partes_endereco.append(f" - {resultado[6]}")

                    endereco_imovel = ''.join(partes_endereco)
            except Exception as e:
                print(f"[AVISO] Erro ao buscar endereço: {e}")
                endereco_imovel = f"Imóvel {imovel}"

        # Buscar nome do índice
        if id_indice:
            indice = ParamIndicesEconomicos.query.get(int(id_indice))
            indice_nome = indice.DSC_INDICE_ECONOMICO if indice else 'N/A'

        # Preparar dados das parcelas para o PDF
        parcelas_pdf = []

        # Calcular totais
        total_quantidade = len(parcelas)
        total_vr_cota = Decimal('0')
        total_atm = Decimal('0')
        total_juros = Decimal('0')
        total_multa = Decimal('0')
        total_desconto = Decimal('0')
        total_geral = Decimal('0')

        for p in parcelas:
            parcelas_pdf.append({
                'DT_VENCIMENTO': p.DT_VENCIMENTO,
                'TEMPO_ATRASO': p.TEMPO_ATRASO,
                'VR_COTA': p.VR_COTA,
                'PERC_ATUALIZACAO': p.PERC_ATUALIZACAO,
                'ATM': p.ATM,
                'VR_JUROS': p.VR_JUROS,
                'VR_MULTA': p.VR_MULTA,
                'VR_DESCONTO': p.VR_DESCONTO,
                'VR_TOTAL': p.VR_TOTAL
            })

            total_vr_cota += p.VR_COTA
            total_atm += p.ATM if p.ATM else Decimal('0')
            total_juros += p.VR_JUROS if p.VR_JUROS else Decimal('0')
            total_multa += p.VR_MULTA if p.VR_MULTA else Decimal('0')
            total_desconto += p.VR_DESCONTO if p.VR_DESCONTO else Decimal('0')
            total_geral += p.VR_TOTAL if p.VR_TOTAL else Decimal('0')

        # Calcular honorários e total final
        honorarios = total_geral * (perc_honorarios / Decimal('100'))
        total_final = total_geral + honorarios

        # Montar dicionário de totais
        totais = {
            'quantidade': total_quantidade,
            'vr_cota': total_vr_cota,
            'atm': total_atm,
            'juros': total_juros,
            'multa': total_multa,
            'desconto': total_desconto,
            'total_geral': total_geral,
            'perc_honorarios': perc_honorarios,
            'honorarios': honorarios,
            'total_final': total_final
        }

        # Importar módulo de geração de PDF
        from app.utils.siscalculo_pdf import gerar_pdf_siscalculo

        # Caminho para a logo
        logo_path = os.path.join(os.path.dirname(__file__), '..', 'static', 'img', 'logo_emgea.png')

        # Criar arquivo temporário para o PDF
        import tempfile
        pdf_temp = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
        pdf_path = pdf_temp.name
        pdf_temp.close()

        # Gerar PDF
        gerar_pdf_siscalculo(
            output_path=pdf_path,
            parcelas=parcelas_pdf,
            totais=totais,
            nome_condominio=nome_condominio,
            endereco_imovel=endereco_imovel,
            imovel=imovel,
            data_atualizacao=dt_atualizacao_filtro.strftime('%d/%m/%Y'),
            indice_nome=indice_nome,
            perc_honorarios=float(perc_honorarios),
            periodo_prescricao=periodo_prescricao,  # ✅ NOVO
            logo_path=logo_path if os.path.exists(logo_path) else None
        )

        # Nome do arquivo para download
        nome_arquivo = f'proposta_negocial_{imovel}_{dt_atualizacao_filtro.strftime("%Y%m%d")}.pdf'

        # Enviar arquivo
        return send_file(
            pdf_path,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=nome_arquivo
        )

    except Exception as e:
        print(f"\n[ERRO] Erro ao gerar PDF: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Erro ao exportar PDF: {str(e)}', 'danger')
        return redirect(url_for('siscalculo.index'))