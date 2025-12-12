
from flask import Blueprint, render_template, request, flash, redirect, url_for, jsonify, send_file
from flask_login import login_required, current_user
from app import db
from app.models.siscalculo import (
    ParamIndicesEconomicos,
    SiscalculoDados,
    SiscalculoCalculos,
    IndicadorEconomico,
    SiscalculoPrescricoes,
    TipoParcela
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
    indices = ParamIndicesEconomicos.obter_indices_permitidos()
    data_atual = date.today()

    # Buscar filtro por contrato
    filtro_contrato = request.args.get('contrato', '').strip()

    # Buscar histórico de processamentos (últimos 10)
    query_historico = db.session.query(
        SiscalculoCalculos.DT_ATUALIZACAO,
        SiscalculoCalculos.IMOVEL,
        SiscalculoCalculos.ID_INDICE_ECONOMICO,
        ParamIndicesEconomicos.DSC_INDICE_ECONOMICO,
        db.func.count(SiscalculoCalculos.DT_VENCIMENTO).label('qtd_registros'),  # ✅ CORRIGIDO
        db.func.sum(SiscalculoCalculos.VR_TOTAL).label('valor_total'),  # ✅ CORRIGIDO
        db.func.max(SiscalculoCalculos.PERC_HONORARIOS).label('PERC_HONORARIOS')
    ).join(
        ParamIndicesEconomicos,
        SiscalculoCalculos.ID_INDICE_ECONOMICO == ParamIndicesEconomicos.ID_INDICE_ECONOMICO
    )

    # Aplicar filtro se informado
    if filtro_contrato:
        query_historico = query_historico.filter(
            SiscalculoCalculos.IMOVEL.like(f'%{filtro_contrato}%')
        )

    historico = query_historico.group_by(
        SiscalculoCalculos.IMOVEL,
        SiscalculoCalculos.DT_ATUALIZACAO,
        SiscalculoCalculos.ID_INDICE_ECONOMICO,
        ParamIndicesEconomicos.DSC_INDICE_ECONOMICO
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
    """Processa o arquivo Excel e realiza os cálculos"""
    print("\n" + "=" * 80)
    print("INICIANDO PROCESSAMENTO SISCALCULO")
    print("=" * 80)

    # Capturar dados do formulário
    dt_atualizacao = request.form.get('dt_atualizacao')
    id_indice = request.form.get('id_indice')
    perc_honorarios = request.form.get('perc_honorarios', '10.00')
    arquivo = request.files.get('arquivo_excel')

    # ✅ Capturar parâmetros de prescrição
    aplicar_prescricao = request.form.get('aplicar_prescricao') == 'on'
    mes_ano_prescricao_inicio = request.form.get('mes_ano_prescricao_inicio')
    mes_ano_prescricao_fim = request.form.get('mes_ano_prescricao_fim')

    print(f"[DEBUG 1] Parâmetros recebidos:")
    print(f"  dt_atualizacao: {dt_atualizacao}")
    print(f"  id_indice: {id_indice}")
    print(f"  perc_honorarios: {perc_honorarios}%")
    print(f"  aplicar_prescricao: {aplicar_prescricao}")
    if aplicar_prescricao:
        print(f"  periodo_prescricao_inicio: {mes_ano_prescricao_inicio}")
        print(f"  periodo_prescricao_fim: {mes_ano_prescricao_fim}")

    # Validações básicas
    print("\n[DEBUG 2] Validando parâmetros...")
    if not dt_atualizacao or not id_indice or not arquivo:
        print("[ERRO 2] Parâmetros obrigatórios faltando")
        flash('Preencha todos os campos obrigatórios.', 'danger')
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
        print(f"[DEBUG 4.1] Colunas encontradas: {list(df.columns)}")
        print(f"[DEBUG 4.1] Total de linhas: {len(df)}")

        # ✅ NOVO: Verificar se tem coluna TIPO DA PARCELA
        tem_coluna_tipo = 'TIPO DA PARCELA' in df.columns
        print(f"[DEBUG 4.1] ✅ Coluna TIPO DA PARCELA presente: {tem_coluna_tipo}")

        if not tem_coluna_tipo:
            flash(
                'ATENÇÃO: Excel não possui a coluna "TIPO DA PARCELA". Usando tipo padrão (1 - Cota Condomínio) para todas as parcelas.',
                'warning')
            # Adicionar coluna vazia se não existir (retrocompatibilidade)
            df['TIPO DA PARCELA'] = 1  # Padrão: Cota Condomínio

    except Exception as e:
        print(f"[ERRO 4] Erro ao ler Excel: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Erro ao ler arquivo Excel: {str(e)}', 'danger')
        return redirect(url_for('siscalculo.index'))

    # ✅ CORREÇÃO PRINCIPAL: Limpar dados anteriores por IMOVEL + DT_ATUALIZACAO + ID_INDICE
    print(
        f"\n[DEBUG 5] Limpando dados anteriores do imóvel {numero_imovel} com dt_atualizacao {dt_atualizacao} e índice {id_indice}...")
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

        # ✅ NOVO: Limpar prescrições anteriores deste mesmo processamento
        deletados_prescricoes = SiscalculoPrescricoes.query.filter_by(
            IMOVEL=numero_imovel,
            DT_ATUALIZACAO=dt_atualizacao,
            ID_INDICE_ECONOMICO=id_indice
        ).delete()

        db.session.commit()
        print(f"[DEBUG 5] ✅ {deletados_dados} registros deletados em SiscalculoDados")
        print(f"[DEBUG 5] ✅ {deletados_calculos} registros deletados em SiscalculoCalculos (índice {id_indice})")
        print(f"[DEBUG 5] ✅ {deletados_prescricoes} registros deletados em SiscalculoPrescricoes")
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
                print(f"  DATA VENCIMENTO (RAW): {row['DATA VENCIMENTO']} (tipo: {type(row['DATA VENCIMENTO'])})")
                print(f"  VALOR COTA: {row['VALOR COTA']}")
                print(f"  TIPO DA PARCELA: {row.get('TIPO DA PARCELA', 1)}")

            # ✅ SOLUÇÃO 1: Converter data com formato específico e validação
            data_raw = row['DATA VENCIMENTO']

            # Verificar se é NaN/None
            if pd.isna(data_raw):
                if idx < 3:
                    print(f"  [AVISO] Data vazia, pulando linha")
                continue

            # ✅ SOLUÇÃO 2: Tentar múltiplos formatos de data
            dt_venc = None
            formatos_validos = [
                '%d/%m/%Y',  # 02/10/2022 (FORMATO BRASILEIRO - PRIORIDADE)
                '%d-%m-%Y',  # 02-10-2022
                '%m/%d/%Y'  # 10/02/2022 (formato americano como fallback)
            ]

            # Se for um número (serial do Excel), converter diretamente
            if isinstance(data_raw, (int, float)):
                # Excel serial date: número de dias desde 01/01/1900
                try:
                    dt_venc = pd.to_datetime('1899-12-30') + pd.Timedelta(days=float(data_raw))
                    if idx < 3:
                        print(f"  [INFO] Convertido de serial Excel: {data_raw} → {dt_venc.date()}")
                except:
                    if idx < 3:
                        print(f"  [ERRO] Serial inválido: {data_raw}")
                    continue

            # Se for timestamp do pandas
            elif isinstance(data_raw, pd.Timestamp):
                dt_venc = data_raw

            # Se for string, tentar formatos
            elif isinstance(data_raw, str):
                for formato in formatos_validos:
                    try:
                        dt_venc = pd.to_datetime(data_raw, format=formato, errors='raise')
                        if idx < 3:
                            print(f"  [INFO] Formato detectado: {formato}")
                        break
                    except:
                        continue

                if dt_venc is None:
                    if idx < 3:
                        print(f"  [ERRO] Formato de data não reconhecido: {data_raw}")
                    erros_insercao += 1
                    continue

            # Se não conseguiu converter
            if dt_venc is None:
                if idx < 3:
                    print(f"  [ERRO] Não foi possível converter data: {data_raw}")
                erros_insercao += 1
                continue

            # ✅ SOLUÇÃO 3: VALIDAÇÃO DE ANO RAZOÁVEL
            ano_vencimento = dt_venc.year
            ano_atual = datetime.now().year

            # Rejeitar datas muito no futuro (mais de 2 anos) ou muito no passado (antes de 1990)
            if ano_vencimento > (ano_atual + 2):
                if idx < 3:
                    print(f"  [ERRO] ⚠️ Data SUSPEITA no futuro: {dt_venc.date()} (ano {ano_vencimento}) - REJEITADA")
                erros_insercao += 1
                continue

            if ano_vencimento < 1990:
                if idx < 3:
                    print(f"  [ERRO] ⚠️ Data SUSPEITA no passado: {dt_venc.date()} (ano {ano_vencimento}) - REJEITADA")
                erros_insercao += 1
                continue

            if idx < 3:
                print(f"  ✅ Data válida: {dt_venc.date()} (ano {ano_vencimento})")

            # Validar valor
            valor = row['VALOR COTA']
            if pd.isna(valor) or valor <= 0:
                if idx < 3:
                    print(f"  [AVISO] Valor inválido ({valor}), pulando linha")
                continue

            # ✅ CRÍTICO: Ler e validar tipo da parcela - NUNCA PODE SER NULL
            tipo_parcela = row.get('TIPO DA PARCELA', 1)

            # Validar e garantir que SEMPRE tenha um valor válido
            try:
                # Se vier como string ou float
                if pd.isna(tipo_parcela) or tipo_parcela is None or str(tipo_parcela).strip() == '':
                    tipo_parcela = 1  # ✅ PADRÃO: Cota Condomínio
                else:
                    tipo_parcela = int(float(tipo_parcela))  # Converter para int

                # Validar range (1 a 5)
                if tipo_parcela not in [1, 2, 3, 4, 5]:
                    if idx < 3:
                        print(f"  [AVISO] Tipo inválido ({tipo_parcela}), usando padrão (1 - Cota Condomínio)")
                    tipo_parcela = 1

            except (ValueError, TypeError) as e:
                if idx < 3:
                    print(f"  [AVISO] Erro ao converter tipo ({tipo_parcela}), usando padrão (1 - Cota Condomínio)")
                tipo_parcela = 1

            # ✅ GARANTIA FINAL: Nunca pode ser None
            if tipo_parcela is None:
                tipo_parcela = 1

            # ✅ Verificar prescrição por MÊS/ANO
            if aplicar_prescricao:
                data_vencimento = dt_venc.date()
                ano_venc = data_vencimento.year
                mes_venc = data_vencimento.month

                chave_venc = ano_venc * 100 + mes_venc
                chave_inicio = ano_inicio * 100 + mes_inicio
                chave_fim = ano_fim * 100 + mes_fim

                if chave_inicio <= chave_venc <= chave_fim:
                    # ✅ SALVAR A PARCELA PRESCRITA NA TABELA ANTES DE EXCLUÍ-LA
                    parcela_prescrita = SiscalculoPrescricoes(
                        IMOVEL=numero_imovel,
                        NOME_CONDOMINIO=nome_condominio,
                        DT_VENCIMENTO=data_vencimento,
                        VR_COTA=Decimal(str(valor)),
                        DT_ATUALIZACAO=dt_atualizacao,
                        ID_INDICE_ECONOMICO=id_indice,
                        PERIODO_PRESCRICAO=periodo_prescricao,
                        USUARIO=current_user.nome,
                        ID_TIPO=tipo_parcela  # ✅ NOVO
                    )

                    db.session.add(parcela_prescrita)

                    if idx < 3 or registros_excluidos_prescricao < 5:
                        print(
                            f"  [PRESCRIÇÃO] Data {data_vencimento} ({mes_venc:02d}/{ano_venc}) - TIPO {tipo_parcela} - SALVA E EXCLUÍDA DO CÁLCULO")

                    registros_excluidos_prescricao += 1
                    continue  # ✅ MANTÉM A LÓGICA: NÃO INCLUI NO CÁLCULO

            # Criar registro
            novo_dado = SiscalculoDados(
                IMOVEL=numero_imovel,
                NOME_CONDOMINIO=nome_condominio,
                DT_VENCIMENTO=dt_venc.date(),
                VR_COTA=Decimal(str(valor)),
                DT_ATUALIZACAO=dt_atualizacao,
                ID_TIPO=tipo_parcela  # ✅ GARANTIDO: Sempre tem valor válido (1-5)
            )

            db.session.add(novo_dado)
            registros_inseridos += 1

            if idx < 3:
                print(f"  [OK] Registro adicionado - Data: {dt_venc.date()} - TIPO: {tipo_parcela}")

        except Exception as e:
            print(f"  [ERRO] Erro na linha {idx}: {str(e)}")
            import traceback
            traceback.print_exc()
            erros_insercao += 1
            continue

    print(f"\n[DEBUG 7] Resumo da inserção:")
    print(f"  Imóvel: {numero_imovel}")
    print(f"  Condomínio: {nome_condominio}")
    print(f"  Total linhas Excel: {len(df)}")
    if aplicar_prescricao:
        print(f"  ✅ Período prescrição: {periodo_prescricao}")
    print(f"  ✅ SALVOS NA TABELA PRESCRIÇÕES: {registros_excluidos_prescricao}")
    print(f"  ✅ INSERIDOS (válidos para cálculo): {registros_inseridos}")
    print(f"  ❌ Erros/Rejeitados: {erros_insercao}")

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
                f'Atenção: Todos os {registros_excluidos_prescricao} registros estavam prescritos e foram salvos na tabela de prescrições.',
                'warning')
        else:
            flash('Nenhum registro válido encontrado.', 'warning')
        return redirect(url_for('siscalculo.index'))

    # Mensagem sobre prescrição
    if registros_excluidos_prescricao > 0:
        flash(f'✅ {registros_excluidos_prescricao} registro(s) prescritos salvos ({periodo_prescricao}).', 'info')

    # Mensagem sobre erros
    if erros_insercao > 0:
        flash(
            f'⚠️ {erros_insercao} linha(s) com erro foram rejeitadas (datas inválidas ou fora do intervalo razoável).',
            'warning')

    # Processar cálculos
    print(f"\n[DEBUG 9] Iniciando cálculos...")
    calculador = CalculadorSiscalculo(
        dt_atualizacao=dt_atualizacao,
        id_indice=id_indice,
        perc_honorarios=perc_honorarios,
        usuario=current_user.nome,
        imovel=numero_imovel
    )

    resultado = calculador.calcular()

    if resultado['sucesso']:
        print(f"[DEBUG 9] ✅ Cálculos concluídos com sucesso!")
        flash(
            f'✅ Processamento concluído! {resultado["registros_processados"]} parcelas calculadas. Valor total: R$ {resultado["valor_total"]:,.2f}',
            'success')

        # Registrar no log
        registrar_log(
            acao='processar_siscalculo',
            entidade='siscalculo',
            entidade_id=None,
            descricao=f'Processamento SISCalculo - Imóvel: {numero_imovel}, Índice: {id_indice}',
            dados_novos={
                'imovel': numero_imovel,
                'dt_atualizacao': dt_atualizacao.strftime('%Y-%m-%d'),
                'id_indice': id_indice,
                'registros_processados': resultado['registros_processados'],
                'valor_total': resultado['valor_total']
            }
        )

        return redirect(url_for('siscalculo.resultados',
                                dt_atualizacao=dt_atualizacao.strftime('%Y-%m-%d'),
                                imovel=numero_imovel,
                                id_indice=id_indice,
                                perc_honorarios=perc_honorarios))
    else:
        print(f"[ERRO 9] Falha nos cálculos: {resultado.get('erro', 'Erro desconhecido')}")
        flash(f'Erro ao processar cálculos: {resultado.get("erro", "Erro desconhecido")}', 'danger')
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
        perc_honorarios_param = request.args.get('perc_honorarios')
        periodo_prescricao = request.args.get('periodo_prescricao', '')

        print(f"[DEBUG RESULTADOS] Parâmetros recebidos:")
        print(f"  dt_atualizacao: {dt_atualizacao_str}")
        print(f"  id_indice: {id_indice}")
        print(f"  imovel: {imovel}")
        print(f"  perc_honorarios: {perc_honorarios_param}")
        print(f"  periodo_prescricao: {periodo_prescricao}")

        if not dt_atualizacao_str:
            flash('Data de atualização não informada.', 'danger')
            return redirect(url_for('siscalculo.index'))

        # Converter data
        dt_atualizacao_filtro = datetime.strptime(dt_atualizacao_str, '%Y-%m-%d').date()

        # Buscar nome do condomínio e endereço
        print(f"\n[DEBUG RESULTADOS] Buscando dados do imóvel {imovel}...")
        nome_condominio = ''
        endereco_imovel = ''

        # Buscar nome do condomínio da primeira parcela
        primeira_parcela = SiscalculoCalculos.query.filter_by(
            DT_ATUALIZACAO=dt_atualizacao_filtro,
            IMOVEL=imovel
        ).first()

        if primeira_parcela:
            # Buscar na tabela de dados temporários
            dado_temp = SiscalculoDados.query.filter_by(
                DT_ATUALIZACAO=dt_atualizacao_filtro,
                IMOVEL=imovel
            ).first()

            if dado_temp and dado_temp.NOME_CONDOMINIO:
                nome_condominio = dado_temp.NOME_CONDOMINIO
                print(f"[DEBUG RESULTADOS] Nome do condomínio encontrado: {nome_condominio}")

        # Buscar endereço do imóvel
        try:
            with db.engine.connect() as connection:
                sql_endereco = text("""
                    SELECT TOP 1 
                        RTRIM(LTRIM(ISNULL(DS_ENDERECO, ''))) + ', ' +
                        RTRIM(LTRIM(ISNULL(NU_ENDERECO, ''))) + ' - ' +
                        RTRIM(LTRIM(ISNULL(DS_BAIRRO, ''))) + ' - ' +
                        RTRIM(LTRIM(ISNULL(DS_CIDADE, ''))) + '/' +
                        RTRIM(LTRIM(ISNULL(DS_ESTADO, ''))) AS ENDERECO_COMPLETO
                    FROM [BDG].[MOV_TB001_IMOVEL]
                    WHERE NU_IMOVEL = :imovel
                """)

                result = connection.execute(sql_endereco, {"imovel": imovel})
                row = result.fetchone()

                if row and row[0]:
                    endereco_imovel = row[0]
                    print(f"[DEBUG RESULTADOS] Endereço encontrado: {endereco_imovel}")
        except Exception as e:
            print(f"[DEBUG RESULTADOS] Erro ao buscar endereço: {str(e)}")

        # Buscar parcelas - ✅ ORDENAR POR TIPO PRIMEIRO
        print(f"\n[DEBUG RESULTADOS] Buscando parcelas...")
        parcelas = SiscalculoCalculos.query.filter_by(
            DT_ATUALIZACAO=dt_atualizacao_filtro,
            IMOVEL=imovel,
            ID_INDICE_ECONOMICO=int(id_indice)
        ).order_by(
            SiscalculoCalculos.DT_VENCIMENTO,  # ✅ CORRETO - data primeiro
            SiscalculoCalculos.ID_TIPO  # Depois tipo
        ).all()

        print(f"[DEBUG RESULTADOS] Total de parcelas encontradas: {len(parcelas)}")

        if not parcelas:
            flash('Nenhum resultado encontrado para os parâmetros informados.', 'warning')
            return redirect(url_for('siscalculo.index'))

        # ✅ NOVO: BUSCAR TOTAIS AGRUPADOS POR TIPO
        print(f"\n[DEBUG RESULTADOS] Buscando totais por tipo...")
        totais_por_tipo = db.session.query(
            SiscalculoCalculos.ID_TIPO,
            TipoParcela.DSC_TIPO,
            db.func.count(SiscalculoCalculos.DT_VENCIMENTO).label('quantidade'),
            db.func.sum(SiscalculoCalculos.VR_TOTAL).label('valor_total')
        ).join(
            TipoParcela,
            SiscalculoCalculos.ID_TIPO == TipoParcela.ID_TIPO
        ).filter(
            SiscalculoCalculos.DT_ATUALIZACAO == dt_atualizacao_filtro,
            SiscalculoCalculos.IMOVEL == imovel,
            SiscalculoCalculos.ID_INDICE_ECONOMICO == int(id_indice)
        ).group_by(
            SiscalculoCalculos.ID_TIPO,
            TipoParcela.DSC_TIPO
        ).order_by(
            SiscalculoCalculos.ID_TIPO
        ).all()

        # Converter para lista de dicionários
        totais_por_tipo_lista = [
            {
                'id_tipo': t.ID_TIPO,
                'descricao': t.DSC_TIPO,
                'quantidade': t.quantidade,
                'valor_total': float(t.valor_total) if t.valor_total else 0
            }
            for t in totais_por_tipo
        ]

        print(f"[DEBUG RESULTADOS] Totais por tipo: {len(totais_por_tipo_lista)} tipos encontrados")
        for t in totais_por_tipo_lista:
            print(f"  Tipo {t['id_tipo']}: {t['descricao']} - {t['quantidade']} parcelas - R$ {t['valor_total']:,.2f}")

        # Calcular totais
        print(f"\n[DEBUG RESULTADOS] Calculando totais gerais...")
        total_vr_cota = sum(p.VR_COTA for p in parcelas if p.VR_COTA)
        total_atm = sum(p.ATM for p in parcelas if p.ATM)
        total_juros = sum(p.VR_JUROS for p in parcelas if p.VR_JUROS)
        total_multa = sum(p.VR_MULTA for p in parcelas if p.VR_MULTA)
        total_geral = sum(p.VR_TOTAL for p in parcelas if p.VR_TOTAL)

        # Percentual de honorários
        if perc_honorarios_param:
            perc_honorarios = Decimal(perc_honorarios_param)
        else:
            # Pegar da primeira parcela
            perc_honorarios = parcelas[0].PERC_HONORARIOS if parcelas[0].PERC_HONORARIOS else Decimal('10.00')

        honorarios = total_geral * (perc_honorarios / 100)
        total_final = total_geral + honorarios

        # Criar objeto de totais
        class Totais:
            def __init__(self):
                self.quantidade = len(parcelas)
                self.vr_cota = total_vr_cota
                self.atm = total_atm
                self.juros = total_juros
                self.multa = total_multa
                self.total_geral = total_geral
                self.perc_honorarios = perc_honorarios
                self.honorarios = honorarios
                self.total_final = total_final

        totais = Totais()

        print(f"[DEBUG RESULTADOS] Totais calculados:")
        print(f"  Quantidade: {totais.quantidade}")
        print(f"  VR Cota: R$ {totais.vr_cota:,.2f}")
        print(f"  ATM: R$ {totais.atm:,.2f}")
        print(f"  Juros: R$ {totais.juros:,.2f}")
        print(f"  Multa: R$ {totais.multa:,.2f}")
        print(f"  Total Geral: R$ {totais.total_geral:,.2f}")
        print(f"  Honorários ({totais.perc_honorarios}%): R$ {totais.honorarios:,.2f}")
        print(f"  Total Final: R$ {totais.total_final:,.2f}")

        # Buscar índice
        indice = None
        if id_indice:
            indice = ParamIndicesEconomicos.query.get(int(id_indice))
            print(
                f"[DEBUG RESULTADOS] Índice encontrado: {indice.DSC_INDICE_ECONOMICO if indice else 'Não encontrado'}")

        print("=" * 80)
        print("RENDERIZANDO TEMPLATE DE RESULTADOS")
        print("=" * 80)

        return render_template('sumov/siscalculo/resultados.html',
                               parcelas=parcelas,
                               totais=totais,
                               totais_por_tipo=totais_por_tipo_lista,  # ✅ PASSAR PARA O TEMPLATE
                               dt_atualizacao=dt_atualizacao_filtro,
                               imovel=imovel,
                               nome_condominio=nome_condominio,
                               endereco_imovel=endereco_imovel,
                               indice=indice,
                               periodo_prescricao=periodo_prescricao)

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
    """
    Exporta os resultados para Excel

    MODIFICAÇÃO: Agora aceita parâmetro opcional 'imovel' para filtrar por contrato específico.
    - COM imovel: exporta apenas um contrato específico
    - SEM imovel: exporta todos os contratos da data (comportamento antigo)
    """
    dt_atualizacao = request.args.get('dt_atualizacao')
    id_indice = request.args.get('id_indice')
    imovel = request.args.get('imovel')  # ✅ NOVO: Parâmetro opcional

    # Capturar percentual de honorários
    perc_honorarios = request.args.get('perc_honorarios', '10.00')
    perc_honorarios = Decimal(perc_honorarios)

    if not dt_atualizacao:
        flash('Data de atualização não informada.', 'danger')
        return redirect(url_for('siscalculo.index'))

    try:
        # Converter data
        dt_atualizacao = datetime.strptime(dt_atualizacao, '%Y-%m-%d').date()

        # Buscar dados com filtros
        query = SiscalculoCalculos.query.filter_by(DT_ATUALIZACAO=dt_atualizacao)

        if id_indice:
            query = query.filter_by(ID_INDICE_ECONOMICO=int(id_indice))
            print(f"[EXPORTAR] Filtrando por índice: {id_indice}")

        # ✅ NOVO: Filtrar por imóvel se fornecido (retrocompatível)
        if imovel:
            query = query.filter_by(IMOVEL=imovel)
            print(f"[EXPORTAR] Filtrando por imóvel: {imovel}")
        else:
            print(f"[EXPORTAR] Exportando TODOS os contratos da data {dt_atualizacao}")

        calculos = query.all()

        if not calculos:
            flash('Nenhum resultado encontrado para exportar.', 'warning')
            return redirect(url_for('siscalculo.resultados',
                                    dt_atualizacao=dt_atualizacao.strftime('%Y-%m-%d')))

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

            # ✅ Usar o percentual correto capturado da URL
            percentual_decimal = float(perc_honorarios) / 100.0  # Converter % para decimal
            resumo['Honorários'] = (resumo['Total'] * percentual_decimal).round(2)
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

        # Nome do arquivo (incluir imóvel se filtrado)
        if imovel:
            nome_arquivo = f'siscalculo_{imovel}_{dt_atualizacao.strftime("%Y%m%d")}.xlsx'
        else:
            nome_arquivo = f'siscalculo_{dt_atualizacao.strftime("%Y%m%d")}.xlsx'

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=nome_arquivo
        )

    except Exception as e:
        print(f"[ERRO EXPORTAR] {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Erro ao exportar resultados: {str(e)}', 'danger')
        return redirect(url_for('siscalculo.index'))


@siscalculo_bp.route('/comparar_indices')
@login_required
def comparar_indices():
    """
    Compara resultados entre diferentes índices

    MODIFICAÇÃO: Agora aceita parâmetro opcional 'imovel' para filtrar por contrato específico.
    - COM imovel: compara índices de um contrato específico
    - SEM imovel: compara índices de todos os contratos processados na data (comportamento antigo)
    """
    dt_atualizacao = request.args.get('dt_atualizacao')
    imovel = request.args.get('imovel')  # ✅ NOVO: Parâmetro opcional

    if not dt_atualizacao:
        flash('Data de atualização não informada.', 'danger')
        return redirect(url_for('siscalculo.index'))

    try:
        dt_atualizacao = datetime.strptime(dt_atualizacao, '%Y-%m-%d').date()

        # Buscar todos os índices calculados para esta data
        query = db.session.query(
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
        )

        # ✅ NOVO: Filtrar por imóvel se fornecido (retrocompatível)
        if imovel:
            query = query.filter(SiscalculoCalculos.IMOVEL == imovel)
            print(f"[COMPARAR ÍNDICES] Filtrando por imóvel: {imovel}")
        else:
            print(f"[COMPARAR ÍNDICES] Comparando TODOS os contratos da data {dt_atualizacao}")

        comparacao = query.group_by(
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
                               dt_atualizacao=dt_atualizacao,
                               imovel=imovel)  # ✅ NOVO: Passar imovel para o template

    except Exception as e:
        flash(f'Erro ao comparar índices: {str(e)}', 'danger')
        return redirect(url_for('siscalculo.index'))


@siscalculo_bp.route('/exportar_pdf')
@login_required
def exportar_pdf():
    """Exporta os resultados para PDF - VERSÃO COMPLETA COM TOTAIS POR TIPO"""
    dt_atualizacao = request.args.get('dt_atualizacao')
    id_indice = request.args.get('id_indice')
    imovel = request.args.get('imovel')
    perc_honorarios_param = request.args.get('perc_honorarios')
    periodo_prescricao = request.args.get('periodo_prescricao', '')

    if not all([dt_atualizacao, id_indice, imovel]):
        flash('Informações insuficientes para gerar o PDF (Data, Imóvel e Índice são necessários).', 'danger')
        return redirect(url_for('siscalculo.index'))

    try:
        # Converter parâmetros
        dt_atualizacao_filtro = datetime.strptime(dt_atualizacao, '%Y-%m-%d').date()
        perc_honorarios = Decimal(perc_honorarios_param) if perc_honorarios_param else Decimal('10.00')

        print(f"\n[PDF] Gerando PDF para:")
        print(f"  Imóvel: {imovel}")
        print(f"  Data: {dt_atualizacao_filtro}")
        print(f"  Índice: {id_indice}")
        print(f"  Honorários: {perc_honorarios}%")

        # Buscar parcelas
        parcelas = SiscalculoCalculos.query.filter_by(
            DT_ATUALIZACAO=dt_atualizacao_filtro,
            IMOVEL=imovel,
            ID_INDICE_ECONOMICO=int(id_indice)
        ).order_by(
            SiscalculoCalculos.ID_TIPO,          # ✅ ORDENAR POR TIPO PRIMEIRO
            SiscalculoCalculos.DT_VENCIMENTO
        ).all()

        if not parcelas:
            flash('Nenhuma parcela encontrada para gerar o PDF.', 'warning')
            return redirect(url_for('siscalculo.resultados',
                                    dt_atualizacao=dt_atualizacao,
                                    imovel=imovel,
                                    id_indice=id_indice,
                                    perc_honorarios=float(perc_honorarios)))

        # ✅ NOVO: BUSCAR TOTAIS POR TIPO
        print(f"\n[PDF] Buscando totais por tipo...")
        totais_por_tipo = db.session.query(
            SiscalculoCalculos.ID_TIPO,
            TipoParcela.DSC_TIPO,
            db.func.count(SiscalculoCalculos.DT_VENCIMENTO).label('quantidade'),
            db.func.sum(SiscalculoCalculos.VR_TOTAL).label('valor_total')
        ).join(
            TipoParcela,
            SiscalculoCalculos.ID_TIPO == TipoParcela.ID_TIPO
        ).filter(
            SiscalculoCalculos.DT_ATUALIZACAO == dt_atualizacao_filtro,
            SiscalculoCalculos.IMOVEL == imovel,
            SiscalculoCalculos.ID_INDICE_ECONOMICO == int(id_indice)
        ).group_by(
            SiscalculoCalculos.ID_TIPO,
            TipoParcela.DSC_TIPO
        ).order_by(
            SiscalculoCalculos.ID_TIPO
        ).all()

        # Converter para lista de dicionários
        totais_por_tipo_lista = [
            {
                'id_tipo': t.ID_TIPO,
                'descricao': t.DSC_TIPO,
                'quantidade': t.quantidade,
                'valor_total': float(t.valor_total) if t.valor_total else 0
            }
            for t in totais_por_tipo
        ]

        print(f"[PDF] Totais por tipo encontrados: {len(totais_por_tipo_lista)}")
        for t in totais_por_tipo_lista:
            print(f"  Tipo {t['id_tipo']}: {t['descricao']} - {t['quantidade']} parcelas - R$ {t['valor_total']:,.2f}")

        # Buscar nome do condomínio
        nome_condominio = ''
        dado_temp = SiscalculoDados.query.filter_by(
            DT_ATUALIZACAO=dt_atualizacao_filtro,
            IMOVEL=imovel
        ).first()

        if dado_temp and dado_temp.NOME_CONDOMINIO:
            nome_condominio = dado_temp.NOME_CONDOMINIO

        # Buscar endereço do imóvel
        endereco_imovel = ''
        try:
            with db.engine.connect() as connection:
                sql_endereco = text("""
                    SELECT TOP 1 
                        RTRIM(LTRIM(ISNULL(DS_ENDERECO, ''))) + ', ' +
                        RTRIM(LTRIM(ISNULL(NU_ENDERECO, ''))) + ' - ' +
                        RTRIM(LTRIM(ISNULL(DS_BAIRRO, ''))) + ' - ' +
                        RTRIM(LTRIM(ISNULL(DS_CIDADE, ''))) + '/' +
                        RTRIM(LTRIM(ISNULL(DS_ESTADO, ''))) AS ENDERECO_COMPLETO
                    FROM [BDG].[MOV_TB001_IMOVEL]
                    WHERE NU_IMOVEL = :imovel
                """)
                result = connection.execute(sql_endereco, {"imovel": imovel})
                row = result.fetchone()
                if row and row[0]:
                    endereco_imovel = row[0]
        except Exception as e:
            print(f"[PDF] Erro ao buscar endereço: {str(e)}")

        # Buscar índice
        indice_nome = 'N/A'
        indice = ParamIndicesEconomicos.query.get(int(id_indice))
        if indice:
            indice_nome = indice.DSC_INDICE_ECONOMICO

        # Converter parcelas para lista de dicionários
        parcelas_pdf = []
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
                'VR_TOTAL': p.VR_TOTAL,
                'ID_TIPO': p.ID_TIPO  # ✅ ADICIONAR ID_TIPO
            })

        # Calcular totais
        total_geral = sum(p.VR_TOTAL or 0 for p in parcelas)
        honorarios = total_geral * (perc_honorarios / Decimal('100'))

        totais = {
            'quantidade': len(parcelas),
            'vr_cota': sum(p.VR_COTA for p in parcelas),
            'atm': sum(p.ATM or 0 for p in parcelas),
            'juros': sum(p.VR_JUROS or 0 for p in parcelas),
            'multa': sum(p.VR_MULTA or 0 for p in parcelas),
            'desconto': sum(p.VR_DESCONTO or 0 for p in parcelas),
            'total_geral': total_geral,
            'honorarios': honorarios,
            'total_final': total_geral + honorarios,
            'perc_honorarios': perc_honorarios
        }

        # Gerar PDF
        from app.utils.siscalculo_pdf import gerar_pdf_siscalculo
        logo_path = os.path.join(os.path.dirname(__file__), '..', 'static', 'img', 'logo_emgea.png')

        import tempfile
        fd, caminho_pdf = tempfile.mkstemp(suffix='.pdf')
        os.close(fd)

        gerar_pdf_siscalculo(
            output_path=caminho_pdf,
            parcelas=parcelas_pdf,
            totais=totais,
            totais_por_tipo=totais_por_tipo_lista,  # ✅ PASSAR TOTAIS POR TIPO
            nome_condominio=nome_condominio,
            endereco_imovel=endereco_imovel,
            imovel=imovel,
            data_atualizacao=dt_atualizacao_filtro.strftime('%d/%m/%Y'),
            indice_nome=indice_nome,
            perc_honorarios=float(perc_honorarios),
            periodo_prescricao=periodo_prescricao,
            logo_path=logo_path if os.path.exists(logo_path) else None
        )

        return send_file(
            caminho_pdf,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=f'SISCalculo_{imovel}_{dt_atualizacao_filtro.strftime("%Y%m%d")}.pdf'
        )

    except Exception as e:
        print(f"\n[ERRO PDF] Erro fatal ao gerar PDF: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Erro interno ao gerar o PDF: {str(e)}', 'danger')
        return redirect(url_for('siscalculo.index'))


@siscalculo_bp.route('/clausula_prejuizo')
@login_required
def clausula_prejuizo():
    """Página principal da Cláusula de Prejuízo"""
    try:
        # Buscar todos os contratos únicos que possuem cálculos
        contratos = db.session.query(
            SiscalculoCalculos.IMOVEL
        ).distinct().order_by(
            SiscalculoCalculos.IMOVEL
        ).all()

        contratos_lista = [c.IMOVEL for c in contratos]

        return render_template('sumov/siscalculo/clausula_prejuizo.html',
                               contratos=contratos_lista)

    except Exception as e:
        flash(f'Erro ao carregar cláusula de prejuízo: {str(e)}', 'danger')
        return redirect(url_for('siscalculo.index'))


@siscalculo_bp.route('/clausula_prejuizo/buscar_datas/<imovel>')
@login_required
def buscar_datas_contrato(imovel):
    """Busca as datas de atualização disponíveis para um contrato"""
    try:
        # Buscar todas as datas de atualização para o contrato selecionado
        datas = db.session.query(
            SiscalculoCalculos.DT_ATUALIZACAO,
            SiscalculoCalculos.ID_INDICE_ECONOMICO,
            ParamIndicesEconomicos.DSC_INDICE_ECONOMICO,
            db.func.sum(SiscalculoCalculos.VR_TOTAL).label('total_geral'),
            db.func.max(SiscalculoCalculos.PERC_HONORARIOS).label('perc_honorarios')
        ).join(
            ParamIndicesEconomicos,
            SiscalculoCalculos.ID_INDICE_ECONOMICO == ParamIndicesEconomicos.ID_INDICE_ECONOMICO
        ).filter(
            SiscalculoCalculos.IMOVEL == imovel
        ).group_by(
            SiscalculoCalculos.DT_ATUALIZACAO,
            SiscalculoCalculos.ID_INDICE_ECONOMICO,
            ParamIndicesEconomicos.DSC_INDICE_ECONOMICO
        ).order_by(
            SiscalculoCalculos.DT_ATUALIZACAO.desc()
        ).all()

        # Converter para formato JSON
        datas_json = []
        for d in datas:
            # Calcular honorários
            total_sem_honorarios = d.total_geral or Decimal('0')
            perc_hon = d.perc_honorarios or Decimal('10.00')

            # Remover honorários do total para obter o valor base
            # total_geral = valor_base * (1 + perc_honorarios/100)
            # valor_base = total_geral / (1 + perc_honorarios/100)
            valor_base = total_sem_honorarios / (Decimal('1') + perc_hon / Decimal('100'))
            honorarios = total_sem_honorarios - valor_base

            datas_json.append({
                'dt_atualizacao': d.DT_ATUALIZACAO.strftime('%Y-%m-%d'),
                'dt_atualizacao_formatada': d.DT_ATUALIZACAO.strftime('%d/%m/%Y'),
                'id_indice': d.ID_INDICE_ECONOMICO,
                'dsc_indice': d.DSC_INDICE_ECONOMICO,
                'total_sem_honorarios': float(valor_base),
                'honorarios': float(honorarios),
                'total_com_honorarios': float(total_sem_honorarios),
                'perc_honorarios': float(perc_hon)
            })

        return jsonify(datas_json)

    except Exception as e:
        return jsonify({'erro': str(e)}), 500


@siscalculo_bp.route('/clausula_prejuizo/calcular', methods=['POST'])
@login_required
def calcular_clausula_prejuizo():
    """Calcula o prejuízo entre duas datas de atualização"""
    try:
        data = request.get_json()

        imovel = data.get('imovel')
        dt_maior = data.get('dt_maior')
        dt_menor = data.get('dt_menor')

        if not all([imovel, dt_maior, dt_menor]):
            return jsonify({'erro': 'Dados incompletos'}), 400

        # Converter datas
        dt_maior_obj = datetime.strptime(dt_maior, '%Y-%m-%d').date()
        dt_menor_obj = datetime.strptime(dt_menor, '%Y-%m-%d').date()

        # Buscar o total com honorários da data maior
        calc_maior = db.session.query(
            db.func.sum(SiscalculoCalculos.VR_TOTAL).label('total'),
            db.func.max(SiscalculoCalculos.PERC_HONORARIOS).label('perc_honorarios')
        ).filter(
            SiscalculoCalculos.IMOVEL == imovel,
            SiscalculoCalculos.DT_ATUALIZACAO == dt_maior_obj
        ).first()

        # Buscar o total com honorários da data menor
        calc_menor = db.session.query(
            db.func.sum(SiscalculoCalculos.VR_TOTAL).label('total'),
            db.func.max(SiscalculoCalculos.PERC_HONORARIOS).label('perc_honorarios')
        ).filter(
            SiscalculoCalculos.IMOVEL == imovel,
            SiscalculoCalculos.DT_ATUALIZACAO == dt_menor_obj
        ).first()

        if not calc_maior or not calc_menor:
            return jsonify({'erro': 'Dados não encontrados para as datas selecionadas'}), 404

        # Valores COM honorários
        valor_maior_com_hon = calc_maior.total or Decimal('0')
        valor_menor_com_hon = calc_menor.total or Decimal('0')

        # ✅ LÓGICA DE PREJUÍZO COM REGRAS DE NEGÓCIO
        prejuizo_com_honorarios = Decimal('0')

        # REGRA 1: Se data maior < data menor (ordem cronológica invertida) → prejuízo = 0
        if dt_maior_obj < dt_menor_obj:
            prejuizo_com_honorarios = Decimal('0')
        # REGRA 2: Se valor menor <= valor maior (não houve perda) → prejuízo = 0
        elif valor_menor_com_hon <= valor_maior_com_hon:
            prejuizo_com_honorarios = Decimal('0')
        # REGRA 3: Houve prejuízo real (valor diminuiu no tempo)
        else:
            prejuizo_com_honorarios = valor_menor_com_hon - valor_maior_com_hon

        return jsonify({
            'valor_maior': float(valor_maior_com_hon),
            'valor_menor': float(valor_menor_com_hon),
            'prejuizo': float(prejuizo_com_honorarios),
            'perc_honorarios': float(calc_maior.perc_honorarios or Decimal('10.00'))
        })

    except Exception as e:
        return jsonify({'erro': str(e)}), 500


@siscalculo_bp.route('/clausula_prejuizo/salvar', methods=['POST'])
@login_required
def salvar_clausula_prejuizo():
    """Salva a cláusula de prejuízo na tabela PEN_TB013_TABELA_PRINCIPAL"""
    try:
        data = request.get_json()

        nu_contrato = data.get('nu_contrato')
        vr_falha = data.get('vr_falha')

        if not nu_contrato or vr_falha is None:
            return jsonify({'erro': 'Dados incompletos'}), 400

        # Converter valor para Decimal e garantir que seja POSITIVO
        vr_falha_decimal = abs(Decimal(str(vr_falha)))

        # Converter número do contrato para Numeric(23,0)
        try:
            nu_contrato_decimal = Decimal(nu_contrato)
        except:
            return jsonify({'erro': 'Número de contrato inválido'}), 400

        # ✅ CORREÇÃO: Buscar o último ID_DETALHAMENTO usando SQL puro
        sql_ultimo_id = text("""
            SELECT ISNULL(MAX(ID_DETALHAMENTO), 0) AS ultimo_id
            FROM BDG.PEN_TB013_TABELA_PRINCIPAL
        """)

        resultado = db.session.execute(sql_ultimo_id).fetchone()
        ultimo_id = resultado[0] if resultado else 0
        novo_id = ultimo_id + 1

        # Preparar dados para inserção
        dados_insercao = {
            'ID_DETALHAMENTO': novo_id,
            'NU_CONTRATO': nu_contrato_decimal,
            'ID_CARTEIRA': 4,
            'ID_OCORRENCIA': 1,
            'ID_STATUS': 3,
            'VR_FALHA': vr_falha_decimal,
            'DEVEDOR': 'CAIXA',
            'USUARIO_CRIACAO': current_user.nome,
            'USUARIO_ALTERACAO': current_user.nome,
            'USUARIO_EXCLUSAO': None,
            'CREATED_AT': datetime.utcnow(),
            'UPDATED_AT': datetime.utcnow(),
            'DELETED_AT': None,
            'NU_OFICIO': None,
            'IC_CONDENACAO': None,
            'INDICIO_DUPLIC': None,
            'DT_PAGTO': None,
            'ID_ACAO': None,
            'DT_DOCUMENTO': None,
            'DT_INICIO_ATUALIZACAO': None,
            'DT_ATUALIZACAO': None,
            'NR_PROCESSO': None,
            'VR_REAL': None,
            'ID_OBSERVACAO': None,
            'ID_ESPECIFICACAO': None,
            'NR_TICKET': None,
            'DSC_DOCUMENTO': None,
            'VR_ISS': None
        }

        # Inserir no banco usando SQL direto
        sql_insert = text("""
            INSERT INTO BDG.PEN_TB013_TABELA_PRINCIPAL (
                ID_DETALHAMENTO, NU_CONTRATO, ID_CARTEIRA, ID_OCORRENCIA, ID_STATUS,
                VR_FALHA, DEVEDOR, USUARIO_CRIACAO, USUARIO_ALTERACAO, USUARIO_EXCLUSAO,
                CREATED_AT, UPDATED_AT, DELETED_AT,
                NU_OFICIO, IC_CONDENACAO, INDICIO_DUPLIC, DT_PAGTO, ID_ACAO,
                DT_DOCUMENTO, DT_INICIO_ATUALIZACAO, DT_ATUALIZACAO,
                NR_PROCESSO, VR_REAL, ID_OBSERVACAO, ID_ESPECIFICACAO, NR_TICKET,
                DSC_DOCUMENTO, VR_ISS
            ) VALUES (
                :ID_DETALHAMENTO, :NU_CONTRATO, :ID_CARTEIRA, :ID_OCORRENCIA, :ID_STATUS,
                :VR_FALHA, :DEVEDOR, :USUARIO_CRIACAO, :USUARIO_ALTERACAO, :USUARIO_EXCLUSAO,
                :CREATED_AT, :UPDATED_AT, :DELETED_AT,
                :NU_OFICIO, :IC_CONDENACAO, :INDICIO_DUPLIC, :DT_PAGTO, :ID_ACAO,
                :DT_DOCUMENTO, :DT_INICIO_ATUALIZACAO, :DT_ATUALIZACAO,
                :NR_PROCESSO, :VR_REAL, :ID_OBSERVACAO, :ID_ESPECIFICACAO, :NR_TICKET,
                :DSC_DOCUMENTO, :VR_ISS
            )
        """)

        db.session.execute(sql_insert, dados_insercao)
        db.session.commit()


        registrar_log(
            acao='INSERT',
            entidade='PEN_TB013_TABELA_PRINCIPAL',
            entidade_id=novo_id,
            descricao=f'Cláusula de prejuízo salva - Contrato: {nu_contrato}, Valor: R$ {vr_falha_decimal:,.2f}, Devedor: CAIXA',
            dados_novos={
                'nu_contrato': str(nu_contrato),
                'vr_falha': str(vr_falha_decimal),
                'devedor': 'CAIXA',
                'id_detalhamento': novo_id,
                'usuario': current_user.nome
            }
        )

        return jsonify({
            'sucesso': True,
            'id_detalhamento': novo_id,
            'mensagem': f'Cláusula de prejuízo salva com sucesso! ID: {novo_id}'
        })

    except Exception as e:
        db.session.rollback()
        import traceback
        traceback.print_exc()
        return jsonify({'erro': str(e)}), 500