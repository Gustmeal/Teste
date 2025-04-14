from flask import Blueprint, render_template, redirect, url_for, request, flash
from app.models.limite_distribuicao import LimiteDistribuicao
from app.models.edital import Edital
from app.models.periodo import PeriodoAvaliacao
from app.models.empresa_participante import EmpresaParticipante
from app.models.criterio_selecao import CriterioSelecao
from app.models.audit_log import AuditLog
from app import db
from datetime import datetime
from flask_login import login_required, current_user
from app.utils.audit import registrar_log
from sqlalchemy import or_, func, text
import pandas as pd
import numpy as np

limite_bp = Blueprint('limite', __name__, url_prefix='/credenciamento')


@limite_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


def truncate_decimal(value, decimal_places=2):
    """Trunca o valor para o número especificado de casas decimais sem arredondamento."""
    factor = 10 ** decimal_places
    return int(value * factor) / factor


def calcular_limites_empresas_mistas(ultimo_edital, ultimo_periodo, empresas):
    """
    Realiza o cálculo dos limites de distribuição quando há empresas que permanecem e empresas novas.

    Args:
        ultimo_edital: Objeto Edital com o último edital
        ultimo_periodo: Objeto PeriodoAvaliacao com o último período
        empresas: Lista de objetos EmpresaParticipante das empresas

    Returns:
        Lista de dicionários com os dados calculados para cada empresa
    """
    try:
        # Obter o número de contratos distribuíveis
        num_contratos = selecionar_contratos()
        if num_contratos <= 0:
            return None

        # Separar as empresas que permanecem das empresas novas
        empresas_permanece = [emp for emp in empresas if emp.DS_CONDICAO == 'PERMANECE']
        empresas_novas = [emp for emp in empresas if emp.DS_CONDICAO == 'NOVA']

        # Verificar período anterior
        periodo_anterior = PeriodoAvaliacao.query.filter(
            PeriodoAvaliacao.ID_EDITAL == ultimo_edital.ID,
            PeriodoAvaliacao.ID_PERIODO < ultimo_periodo.ID_PERIODO,
            PeriodoAvaliacao.DELETED_AT == None
        ).order_by(PeriodoAvaliacao.ID_PERIODO.desc()).first()

        if not periodo_anterior:
            return None

        # 1. Calcular percentual das empresas que permanecem
        # Buscar dados de arrecadação do período anterior
        with db.engine.connect() as connection:
            # Empresas que permanecem
            sql_permanece = text("""
            SELECT 
                EP.ID_EMPRESA,
                EP.NO_EMPRESA,
                EP.NO_EMPRESA_ABREVIADA,
                COALESCE(REE.VR_ARRECADACAO, 0) AS VR_ARRECADACAO
            FROM 
                DEV.DCA_TB002_EMPRESAS_PARTICIPANTES AS EP
            LEFT JOIN (
                SELECT 
                    REE.CO_EMPRESA_COBRANCA,
                    SUM(REE.VR_ARRECADACAO_TOTAL) AS VR_ARRECADACAO
                FROM 
                    BDG.COM_TB062_REMUNERACAO_ESTIMADA AS REE
                WHERE 
                    REE.DT_ARRECADACAO BETWEEN :data_inicio AND :data_fim
                GROUP BY 
                    REE.CO_EMPRESA_COBRANCA
            ) AS REE ON EP.ID_EMPRESA = REE.CO_EMPRESA_COBRANCA
            WHERE 
                EP.ID_EDITAL = :id_edital 
                AND EP.ID_PERIODO = :id_periodo_anterior
                AND EP.DELETED_AT IS NULL
            """).bindparams(
                id_edital=ultimo_edital.ID,
                id_periodo_anterior=periodo_anterior.ID_PERIODO,
                data_inicio=periodo_anterior.DT_INICIO,
                data_fim=periodo_anterior.DT_FIM
            )

            result = connection.execute(sql_permanece)
            todas_empresas_anteriores = {}

            for row in result:
                id_empresa = row[0]
                nome = row[1]
                nome_abreviado = row[2]
                arrecadacao = float(row[3]) if row[3] else 0.0

                todas_empresas_anteriores[id_empresa] = {
                    'id_empresa': id_empresa,
                    'nome': nome,
                    'nome_abreviado': nome_abreviado,
                    'arrecadacao': arrecadacao
                }

            # Calcular total de arrecadação do período anterior
            total_arrecadacao = sum(e['arrecadacao'] for e in todas_empresas_anteriores.values())

            # Calcular percentual de cada empresa no período anterior
            for emp in todas_empresas_anteriores.values():
                emp['pct_arrecadacao'] = truncate_decimal(
                    (emp['arrecadacao'] / total_arrecadacao) * 100) if total_arrecadacao > 0 else 0.0

            # Separar empresas que saem (estavam no período anterior mas não estão em empresas_permanece)
            empresas_saem = {}
            for id_emp, emp in todas_empresas_anteriores.items():
                if not any(e.ID_EMPRESA == id_emp for e in empresas_permanece):
                    empresas_saem[id_emp] = emp

            # Calcular o total de percentual das empresas que saem
            pct_empresas_saem = sum(emp['pct_arrecadacao'] for emp in empresas_saem.values())

            # Distribuir o percentual das empresas que saem entre as que permanecem
            pct_adicional_por_empresa = pct_empresas_saem / len(empresas_permanece) if empresas_permanece else 0.0

            # Calcular novo percentual para empresas que permanecem
            dados_permanece = []
            for i, emp in enumerate(empresas_permanece):
                if emp.ID_EMPRESA in todas_empresas_anteriores:
                    pct_original = todas_empresas_anteriores[emp.ID_EMPRESA]['pct_arrecadacao']
                    nome_abreviado = todas_empresas_anteriores[emp.ID_EMPRESA]['nome_abreviado']
                    arrecadacao = todas_empresas_anteriores[emp.ID_EMPRESA]['arrecadacao']
                else:
                    pct_original = 0.0
                    nome_abreviado = emp.NO_EMPRESA_ABREVIADA
                    arrecadacao = 0.0

                pct_final = truncate_decimal(pct_original + pct_adicional_por_empresa)

                dados_permanece.append({
                    'idx': i + 1,
                    'id_empresa': emp.ID_EMPRESA,
                    'empresa': nome_abreviado or emp.NO_EMPRESA,
                    'situacao': 'PERMANECE',
                    'pct_original': pct_original,
                    'pct_adicional': pct_adicional_por_empresa,
                    'pct_distribuicao': pct_final,
                    'arrecadacao': arrecadacao
                })

            # 2. Distribuir contratos por situação
            # Total de empresas atuais
            total_empresas = len(empresas_permanece) + len(empresas_novas)

            # Qtde de contratos por situação
            qtde_contratos_permanece = int((len(empresas_permanece) / total_empresas) * num_contratos)
            qtde_contratos_novas = num_contratos - qtde_contratos_permanece

            # 3. Distribuição de contratos para empresas que permanecem
            total_pct_permanece = sum(item['pct_distribuicao'] for item in dados_permanece)

            # Atualizar com a quantidade de contratos
            for item in dados_permanece:
                item['contratos'] = int((item['pct_distribuicao'] / total_pct_permanece) * qtde_contratos_permanece)
                item['ajuste_contratos'] = 0

            # Ajustar sobras de contratos para empresas que permanecem
            contratos_distribuidos = sum(item['contratos'] for item in dados_permanece)
            sobra_contratos = qtde_contratos_permanece - contratos_distribuidos

            # Ordenar por percentual (maior para menor)
            dados_permanece.sort(key=lambda x: x['pct_distribuicao'], reverse=True)

            # Distribuir sobras
            for i in range(sobra_contratos):
                indice = i % len(dados_permanece)
                dados_permanece[indice]['contratos'] += 1
                dados_permanece[indice]['ajuste_contratos'] += 1

            # 4. Distribuição para empresas novas (igualitária)
            dados_novas = []

            # Contratos por empresa nova
            contratos_por_empresa_nova = qtde_contratos_novas // len(empresas_novas) if empresas_novas else 0
            sobra_contratos_novas = qtde_contratos_novas - (contratos_por_empresa_nova * len(empresas_novas))

            for i, emp in enumerate(empresas_novas):
                # Adicionar contratos extras para as primeiras empresas se houver sobra
                contratos = contratos_por_empresa_nova + (1 if i < sobra_contratos_novas else 0)

                dados_novas.append({
                    'idx': len(dados_permanece) + i + 1,
                    'id_empresa': emp.ID_EMPRESA,
                    'empresa': emp.NO_EMPRESA_ABREVIADA or emp.NO_EMPRESA,
                    'situacao': 'NOVA',
                    'pct_original': 0.0,
                    'pct_adicional': 0.0,
                    'pct_distribuicao': 0.0,
                    'contratos': contratos,
                    'ajuste_contratos': 0
                })

            # 5. Calcular o percentual final para todas as empresas
            todos_dados = dados_permanece + dados_novas
            total_contratos = sum(item['contratos'] for item in todos_dados)

            for item in todos_dados:
                item['pct_final'] = truncate_decimal((item['contratos'] / total_contratos) * 100)
                item['ajuste'] = 0.00  # Inicializar ajuste

            # Verificar soma dos percentuais
            soma_pct = sum(item['pct_final'] for item in todos_dados)
            diferenca = truncate_decimal(100.00 - soma_pct)

            # Ajustar para garantir soma 100%
            if diferenca > 0:
                # Ordenar por contratos (maior para menor)
                todos_dados.sort(key=lambda x: x['contratos'], reverse=True)

                ajuste_unitario = 0.01
                ajustes_necessarios = int(diferenca / ajuste_unitario)

                # Distribuir ajustes
                for i in range(ajustes_necessarios):
                    indice = i % len(todos_dados)
                    todos_dados[indice]['ajuste'] += ajuste_unitario
                    todos_dados[indice]['pct_final'] += ajuste_unitario

            # Calcular totais finais
            total_pct_final = sum(item['pct_final'] for item in todos_dados)
            total_ajuste = sum(item['ajuste'] for item in todos_dados)

            # Forçar o total para exatamente 100.00%
            if abs(total_pct_final - 100.00) >= 0.01:
                diferenca_final = 100.00 - total_pct_final
                # Ajustar na primeira empresa
                todos_dados[0]['ajuste'] += diferenca_final
                todos_dados[0]['pct_final'] += diferenca_final
                total_pct_final = 100.00
                total_ajuste = sum(item['ajuste'] for item in todos_dados)

            # Adicionar linha de totais
            todos_dados.append({
                'idx': 'TOTAL',
                'id_empresa': None,
                'empresa': 'TOTAL',
                'situacao': '',
                'contratos': total_contratos,
                'pct_final': total_pct_final,
                'ajuste': total_ajuste
            })

            # Calcular totais para o template
            qtde_empresas_permanece = len(empresas_permanece)
            qtde_empresas_novas = len(empresas_novas)
            qtde_empresas_total = qtde_empresas_permanece + qtde_empresas_novas

            total_arrecadacao_permanece = sum(item.get('arrecadacao', 0) for item in dados_permanece)
            total_pct_original_permanece = sum(item.get('pct_original', 0) for item in dados_permanece)
            total_redistribuicao = sum(item.get('pct_adicional', 0) for item in dados_permanece)
            total_pct_distribuicao_permanece = sum(item.get('pct_distribuicao', 0) for item in dados_permanece)

            ajuste_necessario = abs(
                sum(item.get('pct_final', 0) for item in todos_dados if item.get('idx') != 'TOTAL') - 100.0) > 0.001

            # Retornar os resultados e as variáveis para o template
            return {
                'resultados': todos_dados,
                'qtde_empresas_permanece': qtde_empresas_permanece,
                'qtde_empresas_novas': qtde_empresas_novas,
                'qtde_empresas_total': qtde_empresas_total,
                'qtde_contratos_permanece': qtde_contratos_permanece,
                'qtde_contratos_novas': qtde_contratos_novas,
                'total_arrecadacao_permanece': total_arrecadacao_permanece,
                'total_pct_original_permanece': total_pct_original_permanece,
                'total_redistribuicao': total_redistribuicao,
                'total_pct_distribuicao_permanece': total_pct_distribuicao_permanece,
                'ajuste_necessario': ajuste_necessario,
                'total_ajuste': total_ajuste,
                'num_contratos': num_contratos
            }

    except Exception as e:
        print(f"Erro no cálculo para empresas mistas: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return None

def calcular_limites_empresas_novas(ultimo_edital, ultimo_periodo, empresas):
    """
    Realiza o cálculo dos limites de distribuição quando todas as empresas são novas.
    Neste caso, os contratos são distribuídos igualitariamente entre as empresas.

    Args:
        ultimo_edital: Objeto Edital com o último edital
        ultimo_periodo: Objeto PeriodoAvaliacao com o último período
        empresas: Lista de objetos EmpresaParticipante das empresas

    Returns:
        Lista de dicionários com os dados calculados para cada empresa
    """
    try:
        # Obter o número de contratos distribuíveis
        num_contratos = selecionar_contratos()
        if num_contratos <= 0:
            return []

        # Número de empresas participantes
        qtde_empresas = len(empresas)
        if qtde_empresas <= 0:
            return []

        # Calcular o percentual base para cada empresa (truncado para duas casas sem arredondamento)
        percentual_base = truncate_decimal(100.00 / qtde_empresas)

        # Calcular a diferença que precisa ser distribuída
        total_percentual_base = percentual_base * qtde_empresas
        diferenca = truncate_decimal(100.00 - total_percentual_base)

        # Preparar dados para retorno
        resultados = []

        # Primeiro, criar a lista de empresas com percentual base
        for idx, empresa in enumerate(empresas):
            resultados.append({
                'idx': idx + 1,
                'id_empresa': empresa.ID_EMPRESA,
                'empresa': empresa.NO_EMPRESA_ABREVIADA or empresa.NO_EMPRESA,
                'situacao': empresa.DS_CONDICAO,
                'contratos': num_contratos // qtde_empresas,  # Divisão inteira
                'pct_distribuicao': percentual_base,
                'ajuste': 0.00,
                'pct_final': percentual_base
            })

        # Nova abordagem: manter um contador de quantos incrementos cada empresa recebeu
        # e distribuir os incrementos de forma equilibrada
        contador_ajustes = [0] * len(resultados)
        incremento = 0.01
        incrementos_totais_necessarios = int(diferenca / incremento)

        # Usar um algoritmo estrito de nivelamento
        for _ in range(incrementos_totais_necessarios):
            # Encontrar o menor valor atual no contador
            menor_contador = min(contador_ajustes)

            # Coletar todos os índices com este valor
            indices_candidatos = []
            for i, valor in enumerate(contador_ajustes):
                if valor == menor_contador:
                    indices_candidatos.append(i)

            # Escolher aleatoriamente um dos índices candidatos
            from random import choice
            indice_escolhido = choice(indices_candidatos)

            # Aplicar o incremento
            contador_ajustes[indice_escolhido] += 1
            resultados[indice_escolhido]['ajuste'] = contador_ajustes[indice_escolhido] * incremento
            resultados[indice_escolhido]['pct_final'] = percentual_base + resultados[indice_escolhido]['ajuste']

        # Verificação CRÍTICA: Garantir que a soma seja EXATAMENTE 100.00%
        soma_pct_final = sum(item['pct_final'] for item in resultados)

        # Se não for exatamente 100.00%, fazer ajuste adicional
        if abs(soma_pct_final - 100.00) >= 0.001:  # Usando uma pequena tolerância
            # Calcular diferença exata
            diferenca_final = 100.00 - soma_pct_final

            # Arredondar para exatamente 0.01 se estiver próximo
            if abs(diferenca_final - 0.01) < 0.001:
                diferenca_final = 0.01

            # Encontrar a empresa com menos incrementos para adicionar a diferença
            menor_contador_final = min(contador_ajustes)
            indices_menor_final = [i for i, v in enumerate(contador_ajustes) if v == menor_contador_final]
            indice_ajuste_final = choice(indices_menor_final)

            # Aplicar o ajuste final
            resultados[indice_ajuste_final]['ajuste'] += diferenca_final
            resultados[indice_ajuste_final]['pct_final'] = truncate_decimal(
                percentual_base + resultados[indice_ajuste_final]['ajuste']
            )

            # VERIFICAÇÃO IMPORTANTE: Verificar a soma novamente para garantir 100.00%
            nova_soma = sum(item['pct_final'] for item in resultados)
            if abs(nova_soma - 100.00) >= 0.001:
                # Se ainda não for exatamente 100.00%, fazer um ajuste direto final
                # Escolher qualquer empresa e forçar o valor correto
                indice_final = 0

                # Calcular quanto falta para 100.00% exato
                diferenca_absoluta = 100.00 - nova_soma

                # Calcular o valor final correto para esta empresa
                valor_final_correto = resultados[indice_final]['pct_final'] + diferenca_absoluta

                # Atualizar o ajuste com base no novo valor final
                resultados[indice_final]['ajuste'] = valor_final_correto - percentual_base
                resultados[indice_final]['pct_final'] = valor_final_correto

        # Calcular totais finais
        total_pct_distribuicao = sum(item['pct_distribuicao'] for item in resultados)
        total_ajuste = sum(item['ajuste'] for item in resultados)
        total_pct_final = sum(item['pct_final'] for item in resultados)

        # Forçar o total para exatamente 100.00%
        total_pct_final = 100.00

        # Adicionar linha de total
        resultados.append({
            'idx': 'TOTAL',
            'id_empresa': '',
            'empresa': 'TOTAL',
            'situacao': '',
            'contratos': num_contratos,
            'pct_distribuicao': total_pct_distribuicao,
            'ajuste': total_ajuste,
            'pct_final': total_pct_final
        })

        return resultados

    except Exception as e:
        print(f"Erro no cálculo para empresas novas: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return []



@limite_bp.route('/limites')
@login_required
def lista_limites():
    from sqlalchemy.orm import joinedload

    periodo_id = request.args.get('periodo_id', type=int)
    criterio_id = request.args.get('criterio_id', type=int)

    query = db.session.query(
        LimiteDistribuicao,
        CriterioSelecao.DS_CRITERIO_SELECAO
    ).outerjoin(
        CriterioSelecao,
        LimiteDistribuicao.COD_CRITERIO_SELECAO == CriterioSelecao.COD
    ).options(
        joinedload(LimiteDistribuicao.periodo),
        joinedload(LimiteDistribuicao.edital)
    ).filter(
        LimiteDistribuicao.DELETED_AT == None,
        or_(CriterioSelecao.DELETED_AT == None, CriterioSelecao.DELETED_AT.is_(None))
    )

    if periodo_id:
        query = query.filter(LimiteDistribuicao.ID_PERIODO == periodo_id)

    if criterio_id:
        query = query.filter(LimiteDistribuicao.COD_CRITERIO_SELECAO == criterio_id)

    results = query.all()

    limites = []
    for limite, ds_criterio in results:
        # Obter nome da empresa
        empresas = EmpresaParticipante.query.filter_by(ID_EMPRESA=limite.ID_EMPRESA).all()
        empresa_nome = None
        empresa_nome_abreviado = None
        if empresas:
            for empresa in empresas:
                if empresa.NO_EMPRESA:
                    empresa_nome = empresa.NO_EMPRESA
                    empresa_nome_abreviado = empresa.NO_EMPRESA_ABREVIADA
                    break

        if not empresa_nome:
            try:
                from app.models.empresa_responsavel import EmpresaResponsavel
                empresa_resp = EmpresaResponsavel.query.filter_by(pkEmpresaResponsavelCobranca=limite.ID_EMPRESA).first()
                if empresa_resp:
                    empresa_nome = empresa_resp.nmEmpresaResponsavelCobranca
                    empresa_nome_abreviado = empresa_resp.NO_ABREVIADO_EMPRESA
            except:
                empresa_nome = f"Empresa ID {limite.ID_EMPRESA}"
                empresa_nome_abreviado = f"ID {limite.ID_EMPRESA}"

        limite.empresa_nome = empresa_nome
        limite.empresa_nome_abreviado = empresa_nome_abreviado
        limite.criterio_descricao = ds_criterio if ds_criterio else f"Critério {limite.COD_CRITERIO_SELECAO}"

        limites.append(limite)

    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).all()
    criterios = CriterioSelecao.query.filter(CriterioSelecao.DELETED_AT == None).all()

    return render_template(
        'credenciamento/lista_limites.html',
        limites=limites,
        periodos=periodos,
        criterios=criterios,
        filtro_periodo_id=periodo_id,
        filtro_criterio_id=criterio_id
    )



def selecionar_contratos():
    """
    Seleciona o universo de contratos que serão distribuídos e os armazena na tabela DCA_TB006_DISTRIBUIVEIS.
    Usa as tabelas do Banco de Dados Gerencial (BDG).
    Retorna a quantidade de contratos selecionados.
    """
    try:
        # Usar uma conexão direta para executar o SQL
        with db.engine.connect() as connection:
            try:
                # Primeiro, limpar a tabela de distribuíveis
                truncate_sql = text("TRUNCATE TABLE [DEV].[DCA_TB006_DISTRIBUIVEIS]")
                connection.execute(truncate_sql)
                

                # Em seguida, inserir os contratos selecionados
                insert_sql = text("""
                INSERT INTO [DEV].[DCA_TB006_DISTRIBUIVEIS]
                SELECT
                    ECA.fkContratoSISCTR
                    , CON.NR_CPF_CNPJ
                    , SIT.VR_SD_DEVEDOR
                    , CREATED_AT = GETDATE()
                    , UPDATED_AT = NULL
                    , DELETED_AT = NULL
                FROM 
                    [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                
                    INNER JOIN [BDG].[COM_TB001_CONTRATO] AS CON
                        ON ECA.fkContratoSISCTR = CON.fkContratoSISCTR
                
                    INNER JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] AS SIT
                        ON ECA.fkContratoSISCTR = SIT.fkContratoSISCTR
                
                    LEFT JOIN [BDG].[COM_TB013_SUSPENSO_DECISAO_JUDICIAL] AS SDJ
                        ON ECA.fkContratoSISCTR = SDJ.fkContratoSISCTR
                WHERE
                    SIT.[fkSituacaoCredito] = 1
                    AND SDJ.fkContratoSISCTR IS NULL""")

                connection.execute(insert_sql)


                # Contar quantos contratos foram selecionados
                count_sql = text("SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] WHERE DELETED_AT IS NULL")
                result = connection.execute(count_sql)
                num_contratos = result.scalar()
                print(f"Total de contratos selecionados: {num_contratos}")

                return num_contratos

            except Exception as e:
                print(f"Erro durante a execução das consultas SQL: {str(e)}")
                # Log mais detalhado caso ocorra erro
                import traceback
                print(traceback.format_exc())
                raise

    except Exception as e:
        print(f"Erro na seleção de contratos: {str(e)}")
        db.session.rollback()
        return 0


@limite_bp.route('/limites/analise')
@login_required
def analise_limites():
    try:
        # Primeiro, selecionar os contratos distribuíveis
        num_contratos = selecionar_contratos()
        print(f"Contratos selecionados: {num_contratos}")

        # Obter o edital mais recente
        ultimo_edital = Edital.query.filter(Edital.DELETED_AT == None).order_by(Edital.ID.desc()).first()

        if not ultimo_edital:
            flash('Não foram encontrados editais cadastrados.', 'warning')
            return redirect(url_for('edital.lista_editais'))

        # Obter o período mais recente
        ultimo_periodo = PeriodoAvaliacao.query.filter(
            PeriodoAvaliacao.DELETED_AT == None,
            PeriodoAvaliacao.ID_EDITAL == ultimo_edital.ID
        ).order_by(PeriodoAvaliacao.ID_PERIODO.desc()).first()

        if not ultimo_periodo:
            flash('Não foram encontrados períodos para o edital mais recente.', 'warning')
            return redirect(url_for('periodo.lista_periodos'))

        # Buscar empresas participantes do último período
        empresas = EmpresaParticipante.query.filter(
            EmpresaParticipante.ID_EDITAL == ultimo_edital.ID,
            EmpresaParticipante.ID_PERIODO == ultimo_periodo.ID_PERIODO,
            EmpresaParticipante.DELETED_AT == None
        ).all()

        if not empresas:
            flash('Não foram encontradas empresas participantes para o período atual.', 'warning')
            return redirect(url_for('empresa.lista_empresas', periodo_id=ultimo_periodo.ID))

        # Analisar condições das empresas
        todas_permanece = all(empresa.DS_CONDICAO == 'PERMANECE' for empresa in empresas)
        todas_novas = all(empresa.DS_CONDICAO == 'NOVA' for empresa in empresas)
        alguma_permanece = any(empresa.DS_CONDICAO == 'PERMANECE' for empresa in empresas)

        # Período anterior para cálculos
        periodo_anterior = None
        if ultimo_periodo.ID_PERIODO > 1:
            periodo_anterior = PeriodoAvaliacao.query.filter(
                PeriodoAvaliacao.ID_EDITAL == ultimo_edital.ID,
                PeriodoAvaliacao.ID_PERIODO < ultimo_periodo.ID_PERIODO,
                PeriodoAvaliacao.DELETED_AT == None
            ).order_by(PeriodoAvaliacao.ID_PERIODO.desc()).first()

        # Resultados do cálculo
        resultados_calculo = []

        # Se todas as empresas são PERMANECE, aplicar o cálculo 3.3.1
        if todas_permanece:
            # Verificar se há período anterior para cálculos
            if periodo_anterior:
                try:
                    # Criar conexão direta com o banco para obter os dados de arrecadação
                    with db.engine.connect() as connection:
                        # Consulta para obter arrecadação das empresas no período anterior
                        sql = text("""
                        SELECT 
                            EP.ID_EMPRESA,
                            EP.NO_EMPRESA,
                            EP.NO_EMPRESA_ABREVIADA,
                            EP.DS_CONDICAO,
                            -- Buscar dados de arrecadação da tabela real
                            COALESCE(REE.VR_ARRECADACAO, 0) AS VR_ARRECADACAO
                        FROM 
                            DEV.DCA_TB002_EMPRESAS_PARTICIPANTES AS EP
                        LEFT JOIN (
                            SELECT 
                                REE.CO_EMPRESA_COBRANCA,
                                SUM(REE.VR_ARRECADACAO_TOTAL) AS VR_ARRECADACAO
                            FROM 
                                BDG.COM_TB062_REMUNERACAO_ESTIMADA AS REE
                            WHERE 
                                REE.DT_ARRECADACAO BETWEEN :data_inicio AND :data_fim
                            GROUP BY 
                                REE.CO_EMPRESA_COBRANCA
                        ) AS REE ON EP.ID_EMPRESA = REE.CO_EMPRESA_COBRANCA
                        WHERE 
                            EP.ID_EDITAL = :id_edital 
                            AND EP.ID_PERIODO = :id_periodo 
                            AND EP.DS_CONDICAO = 'PERMANECE'
                            AND EP.DELETED_AT IS NULL
                        ORDER BY
                            VR_ARRECADACAO DESC
                        """).bindparams(
                            id_edital=ultimo_edital.ID,
                            id_periodo=ultimo_periodo.ID_PERIODO,
                            data_inicio=periodo_anterior.DT_INICIO,
                            data_fim=periodo_anterior.DT_FIM
                        )

                        # Executar a consulta
                        result = connection.execute(sql)
                        rows = result.fetchall()

                        if not rows:
                            flash('Não foram encontrados dados de arrecadação para o período anterior.', 'warning')
                            return redirect(url_for('limite.lista_limites'))

                        # Processar dados da arrecadação real
                        dados_arrecadacao = []
                        for row in rows:
                            dados_arrecadacao.append({
                                'id_empresa': row[0],
                                'nome': row[1],
                                'nome_abreviado': row[2] or (row[1][0] if row[1] else ''),
                                'situacao': row[3],
                                'arrecadacao': float(row[4]) if row[4] else 0.0
                            })

                        # Ordenar por arrecadação (maior para menor)
                        dados_arrecadacao.sort(key=lambda x: x['arrecadacao'], reverse=True)

                        # Calcular total de arrecadação
                        total_arrecadacao = sum(item['arrecadacao'] for item in dados_arrecadacao)

                        # Calcular percentuais (truncados, sem arredondamento)
                        dados_processados = []
                        for idx, item in enumerate(dados_arrecadacao):
                            # Calcular percentual bruto e truncar para duas casas decimais
                            pct_arrecadacao = truncate_decimal((item['arrecadacao'] / total_arrecadacao) * 100)

                            # Adicionar dados processados
                            dados_processados.append({
                                'idx': idx + 1,
                                'id_empresa': item['id_empresa'],
                                'empresa': item['nome_abreviado'],
                                'situacao': item['situacao'],
                                'arrecadacao': item['arrecadacao'],
                                'pct_arrecadacao': pct_arrecadacao,
                                'ajuste': 0.00,  # Será atualizado depois
                                'pct_final': pct_arrecadacao  # Será atualizado depois
                            })

                        # Calcular a soma dos percentuais truncados
                        soma_percentuais = sum(item['pct_arrecadacao'] for item in dados_processados)

                        # Calcular quanto falta para chegar a 100%
                        diferenca = truncate_decimal(100.00 - soma_percentuais)

                        # Aplicar ajuste de 0.01% às empresas com maior arrecadação até chegar exatamente a 100%
                        if diferenca > 0:
                            # Quantas empresas precisam ser ajustadas inicialmente (um "ciclo" completo de empresas)
                            ajuste_por_empresa = 0.01
                            num_ciclos_completos = int(diferenca / (ajuste_por_empresa * len(dados_processados)))
                            ajustes_restantes = int(
                                (diferenca % (ajuste_por_empresa * len(dados_processados))) / ajuste_por_empresa)

                            # Aplicar ajustes para ciclos completos (todas as empresas recebem ajuste)
                            if num_ciclos_completos > 0:
                                for i in range(len(dados_processados)):
                                    dados_processados[i]['ajuste'] = num_ciclos_completos * ajuste_por_empresa
                                    dados_processados[i]['pct_final'] = truncate_decimal(
                                        dados_processados[i]['pct_arrecadacao'] + dados_processados[i]['ajuste'])

                            # Aplicar ajustes restantes (apenas algumas empresas recebem ajuste adicional)
                            for i in range(ajustes_restantes):
                                # O índice será sempre menor que o número de empresas
                                indice = i % len(dados_processados)
                                dados_processados[indice]['ajuste'] += ajuste_por_empresa
                                dados_processados[indice]['pct_final'] = truncate_decimal(
                                    dados_processados[indice]['pct_arrecadacao'] + dados_processados[indice]['ajuste'])

                            # Recalcular totais
                            total_pct_arrecadacao = sum(item['pct_arrecadacao'] for item in dados_processados)
                            total_ajuste = sum(item['ajuste'] for item in dados_processados)
                            total_pct_final = sum(item['pct_final'] for item in dados_processados)

                            # Verificar se o total final é exatamente 100%
                            if total_pct_final != 100.00:
                                # Se ainda não for exatamente 100%, ajustar a primeira empresa
                                diferenca_restante = 100.00 - total_pct_final
                                dados_processados[0]['pct_final'] = truncate_decimal(
                                    dados_processados[0]['pct_final'] + diferenca_restante)
                                dados_processados[0]['ajuste'] = truncate_decimal(
                                    dados_processados[0]['ajuste'] + diferenca_restante)

                                # Recalcular totais finais
                                total_ajuste = sum(item['ajuste'] for item in dados_processados)
                                total_pct_final = sum(item['pct_final'] for item in dados_processados)

                        # Adicionar linha de total
                        dados_processados.append({
                            'idx': 'TOTAL',
                            'id_empresa': '',
                            'empresa': 'TOTAL',
                            'situacao': '',
                            'arrecadacao': total_arrecadacao,
                            'pct_arrecadacao': soma_percentuais,
                            'ajuste': total_ajuste,
                            'pct_final': total_pct_final
                        })

                        resultados_calculo = dados_processados

                except Exception as e:
                    flash(f'Erro ao processar cálculo: {str(e)}', 'danger')
                    print(f"Erro detalhado: {e}")
                    import traceback
                    print(traceback.format_exc())
                    return redirect(url_for('limite.lista_limites'))
            else:
                flash('Não foi encontrado período anterior para realizar os cálculos.', 'warning')
        elif todas_novas:

                try:
                    # Realizar o cálculo de distribuição igualitária
                    resultados_calculo = calcular_limites_empresas_novas(ultimo_edital, ultimo_periodo, empresas)

                    if not resultados_calculo:
                        flash('Não foi possível calcular a distribuição para empresas novas.', 'warning')
                except Exception as e:
                    flash(f'Erro ao processar cálculo para empresas novas: {str(e)}', 'danger')
                    print(f"Erro detalhado: {e}")
                    import traceback
                    print(traceback.format_exc())
                    return redirect(url_for('limite.lista_limites'))



        elif alguma_permanece and not todas_permanece:

            try:

                # Realizar o cálculo para empresas mistas

                resultado = calcular_limites_empresas_mistas(ultimo_edital, ultimo_periodo, empresas)

                if not resultado or not resultado.get('resultados'):

                    flash('Não foi possível calcular a distribuição para empresas mistas.', 'warning')

                else:

                    # Extrair resultados e variáveis adicionais para o template

                    resultados_calculo = resultado['resultados']

                    qtde_empresas_permanece = resultado['qtde_empresas_permanece']

                    qtde_empresas_novas = resultado['qtde_empresas_novas']

                    qtde_empresas_total = resultado['qtde_empresas_total']

                    qtde_contratos_permanece = resultado['qtde_contratos_permanece']

                    qtde_contratos_novas = resultado['qtde_contratos_novas']

                    total_arrecadacao_permanece = resultado['total_arrecadacao_permanece']

                    total_pct_original_permanece = resultado['total_pct_original_permanece']

                    total_redistribuicao = resultado['total_redistribuicao']

                    total_pct_distribuicao_permanece = resultado['total_pct_distribuicao_permanece']

                    ajuste_necessario = resultado['ajuste_necessario']

                    total_ajuste = resultado['total_ajuste']

                    # Se o número de contratos não estiver definido em num_contratos, use o de resultado

                    if 'num_contratos' in resultado:
                        num_contratos = resultado['num_contratos']

            except Exception as e:

                flash(f'Erro ao processar cálculo para empresas mistas: {str(e)}', 'danger')

                print(f"Erro detalhado: {e}")

                import traceback

                print(traceback.format_exc())

                return redirect(url_for('limite.lista_limites'))

        # Renderizar o template com os resultados da análise
        return render_template(
            'credenciamento/analise_limite.html',
            edital=ultimo_edital,
            periodo=ultimo_periodo,
            periodo_anterior=periodo_anterior,
            empresas=empresas,
            todas_permanece=todas_permanece,
            todas_novas=todas_novas,
            alguma_permanece=alguma_permanece,
            resultados_calculo=resultados_calculo,
            num_contratos=num_contratos  # Passar o número de contratos para o template
        )

    except Exception as e:
        flash(f'Erro durante a análise: {str(e)}', 'danger')
        import traceback
        print(traceback.format_exc())
        return redirect(url_for('limite.lista_limites'))


@limite_bp.route('/limites/salvar', methods=['POST'])
@login_required
def salvar_limites():
    try:
        edital_id = request.form.get('edital_id', type=int)
        periodo_id = request.form.get('periodo_id', type=int)

        # Buscar DT_APURACAO da tabela de situação de contratos
        dt_apuracao = None
        with db.engine.connect() as connection:
            try:
                sql = text("""
                    SELECT TOP 1 DT_REFERENCIA 
                    FROM [BDG].[COM_TB007_SITUACAO_CONTRATOS]
                    ORDER BY DT_REFERENCIA DESC
                """)
                result = connection.execute(sql)
                row = result.fetchone()
                dt_apuracao = row[0].date() if row and isinstance(row[0], datetime) else datetime.now().date()
            except Exception as e:
                print(f"Erro ao buscar DT_APURACAO: {str(e)}")
                dt_apuracao = datetime.now().date()

        # Obter dados enviados do formulário
        empresas_data = request.form.getlist('empresa_id[]')
        percentuais = request.form.getlist('percentual_final[]')

        if len(empresas_data) != len(percentuais):
            flash('Erro: Dados inconsistentes.', 'danger')
            return redirect(url_for('limite.analise_limites'))

        if not empresas_data:
            flash('Erro: Nenhuma empresa informada.', 'danger')
            return redirect(url_for('limite.analise_limites'))

        cod_criterio = 7  # critério fixo

        # EXCLUSÃO FÍSICA dos limites anteriores para o mesmo edital/período/critério
        db.session.query(LimiteDistribuicao).filter(
            LimiteDistribuicao.ID_EDITAL == edital_id,
            LimiteDistribuicao.ID_PERIODO == periodo_id,
            LimiteDistribuicao.COD_CRITERIO_SELECAO == cod_criterio
        ).delete(synchronize_session=False)
        db.session.commit()

        # INSERIR novos limites recebidos do formulário
        limites_criados = 0
        for i in range(len(empresas_data)):
            novo_limite = LimiteDistribuicao(
                ID_EDITAL=edital_id,
                ID_PERIODO=periodo_id,
                ID_EMPRESA=int(empresas_data[i]),
                COD_CRITERIO_SELECAO=cod_criterio,
                DT_APURACAO=dt_apuracao,
                QTDE_MAXIMA=None,
                VALOR_MAXIMO=None,
                PERCENTUAL_FINAL=float(percentuais[i])
            )
            db.session.add(novo_limite)
            limites_criados += 1

        db.session.commit()

        flash(f'Limites salvos com sucesso. Total: {limites_criados}', 'success')
        return redirect(url_for('limite.lista_limites'))

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao salvar limites: {str(e)}', 'danger')
        print(f"Erro detalhado: {e}")
        return redirect(url_for('limite.analise_limites'))



@limite_bp.route('/limites/novo', methods=['GET', 'POST'])
@login_required
def novo_limite():
    editais = Edital.query.filter(Edital.DELETED_AT == None).all()
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).all()
    empresas = EmpresaParticipante.query.filter(EmpresaParticipante.DELETED_AT == None).all()
    criterios = CriterioSelecao.query.filter(CriterioSelecao.DELETED_AT == None).all()

    # Verificar se há editais e períodos cadastrados
    if not editais:
        flash('Não há editais cadastrados. Cadastre um edital primeiro.', 'warning')
        return redirect(url_for('edital.lista_editais'))

    if not periodos:
        flash('Não há períodos cadastrados. Cadastre um período primeiro.', 'warning')
        return redirect(url_for('periodo.lista_periodos'))

    if not empresas:
        flash('Não há empresas cadastradas. Cadastre uma empresa primeiro.', 'warning')
        return redirect(url_for('periodo.lista_periodos'))

    if request.method == 'POST':
        try:
            edital_id = int(request.form['edital_id'])
            periodo_id = int(request.form['periodo_id'])
            empresa_id = int(request.form['empresa_id'])
            cod_criterio = int(request.form['cod_criterio'])

            # Valores opcionais
            qtde_maxima = request.form.get('qtde_maxima')
            if qtde_maxima:
                qtde_maxima = int(qtde_maxima)

            valor_maximo = request.form.get('valor_maximo')
            if valor_maximo:
                valor_maximo = float(valor_maximo)

            percentual_final = request.form.get('percentual_final')
            if percentual_final:
                percentual_final = float(percentual_final)

            # Verificar se já existe limite para esta combinação
            limite_existente = LimiteDistribuicao.query.filter_by(
                ID_EDITAL=edital_id,
                ID_PERIODO=periodo_id,
                ID_EMPRESA=empresa_id,
                COD_CRITERIO_SELECAO=cod_criterio,
                DELETED_AT=None
            ).first()

            if limite_existente:
                flash('Já existe um limite cadastrado com estes critérios.', 'danger')
                return render_template('credenciamento/form_limite.html', editais=editais, periodos=periodos,
                                       empresas=empresas,
                                       criterios=criterios)

            novo_limite = LimiteDistribuicao(
                ID_EDITAL=edital_id,
                ID_PERIODO=periodo_id,
                ID_EMPRESA=empresa_id,
                COD_CRITERIO_SELECAO=cod_criterio,
                QTDE_MAXIMA=qtde_maxima,
                VALOR_MAXIMO=valor_maximo,
                PERCENTUAL_FINAL=percentual_final
            )

            db.session.add(novo_limite)
            db.session.commit()

            # Registrar log de auditoria
            dados_novos = {
                'id_edital': edital_id,
                'id_periodo': periodo_id,
                'id_empresa': empresa_id,
                'cod_criterio': cod_criterio,
                'qtde_maxima': qtde_maxima,
                'valor_maximo': valor_maximo,
                'percentual_final': percentual_final
            }

            registrar_log(
                acao='criar',
                entidade='limite',
                entidade_id=novo_limite.ID,
                descricao=f'Cadastro de limite de distribuição',
                dados_novos=dados_novos
            )

            flash('Limite de distribuição cadastrado com sucesso!', 'success')
            return redirect(url_for('limite.lista_limites'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')

    return render_template('credenciamento/form_limite.html', editais=editais, periodos=periodos, empresas=empresas,
                           criterios=criterios)


@limite_bp.route('/limites/editar/<int:id>', methods=['GET', 'POST'])
@login_required
def editar_limite(id):
    limite = LimiteDistribuicao.query.get_or_404(id)
    editais = Edital.query.filter(Edital.DELETED_AT == None).all()
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).all()
    empresas = EmpresaParticipante.query.filter(EmpresaParticipante.DELETED_AT == None).all()
    criterios = CriterioSelecao.query.filter(CriterioSelecao.DELETED_AT == None).all()

    if request.method == 'POST':
        try:
            # Capturar dados antigos para auditoria
            dados_antigos = {
                'id_edital': limite.ID_EDITAL,
                'id_periodo': limite.ID_PERIODO,
                'id_empresa': limite.ID_EMPRESA,
                'cod_criterio': limite.COD_CRITERIO_SELECAO,
                'qtde_maxima': limite.QTDE_MAXIMA,
                'valor_maximo': limite.VALOR_MAXIMO,
                'percentual_final': limite.PERCENTUAL_FINAL
            }

            # Atualizar dados
            limite.ID_EDITAL = int(request.form['edital_id'])
            limite.ID_PERIODO = int(request.form['periodo_id'])
            limite.ID_EMPRESA = int(request.form['empresa_id'])
            limite.COD_CRITERIO_SELECAO = int(request.form['cod_criterio'])

            # Valores opcionais
            qtde_maxima = request.form.get('qtde_maxima')
            if qtde_maxima:
                limite.QTDE_MAXIMA = int(qtde_maxima)
            else:
                limite.QTDE_MAXIMA = None

            valor_maximo = request.form.get('valor_maximo')
            if valor_maximo:
                limite.VALOR_MAXIMO = float(valor_maximo)
            else:
                limite.VALOR_MAXIMO = None

            percentual_final = request.form.get('percentual_final')
            if percentual_final:
                limite.PERCENTUAL_FINAL = float(percentual_final)
            else:
                limite.PERCENTUAL_FINAL = None

            limite.UPDATED_AT = datetime.utcnow()

            # Verificar se já existe limite para esta combinação (excluindo o próprio registro)
            limite_existente = LimiteDistribuicao.query.filter(
                LimiteDistribuicao.ID_EDITAL == limite.ID_EDITAL,
                LimiteDistribuicao.ID_PERIODO == limite.ID_PERIODO,
                LimiteDistribuicao.ID_EMPRESA == limite.ID_EMPRESA,
                LimiteDistribuicao.COD_CRITERIO_SELECAO == limite.COD_CRITERIO_SELECAO,
                LimiteDistribuicao.DELETED_AT == None,
                LimiteDistribuicao.ID != id
            ).first()

            if limite_existente:
                flash('Já existe um limite cadastrado com estes critérios.', 'danger')
                return render_template(
                    'credenciamento/form_limite.html',
                    limite=limite,
                    editais=editais,
                    periodos=periodos,
                    empresas=empresas,
                    criterios=criterios
                )

            # Dados novos para auditoria
            dados_novos = {
                'id_edital': limite.ID_EDITAL,
                'id_periodo': limite.ID_PERIODO,
                'id_empresa': limite.ID_EMPRESA,
                'cod_criterio': limite.COD_CRITERIO_SELECAO,
                'qtde_maxima': limite.QTDE_MAXIMA,
                'valor_maximo': limite.VALOR_MAXIMO,
                'percentual_final': limite.PERCENTUAL_FINAL
            }

            db.session.commit()

            # Registrar log de auditoria
            registrar_log(
                acao='editar',
                entidade='limite',
                entidade_id=limite.ID,
                descricao=f'Atualização de limite de distribuição',
                dados_antigos=dados_antigos,
                dados_novos=dados_novos
            )

            flash('Limite de distribuição atualizado com sucesso!', 'success')
            return redirect(url_for('limite.lista_limites'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')

    return render_template(
        'credenciamento/form_limite.html',
        limite=limite,
        editais=editais,
        periodos=periodos,
        empresas=empresas,
        criterios=criterios)

@limite_bp.route('/limites/excluir/<int:id>', methods=['POST'])
@login_required
def excluir_limite(id):
    limite = LimiteDistribuicao.query.get_or_404(id)

    try:
        # Capturar dados antigos para auditoria
        dados_antigos = {
            'id_edital': limite.ID_EDITAL,
            'id_periodo': limite.ID_PERIODO,
            'id_empresa': limite.ID_EMPRESA,
            'cod_criterio': limite.COD_CRITERIO_SELECAO,
            'qtde_maxima': limite.QTDE_MAXIMA,
            'valor_maximo': limite.VALOR_MAXIMO,
            'percentual_final': limite.PERCENTUAL_FINAL
        }

        # Soft delete - apenas marca como excluído
        limite.DELETED_AT = datetime.utcnow()
        db.session.commit()

        # Registrar log de auditoria
        registrar_log(
            acao='excluir',
            entidade='limite',
            entidade_id=limite.ID,
            descricao=f'Exclusão de limite de distribuição',
            dados_antigos=dados_antigos
        )

        flash('Limite de distribuição excluído com sucesso!', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao excluir limite de distribuição: {str(e)}', 'danger')

    return redirect(url_for('limite.lista_limites'))


@limite_bp.route('/limites/filtrar', methods=['GET'])
@login_required
def filtrar_limites():
    periodo_id = request.args.get('periodo_id', type=int)
    criterio_id = request.args.get('criterio_id', type=int)

    # Redirecionar para a lista com os parâmetros de filtro
    return redirect(url_for('limite.lista_limites', periodo_id=periodo_id, criterio_id=criterio_id))


@limite_bp.route('/limites/detalhe/<int:id>')
@login_required
def detalhe_limite(id):
    # Consulta para obter o limite e a descrição do critério
    resultado = db.session.query(
        LimiteDistribuicao,
        CriterioSelecao.DS_CRITERIO_SELECAO,
        Edital.NU_EDITAL,
        Edital.ANO,
        PeriodoAvaliacao.DT_INICIO,
        PeriodoAvaliacao.DT_FIM,
        EmpresaParticipante.NO_EMPRESA
    ).outerjoin(
        CriterioSelecao,
        LimiteDistribuicao.COD_CRITERIO_SELECAO == CriterioSelecao.COD
    ).outerjoin(
        Edital,
        LimiteDistribuicao.ID_EDITAL == Edital.ID
    ).outerjoin(
        PeriodoAvaliacao,
        LimiteDistribuicao.ID_PERIODO == PeriodoAvaliacao.ID
    ).outerjoin(
        EmpresaParticipante,
        LimiteDistribuicao.ID_EMPRESA == EmpresaParticipante.ID_EMPRESA
    ).filter(
        LimiteDistribuicao.ID == id,
        LimiteDistribuicao.DELETED_AT == None
    ).first_or_404()

    limite, ds_criterio, nu_edital, nu_ano, dt_inicio, dt_fim, no_empresa = resultado

    return render_template(
        'credenciamento/detalhe_limite.html',
        limite=limite,
        ds_criterio=ds_criterio,
        nu_edital=nu_edital,
        nu_ano=nu_ano,
        dt_inicio=dt_inicio,
        dt_fim=dt_fim,
        no_empresa=no_empresa
    )