from flask import Blueprint, render_template, redirect, url_for, request, flash
from app.models.limite_distribuicao import LimiteDistribuicao
from app.models.edital import Edital
from app.models.periodo import PeriodoAvaliacao
from app.models.empresa_participante import EmpresaParticipante
from app.models.criterio_selecao import CriterioSelecao
from app import db
from datetime import datetime
from flask_login import login_required
from app.utils.audit import registrar_log
from sqlalchemy import or_, func, text
import math

limite_bp = Blueprint('limite', __name__, url_prefix='/credenciamento')


@limite_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}

# Função auxiliar para truncar valores decimais
def truncate_decimal(valor, casas=2):
    """
    Trunca um valor decimal para o número de casas especificado,
    sem realizar nenhum arredondamento.
    """
    fator = 10 ** casas
    return int(valor * fator) / fator

@limite_bp.app_template_filter('truncate_2dec')
def truncate_2dec_filter(value):
    """Filtro Jinja2 para truncar valores com duas casas decimais."""
    if isinstance(value, (int, float)):
        return "{0:.2f}".format(int(value * 100) / 100)
    return value

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
                empresa_resp = EmpresaResponsavel.query.filter_by(
                    pkEmpresaResponsavelCobranca=limite.ID_EMPRESA).first()
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


def ajustar_percentuais_tabela1(empresas):
    """
    Ajusta os percentuais na tabela de participação na arrecadação (Tabela 1)

    Args:
        empresas: Lista de objetos EmpresaParticipante

    Returns:
        Lista de dicionários com percentuais ajustados
    """
    # Converter objetos para dicionários
    dados_empresas = []
    for idx, empresa in enumerate(empresas):
        dados_empresas.append({
            'idx': idx + 1,
            'id_empresa': empresa.ID_EMPRESA,
            'empresa': empresa.NO_EMPRESA_ABREVIADA or empresa.NO_EMPRESA,
            'situacao': empresa.DS_CONDICAO,
            'arrecadacao': getattr(empresa, 'arrecadacao', 0.0),
        })

    # Calcular total de arrecadação
    total_arrecadacao = sum(e['arrecadacao'] for e in dados_empresas)

    # Calcular percentual de arrecadação para cada empresa
    for empresa in dados_empresas:
        if total_arrecadacao > 0:
            empresa['pct_arrecadacao'] = (empresa['arrecadacao'] / total_arrecadacao) * 100
        else:
            empresa['pct_arrecadacao'] = 0.0

    # Separar empresas por situação
    empresas_permanece = [e for e in dados_empresas if e['situacao'] == 'PERMANECE']
    empresas_descredenciadas = [e for e in dados_empresas if e['situacao'] != 'PERMANECE']

    # Garantir que empresas não PERMANECE tenham situação DESCREDENCIADA
    for empresa in empresas_descredenciadas:
        empresa['situacao'] = 'DESCREDENCIADA'

    # Calcular percentual total das empresas que saem
    pct_total_saem = sum(e['pct_arrecadacao'] for e in empresas_descredenciadas)

    # Redistribuir percentual igualmente entre empresas que permanecem
    pct_redistribuicao = pct_total_saem / len(empresas_permanece) if empresas_permanece else 0

    # Inicializar valores para todas as empresas
    for empresa in dados_empresas:
        if empresa['situacao'] == 'PERMANECE':
            empresa['pct_redistribuido'] = pct_redistribuicao
            # %NOVO é a soma do percentual de arrecadação + redistribuído
            empresa['pct_novo'] = empresa['pct_arrecadacao'] + pct_redistribuicao
            # Truncar para 2 casas decimais sem arredondamento
            empresa['pct_novo_truncado'] = int(empresa['pct_novo'] * 100) / 100
            empresa['ajuste'] = 0.0  # Inicializar com zero
        else:
            empresa['pct_redistribuido'] = 0.0
            empresa['pct_novo'] = 0.0
            empresa['pct_novo_truncado'] = 0.0
            empresa['ajuste'] = 0.0
            empresa['pct_novofinal'] = 0.0

    # Calcular a soma dos percentuais truncados
    soma_truncada = sum(e['pct_novo_truncado'] for e in empresas_permanece)

    # Calcular a diferença para 100%
    diferenca = 100.0 - soma_truncada

    # ALGORITMO CORRIGIDO:
    # Aplicar ajustes de 0.01% em ciclos, garantindo que cada empresa receba
    # um incremento por ciclo completo, em ordem decrescente de arrecadação
    if diferenca > 0 and empresas_permanece:
        # Ordenar empresas por arrecadação (decrescente)
        empresas_ordenadas = sorted(empresas_permanece, key=lambda x: x['arrecadacao'], reverse=True)

        # Calcular quantos ajustes de 0.01% são necessários
        ajustes_necessarios = int(diferenca * 100)  # Para 0.01%

        # Inicializar contador de ciclos para cada empresa
        ajustes_recebidos = [0] * len(empresas_ordenadas)

        # Distribuir ajustes em ciclos completos
        for _ in range(ajustes_necessarios):
            # Encontrar o menor número de ajustes que qualquer empresa recebeu
            min_ajustes = min(ajustes_recebidos)

            # Encontrar todas as empresas com esse mínimo de ajustes
            candidatos = []
            for i, ajustes in enumerate(ajustes_recebidos):
                if ajustes == min_ajustes:
                    candidatos.append(i)

            # Se não há candidatos (não deve acontecer), sair do loop
            if not candidatos:
                break

            # Selecionar o primeiro candidato (já está ordenado por arrecadação)
            idx = candidatos[0]

            # Aplicar incremento de 0.01%
            empresas_ordenadas[idx]['ajuste'] += 0.01
            ajustes_recebidos[idx] += 1

    # Calcular percentual final (%NOVO + Ajuste)
    for empresa in dados_empresas:
        if empresa['situacao'] == 'PERMANECE':
            empresa['pct_novofinal'] = empresa['pct_novo_truncado'] + empresa['ajuste']

    # Verificar se a soma total é exatamente 100%
    soma_final = sum(e['pct_novofinal'] for e in empresas_permanece)

    # Ajuste fino para garantir 100%
    if abs(soma_final - 100.0) > 0.001 and empresas_permanece:
        diferenca_final = 100.0 - soma_final
        if abs(diferenca_final) <= 0.01:
            # Aplicar a diferença na empresa com maior arrecadação
            empresas_por_arrecadacao = sorted(empresas_permanece, key=lambda x: x['arrecadacao'], reverse=True)
            empresas_por_arrecadacao[0]['ajuste'] += diferenca_final
            empresas_por_arrecadacao[0]['pct_novofinal'] += diferenca_final

    # Adicionar linha de total
    total_row = {
        'idx': 'TOTAL',
        'empresa': 'TOTAL',
        'situacao': '',
        'arrecadacao': sum(e['arrecadacao'] for e in dados_empresas),
        'pct_arrecadacao': sum(e['pct_arrecadacao'] for e in dados_empresas),
        'pct_redistribuido': sum(e['pct_redistribuido'] for e in dados_empresas),
        'pct_novo': sum(e['pct_novo_truncado'] for e in empresas_permanece),
        'pct_novo_truncado': sum(e['pct_novo_truncado'] for e in empresas_permanece),
        'ajuste': sum(e['ajuste'] for e in empresas_permanece),
        'pct_novofinal': sum(e['pct_novofinal'] for e in empresas_permanece)
    }

    dados_empresas.append(total_row)

    return dados_empresas


def distribuir_contratos_tabela3(empresas_permanece, total_contratos_permanece):
    """
    Distribui contratos para empresas que permanecem com base no percentual final,
    respeitando a ordem de faturamento para ajustes.

    Args:
        empresas_permanece: Lista de objetos com empresas que permanecem
        total_contratos_permanece: Total de contratos a distribuir (da Tabela 2)

    Returns:
        Lista atualizada de empresas com contratos, ajustes e totais calculados
    """
    # Ordenar empresas por faturamento (decrescente)
    empresas_ordenadas = sorted(empresas_permanece, key=lambda x: x.get('arrecadacao', 0), reverse=True)

    # Inicializar campos para cada empresa
    for empresa in empresas_ordenadas:
        # Calcular quantidade base de contratos (truncado, sem arredondamento)
        pct_novofinal = empresa.get('pct_novofinal', 0)
        empresa['contratos'] = int(total_contratos_permanece * pct_novofinal / 100)  # Truncar para inteiro
        empresa['ajuste_contratos'] = 0  # Inicializa ajustes

    # Calcular contratos distribuídos e sobra
    contratos_distribuidos = sum(empresa['contratos'] for empresa in empresas_ordenadas)
    sobra_contratos = total_contratos_permanece - contratos_distribuidos

    # Distribuir sobras uma a uma, em ciclos, por ordem de faturamento
    if sobra_contratos > 0:
        # Inicializar contador de ajustes recebidos por cada empresa
        ajustes_recebidos = [0] * len(empresas_ordenadas)

        # Distribuir cada contrato da sobra
        for _ in range(sobra_contratos):
            # Encontrar empresas com menor número de ajustes
            min_ajustes = min(ajustes_recebidos)
            candidatos = [i for i, v in enumerate(ajustes_recebidos) if v == min_ajustes]

            # Selecionar a primeira empresa candidata (mantém ordem de faturamento)
            idx = candidatos[0]

            # Aplicar ajuste
            empresas_ordenadas[idx]['contratos'] += 1
            empresas_ordenadas[idx]['ajuste_contratos'] += 1
            ajustes_recebidos[idx] += 1

    # Verificar se o total está correto (soma dos contratos = total_contratos_permanece)
    total_final = sum(empresa['contratos'] for empresa in empresas_ordenadas)
    assert total_final == total_contratos_permanece, "Erro na distribuição dos contratos"

    return empresas_ordenadas


def distribuir_restantes_tabela4(empresas_novas, total_contratos_novas):
    """
    Distribui contratos para empresas novas de forma igualitária e aleatória.
    """
    import random
    print(f"DEBUG: Iniciando distribuição para {len(empresas_novas)} empresas novas, total: {total_contratos_novas}")

    # Verificação de segurança
    if not empresas_novas:
        print("AVISO: Lista de empresas novas está vazia!")
        return []

    if total_contratos_novas <= 0:
        print(f"AVISO: Total de contratos para novas empresas é inválido: {total_contratos_novas}")
        return empresas_novas

    n = len(empresas_novas)

    # 1) Base igualitária (truncada)
    base = int(total_contratos_novas / n)  # Usar divisão inteira para truncar
    print(f"DEBUG: Base por empresa: {base}")

    # Inicializar campos em cada empresa
    for i, emp in enumerate(empresas_novas):
        emp['contratos'] = base
        emp['ajuste_contratos'] = 0
        print(f"DEBUG: Empresa {i + 1} ({emp.get('empresa', '?')}): base = {base}")

    # 2) Distribuir a sobra aleatoriamente em ciclos
    sobra = total_contratos_novas - (base * n)
    print(f"DEBUG: Sobra para distribuição aleatória: {sobra}")

    if sobra > 0:
        # Criar índices para distribuição e embaralhar aleatoriamente
        indices = list(range(n))
        random.shuffle(indices)

        # Distribuir um por um seguindo a ordem aleatória em ciclos
        for i in range(sobra):
            idx = indices[i % n]
            empresas_novas[idx]['contratos'] += 1
            empresas_novas[idx]['ajuste_contratos'] += 1
            print(f"DEBUG: Ajuste de +1 contrato para empresa {idx + 1} ({empresas_novas[idx].get('empresa', '?')})")

    # Verificação final
    total_distribuido = sum(emp.get('contratos', 0) for emp in empresas_novas)
    print(f"DEBUG: Total distribuído: {total_distribuido} vs esperado: {total_contratos_novas}")

    return empresas_novas

def ajustar_percentuais_tabela5(empresas_todas, total_contratos):
    """
    Ajusta percentuais finais (Tabela 5)

    Args:
        empresas_todas: Lista de todas as empresas com contratos já distribuídos
        total_contratos: Total geral de contratos

    Returns:
        Lista de empresas com percentuais finais
    """
    import random

    # Calcular percentuais base
    for empresa in empresas_todas:
        # %Base = contratos / total_contratos * 100 (truncado a 2 casas)
        empresa['pct_base'] = int((empresa['contratos'] / total_contratos * 100) * 100) / 100
        empresa['ajuste_pct'] = 0.0

    # Calcular total de percentual base
    total_pct_base = sum(e['pct_base'] for e in empresas_todas)

    # Calcular diferença para 100%
    diferenca = 100.0 - total_pct_base

    # Aplicar ajustes para chegar a 100%
    if diferenca > 0:
        # Separar empresas por tipo
        permanece = [e for e in empresas_todas if e['situacao'] == 'PERMANECE']
        novas = [e for e in empresas_todas if e['situacao'] == 'NOVA']

        # Ordenar "PERMANECE" por arrecadação (decrescente)
        permanece = sorted(permanece, key=lambda x: x.get('arrecadacao', 0), reverse=True)

        # Randomizar ordem das empresas novas
        random.shuffle(novas)

        # Ciclo de aplicação: primeiro PERMANECE, depois NOVAS
        ciclo = permanece + novas

        # Inicializar contadores de ajustes
        ajustes_recebidos = [0] * len(ciclo)

        # Aplicar ajustes de 0.01% em ciclos
        ajustes_necessarios = int(diferenca * 100)  # Quantos incrementos de 0.01%

        for _ in range(ajustes_necessarios):
            # Encontrar a empresa com menos ajustes
            min_ajustes = min(ajustes_recebidos)
            candidatos = [i for i, val in enumerate(ajustes_recebidos) if val == min_ajustes]

            # Selecionar empresa candidata
            idx_selecionado = candidatos[0]

            # Aplicar ajuste
            ciclo[idx_selecionado]['ajuste_pct'] += 0.01
            ajustes_recebidos[idx_selecionado] += 1

    # Calcular percentuais finais
    for empresa in empresas_todas:
        empresa['pct_final'] = empresa['pct_base'] + empresa['ajuste_pct']

    # Verificação final (garantia de 100% exato)
    total_final = sum(e['pct_final'] for e in empresas_todas)
    if abs(total_final - 100.0) > 0.0001 and empresas_todas:
        diferenca_final = 100.0 - total_final
        empresas_todas[0]['ajuste_pct'] += diferenca_final
        empresas_todas[0]['pct_final'] += diferenca_final

    return empresas_todas

def modulo_empresas_mistas(ultimo_edital, ultimo_periodo, periodo_anterior, empresas, num_contratos):
    """
    Função principal para o cálculo de distribuição com empresas mistas

    Args:
        ultimo_edital: Objeto do último edital
        ultimo_periodo: Objeto do último período
        periodo_anterior: Objeto do período anterior
        empresas: Lista de objetos EmpresaParticipante
        num_contratos: Número total de contratos a distribuir

    Returns:
        Dicionário com resultados para cada tabela
    """
    # Separar empresas por situação
    empresas_permanece = [e for e in empresas if e.DS_CONDICAO == 'PERMANECE']
    empresas_novas = [e for e in empresas if e.DS_CONDICAO == 'NOVA']

    # 1. Calcular percentuais e ajustes para Tabela 1
    resultados_tabela1 = ajustar_percentuais_tabela1(empresas)

    # 2. Distribuir contratos por situação (Tabela 2)
    qtde_empresas_permanece = len(empresas_permanece)
    qtde_empresas_novas = len(empresas_novas)
    qtde_total_empresas = qtde_empresas_permanece + qtde_empresas_novas

    # Calcular contratos para cada tipo
    qtde_contratos_permanece = int(
        (qtde_empresas_permanece / qtde_total_empresas) * num_contratos) if qtde_total_empresas > 0 else 0
    qtde_contratos_novas = num_contratos - qtde_contratos_permanece

    # 3. Distribuir contratos para empresas que permanecem (Tabela 3)
    dados_permanece = [e for e in resultados_tabela1 if e['situacao'] == 'PERMANECE']
    resultados_tabela3 = distribuir_contratos_tabela3(dados_permanece, qtde_contratos_permanece)

    # 4. Distribuir contratos para empresas novas (Tabela 4)
    dados_novas = [e for e in resultados_tabela1 if e['situacao'] == 'NOVA']
    resultados_tabela4 = distribuir_restantes_tabela4(dados_novas, qtde_contratos_novas)

    # 5. Calcular percentuais finais (Tabela 5)
    dados_combinados = resultados_tabela3 + resultados_tabela4
    resultados_tabela5 = ajustar_percentuais_tabela5(dados_combinados, num_contratos)

    # Adicionar empresas descredenciadas aos resultados finais
    dados_descredenciadas = [e for e in resultados_tabela1 if e['situacao'] == 'DESCREDENCIADA']

    # Montar o resultado final incluindo todas as empresas
    resultados_finais = resultados_tabela5 + dados_descredenciadas

    # Adicionar linha de total
    # Calcular totais
    total_arrecadacao = sum(e.get('arrecadacao', 0) for e in resultados_finais)
    total_pct_arrecadacao = sum(e.get('pct_arrecadacao', 0) for e in resultados_finais)
    total_pct_redistribuido = sum(e.get('pct_redistribuido', 0) for e in resultados_finais)
    total_pct_novo = sum(e.get('pct_novo_truncado', 0) for e in resultados_finais)
    total_ajuste = sum(e.get('ajuste', 0) for e in resultados_finais)
    total_pct_final = sum(e.get('pct_final', 0) for e in resultados_finais)
    total_contratos = sum(e.get('contratos', 0) for e in resultados_finais)

    # Adicionar linha de total
    linha_total = {
        'idx': 'TOTAL',
        'empresa': 'TOTAL',
        'situacao': '',
        'arrecadacao': total_arrecadacao,
        'pct_arrecadacao': total_pct_arrecadacao,
        'pct_redistribuido': total_pct_redistribuido,
        'pct_novo': total_pct_novo,
        'pct_novo_truncado': total_pct_novo,
        'ajuste': total_ajuste,
        'pct_final': total_pct_final,
        'contratos': total_contratos,
        'ajuste_contratos': sum(e.get('ajuste_contratos', 0) for e in resultados_finais),
        'total_contratos': sum(e.get('total_contratos', 0) for e in resultados_finais),
        'pct_base': sum(e.get('pct_base', 0) for e in resultados_finais),
        'ajuste_pct': sum(e.get('ajuste_pct', 0) for e in resultados_finais),
        'pct_novofinal': sum(e.get('pct_novofinal', 0) for e in resultados_finais)
    }

    resultados_finais.append(linha_total)

    # Retornar resultados consolidados
    return {
        'resultados': resultados_finais,  # Resultado final com todos os dados
        'todas_empresas_anteriores': {e['id_empresa']: e for e in resultados_tabela1},
        'empresas_que_saem': {e['id_empresa']: e for e in resultados_tabela1 if e['situacao'] == 'DESCREDENCIADA'},
        'num_contratos': num_contratos,
        'tipo_calculo': 'mistas',
        'qtde_empresas_permanece': qtde_empresas_permanece,
        'qtde_empresas_novas': qtde_empresas_novas,
        'qtde_contratos_permanece': qtde_contratos_permanece,
        'qtde_contratos_novas': qtde_contratos_novas,
        'total_pct_que_saem': sum(
            e['pct_arrecadacao'] for e in resultados_tabela1 if e['situacao'] == 'DESCREDENCIADA'),
        'valor_redistribuicao': sum(
            e['pct_redistribuido'] for e in resultados_tabela1 if e['situacao'] == 'PERMANECE') / len(
            empresas_permanece) if empresas_permanece else 0
    }

def calcular_limites_empresas_permanece(ultimo_edital, ultimo_periodo, periodo_anterior):
    """
    Realiza o cálculo dos limites de distribuição quando todas as empresas permanecem.

    Args:
        ultimo_edital: Objeto Edital com o último edital
        ultimo_periodo: Objeto PeriodoAvaliacao com o último período
        periodo_anterior: Objeto PeriodoAvaliacao com o período anterior

    Returns:
        dict: Dicionário com os resultados do cálculo e metadados
    """
    try:
        # Obter os contratos distribuíveis
        num_contratos = selecionar_contratos()
        if num_contratos <= 0:
            return None

        # Buscar empresas participantes
        empresas = EmpresaParticipante.query.filter(
            EmpresaParticipante.ID_EDITAL == ultimo_edital.ID,
            EmpresaParticipante.ID_PERIODO == ultimo_periodo.ID_PERIODO,
            EmpresaParticipante.DELETED_AT == None
        ).all()

        if not empresas:
            return None

        # Buscar dados de arrecadação do período anterior
        with db.engine.connect() as connection:
            # Query para obter arrecadação das empresas no período anterior
            sql = text("""
            SELECT 
                EP.ID_EMPRESA,
                EP.NO_EMPRESA,
                EP.NO_EMPRESA_ABREVIADA,
                EP.DS_CONDICAO,
                -- Obter dados de arrecadação da tabela real
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
                return None

            # Processar dados de arrecadação reais
            dados_arrecadacao = []
            for row in rows:
                dados_arrecadacao.append({
                    'id_empresa': row[0],
                    'nome': row[1],
                    'nome_abreviado': row[2] or (row[1][:3] if row[1] else ''),
                    'situacao': row[3],
                    'arrecadacao': float(row[4]) if row[4] else 0.0
                })

            # Ordenar por arrecadação (maior para menor)
            dados_arrecadacao.sort(key=lambda x: x['arrecadacao'], reverse=True)

            # Calcular arrecadação total
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
                    'ajuste': 0.00,  # Será atualizado posteriormente
                    'pct_final': pct_arrecadacao  # Será atualizado posteriormente
                })

            # Calcular a soma dos percentuais truncados
            soma_percentuais = truncate_decimal(sum(item['pct_arrecadacao'] for item in dados_processados))

            # Calcular quanto falta para chegar a 100%
            diferenca = truncate_decimal(100.00 - soma_percentuais)

            # Aplicar ajuste de 0,01% às empresas com maior arrecadação até chegar exatamente a 100%
            if diferenca > 0:
                # Quantas empresas precisam ser ajustadas inicialmente (um "ciclo" completo de empresas)
                ajuste_por_empresa = 0.01
                num_ciclos_completos = int(diferenca / (ajuste_por_empresa * len(dados_processados)))
                ajustes_restantes = int(
                    (diferenca % (ajuste_por_empresa * len(dados_processados))) / ajuste_por_empresa)

                # Aplicar ajustes para ciclos completos (todas as empresas recebem ajuste)
                if num_ciclos_completos > 0:
                    for i in range(len(dados_processados)):
                        dados_processados[i]['ajuste'] = truncate_decimal(num_ciclos_completos * ajuste_por_empresa)
                        dados_processados[i]['pct_final'] = truncate_decimal(
                            dados_processados[i]['pct_arrecadacao'] + dados_processados[i]['ajuste'])

                # Aplicar ajustes restantes (apenas algumas empresas recebem ajuste adicional)
                for i in range(ajustes_restantes):
                    # O índice sempre será menor que o número de empresas
                    indice = i % len(dados_processados)
                    dados_processados[indice]['ajuste'] = truncate_decimal(
                        dados_processados[indice]['ajuste'] + ajuste_por_empresa)
                    dados_processados[indice]['pct_final'] = truncate_decimal(
                        dados_processados[indice]['pct_arrecadacao'] + dados_processados[indice]['ajuste'])

                # Recalcular totais
                total_pct_arrecadacao = truncate_decimal(sum(item['pct_arrecadacao'] for item in dados_processados))
                total_ajuste = truncate_decimal(sum(item['ajuste'] for item in dados_processados))
                total_pct_final = truncate_decimal(sum(item['pct_final'] for item in dados_processados))

                # Verificar se o total final é exatamente 100%
                if total_pct_final != 100.00:
                    # Se ainda não for exatamente 100%, ajustar a primeira empresa
                    diferenca_restante = truncate_decimal(100.00 - total_pct_final)
                    dados_processados[0]['pct_final'] = truncate_decimal(
                        dados_processados[0]['pct_final'] + diferenca_restante)
                    dados_processados[0]['ajuste'] = truncate_decimal(
                        dados_processados[0]['ajuste'] + diferenca_restante)

                    # Recalcular totais finais
                    total_ajuste = truncate_decimal(sum(item['ajuste'] for item in dados_processados))
                    total_pct_final = truncate_decimal(sum(item['pct_final'] for item in dados_processados))

            # Adicionar linha de total
            dados_processados.append({
                'idx': 'TOTAL',
                'id_empresa': '',
                'empresa': 'TOTAL',
                'situacao': '',
                'arrecadacao': total_arrecadacao,
                'pct_arrecadacao': soma_percentuais,
                'ajuste': total_ajuste if 'total_ajuste' in locals() else 0.00,
                'pct_final': total_pct_final if 'total_pct_final' in locals() else soma_percentuais
            })

            # Retornar resultados e metadados
            return {
                'resultados': dados_processados,
                'num_contratos': num_contratos,
                'tipo_calculo': 'permanece'
            }

    except Exception as e:
        print(f"Erro no cálculo para empresas permanece: {str(e)}")
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
        dict: Dicionário com os resultados do cálculo e metadados
    """
    try:
        # Obter o número de contratos distribuíveis
        num_contratos = selecionar_contratos()
        if num_contratos <= 0:
            return None

        # Número de empresas participantes
        qtde_empresas = len(empresas)
        if qtde_empresas <= 0:
            return None

        # Calcular o percentual base para cada empresa (truncado para duas casas sem arredondamento)
        percentual_base = truncate_decimal(100.00 / qtde_empresas)

        # Calcular a diferença que precisa ser distribuída
        total_percentual_base = truncate_decimal(percentual_base * qtde_empresas)
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
            resultados[indice_escolhido]['ajuste'] = truncate_decimal(contador_ajustes[indice_escolhido] * incremento)
            resultados[indice_escolhido]['pct_final'] = truncate_decimal(
                percentual_base + resultados[indice_escolhido]['ajuste'])

        # Verificação CRÍTICA: Garantir que a soma seja EXATAMENTE 100.00%
        soma_pct_final = truncate_decimal(sum(item['pct_final'] for item in resultados))

        # Se não for exatamente 100.00%, fazer ajuste adicional
        if soma_pct_final != 100.00:
            # Calcular diferença exata
            diferenca_final = truncate_decimal(100.00 - soma_pct_final)

            # Arredondar para exatamente 0.01 se estiver próximo
            if abs(diferenca_final - 0.01) < 0.001:
                diferenca_final = 0.01

            # Encontrar a empresa com menos incrementos para adicionar a diferença
            menor_contador_final = min(contador_ajustes)
            indices_menor_final = [i for i, v in enumerate(contador_ajustes) if v == menor_contador_final]
            indice_ajuste_final = choice(indices_menor_final)

            # Aplicar o ajuste final
            resultados[indice_ajuste_final]['ajuste'] = truncate_decimal(
                resultados[indice_ajuste_final]['ajuste'] + diferenca_final)
            resultados[indice_ajuste_final]['pct_final'] = truncate_decimal(
                percentual_base + resultados[indice_ajuste_final]['ajuste']
            )

        # Calcular totais finais
        total_pct_distribuicao = truncate_decimal(sum(item['pct_distribuicao'] for item in resultados))
        total_ajuste = truncate_decimal(sum(item['ajuste'] for item in resultados))
        total_pct_final = truncate_decimal(sum(item['pct_final'] for item in resultados))

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

        # Retornar resultados e metadados
        return {
            'resultados': resultados,
            'num_contratos': num_contratos,
            'tipo_calculo': 'novas'
        }

    except Exception as e:
        print(f"Erro no cálculo para empresas novas: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return None


def calcular_limites_empresas_mistas(ultimo_edital, ultimo_periodo, periodo_anterior, empresas):
    """
    Realiza o cálculo dos limites de distribuição quando há empresas que permanecem e empresas novas,
    incluindo Tabela 1 (pct), Tabela 2 (qtde por situação), Tabela 3 (distribuição para permanece)
    e Tabela 4 (distribuição para novas).
    """
    try:
        # 1) Obter total de contratos distribuíveis (Tabela 2)
        num_contratos = selecionar_contratos()
        if num_contratos <= 0:
            return None

        # 2) Buscar dados de arrecadação do período anterior
        with db.engine.connect() as connection:
            sql = text("""
                SELECT 
                    EP.ID_EMPRESA,
                    COALESCE(REE.VR_ARRECADACAO, 0) AS VR_ARRECADACAO
                FROM DEV.DCA_TB002_EMPRESAS_PARTICIPANTES AS EP
                LEFT JOIN (
                    SELECT 
                        CO_EMPRESA_COBRANCA, 
                        SUM(VR_ARRECADACAO_TOTAL) AS VR_ARRECADACAO
                    FROM BDG.COM_TB062_REMUNERACAO_ESTIMADA
                    WHERE DT_ARRECADACAO BETWEEN :data_inicio AND :data_fim
                    GROUP BY CO_EMPRESA_COBRANCA
                ) REE ON EP.ID_EMPRESA = REE.CO_EMPRESA_COBRANCA
                WHERE EP.ID_EDITAL = :id_edital
                  AND EP.ID_PERIODO = :id_periodo_anterior
                  AND EP.DELETED_AT IS NULL
            """).bindparams(
                id_edital=ultimo_edital.ID,
                id_periodo_anterior=periodo_anterior.ID_PERIODO,
                data_inicio=periodo_anterior.DT_INICIO,
                data_fim=periodo_anterior.DT_FIM
            )
            rows = connection.execute(sql).fetchall()

        # 3) Preencher arrecadação em cada empresa
        for emp in empresas:
            emp.arrecadacao = 0.0
            for row in rows:
                if row[0] == emp.ID_EMPRESA:
                    emp.arrecadacao = float(row[1]) or 0.0
                    break

        # 4) Tabela 1: ajustar percentuais
        resultados_tabela1 = ajustar_percentuais_tabela1(empresas)

        # 5) Tabela 2: contratos por situação
        empresas_permanece = [e for e in empresas if e.DS_CONDICAO == 'PERMANECE']
        empresas_novas = [e for e in empresas if e.DS_CONDICAO == 'NOVA']
        total_empresas = len(empresas_permanece) + len(empresas_novas)
        qtde_contratos_permanece = (
            int((len(empresas_permanece) / total_empresas) * num_contratos)
            if total_empresas > 0 else 0
        )
        qtde_contratos_novas = num_contratos - qtde_contratos_permanece

        # 6) Tabela 3: distribuir contratos para quem permanece
        dados_perm = [e for e in resultados_tabela1 if e['situacao'] == 'PERMANECE']
        res3 = distribuir_contratos_tabela3(dados_perm, qtde_contratos_permanece)

        # Atualizar dados na tabela de resultados principal
        for resultado in resultados_tabela1:
            if resultado['situacao'] == 'PERMANECE':
                # Procurar o item correspondente em res3
                for item in res3:
                    if item['id_empresa'] == resultado['id_empresa']:
                        resultado['contratos'] = item['contratos']
                        resultado['ajuste_contratos'] = item['ajuste_contratos']
                        # Garantir que todos os campos necessários estejam presentes
                        break

        # 7) Tabela 4: distribuir contratos para empresas novas
        print(f"DEBUG: Procurando empresas NOVA...")
        dados_nov = []
        for empresa in empresas:
            if empresa.DS_CONDICAO == 'NOVA':
                # Criar entrada para empresa NOVA (não existe na Tabela 1)
                print(f"DEBUG: Encontrada empresa NOVA: {empresa.NO_EMPRESA}")
                dados_nov.append({
                    'idx': len(dados_nov) + 1,  # Índice sequencial
                    'id_empresa': empresa.ID_EMPRESA,
                    'empresa': empresa.NO_EMPRESA_ABREVIADA or empresa.NO_EMPRESA,
                    'situacao': 'NOVA',
                    'contratos': 0,  # Será preenchido pela distribuição
                    'ajuste_contratos': 0  # Será preenchido pela distribuição
                })

        print(f"DEBUG: Total de empresas NOVA encontradas: {len(dados_nov)}")

        if dados_nov and qtde_contratos_novas > 0:
            print(f"DEBUG: Distribuindo {qtde_contratos_novas} contratos para {len(dados_nov)} empresas NOVA")
            res4 = distribuir_restantes_tabela4(dados_nov, qtde_contratos_novas)

            # Adicionar empresas NOVA ao resultados_tabela1 para tabelas posteriores
            for empresa_nova in res4:
                # Verificar se já existe no resultado_tabela1
                if not any(e.get('id_empresa') == empresa_nova['id_empresa'] and e.get('situacao') == 'NOVA'
                           for e in resultados_tabela1 if e.get('idx') != 'TOTAL'):
                    # Adicionar antes da linha TOTAL
                    resultados_tabela1.insert(-1, empresa_nova)
        else:
            print(f"DEBUG: Não há empresas NOVA ou contratos para distribuir")

        # 8) Calcular percentuais finais para todas as empresas
        # Preparar dados para tabela 5
        empresas_ativas = [e for e in resultados_tabela1 if
                           e['situacao'] in ('PERMANECE', 'NOVA') and e['idx'] != 'TOTAL']

        # Garantir que todas as empresas ativas tenham o campo contratos definido
        for empresa in empresas_ativas:
            if 'contratos' not in empresa:
                print(f"DEBUG MISTAS: Empresa sem contratos: {empresa.get('empresa', '?')}")
                empresa['contratos'] = 0
            if 'ajuste_contratos' not in empresa:
                empresa['ajuste_contratos'] = 0

            # Calcular percentual base para a Tabela 5
            empresa['pct_distribuicao'] = truncate_decimal(empresa['contratos'] / num_contratos * 100)
            empresa['pct_base'] = empresa['pct_distribuicao']  # Adicionar campo pct_base
            empresa['ajuste'] = 0.0  # Inicializar campo de ajuste

        # Calcular total de percentual base
        total_pct_distribuicao = truncate_decimal(sum(e['pct_distribuicao'] for e in empresas_ativas))

        # Calcular diferença para 100%
        diferenca = truncate_decimal(100.00 - total_pct_distribuicao)

        # Aplicar ajustes para chegar a 100%
        if diferenca > 0:
            # Organizar empresas: primeiro PERMANECE (ordem de arrecadação), depois NOVAS
            permanece = sorted(
                [e for e in empresas_ativas if e['situacao'] == 'PERMANECE'],
                key=lambda x: x.get('arrecadacao', 0), reverse=True
            )
            novas = [e for e in empresas_ativas if e['situacao'] == 'NOVA']

            # Aplicar ajustes por ordem de prioridade
            empresas_ordenadas = permanece + novas

            # Calcular quantos ajustes de 0.01% são necessários
            ajustes_necessarios = int(diferenca * 100)

            # Inicializar contadores de ajustes recebidos
            ajustes_recebidos = [0] * len(empresas_ordenadas)

            # Distribuir ajustes
            for _ in range(ajustes_necessarios):
                # Encontrar empresas com menos ajustes
                min_ajustes = min(ajustes_recebidos)
                candidatos = [i for i, v in enumerate(ajustes_recebidos) if v == min_ajustes]

                # Selecionar a primeira empresa candidata
                idx = candidatos[0]

                # Aplicar ajuste
                empresas_ordenadas[idx]['ajuste'] += 0.01
                ajustes_recebidos[idx] += 1

        # Calcular percentual final (base + ajuste)
        for empresa in empresas_ativas:
            empresa['pct_final'] = truncate_decimal(empresa['pct_distribuicao'] + empresa.get('ajuste', 0.0))

        # Verificação final: garantir 100%
        total_final = truncate_decimal(sum(e['pct_final'] for e in empresas_ativas))
        if total_final != 100.00 and empresas_ativas:
            diferenca_final = truncate_decimal(100.00 - total_final)
            if abs(diferenca_final) <= 0.01:
                empresas_ativas[0]['ajuste'] += diferenca_final
                empresas_ativas[0]['pct_final'] += diferenca_final

        # 9) Retornar resultados para o template
        # Atualizar a tabela de resultados principal
        for resultado in resultados_tabela1:
            if resultado['idx'] != 'TOTAL':
                # Procurar o item correspondente em empresas_ativas
                for empresa in empresas_ativas:
                    if empresa['id_empresa'] == resultado.get('id_empresa'):
                        # Copiar todos os campos calculados
                        resultado['pct_distribuicao'] = empresa.get('pct_distribuicao', 0.0)
                        resultado['pct_base'] = empresa.get('pct_base', 0.0)
                        resultado['pct_final'] = empresa.get('pct_final', 0.0)
                        break

        # Atualizar a linha de TOTAL
        total_idx = next((i for i, e in enumerate(resultados_tabela1) if e['idx'] == 'TOTAL'), None)
        if total_idx is not None:
            resultados_tabela1[total_idx]['contratos'] = sum(
                e.get('contratos', 0) for e in resultados_tabela1 if e['idx'] != 'TOTAL')
            resultados_tabela1[total_idx]['ajuste_contratos'] = sum(
                e.get('ajuste_contratos', 0) for e in resultados_tabela1 if e['idx'] != 'TOTAL')
            resultados_tabela1[total_idx]['pct_distribuicao'] = sum(
                e.get('pct_distribuicao', 0) for e in resultados_tabela1 if e['idx'] != 'TOTAL')
            resultados_tabela1[total_idx]['pct_base'] = sum(
                e.get('pct_base', 0) for e in resultados_tabela1 if e['idx'] != 'TOTAL')
            resultados_tabela1[total_idx]['ajuste'] = sum(
                e.get('ajuste', 0) for e in resultados_tabela1 if e['idx'] != 'TOTAL')
            resultados_tabela1[total_idx]['pct_final'] = sum(
                e.get('pct_final', 0) for e in resultados_tabela1 if e['idx'] != 'TOTAL')

        # Retorna o resultado final
        return {
            'resultados': resultados_tabela1,
            'todas_empresas_anteriores': {
                e['id_empresa']: e for e in resultados_tabela1 if e['idx'] != 'TOTAL'
            },
            'empresas_que_saem': {
                e['id_empresa']: e for e in resultados_tabela1
                if e['situacao'] == 'DESCREDENCIADA' and e['idx'] != 'TOTAL'
            },
            'num_contratos': num_contratos,
            'tipo_calculo': 'mistas',
            'qtde_empresas_permanece': len(empresas_permanece),
            'qtde_empresas_novas': len(empresas_novas),
            'qtde_contratos_permanece': qtde_contratos_permanece,
            'qtde_contratos_novas': qtde_contratos_novas,
            'total_pct_que_saem': sum(
                e.get('pct_arrecadacao', 0)
                for e in resultados_tabela1
                if e['situacao'] == 'DESCREDENCIADA' and e['idx'] != 'TOTAL'
            ),
            'valor_redistribuicao': (
                    sum(
                        e.get('pct_redistribuido', 0)
                        for e in resultados_tabela1
                        if e['situacao'] == 'PERMANECE' and e['idx'] != 'TOTAL'
                    ) / max(1, len(empresas_permanece))
            )
        }

    except Exception as e:
        print(f"Erro no cálculo para empresas mistas: {e}")
        import traceback
        traceback.print_exc()
        return None




def ajustar_percentuais_finais(empresas):
    total_pct = truncate_decimal(sum(e['pct_distribuicao'] for e in empresas))
    diferenca = truncate_decimal(100.00 - total_pct)

    if diferenca <= 0:
        for e in empresas:
            e['pct_final'] = truncate_decimal(e['pct_distribuicao'])
            e['ajuste'] = truncate_decimal(0.00)
        return empresas

    # Organiza em ciclos: permanece (por arrecadação), depois novas
    permanece = sorted(
        [e for e in empresas if e['situacao'] == 'PERMANECE'],
        key=lambda x: x.get('arrecadacao', 0), reverse=True
    )
    novas = [e for e in empresas if e['situacao'] == 'NOVA']
    ciclo = permanece + novas

    # Distribui 0.01% por vez para cada empresa, começando pelas que mais arrecadam
    idx = 0
    while truncate_decimal(diferenca, 4) > 0 and ciclo:
        empresa = ciclo[idx % len(ciclo)]
        empresa['ajuste'] = truncate_decimal(empresa.get('ajuste', 0.00) + 0.01)
        diferenca = truncate_decimal(diferenca - 0.01, 4)
        idx += 1

    # Calcula os percentuais finais
    for e in empresas:
        e['ajuste'] = truncate_decimal(e.get('ajuste', 0.00))
        e['pct_final'] = truncate_decimal(e['pct_distribuicao'] + e['ajuste'])

    # Verifica se o total ainda é 100%
    total_final = truncate_decimal(sum(e['pct_final'] for e in empresas))
    if total_final != 100.00 and empresas:
        # Ajustar a última discrepância na primeira empresa
        diferenca_final = truncate_decimal(100.00 - total_final)
        empresas[0]['pct_final'] = truncate_decimal(empresas[0]['pct_final'] + diferenca_final)
        empresas[0]['ajuste'] = truncate_decimal(empresas[0]['ajuste'] + diferenca_final)

    return empresas


@limite_bp.route('/limites/analise')
@login_required
def analise_limites():
    try:
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

        # Obter período anterior para os cálculos
        periodo_anterior = None
        if not todas_novas:  # Se todas são novas, não precisamos do período anterior
            periodo_anterior = PeriodoAvaliacao.query.filter(
                PeriodoAvaliacao.ID_EDITAL == ultimo_edital.ID,
                PeriodoAvaliacao.ID_PERIODO < ultimo_periodo.ID_PERIODO,
                PeriodoAvaliacao.DELETED_AT == None
            ).order_by(PeriodoAvaliacao.ID_PERIODO.desc()).first()

            if not periodo_anterior and not todas_novas:
                flash('Não foi encontrado período anterior para realizar os cálculos.', 'warning')
                return redirect(url_for('limite.lista_limites'))

        # Realizar cálculos conforme a condição das empresas
        resultado_calculo = None
        num_contratos = selecionar_contratos()

        if todas_permanece:
            if periodo_anterior:
                resultado_calculo = calcular_limites_empresas_permanece(
                    ultimo_edital, ultimo_periodo, periodo_anterior)
            else:
                flash('Não foi encontrado período anterior para realizar os cálculos.', 'warning')
                return redirect(url_for('limite.lista_limites'))
        elif todas_novas:
            resultado_calculo = calcular_limites_empresas_novas(
                ultimo_edital, ultimo_periodo, empresas)
        elif alguma_permanece:
            # Empresas mistas (PERMANECE + NOVAS)
            if periodo_anterior:
                resultado_calculo = calcular_limites_empresas_mistas(
                    ultimo_edital, ultimo_periodo, periodo_anterior, empresas)
            else:
                flash('Não foi encontrado período anterior para realizar os cálculos.', 'warning')
                return redirect(url_for('limite.lista_limites'))

        if not resultado_calculo:
            # Se não for possível calcular, redirecionar com mensagem
            flash('Não foi possível realizar o cálculo dos limites. Verifique a configuração das empresas e períodos.',
                  'warning')
            return redirect(url_for('limite.lista_limites'))

        # Verificar o tipo de resultado e preparar dados para o template
        if isinstance(resultado_calculo, dict):
            resultados_calculo = resultado_calculo.get('resultados', [])
            tipo_calculo = resultado_calculo.get('tipo_calculo', '')
            metadados = resultado_calculo.get('metadados', {})
            num_contratos = resultado_calculo.get('num_contratos', 0)

            # Pega a variável diretamente se ela não vier no metadados
            todas_empresas_anteriores = resultado_calculo.get('todas_empresas_anteriores', {})

            dados_template = {
                'edital': ultimo_edital,
                'periodo': ultimo_periodo,
                'periodo_anterior': periodo_anterior,
                'empresas': empresas,
                'todas_permanece': todas_permanece,
                'todas_novas': todas_novas,
                'alguma_permanece': alguma_permanece,
                'resultados_calculo': resultados_calculo,
                'num_contratos': num_contratos,
                'tipo_calculo': tipo_calculo,
                'todas_empresas_anteriores': todas_empresas_anteriores  # ✅ adiciona aqui
            }

            # Adiciona outras variáveis extras (mistas)
            if tipo_calculo == 'mistas':
                for chave, valor in resultado_calculo.items():
                    if chave != 'resultados':
                        dados_template[chave] = valor

            return render_template('credenciamento/analise_limite.html', **dados_template)
        else:
            flash('Erro no formato dos resultados do cálculo.', 'danger')
            return redirect(url_for('limite.lista_limites'))

    except Exception as e:
        flash(f'Erro durante a análise: {str(e)}', 'danger')
        import traceback
        print(f"Erro detalhado na análise de limites: {traceback.format_exc()}")
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

        # Obter dados do formulário
        empresas_data = request.form.getlist('empresa_id[]')
        percentuais = request.form.getlist('percentual_final[]')
        situacoes = request.form.getlist('situacao[]')

        if len(empresas_data) != len(percentuais) or len(empresas_data) != len(situacoes):
            flash('Erro: Dados inconsistentes.', 'danger')
            return redirect(url_for('limite.analise_limites'))

        if not empresas_data:
            flash('Erro: Nenhuma empresa informada.', 'danger')
            return redirect(url_for('limite.analise_limites'))

        cod_criterio = 7  # critério fixo

        # Excluir limites anteriores para o mesmo edital/período/critério
        db.session.query(LimiteDistribuicao).filter(
            LimiteDistribuicao.ID_EDITAL == edital_id,
            LimiteDistribuicao.ID_PERIODO == periodo_id,
            LimiteDistribuicao.COD_CRITERIO_SELECAO == cod_criterio,
            LimiteDistribuicao.DELETED_AT == None
        ).delete(synchronize_session=False)
        db.session.commit()

        # Inserir os novos limites
        limites_criados = 0
        for i in range(len(empresas_data)):
            if situacoes[i].upper() == 'DESCREDENCIADO':
                continue
            percentual = truncate_decimal(float(percentuais[i]) if percentuais[i] else 0.0)

            novo_limite = LimiteDistribuicao(
                ID_EDITAL=edital_id,
                ID_PERIODO=periodo_id,
                ID_EMPRESA=int(empresas_data[i]),
                COD_CRITERIO_SELECAO=cod_criterio,
                DT_APURACAO=dt_apuracao,
                QTDE_MAXIMA=None,
                VALOR_MAXIMO=None,
                PERCENTUAL_FINAL=percentual
            )
            db.session.add(novo_limite)
            limites_criados += 1

        db.session.commit()

        # Registrar log de auditoria
        registrar_log(
            acao='criar',
            entidade='limite',
            entidade_id=0,  # Não temos um ID único para o conjunto
            descricao=f'Criação de {limites_criados} limites de distribuição a partir da análise',
            dados_novos={'total_limites': limites_criados}
        )

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
                valor_maximo = truncate_decimal(float(valor_maximo))

            percentual_final = request.form.get('percentual_final')
            if percentual_final:
                percentual_final = truncate_decimal(float(percentual_final))

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
                                       empresas=empresas, criterios=criterios)

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
                limite.VALOR_MAXIMO = truncate_decimal(float(valor_maximo))
            else:
                limite.VALOR_MAXIMO = None

            percentual_final = request.form.get('percentual_final')
            if percentual_final:
                limite.PERCENTUAL_FINAL = truncate_decimal(float(percentual_final))
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