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
import random
import logging

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


def ajustar_percentuais_tabela1(empresas, include_total=False):
    """
    Ajusta os percentuais na tabela de participação na arrecadação (Tabela 1)

    Considera empresas com DS_CONDICAO 'PERMANECE' e 'DESCREDENCIADA';
    Distribui os ajustes de 0,01% por empresa em ordem decrescente de arrecadação.
    Uma empresa só recebe o segundo incremento após todas receberem o primeiro.

    Args:
        empresas: Lista de objetos EmpresaParticipante
        include_total: Se True, inclui linha de TOTAL nos resultados
    """
    # Converter objetos para dicionários, filtrando NOVAS
    dados_empresas = []
    idx = 1
    for empresa in empresas:
        if empresa.DS_CONDICAO == 'NOVA':
            continue
        sit = 'PERMANECE' if empresa.DS_CONDICAO == 'PERMANECE' else 'DESCREDENCIADA'
        dados_empresas.append({
            'idx': idx,
            'id_empresa': empresa.ID_EMPRESA,
            'empresa': empresa.NO_EMPRESA_ABREVIADA or empresa.NO_EMPRESA,
            'situacao': sit,
            'arrecadacao': getattr(empresa, 'arrecadacao', 0.0),
        })
        idx += 1

    # Calcular total de arrecadação
    total_arrecadacao = sum(e['arrecadacao'] for e in dados_empresas)

    # Calcular percentual de arrecadação para cada empresa
    for e in dados_empresas:
        e['pct_arrecadacao'] = (e['arrecadacao'] / total_arrecadacao * 100) if total_arrecadacao > 0 else 0.0

    # Separar PERMANECEM e DESCREDENCIADAS
    empresas_permanece = [e for e in dados_empresas if e['situacao'] == 'PERMANECE']
    empresas_descred = [e for e in dados_empresas if e['situacao'] == 'DESCREDENCIADA']

    # Redistribuir percentual das DESCREDENCIADAS entre as PERMANECEM
    pct_total_saem = sum(e['pct_arrecadacao'] for e in empresas_descred)
    pct_redistrib = (pct_total_saem / len(empresas_permanece)) if empresas_permanece else 0.0

    # Inicializar campos para todas as empresas
    for e in dados_empresas:
        if e['situacao'] == 'PERMANECE':
            e['pct_redistribuido'] = pct_redistrib
            e['pct_novo'] = e['pct_arrecadacao'] + pct_redistrib
            # Trunca valor em duas casas decimais (sem arredondamento)
            e['pct_novo_truncado'] = int(e['pct_novo'] * 100) / 100
            e['ajuste'] = 0.0
            e['pct_novofinal'] = 0.0
        else:
            e['pct_redistribuido'] = 0.0
            e['pct_novo'] = 0.0
            e['pct_novo_truncado'] = 0.0
            e['ajuste'] = 0.0
            e['pct_novofinal'] = 0.0

    # Soma truncada e diferença para 100%
    soma_trunc = sum(e['pct_novo_truncado'] for e in empresas_permanece)
    diferenca = 100.0 - soma_trunc

    # IMPORTANTE: Nova implementação da distribuição de ajustes
    # Ordenar empresas por arrecadação decrescente
    empresas_ordenadas = sorted(empresas_permanece, key=lambda x: x['arrecadacao'], reverse=True)

    # Inicializar contadores de ajustes recebidos
    ajustes_recebidos = {e['id_empresa']: 0 for e in empresas_ordenadas}

    # Calcular quantos incrementos de 0.01% são necessários
    incrementos = int(diferenca * 100)  # Cada 0.01% = 1 incremento

    # Distribuir incrementos um a um
    for _ in range(incrementos):
        # Encontrar a empresa que tem menos ajustes
        min_ajustes = min(ajustes_recebidos.values())

        # Empresas candidatas (com o menor número de ajustes)
        candidatas = [e for e in empresas_ordenadas if ajustes_recebidos[e['id_empresa']] == min_ajustes]

        # Em caso de empate, escolher a empresa com maior arrecadação
        # (já está ordenado, então é a primeira da lista)
        empresa_escolhida = candidatas[0]

        # Incrementar o contador de ajustes
        ajustes_recebidos[empresa_escolhida['id_empresa']] += 1

        # Adicionar o ajuste à empresa
        empresa_escolhida['ajuste'] += 0.01

    # Calcular pct_novofinal para cada empresa
    for e in empresas_permanece:
        e['pct_novofinal'] = e['pct_novo_truncado'] + e['ajuste']

    # Verificar o total final (deve ser exatamente 100%)
    soma_final = sum(e['pct_novofinal'] for e in empresas_permanece)

    # Ajuste fino se necessário (embora não deveria ser com a implementação acima)
    if abs(soma_final - 100.0) > 0.0001 and empresas_permanece:
        diferenca_final = 100.0 - soma_final
        # Encontrar a empresa com menos ajustes para fazer o ajuste final
        min_ajustes = min(ajustes_recebidos.values())
        candidatas = [e for e in empresas_ordenadas if ajustes_recebidos[e['id_empresa']] == min_ajustes]
        candidatas[0]['ajuste'] += diferenca_final
        candidatas[0]['pct_novofinal'] = candidatas[0]['pct_novo_truncado'] + candidatas[0]['ajuste']

    # Adicionar linha de TOTAL se solicitado
    if include_total:
        total_row = {
            'idx': 'TOTAL',
            'empresa': 'TOTAL',
            'situacao': '',
            'arrecadacao': sum(e['arrecadacao'] for e in dados_empresas),
            'pct_arrecadacao': sum(e['pct_arrecadacao'] for e in dados_empresas),
            'pct_redistribuido': sum(e['pct_redistribuido'] for e in empresas_permanece),
            'pct_novo': sum(e['pct_novo'] for e in empresas_permanece),
            'pct_novo_truncado': sum(e['pct_novo_truncado'] for e in empresas_permanece),
            'ajuste': sum(e['ajuste'] for e in empresas_permanece),
            'pct_novofinal': sum(e.get('pct_novofinal', 0) for e in empresas_permanece)
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
    Redistribui total_contratos_novas igualitariamente entre empresas_novas,
    truncando para inteiro e aplicando ajustes 1 a 1 em ciclos aleatórios até esgotar a sobra.
    """
    import random

    n = len(empresas_novas)
    if n == 0:
        return []  # sem empresas novas

    # 1) Base truncada
    base = total_contratos_novas // n
    for emp in empresas_novas:
        emp['contratos'] = base
        emp['ajuste_contratos'] = 0

    # 2) Calcular sobra
    restante = total_contratos_novas - base * n

    # 3) Ciclo aleatório para distribuir a sobra
    if restante > 0:
        indices = list(range(n))
        random.shuffle(indices)
        for i in range(restante):
            idx = indices[i % n]
            empresas_novas[idx]['contratos'] += 1
            empresas_novas[idx]['ajuste_contratos'] += 1

    # 4) Verificação final
    total_dist = sum(e['contratos'] for e in empresas_novas)
    assert total_dist == total_contratos_novas, (
        f"Soma incorreta: {total_dist} != {total_contratos_novas}"
    )

    return empresas_novas


def ajustar_percentuais_tabela5(empresas_todas, total_contratos):
    # 1) pct_base e init ajuste_pct
    for e in empresas_todas:
        if total_contratos > 0:
            e['pct_base'] = int((e['contratos'] / total_contratos * 100) * 100) / 100
        else:
            e['pct_base'] = 0.0
        e['ajuste_pct'] = 0.0

    # 2) calcula faltante
    soma_base = sum(e['pct_base'] for e in empresas_todas)
    faltante = int(round((100.00 - soma_base) * 100))

    # 3) ciclo distribuindo
    perm = sorted(
        [e for e in empresas_todas if e['situacao'] == 'PERMANECE'],
        key=lambda x: x.get('arrecadacao', 0),
        reverse=True
    )
    nov = [e for e in empresas_todas if e['situacao'] == 'NOVA']
    ciclo = perm + nov
    ciclo_len = len(ciclo)

    if faltante > 0 and ciclo_len > 0:
        for i in range(faltante):
            pos = i % ciclo_len
            if pos < len(perm):
                alvo = perm[pos]
            else:
                # se não houver NOVA, volta a distribuir entre as PERMANECEM
                alvo = random.choice(nov) if nov else perm[pos % len(perm)]
            alvo['ajuste_pct'] += 0.01

    # 4) recalcula pct_final
    for e in empresas_todas:
        e['pct_final'] = e['pct_base'] + e['ajuste_pct']

    # 5) ajuste fino
    total_final = sum(e['pct_final'] for e in empresas_todas)
    delta = round((100.00 - total_final) * 100) / 100
    if abs(delta) >= 0.01 and empresas_todas:
        # prefere primeira PERMANECEM, senão a primeira da lista completa
        alvo = perm[0] if perm else empresas_todas[0]
        alvo['ajuste_pct'] += delta
        alvo['pct_final'] = alvo['pct_base'] + alvo['ajuste_pct']

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

        # 4) Tabela 1: ajustar percentuais (SEM incluir linha TOTAL)
        resultados_tabela1 = ajustar_percentuais_tabela1(empresas, include_total=False)

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

        # Transferir resultados de Tabela 3 para os dados consolidados
        mapa3 = {e['id_empresa']: e for e in res3}
        for e in resultados_tabela1:
            if e['situacao'] == 'PERMANECE':
                m = mapa3.get(e['id_empresa'], {})
                e['contratos'] = m.get('contratos', 0)
                e['ajuste_contratos'] = m.get('ajuste_contratos', 0)
                e['total'] = m.get('total', e['contratos'])

        # 7) Tabela 4: Distribuição para empresas NOVAS
        # Montar lista de dicionários apenas com as NOVAS
        dados_nov_raw = []
        for emp in empresas:
            if emp.DS_CONDICAO == 'NOVA':
                dados_nov_raw.append({
                    'idx': None,
                    'id_empresa': emp.ID_EMPRESA,
                    'empresa': emp.NO_EMPRESA_ABREVIADA or emp.NO_EMPRESA,
                    'situacao': 'NOVA',
                    'contratos': 0,
                    'ajuste_contratos': 0,
                })

        resultados_novas = distribuir_restantes_tabela4(dados_nov_raw, qtde_contratos_novas)

        # 8) Combinar resultados para Tabela 5
        # Limpar entradas NOVA existentes e mesclar as novas
        resultados_sem_novas = [e for e in resultados_tabela1 if e.get('situacao') != 'NOVA']

        # Garantir que apenas empresas relevantes sejam passadas para ajuste de percentuais
        empresas_para_tabela5 = []

        # Adicionar empresas que permanecem
        for e in res3:
            # Copiar apenas os dados necessários para a Tabela 5
            empresas_para_tabela5.append({
                'idx': e.get('idx'),
                'id_empresa': e.get('id_empresa'),
                'empresa': e.get('empresa'),
                'situacao': 'PERMANECE',
                'contratos': e.get('contratos', 0),
                'arrecadacao': e.get('arrecadacao', 0),
                'pct_distribuicao': 0.0,  # Será calculado na Tabela 5
            })

        # Adicionar empresas novas
        for e in resultados_novas:
            empresas_para_tabela5.append({
                'idx': e.get('idx'),
                'id_empresa': e.get('id_empresa'),
                'empresa': e.get('empresa'),
                'situacao': 'NOVA',
                'contratos': e.get('contratos', 0),
                'arrecadacao': 0.0,  # Empresas novas não têm arrecadação
                'pct_distribuicao': 0.0,  # Será calculado na Tabela 5
            })

        # 9) Calcular percentuais finais (Tabela 5)
        resultados_tabela5 = ajustar_percentuais_tabela5(empresas_para_tabela5, num_contratos)

        # 10) Integrar resultados da Tabela 5 de volta ao resultado consolidado
        mapa5 = {e['id_empresa']: e for e in resultados_tabela5 if e.get('idx') != 'TOTAL'}

        # Atualizar dados nas empresas que permanecem
        for e in resultados_sem_novas:
            if e.get('idx') != 'TOTAL' and e['situacao'] == 'PERMANECE':
                m = mapa5.get(e['id_empresa'], {})
                e['pct_distribuicao'] = m.get('pct_base', 0.0)
                e['ajuste_pct'] = m.get('ajuste_pct', 0.0)  # Usar ajuste da Tabela 5
                e['pct_final'] = m.get('pct_final', 0.0)

        # Adicionar empresas novas com percentuais calculados
        for e in resultados_novas:
            m = mapa5.get(e['id_empresa'], {})
            e['pct_distribuicao'] = m.get('pct_base', 0.0)
            e['ajuste_pct'] = m.get('ajuste_pct', 0.0)  # Usar ajuste da Tabela 5
            e['pct_final'] = m.get('pct_final', 0.0)
            resultados_sem_novas.append(e)

        # Agora adicionar a linha TOTAL única para todas as tabelas
        total_arrecadacao = sum(e.get('arrecadacao', 0) for e in resultados_sem_novas if e.get('idx') != 'TOTAL')
        total_pct_arrecadacao = sum(e.get('pct_arrecadacao', 0) for e in resultados_sem_novas if e.get('idx') != 'TOTAL')
        total_pct_redistribuido = sum(e.get('pct_redistribuido', 0) for e in resultados_sem_novas if e.get('idx') != 'TOTAL')
        total_pct_novo = sum(e.get('pct_novo_truncado', 0) for e in resultados_sem_novas if e.get('idx') != 'TOTAL')
        total_ajuste = sum(e.get('ajuste', 0) for e in resultados_sem_novas if e.get('idx') != 'TOTAL')
        total_pct_final = sum(e.get('pct_final', 0) for e in resultados_sem_novas if e.get('idx') != 'TOTAL')
        total_contratos = sum(e.get('contratos', 0) for e in resultados_sem_novas if e.get('idx') != 'TOTAL')
        total_pct_novofinal = sum(e.get('pct_novofinal', 0) for e in resultados_sem_novas if e.get('idx') != 'TOTAL' and e.get('situacao') == 'PERMANECE')

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
            'pct_novofinal': total_pct_novofinal,
            'pct_final': total_pct_final,
            'contratos': total_contratos,
            'ajuste_contratos': sum(e.get('ajuste_contratos', 0) for e in resultados_sem_novas if e.get('idx') != 'TOTAL'),
            'total_contratos': total_contratos,
            'pct_base': sum(e.get('pct_distribuicao', 0) for e in resultados_sem_novas if e.get('idx') != 'TOTAL'),
            'pct_distribuicao': sum(e.get('pct_distribuicao', 0) for e in resultados_sem_novas if e.get('idx') != 'TOTAL'),
        }

        resultados_sem_novas.append(linha_total)

        # 11) Montar retorno
        return {
            'resultados': resultados_sem_novas,
            'todas_empresas_anteriores': {
                e['id_empresa']: e for e in resultados_tabela1 if e.get('idx') != 'TOTAL'
            },
            'empresas_que_saem': {
                e['id_empresa']: e for e in resultados_tabela1
                if e['situacao'] == 'DESCREDENCIADA' and e.get('idx') != 'TOTAL'
            },
            'num_contratos': num_contratos,
            'tipo_calculo': 'mistas',
            'qtde_empresas_permanece': len(empresas_permanece),
            'qtde_empresas_novas': len(empresas_novas),
            'qtde_contratos_permanece': qtde_contratos_permanece,
            'qtde_contratos_novas': qtde_contratos_novas,
            'total_pct_que_saem': sum(
                e['pct_arrecadacao'] for e in resultados_tabela1
                if e['situacao'] == 'DESCREDENCIADA' and e.get('idx') != 'TOTAL'
            ),
            'valor_redistribuicao': (
                sum(
                    e['pct_redistribuido'] for e in resultados_tabela1
                    if e['situacao'] == 'PERMANECE' and e.get('idx') != 'TOTAL'
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


# Modificação no arquivo app/routes/limite_routes.py
# Na função salvar_limites()

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

        # Coletar todos os valores percentuais para validação
        percentuais_validados = []
        for i in range(len(empresas_data)):
            if situacoes[i].upper() == 'DESCREDENCIADA':
                continue

            # Tratar o valor percentual diretamente como float (evitar conversões intermediárias)
            try:
                percentual_valor = float(percentuais[i]) if percentuais[i] else 0.0
                # Arredondar para 2 casas decimais para manter consistência
                percentual_valor = round(percentual_valor, 2)
                percentuais_validados.append((i, percentual_valor))
            except ValueError:
                percentuais_validados.append((i, 0.0))

        # Verificar se a soma é exatamente 100%
        soma_percentuais = sum(pct for _, pct in percentuais_validados)
        if abs(soma_percentuais - 100.0) > 0.01:
            # Se a soma não for 100%, ajustar o maior valor para compensar
            if percentuais_validados:
                maior_idx, _ = max(percentuais_validados, key=lambda x: x[1])
                ajuste = 100.0 - soma_percentuais
                percentuais_validados = [(i, pct + ajuste if i == maior_idx else pct) for i, pct in
                                         percentuais_validados]

        # Inserir os limites com valores validados
        for idx, percentual_valor in percentuais_validados:
            i = idx  # Índice original

            # Criar novo limite apenas para empresas que não são DESCREDENCIADAS
            if situacoes[i].upper() != 'DESCREDENCIADA':
                novo_limite = LimiteDistribuicao(
                    ID_EDITAL=edital_id,
                    ID_PERIODO=periodo_id,
                    ID_EMPRESA=int(empresas_data[i]),
                    COD_CRITERIO_SELECAO=cod_criterio,
                    DT_APURACAO=dt_apuracao,
                    QTDE_MAXIMA=None,
                    VALOR_MAXIMO=None,
                    PERCENTUAL_FINAL=percentual_valor  # Valor já tratado
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


@limite_bp.route('/limites/distribuir-contratos', methods=['GET', 'POST'])
@login_required
def distribuir_contratos():
    """
    Página para distribuição de contratos conforme limites cadastrados.
    Implementa o processo descrito no documento "Distribuição para cobrança.docx".
    """
    try:
        # Encontrar automaticamente o edital mais recente
        ultimo_edital = Edital.query.filter(Edital.DELETED_AT == None).order_by(Edital.ID.desc()).first()

        if not ultimo_edital:
            flash('Não foram encontrados editais cadastrados.', 'warning')
            return redirect(url_for('edital.lista_editais'))

        # Encontrar automaticamente o período mais recente do último edital
        ultimo_periodo = PeriodoAvaliacao.query.filter(
            PeriodoAvaliacao.ID_EDITAL == ultimo_edital.ID,
            PeriodoAvaliacao.DELETED_AT == None
        ).order_by(PeriodoAvaliacao.ID_PERIODO.desc()).first()

        if not ultimo_periodo:
            flash('Não foram encontrados períodos para o edital mais recente.', 'warning')
            return redirect(url_for('periodo.lista_periodos'))

        # Logging detalhado para debug
        logging.info(
            f"Distribuição de contratos - Edital selecionado: ID={ultimo_edital.ID}, NU_EDITAL={ultimo_edital.NU_EDITAL}")
        logging.info(
            f"Distribuição de contratos - Período selecionado: ID={ultimo_periodo.ID}, ID_PERIODO={ultimo_periodo.ID_PERIODO}")

        # Status da execução
        resultados = None

        # Se for POST, processar a distribuição
        if request.method == 'POST':
            try:
                # Usar sempre o último edital e período identificados
                edital_id = ultimo_edital.ID
                periodo_id = ultimo_periodo.ID_PERIODO  # IMPORTANTE: Usa ID_PERIODO em vez de ID

                logging.info(f"Iniciando distribuição para: edital_id={edital_id}, periodo_id={periodo_id}")

                # Importar a função para selecionar contratos distribuíveis
                from app.utils.distribuir_contratos import selecionar_contratos_distribuiveis, \
                    processar_distribuicao_completa

                # Opção para execução completa ou apenas seleção de contratos
                modo_execucao = request.form.get('modo_execucao', 'selecao')
                logging.info(f"Modo de execução selecionado: {modo_execucao}")

                if modo_execucao == 'completo':
                    # Executar o processo completo de distribuição
                    logging.info("Iniciando processo completo de distribuição...")
                    resultados = processar_distribuicao_completa(edital_id, periodo_id)
                    logging.info(f"Resultados obtidos: {resultados}")

                    if resultados['contratos_distribuiveis'] > 0:
                        flash(
                            f'Processo de distribuição concluído. {resultados["total_distribuido"]} contratos distribuídos de um total de {resultados["contratos_distribuiveis"]} contratos distribuíveis.',
                            'success')
                    else:
                        flash('Nenhum contrato disponível para distribuição.', 'warning')
                else:
                    # Executar apenas a seleção de contratos distribuíveis
                    logging.info("Iniciando seleção de contratos distribuíveis...")
                    num_contratos = selecionar_contratos_distribuiveis()
                    logging.info(f"Contratos selecionados: {num_contratos}")

                    if num_contratos > 0:
                        flash(
                            f'Seleção de contratos concluída. {num_contratos} contratos disponíveis para distribuição.',
                            'success')
                        resultados = {'contratos_distribuiveis': num_contratos}
                    else:
                        flash('Nenhum contrato disponível para distribuição.', 'warning')

            except Exception as e:
                logging.error(f"Erro ao processar a distribuição: {str(e)}")
                import traceback
                logging.error(traceback.format_exc())
                flash(f'Erro ao processar a distribuição: {str(e)}', 'danger')

        # Renderizar o formulário inicial
        return render_template(
            'credenciamento/distribuir_contratos.html',
            ultimo_edital=ultimo_edital,
            ultimo_periodo=ultimo_periodo,
            resultados=resultados
        )
    except Exception as e:
        logging.error(f"Erro na página de distribuição de contratos: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        flash(f'Erro na página de distribuição: {str(e)}', 'danger')
        return redirect(url_for('limite.lista_limites'))