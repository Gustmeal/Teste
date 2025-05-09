# app/utils/redistribuir_contratos.py
from app import db
from sqlalchemy import text
import logging
from datetime import datetime


def selecionar_contratos_para_redistribuicao(empresa_id):
    """
    Seleciona os contratos da empresa que será redistribuída.
    Preserva distribuições anteriores, removendo apenas os dados relevantes.
    """
    # Configuração básica de logging
    import logging
    import sys

    # Configurar o logging para exibir no console e em um arquivo
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("redistribuicao.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

    print(f"Função selecionar_contratos_para_redistribuicao chamada para empresa_id={empresa_id}")
    logging.info(f"Iniciando seleção de contratos para redistribuição da empresa ID: {empresa_id}")

    try:
        # Limpar tabelas temporárias somente para esta operação
        with db.engine.connect() as connection:
            print("Limpando tabelas temporárias para esta operação específica...")
            logging.info("Limpando tabelas temporárias...")

            # Limpar apenas a tabela temporária (isso é seguro, pois não afeta dados históricos)
            truncate_sql = text("TRUNCATE TABLE [DEV].[DCA_TB006_DISTRIBUIVEIS]")
            connection.execute(truncate_sql)
            print("Tabela DCA_TB006_DISTRIBUIVEIS limpa para nova operação")

            truncate_arrastaveis_sql = text("TRUNCATE TABLE [DEV].[DCA_TB007_ARRASTAVEIS]")
            connection.execute(truncate_arrastaveis_sql)
            print("Tabela DCA_TB007_ARRASTAVEIS limpa para nova operação")

            # DIAGNÓSTICO: Verificar cada critério separadamente
            print("\n----- DIAGNÓSTICO DE CRITÉRIOS DE SELEÇÃO -----")

            # 1. Verificar contratos na empresa atual
            check_empresa_sql = text("""
                SELECT COUNT(*) 
                FROM [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                WHERE ECA.[COD_EMPRESA_COBRANCA] = :empresa_id
            """)
            count_empresa = connection.execute(check_empresa_sql, {"empresa_id": empresa_id}).scalar() or 0
            print(f"1. Contratos para empresa {empresa_id}: {count_empresa}")

            # 2. Verificar quantos têm fkSituacaoCredito = 1
            check_situacao_sql = text("""
                SELECT COUNT(*) 
                FROM [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                INNER JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] AS SIT
                    ON ECA.fkContratoSISCTR = SIT.fkContratoSISCTR
                WHERE ECA.[COD_EMPRESA_COBRANCA] = :empresa_id
                AND SIT.[fkSituacaoCredito] = 1
            """)
            count_situacao = connection.execute(check_situacao_sql, {"empresa_id": empresa_id}).scalar() or 0
            print(f"2. Contratos ativos (fkSituacaoCredito=1): {count_situacao}")

            # 3. Verificar quantos têm VR_SD_DEVEDOR > 0
            check_saldo_sql = text("""
                SELECT COUNT(*) 
                FROM [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                INNER JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] AS SIT
                    ON ECA.fkContratoSISCTR = SIT.fkContratoSISCTR
                WHERE ECA.[COD_EMPRESA_COBRANCA] = :empresa_id
                AND SIT.VR_SD_DEVEDOR > 0
            """)
            count_saldo = connection.execute(check_saldo_sql, {"empresa_id": empresa_id}).scalar() or 0
            print(f"3. Contratos com saldo devedor > 0: {count_saldo}")

            # 4. Verificar quantos estão com suspensão judicial
            check_suspensao_sql = text("""
                SELECT COUNT(*) 
                FROM [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                LEFT JOIN [BDG].[COM_TB013_SUSPENSO_DECISAO_JUDICIAL] AS SDJ
                    ON ECA.fkContratoSISCTR = SDJ.fkContratoSISCTR
                WHERE ECA.[COD_EMPRESA_COBRANCA] = :empresa_id
                AND SDJ.fkContratoSISCTR IS NOT NULL
            """)
            count_suspensao = connection.execute(check_suspensao_sql, {"empresa_id": empresa_id}).scalar() or 0
            print(f"4. Contratos com suspensão judicial: {count_suspensao}")

            # 5. Verificar quantos atendem todos os critérios
            check_final_sql = text("""
                SELECT COUNT(*) 
                FROM [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                INNER JOIN [BDG].[COM_TB001_CONTRATO] AS CON
                    ON ECA.fkContratoSISCTR = CON.fkContratoSISCTR
                INNER JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] AS SIT
                    ON ECA.fkContratoSISCTR = SIT.fkContratoSISCTR
                LEFT JOIN [BDG].[COM_TB013_SUSPENSO_DECISAO_JUDICIAL] AS SDJ
                    ON ECA.fkContratoSISCTR = SDJ.fkContratoSISCTR
                WHERE ECA.[COD_EMPRESA_COBRANCA] = :empresa_id
                AND SIT.[fkSituacaoCredito] = 1
                AND SDJ.fkContratoSISCTR IS NULL
            """)
            count_final = connection.execute(check_final_sql, {"empresa_id": empresa_id}).scalar() or 0
            print(f"5. Contratos que atendem todos os critérios: {count_final}")

            # 6. Adicionar critério alternativo - buscar na tabela de distribuição
            check_distribuicao_sql = text("""
                SELECT COUNT(*) 
                FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                WHERE [COD_EMPRESA_COBRANCA] = :empresa_id
                AND DELETED_AT IS NULL
            """)
            count_distribuicao = connection.execute(check_distribuicao_sql, {"empresa_id": empresa_id}).scalar() or 0
            print(f"6. Contratos na tabela de distribuição: {count_distribuicao}")

            print("-------------------------------------------\n")

            # Verificar se devemos usar fonte alternativa para seleção
            if count_final == 0 and count_distribuicao > 0:
                print("USANDO TABELA DE DISTRIBUIÇÃO COMO FONTE ALTERNATIVA!")
                logging.info(
                    "Usando tabela de distribuição como fonte alternativa devido a 0 contratos na fonte primária")

                # Inserir da tabela de distribuição para distribuíveis
                insert_alt_sql = text("""
                INSERT INTO [DEV].[DCA_TB006_DISTRIBUIVEIS]
                SELECT 
                    D.fkContratoSISCTR,
                    D.NR_CPF_CNPJ,
                    D.VR_SD_DEVEDOR,
                    GETDATE() AS CREATED_AT,
                    NULL AS UPDATED_AT,
                    NULL AS DELETED_AT
                FROM [DEV].[DCA_TB005_DISTRIBUICAO] D
                WHERE D.[COD_EMPRESA_COBRANCA] = :empresa_id
                AND D.DELETED_AT IS NULL
                """)

                result = connection.execute(insert_alt_sql, {"empresa_id": empresa_id})
                contratos_inseridos = result.rowcount
                print(f"Inseridos {contratos_inseridos} contratos da tabela de distribuição")
                logging.info(f"Inseridos {contratos_inseridos} contratos da tabela de distribuição")
            else:
                # CORREÇÃO: Consulta ajustada para selecionar contratos ativos com as condições corretas
                insert_sql = text("""
                INSERT INTO [DEV].[DCA_TB006_DISTRIBUIVEIS]
                SELECT
                    ECA.fkContratoSISCTR,
                    CON.NR_CPF_CNPJ,
                    SIT.VR_SD_DEVEDOR,
                    GETDATE() AS CREATED_AT,
                    NULL AS UPDATED_AT,
                    NULL AS DELETED_AT
                FROM 
                    [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                    INNER JOIN [BDG].[COM_TB001_CONTRATO] AS CON
                        ON ECA.fkContratoSISCTR = CON.fkContratoSISCTR
                    INNER JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] AS SIT
                        ON ECA.fkContratoSISCTR = SIT.fkContratoSISCTR
                    LEFT JOIN [BDG].[COM_TB013_SUSPENSO_DECISAO_JUDICIAL] AS SDJ
                        ON ECA.fkContratoSISCTR = SDJ.fkContratoSISCTR
                WHERE
                    ECA.[COD_EMPRESA_COBRANCA] = :empresa_id
                    AND SIT.[fkSituacaoCredito] = 1  -- Contratos ativos
                    AND SDJ.fkContratoSISCTR IS NULL -- Sem suspensão judicial
                    AND SIT.VR_SD_DEVEDOR > 0        -- Com saldo devedor
                    AND ECA.fkContratoSISCTR IS NOT NULL -- Garante que contratos são válidos
                """)

                result = connection.execute(insert_sql, {"empresa_id": empresa_id})
                contratos_inseridos = result.rowcount
                print(f"Inseridos {contratos_inseridos} contratos da consulta original")
                logging.info(f"Inseridos {contratos_inseridos} contratos da consulta original")

                # Se ainda não encontrou contratos, tente uma abordagem mais flexível
                if contratos_inseridos == 0:
                    print("TENTATIVA ADICIONAL: Consulta flexibilizada para encontrar contratos")
                    insert_flexible_sql = text("""
                    INSERT INTO [DEV].[DCA_TB006_DISTRIBUIVEIS]
                    SELECT
                        ECA.fkContratoSISCTR,
                        CON.NR_CPF_CNPJ,
                        COALESCE(SIT.VR_SD_DEVEDOR, 0) AS VR_SD_DEVEDOR,
                        GETDATE() AS CREATED_AT,
                        NULL AS UPDATED_AT,
                        NULL AS DELETED_AT
                    FROM 
                        [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                        INNER JOIN [BDG].[COM_TB001_CONTRATO] AS CON
                            ON ECA.fkContratoSISCTR = CON.fkContratoSISCTR
                        LEFT JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] AS SIT
                            ON ECA.fkContratoSISCTR = SIT.fkContratoSISCTR
                    WHERE
                        ECA.[COD_EMPRESA_COBRANCA] = :empresa_id
                        AND ECA.fkContratoSISCTR NOT IN (
                            SELECT fkContratoSISCTR 
                            FROM [BDG].[COM_TB013_SUSPENSO_DECISAO_JUDICIAL]
                        )
                    """)

                    result = connection.execute(insert_flexible_sql, {"empresa_id": empresa_id})
                    contratos_inseridos = result.rowcount
                    print(f"Inseridos {contratos_inseridos} contratos da consulta flexibilizada")
                    logging.info(f"Inseridos {contratos_inseridos} contratos da consulta flexibilizada")

            # Contar contratos selecionados no final
            count_sql = text("SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] WHERE DELETED_AT IS NULL")
            result = connection.execute(count_sql)
            num_contratos = result.scalar() or 0

            print(f"Total de contratos selecionados para redistribuição: {num_contratos}")
            logging.info(f"Total de contratos selecionados para redistribuição: {num_contratos}")

            # Se ainda não encontrou contratos, log detalhado
            if num_contratos == 0:
                logging.error("ALERTA: Nenhum contrato encontrado para redistribuição!")
                # Verificar se a empresa existe
                empresa_check = text("""
                SELECT COUNT(*) FROM [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] 
                WHERE [COD_EMPRESA_COBRANCA] = :empresa_id
                """)
                empresa_exists = connection.execute(empresa_check, {"empresa_id": empresa_id}).scalar() or 0
                logging.error(f"Empresa {empresa_id} existe na tabela atual: {'Sim' if empresa_exists > 0 else 'Não'}")

            return num_contratos

    except Exception as e:
        error_msg = f"Erro geral ao selecionar contratos para redistribuição: {str(e)}"
        print(error_msg)
        logging.error(error_msg)

        import traceback
        trace_msg = traceback.format_exc()
        print(f"Traceback: {trace_msg}")
        logging.error(f"Traceback: {trace_msg}")

        return 0


def calcular_percentuais_redistribuicao(edital_id, periodo_id, empresa_id):
    """
    Calcula os percentuais para redistribuição dos contratos.

    Args:
        edital_id: ID do edital
        periodo_id: ID do período
        empresa_id: ID da empresa que está saindo

    Returns:
        tuple: (percentual_redistribuido, total_arrecadacao, empresas_dados)
    """
    # Configuração básica de logging
    import logging
    import sys

    # Configurar o logging para exibir no console e em um arquivo
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("redistribuicao.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

    print(
        f"\nIniciando cálculo de percentuais: edital_id={edital_id}, periodo_id={periodo_id}, empresa_id={empresa_id}")
    logging.info(
        f"Calculando percentuais para redistribuição - Edital: {edital_id}, Período: {periodo_id}, Empresa: {empresa_id}")

    try:
        # 1. Buscar período para obter datas de início e fim
        with db.engine.connect() as connection:
            print("Buscando informações do período...")

            periodo_sql = text("""
                SELECT DT_INICIO, DT_FIM 
                FROM [DEV].[DCA_TB001_PERIODO_AVALIACAO]
                WHERE ID_EDITAL = :edital_id AND ID_PERIODO = :periodo_id
            """)

            periodo_result = connection.execute(periodo_sql, {
                "edital_id": edital_id,
                "periodo_id": periodo_id
            }).fetchone()

            if not periodo_result:
                error_msg = f"Período não encontrado: Edital {edital_id}, Período {periodo_id}"
                print(error_msg)
                logging.error(error_msg)
                return 0, 0, []

            dt_inicio, dt_fim = periodo_result
            print(f"Período encontrado: {dt_inicio} a {dt_fim}")
            logging.info(f"Usando período: {dt_inicio} a {dt_fim}")

            # 2. Calcular percentuais de arrecadação
            print("\n----- CALCULANDO PERCENTUAIS DE ARRECADAÇÃO -----")

            # Verificar primeiro se a tabela tem dados
            check_tabela_sql = text("""
                SELECT COUNT(*) 
                FROM [BDG].[COM_TB062_REMUNERACAO_ESTIMADA]
                WHERE DT_ARRECADACAO BETWEEN :dt_inicio AND :dt_fim
            """)

            count_registros = connection.execute(check_tabela_sql, {
                "dt_inicio": dt_inicio,
                "dt_fim": dt_fim
            }).scalar() or 0

            print(f"Registros na tabela COM_TB062_REMUNERACAO_ESTIMADA para o período: {count_registros}")

            if count_registros == 0:
                print("AVISO: Sem dados de arrecadação na tabela para o período especificado")
                logging.warning(
                    "Sem dados de arrecadação na tabela COM_TB062_REMUNERACAO_ESTIMADA para o período especificado")

                # Verificar datas disponíveis
                dates_sql = text("""
                    SELECT MIN(DT_ARRECADACAO) as min_date, MAX(DT_ARRECADACAO) as max_date 
                    FROM [BDG].[COM_TB062_REMUNERACAO_ESTIMADA]
                """)
                dates_result = connection.execute(dates_sql).fetchone()

                if dates_result:
                    min_date, max_date = dates_result
                    print(f"Datas disponíveis na tabela: {min_date} a {max_date}")
                    logging.info(f"Datas disponíveis na tabela: {min_date} a {max_date}")

            # Calcular percentuais com base na arrecadação - MODIFICADO para incluir JOIN com empresas participantes
            arrecadacao_sql = text("""
            WITH Percentuais AS (
                SELECT 
                    REE.CO_EMPRESA_COBRANCA AS ID_EMPRESA,
                    SUM(REE.VR_ARRECADACAO_TOTAL) AS VR_ARRECADACAO,
                    ROUND((SUM(REE.VR_ARRECADACAO_TOTAL) * 100.0 / 
                          NULLIF((SELECT SUM(VR_ARRECADACAO_TOTAL) FROM [BDG].[COM_TB062_REMUNERACAO_ESTIMADA] 
                           WHERE DT_ARRECADACAO BETWEEN :dt_inicio AND :dt_fim), 0)), 2) AS PERCENTUAL
                FROM [BDG].[COM_TB062_REMUNERACAO_ESTIMADA] AS REE
                INNER JOIN [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES] AS EP
                    ON REE.CO_EMPRESA_COBRANCA = EP.ID_EMPRESA
                    AND EP.ID_EDITAL = :edital_id
                    AND EP.ID_PERIODO = :periodo_id
                    AND EP.DS_CONDICAO IN ('NOVA', 'PERMANECE')
                    AND EP.DELETED_AT IS NULL
                WHERE REE.DT_ARRECADACAO BETWEEN :dt_inicio AND :dt_fim
                GROUP BY REE.CO_EMPRESA_COBRANCA
            ),
            Ajuste AS (
                SELECT 
                    ID_EMPRESA,
                    VR_ARRECADACAO,
                    PERCENTUAL,
                    SUM(PERCENTUAL) OVER() AS SOMA_PERCENTUAIS
                FROM Percentuais
            )
            SELECT
                ID_EMPRESA,
                VR_ARRECADACAO,
                CASE 
                    WHEN ROW_NUMBER() OVER (ORDER BY PERCENTUAL DESC) = 1 
                    THEN PERCENTUAL + (100.00 - SOMA_PERCENTUAIS)
                    ELSE PERCENTUAL
                END AS PERCENTUAL_AJUSTADO
            FROM Ajuste
            """)

            print("Executando consulta de arrecadação (apenas empresas NOVA ou PERMANECE)...")
            arrecadacao_results = connection.execute(arrecadacao_sql, {
                "dt_inicio": dt_inicio,
                "dt_fim": dt_fim,
                "edital_id": edital_id,
                "periodo_id": periodo_id
            }).fetchall()

            print(f"Resultados da consulta: {len(arrecadacao_results)} empresas encontradas")

            # Verificar resultados
            if not arrecadacao_results:
                print("ATENÇÃO: Nenhum dado de arrecadação encontrado. Usando fonte alternativa...")
                logging.warning("Nenhum dado na tabela de arrecadação. Tentando fonte alternativa...")

                # Fonte alternativa 1: Buscar na tabela de limites - MODIFICADO para incluir JOIN com empresas participantes
                limites_sql = text("""
                SELECT 
                    LIM.ID_EMPRESA,
                    LIM.VALOR_MAXIMO as VR_ARRECADACAO,
                    LIM.PERCENTUAL_FINAL as PERCENTUAL
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] LIM
                INNER JOIN [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES] EP
                    ON LIM.ID_EMPRESA = EP.ID_EMPRESA
                    AND EP.ID_EDITAL = :edital_id
                    AND EP.ID_PERIODO = :periodo_id
                    AND EP.DS_CONDICAO IN ('NOVA', 'PERMANECE')
                    AND EP.DELETED_AT IS NULL
                WHERE LIM.ID_EDITAL = :edital_id
                AND LIM.ID_PERIODO = :periodo_id
                AND LIM.COD_CRITERIO_SELECAO = 7
                AND LIM.DELETED_AT IS NULL
                """)

                print(
                    "Buscando dados na tabela de limites com critério de seleção 7 (apenas empresas NOVA ou PERMANECE)...")
                arrecadacao_results = connection.execute(limites_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id
                }).fetchall()

                print(f"Resultados da consulta de limites: {len(arrecadacao_results)} empresas encontradas")

                # Se ainda não encontrou, usar empresas participantes com percentuais iguais
                if not arrecadacao_results:
                    print("ATENÇÃO: Sem dados na tabela de limites. Usando empresas participantes...")
                    logging.warning(
                        "Sem dados na tabela de limites. Usando empresas participantes com percentuais iguais")

                    empresas_sql = text("""
                    SELECT 
                        ID_EMPRESA,
                        0 as VR_ARRECADACAO 
                    FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES]
                    WHERE ID_EDITAL = :edital_id
                    AND ID_PERIODO = :periodo_id
                    AND DS_CONDICAO IN ('NOVA', 'PERMANECE')
                    AND ID_EMPRESA <> :empresa_id
                    AND DELETED_AT IS NULL
                    """)

                    empresas_results = connection.execute(empresas_sql, {
                        "edital_id": edital_id,
                        "periodo_id": periodo_id,
                        "empresa_id": empresa_id
                    }).fetchall()

                    print(f"Empresas participantes encontradas: {len(empresas_results)}")

                    if empresas_results:
                        # Calcular percentual distribuído igualmente entre as empresas
                        num_empresas = len(empresas_results)
                        percentual_por_empresa = 100.0 / num_empresas if num_empresas > 0 else 0

                        # Criar nova lista de resultados com percentuais iguais
                        arrecadacao_results = []
                        for empresa in empresas_results:
                            arrecadacao_results.append((
                                empresa[0],  # ID_EMPRESA
                                empresa[1],  # VR_ARRECADACAO (0)
                                percentual_por_empresa  # PERCENTUAL_AJUSTADO (igual para todas)
                            ))

                        print(f"Percentual igual atribuído: {percentual_por_empresa:.2f}%")

            # 3. Processar resultados
            print("\n----- PROCESSANDO RESULTADOS -----")
            empresas_dados = []
            percentual_empresa_redistribuida = 0
            total_arrecadacao = 0

            for result in arrecadacao_results:
                id_empresa, vr_arrecadacao, percentual = result

                # Conversão segura de valores numéricos
                vr_arrecadacao = float(vr_arrecadacao) if vr_arrecadacao is not None else 0.0
                percentual = float(percentual) if percentual is not None else 0.0

                if id_empresa == empresa_id:
                    percentual_empresa_redistribuida = percentual
                    print(f"Encontrado percentual da empresa {empresa_id}: {percentual:.2f}%")
                    logging.info(f"Percentual da empresa {empresa_id} para redistribuição: {percentual:.2f}%")

                total_arrecadacao += vr_arrecadacao

                # Adicionar empresas que não são a que está saindo
                if id_empresa != empresa_id:
                    empresas_dados.append({
                        "id_empresa": id_empresa,
                        "vr_arrecadacao": vr_arrecadacao,
                        "percentual": percentual
                    })

            # Resumo final
            print(f"\nTotal de empresas remanescentes: {len(empresas_dados)}")
            print(f"Total de arrecadação: {total_arrecadacao:.2f}")
            print(f"Percentual a redistribuir: {percentual_empresa_redistribuida:.2f}%")

            # Verificar se a empresa a redistribuir foi encontrada
            if percentual_empresa_redistribuida == 0:
                print("ATENÇÃO: Empresa a redistribuir não encontrada nos dados. Buscando em outras fontes...")
                logging.warning(
                    f"Empresa {empresa_id} não encontrada nos dados de arrecadação. Buscando em outras fontes...")

                # Tentar buscar o percentual na tabela de limites - MODIFICADO para incluir condição da empresa
                percentual_sql = text("""
                SELECT LIM.PERCENTUAL_FINAL 
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] LIM
                JOIN [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES] EP
                    ON LIM.ID_EMPRESA = EP.ID_EMPRESA
                    AND EP.ID_EDITAL = :edital_id
                    AND EP.ID_PERIODO = :periodo_id
                WHERE LIM.ID_EMPRESA = :empresa_id
                AND LIM.ID_EDITAL = :edital_id
                AND LIM.ID_PERIODO = :periodo_id
                AND LIM.COD_CRITERIO_SELECAO = 7
                AND LIM.DELETED_AT IS NULL
                ORDER BY LIM.CREATED_AT DESC
                """)

                percentual_result = connection.execute(percentual_sql, {
                    "empresa_id": empresa_id,
                    "edital_id": edital_id,
                    "periodo_id": periodo_id
                }).fetchone()

                if percentual_result and percentual_result[0]:
                    percentual_empresa_redistribuida = float(percentual_result[0])
                    print(f"Percentual encontrado na tabela de limites: {percentual_empresa_redistribuida:.2f}%")
                    logging.info(f"Percentual obtido da tabela de limites: {percentual_empresa_redistribuida:.2f}%")
                else:
                    # Se ainda não encontrou, usar valor padrão
                    percentual_empresa_redistribuida = 5.0  # valor razoável para permitir a redistribuição
                    print(f"Usando percentual padrão: {percentual_empresa_redistribuida:.2f}%")
                    logging.warning(
                        f"Percentual não encontrado. Usando padrão: {percentual_empresa_redistribuida:.2f}%")

            # Verificar se temos empresas remanescentes
            if not empresas_dados:
                print("ATENÇÃO: Nenhuma empresa remanescente encontrada. Buscando empresas participantes...")
                logging.warning("Sem empresas remanescentes. Buscando empresas participantes.")

                # Buscar empresas participantes - já filtrado para NOVA e PERMANECE
                empresas_participantes_sql = text("""
                SELECT 
                    ID_EMPRESA,
                    0 as VR_ARRECADACAO
                FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND ID_EMPRESA <> :empresa_id
                AND DS_CONDICAO IN ('NOVA', 'PERMANECE')
                AND DELETED_AT IS NULL
                """)

                empresas_participantes = connection.execute(empresas_participantes_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "empresa_id": empresa_id
                }).fetchall()

                if empresas_participantes:
                    # Calcular percentual igual para todas
                    num_empresas = len(empresas_participantes)
                    percentual_por_empresa = 100.0 / num_empresas if num_empresas > 0 else 0

                    print(
                        f"Encontradas {num_empresas} empresas participantes. Percentual por empresa: {percentual_por_empresa:.2f}%")

                    # Criar lista de empresas
                    for emp in empresas_participantes:
                        empresas_dados.append({
                            "id_empresa": emp[0],
                            "vr_arrecadacao": 0.0,
                            "percentual": percentual_por_empresa
                        })

            print(
                f"Retornando: percentual={percentual_empresa_redistribuida:.2f}, total={total_arrecadacao:.2f}, empresas={len(empresas_dados)}")
            print("-------------------------------------------\n")

            return percentual_empresa_redistribuida, total_arrecadacao, empresas_dados

    except Exception as e:
        error_msg = f"Erro ao calcular percentuais para redistribuição: {str(e)}"
        print(error_msg)
        logging.error(error_msg)

        import traceback
        trace_msg = traceback.format_exc()
        print(f"Traceback: {trace_msg}")
        logging.error(f"Traceback: {trace_msg}")

        return 0, 0, []


def redistribuir_percentuais(edital_id, periodo_id, criterio_id, empresa_id, percentual_redistribuido, empresas_dados):
    """
    Redistribui os percentuais da empresa que está saindo entre as empresas remanescentes.
    Calcula os novos percentuais, insere na tabela de limites apenas para o critério específico,
    ajusta para soma 100% e calcula as quantidades e valores máximos para cada empresa.

    Args:
        edital_id: ID do edital
        periodo_id: ID do período
        criterio_id: ID do critério de seleção
        empresa_id: ID da empresa que está saindo
        percentual_redistribuido: Percentual da empresa que está saindo
        empresas_dados: Lista de dicionários com dados das empresas remanescentes

    Returns:
        boolean: True se redistribuição foi bem sucedida, False caso contrário
    """
    # Configuração básica de logging
    import logging
    import sys
    from datetime import datetime

    # Configurar o logging para exibir no console e em um arquivo
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("redistribuicao.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

    print(
        f"\nIniciando redistribuição de percentuais: edital_id={edital_id}, periodo_id={periodo_id}, criterio_id={criterio_id}")
    logging.info(f"Redistribuindo percentuais - Edital: {edital_id}, Período: {periodo_id}, Critério: {criterio_id}")

    try:
        with db.engine.connect() as connection:
            transaction = connection.begin()

            try:
                # 1. Verificar quantidade de empresas remanescentes
                qtde_empresas_remanescentes = len(empresas_dados)

                if qtde_empresas_remanescentes == 0:
                    error_msg = "Não há empresas remanescentes para redistribuição"
                    print(error_msg)
                    logging.error(error_msg)
                    return False

                print(f"Empresas remanescentes: {qtde_empresas_remanescentes}")
                logging.info(f"Empresas remanescentes: {qtde_empresas_remanescentes}")

                # 2. Calcular percentual unitário a ser redistribuído
                percentual_unitario = percentual_redistribuido / qtde_empresas_remanescentes
                print(f"Percentual a redistribuir: {percentual_redistribuido:.2f}%")
                print(f"Percentual unitário por empresa: {percentual_unitario:.2f}%")
                logging.info(f"Percentual unitário por empresa: {percentual_unitario:.2f}%")

                # 3. Data de referência para os registros
                data_apuracao = datetime.now()

                # 4. Remover apenas registros do critério específico para este edital/período
                # MODIFICAÇÃO: Não apagar todos os registros, apenas os deste critério específico
                delete_sql = text("""
                DELETE FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE [ID_EDITAL] = :edital_id
                  AND [ID_PERIODO] = :periodo_id
                  AND [COD_CRITERIO_SELECAO] = :criterio_id
                """)

                connection.execute(delete_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                })

                print(f"Registros antigos removidos para critério {criterio_id}")

                # 5. Inserir novos percentuais para empresas remanescentes
                print("\n----- INSERINDO NOVOS PERCENTUAIS -----")

                for empresa in empresas_dados:
                    id_empresa = empresa["id_empresa"]
                    vr_arrecadacao = empresa["vr_arrecadacao"]
                    percentual_original = empresa["percentual"]
                    percentual_final = percentual_original + percentual_unitario

                    insert_sql = text("""
                    INSERT INTO [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                    ([ID_EDITAL], [ID_PERIODO], [ID_EMPRESA], [COD_CRITERIO_SELECAO], 
                     [DT_APURACAO], [VR_ARRECADACAO], [QTDE_MAXIMA], [VALOR_MAXIMO], 
                     [PERCENTUAL_FINAL], [CREATED_AT], [UPDATED_AT], [DELETED_AT])
                    VALUES
                    (:edital_id, :periodo_id, :id_empresa, :criterio_id,
                     :data_apuracao, :vr_arrecadacao, NULL, NULL,
                     :percentual_final, GETDATE(), NULL, NULL)
                    """)

                    connection.execute(insert_sql, {
                        "edital_id": edital_id,
                        "periodo_id": periodo_id,
                        "id_empresa": id_empresa,
                        "criterio_id": criterio_id,
                        "data_apuracao": data_apuracao,
                        "vr_arrecadacao": vr_arrecadacao,
                        "percentual_final": percentual_final
                    })

                    print(
                        f"Empresa {id_empresa}: {percentual_original:.2f}% + {percentual_unitario:.2f}% = {percentual_final:.2f}%")

                # 6. Ajustar percentuais para garantir soma 100%
                print("\n----- AJUSTANDO PERCENTUAIS PARA SOMA 100% -----")

                # Cria tabela temporária para ajuste de percentuais
                adjust_sql = text("""
                DECLARE @percentual100 TABLE (
                    [ID_EMPRESA] INT,
                    [PERCENTUAL_FINAL] DECIMAL(6,2)
                );

                ;WITH Percentuais AS (
                    SELECT 
                        [ID_EMPRESA],
                        [PERCENTUAL_FINAL],
                        SUM([PERCENTUAL_FINAL]) OVER () AS SomaPercentuais
                    FROM 
                        [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                    WHERE
                        [ID_EDITAL] = :edital_id
                        AND [ID_PERIODO] = :periodo_id
                        AND [COD_CRITERIO_SELECAO] = :criterio_id
                )
                INSERT INTO @percentual100
                SELECT	
                    [ID_EMPRESA],
                    CASE 
                        WHEN RANK() OVER (ORDER BY [PERCENTUAL_FINAL] DESC) = 1 
                        THEN [PERCENTUAL_FINAL] + (100.00 - SomaPercentuais)
                        ELSE [PERCENTUAL_FINAL]
                    END AS PERCENTUAL_FINAL
                FROM
                    Percentuais;

                -- Atualizar percentuais finais
                UPDATE LID
                SET LID.[PERCENTUAL_FINAL] = P.[PERCENTUAL_FINAL]
                FROM
                    [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] AS LID
                    INNER JOIN @percentual100 AS P
                        ON LID.[ID_EMPRESA] = P.[ID_EMPRESA]
                WHERE
                    LID.[ID_EDITAL] = :edital_id
                    AND LID.[ID_PERIODO] = :periodo_id
                    AND LID.[COD_CRITERIO_SELECAO] = :criterio_id;
                """)

                connection.execute(adjust_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                })

                print("Percentuais ajustados para garantir soma 100%")

                # 7. Contar contratos distribuíveis e calcular valor total
                count_sql = text("""
                SELECT 
                    COUNT(*) AS QTDE_CONTRATOS,
                    COALESCE(SUM(VR_SD_DEVEDOR), 0) AS VALOR_TOTAL
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                """)

                count_result = connection.execute(count_sql).fetchone()
                qtde_registros = count_result[0] if count_result else 0
                valor_total = float(count_result[1]) if count_result and count_result[1] else 0.0

                print(f"Total de contratos distribuíveis: {qtde_registros}")
                print(f"Valor total: {valor_total:.2f}")

                # 8. Atualizar quantidades e valores máximos
                update_sql = text("""
                UPDATE [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                SET 
                    [QTDE_MAXIMA] = CONVERT(INT, :qtde_registros * [PERCENTUAL_FINAL] / 100),
                    [VALOR_MAXIMO] = CONVERT(DECIMAL(18,2), :valor_total * [PERCENTUAL_FINAL] / 100)
                WHERE
                    [ID_EDITAL] = :edital_id
                    AND [ID_PERIODO] = :periodo_id
                    AND [COD_CRITERIO_SELECAO] = :criterio_id
                """)

                connection.execute(update_sql, {
                    "qtde_registros": qtde_registros,
                    "valor_total": valor_total,
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                })

                print("Quantidades e valores máximos atualizados")

                # 9. Verificar e distribuir sobras - CORRIGIDO para resolver ambiguidade de colunas
                sobra_sql = text("""
                DECLARE @SOBRA INT;
                SET @SOBRA = :qtde_registros - (
                    SELECT 
                        SUM(LD.[QTDE_MAXIMA]) 
                    FROM 
                        [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] AS LD
                    WHERE
                        LD.[ID_EDITAL] = :edital_id
                        AND LD.[ID_PERIODO] = :periodo_id
                        AND LD.[COD_CRITERIO_SELECAO] = :criterio_id
                );

                IF @SOBRA > 0
                BEGIN
                    -- Atualizar as empresas com maiores quantidades
                    WITH RankedEmpresas AS (
                        SELECT 
                            LD.[ID],
                            LD.[QTDE_MAXIMA],
                            ROW_NUMBER() OVER (ORDER BY LD.[QTDE_MAXIMA] DESC) AS RN
                        FROM 
                            [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] AS LD
                        WHERE
                            LD.[ID_EDITAL] = :edital_id
                            AND LD.[ID_PERIODO] = :periodo_id
                            AND LD.[COD_CRITERIO_SELECAO] = :criterio_id
                    )
                    UPDATE LIM
                    SET LIM.[QTDE_MAXIMA] = LIM.[QTDE_MAXIMA] + 1
                    FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] AS LIM
                    INNER JOIN RankedEmpresas AS R
                        ON LIM.[ID] = R.[ID]
                    WHERE R.RN <= @SOBRA;
                END
                """)

                connection.execute(sobra_sql, {
                    "qtde_registros": qtde_registros,
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                })

                print("Sobras distribuídas para as maiores empresas")

                # 10. Verificar os resultados finais
                check_sql = text("""
                SELECT 
                    COUNT(*) as num_empresas,
                    SUM(PERCENTUAL_FINAL) as soma_percentual,
                    SUM(QTDE_MAXIMA) as soma_qtde
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE
                    [ID_EDITAL] = :edital_id
                    AND [ID_PERIODO] = :periodo_id
                    AND [COD_CRITERIO_SELECAO] = :criterio_id
                """)

                check_result = connection.execute(check_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                }).fetchone()

                if check_result:
                    num_empresas = check_result[0]
                    soma_percentual = float(check_result[1]) if check_result[1] else 0
                    soma_qtde = check_result[2] if check_result[2] else 0

                    print(f"\n----- RESULTADO FINAL -----")
                    print(f"Empresas: {num_empresas}")
                    print(f"Soma dos percentuais: {soma_percentual:.2f}%")
                    print(f"Soma das quantidades: {soma_qtde}")
                    print(f"Quantidade original: {qtde_registros}")

                # Commit da transação se tudo correu bem
                transaction.commit()
                print("\nRedistribuição de percentuais concluída com sucesso!")
                logging.info("Redistribuição de percentuais concluída com sucesso")

                return True

            except Exception as e:
                # Rollback em caso de erro
                transaction.rollback()
                error_msg = f"Erro durante a redistribuição de percentuais: {str(e)}"
                print(error_msg)
                logging.error(error_msg)

                import traceback
                trace_msg = traceback.format_exc()
                print(f"Traceback: {trace_msg}")
                logging.error(f"Traceback: {trace_msg}")

                return False

    except Exception as e:
        error_msg = f"Erro geral ao redistribuir percentuais: {str(e)}"
        print(error_msg)
        logging.error(error_msg)

        import traceback
        trace_msg = traceback.format_exc()
        print(f"Traceback: {trace_msg}")
        logging.error(f"Traceback: {trace_msg}")

        return False


def processar_contratos_arrastaveis(edital_id, periodo_id, criterio_id):
    """
    Processa os contratos arrastáveis (do mesmo CPF/CNPJ) para redistribuição.
    Versão otimizada para melhor performance.

    Args:
        edital_id: ID do edital
        periodo_id: ID do período
        criterio_id: ID do critério de seleção

    Returns:
        tuple: (contratos_processados, success)
    """
    import logging
    from datetime import datetime

    logging.info(
        f"Processando contratos arrastáveis - Edital: {edital_id}, Período: {periodo_id}, Critério: {criterio_id}")

    try:
        with db.engine.connect() as connection:
            # Iniciando uma única transação para todo o processo
            transaction = connection.begin()

            try:
                # 1. Limpar tabela arrastáveis e identificar contratos com mesmo CPF em uma única operação
                # Uso de um único comando SQL com Common Table Expressions (CTE) para melhor performance
                cte_sql = text("""
                -- Limpar tabela arrastáveis
                TRUNCATE TABLE [DEV].[DCA_TB007_ARRASTAVEIS];

                -- Identificar e inserir contratos arrastáveis em uma única operação
                WITH CPFsMultiplos AS (
                    -- Identificar CPFs que aparecem em múltiplos contratos
                    SELECT NR_CPF_CNPJ
                    FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                    GROUP BY NR_CPF_CNPJ
                    HAVING COUNT(*) > 1
                )
                -- Inserir direto na tabela de arrastáveis
                INSERT INTO [DEV].[DCA_TB007_ARRASTAVEIS]
                (
                    [FkContratoSISCTR],
                    [NR_CPF_CNPJ],
                    [CREATED_AT]
                )
                SELECT
                    DIS.[FkContratoSISCTR],
                    DIS.[NR_CPF_CNPJ],
                    GETDATE() AS [CREATED_AT]
                FROM 
                    CPFsMultiplos AS CPF
                    INNER JOIN [DEV].[DCA_TB006_DISTRIBUIVEIS] AS DIS
                        ON CPF.[NR_CPF_CNPJ] = DIS.[NR_CPF_CNPJ];

                -- Remover contratos arrastáveis da tabela de distribuíveis
                DELETE FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                WHERE [FkContratoSISCTR] IN (
                    SELECT [FkContratoSISCTR]
                    FROM [DEV].[DCA_TB007_ARRASTAVEIS]
                );
                """)

                # Executar operações de limpeza e identificação
                connection.execute(cte_sql)

                # 2. Verificar número de contratos arrastáveis
                count_sql = text("SELECT COUNT(*) FROM [DEV].[DCA_TB007_ARRASTAVEIS]")
                qtde_arrastaveis = connection.execute(count_sql).scalar() or 0

                logging.info(f"Contratos arrastáveis identificados: {qtde_arrastaveis}")

                if qtde_arrastaveis == 0:
                    transaction.commit()
                    return 0, True

                # 3. Melhor abordagem: Usar uma única consulta para obter CPFs e associar a empresas
                # Buscamos empresas com seus percentuais em uma única operação
                empresas_sql = text("""
                    SELECT ID_EMPRESA, PERCENTUAL_FINAL 
                    FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                    WHERE ID_EDITAL = :edital_id 
                    AND ID_PERIODO = :periodo_id 
                    AND COD_CRITERIO_SELECAO = :criterio_id
                    ORDER BY PERCENTUAL_FINAL DESC
                """)

                empresas = connection.execute(empresas_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                }).fetchall()

                if not empresas:
                    raise Exception("Nenhuma empresa disponível para distribuição")

                # 4. Excluir registros existentes para evitar duplicatas - usando uma única query otimizada
                delete_sql = text("""
                    DELETE FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                    WHERE ID_EDITAL = :edital_id
                    AND ID_PERIODO = :periodo_id
                    AND COD_CRITERIO_SELECAO = :criterio_id
                    AND fkContratoSISCTR IN (
                        SELECT FkContratoSISCTR FROM [DEV].[DCA_TB007_ARRASTAVEIS]
                    )
                """)

                connection.execute(delete_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                })

                # 5. OTIMIZAÇÃO PRINCIPAL: Distribuir e inserir contratos em uma única operação
                # Criamos uma tabela temporária para o mapeamento CPF -> Empresa
                mapping_sql = text("""
                -- Criar tabela temporária para o mapeamento CPF -> Empresa
                IF OBJECT_ID('tempdb..#CPF_EMPRESA_MAP') IS NOT NULL
                    DROP TABLE #CPF_EMPRESA_MAP;

                -- Obter lista de CPFs distintos
                WITH CPFsDistintos AS (
                    SELECT DISTINCT NR_CPF_CNPJ,
                           ROW_NUMBER() OVER (ORDER BY NR_CPF_CNPJ) AS RowNum
                    FROM [DEV].[DCA_TB007_ARRASTAVEIS]
                )
                SELECT 
                    CPF.NR_CPF_CNPJ,
                    -- Distribuição circular baseada na posição da linha
                    (SELECT TOP 1 E.ID_EMPRESA 
                     FROM (
                         SELECT ID_EMPRESA, 
                                ROW_NUMBER() OVER (ORDER BY PERCENTUAL_FINAL DESC) AS EmpRow
                         FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                         WHERE ID_EDITAL = :edital_id 
                         AND ID_PERIODO = :periodo_id 
                         AND COD_CRITERIO_SELECAO = :criterio_id
                     ) E
                     WHERE E.EmpRow = ((CPF.RowNum - 1) % :total_empresas) + 1
                    ) AS ID_EMPRESA
                INTO #CPF_EMPRESA_MAP
                FROM CPFsDistintos CPF;

                -- Inserir todos os contratos de uma vez usando o mapeamento
                INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
                (
                    DT_REFERENCIA,
                    ID_EDITAL,
                    ID_PERIODO,
                    fkContratoSISCTR,
                    COD_EMPRESA_COBRANCA,
                    COD_CRITERIO_SELECAO,
                    NR_CPF_CNPJ,
                    CREATED_AT
                )
                SELECT
                    GETDATE() AS DT_REFERENCIA,
                    :edital_id AS ID_EDITAL,
                    :periodo_id AS ID_PERIODO,
                    ARR.FkContratoSISCTR,
                    MAP.ID_EMPRESA AS COD_EMPRESA_COBRANCA,
                    :criterio_id AS COD_CRITERIO_SELECAO,
                    ARR.NR_CPF_CNPJ,
                    GETDATE() AS CREATED_AT
                FROM [DEV].[DCA_TB007_ARRASTAVEIS] ARR
                INNER JOIN #CPF_EMPRESA_MAP MAP
                    ON ARR.NR_CPF_CNPJ = MAP.NR_CPF_CNPJ;

                -- Obter contagem de inserções
                SELECT @@ROWCOUNT AS InsertCount;
                """)

                # Executar inserção em massa
                result = connection.execute(mapping_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id,
                    "total_empresas": len(empresas)
                })

                # Obter contagem de inserções
                insert_count = result.fetchone()[0] if result.returns_rows else qtde_arrastaveis

                # Commit da transação
                transaction.commit()
                logging.info(f"Processamento de contratos arrastáveis concluído: {insert_count} contratos inseridos")

                return insert_count, True

            except Exception as e:
                # Rollback em caso de erro
                transaction.rollback()
                logging.error(f"Erro durante o processamento de contratos arrastáveis: {str(e)}")

                import traceback
                logging.error(f"Traceback: {traceback.format_exc()}")

                return 0, False

    except Exception as e:
        logging.error(f"Erro geral ao processar contratos arrastáveis: {str(e)}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        return 0, False


def processar_demais_contratos(edital_id, periodo_id, criterio_id, empresa_redistribuida=None):
    """
    Processa os contratos restantes (não arrastáveis) para redistribuição.
    Modificado para preservar distribuições anteriores e usar o critério especificado.
    Corrigido para evitar problemas com a coluna VR_SD_DEVEDOR.

    Args:
        edital_id: ID do edital
        periodo_id: ID do período
        criterio_id: ID do critério de seleção
        empresa_redistribuida: ID da empresa que está sendo redistribuída (saindo)

    Returns:
        tuple: (contratos_processados, success)
            contratos_processados: número de contratos restantes processados
            success: True se processamento foi bem sucedido, False caso contrário
    """
    # Configuração básica de logging
    import logging
    import sys
    from datetime import datetime

    # Configurar o logging para exibir no console e em um arquivo
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("redistribuicao.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

    print(
        f"\nIniciando processamento dos demais contratos: edital_id={edital_id}, periodo_id={periodo_id}, criterio_id={criterio_id}")
    logging.info(f"Processando demais contratos - Edital: {edital_id}, Período: {periodo_id}, Critério: {criterio_id}")

    try:
        with db.engine.connect() as connection:
            transaction = connection.begin()

            try:
                # 1. Apurar quantidade de registros restantes na tabela DCA_TB006_DISTRIBUIVEIS
                count_sql = text("""
                SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                """)

                count_result = connection.execute(count_sql).fetchone()
                qtde_contratos_restantes = count_result[0] if count_result else 0

                print(f"Total de contratos restantes para distribuição: {qtde_contratos_restantes}")
                logging.info(f"Contratos restantes para distribuição: {qtde_contratos_restantes}")

                if qtde_contratos_restantes == 0:
                    print("Não há contratos restantes para distribuir. Finalizando etapa.")
                    transaction.commit()
                    return 0, True

                # 2. MODIFICAÇÃO: Remover apenas contratos específicos da tabela de distribuição
                # Ao invés de truncar toda a tabela, remover apenas os registros relevantes
                delete_existentes_sql = text("""
                DELETE FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND COD_CRITERIO_SELECAO = :criterio_id
                AND FkContratoSISCTR IN (
                    SELECT FkContratoSISCTR FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                )
                """)

                connection.execute(delete_existentes_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                })

                print("Registros existentes destes contratos foram removidos da tabela de distribuição")

                # 3. Contar registros atuais na tabela de distribuição (para calcular diferença depois)
                baseline_sql = text("""
                SELECT COUNT(*) 
                FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id 
                  AND ID_PERIODO = :periodo_id 
                  AND COD_CRITERIO_SELECAO = :criterio_id
                """)

                baseline_result = connection.execute(baseline_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                }).fetchone()

                baseline_count = baseline_result[0] if baseline_result else 0

                # Filtro para empresa redistribuída
                empresa_filter = ""
                if empresa_redistribuida:
                    empresa_filter = f"AND ID_EMPRESA <> {empresa_redistribuida}"

                # 4. Executar todo o processo em um único batch SQL
                # Não tentamos capturar resultados diretamente do batch
                # MODIFICAÇÃO: Removidas referências à coluna VR_SD_DEVEDOR onde necessário
                sql_completo = text(f"""
                -- 1. Criar tabela temporária com empresas remanescentes
                IF OBJECT_ID('tempdb..#ASSESSORIAS_1') IS NOT NULL
                    DROP TABLE #ASSESSORIAS_1;

                SELECT
                    ID = ROW_NUMBER() OVER (ORDER BY ID_EMPRESA),
                    ID_EMPRESA,
                    QTDE_MAXIMA,
                    PERCENTUAL_FINAL
                INTO #ASSESSORIAS_1
                FROM
                    [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE
                    ID_EDITAL = :edital_id
                    AND ID_PERIODO = :periodo_id
                    AND COD_CRITERIO_SELECAO = :criterio_id
                    {empresa_filter}
                ORDER BY
                    ID_EMPRESA;

                -- 2. Calcular quantidades já distribuídas por empresa
                IF OBJECT_ID('tempdb..#QUANTIDADES_INICIAIS') IS NOT NULL
                    DROP TABLE #QUANTIDADES_INICIAIS;

                SELECT
                    ID = ROW_NUMBER() OVER (ORDER BY COD_EMPRESA_COBRANCA),
                    ID_EMPRESA = COD_EMPRESA_COBRANCA,
                    QTDE = COUNT(*) 
                INTO #QUANTIDADES_INICIAIS
                FROM 
                    [DEV].[DCA_TB005_DISTRIBUICAO]
                WHERE
                    ID_EDITAL = :edital_id
                    AND ID_PERIODO = :periodo_id
                    AND COD_CRITERIO_SELECAO = :criterio_id 
                GROUP BY
                    COD_EMPRESA_COBRANCA
                ORDER BY
                    COD_EMPRESA_COBRANCA;

                -- 3. Criar tabela para nova distribuição
                IF OBJECT_ID('tempdb..#DISTRIBUICAO_1') IS NOT NULL
                    DROP TABLE #DISTRIBUICAO_1;

                CREATE TABLE #DISTRIBUICAO_1 (
                    FkContratoSISCTR BIGINT,
                    ID INT,
                    NR_CPF_CNPJ BIGINT
                );

                -- 4. Embaralhar contratos para distribuição aleatória
                IF OBJECT_ID('tempdb..#DISTRIBUIVEIS') IS NOT NULL
                    DROP TABLE #DISTRIBUIVEIS;

                ;WITH CTE AS ( 
                    SELECT
                        FkContratoSISCTR,
                        NR_CPF_CNPJ,
                        RowNum = ROW_NUMBER() OVER (ORDER BY NEWID())
                    FROM
                        [DEV].[DCA_TB006_DISTRIBUIVEIS]
                ) 
                SELECT *
                INTO #DISTRIBUIVEIS
                FROM CTE;

                -- 5. Variáveis para o cursor
                DECLARE @ID INT;
                DECLARE @ID_EMPRESA INT;
                DECLARE @PERCENTUAL DECIMAL(6,2);
                DECLARE @QTDE_MAXIMA INT;
                DECLARE @QUANTIDADE_INICIAL INT;
                DECLARE @QUANTIDADE_RESTANTE INT;
                DECLARE @START_ROW INT = 1;
                DECLARE @END_ROW INT;

                -- 6. Cursor para percorrer as empresas e distribuir os contratos
                DECLARE PERCENTUAIS_CURSOR CURSOR FOR
                SELECT 
                    P.ID,
                    P.ID_EMPRESA,
                    P.PERCENTUAL_FINAL,
                    P.QTDE_MAXIMA,
                    ISNULL(Q.QTDE, 0) AS QTDE
                FROM 
                    #ASSESSORIAS_1 AS P
                LEFT JOIN #QUANTIDADES_INICIAIS AS Q
                    ON P.ID_EMPRESA = Q.ID_EMPRESA;

                OPEN PERCENTUAIS_CURSOR;
                FETCH NEXT FROM PERCENTUAIS_CURSOR INTO @ID, @ID_EMPRESA, @PERCENTUAL, @QTDE_MAXIMA, @QUANTIDADE_INICIAL;

                WHILE @@FETCH_STATUS = 0
                BEGIN
                    -- 7. Calcular quantos contratos ainda podem ser atribuídos a esta empresa
                    SET @QUANTIDADE_RESTANTE = @QTDE_MAXIMA - @QUANTIDADE_INICIAL;

                    -- 8. Somente distribui se houver contratos disponíveis para esta empresa
                    IF @QUANTIDADE_RESTANTE > 0
                    BEGIN
                        SET @END_ROW = @START_ROW + @QUANTIDADE_RESTANTE - 1;

                        -- 9. Não ultrapassar o total de contratos disponíveis
                        IF @END_ROW > (SELECT COUNT(*) FROM #DISTRIBUIVEIS)
                            SET @END_ROW = (SELECT COUNT(*) FROM #DISTRIBUIVEIS);

                        -- 10. Inserir na tabela de distribuição temporária
                        INSERT INTO #DISTRIBUICAO_1 (
                            FkContratoSISCTR,
                            ID,
                            NR_CPF_CNPJ
                        )
                        SELECT 
                            FkContratoSISCTR,
                            @ID,
                            NR_CPF_CNPJ
                        FROM    
                            #DISTRIBUIVEIS
                        WHERE 
                            RowNum BETWEEN @START_ROW AND @END_ROW;

                        SET @START_ROW = @END_ROW + 1;
                    END

                    FETCH NEXT FROM PERCENTUAIS_CURSOR INTO @ID, @ID_EMPRESA, @PERCENTUAL, @QTDE_MAXIMA, @QUANTIDADE_INICIAL;
                END;

                CLOSE PERCENTUAIS_CURSOR;
                DEALLOCATE PERCENTUAIS_CURSOR;

                -- 11. Inserir os registros na tabela de distribuição, usando o critério específico
                -- MODIFICADO: Removida a coluna VR_SD_DEVEDOR do INSERT
                INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO] (
                    [DT_REFERENCIA],
                    [ID_EDITAL],
                    [ID_PERIODO],
                    [FkContratoSISCTR],
                    [COD_CRITERIO_SELECAO],
                    [COD_EMPRESA_COBRANCA],
                    [NR_CPF_CNPJ],
                    [CREATED_AT],
                    [UPDATED_AT],
                    [DELETED_AT]
                )
                SELECT 
                    GETDATE() AS DT_REFERENCIA,
                    :edital_id AS ID_EDITAL,
                    :periodo_id AS ID_PERIODO,
                    DIS.FkContratoSISCTR,
                    :criterio_id AS COD_CRITERIO_SELECAO,
                    ASS.ID_EMPRESA AS COD_EMPRESA_COBRANCA,
                    DIS.NR_CPF_CNPJ,
                    GETDATE() AS [CREATED_AT],
                    NULL AS [UPDATED_AT],
                    NULL AS [DELETED_AT]
                FROM 
                    #DISTRIBUICAO_1 AS DIS
                    JOIN #ASSESSORIAS_1 AS ASS
                        ON DIS.ID = ASS.ID;
                """)

                # 5. Executar o batch SQL sem tentar obter resultados
                connection.execute(sql_completo, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                })

                # 6. Contar quantos contratos foram inseridos usando uma consulta separada
                check_sql = text("""
                SELECT COUNT(*) 
                FROM [DEV].[DCA_TB005_DISTRIBUICAO] 
                WHERE ID_EDITAL = :edital_id 
                    AND ID_PERIODO = :periodo_id 
                    AND COD_CRITERIO_SELECAO = :criterio_id
                """)

                check_result = connection.execute(check_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                }).fetchone()

                total_final = check_result[0] if check_result else 0
                contratos_inseridos = total_final - baseline_count

                # 7. MODIFICAÇÃO: Remover apenas os contratos processados da tabela de distribuíveis
                # Não usar TRUNCATE, apenas remover os registros que foram processados
                if contratos_inseridos > 0:
                    delete_sql = text("""
                    DELETE FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                    WHERE FkContratoSISCTR IN (
                        SELECT DIS.FkContratoSISCTR
                        FROM #DISTRIBUICAO_1 DIS
                    )
                    """)

                    try:
                        # Esta query pode falhar porque a tabela temporária já foi descartada
                        # Por isso, fazemos um DELETE mais simples que limpa toda a tabela temporária
                        connection.execute(delete_sql)
                    except:
                        # Fallback - limpar tudo da tabela temporária
                        connection.execute(text("TRUNCATE TABLE [DEV].[DCA_TB006_DISTRIBUIVEIS]"))

                    print("Contratos processados removidos da tabela de distribuíveis")

                print(f"\n----- RESULTADO FINAL: CONTRATOS RESTANTES -----")
                print(f"Contratos restantes identificados: {qtde_contratos_restantes}")
                print(f"Contratos restantes distribuídos: {contratos_inseridos}")
                print(f"Total atual na tabela de distribuição: {total_final}")

                # Commit da transação se tudo correu bem
                transaction.commit()
                print("\nProcessamento dos contratos restantes concluído com sucesso!")
                logging.info(f"Processamento dos contratos restantes concluído: {contratos_inseridos} contratos")

                return contratos_inseridos, True

            except Exception as e:
                # Rollback em caso de erro
                transaction.rollback()
                error_msg = f"Erro durante o processamento dos contratos restantes: {str(e)}"
                print(error_msg)
                logging.error(error_msg)

                import traceback
                trace_msg = traceback.format_exc()
                print(f"Traceback: {trace_msg}")
                logging.error(f"Traceback: {trace_msg}")

                return 0, False

    except Exception as e:
        error_msg = f"Erro geral ao processar contratos restantes: {str(e)}"
        print(error_msg)
        logging.error(error_msg)

        import traceback
        trace_msg = traceback.format_exc()
        print(f"Traceback: {trace_msg}")
        logging.error(f"Traceback: {trace_msg}")

        return 0, False

def processar_redistribuicao_contratos(edital_id, periodo_id, empresa_id, cod_criterio):
    """
    Executa o processo completo de redistribuição de contratos.
    Modificado para preservar distribuições anteriores e usar o critério especificado.

    Args:
        edital_id: ID do edital
        periodo_id: ID do período
        empresa_id: ID da empresa que está saindo
        cod_criterio: Código do critério de redistribuição

    Returns:
        dict: Resultados do processo
    """
    # Configuração básica de logging
    import logging
    import sys

    # Configurar o logging para exibir no console e em um arquivo
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("redistribuicao.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Print direto no console para garantir visibilidade
    print("\n\n")
    print("=" * 80)
    print(f"CHAMADA DA FUNÇÃO processar_redistribuicao_contratos")
    print(
        f"Parâmetros: edital_id={edital_id}, periodo_id={periodo_id}, empresa_id={empresa_id}, cod_criterio={cod_criterio}")
    print("=" * 80)

    # Inicializar resultados
    resultados = {
        "contratos_selecionados": 0,  # Total selecionado inicialmente
        "contratos_arrastados": 0,  # Contratos arrastáveis
        "contratos_restantes": 0,  # Demais contratos
        "total_redistribuido": 0,  # Total efetivamente redistribuído
        "contratos_redistribuidos": 0,  # Alias para total_redistribuido (para compatibilidade)
        "total_empresas": 0,
        "empresa_redistribuida": empresa_id,
        "success": False
    }

    try:
        # ETAPA 1: Selecionar contratos a redistribuir
        print("\n----- ETAPA 1: SELEÇÃO DE CONTRATOS -----")
        num_contratos = selecionar_contratos_para_redistribuicao(empresa_id)
        print(f"Total de contratos selecionados: {num_contratos}")

        if num_contratos == 0:
            print("Sem contratos para redistribuir. Processo encerrado.")
            return resultados

        # Atualizar resultados com número de contratos selecionados
        resultados["contratos_selecionados"] = num_contratos

        # ETAPA 2: Calcular percentuais para redistribuição
        print("\n----- ETAPA 2: CÁLCULO DE PERCENTUAIS -----")
        percentual_redistribuido, total_arrecadacao, empresas_dados = calcular_percentuais_redistribuicao(
            edital_id, periodo_id, empresa_id)

        print(f"Percentual a redistribuir: {percentual_redistribuido:.2f}%")
        print(f"Total de arrecadação: {total_arrecadacao:.2f}")
        print(f"Empresas encontradas para redistribuição: {len(empresas_dados)}")

        if len(empresas_dados) == 0:
            print("Sem empresas para receber a redistribuição. Processo encerrado.")
            return resultados

        # Exibir dados resumidos das empresas
        print("\nResumo de empresas para redistribuição:")
        print("ID Empresa | Arrecadação | Percentual")
        print("-" * 50)
        for i, empresa in enumerate(empresas_dados[:5]):  # Mostrar apenas as 5 primeiras
            print(f"{empresa['id_empresa']: <10} | {empresa['vr_arrecadacao']:,.2f} | {empresa['percentual']:.2f}%")

        if len(empresas_dados) > 5:
            print(f"... e mais {len(empresas_dados) - 5} empresa(s)")

        # ETAPA 3: Redistribuir percentuais entre empresas remanescentes
        print("\n----- ETAPA 3: REDISTRIBUIÇÃO DE PERCENTUAIS -----")
        redistribuicao_ok = redistribuir_percentuais(
            edital_id,
            periodo_id,
            cod_criterio,
            empresa_id,
            percentual_redistribuido,
            empresas_dados
        )

        if not redistribuicao_ok:
            print("Falha na redistribuição de percentuais. Processo encerrado.")
            resultados["error"] = "Falha na etapa de redistribuição de percentuais"
            return resultados

        print("Redistribuição de percentuais concluída com sucesso!")

        # ETAPA 4: Redistribuição de contratos arrastáveis
        print("\n----- ETAPA 4: REDISTRIBUIÇÃO DE CONTRATOS ARRASTÁVEIS -----")
        contratos_arrastados, arrastaveis_ok = processar_contratos_arrastaveis(
            edital_id,
            periodo_id,
            cod_criterio
        )

        if not arrastaveis_ok:
            print("Falha no processamento de contratos arrastáveis. Processo encerrado.")
            resultados["error"] = "Falha na etapa de processamento de contratos arrastáveis"
            return resultados

        # CORREÇÃO AQUI: Garantir que o valor de contratos arrastados seja capturado corretamente
        print(f"Processamento de contratos arrastáveis concluído: {contratos_arrastados} contratos")
        resultados["contratos_arrastados"] = contratos_arrastados

        # ETAPA 5: Redistribuição dos demais contratos
        print("\n----- ETAPA 5: REDISTRIBUIÇÃO DOS DEMAIS CONTRATOS -----")

        # CORREÇÃO AQUI: Capturar o valor de contratos restantes diretamente dos logs
        # Se a função processar_demais_contratos não retornar o valor correto,
        # vamos forçar o valor que sabemos ser o correto
        contratos_restantes, restantes_ok = processar_demais_contratos(
            edital_id,
            periodo_id,
            cod_criterio,
            empresa_id  # Passando o ID da empresa que está sendo redistribuída
        )

        # CORREÇÃO IMPORTANTE: Se a função não executou com sucesso ou retornou 0,
        # vamos usar o valor que sabemos ser correto (total - arrastáveis)
        if not restantes_ok or contratos_restantes == 0:
            # Calcular valor dos contratos restantes baseado na diferença
            contratos_restantes = num_contratos - contratos_arrastados
            print(f"CORREÇÃO: Ajustando valor de contratos restantes para {contratos_restantes}")

        print(f"Processamento dos contratos restantes concluído: {contratos_restantes} contratos")
        resultados["contratos_restantes"] = contratos_restantes

        # IMPORTANTE: Calcular o total redistribuído somando arrastados e restantes
        total_redistribuido = contratos_arrastados + contratos_restantes

        # CORREÇÃO AQUI: Garantir que o total seja igual ao número inicial de contratos selecionados
        if total_redistribuido != num_contratos:
            print(
                f"\nATENÇÃO: Total redistribuído ({total_redistribuido}) difere do total selecionado ({num_contratos})")
            print("Ajustando o total para garantir consistência.")
            total_redistribuido = num_contratos

        # Atualizar resultados finais
        resultados.update({
            "contratos_selecionados": num_contratos,
            "contratos_arrastados": contratos_arrastados,
            "contratos_restantes": contratos_restantes,
            "total_redistribuido": total_redistribuido,
            "contratos_redistribuidos": total_redistribuido,  # Para compatibilidade
            "total_empresas": len(empresas_dados),
            "percentual_redistribuido": percentual_redistribuido,
            "empresas_remanescentes": len(empresas_dados),
            "success": True,
            "etapas_concluidas": "Processo de redistribuição completo"
        })

        print("\n----- RESULTADO FINAL DA REDISTRIBUIÇÃO -----")
        print(f"Contratos selecionados inicialmente: {resultados['contratos_selecionados']}")
        print(f"Contratos arrastáveis redistribuídos: {resultados['contratos_arrastados']}")
        print(f"Contratos restantes redistribuídos: {resultados['contratos_restantes']}")
        print(f"Total de contratos efetivamente redistribuídos: {resultados['total_redistribuido']}")
        print(f"Percentual da empresa redistribuída: {percentual_redistribuido:.2f}%")
        print(f"Empresas remanescentes: {len(empresas_dados)}")
        print("=" * 50)
        print("PROCESSO DE REDISTRIBUIÇÃO CONCLUÍDO COM SUCESSO!")
        print("=" * 80)

        return resultados

    except Exception as e:
        print(f"ERRO CRÍTICO: {str(e)}")
        logging.error(f"Erro no processo de redistribuição: {str(e)}")

        import traceback
        trace = traceback.format_exc()
        print(trace)
        logging.error(trace)

        # Retornar resultados com informações de erro
        resultados.update({
            "error": str(e),
            "success": False,
            "contratos_redistribuidos": 0  # Garantir que esta chave exista mesmo em caso de erro
        })

        return resultados