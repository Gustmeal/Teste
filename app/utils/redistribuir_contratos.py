# app/utils/redistribuir_contratos.py
from app import db
from sqlalchemy import text
import logging
from datetime import datetime


def selecionar_contratos_para_redistribuicao(empresa_id):
    """
    Seleciona os contratos da empresa que será redistribuída.

    Args:
        empresa_id: ID da empresa que está saindo

    Returns:
        int: Total de contratos a redistribuir
    """
    try:
        # Limpar tabelas temporárias
        with db.engine.connect() as connection:
            # Truncar tabela de distribuíveis
            truncate_sql = text("TRUNCATE TABLE [DEV].[DCA_TB006_DISTRIBUIVEIS]")
            connection.execute(truncate_sql)

            # Truncar tabela de arrastaveis
            truncate_arrastaveis_sql = text("TRUNCATE TABLE [DEV].[DCA_TB007_ARRASTAVEIS]")
            connection.execute(truncate_arrastaveis_sql)

            # DIAGNÓSTICO: Verificar contratos existentes para esta empresa (sem filtros)
            check_total_contratos_sql = text("""
                SELECT COUNT(*) 
                FROM [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] 
                WHERE [COD_EMPRESA_COBRANCA] = :empresa_id
            """)
            total_contratos = connection.execute(check_total_contratos_sql, {"empresa_id": empresa_id}).scalar() or 0

            if total_contratos == 0:
                logging.error(
                    f"Empresa ID {empresa_id} não possui nenhum contrato na tabela COM_TB011_EMPRESA_COBRANCA_ATUAL")
                return 0

            logging.info(f"Total de contratos encontrados para empresa {empresa_id}: {total_contratos}")

            # DIAGNÓSTICO: Verificar contratos ativos
            check_ativos_sql = text("""
                SELECT COUNT(*) 
                FROM [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                INNER JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] AS SIT
                    ON ECA.fkContratoSISCTR = SIT.fkContratoSISCTR
                WHERE ECA.[COD_EMPRESA_COBRANCA] = :empresa_id
                AND SIT.[fkSituacaoCredito] = 1
            """)
            contratos_ativos = connection.execute(check_ativos_sql, {"empresa_id": empresa_id}).scalar() or 0
            logging.info(f"Contratos ativos (situação=1) para empresa {empresa_id}: {contratos_ativos}")

            # DIAGNÓSTICO: Verificar contratos com suspensão judicial
            check_suspensos_sql = text("""
                SELECT COUNT(*) 
                FROM [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                INNER JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] AS SIT
                    ON ECA.fkContratoSISCTR = SIT.fkContratoSISCTR
                INNER JOIN [BDG].[COM_TB013_SUSPENSO_DECISAO_JUDICIAL] AS SDJ
                    ON ECA.fkContratoSISCTR = SDJ.fkContratoSISCTR
                WHERE ECA.[COD_EMPRESA_COBRANCA] = :empresa_id
                AND SIT.[fkSituacaoCredito] = 1
            """)
            contratos_suspensos = connection.execute(check_suspensos_sql, {"empresa_id": empresa_id}).scalar() or 0
            logging.info(f"Contratos com suspensão judicial para empresa {empresa_id}: {contratos_suspensos}")

            # Inserir contratos da empresa que está saindo na tabela de distribuíveis
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
                AND SIT.[fkSituacaoCredito] = 1
                AND SDJ.fkContratoSISCTR IS NULL
            """)

            result = connection.execute(insert_sql, {"empresa_id": empresa_id})
            logging.info(f"Contratos da empresa {empresa_id} inseridos: {result.rowcount}")

            # Contar contratos selecionados
            count_sql = text("SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] WHERE DELETED_AT IS NULL")
            result = connection.execute(count_sql)
            num_contratos = result.scalar() or 0

            # Resumo do diagnóstico
            if num_contratos == 0:
                razoes = []
                if total_contratos == 0:
                    razoes.append("A empresa não possui nenhum contrato no sistema")
                elif contratos_ativos == 0:
                    razoes.append("A empresa não possui contratos ativos (situação = 1)")
                elif contratos_ativos == contratos_suspensos:
                    razoes.append("Todos os contratos ativos da empresa possuem suspensão judicial")

                motivo = " | ".join(razoes) if razoes else "Motivo desconhecido"
                logging.warning(f"Nenhum contrato encontrado para a empresa {empresa_id}. Motivo: {motivo}")

            return num_contratos

    except Exception as e:
        logging.error(f"Erro ao selecionar contratos para redistribuição: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
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
    try:
        # 1. Buscar período para obter datas de início e fim
        periodo_sql = text("""
            SELECT DT_INICIO, DT_FIM 
            FROM [DEV].[DCA_TB001_PERIODO_AVALIACAO]
            WHERE ID_EDITAL = :edital_id AND ID_PERIODO = :periodo_id
        """)

        with db.engine.connect() as connection:
            periodo_result = connection.execute(periodo_sql,
                                                {"edital_id": edital_id, "periodo_id": periodo_id}).fetchone()

            if not periodo_result:
                logging.error(f"Período não encontrado: Edital {edital_id}, Período {periodo_id}")
                return 0, 0, []

            dt_inicio, dt_fim = periodo_result

            # 2. Calcular percentuais de arrecadação
            arrecadacao_sql = text("""
            WITH Percentuais AS (
                SELECT 
                    REE.CO_EMPRESA_COBRANCA AS ID_EMPRESA,
                    SUM(REE.VR_ARRECADACAO_TOTAL) AS VR_ARRECADACAO,
                    ROUND((SUM(REE.VR_ARRECADACAO_TOTAL) * 100.0 / 
                          (SELECT SUM(VR_ARRECADACAO_TOTAL) FROM [BDG].[COM_TB062_REMUNERACAO_ESTIMADA] 
                           WHERE DT_ARRECADACAO BETWEEN :dt_inicio AND :dt_fim)), 2) AS PERCENTUAL
                FROM [BDG].[COM_TB062_REMUNERACAO_ESTIMADA] AS REE
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

            arrecadacao_results = connection.execute(arrecadacao_sql, {
                "dt_inicio": dt_inicio,
                "dt_fim": dt_fim
            }).fetchall()

            if not arrecadacao_results:
                logging.error("Nenhum dado de arrecadação encontrado para cálculo de percentuais")
                return 0, 0, []

            # 3. Processar resultados
            empresas_dados = []
            percentual_empresa_redistribuida = 0
            total_arrecadacao = 0

            for result in arrecadacao_results:
                id_empresa, vr_arrecadacao, percentual = result

                if id_empresa == empresa_id:
                    percentual_empresa_redistribuida = percentual

                total_arrecadacao += vr_arrecadacao

                empresas_dados.append({
                    "id_empresa": id_empresa,
                    "vr_arrecadacao": vr_arrecadacao,
                    "percentual": percentual
                })

            return percentual_empresa_redistribuida, total_arrecadacao, empresas_dados

    except Exception as e:
        logging.error(f"Erro ao calcular percentuais para redistribuição: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return 0, 0, []


def redistribuir_percentuais(edital_id, periodo_id, empresa_id, cod_criterio, dt_apuracao=None):
    """
    Redistribui percentuais e cria novos limites de distribuição.

    Args:
        edital_id: ID do edital
        periodo_id: ID do período
        empresa_id: ID da empresa que está saindo
        cod_criterio: Código do critério de redistribuição
        dt_apuracao: Data de apuração (opcional)

    Returns:
        bool: True se a redistribuição foi bem-sucedida
    """
    try:
        # Se não foi fornecida data de apuração, usar a data atual
        if dt_apuracao is None:
            dt_apuracao = datetime.now().date()

        # 1. Calcular percentuais de redistribuição
        percentual_redistribuido, total_arrecadacao, empresas_dados = calcular_percentuais_redistribuicao(
            edital_id, periodo_id, empresa_id)

        if percentual_redistribuido == 0 or not empresas_dados:
            logging.error("Falha ao calcular percentuais para redistribuição")
            return False

        # 2. Contar empresas remanescentes
        empresas_remanescentes = [e for e in empresas_dados if e["id_empresa"] != empresa_id]
        qtde_empresas_remanescentes = len(empresas_remanescentes)

        if qtde_empresas_remanescentes == 0:
            logging.error("Nenhuma empresa remanescente para redistribuição")
            return False

        # 3. Calcular percentual unitário a ser redistribuído
        percentual_unitario = percentual_redistribuido / qtde_empresas_remanescentes

        # 4. Inserir novos limites de distribuição
        with db.engine.connect() as connection:
            # 4.1 Primeiro, remover limites anteriores com este critério (se existirem)
            delete_old_limits = text("""
            DELETE FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
            WHERE ID_EDITAL = :edital_id
            AND ID_PERIODO = :periodo_id
            AND COD_CRITERIO_SELECAO = :cod_criterio
            """)

            connection.execute(delete_old_limits, {
                "edital_id": edital_id,
                "periodo_id": periodo_id,
                "cod_criterio": cod_criterio
            })

            # 4.2 Inserir novos limites com percentuais recalculados
            for empresa in empresas_remanescentes:
                novo_percentual = empresa["percentual"] + percentual_unitario

                insert_limit = text("""
                INSERT INTO [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                (ID_EDITAL, ID_PERIODO, ID_EMPRESA, COD_CRITERIO_SELECAO, 
                DT_APURACAO, VR_ARRECADACAO, PERCENTUAL_FINAL, CREATED_AT)
                VALUES (:edital_id, :periodo_id, :empresa_id, :cod_criterio,
                :dt_apuracao, :vr_arrecadacao, :percentual, GETDATE())
                """)

                connection.execute(insert_limit, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "empresa_id": empresa["id_empresa"],
                    "cod_criterio": cod_criterio,
                    "dt_apuracao": dt_apuracao,
                    "vr_arrecadacao": empresa["vr_arrecadacao"],
                    "percentual": novo_percentual
                })

            # 4.3 Ajustar percentuais para garantir soma = 100%
            ajustar_percentuais_sql = text("""
            WITH Percentuais AS (
                SELECT 
                    ID_EMPRESA,
                    PERCENTUAL_FINAL,
                    SUM(PERCENTUAL_FINAL) OVER() AS SomaPercentuais
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND COD_CRITERIO_SELECAO = :cod_criterio
            )
            UPDATE LID
            SET LID.PERCENTUAL_FINAL = 
                CASE 
                    WHEN ROW_NUMBER() OVER (ORDER BY P.PERCENTUAL_FINAL DESC) = 1 
                    THEN P.PERCENTUAL_FINAL + (100.00 - P.SomaPercentuais)
                    ELSE P.PERCENTUAL_FINAL
                END
            FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] LID
            JOIN Percentuais P ON LID.ID_EMPRESA = P.ID_EMPRESA
            WHERE LID.ID_EDITAL = :edital_id
            AND LID.ID_PERIODO = :periodo_id
            AND LID.COD_CRITERIO_SELECAO = :cod_criterio
            """)

            connection.execute(ajustar_percentuais_sql, {
                "edital_id": edital_id,
                "periodo_id": periodo_id,
                "cod_criterio": cod_criterio
            })

            # 5. Calcular valores máximos e quantidades máximas
            num_contratos = selecionar_contratos_para_redistribuicao(empresa_id)
            if num_contratos == 0:
                logging.warning("Nenhum contrato para redistribuição")

            # Obter soma do saldo devedor
            saldo_sql = text("SELECT SUM(VR_SD_DEVEDOR) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]")
            saldo_total = connection.execute(saldo_sql).scalar() or 0

            # Atualizar qtde_maxima e valor_maximo
            update_limits_sql = text("""
            UPDATE [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
            SET QTDE_MAXIMA = FLOOR(:num_contratos * PERCENTUAL_FINAL / 100),
                VALOR_MAXIMO = :saldo_total * PERCENTUAL_FINAL / 100
            WHERE ID_EDITAL = :edital_id
            AND ID_PERIODO = :periodo_id
            AND COD_CRITERIO_SELECAO = :cod_criterio
            """)

            connection.execute(update_limits_sql, {
                "edital_id": edital_id,
                "periodo_id": periodo_id,
                "cod_criterio": cod_criterio,
                "num_contratos": num_contratos,
                "saldo_total": saldo_total
            })

            # 6. Distribuir contratos restantes (uma por empresa)
            distribuir_sobras_sql = text("""
            DECLARE @sobra INT = :num_contratos - (
                SELECT SUM(QTDE_MAXIMA)
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND COD_CRITERIO_SELECAO = :cod_criterio
            );

            IF @sobra > 0
            BEGIN
                WITH EmpresasOrdenadas AS (
                    SELECT ID, QTDE_MAXIMA,
                    ROW_NUMBER() OVER (ORDER BY QTDE_MAXIMA DESC) as RowNum
                    FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                    WHERE ID_EDITAL = :edital_id
                    AND ID_PERIODO = :periodo_id
                    AND COD_CRITERIO_SELECAO = :cod_criterio
                )
                UPDATE LID
                SET QTDE_MAXIMA = QTDE_MAXIMA + 1
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] LID
                JOIN EmpresasOrdenadas EO ON LID.ID = EO.ID
                WHERE EO.RowNum <= @sobra;
            END
            """)

            connection.execute(distribuir_sobras_sql, {
                "edital_id": edital_id,
                "periodo_id": periodo_id,
                "cod_criterio": cod_criterio,
                "num_contratos": num_contratos
            })

        return True

    except Exception as e:
        logging.error(f"Erro ao redistribuir percentuais: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return False


def processar_redistribuicao_contratos(edital_id, periodo_id, empresa_id, cod_criterio):
    """
    Executa o processo completo de redistribuição de contratos.

    Args:
        edital_id: ID do edital
        periodo_id: ID do período
        empresa_id: ID da empresa que está saindo
        cod_criterio: Código do critério de redistribuição

    Returns:
        dict: Resultados do processo
    """
    resultados = {
        "contratos_redistribuidos": 0,
        "contratos_arrastados": 0,
        "contratos_restantes": 0,
        "total_empresas": 0,
        "empresa_redistribuida": empresa_id,
        "success": False
    }

    try:
        # 1. Selecionar contratos a redistribuir
        num_contratos = selecionar_contratos_para_redistribuicao(empresa_id)
        if num_contratos == 0:
            return resultados

        # 2. Redistribuir percentuais e criar limites
        success = redistribuir_percentuais(edital_id, periodo_id, empresa_id, cod_criterio)
        if not success:
            return resultados

        # 3. Identificar contratos arrastáveis (mesmo CPF)
        with db.engine.connect() as connection:
            # 3.1 Limpar tabela de arrastáveis
            truncate_arrastaveis = text("TRUNCATE TABLE [DEV].[DCA_TB007_ARRASTAVEIS]")
            connection.execute(truncate_arrastaveis)

            # 3.2 Identificar CPFs com múltiplos contratos
            insert_arrastaveis = text("""
            WITH CPFsMultiplos AS (
                SELECT NR_CPF_CNPJ
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                GROUP BY NR_CPF_CNPJ
                HAVING COUNT(*) > 1
            )
            INSERT INTO [DEV].[DCA_TB007_ARRASTAVEIS]
            (FkContratoSISCTR, NR_CPF_CNPJ, VR_SD_DEVEDOR, CREATED_AT)
            SELECT D.FkContratoSISCTR, D.NR_CPF_CNPJ, D.VR_SD_DEVEDOR, GETDATE()
            FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] D
            JOIN CPFsMultiplos C ON D.NR_CPF_CNPJ = C.NR_CPF_CNPJ
            """)

            connection.execute(insert_arrastaveis)

            # 3.3 Remover contratos arrastáveis da tabela de distribuíveis
            delete_from_distribuiveis = text("""
            DELETE FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
            WHERE FkContratoSISCTR IN (
                SELECT FkContratoSISCTR FROM [DEV].[DCA_TB007_ARRASTAVEIS]
            )
            """)

            connection.execute(delete_from_distribuiveis)

            # 3.4 Contar contratos arrastáveis
            arrastaveis_count = connection.execute(
                text("SELECT COUNT(*) FROM [DEV].[DCA_TB007_ARRASTAVEIS]")
            ).scalar() or 0

            resultados["contratos_arrastados"] = arrastaveis_count

            # 4. Distribuir contratos arrastáveis
            if arrastaveis_count > 0:
                # Implementar a lógica de distribuição de arrastáveis
                # (Similar à função aplicar_regra_arrasto_sem_acordo)
                distribuir_arrastados_sql = text("""
                -- Script para distribuir contratos arrastáveis
                -- Declaração de variáveis locais
                DECLARE @dt_referencia DATE = GETDATE();

                -- 1. Obter empresas e seus percentuais
                IF OBJECT_ID('tempdb..#Empresas') IS NOT NULL
                    DROP TABLE #Empresas;

                SELECT 
                    LD.ID_EMPRESA,
                    LD.PERCENTUAL_FINAL,
                    LD.QTDE_MAXIMA,
                    ROW_NUMBER() OVER (ORDER BY NEWID()) AS ordem
                INTO #Empresas
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
                WHERE LD.ID_EDITAL = :edital_id
                AND LD.ID_PERIODO = :periodo_id
                AND LD.COD_CRITERIO_SELECAO = :cod_criterio;

                -- 2. Verificar quantidade de CPFs distintos
                IF OBJECT_ID('tempdb..#CPFs') IS NOT NULL
                    DROP TABLE #CPFs;

                SELECT 
                    NR_CPF_CNPJ,
                    COUNT(*) AS qtd_contratos,
                    ROW_NUMBER() OVER (ORDER BY NR_CPF_CNPJ) AS ordem
                INTO #CPFs
                FROM [DEV].[DCA_TB007_ARRASTAVEIS]
                GROUP BY NR_CPF_CNPJ;

                -- 3. Distribuir CPFs entre empresas
                IF OBJECT_ID('tempdb..#DistribuicaoCPFs') IS NOT NULL
                    DROP TABLE #DistribuicaoCPFs;

                WITH EmpresaOrdens AS (
                    SELECT 
                        ID_EMPRESA,
                        ordem,
                        PERCENTUAL_FINAL,
                        FLOOR((SELECT COUNT(*) FROM #CPFs) * PERCENTUAL_FINAL / 100.0) AS cpfs_alocados,
                        ROW_NUMBER() OVER (ORDER BY PERCENTUAL_FINAL DESC) AS rank_percentual
                    FROM #Empresas
                )
                SELECT 
                    CPF.NR_CPF_CNPJ,
                    EO.ID_EMPRESA,
                    CPF.qtd_contratos
                INTO #DistribuicaoCPFs
                FROM #CPFs CPF
                CROSS APPLY (
                    SELECT TOP 1 ID_EMPRESA
                    FROM EmpresaOrdens EO
                    ORDER BY 
                        CASE 
                            WHEN CPF.ordem % (SELECT COUNT(*) FROM #Empresas) = EO.ordem % (SELECT COUNT(*) FROM #Empresas) 
                            THEN 0 ELSE 1 
                        END,
                        EO.ordem
                ) AS EO;

                -- 4. Inserir todos os contratos na tabela de distribuição
                INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
                (DT_REFERENCIA, ID_EDITAL, ID_PERIODO, fkContratoSISCTR, 
                COD_EMPRESA_COBRANCA, COD_CRITERIO_SELECAO, NR_CPF_CNPJ, 
                VR_SD_DEVEDOR, CREATED_AT)
                SELECT 
                    @dt_referencia,
                    :edital_id,
                    :periodo_id,
                    A.FkContratoSISCTR,
                    D.ID_EMPRESA,
                    :cod_criterio,
                    A.NR_CPF_CNPJ,
                    A.VR_SD_DEVEDOR,
                    GETDATE()
                FROM [DEV].[DCA_TB007_ARRASTAVEIS] A
                JOIN #DistribuicaoCPFs D ON A.NR_CPF_CNPJ = D.NR_CPF_CNPJ;

                -- 5. Limpar tabelas temporárias
                DROP TABLE IF EXISTS #Empresas;
                DROP TABLE IF EXISTS #CPFs;
                DROP TABLE IF EXISTS #DistribuicaoCPFs;
                """)

                connection.execute(distribuir_arrastados_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "cod_criterio": cod_criterio
                })

            # 5. Distribuir contratos restantes
            restantes_count = connection.execute(
                text("SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]")
            ).scalar() or 0

            resultados["contratos_restantes"] = restantes_count

            if restantes_count > 0:
                # Implementar a lógica de distribuição dos contratos restantes
                distribuir_restantes_sql = text("""
                -- Script para distribuir contratos restantes
                DECLARE @dt_referencia DATE = GETDATE();

                -- 1. Obter empresas e seus limites
                IF OBJECT_ID('tempdb..#EmpresasLimites') IS NOT NULL
                    DROP TABLE #EmpresasLimites;

                SELECT 
                    LD.ID_EMPRESA,
                    LD.PERCENTUAL_FINAL,
                    LD.QTDE_MAXIMA,
                    (SELECT COUNT(*) FROM [DEV].[DCA_TB005_DISTRIBUICAO] 
                     WHERE COD_EMPRESA_COBRANCA = LD.ID_EMPRESA
                     AND ID_EDITAL = :edital_id
                     AND ID_PERIODO = :periodo_id
                     AND COD_CRITERIO_SELECAO = :cod_criterio) AS contratos_atuais,
                    0 AS contratos_restantes
                INTO #EmpresasLimites
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
                WHERE LD.ID_EDITAL = :edital_id
                AND LD.ID_PERIODO = :periodo_id
                AND LD.COD_CRITERIO_SELECAO = :cod_criterio;

                -- 2. Calcular quantos contratos restam para cada empresa
                UPDATE #EmpresasLimites
                SET contratos_restantes = 
                    CASE 
                        WHEN QTDE_MAXIMA > contratos_atuais 
                        THEN QTDE_MAXIMA - contratos_atuais 
                        ELSE 0 
                    END;

                -- 3. Preparar contratos restantes em ordem aleatória
                IF OBJECT_ID('tempdb..#ContratosRestantes') IS NOT NULL
                    DROP TABLE #ContratosRestantes;

                SELECT 
                    FkContratoSISCTR,
                    NR_CPF_CNPJ,
                    VR_SD_DEVEDOR,
                    ROW_NUMBER() OVER (ORDER BY NEWID()) AS ordem
                INTO #ContratosRestantes
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS];

                -- 4. Distribuir contratos entre empresas
                IF OBJECT_ID('tempdb..#DistribuicaoFinal') IS NOT NULL
                    DROP TABLE #DistribuicaoFinal;

                WITH EmpresasFaixas AS (
                    SELECT 
                        ID_EMPRESA,
                        contratos_restantes,
                        SUM(contratos_restantes) OVER (ORDER BY PERCENTUAL_FINAL DESC 
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - contratos_restantes + 1 AS inicio_faixa,
                        SUM(contratos_restantes) OVER (ORDER BY PERCENTUAL_FINAL DESC 
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fim_faixa
                    FROM #EmpresasLimites
                    WHERE contratos_restantes > 0
                )
                SELECT 
                    CR.FkContratoSISCTR,
                    CR.NR_CPF_CNPJ,
                    CR.VR_SD_DEVEDOR,
                    EF.ID_EMPRESA
                INTO #DistribuicaoFinal
                FROM #ContratosRestantes CR
                CROSS APPLY (
                    SELECT TOP 1 ID_EMPRESA
                    FROM EmpresasFaixas EF 
                    WHERE CR.ordem BETWEEN EF.inicio_faixa AND EF.fim_faixa
                    ORDER BY EF.ID_EMPRESA
                ) AS EF
                WHERE CR.ordem <= (SELECT SUM(contratos_restantes) FROM #EmpresasLimites);

                -- 5. Inserir na tabela de distribuição
                INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
                (DT_REFERENCIA, ID_EDITAL, ID_PERIODO, fkContratoSISCTR, 
                COD_EMPRESA_COBRANCA, COD_CRITERIO_SELECAO, NR_CPF_CNPJ, 
                VR_SD_DEVEDOR, CREATED_AT)
                SELECT 
                    @dt_referencia,
                    :edital_id, 
                    :periodo_id,
                    DF.FkContratoSISCTR,
                    DF.ID_EMPRESA,
                    :cod_criterio,
                    DF.NR_CPF_CNPJ,
                    DF.VR_SD_DEVEDOR,
                    GETDATE()
                FROM #DistribuicaoFinal DF;

                -- 6. Limpar tabelas temporárias
                DROP TABLE IF EXISTS #EmpresasLimites;
                DROP TABLE IF EXISTS #ContratosRestantes;
                DROP TABLE IF EXISTS #DistribuicaoFinal;
                """)

                connection.execute(distribuir_restantes_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "cod_criterio": cod_criterio
                })

            # 6. Contar total de contratos redistribuídos
            total_redistribuidos = connection.execute(
                text("""
                SELECT COUNT(*) 
                FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND COD_CRITERIO_SELECAO = :cod_criterio
                """),
                {"edital_id": edital_id, "periodo_id": periodo_id, "cod_criterio": cod_criterio}
            ).scalar() or 0

            # 7. Obter número de empresas
            total_empresas = connection.execute(
                text("""
                SELECT COUNT(DISTINCT COD_EMPRESA_COBRANCA)
                FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND COD_CRITERIO_SELECAO = :cod_criterio
                """),
                {"edital_id": edital_id, "periodo_id": periodo_id, "cod_criterio": cod_criterio}
            ).scalar() or 0

            # 8. Atualizar resultados
            resultados["contratos_redistribuidos"] = total_redistribuidos
            resultados["total_empresas"] = total_empresas
            resultados["success"] = True

        return resultados

    except Exception as e:
        logging.error(f"Erro ao processar redistribuição: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return resultados