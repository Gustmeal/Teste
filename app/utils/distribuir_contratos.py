from app import db
from sqlalchemy import text
import logging


def selecionar_contratos_distribuiveis():
    """
    Seleciona o universo de contratos que serão distribuídos e os armazena na tabela DCA_TB006_DISTRIBUIVEIS.
    Usa as tabelas do Banco de Dados Gerencial (BDG).

    NOVA LÓGICA: Filtra apenas contratos que existem na tabela TEMP_DISTRIBUICAO_SERASA_ASSESSORIA
    com ONDE = 'ASSESSORIA' e SIT_ESPECIAL IS NULL.
    """
    try:
        # Usar uma conexão direta para executar o SQL
        with db.engine.connect() as connection:
            try:
                # Primeiro, limpar COMPLETAMENTE a tabela de distribuíveis
                logging.info("Limpando tabela de distribuíveis...")
                truncate_sql = text("TRUNCATE TABLE [BDG].[DCA_TB006_DISTRIBUIVEIS]")
                connection.execute(truncate_sql)
                logging.info("Tabela de distribuíveis limpa com sucesso")

                # Limpar TAMBÉM a tabela de arrastaveis, que pode conter dados de processamentos anteriores
                truncate_arrastaveis_sql = text("TRUNCATE TABLE [BDG].[DCA_TB007_ARRASTAVEIS]")
                connection.execute(truncate_arrastaveis_sql)
                logging.info("Tabela de arrastaveis limpa com sucesso")

                # Verificar se a limpeza foi bem-sucedida
                check_sql = text("SELECT COUNT(*) FROM [BDG].[DCA_TB006_DISTRIBUIVEIS]")
                count_after_truncate = connection.execute(check_sql).scalar()
                logging.info(f"Contagem após limpeza: {count_after_truncate}")

                # NOVA LÓGICA: Inserir apenas contratos que passam pelo filtro da tabela TEMP_DISTRIBUICAO_SERASA_ASSESSORIA
                logging.info(
                    "Selecionando contratos distribuíveis com novo filtro (TEMP_DISTRIBUICAO_SERASA_ASSESSORIA)...")
                insert_sql = text("""
                    INSERT INTO [BDG].[DCA_TB006_DISTRIBUIVEIS]
                    SELECT 
                        ECA.fkContratoSISCTR,
                        CON.NR_CPF_CNPJ,
                        SIT.VR_SD_DEVEDOR,
                        CREATED_AT = GETDATE(),
                        UPDATED_AT = NULL,
                        DELETED_AT = NULL
                    FROM
                        [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                        INNER JOIN [BDG].[COM_TB001_CONTRATO] AS CON
                            ON ECA.fkContratoSISCTR = CON.fkContratoSISCTR
                        INNER JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] AS SIT
                            ON ECA.fkContratoSISCTR = SIT.fkContratoSISCTR
                        LEFT JOIN [BDG].[COM_TB013_SUSPENSO_DECISAO_JUDICIAL] AS SDJ
                            ON ECA.fkContratoSISCTR = SDJ.fkContratoSISCTR
                        -- NOVO FILTRO: Apenas contratos que estão na tabela TEMP_DISTRIBUICAO_SERASA_ASSESSORIA
                        INNER JOIN [BDDASHBOARDBI].[BDG].[TEMP_DISTRIBUICAO_SERASA_ASSESSORIA] AS TEMP
                            ON ECA.fkContratoSISCTR = TEMP.fkContratoSISCTR
                    WHERE
                        SIT.[fkSituacaoCredito] = 1  -- Contratos ativos
                        AND SDJ.fkContratoSISCTR IS NULL  -- Sem suspensão judicial
                        -- FILTROS DA TABELA TEMP_DISTRIBUICAO_SERASA_ASSESSORIA
                        AND TEMP.ONDE = 'ASSESSORIA'
                        AND TEMP.SIT_ESPECIAL IS NULL
                """)

                # Contar contratos nas tabelas de origem antes da inserção (para debug)
                origem_count_sql = text("""
                    SELECT COUNT(*) 
                    FROM [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                        INNER JOIN [BDG].[COM_TB001_CONTRATO] AS CON
                            ON ECA.fkContratoSISCTR = CON.fkContratoSISCTR
                        INNER JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] AS SIT
                            ON ECA.fkContratoSISCTR = SIT.fkContratoSISCTR
                        LEFT JOIN [BDG].[COM_TB013_SUSPENSO_DECISAO_JUDICIAL] AS SDJ
                            ON ECA.fkContratoSISCTR = SDJ.fkContratoSISCTR
                        INNER JOIN [BDDASHBOARDBI].[BDG].[TEMP_DISTRIBUICAO_SERASA_ASSESSORIA] AS TEMP
                            ON ECA.fkContratoSISCTR = TEMP.fkContratoSISCTR
                    WHERE
                        SIT.[fkSituacaoCredito] = 1
                        AND SDJ.fkContratoSISCTR IS NULL
                        AND TEMP.ONDE = 'ASSESSORIA'
                        AND TEMP.SIT_ESPECIAL IS NULL
                """)
                origem_count = connection.execute(origem_count_sql).scalar()
                logging.info(f"Contratos elegíveis com novo filtro nas tabelas de origem: {origem_count}")

                result = connection.execute(insert_sql)
                logging.info(f"Inserção concluída - linhas afetadas: {result.rowcount}")

                # Contar quantos contratos foram selecionados
                logging.info("Contando contratos selecionados...")
                count_sql = text("SELECT COUNT(*) FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] WHERE DELETED_AT IS NULL")
                result = connection.execute(count_sql)
                num_contratos = result.scalar()

                logging.info(f"Total de contratos selecionados com novo filtro: {num_contratos}")
                return num_contratos

            except Exception as e:
                logging.error(f"Erro durante a execução das consultas SQL: {str(e)}")
                # Log mais detalhado caso ocorra erro
                import traceback
                logging.error(traceback.format_exc())
                raise

    except Exception as e:
        logging.error(f"Erro na seleção de contratos: {str(e)}")
        return 0


def distribuir_acordos_vigentes_empresas_permanece(edital_id, periodo_id):
    """
    Distribui contratos com acordos vigentes para empresas que permanecem.
    Implementa o item 1.1.1 dos requisitos.

    Args:
        edital_id: ID do edital a ser processado
        periodo_id: ID do período a ser processado

    Returns:
        int: Total de contratos distribuídos
    """
    contratos_distribuidos = 0

    try:
        print(
            f"Iniciando distribuição de acordos vigentes para empresas que permanecem - Edital: {edital_id}, Período: {periodo_id}")

        # 1. Limpar a tabela de distribuição apenas para este edital/período
        db.session.execute(text(
            "DELETE FROM [BDG].[DCA_TB005_DISTRIBUICAO] WHERE ID_EDITAL = :edital_id AND ID_PERIODO = :periodo_id"),
            {"edital_id": edital_id, "periodo_id": periodo_id})
        print("Tabela DCA_TB005_DISTRIBUICAO limpa para este edital/período")

        # 2. Inserir contratos com acordos vigentes para empresas que permanecem
        # IMPORTANTE: usar EXISTS para controle de duplicatas
        resultado_acordos = db.session.execute(
            text("""
                 INSERT INTO [BDG].[DCA_TB005_DISTRIBUICAO]
                 ([DT_REFERENCIA], [ID_EDITAL], [ID_PERIODO], [fkContratoSISCTR],
                     [COD_EMPRESA_COBRANCA], [COD_CRITERIO_SELECAO], [NR_CPF_CNPJ],
                     [VR_SD_DEVEDOR], [CREATED_AT])

                 SELECT GETDATE() AS [DT_REFERENCIA],
                    :edital_id,
                    :periodo_id,
                    DIS.[FkContratoSISCTR],
                    EMP.ID_EMPRESA AS COD_EMPRESA_COBRANCA,
                    1 AS COD_CRITERIO_SELECAO, -- Contrato com acordo para assessoria que permanece
                    DIS.[NR_CPF_CNPJ],
                    DIS.[VR_SD_DEVEDOR],
                    GETDATE() AS [CREATED_AT]
                 FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] AS DIS
                     INNER JOIN [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                 ON DIS.[FkContratoSISCTR] = ECA.fkContratoSISCTR
                     INNER JOIN [BDG].[COM_TB009_ACORDOS_LIQUIDADOS_VIGENTES] AS ALV
                     ON DIS.[FkContratoSISCTR] = ALV.fkContratoSISCTR
                     INNER JOIN [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES] AS EMP
                     ON ECA.COD_EMPRESA_COBRANCA = EMP.ID_EMPRESA
                 WHERE EMP.ID_EDITAL = :edital_id
                   AND EMP.ID_PERIODO = :periodo_id
                   AND EMP.DS_CONDICAO = 'PERMANECE'
                   AND ALV.fkEstadoAcordo = 1
                 -- Garantir que não haja duplicatas
                   AND NOT EXISTS (
                     SELECT 1 FROM [BDG].[DCA_TB005_DISTRIBUICAO] D
                     WHERE D.fkContratoSISCTR = DIS.[FkContratoSISCTR]
                   AND D.ID_EDITAL = :edital_id
                   AND D.ID_PERIODO = :periodo_id
                     )
                 """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        )
        contratos_distribuidos = resultado_acordos.rowcount
        print(f"{contratos_distribuidos} contratos com acordo distribuídos para empresas que permanecem")

        # 3. Remover os contratos inseridos da tabela de distribuíveis - usando DELETE otimizado
        if contratos_distribuidos > 0:
            db.session.execute(
                text("""
                    DELETE DIS
                    FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] AS DIS
                    WHERE DIS.[FkContratoSISCTR] IN (
                        SELECT DIST.[fkContratoSISCTR]
                        FROM [BDG].[DCA_TB005_DISTRIBUICAO] AS DIST 
                        WHERE DIST.ID_EDITAL = :edital_id
                        AND DIST.ID_PERIODO = :periodo_id
                        AND DIST.COD_CRITERIO_SELECAO = 1
                    )
                """),
                {"edital_id": edital_id, "periodo_id": periodo_id}
            )

        db.session.commit()
        print("Processamento de acordos vigentes para empresas que permanecem concluído com sucesso")

    except Exception as e:
        db.session.rollback()
        print(f"Erro ao distribuir contratos com acordo para empresas que permanecem: {str(e)}")
        import traceback
        print(traceback.format_exc())

    return contratos_distribuidos


def distribuir_acordos_vigentes_empresas_descredenciadas(edital_id, periodo_id):
    """
    [VERSÃO CORRIGIDA]
    Distribui contratos com acordos vigentes de empresas descredenciadas, agrupando por CPF
    para respeitar a regra de arrasto. Implementa o item 1.1.2 dos requisitos.

    Args:
        edital_id: ID do edital a ser processado
        periodo_id: ID do período a ser processado

    Returns:
        int: Total de contratos distribuídos
    """
    ### ALTERAÇÕES REALIZADAS ###
    # 1. A lógica foi migrada para um script T-SQL único e otimizado.
    # 2. A distribuição agora é baseada em CPFs únicos, não em contratos individuais.
    #    Isso garante que todos os contratos de um mesmo CPF sejam atribuídos à mesma empresa.
    # 3. O código de critério (3) foi mantido, pois está correto para esta regra.
    ############################

    contratos_distribuidos = 0
    print(
        f"Iniciando distribuição (AGRUPADA POR CPF) de acordos de empresas descredenciadas - Edital: {edital_id}, Período: {periodo_id}")

    try:
        sql_script = text("""
            SET NOCOUNT ON;

            -- Temp tables para evitar recriação
            IF OBJECT_ID('tempdb..#CPFsParaDistribuir') IS NOT NULL DROP TABLE #CPFsParaDistribuir;
            IF OBJECT_ID('tempdb..#EmpresasReceptoras') IS NOT NULL DROP TABLE #EmpresasReceptoras;
            IF OBJECT_ID('tempdb..#CpfEmpresaMap') IS NOT NULL DROP TABLE #CpfEmpresaMap;

            -- ETAPA 1: Identificar os CPFs únicos que precisam ser redistribuídos.
            -- Estes são CPFs de contratos com acordos vigentes em empresas descredenciadas.
            SELECT
                DIS.NR_CPF_CNPJ,
                ROW_NUMBER() OVER (ORDER BY NEWID()) as CpfRowNumber -- Ordem aleatória para distribuição justa
            INTO #CPFsParaDistribuir
            FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] DIS
            INNER JOIN [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] ECA ON DIS.FkContratoSISCTR = ECA.fkContratoSISCTR
            INNER JOIN [BDG].[COM_TB009_ACORDOS_LIQUIDADOS_VIGENTES] ALV ON DIS.FkContratoSISCTR = ALV.fkContratoSISCTR
            INNER JOIN [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES] EMP ON ECA.COD_EMPRESA_COBRANCA = EMP.ID_EMPRESA
            WHERE ALV.fkEstadoAcordo = 1
              AND EMP.DS_CONDICAO = 'DESCREDENCIADA'
              AND EMP.ID_EDITAL = :edital_id
              AND EMP.ID_PERIODO = :periodo_id
              AND NOT EXISTS (
                  SELECT 1 FROM [BDG].[DCA_TB005_DISTRIBUICAO] DIST
                  WHERE DIST.NR_CPF_CNPJ = DIS.NR_CPF_CNPJ
                    AND DIST.ID_EDITAL = :edital_id
                    AND DIST.ID_PERIODO = :periodo_id
              )
            GROUP BY DIS.NR_CPF_CNPJ;

            DECLARE @TotalCPFs INT;
            SELECT @TotalCPFs = COUNT(*) FROM #CPFsParaDistribuir;

            IF @TotalCPFs = 0 BEGIN
                SELECT 0 AS ContratosDistribuidos;
                RETURN;
            END;

            -- ETAPA 2: Obter as empresas que podem receber os contratos.
            SELECT
                ID_EMPRESA,
                ROW_NUMBER() OVER (ORDER BY ID_EMPRESA) as EmpresaRowNumber
            INTO #EmpresasReceptoras
            FROM [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES]
            WHERE ID_EDITAL = :edital_id
              AND ID_PERIODO = :periodo_id
              AND DS_CONDICAO <> 'DESCREDENCIADA';

            DECLARE @TotalEmpresas INT;
            SELECT @TotalEmpresas = COUNT(*) FROM #EmpresasReceptoras;

            IF @TotalEmpresas = 0 BEGIN
                SELECT 0 AS ContratosDistribuidos;
                RETURN;
            END;

            -- ETAPA 3: Mapear cada CPF para uma empresa receptora (distribuição round-robin).
            SELECT
                C.NR_CPF_CNPJ,
                E.ID_EMPRESA
            INTO #CpfEmpresaMap
            FROM #CPFsParaDistribuir C
            JOIN #EmpresasReceptoras E ON (C.CpfRowNumber - 1) % @TotalEmpresas + 1 = E.EmpresaRowNumber;

            -- ETAPA 4: Inserir todos os contratos, usando o mapa CPF -> Empresa.
            INSERT INTO [BDG].[DCA_TB005_DISTRIBUICAO]
                   ([DT_REFERENCIA], [ID_EDITAL], [ID_PERIODO], [fkContratoSISCTR],
                    [COD_EMPRESA_COBRANCA], [COD_CRITERIO_SELECAO], [NR_CPF_CNPJ],
                    [VR_SD_DEVEDOR], [CREATED_AT])
            SELECT GETDATE(), :edital_id, :periodo_id, D.FkContratoSISCTR,
                   MAP.ID_EMPRESA,
                   3, -- Código 3: Contrato com acordo com assessoria descredenciada
                   D.NR_CPF_CNPJ, D.VR_SD_DEVEDOR, GETDATE()
            FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] D
            JOIN #CpfEmpresaMap MAP ON D.NR_CPF_CNPJ = MAP.NR_CPF_CNPJ;

            DECLARE @ContratosInseridos INT = @@ROWCOUNT;

            -- ETAPA 5: Remover os CPFs processados da tabela de distribuíveis.
            DELETE D
            FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] D
            WHERE EXISTS (SELECT 1 FROM #CpfEmpresaMap MAP WHERE MAP.NR_CPF_CNPJ = D.NR_CPF_CNPJ);

            -- Limpeza
            DROP TABLE #CPFsParaDistribuir;
            DROP TABLE #EmpresasReceptoras;
            DROP TABLE #CpfEmpresaMap;

            SELECT @ContratosInseridos AS ContratosDistribuidos;
        """)

        with db.engine.begin() as connection:
            result = connection.execute(sql_script, {"edital_id": edital_id, "periodo_id": periodo_id})
            contratos_distribuidos = result.scalar_one_or_none() or 0

        print(
            f"Processo de distribuição de descredenciadas (agrupado por CPF) concluído: {contratos_distribuidos} contratos distribuídos.")

    except Exception as e:
        db.session.rollback()
        print(f"Erro ao distribuir contratos de empresas descredenciadas: {str(e)}")
        import traceback
        print(traceback.format_exc())

    return contratos_distribuidos


def aplicar_regra_arrasto_acordos(edital_id, periodo_id):
    """
    Aplica a regra de arrasto com acordo para distribuição de contratos.
    CORREÇÃO: Só arrasta contratos de CPFs que ainda têm contratos na tabela de distribuíveis.
    """
    from app import db
    from sqlalchemy import text
    import time, logging

    contratos_inseridos = 0
    start_time = time.time()

    try:
        print(f"Iniciando distribuição pela regra de arrasto com acordo - Edital: {edital_id}, Período: {periodo_id}")

        # CORREÇÃO: Buscar apenas CPFs com acordos que ainda têm contratos para distribuir
        acordos_count = db.session.execute(
            text("""
                 SELECT COUNT(DISTINCT D.NR_CPF_CNPJ)
                 FROM [BDG].[DCA_TB005_DISTRIBUICAO] D
                 WHERE D.ID_EDITAL = :edital
                   AND D.ID_PERIODO = :periodo
                   AND D.COD_CRITERIO_SELECAO IN (1
                     , 3)
                 -- CORREÇÃO: Verificar se ainda existem contratos deste CPF na tabela de distribuíveis
                   AND EXISTS (
                     SELECT 1
                     FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] DIS
                     WHERE DIS.NR_CPF_CNPJ = D.NR_CPF_CNPJ
                     )
                 """),
            {"edital": edital_id, "periodo": periodo_id}
        ).scalar()

        if not acordos_count:
            print("Nenhum CPF/CNPJ com acordo e contratos pendentes foi encontrado.")
            return 0

        print(f"Total de CPFs/CNPJs com acordo e contratos pendentes: {acordos_count}")

        # CORREÇÃO: Buscar CPFs/CNPJs com acordos que ainda têm contratos para distribuir
        cpfs_acordos = db.session.execute(
            text("""
                 SELECT DISTINCT D.NR_CPF_CNPJ, D.COD_EMPRESA_COBRANCA
                 FROM [BDG].[DCA_TB005_DISTRIBUICAO] D
                 WHERE D.ID_EDITAL = :edital
                   AND D.ID_PERIODO = :periodo
                   AND D.COD_CRITERIO_SELECAO IN (1
                     , 3)
                 -- CORREÇÃO: Verificar se ainda existem contratos deste CPF na tabela de distribuíveis
                   AND EXISTS (
                     SELECT 1
                     FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] DIS
                     WHERE DIS.NR_CPF_CNPJ = D.NR_CPF_CNPJ
                     )
                 """),
            {"edital": edital_id, "periodo": periodo_id}
        ).fetchall()

        # 2. Para cada CPF, buscar e processar seus contratos
        for acordo in cpfs_acordos:
            cpf_cnpj = acordo[0]
            empresa_cobranca = acordo[1]

            # Buscar contratos distribuíveis para este CPF/CNPJ
            contratos = db.session.execute(
                text("""
                     SELECT FkContratoSISCTR, VR_SD_DEVEDOR
                     FROM [BDG].[DCA_TB006_DISTRIBUIVEIS]
                     WHERE NR_CPF_CNPJ = :cpf_cnpj
                     """),
                {"cpf_cnpj": cpf_cnpj}
            ).fetchall()

            # Processar cada contrato
            for contrato in contratos:
                contrato_id = contrato[0]
                valor_sd = contrato[1]

                # Verificar se já foi distribuído
                ja_distribuido = db.session.execute(
                    text("""
                         SELECT COUNT(*)
                         FROM [BDG].[DCA_TB005_DISTRIBUICAO]
                         WHERE fkContratoSISCTR = :contrato_id
                           AND ID_EDITAL = :edital_id
                           AND ID_PERIODO = :periodo_id
                         """),
                    {"contrato_id": contrato_id, "edital_id": edital_id, "periodo_id": periodo_id}
                ).scalar()

                if ja_distribuido == 0:  # Se não foi distribuído
                    # Inserir na distribuição
                    db.session.execute(
                        text("""
                             INSERT INTO [BDG].[DCA_TB005_DISTRIBUICAO]
                             ([DT_REFERENCIA], [ID_EDITAL], [ID_PERIODO], [fkContratoSISCTR],
                                 [COD_EMPRESA_COBRANCA], [COD_CRITERIO_SELECAO], [NR_CPF_CNPJ],
                                 [VR_SD_DEVEDOR], [CREATED_AT])
                             VALUES (
                                 GETDATE(), :edital_id, :periodo_id, :contrato_id, :empresa_cobranca, 6, -- Código 6: Regra de Arrasto
                                 :cpf_cnpj, :valor_sd, GETDATE()
                                 )
                             """),
                        {
                            "edital_id": edital_id,
                            "periodo_id": periodo_id,
                            "contrato_id": contrato_id,
                            "empresa_cobranca": empresa_cobranca,
                            "cpf_cnpj": cpf_cnpj,
                            "valor_sd": valor_sd
                        }
                    )

                    # Remover da tabela de distribuíveis
                    db.session.execute(
                        text("""
                             DELETE
                             FROM [BDG].[DCA_TB006_DISTRIBUIVEIS]
                             WHERE FkContratoSISCTR = :contrato_id
                             """),
                        {"contrato_id": contrato_id}
                    )

                    contratos_inseridos += 1

            # Commit após processar todos os contratos do CPF
            db.session.commit()

        elapsed_time = time.time() - start_time
        print(
            f"Regra de arrasto com acordo concluída: {contratos_inseridos} contratos distribuídos em {elapsed_time:.2f}s")

        return contratos_inseridos

    except Exception as e:
        db.session.rollback()
        print(f"Erro na regra de arrasto com acordo: {str(e)}")
        import traceback
        print(traceback.format_exc())

        return 0


def aplicar_regra_arrasto_sem_acordo(edital_id, periodo_id):
    """
    [VERSÃO CORRIGIDA]
    Aplica a regra de arrasto para múltiplos contratos sem acordo de forma OTIMIZADA e PROPORCIONAL,
    distribuindo os CPFs de acordo com o percentual de participação de cada empresa.
    Só arrasta CPFs que não têm nenhum contrato distribuído ainda.
    """
    ### ALTERAÇÕES REALIZADAS ###
    # 1. Corrigido o COD_CRITERIO_SELECAO de 3 para 6.
    #    Agora ele usa corretamente o código para "Regra do arrasto".
    ############################

    print(f"Iniciando distribuição OTIMIZADA e PROPORCIONAL (sem acordo) - regra de arrasto")

    try:
        with db.engine.begin() as connection:
            sql_script = text("""
                SET NOCOUNT ON;

                -- ETAPA 1: Identificar e numerar aleatoriamente os CPFs que se enquadram na regra.
                IF OBJECT_ID('tempdb..#CPFsParaArrasto') IS NOT NULL DROP TABLE #CPFsParaArrasto;

                SELECT
                    D.NR_CPF_CNPJ,
                    ROW_NUMBER() OVER (ORDER BY NEWID()) as CpfRowNumber
                INTO #CPFsParaArrasto
                FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] D
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM [BDG].[DCA_TB005_DISTRIBUICAO] DIST
                    WHERE DIST.NR_CPF_CNPJ = D.NR_CPF_CNPJ
                    AND DIST.ID_EDITAL = :edital_id
                    AND DIST.ID_PERIODO = :periodo_id
                )
                AND D.NR_CPF_CNPJ IN (
                    SELECT NR_CPF_CNPJ
                    FROM [BDG].[DCA_TB006_DISTRIBUIVEIS]
                    GROUP BY NR_CPF_CNPJ
                    HAVING COUNT(*) > 1
                )
                GROUP BY D.NR_CPF_CNPJ;

                DECLARE @TotalCPFsParaArrastar INT;
                SELECT @TotalCPFsParaArrastar = COUNT(*) FROM #CPFsParaArrasto;

                IF @TotalCPFsParaArrastar = 0 BEGIN
                    SELECT 0 AS ContratosInseridos;
                    RETURN;
                END;

                -- ETAPA 2: Calcular a cota de CPFs para cada empresa com base no seu percentual.
                IF OBJECT_ID('tempdb..#EmpresasInfo') IS NOT NULL DROP TABLE #EmpresasInfo;

                SELECT
                    EP.ID_EMPRESA,
                    LD.PERCENTUAL_FINAL AS percentual,
                    (@TotalCPFsParaArrastar * LD.PERCENTUAL_FINAL / 100.0) AS meta_exata,
                    FLOOR(@TotalCPFsParaArrastar * LD.PERCENTUAL_FINAL / 100.0) AS meta_inteira,
                    (@TotalCPFsParaArrastar * LD.PERCENTUAL_FINAL / 100.0) - FLOOR(@TotalCPFsParaArrastar * LD.PERCENTUAL_FINAL / 100.0) AS parte_fracionaria
                INTO #EmpresasInfo
                FROM [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES] EP
                JOIN [BDG].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
                    ON EP.ID_EMPRESA = LD.ID_EMPRESA
                    AND EP.ID_EDITAL = LD.ID_EDITAL
                    AND EP.ID_PERIODO = LD.ID_PERIODO
                WHERE EP.ID_EDITAL = :edital_id
                AND EP.ID_PERIODO = :periodo_id
                AND EP.DS_CONDICAO <> 'DESCREDENCIADA'
                AND LD.PERCENTUAL_FINAL > 0;

                IF NOT EXISTS (SELECT 1 FROM #EmpresasInfo) BEGIN
                    SELECT 0 AS ContratosInseridos;
                    RETURN;
                END;

                -- ETAPA 3: Calcular a cota final, distribuindo os CPFs "extras" para as maiores frações.
                DECLARE @TotalMetaInteira INT;
                SELECT @TotalMetaInteira = SUM(meta_inteira) FROM #EmpresasInfo;

                DECLARE @ContratosExtras INT;
                SET @ContratosExtras = @TotalCPFsParaArrastar - @TotalMetaInteira;

                ALTER TABLE #EmpresasInfo ADD total_a_receber INT;

                WITH RankingFracao AS (
                    SELECT ID_EMPRESA, ROW_NUMBER() OVER (ORDER BY parte_fracionaria DESC, ID_EMPRESA) as rn
                    FROM #EmpresasInfo
                )
                UPDATE #EmpresasInfo
                SET total_a_receber = meta_inteira +
                    CASE WHEN RF.rn <= @ContratosExtras THEN 1 ELSE 0 END
                FROM #EmpresasInfo EI
                JOIN RankingFracao RF ON EI.ID_EMPRESA = RF.ID_EMPRESA;

                -- ETAPA 4: Criar as "faixas de atribuição" para cada empresa.
                IF OBJECT_ID('tempdb..#FaixasDeAtribuicao') IS NOT NULL DROP TABLE #FaixasDeAtribuicao;

                SELECT
                    ID_EMPRESA,
                    total_a_receber,
                    ISNULL(SUM(total_a_receber) OVER (ORDER BY ID_EMPRESA ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) + 1 as inicio_faixa,
                    SUM(total_a_receber) OVER (ORDER BY ID_EMPRESA) as fim_faixa
                INTO #FaixasDeAtribuicao
                FROM #EmpresasInfo;

                -- ETAPA 5: Criar o mapa final (CPF -> Empresa) juntando os CPFs numerados com suas faixas.
                IF OBJECT_ID('tempdb..#ArrastoMapping') IS NOT NULL DROP TABLE #ArrastoMapping;

                SELECT
                    C.NR_CPF_CNPJ,
                    F.ID_EMPRESA
                INTO #ArrastoMapping
                FROM #CPFsParaArrasto C
                JOIN #FaixasDeAtribuicao F ON C.CpfRowNumber BETWEEN F.inicio_faixa AND F.fim_faixa;

                -- ETAPAS FINAIS: Inserir e Deletar em massa usando o mapa de atribuição.
                INSERT INTO [BDG].[DCA_TB005_DISTRIBUICAO]
                ([DT_REFERENCIA], [ID_EDITAL], [ID_PERIODO], [fkContratoSISCTR],
                 [COD_EMPRESA_COBRANCA], [COD_CRITERIO_SELECAO], [NR_CPF_CNPJ],
                 [VR_SD_DEVEDOR], [CREATED_AT])
                SELECT
                    GETDATE(), :edital_id, :periodo_id, D.[FkContratoSISCTR],
                    M.ID_EMPRESA,
                    6, -- CÓDIGO CORRIGIDO: 6 = Regra do arrasto
                    D.[NR_CPF_CNPJ], D.[VR_SD_DEVEDOR], GETDATE()
                FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] D
                JOIN #ArrastoMapping M ON D.NR_CPF_CNPJ = M.NR_CPF_CNPJ;

                DECLARE @ContratosInseridos INT = @@ROWCOUNT;

                DELETE D
                FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] D
                JOIN #ArrastoMapping M ON D.NR_CPF_CNPJ = M.NR_CPF_CNPJ;

                DROP TABLE #CPFsParaArrasto;
                DROP TABLE #EmpresasInfo;
                DROP TABLE #FaixasDeAtribuicao;
                DROP TABLE #ArrastoMapping;

                SELECT @ContratosInseridos AS ContratosInseridos;
            """)

            result = connection.execute(sql_script, {"edital_id": edital_id, "periodo_id": periodo_id})
            contratos_distribuidos = result.scalar_one_or_none() or 0

            print(f"Regra de arrasto PROPORCIONAL concluída: {contratos_distribuidos} contratos distribuídos")
            return contratos_distribuidos

    except Exception as e:
        print(f"Erro na regra de arrasto PROPORCIONAL: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return 0


def distribuir_demais_contratos(edital_id, periodo_id):
    """
    [VERSÃO CORRIGIDA - RESPEITA REGRA DE ARRASTO]
    Distribui os contratos restantes entre as empresas, AGRUPANDO POR CPF.
    Implementa o item 1.1.5 dos requisitos: Demais contratos sem acordo.
    """
    contratos_distribuidos = 0

    try:
        print(f"Iniciando distribuição dos demais contratos (AGRUPADO POR CPF) - Edital: {edital_id}, Período: {periodo_id}")

        # Verificar se existem empresas e contratos antes de iniciar o processamento
        empresas_count = db.session.execute(
            text("""
                 SELECT COUNT(*)
                 FROM [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES] EP
                     LEFT JOIN [BDG].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
                 ON EP.ID_EMPRESA = LD.ID_EMPRESA
                     AND EP.ID_EDITAL = LD.ID_EDITAL
                     AND EP.ID_PERIODO = LD.ID_PERIODO
                 WHERE EP.ID_EDITAL = :edital_id
                   AND EP.ID_PERIODO = :periodo_id
                   AND EP.DS_CONDICAO <> 'DESCREDENCIADA'
                   AND (LD.PERCENTUAL_FINAL > 0 OR LD.PERCENTUAL_FINAL IS NULL)
                 """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        ).scalar()

        if not empresas_count:
            print("Nenhuma empresa participante encontrada.")
            return 0

        # Executar o script SQL principal
        db.session.execute(
            text("""
            -- Declaração de variáveis
            DECLARE @contratos_distribuidos INT = 0;

            -- ETAPA 1: Identificar CPFs únicos restantes
            IF OBJECT_ID('tempdb..#CPFsRestantes') IS NOT NULL
                DROP TABLE #CPFsRestantes;

            SELECT DISTINCT 
                NR_CPF_CNPJ,
                COUNT(*) as qtd_contratos,
                SUM(VR_SD_DEVEDOR) as valor_total,
                ROW_NUMBER() OVER (ORDER BY NEWID()) as ordem_cpf
            INTO #CPFsRestantes
            FROM [BDG].[DCA_TB006_DISTRIBUIVEIS]
            GROUP BY NR_CPF_CNPJ;

            DECLARE @total_cpfs INT;
            SELECT @total_cpfs = COUNT(*) FROM #CPFsRestantes;

            IF @total_cpfs = 0
            BEGIN
                SELECT 0 AS contratos_distribuidos;
                RETURN;
            END

            -- ETAPA 2: Buscar empresas e seus percentuais + contratos atuais
            IF OBJECT_ID('tempdb..#EmpresasInfo') IS NOT NULL
                DROP TABLE #EmpresasInfo;

            SELECT 
                EP.ID_EMPRESA,
                COALESCE(LD.PERCENTUAL_FINAL, 0) AS percentual,
                COALESCE((
                    SELECT COUNT(DISTINCT NR_CPF_CNPJ) 
                    FROM [BDG].[DCA_TB005_DISTRIBUICAO] D
                    WHERE D.ID_EDITAL = EP.ID_EDITAL
                    AND D.ID_PERIODO = EP.ID_PERIODO
                    AND D.COD_EMPRESA_COBRANCA = EP.ID_EMPRESA
                ), 0) AS cpfs_atuais,
                0 AS meta_cpfs_exata,
                0 AS meta_cpfs_inteira,
                0 AS cpfs_faltantes,
                0 AS parte_fracionaria,
                0 AS cpfs_extra,
                0 AS total_cpfs_a_receber,
                ROW_NUMBER() OVER (ORDER BY COALESCE(LD.PERCENTUAL_FINAL, 0) DESC) AS ranking
            INTO #EmpresasInfo
            FROM [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES] EP
            LEFT JOIN [BDG].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
                ON EP.ID_EMPRESA = LD.ID_EMPRESA
                AND EP.ID_EDITAL = LD.ID_EDITAL
                AND EP.ID_PERIODO = LD.ID_PERIODO
            WHERE EP.ID_EDITAL = :edital_id
            AND EP.ID_PERIODO = :periodo_id
            AND EP.DS_CONDICAO <> 'DESCREDENCIADA';

            -- ETAPA 3: Normalizar percentuais
            DECLARE @total_percentual DECIMAL(10,6);
            DECLARE @total_empresas INT;

            SELECT 
                @total_percentual = SUM(percentual),
                @total_empresas = COUNT(*)
            FROM #EmpresasInfo;

            IF @total_percentual <= 0
            BEGIN
                UPDATE #EmpresasInfo
                SET percentual = 100.0 / @total_empresas;
            END
            ELSE IF ABS(@total_percentual - 100) > 0.01
            BEGIN
                UPDATE #EmpresasInfo
                SET percentual = percentual * 100.0 / @total_percentual;
            END;

            -- ETAPA 4: Calcular quantos CPFs cada empresa deve receber
            DECLARE @total_cpfs_atuais INT;
            DECLARE @total_cpfs_final INT;

            SELECT @total_cpfs_atuais = SUM(cpfs_atuais)
            FROM #EmpresasInfo;

            SET @total_cpfs_final = @total_cpfs_atuais + @total_cpfs;

            -- Calcular metas para cada empresa
            UPDATE #EmpresasInfo
            SET
                meta_cpfs_exata = @total_cpfs_final * percentual / 100.0,
                meta_cpfs_inteira = FLOOR(@total_cpfs_final * percentual / 100.0),
                parte_fracionaria = @total_cpfs_final * percentual / 100.0 - FLOOR(@total_cpfs_final * percentual / 100.0);

            -- Calcular quantos CPFs faltam para cada empresa
            UPDATE #EmpresasInfo
            SET cpfs_faltantes = CASE
                                    WHEN meta_cpfs_inteira > cpfs_atuais THEN meta_cpfs_inteira - cpfs_atuais
                                    ELSE 0
                                  END;

            -- Verificar quantos CPFs faltam distribuir pelos fracionais
            DECLARE @total_faltantes INT;
            DECLARE @cpfs_nao_alocados INT;

            SELECT @total_faltantes = SUM(cpfs_faltantes)
            FROM #EmpresasInfo;

            SET @cpfs_nao_alocados = @total_cpfs - @total_faltantes;

            -- Distribuir CPFs extras por maiores fracionais
            IF @cpfs_nao_alocados > 0
            BEGIN
                WITH EmpresasOrdenadas AS (
                    SELECT 
                        ID_EMPRESA,
                        parte_fracionaria,
                        ROW_NUMBER() OVER (ORDER BY parte_fracionaria DESC) AS ranking_fracao
                    FROM #EmpresasInfo
                )
                UPDATE #EmpresasInfo
                SET cpfs_extra = CASE WHEN EO.ranking_fracao <= @cpfs_nao_alocados THEN 1 ELSE 0 END
                FROM #EmpresasInfo EI
                JOIN EmpresasOrdenadas EO ON EI.ID_EMPRESA = EO.ID_EMPRESA;
            END;

            -- Calcular total de CPFs a receber
            UPDATE #EmpresasInfo
            SET total_cpfs_a_receber = cpfs_faltantes + cpfs_extra;

            -- ETAPA 5: Criar tabela de atribuição CPF -> Empresa
            IF OBJECT_ID('tempdb..#AtribuicaoCPF') IS NOT NULL
                DROP TABLE #AtribuicaoCPF;

            CREATE TABLE #AtribuicaoCPF (
                NR_CPF_CNPJ BIGINT,
                ID_EMPRESA INT
            );

            -- Atribuir CPFs às empresas respeitando os limites calculados
            DECLARE @ordem_atual INT = 1;
            DECLARE @empresa_id INT, @cpfs_empresa INT;

            DECLARE cursor_empresas CURSOR FOR
            SELECT ID_EMPRESA, total_cpfs_a_receber
            FROM #EmpresasInfo
            WHERE total_cpfs_a_receber > 0
            ORDER BY NEWID(); -- Randomiza a ordem das empresas

            OPEN cursor_empresas;
            FETCH NEXT FROM cursor_empresas INTO @empresa_id, @cpfs_empresa;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                IF @cpfs_empresa > 0
                BEGIN
                    -- Inserir os próximos N CPFs para esta empresa
                    INSERT INTO #AtribuicaoCPF (NR_CPF_CNPJ, ID_EMPRESA)
                    SELECT TOP(@cpfs_empresa) 
                        NR_CPF_CNPJ, 
                        @empresa_id
                    FROM #CPFsRestantes
                    WHERE ordem_cpf >= @ordem_atual
                    ORDER BY ordem_cpf;

                    SET @ordem_atual = @ordem_atual + @cpfs_empresa;
                END

                FETCH NEXT FROM cursor_empresas INTO @empresa_id, @cpfs_empresa;
            END;

            CLOSE cursor_empresas;
            DEALLOCATE cursor_empresas;

            -- ETAPA 6: Inserir TODOS os contratos de cada CPF na empresa designada
            BEGIN TRANSACTION;

            INSERT INTO [BDG].[DCA_TB005_DISTRIBUICAO]
            (
                [DT_REFERENCIA], [ID_EDITAL], [ID_PERIODO], [fkContratoSISCTR], 
                [COD_EMPRESA_COBRANCA], [COD_CRITERIO_SELECAO], [NR_CPF_CNPJ], 
                [VR_SD_DEVEDOR], [CREATED_AT]
            )
            SELECT 
                GETDATE(), 
                :edital_id, 
                :periodo_id, 
                D.[FkContratoSISCTR], 
                A.ID_EMPRESA, 
                4, -- Código 4: Demais Contratos Sem Acordo
                D.[NR_CPF_CNPJ], 
                D.[VR_SD_DEVEDOR], 
                GETDATE()
            FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] D
            INNER JOIN #AtribuicaoCPF A ON D.NR_CPF_CNPJ = A.NR_CPF_CNPJ;

            SET @contratos_distribuidos = @@ROWCOUNT;

            -- Remover contratos distribuídos
            DELETE D
            FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] D
            WHERE EXISTS (
                SELECT 1 FROM #AtribuicaoCPF A 
                WHERE A.NR_CPF_CNPJ = D.NR_CPF_CNPJ
            );

            COMMIT;

            -- Salvar resultado
            IF OBJECT_ID('tempdb..##ResultadoDemaisContratos') IS NOT NULL
                DROP TABLE ##ResultadoDemaisContratos;

            CREATE TABLE ##ResultadoDemaisContratos (contratos_distribuidos INT);
            INSERT INTO ##ResultadoDemaisContratos VALUES (@contratos_distribuidos);

            -- Limpeza
            IF OBJECT_ID('tempdb..#CPFsRestantes') IS NOT NULL DROP TABLE #CPFsRestantes;
            IF OBJECT_ID('tempdb..#EmpresasInfo') IS NOT NULL DROP TABLE #EmpresasInfo;
            IF OBJECT_ID('tempdb..#AtribuicaoCPF') IS NOT NULL DROP TABLE #AtribuicaoCPF;
            """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        )

        # Recuperar resultado
        result = db.session.execute(text("SELECT contratos_distribuidos FROM ##ResultadoDemaisContratos")).scalar()
        contratos_distribuidos = result if result is not None else 0

        # Limpar tabela temporária global
        db.session.execute(text(
            "IF OBJECT_ID('tempdb..##ResultadoDemaisContratos') IS NOT NULL DROP TABLE ##ResultadoDemaisContratos"))

        print(f"Distribuição dos demais contratos (AGRUPADO POR CPF) concluída: {contratos_distribuidos} contratos distribuídos")

    except Exception as e:
        db.session.rollback()
        # Limpeza em caso de erro
        try:
            db.session.execute(text("""
                IF OBJECT_ID('tempdb..#CPFsRestantes') IS NOT NULL DROP TABLE #CPFsRestantes;
                IF OBJECT_ID('tempdb..#EmpresasInfo') IS NOT NULL DROP TABLE #EmpresasInfo;
                IF OBJECT_ID('tempdb..#AtribuicaoCPF') IS NOT NULL DROP TABLE #AtribuicaoCPF;
                IF OBJECT_ID('tempdb..##ResultadoDemaisContratos') IS NOT NULL DROP TABLE ##ResultadoDemaisContratos;
            """))
            db.session.commit()
        except:
            pass

        print(f"Erro ao distribuir demais contratos: {str(e)}")
        import traceback
        print(traceback.format_exc())

    return contratos_distribuidos

def atualizar_limites_distribuicao(edital_id, periodo_id):
    """
    Atualiza a tabela de limites de distribuição preenchendo:
    - VR_ARRECADACAO: valor arrecadado por cada empresa
    - QTDE_MAXIMA: quantidade máxima de contratos por empresa
    - VALOR_MAXIMO: valor máximo distribuído por empresa
    - PERCENTUAL_FINAL: percentual final mantido como cadastrado originalmente
    """
    try:
        print(f"Atualizando tabela de limites de distribuição - Edital: {edital_id}, Período: {periodo_id}")

        # 1. Obter estatísticas de distribuição por empresa
        estatisticas = db.session.execute(
            text("""
                 SELECT COD_EMPRESA_COBRANCA,
                        COUNT(*)           as quantidade,
                        SUM(VR_SD_DEVEDOR) as valor_total
                 FROM [BDG].[DCA_TB005_DISTRIBUICAO]
                 WHERE ID_EDITAL = :edital_id
                   AND ID_PERIODO = :periodo_id
                   AND DELETED_AT IS NULL
                 GROUP BY COD_EMPRESA_COBRANCA
                 """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        ).fetchall()

        # 2. Para cada empresa, atualizar informações nos limites EXCETO percentual
        for empresa_id, qtde, valor_total in estatisticas:
            # Verificar se o limite já existe para esta empresa
            limite_existente = db.session.execute(
                text("""
                     SELECT ID, PERCENTUAL_FINAL
                     FROM [BDG].[DCA_TB003_LIMITES_DISTRIBUICAO]
                     WHERE ID_EDITAL = :edital_id
                       AND ID_PERIODO = :periodo_id
                       AND ID_EMPRESA = :empresa_id
                       AND DELETED_AT IS NULL
                     """),
                {"edital_id": edital_id, "periodo_id": periodo_id, "empresa_id": empresa_id}
            ).fetchone()

            if not limite_existente:
                # Se não existe, inserir novo registro com percentual padrão
                db.session.execute(
                    text("""
                         INSERT INTO [BDG].[DCA_TB003_LIMITES_DISTRIBUICAO]
                         (ID_EDITAL,
                          ID_PERIODO,
                          ID_EMPRESA,
                          COD_CRITERIO_SELECAO,
                          QTDE_MAXIMA,
                          VALOR_MAXIMO,
                          PERCENTUAL_FINAL,
                          VR_ARRECADACAO,
                          DT_APURACAO,
                          CREATED_AT)
                         VALUES (
                             :edital_id, :periodo_id, :empresa_id, 4, -- Código padrão
                             :qtde, :valor_total, 0,                  -- Percentual provisório
                             :valor_total, GETDATE(), GETDATE()
                             )
                         """),
                    {
                        "edital_id": edital_id,
                        "periodo_id": periodo_id,
                        "empresa_id": empresa_id,
                        "qtde": qtde,
                        "valor_total": valor_total
                    }
                )
            else:
                # Se existe, atualizar apenas quantidade e valor, mantendo percentual
                db.session.execute(
                    text("""
                         UPDATE [BDG].[DCA_TB003_LIMITES_DISTRIBUICAO]
                         SET
                             VR_ARRECADACAO = :valor_total, QTDE_MAXIMA = :qtde, VALOR_MAXIMO = :valor_total, UPDATED_AT = GETDATE(), DT_APURACAO = GETDATE()
                         -- PERCENTUAL_FINAL não é alterado
                         WHERE ID_EDITAL = :edital_id
                           AND ID_PERIODO = :periodo_id
                           AND ID_EMPRESA = :empresa_id
                         """),
                    {
                        "edital_id": edital_id,
                        "periodo_id": periodo_id,
                        "empresa_id": empresa_id,
                        "qtde": qtde,
                        "valor_total": valor_total
                    }
                )

        db.session.commit()
        print("Atualização dos limites de distribuição concluída com sucesso")

        # Exibir resumo dos limites atualizados
        limites = db.session.execute(
            text("""
                 SELECT ID_EMPRESA,
                        VR_ARRECADACAO,
                        QTDE_MAXIMA,
                        VALOR_MAXIMO,
                        PERCENTUAL_FINAL
                 FROM [BDG].[DCA_TB003_LIMITES_DISTRIBUICAO]
                 WHERE ID_EDITAL = :edital_id
                   AND ID_PERIODO = :periodo_id
                   AND DELETED_AT IS NULL
                 ORDER BY PERCENTUAL_FINAL DESC
                 """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        ).fetchall()

        print("\nResumo dos limites de distribuição:")
        print("Empresa | Arrecadação | Qtd Max | Valor Max | Percentual Final")
        print("--------------------------------------------------------------")

        total_contratos = 0
        total_percentual = 0

        for limite in limites:
            empresa_id = limite[0]
            arrecadacao = limite[1] or 0
            qtd_max = limite[2] or 0
            valor_max = limite[3] or 0
            percentual = limite[4] or 0

            total_contratos += qtd_max
            total_percentual += percentual

            print(
                f"{empresa_id} | R$ {float(arrecadacao):.2f} | {qtd_max} | R$ {float(valor_max):.2f} | {percentual:.2f}%")

        print("--------------------------------------------------------------")
        print(f"TOTAL | - | {total_contratos} | - | {total_percentual:.2f}%")

    except Exception as e:
        db.session.rollback()
        print(f"Erro ao atualizar limites de distribuição: {str(e)}")
        import traceback
        print(traceback.format_exc())


def obter_resultados_finais_distribuicao(edital_id, periodo_id):
    """
    Obtém os resultados finais da distribuição com totais e percentuais.

    Args:
        edital_id: ID do edital
        periodo_id: ID do período

    Returns:
        dict: Contendo a lista de resultados e totais
    """
    try:
        print(f"Obtendo resultados finais da distribuição - Edital: {edital_id}, Período: {periodo_id}")

        # Executar consulta SQL para obter resultados da distribuição
        query = text("""
                     SELECT DIS.[COD_EMPRESA_COBRANCA],
                            EMP.NO_EMPRESA_ABREVIADA,
                            COUNT(*)                 AS QTDE,
                            SUM(DIS.[VR_SD_DEVEDOR]) AS SALDO
                     FROM [BDG].[DCA_TB005_DISTRIBUICAO] AS DIS
                         INNER JOIN [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES] AS EMP
                     ON DIS.[COD_EMPRESA_COBRANCA] = EMP.ID_EMPRESA
                         AND EMP.ID_EDITAL = :edital_id
                         AND EMP.ID_PERIODO = :periodo_id
                     WHERE DIS.ID_EDITAL = :edital_id
                       AND DIS.ID_PERIODO = :periodo_id
                       AND DIS.DELETED_AT IS NULL
                     GROUP BY [COD_EMPRESA_COBRANCA], EMP.NO_EMPRESA_ABREVIADA
                     ORDER BY EMP.NO_EMPRESA_ABREVIADA
                     """)

        resultados = db.session.execute(query, {"edital_id": edital_id, "periodo_id": periodo_id}).fetchall()

        # Calcular totais
        total_qtde = 0
        total_saldo = 0

        # Converter para lista de dicionários e calcular totais
        lista_resultados = []
        for resultado in resultados:
            cod_empresa = resultado[0]
            empresa_abrev = resultado[1]
            qtde = resultado[2]
            saldo = float(resultado[3]) if resultado[3] else 0.0

            total_qtde += qtde
            total_saldo += saldo

            lista_resultados.append({
                'cod_empresa': cod_empresa,
                'empresa_abrev': empresa_abrev,
                'qtde': qtde,
                'saldo': saldo
            })

        # Adicionar percentuais
        for resultado in lista_resultados:
            resultado['pct_qtde'] = (resultado['qtde'] / total_qtde * 100) if total_qtde > 0 else 0
            resultado['pct_saldo'] = (resultado['saldo'] / total_saldo * 100) if total_saldo > 0 else 0

        return {
            'resultados': lista_resultados,
            'total_qtde': total_qtde,
            'total_saldo': total_saldo
        }

    except Exception as e:
        print(f"Erro ao obter resultados finais da distribuição: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return {'resultados': [], 'total_qtde': 0, 'total_saldo': 0}


def distribuir_contratos_descredenciada_igualitariamente_especifica(edital_id, periodo_id, empresa_descredenciada_id,
                                                                    data_fim_periodo_anterior):
    """
    [VERSÃO CORRIGIDA E OTIMIZADA]
    Distribui igualitariamente os CONTRATOS de uma empresa descredenciada,
    AGRUPANDO POR CPF para respeitar as decisões de arrasto.
    """
    ### ALTERAÇÕES REALIZADAS ###
    # 1. A lógica SQL foi completamente substituída para operar sobre CPFs, não contratos.
    #    Isso resolve a falha fundamental da regra de arrasto.
    # 2. A estrutura agora usa tabelas temporárias para mapear CPFs a empresas antes de inserir.
    # 3. O código de critério (3) foi mantido, pois está correto para esta regra.
    ############################

    try:
        print(f"--- EXECUTANDO VERSÃO CORRIGIDA (AGRUPADA POR CPF) ---")
        print(f"Iniciando distribuição igualitária para empresa {empresa_descredenciada_id}")

        sql_script = text("""
            SET NOCOUNT ON;

            IF OBJECT_ID('tempdb..#CPFsParaDistribuir') IS NOT NULL DROP TABLE #CPFsParaDistribuir;
            IF OBJECT_ID('tempdb..#EmpresasReceptoras') IS NOT NULL DROP TABLE #EmpresasReceptoras;
            IF OBJECT_ID('tempdb..#CpfEmpresaMap') IS NOT NULL DROP TABLE #CpfEmpresaMap;

            -- ETAPA 1: Identificar os CPFs únicos da empresa descredenciada que ainda não foram distribuídos.
            SELECT
                DIS.NR_CPF_CNPJ,
                ROW_NUMBER() OVER (ORDER BY NEWID()) as CpfRowNumber
            INTO #CPFsParaDistribuir
            FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] AS DIS
            INNER JOIN [BDG].[COM_TB012_EMPRESA_COBRANCA_ANTERIORES] AS ANT
                ON DIS.FkContratoSISCTR = ANT.fkContratoSISCTR
            WHERE
                ANT.COD_EMPRESA_COBRANCA = :empresa_id
                AND ANT.DT_PERIODO_FIM = :data_fim_periodo_anterior
                AND NOT EXISTS (
                    SELECT 1 FROM [BDG].[DCA_TB005_DISTRIBUICAO] D
                    WHERE D.NR_CPF_CNPJ = DIS.NR_CPF_CNPJ
                      AND D.ID_EDITAL = :edital_id
                      AND D.ID_PERIODO = :periodo_id
                )
            GROUP BY DIS.NR_CPF_CNPJ;

            DECLARE @TotalCPFs INT;
            SELECT @TotalCPFs = COUNT(*) FROM #CPFsParaDistribuir;
            IF @TotalCPFs = 0 BEGIN
                SELECT 0 AS ContratosDistribuidos;
                RETURN;
            END;

            -- ETAPA 2: Obter as empresas que receberão os contratos.
            SELECT
                ID_EMPRESA,
                ROW_NUMBER() OVER (ORDER BY ID_EMPRESA) as EmpresaRowNumber
            INTO #EmpresasReceptoras
            FROM [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES]
            WHERE ID_EDITAL = :edital_id
              AND ID_PERIODO = :periodo_id
              AND DS_CONDICAO = 'PERMANECE' -- Ou a condição que for correta para receptoras
              AND DELETED_AT IS NULL;

            DECLARE @TotalEmpresas INT;
            SELECT @TotalEmpresas = COUNT(*) FROM #EmpresasReceptoras;
            IF @TotalEmpresas = 0 BEGIN
                SELECT 0 AS ContratosDistribuidos;
                RETURN;
            END;

            -- ETAPA 3: Mapear cada CPF para uma empresa receptora.
            SELECT
                C.NR_CPF_CNPJ,
                E.ID_EMPRESA
            INTO #CpfEmpresaMap
            FROM #CPFsParaDistribuir C
            JOIN #EmpresasReceptoras E ON (C.CpfRowNumber - 1) % @TotalEmpresas + 1 = E.EmpresaRowNumber;

            -- ETAPA 4: Inserir todos os contratos dos CPFs mapeados.
            INSERT INTO [BDG].[DCA_TB005_DISTRIBUICAO]
            ([DT_REFERENCIA], [ID_EDITAL], [ID_PERIODO], [fkContratoSISCTR],
             [COD_EMPRESA_COBRANCA], [COD_CRITERIO_SELECAO], [NR_CPF_CNPJ],
             [VR_SD_DEVEDOR], [CREATED_AT])
            SELECT
                GETDATE(), :edital_id, :periodo_id, DIS.FkContratoSISCTR,
                MAP.ID_EMPRESA,
                3, -- Código 3: Contrato com acordo com assessoria descredenciada
                DIS.NR_CPF_CNPJ, DIS.VR_SD_DEVEDOR, GETDATE()
            FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] AS DIS
            JOIN #CpfEmpresaMap AS MAP ON DIS.NR_CPF_CNPJ = MAP.NR_CPF_CNPJ;

            DECLARE @ContratosInseridos INT = @@ROWCOUNT;

            -- ETAPA 5: Remover os CPFs processados da tabela de distribuíveis.
            DELETE DIS
            FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] AS DIS
            WHERE EXISTS (SELECT 1 FROM #CpfEmpresaMap MAP WHERE MAP.NR_CPF_CNPJ = DIS.NR_CPF_CNPJ);

            DROP TABLE #CPFsParaDistribuir;
            DROP TABLE #EmpresasReceptoras;
            DROP TABLE #CpfEmpresaMap;

            SELECT @ContratosInseridos AS ContratosDistribuidos;
        """)

        params = {
            "empresa_id": empresa_descredenciada_id,
            "data_fim_periodo_anterior": data_fim_periodo_anterior,
            "edital_id": edital_id,
            "periodo_id": periodo_id
        }

        with db.engine.begin() as connection:
            result = connection.execute(sql_script, params)
            contratos_distribuidos = result.scalar_one_or_none() or 0

        if contratos_distribuidos > 0:
            print(
                f"Distribuição igualitária (agrupada por CPF) concluída: {contratos_distribuidos} contratos distribuídos.")
        else:
            print("Nenhum CPF elegível foi encontrado para a distribuição igualitária.")

        return {
            "success": True,
            "message": "Processo de distribuição igualitária (agrupado por CPF) concluído.",
            "contratos_distribuidos": contratos_distribuidos
        }

    except Exception as e:
        # O rollback é gerenciado pelo `with db.engine.begin() as connection:`
        print(f"Erro na distribuição igualitária: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return {"success": False, "message": "Erro: " + str(e), "contratos_distribuidos": 0}


def processar_distribuicao_completa(edital_id, periodo_id, usar_distribuicao_igualitaria=False,
                                    empresa_descredenciada_id=None, data_fim_periodo_anterior=None):
    """
    Executa todo o processo de distribuição em ordem.
    Implementa a lógica de substituição completa para a distribuição de descredenciadas.
    """
    resultados = {
        'contratos_distribuiveis': 0,
        'acordos_empresas_permanece': 0,
        'acordos_empresas_descredenciadas': 0,
        'regra_arrasto_acordos': 0,
        'regra_arrasto_sem_acordo': 0,
        'demais_contratos': 0,
        'total_distribuido': 0,
        'usou_distribuicao_igualitaria': usar_distribuicao_igualitaria
    }

    try:
        resultados['contratos_distribuiveis'] = selecionar_contratos_distribuiveis()
        if resultados['contratos_distribuiveis'] == 0:
            print("Nenhum contrato distribuível encontrado. Processo encerrado.")
            return resultados

        resultados['acordos_empresas_permanece'] = distribuir_acordos_vigentes_empresas_permanece(edital_id, periodo_id)

        if usar_distribuicao_igualitaria and empresa_descredenciada_id and data_fim_periodo_anterior:
            print(f"MODO ESPECIAL ATIVADO: Substituindo a distribuição de descredenciadas pela regra igualitária.")
            resultado_igualitaria = distribuir_contratos_descredenciada_igualitariamente_especifica(
                edital_id, periodo_id, empresa_descredenciada_id, data_fim_periodo_anterior
            )
            if resultado_igualitaria['success']:
                resultados['acordos_empresas_descredenciadas'] = resultado_igualitaria['contratos_distribuidos']
        else:
            print("MODO PADRÃO ATIVADO: Executando a distribuição normal para empresas descredenciadas.")
            resultados['acordos_empresas_descredenciadas'] = distribuir_acordos_vigentes_empresas_descredenciadas(
                edital_id, periodo_id
            )

        resultados['regra_arrasto_acordos'] = aplicar_regra_arrasto_acordos(edital_id, periodo_id)
        resultados['regra_arrasto_sem_acordo'] = aplicar_regra_arrasto_sem_acordo(edital_id, periodo_id)
        resultados['demais_contratos'] = distribuir_demais_contratos(edital_id, periodo_id)

        resultados['total_distribuido'] = (
                resultados['acordos_empresas_permanece'] +
                resultados['acordos_empresas_descredenciadas'] +
                resultados['regra_arrasto_acordos'] +
                resultados['regra_arrasto_sem_acordo'] +
                resultados['demais_contratos']
        )

        contratos_restantes = db.session.execute(text("SELECT COUNT(*) FROM [BDG].[DCA_TB006_DISTRIBUIVEIS]")).scalar()
        resultados['contratos_restantes'] = contratos_restantes
        atualizar_limites_distribuicao(edital_id, periodo_id)
        resultados['resultados_finais'] = obter_resultados_finais_distribuicao(edital_id, periodo_id)

        return resultados

    except Exception as e:
        logging.error(f"Erro CRÍTICO no processo de distribuição completa: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return resultados