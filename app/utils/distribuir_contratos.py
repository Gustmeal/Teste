from app import db
from sqlalchemy import text
import logging


def selecionar_contratos_distribuiveis():
    """
    Seleciona o universo de contratos que serão distribuídos e os armazena na tabela DCA_TB006_DISTRIBUIVEIS.
    Usa as tabelas do Banco de Dados Gerencial (BDG).
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

                # Em seguida, inserir os contratos selecionados com critérios melhorados
                logging.info("Selecionando contratos distribuíveis...")
                insert_sql = text("""
                INSERT INTO [BDG].[DCA_TB006_DISTRIBUIVEIS]
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
                    AND SDJ.fkContratoSISCTR IS NULL 
                    AND ECA.COD_EMPRESA_COBRANCA NOT IN (422,407)
                    -- Garantir que não haja duplicatas
                    AND NOT EXISTS (
                        SELECT 1 FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] D 
                        WHERE D.FkContratoSISCTR = ECA.fkContratoSISCTR
                    )""")

                # Verificar informações das tabelas de origem
                origem_count_sql = text("""
                SELECT COUNT(*) FROM [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                INNER JOIN [BDG].[COM_TB001_CONTRATO] AS CON
                    ON ECA.fkContratoSISCTR = CON.fkContratoSISCTR
                INNER JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] AS SIT
                    ON ECA.fkContratoSISCTR = SIT.fkContratoSISCTR
                WHERE SIT.[fkSituacaoCredito] = 1
                """)
                origem_count = connection.execute(origem_count_sql).scalar()
                logging.info(f"Contratos elegíveis nas tabelas de origem: {origem_count}")

                result = connection.execute(insert_sql)
                logging.info(f"Inserção concluída - linhas afetadas: {result.rowcount}")

                # Contar quantos contratos foram selecionados
                logging.info("Contando contratos selecionados...")
                count_sql = text("SELECT COUNT(*) FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] WHERE DELETED_AT IS NULL")
                result = connection.execute(count_sql)
                num_contratos = result.scalar()

                logging.info(f"Total de contratos selecionados: {num_contratos}")
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
        print(f"Iniciando distribuição de acordos vigentes para empresas que permanecem - Edital: {edital_id}, Período: {periodo_id}")

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

                SELECT 
                    GETDATE() AS [DT_REFERENCIA],
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
    Distribui contratos com acordos vigentes de empresas descredenciadas.
    Implementa o item 1.1.2 dos requisitos.

    Args:
        edital_id: ID do edital a ser processado
        periodo_id: ID do período a ser processado

    Returns:
        int: Total de contratos distribuídos
    """
    contratos_distribuidos = 0

    try:
        print(
            f"Iniciando distribuição de acordos vigentes de empresas descredenciadas - Edital: {edital_id}, Período: {periodo_id}")

        # 1. Obter lista de empresas participantes (não descredenciadas)
        empresas_participantes = db.session.execute(
            text("""
                SELECT ID_EMPRESA
                FROM [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND DS_CONDICAO <> 'DESCREDENCIADA'
            """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        ).fetchall()

        if not empresas_participantes:
            print("Nenhuma empresa participante encontrada para o período atual")
            return 0

        empresas_ids = [empresa[0] for empresa in empresas_participantes]
        total_empresas = len(empresas_ids)
        print(f"Total de empresas participantes: {total_empresas}")

        # 2. Identificar contratos com acordos vigentes de empresas descredenciadas
        query_contratos_descredenciados = """
            SELECT 
                DIS.[FkContratoSISCTR],
                DIS.[NR_CPF_CNPJ],
                DIS.[VR_SD_DEVEDOR]
            FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] DIS
            INNER JOIN [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] ECA 
                ON DIS.[FkContratoSISCTR] = ECA.fkContratoSISCTR
            INNER JOIN [BDG].[COM_TB009_ACORDOS_LIQUIDADOS_VIGENTES] ALV
                ON DIS.[FkContratoSISCTR] = ALV.fkContratoSISCTR
            INNER JOIN [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES] EMP
                ON ECA.COD_EMPRESA_COBRANCA = EMP.ID_EMPRESA
            WHERE ALV.fkEstadoAcordo = 1
                AND EMP.DS_CONDICAO = 'DESCREDENCIADA'
                AND EMP.ID_EDITAL = :edital_id
                AND EMP.ID_PERIODO = :periodo_id
                -- Garantir que o contrato não foi distribuído anteriormente
                AND NOT EXISTS (
                    SELECT 1 FROM [BDG].[DCA_TB005_DISTRIBUICAO] DIST
                    WHERE DIST.fkContratoSISCTR = DIS.[FkContratoSISCTR]
                    AND DIST.ID_EDITAL = :edital_id
                    AND DIST.ID_PERIODO = :periodo_id
                )
        """

        contratos_descred = db.session.execute(
            text(query_contratos_descredenciados),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        ).fetchall()

        total_contratos = len(contratos_descred)
        if total_contratos == 0:
            print("Nenhum contrato com acordo vigente de empresa descredenciada encontrado")
            return 0

        print(f"Total de contratos com acordo de empresas descredenciadas: {total_contratos}")

        # 3. Distribuir contratos equitativamente entre as empresas
        empresa_index = 0
        contratos_inseridos = 0

        # Criar lista de contratos a remover após inserção
        contratos_a_remover = []

        # Inserir contratos um por vez
        for contrato in contratos_descred:
            # Rotação circular pelas empresas para distribuição igualitária
            empresa_atual = empresas_ids[empresa_index]

            # Inserir contrato
            db.session.execute(
                text("""
                    INSERT INTO [BDG].[DCA_TB005_DISTRIBUICAO]
                    ([DT_REFERENCIA], [ID_EDITAL], [ID_PERIODO], [fkContratoSISCTR], 
                    [COD_EMPRESA_COBRANCA], [COD_CRITERIO_SELECAO], [NR_CPF_CNPJ], 
                    [VR_SD_DEVEDOR], [CREATED_AT])
                    VALUES (
                        GETDATE(),
                        :edital_id,
                        :periodo_id,
                        :contrato,
                        :empresa,
                        3, -- Contrato com acordo com assessoria descredenciada
                        :cpf_cnpj,
                        :valor,
                        GETDATE()
                    )
                """),
                {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "contrato": contrato[0],  # FkContratoSISCTR
                    "cpf_cnpj": contrato[1],  # NR_CPF_CNPJ
                    "valor": contrato[2],  # VR_SD_DEVEDOR
                    "empresa": empresa_atual
                }
            )
            contratos_inseridos += 1
            contratos_a_remover.append(contrato[0])  # Adicionar à lista de contratos a remover

            # Avança para a próxima empresa na lista circular
            empresa_index = (empresa_index + 1) % total_empresas

            # Commit a cada 100 registros para evitar bloqueios longos
            if contratos_inseridos % 100 == 0:
                db.session.commit()
                print(f"Progresso: {contratos_inseridos}/{total_contratos} contratos inseridos")

        contratos_distribuidos = contratos_inseridos
        print(f"{contratos_distribuidos} contratos com acordo de empresas descredenciadas distribuídos")

        # 4. Excluir os contratos inseridos da tabela de distribuíveis
        if contratos_distribuidos > 0:
            # Usar a lista de contratos para remoção direta
            placeholders = ','.join(':c' + str(i) for i in range(len(contratos_a_remover)))
            params = {f'c{i}': contrato_id for i, contrato_id in enumerate(contratos_a_remover)}
            params.update({"edital_id": edital_id, "periodo_id": periodo_id})

            db.session.execute(
                text(f"""
                    DELETE FROM [BDG].[DCA_TB006_DISTRIBUIVEIS]
                    WHERE [FkContratoSISCTR] IN ({placeholders})
                """),
                params
            )

        db.session.commit()
        print("Processo de distribuição de acordos vigentes de empresas descredenciadas concluído com sucesso")

    except Exception as e:
        db.session.rollback()
        print(f"Erro ao distribuir contratos de empresas descredenciadas: {str(e)}")
        import traceback
        print(traceback.format_exc())

    return contratos_distribuidos


def aplicar_regra_arrasto_acordos(edital_id, periodo_id):
    """
    Aplica a regra de arrasto com acordo para distribuição de contratos.
    """
    from app import db
    from sqlalchemy import text
    import time, logging

    contratos_inseridos = 0
    start_time = time.time()

    try:
        print(f"Iniciando distribuição pela regra de arrasto com acordo - Edital: {edital_id}, Período: {periodo_id}")

        # Verificar se existem acordos para processar
        acordos_count = db.session.execute(
            text("""
                SELECT COUNT(DISTINCT NR_CPF_CNPJ) 
                FROM [BDG].[DCA_TB005_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital
                AND ID_PERIODO = :periodo
                AND COD_CRITERIO_SELECAO IN (1, 3)
            """),
            {"edital": edital_id, "periodo": periodo_id}
        ).scalar()

        if not acordos_count:
            print("Nenhum CPF/CNPJ com acordo foi encontrado.")
            return 0

        print(f"Total de CPFs/CNPJs com acordo: {acordos_count}")

        # ABORDAGEM SIMPLIFICADA: Processar cada CPF individualmente
        # para evitar problemas com a transação complexa

        # 1. Buscar CPFs/CNPJs com acordos
        cpfs_acordos = db.session.execute(
            text("""
                SELECT DISTINCT NR_CPF_CNPJ, COD_EMPRESA_COBRANCA
                FROM [BDG].[DCA_TB005_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital
                AND ID_PERIODO = :periodo
                AND COD_CRITERIO_SELECAO IN (1, 3)
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
                            (
                                [DT_REFERENCIA], [ID_EDITAL], [ID_PERIODO], [fkContratoSISCTR], 
                                [COD_EMPRESA_COBRANCA], [COD_CRITERIO_SELECAO], [NR_CPF_CNPJ], 
                                [VR_SD_DEVEDOR], [CREATED_AT]
                            )
                            VALUES (
                                GETDATE(),
                                :edital_id,
                                :periodo_id,
                                :contrato_id,
                                :empresa_cobranca,
                                6, -- Código 6: Regra de Arrasto
                                :cpf_cnpj,
                                :valor_sd,
                                GETDATE()
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
                            DELETE FROM [BDG].[DCA_TB006_DISTRIBUIVEIS]
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
    Aplica a regra de arrasto para contratos sem acordo.
    Versão corrigida que resolve o erro ResourceClosedError.

    Args:
        edital_id: ID do edital a ser processado
        periodo_id: ID do período a ser processado

    Returns:
        int: Total de contratos distribuídos
    """
    contratos_distribuidos = 0

    try:
        print(
            f"Iniciando distribuição de contratos sem acordo - regra de arrasto - Edital: {edital_id}, Período: {periodo_id}")

        # Primeiro, verificamos se existem empresas participantes
        empresas = db.session.execute(
            text("""
                SELECT COUNT(*) 
                FROM [BDG].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
                JOIN [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES] EP ON LD.ID_EMPRESA = EP.ID_EMPRESA
                    AND LD.ID_EDITAL = EP.ID_EDITAL
                    AND LD.ID_PERIODO = EP.ID_PERIODO
                WHERE LD.ID_EDITAL = :edital_id
                AND LD.ID_PERIODO = :periodo_id
                AND EP.DS_CONDICAO <> 'DESCREDENCIADA'
                AND LD.PERCENTUAL_FINAL > 0
            """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        ).scalar()

        if not empresas:
            print("Nenhuma empresa participante encontrada com percentual de distribuição")
            return 0

        # Depois, verificamos se existem CPFs com múltiplos contratos
        cpfs_multiplos = db.session.execute(
            text("""
                SELECT COUNT(*) 
                FROM (
                    SELECT [NR_CPF_CNPJ], COUNT(*) as qtd_contratos
                    FROM [BDG].[DCA_TB006_DISTRIBUIVEIS]
                    GROUP BY [NR_CPF_CNPJ]
                    HAVING COUNT(*) > 1
                ) AS CPFsMultiplos
            """)
        ).scalar()

        if not cpfs_multiplos:
            print("Nenhum CPF com múltiplos contratos encontrado para aplicar regra de arrasto")
            return 0

        print(f"{cpfs_multiplos} CPFs com múltiplos contratos encontrados")

        # Script SQL que realiza todo o processamento, sem tentar recuperar um resultado diretamente
        db.session.execute(
            text("""
            -- Declarar variável para contagem
            DECLARE @contratos_distribuidos INT = 0;

            -- ETAPA 1: Identificar empresas participantes
            IF OBJECT_ID('tempdb..#Empresas') IS NOT NULL
                DROP TABLE #Empresas;

            SELECT 
                LD.ID_EMPRESA,
                LD.PERCENTUAL_FINAL AS percentual,
                ROW_NUMBER() OVER (ORDER BY LD.PERCENTUAL_FINAL DESC) AS ranking
            INTO #Empresas
            FROM [BDG].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
            JOIN [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES] EP 
                ON LD.ID_EMPRESA = EP.ID_EMPRESA
                AND LD.ID_EDITAL = EP.ID_EDITAL
                AND LD.ID_PERIODO = EP.ID_PERIODO
            WHERE LD.ID_EDITAL = :edital_id
            AND LD.ID_PERIODO = :periodo_id
            AND EP.DS_CONDICAO <> 'DESCREDENCIADA'
            AND LD.PERCENTUAL_FINAL > 0;

            -- ETAPA 2: Normalizar percentuais se necessário
            DECLARE @total_percentual DECIMAL(10, 6);
            SELECT @total_percentual = SUM(percentual) FROM #Empresas;

            IF @total_percentual <= 0
            BEGIN
                UPDATE #Empresas
                SET percentual = 100.0 / (SELECT COUNT(*) FROM #Empresas);
            END
            ELSE IF ABS(@total_percentual - 100) > 0.01
            BEGIN
                UPDATE #Empresas
                SET percentual = percentual * 100.0 / @total_percentual;
            END;
  
            -- ETAPA 3: Identificar CPFs com múltiplos contratos e marcar para distribuição
            IF OBJECT_ID('tempdb..#CPFsMultiplos') IS NOT NULL
                DROP TABLE #CPFsMultiplos;

            SELECT 
                NR_CPF_CNPJ,
                COUNT(*) AS qtd_contratos,
                ROW_NUMBER() OVER (ORDER BY NR_CPF_CNPJ) AS ordem,
                NULL AS empresa_id
            INTO #CPFsMultiplos
            FROM [BDG].[DCA_TB006_DISTRIBUIVEIS]
            GROUP BY NR_CPF_CNPJ
            HAVING COUNT(*) > 1;

            DECLARE @total_cpfs INT;
            SELECT @total_cpfs = COUNT(*) FROM #CPFsMultiplos;

            -- ETAPA 4: Distribuir CPFs entre empresas com base nos percentuais

            -- 4.1: Calcular quantos CPFs cada empresa deve receber
            IF OBJECT_ID('tempdb..#DistribuicaoEmpresas') IS NOT NULL
                DROP TABLE #DistribuicaoEmpresas;

            SELECT 
                ID_EMPRESA,
                percentual,
                FLOOR(@total_cpfs * percentual / 100.0) AS cpfs_inteiros,
                @total_cpfs * percentual / 100.0 - FLOOR(@total_cpfs * percentual / 100.0) AS parte_fracionaria,
                0 AS cpfs_extra,
                0 AS total_cpfs
            INTO #DistribuicaoEmpresas
            FROM #Empresas;

            -- 4.2: Distribuir CPFs restantes com base nas partes fracionárias
            DECLARE @total_inteiros INT, @cpfs_restantes INT;
            SELECT @total_inteiros = SUM(cpfs_inteiros) FROM #DistribuicaoEmpresas;
            SET @cpfs_restantes = @total_cpfs - @total_inteiros;

            IF @cpfs_restantes > 0
            BEGIN
                WITH EmpresasOrdenadas AS (
                    SELECT 
                        ID_EMPRESA,
                        parte_fracionaria,
                        ROW_NUMBER() OVER (ORDER BY parte_fracionaria DESC) AS ranking_fracao
                    FROM #DistribuicaoEmpresas
                )
                UPDATE #DistribuicaoEmpresas
                SET cpfs_extra = CASE WHEN EO.ranking_fracao <= @cpfs_restantes THEN 1 ELSE 0 END
                FROM #DistribuicaoEmpresas DE
                JOIN EmpresasOrdenadas EO ON DE.ID_EMPRESA = EO.ID_EMPRESA;
            END;

            -- 4.3: Calcular total final de CPFs por empresa
            UPDATE #DistribuicaoEmpresas
            SET total_cpfs = cpfs_inteiros + cpfs_extra;

            -- ETAPA 5: Atribuir empresas aos CPFs

            -- 5.1: Criar tabela de faixas de CPFs por empresa
            IF OBJECT_ID('tempdb..#FaixasCPFs') IS NOT NULL
                DROP TABLE #FaixasCPFs;

            CREATE TABLE #FaixasCPFs (
                ID_EMPRESA INT,
                ordem_inicio INT,
                ordem_fim INT
            );

            -- 5.2: Definir faixas de CPFs por empresa
            DECLARE @ordem_atual INT = 1;
            DECLARE @empresa_id INT, @total_cpfs_empresa INT;

            DECLARE cursor_empresas CURSOR FOR
            SELECT ID_EMPRESA, total_cpfs
            FROM #DistribuicaoEmpresas
            WHERE total_cpfs > 0
            ORDER BY percentual DESC;

            OPEN cursor_empresas;
            FETCH NEXT FROM cursor_empresas INTO @empresa_id, @total_cpfs_empresa;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                INSERT INTO #FaixasCPFs (ID_EMPRESA, ordem_inicio, ordem_fim)
                VALUES (@empresa_id, @ordem_atual, @ordem_atual + @total_cpfs_empresa - 1);

                SET @ordem_atual = @ordem_atual + @total_cpfs_empresa;

                FETCH NEXT FROM cursor_empresas INTO @empresa_id, @total_cpfs_empresa;
            END;

            CLOSE cursor_empresas;
            DEALLOCATE cursor_empresas;

            -- 5.3: Atribuir empresas aos CPFs
            UPDATE #CPFsMultiplos
            SET empresa_id = F.ID_EMPRESA
            FROM #CPFsMultiplos C
            JOIN #FaixasCPFs F ON C.ordem BETWEEN F.ordem_inicio AND F.ordem_fim;

            -- ETAPA 6: Inserir todos os contratos de uma vez e remover da tabela de distribuíveis

            BEGIN TRANSACTION;

            -- 6.1: Inserir todos os contratos
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
                C.empresa_id, 
                3, -- Código 3: Regra de Arrasto Sem Acordo
                D.[NR_CPF_CNPJ], 
                D.[VR_SD_DEVEDOR], 
                GETDATE()
            FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] D
            INNER JOIN #CPFsMultiplos C ON D.[NR_CPF_CNPJ] = C.NR_CPF_CNPJ;

            -- Capturar o número total de contratos inseridos
            SET @contratos_distribuidos = @@ROWCOUNT;

            -- 6.2: Remover contratos distribuídos
            DELETE D
            FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] D
            INNER JOIN #CPFsMultiplos C ON D.[NR_CPF_CNPJ] = C.NR_CPF_CNPJ;

            COMMIT;

            -- ETAPA 7: Salvar o resultado em uma tabela temporária global para recuperação posterior
            IF OBJECT_ID('tempdb..##ResultadoArrasto') IS NOT NULL
                DROP TABLE ##ResultadoArrasto;

            CREATE TABLE ##ResultadoArrasto (contratos_distribuidos INT);
            INSERT INTO ##ResultadoArrasto VALUES (@contratos_distribuidos);

            -- Limpeza
            DROP TABLE #Empresas;
            DROP TABLE #CPFsMultiplos;
            DROP TABLE #DistribuicaoEmpresas;
            DROP TABLE #FaixasCPFs;
            """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        )

        # Recuperar o resultado da tabela temporária global
        result = db.session.execute(text("SELECT contratos_distribuidos FROM ##ResultadoArrasto")).scalar()
        contratos_distribuidos = result if result is not None else 0

        # Limpar a tabela temporária global
        db.session.execute(text("IF OBJECT_ID('tempdb..##ResultadoArrasto') IS NOT NULL DROP TABLE ##ResultadoArrasto"))

        print(f"Regra de arrasto processada com sucesso: {contratos_distribuidos} contratos distribuídos")

    except Exception as e:
        db.session.rollback()
        # Limpeza das tabelas temporárias em caso de erro
        try:
            db.session.execute(text("""
                IF OBJECT_ID('tempdb..#Empresas') IS NOT NULL DROP TABLE #Empresas;
                IF OBJECT_ID('tempdb..#CPFsMultiplos') IS NOT NULL DROP TABLE #CPFsMultiplos;
                IF OBJECT_ID('tempdb..#DistribuicaoEmpresas') IS NOT NULL DROP TABLE #DistribuicaoEmpresas;
                IF OBJECT_ID('tempdb..#FaixasCPFs') IS NOT NULL DROP TABLE #FaixasCPFs;
                IF OBJECT_ID('tempdb..##ResultadoArrasto') IS NOT NULL DROP TABLE ##ResultadoArrasto;
            """))
            db.session.commit()
        except:
            pass

        print(f"Erro ao distribuir contratos pela regra de arrasto: {str(e)}")
        import traceback
        print(traceback.format_exc())

    return contratos_distribuidos


def distribuir_demais_contratos(edital_id, periodo_id):
    """
    Distribui os contratos restantes entre as empresas.
    Implementa o item 1.1.5 dos requisitos: Demais contratos sem acordo.

    Args:
        edital_id: ID do edital a ser processado
        periodo_id: ID do período a ser processado

    Returns:
        int: Total de contratos distribuídos
    """
    contratos_distribuidos = 0

    try:
        print(f"Iniciando distribuição dos demais contratos sem acordo - Edital: {edital_id}, Período: {periodo_id}")

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

        contratos_count = db.session.execute(
            text("SELECT COUNT(*) FROM [BDG].[DCA_TB006_DISTRIBUIVEIS]")
        ).scalar()

        if not contratos_count:
            print("Nenhum contrato restante para distribuir.")
            return 0

        print(f"Total de contratos restantes: {contratos_count}")

        # Executar o script SQL principal sem tentar recuperar resultado diretamente
        db.session.execute(
            text("""
            -- Declaração de variáveis
            DECLARE @contratos_distribuidos INT = 0;

            -- ETAPA 1: Buscar empresas e seus percentuais + contratos atuais
            IF OBJECT_ID('tempdb..#EmpresasInfo') IS NOT NULL
                DROP TABLE #EmpresasInfo;

            SELECT 
                EP.ID_EMPRESA,
                COALESCE(LD.PERCENTUAL_FINAL, 0) AS percentual,
                COALESCE((
                    SELECT COUNT(*) 
                    FROM [BDG].[DCA_TB005_DISTRIBUICAO] D
                    WHERE D.ID_EDITAL = EP.ID_EDITAL
                    AND D.ID_PERIODO = EP.ID_PERIODO
                    AND D.COD_EMPRESA_COBRANCA = EP.ID_EMPRESA
                ), 0) AS contratos_atuais,
                0 AS meta_total_exata,
                0 AS meta_total_inteira,
                0 AS contratos_faltantes,
                0 AS parte_fracionaria,
                0 AS contratos_extra,
                0 AS total_a_receber,
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

            -- ETAPA 2: Normalizar percentuais
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

            -- ETAPA 3: Calcular metas e contratos a distribuir
            DECLARE @total_contratos_restantes INT;
            SELECT @total_contratos_restantes = COUNT(*) 
            FROM [BDG].[DCA_TB006_DISTRIBUIVEIS];

            -- 3.1 Calcular total de contratos (atuais + restantes)
            DECLARE @total_contratos_atuais INT;
            DECLARE @total_contratos INT;

            SELECT @total_contratos_atuais = SUM(contratos_atuais)
            FROM #EmpresasInfo;

            SET @total_contratos = @total_contratos_atuais + @total_contratos_restantes;

            -- 3.2 Calcular metas para cada empresa
            UPDATE #EmpresasInfo
            SET
                meta_total_exata = @total_contratos * percentual / 100.0,
                meta_total_inteira = FLOOR(@total_contratos * percentual / 100.0),
                parte_fracionaria = @total_contratos * percentual / 100.0 - FLOOR(@total_contratos * percentual / 100.0);

            -- 3.3 Calcular quantos contratos faltam para cada empresa
            UPDATE #EmpresasInfo
            SET contratos_faltantes = CASE
                                        WHEN meta_total_inteira > contratos_atuais THEN meta_total_inteira - contratos_atuais
                                        ELSE 0
                                      END;

            -- 3.4 Verificar quantos contratos faltam distribuir pelos fracionais
            DECLARE @total_faltantes INT;
            DECLARE @contratos_nao_alocados INT;

            SELECT @total_faltantes = SUM(contratos_faltantes)
            FROM #EmpresasInfo;

            SET @contratos_nao_alocados = @total_contratos_restantes - @total_faltantes;

            -- 3.5 Distribuir contratos extras por maiores fracionais
            IF @contratos_nao_alocados > 0
            BEGIN
                WITH EmpresasOrdenadas AS (
                    SELECT 
                        ID_EMPRESA,
                        parte_fracionaria,
                        ROW_NUMBER() OVER (ORDER BY parte_fracionaria DESC) AS ranking_fracao
                    FROM #EmpresasInfo
                )
                UPDATE #EmpresasInfo
                SET contratos_extra = CASE WHEN EO.ranking_fracao <= @contratos_nao_alocados THEN 1 ELSE 0 END
                FROM #EmpresasInfo EI
                JOIN EmpresasOrdenadas EO ON EI.ID_EMPRESA = EO.ID_EMPRESA;
            END;

            -- 3.6 Calcular total de contratos a receber
            UPDATE #EmpresasInfo
            SET total_a_receber = contratos_faltantes + contratos_extra;

            -- ETAPA 4: Criar tabela de contratos distribuíveis estrategicamente embaralhados
            IF OBJECT_ID('tempdb..#ContratosEmbaralhados') IS NOT NULL
                DROP TABLE #ContratosEmbaralhados;

            -- 4.1 Embaralhar todos os contratos aleatoriamente sem considerar valor
            SELECT 
                [FkContratoSISCTR],
                [NR_CPF_CNPJ],
                [VR_SD_DEVEDOR],
                ROW_NUMBER() OVER (ORDER BY NEWID()) AS ordem
            INTO #ContratosEmbaralhados
            FROM [BDG].[DCA_TB006_DISTRIBUIVEIS];

            -- ETAPA 5: Distribuir contratos para empresas

            -- 5.1 Criar tabela para faixas de ordem por empresa
            IF OBJECT_ID('tempdb..#FaixasOrdem') IS NOT NULL
                DROP TABLE #FaixasOrdem;

            CREATE TABLE #FaixasOrdem (
                ID_EMPRESA INT,
                ordem_inicio INT,
                ordem_fim INT
            );

            -- 5.2 Definir faixas de ordem por empresa
            DECLARE @ordem_atual INT = 1;
            DECLARE @empresa_id INT, @contratos_empresa INT;

            -- Usar NEWID() para randomizar a ordem das empresas na distribuição
            DECLARE cursor_empresas CURSOR FOR
            SELECT ID_EMPRESA, total_a_receber
            FROM #EmpresasInfo
            WHERE total_a_receber > 0
            ORDER BY NEWID(); -- Randomiza a ordem das empresas

            OPEN cursor_empresas;
            FETCH NEXT FROM cursor_empresas INTO @empresa_id, @contratos_empresa;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                IF @contratos_empresa > 0
                BEGIN
                    INSERT INTO #FaixasOrdem (ID_EMPRESA, ordem_inicio, ordem_fim)
                    VALUES (@empresa_id, @ordem_atual, @ordem_atual + @contratos_empresa - 1);

                    SET @ordem_atual = @ordem_atual + @contratos_empresa;
                END

                FETCH NEXT FROM cursor_empresas INTO @empresa_id, @contratos_empresa;
            END;

            CLOSE cursor_empresas;
            DEALLOCATE cursor_empresas;

            -- 5.3 Atribuir empresas aos contratos
            IF OBJECT_ID('tempdb..#AtribuicaoFinal') IS NOT NULL
                DROP TABLE #AtribuicaoFinal;

            SELECT 
                C.[FkContratoSISCTR],
                C.[NR_CPF_CNPJ],
                C.[VR_SD_DEVEDOR],
                F.ID_EMPRESA
            INTO #AtribuicaoFinal
            FROM #ContratosEmbaralhados C
            JOIN #FaixasOrdem F ON C.ordem BETWEEN F.ordem_inicio AND F.ordem_fim;

            -- ETAPA 6: Inserir contratos e remover da tabela de distribuíveis

            BEGIN TRANSACTION;

            -- 6.1 Inserir todos os contratos
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
                [FkContratoSISCTR], 
                ID_EMPRESA, 
                4, -- Código 4: Demais Contratos Sem Acordo
                [NR_CPF_CNPJ], 
                [VR_SD_DEVEDOR], 
                GETDATE()
            FROM #AtribuicaoFinal;

            -- Capturar o número total de contratos inseridos
            SET @contratos_distribuidos = @@ROWCOUNT;

            -- 6.2 Remover contratos distribuídos
            DELETE D
            FROM [BDG].[DCA_TB006_DISTRIBUIVEIS] D
            WHERE [FkContratoSISCTR] IN (
                SELECT [FkContratoSISCTR] FROM #AtribuicaoFinal
            );

            COMMIT;

            -- ETAPA 7: Salvar o resultado para recuperação posterior
            IF OBJECT_ID('tempdb..##ResultadoDemaisContratos') IS NOT NULL
                DROP TABLE ##ResultadoDemaisContratos;

            CREATE TABLE ##ResultadoDemaisContratos (contratos_distribuidos INT);
            INSERT INTO ##ResultadoDemaisContratos VALUES (@contratos_distribuidos);

            -- Limpeza
            IF OBJECT_ID('tempdb..#EmpresasInfo') IS NOT NULL DROP TABLE #EmpresasInfo;
            IF OBJECT_ID('tempdb..#ContratosEmbaralhados') IS NOT NULL DROP TABLE #ContratosEmbaralhados;
            IF OBJECT_ID('tempdb..#FaixasOrdem') IS NOT NULL DROP TABLE #FaixasOrdem;
            IF OBJECT_ID('tempdb..#AtribuicaoFinal') IS NOT NULL DROP TABLE #AtribuicaoFinal;
            """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        )

        # Recuperar o resultado da tabela temporária global
        result = db.session.execute(text("SELECT contratos_distribuidos FROM ##ResultadoDemaisContratos")).scalar()
        contratos_distribuidos = result if result is not None else 0

        # Limpar a tabela temporária global
        db.session.execute(text(
            "IF OBJECT_ID('tempdb..##ResultadoDemaisContratos') IS NOT NULL DROP TABLE ##ResultadoDemaisContratos"))

        print(f"Distribuição dos demais contratos concluída: {contratos_distribuidos} contratos distribuídos")

    except Exception as e:
        db.session.rollback()
        # Limpeza das tabelas temporárias em caso de erro
        try:
            db.session.execute(text("""
                IF OBJECT_ID('tempdb..#EmpresasInfo') IS NOT NULL DROP TABLE #EmpresasInfo;
                IF OBJECT_ID('tempdb..#ContratosEmbaralhados') IS NOT NULL DROP TABLE #ContratosEmbaralhados;
                IF OBJECT_ID('tempdb..#FaixasOrdem') IS NOT NULL DROP TABLE #FaixasOrdem;
                IF OBJECT_ID('tempdb..#AtribuicaoFinal') IS NOT NULL DROP TABLE #AtribuicaoFinal;
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
                SELECT 
                    COD_EMPRESA_COBRANCA,
                    COUNT(*) as quantidade,
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
                        (
                            ID_EDITAL,
                            ID_PERIODO,
                            ID_EMPRESA,
                            COD_CRITERIO_SELECAO,
                            QTDE_MAXIMA,
                            VALOR_MAXIMO,
                            PERCENTUAL_FINAL,
                            VR_ARRECADACAO,
                            DT_APURACAO,
                            CREATED_AT
                        )
                        VALUES (
                            :edital_id,
                            :periodo_id,
                            :empresa_id,
                            4, -- Código padrão
                            :qtde,
                            :valor_total,
                            0, -- Percentual provisório
                            :valor_total,
                            GETDATE(),
                            GETDATE()
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
                            VR_ARRECADACAO = :valor_total,
                            QTDE_MAXIMA = :qtde,
                            VALOR_MAXIMO = :valor_total,
                            UPDATED_AT = GETDATE(),
                            DT_APURACAO = GETDATE()
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
                SELECT 
                    ID_EMPRESA,
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
            SELECT 
                DIS.[COD_EMPRESA_COBRANCA], 
                EMP.NO_EMPRESA_ABREVIADA, 
                COUNT(*) AS QTDE, 
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


def processar_distribuicao_completa(edital_id, periodo_id):
    """
    Executa todo o processo de distribuição em ordem.

    Args:
        edital_id (int): ID do edital
        periodo_id (int): ID do período

    Returns:
        dict: Estatísticas do processo de distribuição
    """
    resultados = {
        'contratos_distribuiveis': 0,
        'acordos_empresas_permanece': 0,
        'acordos_empresas_descredenciadas': 0,
        'regra_arrasto_acordos': 0,
        'regra_arrasto_sem_acordo': 0,
        'demais_contratos': 0,
        'total_distribuido': 0
    }

    try:
        # 0. Selecionar contratos distribuíveis (pré-requisito)
        resultados['contratos_distribuiveis'] = selecionar_contratos_distribuiveis()

        if resultados['contratos_distribuiveis'] == 0:
            return resultados

        # 1.1.1. Contratos com acordo vigente de empresa que permanece
        resultados['acordos_empresas_permanece'] = distribuir_acordos_vigentes_empresas_permanece(edital_id, periodo_id)

        # 1.1.2. Contratos com acordo vigente com empresa descredenciada
        resultados['acordos_empresas_descredenciadas'] = distribuir_acordos_vigentes_empresas_descredenciadas(edital_id,
                                                                                                              periodo_id)

        # 1.1.3. Contratos com acordo vigente – regra do arrasto
        resultados['regra_arrasto_acordos'] = aplicar_regra_arrasto_acordos(edital_id, periodo_id)

        # 1.1.4. Demais contratos sem acordo – regra do arrasto
        resultados['regra_arrasto_sem_acordo'] = aplicar_regra_arrasto_sem_acordo(edital_id, periodo_id)

        # 1.1.5. Demais contratos sem acordo
        resultados['demais_contratos'] = distribuir_demais_contratos(edital_id, periodo_id)

        # Calcular total
        resultados['total_distribuido'] = (
                resultados['acordos_empresas_permanece'] +
                resultados['acordos_empresas_descredenciadas'] +
                resultados['regra_arrasto_acordos'] +
                resultados['regra_arrasto_sem_acordo'] +
                resultados['demais_contratos']
        )

        # Verificar contratos restantes não distribuídos
        contratos_restantes = db.session.execute(
            text("SELECT COUNT(*) FROM [BDG].[DCA_TB006_DISTRIBUIVEIS]")
        ).scalar()

        resultados['contratos_restantes'] = contratos_restantes
        resultados['total_com_restantes'] = resultados['total_distribuido'] + contratos_restantes

        # Atualizar limites de distribuição
        atualizar_limites_distribuicao(edital_id, periodo_id)

        # NOVO: Obter resultados finais da distribuição
        resultados_finais = obter_resultados_finais_distribuicao(edital_id, periodo_id)
        resultados['resultados_finais'] = resultados_finais

        return resultados

    except Exception as e:
        logging.error(f"Erro no processo de distribuição: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return resultados