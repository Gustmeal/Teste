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
                truncate_sql = text("TRUNCATE TABLE [DEV].[DCA_TB006_DISTRIBUIVEIS]")
                connection.execute(truncate_sql)
                logging.info("Tabela de distribuíveis limpa com sucesso")

                # Limpar TAMBÉM a tabela de arrastaveis, que pode conter dados de processamentos anteriores
                truncate_arrastaveis_sql = text("TRUNCATE TABLE [DEV].[DCA_TB007_ARRASTAVEIS]")
                connection.execute(truncate_arrastaveis_sql)
                logging.info("Tabela de arrastaveis limpa com sucesso")

                # Verificar se a limpeza foi bem-sucedida
                check_sql = text("SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]")
                count_after_truncate = connection.execute(check_sql).scalar()
                logging.info(f"Contagem após limpeza: {count_after_truncate}")

                # Em seguida, inserir os contratos selecionados com critérios melhorados
                logging.info("Selecionando contratos distribuíveis...")
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
                    AND SDJ.fkContratoSISCTR IS NULL
                    -- Garantir que não haja duplicatas
                    AND NOT EXISTS (
                        SELECT 1 FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] D 
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
                count_sql = text("SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] WHERE DELETED_AT IS NULL")
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
            "DELETE FROM [DEV].[DCA_TB005_DISTRIBUICAO] WHERE ID_EDITAL = :edital_id AND ID_PERIODO = :periodo_id"),
                           {"edital_id": edital_id, "periodo_id": periodo_id})
        print("Tabela DCA_TB005_DISTRIBUICAO limpa para este edital/período")

        # 2. Inserir contratos com acordos vigentes para empresas que permanecem
        # IMPORTANTE: usar EXISTS para controle de duplicatas
        resultado_acordos = db.session.execute(
            text("""
                INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
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
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] AS DIS
                    INNER JOIN [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                        ON DIS.[FkContratoSISCTR] = ECA.fkContratoSISCTR
                    INNER JOIN [BDG].[COM_TB009_ACORDOS_LIQUIDADOS_VIGENTES] AS ALV
                        ON DIS.[FkContratoSISCTR] = ALV.fkContratoSISCTR
                    INNER JOIN [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES] AS EMP
                        ON ECA.COD_EMPRESA_COBRANCA = EMP.ID_EMPRESA
                WHERE EMP.ID_EDITAL = :edital_id
                    AND EMP.ID_PERIODO = :periodo_id
                    AND EMP.DS_CONDICAO = 'PERMANECE'
                    AND ALV.fkEstadoAcordo = 1
                    -- Garantir que não haja duplicatas
                    AND NOT EXISTS (
                        SELECT 1 FROM [DEV].[DCA_TB005_DISTRIBUICAO] D
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
                    FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] AS DIS
                    WHERE DIS.[FkContratoSISCTR] IN (
                        SELECT DIST.[fkContratoSISCTR]
                        FROM [DEV].[DCA_TB005_DISTRIBUICAO] AS DIST 
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
                FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES]
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
            FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] DIS
            INNER JOIN [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] ECA 
                ON DIS.[FkContratoSISCTR] = ECA.fkContratoSISCTR
            INNER JOIN [BDG].[COM_TB009_ACORDOS_LIQUIDADOS_VIGENTES] ALV
                ON DIS.[FkContratoSISCTR] = ALV.fkContratoSISCTR
            INNER JOIN [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES] EMP
                ON ECA.COD_EMPRESA_COBRANCA = EMP.ID_EMPRESA
            WHERE ALV.fkEstadoAcordo = 1
                AND EMP.DS_CONDICAO = 'DESCREDENCIADA'
                AND EMP.ID_EDITAL = :edital_id
                AND EMP.ID_PERIODO = :periodo_id
                -- Garantir que o contrato não foi distribuído anteriormente
                AND NOT EXISTS (
                    SELECT 1 FROM [DEV].[DCA_TB005_DISTRIBUICAO] DIST
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
                    INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
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
                    DELETE FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
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
                FROM [DEV].[DCA_TB005_DISTRIBUICAO]
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
                FROM [DEV].[DCA_TB005_DISTRIBUICAO]
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
                    FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
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
                        FROM [DEV].[DCA_TB005_DISTRIBUICAO]
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
                            INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
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
                            DELETE FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
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
    Aplica a regra de arrasto para contratos sem acordo (regra 1.1.4).
    Versão otimizada para melhor performance.
    """
    from app import db
    from sqlalchemy import text
    import time
    import logging

    logger = logging.getLogger(__name__)
    start_time = time.time()
    resultados = {
        "inseridos_arrastaveis": 0,
        "distribuidos": 0,
        "cpfs_processados": 0
    }

    try:
        logger.info(
            f"Iniciando regra de arrasto para contratos sem acordo - Edital: {edital_id}, Período: {periodo_id}")

        # 1. VERIFICAR PRÉ-REQUISITOS - Execução única
        with db.engine.connect() as conn:
            # Verificar se existem empresas para distribuição
            empresas_count = conn.execute(
                text(
                    "SELECT COUNT(*) FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] WHERE ID_EDITAL = :e AND ID_PERIODO = :p AND DELETED_AT IS NULL"),
                {"e": edital_id, "p": periodo_id}
            ).scalar() or 0

            if empresas_count == 0:
                logger.warning("Nenhuma empresa disponível para distribuição.")
                return 0

            # 2. IDENTIFICAR CPFs COM MÚLTIPLOS CONTRATOS - Direto, sem tabela temporária global
            logger.info("Identificando CPFs com múltiplos contratos...")

            # Criar tabela temporária apenas para CPFs múltiplos (usando # local em vez de ## global)
            conn.execute(text("IF OBJECT_ID('tempdb..#CPFsMultiplos') IS NOT NULL DROP TABLE #CPFsMultiplos"))

            # Criar índice já na criação
            conn.execute(text("""
                SELECT DISTINCT NR_CPF_CNPJ
                INTO #CPFsMultiplos
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                WHERE NR_CPF_CNPJ IS NOT NULL
                GROUP BY NR_CPF_CNPJ
                HAVING COUNT(*) > 1
            """))

            # Criar índice para otimizar joins
            conn.execute(text("CREATE CLUSTERED INDEX IX_CPFsMultiplos ON #CPFsMultiplos(NR_CPF_CNPJ)"))

            # Contar CPFs com múltiplos contratos
            cpfs_multiplos = conn.execute(text("SELECT COUNT(*) FROM #CPFsMultiplos")).scalar() or 0
            resultados["cpfs_processados"] = cpfs_multiplos

            if cpfs_multiplos == 0:
                logger.warning("Nenhum CPF com múltiplos contratos encontrado.")
                return 0

            logger.info(f"Encontrados {cpfs_multiplos} CPFs com múltiplos contratos")

            # 3. MOVER PARA ARRASTAVEIS - Em uma única operação otimizada
            logger.info("Movendo contratos para tabela de arrastaveis...")

            # CORREÇÃO: Removido o campo VR_SD_DEVEDOR do INSERT
            resultado_insercao = conn.execute(text("""
                INSERT INTO [DEV].[DCA_TB007_ARRASTAVEIS] 
                    (FkContratoSISCTR, NR_CPF_CNPJ, CREATED_AT)
                OUTPUT INSERTED.FkContratoSISCTR
                SELECT 
                    D.FkContratoSISCTR, 
                    D.NR_CPF_CNPJ,
                    GETDATE()
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] D
                INNER JOIN #CPFsMultiplos M ON D.NR_CPF_CNPJ = M.NR_CPF_CNPJ
                WHERE NOT EXISTS (
                    SELECT 1 FROM [DEV].[DCA_TB007_ARRASTAVEIS] A
                    WHERE A.FkContratoSISCTR = D.FkContratoSISCTR
                )
            """))

            # Capturar IDs dos contratos inseridos
            contratos_inseridos = [row[0] for row in resultado_insercao.fetchall()]
            total_inseridos = len(contratos_inseridos)
            resultados["inseridos_arrastaveis"] = total_inseridos

            logger.info(f"Movidos {total_inseridos} contratos para a tabela de arrastaveis")

            if total_inseridos == 0:
                logger.warning("Nenhum contrato movido para arrastaveis.")
                return 0

            # 4. REMOVER DOS DISTRIBUÍVEIS - Em uma única operação
            if contratos_inseridos:
                # Criar tabela temporária para IDs a remover
                conn.execute(text("IF OBJECT_ID('tempdb..#ContratosRemover') IS NOT NULL DROP TABLE #ContratosRemover"))
                conn.execute(text("CREATE TABLE #ContratosRemover (FkContratoSISCTR BIGINT PRIMARY KEY)"))

                # Inserir em lotes para evitar strings de parâmetros muito grandes
                batch_size = 1000
                for i in range(0, len(contratos_inseridos), batch_size):
                    batch = contratos_inseridos[i:i + batch_size]
                    placeholders = ','.join(['?'] * len(batch))
                    conn.execute(
                        text(
                            f"INSERT INTO #ContratosRemover (FkContratoSISCTR) VALUES {','.join(['(?)'] * len(batch))}"),
                        batch
                    )

                # Remover em uma operação
                conn.execute(text("""
                    DELETE D
                    FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] D
                    INNER JOIN #ContratosRemover R ON D.FkContratoSISCTR = R.FkContratoSISCTR
                """))

                logger.info(f"Removidos {total_inseridos} contratos da tabela de distribuíveis")

            # 5. PREPARAR DISTRIBUIÇÃO - Cálculos simplificados
            logger.info("Preparando informações para distribuição...")

            # Obter empresas e percentuais em uma única consulta
            empresas_info = conn.execute(text("""
                SELECT 
                    ID_EMPRESA,
                    COALESCE(PERCENTUAL_FINAL, 0) AS percentual
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE ID_EDITAL = :e AND ID_PERIODO = :p AND DELETED_AT IS NULL
                ORDER BY PERCENTUAL_FINAL DESC
            """), {"e": edital_id, "p": periodo_id}).fetchall()

            # Normalizar percentuais
            total_percentual = sum(float(emp[1]) for emp in empresas_info)
            if total_percentual <= 0:
                # Distribuição igualitária
                percentual_base = 100.0 / len(empresas_info)
                empresas_normalizado = [(emp[0], percentual_base) for emp in empresas_info]
            else:
                # Normalização proporcional
                fator = 100.0 / total_percentual
                empresas_normalizado = [(emp[0], float(emp[1]) * fator) for emp in empresas_info]

            # 6. ASSOCIAR CPFs ÀS EMPRESAS - Abordagem lote por lote
            logger.info("Distribuindo CPFs entre empresas...")

            # Calcular quantos CPFs cada empresa deve receber
            total_cpfs = cpfs_multiplos
            cpfs_por_empresa = []

            cpfs_restantes = total_cpfs
            for emp_id, pct in empresas_normalizado:
                # Parte inteira
                quantidade = int((pct / 100.0) * total_cpfs)
                cpfs_por_empresa.append((emp_id, quantidade))
                cpfs_restantes -= quantidade

            # Distribuir CPFs restantes para as empresas com maior percentual
            for i in range(cpfs_restantes):
                idx = i % len(cpfs_por_empresa)
                cpfs_por_empresa[idx] = (cpfs_por_empresa[idx][0], cpfs_por_empresa[idx][1] + 1)

            # Criar tabela temporária para associação CPF -> Empresa
            conn.execute(text("IF OBJECT_ID('tempdb..#CPFsDistribuicao') IS NOT NULL DROP TABLE #CPFsDistribuicao"))
            conn.execute(text("""
                CREATE TABLE #CPFsDistribuicao (
                    NR_CPF_CNPJ VARCHAR(20) PRIMARY KEY,
                    ID_EMPRESA INT
                )
            """))

            # Atribuir CPFs às empresas em lotes
            cpfs_todos = conn.execute(text("SELECT NR_CPF_CNPJ FROM #CPFsMultiplos ORDER BY NEWID()")).fetchall()

            inicio = 0
            for emp_id, quantidade in cpfs_por_empresa:
                if quantidade <= 0:
                    continue

                fim = min(inicio + quantidade, len(cpfs_todos))
                lote_cpfs = [cpf[0] for cpf in cpfs_todos[inicio:fim]]

                if lote_cpfs:
                    # Inserção lote a lote para evitar tamanho excessivo de query
                    for j in range(0, len(lote_cpfs), 1000):
                        sublote = lote_cpfs[j:j + 1000]
                        valores = []

                        for cpf in sublote:
                            valores.append(f"('{cpf}', {emp_id})")

                        conn.execute(text(f"""
                            INSERT INTO #CPFsDistribuicao (NR_CPF_CNPJ, ID_EMPRESA)
                            VALUES {','.join(valores)}
                        """))

                inicio = fim

            # 7. FAZER A DISTRIBUIÇÃO FINAL - Em uma única operação
            logger.info("Executando distribuição final...")

            # CORREÇÃO: Obtém o valor do campo VR_SD_DEVEDOR da tabela distribuíveis
            resultado_final = conn.execute(text("""
                INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
                (
                    [DT_REFERENCIA], [ID_EDITAL], [ID_PERIODO], [fkContratoSISCTR],
                    [COD_EMPRESA_COBRANCA], [COD_CRITERIO_SELECAO], [NR_CPF_CNPJ],
                    [VR_SD_DEVEDOR], [CREATED_AT]
                )
                SELECT
                    GETDATE(), :edital_id, :periodo_id, A.FkContratoSISCTR,
                    D.ID_EMPRESA, 6, A.NR_CPF_CNPJ,
                    DIST.VR_SD_DEVEDOR, GETDATE()
                FROM [DEV].[DCA_TB007_ARRASTAVEIS] A
                INNER JOIN #CPFsDistribuicao D ON A.NR_CPF_CNPJ = D.NR_CPF_CNPJ
                LEFT JOIN [DEV].[DCA_TB006_DISTRIBUIVEIS] DIST ON DIST.FkContratoSISCTR = A.FkContratoSISCTR
                WHERE A.DELETED_AT IS NULL
            """), {"edital_id": edital_id, "periodo_id": periodo_id})

            contratos_distribuidos = resultado_final.rowcount
            resultados["distribuidos"] = contratos_distribuidos

            logger.info(f"Distribuição inseriu {contratos_distribuidos} contratos")

            # 8. MARCAR COMO PROCESSADOS
            logger.info("Marcando contratos como processados...")

            conn.execute(text("""
                UPDATE [DEV].[DCA_TB007_ARRASTAVEIS]
                SET DELETED_AT = GETDATE()
                WHERE DELETED_AT IS NULL
            """))

            # 9. LIMPEZA FINAL
            logger.info("Realizando limpeza final...")

            conn.execute(text("""
                IF OBJECT_ID('tempdb..#CPFsMultiplos') IS NOT NULL DROP TABLE #CPFsMultiplos;
                IF OBJECT_ID('tempdb..#ContratosRemover') IS NOT NULL DROP TABLE #ContratosRemover;
                IF OBJECT_ID('tempdb..#CPFsDistribuicao') IS NOT NULL DROP TABLE #CPFsDistribuicao;
            """))

        # Verificação final (fora do with connection)
        restantes = db.session.execute(
            text("SELECT COUNT(*) FROM [DEV].[DCA_TB007_ARRASTAVEIS] WHERE DELETED_AT IS NULL")
        ).scalar() or 0

        if restantes > 0:
            logger.warning(f"ATENÇÃO: Ainda restam {restantes} contratos não distribuídos na tabela de arrastaveis")

        # Log de finalização
        elapsed_time = time.time() - start_time
        logger.info(f"Regra de arrasto sem acordo otimizada concluída em {elapsed_time:.2f}s")
        logger.info(
            f"Resultados: {resultados['distribuidos']} contratos distribuídos, {resultados['cpfs_processados']} CPFs")

        return resultados["distribuidos"]

    except Exception as e:
        db.session.rollback()
        logger.error(f"Erro crítico na regra de arrasto sem acordo: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

        # Limpeza em caso de erro
        try:
            with db.engine.connect() as conn:
                conn.execute(text("""
                    IF OBJECT_ID('tempdb..#CPFsMultiplos') IS NOT NULL DROP TABLE #CPFsMultiplos;
                    IF OBJECT_ID('tempdb..#ContratosRemover') IS NOT NULL DROP TABLE #ContratosRemover;
                    IF OBJECT_ID('tempdb..#CPFsDistribuicao') IS NOT NULL DROP TABLE #CPFsDistribuicao;
                """))
        except Exception as cleanup_e:
            logger.error(f"Erro durante a limpeza: {cleanup_e}")

        return 0

def distribuir_demais_contratos(edital_id, periodo_id):
    """
    Distribui os contratos restantes entre as empresas.
    Implementa o item 1.1.5 dos requisitos.

    Args:
        edital_id: ID do edital a ser processado
        periodo_id: ID do período a ser processado

    Returns:
        int: Total de contratos distribuídos
    """
    contratos_distribuidos = 0
    import logging
    logger = logging.getLogger(__name__)

    try:
        logger.info(
            f"Iniciando distribuição dos demais contratos sem acordo - Edital: {edital_id}, Período: {periodo_id}")

        # Carregar os limites de distribuição para saber qual percentual de cada empresa
        limites = db.session.execute(
            text("""
                SELECT ID_EMPRESA, PERCENTUAL_FINAL 
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND DELETED_AT IS NULL
                AND PERCENTUAL_FINAL > 0
            """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        ).fetchall()

        if not limites:
            logger.warning("Não foram encontrados limites de distribuição com percentuais definidos")
            return 0

        # Contar total de contratos disponíveis para distribuição
        total_contratos = db.session.execute(
            text("SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]")
        ).scalar() or 0

        logger.info(f"Total de contratos disponíveis para distribuição: {total_contratos}")

        if total_contratos == 0:
            logger.info("Nenhum contrato restante para distribuir")
            return 0

        # Preparar informações de distribuição por empresa
        empresas_info = []
        total_percentual = 0

        for id_empresa, percentual in limites:
            if percentual is not None:
                total_percentual += float(percentual)
                empresas_info.append({
                    'id_empresa': id_empresa,
                    'percentual': float(percentual),
                    'contratos_a_receber': 0,  # Será calculado
                    'contratos_atuais': 0  # Será calculado
                })

        # Normalizar percentuais se a soma não for 100%
        if abs(total_percentual - 100.0) > 0.01 and len(empresas_info) > 0:
            fator = 100.0 / total_percentual
            for empresa in empresas_info:
                empresa['percentual'] = empresa['percentual'] * fator

        # Consultar quantos contratos cada empresa já recebeu
        for empresa in empresas_info:
            contratos_atuais = db.session.execute(
                text("""
                    SELECT COUNT(*)
                    FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                    WHERE ID_EDITAL = :edital_id
                    AND ID_PERIODO = :periodo_id
                    AND COD_EMPRESA_COBRANCA = :id_empresa
                    AND DELETED_AT IS NULL
                """),
                {"edital_id": edital_id, "periodo_id": periodo_id, "id_empresa": empresa['id_empresa']}
            ).scalar() or 0

            empresa['contratos_atuais'] = contratos_atuais

        # Calcular contratos a receber baseado no percentual e no total
        total_contratos_a_distribuir = total_contratos  # Todos os contratos restantes

        for empresa in empresas_info:
            # Calcular quantos contratos a empresa deveria receber no total (atual + novo)
            total_ideal = int(round((empresa['percentual'] / 100.0) * (
                        total_contratos + sum(e['contratos_atuais'] for e in empresas_info))))

            # Quantos contratos adicionais a empresa deve receber
            empresa['contratos_a_receber'] = max(0, total_ideal - empresa['contratos_atuais'])

        # Ajustar se a soma dos contratos a receber for diferente do total disponível
        total_a_receber = sum(empresa['contratos_a_receber'] for empresa in empresas_info)

        if total_a_receber != total_contratos:
            # Distribuir a diferença pelas empresas com maior percentual
            diferenca = total_contratos - total_a_receber

            if diferenca > 0:  # Faltam contratos a distribuir
                # Ordenar empresas por percentual (maior primeiro)
                empresas_sorted = sorted(empresas_info, key=lambda x: x['percentual'], reverse=True)

                # Adicionar um contrato por vez às empresas com maior percentual
                idx = 0
                for _ in range(diferenca):
                    empresas_sorted[idx % len(empresas_sorted)]['contratos_a_receber'] += 1
                    idx += 1
            elif diferenca < 0:  # Excesso de contratos a distribuir
                # Ordenar empresas por percentual (menor primeiro)
                empresas_sorted = sorted(empresas_info, key=lambda x: x['percentual'])

                # Remover um contrato por vez das empresas com menor percentual
                idx = 0
                for _ in range(-diferenca):
                    if empresas_sorted[idx % len(empresas_sorted)]['contratos_a_receber'] > 0:
                        empresas_sorted[idx % len(empresas_sorted)]['contratos_a_receber'] -= 1
                    idx += 1

        # Agora distribuir os contratos para cada empresa
        logger.info("Iniciando a distribuição dos contratos por empresa")

        contratos_restantes = db.session.execute(
            text("""
                SELECT [FkContratoSISCTR], [NR_CPF_CNPJ], [VR_SD_DEVEDOR]
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                ORDER BY NEWID()  -- Ordem aleatória
            """)
        ).fetchall()

        # Lista para manter controle de quais contratos inserimos
        contratos_inseridos = []
        idx_contrato = 0

        # Para cada empresa, adicionar a quantidade calculada de contratos
        for empresa in empresas_info:
            contratos_empresa = 0

            # Inserir até a quantidade calculada para esta empresa
            while contratos_empresa < empresa['contratos_a_receber'] and idx_contrato < len(contratos_restantes):
                contrato = contratos_restantes[idx_contrato]

                db.session.execute(
                    text("""
                        INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
                        ([DT_REFERENCIA], [ID_EDITAL], [ID_PERIODO], [fkContratoSISCTR], 
                        [COD_EMPRESA_COBRANCA], [COD_CRITERIO_SELECAO], [NR_CPF_CNPJ], 
                        [VR_SD_DEVEDOR], [CREATED_AT])
                        VALUES (
                            GETDATE(),
                            :edital_id,
                            :periodo_id,
                            :contrato_id,
                            :empresa_id,
                            4, -- Código 4: Demais contratos sem acordo
                            :cpf_cnpj,
                            :valor,
                            GETDATE()
                        )
                    """),
                    {
                        "edital_id": edital_id,
                        "periodo_id": periodo_id,
                        "contrato_id": contrato[0],  # FkContratoSISCTR
                        "empresa_id": empresa['id_empresa'],
                        "cpf_cnpj": contrato[1],  # NR_CPF_CNPJ
                        "valor": contrato[2]  # VR_SD_DEVEDOR
                    }
                )

                contratos_inseridos.append(contrato[0])  # Adicionar à lista de contratos inseridos
                contratos_empresa += 1
                idx_contrato += 1
                contratos_distribuidos += 1

            logger.info(f"Empresa {empresa['id_empresa']}: {contratos_empresa} contratos distribuídos")

            # Commit a cada empresa para evitar longos bloqueios
            db.session.commit()

        # Remover contratos inseridos da tabela de distribuíveis
        if contratos_inseridos:
            # Criar placeholders para o WHERE IN
            placeholders = ",".join(f":c{i}" for i in range(len(contratos_inseridos)))
            params = {f"c{i}": contrato_id for i, contrato_id in enumerate(contratos_inseridos)}

            db.session.execute(
                text(f"DELETE FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] WHERE [FkContratoSISCTR] IN ({placeholders})"),
                params
            )
            db.session.commit()

        logger.info(f"Distribuição finalizada: {contratos_distribuidos} contratos distribuídos")
        return contratos_distribuidos

    except Exception as e:
        db.session.rollback()
        logger.error(f"Erro ao distribuir demais contratos: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 0


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
                FROM [DEV].[DCA_TB005_DISTRIBUICAO]
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
                    FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
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
                        INSERT INTO [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
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
                        UPDATE [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
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
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
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
        # CORREÇÃO: Agora recebemos diretamente um inteiro
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
            text("SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]")
        ).scalar()

        resultados['contratos_restantes'] = contratos_restantes
        resultados['total_com_restantes'] = resultados['total_distribuido'] + contratos_restantes

        # Atualizar limites de distribuição
        atualizar_limites_distribuicao(edital_id, periodo_id)

        return resultados

    except Exception as e:
        logging.error(f"Erro no processo de distribuição: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return resultados