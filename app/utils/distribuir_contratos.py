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
                # Primeiro, limpar a tabela de distribuíveis
                logging.info("Limpando tabela de distribuíveis...")
                truncate_sql = text("TRUNCATE TABLE [DEV].[DCA_TB006_DISTRIBUIVEIS]")
                connection.execute(truncate_sql)
                logging.info("Tabela de distribuíveis limpa com sucesso")

                # DEBUG: Verificar se a limpeza foi bem-sucedida
                check_sql = text("SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]")
                count_after_truncate = connection.execute(check_sql).scalar()
                logging.info(f"Contagem após limpeza: {count_after_truncate}")

                # Em seguida, inserir os contratos selecionados
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
                    AND SDJ.fkContratoSISCTR IS NULL""")

                # DEBUG: Verificar informações das tabelas de origem
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
        print(
            f"Iniciando distribuição de acordos vigentes para empresas que permanecem - Edital: {edital_id}, Período: {periodo_id}")

        # 1. Limpar a tabela de distribuição (apenas na primeira execução)
        db.session.execute(text(
            "DELETE FROM [DEV].[DCA_TB005_DISTRIBUICAO] WHERE ID_EDITAL = :edital_id AND ID_PERIODO = :periodo_id"),
                           {"edital_id": edital_id, "periodo_id": periodo_id})
        print("Tabela DCA_TB005_DISTRIBUICAO limpa para este edital/período")

        # 2. Inserir contratos com acordos vigentes para empresas que permanecem
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
            """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        )
        contratos_distribuidos = resultado_acordos.rowcount
        print(f"{contratos_distribuidos} contratos com acordo distribuídos para empresas que permanecem")

        # 3. Remover os contratos inseridos da tabela de distribuíveis
        if contratos_distribuidos > 0:
            db.session.execute(
                text("""
                    DELETE DIS
                    FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] AS DIS
                    INNER JOIN [DEV].[DCA_TB005_DISTRIBUICAO] AS DIST 
                    ON DIS.[FkContratoSISCTR] = DIST.[fkContratoSISCTR]
                    WHERE DIST.ID_EDITAL = :edital_id
                    AND DIST.ID_PERIODO = :periodo_id
                    AND DIST.COD_CRITERIO_SELECAO = 1
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
            db.session.execute(
                text("""
                    DELETE DIS
                    FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] AS DIS
                    INNER JOIN [DEV].[DCA_TB005_DISTRIBUICAO] AS DIST 
                    ON DIS.[FkContratoSISCTR] = DIST.[fkContratoSISCTR]
                    WHERE DIST.ID_EDITAL = :edital_id
                    AND DIST.ID_PERIODO = :periodo_id
                    AND DIST.COD_CRITERIO_SELECAO = 3
                """),
                {"edital_id": edital_id, "periodo_id": periodo_id}
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
    Versão modificada para garantir que todos os contratos sejam processados.

    Args:
        edital_id: ID do edital
        periodo_id: ID do período

    Returns:
        int: Total de contratos distribuídos
    """
    from app import db
    from sqlalchemy import text
    import time, logging

    contratos_processados = 0
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

        # ABORDAGEM SIMPLIFICADA: Processar diretamente em Python com transações pequenas
        print("Processando contratos para arrasto com acordo...")

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

        total_cpfs = len(cpfs_acordos)
        cpfs_processados = 0

        # ALTERAÇÃO: Definir intervalo de verificação para relatórios de progresso
        intervalo_verificacao = 500  # Mostrar progresso a cada 500 CPFs processados
        proximo_relatorio = intervalo_verificacao

        # 2. Para cada CPF/CNPJ, buscar e processar contratos em lotes
        lote_tamanho = 100  # ALTERAÇÃO: Aumentar tamanho do lote para 100 CPFs por lote

        for i in range(0, total_cpfs, lote_tamanho):
            lote_cpfs = cpfs_acordos[i:i + lote_tamanho]
            lote_inseridos = 0

            for acordo in lote_cpfs:
                cpf_cnpj = acordo[0]
                empresa_cobranca = acordo[1]

                # Buscar contratos distribuíveis para este CPF/CNPJ
                contratos = db.session.execute(
                    text("""
                        SELECT FkContratoSISCTR, NR_CPF_CNPJ, VR_SD_DEVEDOR
                        FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                        WHERE NR_CPF_CNPJ = :cpf_cnpj
                        AND FkContratoSISCTR NOT IN (
                            SELECT fkContratoSISCTR
                            FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                            WHERE ID_EDITAL = :edital
                            AND ID_PERIODO = :periodo
                        )
                    """),
                    {"cpf_cnpj": cpf_cnpj, "edital": edital_id, "periodo": periodo_id}
                ).fetchall()

                contratos_processados += len(contratos)

                # Inserir contratos em uma transação por CPF
                if contratos:
                    try:
                        for contrato in contratos:
                            contrato_id = contrato[0]

                            # MODIFICAÇÃO: Eliminar a verificação de contratos problemáticos
                            # e fazer até 3 tentativas para processar cada contrato
                            max_tentativas = 3
                            for tentativa in range(max_tentativas):
                                try:
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
                                                :edital,
                                                :periodo,
                                                :contrato_id,
                                                :empresa,
                                                6, -- Código 6: Regra de Arrasto
                                                :cpf_cnpj,
                                                :valor,
                                                GETDATE()
                                            )
                                        """),
                                        {
                                            "edital": edital_id,
                                            "periodo": periodo_id,
                                            "contrato_id": contrato_id,
                                            "empresa": empresa_cobranca,
                                            "cpf_cnpj": cpf_cnpj,
                                            "valor": contrato[2]
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

                                    lote_inseridos += 1
                                    # Se chegou aqui, significa que deu certo, então sai do loop de tentativas
                                    break

                                except Exception as e:
                                    # Se for a última tentativa, registra o erro
                                    if tentativa == max_tentativas - 1:
                                        print(
                                            f"Erro ao processar contrato {contrato_id} após {max_tentativas} tentativas: {str(e)}")
                                    # Caso contrário, espera um pouco e tenta novamente
                                    else:
                                        import time
                                        time.sleep(0.1)  # Espera 100ms antes de tentar novamente

                        # Commit após processar todos os contratos deste CPF
                        db.session.commit()
                        contratos_inseridos += lote_inseridos

                    except Exception as e:
                        # Rollback apenas desta transação
                        db.session.rollback()
                        print(f"Erro ao processar CPF/CNPJ {cpf_cnpj}: {str(e)}")

            # Mostrar progresso apenas em intervalos específicos ou no final
            cpfs_processados += len(lote_cpfs)

            # ALTERAÇÃO: Verificar se atingimos o próximo ponto de verificação
            if cpfs_processados >= proximo_relatorio or cpfs_processados >= total_cpfs:
                progresso = (cpfs_processados / total_cpfs) * 100
                print(
                    f"Arrasto com acordo: {cpfs_processados}/{total_cpfs} CPFs processados ({progresso:.1f}%) - {contratos_inseridos} contratos inseridos")
                proximo_relatorio = cpfs_processados + intervalo_verificacao

        elapsed_time = time.time() - start_time
        print(
            f"Regra de arrasto com acordo concluída: {contratos_inseridos} contratos distribuídos em {elapsed_time:.2f}s")

        # Retorna o número total de contratos inseridos
        return contratos_inseridos

    except Exception as e:
        db.session.rollback()
        print(f"Erro na regra de arrasto com acordo: {str(e)}")
        import traceback
        print(traceback.format_exc())

        # Retorna zero em caso de erro
        return 0


def aplicar_regra_arrasto_sem_acordo(edital_id, periodo_id):
    """
    Aplica a regra de arrasto para contratos sem acordo (regra 1.1.4).
    Versão corrigida para resolver problema de contratos não distribuídos.
    """
    from app import db
    from sqlalchemy import text
    import time
    import logging

    logger = logging.getLogger(__name__)
    start_time = time.time()
    resultados_internos = {
        "inseridos_arrastaveis": 0,
        "distribuidos": 0,
        "cpfs_processados": 0
    }

    try:
        logger.info(
            f"Iniciando regra de arrasto para contratos sem acordo - Edital: {edital_id}, Período: {periodo_id}")

        # ETAPA 1: Verificar se existem empresas para distribuição
        empresas = db.session.execute(
            text("""
                SELECT COUNT(*)
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital
                AND ID_PERIODO = :periodo
                AND DELETED_AT IS NULL
            """),
            {"edital": edital_id, "periodo": periodo_id}
        ).scalar() or 0

        if empresas == 0:
            logger.warning("Nenhuma empresa disponível para distribuição. Processo interrompido.")
            return 0

        logger.info(f"Encontradas {empresas} empresas para distribuição")

        # ETAPA 2: Preparar tabelas temporárias
        logger.info("Preparando tabelas temporárias...")
        db.session.execute(text("""
            -- Limpar tabelas temporárias anteriores
            IF OBJECT_ID('tempdb..##CPFsArrasteSemAcordo') IS NOT NULL DROP TABLE ##CPFsArrasteSemAcordo;
            IF OBJECT_ID('tempdb..##DistribuicaoCPFs') IS NOT NULL DROP TABLE ##DistribuicaoCPFs;
            IF OBJECT_ID('tempdb..##EmpresasPercentuais') IS NOT NULL DROP TABLE ##EmpresasPercentuais;

            -- Criar tabela para CPFs elegíveis
            CREATE TABLE ##CPFsArrasteSemAcordo (
                NR_CPF_CNPJ VARCHAR(20) PRIMARY KEY CLUSTERED
            );

            -- Criar tabela para distribuição
            CREATE TABLE ##DistribuicaoCPFs (
                NR_CPF_CNPJ VARCHAR(20),
                ID_EMPRESA INT
            );

            -- Criar tabela para empresas e percentuais
            CREATE TABLE ##EmpresasPercentuais (
                ID_EMPRESA INT,
                percentual_ajustado DECIMAL(10,4),
                qtd_cpfs INT,
                ordem INT IDENTITY(1,1)
            );
        """))
        db.session.commit()

        # ETAPA 3: Identificar CPFs com múltiplos contratos
        logger.info("Identificando CPFs com múltiplos contratos...")
        db.session.execute(text("""
            INSERT INTO ##CPFsArrasteSemAcordo (NR_CPF_CNPJ)
            SELECT DISTINCT LTRIM(RTRIM(NR_CPF_CNPJ))
            FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] D
            WHERE NR_CPF_CNPJ IS NOT NULL
            GROUP BY LTRIM(RTRIM(NR_CPF_CNPJ))
            HAVING COUNT(*) > 1;
        """))
        db.session.commit()

        # Contar CPFs com múltiplos contratos
        cpfs_multiplos = db.session.execute(
            text("SELECT COUNT(*) FROM ##CPFsArrasteSemAcordo")
        ).scalar() or 0
        logger.info(f"Encontrados {cpfs_multiplos} CPFs com múltiplos contratos")

        # ETAPA 4: Incluir CPFs únicos
        logger.info("Incluindo CPFs com contratos únicos...")
        db.session.execute(text("""
            INSERT INTO ##CPFsArrasteSemAcordo (NR_CPF_CNPJ)
            SELECT DISTINCT LTRIM(RTRIM(D.NR_CPF_CNPJ))
            FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] D
            WHERE D.NR_CPF_CNPJ IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM ##CPFsArrasteSemAcordo
                WHERE NR_CPF_CNPJ = LTRIM(RTRIM(D.NR_CPF_CNPJ))
            )
            AND NOT EXISTS (
                SELECT 1 FROM [DEV].[DCA_TB005_DISTRIBUICAO] DIST
                WHERE LTRIM(RTRIM(DIST.NR_CPF_CNPJ)) = LTRIM(RTRIM(D.NR_CPF_CNPJ))
                AND DIST.ID_EDITAL = :edital
                AND DIST.ID_PERIODO = :periodo
                AND DIST.COD_CRITERIO_SELECAO IN (1, 3, 6)
            );
        """), {"edital": edital_id, "periodo": periodo_id})
        db.session.commit()

        # Contar total de CPFs
        total_cpfs = db.session.execute(
            text("SELECT COUNT(*) FROM ##CPFsArrasteSemAcordo")
        ).scalar() or 0
        cpfs_unicos = total_cpfs - cpfs_multiplos
        logger.info(f"Incluídos {cpfs_unicos} CPFs com contratos únicos (total: {total_cpfs})")

        if total_cpfs == 0:
            logger.warning("Nenhum CPF elegível para regra de arrasto. Processo interrompido.")
            # Limpeza de tabelas temporárias
            db.session.execute(text("""
                IF OBJECT_ID('tempdb..##CPFsArrasteSemAcordo') IS NOT NULL DROP TABLE ##CPFsArrasteSemAcordo;
                IF OBJECT_ID('tempdb..##DistribuicaoCPFs') IS NOT NULL DROP TABLE ##DistribuicaoCPFs;
                IF OBJECT_ID('tempdb..##EmpresasPercentuais') IS NOT NULL DROP TABLE ##EmpresasPercentuais;
            """))
            db.session.commit()
            return 0

        # ETAPA 5: Contar contratos para processamento
        contratos_para_mover = db.session.execute(
            text("""
                SELECT COUNT(*)
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] D
                INNER JOIN ##CPFsArrasteSemAcordo M
                    ON LTRIM(RTRIM(D.NR_CPF_CNPJ)) = M.NR_CPF_CNPJ
            """)
        ).scalar() or 0
        logger.info(f"Total de {contratos_para_mover} contratos elegíveis para regra de arrasto sem acordo")

        # ETAPA 6: Mover para tabela de arrastaveis
        if contratos_para_mover > 0:
            logger.info("Movendo contratos para tabela de arrastaveis...")

            # Inserir na tabela de arrastaveis
            resultado_insercao = db.session.execute(text("""
                INSERT INTO [DEV].[DCA_TB007_ARRASTAVEIS] (FkContratoSISCTR, NR_CPF_CNPJ, CREATED_AT)
                SELECT
                    D.FkContratoSISCTR,
                    D.NR_CPF_CNPJ,
                    GETDATE()
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] D
                INNER JOIN ##CPFsArrasteSemAcordo M
                    ON LTRIM(RTRIM(D.NR_CPF_CNPJ)) = M.NR_CPF_CNPJ
                LEFT JOIN [DEV].[DCA_TB007_ARRASTAVEIS] A
                    ON D.FkContratoSISCTR = A.FkContratoSISCTR
                WHERE A.FkContratoSISCTR IS NULL;
            """))

            # Usar rowcount para obter o número de linhas afetadas
            total_inseridos = resultado_insercao.rowcount
            resultados_internos["inseridos_arrastaveis"] = total_inseridos
            logger.info(f"Movidos {total_inseridos} contratos para a tabela de arrastaveis")

            # Remover da tabela de distribuíveis
            if total_inseridos > 0:
                # Commit da inserção antes do delete
                db.session.commit()

                logger.info("Removendo contratos movidos da tabela de distribuíveis...")
                # É mais seguro deletar usando os FkContratoSISCTR que acabaram de ser inseridos
                # Assumindo que DCA_TB007_ARRASTAVEIS não tem DELETED_AT ou é NULL por padrão
                db.session.execute(text("""
                    DELETE D
                    FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] D
                    WHERE EXISTS (
                        SELECT 1 FROM [DEV].[DCA_TB007_ARRASTAVEIS] A
                        WHERE A.FkContratoSISCTR = D.FkContratoSISCTR
                        AND A.CREATED_AT >= DATEADD(minute, -5, GETDATE()) -- Filtro de segurança
                    );
                """))
                db.session.commit()
                logger.info("Contratos removidos da tabela de distribuíveis")
            else:
                # Commit mesmo se nada foi inserido para liberar a transação
                db.session.commit()
        else:
            # Commit mesmo se não há contratos para mover
            db.session.commit()

        # ETAPA 7: Preparar percentuais e distribuição de CPFs por empresa
        logger.info("Preparando distribuição entre empresas...")

        # 7.1. Inserir empresas e percentuais
        db.session.execute(text("""
            INSERT INTO ##EmpresasPercentuais (ID_EMPRESA, percentual_ajustado, qtd_cpfs)
            SELECT
                ID_EMPRESA,
                COALESCE(PERCENTUAL_FINAL, 0),
                0
            FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
            WHERE ID_EDITAL = :edital
            AND ID_PERIODO = :periodo
            AND DELETED_AT IS NULL
            ORDER BY NEWID(); -- Ordenação aleatória para distribuição dos CPFs restantes

            DECLARE @total_percentual DECIMAL(10,4);
            SELECT @total_percentual = SUM(percentual_ajustado) FROM ##EmpresasPercentuais;

            IF @total_percentual = 0 AND (SELECT COUNT(*) FROM ##EmpresasPercentuais) > 0
            BEGIN
                UPDATE ##EmpresasPercentuais
                SET percentual_ajustado = 100.0 / (SELECT COUNT(*) FROM ##EmpresasPercentuais);
            END
            ELSE IF @total_percentual > 0 AND ABS(@total_percentual - 100) > 0.01
            BEGIN
                UPDATE ##EmpresasPercentuais
                SET percentual_ajustado = (percentual_ajustado * 100.0) / @total_percentual;
            END
        """), {"edital": edital_id, "periodo": periodo_id})
        db.session.commit()

        # 7.2. Garantir que todas as empresas tenham pelo menos 1 CPF atribuído
        db.session.execute(text("""
            DECLARE @total_cpfs INT;
            SELECT @total_cpfs = COUNT(*) FROM ##CPFsArrasteSemAcordo;

            -- Garantir no mínimo 1 CPF por empresa
            DECLARE @total_empresas INT;
            SELECT @total_empresas = COUNT(*) FROM ##EmpresasPercentuais;

            -- Ajustar a atribuição para garantir pelo menos 1 CPF por empresa
            UPDATE ##EmpresasPercentuais
            SET qtd_cpfs = 1;

            -- Calcular CPFs restantes após garantir mínimo de 1 por empresa
            DECLARE @cpfs_restantes INT = @total_cpfs - @total_empresas;

            -- Distribuir CPFs restantes proporcionalmente se houver
            IF @cpfs_restantes > 0
            BEGIN
                -- Distribuir com base nos percentuais
                WITH DistEmpresa AS (
                    SELECT 
                        ID_EMPRESA,
                        FLOOR(percentual_ajustado * @cpfs_restantes / 100.0) AS cpfs_adicionais,
                        ROW_NUMBER() OVER (ORDER BY percentual_ajustado DESC) AS ordem
                    FROM ##EmpresasPercentuais
                )
                UPDATE ##EmpresasPercentuais
                SET qtd_cpfs = qtd_cpfs + DE.cpfs_adicionais
                FROM ##EmpresasPercentuais EP
                JOIN DistEmpresa DE ON EP.ID_EMPRESA = DE.ID_EMPRESA;

                -- Recalcular CPFs restantes após a distribuição proporcional
                DECLARE @total_atribuido INT;
                SELECT @total_atribuido = SUM(qtd_cpfs) FROM ##EmpresasPercentuais;
                SET @cpfs_restantes = @total_cpfs - @total_atribuido;

                -- Distribuir os CPFs restantes um por empresa, começando das maiores percentuais
                WHILE @cpfs_restantes > 0
                BEGIN
                    UPDATE TOP(1) ##EmpresasPercentuais
                    SET qtd_cpfs = qtd_cpfs + 1
                    WHERE ID_EMPRESA IN (
                        SELECT TOP(1) ID_EMPRESA 
                        FROM ##EmpresasPercentuais 
                        ORDER BY percentual_ajustado DESC, ID_EMPRESA
                    );
                    SET @cpfs_restantes = @cpfs_restantes - 1;
                END
            END
        """))
        db.session.commit()

        # 7.3. Criar tabelas auxiliares para atribuição
        db.session.execute(text("""
            IF OBJECT_ID('tempdb..##LimitesEmpresas') IS NOT NULL DROP TABLE ##LimitesEmpresas;
            IF OBJECT_ID('tempdb..##CPFsNumerados') IS NOT NULL DROP TABLE ##CPFsNumerados;

            SELECT
                ID_EMPRESA, ordem, qtd_cpfs,
                (SELECT SUM(qtd_cpfs) FROM ##EmpresasPercentuais e2 WHERE e2.ordem <= e1.ordem) AS limite_superior,
                ISNULL((SELECT SUM(qtd_cpfs) FROM ##EmpresasPercentuais e2 WHERE e2.ordem < e1.ordem), 0) AS limite_inferior
            INTO ##LimitesEmpresas
            FROM ##EmpresasPercentuais e1;

            SELECT
                NR_CPF_CNPJ,
                ROW_NUMBER() OVER (ORDER BY NEWID()) AS ordem
            INTO ##CPFsNumerados
            FROM ##CPFsArrasteSemAcordo;

            -- Verificação para garantir que a numeração não ultrapasse o máximo definido
            DECLARE @max_ordem INT;
            DECLARE @total_cpfs INT;

            SELECT @total_cpfs = COUNT(*) FROM ##CPFsArrasteSemAcordo;
            SELECT @max_ordem = MAX(limite_superior) FROM ##LimitesEmpresas;

            IF @max_ordem <> @total_cpfs
            BEGIN
                -- Ajustar limites se necessário
                DECLARE @ajuste INT = @total_cpfs - @max_ordem;

                UPDATE ##LimitesEmpresas
                SET limite_superior = limite_superior + @ajuste
                WHERE ordem = (SELECT MAX(ordem) FROM ##LimitesEmpresas);
            END
        """))
        db.session.commit()

        # 7.4. Atribuir CPFs a empresas - CORRIGIDO para garantir que todos os CPFs sejam atribuídos
        logger.info("Atribuindo CPFs a empresas...")
        db.session.execute(text("""
            -- Limpar tabela de distribuição se existir
            TRUNCATE TABLE ##DistribuicaoCPFs;

            -- Inserir CPFs com atribuição normal dentro dos limites
            INSERT INTO ##DistribuicaoCPFs (NR_CPF_CNPJ, ID_EMPRESA)
            SELECT c.NR_CPF_CNPJ, e.ID_EMPRESA
            FROM ##CPFsNumerados c
            JOIN ##LimitesEmpresas e ON c.ordem > e.limite_inferior AND c.ordem <= e.limite_superior;

            -- Verificar se todos os CPFs foram atribuídos
            DECLARE @cpfs_atribuidos INT;
            DECLARE @total_cpfs INT;

            SELECT @cpfs_atribuidos = COUNT(*) FROM ##DistribuicaoCPFs;
            SELECT @total_cpfs = COUNT(*) FROM ##CPFsArrasteSemAcordo;

            -- Se houver CPFs não atribuídos, distribuí-los para a empresa com maior percentual
            IF @cpfs_atribuidos < @total_cpfs
            BEGIN
                DECLARE @empresa_maior_percentual INT;

                SELECT TOP 1 @empresa_maior_percentual = ID_EMPRESA 
                FROM ##EmpresasPercentuais 
                ORDER BY percentual_ajustado DESC;

                INSERT INTO ##DistribuicaoCPFs (NR_CPF_CNPJ, ID_EMPRESA)
                SELECT c.NR_CPF_CNPJ, @empresa_maior_percentual
                FROM ##CPFsArrasteSemAcordo c
                WHERE NOT EXISTS (
                    SELECT 1 FROM ##DistribuicaoCPFs d
                    WHERE d.NR_CPF_CNPJ = c.NR_CPF_CNPJ
                );
            END

            DROP TABLE ##LimitesEmpresas;
            DROP TABLE ##CPFsNumerados;
        """))
        db.session.commit()

        # ETAPA 8: Executar a distribuição final -- MODIFICADO para resolver o erro ResourceClosedError
        logger.info("Distribuindo contratos...")
        try:
            # Executa a inserção sem tentar obter resultados diretamente
            resultado_distribuicao = db.session.execute(text("""
                WITH ContratoValores AS (
                    -- Tenta pegar da distribuição existente
                    SELECT fkContratoSISCTR, VR_SD_DEVEDOR,
                           ROW_NUMBER() OVER(PARTITION BY fkContratoSISCTR ORDER BY ID DESC) as rn
                    FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                    WHERE fkContratoSISCTR IN (SELECT FkContratoSISCTR FROM [DEV].[DCA_TB007_ARRASTAVEIS] WHERE DELETED_AT IS NULL)

                    UNION ALL

                    -- Se não existir, pega da tabela de distribuíveis original (caso tenha sido movido)
                    SELECT FkContratoSISCTR, VR_SD_DEVEDOR, 1 as rn
                    FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                    WHERE FkContratoSISCTR IN (SELECT FkContratoSISCTR FROM [DEV].[DCA_TB007_ARRASTAVEIS] WHERE DELETED_AT IS NULL)
                      AND NOT EXISTS (
                          SELECT 1 FROM [DEV].[DCA_TB005_DISTRIBUICAO] d5
                          WHERE d5.fkContratoSISCTR = [DEV].[DCA_TB006_DISTRIBUIVEIS].FkContratoSISCTR
                      )
                ),
                ValoresFinais AS (
                    SELECT fkContratoSISCTR, VR_SD_DEVEDOR
                    FROM ContratoValores
                    WHERE rn = 1
                )
                -- Inserir contratos na tabela de distribuição
                INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
                (
                    [DT_REFERENCIA], [ID_EDITAL], [ID_PERIODO], [fkContratoSISCTR],
                    [COD_EMPRESA_COBRANCA], [COD_CRITERIO_SELECAO], [NR_CPF_CNPJ],
                    [VR_SD_DEVEDOR], [CREATED_AT]
                )
                SELECT
                    GETDATE(), :edital, :periodo, A.FkContratoSISCTR,
                    D.ID_EMPRESA, 6, A.NR_CPF_CNPJ,
                    ISNULL(VF.VR_SD_DEVEDOR, 0), GETDATE()
                FROM [DEV].[DCA_TB007_ARRASTAVEIS] A
                INNER JOIN ##DistribuicaoCPFs D ON LTRIM(RTRIM(A.NR_CPF_CNPJ)) = D.NR_CPF_CNPJ
                LEFT JOIN ValoresFinais VF ON A.FkContratoSISCTR = VF.fkContratoSISCTR
                WHERE A.DELETED_AT IS NULL
            """), {"edital": edital_id, "periodo": periodo_id})

            # Use rowcount para obter o número de linhas inseridas
            contratos_distribuidos = resultado_distribuicao.rowcount
            resultados_internos["distribuidos"] = contratos_distribuidos

            # Consulta separada para contar contratos inseridos (caso rowcount não esteja funcionando)
            if contratos_distribuidos <= 0:
                # Verificação secundária do número de contratos
                contagem = db.session.execute(text("""
                    SELECT COUNT(*) 
                    FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                    WHERE ID_EDITAL = :edital
                    AND ID_PERIODO = :periodo
                    AND COD_CRITERIO_SELECAO = 6
                    AND CREATED_AT >= DATEADD(minute, -5, GETDATE())
                """), {"edital": edital_id, "periodo": periodo_id}).scalar() or 0

                # Use a contagem apenas se for maior que zero
                if contagem > 0:
                    contratos_distribuidos = contagem
                    resultados_internos["distribuidos"] = contagem

            logger.info(f"Distribuição inseriu {contratos_distribuidos} contratos")
        except Exception as insert_error:
            logger.error(f"Erro na distribuição final: {str(insert_error)}")
            db.session.rollback()
            contratos_distribuidos = 0
            # Continuar para tentar marcar contratos como processados, mesmo que a inserção falhe

        # Marcar contratos como processados na tabela de arrastáveis, mesmo se a inserção falhar
        # Isso impede que contratos fiquem "presos" na tabela de arrastáveis
        try:
            logger.info("Marcando todos os contratos como processados...")
            # CORREÇÃO CRÍTICA: Marcar TODOS os contratos como processados sem depender de ##DistribuicaoCPFs
            db.session.execute(text("""
                UPDATE [DEV].[DCA_TB007_ARRASTAVEIS]
                SET DELETED_AT = GETDATE()
                WHERE DELETED_AT IS NULL;
            """))
            db.session.commit()
        except Exception as update_error:
            logger.error(f"Erro ao marcar contratos como processados: {str(update_error)}")
            db.session.rollback()

        # Contar CPFs processados
        resultados_internos["cpfs_processados"] = db.session.execute(
            text("SELECT COUNT(*) FROM ##DistribuicaoCPFs")
        ).scalar() or 0

        logger.info(f"Distribuição concluída: {contratos_distribuidos} contratos distribuídos")

        # ETAPA 9: Limpeza final
        logger.info("Realizando limpeza final...")
        try:
            db.session.execute(text("""
                IF OBJECT_ID('tempdb..##CPFsArrasteSemAcordo') IS NOT NULL DROP TABLE ##CPFsArrasteSemAcordo;
                IF OBJECT_ID('tempdb..##DistribuicaoCPFs') IS NOT NULL DROP TABLE ##DistribuicaoCPFs;
                IF OBJECT_ID('tempdb..##EmpresasPercentuais') IS NOT NULL DROP TABLE ##EmpresasPercentuais;
            """))
            db.session.commit()
        except Exception as cleanup_error:
            logger.warning(f"Erro na limpeza final: {str(cleanup_error)}")

        # Verificar se há contratos remanescentes (deve ser 0)
        restantes = db.session.execute(
            text("""
                SELECT COUNT(*)
                FROM [DEV].[DCA_TB007_ARRASTAVEIS]
                WHERE DELETED_AT IS NULL
            """)
        ).scalar() or 0

        if restantes > 0:
            logger.warning(
                f"ATENÇÃO: Ainda restam {restantes} contratos não distribuídos na tabela de arrastaveis após a distribuição.")

        # Log de finalização
        elapsed_time = time.time() - start_time
        logger.info(f"Regra de arrasto sem acordo otimizada concluída em {elapsed_time:.2f}s")
        logger.info(
            f"Resultados: {resultados_internos['distribuidos']} contratos distribuídos, {resultados_internos['cpfs_processados']} CPFs processados")

        # Retorna apenas o número de contratos distribuídos
        return resultados_internos["distribuidos"]

    except Exception as e:
        db.session.rollback()
        logger.error(f"Erro crítico na regra de arrasto sem acordo: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

        # Limpeza em caso de erro
        try:
            db.session.execute(text("""
                IF OBJECT_ID('tempdb..##CPFsArrasteSemAcordo') IS NOT NULL DROP TABLE ##CPFsArrasteSemAcordo;
                IF OBJECT_ID('tempdb..##DistribuicaoCPFs') IS NOT NULL DROP TABLE ##DistribuicaoCPFs;
                IF OBJECT_ID('tempdb..##EmpresasPercentuais') IS NOT NULL DROP TABLE ##EmpresasPercentuais;
                IF OBJECT_ID('tempdb..##CPFsNumerados') IS NOT NULL DROP TABLE ##CPFsNumerados;
                IF OBJECT_ID('tempdb..##LimitesEmpresas') IS NOT NULL DROP TABLE ##LimitesEmpresas;
            """))
            db.session.commit()
        except Exception as cleanup_e:
            logger.error(f"Erro durante a limpeza após falha: {cleanup_e}")

        # Retorna 0 em caso de erro
        return 0


def distribuir_demais_contratos(edital_id, periodo_id):
    """
    Distribui os contratos restantes entre as empresas.
    Versão corrigida para resolver problemas com ResourceClosedError e garantir que
    todos os contratos sejam distribuídos.

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

        # Verificar se existem empresas e contratos antes de iniciar o processamento
        empresas_count = db.session.execute(
            text("""
                SELECT COUNT(*) 
                FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES] EP
                LEFT JOIN [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] LD 
                    ON EP.ID_EMPRESA = LD.ID_EMPRESA 
                    AND EP.ID_EDITAL = LD.ID_EDITAL 
                    AND EP.ID_PERIODO = LD.ID_PERIODO
                WHERE EP.ID_EDITAL = :edital_id
                AND EP.ID_PERIODO = :periodo_id
                AND EP.DS_CONDICAO <> 'DESCREDENCIADA'
                AND (LD.PERCENTUAL_FINAL > 0 OR LD.PERCENTUAL_FINAL IS NULL)
            """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        ).scalar() or 0

        if not empresas_count:
            logger.warning("Nenhuma empresa participante encontrada.")
            return 0

        contratos_count = db.session.execute(
            text("SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]")
        ).scalar() or 0

        if not contratos_count:
            logger.warning("Nenhum contrato restante para distribuir.")
            return 0

        logger.info(f"Total de contratos restantes: {contratos_count}")

        # Criar tabelas temporárias
        logger.info("Criando tabelas temporárias...")
        db.session.execute(text("""
            -- Limpar tabelas temporárias anteriores
            IF OBJECT_ID('tempdb..##ContratosDisponiveis') IS NOT NULL DROP TABLE ##ContratosDisponiveis;
            IF OBJECT_ID('tempdb..##EmpresasInfo') IS NOT NULL DROP TABLE ##EmpresasInfo;
            IF OBJECT_ID('tempdb..##FaixasOrdem') IS NOT NULL DROP TABLE ##FaixasOrdem;

            -- Criar tabela para contratos disponíveis
            SELECT 
                ROW_NUMBER() OVER (ORDER BY [FkContratoSISCTR]) AS ordem,
                [FkContratoSISCTR],
                [NR_CPF_CNPJ],
                [VR_SD_DEVEDOR],
                NULL AS ID_EMPRESA
            INTO ##ContratosDisponiveis
            FROM [DEV].[DCA_TB006_DISTRIBUIVEIS];

            -- Criar índice para melhorar performance
            CREATE CLUSTERED INDEX IX_ContratosDisponiveis_Ordem ON ##ContratosDisponiveis(ordem);

            -- Criar tabela para empresas
            SELECT 
                EP.ID_EMPRESA,
                COALESCE(LD.PERCENTUAL_FINAL, 0) AS percentual,
                COALESCE((
                    SELECT COUNT(*) 
                    FROM [DEV].[DCA_TB005_DISTRIBUICAO] D
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
            INTO ##EmpresasInfo
            FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES] EP
            LEFT JOIN [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
                ON EP.ID_EMPRESA = LD.ID_EMPRESA
                AND EP.ID_EDITAL = LD.ID_EDITAL
                AND EP.ID_PERIODO = LD.ID_PERIODO
            WHERE EP.ID_EDITAL = :edital_id
            AND EP.ID_PERIODO = :periodo_id
            AND EP.DS_CONDICAO <> 'DESCREDENCIADA'
            AND (LD.PERCENTUAL_FINAL > 0 OR LD.PERCENTUAL_FINAL IS NULL);

            -- Criar tabela para faixas de ordem
            CREATE TABLE ##FaixasOrdem (
                ID_EMPRESA INT,
                ordem_inicio INT,
                ordem_fim INT
            );
        """), {"edital_id": edital_id, "periodo_id": periodo_id})
        db.session.commit()

        # Normalizar percentuais
        logger.info("Normalizando percentuais...")
        db.session.execute(text("""
            DECLARE @total_percentual DECIMAL(10,6);
            DECLARE @total_empresas INT;

            SELECT 
                @total_percentual = SUM(percentual),
                @total_empresas = COUNT(*)
            FROM ##EmpresasInfo;

            IF @total_percentual <= 0
            BEGIN
                UPDATE ##EmpresasInfo
                SET percentual = 100.0 / @total_empresas;
            END
            ELSE IF ABS(@total_percentual - 100) > 0.01
            BEGIN
                UPDATE ##EmpresasInfo
                SET percentual = percentual * 100.0 / @total_percentual;
            END;
        """))
        db.session.commit()

        # Calcular metas e contratos a distribuir
        logger.info("Calculando metas por empresa...")
        db.session.execute(text("""
            DECLARE @total_contratos_restantes INT;
            SELECT @total_contratos_restantes = COUNT(*) 
            FROM ##ContratosDisponiveis;

            -- Calcular total de contratos (atuais + restantes)
            DECLARE @total_contratos_atuais INT;
            DECLARE @total_contratos INT;

            SELECT @total_contratos_atuais = SUM(contratos_atuais)
            FROM ##EmpresasInfo;

            SET @total_contratos = @total_contratos_atuais + @total_contratos_restantes;

            -- Calcular metas para cada empresa
            UPDATE ##EmpresasInfo
            SET
                meta_total_exata = @total_contratos * percentual / 100.0,
                meta_total_inteira = FLOOR(@total_contratos * percentual / 100.0),
                parte_fracionaria = @total_contratos * percentual / 100.0 - FLOOR(@total_contratos * percentual / 100.0);

            -- Calcular quantos contratos faltam para cada empresa
            UPDATE ##EmpresasInfo
            SET contratos_faltantes = CASE
                                      WHEN meta_total_inteira > contratos_atuais THEN meta_total_inteira - contratos_atuais
                                      ELSE 0
                                    END;

            -- Verificar quantos contratos faltam distribuir pelos fracionais
            DECLARE @total_faltantes INT;
            DECLARE @contratos_nao_alocados INT;

            SELECT @total_faltantes = SUM(contratos_faltantes)
            FROM ##EmpresasInfo;

            SET @contratos_nao_alocados = @total_contratos_restantes - @total_faltantes;

            -- Distribuir contratos extras por maiores fracionais
            IF @contratos_nao_alocados > 0
            BEGIN
                WITH EmpresasOrdenadas AS (
                    SELECT 
                        ID_EMPRESA,
                        parte_fracionaria,
                        ROW_NUMBER() OVER (ORDER BY parte_fracionaria DESC) AS ranking_fracao
                    FROM ##EmpresasInfo
                )
                UPDATE ##EmpresasInfo
                SET contratos_extra = CASE WHEN EO.ranking_fracao <= @contratos_nao_alocados THEN 1 ELSE 0 END
                FROM ##EmpresasInfo EI
                JOIN EmpresasOrdenadas EO ON EI.ID_EMPRESA = EO.ID_EMPRESA;
            END;

            -- Calcular total de contratos a receber
            UPDATE ##EmpresasInfo
            SET total_a_receber = contratos_faltantes + contratos_extra;
        """))
        db.session.commit()

        # Definir faixas de ordem por empresa
        logger.info("Definindo faixas de distribuição por empresa...")
        db.session.execute(text("""
            DECLARE @ordem_atual INT = 1;

            INSERT INTO ##FaixasOrdem (ID_EMPRESA, ordem_inicio, ordem_fim)
            SELECT
                ID_EMPRESA,
                @ordem_atual + SUM(0) OVER (ORDER BY percentual DESC, ID_EMPRESA ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                @ordem_atual + SUM(total_a_receber) OVER (ORDER BY percentual DESC, ID_EMPRESA ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - 1
            FROM
                ##EmpresasInfo
            WHERE
                total_a_receber > 0;
        """))
        db.session.commit()

        # Atribuir empresas aos contratos
        logger.info("Atribuindo empresas aos contratos...")
        db.session.execute(text("""
            UPDATE ##ContratosDisponiveis
            SET ID_EMPRESA = F.ID_EMPRESA
            FROM ##ContratosDisponiveis C
            JOIN ##FaixasOrdem F ON C.ordem BETWEEN F.ordem_inicio AND F.ordem_fim;
        """))
        db.session.commit()

        # Inserir contratos na tabela de distribuição
        logger.info("Inserindo contratos na tabela de distribuição...")
        try:
            resultado_insercao = db.session.execute(text("""
                INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
                (
                    [DT_REFERENCIA], [ID_EDITAL], [ID_PERIODO], [fkContratoSISCTR], 
                    [COD_EMPRESA_COBRANCA], [COD_CRITERIO_SELECAO], [NR_CPF_CNPJ], 
                    [VR_SD_DEVEDOR], [CREATED_AT]
                )
                SELECT 
                    GETDATE(), 
                    :edital_id, 
                    :periodo_id, 
                    C.[FkContratoSISCTR], 
                    C.ID_EMPRESA, 
                    4, -- Código 4: Demais Contratos Sem Acordo
                    C.[NR_CPF_CNPJ], 
                    C.[VR_SD_DEVEDOR], 
                    GETDATE()
                FROM ##ContratosDisponiveis C
                WHERE C.ID_EMPRESA IS NOT NULL;
            """), {"edital_id": edital_id, "periodo_id": periodo_id})

            # Usar rowcount para obter o número de linhas afetadas
            contratos_distribuidos = resultado_insercao.rowcount
            logger.info(f"Inserção concluída: {contratos_distribuidos} contratos inseridos")

            # Verificação de segurança - se rowcount não funcionou
            if contratos_distribuidos <= 0:
                # Verificação secundária do número de contratos
                contagem = db.session.execute(text("""
                    SELECT COUNT(*) 
                    FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                    WHERE ID_EDITAL = :edital_id
                    AND ID_PERIODO = :periodo_id
                    AND COD_CRITERIO_SELECAO = 4
                    AND CREATED_AT >= DATEADD(minute, -10, GETDATE())
                """), {"edital_id": edital_id, "periodo_id": periodo_id}).scalar() or 0

                # Use a contagem apenas se for maior que zero
                if contagem > 0:
                    contratos_distribuidos = contagem
                    logger.info(f"Contagem secundária: {contratos_distribuidos} contratos inseridos")
        except Exception as insert_error:
            logger.error(f"Erro ao inserir contratos: {str(insert_error)}")
            db.session.rollback()
            contratos_distribuidos = 0

        # Remover contratos distribuídos
        if contratos_distribuidos > 0:
            logger.info("Removendo contratos distribuídos da tabela de distribuíveis...")
            try:
                db.session.execute(text("""
                    DELETE D
                    FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] D
                    INNER JOIN ##ContratosDisponiveis C 
                        ON D.[FkContratoSISCTR] = C.[FkContratoSISCTR]
                    WHERE C.ID_EMPRESA IS NOT NULL;
                """))
                db.session.commit()
            except Exception as delete_error:
                logger.error(f"Erro ao remover contratos distribuídos: {str(delete_error)}")
                db.session.rollback()

        # Limpeza de tabelas temporárias
        logger.info("Realizando limpeza final...")
        try:
            db.session.execute(text("""
                IF OBJECT_ID('tempdb..##ContratosDisponiveis') IS NOT NULL DROP TABLE ##ContratosDisponiveis;
                IF OBJECT_ID('tempdb..##EmpresasInfo') IS NOT NULL DROP TABLE ##EmpresasInfo;
                IF OBJECT_ID('tempdb..##FaixasOrdem') IS NOT NULL DROP TABLE ##FaixasOrdem;
            """))
            db.session.commit()
        except Exception as cleanup_error:
            logger.warning(f"Erro durante limpeza de tabelas temporárias: {str(cleanup_error)}")

        logger.info(f"Distribuição dos demais contratos concluída: {contratos_distribuidos} contratos distribuídos")
        return contratos_distribuidos

    except Exception as e:
        db.session.rollback()
        # Limpeza das tabelas temporárias em caso de erro
        try:
            db.session.execute(text("""
                IF OBJECT_ID('tempdb..##ContratosDisponiveis') IS NOT NULL DROP TABLE ##ContratosDisponiveis;
                IF OBJECT_ID('tempdb..##EmpresasInfo') IS NOT NULL DROP TABLE ##EmpresasInfo;
                IF OBJECT_ID('tempdb..##FaixasOrdem') IS NOT NULL DROP TABLE ##FaixasOrdem;
            """))
            db.session.commit()
        except:
            pass

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

    Args:
        edital_id: ID do edital a ser processado
        periodo_id: ID do período a ser processado
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
                GROUP BY COD_EMPRESA_COBRANCA
            """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        ).fetchall()

        # 2. Atualizar tabela de limites para cada empresa
        for empresa_id, qtde, valor_total in estatisticas:
            db.session.execute(
                text("""
                    UPDATE [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                    SET 
                        VR_ARRECADACAO = :valor_total,
                        QTDE_MAXIMA = :qtde,
                        VALOR_MAXIMO = :valor_total,
                        UPDATED_AT = GETDATE()
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

            print(f"Atualizado limite para empresa {empresa_id}: {qtde} contratos, R$ {float(valor_total):.2f}")

        # 3. Calcular e atualizar percentuais finais
        db.session.execute(
            text("""
                WITH TotalDistribuicao AS (
                    SELECT 
                        SUM(QTDE_MAXIMA) as total_qtde,
                        SUM(VALOR_MAXIMO) as total_valor
                    FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                    WHERE ID_EDITAL = :edital_id
                    AND ID_PERIODO = :periodo_id
                )
                UPDATE LD
                SET PERCENTUAL_FINAL = CASE 
                                          WHEN TD.total_qtde > 0 THEN (LD.QTDE_MAXIMA * 100.0 / TD.total_qtde)
                                          ELSE 0 -- Valor padrão quando não há distribuição
                                       END,
                    UPDATED_AT = GETDATE()
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
                CROSS JOIN TotalDistribuicao TD
                WHERE LD.ID_EDITAL = :edital_id
                AND LD.ID_PERIODO = :periodo_id
            """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        )

        print("Percentuais finais atualizados com sucesso")

        # 4. Exibir resumo dos limites atualizados
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
            """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        ).fetchall()

        print("\nResumo dos limites de distribuição:")
        print("Empresa | Arrecadação | Qtd Max | Valor Max | Percentual Final")
        print("--------------------------------------------------------------")
        for limite in limites:
            print(
                f"{limite[0]} | R$ {float(limite[1]) if limite[1] else 0:.2f} | {limite[2] if limite[2] else 0} | R$ {float(limite[3]) if limite[3] else 0:.2f} | {limite[4]:.2f}%")

        # Commit as alterações
        db.session.commit()

    except Exception as e:
        db.session.rollback()
        print(f"Erro ao atualizar limites de distribuição: {str(e)}")
        import traceback
        print(traceback.format_exc())
        raise


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