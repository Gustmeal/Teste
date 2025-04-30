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
    Versão simplificada para evitar erros de SQL.
    """
    from app import db
    from sqlalchemy import text
    import time
    import logging

    logger = logging.getLogger(__name__)
    start_time = time.time()
    contratos_distribuidos = 0

    try:
        logger.info(
            f"Iniciando regra de arrasto para contratos sem acordo - Edital: {edital_id}, Período: {periodo_id}")

        # 1. Identificar CPFs com múltiplos contratos
        cpfs_multiplos = db.session.execute(
            text("""
                SELECT DISTINCT LTRIM(RTRIM(NR_CPF_CNPJ)) AS cpf_cnpj
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                WHERE NR_CPF_CNPJ IS NOT NULL
                GROUP BY LTRIM(RTRIM(NR_CPF_CNPJ))
                HAVING COUNT(*) > 1
            """)
        ).fetchall()

        if not cpfs_multiplos:
            logger.info("Nenhum CPF com múltiplos contratos encontrado.")
            return 0

        total_cpfs = len(cpfs_multiplos)
        logger.info(f"Encontrados {total_cpfs} CPFs com múltiplos contratos")

        # 2. Buscar empresas participantes com seus percentuais
        empresas = db.session.execute(
            text("""
                SELECT 
                    EP.ID_EMPRESA,
                    COALESCE(LD.PERCENTUAL_FINAL, 0) AS percentual
                FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES] EP
                LEFT JOIN [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
                    ON EP.ID_EMPRESA = LD.ID_EMPRESA
                    AND EP.ID_EDITAL = LD.ID_EDITAL
                    AND EP.ID_PERIODO = LD.ID_PERIODO
                WHERE EP.ID_EDITAL = :edital_id
                AND EP.ID_PERIODO = :periodo_id
                AND EP.DS_CONDICAO <> 'DESCREDENCIADA'
            """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        ).fetchall()

        if not empresas:
            logger.warning("Nenhuma empresa encontrada para distribuição.")
            return 0

        # 3. Normalizar percentuais
        total_percentual = sum(empresa[1] for empresa in empresas)
        if total_percentual <= 0:
            # Distribuir igualmente
            percentual_igual = 100.0 / len(empresas)
            empresas_normalizadas = [(empresa[0], percentual_igual) for empresa in empresas]
        else:
            # Normalizar para soma = 100
            fator = 100.0 / total_percentual
            empresas_normalizadas = [(empresa[0], empresa[1] * fator) for empresa in empresas]

        # 4. Determinar quantos CPFs por empresa
        cpfs_por_empresa = {}
        cpfs_alocados = 0

        for empresa_id, percentual in empresas_normalizadas:
            qtd_cpfs = max(1, int(total_cpfs * percentual / 100))
            cpfs_por_empresa[empresa_id] = qtd_cpfs
            cpfs_alocados += qtd_cpfs

        # Ajustar sobrando ou faltando
        diferenca = total_cpfs - cpfs_alocados
        if diferenca != 0:
            # Ordenar empresas por percentual
            empresas_ordenadas = sorted(empresas_normalizadas, key=lambda x: x[1],
                                        reverse=True if diferenca > 0 else False)

            # Ajustar alocação
            for i in range(abs(diferenca)):
                if i < len(empresas_ordenadas):
                    empresa_id = empresas_ordenadas[i][0]
                    cpfs_por_empresa[empresa_id] += 1 if diferenca > 0 else -1

        # 5. Distribuir CPFs por empresa
        empresa_index = 0
        empresas_ids = [empresa[0] for empresa in empresas_normalizadas]

        for cpf_record in cpfs_multiplos:
            cpf = cpf_record[0]

            # Determinar empresa para este CPF
            while True:
                empresa_id = empresas_ids[empresa_index % len(empresas_ids)]
                if cpfs_por_empresa.get(empresa_id, 0) > 0:
                    cpfs_por_empresa[empresa_id] -= 1
                    break
                empresa_index += 1

            # Buscar contratos deste CPF
            contratos = db.session.execute(
                text("""
                    SELECT FkContratoSISCTR, NR_CPF_CNPJ, VR_SD_DEVEDOR
                    FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                    WHERE LTRIM(RTRIM(NR_CPF_CNPJ)) = :cpf
                """),
                {"cpf": cpf}
            ).fetchall()

            # Inserir contratos na distribuição
            for contrato in contratos:
                contrato_id = contrato[0]
                cpf_cnpj = contrato[1]
                valor = contrato[2]

                # Verificar se já está distribuído
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

                if ja_distribuido == 0:  # Se não distribuído
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
                                :empresa_id,
                                6, -- Código 6: Regra de Arrasto
                                :cpf_cnpj,
                                :valor,
                                GETDATE()
                            )
                        """),
                        {
                            "edital_id": edital_id,
                            "periodo_id": periodo_id,
                            "contrato_id": contrato_id,
                            "empresa_id": empresa_id,
                            "cpf_cnpj": cpf_cnpj,
                            "valor": valor
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

                    contratos_distribuidos += 1

            # Commit após cada CPF
            db.session.commit()
            empresa_index += 1

        elapsed_time = time.time() - start_time
        logger.info(
            f"Regra de arrasto sem acordo concluída: {contratos_distribuidos} contratos distribuídos em {elapsed_time:.2f}s")

        return contratos_distribuidos

    except Exception as e:
        db.session.rollback()
        logger.error(f"Erro crítico na regra de arrasto sem acordo: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

        return 0

def distribuir_demais_contratos(edital_id, periodo_id):
    """
    Distribui os contratos restantes entre as empresas.
    Versão simplificada para evitar erros de SQL.
    """
    contratos_distribuidos = 0

    try:
        print(f"Iniciando distribuição dos demais contratos sem acordo - Edital: {edital_id}, Período: {periodo_id}")

        # 1. Verificar se existem empresas participantes
        empresas = db.session.execute(
            text("""
                SELECT EP.ID_EMPRESA, COALESCE(LD.PERCENTUAL_FINAL, 0) AS percentual
                FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES] EP
                LEFT JOIN [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
                    ON EP.ID_EMPRESA = LD.ID_EMPRESA
                    AND EP.ID_EDITAL = LD.ID_EDITAL
                    AND EP.ID_PERIODO = LD.ID_PERIODO
                WHERE EP.ID_EDITAL = :edital_id
                AND EP.ID_PERIODO = :periodo_id
                AND EP.DS_CONDICAO <> 'DESCREDENCIADA'
            """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        ).fetchall()

        if not empresas:
            print("Nenhuma empresa participante encontrada.")
            return 0

        # 2. Verificar se existem contratos para distribuir
        contratos_count = db.session.execute(
            text("SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]")
        ).scalar() or 0

        if contratos_count == 0:
            print("Nenhum contrato restante para distribuir.")
            return 0

        print(f"Total de contratos restantes: {contratos_count}")

        # 3. Normalizar percentuais
        total_percentual = sum(empresa[1] for empresa in empresas)
        if total_percentual <= 0:
            # Distribuir igualmente
            percentual_igual = 100.0 / len(empresas)
            empresas_normalizadas = [(empresa[0], percentual_igual) for empresa in empresas]
        else:
            # Normalizar para soma = 100
            fator = 100.0 / total_percentual
            empresas_normalizadas = [(empresa[0], empresa[1] * fator) for empresa in empresas]

        # 4. Contar contratos atuais por empresa
        contratos_atuais = {}
        for empresa_id, _ in empresas_normalizadas:
            count = db.session.execute(
                text("""
                    SELECT COUNT(*)
                    FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                    WHERE ID_EDITAL = :edital_id
                    AND ID_PERIODO = :periodo_id
                    AND COD_EMPRESA_COBRANCA = :empresa_id
                """),
                {"edital_id": edital_id, "periodo_id": periodo_id, "empresa_id": empresa_id}
            ).scalar() or 0
            contratos_atuais[empresa_id] = count

        # 5. Calcular total de contratos (atuais + novos)
        total_contratos_atuais = sum(contratos_atuais.values())
        total_geral = total_contratos_atuais + contratos_count

        # 6. Calcular meta total por empresa
        metas_empresa = {}
        for empresa_id, percentual in empresas_normalizadas:
            meta = int(total_geral * percentual / 100)
            metas_empresa[empresa_id] = meta

        # 7. Calcular contratos adicionais necessários
        contratos_adicionais = {}
        for empresa_id, meta in metas_empresa.items():
            atual = contratos_atuais.get(empresa_id, 0)
            adicional = max(0, meta - atual)
            contratos_adicionais[empresa_id] = adicional

        # 8. Verificar se o total a distribuir é consistente
        total_a_distribuir = sum(contratos_adicionais.values())
        if total_a_distribuir != contratos_count:
            # Ajustar distribuição
            diferenca = contratos_count - total_a_distribuir
            empresas_ordenadas = sorted(
                [(empresa_id, percentual) for empresa_id, percentual in empresas_normalizadas],
                key=lambda x: x[1],
                reverse=(diferenca > 0)
            )

            # Ajustar contratos adicionais
            for i in range(abs(diferenca)):
                if i < len(empresas_ordenadas):
                    empresa_id = empresas_ordenadas[i][0]
                    if diferenca > 0:
                        contratos_adicionais[empresa_id] += 1
                    elif contratos_adicionais[empresa_id] > 0:
                        contratos_adicionais[empresa_id] -= 1

        # 9. Buscar contratos para distribuição
        contratos = db.session.execute(
            text("""
                SELECT FkContratoSISCTR, NR_CPF_CNPJ, VR_SD_DEVEDOR 
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
            """)
        ).fetchall()

        # 10. Distribuir contratos por empresa
        contratos_por_empresa = {}
        for empresa_id, adicional in contratos_adicionais.items():
            if adicional > 0:
                contratos_por_empresa[empresa_id] = []

        contrato_index = 0
        # Distribuir os contratos entre as empresas
        for empresa_id, adicional in contratos_adicionais.items():
            for _ in range(adicional):
                if contrato_index < len(contratos):
                    contratos_por_empresa.setdefault(empresa_id, []).append(contratos[contrato_index])
                    contrato_index += 1

        # 11. Inserir contratos na tabela de distribuição
        for empresa_id, contratos_lista in contratos_por_empresa.items():
            for contrato in contratos_lista:
                contrato_id = contrato[0]
                cpf_cnpj = contrato[1]
                valor = contrato[2]

                # Verificar se já está distribuído
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

                if ja_distribuido == 0:  # Se não distribuído
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
                                :empresa_id,
                                4, -- Demais contratos sem acordo
                                :cpf_cnpj,
                                :valor,
                                GETDATE()
                            )
                        """),
                        {
                            "edital_id": edital_id,
                            "periodo_id": periodo_id,
                            "contrato_id": contrato_id,
                            "empresa_id": empresa_id,
                            "cpf_cnpj": cpf_cnpj,
                            "valor": valor
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

                    contratos_distribuidos += 1

            # Commit após cada empresa
            db.session.commit()

        print(f"Distribuição dos demais contratos concluída: {contratos_distribuidos} contratos distribuídos")

    except Exception as e:
        db.session.rollback()
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
    - PERCENTUAL_FINAL: percentual final com base na quantidade de contratos
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

        # 2. Verificar existência de limites já definidos
        limites_existem = db.session.execute(
            text("""
                SELECT COUNT(*)
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
            """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        ).scalar() > 0

        # 3. Para cada empresa, atualizar ou inserir informações nos limites
        total_quantidade = 0
        for empresa_id, qtde, valor_total in estatisticas:
            total_quantidade += qtde

            if not limites_existem:
                # Inserir novo registro
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
                            0, -- Será atualizado depois
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
                # Atualizar registro existente
                db.session.execute(
                    text("""
                        UPDATE [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                        SET 
                            VR_ARRECADACAO = :valor_total,
                            QTDE_MAXIMA = :qtde,
                            VALOR_MAXIMO = :valor_total,
                            UPDATED_AT = GETDATE(),
                            DT_APURACAO = GETDATE()
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

        # 4. Atualizar percentuais finais
        if total_quantidade > 0:
            for empresa_id, qtde, valor_total in estatisticas:
                percentual = (qtde / total_quantidade) * 100

                db.session.execute(
                    text("""
                        UPDATE [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                        SET PERCENTUAL_FINAL = :percentual
                        WHERE ID_EDITAL = :edital_id
                        AND ID_PERIODO = :periodo_id
                        AND ID_EMPRESA = :empresa_id
                    """),
                    {
                        "edital_id": edital_id,
                        "periodo_id": periodo_id,
                        "empresa_id": empresa_id,
                        "percentual": percentual
                    }
                )

        # 5. Verificar se a soma é 100%
        soma_percentuais = db.session.execute(
            text("""
                SELECT SUM(PERCENTUAL_FINAL)
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
            """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        ).scalar() or 0

        # 6. Ajustar o maior percentual para garantir soma = 100%
        if abs(soma_percentuais - 100) > 0.01:
            ajuste = 100 - soma_percentuais

            # Buscar o ID do limite com maior percentual
            maior_percentual = db.session.execute(
                text("""
                    SELECT TOP 1 ID
                    FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                    WHERE ID_EDITAL = :edital_id
                    AND ID_PERIODO = :periodo_id
                    ORDER BY PERCENTUAL_FINAL DESC
                """),
                {"edital_id": edital_id, "periodo_id": periodo_id}
            ).scalar()

            if maior_percentual:
                db.session.execute(
                    text("""
                        UPDATE [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                        SET PERCENTUAL_FINAL = PERCENTUAL_FINAL + :ajuste
                        WHERE ID = :id
                    """),
                    {"id": maior_percentual, "ajuste": ajuste}
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