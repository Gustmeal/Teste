from app import db
from sqlalchemy import text
import logging


def selecionar_contratos_distribuiveis():
    """
    Seleciona o universo de contratos que serão distribuídos e os armazena na tabela DCA_TB006_DISTRIBUIVEIS.
    Usa as tabelas do Banco de Dados Gerencial (BDG).

    Returns:
        int: Quantidade de contratos selecionados
    """
    try:
        # Usar uma conexão direta para executar o SQL
        with db.engine.connect() as connection:
            try:
                # Primeiro, limpar a tabela de distribuíveis
                logging.info("Limpando tabela de distribuíveis...")
                truncate_sql = text("TRUNCATE TABLE [DEV].[DCA_TB006_DISTRIBUIVEIS]")
                connection.execute(truncate_sql)

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

                connection.execute(insert_sql)

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


def distribuir_acordos_vigentes_empresas_descredenciadas(edital_id, periodo_id):
    """
    Distribui contratos com acordo vigente de empresas descredenciadas.

    Este passo implementa o item 1.1.3 do documento:
    - Identifica contratos com acordo vigente de empresas que não participam mais do período atual
    - Distribui esses contratos igualitariamente entre as empresas do novo período
    - Usa critério de seleção 3 (Contrato com acordo com assessoria descredenciada)

    Args:
        edital_id (int): ID do edital
        periodo_id (int): ID do período

    Returns:
        int: Quantidade de contratos distribuídos
    """
    try:
        # Usar uma conexão direta para executar o SQL
        with db.engine.connect() as connection:
            try:
                # Obter período anterior para referência
                periodo_anterior_sql = text("""
                SELECT TOP 1 ID_PERIODO 
                FROM DEV.DCA_TB001_PERIODO_AVALIACAO 
                WHERE ID_EDITAL = :edital_id 
                  AND ID_PERIODO < :periodo_id 
                  AND DELETED_AT IS NULL 
                ORDER BY ID_PERIODO DESC
                """).bindparams(edital_id=edital_id, periodo_id=periodo_id)

                periodo_anterior_result = connection.execute(periodo_anterior_sql).fetchone()
                if not periodo_anterior_result:
                    logging.warning("Não foi encontrado período anterior para referência")
                    return 0

                periodo_anterior_id = periodo_anterior_result[0]

                # Verificar se existem empresas no período atual
                empresas_atuais_sql = text("""
                SELECT COUNT(*) 
                FROM DEV.DCA_TB002_EMPRESAS_PARTICIPANTES 
                WHERE ID_EDITAL = :edital_id 
                  AND ID_PERIODO = :periodo_id
                  AND DELETED_AT IS NULL
                """).bindparams(edital_id=edital_id, periodo_id=periodo_id)

                qtde_empresas_atuais = connection.execute(empresas_atuais_sql).scalar()
                if qtde_empresas_atuais == 0:
                    logging.warning("Não existem empresas no período atual para redistribuição")
                    return 0

                # Obter a data de referência atual
                data_referencia_sql = text("SELECT CONVERT(DATE, GETDATE())")
                dt_referencia = connection.execute(data_referencia_sql).scalar()

                # CORREÇÃO: Alterado o filtro para usar fkEstadoAcordo em vez de ESTADO_ACORDO
                contratos_descred_sql = text("""
                -- Criar tabela temporária para armazenar contratos a serem redistribuídos
                SELECT 
                    D.FkContratoSISCTR,
                    D.NR_CPF_CNPJ,
                    D.VR_SD_DEVEDOR,
                    ROW_NUMBER() OVER (ORDER BY D.NR_CPF_CNPJ) AS RowNum
                INTO #CONTRATOS_DESCREDENCIADOS
                FROM DEV.DCA_TB006_DISTRIBUIVEIS D

                -- Join com a tabela de acordos vigentes
                INNER JOIN BDG.COM_TB009_ACORDOS_LIQUIDADOS_VIGENTES ALV
                    ON D.FkContratoSISCTR = ALV.FkContratoSISCTR

                -- Join com a empresa atual do contrato
                INNER JOIN BDG.COM_TB011_EMPRESA_COBRANCA_ATUAL ECA
                    ON D.FkContratoSISCTR = ECA.FkContratoSISCTR

                -- Filtro: contrato tem acordo vigente (estado = 1)
                -- E a empresa atual NÃO está entre as empresas do período atual
                WHERE ALV.fkEstadoAcordo = 1
                AND NOT EXISTS (
                    SELECT 1 
                    FROM DEV.DCA_TB002_EMPRESAS_PARTICIPANTES EP
                    WHERE EP.ID_EDITAL = :edital_id
                      AND EP.ID_PERIODO = :periodo_id
                      AND EP.ID_EMPRESA = ECA.COD_EMPRESA_COBRANCA
                      AND EP.DELETED_AT IS NULL
                )
                """).bindparams(edital_id=edital_id, periodo_id=periodo_id)

                # Executar seleção de contratos descredenciados
                connection.execute(contratos_descred_sql)

                # Contar quantos contratos foram selecionados
                count_sql = text("SELECT COUNT(*) FROM #CONTRATOS_DESCREDENCIADOS")
                num_contratos_descred = connection.execute(count_sql).scalar()

                if num_contratos_descred == 0:
                    logging.info("Não foram encontrados contratos com acordo de empresas descredenciadas")
                    connection.execute(text("DROP TABLE IF EXISTS #CONTRATOS_DESCREDENCIADOS"))
                    return 0

                # Obter empresas do período atual para redistribuição
                empresas_atuais_sql = text("""
                SELECT 
                    ID_EMPRESA,
                    ROW_NUMBER() OVER (ORDER BY NEWID()) AS RowNum
                INTO #EMPRESAS_ATUAIS
                FROM DEV.DCA_TB002_EMPRESAS_PARTICIPANTES 
                WHERE ID_EDITAL = :edital_id 
                  AND ID_PERIODO = :periodo_id
                  AND DELETED_AT IS NULL
                """).bindparams(edital_id=edital_id, periodo_id=periodo_id)

                connection.execute(empresas_atuais_sql)

                # Obter total de empresas atuais
                count_empresas_sql = text("SELECT COUNT(*) FROM #EMPRESAS_ATUAIS")
                qtde_empresas = connection.execute(count_empresas_sql).scalar()

                # Calcular distribuição igualitária
                contracts_per_company = num_contratos_descred // qtde_empresas
                remainder = num_contratos_descred % qtde_empresas

                # Criar tabela temporária de mapeamento
                map_sql = text("""
                CREATE TABLE #MAPEAMENTO (
                    RowNumContrato INT,
                    RowNumEmpresa INT
                )
                """)
                connection.execute(map_sql)

                # Preencher mapeamento para distribuição igualitária
                for i in range(1, qtde_empresas + 1):
                    start_row = (i - 1) * contracts_per_company + 1
                    extra = 1 if i <= remainder else 0
                    end_row = start_row + contracts_per_company - 1 + extra

                    map_fill_sql = text("""
                    INSERT INTO #MAPEAMENTO
                    SELECT 
                        RowNum,
                        :empresa_row_num
                    FROM #CONTRATOS_DESCREDENCIADOS
                    WHERE RowNum BETWEEN :start_row AND :end_row
                    """).bindparams(empresa_row_num=i, start_row=start_row, end_row=end_row)

                    connection.execute(map_fill_sql)

                # Executar a distribuição
                distribuir_sql = text("""
                INSERT INTO DEV.DCA_TB005_DISTRIBUICAO (
                    DT_REFERENCIA,
                    ID_EDITAL,
                    ID_PERIODO,
                    FkContratoSISCTR,
                    COD_CRITERIO_SELECAO,
                    COD_EMPRESA_COBRANCA,
                    NR_CPF_CNPJ,
                    VR_SD_DEVEDOR,
                    CREATED_AT,
                    UPDATED_AT,
                    DELETED_AT
                )
                SELECT
                    :dt_referencia,
                    :edital_id,
                    :periodo_id,
                    C.FkContratoSISCTR,
                    3, -- Código 3: Contrato com acordo com assessoria descredenciada
                    E.ID_EMPRESA,
                    C.NR_CPF_CNPJ,
                    C.VR_SD_DEVEDOR,
                    GETDATE(),
                    NULL,
                    NULL
                FROM #CONTRATOS_DESCREDENCIADOS C
                INNER JOIN #MAPEAMENTO M ON C.RowNum = M.RowNumContrato
                INNER JOIN #EMPRESAS_ATUAIS E ON M.RowNumEmpresa = E.RowNum
                """).bindparams(
                    dt_referencia=dt_referencia,
                    edital_id=edital_id,
                    periodo_id=periodo_id
                )

                # Executar a distribuição
                result = connection.execute(distribuir_sql)
                contratos_distribuidos = result.rowcount

                # Remover os contratos distribuídos da tabela de distribuíveis
                if contratos_distribuidos > 0:
                    remover_sql = text("""
                    DELETE FROM DEV.DCA_TB006_DISTRIBUIVEIS
                    WHERE FkContratoSISCTR IN (
                        SELECT FkContratoSISCTR 
                        FROM DEV.DCA_TB005_DISTRIBUICAO
                        WHERE ID_EDITAL = :edital_id
                        AND ID_PERIODO = :periodo_id
                        AND COD_CRITERIO_SELECAO = 3
                    )
                    """).bindparams(edital_id=edital_id, periodo_id=periodo_id)

                    connection.execute(remover_sql)

                # Limpar tabelas temporárias
                connection.execute(text("DROP TABLE IF EXISTS #CONTRATOS_DESCREDENCIADOS"))
                connection.execute(text("DROP TABLE IF EXISTS #EMPRESAS_ATUAIS"))
                connection.execute(text("DROP TABLE IF EXISTS #MAPEAMENTO"))

                logging.info(f"Contratos com acordo de empresas descredenciadas distribuídos: {contratos_distribuidos}")
                return contratos_distribuidos

            except Exception as e:
                logging.error(
                    f"Erro durante a distribuição de contratos com acordo de empresas descredenciadas: {str(e)}")
                import traceback
                logging.error(traceback.format_exc())
                # Limpar tabelas temporárias em caso de erro
                try:
                    connection.execute(text("DROP TABLE IF EXISTS #CONTRATOS_DESCREDENCIADOS"))
                    connection.execute(text("DROP TABLE IF EXISTS #EMPRESAS_ATUAIS"))
                    connection.execute(text("DROP TABLE IF EXISTS #MAPEAMENTO"))
                except:
                    pass
                raise

    except Exception as e:
        logging.error(f"Erro ao distribuir contratos com acordo de empresas descredenciadas: {str(e)}")
        return 0

def distribuir_acordos_vigentes_empresas_permanece(edital_id, periodo_id):
    """
    Distribui contratos com acordo vigente para empresas que permanecem no período avaliativo.
    """
    try:
        with db.engine.connect() as connection:
            try:
                # Buscar período anterior (mantido sem alteração)
                periodo_anterior_sql = text("""
                SELECT TOP 1 ID_PERIODO 
                FROM DEV.DCA_TB001_PERIODO_AVALIACAO 
                WHERE ID_EDITAL = :edital_id 
                  AND ID_PERIODO < :periodo_id 
                  AND DELETED_AT IS NULL 
                ORDER BY ID_PERIODO DESC
                """).bindparams(edital_id=edital_id, periodo_id=periodo_id)

                periodo_anterior_result = connection.execute(periodo_anterior_sql).fetchone()
                if not periodo_anterior_result:
                    logging.warning("Não foi encontrado período anterior para referência")
                    return 0

                periodo_anterior_id = periodo_anterior_result[0]

                # Obtém a data de referência atual
                data_referencia_sql = text("SELECT CONVERT(DATE, GETDATE())")
                dt_referencia = connection.execute(data_referencia_sql).scalar()

                # CORREÇÃO: Alterado o filtro para usar fkEstadoAcordo em vez de ESTADO_ACORDO
                distribuir_sql = text("""
                -- Inserir na tabela de distribuição os contratos com acordo vigente
                -- para empresas que permanecem no período atual
                INSERT INTO DEV.DCA_TB005_DISTRIBUICAO (
                    DT_REFERENCIA,
                    ID_EDITAL,
                    ID_PERIODO,
                    FkContratoSISCTR,
                    COD_CRITERIO_SELECAO,
                    COD_EMPRESA_COBRANCA,
                    NR_CPF_CNPJ,
                    VR_SD_DEVEDOR,
                    CREATED_AT,
                    UPDATED_AT,
                    DELETED_AT
                )
                SELECT
                    :dt_referencia,
                    :edital_id,
                    :periodo_id,
                    DIST.FkContratoSISCTR,
                    1, -- Código 1: Contrato com acordo para assessoria que permanece
                    DIST.NR_EMPRESA_ATUAL,
                    DIST.NR_CPF_CNPJ,
                    DIST.VR_SD_DEVEDOR,
                    GETDATE(),
                    NULL,
                    NULL
                FROM (
                    -- Seleciona contratos com acordo vigente
                    SELECT 
                        D.FkContratoSISCTR,
                        D.NR_CPF_CNPJ,
                        D.VR_SD_DEVEDOR,
                        ECA.COD_EMPRESA_COBRANCA AS NR_EMPRESA_ATUAL
                    FROM DEV.DCA_TB006_DISTRIBUIVEIS D

                    -- Join com a tabela de acordos vigentes
                    INNER JOIN BDG.COM_TB009_ACORDOS_LIQUIDADOS_VIGENTES ALV
                        ON D.FkContratoSISCTR = ALV.FkContratoSISCTR

                    -- Join com a empresa atual do contrato
                    INNER JOIN BDG.COM_TB011_EMPRESA_COBRANCA_ATUAL ECA
                        ON D.FkContratoSISCTR = ECA.FkContratoSISCTR

                    -- Join com empresas que permanecem no período atual
                    INNER JOIN DEV.DCA_TB002_EMPRESAS_PARTICIPANTES EP_ATUAL
                        ON ECA.COD_EMPRESA_COBRANCA = EP_ATUAL.ID_EMPRESA
                        AND EP_ATUAL.ID_EDITAL = :edital_id
                        AND EP_ATUAL.ID_PERIODO = :periodo_id
                        AND EP_ATUAL.DS_CONDICAO = 'PERMANECE'
                        AND EP_ATUAL.DELETED_AT IS NULL

                    -- Filtro por acordo vigente (Estado do Acordo = 1)
                    WHERE ALV.fkEstadoAcordo = 1
                ) AS DIST
                """).bindparams(
                    dt_referencia=dt_referencia,
                    edital_id=edital_id,
                    periodo_id=periodo_id
                )

                # Resto da função mantido sem alteração
                result = connection.execute(distribuir_sql)
                contratos_distribuidos = result.rowcount

                if contratos_distribuidos > 0:
                    remover_sql = text("""
                    DELETE FROM DEV.DCA_TB006_DISTRIBUIVEIS
                    WHERE FkContratoSISCTR IN (
                        SELECT FkContratoSISCTR 
                        FROM DEV.DCA_TB005_DISTRIBUICAO
                        WHERE ID_EDITAL = :edital_id
                        AND ID_PERIODO = :periodo_id
                        AND COD_CRITERIO_SELECAO = 1
                    )
                    """).bindparams(edital_id=edital_id, periodo_id=periodo_id)

                    connection.execute(remover_sql)

                logging.info(f"Contratos com acordo vigente distribuídos: {contratos_distribuidos}")
                return contratos_distribuidos

            except Exception as e:
                logging.error(f"Erro durante a distribuição de contratos com acordo vigente: {str(e)}")
                import traceback
                logging.error(traceback.format_exc())
                raise

    except Exception as e:
        logging.error(f"Erro ao distribuir contratos com acordo vigente: {str(e)}")
        return 0


def aplicar_regra_arrasto_acordos(edital_id, periodo_id):
    """
    Aplica a regra de arrasto para contratos com acordo vigente.

    Este passo implementa o item 1.1.6 do documento:
    - Verifica se existem outros contratos do mesmo CPF dos que já foram distribuídos
    - Distribui todos os contratos com o mesmo CPF para a mesma empresa
    - Usa critério de seleção 6 (Regra de Arrasto)

    Args:
        edital_id (int): ID do edital
        periodo_id (int): ID do período

    Returns:
        int: Quantidade de contratos distribuídos
    """
    try:
        # Usar uma conexão direta para executar o SQL
        with db.engine.connect() as connection:
            try:
                # Obtém a data de referência atual
                data_referencia_sql = text("""
                SELECT CONVERT(DATE, GETDATE())
                """)
                dt_referencia = connection.execute(data_referencia_sql).scalar()

                # Aplicar regra de arrasto
                arrasto_sql = text("""
                -- Inserir na tabela de distribuição os contratos do mesmo CPF
                -- que já possuem um contrato com acordo distribuído
                INSERT INTO DEV.DCA_TB005_DISTRIBUICAO (
                    DT_REFERENCIA,
                    ID_EDITAL,
                    ID_PERIODO,
                    FkContratoSISCTR,
                    COD_CRITERIO_SELECAO,
                    COD_EMPRESA_COBRANCA,
                    NR_CPF_CNPJ,
                    VR_SD_DEVEDOR,
                    CREATED_AT,
                    UPDATED_AT,
                    DELETED_AT
                )
                SELECT
                    :dt_referencia,
                    :edital_id,
                    :periodo_id,
                    D.FkContratoSISCTR,
                    6, -- Código 6: Regra de Arrasto
                    DIST.COD_EMPRESA_COBRANCA,
                    D.NR_CPF_CNPJ,
                    D.VR_SD_DEVEDOR,
                    GETDATE(),
                    NULL,
                    NULL
                FROM DEV.DCA_TB006_DISTRIBUIVEIS D
                INNER JOIN (
                    -- Obter CPFs já distribuídos e suas empresas
                    SELECT DISTINCT 
                        NR_CPF_CNPJ, 
                        COD_EMPRESA_COBRANCA
                    FROM DEV.DCA_TB005_DISTRIBUICAO
                    WHERE ID_EDITAL = :edital_id
                    AND ID_PERIODO = :periodo_id
                    AND NR_CPF_CNPJ IS NOT NULL
                ) AS DIST ON D.NR_CPF_CNPJ = DIST.NR_CPF_CNPJ
                -- Garantir que não está duplicando contratos já distribuídos
                WHERE D.FkContratoSISCTR NOT IN (
                    SELECT FkContratoSISCTR 
                    FROM DEV.DCA_TB005_DISTRIBUICAO
                    WHERE ID_EDITAL = :edital_id
                    AND ID_PERIODO = :periodo_id
                )
                """).bindparams(
                    dt_referencia=dt_referencia,
                    edital_id=edital_id,
                    periodo_id=periodo_id
                )

                # Executar o arrasto
                result = connection.execute(arrasto_sql)
                contratos_arrastados = result.rowcount

                # Remover os contratos arrastados da tabela de distribuíveis
                if contratos_arrastados > 0:
                    remover_sql = text("""
                    DELETE FROM DEV.DCA_TB006_DISTRIBUIVEIS
                    WHERE FkContratoSISCTR IN (
                        SELECT FkContratoSISCTR 
                        FROM DEV.DCA_TB005_DISTRIBUICAO
                        WHERE ID_EDITAL = :edital_id
                        AND ID_PERIODO = :periodo_id
                        AND COD_CRITERIO_SELECAO = 6
                    )
                    """).bindparams(edital_id=edital_id, periodo_id=periodo_id)

                    connection.execute(remover_sql)

                logging.info(f"Contratos arrastados para acordo vigente: {contratos_arrastados}")
                return contratos_arrastados

            except Exception as e:
                logging.error(f"Erro durante a aplicação da regra de arrasto para acordos: {str(e)}")
                import traceback
                logging.error(traceback.format_exc())
                raise

    except Exception as e:
        logging.error(f"Erro ao aplicar regra de arrasto para acordos: {str(e)}")
        return 0


def aplicar_regra_arrasto_sem_acordo(edital_id, periodo_id):
    """
    Aplica a regra de arrasto para contratos sem acordo.

    Este passo implementa o item 1.1.5 do documento:
    - Dos contratos restantes, sem acordo vigente, separa todos os contratos cujo CPF possui outros contratos
    - Distribui esses contratos respeitando a regra de arrasto (mesmo CPF vai para a mesma empresa)
    - Usa critério de seleção 6 (Regra de Arrasto)

    Args:
        edital_id (int): ID do edital
        periodo_id (int): ID do período

    Returns:
        int: Quantidade de contratos distribuídos
    """
    try:
        # Usar uma conexão direta para executar o SQL
        with db.engine.connect() as connection:
            # Verificando se há transação ativa e fazendo commit/rollback se necessário
            connection.execute(text("IF @@TRANCOUNT > 0 COMMIT"))

            try:
                # Limpar tabela de arrastáveis antes de iniciar
                truncate_sql = text("TRUNCATE TABLE [DEV].[DCA_TB007_ARRASTAVEIS]")
                connection.execute(truncate_sql)

                # Identificar CPFs com múltiplos contratos e movê-los para tabela de arrastáveis
                mover_arrastaveis_sql = text("""
                -- Identificar contratos onde o CPF aparece mais de uma vez
                WITH arrastaveis AS (
                    SELECT
                        ID,
                        FkContratoSISCTR,
                        NR_CPF_CNPJ,
                        NU_LINHA = ROW_NUMBER() OVER (
                            PARTITION BY NR_CPF_CNPJ 
                            ORDER BY NR_CPF_CNPJ DESC
                        )
                    FROM DEV.DCA_TB006_DISTRIBUIVEIS
                ),
                cpfArrastaveis AS (
                    -- Selecionar CPFs que aparecem mais de uma vez
                    SELECT DISTINCT NR_CPF_CNPJ
                    FROM arrastaveis
                    WHERE NU_LINHA > 1
                )

                -- Inserir na tabela de arrastáveis
                INSERT INTO DEV.DCA_TB007_ARRASTAVEIS (
                    FkContratoSISCTR,
                    NR_CPF_CNPJ,
                    VR_SD_DEVEDOR,
                    CREATED_AT,
                    UPDATED_AT,
                    DELETED_AT
                )
                SELECT
                    DIS.FkContratoSISCTR,
                    DIS.NR_CPF_CNPJ,
                    DIS.VR_SD_DEVEDOR,
                    GETDATE(),
                    NULL,
                    NULL
                FROM cpfArrastaveis AS CAR
                INNER JOIN DEV.DCA_TB006_DISTRIBUIVEIS AS DIS
                    ON CAR.NR_CPF_CNPJ = DIS.NR_CPF_CNPJ
                """)

                connection.execute(mover_arrastaveis_sql)

                # Contar quantos contratos foram movidos para arrastáveis
                count_arrastaveis_sql = text("SELECT COUNT(*) FROM DEV.DCA_TB007_ARRASTAVEIS")
                qtde_arrastaveis = connection.execute(count_arrastaveis_sql).scalar()

                if qtde_arrastaveis == 0:
                    logging.info("Não foram encontrados contratos para aplicar a regra de arrasto sem acordo")
                    return 0

                # Remover os contratos movidos da tabela de distribuíveis
                remover_da_origem_sql = text("""
                DELETE FROM DEV.DCA_TB006_DISTRIBUIVEIS
                WHERE FkContratoSISCTR IN (
                    SELECT FkContratoSISCTR 
                    FROM DEV.DCA_TB007_ARRASTAVEIS
                )
                """)
                connection.execute(remover_da_origem_sql)

                # Obter a data de referência atual
                data_referencia_sql = text("SELECT CONVERT(DATE, GETDATE())")
                dt_referencia = connection.execute(data_referencia_sql).scalar()

                # Garantir que a tabela temporária seja criada corretamente
                # Buscar limites de distribuição para as empresas - usar uma única instrução SQL
                limites_sql = text("""
                -- Verificar se a tabela temporária já existe e excluí-la se necessário
                IF OBJECT_ID('tempdb..#PERCENTUAIS_EMPRESAS') IS NOT NULL
                    DROP TABLE #PERCENTUAIS_EMPRESAS;

                -- Criar tabela temporária com percentuais
                SELECT 
                    ID_EMPRESA,
                    PERCENTUAL_FINAL
                INTO #PERCENTUAIS_EMPRESAS
                FROM DEV.DCA_TB003_LIMITES_DISTRIBUICAO
                WHERE ID_EDITAL = :edital_id
                  AND ID_PERIODO = :periodo_id
                  AND DELETED_AT IS NULL
                ORDER BY NEWID() -- Ordem aleatória para distribuição
                """).bindparams(edital_id=edital_id, periodo_id=periodo_id)

                connection.execute(limites_sql)

                # Verificar se existem limites de distribuição
                count_limites_sql = text("SELECT COUNT(*) FROM #PERCENTUAIS_EMPRESAS")
                qtde_empresas = connection.execute(count_limites_sql).scalar()

                if qtde_empresas == 0:
                    logging.warning("Não foram encontrados limites de distribuição para as empresas")
                    connection.execute(text("DROP TABLE IF EXISTS #PERCENTUAIS_EMPRESAS"))
                    return 0

                # Atribuir um ID sequencial para cada empresa
                id_empresas_sql = text("""
                SELECT 
                    ID = ROW_NUMBER() OVER (ORDER BY ID_EMPRESA),
                    ID_EMPRESA,
                    PERCENTUAL_FINAL,
                    QTDE = CONVERT(INT, :qtde_arrastaveis * PERCENTUAL_FINAL / 100)
                INTO #ASSESSORIAS
                FROM #PERCENTUAIS_EMPRESAS
                """).bindparams(qtde_arrastaveis=qtde_arrastaveis)

                connection.execute(id_empresas_sql)

                # Verificar soma das quantidades calculadas
                soma_qtde_sql = text("SELECT SUM(QTDE) FROM #ASSESSORIAS")
                soma_qtde = connection.execute(soma_qtde_sql).scalar()

                # Distribuir a sobra para as empresas com maior participação
                sobra = qtde_arrastaveis - soma_qtde

                if sobra > 0:
                    distribuir_sobra_sql = text("""
                    UPDATE TOP (:sobra) #ASSESSORIAS
                    SET QTDE = QTDE + 1
                    ORDER BY PERCENTUAL_FINAL DESC
                    """).bindparams(sobra=sobra)

                    connection.execute(distribuir_sobra_sql)

                # Ordenar os contratos por CPF para facilitar distribuição em blocos
                ordenar_arrastaveis_sql = text("""
                SELECT
                    FkContratoSISCTR,
                    NR_CPF_CNPJ,
                    VR_SD_DEVEDOR,
                    RowNum = ROW_NUMBER() OVER (ORDER BY NR_CPF_CNPJ)
                INTO #ARRASTAVEIS
                FROM DEV.DCA_TB007_ARRASTAVEIS
                """)

                connection.execute(ordenar_arrastaveis_sql)

                # Criar tabela temporária para distribuição
                connection.execute(text("""
                CREATE TABLE #DISTRIBUICAO (
                    FkContratoSISCTR BIGINT,
                    ID INT,
                    NR_CPF_CNPJ BIGINT,
                    VR_SD_DEVEDOR DECIMAL(18,2)
                )
                """))

                # Distribuir contratos por empresa usando CURSOR
                connection.execute(text("""
                DECLARE @ID INT
                DECLARE @ID_EMPRESA INT
                DECLARE @PERCENTUAL DECIMAL(6,2)
                DECLARE @QUANTIDADE INT
                DECLARE @START_ROW INT = 1
                DECLARE @END_ROW INT
                DECLARE @CPF_END_ROW BIGINT
                DECLARE @CPF_END_ROW_MAIS_UM BIGINT
                DECLARE @MUDOU_CPF INT

                DECLARE PERCENTUAIS_CURSOR CURSOR FOR
                SELECT ID, ID_EMPRESA, PERCENTUAL_FINAL, QTDE
                FROM #ASSESSORIAS

                OPEN PERCENTUAIS_CURSOR
                FETCH NEXT FROM PERCENTUAIS_CURSOR INTO @ID, @ID_EMPRESA, @PERCENTUAL, @QUANTIDADE

                WHILE @@FETCH_STATUS = 0
                BEGIN
                    -- Calcular linha final para esta empresa
                    SET @END_ROW = @START_ROW + @QUANTIDADE - 1

                    -- Ajustar para não separar contratos do mesmo CPF
                    SET @MUDOU_CPF = 0

                    WHILE (@MUDOU_CPF = 0 AND @END_ROW < (SELECT MAX(RowNum) FROM #ARRASTAVEIS))
                    BEGIN
                        SET @CPF_END_ROW = (SELECT NR_CPF_CNPJ FROM #ARRASTAVEIS WHERE RowNum = @END_ROW)
                        SET @CPF_END_ROW_MAIS_UM = (SELECT NR_CPF_CNPJ FROM #ARRASTAVEIS WHERE RowNum = @END_ROW + 1)

                        IF @CPF_END_ROW = @CPF_END_ROW_MAIS_UM
                        BEGIN
                            -- Mesmo CPF, incluir próximo contrato
                            SET @END_ROW = @END_ROW + 1
                        END
                        ELSE
                        BEGIN
                            -- CPF diferente, parar ajuste
                            SET @MUDOU_CPF = 1
                        END
                    END

                    -- Inserir na tabela de distribuição
                    INSERT INTO #DISTRIBUICAO
                    SELECT
                        FkContratoSISCTR,
                        @ID,
                        NR_CPF_CNPJ,
                        VR_SD_DEVEDOR
                    FROM #ARRASTAVEIS
                    WHERE RowNum BETWEEN @START_ROW AND @END_ROW

                    -- Atualizar linha inicial para próxima empresa
                    SET @START_ROW = @END_ROW + 1

                    FETCH NEXT FROM PERCENTUAIS_CURSOR INTO @ID, @ID_EMPRESA, @PERCENTUAL, @QUANTIDADE
                END

                CLOSE PERCENTUAIS_CURSOR
                DEALLOCATE PERCENTUAIS_CURSOR
                """))

                # Inserir na tabela final de distribuição
                inserir_distribuicao_sql = text("""
                INSERT INTO DEV.DCA_TB005_DISTRIBUICAO (
                    DT_REFERENCIA,
                    ID_EDITAL,
                    ID_PERIODO,
                    FkContratoSISCTR,
                    COD_CRITERIO_SELECAO,
                    COD_EMPRESA_COBRANCA,
                    NR_CPF_CNPJ,
                    VR_SD_DEVEDOR,
                    CREATED_AT,
                    UPDATED_AT,
                    DELETED_AT
                )
                SELECT
                    :dt_referencia,
                    :edital_id,
                    :periodo_id,
                    DIS.FkContratoSISCTR,
                    6, -- Código 6: Regra de Arrasto
                    ASS.ID_EMPRESA,
                    DIS.NR_CPF_CNPJ,
                    DIS.VR_SD_DEVEDOR,
                    GETDATE(),
                    NULL,
                    NULL
                FROM #DISTRIBUICAO AS DIS
                JOIN #ASSESSORIAS AS ASS
                ON DIS.ID = ASS.ID
                """).bindparams(
                    dt_referencia=dt_referencia,
                    edital_id=edital_id,
                    periodo_id=periodo_id
                )

                result = connection.execute(inserir_distribuicao_sql)
                contratos_distribuidos = result.rowcount

                # Limpar tabelas temporárias
                connection.execute(text("DROP TABLE IF EXISTS #PERCENTUAIS_EMPRESAS"))
                connection.execute(text("DROP TABLE IF EXISTS #ASSESSORIAS"))
                connection.execute(text("DROP TABLE IF EXISTS #ARRASTAVEIS"))
                connection.execute(text("DROP TABLE IF EXISTS #DISTRIBUICAO"))

                logging.info(f"Contratos arrastáveis sem acordo distribuídos: {contratos_distribuidos}")
                return contratos_distribuidos

            except Exception as e:
                logging.error(f"Erro durante a aplicação da regra de arrasto para contratos sem acordo: {str(e)}")
                import traceback
                logging.error(traceback.format_exc())
                # Limpar tabelas temporárias em caso de erro
                try:
                    connection.execute(text("DROP TABLE IF EXISTS #PERCENTUAIS_EMPRESAS"))
                    connection.execute(text("DROP TABLE IF EXISTS #ASSESSORIAS"))
                    connection.execute(text("DROP TABLE IF EXISTS #ARRASTAVEIS"))
                    connection.execute(text("DROP TABLE IF EXISTS #DISTRIBUICAO"))
                except:
                    pass
                raise

    except Exception as e:
        logging.error(f"Erro ao aplicar regra de arrasto para contratos sem acordo: {str(e)}")
        return 0


def distribuir_demais_contratos(edital_id, periodo_id):
    """
    Distribui os demais contratos sem acordo conforme percentuais.

    Este passo implementa o item 1.1.5 do documento:
    - Distribui os contratos restantes considerando os percentuais de cada empresa
    - Usa critério de seleção 4 (Demais contratos sem acordo)

    Args:
        edital_id (int): ID do edital
        periodo_id (int): ID do período

    Returns:
        int: Quantidade de contratos distribuídos
    """
    try:
        with db.engine.connect() as connection:
            # Verificando se há transação ativa e fazendo commit/rollback se necessário
            connection.execute(text("IF @@TRANCOUNT > 0 COMMIT"))

            try:
                # Contar quantos contratos restantes sem acordo existem
                count_restantes_sql = text("SELECT COUNT(*) FROM DEV.DCA_TB006_DISTRIBUIVEIS")
                qtde_contratos_restantes = connection.execute(count_restantes_sql).scalar()

                if qtde_contratos_restantes == 0:
                    logging.info("Não há contratos restantes para distribuir")
                    return 0

                # Obter a data de referência atual
                data_referencia_sql = text("SELECT CONVERT(DATE, GETDATE())")
                dt_referencia = connection.execute(data_referencia_sql).scalar()

                # Garantir que a tabela temporária seja criada corretamente
                # Buscar limites de distribuição para empresas - usar uma única instrução SQL
                limites_sql = text("""
                -- Verificar se a tabela temporária já existe e excluí-la se necessário
                IF OBJECT_ID('tempdb..#LIMITES') IS NOT NULL
                    DROP TABLE #LIMITES;

                -- Selecionar empresas e seus limites
                SELECT 
                    LD.ID_EMPRESA,
                    LD.PERCENTUAL_FINAL,
                    -- Contar quantos contratos já foram distribuídos para esta empresa
                    QTDE_JA_DISTRIBUIDA = ISNULL((
                        SELECT COUNT(*) 
                        FROM DEV.DCA_TB005_DISTRIBUICAO DIST
                        WHERE DIST.ID_EDITAL = :edital_id
                          AND DIST.ID_PERIODO = :periodo_id
                          AND DIST.COD_EMPRESA_COBRANCA = LD.ID_EMPRESA
                          AND DIST.DELETED_AT IS NULL
                    ), 0)
                INTO #LIMITES
                FROM DEV.DCA_TB003_LIMITES_DISTRIBUICAO LD
                WHERE LD.ID_EDITAL = :edital_id
                  AND LD.ID_PERIODO = :periodo_id
                  AND LD.DELETED_AT IS NULL
                """).bindparams(edital_id=edital_id, periodo_id=periodo_id)

                connection.execute(limites_sql)

                # Verificar total de empresas
                count_empresas_sql = text("SELECT COUNT(*) FROM #LIMITES")
                total_empresas = connection.execute(count_empresas_sql).scalar()

                if total_empresas == 0:
                    logging.warning("Não foram encontrados limites cadastrados para as empresas")
                    connection.execute(text("DROP TABLE IF EXISTS #LIMITES"))
                    return 0

                # Calcular total já distribuído
                total_distribuido_sql = text("SELECT SUM(QTDE_JA_DISTRIBUIDA) FROM #LIMITES")
                total_ja_distribuido = connection.execute(total_distribuido_sql).scalar() or 0

                # Obter total de contratos no universo
                total_universo_sql = text("""
                SELECT 
                    TOTAL_UNIVERSO = (
                        SELECT COUNT(*) FROM DEV.DCA_TB005_DISTRIBUICAO
                        WHERE ID_EDITAL = :edital_id
                          AND ID_PERIODO = :periodo_id
                          AND DELETED_AT IS NULL
                    ) + (
                        SELECT COUNT(*) FROM DEV.DCA_TB006_DISTRIBUIVEIS
                    )
                """).bindparams(edital_id=edital_id, periodo_id=periodo_id)

                total_universo = connection.execute(total_universo_sql).scalar() or 0

                if total_universo == 0:
                    logging.warning("Não há contratos no universo total")
                    connection.execute(text("DROP TABLE IF EXISTS #LIMITES"))
                    return 0

                # Calcular a quantidade ainda a distribuir para cada empresa
                calcular_qtde_sql = text("""
                -- Calcular quantidade restante a distribuir para cada empresa
                SELECT 
                    ID = ROW_NUMBER() OVER (ORDER BY NEWID()), -- Ordem aleatória 
                    ID_EMPRESA,
                    PERCENTUAL_FINAL,
                    QTDE_JA_DISTRIBUIDA,
                    QTDE_TOTAL_ESPERADA = FLOOR(:total_universo * PERCENTUAL_FINAL / 100),
                    QTDE_A_DISTRIBUIR = FLOOR(:total_universo * PERCENTUAL_FINAL / 100) - QTDE_JA_DISTRIBUIDA
                INTO #ASSESSORIAS
                FROM #LIMITES
                WHERE FLOOR(:total_universo * PERCENTUAL_FINAL / 100) > QTDE_JA_DISTRIBUIDA
                """).bindparams(total_universo=total_universo)

                connection.execute(calcular_qtde_sql)

                # Calcular total a distribuir
                total_a_distribuir_sql = text("SELECT SUM(QTDE_A_DISTRIBUIR) FROM #ASSESSORIAS")
                total_a_distribuir = connection.execute(total_a_distribuir_sql).scalar() or 0

                # Verificar se há excesso de contratos a distribuir
                ajuste = 0
                if total_a_distribuir > qtde_contratos_restantes:
                    ajuste = total_a_distribuir - qtde_contratos_restantes
                    # Ajustar proporcionalmente
                    connection.execute(text("""
                    UPDATE #ASSESSORIAS
                    SET QTDE_A_DISTRIBUIR = QTDE_A_DISTRIBUIR - 
                        CEILING((QTDE_A_DISTRIBUIR * 1.0 / :total_a_distribuir) * :ajuste)
                    WHERE QTDE_A_DISTRIBUIR > 0
                    """).bindparams(total_a_distribuir=total_a_distribuir, ajuste=ajuste))

                # Ordenar os contratos de forma aleatória
                connection.execute(text("""
                SELECT
                    FkContratoSISCTR,
                    NR_CPF_CNPJ,
                    RowNum = ROW_NUMBER() OVER (ORDER BY NEWID())
                INTO #DISTRIBUIVEIS
                FROM DEV.DCA_TB006_DISTRIBUIVEIS
                """))

                # Criar tabela para distribuição
                connection.execute(text("""
                CREATE TABLE #DISTRIBUICAO (
                    FkContratoSISCTR BIGINT,
                    ID INT,
                    NR_CPF_CNPJ BIGINT
                )
                """))

                # Distribuir os contratos usando cursor
                connection.execute(text("""
                DECLARE @ID INT
                DECLARE @ID_EMPRESA INT
                DECLARE @PERCENTUAL DECIMAL(6,2)
                DECLARE @QUANTIDADE INT
                DECLARE @START_ROW INT = 1
                DECLARE @END_ROW INT

                DECLARE DISTRIBUICAO_CURSOR CURSOR FOR
                SELECT ID, ID_EMPRESA, PERCENTUAL_FINAL, QTDE_A_DISTRIBUIR
                FROM #ASSESSORIAS
                WHERE QTDE_A_DISTRIBUIR > 0

                OPEN DISTRIBUICAO_CURSOR
                FETCH NEXT FROM DISTRIBUICAO_CURSOR INTO @ID, @ID_EMPRESA, @PERCENTUAL, @QUANTIDADE

                WHILE @@FETCH_STATUS = 0
                BEGIN
                    -- Calcular linha final para esta empresa
                    SET @END_ROW = @START_ROW + @QUANTIDADE - 1

                    -- Inserir na tabela de distribuição
                    INSERT INTO #DISTRIBUICAO
                    SELECT
                        FkContratoSISCTR,
                        @ID,
                        NR_CPF_CNPJ
                    FROM #DISTRIBUIVEIS
                    WHERE RowNum BETWEEN @START_ROW AND @END_ROW

                    -- Atualizar linha inicial para próxima empresa
                    SET @START_ROW = @END_ROW + 1

                    FETCH NEXT FROM DISTRIBUICAO_CURSOR INTO @ID, @ID_EMPRESA, @PERCENTUAL, @QUANTIDADE
                END

                CLOSE DISTRIBUICAO_CURSOR
                DEALLOCATE DISTRIBUICAO_CURSOR
                """))

                # Inserir na tabela final de distribuição
                inserir_distribuicao_sql = text("""
                INSERT INTO DEV.DCA_TB005_DISTRIBUICAO (
                    DT_REFERENCIA,
                    ID_EDITAL,
                    ID_PERIODO,
                    FkContratoSISCTR,
                    COD_CRITERIO_SELECAO,
                    COD_EMPRESA_COBRANCA,
                    NR_CPF_CNPJ,
                    CREATED_AT,
                    UPDATED_AT,
                    DELETED_AT
                )
                SELECT
                    :dt_referencia,
                    :edital_id,
                    :periodo_id,
                    DIS.FkContratoSISCTR,
                    4, -- Código 4: Demais contratos sem acordo
                    ASS.ID_EMPRESA,
                    DIS.NR_CPF_CNPJ,
                    GETDATE(),
                    NULL,
                    NULL
                FROM #DISTRIBUICAO AS DIS
                JOIN #ASSESSORIAS AS ASS
                ON DIS.ID = ASS.ID
                """).bindparams(
                    dt_referencia=dt_referencia,
                    edital_id=edital_id,
                    periodo_id=periodo_id
                )

                result = connection.execute(inserir_distribuicao_sql)
                contratos_distribuidos = result.rowcount

                # Remover os contratos distribuídos da tabela de distribuíveis
                if contratos_distribuidos > 0:
                    remover_sql = text("""
                    DELETE FROM DEV.DCA_TB006_DISTRIBUIVEIS
                    WHERE FkContratoSISCTR IN (
                        SELECT FkContratoSISCTR FROM #DISTRIBUICAO
                    )
                    """)
                    connection.execute(remover_sql)

                # Limpar tabelas temporárias
                connection.execute(text("DROP TABLE IF EXISTS #LIMITES"))
                connection.execute(text("DROP TABLE IF EXISTS #ASSESSORIAS"))
                connection.execute(text("DROP TABLE IF EXISTS #DISTRIBUIVEIS"))
                connection.execute(text("DROP TABLE IF EXISTS #DISTRIBUICAO"))

                logging.info(f"Demais contratos sem acordo distribuídos: {contratos_distribuidos}")
                return contratos_distribuidos

            except Exception as e:
                logging.error(f"Erro durante a distribuição dos demais contratos: {str(e)}")
                import traceback
                logging.error(traceback.format_exc())
                # Limpar tabelas temporárias em caso de erro
                try:
                    connection.execute(text("DROP TABLE IF EXISTS #LIMITES"))
                    connection.execute(text("DROP TABLE IF EXISTS #ASSESSORIAS"))
                    connection.execute(text("DROP TABLE IF EXISTS #DISTRIBUIVEIS"))
                    connection.execute(text("DROP TABLE IF EXISTS #DISTRIBUICAO"))
                except:
                    pass
                raise

    except Exception as e:
        logging.error(f"Erro ao distribuir os demais contratos: {str(e)}")
        return 0

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

        return resultados

    except Exception as e:
        logging.error(f"Erro no processo de distribuição: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return resultados
