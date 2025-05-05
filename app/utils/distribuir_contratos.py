from app import db
from sqlalchemy import text, create_engine
from sqlalchemy.pool import QueuePool
import pandas as pd
import logging
import time
from datetime import datetime
import os
import traceback

# Configuração de logging melhorada
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("distribuicao.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# Configuração de pool de conexões
def get_engine():
    """Retorna engine com connection pooling configurado"""
    connection_string = db.engine.url
    return create_engine(
        connection_string,
        poolclass=QueuePool,
        pool_size=10,
        max_overflow=20,
        pool_recycle=3600
    )


def executar_query(query, params=None, fetchall=False):
    """Função centralizada para executar queries com tratamento de erros"""
    try:
        with db.engine.connect() as connection:
            if params:
                result = connection.execute(text(query), params)
            else:
                result = connection.execute(text(query))

            if fetchall:
                return result.fetchall()
            return result
    except Exception as e:
        logger.error(f"Erro ao executar query: {str(e)}")
        logger.error(traceback.format_exc())
        raise


def selecionar_contratos_distribuiveis():
    """
    Seleciona o universo de contratos que serão distribuídos e os armazena na tabela DCA_TB006_DISTRIBUIVEIS.
    Usa as tabelas do Banco de Dados Gerencial (BDG).
    """
    try:
        logger.info("Iniciando seleção de contratos distribuíveis")
        # Limpar tabelas de distribuíveis e arrastaveis
        executar_query("TRUNCATE TABLE [DEV].[DCA_TB006_DISTRIBUIVEIS]")
        executar_query("TRUNCATE TABLE [DEV].[DCA_TB007_ARRASTAVEIS]")

        # Inserir os contratos selecionados
        query_selecao = """
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
            )
        """

        # Executar a inserção
        result = executar_query(query_selecao)
        logger.info(f"Inserção concluída - linhas afetadas: {result.rowcount}")

        # Contar quantos contratos foram selecionados
        result = executar_query("SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] WHERE DELETED_AT IS NULL",
                                fetchall=True)
        num_contratos = result[0][0] if result else 0

        logger.info(f"Total de contratos selecionados: {num_contratos}")
        return num_contratos

    except Exception as e:
        logger.error(f"Erro na seleção de contratos: {str(e)}")
        return 0


def distribuir_acordos_vigentes_empresas_permanece(edital_id, periodo_id):
    """
    Distribui contratos com acordos vigentes para empresas que permanecem.
    Implementa o item 1.1.1 dos requisitos.
    """
    start_time = time.time()
    logger.info(
        f"Iniciando distribuição de acordos vigentes para empresas que permanecem - Edital: {edital_id}, Período: {periodo_id}")

    try:
        # 1. Limpar a tabela de distribuição apenas para este edital/período
        executar_query(
            "DELETE FROM [DEV].[DCA_TB005_DISTRIBUICAO] WHERE ID_EDITAL = :edital_id AND ID_PERIODO = :periodo_id",
            {"edital_id": edital_id, "periodo_id": periodo_id}
        )

        # 2. Inserir contratos com acordos vigentes para empresas que permanecem
        query = """
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
        """

        resultado = executar_query(query, {"edital_id": edital_id, "periodo_id": periodo_id})
        contratos_distribuidos = resultado.rowcount

        # 3. Remover os contratos inseridos da tabela de distribuíveis
        if contratos_distribuidos > 0:
            query_delete = """
            DELETE DIS
            FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] AS DIS
            WHERE DIS.[FkContratoSISCTR] IN (
                SELECT DIST.[fkContratoSISCTR]
                FROM [DEV].[DCA_TB005_DISTRIBUICAO] AS DIST 
                WHERE DIST.ID_EDITAL = :edital_id
                AND DIST.ID_PERIODO = :periodo_id
                AND DIST.COD_CRITERIO_SELECAO = 1
            )
            """
            executar_query(query_delete, {"edital_id": edital_id, "periodo_id": periodo_id})

        db.session.commit()
        elapsed_time = time.time() - start_time
        logger.info(
            f"{contratos_distribuidos} contratos com acordo distribuídos para empresas que permanecem em {elapsed_time:.2f}s")

        return contratos_distribuidos

    except Exception as e:
        db.session.rollback()
        logger.error(f"Erro ao distribuir contratos com acordo para empresas que permanecem: {str(e)}")
        logger.error(traceback.format_exc())
        return 0


def distribuir_acordos_vigentes_empresas_descredenciadas(edital_id, periodo_id):
    """
    Distribui contratos com acordos vigentes de empresas descredenciadas.
    Implementa o item 1.1.2 dos requisitos.
    """
    start_time = time.time()
    logger.info(
        f"Iniciando distribuição de acordos de empresas descredenciadas - Edital: {edital_id}, Período: {periodo_id}")

    try:
        # 1. Obter lista de empresas participantes (não descredenciadas)
        empresas_query = """
        SELECT ID_EMPRESA
        FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES]
        WHERE ID_EDITAL = :edital_id
        AND ID_PERIODO = :periodo_id
        AND DS_CONDICAO <> 'DESCREDENCIADA'
        """

        empresas = executar_query(empresas_query, {"edital_id": edital_id, "periodo_id": periodo_id}, fetchall=True)

        if not empresas:
            logger.warning("Nenhuma empresa participante encontrada para o período atual")
            return 0

        empresas_ids = [empresa[0] for empresa in empresas]
        total_empresas = len(empresas_ids)

        # 2. Identificar contratos com acordos vigentes de empresas descredenciadas
        contratos_query = """
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

        contratos = executar_query(contratos_query, {"edital_id": edital_id, "periodo_id": periodo_id}, fetchall=True)
        total_contratos = len(contratos)

        if total_contratos == 0:
            logger.info("Nenhum contrato com acordo vigente de empresa descredenciada encontrado")
            return 0

        # 3. Otimização: Usar pandas para distribuição equitativa entre empresas
        df_contratos = pd.DataFrame(contratos, columns=['contrato_id', 'cpf_cnpj', 'valor'])

        # Distribuir igualmente entre empresas (rotação circular)
        df_contratos['empresa_id'] = [empresas_ids[i % total_empresas] for i in range(len(df_contratos))]

        # 4. Inserção em lote
        contratos_inseridos = 0
        batch_size = 1000  # Tamanho do lote para inserções

        for i in range(0, len(df_contratos), batch_size):
            batch = df_contratos.iloc[i:i + batch_size]

            # Preparar parâmetros para inserção em lote
            valores = []
            for _, row in batch.iterrows():
                valores.append(
                    f"(GETDATE(), {edital_id}, {periodo_id}, {row['contrato_id']}, {row['empresa_id']}, 3, {row['cpf_cnpj']}, {row['valor']}, GETDATE())")

            query_insert = f"""
            INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
            ([DT_REFERENCIA], [ID_EDITAL], [ID_PERIODO], [fkContratoSISCTR], 
            [COD_EMPRESA_COBRANCA], [COD_CRITERIO_SELECAO], [NR_CPF_CNPJ], 
            [VR_SD_DEVEDOR], [CREATED_AT])
            VALUES {','.join(valores)}
            """

            executar_query(query_insert)
            contratos_inseridos += len(batch)

            logger.info(f"Progresso: {contratos_inseridos}/{total_contratos} contratos inseridos")

        # 5. Excluir os contratos inseridos da tabela de distribuíveis
        contratos_ids = df_contratos['contrato_id'].tolist()

        # Excluir em lotes
        for i in range(0, len(contratos_ids), batch_size):
            batch_ids = contratos_ids[i:i + batch_size]
            placeholders = ','.join(str(id) for id in batch_ids)

            query_delete = f"""
            DELETE FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
            WHERE [FkContratoSISCTR] IN ({placeholders})
            """

            executar_query(query_delete)

        db.session.commit()
        elapsed_time = time.time() - start_time
        logger.info(
            f"{contratos_inseridos} contratos com acordo de empresas descredenciadas distribuídos em {elapsed_time:.2f}s")

        return contratos_inseridos

    except Exception as e:
        db.session.rollback()
        logger.error(f"Erro ao distribuir contratos de empresas descredenciadas: {str(e)}")
        logger.error(traceback.format_exc())
        return 0


def aplicar_regra_arrasto_acordos(edital_id, periodo_id):
    """
    Aplica a regra de arrasto com acordo para distribuição de contratos.
    Implementa o item 1.1.3 dos requisitos.
    """
    start_time = time.time()
    logger.info(f"Iniciando regra de arrasto com acordo - Edital: {edital_id}, Período: {periodo_id}")
    contratos_inseridos = 0

    try:
        # 1. Verificar existência de acordos
        query_acordos = """
        SELECT DISTINCT NR_CPF_CNPJ, COD_EMPRESA_COBRANCA
        FROM [DEV].[DCA_TB005_DISTRIBUICAO]
        WHERE ID_EDITAL = :edital_id
        AND ID_PERIODO = :periodo_id
        AND COD_CRITERIO_SELECAO IN (1, 3)
        """

        acordos = executar_query(query_acordos, {"edital_id": edital_id, "periodo_id": periodo_id}, fetchall=True)

        if not acordos:
            logger.info("Nenhum CPF/CNPJ com acordo foi encontrado.")
            return 0

        total_cpfs = len(acordos)
        logger.info(f"Total de CPFs/CNPJs com acordo: {total_cpfs}")

        # 2. Usar pandas para processamento eficiente
        df_acordos = pd.DataFrame(acordos, columns=['cpf_cnpj', 'empresa_id'])

        # Processar em lotes para otimizar
        batch_size = 100
        batches = [df_acordos[i:i + batch_size] for i in range(0, len(df_acordos), batch_size)]

        for batch_index, batch in enumerate(batches):
            logger.info(f"Processando lote {batch_index + 1}/{len(batches)} de CPFs com acordo")

            for _, acordo in batch.iterrows():
                cpf_cnpj = acordo['cpf_cnpj']
                empresa_id = acordo['empresa_id']

                # Buscar contratos do mesmo CPF
                query_contratos = """
                SELECT FkContratoSISCTR, VR_SD_DEVEDOR
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                WHERE NR_CPF_CNPJ = :cpf_cnpj
                """

                contratos = executar_query(query_contratos, {"cpf_cnpj": cpf_cnpj}, fetchall=True)

                if not contratos:
                    continue

                contratos_batch = []
                ids_remover = []

                for contrato in contratos:
                    contrato_id = contrato[0]
                    valor = contrato[1]

                    # Verificar se já foi distribuído
                    query_check = """
                    SELECT COUNT(*)
                    FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                    WHERE fkContratoSISCTR = :contrato_id
                    AND ID_EDITAL = :edital_id
                    AND ID_PERIODO = :periodo_id
                    """

                    check = executar_query(
                        query_check,
                        {"contrato_id": contrato_id, "edital_id": edital_id, "periodo_id": periodo_id},
                        fetchall=True
                    )

                    if check[0][0] == 0:  # Não distribuído
                        contratos_batch.append((
                            contrato_id,
                            empresa_id,
                            cpf_cnpj,
                            valor
                        ))
                        ids_remover.append(str(contrato_id))

                if contratos_batch:
                    # Inserir em lote
                    valores = []
                    for c_id, e_id, cpf, val in contratos_batch:
                        valores.append(
                            f"(GETDATE(), {edital_id}, {periodo_id}, {c_id}, {e_id}, 6, {cpf}, {val}, GETDATE())")

                    query_insert = f"""
                    INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
                    ([DT_REFERENCIA], [ID_EDITAL], [ID_PERIODO], [fkContratoSISCTR], 
                    [COD_EMPRESA_COBRANCA], [COD_CRITERIO_SELECAO], [NR_CPF_CNPJ], 
                    [VR_SD_DEVEDOR], [CREATED_AT])
                    VALUES {','.join(valores)}
                    """

                    executar_query(query_insert)

                    # Remover da tabela de distribuíveis
                    if ids_remover:
                        query_delete = f"""
                        DELETE FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                        WHERE FkContratoSISCTR IN ({','.join(ids_remover)})
                        """

                        executar_query(query_delete)

                    contratos_inseridos += len(contratos_batch)

            # Commit após cada lote
            db.session.commit()
            logger.info(f"Progresso: {contratos_inseridos} contratos distribuídos até o momento")

        elapsed_time = time.time() - start_time
        logger.info(
            f"Regra de arrasto com acordo concluída: {contratos_inseridos} contratos distribuídos em {elapsed_time:.2f}s")

        return contratos_inseridos

    except Exception as e:
        db.session.rollback()
        logger.error(f"Erro na regra de arrasto com acordo: {str(e)}")
        logger.error(traceback.format_exc())
        return 0


def aplicar_regra_arrasto_sem_acordo(edital_id, periodo_id):
    """
    Aplica a regra de arrasto para contratos sem acordo.
    Implementa o item 1.1.4 dos requisitos.
    """
    start_time = time.time()
    logger.info(f"Iniciando regra de arrasto sem acordo - Edital: {edital_id}, Período: {periodo_id}")

    try:
        # 1. Identificar CPFs com múltiplos contratos usando pandas
        query_cpfs = """
        SELECT NR_CPF_CNPJ, COUNT(*) as num_contratos
        FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
        WHERE NR_CPF_CNPJ IS NOT NULL
        GROUP BY NR_CPF_CNPJ
        HAVING COUNT(*) > 1
        """

        cpfs_result = executar_query(query_cpfs, fetchall=True)

        if not cpfs_result:
            logger.info("Nenhum CPF com múltiplos contratos encontrado.")
            return 0

        # Converter para DataFrame
        df_cpfs = pd.DataFrame(cpfs_result, columns=['cpf_cnpj', 'num_contratos'])
        total_cpfs = len(df_cpfs)
        logger.info(f"Encontrados {total_cpfs} CPFs com múltiplos contratos")

        # 2. Buscar empresas participantes com percentuais
        query_empresas = """
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
        """

        empresas = executar_query(query_empresas, {"edital_id": edital_id, "periodo_id": periodo_id}, fetchall=True)

        if not empresas:
            logger.warning("Nenhuma empresa encontrada para distribuição.")
            return 0

        # Converter para DataFrame
        df_empresas = pd.DataFrame(empresas, columns=['empresa_id', 'percentual'])

        # 3. Normalizar percentuais
        total_percentual = df_empresas['percentual'].sum()

        if total_percentual <= 0:
            percentual_igual = 100.0 / len(df_empresas)
            df_empresas['percentual_norm'] = percentual_igual
        else:
            fator = 100.0 / total_percentual
            df_empresas['percentual_norm'] = df_empresas['percentual'] * fator

        # 4. Calcular alocação de CPFs por empresa
        df_empresas['qtd_cpfs'] = (df_empresas['percentual_norm'] * total_cpfs / 100).apply(lambda x: max(1, int(x)))

        # Ajustar para caso a soma não bata com o total
        total_alocado = df_empresas['qtd_cpfs'].sum()
        diferenca = total_cpfs - total_alocado

        if diferenca != 0:
            # Ordenar por percentual (descendente ou ascendente dependendo do ajuste)
            ordem = False if diferenca > 0 else True  # False = descendente
            df_empresas_sorted = df_empresas.sort_values('percentual_norm', ascending=ordem)

            for idx in range(abs(diferenca)):
                if idx < len(df_empresas_sorted):
                    empresa_idx = df_empresas_sorted.index[idx % len(df_empresas_sorted)]
                    df_empresas.at[empresa_idx, 'qtd_cpfs'] += 1 if diferenca > 0 else -1

        # 5. Distribuir CPFs entre empresas
        empresa_index = 0
        empresas_ids = df_empresas['empresa_id'].tolist()
        cpfs_por_empresa = df_empresas.set_index('empresa_id')['qtd_cpfs'].to_dict()

        # Preparar para processamento em lotes
        batch_size = 100
        df_cpfs_batches = [df_cpfs[i:i + batch_size] for i in range(0, len(df_cpfs), batch_size)]

        contratos_distribuidos = 0

        for batch_index, batch_cpfs in enumerate(df_cpfs_batches):
            logger.info(f"Processando lote {batch_index + 1}/{len(df_cpfs_batches)} de CPFs múltiplos")

            for _, cpf_row in batch_cpfs.iterrows():
                cpf = cpf_row['cpf_cnpj']

                # Determinar empresa para este CPF
                empresa_id = None
                while True:
                    empresa_candidata = empresas_ids[empresa_index % len(empresas_ids)]
                    if cpfs_por_empresa.get(empresa_candidata, 0) > 0:
                        empresa_id = empresa_candidata
                        cpfs_por_empresa[empresa_id] -= 1
                        break
                    empresa_index += 1

                # Buscar contratos deste CPF
                query_contratos = """
                SELECT FkContratoSISCTR, NR_CPF_CNPJ, VR_SD_DEVEDOR
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                WHERE NR_CPF_CNPJ = :cpf
                """

                contratos = executar_query(query_contratos, {"cpf": cpf}, fetchall=True)

                if not contratos:
                    continue

                contratos_batch = []
                ids_remover = []

                for contrato in contratos:
                    contrato_id = contrato[0]
                    cpf_cnpj = contrato[1]
                    valor = contrato[2]

                    # Verificar se já distribuído
                    query_check = """
                    SELECT COUNT(*)
                    FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                    WHERE fkContratoSISCTR = :contrato_id
                    AND ID_EDITAL = :edital_id
                    AND ID_PERIODO = :periodo_id
                    """

                    check = executar_query(
                        query_check,
                        {"contrato_id": contrato_id, "edital_id": edital_id, "periodo_id": periodo_id},
                        fetchall=True
                    )

                    if check[0][0] == 0:  # Não distribuído
                        contratos_batch.append((
                            contrato_id,
                            empresa_id,
                            cpf_cnpj,
                            valor
                        ))
                        ids_remover.append(str(contrato_id))

                if contratos_batch:
                    # Inserir em lote
                    valores = []
                    for c_id, e_id, cpf, val in contratos_batch:
                        if val is None:
                            val = 0
                        valores.append(
                            f"(GETDATE(), {edital_id}, {periodo_id}, {c_id}, {e_id}, 6, {cpf}, {val}, GETDATE())")

                    query_insert = f"""
                    INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
                    ([DT_REFERENCIA], [ID_EDITAL], [ID_PERIODO], [fkContratoSISCTR], 
                    [COD_EMPRESA_COBRANCA], [COD_CRITERIO_SELECAO], [NR_CPF_CNPJ], 
                    [VR_SD_DEVEDOR], [CREATED_AT])
                    VALUES {','.join(valores)}
                    """

                    executar_query(query_insert)

                    # Remover da tabela de distribuíveis
                    if ids_remover:
                        query_delete = f"""
                        DELETE FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                        WHERE FkContratoSISCTR IN ({','.join(ids_remover)})
                        """

                        executar_query(query_delete)

                    contratos_distribuidos += len(contratos_batch)

                empresa_index += 1

            # Commit após cada lote
            db.session.commit()
            logger.info(f"Progresso: {contratos_distribuidos} contratos distribuídos até o momento")

        elapsed_time = time.time() - start_time
        logger.info(
            f"Regra de arrasto sem acordo concluída: {contratos_distribuidos} contratos distribuídos em {elapsed_time:.2f}s")

        return contratos_distribuidos

    except Exception as e:
        db.session.rollback()
        logger.error(f"Erro crítico na regra de arrasto sem acordo: {str(e)}")
        logger.error(traceback.format_exc())
        return 0


def distribuir_demais_contratos(edital_id, periodo_id):
    """
    Distribui os contratos restantes entre as empresas.
    Implementa o item 1.1.5 dos requisitos.
    """
    start_time = time.time()
    logger.info(f"Iniciando distribuição dos demais contratos sem acordo - Edital: {edital_id}, Período: {periodo_id}")

    try:
        # 1. Buscar empresas participantes e seus percentuais
        query_empresas = """
        SELECT EP.ID_EMPRESA, COALESCE(LD.PERCENTUAL_FINAL, 0) AS percentual
        FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES] EP
        LEFT JOIN [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
            ON EP.ID_EMPRESA = LD.ID_EMPRESA
            AND EP.ID_EDITAL = LD.ID_EDITAL
            AND EP.ID_PERIODO = LD.ID_PERIODO
        WHERE EP.ID_EDITAL = :edital_id
        AND EP.ID_PERIODO = :periodo_id
        AND EP.DS_CONDICAO <> 'DESCREDENCIADA'
        """

        empresas = executar_query(query_empresas, {"edital_id": edital_id, "periodo_id": periodo_id}, fetchall=True)

        if not empresas:
            logger.warning("Nenhuma empresa participante encontrada.")
            return 0

        # Converter para DataFrame
        df_empresas = pd.DataFrame(empresas, columns=['empresa_id', 'percentual'])

        # 2. Verificar se existem contratos para distribuir
        query_contratos_count = "SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]"
        contratos_count = executar_query(query_contratos_count, fetchall=True)[0][0]

        if contratos_count == 0:
            logger.info("Nenhum contrato restante para distribuir.")
            return 0

        logger.info(f"Total de contratos restantes: {contratos_count}")

        # 3. Normalizar percentuais
        total_percentual = df_empresas['percentual'].sum()

        if total_percentual <= 0:
            percentual_igual = 100.0 / len(df_empresas)
            df_empresas['percentual_norm'] = percentual_igual
        else:
            fator = 100.0 / total_percentual
            df_empresas['percentual_norm'] = df_empresas['percentual'] * fator

        # 4. Contar contratos atuais por empresa
        query_contratos_atuais = """
        SELECT COD_EMPRESA_COBRANCA, COUNT(*) as qtd
        FROM [DEV].[DCA_TB005_DISTRIBUICAO]
        WHERE ID_EDITAL = :edital_id
        AND ID_PERIODO = :periodo_id
        GROUP BY COD_EMPRESA_COBRANCA
        """

        contratos_atuais = executar_query(
            query_contratos_atuais,
            {"edital_id": edital_id, "periodo_id": periodo_id},
            fetchall=True
        )

        df_contratos_atuais = pd.DataFrame(
            contratos_atuais,
            columns=['empresa_id', 'qtd_atual']
        ).set_index('empresa_id')

        # Adicionar à tabela de empresas
        df_empresas['qtd_atual'] = df_empresas['empresa_id'].apply(
            lambda x: df_contratos_atuais.at[x, 'qtd_atual'] if x in df_contratos_atuais.index else 0
        )

        # 5. Calcular meta total e contratos adicionais
        total_contratos_atuais = df_empresas['qtd_atual'].sum()
        total_geral = total_contratos_atuais + contratos_count

        df_empresas['meta_total'] = (df_empresas['percentual_norm'] * total_geral / 100).apply(int)
        df_empresas['adicional'] = df_empresas.apply(
            lambda row: max(0, row['meta_total'] - row['qtd_atual']), axis=1
        )

        # 6. Verificar e ajustar inconsistências
        total_adicional = df_empresas['adicional'].sum()
        if total_adicional != contratos_count:
            diferenca = contratos_count - total_adicional
            logger.info(f"Ajustando diferença de {diferenca} contratos")

            # Ordenar por percentual (decrescente ou crescente conforme necessidade)
            ordem = False if diferenca > 0 else True  # False = descendente
            df_empresas_sorted = df_empresas.sort_values('percentual_norm', ascending=ordem)

            for idx in range(abs(diferenca)):
                if idx < len(df_empresas_sorted):
                    empresa_idx = df_empresas_sorted.index[idx % len(df_empresas_sorted)]
                    df_empresas.at[empresa_idx, 'adicional'] += 1 if diferenca > 0 else -1

        # 7. Buscar contratos para distribuição em lotes
        contratos_distribuidos = 0
        batch_size = 1000

        # Preparar dicionário de alocação
        contratos_por_empresa = df_empresas[['empresa_id', 'adicional']].set_index('empresa_id')['adicional'].to_dict()

        # Ordenar empresas por percentual para distribuição
        empresas_ordenadas = df_empresas.sort_values('percentual_norm', ascending=False)['empresa_id'].tolist()

        # Processar em vários lotes pequenos
        offset = 0
        while offset < contratos_count:
            query_contratos = f"""
            SELECT TOP {batch_size} FkContratoSISCTR, NR_CPF_CNPJ, VR_SD_DEVEDOR 
            FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
            ORDER BY FkContratoSISCTR
            OFFSET {offset} ROWS
            """

            contratos_batch = executar_query(query_contratos, fetchall=True)

            if not contratos_batch:
                break

            # Converter para DataFrame
            df_contratos = pd.DataFrame(contratos_batch, columns=['contrato_id', 'cpf_cnpj', 'valor'])

            # Distribuir estes contratos
            resultados_batch = []

            for empresa_id in empresas_ordenadas:
                adicional_pendente = contratos_por_empresa.get(empresa_id, 0)
                if adicional_pendente <= 0 or df_contratos.empty:
                    continue

                # Pegar os contratos alocados para esta empresa
                contratos_empresa = df_contratos.iloc[:adicional_pendente]
                df_contratos = df_contratos.iloc[adicional_pendente:]

                # Adicionar a empresa_id
                contratos_empresa['empresa_id'] = empresa_id

                # Adicionar à lista de resultados
                resultados_batch.append(contratos_empresa)

                # Atualizar o contador
                contratos_por_empresa[empresa_id] -= len(contratos_empresa)

            # Juntar resultados
            if resultados_batch:
                df_resultados = pd.concat(resultados_batch)

                # Preparar para inserção em lote
                valores = []
                ids_remover = []

                for _, row in df_resultados.iterrows():
                    contrato_id = row['contrato_id']
                    cpf_cnpj = row['cpf_cnpj']
                    valor = row['valor'] if row['valor'] is not None else 0
                    empresa_id = row['empresa_id']

                    valores.append(
                        f"(GETDATE(), {edital_id}, {periodo_id}, {contrato_id}, {empresa_id}, 4, {cpf_cnpj}, {valor}, GETDATE())")
                    ids_remover.append(str(contrato_id))

                # Inserir na distribuição
                if valores:
                    query_insert = f"""
                    INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
                    ([DT_REFERENCIA], [ID_EDITAL], [ID_PERIODO], [fkContratoSISCTR], 
                    [COD_EMPRESA_COBRANCA], [COD_CRITERIO_SELECAO], [NR_CPF_CNPJ], 
                    [VR_SD_DEVEDOR], [CREATED_AT])
                    VALUES {','.join(valores)}
                    """

                    executar_query(query_insert)

                # Remover da tabela de distribuíveis
                if ids_remover:
                    query_delete = f"""
                    DELETE FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                    WHERE FkContratoSISCTR IN ({','.join(ids_remover)})
                    """

                    executar_query(query_delete)

                contratos_distribuidos += len(df_resultados)

            # Atualizar offset
            offset += batch_size

            # Commit após cada lote
            db.session.commit()
            logger.info(f"Progresso: {contratos_distribuidos}/{contratos_count} contratos distribuídos")

        elapsed_time = time.time() - start_time
        logger.info(
            f"Distribuição dos demais contratos concluída: {contratos_distribuidos} contratos em {elapsed_time:.2f}s")

        return contratos_distribuidos

    except Exception as e:
        db.session.rollback()
        logger.error(f"Erro ao distribuir demais contratos: {str(e)}")
        logger.error(traceback.format_exc())
        return 0


def atualizar_limites_distribuicao(edital_id, periodo_id):
    """
    Atualiza a tabela de limites de distribuição preenchendo:
    - VR_ARRECADACAO: valor arrecadado por cada empresa
    - QTDE_MAXIMA: quantidade máxima de contratos por empresa
    - VALOR_MAXIMO: valor máximo distribuído por empresa
    - PERCENTUAL_FINAL: mantém os percentuais originais
    """
    start_time = time.time()
    logger.info(f"Atualizando tabela de limites de distribuição - Edital: {edital_id}, Período: {periodo_id}")

    try:
        # 1. Obter estatísticas de distribuição por empresa
        query_estatisticas = """
        SELECT 
            COD_EMPRESA_COBRANCA,
            COUNT(*) as quantidade,
            SUM(VR_SD_DEVEDOR) as valor_total
        FROM [DEV].[DCA_TB005_DISTRIBUICAO]
        WHERE ID_EDITAL = :edital_id
        AND ID_PERIODO = :periodo_id
        AND DELETED_AT IS NULL
        GROUP BY COD_EMPRESA_COBRANCA
        """

        estatisticas = executar_query(
            query_estatisticas,
            {"edital_id": edital_id, "periodo_id": periodo_id},
            fetchall=True
        )

        df_estatisticas = pd.DataFrame(estatisticas, columns=['empresa_id', 'quantidade', 'valor_total'])

        # 2. Buscar limites existentes com seus percentuais originais
        query_limites = """
        SELECT ID, ID_EMPRESA, PERCENTUAL_FINAL
        FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
        WHERE ID_EDITAL = :edital_id
        AND ID_PERIODO = :periodo_id
        AND DELETED_AT IS NULL
        """

        limites_existentes = executar_query(
            query_limites,
            {"edital_id": edital_id, "periodo_id": periodo_id},
            fetchall=True
        )

        df_limites = pd.DataFrame(limites_existentes,
                                  columns=['id', 'empresa_id', 'percentual']) if limites_existentes else None

        # 3. Mesclar estatísticas com limites existentes
        for _, row in df_estatisticas.iterrows():
            empresa_id = row['empresa_id']
            quantidade = row['quantidade']
            valor_total = row['valor_total'] if row['valor_total'] is not None else 0

            # Verificar se existe limite para esta empresa
            limite_existente = None
            if df_limites is not None:
                limite_rows = df_limites[df_limites['empresa_id'] == empresa_id]
                if not limite_rows.empty:
                    limite_existente = limite_rows.iloc[0]

            if limite_existente is not None:
                # Atualizar limite existente mantendo o percentual original
                query_update = """
                UPDATE [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                SET 
                    QTDE_MAXIMA = :quantidade,
                    VALOR_MAXIMO = :valor_total,
                    VR_ARRECADACAO = :valor_total,
                    DT_APURACAO = GETDATE(),
                    UPDATED_AT = GETDATE()
                    -- PERCENTUAL_FINAL não é alterado
                WHERE ID = :id
                """

                executar_query(
                    query_update,
                    {
                        "id": limite_existente['id'],
                        "quantidade": quantidade,
                        "valor_total": valor_total
                    }
                )
            else:
                # Inserir novo limite
                query_insert = """
                INSERT INTO [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                (
                    ID_EDITAL, ID_PERIODO, ID_EMPRESA,
                    COD_CRITERIO_SELECAO, QTDE_MAXIMA, VALOR_MAXIMO,
                    PERCENTUAL_FINAL, VR_ARRECADACAO, DT_APURACAO, CREATED_AT
                )
                VALUES (
                    :edital_id, :periodo_id, :empresa_id,
                    4, :quantidade, :valor_total,
                    :percentual, :valor_total, GETDATE(), GETDATE()
                )
                """

                # Calcular percentual baseado na proporção de contratos
                total_qtd = df_estatisticas['quantidade'].sum()
                percentual = (quantidade / total_qtd * 100) if total_qtd > 0 else 0

                executar_query(
                    query_insert,
                    {
                        "edital_id": edital_id,
                        "periodo_id": periodo_id,
                        "empresa_id": empresa_id,
                        "quantidade": quantidade,
                        "valor_total": valor_total,
                        "percentual": percentual
                    }
                )

        db.session.commit()
        elapsed_time = time.time() - start_time
        logger.info(f"Atualização dos limites concluída em {elapsed_time:.2f}s")

        # Exibir resumo dos limites atualizados
        query_resumo = """
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
        """

        limites = executar_query(
            query_resumo,
            {"edital_id": edital_id, "periodo_id": periodo_id},
            fetchall=True
        )

        logger.info("\nResumo dos limites de distribuição:")
        logger.info("Empresa | Arrecadação | Qtd Max | Valor Max | Percentual Final")
        logger.info("--------------------------------------------------------------")

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

            logger.info(
                f"{empresa_id} | R$ {float(arrecadacao):.2f} | {qtd_max} | R$ {float(valor_max):.2f} | {percentual:.2f}%")

        logger.info("--------------------------------------------------------------")
        logger.info(f"TOTAL | - | {total_contratos} | - | {total_percentual:.2f}%")

    except Exception as e:
        db.session.rollback()
        logger.error(f"Erro ao atualizar limites de distribuição: {str(e)}")
        logger.error(traceback.format_exc())


def processar_distribuicao_completa(edital_id, periodo_id):
    """
    Executa todo o processo de distribuição em ordem.

    Args:
        edital_id (int): ID do edital
        periodo_id (int): ID do período

    Returns:
        dict: Estatísticas do processo de distribuição
    """
    start_time = time.time()
    logger.info(f"Iniciando processo completo de distribuição - Edital: {edital_id}, Período: {periodo_id}")

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
        # 0. Selecionar contratos distribuíveis (Passo inicial)
        resultados['contratos_distribuiveis'] = selecionar_contratos_distribuiveis()

        if resultados['contratos_distribuiveis'] == 0:
            logger.warning("Nenhum contrato disponível para distribuição.")
            return resultados

        # 1.1.1. Contratos com acordo vigente de empresa que permanece
        resultados['acordos_empresas_permanece'] = distribuir_acordos_vigentes_empresas_permanece(edital_id, periodo_id)
        logger.info(f"Passo 1.1.1 concluído: {resultados['acordos_empresas_permanece']} contratos distribuídos")

        # 1.1.2. Contratos com acordo vigente com empresa descredenciada
        resultados['acordos_empresas_descredenciadas'] = distribuir_acordos_vigentes_empresas_descredenciadas(edital_id,
                                                                                                              periodo_id)
        logger.info(f"Passo 1.1.2 concluído: {resultados['acordos_empresas_descredenciadas']} contratos distribuídos")

        # 1.1.3. Contratos com acordo vigente – regra do arrasto
        resultados['regra_arrasto_acordos'] = aplicar_regra_arrasto_acordos(edital_id, periodo_id)
        logger.info(f"Passo 1.1.3 concluído: {resultados['regra_arrasto_acordos']} contratos distribuídos")

        # 1.1.4. Demais contratos sem acordo – regra do arrasto
        resultados['regra_arrasto_sem_acordo'] = aplicar_regra_arrasto_sem_acordo(edital_id, periodo_id)
        logger.info(f"Passo 1.1.4 concluído: {resultados['regra_arrasto_sem_acordo']} contratos distribuídos")

        # 1.1.5. Demais contratos sem acordo
        resultados['demais_contratos'] = distribuir_demais_contratos(edital_id, periodo_id)
        logger.info(f"Passo 1.1.5 concluído: {resultados['demais_contratos']} contratos distribuídos")

        # Calcular total
        resultados['total_distribuido'] = (
                resultados['acordos_empresas_permanece'] +
                resultados['acordos_empresas_descredenciadas'] +
                resultados['regra_arrasto_acordos'] +
                resultados['regra_arrasto_sem_acordo'] +
                resultados['demais_contratos']
        )

        # Verificar contratos restantes não distribuídos
        contratos_restantes = executar_query("SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]", fetchall=True)[0][
            0]

        resultados['contratos_restantes'] = contratos_restantes
        resultados['total_com_restantes'] = resultados['total_distribuido'] + contratos_restantes

        # Atualizar limites de distribuição (MANTENDO os percentuais originais)
        atualizar_limites_distribuicao(edital_id, periodo_id)

        # Calcular tempo total
        elapsed_time = time.time() - start_time
        logger.info(f"Processo completo de distribuição concluído em {elapsed_time:.2f}s")
        logger.info(f"Total distribuído: {resultados['total_distribuido']} contratos")

        return resultados

    except Exception as e:
        logger.error(f"Erro no processo de distribuição: {str(e)}")
        logger.error(traceback.format_exc())
        return resultados