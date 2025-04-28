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


def distribuir_acordos_vigentes_empresas_permanece(edital_id, periodo_id):
    """
    Distribui contratos com acordo vigente para empresas que permanecem no período avaliativo.
    """
    try:
        logging.info(f"INICIANDO distribuir_acordos_vigentes_empresas_permanece")
        logging.info(f"Parâmetros recebidos: edital_id={edital_id}, periodo_id={periodo_id}")

        with db.engine.connect() as connection:
            try:
                # Verificar se o edital_id existe
                verify_edital_sql = text("""
                SELECT ID FROM DEV.DCA_TB008_EDITAIS 
                WHERE ID = :edital_id AND DELETED_AT IS NULL
                """).bindparams(edital_id=edital_id)

                edital_exists = connection.execute(verify_edital_sql).scalar()
                logging.info(f"Verificação do edital ID={edital_id}: {'EXISTE' if edital_exists else 'NÃO EXISTE'}")

                # CORREÇÃO: Verificando pela coluna ID_PERIODO em vez de ID
                verify_periodo_sql = text("""
                SELECT ID FROM DEV.DCA_TB001_PERIODO_AVALIACAO 
                WHERE ID_PERIODO = :periodo_id 
                AND ID_EDITAL = :edital_id
                AND DELETED_AT IS NULL
                """).bindparams(periodo_id=periodo_id, edital_id=edital_id)

                periodo_exists = connection.execute(verify_periodo_sql).scalar()
                logging.info(
                    f"Verificação do período ID_PERIODO={periodo_id}: {'EXISTE' if periodo_exists else 'NÃO EXISTE'}")

                if not edital_exists or not periodo_exists:
                    logging.error(
                        f"Edital ID={edital_id} ou Período ID_PERIODO={periodo_id} não encontrado no banco de dados")
                    return 0

                # DEBUG 2: Listar IDs das empresas para confirmar
                list_empresas_sql = text("""
                SELECT ID_EMPRESA FROM DEV.DCA_TB002_EMPRESAS_PARTICIPANTES
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND DS_CONDICAO = 'PERMANECE'
                AND DELETED_AT IS NULL
                """).bindparams(edital_id=edital_id, periodo_id=periodo_id)

                empresas_result = connection.execute(list_empresas_sql).fetchall()
                empresas_ids = [row[0] for row in empresas_result]
                logging.info(f"DEBUG 2: IDs das empresas que permanecem: {empresas_ids}")

                # DEBUG 3: Verificar acordos vigentes
                count_acordos_sql = text("""
                SELECT COUNT(*) FROM BDG.COM_TB009_ACORDOS_LIQUIDADOS_VIGENTES
                WHERE fkEstadoAcordo = 1
                """)

                acordos_count = connection.execute(count_acordos_sql).scalar() or 0
                logging.info(f"DEBUG 3: Total de acordos vigentes: {acordos_count}")

                if acordos_count == 0:
                    logging.warning("Não existem acordos vigentes (fkEstadoAcordo = 1) no banco de dados")
                    return 0

                # DEBUG 4: Verificar JOIN entre empresas e empresa_cobranca_atual
                count_empresas_cobranca_sql = text("""
                SELECT COUNT(*) 
                FROM DEV.DCA_TB002_EMPRESAS_PARTICIPANTES AS EMP
                INNER JOIN BDG.COM_TB011_EMPRESA_COBRANCA_ATUAL AS ECA
                    ON EMP.ID_EMPRESA = ECA.COD_EMPRESA_COBRANCA
                WHERE EMP.ID_EDITAL = :edital_id
                AND EMP.ID_PERIODO = :periodo_id
                AND EMP.DS_CONDICAO = 'PERMANECE'
                AND EMP.DELETED_AT IS NULL
                """).bindparams(edital_id=edital_id, periodo_id=periodo_id)

                empresas_cobranca_count = connection.execute(count_empresas_cobranca_sql).scalar() or 0
                logging.info(f"DEBUG 4: JOIN Empresas + Empresa_Cobranca: {empresas_cobranca_count}")

                if empresas_cobranca_count == 0:
                    logging.warning(
                        "Não foi encontrada correspondência entre empresas participantes e tabela COM_TB011_EMPRESA_COBRANCA_ATUAL")
                    # Vamos verificar uma amostra de empresas para debug
                    for empresa_id in empresas_ids[:3]:  # Primeiras 3 empresas
                        check_empresa_sql = text("""
                        SELECT COUNT(*) FROM BDG.COM_TB011_EMPRESA_COBRANCA_ATUAL
                        WHERE COD_EMPRESA_COBRANCA = :empresa_id
                        """).bindparams(empresa_id=empresa_id)
                        count = connection.execute(check_empresa_sql).scalar() or 0
                        logging.info(
                            f"DEBUG 4.1: Empresa ID={empresa_id} tem {count} registros em COM_TB011_EMPRESA_COBRANCA_ATUAL")
                    return 0

                # DEBUG 5: Verificar JOIN entre empresas, cobranca_atual e acordos_vigentes
                count_empresas_acordos_sql = text("""
                SELECT COUNT(*) 
                FROM DEV.DCA_TB002_EMPRESAS_PARTICIPANTES AS EMP
                INNER JOIN BDG.COM_TB011_EMPRESA_COBRANCA_ATUAL AS ECA
                    ON EMP.ID_EMPRESA = ECA.COD_EMPRESA_COBRANCA
                INNER JOIN BDG.COM_TB009_ACORDOS_LIQUIDADOS_VIGENTES AS ALV
                    ON ECA.fkContratoSISCTR = ALV.fkContratoSISCTR
                WHERE EMP.ID_EDITAL = :edital_id
                AND EMP.ID_PERIODO = :periodo_id
                AND EMP.DS_CONDICAO = 'PERMANECE'
                AND EMP.DELETED_AT IS NULL
                AND ALV.fkEstadoAcordo = 1
                """).bindparams(edital_id=edital_id, periodo_id=periodo_id)

                empresas_acordos_count = connection.execute(count_empresas_acordos_sql).scalar() or 0
                logging.info(f"DEBUG 5: JOIN Empresas + Cobranca + Acordos: {empresas_acordos_count}")

                if empresas_acordos_count == 0:
                    logging.warning("Não foi encontrada correspondência após incluir a tabela de acordos vigentes")
                    return 0

                # DEBUG 6: Verificar JOIN completo incluindo tabela de distribuíveis
                count_completo_sql = text("""
                SELECT COUNT(*) 
                FROM DEV.DCA_TB002_EMPRESAS_PARTICIPANTES AS EMP
                INNER JOIN BDG.COM_TB011_EMPRESA_COBRANCA_ATUAL AS ECA
                    ON EMP.ID_EMPRESA = ECA.COD_EMPRESA_COBRANCA
                INNER JOIN BDG.COM_TB009_ACORDOS_LIQUIDADOS_VIGENTES AS ALV
                    ON ECA.fkContratoSISCTR = ALV.fkContratoSISCTR
                INNER JOIN DEV.DCA_TB006_DISTRIBUIVEIS AS DIS
                    ON ECA.fkContratoSISCTR = DIS.FkContratoSISCTR
                WHERE EMP.ID_EDITAL = :edital_id
                AND EMP.ID_PERIODO = :periodo_id
                AND EMP.DS_CONDICAO = 'PERMANECE'
                AND EMP.DELETED_AT IS NULL
                AND ALV.fkEstadoAcordo = 1
                AND DIS.DELETED_AT IS NULL
                """).bindparams(edital_id=edital_id, periodo_id=periodo_id)

                join_completo_count = connection.execute(count_completo_sql).scalar() or 0
                logging.info(f"DEBUG 6: JOIN Completo (incluindo distribuíveis): {join_completo_count}")

                if join_completo_count == 0:
                    logging.warning("Não foi encontrada correspondência após incluir a tabela de distribuíveis")
                    # Verificar contagem na tabela de distribuíveis para confirmar que há dados
                    count_dist_sql = text("SELECT COUNT(*) FROM DEV.DCA_TB006_DISTRIBUIVEIS WHERE DELETED_AT IS NULL")
                    dist_count = connection.execute(count_dist_sql).scalar() or 0
                    logging.info(f"DEBUG 6.1: Total de registros em DCA_TB006_DISTRIBUIVEIS: {dist_count}")
                    return 0

                # Obtém a data de referência atual
                data_referencia_sql = text("SELECT CONVERT(DATE, GETDATE())")
                dt_referencia = connection.execute(data_referencia_sql).scalar()
                logging.info(f"Data de referência: {dt_referencia}")

                # Construir a SQL para distribuição
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
                    ECA.fkContratoSISCTR,
                    1, -- Código 1: Contrato com acordo para assessoria que permanece
                    EMP.ID_EMPRESA,
                    DIS.NR_CPF_CNPJ,
                    DIS.VR_SD_DEVEDOR,
                    GETDATE(),
                    NULL,
                    NULL
                FROM DEV.DCA_TB002_EMPRESAS_PARTICIPANTES AS EMP
                INNER JOIN BDG.COM_TB011_EMPRESA_COBRANCA_ATUAL AS ECA
                    ON EMP.ID_EMPRESA = ECA.COD_EMPRESA_COBRANCA
                INNER JOIN BDG.COM_TB009_ACORDOS_LIQUIDADOS_VIGENTES AS ALV
                    ON ECA.fkContratoSISCTR = ALV.fkContratoSISCTR
                INNER JOIN DEV.DCA_TB006_DISTRIBUIVEIS AS DIS
                    ON ECA.fkContratoSISCTR = DIS.FkContratoSISCTR
                WHERE EMP.ID_EDITAL = :edital_id
                AND EMP.ID_PERIODO = :periodo_id
                AND EMP.DS_CONDICAO = 'PERMANECE'
                AND EMP.DELETED_AT IS NULL
                AND ALV.fkEstadoAcordo = 1
                AND DIS.DELETED_AT IS NULL
                """).bindparams(
                    dt_referencia=dt_referencia,
                    edital_id=edital_id,
                    periodo_id=periodo_id
                )

                # DEBUG 7: Imprimir SQL a ser executada
                sql_to_execute = str(distribuir_sql.compile(
                    compile_kwargs={"literal_binds": True}))
                logging.info(f"DEBUG 7: SQL a ser executada: {sql_to_execute}")

                # Executar a inserção
                logging.info("Executando SQL para distribuir contratos...")
                result = connection.execute(distribuir_sql)
                contratos_distribuidos = result.rowcount
                logging.info(f"Resultado da inserção: {contratos_distribuidos} registros inseridos")

                if contratos_distribuidos > 0:
                    # Marcar como excluídos na tabela de distribuíveis
                    marcar_como_excluido_sql = text("""
                    UPDATE DEV.DCA_TB006_DISTRIBUIVEIS
                    SET DELETED_AT = GETDATE()
                    WHERE FkContratoSISCTR IN (
                        SELECT FkContratoSISCTR 
                        FROM DEV.DCA_TB005_DISTRIBUICAO
                        WHERE ID_EDITAL = :edital_id
                        AND ID_PERIODO = :periodo_id
                        AND COD_CRITERIO_SELECAO = 1
                    )
                    AND DELETED_AT IS NULL
                    """).bindparams(edital_id=edital_id, periodo_id=periodo_id)

                    # DEBUG 8: Mostrar SQL de atualização
                    update_sql_str = str(marcar_como_excluido_sql.compile(
                        compile_kwargs={"literal_binds": True}))
                    logging.info(f"DEBUG 8: SQL para marcar como excluído: {update_sql_str}")

                    # Executar a atualização
                    update_result = connection.execute(marcar_como_excluido_sql)
                    atualizados = update_result.rowcount
                    logging.info(f"Marcados {atualizados} contratos como excluídos na tabela de distribuíveis")

                logging.info(f"CONCLUÍDO: Contratos com acordo vigente distribuídos: {contratos_distribuidos}")
                return contratos_distribuidos

            except Exception as e:
                logging.error(f"Erro durante a distribuição de contratos com acordo vigente: {str(e)}")
                import traceback
                logging.error(traceback.format_exc())
                raise

    except Exception as e:
        logging.error(f"Erro ao distribuir contratos com acordo vigente: {str(e)}")
        return 0

def distribuir_acordos_vigentes_empresas_descredenciadas(edital_id, periodo_id):
    """
    Função ainda não implementada. Retorna 0 por enquanto.
    """
    return 0

def aplicar_regra_arrasto_acordos(edital_id, periodo_id):
    """
    Função ainda não implementada. Retorna 0 por enquanto.
    """
    return 0


def aplicar_regra_arrasto_sem_acordo(edital_id, periodo_id):
    """
    Função ainda não implementada. Retorna 0 por enquanto.
    """
    return 0


def distribuir_demais_contratos(edital_id, periodo_id):
    """
    Função ainda não implementada. Retorna 0 por enquanto.
    """
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



