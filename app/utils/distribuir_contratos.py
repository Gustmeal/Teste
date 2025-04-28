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

    Implementa o item 1.1.1 do documento:
    - Baseado na DCA_TB002_EMPRESAS_PARTICIPANTES (DS_CONDICAO = 'PERMANECE')
    - Estado do Acordo = 1
    - Critério de seleção 1
    - Remove da tabela de distribuíveis após distribuição

    Args:
        edital_id (int): ID do edital
        periodo_id (int): ID do período

    Returns:
        int: Quantidade de contratos distribuídos
    """
    try:
        with db.engine.connect() as connection:
            # Obter a data de referência atual
            dt_referencia = connection.execute(text("SELECT CONVERT(DATE, GETDATE())")).scalar()

            # Inserir contratos de empresas que permanecem
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
                    EMP.ID_EDITAL,
                    EMP.ID_PERIODO,
                    ECA.fkContratoSISCTR,
                    1, -- Código 1: Contrato com acordo para assessoria que permanece
                    EMP.ID_EMPRESA,
                    DIS.NR_CPF_CNPJ,
                    DIS.VR_SD_DEVEDOR,
                    GETDATE(),
                    NULL,
                    NULL
                FROM DEV.DCA_TB002_EMPRESAS_PARTICIPANTES EMP
                INNER JOIN BDG.COM_TB011_EMPRESA_COBRANCA_ATUAL ECA
                    ON EMP.ID_EMPRESA = ECA.COD_EMPRESA_COBRANCA
                INNER JOIN BDG.COM_TB009_ACORDOS_LIQUIDADOS_VIGENTES ALV
                    ON ECA.fkContratoSISCTR = ALV.fkContratoSISCTR
                INNER JOIN DEV.DCA_TB006_DISTRIBUIVEIS DIS
                    ON ECA.fkContratoSISCTR = DIS.FkContratoSISCTR
                WHERE
                    EMP.ID_EDITAL = :edital_id
                    AND EMP.ID_PERIODO = :periodo_id
                    AND EMP.DS_CONDICAO = 'PERMANECE'
                    AND ALV.fkEstadoAcordo = 1
                    AND EMP.DELETED_AT IS NULL
            """).bindparams(
                dt_referencia=dt_referencia,
                edital_id=edital_id,
                periodo_id=periodo_id
            )

            result = connection.execute(distribuir_sql)
            contratos_distribuidos = result.rowcount or 0

            # Se distribuiu, remover da tabela de distribuíveis
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
                """).bindparams(
                    edital_id=edital_id,
                    periodo_id=periodo_id
                )
                connection.execute(remover_sql)

            logging.info(f"Contratos distribuídos para empresas que permanecem: {contratos_distribuidos}")
            return contratos_distribuidos

    except Exception as e:
        logging.error(f"Erro em distribuir_acordos_vigentes_empresas_permanece: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
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



