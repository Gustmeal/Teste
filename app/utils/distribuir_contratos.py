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
    Distribui contratos com acordos vigentes para empresas que permanecem
    e aplica a regra de arrasto para contratos com mesmo CPF.

    Args:
        edital_id: ID do edital a ser processado
        periodo_id: ID do período a ser processado

    Returns:
        int: Total de contratos distribuídos
    """
    contratos_acordo = 0
    contratos_arrasto = 0

    try:
        print(
            f"Iniciando distribuição de acordos vigentes para empresas que permanecem - Edital: {edital_id}, Período: {periodo_id}")

        # 1. Limpar a tabela de distribuição (se necessário - verifique se seu código já faz isso em outro lugar)
        db.session.execute(text("DELETE FROM [DEV].[DCA_TB005_DISTRIBUICAO]"))
        print("Tabela DCA_TB005_DISTRIBUICAO limpa com sucesso")

        # 2. Inserir contratos com acordos vigentes para empresas que permanecem
        resultado_acordos = db.session.execute(
            text("""
                INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
                ([DT_REFERENCIA], [ID_EDITAL], [ID_PERIODO], [fkContratoSISCTR], 
                [COD_EMPRESA_COBRANCA], [COD_CRITERIO_SELECAO], [NR_CPF_CNPJ], 
                [VR_SD_DEVEDOR], [CREATED_AT])

                SELECT 
                    GETDATE() AS [DT_REFERENCIA],
                    EMP.ID_EDITAL,
                    EMP.ID_PERIODO,
                    ECA.fkContratoSISCTR,
                    EMP.ID_EMPRESA AS COD_EMPRESA_COBRANCA,
                    1 AS COD_CRITERIO_SELECAO, -- Contrato com acordo para assessoria que permanece
                    DIS.[NR_CPF_CNPJ],
                    DIS.[VR_SD_DEVEDOR],
                    GETDATE() AS [CREATED_AT]
                FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES] AS EMP
                    INNER JOIN [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                        ON EMP.ID_EMPRESA = ECA.COD_EMPRESA_COBRANCA
                    INNER JOIN [BDG].[COM_TB009_ACORDOS_LIQUIDADOS_VIGENTES] AS ALV
                        ON ECA.fkContratoSISCTR = ALV.fkContratoSISCTR
                    INNER JOIN [DEV].[DCA_TB006_DISTRIBUIVEIS] AS DIS
                        ON ECA.fkContratoSISCTR = DIS.[FkContratoSISCTR]
                WHERE EMP.ID_EDITAL = :edital_id
                    AND EMP.ID_PERIODO = :periodo_id
                    AND EMP.DS_CONDICAO = 'PERMANECE'
                    AND ALV.fkEstadoAcordo = 1
            """),
            {"edital_id": edital_id, "periodo_id": periodo_id}
        )
        contratos_acordo = resultado_acordos.rowcount
        print(f"{contratos_acordo} contratos com acordo distribuídos")

        # 3. Aplicar regra de arrasto - distribuir contratos do mesmo CPF para a mesma empresa
        resultado_arrasto = db.session.execute(
            text("""
                INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
                ([DT_REFERENCIA], [ID_EDITAL], [ID_PERIODO], [fkContratoSISCTR], 
                [COD_EMPRESA_COBRANCA], [COD_CRITERIO_SELECAO], [NR_CPF_CNPJ], 
                [VR_SD_DEVEDOR], [CREATED_AT])

                SELECT 
                    GETDATE() AS [DT_REFERENCIA],
                    DIST.ID_EDITAL,
                    DIST.ID_PERIODO,
                    DIS.FkContratoSISCTR,
                    DIST.COD_EMPRESA_COBRANCA,
                    6 AS COD_CRITERIO_SELECAO, -- Regra de Arrasto
                    DIS.[NR_CPF_CNPJ],
                    DIS.[VR_SD_DEVEDOR],
                    GETDATE() AS [CREATED_AT]
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] DIS
                INNER JOIN [DEV].[DCA_TB005_DISTRIBUICAO] DIST 
                    ON DIS.[NR_CPF_CNPJ] = DIST.[NR_CPF_CNPJ]
                    AND DIS.[FkContratoSISCTR] <> DIST.[fkContratoSISCTR]
                WHERE DIST.[COD_CRITERIO_SELECAO] = 1
                    AND NOT EXISTS (
                        SELECT 1 
                        FROM [DEV].[DCA_TB005_DISTRIBUICAO] 
                        WHERE fkContratoSISCTR = DIS.[FkContratoSISCTR]
                    )
            """)
        )
        contratos_arrasto = resultado_arrasto.rowcount
        print(f"{contratos_arrasto} contratos distribuídos por regra de arrasto")

        # 4. Excluir os contratos inseridos da tabela de distribuíveis
        resultado_delete = db.session.execute(
            text("""
                DELETE DIS
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] AS DIS
                INNER JOIN [DEV].[DCA_TB005_DISTRIBUICAO] AS DIST 
                    ON DIS.[FkContratoSISCTR] = DIST.[fkContratoSISCTR]
            """)
        )
        print(f"{resultado_delete.rowcount} contratos removidos da tabela de distribuíveis")

        db.session.commit()
        print("Processo de distribuição de acordos vigentes concluído com sucesso")

    except Exception as e:
        db.session.rollback()
        print(f"Erro ao distribuir contratos: {str(e)}")

    # Retorna um valor inteiro (soma dos contratos distribuídos)
    return contratos_acordo + contratos_arrasto

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



