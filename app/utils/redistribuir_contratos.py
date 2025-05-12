# app/utils/redistribuir_contratos.py
from app import db
from sqlalchemy import text
import logging
from datetime import datetime


def selecionar_contratos_para_redistribuicao(empresa_id):
    """
    Seleciona os contratos da empresa que será redistribuída.
    Preserva distribuições anteriores, removendo apenas os dados relevantes.
    """
    # Configuração básica de logging
    import logging
    import sys

    # Configurar o logging para exibir no console e em um arquivo
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("redistribuicao.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

    print(f"Função selecionar_contratos_para_redistribuicao chamada para empresa_id={empresa_id}")
    logging.info(f"Iniciando seleção de contratos para redistribuição da empresa ID: {empresa_id}")

    try:
        # Limpar tabelas temporárias somente para esta operação
        with db.engine.connect() as connection:
            print("Limpando tabelas temporárias para esta operação específica...")
            logging.info("Limpando tabelas temporárias...")

            # Limpar apenas a tabela temporária (isso é seguro, pois não afeta dados históricos)
            truncate_sql = text("TRUNCATE TABLE [DEV].[DCA_TB006_DISTRIBUIVEIS]")
            connection.execute(truncate_sql)
            print("Tabela DCA_TB006_DISTRIBUIVEIS limpa para nova operação")

            truncate_arrastaveis_sql = text("TRUNCATE TABLE [DEV].[DCA_TB007_ARRASTAVEIS]")
            connection.execute(truncate_arrastaveis_sql)
            print("Tabela DCA_TB007_ARRASTAVEIS limpa para nova operação")

            # DIAGNÓSTICO: Verificar cada critério separadamente
            print("\n----- DIAGNÓSTICO DE CRITÉRIOS DE SELEÇÃO -----")

            # 1. Verificar contratos na empresa atual
            check_empresa_sql = text("""
                SELECT COUNT(*) 
                FROM [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                WHERE ECA.[COD_EMPRESA_COBRANCA] = :empresa_id
            """)
            count_empresa = connection.execute(check_empresa_sql, {"empresa_id": empresa_id}).scalar() or 0
            print(f"1. Contratos para empresa {empresa_id}: {count_empresa}")

            # 2. Verificar quantos têm fkSituacaoCredito = 1
            check_situacao_sql = text("""
                SELECT COUNT(*) 
                FROM [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                INNER JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] AS SIT
                    ON ECA.fkContratoSISCTR = SIT.fkContratoSISCTR
                WHERE ECA.[COD_EMPRESA_COBRANCA] = :empresa_id
                AND SIT.[fkSituacaoCredito] = 1
            """)
            count_situacao = connection.execute(check_situacao_sql, {"empresa_id": empresa_id}).scalar() or 0
            print(f"2. Contratos ativos (fkSituacaoCredito=1): {count_situacao}")

            # 3. Verificar quantos têm VR_SD_DEVEDOR > 0
            check_saldo_sql = text("""
                SELECT COUNT(*) 
                FROM [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                INNER JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] AS SIT
                    ON ECA.fkContratoSISCTR = SIT.fkContratoSISCTR
                WHERE ECA.[COD_EMPRESA_COBRANCA] = :empresa_id
                AND SIT.VR_SD_DEVEDOR > 0
            """)
            count_saldo = connection.execute(check_saldo_sql, {"empresa_id": empresa_id}).scalar() or 0
            print(f"3. Contratos com saldo devedor > 0: {count_saldo}")

            # 4. Verificar quantos estão com suspensão judicial
            check_suspensao_sql = text("""
                SELECT COUNT(*) 
                FROM [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                LEFT JOIN [BDG].[COM_TB013_SUSPENSO_DECISAO_JUDICIAL] AS SDJ
                    ON ECA.fkContratoSISCTR = SDJ.fkContratoSISCTR
                WHERE ECA.[COD_EMPRESA_COBRANCA] = :empresa_id
                AND SDJ.fkContratoSISCTR IS NOT NULL
            """)
            count_suspensao = connection.execute(check_suspensao_sql, {"empresa_id": empresa_id}).scalar() or 0
            print(f"4. Contratos com suspensão judicial: {count_suspensao}")

            # 5. Verificar quantos atendem todos os critérios
            check_final_sql = text("""
                SELECT COUNT(*) 
                FROM [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                INNER JOIN [BDG].[COM_TB001_CONTRATO] AS CON
                    ON ECA.fkContratoSISCTR = CON.fkContratoSISCTR
                INNER JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] AS SIT
                    ON ECA.fkContratoSISCTR = SIT.fkContratoSISCTR
                LEFT JOIN [BDG].[COM_TB013_SUSPENSO_DECISAO_JUDICIAL] AS SDJ
                    ON ECA.fkContratoSISCTR = SDJ.fkContratoSISCTR
                WHERE ECA.[COD_EMPRESA_COBRANCA] = :empresa_id
                AND SIT.[fkSituacaoCredito] = 1
                AND SDJ.fkContratoSISCTR IS NULL
            """)
            count_final = connection.execute(check_final_sql, {"empresa_id": empresa_id}).scalar() or 0
            print(f"5. Contratos que atendem todos os critérios: {count_final}")

            # 6. Adicionar critério alternativo - buscar na tabela de distribuição
            check_distribuicao_sql = text("""
                SELECT COUNT(*) 
                FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                WHERE [COD_EMPRESA_COBRANCA] = :empresa_id
                AND DELETED_AT IS NULL
            """)
            count_distribuicao = connection.execute(check_distribuicao_sql, {"empresa_id": empresa_id}).scalar() or 0
            print(f"6. Contratos na tabela de distribuição: {count_distribuicao}")

            print("-------------------------------------------\n")

            # Verificar se devemos usar fonte alternativa para seleção
            if count_final == 0 and count_distribuicao > 0:
                print("USANDO TABELA DE DISTRIBUIÇÃO COMO FONTE ALTERNATIVA!")
                logging.info(
                    "Usando tabela de distribuição como fonte alternativa devido a 0 contratos na fonte primária")

                # Inserir da tabela de distribuição para distribuíveis
                insert_alt_sql = text("""
                INSERT INTO [DEV].[DCA_TB006_DISTRIBUIVEIS]
                SELECT 
                    D.fkContratoSISCTR,
                    D.NR_CPF_CNPJ,
                    D.VR_SD_DEVEDOR,
                    GETDATE() AS CREATED_AT,
                    NULL AS UPDATED_AT,
                    NULL AS DELETED_AT
                FROM [DEV].[DCA_TB005_DISTRIBUICAO] D
                WHERE D.[COD_EMPRESA_COBRANCA] = :empresa_id
                AND D.DELETED_AT IS NULL
                """)

                result = connection.execute(insert_alt_sql, {"empresa_id": empresa_id})
                contratos_inseridos = result.rowcount
                print(f"Inseridos {contratos_inseridos} contratos da tabela de distribuição")
                logging.info(f"Inseridos {contratos_inseridos} contratos da tabela de distribuição")
            else:
                # CORREÇÃO: Consulta ajustada para selecionar contratos ativos com as condições corretas
                insert_sql = text("""
                INSERT INTO [DEV].[DCA_TB006_DISTRIBUIVEIS]
                SELECT
                    ECA.fkContratoSISCTR,
                    CON.NR_CPF_CNPJ,
                    SIT.VR_SD_DEVEDOR,
                    GETDATE() AS CREATED_AT,
                    NULL AS UPDATED_AT,
                    NULL AS DELETED_AT
                FROM 
                    [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                    INNER JOIN [BDG].[COM_TB001_CONTRATO] AS CON
                        ON ECA.fkContratoSISCTR = CON.fkContratoSISCTR
                    INNER JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] AS SIT
                        ON ECA.fkContratoSISCTR = SIT.fkContratoSISCTR
                    LEFT JOIN [BDG].[COM_TB013_SUSPENSO_DECISAO_JUDICIAL] AS SDJ
                        ON ECA.fkContratoSISCTR = SDJ.fkContratoSISCTR
                WHERE
                    ECA.[COD_EMPRESA_COBRANCA] = :empresa_id
                    AND SIT.[fkSituacaoCredito] = 1  -- Contratos ativos
                    AND SDJ.fkContratoSISCTR IS NULL -- Sem suspensão judicial
                    AND SIT.VR_SD_DEVEDOR > 0        -- Com saldo devedor
                    AND ECA.fkContratoSISCTR IS NOT NULL -- Garante que contratos são válidos
                """)

                result = connection.execute(insert_sql, {"empresa_id": empresa_id})
                contratos_inseridos = result.rowcount
                print(f"Inseridos {contratos_inseridos} contratos da consulta original")
                logging.info(f"Inseridos {contratos_inseridos} contratos da consulta original")

                # Se ainda não encontrou contratos, tente uma abordagem mais flexível
                if contratos_inseridos == 0:
                    print("TENTATIVA ADICIONAL: Consulta flexibilizada para encontrar contratos")
                    insert_flexible_sql = text("""
                    INSERT INTO [DEV].[DCA_TB006_DISTRIBUIVEIS]
                    SELECT
                        ECA.fkContratoSISCTR,
                        CON.NR_CPF_CNPJ,
                        COALESCE(SIT.VR_SD_DEVEDOR, 0) AS VR_SD_DEVEDOR,
                        GETDATE() AS CREATED_AT,
                        NULL AS UPDATED_AT,
                        NULL AS DELETED_AT
                    FROM 
                        [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                        INNER JOIN [BDG].[COM_TB001_CONTRATO] AS CON
                            ON ECA.fkContratoSISCTR = CON.fkContratoSISCTR
                        LEFT JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] AS SIT
                            ON ECA.fkContratoSISCTR = SIT.fkContratoSISCTR
                    WHERE
                        ECA.[COD_EMPRESA_COBRANCA] = :empresa_id
                        AND ECA.fkContratoSISCTR NOT IN (
                            SELECT fkContratoSISCTR 
                            FROM [BDG].[COM_TB013_SUSPENSO_DECISAO_JUDICIAL]
                        )
                    """)

                    result = connection.execute(insert_flexible_sql, {"empresa_id": empresa_id})
                    contratos_inseridos = result.rowcount
                    print(f"Inseridos {contratos_inseridos} contratos da consulta flexibilizada")
                    logging.info(f"Inseridos {contratos_inseridos} contratos da consulta flexibilizada")

            # Contar contratos selecionados no final
            count_sql = text("SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] WHERE DELETED_AT IS NULL")
            result = connection.execute(count_sql)
            num_contratos = result.scalar() or 0

            print(f"Total de contratos selecionados para redistribuição: {num_contratos}")
            logging.info(f"Total de contratos selecionados para redistribuição: {num_contratos}")

            # Se ainda não encontrou contratos, log detalhado
            if num_contratos == 0:
                logging.error("ALERTA: Nenhum contrato encontrado para redistribuição!")
                # Verificar se a empresa existe
                empresa_check = text("""
                SELECT COUNT(*) FROM [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] 
                WHERE [COD_EMPRESA_COBRANCA] = :empresa_id
                """)
                empresa_exists = connection.execute(empresa_check, {"empresa_id": empresa_id}).scalar() or 0
                logging.error(f"Empresa {empresa_id} existe na tabela atual: {'Sim' if empresa_exists > 0 else 'Não'}")

            return num_contratos

    except Exception as e:
        error_msg = f"Erro geral ao selecionar contratos para redistribuição: {str(e)}"
        print(error_msg)
        logging.error(error_msg)

        import traceback
        trace_msg = traceback.format_exc()
        print(f"Traceback: {trace_msg}")
        logging.error(f"Traceback: {trace_msg}")

        return 0


def calcular_percentuais_redistribuicao(edital_id, periodo_id, empresa_id):
    """
    Calcula os percentuais para redistribuição dos contratos.

    Args:
        edital_id: ID do edital
        periodo_id: ID do período
        empresa_id: ID da empresa que está saindo

    Returns:
        tuple: (percentual_redistribuido, total_arrecadacao, empresas_dados)
    """
    # Configuração básica de logging
    import logging
    import sys

    # Configurar o logging para exibir no console e em um arquivo
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("redistribuicao.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

    print(
        f"\nIniciando cálculo de percentuais: edital_id={edital_id}, periodo_id={periodo_id}, empresa_id={empresa_id}")
    logging.info(
        f"Calculando percentuais para redistribuição - Edital: {edital_id}, Período: {periodo_id}, Empresa: {empresa_id}")

    try:
        # 1. Buscar período para obter datas de início e fim
        with db.engine.connect() as connection:
            print("Buscando informações do período...")

            periodo_sql = text("""
                SELECT DT_INICIO, DT_FIM 
                FROM [DEV].[DCA_TB001_PERIODO_AVALIACAO]
                WHERE ID_EDITAL = :edital_id AND ID_PERIODO = :periodo_id
            """)

            periodo_result = connection.execute(periodo_sql, {
                "edital_id": edital_id,
                "periodo_id": periodo_id
            }).fetchone()

            if not periodo_result:
                error_msg = f"Período não encontrado: Edital {edital_id}, Período {periodo_id}"
                print(error_msg)
                logging.error(error_msg)
                return 0, 0, []

            dt_inicio, dt_fim = periodo_result
            print(f"Período encontrado: {dt_inicio} a {dt_fim}")
            logging.info(f"Usando período: {dt_inicio} a {dt_fim}")

            # 2. Calcular percentuais de arrecadação
            print("\n----- CALCULANDO PERCENTUAIS DE ARRECADAÇÃO -----")

            # DEBUG: Verificar empresas disponíveis
            debug_sql = text("""
                SELECT ID_EMPRESA, NO_EMPRESA, DS_CONDICAO
                FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND DELETED_AT IS NULL
                ORDER BY DS_CONDICAO, ID_EMPRESA
            """)

            debug_result = connection.execute(debug_sql, {
                "edital_id": edital_id,
                "periodo_id": periodo_id
            }).fetchall()

            print("\nEmpresas cadastradas no período:")
            for emp in debug_result:
                print(f"  ID: {emp[0]}, Nome: {emp[1]}, Condição: {emp[2]}")

            # Calcular percentuais com base na arrecadação - CORRIGIDO para incluir TODAS as empresas
            arrecadacao_sql = text("""
            WITH TodasEmpresas AS (
                -- Pegar TODAS as empresas do período, independente de terem arrecadação
                SELECT 
                    EP.ID_EMPRESA,
                    EP.NO_EMPRESA,
                    EP.DS_CONDICAO
                FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES] AS EP
                WHERE EP.ID_EDITAL = :edital_id
                  AND EP.ID_PERIODO = :periodo_id
                  AND EP.DELETED_AT IS NULL
            ),
            ArrecadacaoEmpresas AS (
                -- Buscar arrecadação do período anterior (se houver)
                SELECT 
                    REE.CO_EMPRESA_COBRANCA AS ID_EMPRESA,
                    SUM(REE.VR_ARRECADACAO_TOTAL) AS VR_ARRECADACAO
                FROM [BDG].[COM_TB062_REMUNERACAO_ESTIMADA] AS REE
                WHERE REE.DT_ARRECADACAO BETWEEN :dt_inicio AND :dt_fim
                GROUP BY REE.CO_EMPRESA_COBRANCA
            ),
            ResultadoFinal AS (
                SELECT 
                    TE.ID_EMPRESA,
                    TE.DS_CONDICAO,
                    COALESCE(AE.VR_ARRECADACAO, 0) AS VR_ARRECADACAO,
                    CASE 
                        WHEN (SELECT SUM(COALESCE(AE2.VR_ARRECADACAO, 0)) 
                              FROM TodasEmpresas TE2 
                              LEFT JOIN ArrecadacaoEmpresas AE2 ON TE2.ID_EMPRESA = AE2.ID_EMPRESA) = 0
                        THEN 0  -- Se não há arrecadação total, percentual é 0
                        ELSE ROUND((COALESCE(AE.VR_ARRECADACAO, 0) * 100.0 / 
                              NULLIF((SELECT SUM(COALESCE(AE2.VR_ARRECADACAO, 0)) 
                                     FROM TodasEmpresas TE2 
                                     LEFT JOIN ArrecadacaoEmpresas AE2 ON TE2.ID_EMPRESA = AE2.ID_EMPRESA), 0)), 2)
                    END AS PERCENTUAL
                FROM TodasEmpresas TE
                LEFT JOIN ArrecadacaoEmpresas AE ON TE.ID_EMPRESA = AE.ID_EMPRESA
                WHERE TE.DS_CONDICAO IN ('NOVA', 'PERMANECE')  -- Apenas empresas que podem receber
            )
            SELECT * FROM ResultadoFinal
            ORDER BY ID_EMPRESA
            """)

            print("Executando consulta de arrecadação...")
            arrecadacao_results = connection.execute(arrecadacao_sql, {
                "dt_inicio": dt_inicio,
                "dt_fim": dt_fim,
                "edital_id": edital_id,
                "periodo_id": periodo_id
            }).fetchall()

            print(f"Resultados da consulta: {len(arrecadacao_results)} empresas encontradas")

            # Verificar resultados
            if not arrecadacao_results:
                print("ATENÇÃO: Nenhuma empresa NOVA ou PERMANECE encontrada.")
                logging.error("Nenhuma empresa NOVA ou PERMANECE encontrada para redistribuição")
                return 0, 0, []

            # 3. Processar resultados
            print("\n----- PROCESSANDO RESULTADOS -----")
            print(f"Total de empresas encontradas para receber redistribuição: {len(arrecadacao_results)}")

            # Filtrar apenas empresas receptoras (excluir a que está saindo)
            empresas_receptoras = [e for e in arrecadacao_results if e[0] != empresa_id]
            print(f"Empresas receptoras (excluindo a que sai): {len(empresas_receptoras)}")

            # Listar IDs das empresas receptoras para debug
            for i, emp in enumerate(empresas_receptoras):
                print(
                    f"  {i + 1}. Empresa ID: {emp[0]}, Condição: {emp[1]}, Arrecadação: {emp[2]:.2f}, Percentual: {emp[3]:.2f}%")

            # Se não há arrecadação, distribuir igualmente
            total_arrecadacao = sum(float(e[2]) for e in arrecadacao_results)

            if total_arrecadacao == 0:
                print("\nATENÇÃO: Sem dados de arrecadação. Distribuindo igualmente entre empresas.")
                percentual_igual = 100.0 / len(empresas_receptoras) if empresas_receptoras else 0

                empresas_dados = []
                for emp in empresas_receptoras:
                    empresas_dados.append({
                        "id_empresa": emp[0],
                        "vr_arrecadacao": 0.0,
                        "percentual": percentual_igual
                    })

                # Percentual da empresa que sai (assumir proporção igual)
                percentual_empresa_redistribuida = 100.0 / (len(empresas_receptoras) + 1)
            else:
                # Usar percentuais calculados com base na arrecadação
                empresas_dados = []
                percentual_empresa_redistribuida = 0

                for emp in arrecadacao_results:
                    id_empresa, ds_condicao, vr_arrecadacao, percentual = emp

                    vr_arrecadacao = float(vr_arrecadacao) if vr_arrecadacao else 0.0
                    percentual = float(percentual) if percentual else 0.0

                    if id_empresa == empresa_id:
                        percentual_empresa_redistribuida = percentual
                    else:
                        empresas_dados.append({
                            "id_empresa": id_empresa,
                            "vr_arrecadacao": vr_arrecadacao,
                            "percentual": percentual
                        })

            # Se não encontrou percentual da empresa que sai, buscar em outras fontes
            if percentual_empresa_redistribuida == 0:
                print("\nBuscando percentual da empresa que sai em outras fontes...")

                # Tentar buscar o percentual na tabela de limites
                percentual_sql = text("""
                SELECT TOP 1 PERCENTUAL_FINAL 
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE ID_EMPRESA = :empresa_id
                AND ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND DELETED_AT IS NULL
                ORDER BY CREATED_AT DESC
                """)

                percentual_result = connection.execute(percentual_sql, {
                    "empresa_id": empresa_id,
                    "edital_id": edital_id,
                    "periodo_id": periodo_id
                }).fetchone()

                if percentual_result and percentual_result[0]:
                    percentual_empresa_redistribuida = float(percentual_result[0])
                    print(f"Percentual encontrado na tabela de limites: {percentual_empresa_redistribuida:.2f}%")
                else:
                    # Se não encontrou, assumir distribuição igual
                    percentual_empresa_redistribuida = 100.0 / (len(empresas_dados) + 1)
                    print(f"Usando percentual calculado: {percentual_empresa_redistribuida:.2f}%")

            # Resumo final
            print(f"\nTotal de empresas que receberão redistribuição: {len(empresas_dados)}")
            print(f"Total de arrecadação: {total_arrecadacao:.2f}")
            print(f"Percentual a redistribuir: {percentual_empresa_redistribuida:.2f}%")

            return percentual_empresa_redistribuida, total_arrecadacao, empresas_dados

    except Exception as e:
        error_msg = f"Erro ao calcular percentuais para redistribuição: {str(e)}"
        print(error_msg)
        logging.error(error_msg)

        import traceback
        trace_msg = traceback.format_exc()
        print(f"Traceback: {trace_msg}")
        logging.error(f"Traceback: {trace_msg}")

        return 0, 0, []

def redistribuir_percentuais(edital_id, periodo_id, criterio_id, empresa_id, percentual_redistribuido, empresas_dados):
    """
    Redistribui os percentuais da empresa que está saindo entre as empresas remanescentes.
    Calcula os novos percentuais, insere na tabela de limites apenas para o critério específico,
    ajusta para soma 100% e calcula as quantidades e valores máximos para cada empresa.

    Args:
        edital_id: ID do edital
        periodo_id: ID do período
        criterio_id: ID do critério de seleção
        empresa_id: ID da empresa que está saindo
        percentual_redistribuido: Percentual da empresa que está saindo
        empresas_dados: Lista de dicionários com dados das empresas remanescentes

    Returns:
        boolean: True se redistribuição foi bem sucedida, False caso contrário
    """
    # Configuração básica de logging
    import logging
    import sys
    from datetime import datetime

    # Configurar o logging para exibir no console e em um arquivo
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("redistribuicao.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

    print(
        f"\nIniciando redistribuição de percentuais: edital_id={edital_id}, periodo_id={periodo_id}, criterio_id={criterio_id}")
    logging.info(f"Redistribuindo percentuais - Edital: {edital_id}, Período: {periodo_id}, Critério: {criterio_id}")

    try:
        with db.engine.connect() as connection:
            transaction = connection.begin()

            try:
                # 1. Verificar quantidade de empresas remanescentes
                qtde_empresas_remanescentes = len(empresas_dados)

                if qtde_empresas_remanescentes == 0:
                    error_msg = "Não há empresas remanescentes para redistribuição"
                    print(error_msg)
                    logging.error(error_msg)
                    return False

                print(f"Empresas remanescentes: {qtde_empresas_remanescentes}")
                logging.info(f"Empresas remanescentes: {qtde_empresas_remanescentes}")

                # 2. Calcular percentual unitário a ser redistribuído
                percentual_unitario = percentual_redistribuido / qtde_empresas_remanescentes
                print(f"Percentual a redistribuir: {percentual_redistribuido:.2f}%")
                print(f"Percentual unitário por empresa: {percentual_unitario:.2f}%")
                logging.info(f"Percentual unitário por empresa: {percentual_unitario:.2f}%")

                # 3. Data de referência para os registros
                data_apuracao = datetime.now()

                # 4. Remover apenas registros do critério específico para este edital/período
                # MODIFICAÇÃO: Não apagar todos os registros, apenas os deste critério específico
                delete_sql = text("""
                DELETE FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE [ID_EDITAL] = :edital_id
                  AND [ID_PERIODO] = :periodo_id
                  AND [COD_CRITERIO_SELECAO] = :criterio_id
                """)

                connection.execute(delete_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                })

                print(f"Registros antigos removidos para critério {criterio_id}")

                # 5. Inserir novos percentuais para empresas remanescentes
                print("\n----- INSERINDO NOVOS PERCENTUAIS -----")

                for empresa in empresas_dados:
                    id_empresa = empresa["id_empresa"]
                    vr_arrecadacao = empresa["vr_arrecadacao"]
                    percentual_original = empresa["percentual"]
                    percentual_final = percentual_original + percentual_unitario

                    insert_sql = text("""
                    INSERT INTO [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                    ([ID_EDITAL], [ID_PERIODO], [ID_EMPRESA], [COD_CRITERIO_SELECAO], 
                     [DT_APURACAO], [VR_ARRECADACAO], [QTDE_MAXIMA], [VALOR_MAXIMO], 
                     [PERCENTUAL_FINAL], [CREATED_AT], [UPDATED_AT], [DELETED_AT])
                    VALUES
                    (:edital_id, :periodo_id, :id_empresa, :criterio_id,
                     :data_apuracao, :vr_arrecadacao, NULL, NULL,
                     :percentual_final, GETDATE(), NULL, NULL)
                    """)

                    connection.execute(insert_sql, {
                        "edital_id": edital_id,
                        "periodo_id": periodo_id,
                        "id_empresa": id_empresa,
                        "criterio_id": criterio_id,
                        "data_apuracao": data_apuracao,
                        "vr_arrecadacao": vr_arrecadacao,
                        "percentual_final": percentual_final
                    })

                    print(
                        f"Empresa {id_empresa}: {percentual_original:.2f}% + {percentual_unitario:.2f}% = {percentual_final:.2f}%")

                # 6. Ajustar percentuais para garantir soma 100%
                print("\n----- AJUSTANDO PERCENTUAIS PARA SOMA 100% -----")

                # Cria tabela temporária para ajuste de percentuais
                adjust_sql = text("""
                DECLARE @percentual100 TABLE (
                    [ID_EMPRESA] INT,
                    [PERCENTUAL_FINAL] DECIMAL(6,2)
                );

                ;WITH Percentuais AS (
                    SELECT 
                        [ID_EMPRESA],
                        [PERCENTUAL_FINAL],
                        SUM([PERCENTUAL_FINAL]) OVER () AS SomaPercentuais
                    FROM 
                        [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                    WHERE
                        [ID_EDITAL] = :edital_id
                        AND [ID_PERIODO] = :periodo_id
                        AND [COD_CRITERIO_SELECAO] = :criterio_id
                )
                INSERT INTO @percentual100
                SELECT	
                    [ID_EMPRESA],
                    CASE 
                        WHEN RANK() OVER (ORDER BY [PERCENTUAL_FINAL] DESC) = 1 
                        THEN [PERCENTUAL_FINAL] + (100.00 - SomaPercentuais)
                        ELSE [PERCENTUAL_FINAL]
                    END AS PERCENTUAL_FINAL
                FROM
                    Percentuais;

                -- Atualizar percentuais finais
                UPDATE LID
                SET LID.[PERCENTUAL_FINAL] = P.[PERCENTUAL_FINAL]
                FROM
                    [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] AS LID
                    INNER JOIN @percentual100 AS P
                        ON LID.[ID_EMPRESA] = P.[ID_EMPRESA]
                WHERE
                    LID.[ID_EDITAL] = :edital_id
                    AND LID.[ID_PERIODO] = :periodo_id
                    AND LID.[COD_CRITERIO_SELECAO] = :criterio_id;
                """)

                connection.execute(adjust_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                })

                print("Percentuais ajustados para garantir soma 100%")

                # 7. Contar contratos distribuíveis e calcular valor total
                count_sql = text("""
                SELECT 
                    COUNT(*) AS QTDE_CONTRATOS,
                    COALESCE(SUM(VR_SD_DEVEDOR), 0) AS VALOR_TOTAL
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                """)

                count_result = connection.execute(count_sql).fetchone()
                qtde_registros = count_result[0] if count_result else 0
                valor_total = float(count_result[1]) if count_result and count_result[1] else 0.0

                print(f"Total de contratos distribuíveis: {qtde_registros}")
                print(f"Valor total: {valor_total:.2f}")

                # 8. Atualizar quantidades e valores máximos
                update_sql = text("""
                UPDATE [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                SET 
                    [QTDE_MAXIMA] = CONVERT(INT, :qtde_registros * [PERCENTUAL_FINAL] / 100),
                    [VALOR_MAXIMO] = CONVERT(DECIMAL(18,2), :valor_total * [PERCENTUAL_FINAL] / 100)
                WHERE
                    [ID_EDITAL] = :edital_id
                    AND [ID_PERIODO] = :periodo_id
                    AND [COD_CRITERIO_SELECAO] = :criterio_id
                """)

                connection.execute(update_sql, {
                    "qtde_registros": qtde_registros,
                    "valor_total": valor_total,
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                })

                print("Quantidades e valores máximos atualizados")

                # 9. Verificar e distribuir sobras - CORRIGIDO para resolver ambiguidade de colunas
                sobra_sql = text("""
                DECLARE @SOBRA INT;
                SET @SOBRA = :qtde_registros - (
                    SELECT 
                        SUM(LD.[QTDE_MAXIMA]) 
                    FROM 
                        [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] AS LD
                    WHERE
                        LD.[ID_EDITAL] = :edital_id
                        AND LD.[ID_PERIODO] = :periodo_id
                        AND LD.[COD_CRITERIO_SELECAO] = :criterio_id
                );

                IF @SOBRA > 0
                BEGIN
                    -- Atualizar as empresas com maiores quantidades
                    WITH RankedEmpresas AS (
                        SELECT 
                            LD.[ID],
                            LD.[QTDE_MAXIMA],
                            ROW_NUMBER() OVER (ORDER BY LD.[QTDE_MAXIMA] DESC) AS RN
                        FROM 
                            [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] AS LD
                        WHERE
                            LD.[ID_EDITAL] = :edital_id
                            AND LD.[ID_PERIODO] = :periodo_id
                            AND LD.[COD_CRITERIO_SELECAO] = :criterio_id
                    )
                    UPDATE LIM
                    SET LIM.[QTDE_MAXIMA] = LIM.[QTDE_MAXIMA] + 1
                    FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] AS LIM
                    INNER JOIN RankedEmpresas AS R
                        ON LIM.[ID] = R.[ID]
                    WHERE R.RN <= @SOBRA;
                END
                """)

                connection.execute(sobra_sql, {
                    "qtde_registros": qtde_registros,
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                })

                print("Sobras distribuídas para as maiores empresas")

                # 10. Verificar os resultados finais
                check_sql = text("""
                SELECT 
                    COUNT(*) as num_empresas,
                    SUM(PERCENTUAL_FINAL) as soma_percentual,
                    SUM(QTDE_MAXIMA) as soma_qtde
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE
                    [ID_EDITAL] = :edital_id
                    AND [ID_PERIODO] = :periodo_id
                    AND [COD_CRITERIO_SELECAO] = :criterio_id
                """)

                check_result = connection.execute(check_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                }).fetchone()

                if check_result:
                    num_empresas = check_result[0]
                    soma_percentual = float(check_result[1]) if check_result[1] else 0
                    soma_qtde = check_result[2] if check_result[2] else 0

                    print(f"\n----- RESULTADO FINAL -----")
                    print(f"Empresas: {num_empresas}")
                    print(f"Soma dos percentuais: {soma_percentual:.2f}%")
                    print(f"Soma das quantidades: {soma_qtde}")
                    print(f"Quantidade original: {qtde_registros}")

                # Commit da transação se tudo correu bem
                transaction.commit()
                print("\nRedistribuição de percentuais concluída com sucesso!")
                logging.info("Redistribuição de percentuais concluída com sucesso")

                return True

            except Exception as e:
                # Rollback em caso de erro
                transaction.rollback()
                error_msg = f"Erro durante a redistribuição de percentuais: {str(e)}"
                print(error_msg)
                logging.error(error_msg)

                import traceback
                trace_msg = traceback.format_exc()
                print(f"Traceback: {trace_msg}")
                logging.error(f"Traceback: {trace_msg}")

                return False

    except Exception as e:
        error_msg = f"Erro geral ao redistribuir percentuais: {str(e)}"
        print(error_msg)
        logging.error(error_msg)

        import traceback
        trace_msg = traceback.format_exc()
        print(f"Traceback: {trace_msg}")
        logging.error(f"Traceback: {trace_msg}")

        return False


def processar_contratos_arrastaveis(edital_id, periodo_id, criterio_id):
    """
    Processa os contratos arrastáveis (do mesmo CPF/CNPJ) para redistribuição.
    Versão que remove registros antigos antes de inserir novos.
    """
    logging.info(
        f"Processando contratos arrastáveis - Edital: {edital_id}, Período: {periodo_id}, Critério: {criterio_id}")

    try:
        with db.engine.connect() as connection:
            transaction = connection.begin()

            try:
                # 1. Limpar tabela arrastáveis
                truncate_sql = text("""TRUNCATE TABLE [DEV].[DCA_TB007_ARRASTAVEIS]""")
                connection.execute(truncate_sql)
                print("Tabela DCA_TB007_ARRASTAVEIS truncada com sucesso")

                # 2. Identificar e inserir contratos arrastáveis (mesmo CPF/CNPJ)
                insert_arrastaveis_sql = text("""
                WITH arrastaveis AS (
                    SELECT
                        ID,
                        [FkContratoSISCTR],
                        [NR_CPF_CNPJ],
                        NU_LINHA = 
                            ROW_NUMBER() OVER (PARTITION BY [NR_CPF_CNPJ] 
                            ORDER BY [NR_CPF_CNPJ] DESC)
                    FROM
                        [DEV].[DCA_TB006_DISTRIBUIVEIS]
                ),
                cpfArrastaveis as (
                    SELECT DISTINCT
                        [NR_CPF_CNPJ]
                    FROM 
                        arrastaveis
                    WHERE NU_LINHA > 1
                )
                INSERT INTO [DEV].[DCA_TB007_ARRASTAVEIS]
                (
                    [FkContratoSISCTR],
                    [NR_CPF_CNPJ],
                    [CREATED_AT],
                    [UPDATED_AT],
                    [DELETED_AT]
                )
                SELECT
                    DIS.[FkContratoSISCTR],
                    DIS.[NR_CPF_CNPJ],
                    GETDATE() AS [CREATED_AT],
                    NULL AS [UPDATED_AT],
                    NULL AS [DELETED_AT]
                FROM 
                    cpfArrastaveis AS CAR
                    INNER JOIN [DEV].[DCA_TB006_DISTRIBUIVEIS] AS DIS
                        ON CAR.[NR_CPF_CNPJ] = DIS.[NR_CPF_CNPJ]
                """)

                connection.execute(insert_arrastaveis_sql)

                # 3. Verificar número de contratos arrastáveis inseridos
                count_sql = text("""
                SELECT COUNT(*) FROM [DEV].[DCA_TB007_ARRASTAVEIS]
                """)

                count_result = connection.execute(count_sql).fetchone()
                qtde_arrastaveis = count_result[0] if count_result else 0

                print(f"Total de contratos arrastáveis identificados: {qtde_arrastaveis}")

                if qtde_arrastaveis == 0:
                    print("Não foram encontrados contratos arrastáveis. Finalizando etapa.")
                    transaction.commit()
                    return 0, True

                # 4. Remover contratos arrastáveis da tabela de distribuíveis
                delete_sql = text("""
                DELETE FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                WHERE [FkContratoSISCTR] IN (
                    SELECT [FkContratoSISCTR]
                    FROM [DEV].[DCA_TB007_ARRASTAVEIS]
                )
                """)

                delete_result = connection.execute(delete_sql)
                print(f"Contratos arrastáveis removidos da tabela de distribuíveis: {delete_result.rowcount}")

                # 5. NOVA ABORDAGEM: Remover registros antigos antes de inserir novos
                # Excluir registros existentes para os contratos que serão processados
                delete_existing_sql = text("""
                DELETE FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                WHERE [ID_EDITAL] = :edital_id
                  AND [ID_PERIODO] = :periodo_id
                  AND [fkContratoSISCTR] IN (
                      SELECT [FkContratoSISCTR]
                      FROM [DEV].[DCA_TB007_ARRASTAVEIS]
                  )
                """)

                delete_existing_result = connection.execute(delete_existing_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id
                })

                print(f"Registros antigos removidos da tabela de distribuição: {delete_existing_result.rowcount}")

                # 6. Inserir novos registros na tabela de distribuição
                insert_distribuicao_sql = text("""
                INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO] (
                    [DT_REFERENCIA],
                    [ID_EDITAL],
                    [ID_PERIODO],
                    [fkContratoSISCTR],
                    [COD_CRITERIO_SELECAO],
                    [COD_EMPRESA_COBRANCA],
                    [NR_CPF_CNPJ],
                    [VR_SD_DEVEDOR],
                    [CREATED_AT]
                )
                SELECT 
                    GETDATE() AS [DT_REFERENCIA],
                    :edital_id AS [ID_EDITAL],
                    :periodo_id AS [ID_PERIODO],
                    ARR.[FkContratoSISCTR],
                    :criterio_id AS [COD_CRITERIO_SELECAO],
                    LD.[ID_EMPRESA] AS [COD_EMPRESA_COBRANCA],
                    ARR.[NR_CPF_CNPJ],
                    SIT.[VR_SD_DEVEDOR],
                    GETDATE() AS [CREATED_AT]
                FROM 
                    [DEV].[DCA_TB007_ARRASTAVEIS] ARR
                    -- Obter o saldo devedor da tabela de situação
                    INNER JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] SIT
                        ON ARR.[FkContratoSISCTR] = SIT.[fkContratoSISCTR]
                    -- Distribuir por empresas conforme percentuais
                    CROSS APPLY (
                        SELECT TOP 1 LD.[ID_EMPRESA]
                        FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
                        WHERE 
                            LD.[ID_EDITAL] = :edital_id
                            AND LD.[ID_PERIODO] = :periodo_id
                            AND LD.[COD_CRITERIO_SELECAO] = :criterio_id
                            AND LD.[DELETED_AT] IS NULL
                        ORDER BY NEWID() -- Distribuição aleatória entre empresas
                    ) AS LD
                """)

                insert_result = connection.execute(insert_distribuicao_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                })

                contratos_inseridos = insert_result.rowcount
                print(f"Contratos arrastáveis inseridos na tabela de distribuição: {contratos_inseridos}")

                # 7. Contar quantos contratos foram efetivamente redistribuídos
                total_sql = text("""
                SELECT COUNT(*) 
                FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                WHERE [ID_EDITAL] = :edital_id
                  AND [ID_PERIODO] = :periodo_id
                  AND [COD_CRITERIO_SELECAO] = :criterio_id
                  AND [fkContratoSISCTR] IN (
                      SELECT [FkContratoSISCTR]
                      FROM [DEV].[DCA_TB007_ARRASTAVEIS]
                  )
                """)

                total_processados = connection.execute(total_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                }).scalar()

                print(f"Total de contratos arrastáveis redistribuídos: {total_processados}")

                transaction.commit()
                print("Processamento de contratos arrastáveis concluído com sucesso!")

                return total_processados, True

            except Exception as e:
                transaction.rollback()
                error_msg = f"Erro durante o processamento de contratos arrastáveis: {str(e)}"
                print(error_msg)
                logging.error(error_msg)
                return 0, False

    except Exception as e:
        error_msg = f"Erro geral ao processar contratos arrastáveis: {str(e)}"
        print(error_msg)
        logging.error(error_msg)
        return 0, False


def processar_demais_contratos(edital_id, periodo_id, criterio_id, empresa_redistribuida=None):
    """
    Processa os contratos restantes (não arrastáveis) para redistribuição.
    Versão que remove registros antigos antes de inserir novos.
    """
    logging.info(f"Processando demais contratos - Edital: {edital_id}, Período: {periodo_id}, Critério: {criterio_id}")

    try:
        with db.engine.connect() as connection:
            transaction = connection.begin()

            try:
                # 1. Verificar contratos restantes na tabela
                count_sql = text("""
                SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                """)

                count_result = connection.execute(count_sql).fetchone()
                qtde_contratos_restantes = count_result[0] if count_result else 0

                print(f"Total de contratos restantes para distribuição: {qtde_contratos_restantes}")

                if qtde_contratos_restantes == 0:
                    print("Não há contratos restantes para distribuir. Finalizando etapa.")
                    transaction.commit()
                    return 0, True

                # 2. Contar registros atuais na tabela de distribuição (para cálculo da diferença)
                baseline_sql = text("""
                SELECT COUNT(*) 
                FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id 
                  AND ID_PERIODO = :periodo_id 
                  AND COD_CRITERIO_SELECAO = :criterio_id
                """)

                baseline_result = connection.execute(baseline_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                }).fetchone()

                baseline_count = baseline_result[0] if baseline_result else 0

                # 3. NOVA ABORDAGEM: Remover registros antigos antes de inserir novos
                # Excluir registros existentes para os contratos que serão processados
                delete_existing_sql = text("""
                DELETE FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                WHERE [ID_EDITAL] = :edital_id
                  AND [ID_PERIODO] = :periodo_id
                  AND [fkContratoSISCTR] IN (
                      SELECT [FkContratoSISCTR]
                      FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                  )
                """)

                delete_existing_result = connection.execute(delete_existing_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id
                })

                print(f"Registros antigos removidos da tabela de distribuição: {delete_existing_result.rowcount}")

                # 4. Inserir contratos restantes com distribuição proporcional
                insert_sql = text("""
                INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO] (
                    [DT_REFERENCIA],
                    [ID_EDITAL],
                    [ID_PERIODO],
                    [fkContratoSISCTR],
                    [COD_CRITERIO_SELECAO],
                    [COD_EMPRESA_COBRANCA],
                    [NR_CPF_CNPJ],
                    [VR_SD_DEVEDOR],
                    [CREATED_AT]
                )
                SELECT 
                    GETDATE() AS [DT_REFERENCIA],
                    :edital_id AS [ID_EDITAL],
                    :periodo_id AS [ID_PERIODO],
                    D.[FkContratoSISCTR],
                    :criterio_id AS [COD_CRITERIO_SELECAO],
                    LD.[ID_EMPRESA] AS [COD_EMPRESA_COBRANCA],
                    D.[NR_CPF_CNPJ],
                    D.[VR_SD_DEVEDOR],
                    GETDATE() AS [CREATED_AT]
                FROM 
                    [DEV].[DCA_TB006_DISTRIBUIVEIS] D
                    CROSS APPLY (
                        SELECT TOP 1 L.[ID_EMPRESA]
                        FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] L
                        WHERE 
                            L.[ID_EDITAL] = :edital_id
                            AND L.[ID_PERIODO] = :periodo_id
                            AND L.[COD_CRITERIO_SELECAO] = :criterio_id
                            AND L.[DELETED_AT] IS NULL
                            AND (:empresa_redistribuida IS NULL OR L.[ID_EMPRESA] <> :empresa_redistribuida)
                        ORDER BY 
                            -- Priorizar empresas com menos contratos em relação ao seu percentual
                            (SELECT COUNT(*) FROM [DEV].[DCA_TB005_DISTRIBUICAO] 
                                WHERE COD_EMPRESA_COBRANCA = L.[ID_EMPRESA] 
                                AND ID_EDITAL = :edital_id 
                                AND ID_PERIODO = :periodo_id) / NULLIF(L.[PERCENTUAL_FINAL], 0) ASC,
                            NEWID() -- Randomiza se empatar
                    ) AS LD
                """)

                insert_result = connection.execute(insert_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id,
                    "empresa_redistribuida": empresa_redistribuida
                })

                contratos_inseridos = insert_result.rowcount
                print(f"Contratos restantes inseridos na tabela de distribuição: {contratos_inseridos}")

                # 5. Verificar contagem final
                if contratos_inseridos != qtde_contratos_restantes:
                    print(
                        f"ATENÇÃO: Diferença na contagem - restantes: {qtde_contratos_restantes}, inseridos: {contratos_inseridos}")
                    logging.warning(
                        f"Diferença na contagem de contratos restantes - {qtde_contratos_restantes} vs {contratos_inseridos}")

                # 6. Limpar tabela de distribuíveis após processamento
                if contratos_inseridos > 0:
                    connection.execute(text("TRUNCATE TABLE [DEV].[DCA_TB006_DISTRIBUIVEIS]"))
                    print("Tabela de distribuíveis limpa após processamento")

                # 7. Contar total após inserção (para verificação)
                final_count_sql = text("""
                SELECT COUNT(*) 
                FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id 
                  AND ID_PERIODO = :periodo_id 
                  AND COD_CRITERIO_SELECAO = :criterio_id
                """)

                final_count_result = connection.execute(final_count_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                }).fetchone()

                total_final = final_count_result[0] if final_count_result else 0
                print(f"Total atualizado na tabela de distribuição: {total_final}")
                print(f"Diferença após processamento: {total_final - baseline_count}")

                transaction.commit()
                print("Processamento dos contratos restantes concluído com sucesso!")

                # Retornar a contagem precisa
                return contratos_inseridos, True

            except Exception as e:
                transaction.rollback()
                error_msg = f"Erro durante o processamento dos contratos restantes: {str(e)}"
                print(error_msg)
                logging.error(error_msg)
                return 0, False

    except Exception as e:
        error_msg = f"Erro geral ao processar contratos restantes: {str(e)}"
        print(error_msg)
        logging.error(error_msg)
        return 0, False

def processar_redistribuicao_contratos(edital_id, periodo_id, empresa_id, cod_criterio):
    """
    Executa o processo completo de redistribuição de contratos.
    Modificado para preservar distribuições anteriores e usar o critério especificado.

    Args:
        edital_id: ID do edital
        periodo_id: ID do período
        empresa_id: ID da empresa que está saindo
        cod_criterio: Código do critério de redistribuição

    Returns:
        dict: Resultados do processo
    """
    # Configuração básica de logging
    import logging
    import sys

    # Configurar o logging para exibir no console e em um arquivo
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("redistribuicao.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Print direto no console para garantir visibilidade
    print("\n\n")
    print("=" * 80)
    print(f"CHAMADA DA FUNÇÃO processar_redistribuicao_contratos")
    print(
        f"Parâmetros: edital_id={edital_id}, periodo_id={periodo_id}, empresa_id={empresa_id}, cod_criterio={cod_criterio}")
    print("=" * 80)

    # Inicializar resultados
    resultados = {
        "contratos_selecionados": 0,  # Total selecionado inicialmente
        "contratos_arrastados": 0,  # Contratos arrastáveis
        "contratos_restantes": 0,  # Demais contratos
        "total_redistribuido": 0,  # Total efetivamente redistribuído
        "contratos_redistribuidos": 0,  # Alias para total_redistribuido (para compatibilidade)
        "total_empresas": 0,
        "empresa_redistribuida": empresa_id,
        "success": False
    }

    try:
        # VALIDAÇÃO INICIAL: Verificar empresas receptoras
        with db.engine.connect() as connection:
            # Primeiro, contar as empresas
            count_sql = text("""
                SELECT COUNT(DISTINCT ID_EMPRESA) as qtde_empresas
                FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND DS_CONDICAO IN ('NOVA', 'PERMANECE')
                AND ID_EMPRESA <> :empresa_id
                AND DELETED_AT IS NULL
            """)

            count_result = connection.execute(count_sql, {
                "edital_id": edital_id,
                "periodo_id": periodo_id,
                "empresa_id": empresa_id
            }).fetchone()

            empresas_receptoras = count_result[0] if count_result else 0

            # Depois, buscar os IDs das empresas (se necessário para debug)
            ids_sql = text("""
                SELECT DISTINCT ID_EMPRESA
                FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND DS_CONDICAO IN ('NOVA', 'PERMANECE')
                AND ID_EMPRESA <> :empresa_id
                AND DELETED_AT IS NULL
            """)

            ids_result = connection.execute(ids_sql, {
                "edital_id": edital_id,
                "periodo_id": periodo_id,
                "empresa_id": empresa_id
            }).fetchall()

            ids_empresas = ", ".join([str(row[0]) for row in ids_result]) if ids_result else ""

            print(f"Empresas receptoras encontradas: {empresas_receptoras}")
            print(f"IDs das empresas receptoras: {ids_empresas}")
            logging.info(f"Empresas receptoras: {empresas_receptoras} - IDs: {ids_empresas}")

            if empresas_receptoras == 0:
                print("ERRO: Nenhuma empresa disponível para receber a redistribuição")
                resultados["error"] = "Nenhuma empresa disponível para receber a redistribuição"
                return resultados

            if empresas_receptoras < 2:
                print(f"AVISO: Apenas {empresas_receptoras} empresa(s) disponível(is) para receber a redistribuição")
                logging.warning(f"Redistribuição com poucas empresas: {empresas_receptoras}")

        # ETAPA 1: Selecionar contratos a redistribuir
        print("\n----- ETAPA 1: SELEÇÃO DE CONTRATOS -----")
        num_contratos = selecionar_contratos_para_redistribuicao(empresa_id)
        print(f"Total de contratos selecionados: {num_contratos}")

        if num_contratos == 0:
            print("Sem contratos para redistribuir. Processo encerrado.")
            return resultados

        # Atualizar resultados com número de contratos selecionados
        resultados["contratos_selecionados"] = num_contratos

        # ETAPA 2: Calcular percentuais para redistribuição
        print("\n----- ETAPA 2: CÁLCULO DE PERCENTUAIS -----")
        percentual_redistribuido, total_arrecadacao, empresas_dados = calcular_percentuais_redistribuicao(
            edital_id, periodo_id, empresa_id)

        print(f"Percentual a redistribuir: {percentual_redistribuido:.2f}%")
        print(f"Total de arrecadação: {total_arrecadacao:.2f}")
        print(f"Empresas encontradas para redistribuição: {len(empresas_dados)}")

        if len(empresas_dados) == 0:
            print("Sem empresas para receber a redistribuição. Processo encerrado.")
            return resultados

        # Exibir dados resumidos das empresas
        print("\nResumo de empresas para redistribuição:")
        print("ID Empresa | Arrecadação | Percentual")
        print("-" * 50)
        for i, empresa in enumerate(empresas_dados[:5]):  # Mostrar apenas as 5 primeiras
            print(f"{empresa['id_empresa']: <10} | {empresa['vr_arrecadacao']:,.2f} | {empresa['percentual']:.2f}%")

        if len(empresas_dados) > 5:
            print(f"... e mais {len(empresas_dados) - 5} empresa(s)")

        # ETAPA 3: Redistribuir percentuais entre empresas remanescentes
        print("\n----- ETAPA 3: REDISTRIBUIÇÃO DE PERCENTUAIS -----")
        redistribuicao_ok = redistribuir_percentuais(
            edital_id,
            periodo_id,
            cod_criterio,
            empresa_id,
            percentual_redistribuido,
            empresas_dados
        )

        if not redistribuicao_ok:
            print("Falha na redistribuição de percentuais. Processo encerrado.")
            resultados["error"] = "Falha na etapa de redistribuição de percentuais"
            return resultados

        print("Redistribuição de percentuais concluída com sucesso!")

        # ETAPA 4: Redistribuição de contratos arrastáveis
        print("\n----- ETAPA 4: REDISTRIBUIÇÃO DE CONTRATOS ARRASTÁVEIS -----")
        contratos_arrastados, arrastaveis_ok = processar_contratos_arrastaveis(
            edital_id,
            periodo_id,
            cod_criterio
        )

        if not arrastaveis_ok:
            print("Falha no processamento de contratos arrastáveis. Processo encerrado.")
            resultados["error"] = "Falha na etapa de processamento de contratos arrastáveis"
            return resultados

        print(f"Processamento de contratos arrastáveis concluído: {contratos_arrastados} contratos")
        resultados["contratos_arrastados"] = contratos_arrastados

        # ETAPA 5: Redistribuição dos demais contratos
        print("\n----- ETAPA 5: REDISTRIBUIÇÃO DOS DEMAIS CONTRATOS -----")

        contratos_restantes, restantes_ok = processar_demais_contratos(
            edital_id,
            periodo_id,
            cod_criterio,
            empresa_id  # Passando o ID da empresa que está sendo redistribuída
        )

        # Se a função não executou com sucesso ou retornou 0, calcular valor correto
        if not restantes_ok or contratos_restantes == 0:
            # Calcular valor dos contratos restantes baseado na diferença
            contratos_restantes = num_contratos - contratos_arrastados
            print(f"CORREÇÃO: Ajustando valor de contratos restantes para {contratos_restantes}")

        print(f"Processamento dos contratos restantes concluído: {contratos_restantes} contratos")
        resultados["contratos_restantes"] = contratos_restantes

        # IMPORTANTE: Calcular o total redistribuído somando arrastados e restantes
        total_redistribuido = contratos_arrastados + contratos_restantes

        # Garantir que o total seja igual ao número inicial de contratos selecionados
        if total_redistribuido != num_contratos:
            print(
                f"\nATENÇÃO: Total redistribuído ({total_redistribuido}) difere do total selecionado ({num_contratos})")
            print("Ajustando o total para garantir consistência.")
            total_redistribuido = num_contratos

        # Atualizar resultados finais
        resultados.update({
            "contratos_selecionados": num_contratos,
            "contratos_arrastados": contratos_arrastados,
            "contratos_restantes": contratos_restantes,
            "total_redistribuido": total_redistribuido,
            "contratos_redistribuidos": total_redistribuido,  # Para compatibilidade
            "total_empresas": len(empresas_dados),
            "percentual_redistribuido": percentual_redistribuido,
            "empresas_remanescentes": len(empresas_dados),
            "success": True,
            "etapas_concluidas": "Processo de redistribuição completo"
        })

        print("\n----- RESULTADO FINAL DA REDISTRIBUIÇÃO -----")
        print(f"Contratos selecionados inicialmente: {resultados['contratos_selecionados']}")
        print(f"Contratos arrastáveis redistribuídos: {resultados['contratos_arrastados']}")
        print(f"Contratos restantes redistribuídos: {resultados['contratos_restantes']}")
        print(f"Total de contratos efetivamente redistribuídos: {resultados['total_redistribuido']}")
        print(f"Percentual da empresa redistribuída: {percentual_redistribuido:.2f}%")
        print(f"Empresas remanescentes: {len(empresas_dados)}")
        print("=" * 50)
        print("PROCESSO DE REDISTRIBUIÇÃO CONCLUÍDO COM SUCESSO!")
        print("=" * 80)

        return resultados

    except Exception as e:
        print(f"ERRO CRÍTICO: {str(e)}")
        logging.error(f"Erro no processo de redistribuição: {str(e)}")

        import traceback
        trace = traceback.format_exc()
        print(trace)
        logging.error(trace)

        # Retornar resultados com informações de erro
        resultados.update({
            "error": str(e),
            "success": False,
            "contratos_redistribuidos": 0  # Garantir que esta chave exista mesmo em caso de erro
        })

        return resultados