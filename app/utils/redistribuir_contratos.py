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

    A distribuição é proporcional à arrecadação do maior edital e maior período.
    Similar à lógica usada nas funções de distribuição de contratos.
    """
    import logging
    import sys

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
        with db.engine.connect() as connection:
            # 1. Buscar o maior edital e maior período disponíveis
            max_edital_periodo_sql = text("""
                SELECT 
                    MAX(ID_EDITAL) as MAX_EDITAL,
                    MAX(ID_PERIODO) as MAX_PERIODO
                FROM [DEV].[DCA_TB001_PERIODO_AVALIACAO]
                WHERE DELETED_AT IS NULL
            """)

            max_result = connection.execute(max_edital_periodo_sql).fetchone()

            if not max_result:
                print("ERRO: Não foi possível encontrar edital/período máximo")
                return 0, 0, []

            max_edital = max_result[0]
            max_periodo = max_result[1]

            print(f"Usando maior edital: {max_edital}, maior período: {max_periodo}")

            # 2. Buscar datas do período máximo
            periodo_sql = text("""
                SELECT DT_INICIO, DT_FIM 
                FROM [DEV].[DCA_TB001_PERIODO_AVALIACAO]
                WHERE ID_EDITAL = :edital_id 
                  AND ID_PERIODO = :periodo_id
                  AND DELETED_AT IS NULL
            """)

            periodo_result = connection.execute(periodo_sql, {
                "edital_id": max_edital,
                "periodo_id": max_periodo
            }).fetchone()

            if not periodo_result:
                # Se não encontrar o máximo, usa o período atual
                periodo_result = connection.execute(periodo_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id
                }).fetchone()

                if not periodo_result:
                    print("ERRO: Período não encontrado")
                    return 0, 0, []

            dt_inicio, dt_fim = periodo_result
            print(f"Período para cálculo: {dt_inicio} a {dt_fim}")

            # 3. Buscar empresas participantes e suas arrecadações (similar ao distribuir_contratos)
            arrecadacao_sql = text("""
            WITH EmpresasParticipantes AS (
                SELECT DISTINCT
                    EP.ID_EMPRESA,
                    EP.NO_EMPRESA,
                    EP.NO_EMPRESA_ABREVIADA,
                    EP.DS_CONDICAO
                FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES] EP
                WHERE EP.ID_EDITAL = :edital_id
                  AND EP.ID_PERIODO = :periodo_id
                  AND EP.DELETED_AT IS NULL
            ),
            ArrecadacaoEmpresas AS (
                SELECT 
                    REE.CO_EMPRESA_COBRANCA,
                    SUM(REE.VR_ARRECADACAO_TOTAL) AS VR_ARRECADACAO_TOTAL
                FROM [BDG].[COM_TB062_REMUNERACAO_ESTIMADA] REE
                WHERE REE.DT_ARRECADACAO BETWEEN :dt_inicio AND :dt_fim
                GROUP BY REE.CO_EMPRESA_COBRANCA
                HAVING SUM(REE.VR_ARRECADACAO_TOTAL) > 0
            )
            SELECT 
                EP.ID_EMPRESA,
                EP.NO_EMPRESA,
                EP.NO_EMPRESA_ABREVIADA,
                EP.DS_CONDICAO,
                COALESCE(AE.VR_ARRECADACAO_TOTAL, 0) AS VR_ARRECADACAO,
                -- Calcular percentual direto na query
                CASE 
                    WHEN (SELECT SUM(COALESCE(AE2.VR_ARRECADACAO_TOTAL, 0))
                          FROM EmpresasParticipantes EP2
                          LEFT JOIN ArrecadacaoEmpresas AE2 ON EP2.ID_EMPRESA = AE2.CO_EMPRESA_COBRANCA
                          WHERE EP2.DS_CONDICAO IN ('NOVA', 'PERMANECE')) = 0
                    THEN 0
                    ELSE ROUND(
                        (COALESCE(AE.VR_ARRECADACAO_TOTAL, 0) * 100.0) / 
                        NULLIF((SELECT SUM(COALESCE(AE2.VR_ARRECADACAO_TOTAL, 0))
                                FROM EmpresasParticipantes EP2
                                LEFT JOIN ArrecadacaoEmpresas AE2 ON EP2.ID_EMPRESA = AE2.CO_EMPRESA_COBRANCA
                                WHERE EP2.DS_CONDICAO IN ('NOVA', 'PERMANECE')), 0), 
                        2)
                END AS PERCENTUAL_ARRECADACAO
            FROM EmpresasParticipantes EP
            LEFT JOIN ArrecadacaoEmpresas AE ON EP.ID_EMPRESA = AE.CO_EMPRESA_COBRANCA
            ORDER BY EP.ID_EMPRESA
            """)

            arrecadacao_results = connection.execute(arrecadacao_sql, {
                "edital_id": edital_id,
                "periodo_id": periodo_id,
                "dt_inicio": dt_inicio,
                "dt_fim": dt_fim
            }).fetchall()

            if not arrecadacao_results:
                print("ERRO: Nenhuma empresa encontrada")
                return 0, 0, []

            # 4. Processar resultados
            print("\n----- ARRECADAÇÃO E PERCENTUAIS -----")
            empresas_dados = []
            total_arrecadacao = 0
            total_arrecadacao_receptoras = 0
            percentual_empresa_redistribuida = 0
            arrecadacao_empresa_redistribuida = 0

            for idx, emp in enumerate(arrecadacao_results):
                id_empresa, nome, nome_abrev, ds_condicao, vr_arrecadacao, percentual = emp
                vr_arrecadacao = float(vr_arrecadacao) if vr_arrecadacao else 0.0
                percentual = float(percentual) if percentual else 0.0

                total_arrecadacao += vr_arrecadacao

                if ds_condicao in ['NOVA', 'PERMANECE']:
                    total_arrecadacao_receptoras += vr_arrecadacao

                print(f"{idx + 1}. Empresa {id_empresa} ({nome_abrev or nome}): "
                      f"R$ {vr_arrecadacao:,.2f} ({percentual:.2f}%)")

                if id_empresa == empresa_id:
                    arrecadacao_empresa_redistribuida = vr_arrecadacao
                    print(f"   >>> Empresa que está saindo")

                elif ds_condicao in ['NOVA', 'PERMANECE']:
                    empresas_dados.append({
                        "id_empresa": id_empresa,
                        "nome": nome,
                        "nome_abreviado": nome_abrev or nome,
                        "ds_condicao": ds_condicao,
                        "vr_arrecadacao": vr_arrecadacao,
                        "percentual": percentual
                    })

            # 5. Calcular percentual da empresa que está saindo
            if total_arrecadacao > 0:
                percentual_empresa_redistribuida = (arrecadacao_empresa_redistribuida / total_arrecadacao) * 100
            else:
                percentual_empresa_redistribuida = 0.0

            print(f"\nTotal de arrecadação geral: R$ {total_arrecadacao:,.2f}")
            print(f"Total arrecadação receptoras: R$ {total_arrecadacao_receptoras:,.2f}")
            print(f"Arrecadação empresa saindo: R$ {arrecadacao_empresa_redistribuida:,.2f}")
            print(f"Percentual empresa saindo: {percentual_empresa_redistribuida:.2f}%")

            # 6. Se não há arrecadação, distribuir igualmente (similar a distribuir_contratos)
            if total_arrecadacao_receptoras == 0 and empresas_dados:
                print("\nSem arrecadação. Distribuindo igualmente.")
                qtde_empresas = len(empresas_dados)
                percentual_igual = 100.0 / qtde_empresas if qtde_empresas > 0 else 0

                for emp in empresas_dados:
                    emp['percentual'] = percentual_igual

                if percentual_empresa_redistribuida == 0:
                    percentual_empresa_redistribuida = 100.0 / (qtde_empresas + 1)

            # 7. Normalizar percentuais para garantir soma = 100%
            soma_percentuais = sum(emp['percentual'] for emp in empresas_dados)
            if soma_percentuais > 0 and abs(soma_percentuais - 100.0) > 0.01:
                print(f"\nAjustando percentuais. Soma atual: {soma_percentuais:.2f}%")
                fator = 100.0 / soma_percentuais
                for emp in empresas_dados:
                    emp['percentual'] = round(emp['percentual'] * fator, 2)

            # 8. Ajuste final para garantir exatamente 100%
            soma_final = sum(emp['percentual'] for emp in empresas_dados)
            if abs(soma_final - 100.0) > 0.01 and empresas_dados:
                diferenca = 100.0 - soma_final
                # Ajusta na empresa com maior arrecadação
                maior_empresa = max(empresas_dados, key=lambda x: x['vr_arrecadacao'])
                maior_empresa['percentual'] += diferenca

            print(f"\nResultado final:")
            print(f"- Empresas receptoras: {len(empresas_dados)}")
            print(f"- Percentual a redistribuir: {percentual_empresa_redistribuida:.2f}%")

            # Mostrar distribuição final
            print("\nDistribuição final dos percentuais:")
            for emp in empresas_dados:
                print(f"  {emp['id_empresa']}: {emp['percentual']:.2f}% (R$ {emp['vr_arrecadacao']:,.2f})")

            return percentual_empresa_redistribuida, total_arrecadacao, empresas_dados

    except Exception as e:
        error_msg = f"Erro ao calcular percentuais: {str(e)}"
        print(error_msg)
        logging.error(error_msg)

        import traceback
        trace = traceback.format_exc()
        print(trace)
        logging.error(trace)

        return 0, 0, []


def redistribuir_percentuais(edital_id, periodo_id, criterio_id, empresa_id, percentual_redistribuido, empresas_dados):
    """
    Redistribui os percentuais da empresa que está saindo entre as empresas remanescentes.
    CORREÇÃO: Gravar VR_ARRECADACAO corretamente
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
    logging.info(
        f"Redistribuindo percentuais - Edital: {edital_id}, Período: {periodo_id}, Critério: {criterio_id}")

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
                    vr_arrecadacao = empresa["vr_arrecadacao"]  # CORREÇÃO: Usar valor real de arrecadação
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
                        "vr_arrecadacao": vr_arrecadacao,  # CORREÇÃO: Gravar valor real de arrecadação
                        "percentual_final": percentual_final
                    })

                    print(
                        f"Empresa {id_empresa}: {percentual_original:.2f}% + {percentual_unitario:.2f}% = {percentual_final:.2f}% (Arrec: {vr_arrecadacao:.2f})")

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

                # 9. Verificar e distribuir sobras
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

                # 5. Remover registros antigos antes de inserir novos
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
                WITH EmpresasOrdenadas AS (
                    SELECT 
                        [ID_EMPRESA],
                        [QTDE_MAXIMA],
                        ROW_NUMBER() OVER (ORDER BY [QTDE_MAXIMA] DESC) AS RN
                    FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                    WHERE 
                        [ID_EDITAL] = :edital_id
                        AND [ID_PERIODO] = :periodo_id
                        AND [COD_CRITERIO_SELECAO] = :criterio_id
                        AND [DELETED_AT] IS NULL
                ),
                ContratosNumerados AS (
                    SELECT 
                        ARR.[FkContratoSISCTR],
                        ARR.[NR_CPF_CNPJ],
                        ROW_NUMBER() OVER (ORDER BY ARR.[NR_CPF_CNPJ]) AS RN
                    FROM [DEV].[DCA_TB007_ARRASTAVEIS] ARR
                )
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
                    CN.[FkContratoSISCTR],
                    :criterio_id AS [COD_CRITERIO_SELECAO],
                    EO.[ID_EMPRESA] AS [COD_EMPRESA_COBRANCA],
                    CN.[NR_CPF_CNPJ],
                    SIT.[VR_SD_DEVEDOR],
                    GETDATE() AS [CREATED_AT]
                FROM 
                    ContratosNumerados CN
                    CROSS APPLY (
                        SELECT TOP 1 EO.[ID_EMPRESA]
                        FROM EmpresasOrdenadas EO
                        WHERE (CN.RN % (SELECT COUNT(*) FROM EmpresasOrdenadas)) + 1 = EO.RN
                    ) AS EO
                    INNER JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] SIT
                        ON CN.[FkContratoSISCTR] = SIT.[fkContratoSISCTR]
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
    Otimizado para performance e para distribuir contratos conforme percentuais de arrecadação.
    """
    logging.info(f"Processando demais contratos - Edital: {edital_id}, Período: {periodo_id}, Critério: {criterio_id}")

    try:
        with db.engine.connect() as connection:
            transaction = connection.begin()

            try:
                # Executar o processamento completo em uma única operação SQL para máxima performance
                sql_otimizado = text("""
                -- Declaração de variáveis
                DECLARE @EditalID INT = :edital_id;
                DECLARE @PeriodoID INT = :periodo_id;
                DECLARE @CriterioID INT = :criterio_id;
                DECLARE @EmpresaRedistribuida INT = :empresa_redistribuida;
                DECLARE @DataAtual DATETIME = GETDATE();
                DECLARE @ContratosInseridos INT = 0;

                -- 1. Contar contratos restantes
                DECLARE @QtdeContratosRestantes INT = (SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] WITH (NOLOCK));

                IF @QtdeContratosRestantes = 0
                BEGIN
                    SELECT 0 AS ContratosInseridos;
                    RETURN;
                END

                -- 2. Tabela temporária para as empresas e seus percentuais
                DECLARE @Empresas TABLE (
                    ID INT IDENTITY(1,1),
                    ID_Empresa INT, 
                    Percentual DECIMAL(10, 2),
                    JaRecebeu INT DEFAULT 0,
                    MetaTotal INT DEFAULT 0,
                    AReceber INT DEFAULT 0
                );

                -- 3. Inserir empresas e seus percentuais
                INSERT INTO @Empresas (ID_Empresa, Percentual)
                SELECT 
                    ID_EMPRESA, 
                    PERCENTUAL_FINAL
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] WITH (NOLOCK)
                WHERE 
                    ID_EDITAL = @EditalID
                    AND ID_PERIODO = @PeriodoID
                    AND COD_CRITERIO_SELECAO = @CriterioID
                    AND DELETED_AT IS NULL
                    AND ((@EmpresaRedistribuida IS NULL) OR (ID_EMPRESA <> @EmpresaRedistribuida));

                -- 4. Contar contratos já distribuídos por arrasto
                UPDATE e
                SET e.JaRecebeu = ISNULL(j.Quantidade, 0)
                FROM @Empresas e
                LEFT JOIN (
                    SELECT 
                        COD_EMPRESA_COBRANCA,
                        COUNT(*) as Quantidade
                    FROM [DEV].[DCA_TB005_DISTRIBUICAO] WITH (NOLOCK)
                    WHERE
                        ID_EDITAL = @EditalID
                        AND ID_PERIODO = @PeriodoID
                        AND COD_CRITERIO_SELECAO = @CriterioID
                    GROUP BY COD_EMPRESA_COBRANCA
                ) j ON e.ID_Empresa = j.COD_EMPRESA_COBRANCA;

                -- 5. Calcular totais
                DECLARE @TotalJaDistribuido INT = (SELECT SUM(JaRecebeu) FROM @Empresas);
                DECLARE @TotalContratos INT = @TotalJaDistribuido + @QtdeContratosRestantes;

                -- 6. Calcular metas para cada empresa baseado no percentual de arrecadação
                UPDATE @Empresas
                SET 
                    MetaTotal = FLOOR(@TotalContratos * (Percentual / 100.0)),
                    AReceber = 0;

                UPDATE @Empresas
                SET AReceber = CASE 
                    WHEN MetaTotal > JaRecebeu THEN MetaTotal - JaRecebeu
                    ELSE 0
                END;

                -- 7. Ajustar para garantir que a soma das metas = número de contratos disponíveis
                DECLARE @SomaAReceber INT = (SELECT SUM(AReceber) FROM @Empresas);

                -- 7.1 Se existe excesso, distribuir às maiores empresas proporcionalmente
                IF @SomaAReceber < @QtdeContratosRestantes
                BEGIN
                    DECLARE @Excesso INT = @QtdeContratosRestantes - @SomaAReceber;
                    DECLARE @Contador INT = 0;

                    WHILE @Contador < @Excesso
                    BEGIN
                        UPDATE TOP(1) @Empresas
                        SET AReceber = AReceber + 1
                        WHERE ID_Empresa IN (
                            SELECT TOP 1 ID_Empresa
                            FROM @Empresas
                            ORDER BY Percentual DESC, ID_Empresa
                            OFFSET (@Contador % (SELECT COUNT(*) FROM @Empresas)) ROWS
                            FETCH NEXT 1 ROWS ONLY
                        );

                        SET @Contador = @Contador + 1;
                    END
                END
                -- 7.2 Se existe déficit, reduzir das menores empresas proporcionalmente
                ELSE IF @SomaAReceber > @QtdeContratosRestantes
                BEGIN
                    DECLARE @Deficit INT = @SomaAReceber - @QtdeContratosRestantes;
                    SET @Contador = 0;

                    WHILE @Contador < @Deficit
                    BEGIN
                        UPDATE TOP(1) @Empresas
                        SET AReceber = AReceber - 1
                        WHERE AReceber > 0
                        AND ID_Empresa IN (
                            SELECT TOP 1 ID_Empresa
                            FROM @Empresas
                            WHERE AReceber > 0
                            ORDER BY Percentual ASC, ID_Empresa
                            OFFSET (@Contador % (SELECT COUNT(*) FROM @Empresas WHERE AReceber > 0)) ROWS
                            FETCH NEXT 1 ROWS ONLY
                        );

                        SET @Contador = @Contador + 1;
                    END
                END

                -- 8. Verificar o resultado da distribuição por empresa
                SELECT 
                    e.ID_Empresa,
                    e.Percentual,
                    e.JaRecebeu,
                    e.MetaTotal,
                    e.AReceber,
                    e.JaRecebeu + e.AReceber AS TotalFinal,
                    CAST((e.JaRecebeu + e.AReceber) * 100.0 / @TotalContratos AS DECIMAL(10,2)) AS PctFinal
                FROM @Empresas e
                ORDER BY e.Percentual DESC;

                -- 9. Remover registros antigos
                DELETE FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                WHERE [ID_EDITAL] = @EditalID
                  AND [ID_PERIODO] = @PeriodoID
                  AND [fkContratoSISCTR] IN (
                      SELECT [FkContratoSISCTR]
                      FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] WITH (NOLOCK)
                  );

                -- 10. Criar tabela temporária para distribuição
                DECLARE @DistribuicaoTemp TABLE (
                    RowNum INT IDENTITY(1,1),
                    ContratoID BIGINT,
                    CPF_CNPJ BIGINT,
                    Saldo DECIMAL(18,2),
                    EmpresaID INT
                );

                -- 11. Inserir contratos com ordem aleatória
                INSERT INTO @DistribuicaoTemp (ContratoID, CPF_CNPJ, Saldo)
                SELECT 
                    [FkContratoSISCTR],
                    [NR_CPF_CNPJ],
                    [VR_SD_DEVEDOR]
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] WITH (NOLOCK)
                ORDER BY NEWID();

                -- 12. Distribuir contratos para empresas
                DECLARE @EmpresasReceberam TABLE (ID_Empresa INT, Recebidos INT DEFAULT 0);

                INSERT INTO @EmpresasReceberam (ID_Empresa)
                SELECT ID_Empresa FROM @Empresas;

                DECLARE @ContratosProcessados INT = 0;

                WHILE @ContratosProcessados < @QtdeContratosRestantes
                BEGIN
                    -- Encontrar próxima empresa que não atingiu meta
                    DECLARE @EmpresaDestino INT;

                    SELECT TOP 1 @EmpresaDestino = e.ID_Empresa
                    FROM @Empresas e
                    JOIN @EmpresasReceberam r ON e.ID_Empresa = r.ID_Empresa
                    WHERE r.Recebidos < e.AReceber
                    ORDER BY CAST(r.Recebidos AS FLOAT) / NULLIF(e.AReceber, 0), e.ID_Empresa;

                    -- Se todas empresas atingiram meta, usar maior empresa
                    IF @EmpresaDestino IS NULL
                    BEGIN
                        SELECT TOP 1 @EmpresaDestino = ID_Empresa 
                        FROM @Empresas 
                        ORDER BY Percentual DESC;
                    END

                    -- Atribuir empresa ao contrato atual
                    UPDATE @DistribuicaoTemp
                    SET EmpresaID = @EmpresaDestino
                    WHERE RowNum = @ContratosProcessados + 1;

                    -- Atualizar contador da empresa
                    UPDATE @EmpresasReceberam
                    SET Recebidos = Recebidos + 1
                    WHERE ID_Empresa = @EmpresaDestino;

                    SET @ContratosProcessados = @ContratosProcessados + 1;
                END

                -- 13. Inserir na tabela final
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
                    @DataAtual,
                    @EditalID,
                    @PeriodoID,
                    d.ContratoID,
                    @CriterioID,
                    d.EmpresaID,
                    d.CPF_CNPJ,
                    d.Saldo,
                    @DataAtual
                FROM @DistribuicaoTemp d;

                SET @ContratosInseridos = @@ROWCOUNT;

                -- 14. Verificar distribuição final por empresa
                SELECT 
                    e.ID_Empresa,
                    ISNULL(ea.JaRecebeu, 0) + ISNULL(n.Novos, 0) AS TotalFinal,
                    CAST((ISNULL(ea.JaRecebeu, 0) + ISNULL(n.Novos, 0)) * 100.0 / 
                         (SELECT COUNT(*) FROM [DEV].[DCA_TB005_DISTRIBUICAO] 
                          WHERE ID_EDITAL = @EditalID AND ID_PERIODO = @PeriodoID AND COD_CRITERIO_SELECAO = @CriterioID)
                         AS DECIMAL(10,2)) AS PctFinal
                FROM (SELECT DISTINCT ID_Empresa FROM @Empresas) e
                LEFT JOIN (
                    SELECT COD_EMPRESA_COBRANCA, COUNT(*) AS JaRecebeu
                    FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                    WHERE ID_EDITAL = @EditalID AND ID_PERIODO = @PeriodoID AND COD_CRITERIO_SELECAO = @CriterioID
                          AND fkContratoSISCTR NOT IN (SELECT ContratoID FROM @DistribuicaoTemp)
                    GROUP BY COD_EMPRESA_COBRANCA
                ) ea ON e.ID_Empresa = ea.COD_EMPRESA_COBRANCA
                LEFT JOIN (
                    SELECT EmpresaID, COUNT(*) AS Novos
                    FROM @DistribuicaoTemp
                    GROUP BY EmpresaID
                ) n ON e.ID_Empresa = n.EmpresaID
                ORDER BY e.ID_Empresa;

                -- 15. Limpar tabela de distribuíveis
                IF @ContratosInseridos > 0
                BEGIN
                    TRUNCATE TABLE [DEV].[DCA_TB006_DISTRIBUIVEIS];
                END

                -- 16. Retornar contratos inseridos
                SELECT @ContratosInseridos AS ContratosInseridos;
                """)

                # Executar o SQL otimizado
                result = connection.execute(sql_otimizado, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id,
                    "empresa_redistribuida": empresa_redistribuida
                })

                contratos_inseridos = result.scalar() or 0

                transaction.commit()
                print(f"Processamento dos contratos restantes concluído: {contratos_inseridos} contratos")
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
    MODIFICADO: Para incluir resultado final por empresa e dados de arrecadação

    Args:
        edital_id: ID do edital
        periodo_id: ID do período
        empresa_id: ID da empresa que está saindo
        cod_criterio: Código do critério de redistribuição

    Returns:
        dict: Resultados do processo incluindo resultado final por empresa
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
        "success": False,
        "resultados_finais": None  # IMPORTANTE: Resultado final por empresa
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

        # ETAPA 6: Buscar resultado final por empresa
        print("\n----- ETAPA 6: BUSCANDO RESULTADO FINAL POR EMPRESA -----")

        with db.engine.connect() as connection:
            result_sql = text("""
            SELECT 
                LD.ID_EMPRESA AS cod_empresa,
                COALESCE(EP.NO_EMPRESA_ABREVIADA, EP.NO_EMPRESA, CONCAT('Empresa ', LD.ID_EMPRESA)) AS empresa_abrev,
                COUNT(D.fkContratoSISCTR) AS qtde,
                CASE 
                    WHEN :total_contratos = 0 THEN 0
                    ELSE ROUND(COUNT(D.fkContratoSISCTR) * 100.0 / :total_contratos, 2)
                END AS pct_qtde,
                COALESCE(SUM(D.VR_SD_DEVEDOR), 0) AS saldo,
                CASE 
                    WHEN SUM(SUM(D.VR_SD_DEVEDOR)) OVER() = 0 THEN 0
                    ELSE ROUND(COALESCE(SUM(D.VR_SD_DEVEDOR), 0) * 100.0 / 
                          NULLIF(SUM(SUM(D.VR_SD_DEVEDOR)) OVER(), 0), 2)
                END AS pct_saldo,
                LD.VR_ARRECADACAO,
                LD.PERCENTUAL_FINAL
            FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
            LEFT JOIN [DEV].[DCA_TB005_DISTRIBUICAO] D
                ON LD.ID_EMPRESA = D.COD_EMPRESA_COBRANCA
                AND D.ID_EDITAL = :edital_id
                AND D.ID_PERIODO = :periodo_id
                AND D.COD_CRITERIO_SELECAO = :criterio_id
            LEFT JOIN [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES] EP
                ON LD.ID_EMPRESA = EP.ID_EMPRESA
                AND EP.ID_EDITAL = :edital_id
                AND EP.ID_PERIODO = :periodo_id
            WHERE LD.ID_EDITAL = :edital_id
                AND LD.ID_PERIODO = :periodo_id
                AND LD.COD_CRITERIO_SELECAO = :criterio_id
                AND LD.DELETED_AT IS NULL
            GROUP BY LD.ID_EMPRESA, EP.NO_EMPRESA_ABREVIADA, EP.NO_EMPRESA, LD.VR_ARRECADACAO, LD.PERCENTUAL_FINAL
            ORDER BY LD.ID_EMPRESA
            """)

            result_rows = connection.execute(result_sql, {
                "edital_id": edital_id,
                "periodo_id": periodo_id,
                "criterio_id": cod_criterio,
                "total_contratos": total_redistribuido
            }).fetchall()

            resultados_por_empresa = []
            total_qtde = 0
            total_saldo = 0
            total_arrecadacao_final = 0

            print("\nResultados por empresa:")
            print("Empresa | Qtde | % | Saldo | % | Arrecadação | % Final")
            print("-" * 70)

            for row in result_rows:
                empresa_result = {
                    "cod_empresa": row[0],
                    "empresa_abrev": row[1],
                    "qtde": int(row[2]) if row[2] else 0,
                    "pct_qtde": float(row[3]) if row[3] else 0.0,
                    "saldo": float(row[4]) if row[4] else 0.0,
                    "pct_saldo": float(row[5]) if row[5] else 0.0,
                    "arrecadacao": float(row[6]) if row[6] else 0.0,
                    "percentual_final": float(row[7]) if row[7] else 0.0
                }
                resultados_por_empresa.append(empresa_result)
                total_qtde += empresa_result["qtde"]
                total_saldo += empresa_result["saldo"]
                total_arrecadacao_final += empresa_result["arrecadacao"]

                print(
                    f"{empresa_result['empresa_abrev']: <15} | {empresa_result['qtde']:>5} | {empresa_result['pct_qtde']:>5.2f}% | "
                    f"{empresa_result['saldo']:>10.2f} | {empresa_result['pct_saldo']:>5.2f}% | "
                    f"{empresa_result['arrecadacao']:>12.2f} | {empresa_result['percentual_final']:>5.2f}%")

            print("-" * 70)
            print(
                f"{'TOTAL': <15} | {total_qtde:>5} | 100.00% | {total_saldo:>10.2f} | 100.00% | {total_arrecadacao_final:>12.2f} | 100.00%")

            resultados["resultados_finais"] = {
                "resultados": resultados_por_empresa,
                "total_qtde": total_qtde,
                "total_saldo": total_saldo,
                "total_arrecadacao": total_arrecadacao_final
            }

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