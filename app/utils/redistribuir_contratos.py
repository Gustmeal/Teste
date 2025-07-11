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
    CORRIGIDO: Trunca os percentuais e ajusta para garantir soma 100,00%
    """
    import logging
    import sys
    import math  # Adicionado para truncamento

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
            # Função para truncar em duas casas decimais
            def truncar_duas_casas(valor):
                # Multiplica por 100, trunca e divide por 100
                return math.trunc(valor * 100) / 100

            # 1. Buscar o período para obter datas
            periodo_sql = text("""
                SELECT DT_INICIO, DT_FIM 
                FROM [BDG].[DCA_TB001_PERIODO_AVALIACAO]
                WHERE ID_EDITAL = :edital_id 
                  AND ID_PERIODO = :periodo_id
                  AND DELETED_AT IS NULL
            """)

            periodo_result = connection.execute(periodo_sql, {
                "edital_id": edital_id,
                "periodo_id": periodo_id
            }).fetchone()

            if not periodo_result:
                print("ERRO: Período não encontrado")
                return 0, 0, []

            dt_inicio, dt_fim = periodo_result
            print(f"Período para cálculo: {dt_inicio} a {dt_fim}")

            # 2. Buscar todas as empresas relevantes (NOVA, PERMANECE e a selecionada)
            empresas_sql = text("""
            SELECT 
                EP.ID_EMPRESA,
                EP.NO_EMPRESA,
                EP.NO_EMPRESA_ABREVIADA,
                EP.DS_CONDICAO
            FROM [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES] EP
            WHERE EP.ID_EDITAL = :edital_id
              AND EP.ID_PERIODO = :periodo_id
              AND (EP.DS_CONDICAO IN ('NOVA', 'PERMANECE') OR EP.ID_EMPRESA = :empresa_id)
              AND EP.DELETED_AT IS NULL
            """)

            empresas_result = connection.execute(empresas_sql, {
                "edital_id": edital_id,
                "periodo_id": periodo_id,
                "empresa_id": empresa_id
            }).fetchall()

            if not empresas_result:
                print("ERRO: Nenhuma empresa encontrada")
                return 0, 0, []

            # Criar lista de IDs de empresas relevantes
            empresas_ids = [row[0] for row in empresas_result]
            empresas_ids_str = ','.join(str(id) for id in empresas_ids)

            # 3. Buscar arrecadação das empresas relevantes
            arrecadacao_sql = text(f"""
            SELECT 
                REE.CO_EMPRESA_COBRANCA,
                SUM(REE.VR_ARRECADACAO_TOTAL) AS VR_ARRECADACAO_TOTAL
            FROM [BDG].[COM_TB062_REMUNERACAO_ESTIMADA] REE
            WHERE REE.DT_ARRECADACAO BETWEEN :dt_inicio AND :dt_fim
              AND REE.CO_EMPRESA_COBRANCA IN ({empresas_ids_str})
            GROUP BY REE.CO_EMPRESA_COBRANCA
            """)

            arrecadacao_result = connection.execute(arrecadacao_sql, {
                "dt_inicio": dt_inicio,
                "dt_fim": dt_fim
            }).fetchall()

            # 4. Calcular arrecadação total e processar dados
            total_arrecadacao = 0
            arrecadacao_por_empresa = {}

            for row in arrecadacao_result:
                empresa_id_arr, vr_arrecadacao = row
                vr_arrecadacao = float(vr_arrecadacao) if vr_arrecadacao else 0.0
                arrecadacao_por_empresa[empresa_id_arr] = vr_arrecadacao
                total_arrecadacao += vr_arrecadacao

            # 5. Processar dados das empresas
            todas_empresas = []
            empresa_saindo = None
            empresas_receptoras = []

            for row in empresas_result:
                id_empresa, nome, nome_abrev, ds_condicao = row
                vr_arrecadacao = arrecadacao_por_empresa.get(id_empresa, 0.0)

                # Truncar o percentual em vez de arredondar
                percentual = truncar_duas_casas(
                    (vr_arrecadacao / total_arrecadacao * 100)) if total_arrecadacao > 0 else 0

                empresa_info = {
                    "id_empresa": id_empresa,
                    "nome": nome,
                    "nome_abreviado": nome_abrev or nome,
                    "ds_condicao": ds_condicao,
                    "vr_arrecadacao": vr_arrecadacao,
                    "percentual": percentual
                }

                todas_empresas.append(empresa_info)

                # Separar empresa que sai e receptoras
                if id_empresa == empresa_id:
                    empresa_saindo = empresa_info
                elif ds_condicao in ('NOVA', 'PERMANECE'):
                    empresas_receptoras.append(empresa_info)

            if not empresa_saindo:
                print(f"ERRO: Empresa específica {empresa_id} não encontrada")
                return 0, 0, []

            # 6. Calcular percentual da empresa que sai
            percentual_empresa_saindo = empresa_saindo["percentual"]

            print(f"\n----- DADOS DA EMPRESA QUE ESTÁ SAINDO -----")
            print(f"Empresa: {empresa_saindo['id_empresa']} ({empresa_saindo['nome_abreviado']})")
            print(f"Arrecadação: R$ {empresa_saindo['vr_arrecadacao']:,.2f}")
            print(f"Arrecadação total relevante: R$ {total_arrecadacao:,.2f}")
            print(f"Percentual da empresa: {percentual_empresa_saindo:.2f}%")

            # 7. Calcular o adicional a ser distribuído para cada empresa receptora
            qtde_receptoras = len(empresas_receptoras)
            if qtde_receptoras > 0 and percentual_empresa_saindo > 0:
                # MODIFICADO: Truncar o percentual adicional em duas casas decimais
                percentual_adicional = truncar_duas_casas(percentual_empresa_saindo / qtde_receptoras)
            else:
                percentual_adicional = 0

            # 8. Aplicar o percentual adicional a cada empresa receptora
            for empresa in empresas_receptoras:
                empresa["percentual_original"] = empresa["percentual"]
                empresa["adicional_redistribuido"] = percentual_adicional
                # MODIFICADO: Truncar o percentual final em duas casas decimais
                empresa["percentual_final"] = truncar_duas_casas(empresa["percentual"] + percentual_adicional)

            # 9. AJUSTE MELHORADO: Distribuir 0,01% até completar 100%
            soma_percentuais_finais = sum(e["percentual_final"] for e in empresas_receptoras)
            diferenca_pontos = int(round((100.0 - soma_percentuais_finais) * 100))  # Diferença em pontos de 0,01%

            print(f"\nSoma dos percentuais após truncamento: {soma_percentuais_finais:.2f}%")
            print(f"Diferença para 100%: {100.0 - soma_percentuais_finais:.2f}% ({diferenca_pontos} pontos de 0,01%)")

            if diferenca_pontos > 0:
                # Ordenar empresas por arrecadação (do maior para o menor)
                empresas_ordenadas = sorted(empresas_receptoras, key=lambda x: x["vr_arrecadacao"], reverse=True)

                # Loop para adicionar 0,01% até completar 100%
                ajustes_por_empresa = {emp["id_empresa"]: 0 for emp in empresas_ordenadas}

                print("\nAjustando percentuais:")

                while diferenca_pontos > 0:
                    # Adicionar 0,01% para cada empresa, considerando distribuição justa
                    empresas_para_ajustar = sorted(empresas_ordenadas,
                                                   key=lambda x: (
                                                   ajustes_por_empresa[x["id_empresa"]], -x["vr_arrecadacao"]))

                    for empresa in empresas_para_ajustar:
                        if diferenca_pontos <= 0:
                            break

                        empresa["percentual_final"] += 0.01
                        ajustes_por_empresa[empresa["id_empresa"]] += 1
                        diferenca_pontos -= 1
                        print(f"  Adicionando 0,01% à empresa {empresa['id_empresa']} - "
                              f"Total: {empresa['percentual_final']:.2f}% (Ajuste #{ajustes_por_empresa[empresa['id_empresa']]})")

            # 10. Verificação final
            soma_final = sum(e["percentual_final"] for e in empresas_receptoras)

            # Exibir resultados para verificação
            print("\n----- DISTRIBUIÇÃO DE PERCENTUAIS -----")
            print(f"Percentual da empresa saindo: {percentual_empresa_saindo:.2f}%")
            print(f"Percentual adicional por empresa (truncado): {percentual_adicional:.2f}%")
            print(f"Quantidade de empresas receptoras: {qtde_receptoras}")

            print("\nEmpresas receptoras:")
            for empresa in empresas_receptoras:
                print(f"{empresa['id_empresa']} ({empresa['nome_abreviado']}): "
                      f"{empresa['percentual_original']:.2f}% + {empresa['adicional_redistribuido']:.2f}% = "
                      f"{empresa['percentual_final']:.2f}%")

            print(f"\nSoma final dos percentuais: {soma_final:.2f}%")

            # Se a soma não for exatamente 100%, há um problema
            if abs(soma_final - 100.0) > 0.01:
                print("ALERTA: Soma dos percentuais finais ainda não é 100%!")

            return percentual_empresa_saindo, total_arrecadacao, empresas_receptoras

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
    CORRIGIDO: Truncamento e distribuição rigorosa para garantir 100%
    """
    # Configuração básica de logging
    import logging
    import sys
    import math  # Para truncamento
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
                # Função para truncar em duas casas decimais
                def truncar_duas_casas(valor):
                    return math.trunc(valor * 100) / 100

                # 1. Verificar quantidade de empresas remanescentes
                qtde_empresas_remanescentes = len(empresas_dados)

                if qtde_empresas_remanescentes == 0:
                    error_msg = "Não há empresas remanescentes para redistribuição"
                    print(error_msg)
                    logging.error(error_msg)
                    return False

                print(f"Empresas remanescentes: {qtde_empresas_remanescentes}")

                # 2. ETAPA 1: Truncar percentual da empresa que sai em duas casas
                percentual_redistribuido_truncado = truncar_duas_casas(percentual_redistribuido)
                print(f"Percentual original a redistribuir: {percentual_redistribuido:.4f}%")
                print(f"Percentual truncado a redistribuir: {percentual_redistribuido_truncado:.2f}%")

                # Calcular percentual unitário (truncado)
                percentual_unitario = truncar_duas_casas(
                    percentual_redistribuido_truncado / qtde_empresas_remanescentes)
                print(f"Percentual unitário por empresa (truncado): {percentual_unitario:.2f}%")

                # 3. Data de referência para os registros
                data_apuracao = datetime.now()

                # 4. Remover apenas registros do critério específico
                delete_sql = text("""
                DELETE FROM [BDG].[DCA_TB003_LIMITES_DISTRIBUICAO]
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

                # 5. ETAPA 2: Aplicar o percentual unitário
                print("\n----- REDISTRIBUIÇÃO DO PERCENTUAL -----")
                for empresa in empresas_dados:
                    percentual_original = empresa.get("percentual_original", empresa["percentual"])
                    percentual_somado = percentual_original + percentual_unitario
                    empresa["percentual_final"] = percentual_somado
                    print(
                        f"Empresa {empresa['id_empresa']}: {percentual_original:.2f}% + {percentual_unitario:.2f}% = {percentual_somado:.2f}%")

                # 6. ETAPA 3: Verificar soma e ajustar rigorosamente
                soma_percentuais = sum(empresa["percentual_final"] for empresa in empresas_dados)
                print(f"\n----- VERIFICAÇÃO DA SOMA -----")
                print(f"Soma após redistribuição: {soma_percentuais:.4f}%")

                # Calcular diferença em pontos de 0,01% (mais preciso)
                diferenca_centavos = round((100.0 - soma_percentuais) * 100)
                print(f"Diferença para 100%: {diferenca_centavos} centavos de %")

                # 7. ETAPA 4: Ajustar distribuindo 0,01% conforme regras
                if diferenca_centavos != 0:
                    print(f"\n----- AJUSTE PARA 100% -----")

                    # Ordenar empresas por arrecadação (ordem decrescente)
                    empresas_ordenadas = sorted(empresas_dados, key=lambda x: x["vr_arrecadacao"], reverse=True)

                    # Contador de quantos ajustes cada empresa recebeu
                    ajustes_por_empresa = {}
                    for emp in empresas_dados:
                        ajustes_por_empresa[emp["id_empresa"]] = 0

                    print("Iniciando ajustes de 0,01%:")

                    # Loop para distribuir os centavos
                    while diferenca_centavos != 0:
                        # Determinar se vamos adicionar ou subtrair
                        incremento = 0.01 if diferenca_centavos > 0 else -0.01

                        # Encontrar a empresa com menor número de ajustes
                        # Entre as que têm o mesmo número de ajustes, escolher a com maior arrecadação
                        min_ajustes = min(ajustes_por_empresa.values())
                        empresas_candidatas = [emp for emp in empresas_ordenadas
                                               if ajustes_por_empresa[emp["id_empresa"]] == min_ajustes]

                        # Se estamos subtraindo, inverter ordem (menor arrecadação primeiro)
                        if incremento < 0:
                            empresas_candidatas.reverse()

                        # Aplicar ajuste na primeira empresa candidata
                        empresa_escolhida = empresas_candidatas[0]
                        empresa_escolhida["percentual_final"] = round(
                            empresa_escolhida["percentual_final"] + incremento, 2)
                        ajustes_por_empresa[empresa_escolhida["id_empresa"]] += 1

                        # Ajustar diferença
                        diferenca_centavos -= (1 if incremento > 0 else -1)

                        print(
                            f"  {'Adicionando' if incremento > 0 else 'Subtraindo'} 0,01% à empresa {empresa_escolhida['id_empresa']} "
                            f"(ajuste #{ajustes_por_empresa[empresa_escolhida['id_empresa']]}) - "
                            f"Novo: {empresa_escolhida['percentual_final']:.2f}%")

                    # Verificação final rigorosa
                    soma_final = sum(empresa["percentual_final"] for empresa in empresas_dados)
                    print(f"Soma final após ajustes: {soma_final:.2f}%")

                    # Garantir que é exatamente 100.00
                    if round(soma_final, 2) != 100.00:
                        raise ValueError(f"ERRO CRÍTICO: Soma final {soma_final:.2f}% não é 100.00%!")
                else:
                    print("Soma já está em 100,00% - nenhum ajuste necessário")

                # 8. Inserir novos percentuais
                print("\n----- INSERINDO NOVOS PERCENTUAIS -----")

                for empresa in empresas_dados:
                    id_empresa = empresa["id_empresa"]
                    vr_arrecadacao = empresa["vr_arrecadacao"]
                    percentual_original = empresa.get("percentual_original", empresa["percentual"])
                    percentual_final = empresa["percentual_final"]

                    insert_sql = text("""
                    INSERT INTO [BDG].[DCA_TB003_LIMITES_DISTRIBUICAO]
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
                        f"Empresa {id_empresa}: {percentual_original:.2f}% + {percentual_unitario:.2f}% = {percentual_final:.2f}% (Arrec: {vr_arrecadacao:.2f})")

                # 9. Contar contratos e atualizar quantidades
                count_sql = text("""
                SELECT 
                    COUNT(*) AS QTDE_CONTRATOS,
                    COALESCE(SUM(VR_SD_DEVEDOR), 0) AS VALOR_TOTAL
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                """)

                count_result = connection.execute(count_sql).fetchone()
                qtde_registros = count_result[0] if count_result else 0
                valor_total = float(count_result[1]) if count_result and count_result[1] else 0.0

                # 10. Atualizar quantidades e valores máximos
                update_sql = text("""
                UPDATE [BDG].[DCA_TB003_LIMITES_DISTRIBUICAO]
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

                # 11. Distribuir sobras de contratos
                sobra_sql = text("""
                DECLARE @SOBRA INT;
                SET @SOBRA = :qtde_registros - (
                    SELECT 
                        SUM(LD.[QTDE_MAXIMA]) 
                    FROM 
                        [BDG].[DCA_TB003_LIMITES_DISTRIBUICAO] AS LD
                    WHERE
                        LD.[ID_EDITAL] = :edital_id
                        AND LD.[ID_PERIODO] = :periodo_id
                        AND LD.[COD_CRITERIO_SELECAO] = :criterio_id
                );

                IF @SOBRA > 0
                BEGIN
                    WITH RankedEmpresas AS (
                        SELECT 
                            LD.[ID],
                            LD.[QTDE_MAXIMA],
                            ROW_NUMBER() OVER (ORDER BY LD.[QTDE_MAXIMA] DESC) AS RN
                        FROM 
                            [BDG].[DCA_TB003_LIMITES_DISTRIBUICAO] AS LD
                        WHERE
                            LD.[ID_EDITAL] = :edital_id
                            AND LD.[ID_PERIODO] = :periodo_id
                            AND LD.[COD_CRITERIO_SELECAO] = :criterio_id
                    )
                    UPDATE LIM
                    SET LIM.[QTDE_MAXIMA] = LIM.[QTDE_MAXIMA] + 1
                    FROM [BDG].[DCA_TB003_LIMITES_DISTRIBUICAO] AS LIM
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

                # 12. Verificação final
                check_sql = text("""
                SELECT 
                    ID_EMPRESA,
                    PERCENTUAL_FINAL,
                    QTDE_MAXIMA
                FROM [BDG].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE
                    [ID_EDITAL] = :edital_id
                    AND [ID_PERIODO] = :periodo_id
                    AND [COD_CRITERIO_SELECAO] = :criterio_id
                ORDER BY PERCENTUAL_FINAL DESC
                """)

                check_result = connection.execute(check_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                }).fetchall()

                print("\n----- LIMITES FINAIS POR EMPRESA -----")
                total_percentual = 0
                total_qtde = 0

                for row in check_result:
                    id_empresa, percentual, qtde = row
                    total_percentual += float(percentual) if percentual else 0
                    total_qtde += int(qtde) if qtde else 0
                    print(f"Empresa {id_empresa}: {float(percentual):.2f}%, {int(qtde)} contratos")

                print(f"TOTAL: {total_percentual:.2f}%, {total_qtde} contratos")

                # Verificação final crítica
                if round(total_percentual, 2) != 100.00:
                    raise ValueError(f"ERRO FINAL: Total de percentuais {total_percentual:.2f}% não é 100.00%!")

                transaction.commit()
                print("\nRedistribuição de percentuais concluída com sucesso!")
                logging.info("Redistribuição de percentuais concluída com sucesso")

                return True

            except Exception as e:
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
    MODIFICADO: Distribuição proporcional conforme percentuais de arrecadação
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
                # CORREÇÃO: Remover a coluna VR_SD_DEVEDOR que não existe na tabela
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

                # 5. Buscar informações de limites e quantidades já distribuídas por empresa
                empresas_info_sql = text("""
                SELECT 
                    LD.ID_EMPRESA,
                    LD.PERCENTUAL_FINAL,
                    LD.QTDE_MAXIMA,
                    COALESCE((
                        SELECT COUNT(*) 
                        FROM [DEV].[DCA_TB005_DISTRIBUICAO] D 
                        WHERE D.COD_EMPRESA_COBRANCA = LD.ID_EMPRESA
                          AND D.ID_EDITAL = :edital_id
                          AND D.ID_PERIODO = :periodo_id
                          AND D.COD_CRITERIO_SELECAO = :criterio_id
                    ), 0) AS QTDE_ATUAL,
                    LD.VR_ARRECADACAO
                FROM [BDG].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
                WHERE LD.ID_EDITAL = :edital_id
                  AND LD.ID_PERIODO = :periodo_id
                  AND LD.COD_CRITERIO_SELECAO = :criterio_id
                  AND LD.DELETED_AT IS NULL
                ORDER BY LD.PERCENTUAL_FINAL DESC
                """)

                empresas_info_result = connection.execute(empresas_info_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                }).fetchall()

                # Preparar estrutura de dados para empresas
                empresas = []
                for row in empresas_info_result:
                    id_empresa, percentual, qtde_maxima, qtde_atual, vr_arrecadacao = row
                    empresas.append({
                        "id_empresa": id_empresa,
                        "percentual": float(percentual) if percentual else 0.0,
                        "qtde_maxima": int(qtde_maxima) if qtde_maxima else 0,
                        "qtde_atual": int(qtde_atual) if qtde_atual else 0,
                        "vr_arrecadacao": float(vr_arrecadacao) if vr_arrecadacao else 0.0,
                        "qtde_disponivel": (int(qtde_maxima) if qtde_maxima else 0) - (
                            int(qtde_atual) if qtde_atual else 0),
                        "cpfs_atribuidos": set()  # Conjunto para controlar CPFs já atribuídos
                    })

                # Total disponível entre todas as empresas
                total_disponivel = sum(emp["qtde_disponivel"] for emp in empresas)
                if total_disponivel < qtde_arrastaveis:
                    print(
                        f"ALERTA: Capacidade disponível ({total_disponivel}) menor que contratos arrastáveis ({qtde_arrastaveis})")
                    # Ajustar proporcionalmente
                    for emp in empresas:
                        if total_disponivel > 0:
                            emp["qtde_disponivel_ajustada"] = round(
                                emp["qtde_disponivel"] * qtde_arrastaveis / total_disponivel)
                        else:
                            emp["qtde_disponivel_ajustada"] = round(emp["percentual"] * qtde_arrastaveis / 100)
                else:
                    for emp in empresas:
                        emp["qtde_disponivel_ajustada"] = emp["qtde_disponivel"]

                # 6. Agrupar contratos por CPF/CNPJ - CORRIGIDA
                cpf_group_sql = text("""
                SELECT 
                    ARR.NR_CPF_CNPJ,
                    COUNT(*) AS NUM_CONTRATOS
                FROM [DEV].[DCA_TB007_ARRASTAVEIS] ARR
                GROUP BY ARR.NR_CPF_CNPJ
                ORDER BY COUNT(*) DESC
                """)

                cpf_groups = connection.execute(cpf_group_sql).fetchall()

                # 7. Distribuir CPFs por empresa
                cpfs_distribuidos = {}  # Dicionário para mapear CPF -> empresa_id

                # Ordenar empresas pelo percentual (do maior para o menor)
                empresas_ordenadas = sorted(empresas, key=lambda x: x["percentual"], reverse=True)

                # Ordenar CPFs pelo número de contratos (do maior para o menor)
                cpf_groups_sorted = sorted(cpf_groups, key=lambda x: x[1], reverse=True)

                # Distribuir CPFs para as empresas proporcionalmente
                for cpf_info in cpf_groups_sorted:
                    cpf = cpf_info[0]
                    num_contratos = cpf_info[1]

                    # Encontrar empresa com menor % de uso da capacidade
                    empresa_escolhida = None
                    menor_uso_percentual = float('inf')

                    for emp in empresas_ordenadas:
                        if emp["qtde_disponivel_ajustada"] <= 0:
                            continue

                        # Calcular % de uso (contratos atuais / quantidade máxima)
                        if emp["qtde_maxima"] > 0:
                            uso_percentual = emp["qtde_atual"] / emp["qtde_maxima"]
                        else:
                            uso_percentual = 1.0

                        if uso_percentual < menor_uso_percentual:
                            menor_uso_percentual = uso_percentual
                            empresa_escolhida = emp

                    # Se não encontrou empresa disponível, usar a maior
                    if not empresa_escolhida and empresas_ordenadas:
                        empresa_escolhida = empresas_ordenadas[0]

                    # Atribuir CPF à empresa escolhida
                    if empresa_escolhida:
                        cpfs_distribuidos[cpf] = empresa_escolhida["id_empresa"]
                        empresa_escolhida["qtde_atual"] += num_contratos
                        empresa_escolhida["qtde_disponivel_ajustada"] -= num_contratos
                        empresa_escolhida["cpfs_atribuidos"].add(cpf)

                # 8. Inserir na tabela de distribuição
                print("\nDistribuição por empresa:")
                for emp in empresas:
                    print(
                        f"Empresa {emp['id_empresa']}: {len(emp['cpfs_atribuidos'])} CPFs, estimativa de {emp['qtde_atual']} contratos")

                # 9. Excluir registros antigos antes de inserir
                delete_existing_sql = text("""
                DELETE FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                WHERE [ID_EDITAL] = :edital_id
                  AND [ID_PERIODO] = :periodo_id
                  AND [fkContratoSISCTR] IN (
                      SELECT [FkContratoSISCTR]
                      FROM [DEV].[DCA_TB007_ARRASTAVEIS]
                  )
                """)

                connection.execute(delete_existing_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id
                })

                # 10. Inserir na tabela final
                contratos_inseridos = 0
                for cpf, empresa_id in cpfs_distribuidos.items():
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
                        ARR.[FkContratoSISCTR],
                        :criterio_id AS [COD_CRITERIO_SELECAO],
                        :empresa_id AS [COD_EMPRESA_COBRANCA],
                        ARR.[NR_CPF_CNPJ],
                        SIT.[VR_SD_DEVEDOR],
                        GETDATE() AS [CREATED_AT]
                    FROM 
                        [DEV].[DCA_TB007_ARRASTAVEIS] ARR
                        INNER JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] SIT
                            ON ARR.[FkContratoSISCTR] = SIT.[fkContratoSISCTR]
                    WHERE
                        ARR.[NR_CPF_CNPJ] = :cpf
                    """)

                    result = connection.execute(insert_sql, {
                        "edital_id": edital_id,
                        "periodo_id": periodo_id,
                        "criterio_id": criterio_id,
                        "empresa_id": empresa_id,
                        "cpf": cpf
                    })

                    contratos_inseridos += result.rowcount

                print(f"Contratos arrastáveis inseridos na tabela de distribuição: {contratos_inseridos}")

                # 11. Verificar resultados finais
                results_sql = text("""
                SELECT 
                    COD_EMPRESA_COBRANCA,
                    COUNT(*) AS QTDE,
                    COUNT(DISTINCT NR_CPF_CNPJ) AS QTDE_CPFS
                FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                WHERE [ID_EDITAL] = :edital_id
                  AND [ID_PERIODO] = :periodo_id
                  AND [COD_CRITERIO_SELECAO] = :criterio_id
                  AND [fkContratoSISCTR] IN (
                      SELECT [FkContratoSISCTR]
                      FROM [DEV].[DCA_TB007_ARRASTAVEIS]
                  )
                GROUP BY COD_EMPRESA_COBRANCA
                ORDER BY COD_EMPRESA_COBRANCA
                """)

                check_results = connection.execute(results_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                }).fetchall()

                print("\nResultados finais por empresa:")
                for row in check_results:
                    empresa_id, qtde, qtde_cpfs = row
                    print(f"Empresa {empresa_id}: {qtde} contratos, {qtde_cpfs} CPFs")

                transaction.commit()
                print("Processamento de contratos arrastáveis concluído com sucesso!")

                return contratos_inseridos, True

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
    MODIFICADO: Distribuição proporcional conforme percentuais de arrecadação
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

                # 2. Buscar informações de limites e quantidades já distribuídas por empresa
                empresas_info_sql = text("""
                SELECT 
                    LD.ID_EMPRESA,
                    LD.PERCENTUAL_FINAL,
                    LD.QTDE_MAXIMA,
                    COALESCE((
                        SELECT COUNT(*) 
                        FROM [DEV].[DCA_TB005_DISTRIBUICAO] D 
                        WHERE D.COD_EMPRESA_COBRANCA = LD.ID_EMPRESA
                          AND D.ID_EDITAL = :edital_id
                          AND D.ID_PERIODO = :periodo_id
                          AND D.COD_CRITERIO_SELECAO = :criterio_id
                    ), 0) AS QTDE_ATUAL,
                    LD.VR_ARRECADACAO
                FROM [BDG].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
                WHERE LD.ID_EDITAL = :edital_id
                  AND LD.ID_PERIODO = :periodo_id
                  AND LD.COD_CRITERIO_SELECAO = :criterio_id
                  AND LD.DELETED_AT IS NULL
                  AND (:empresa_redistribuida IS NULL OR LD.ID_EMPRESA <> :empresa_redistribuida)
                ORDER BY LD.PERCENTUAL_FINAL DESC
                """)

                empresas_info_result = connection.execute(empresas_info_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id,
                    "empresa_redistribuida": empresa_redistribuida
                }).fetchall()

                # Preparar estrutura de dados para empresas
                empresas = []
                for row in empresas_info_result:
                    id_empresa, percentual, qtde_maxima, qtde_atual, vr_arrecadacao = row
                    empresas.append({
                        "id_empresa": id_empresa,
                        "percentual": float(percentual) if percentual else 0.0,
                        "qtde_maxima": int(qtde_maxima) if qtde_maxima else 0,
                        "qtde_atual": int(qtde_atual) if qtde_atual else 0,
                        "vr_arrecadacao": float(vr_arrecadacao) if vr_arrecadacao else 0.0,
                        "qtde_disponivel": (int(qtde_maxima) if qtde_maxima else 0) - (
                            int(qtde_atual) if qtde_atual else 0)
                    })

                # Total disponível entre todas as empresas
                total_disponivel = sum(emp["qtde_disponivel"] for emp in empresas)
                if total_disponivel < qtde_contratos_restantes:
                    print(
                        f"ALERTA: Capacidade disponível ({total_disponivel}) menor que contratos restantes ({qtde_contratos_restantes})")
                    # Ajustar proporcionalmente
                    for emp in empresas:
                        if total_disponivel > 0:
                            emp["qtde_disponivel_ajustada"] = round(
                                emp["qtde_disponivel"] * qtde_contratos_restantes / total_disponivel)
                        else:
                            emp["qtde_disponivel_ajustada"] = round(emp["percentual"] * qtde_contratos_restantes / 100)
                else:
                    for emp in empresas:
                        emp["qtde_disponivel_ajustada"] = emp["qtde_disponivel"]

                # 3. Imprimir informações para debug
                print("\nLimites por empresa:")
                for emp in empresas:
                    print(f"Empresa {emp['id_empresa']}: {emp['percentual']:.2f}%, "
                          f"Máx: {emp['qtde_maxima']}, Atual: {emp['qtde_atual']}, "
                          f"Disponível: {emp['qtde_disponivel']}, Ajustado: {emp['qtde_disponivel_ajustada']}")

                # 4. Excluir registros antigos antes de inserir
                delete_existing_sql = text("""
                DELETE FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                WHERE [ID_EDITAL] = :edital_id
                  AND [ID_PERIODO] = :periodo_id
                  AND [fkContratoSISCTR] IN (
                      SELECT [FkContratoSISCTR]
                      FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                  )
                """)

                connection.execute(delete_existing_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id
                })

                # 5. Buscar contratos a distribuir
                contratos_sql = text("""
                SELECT 
                    [FkContratoSISCTR],
                    [NR_CPF_CNPJ],
                    [VR_SD_DEVEDOR]
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                ORDER BY NEWID()  -- Ordenar aleatoriamente
                """)

                contratos = connection.execute(contratos_sql).fetchall()

                # 6. Distribuir contratos por empresa
                contratos_por_empresa = {}
                for emp in empresas:
                    contratos_por_empresa[emp["id_empresa"]] = []

                # Distribuir contratos
                for contrato in contratos:
                    fkContratoSISCTR, nr_cpf_cnpj, vr_sd_devedor = contrato

                    # Encontrar empresa com menor % de uso da capacidade
                    empresa_escolhida = None
                    menor_uso_percentual = float('inf')

                    for emp in empresas:
                        if emp["qtde_disponivel_ajustada"] <= 0:
                            continue

                        # Calcular % de uso (contratos atuais / quantidade máxima)
                        if emp["qtde_maxima"] > 0:
                            uso_percentual = (emp["qtde_atual"] + len(contratos_por_empresa[emp["id_empresa"]])) / emp[
                                "qtde_maxima"]
                        else:
                            uso_percentual = 1.0

                        if uso_percentual < menor_uso_percentual:
                            menor_uso_percentual = uso_percentual
                            empresa_escolhida = emp

                    # Se não encontrou empresa disponível, usar a maior
                    if not empresa_escolhida and empresas:
                        empresa_escolhida = max(empresas, key=lambda x: x["percentual"])

                    # Adicionar contrato à empresa escolhida
                    if empresa_escolhida:
                        contratos_por_empresa[empresa_escolhida["id_empresa"]].append({
                            "fkContratoSISCTR": fkContratoSISCTR,
                            "nr_cpf_cnpj": nr_cpf_cnpj,
                            "vr_sd_devedor": vr_sd_devedor
                        })
                        empresa_escolhida["qtde_disponivel_ajustada"] -= 1

                # 7. Inserir contratos na tabela de distribuição
                contratos_inseridos = 0
                for empresa_id, contratos in contratos_por_empresa.items():
                    if not contratos:
                        continue

                    # Inserir em bloco para melhor performance
                    for contrato in contratos:
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
                        ) VALUES (
                            GETDATE(),
                            :edital_id,
                            :periodo_id,
                            :fkContratoSISCTR,
                            :criterio_id,
                            :empresa_id,
                            :nr_cpf_cnpj,
                            :vr_sd_devedor,
                            GETDATE()
                        )
                        """)

                        result = connection.execute(insert_sql, {
                            "edital_id": edital_id,
                            "periodo_id": periodo_id,
                            "criterio_id": criterio_id,
                            "empresa_id": empresa_id,
                            "fkContratoSISCTR": contrato["fkContratoSISCTR"],
                            "nr_cpf_cnpj": contrato["nr_cpf_cnpj"],
                            "vr_sd_devedor": contrato["vr_sd_devedor"]
                        })

                        contratos_inseridos += 1

                print(f"\nTotal de contratos restantes inseridos: {contratos_inseridos}")

                # 8. Verificar resultados finais
                results_sql = text("""
                SELECT 
                    COD_EMPRESA_COBRANCA,
                    COUNT(*) AS QTDE
                FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                WHERE [ID_EDITAL] = :edital_id
                  AND [ID_PERIODO] = :periodo_id
                  AND [COD_CRITERIO_SELECAO] = :criterio_id
                GROUP BY COD_EMPRESA_COBRANCA
                ORDER BY COD_EMPRESA_COBRANCA
                """)

                check_results = connection.execute(results_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "criterio_id": criterio_id
                }).fetchall()

                print("\nResultados finais por empresa:")
                for row in check_results:
                    empresa_id, qtde = row
                    # Encontrar percentual dessa empresa
                    percentual = next((emp["percentual"] for emp in empresas if emp["id_empresa"] == empresa_id), 0)
                    print(f"Empresa {empresa_id}: {qtde} contratos ({percentual:.2f}%)")

                # 9. Limpar tabela de distribuíveis após processamento
                if contratos_inseridos > 0:
                    connection.execute(text("TRUNCATE TABLE [DEV].[DCA_TB006_DISTRIBUIVEIS]"))
                    print("Tabela de distribuíveis limpa após processamento")

                transaction.commit()
                print("Processamento dos contratos restantes concluído com sucesso!")

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
    MODIFICADO: Redistribui valores de arrecadação e percentuais para visualização correta no template
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
                FROM [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES]
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
                FROM [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES]
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
            # Buscar informações sobre a empresa que está saindo
            empresa_saindo_sql = text("""
               SELECT 
                   EP.ID_EMPRESA,
                   COALESCE(EP.NO_EMPRESA_ABREVIADA, EP.NO_EMPRESA) AS empresa_abrev,
                   (SELECT SUM(REE.VR_ARRECADACAO_TOTAL) 
                    FROM [BDG].[COM_TB062_REMUNERACAO_ESTIMADA] REE
                    WHERE REE.CO_EMPRESA_COBRANCA = EP.ID_EMPRESA) AS arrecadacao
               FROM [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES] EP
               WHERE EP.ID_EDITAL = :edital_id
                 AND EP.ID_PERIODO = :periodo_id
                 AND EP.ID_EMPRESA = :empresa_id
               """)

            empresa_saindo_result = connection.execute(empresa_saindo_sql, {
                "edital_id": edital_id,
                "periodo_id": periodo_id,
                "empresa_id": empresa_id
            }).fetchone()

            arrecadacao_empresa_saindo = float(empresa_saindo_result[2]) if empresa_saindo_result and \
                                                                            empresa_saindo_result[2] else 0.0

            # Agora buscar o resultado da distribuição COM OS PERCENTUAIS FINAIS CORRETOS
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
                   LD.PERCENTUAL_FINAL  -- IMPORTANTE: usar o percentual final ajustado
               FROM [BDG].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
               LEFT JOIN [DEV].[DCA_TB005_DISTRIBUICAO] D
                   ON LD.ID_EMPRESA = D.COD_EMPRESA_COBRANCA
                   AND D.ID_EDITAL = :edital_id
                   AND D.ID_PERIODO = :periodo_id
                   AND D.COD_CRITERIO_SELECAO = :criterio_id
               LEFT JOIN [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES] EP
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

            # Calcular o adicional de arrecadação por empresa
            qtde_empresas = len(result_rows)
            adicional_arrecadacao = arrecadacao_empresa_saindo / qtde_empresas if qtde_empresas > 0 else 0

            print("\nResultados por empresa:")
            print("Empresa | Qtde | % | Saldo | % | Arrecadação | % Final")
            print("-" * 70)

            for row in result_rows:
                # Valores originais do banco
                cod_empresa, empresa_abrev, qtde, pct_qtde, saldo, pct_saldo, vr_arrecadacao, percentual_final = row

                # Valores ajustados
                vr_arrecadacao_ajustado = float(vr_arrecadacao) if vr_arrecadacao else 0.0
                vr_arrecadacao_ajustado += adicional_arrecadacao  # Adicionar parte da empresa que saiu

                empresa_result = {
                    "cod_empresa": cod_empresa,
                    "empresa_abrev": empresa_abrev,
                    "qtde": int(qtde) if qtde else 0,
                    "pct_qtde": float(pct_qtde) if pct_qtde else 0.0,
                    "saldo": float(saldo) if saldo else 0.0,
                    "pct_saldo": float(pct_saldo) if pct_saldo else 0.0,
                    "arrecadacao": vr_arrecadacao_ajustado,  # Valor redistribuído
                    "percentual_final": float(percentual_final) if percentual_final else 0.0
                    # USAR PERCENTUAL FINAL CORRETO
                }
                resultados_por_empresa.append(empresa_result)
                total_qtde += empresa_result["qtde"]
                total_saldo += empresa_result["saldo"]
                total_arrecadacao_final += vr_arrecadacao_ajustado

                print(
                    f"{empresa_result['empresa_abrev']: <15} | {empresa_result['qtde']:>5} | {empresa_result['pct_qtde']:>5.2f}% | "
                    f"{empresa_result['saldo']:>10.2f} | {empresa_result['pct_saldo']:>5.2f}% | "
                    f"{vr_arrecadacao_ajustado:>12.2f} | {empresa_result['percentual_final']:>5.2f}%")

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