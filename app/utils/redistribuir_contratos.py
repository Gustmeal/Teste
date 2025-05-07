# app/utils/redistribuir_contratos.py
from app import db
from sqlalchemy import text
import logging
from datetime import datetime


def selecionar_contratos_para_redistribuicao(empresa_id):
    """
    Seleciona os contratos da empresa que será redistribuída.
    Versão aprimorada que busca contratos em múltiplas fontes.
    """
    try:
        # Limpar tabelas temporárias
        with db.engine.connect() as connection:
            # Truncar tabelas
            connection.execute(text("TRUNCATE TABLE [DEV].[DCA_TB006_DISTRIBUIVEIS]"))
            connection.execute(text("TRUNCATE TABLE [DEV].[DCA_TB007_ARRASTAVEIS]"))

            # Verificar se a empresa tem limites cadastrados
            check_limites_sql = text("""
                SELECT TOP 1 PERCENTUAL_FINAL, QTDE_MAXIMA 
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE ID_EMPRESA = :empresa_id
                AND DELETED_AT IS NULL
                ORDER BY CREATED_AT DESC
            """)
            limite_result = connection.execute(check_limites_sql, {"empresa_id": empresa_id}).fetchone()

            if limite_result:
                percentual, qtde = limite_result
                logging.info(f"Empresa {empresa_id} tem limite cadastrado: {percentual}% (max: {qtde} contratos)")

            # PRIMEIRA TENTATIVA: Buscar da tabela de empresas atual
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
                AND SDJ.fkContratoSISCTR IS NULL
                AND SIT.VR_SD_DEVEDOR > 0
            """)

            result = connection.execute(insert_sql, {"empresa_id": empresa_id})
            contratos_atuais = result.rowcount
            logging.info(f"Contratos atuais da empresa {empresa_id}: {contratos_atuais}")

            # SEGUNDA TENTATIVA: Se não encontrou na tabela atual, buscar na tabela de distribuição
            if contratos_atuais == 0:
                # Verificar se a empresa tem contratos na tabela de distribuição
                insert_from_dist_sql = text("""
                INSERT INTO [DEV].[DCA_TB006_DISTRIBUIVEIS]
                SELECT DISTINCT
                    DIS.fkContratoSISCTR,
                    DIS.NR_CPF_CNPJ,
                    DIS.VR_SD_DEVEDOR,
                    GETDATE() AS CREATED_AT,
                    NULL AS UPDATED_AT,
                    NULL AS DELETED_AT
                FROM 
                    [DEV].[DCA_TB005_DISTRIBUICAO] AS DIS
                    INNER JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] AS SIT
                        ON DIS.fkContratoSISCTR = SIT.fkContratoSISCTR
                    LEFT JOIN [BDG].[COM_TB013_SUSPENSO_DECISAO_JUDICIAL] AS SDJ
                        ON DIS.fkContratoSISCTR = SDJ.fkContratoSISCTR
                WHERE
                    DIS.[COD_EMPRESA_COBRANCA] = :empresa_id
                    AND DIS.DELETED_AT IS NULL
                    AND SDJ.fkContratoSISCTR IS NULL
                    AND SIT.VR_SD_DEVEDOR > 0
                """)

                result = connection.execute(insert_from_dist_sql, {"empresa_id": empresa_id})
                contratos_dist = result.rowcount
                logging.info(f"Contratos da tabela de distribuição da empresa {empresa_id}: {contratos_dist}")

            # Contar contratos selecionados
            count_sql = text("SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] WHERE DELETED_AT IS NULL")
            result = connection.execute(count_sql)
            num_contratos = result.scalar() or 0

            if num_contratos == 0:
                logging.warning(f"Nenhum contrato válido encontrado para a empresa {empresa_id}.")

                if limite_result and limite_result[1] > 0:
                    logging.warning(
                        f"A empresa {empresa_id} tem limite de {limite_result[1]} contratos, mas nenhum contrato atual foi encontrado.")
                    logging.warning("Verifique se os contratos já foram redistribuídos anteriormente.")

                    # Opção: verificar se a empresa aparece na tabela de empresas descredenciadas
                    emp_check_sql = text("""
                    SELECT COUNT(*) FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES]
                    WHERE ID_EMPRESA = :empresa_id 
                    AND DS_CONDICAO = 'DESCREDENCIADA NO PERÍODO'
                    """)
                    result = connection.execute(emp_check_sql, {"empresa_id": empresa_id})
                    if result.scalar() > 0:
                        logging.warning(
                            f"Empresa {empresa_id} está marcada como DESCREDENCIADA NO PERÍODO, mas não tem contratos para redistribuir")

            return num_contratos

    except Exception as e:
        logging.error(f"Erro ao selecionar contratos para redistribuição: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return 0


def calcular_percentuais_redistribuicao(edital_id, periodo_id, empresa_id):
    """
    Calcula os percentuais para redistribuição dos contratos.
    Versão melhorada que busca em múltiplas fontes.

    Args:
        edital_id: ID do edital
        periodo_id: ID do período
        empresa_id: ID da empresa que está saindo

    Returns:
        tuple: (percentual_redistribuido, total_arrecadacao, empresas_dados)
    """
    try:
        # 1. Buscar período para obter datas de início e fim
        periodo_sql = text("""
            SELECT DT_INICIO, DT_FIM 
            FROM [DEV].[DCA_TB001_PERIODO_AVALIACAO]
            WHERE ID_EDITAL = :edital_id AND ID_PERIODO = :periodo_id
        """)

        with db.engine.connect() as connection:
            periodo_result = connection.execute(periodo_sql,
                                                {"edital_id": edital_id, "periodo_id": periodo_id}).fetchone()

            if not periodo_result:
                logging.error(f"Período não encontrado: Edital {edital_id}, Período {periodo_id}")
                return 0, 0, []

            dt_inicio, dt_fim = periodo_result

            # OPÇÃO 1: Calcular percentuais da tabela de arrecadação
            arrecadacao_sql = text("""
            WITH Percentuais AS (
                SELECT 
                    REE.CO_EMPRESA_COBRANCA AS ID_EMPRESA,
                    SUM(REE.VR_ARRECADACAO_TOTAL) AS VR_ARRECADACAO,
                    ROUND((SUM(REE.VR_ARRECADACAO_TOTAL) * 100.0 / 
                          NULLIF((SELECT SUM(VR_ARRECADACAO_TOTAL) FROM [BDG].[COM_TB062_REMUNERACAO_ESTIMADA] 
                           WHERE DT_ARRECADACAO BETWEEN :dt_inicio AND :dt_fim), 0)), 2) AS PERCENTUAL
                FROM [BDG].[COM_TB062_REMUNERACAO_ESTIMADA] AS REE
                WHERE REE.DT_ARRECADACAO BETWEEN :dt_inicio AND :dt_fim
                GROUP BY REE.CO_EMPRESA_COBRANCA
            ),
            Ajuste AS (
                SELECT 
                    ID_EMPRESA,
                    VR_ARRECADACAO,
                    PERCENTUAL,
                    SUM(PERCENTUAL) OVER() AS SOMA_PERCENTUAIS
                FROM Percentuais
            )
            SELECT
                ID_EMPRESA,
                VR_ARRECADACAO,
                CASE 
                    WHEN ROW_NUMBER() OVER (ORDER BY PERCENTUAL DESC) = 1 
                    THEN PERCENTUAL + (100.00 - SOMA_PERCENTUAIS)
                    ELSE PERCENTUAL
                END AS PERCENTUAL_AJUSTADO
            FROM Ajuste
            """)

            arrecadacao_results = connection.execute(arrecadacao_sql, {
                "dt_inicio": dt_inicio,
                "dt_fim": dt_fim
            }).fetchall()

            # Se não houver dados de arrecadação, tentar buscar da tabela de limites
            if not arrecadacao_results:
                logging.warning("Dados de arrecadação não encontrados, buscando percentuais da tabela de limites...")

                # OPÇÃO 2: Buscar percentuais da tabela de limites
                limites_sql = text("""
                SELECT 
                    ID_EMPRESA,
                    COALESCE(VR_ARRECADACAO, 0) AS VR_ARRECADACAO,
                    PERCENTUAL_FINAL
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND DELETED_AT IS NULL
                """)

                arrecadacao_results = connection.execute(limites_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id
                }).fetchall()

                if not arrecadacao_results:
                    logging.warning("Percentuais não encontrados na tabela de limites, usando valores iguais...")

                    # OPÇÃO 3: Se ainda não tiver dados, buscar todas as empresas participantes
                    empresas_sql = text("""
                    SELECT 
                        ID_EMPRESA,
                        0 AS VR_ARRECADACAO,
                        0 AS PERCENTUAL
                    FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES]
                    WHERE ID_EDITAL = :edital_id
                    AND ID_PERIODO = :periodo_id
                    AND DS_CONDICAO != 'DESCREDENCIADA NO PERÍODO'
                    AND DELETED_AT IS NULL
                    """)

                    arrecadacao_results = connection.execute(empresas_sql, {
                        "edital_id": edital_id,
                        "periodo_id": periodo_id
                    }).fetchall()

                    # Se encontrou empresas, atribuir percentuais iguais
                    if arrecadacao_results:
                        total_empresas = len(arrecadacao_results)
                        percentual_igual = 100.0 / total_empresas

                        # Criar lista com percentuais iguais
                        arrecadacao_results = [
                            (r[0], 0, percentual_igual) for r in arrecadacao_results
                        ]

                        logging.info(
                            f"Atribuindo percentual igual ({percentual_igual}%) para {total_empresas} empresas")

            if not arrecadacao_results:
                logging.error("Nenhum dado encontrado para cálculo de percentuais")
                return 0, 0, []

            # 3. Processar resultados
            empresas_dados = []
            percentual_empresa_redistribuida = 0
            total_arrecadacao = 0

            for result in arrecadacao_results:
                id_empresa, vr_arrecadacao, percentual = result

                # Ajustar valores nulos
                vr_arrecadacao = float(vr_arrecadacao) if vr_arrecadacao is not None else 0.0
                percentual = float(percentual) if percentual is not None else 0.0

                if id_empresa == empresa_id:
                    percentual_empresa_redistribuida = percentual

                total_arrecadacao += vr_arrecadacao

                empresas_dados.append({
                    "id_empresa": id_empresa,
                    "vr_arrecadacao": vr_arrecadacao,
                    "percentual": percentual
                })

            # Verificar se a empresa que está saindo foi encontrada
            if percentual_empresa_redistribuida == 0:
                # Buscar diretamente da tabela de limites
                empresa_saindo_sql = text("""
                SELECT PERCENTUAL_FINAL
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND ID_EMPRESA = :empresa_id
                AND DELETED_AT IS NULL
                """)

                result = connection.execute(empresa_saindo_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "empresa_id": empresa_id
                }).fetchone()

                if result:
                    percentual_empresa_redistribuida = float(result[0]) if result[0] is not None else 0.0
                    logging.info(
                        f"Percentual da empresa {empresa_id} obtido da tabela de limites: {percentual_empresa_redistribuida}%")
                else:
                    # Se não encontrar, usar um valor padrão
                    percentual_empresa_redistribuida = 5.0  # Valor razoável para não bloquear o processo
                    logging.warning(
                        f"Percentual da empresa {empresa_id} não encontrado, usando valor padrão: {percentual_empresa_redistribuida}%")

            return percentual_empresa_redistribuida, total_arrecadacao, empresas_dados

    except Exception as e:
        logging.error(f"Erro ao calcular percentuais para redistribuição: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return 0, 0, []


def redistribuir_percentuais(edital_id, periodo_id, empresa_id, cod_criterio, dt_apuracao=None):
    """
    Redistribui percentuais e cria novos limites de distribuição.
    Versão corrigida com tratamento adequado de tipos numéricos.
    """
    try:
        # Se não foi fornecida data de apuração, usar a data atual
        if dt_apuracao is None:
            dt_apuracao = datetime.now().date()

        from decimal import Decimal, ROUND_HALF_UP

        # Função auxiliar para garantir conversão segura para decimal
        def to_decimal(value):
            if value is None:
                return Decimal('0')
            if isinstance(value, Decimal):
                return value
            return Decimal(str(float(value)))

        # 1. Calcular percentuais de redistribuição
        percentual_redistribuido, total_arrecadacao, empresas_dados = calcular_percentuais_redistribuicao(
            edital_id, periodo_id, empresa_id)

        # Converter para decimal
        percentual_redistribuido = to_decimal(percentual_redistribuido)
        total_arrecadacao = to_decimal(total_arrecadacao)

        # Se percentual for zero mas temos empresas, forçar um mínimo
        if percentual_redistribuido == Decimal('0') and empresas_dados:
            percentual_redistribuido = Decimal('5.0')
            logging.warning(f"Percentual zero detectado, usando valor mínimo: {percentual_redistribuido}%")

        if not empresas_dados:
            logging.error("Nenhuma empresa encontrada para redistribuição")

            # Buscar empresas participantes diretamente
            with db.engine.connect() as connection:
                empresas_sql = text("""
                SELECT 
                    ID_EMPRESA
                FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND ID_EMPRESA != :empresa_id
                AND DS_CONDICAO != 'DESCREDENCIADA NO PERÍODO'
                AND DELETED_AT IS NULL
                """)

                emp_results = connection.execute(empresas_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "empresa_id": empresa_id
                }).fetchall()

                if emp_results:
                    total_empresas = len(emp_results)
                    percentual_igual = Decimal('100.0') / Decimal(str(total_empresas))

                    empresas_dados = [
                        {"id_empresa": r[0], "vr_arrecadacao": Decimal('0'), "percentual": percentual_igual}
                        for r in emp_results
                    ]

                    logging.info(f"Criando dados de empresas manualmente para {total_empresas} empresas")

                    percentual_redistribuido = percentual_igual
                    logging.info(f"Definindo percentual da empresa que sai: {percentual_redistribuido}%")
                else:
                    logging.error("Nenhuma empresa participante encontrada")
                    return False

        # Garantir que todos os valores numéricos sejam Decimal
        for empresa in empresas_dados:
            empresa["vr_arrecadacao"] = to_decimal(empresa["vr_arrecadacao"])
            empresa["percentual"] = to_decimal(empresa["percentual"])

        # 2. Contar empresas remanescentes
        empresas_remanescentes = [e for e in empresas_dados if e["id_empresa"] != empresa_id]
        qtde_empresas_remanescentes = len(empresas_remanescentes)

        if qtde_empresas_remanescentes == 0:
            logging.error("Nenhuma empresa remanescente para redistribuição")
            return False

        # 3. Calcular percentual unitário a ser redistribuído
        if qtde_empresas_remanescentes > 0:
            percentual_unitario = percentual_redistribuido / Decimal(str(qtde_empresas_remanescentes))
        else:
            percentual_unitario = Decimal('0')

        logging.info(
            f"Percentual unitário: {percentual_unitario}% (total: {percentual_redistribuido}% / {qtde_empresas_remanescentes} empresas)")

        # 4. Inserir/atualizar limites de distribuição
        with db.engine.connect() as connection:
            for empresa in empresas_remanescentes:
                # Calcular novo percentual
                novo_percentual = empresa["percentual"] + percentual_unitario

                # Verificar se o registro já existe
                check_sql = text("""
                SELECT ID FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND ID_EMPRESA = :empresa_id
                AND COD_CRITERIO_SELECAO = :cod_criterio
                """)

                existing_id = connection.execute(check_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "empresa_id": empresa["id_empresa"],
                    "cod_criterio": cod_criterio
                }).scalar()

                if existing_id:
                    # Se existe, atualizar
                    update_sql = text("""
                    UPDATE [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                    SET DT_APURACAO = :dt_apuracao,
                        VR_ARRECADACAO = :vr_arrecadacao,
                        PERCENTUAL_FINAL = :percentual,
                        UPDATED_AT = GETDATE()
                    WHERE ID = :id
                    """)

                    connection.execute(update_sql, {
                        "dt_apuracao": dt_apuracao,
                        "vr_arrecadacao": float(empresa["vr_arrecadacao"]),
                        "percentual": float(novo_percentual),
                        "id": existing_id
                    })

                    logging.info(
                        f"Atualizado limite existente para empresa {empresa['id_empresa']}: {novo_percentual}%")
                else:
                    # Se não existe, inserir
                    insert_sql = text("""
                    INSERT INTO [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                    (ID_EDITAL, ID_PERIODO, ID_EMPRESA, COD_CRITERIO_SELECAO, 
                    DT_APURACAO, VR_ARRECADACAO, PERCENTUAL_FINAL, CREATED_AT)
                    VALUES (:edital_id, :periodo_id, :empresa_id, :cod_criterio,
                    :dt_apuracao, :vr_arrecadacao, :percentual, GETDATE())
                    """)

                    connection.execute(insert_sql, {
                        "edital_id": edital_id,
                        "periodo_id": periodo_id,
                        "empresa_id": empresa["id_empresa"],
                        "cod_criterio": cod_criterio,
                        "dt_apuracao": dt_apuracao,
                        "vr_arrecadacao": float(empresa["vr_arrecadacao"]),
                        "percentual": float(novo_percentual)
                    })

                    logging.info(f"Inserido novo limite para empresa {empresa['id_empresa']}: {novo_percentual}%")

            # 4.3 Ajustar percentuais para garantir soma = 100%
            soma_sql = text("""
            SELECT SUM(CAST(PERCENTUAL_FINAL AS DECIMAL(10,2))) 
            FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
            WHERE ID_EDITAL = :edital_id
            AND ID_PERIODO = :periodo_id
            AND COD_CRITERIO_SELECAO = :cod_criterio
            """)

            soma_percentuais_result = connection.execute(soma_sql, {
                "edital_id": edital_id,
                "periodo_id": periodo_id,
                "cod_criterio": cod_criterio
            }).scalar()

            # Converter para decimal
            soma_percentuais = to_decimal(soma_percentuais_result)
            diferenca = Decimal('100.0') - soma_percentuais

            if abs(diferenca) > Decimal('0.01'):  # Se a diferença for significativa
                maior_sql = text("""
                SELECT TOP 1 ID
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND COD_CRITERIO_SELECAO = :cod_criterio
                ORDER BY PERCENTUAL_FINAL DESC
                """)

                maior_id = connection.execute(maior_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "cod_criterio": cod_criterio
                }).scalar()

                if maior_id:
                    ajuste_sql = text("""
                    UPDATE [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                    SET PERCENTUAL_FINAL = PERCENTUAL_FINAL + :diferenca
                    WHERE ID = :id
                    """)

                    connection.execute(ajuste_sql, {
                        "diferenca": float(diferenca),
                        "id": maior_id
                    })

                    logging.info(f"Ajustado percentual do registro ID={maior_id} em {diferenca}% para total = 100%")

            # 5. Calcular valores máximos e quantidades máximas
            num_contratos = selecionar_contratos_para_redistribuicao(empresa_id)

            # Se não tiver contratos, usar valor padrão para permitir a redistribuição
            if num_contratos == 0:
                check_limites_sql = text("""
                SELECT SUM(QTDE_MAXIMA) 
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND DELETED_AT IS NULL
                """)

                result = connection.execute(check_limites_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id
                }).scalar()

                if result:
                    num_contratos = int(result)
                    logging.info(f"Usando quantidade de contratos dos limites anteriores: {num_contratos}")
                else:
                    num_contratos = 1000
                    logging.warning(f"Sem contratos para redistribuir, usando valor padrão: {num_contratos}")

            # Obter soma do saldo devedor
            saldo_sql = text("SELECT SUM(CAST(VR_SD_DEVEDOR AS DECIMAL(18,2))) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]")
            saldo_result = connection.execute(saldo_sql).scalar()

            # Converter para float (SQL Server espera float para os parâmetros)
            saldo_total = float(to_decimal(saldo_result))

            if saldo_total == 0:
                saldo_total = 1000000.0
                logging.warning(f"Saldo total zero, usando valor padrão: {saldo_total}")

            # Atualizar qtde_maxima e valor_maximo
            update_limits_sql = text("""
            UPDATE [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
            SET QTDE_MAXIMA = FLOOR(:num_contratos * PERCENTUAL_FINAL / 100),
                VALOR_MAXIMO = :saldo_total * PERCENTUAL_FINAL / 100
            WHERE ID_EDITAL = :edital_id
            AND ID_PERIODO = :periodo_id
            AND COD_CRITERIO_SELECAO = :cod_criterio
            """)

            connection.execute(update_limits_sql, {
                "edital_id": edital_id,
                "periodo_id": periodo_id,
                "cod_criterio": cod_criterio,
                "num_contratos": num_contratos,
                "saldo_total": saldo_total
            })

            # 6. Distribuir sobras (uma por empresa)
            calc_sobra_sql = text("""
            SELECT :num_contratos - SUM(QTDE_MAXIMA)
            FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
            WHERE ID_EDITAL = :edital_id
            AND ID_PERIODO = :periodo_id
            AND COD_CRITERIO_SELECAO = :cod_criterio
            """)

            sobra_result = connection.execute(calc_sobra_sql, {
                "edital_id": edital_id,
                "periodo_id": periodo_id,
                "cod_criterio": cod_criterio,
                "num_contratos": num_contratos
            }).scalar() or 0

            sobra = int(sobra_result)

            if sobra > 0:
                ids_query = text("""
                SELECT TOP :sobra ID
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND COD_CRITERIO_SELECAO = :cod_criterio
                ORDER BY QTDE_MAXIMA DESC
                """)

                ids_result = connection.execute(ids_query, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "cod_criterio": cod_criterio,
                    "sobra": sobra
                }).fetchall()

                for row in ids_result:
                    update_sql = text("""
                    UPDATE [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                    SET QTDE_MAXIMA = QTDE_MAXIMA + 1
                    WHERE ID = :id
                    """)

                    connection.execute(update_sql, {"id": row[0]})

                logging.info(f"Distribuídas {len(ids_result)} sobras de contratos")

        return True

    except Exception as e:
        logging.error(f"Erro ao redistribuir percentuais: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return False

def processar_redistribuicao_contratos(edital_id, periodo_id, empresa_id, cod_criterio):
    """
    Executa o processo completo de redistribuição de contratos.

    Args:
        edital_id: ID do edital
        periodo_id: ID do período
        empresa_id: ID da empresa que está saindo
        cod_criterio: Código do critério de redistribuição

    Returns:
        dict: Resultados do processo
    """
    resultados = {
        "contratos_redistribuidos": 0,
        "contratos_arrastados": 0,
        "contratos_restantes": 0,
        "total_empresas": 0,
        "empresa_redistribuida": empresa_id,
        "success": False
    }

    try:
        # 1. Selecionar contratos a redistribuir
        num_contratos = selecionar_contratos_para_redistribuicao(empresa_id)
        if num_contratos == 0:
            return resultados

        # 2. Redistribuir percentuais e criar limites
        success = redistribuir_percentuais(edital_id, periodo_id, empresa_id, cod_criterio)
        if not success:
            return resultados

        # 3. Identificar contratos arrastáveis (mesmo CPF)
        with db.engine.connect() as connection:
            # 3.1 Limpar tabela de arrastáveis
            truncate_arrastaveis = text("TRUNCATE TABLE [DEV].[DCA_TB007_ARRASTAVEIS]")
            connection.execute(truncate_arrastaveis)

            # 3.2 Identificar CPFs com múltiplos contratos
            insert_arrastaveis = text("""
            WITH CPFsMultiplos AS (
                SELECT NR_CPF_CNPJ
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                GROUP BY NR_CPF_CNPJ
                HAVING COUNT(*) > 1
            )
            INSERT INTO [DEV].[DCA_TB007_ARRASTAVEIS]
            (FkContratoSISCTR, NR_CPF_CNPJ, VR_SD_DEVEDOR, CREATED_AT)
            SELECT D.FkContratoSISCTR, D.NR_CPF_CNPJ, D.VR_SD_DEVEDOR, GETDATE()
            FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] D
            JOIN CPFsMultiplos C ON D.NR_CPF_CNPJ = C.NR_CPF_CNPJ
            """)

            connection.execute(insert_arrastaveis)

            # 3.3 Remover contratos arrastáveis da tabela de distribuíveis
            delete_from_distribuiveis = text("""
            DELETE FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
            WHERE FkContratoSISCTR IN (
                SELECT FkContratoSISCTR FROM [DEV].[DCA_TB007_ARRASTAVEIS]
            )
            """)

            connection.execute(delete_from_distribuiveis)

            # 3.4 Contar contratos arrastáveis
            arrastaveis_count = connection.execute(
                text("SELECT COUNT(*) FROM [DEV].[DCA_TB007_ARRASTAVEIS]")
            ).scalar() or 0

            resultados["contratos_arrastados"] = arrastaveis_count

            # 4. Distribuir contratos arrastáveis
            if arrastaveis_count > 0:
                # Implementar a lógica de distribuição de arrastáveis
                # (Similar à função aplicar_regra_arrasto_sem_acordo)
                distribuir_arrastados_sql = text("""
                -- Script para distribuir contratos arrastáveis
                -- Declaração de variáveis locais
                DECLARE @dt_referencia DATE = GETDATE();

                -- 1. Obter empresas e seus percentuais
                IF OBJECT_ID('tempdb..#Empresas') IS NOT NULL
                    DROP TABLE #Empresas;

                SELECT 
                    LD.ID_EMPRESA,
                    LD.PERCENTUAL_FINAL,
                    LD.QTDE_MAXIMA,
                    ROW_NUMBER() OVER (ORDER BY NEWID()) AS ordem
                INTO #Empresas
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
                WHERE LD.ID_EDITAL = :edital_id
                AND LD.ID_PERIODO = :periodo_id
                AND LD.COD_CRITERIO_SELECAO = :cod_criterio;

                -- 2. Verificar quantidade de CPFs distintos
                IF OBJECT_ID('tempdb..#CPFs') IS NOT NULL
                    DROP TABLE #CPFs;

                SELECT 
                    NR_CPF_CNPJ,
                    COUNT(*) AS qtd_contratos,
                    ROW_NUMBER() OVER (ORDER BY NR_CPF_CNPJ) AS ordem
                INTO #CPFs
                FROM [DEV].[DCA_TB007_ARRASTAVEIS]
                GROUP BY NR_CPF_CNPJ;

                -- 3. Distribuir CPFs entre empresas
                IF OBJECT_ID('tempdb..#DistribuicaoCPFs') IS NOT NULL
                    DROP TABLE #DistribuicaoCPFs;

                WITH EmpresaOrdens AS (
                    SELECT 
                        ID_EMPRESA,
                        ordem,
                        PERCENTUAL_FINAL,
                        FLOOR((SELECT COUNT(*) FROM #CPFs) * PERCENTUAL_FINAL / 100.0) AS cpfs_alocados,
                        ROW_NUMBER() OVER (ORDER BY PERCENTUAL_FINAL DESC) AS rank_percentual
                    FROM #Empresas
                )
                SELECT 
                    CPF.NR_CPF_CNPJ,
                    EO.ID_EMPRESA,
                    CPF.qtd_contratos
                INTO #DistribuicaoCPFs
                FROM #CPFs CPF
                CROSS APPLY (
                    SELECT TOP 1 ID_EMPRESA
                    FROM EmpresaOrdens EO
                    ORDER BY 
                        CASE 
                            WHEN CPF.ordem % (SELECT COUNT(*) FROM #Empresas) = EO.ordem % (SELECT COUNT(*) FROM #Empresas) 
                            THEN 0 ELSE 1 
                        END,
                        EO.ordem
                ) AS EO;

                -- 4. Inserir todos os contratos na tabela de distribuição
                INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
                (DT_REFERENCIA, ID_EDITAL, ID_PERIODO, fkContratoSISCTR, 
                COD_EMPRESA_COBRANCA, COD_CRITERIO_SELECAO, NR_CPF_CNPJ, 
                VR_SD_DEVEDOR, CREATED_AT)
                SELECT 
                    @dt_referencia,
                    :edital_id,
                    :periodo_id,
                    A.FkContratoSISCTR,
                    D.ID_EMPRESA,
                    :cod_criterio,
                    A.NR_CPF_CNPJ,
                    A.VR_SD_DEVEDOR,
                    GETDATE()
                FROM [DEV].[DCA_TB007_ARRASTAVEIS] A
                JOIN #DistribuicaoCPFs D ON A.NR_CPF_CNPJ = D.NR_CPF_CNPJ;

                -- 5. Limpar tabelas temporárias
                DROP TABLE IF EXISTS #Empresas;
                DROP TABLE IF EXISTS #CPFs;
                DROP TABLE IF EXISTS #DistribuicaoCPFs;
                """)

                connection.execute(distribuir_arrastados_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "cod_criterio": cod_criterio
                })

            # 5. Distribuir contratos restantes
            restantes_count = connection.execute(
                text("SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]")
            ).scalar() or 0

            resultados["contratos_restantes"] = restantes_count

            if restantes_count > 0:
                # Implementar a lógica de distribuição dos contratos restantes
                distribuir_restantes_sql = text("""
                -- Script para distribuir contratos restantes
                DECLARE @dt_referencia DATE = GETDATE();

                -- 1. Obter empresas e seus limites
                IF OBJECT_ID('tempdb..#EmpresasLimites') IS NOT NULL
                    DROP TABLE #EmpresasLimites;

                SELECT 
                    LD.ID_EMPRESA,
                    LD.PERCENTUAL_FINAL,
                    LD.QTDE_MAXIMA,
                    (SELECT COUNT(*) FROM [DEV].[DCA_TB005_DISTRIBUICAO] 
                     WHERE COD_EMPRESA_COBRANCA = LD.ID_EMPRESA
                     AND ID_EDITAL = :edital_id
                     AND ID_PERIODO = :periodo_id
                     AND COD_CRITERIO_SELECAO = :cod_criterio) AS contratos_atuais,
                    0 AS contratos_restantes
                INTO #EmpresasLimites
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
                WHERE LD.ID_EDITAL = :edital_id
                AND LD.ID_PERIODO = :periodo_id
                AND LD.COD_CRITERIO_SELECAO = :cod_criterio;

                -- 2. Calcular quantos contratos restam para cada empresa
                UPDATE #EmpresasLimites
                SET contratos_restantes = 
                    CASE 
                        WHEN QTDE_MAXIMA > contratos_atuais 
                        THEN QTDE_MAXIMA - contratos_atuais 
                        ELSE 0 
                    END;

                -- 3. Preparar contratos restantes em ordem aleatória
                IF OBJECT_ID('tempdb..#ContratosRestantes') IS NOT NULL
                    DROP TABLE #ContratosRestantes;

                SELECT 
                    FkContratoSISCTR,
                    NR_CPF_CNPJ,
                    VR_SD_DEVEDOR,
                    ROW_NUMBER() OVER (ORDER BY NEWID()) AS ordem
                INTO #ContratosRestantes
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS];

                -- 4. Distribuir contratos entre empresas
                IF OBJECT_ID('tempdb..#DistribuicaoFinal') IS NOT NULL
                    DROP TABLE #DistribuicaoFinal;

                WITH EmpresasFaixas AS (
                    SELECT 
                        ID_EMPRESA,
                        contratos_restantes,
                        SUM(contratos_restantes) OVER (ORDER BY PERCENTUAL_FINAL DESC 
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - contratos_restantes + 1 AS inicio_faixa,
                        SUM(contratos_restantes) OVER (ORDER BY PERCENTUAL_FINAL DESC 
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fim_faixa
                    FROM #EmpresasLimites
                    WHERE contratos_restantes > 0
                )
                SELECT 
                    CR.FkContratoSISCTR,
                    CR.NR_CPF_CNPJ,
                    CR.VR_SD_DEVEDOR,
                    EF.ID_EMPRESA
                INTO #DistribuicaoFinal
                FROM #ContratosRestantes CR
                CROSS APPLY (
                    SELECT TOP 1 ID_EMPRESA
                    FROM EmpresasFaixas EF 
                    WHERE CR.ordem BETWEEN EF.inicio_faixa AND EF.fim_faixa
                    ORDER BY EF.ID_EMPRESA
                ) AS EF
                WHERE CR.ordem <= (SELECT SUM(contratos_restantes) FROM #EmpresasLimites);

                -- 5. Inserir na tabela de distribuição
                INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
                (DT_REFERENCIA, ID_EDITAL, ID_PERIODO, fkContratoSISCTR, 
                COD_EMPRESA_COBRANCA, COD_CRITERIO_SELECAO, NR_CPF_CNPJ, 
                VR_SD_DEVEDOR, CREATED_AT)
                SELECT 
                    @dt_referencia,
                    :edital_id, 
                    :periodo_id,
                    DF.FkContratoSISCTR,
                    DF.ID_EMPRESA,
                    :cod_criterio,
                    DF.NR_CPF_CNPJ,
                    DF.VR_SD_DEVEDOR,
                    GETDATE()
                FROM #DistribuicaoFinal DF;

                -- 6. Limpar tabelas temporárias
                DROP TABLE IF EXISTS #EmpresasLimites;
                DROP TABLE IF EXISTS #ContratosRestantes;
                DROP TABLE IF EXISTS #DistribuicaoFinal;
                """)

                connection.execute(distribuir_restantes_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "cod_criterio": cod_criterio
                })

            # 6. Contar total de contratos redistribuídos
            total_redistribuidos = connection.execute(
                text("""
                SELECT COUNT(*) 
                FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND COD_CRITERIO_SELECAO = :cod_criterio
                """),
                {"edital_id": edital_id, "periodo_id": periodo_id, "cod_criterio": cod_criterio}
            ).scalar() or 0

            # 7. Obter número de empresas
            total_empresas = connection.execute(
                text("""
                SELECT COUNT(DISTINCT COD_EMPRESA_COBRANCA)
                FROM [DEV].[DCA_TB005_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND COD_CRITERIO_SELECAO = :cod_criterio
                """),
                {"edital_id": edital_id, "periodo_id": periodo_id, "cod_criterio": cod_criterio}
            ).scalar() or 0

            # 8. Atualizar resultados
            resultados["contratos_redistribuidos"] = total_redistribuidos
            resultados["total_empresas"] = total_empresas
            resultados["success"] = True

        return resultados

    except Exception as e:
        logging.error(f"Erro ao processar redistribuição: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return resultados