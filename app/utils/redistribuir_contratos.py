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
    Versão robusta que busca em múltiplas fontes e garante que algum resultado seja obtido.

    Args:
        edital_id: ID do edital
        periodo_id: ID do período
        empresa_id: ID da empresa que está saindo

    Returns:
        tuple: (percentual_redistribuido, total_arrecadacao, empresas_dados)
    """
    from decimal import Decimal
    import logging

    # Função auxiliar para tratar valores numéricos
    def to_float(value):
        if value is None:
            return 0.0
        if isinstance(value, Decimal):
            return float(value)
        return float(value)

    try:
        logging.info(
            f"Calculando percentuais de redistribuição - Edital: {edital_id}, Período: {periodo_id}, Empresa: {empresa_id}")

        # 1. Buscar período CORRETO para obter datas
        # IMPORTANTE: Precisamos buscar o período ANTERIOR, não o atual
        periodo_anterior_sql = text("""
            -- Primeiro busca período atual
            WITH periodo_atual AS (
                SELECT ID, ID_PERIODO, DT_INICIO, DT_FIM 
                FROM [DEV].[DCA_TB001_PERIODO_AVALIACAO]
                WHERE ID_EDITAL = :edital_id AND ID_PERIODO = :periodo_id
            )
            -- Depois busca o período anterior
            SELECT p.ID, p.ID_PERIODO, p.DT_INICIO, p.DT_FIM 
            FROM [DEV].[DCA_TB001_PERIODO_AVALIACAO] p
            JOIN periodo_atual pa ON p.ID_EDITAL = pa.ID_EDITAL
            WHERE p.ID_PERIODO < pa.ID_PERIODO
            ORDER BY p.ID_PERIODO DESC
        """)

        with db.engine.connect() as connection:
            # Primeira tentativa: tentar buscar período anterior
            periodo_result = connection.execute(periodo_anterior_sql, {
                "edital_id": edital_id,
                "periodo_id": periodo_id
            }).fetchone()

            # Se não encontrar o período anterior, usar o atual
            if not periodo_result:
                logging.warning(f"Período anterior não encontrado, tentando período atual")
                periodo_atual_sql = text("""
                    SELECT ID, ID_PERIODO, DT_INICIO, DT_FIM 
                    FROM [DEV].[DCA_TB001_PERIODO_AVALIACAO]
                    WHERE ID_EDITAL = :edital_id AND ID_PERIODO = :periodo_id
                """)

                periodo_result = connection.execute(periodo_atual_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id
                }).fetchone()

            if not periodo_result:
                logging.error(f"Nenhum período encontrado: Edital {edital_id}, Período {periodo_id}")
                return 0, 0, []

            _, periodo_id_efetivo, dt_inicio, dt_fim = periodo_result

            # Log detalhado para debug
            logging.info(f"Buscando arrecadação para o período de {dt_inicio} até {dt_fim}")

            # ESTRATÉGIA 1: Buscar todas as empresas participantes atuais como base
            empresas_sql = text("""
            SELECT 
                EP.ID_EMPRESA,
                EP.NO_EMPRESA, 
                EP.NO_EMPRESA_ABREVIADA,
                EP.DS_CONDICAO
            FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES] EP
            WHERE EP.ID_EDITAL = :edital_id
            AND EP.ID_PERIODO = :periodo_id
            AND EP.DS_CONDICAO <> 'DESCREDENCIADA NO PERÍODO'
            AND EP.DELETED_AT IS NULL
            ORDER BY EP.ID_EMPRESA
            """)

            empresas_result = connection.execute(empresas_sql, {
                "edital_id": edital_id,
                "periodo_id": periodo_id
            }).fetchall()

            # Log do número de empresas encontradas
            if empresas_result:
                logging.info(f"Encontradas {len(empresas_result)} empresas participantes")
            else:
                logging.warning("Nenhuma empresa participante encontrada")

                # Tentativa alternativa: buscar empresas de qualquer período
                empresas_alt_sql = text("""
                SELECT 
                    EP.ID_EMPRESA,
                    EP.NO_EMPRESA, 
                    EP.NO_EMPRESA_ABREVIADA,
                    EP.DS_CONDICAO
                FROM [DEV].[DCA_TB002_EMPRESAS_PARTICIPANTES] EP
                WHERE EP.ID_EDITAL = :edital_id
                AND EP.DELETED_AT IS NULL
                ORDER BY EP.ID_EMPRESA
                """)

                empresas_result = connection.execute(empresas_alt_sql, {
                    "edital_id": edital_id
                }).fetchall()

                if empresas_result:
                    logging.info(f"Encontradas {len(empresas_result)} empresas em períodos alternativos")
                else:
                    logging.error("Nenhuma empresa encontrada em nenhum período")
                    return 0, 0, []

            # Criar lista de empresas base com valores zerados
            empresas_dados = []
            for empresa in empresas_result:
                emp_id, emp_nome, emp_abreviada, emp_condicao = empresa

                # Pular a empresa que está saindo
                if emp_id == empresa_id:
                    continue

                empresas_dados.append({
                    "id_empresa": emp_id,
                    "nome": emp_nome,
                    "nome_abreviado": emp_abreviada,
                    "condicao": emp_condicao,
                    "vr_arrecadacao": 0.0,
                    "percentual": 0.0
                })

            if not empresas_dados:
                logging.error("Nenhuma empresa remanescente encontrada após filtrar")
                return 0, 0, []

            # ESTRATÉGIA 2: Buscar arrecadação da tabela de remuneração
            logging.info("Buscando dados de arrecadação na tabela COM_TB062_REMUNERACAO_ESTIMADA")

            # Primeiro verificar se a tabela existe e tem dados no período
            check_table_sql = text("""
            SELECT COUNT(*) FROM [BDG].[COM_TB062_REMUNERACAO_ESTIMADA]
            WHERE DT_ARRECADACAO BETWEEN :dt_inicio AND :dt_fim
            """)

            count_result = connection.execute(check_table_sql, {
                "dt_inicio": dt_inicio,
                "dt_fim": dt_fim
            }).scalar() or 0

            logging.info(f"Tabela de arrecadação contém {count_result} registros no período")

            if count_result > 0:
                # Buscar arrecadação por empresa
                arrecadacao_sql = text("""
                SELECT 
                    REE.CO_EMPRESA_COBRANCA AS ID_EMPRESA,
                    SUM(REE.VR_ARRECADACAO_TOTAL) AS VR_ARRECADACAO
                FROM [BDG].[COM_TB062_REMUNERACAO_ESTIMADA] AS REE
                WHERE REE.DT_ARRECADACAO BETWEEN :dt_inicio AND :dt_fim
                GROUP BY REE.CO_EMPRESA_COBRANCA
                """)

                arrecadacao_results = connection.execute(arrecadacao_sql, {
                    "dt_inicio": dt_inicio,
                    "dt_fim": dt_fim
                }).fetchall()

                if arrecadacao_results:
                    logging.info(f"Encontrados dados de arrecadação para {len(arrecadacao_results)} empresas")

                    # Preencher valores de arrecadação
                    for result in arrecadacao_results:
                        emp_id, vr_arrec = result
                        for empresa in empresas_dados:
                            if empresa["id_empresa"] == emp_id:
                                empresa["vr_arrecadacao"] = to_float(vr_arrec) or 0.0

                    # Calcular percentuais baseados na arrecadação total
                    total_arrecadacao = sum(empresa["vr_arrecadacao"] for empresa in empresas_dados)

                    if total_arrecadacao > 0:
                        for empresa in empresas_dados:
                            empresa["percentual"] = (empresa["vr_arrecadacao"] / total_arrecadacao) * 100.0

                    # Buscar arrecadação da empresa que está saindo
                    empresa_saindo_sql = text("""
                    SELECT 
                        SUM(REE.VR_ARRECADACAO_TOTAL) AS VR_ARRECADACAO
                    FROM [BDG].[COM_TB062_REMUNERACAO_ESTIMADA] AS REE
                    WHERE REE.DT_ARRECADACAO BETWEEN :dt_inicio AND :dt_fim
                    AND REE.CO_EMPRESA_COBRANCA = :empresa_id
                    """)

                    empresa_result = connection.execute(empresa_saindo_sql, {
                        "dt_inicio": dt_inicio,
                        "dt_fim": dt_fim,
                        "empresa_id": empresa_id
                    }).scalar() or 0

                    valor_empresa_saindo = to_float(empresa_result)

                    # Calcular percentual da empresa que está saindo
                    if total_arrecadacao > 0 and valor_empresa_saindo > 0:
                        percentual_redistribuido = (valor_empresa_saindo / (
                                    total_arrecadacao + valor_empresa_saindo)) * 100.0
                        logging.info(f"Percentual da empresa {empresa_id} calculado: {percentual_redistribuido:.2f}%")
                    else:
                        percentual_redistribuido = 0.0
                else:
                    logging.warning("Nenhum dado de arrecadação encontrado para as empresas")
            else:
                logging.warning("Nenhum registro na tabela de arrecadação para o período especificado")

            # ESTRATÉGIA 3: Se não encontrou dados de arrecadação, buscar na tabela de limites
            total_percentual = sum(empresa["percentual"] for empresa in empresas_dados)
            percentual_redistribuido_encontrado = percentual_redistribuido if 'percentual_redistribuido' in locals() else 0.0

            if total_percentual <= 0 or percentual_redistribuido_encontrado <= 0:
                logging.info("Buscando percentuais da tabela de limites")

                # Buscar limites cadastrados
                limites_sql = text("""
                SELECT ID_EMPRESA, PERCENTUAL_FINAL
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND DELETED_AT IS NULL
                """)

                limites_results = connection.execute(limites_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id
                }).fetchall()

                if limites_results:
                    logging.info(f"Encontrados {len(limites_results)} limites de distribuição")

                    # Preencher percentuais dos limites
                    for result in limites_results:
                        emp_id, percentual = result

                        # Verificar se é a empresa que está saindo
                        if emp_id == empresa_id:
                            if percentual:
                                percentual_redistribuido = to_float(percentual)
                                logging.info(
                                    f"Percentual da empresa {empresa_id} encontrado na tabela de limites: {percentual_redistribuido:.2f}%")
                        else:
                            for empresa in empresas_dados:
                                if empresa["id_empresa"] == emp_id and percentual:
                                    empresa["percentual"] = to_float(percentual)

                # Verificar se o percentual da empresa que sai foi encontrado
                if percentual_redistribuido_encontrado <= 0 and 'percentual_redistribuido' in locals() and percentual_redistribuido > 0:
                    percentual_redistribuido_encontrado = percentual_redistribuido

            # ESTRATÉGIA 4 (FINAL): Se ainda não encontrou percentuais, definir valores padrão
            total_percentual = sum(empresa["percentual"] for empresa in empresas_dados)

            if total_percentual <= 0:
                logging.warning("Nenhum percentual válido encontrado, definindo percentuais iguais")
                percentual_igual = 100.0 / max(1, len(empresas_dados))

                for empresa in empresas_dados:
                    empresa["percentual"] = percentual_igual

            # Normalizar percentuais para somar 100%
            total_atual = sum(empresa["percentual"] for empresa in empresas_dados)
            if total_atual > 0 and abs(total_atual - 100.0) > 0.01:
                fator = 100.0 / total_atual
                for empresa in empresas_dados:
                    empresa["percentual"] *= fator

            # Se ainda não tem percentual da empresa que sai, usar valor padrão
            if percentual_redistribuido_encontrado <= 0:
                percentual_redistribuido = 5.0
                logging.warning(f"Usando percentual padrão para redistribuição: {percentual_redistribuido:.2f}%")
            else:
                percentual_redistribuido = percentual_redistribuido_encontrado

            # Calcular total de arrecadação
            total_arrecadacao = sum(empresa["vr_arrecadacao"] for empresa in empresas_dados)

            # Se total de arrecadação é zero, definir um valor padrão
            if total_arrecadacao <= 0:
                total_arrecadacao = 1000000.0  # Valor fictício apenas para que os cálculos prossigam
                logging.warning(f"Definindo valor padrão para arrecadação total: {total_arrecadacao}")

            # Log detalhado de resultados
            logging.info(f"Empresas remanescentes: {len(empresas_dados)}")
            logging.info(f"Percentual a redistribuir: {percentual_redistribuido:.2f}%")
            logging.info(f"Total de arrecadação: {total_arrecadacao:.2f}")

            return percentual_redistribuido, total_arrecadacao, empresas_dados

    except Exception as e:
        logging.error(f"Erro ao calcular percentuais para redistribuição: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return 0, 0, []


def redistribuir_percentuais(edital_id, periodo_id, empresa_id, cod_criterio, dt_apuracao=None):
    """
    Redistribui percentuais e cria novos limites de distribuição.

    Args:
        edital_id: ID do edital
        periodo_id: ID do período
        empresa_id: ID da empresa que está saindo
        cod_criterio: Código do critério de redistribuição
        dt_apuracao: Data de apuração (opcional)

    Returns:
        bool: True se a redistribuição foi bem-sucedida
    """
    # Função vazia, retornando False
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
    # Função vazia, retornando dicionário com valores padrão
    resultados = {
        "contratos_redistribuidos": 0,
        "contratos_arrastados": 0,
        "contratos_restantes": 0,
        "total_empresas": 0,
        "empresa_redistribuida": empresa_id,
        "success": False
    }

    return resultados

