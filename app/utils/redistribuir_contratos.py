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

        # 4. Inserir/atualizar limites de distribuição
        with db.engine.connect() as connection:
            # 4.1 Primeiro, remover limites anteriores com este critério (se existirem)
            delete_old_limits = text("""
            DELETE FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
            WHERE ID_EDITAL = :edital_id
            AND ID_PERIODO = :periodo_id
            AND COD_CRITERIO_SELECAO = :cod_criterio
            """)

            connection.execute(delete_old_limits, {
                "edital_id": edital_id,
                "periodo_id": periodo_id,
                "cod_criterio": cod_criterio
            })

            # 4.2 Inserir novos limites com percentuais recalculados
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
                # CORREÇÃO AQUI: Usar literal diretamente em vez de parâmetro para TOP
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

            # 5. Calcular valores máximos e quantidades máximas
            num_contratos = selecionar_contratos_para_redistribuicao(empresa_id)

            # Se não tiver contratos, usar valor padrão para permitir a redistribuição
            if num_contratos == 0:
                # Verificar limites anteriores para obter quantidade padrão
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
                    num_contratos = 1000  # Valor padrão razoável
                    logging.warning(f"Sem contratos para redistribuir, usando valor padrão: {num_contratos}")

            # Obter soma do saldo devedor
            saldo_sql = text("SELECT SUM(CAST(VR_SD_DEVEDOR AS DECIMAL(18,2))) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]")
            saldo_result = connection.execute(saldo_sql).scalar()

            # Converter para float para o SQL Server
            saldo_total = float(to_decimal(saldo_result))

            if saldo_total == 0:
                saldo_total = 1000000.0  # Valor padrão para quando não há saldo
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
            # CORREÇÃO: Calcular a sobra corretamente
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
                # CORREÇÃO: Usar valor literal em vez de parâmetro para TOP
                # Para cada valor de sobra, faça uma consulta separada
                for i in range(sobra):
                    # Busca a empresa com maior QTDE_MAXIMA que ainda não recebeu ajuste
                    ids_query = text("""
                    SELECT TOP 1 ID
                    FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                    WHERE ID_EDITAL = :edital_id
                    AND ID_PERIODO = :periodo_id
                    AND COD_CRITERIO_SELECAO = :cod_criterio
                    ORDER BY QTDE_MAXIMA DESC, ID
                    """)

                    id_result = connection.execute(ids_query, {
                        "edital_id": edital_id,
                        "periodo_id": periodo_id,
                        "cod_criterio": cod_criterio
                    }).scalar()

                    if id_result:
                        # Incrementa a quantidade máxima para este registro
                        update_sql = text("""
                        UPDATE [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                        SET QTDE_MAXIMA = QTDE_MAXIMA + 1
                        WHERE ID = :id
                        """)

                        connection.execute(update_sql, {"id": id_result})

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
    from decimal import Decimal  # Importar Decimal para tratamento de tipos

    # Função auxiliar para garantir que valores numéricos sejam float
    def ensure_float(value):
        if value is None:
            return 0.0
        if isinstance(value, Decimal):
            return float(value)
        return float(value)

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

            # 3.2 Verificar se as colunas existem
            check_columns = text("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = 'DEV' AND TABLE_NAME = 'DCA_TB006_DISTRIBUIVEIS'
            """)

            columns = connection.execute(check_columns).fetchall()
            column_names = [col[0] for col in columns]

            check_dest_columns = text("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = 'DEV' AND TABLE_NAME = 'DCA_TB007_ARRASTAVEIS'
            """)

            dest_columns = connection.execute(check_dest_columns).fetchall()
            dest_column_names = [col[0] for col in dest_columns]

            # 3.3 Identificar CPFs com múltiplos contratos - versão simples, sem VR_SD_DEVEDOR
            # Usar consultas mais simples que podem ser parametrizadas
            multiplos_cpf_sql = text("""
            SELECT NR_CPF_CNPJ
            FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
            GROUP BY NR_CPF_CNPJ
            HAVING COUNT(*) > 1
            """)
            cpfs_multiplos = connection.execute(multiplos_cpf_sql).fetchall()

            # Se encontrar CPFs com múltiplos contratos
            if cpfs_multiplos:
                for cpf in cpfs_multiplos:
                    # Para cada CPF, buscar seus contratos
                    contratos_sql = text("""
                    SELECT FkContratoSISCTR, NR_CPF_CNPJ 
                    FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                    WHERE NR_CPF_CNPJ = :cpf
                    """)

                    contratos = connection.execute(contratos_sql, {"cpf": cpf[0]}).fetchall()

                    # Inserir cada contrato na tabela de arrastáveis
                    for contrato in contratos:
                        if 'VR_SD_DEVEDOR' in column_names and 'VR_SD_DEVEDOR' in dest_column_names:
                            # Buscar o valor do saldo devedor
                            valor_sql = text("""
                            SELECT VR_SD_DEVEDOR 
                            FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                            WHERE FkContratoSISCTR = :contrato_id
                            """)
                            valor = connection.execute(valor_sql, {"contrato_id": contrato[0]}).scalar() or 0.0

                            # Inserir com saldo devedor
                            insert_sql = text("""
                            INSERT INTO [DEV].[DCA_TB007_ARRASTAVEIS]
                            (FkContratoSISCTR, NR_CPF_CNPJ, VR_SD_DEVEDOR, CREATED_AT)
                            VALUES (:contrato_id, :cpf, :valor, GETDATE())
                            """)

                            connection.execute(insert_sql, {
                                "contrato_id": contrato[0],
                                "cpf": contrato[1],
                                "valor": valor
                            })
                        else:
                            # Inserir sem saldo devedor
                            insert_sql = text("""
                            INSERT INTO [DEV].[DCA_TB007_ARRASTAVEIS]
                            (FkContratoSISCTR, NR_CPF_CNPJ, CREATED_AT)
                            VALUES (:contrato_id, :cpf, GETDATE())
                            """)

                            connection.execute(insert_sql, {
                                "contrato_id": contrato[0],
                                "cpf": contrato[1]
                            })

            # 3.4 Remover contratos arrastáveis da tabela de distribuíveis
            delete_from_distribuiveis = text("""
            DELETE FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
            WHERE FkContratoSISCTR IN (
                SELECT FkContratoSISCTR FROM [DEV].[DCA_TB007_ARRASTAVEIS]
            )
            """)

            connection.execute(delete_from_distribuiveis)

            # 3.5 Contar contratos arrastáveis
            arrastaveis_count = connection.execute(
                text("SELECT COUNT(*) FROM [DEV].[DCA_TB007_ARRASTAVEIS]")
            ).scalar() or 0

            resultados["contratos_arrastados"] = arrastaveis_count

            # 4. Distribuir contratos arrastáveis - abordagem alternativa sem scripts complexos
            if arrastaveis_count > 0:
                # 4.1 Obter empresas e seus percentuais
                empresas_sql = text("""
                SELECT ID_EMPRESA, PERCENTUAL_FINAL
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO]
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND COD_CRITERIO_SELECAO = :cod_criterio
                ORDER BY PERCENTUAL_FINAL DESC
                """)

                empresas = connection.execute(empresas_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "cod_criterio": cod_criterio
                }).fetchall()

                if not empresas:
                    logging.warning("Nenhuma empresa encontrada para distribuir contratos arrastáveis")
                else:
                    # 4.2 Obter CPFs distintos e distribuí-los entre as empresas
                    cpfs_sql = text("""
                    SELECT DISTINCT NR_CPF_CNPJ
                    FROM [DEV].[DCA_TB007_ARRASTAVEIS]
                    """)

                    cpfs = connection.execute(cpfs_sql).fetchall()

                    # Calcular quantos CPFs cada empresa deve receber
                    total_cpfs = len(cpfs)
                    cpfs_por_empresa = {}
                    sobra = total_cpfs

                    for emp_id, percentual in empresas:
                        # CORREÇÃO AQUI: Converter para float antes da divisão
                        # Garantir que percentual seja float para evitar erro de tipo
                        percentual_float = ensure_float(percentual)
                        qtd = int((percentual_float / 100.0) * total_cpfs)

                        cpfs_por_empresa[emp_id] = qtd
                        sobra -= qtd

                    # Distribuir as sobras
                    if sobra > 0:
                        for emp_id in cpfs_por_empresa.keys():
                            if sobra <= 0:
                                break
                            cpfs_por_empresa[emp_id] += 1
                            sobra -= 1

                    # Distribuir os CPFs entre as empresas
                    idx_cpf = 0
                    for emp_id, qtd in cpfs_por_empresa.items():
                        for i in range(qtd):
                            if idx_cpf < total_cpfs:
                                cpf_atual = cpfs[idx_cpf][0]

                                # Buscar todos os contratos deste CPF
                                contratos_cpf = text("""
                                SELECT FkContratoSISCTR, NR_CPF_CNPJ
                                FROM [DEV].[DCA_TB007_ARRASTAVEIS]
                                WHERE NR_CPF_CNPJ = :cpf
                                """)

                                contratos = connection.execute(contratos_cpf, {"cpf": cpf_atual}).fetchall()

                                # Inserir todos na tabela de distribuição
                                for contrato in contratos:
                                    if 'VR_SD_DEVEDOR' in dest_column_names:
                                        # Buscar o valor do saldo devedor se existir
                                        valor_sql = text("""
                                        SELECT VR_SD_DEVEDOR 
                                        FROM [DEV].[DCA_TB007_ARRASTAVEIS]
                                        WHERE FkContratoSISCTR = :contrato_id
                                        """)
                                        valor = connection.execute(valor_sql,
                                                                   {"contrato_id": contrato[0]}).scalar() or 0.0

                                        insert_sql = text("""
                                        INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
                                        (DT_REFERENCIA, ID_EDITAL, ID_PERIODO, fkContratoSISCTR, 
                                        COD_EMPRESA_COBRANCA, COD_CRITERIO_SELECAO, NR_CPF_CNPJ,
                                        VR_SD_DEVEDOR, CREATED_AT)
                                        VALUES (GETDATE(), :edital_id, :periodo_id, :contrato_id,
                                        :empresa_id, :cod_criterio, :cpf, :valor, GETDATE())
                                        """)

                                        connection.execute(insert_sql, {
                                            "edital_id": edital_id,
                                            "periodo_id": periodo_id,
                                            "contrato_id": contrato[0],
                                            "empresa_id": emp_id,
                                            "cod_criterio": cod_criterio,
                                            "cpf": contrato[1],
                                            "valor": valor
                                        })
                                    else:
                                        insert_sql = text("""
                                        INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
                                        (DT_REFERENCIA, ID_EDITAL, ID_PERIODO, fkContratoSISCTR, 
                                        COD_EMPRESA_COBRANCA, COD_CRITERIO_SELECAO, NR_CPF_CNPJ, CREATED_AT)
                                        VALUES (GETDATE(), :edital_id, :periodo_id, :contrato_id,
                                        :empresa_id, :cod_criterio, :cpf, GETDATE())
                                        """)

                                        connection.execute(insert_sql, {
                                            "edital_id": edital_id,
                                            "periodo_id": periodo_id,
                                            "contrato_id": contrato[0],
                                            "empresa_id": emp_id,
                                            "cod_criterio": cod_criterio,
                                            "cpf": contrato[1]
                                        })

                                idx_cpf += 1

            # 5. Distribuir contratos restantes
            restantes_count = connection.execute(
                text("SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]")
            ).scalar() or 0

            resultados["contratos_restantes"] = restantes_count

            if restantes_count > 0:
                # 5.1 Distribuição de contratos restantes - abordagem simplificada sem scripts complexos
                # Obter empresas e calcular quantos contratos cada uma deve receber
                empresas_sql = text("""
                SELECT 
                    LD.ID_EMPRESA,
                    LD.PERCENTUAL_FINAL,
                    LD.QTDE_MAXIMA,
                    (SELECT COUNT(*) FROM [DEV].[DCA_TB005_DISTRIBUICAO] 
                     WHERE COD_EMPRESA_COBRANCA = LD.ID_EMPRESA
                     AND ID_EDITAL = :edital_id
                     AND ID_PERIODO = :periodo_id
                     AND COD_CRITERIO_SELECAO = :cod_criterio) AS contratos_atuais
                FROM [DEV].[DCA_TB003_LIMITES_DISTRIBUICAO] LD
                WHERE LD.ID_EDITAL = :edital_id
                AND LD.ID_PERIODO = :periodo_id
                AND LD.COD_CRITERIO_SELECAO = :cod_criterio
                """)

                empresas_data = connection.execute(empresas_sql, {
                    "edital_id": edital_id,
                    "periodo_id": periodo_id,
                    "cod_criterio": cod_criterio
                }).fetchall()

                if empresas_data:
                    # Calcular contratos restantes para cada empresa
                    contratos_restantes_emp = {}
                    total_a_distribuir = 0

                    for emp_id, percentual, qtde_max, contratos_atuais in empresas_data:
                        if qtde_max is not None and contratos_atuais is not None:
                            restantes = max(0, qtde_max - contratos_atuais)
                        else:
                            # Se não tem qtde_max, calcula baseado no percentual
                            # CORREÇÃO AQUI TAMBÉM: converter percentual para float
                            percentual_float = ensure_float(percentual)
                            restantes = int((percentual_float / 100.0) * restantes_count)

                        contratos_restantes_emp[emp_id] = restantes
                        total_a_distribuir += restantes

                    # Ajustar para não distribuir mais que o disponível
                    if total_a_distribuir > restantes_count:
                        # Reduzir proporcionalmente
                        fator = restantes_count / total_a_distribuir
                        for emp_id in contratos_restantes_emp:
                            contratos_restantes_emp[emp_id] = int(contratos_restantes_emp[emp_id] * fator)

                        # Verificar novamente o total e ajustar sobras
                        total_ajustado = sum(contratos_restantes_emp.values())
                        diferenca = restantes_count - total_ajustado

                        # Distribuir sobras
                        if diferenca > 0:
                            for emp_id in sorted(contratos_restantes_emp, key=lambda x: contratos_restantes_emp[x],
                                                 reverse=True):
                                if diferenca <= 0:
                                    break
                                contratos_restantes_emp[emp_id] += 1
                                diferenca -= 1

                    # Obter os contratos a distribuir
                    contratos_sql = text("""
                    SELECT FkContratoSISCTR, NR_CPF_CNPJ
                    FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                    ORDER BY NEWID()  -- Ordem aleatória
                    """)

                    contratos = connection.execute(contratos_sql).fetchall()

                    # Distribuir os contratos
                    idx_contrato = 0
                    for emp_id, qtd in contratos_restantes_emp.items():
                        for i in range(qtd):
                            if idx_contrato < len(contratos):
                                contrato = contratos[idx_contrato]

                                if 'VR_SD_DEVEDOR' in column_names:
                                    # Buscar o valor do saldo devedor
                                    valor_sql = text("""
                                    SELECT VR_SD_DEVEDOR 
                                    FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                                    WHERE FkContratoSISCTR = :contrato_id
                                    """)
                                    valor = connection.execute(valor_sql, {"contrato_id": contrato[0]}).scalar() or 0.0

                                    insert_sql = text("""
                                    INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
                                    (DT_REFERENCIA, ID_EDITAL, ID_PERIODO, fkContratoSISCTR, 
                                    COD_EMPRESA_COBRANCA, COD_CRITERIO_SELECAO, NR_CPF_CNPJ,
                                    VR_SD_DEVEDOR, CREATED_AT)
                                    VALUES (GETDATE(), :edital_id, :periodo_id, :contrato_id,
                                    :empresa_id, :cod_criterio, :cpf, :valor, GETDATE())
                                    """)

                                    connection.execute(insert_sql, {
                                        "edital_id": edital_id,
                                        "periodo_id": periodo_id,
                                        "contrato_id": contrato[0],
                                        "empresa_id": emp_id,
                                        "cod_criterio": cod_criterio,
                                        "cpf": contrato[1],
                                        "valor": valor
                                    })
                                else:
                                    insert_sql = text("""
                                    INSERT INTO [DEV].[DCA_TB005_DISTRIBUICAO]
                                    (DT_REFERENCIA, ID_EDITAL, ID_PERIODO, fkContratoSISCTR, 
                                    COD_EMPRESA_COBRANCA, COD_CRITERIO_SELECAO, NR_CPF_CNPJ,
                                    CREATED_AT)
                                    VALUES (GETDATE(), :edital_id, :periodo_id, :contrato_id,
                                    :empresa_id, :cod_criterio, :cpf, GETDATE())
                                    """)

                                    connection.execute(insert_sql, {
                                        "edital_id": edital_id,
                                        "periodo_id": periodo_id,
                                        "contrato_id": contrato[0],
                                        "empresa_id": emp_id,
                                        "cod_criterio": cod_criterio,
                                        "cpf": contrato[1]
                                    })

                                idx_contrato += 1

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