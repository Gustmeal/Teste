# app/utils/meta_calculator.py
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from sqlalchemy import text
from app import db
from app.models.meta_avaliacao import MetaAvaliacao, MetaSemestral
import calendar


class MetaCalculator:
    def __init__(self, edital_id, periodo_id, fator_incremento=1.00):
        self.edital_id = edital_id
        self.periodo_id = periodo_id
        self.periodo_info = None
        self.meses_periodo = []
        self.dias_uteis_periodo = {}
        self.metas_siscor = {}
        self.metas_diarias = {}
        self.metas_periodo = {}
        self.metas_estendidas = {}
        self.total_saldo_devedor = Decimal('0')
        self.incremento_meta = Decimal(str(fator_incremento))
        self.total_meta_siscor = Decimal('0')
        self.total_dias_uteis_periodo = 0
        self.meta_por_dia_util = Decimal('0')

        # Datas das redistribuições
        self.data_inicial = date(2025, 1, 15)
        self.data_redistribuicao_1 = date(2025, 3, 26)  # Real sai
        self.data_redistribuicao_2 = date(2025, 5, 16)  # H.Costa sai

    def calcular_metas_completas(self):
        """
        Executa todo o processo de cálculo de metas com redistribuições proporcionais
        """
        # 1. Obter informações do período
        self.periodo_info = self._obter_periodo()
        if not self.periodo_info:
            raise ValueError("Período não encontrado")

        # 2. Identificar meses do período
        self.meses_periodo = self._obter_meses_periodo()

        # 3. Calcular dias úteis de cada mês
        self._calcular_dias_uteis_todos_meses()

        # 4. Obter metas SISCOR de cada mês
        self._obter_metas_siscor_todos_meses()

        # 5. Calcular metas do período avaliativo
        self._calcular_metas_periodo_novo()

        # 6. Calcular metas estendidas
        self._calcular_metas_estendidas()

        print("\n=== INICIANDO CÁLCULO DE METAS COM REDISTRIBUIÇÕES PROPORCIONAIS ===")

        # Executar o cálculo com proporcionalidade
        resultado_final = self._executar_calculo_proporcional()

        return resultado_final

    def _obter_periodo(self):
        """Obtém dados do período"""
        sql = text("""
            SELECT p.ID, p.ID_PERIODO, p.DT_INICIO, p.DT_FIM,
                   e.NU_EDITAL, e.ANO
            FROM BDG.DCA_TB001_PERIODO_AVALIACAO p
            JOIN BDG.DCA_TB008_EDITAIS e ON p.ID_EDITAL = e.ID
            WHERE p.ID = :periodo_id
            AND p.ID_EDITAL = :edital_id
            AND p.DELETED_AT IS NULL
        """)

        result = db.session.execute(sql, {
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_id
        }).fetchone()

        if not result:
            return None

        return {
            'id': result[0],
            'id_periodo': result[1],
            'dt_inicio': result[2],
            'dt_fim': result[3],
            'nu_edital': result[4],
            'ano_edital': result[5]
        }

    def _obter_meses_periodo(self):
        """Retorna lista de meses no período"""
        meses = []
        data_atual = self.periodo_info['dt_inicio']
        data_fim = self.periodo_info['dt_fim']

        meses_portugues = {
            1: 'JAN', 2: 'FEV', 3: 'MAR', 4: 'ABR',
            5: 'MAI', 6: 'JUN', 7: 'JUL', 8: 'AGO',
            9: 'SET', 10: 'OUT', 11: 'NOV', 12: 'DEZ'
        }

        data_atual = data_atual.replace(day=1)

        while data_atual <= data_fim:
            meses.append({
                'ano': data_atual.year,
                'mes': data_atual.month,
                'ano_mes': data_atual.strftime('%Y-%m'),
                'nome_mes': meses_portugues[data_atual.month]
            })
            data_atual = data_atual + relativedelta(months=1)

        return meses

    def _calcular_dias_uteis_todos_meses(self):
        """Calcula dias úteis de todos os meses do período"""
        self.total_dias_uteis_periodo = 0

        for mes in self.meses_periodo:
            sql_total = text("""
                SELECT COUNT(*) as QTDE_DIAS_UTEIS
                FROM BDG.AUX_TB004_CALENDARIO
                WHERE ANO = :ano 
                AND MES = :mes
                AND E_DIA_UTIL = 1
            """)

            result_total = db.session.execute(sql_total, {
                'ano': mes['ano'],
                'mes': mes['mes']
            }).fetchone()

            dias_uteis_total = result_total[0] if result_total and result_total[0] else 22

            sql_periodo = text("""
                SELECT COUNT(*) as QTDE_DIAS_UTEIS
                FROM BDG.AUX_TB004_CALENDARIO
                WHERE ANO = :ano 
                AND MES = :mes
                AND E_DIA_UTIL = 1
                AND DT_REFERENCIA BETWEEN :dt_inicio AND :dt_fim
            """)

            primeiro_dia_mes = date(mes['ano'], mes['mes'], 1)
            ultimo_dia_mes = date(mes['ano'], mes['mes'], calendar.monthrange(mes['ano'], mes['mes'])[1])

            dt_inicio_calc = max(self.periodo_info['dt_inicio'], primeiro_dia_mes)
            dt_fim_calc = min(self.periodo_info['dt_fim'], ultimo_dia_mes)

            result_periodo = db.session.execute(sql_periodo, {
                'ano': mes['ano'],
                'mes': mes['mes'],
                'dt_inicio': dt_inicio_calc,
                'dt_fim': dt_fim_calc
            }).fetchone()

            dias_uteis_periodo = result_periodo[0] if result_periodo and result_periodo[0] else 0
            self.total_dias_uteis_periodo += dias_uteis_periodo

            self.dias_uteis_periodo[mes['ano_mes']] = {
                'total': dias_uteis_total,
                'periodo': dias_uteis_periodo
            }

    def _obter_metas_siscor_todos_meses(self):
        """Obtém metas SISCOR de todos os meses"""
        self.total_meta_siscor = Decimal('0')

        for mes in self.meses_periodo:
            sql = text("""
                WITH FaseAtual AS (
                    SELECT MAX(ID_TIPO_FASE_ORC) as FASE
                    FROM BDG.COR_TB002_REPROGRAMACAO_ORCAMENTARIA_SISCOR
                    WHERE DT_PREVISAO_ORCAMENTO/100 = :ano
                )
                SELECT SUM(VR_PREVISAO_ORCAMENTO) as META_TOTAL
                FROM BDG.COR_TB002_REPROGRAMACAO_ORCAMENTARIA_SISCOR, FaseAtual
                WHERE ID_NATUREZA = 3
                AND UNIDADE = 'SUPEC'
                AND DT_PREVISAO_ORCAMENTO/100 = :ano
                AND DT_PREVISAO_ORCAMENTO - (DT_PREVISAO_ORCAMENTO/100 * 100) = :mes
                AND ID_TIPO_FASE_ORC = FaseAtual.FASE
            """)

            result = db.session.execute(sql, {
                'ano': mes['ano'],
                'mes': mes['mes']
            }).fetchone()

            meta_valor = Decimal(str(result[0])) if result and result[0] else Decimal('0')
            self.metas_siscor[mes['ano_mes']] = meta_valor  # AQUI ESTAVA O ERRO - COLCHETE CORRIGIDO
            self.total_meta_siscor += meta_valor

    def _calcular_metas_periodo_novo(self):
        """Calcula metas do período usando a lógica correta"""
        if self.total_dias_uteis_periodo > 0:
            self.meta_por_dia_util = (self.total_meta_siscor / self.total_dias_uteis_periodo).quantize(
                Decimal('0.01'), rounding=ROUND_HALF_UP
            )
        else:
            self.meta_por_dia_util = Decimal('0')

        for mes in self.meses_periodo:
            ano_mes = mes['ano_mes']
            dias_periodo = self.dias_uteis_periodo[ano_mes]['periodo']

            meta_periodo = (self.meta_por_dia_util * dias_periodo).quantize(
                Decimal('0.01'), rounding=ROUND_HALF_UP
            )

            self.metas_periodo[ano_mes] = meta_periodo

    def _calcular_metas_estendidas(self):
        """Calcula metas estendidas (meta período × incremento)"""
        for mes in self.meses_periodo:
            ano_mes = mes['ano_mes']
            meta_periodo = self.metas_periodo[ano_mes]

            meta_estendida = meta_periodo * self.incremento_meta
            meta_estendida = meta_estendida.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

            self.metas_estendidas[ano_mes] = meta_estendida

    def _calcular_dias_uteis_proporcional(self, ano, mes, data_corte, ate_data=True):
        """
        Calcula dias úteis proporcionais em um mês
        ate_data=True: conta do início do mês até a data_corte
        ate_data=False: conta da data_corte+1 até o fim do mês
        """
        if ate_data:
            # Do início do mês até a data de corte
            dt_inicio = date(ano, mes, 1)
            dt_fim = data_corte
        else:
            # Do dia seguinte à data de corte até o fim do mês
            dt_inicio = data_corte + relativedelta(days=1)
            dt_fim = date(ano, mes, calendar.monthrange(ano, mes)[1])

        sql = text("""
            SELECT COUNT(*) as QTDE_DIAS_UTEIS
            FROM BDG.AUX_TB004_CALENDARIO
            WHERE ANO = :ano 
            AND MES = :mes
            AND E_DIA_UTIL = 1
            AND DT_REFERENCIA BETWEEN :dt_inicio AND :dt_fim
        """)

        result = db.session.execute(sql, {
            'ano': ano,
            'mes': mes,
            'dt_inicio': dt_inicio,
            'dt_fim': dt_fim
        }).fetchone()

        return result[0] if result and result[0] else 0

    def _executar_calculo_proporcional(self):
        """
        Executa o cálculo com redistribuições proporcionais aos dias úteis
        IMPORTANTE: Usa os SDs e percentuais corretos de cada etapa
        """

        # ========== ETAPA 1: Cálculo inicial (15/01/2025) ==========
        print("\n=== ETAPA 1: Cálculo inicial (15/01/2025) - Todas empresas exceto Savas_Heinzen ===")

        # Buscar distribuições da data inicial
        sql_etapa1 = text("""
            SELECT DISTINCT 
                DIS.COD_EMPRESA_COBRANCA,
                EMP.[NO_EMPRESA_ABREVIADA],
                EMP.[DS_CONDICAO],
                COUNT(*) as QTD_CONTRATOS,
                SUM(DIS.[VR_SD_DEVEDOR]) as TOTAL_SD
            FROM [BDG].[DCA_TB005_DISTRIBUICAO] AS DIS
            INNER JOIN [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES] AS EMP
                ON DIS.ID_EDITAL = EMP.ID_EDITAL
                AND DIS.ID_PERIODO = EMP.ID_PERIODO
                AND DIS.COD_EMPRESA_COBRANCA = EMP.ID_EMPRESA
            WHERE DIS.ID_EDITAL = :edital_id
            AND DIS.ID_PERIODO = :periodo_id
            AND DIS.DT_REFERENCIA <= '2025-01-15'
            AND EMP.NO_EMPRESA_ABREVIADA <> 'Savas_Heinzen'
            GROUP BY DIS.COD_EMPRESA_COBRANCA, EMP.[NO_EMPRESA_ABREVIADA], EMP.[DS_CONDICAO]
            ORDER BY EMP.[NO_EMPRESA_ABREVIADA]
        """)

        result = db.session.execute(sql_etapa1, {
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_info['id_periodo']
        })

        # Mapear todas as empresas com seus dados da ETAPA 1
        empresas_etapa1 = {}
        total_sd_etapa1 = Decimal('0')

        for row in result:
            id_empresa = row[0]
            saldo = Decimal(str(row[4]))
            empresas_etapa1[id_empresa] = {
                'id_empresa': id_empresa,
                'nome_abreviado': row[1],
                'condicao': row[2],
                'qtd_contratos': row[3],
                'saldo_devedor': saldo,
                'percentual': Decimal('0')
            }
            total_sd_etapa1 += saldo

        # Calcular percentuais da ETAPA 1
        for emp_id, empresa in empresas_etapa1.items():
            if total_sd_etapa1 > 0:
                empresa['percentual'] = (empresa['saldo_devedor'] / total_sd_etapa1 * 100).quantize(
                    Decimal('0.00000001'), rounding=ROUND_HALF_UP
                )

        print(f"Total SD Etapa 1: R$ {float(total_sd_etapa1):,.2f}")
        for empresa in empresas_etapa1.values():
            print(
                f"  {empresa['nome_abreviado']}: SD={float(empresa['saldo_devedor']):,.2f}, %={float(empresa['percentual']):.8f}%")

        # ========== ETAPA 2: Real sai (26/03/2025) ==========
        print("\n=== ETAPA 2: Redistribuição (26/03/2025) - Real sai ===")

        sql_etapa2 = text("""
            SELECT DISTINCT 
                DIS.COD_EMPRESA_COBRANCA,
                EMP.[NO_EMPRESA_ABREVIADA],
                EMP.[DS_CONDICAO],
                COUNT(*) as QTD_CONTRATOS,
                SUM(DIS.[VR_SD_DEVEDOR]) as TOTAL_SD
            FROM [BDG].[DCA_TB005_DISTRIBUICAO] AS DIS
            INNER JOIN [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES] AS EMP
                ON DIS.ID_EDITAL = EMP.ID_EDITAL
                AND DIS.ID_PERIODO = EMP.ID_PERIODO
                AND DIS.COD_EMPRESA_COBRANCA = EMP.ID_EMPRESA
            WHERE DIS.ID_EDITAL = :edital_id
            AND DIS.ID_PERIODO = :periodo_id
            AND DIS.DT_REFERENCIA <= '2025-03-26'
            AND EMP.NO_EMPRESA_ABREVIADA NOT IN ('Savas_Heinzen', 'Real')
            GROUP BY DIS.COD_EMPRESA_COBRANCA, EMP.[NO_EMPRESA_ABREVIADA], EMP.[DS_CONDICAO]
            ORDER BY EMP.[NO_EMPRESA_ABREVIADA]
        """)

        result = db.session.execute(sql_etapa2, {
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_info['id_periodo']
        })

        # Mapear empresas da ETAPA 2
        empresas_etapa2 = {}
        total_sd_etapa2 = Decimal('0')

        for row in result:
            id_empresa = row[0]
            saldo = Decimal(str(row[4]))
            empresas_etapa2[id_empresa] = {
                'nome_abreviado': row[1],
                'saldo_devedor': saldo,
                'percentual': Decimal('0')
            }
            total_sd_etapa2 += saldo

        # Calcular percentuais da ETAPA 2
        for emp_id, empresa in empresas_etapa2.items():
            if total_sd_etapa2 > 0:
                empresa['percentual'] = (empresa['saldo_devedor'] / total_sd_etapa2 * 100).quantize(
                    Decimal('0.00000001'), rounding=ROUND_HALF_UP
                )

        print(f"Total SD Etapa 2: R$ {float(total_sd_etapa2):,.2f}")
        for emp_id, empresa in empresas_etapa2.items():
            print(
                f"  {empresa['nome_abreviado']}: SD={float(empresa['saldo_devedor']):,.2f}, %={float(empresa['percentual']):.8f}%")

        # ========== ETAPA 3: H.Costa sai (16/05/2025) ==========
        print("\n=== ETAPA 3: Redistribuição (16/05/2025) - H.Costa sai ===")

        sql_etapa3 = text("""
            SELECT DISTINCT 
                DIS.COD_EMPRESA_COBRANCA,
                EMP.[NO_EMPRESA_ABREVIADA],
                EMP.[DS_CONDICAO],
                COUNT(*) as QTD_CONTRATOS,
                SUM(DIS.[VR_SD_DEVEDOR]) as TOTAL_SD
            FROM [BDG].[DCA_TB005_DISTRIBUICAO] AS DIS
            INNER JOIN [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES] AS EMP
                ON DIS.ID_EDITAL = EMP.ID_EDITAL
                AND DIS.ID_PERIODO = EMP.ID_PERIODO
                AND DIS.COD_EMPRESA_COBRANCA = EMP.ID_EMPRESA
            WHERE DIS.ID_EDITAL = :edital_id
            AND DIS.ID_PERIODO = :periodo_id
            AND DIS.DT_REFERENCIA <= '2025-05-16'
            AND EMP.NO_EMPRESA_ABREVIADA NOT IN ('Savas_Heinzen', 'Real', 'H.Costa')
            GROUP BY DIS.COD_EMPRESA_COBRANCA, EMP.[NO_EMPRESA_ABREVIADA], EMP.[DS_CONDICAO]
            ORDER BY EMP.[NO_EMPRESA_ABREVIADA]
        """)

        result = db.session.execute(sql_etapa3, {
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_info['id_periodo']
        })

        # Mapear empresas da ETAPA 3
        empresas_etapa3 = {}
        total_sd_etapa3 = Decimal('0')

        for row in result:
            id_empresa = row[0]
            saldo = Decimal(str(row[4]))
            empresas_etapa3[id_empresa] = {
                'nome_abreviado': row[1],
                'saldo_devedor': saldo,
                'percentual': Decimal('0')
            }
            total_sd_etapa3 += saldo

        # Calcular percentuais da ETAPA 3
        for emp_id, empresa in empresas_etapa3.items():
            if total_sd_etapa3 > 0:
                empresa['percentual'] = (empresa['saldo_devedor'] / total_sd_etapa3 * 100).quantize(
                    Decimal('0.00000001'), rounding=ROUND_HALF_UP
                )

        print(f"Total SD Etapa 3: R$ {float(total_sd_etapa3):,.2f}")
        for emp_id, empresa in empresas_etapa3.items():
            print(
                f"  {empresa['nome_abreviado']}: SD={float(empresa['saldo_devedor']):,.2f}, %={float(empresa['percentual']):.8f}%")

        # ========== CALCULAR METAS COM OS PERCENTUAIS CORRETOS ==========
        print("\n=== CALCULANDO METAS ===")

        # Estrutura para armazenar metas
        resultado_metas = {}

        # Inicializar estrutura para todas as empresas
        for id_empresa, empresa in empresas_etapa1.items():
            resultado_metas[id_empresa] = {
                'dados_originais': empresa.copy(),
                'metas_mensais': {},
                'ativo': True
            }

        # JANEIRO e FEVEREIRO - Usar percentuais da ETAPA 1
        print("\nCalculando JAN e FEV com percentuais da etapa 1:")
        for id_empresa, empresa in empresas_etapa1.items():
            percentual_decimal = empresa['percentual'] / Decimal('100')

            for i in range(2):  # JAN e FEV
                mes = self.meses_periodo[i]
                ano_mes = mes['ano_mes']
                meta_estendida = self.metas_estendidas[ano_mes]

                meta_arrecadacao = (meta_estendida * percentual_decimal).quantize(
                    Decimal('0.01'), rounding=ROUND_HALF_UP
                )

                resultado_metas[id_empresa]['metas_mensais'][ano_mes] = {
                    'meta_arrecadacao': float(meta_arrecadacao),
                    'meta_acionamento': None,
                    'meta_liquidacao': None,
                    'meta_bonificacao': float((meta_arrecadacao * Decimal('0.05')).quantize(
                        Decimal('0.01'), rounding=ROUND_HALF_UP
                    ))
                }

        # MARÇO - Cálculo proporcional (Real sai em 26/03)
        print("\nCalculando MARÇO com redistribuição em 26/03:")
        dias_marco_total = Decimal(str(self.dias_uteis_periodo['2025-03']['total']))
        dias_marco_ate_26 = Decimal(str(self._calcular_dias_uteis_proporcional(2025, 3, date(2025, 3, 26), True)))
        dias_marco_apos_26 = Decimal(str(self._calcular_dias_uteis_proporcional(2025, 3, date(2025, 3, 26), False)))
        print(f"Dias úteis março: total={dias_marco_total}, até 26={dias_marco_ate_26}, após 26={dias_marco_apos_26}")

        meta_estendida_marco = self.metas_estendidas['2025-03']

        for id_empresa, empresa in empresas_etapa1.items():
            if empresa['nome_abreviado'] == 'Real':
                # Real trabalha até 26/03 com percentual da etapa 1
                percentual_etapa1 = empresa['percentual'] / Decimal('100')
                meta_proporcional = (
                            meta_estendida_marco * percentual_etapa1 * dias_marco_ate_26 / dias_marco_total).quantize(
                    Decimal('0.01'), rounding=ROUND_HALF_UP
                )

                resultado_metas[id_empresa]['metas_mensais']['2025-03'] = {
                    'meta_arrecadacao': float(meta_proporcional),
                    'meta_acionamento': None,
                    'meta_liquidacao': None,
                    'meta_bonificacao': float((meta_proporcional * Decimal('0.05')).quantize(
                        Decimal('0.01'), rounding=ROUND_HALF_UP
                    ))
                }
                resultado_metas[id_empresa]['ativo'] = False

            elif id_empresa in empresas_etapa2:
                # Outras empresas: até 26 com % etapa1, após 26 com % etapa2
                percentual_etapa1 = empresa['percentual'] / Decimal('100')
                percentual_etapa2 = empresas_etapa2[id_empresa]['percentual'] / Decimal('100')

                meta_parte1 = meta_estendida_marco * percentual_etapa1 * dias_marco_ate_26 / dias_marco_total
                meta_parte2 = meta_estendida_marco * percentual_etapa2 * dias_marco_apos_26 / dias_marco_total
                meta_total = (meta_parte1 + meta_parte2).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

                resultado_metas[id_empresa]['metas_mensais']['2025-03'] = {
                    'meta_arrecadacao': float(meta_total),
                    'meta_acionamento': None,
                    'meta_liquidacao': None,
                    'meta_bonificacao': float((meta_total * Decimal('0.05')).quantize(
                        Decimal('0.01'), rounding=ROUND_HALF_UP
                    ))
                }

        # ABRIL - Usar percentuais da ETAPA 2 (sem Real)
        print("\nCalculando ABRIL com percentuais da etapa 2:")
        meta_estendida_abril = self.metas_estendidas['2025-04']

        for id_empresa in empresas_etapa1:
            if empresas_etapa1[id_empresa]['nome_abreviado'] == 'Real':
                # Real não recebe em abril
                resultado_metas[id_empresa]['metas_mensais']['2025-04'] = {
                    'meta_arrecadacao': 0.0,
                    'meta_acionamento': None,
                    'meta_liquidacao': None,
                    'meta_bonificacao': 0.0
                }
            elif id_empresa in empresas_etapa2:
                # Empresas ativas usam percentual da etapa 2
                percentual_etapa2 = empresas_etapa2[id_empresa]['percentual'] / Decimal('100')
                meta_abril = (meta_estendida_abril * percentual_etapa2).quantize(
                    Decimal('0.01'), rounding=ROUND_HALF_UP
                )

                resultado_metas[id_empresa]['metas_mensais']['2025-04'] = {
                    'meta_arrecadacao': float(meta_abril),
                    'meta_acionamento': None,
                    'meta_liquidacao': None,
                    'meta_bonificacao': float((meta_abril * Decimal('0.05')).quantize(
                        Decimal('0.01'), rounding=ROUND_HALF_UP
                    ))
                }

        # MAIO - Cálculo proporcional (H.Costa sai em 16/05)
        print("\nCalculando MAIO com redistribuição em 16/05:")
        dias_maio_total = Decimal(str(self.dias_uteis_periodo['2025-05']['total']))
        dias_maio_ate_16 = Decimal(str(self._calcular_dias_uteis_proporcional(2025, 5, date(2025, 5, 16), True)))
        dias_maio_apos_16 = Decimal(str(self._calcular_dias_uteis_proporcional(2025, 5, date(2025, 5, 16), False)))
        print(f"Dias úteis maio: total={dias_maio_total}, até 16={dias_maio_ate_16}, após 16={dias_maio_apos_16}")

        meta_estendida_maio = self.metas_estendidas['2025-05']

        for id_empresa in empresas_etapa1:
            nome_empresa = empresas_etapa1[id_empresa]['nome_abreviado']

            if nome_empresa == 'Real':
                # Real já saiu, não recebe
                resultado_metas[id_empresa]['metas_mensais']['2025-05'] = {
                    'meta_arrecadacao': 0.0,
                    'meta_acionamento': None,
                    'meta_liquidacao': None,
                    'meta_bonificacao': 0.0
                }

            elif nome_empresa == 'H.Costa':
                # H.Costa trabalha até 16/05 com percentual da etapa 2
                if id_empresa in empresas_etapa2:
                    percentual_etapa2 = empresas_etapa2[id_empresa]['percentual'] / Decimal('100')
                    meta_proporcional = (
                                meta_estendida_maio * percentual_etapa2 * dias_maio_ate_16 / dias_maio_total).quantize(
                        Decimal('0.01'), rounding=ROUND_HALF_UP
                    )

                    resultado_metas[id_empresa]['metas_mensais']['2025-05'] = {
                        'meta_arrecadacao': float(meta_proporcional),
                        'meta_acionamento': None,
                        'meta_liquidacao': None,
                        'meta_bonificacao': float((meta_proporcional * Decimal('0.05')).quantize(
                            Decimal('0.01'), rounding=ROUND_HALF_UP
                        ))
                    }
                    resultado_metas[id_empresa]['ativo'] = False

            elif id_empresa in empresas_etapa3:
                # Outras empresas: até 16 com % etapa2, após 16 com % etapa3
                percentual_etapa2 = empresas_etapa2[id_empresa]['percentual'] / Decimal('100')
                percentual_etapa3 = empresas_etapa3[id_empresa]['percentual'] / Decimal('100')

                meta_parte1 = meta_estendida_maio * percentual_etapa2 * dias_maio_ate_16 / dias_maio_total
                meta_parte2 = meta_estendida_maio * percentual_etapa3 * dias_maio_apos_16 / dias_maio_total
                meta_total = (meta_parte1 + meta_parte2).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

                resultado_metas[id_empresa]['metas_mensais']['2025-05'] = {
                    'meta_arrecadacao': float(meta_total),
                    'meta_acionamento': None,
                    'meta_liquidacao': None,
                    'meta_bonificacao': float((meta_total * Decimal('0.05')).quantize(
                        Decimal('0.01'), rounding=ROUND_HALF_UP
                    ))
                }

        # JUNHO e JULHO - Usar percentuais da ETAPA 3 (sem Real e H.Costa)
        print("\nCalculando JUN e JUL com percentuais da etapa 3:")
        for i in range(5, len(self.meses_periodo)):  # JUN e JUL
            mes = self.meses_periodo[i]
            ano_mes = mes['ano_mes']
            meta_estendida = self.metas_estendidas[ano_mes]

            for id_empresa in empresas_etapa1:
                nome_empresa = empresas_etapa1[id_empresa]['nome_abreviado']

                if nome_empresa in ['Real', 'H.Costa']:
                    # Empresas que saíram não recebem
                    resultado_metas[id_empresa]['metas_mensais'][ano_mes] = {
                        'meta_arrecadacao': 0.0,
                        'meta_acionamento': None,
                        'meta_liquidacao': None,
                        'meta_bonificacao': 0.0
                    }

                elif id_empresa in empresas_etapa3:
                    # Empresas ativas usam percentual da etapa 3
                    percentual_etapa3 = empresas_etapa3[id_empresa]['percentual'] / Decimal('100')
                    meta_mes = (meta_estendida * percentual_etapa3).quantize(
                        Decimal('0.01'), rounding=ROUND_HALF_UP
                    )

                    resultado_metas[id_empresa]['metas_mensais'][ano_mes] = {
                        'meta_arrecadacao': float(meta_mes),
                        'meta_acionamento': None,
                        'meta_liquidacao': None,
                        'meta_bonificacao': float((meta_mes * Decimal('0.05')).quantize(
                            Decimal('0.01'), rounding=ROUND_HALF_UP
                        ))
                    }

        # ========== PREPARAR RESULTADO FINAL ==========
        resultado_final = {
            'periodo_info': self.periodo_info,
            'meses': self.meses_periodo,
            'dias_uteis': self.dias_uteis_periodo,
            'metas_siscor': {k: float(v) for k, v in self.metas_siscor.items()},
            'metas_diarias': {},
            'metas_periodo': {k: float(v) for k, v in self.metas_periodo.items()},
            'incremento_meta': float(self.incremento_meta),
            'metas_estendidas': {k: float(v) for k, v in self.metas_estendidas.items()},
            'total_saldo_devedor': float(total_sd_etapa1),  # Total inicial
            'total_meta_siscor': float(self.total_meta_siscor),
            'total_dias_uteis_periodo': self.total_dias_uteis_periodo,
            'meta_por_dia_util': float(self.meta_por_dia_util),
            'empresas': [],
            'metas_detalhadas': []
        }

        # Montar resultado final
        for id_empresa, dados_meta in resultado_metas.items():
            empresa_original = dados_meta['dados_originais']
            total_arrecadacao = Decimal('0')
            total_bonificacao = Decimal('0')

            # Determinar valores finais para exibição
            if id_empresa in empresas_etapa3:
                # Empresa ativa até o fim - usar valores da etapa 3
                saldo_final = float(empresas_etapa3[id_empresa]['saldo_devedor'])
                percentual_final = float(empresas_etapa3[id_empresa]['percentual'])
                nome_empresa = empresas_etapa3[id_empresa]['nome_abreviado']
            else:
                # Empresa saiu - mostrar zerado
                saldo_final = 0.0
                percentual_final = 0.0
                nome_empresa = empresa_original['nome_abreviado']

            # Calcular totais
            for mes in self.meses_periodo:
                ano_mes = mes['ano_mes']
                if ano_mes in dados_meta['metas_mensais']:
                    meta_arrecadacao = Decimal(str(dados_meta['metas_mensais'][ano_mes]['meta_arrecadacao']))
                    meta_bonificacao = Decimal(str(dados_meta['metas_mensais'][ano_mes]['meta_bonificacao']))

                    total_arrecadacao += meta_arrecadacao
                    total_bonificacao += meta_bonificacao

                    # Adicionar ao detalhamento
                    resultado_final['metas_detalhadas'].append({
                        'id_empresa': id_empresa,
                        'nome_empresa': nome_empresa,
                        'competencia': ano_mes,
                        'nome_mes': mes['nome_mes'],
                        'dias_uteis_total': self.dias_uteis_periodo[ano_mes]['total'],
                        'dias_uteis_periodo': self.dias_uteis_periodo[ano_mes]['periodo'],
                        'meta_siscor': float(self.metas_siscor[ano_mes]),
                        'meta_diaria': 0.0,
                        'meta_periodo': float(self.metas_periodo[ano_mes]),
                        'meta_estendida': float(self.metas_estendidas[ano_mes]),
                        'percentual': percentual_final,
                        'meta_arrecadacao': float(meta_arrecadacao),
                        'meta_acionamento': None,
                        'meta_liquidacao': None,
                        'meta_bonificacao': float(meta_bonificacao)
                    })

            # Adicionar empresa ao resultado
            empresa_dados = {
                'id_empresa': id_empresa,
                'nome_abreviado': nome_empresa,
                'nome': nome_empresa,
                'saldo_devedor': saldo_final,
                'percentual': percentual_final,
                'condicao': 'ATIVA' if dados_meta['ativo'] else 'DESCREDENCIADA NO PERÍODO',
                'metas_mensais': dados_meta['metas_mensais'],
                'total_arrecadacao': float(total_arrecadacao),
                'total_acionamento': None,
                'total_liquidacao': None,
                'total_bonificacao': float(total_bonificacao)
            }

            resultado_final['empresas'].append(empresa_dados)

        # Ordenar por nome
        resultado_final['empresas'].sort(key=lambda x: x['nome_abreviado'])

        print("\n=== VERIFICAÇÃO FINAL ===")
        print(f"Total SD: R$ {resultado_final['total_saldo_devedor']:,.2f}")

        # Debug dos valores de maio
        print("\nValores de MAIO:")
        for emp in resultado_final['empresas']:
            if emp['nome_abreviado'] in ['Alpha', 'Avant', 'H.Costa']:
                maio_valor = emp['metas_mensais']['2025-05']['meta_arrecadacao']
                print(f"  {emp['nome_abreviado']}: R$ {maio_valor:,.2f}")

        return resultado_final
    def salvar_metas(self, metas_calculadas):
        """Salva as metas calculadas no banco de dados"""
        try:
            # Deletar metas anteriores (soft delete)
            MetaAvaliacao.query.filter_by(
                ID_EDITAL=self.edital_id,
                ID_PERIODO=self.periodo_id,
                DELETED_AT=None
            ).update({'DELETED_AT': datetime.now()})

            MetaSemestral.query.filter_by(
                ID_EDITAL=self.edital_id,
                ID_PERIODO=self.periodo_id,
                DELETED_AT=None
            ).update({'DELETED_AT': datetime.now()})

            # Salvar novas metas mensais
            for meta in metas_calculadas['metas_detalhadas']:
                nova_meta = MetaAvaliacao(
                    ID_EDITAL=self.edital_id,
                    ID_PERIODO=self.periodo_id,
                    ID_EMPRESA=meta['id_empresa'],
                    ANO_MES_COMPETENCIA=meta['competencia'],
                    VR_META_ARRECADACAO=Decimal(str(meta['meta_arrecadacao'])),
                    VR_META_ACIONAMENTO=None,
                    QTDE_META_LIQUIDACAO=None,
                    QTDE_META_BONIFICACAO=Decimal(str(meta['meta_bonificacao']))
                )
                db.session.add(nova_meta)

            # Salvar metas semestrais
            for empresa in metas_calculadas['empresas']:
                meta_semestral = MetaSemestral(
                    ID_EDITAL=self.edital_id,
                    ID_PERIODO=self.periodo_id,
                    ID_EMPRESA=empresa['id_empresa'],
                    NO_ABREVIADO_EMPRESA=empresa['nome_abreviado'],
                    VR_SALDO_DEVEDOR=Decimal(str(empresa['saldo_devedor'])),
                    PERC_SD_EMPRESA=Decimal(str(empresa['percentual'])),
                    VR_META_TOTAL=Decimal(str(empresa['total_arrecadacao']))
                )
                db.session.add(meta_semestral)

            db.session.commit()
            return True
        except Exception as e:
            db.session.rollback()
            raise e