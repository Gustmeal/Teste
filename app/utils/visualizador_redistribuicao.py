from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from sqlalchemy import text
from app import db
import calendar


class VisualizadorRedistribuicao:
    def __init__(self, edital_id, periodo_id):
        self.edital_id = edital_id
        self.periodo_id = periodo_id
        self.data_inicial = date(2025, 1, 15)
        self.data_redistrib_real = date(2025, 3, 26)
        self.data_redistrib_hcosta = date(2025, 5, 16)

    def calcular_redistribuicao_completa(self):
        """Calcula as duas tabelas de redistribuição conforme Excel"""
        # Buscar dados básicos
        periodo_info = self._buscar_periodo_info()
        meses = self._identificar_meses_periodo(periodo_info)

        # Buscar metas mensais da TB013
        metas_mensais = self._buscar_metas_mensais(meses)

        # Buscar dados do período da TB014
        dados_periodo = self._buscar_dados_periodo()

        # Calcular dias úteis e metas
        for mes in meses:
            mes['dias_uteis'] = self._buscar_dias_uteis_mes(mes['ano'], mes['mes'])
            mes['dias_uteis_periodo'] = self._calcular_dias_uteis_periodo(
                mes['ano'], mes['mes'], periodo_info
            )
            mes['meta_siscor'] = metas_mensais.get(mes['competencia'], Decimal('0'))
            mes['meta_periodo'] = self._calcular_meta_periodo(
                mes, dados_periodo, meses
            )
            mes['meta_estendida'] = mes['meta_periodo'] * dados_periodo['incremento']

        # Tabela 1 - Distribuição inicial (15/01/2025)
        tabela1 = self._calcular_tabela_inicial(meses, dados_periodo)

        # Tabela 2 - Após redistribuição Real (26/03/2025)
        tabela2 = self._calcular_tabela_redistribuicao_real(meses, dados_periodo)

        # Cálculo específico da Real
        calculo_real = self._calcular_detalhes_real(tabela1)

        return {
            'tabela1': tabela1,
            'tabela2': tabela2,
            'calculo_real': calculo_real
        }

    def _buscar_periodo_info(self):
        """Busca informações do período"""
        sql = text("""
            SELECT p.ID, p.ID_PERIODO, p.DT_INICIO, p.DT_FIM
            FROM DEV.DCA_TB001_PERIODO_AVALIACAO p
            WHERE p.ID = :periodo_id
            AND p.ID_EDITAL = :edital_id
            AND p.DELETED_AT IS NULL
        """)

        result = db.session.execute(sql, {
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_id
        }).fetchone()

        if not result:
            raise ValueError("Período não encontrado")

        return {
            'id': result[0],
            'id_periodo': result[1],
            'dt_inicio': result[2],
            'dt_fim': result[3]
        }

    def _identificar_meses_periodo(self, periodo_info):
        """Identifica os meses do período"""
        meses = []
        meses_nomes = {
            1: 'JAN', 2: 'FEV', 3: 'MAR', 4: 'ABR',
            5: 'MAI', 6: 'JUN', 7: 'JUL', 8: 'AGO',
            9: 'SET', 10: 'OUT', 11: 'NOV', 12: 'DEZ'
        }

        data_atual = periodo_info['dt_inicio'].replace(day=1)
        data_fim = periodo_info['dt_fim']

        while data_atual <= data_fim:
            meses.append({
                'ano': data_atual.year,
                'mes': data_atual.month,
                'competencia': data_atual.strftime('%Y-%m'),
                'nome': meses_nomes[data_atual.month]
            })
            data_atual = data_atual + relativedelta(months=1)

        return meses

    def _buscar_metas_mensais(self, meses):
        """Busca metas mensais da TB013"""
        sql = text("""
            SELECT COMPETENCIA, VR_MENSAL_SISCOR
            FROM DEV.DCA_TB013_METAS
            WHERE ID_EDITAL = :edital_id
            AND ID_PERIODO = :periodo_id
            AND DELETED_AT IS NULL
        """)

        result = db.session.execute(sql, {
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_id
        })

        metas = {}
        for row in result:
            metas[row[0]] = Decimal(str(row[1])) if row[1] else Decimal('0')

        return metas

    def _buscar_dados_periodo(self):
        """Busca dados do período da TB014"""
        sql = text("""
            SELECT TOP 1 
                VR_GLOBAL_SISCOR, QTDE_DIAS_UTEIS_PERIODO, 
                INDICE_INCREMENTO_META, VR_META_A_DISTRIBUIR, VR_POR_DIA_UTIL
            FROM DEV.DCA_TB014_METAS_PERIODO_AVALIATIVO
            WHERE ID_EDITAL = :edital_id
            AND ID_PERIODO = :periodo_id
            AND DELETED_AT IS NULL
            ORDER BY DT_REFERENCIA DESC
        """)

        result = db.session.execute(sql, {
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_id
        }).fetchone()

        if result:
            return {
                'valor_global': Decimal(str(result[0])) if result[0] else Decimal('0'),
                'dias_uteis_total': result[1] or 0,
                'incremento': Decimal(str(result[2])) if result[2] else Decimal('1.00'),
                'meta_distribuir': Decimal(str(result[3])) if result[3] else Decimal('0'),
                'valor_por_dia': Decimal(str(result[4])) if result[4] else Decimal('0')
            }
        else:
            # Valores padrão
            return {
                'valor_global': Decimal('0'),
                'dias_uteis_total': 0,
                'incremento': Decimal('1.00'),
                'meta_distribuir': Decimal('0'),
                'valor_por_dia': Decimal('0')
            }

    def _buscar_dias_uteis_mes(self, ano, mes):
        """Busca dias úteis totais do mês"""
        sql = text("""
            SELECT COUNT(*) as QTDE_DIAS_UTEIS
            FROM BDG.AUX_TB004_CALENDARIO
            WHERE ANO = :ano 
            AND MES = :mes
            AND E_DIA_UTIL = 1
        """)

        result = db.session.execute(sql, {'ano': ano, 'mes': mes}).fetchone()
        return result[0] if result and result[0] else 0

    def _calcular_dias_uteis_periodo(self, ano, mes, periodo_info):
        """Calcula dias úteis do mês dentro do período"""
        primeiro_dia = date(ano, mes, 1)
        ultimo_dia = date(ano, mes, calendar.monthrange(ano, mes)[1])

        dt_inicio = max(periodo_info['dt_inicio'], primeiro_dia)
        dt_fim = min(periodo_info['dt_fim'], ultimo_dia)

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

    def _calcular_meta_periodo(self, mes, dados_periodo, todos_meses):
        """Calcula meta do período para o mês"""
        # Soma total de dias úteis do período
        total_dias = sum(m['dias_uteis_periodo'] for m in todos_meses)

        if total_dias > 0 and dados_periodo['valor_global'] > 0:
            valor_por_dia = dados_periodo['valor_global'] / Decimal(str(total_dias))
            return (valor_por_dia * Decimal(str(mes['dias_uteis_periodo']))).quantize(
                Decimal('0.01'), rounding=ROUND_HALF_UP
            )

        return Decimal('0')

    def _buscar_distribuicao_empresas(self, data_ref):
        """Busca distribuição de empresas para uma data específica"""
        sql = text("""
            SELECT 
                mpd.ID_EMPRESA,
                emp.NO_EMPRESA_ABREVIADA,
                mpd.VR_SALDO_DEVEDOR_DISTRIBUIDO,
                mpd.PERCENTUAL_SALDO_DEVEDOR
            FROM DEV.DCA_TB015_METAS_PERCENTUAIS_DISTRIBUICAO mpd
            JOIN DEV.DCA_TB002_EMPRESAS_PARTICIPANTES emp 
                ON mpd.ID_EMPRESA = emp.ID_EMPRESA
                AND mpd.ID_EDITAL = emp.ID_EDITAL
                AND mpd.ID_PERIODO = emp.ID_PERIODO
            WHERE mpd.ID_EDITAL = :edital_id
            AND mpd.ID_PERIODO = :periodo_id
            AND mpd.DT_REFERENCIA = :data_ref
            AND mpd.DELETED_AT IS NULL
            ORDER BY emp.NO_EMPRESA_ABREVIADA
        """)

        result = db.session.execute(sql, {
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_id,
            'data_ref': data_ref
        })

        empresas = []
        total_sd = Decimal('0')

        for row in result:
            sd = Decimal(str(row[2])) if row[2] else Decimal('0')
            percentual = Decimal(str(row[3])) if row[3] else Decimal('0')

            empresas.append({
                'id': row[0],
                'nome': row[1],
                'saldo_devedor': sd,
                'percentual': percentual
            })
            total_sd += sd

        return empresas, total_sd

    def _calcular_tabela_inicial(self, meses, dados_periodo):
        """Calcula a tabela inicial com todas empresas (15/01/2025)"""
        empresas, total_sd = self._buscar_distribuicao_empresas(self.data_inicial)

        # Calcular metas para cada empresa
        for empresa in empresas:
            empresa['metas'] = {}
            for mes in meses:
                meta_estendida = mes['meta_estendida']
                percentual_decimal = empresa['percentual'] / Decimal('100')
                empresa['metas'][mes['competencia']] = (
                        meta_estendida * percentual_decimal
                ).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        # Calcular totais
        total_dias_uteis = sum(m['dias_uteis_periodo'] for m in meses)
        meta_por_dia = dados_periodo['valor_global'] / Decimal(
            str(total_dias_uteis)) if total_dias_uteis > 0 else Decimal('0')

        return {
            'meses': [{
                'competencia': m['competencia'],
                'nome': m['nome'],
                'dias_uteis': m['dias_uteis'],
                'dias_uteis_periodo': m['dias_uteis_periodo'],
                'meta_siscor': float(m['meta_siscor']),
                'meta_periodo': float(m['meta_periodo']),
                'meta_estendida': float(m['meta_estendida'])
            } for m in meses],
            'empresas': [{
                'id': e['id'],
                'nome': e['nome'],
                'saldo_devedor': float(e['saldo_devedor']),
                'percentual': float(e['percentual']),
                'metas': {k: float(v) for k, v in e['metas'].items()}
            } for e in empresas],
            'total_saldo_devedor': float(total_sd),
            'incremento': float(dados_periodo['incremento']),
            'total_dias_uteis_periodo': total_dias_uteis,
            'meta_por_dia_util': float(meta_por_dia)
        }

    def _calcular_tabela_redistribuicao_real(self, meses, dados_periodo):
        """Calcula tabela após redistribuição da Real (26/03/2025)"""
        # Buscar distribuições
        empresas_inicial, _ = self._buscar_distribuicao_empresas(self.data_inicial)
        empresas_redistrib, total_sd = self._buscar_distribuicao_empresas(self.data_redistrib_real)

        # Criar mapa de empresas
        empresas_map = {e['nome']: e for e in empresas_inicial}

        # Calcular metas considerando redistribuição
        for empresa in empresas_redistrib:
            empresa['metas'] = {}

            for mes in meses:
                competencia = mes['competencia']
                meta_estendida = mes['meta_estendida']

                # Janeiro e Fevereiro - usar percentual inicial
                if mes['mes'] in [1, 2]:
                    if empresa['nome'] in empresas_map:
                        percentual_inicial = empresas_map[empresa['nome']]['percentual'] / Decimal('100')
                        empresa['metas'][competencia] = (
                                meta_estendida * percentual_inicial
                        ).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    else:
                        empresa['metas'][competencia] = Decimal('0')

                # Março - cálculo proporcional para Real
                elif mes['mes'] == 3:
                    if empresa['nome'] == 'Real':
                        # Real trabalha até 26/03
                        dias_uteis_total = mes['dias_uteis']
                        dias_uteis_ate_26 = self._calcular_dias_uteis_ate_data(2025, 3, self.data_redistrib_real)

                        percentual_inicial = empresas_map[empresa['nome']]['percentual'] / Decimal('100')
                        meta_proporcional = (
                                meta_estendida * percentual_inicial *
                                Decimal(str(dias_uteis_ate_26)) / Decimal(str(dias_uteis_total))
                        ).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

                        empresa['metas'][competencia] = meta_proporcional
                    else:
                        # Outras empresas - parte com percentual inicial + parte com novo percentual
                        dias_uteis_total = mes['dias_uteis']
                        dias_uteis_ate_26 = self._calcular_dias_uteis_ate_data(2025, 3, self.data_redistrib_real)
                        dias_uteis_apos_26 = dias_uteis_total - dias_uteis_ate_26

                        # Percentual inicial (até 26/03)
                        if empresa['nome'] in empresas_map:
                            percentual_inicial = empresas_map[empresa['nome']]['percentual'] / Decimal('100')
                            meta_parte1 = (
                                    meta_estendida * percentual_inicial *
                                    Decimal(str(dias_uteis_ate_26)) / Decimal(str(dias_uteis_total))
                            )
                        else:
                            meta_parte1 = Decimal('0')

                        # Novo percentual (após 26/03)
                        percentual_novo = empresa['percentual'] / Decimal('100')
                        meta_parte2 = (
                                meta_estendida * percentual_novo *
                                Decimal(str(dias_uteis_apos_26)) / Decimal(str(dias_uteis_total))
                        )

                        empresa['metas'][competencia] = (meta_parte1 + meta_parte2).quantize(
                            Decimal('0.01'), rounding=ROUND_HALF_UP
                        )

                # Abril em diante - usar novo percentual (sem Real)
                else:
                    percentual_novo = empresa['percentual'] / Decimal('100')
                    empresa['metas'][competencia] = (
                            meta_estendida * percentual_novo
                    ).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        # Adicionar Real com zeros após março
        real_data = None
        for emp in empresas_inicial:
            if emp['nome'] == 'Real':
                real_data = emp
                break

        if real_data:
            real_redistrib = {
                'id': real_data['id'],
                'nome': real_data['nome'],
                'saldo_devedor': real_data['saldo_devedor'],
                'percentual': real_data['percentual'],
                'metas': {}
            }

            # Copiar metas já calculadas
            for emp in empresas_redistrib:
                if emp['nome'] == 'Real':
                    real_redistrib['metas'] = emp['metas'].copy()
                    break

            # Adicionar zeros para os meses restantes
            for mes in meses:
                if mes['mes'] > 3:
                    real_redistrib['metas'][mes['competencia']] = Decimal('0')

            # Inserir Real na posição correta
            empresas_final = []
            real_inserido = False

            for emp in empresas_redistrib:
                if emp['nome'] != 'Real':
                    if not real_inserido and emp['nome'] > 'Real':
                        empresas_final.append(real_redistrib)
                        real_inserido = True
                    empresas_final.append(emp)

            if not real_inserido:
                empresas_final.append(real_redistrib)

            empresas_redistrib = empresas_final

        # Calcular totais
        total_dias_uteis = sum(m['dias_uteis_periodo'] for m in meses)
        meta_por_dia = dados_periodo['valor_global'] / Decimal(
            str(total_dias_uteis)) if total_dias_uteis > 0 else Decimal('0')

        return {
            'meses': [{
                'competencia': m['competencia'],
                'nome': m['nome'],
                'dias_uteis': m['dias_uteis'],
                'dias_uteis_periodo': m['dias_uteis_periodo'],
                'meta_siscor': float(m['meta_siscor']),
                'meta_periodo': float(m['meta_periodo']),
                'meta_estendida': float(m['meta_estendida'])
            } for m in meses],
            'empresas': [{
                'id': e['id'],
                'nome': e['nome'],
                'saldo_devedor': float(e['saldo_devedor']),
                'percentual': float(e['percentual']),
                'metas': {k: float(v) for k, v in e['metas'].items()}
            } for e in empresas_redistrib],
            'total_saldo_devedor': float(total_sd),
            'incremento': float(dados_periodo['incremento']),
            'total_dias_uteis_periodo': total_dias_uteis,
            'meta_por_dia_util': float(meta_por_dia)
        }

    def _calcular_dias_uteis_ate_data(self, ano, mes, data):
        """Calcula dias úteis do início do mês até a data especificada"""
        sql = text("""
            SELECT COUNT(*) as QTDE_DIAS_UTEIS
            FROM BDG.AUX_TB004_CALENDARIO
            WHERE ANO = :ano 
            AND MES = :mes
            AND E_DIA_UTIL = 1
            AND DT_REFERENCIA BETWEEN :dt_inicio AND :dt_fim
        """)

        dt_inicio = date(ano, mes, 1)

        result = db.session.execute(sql, {
            'ano': ano,
            'mes': mes,
            'dt_inicio': dt_inicio,
            'dt_fim': data
        }).fetchone()

        return result[0] if result and result[0] else 0

    def _calcular_detalhes_real(self, tabela1):
        """Calcula os detalhes específicos da redistribuição da Real"""
        # Encontrar dados da Real
        real_data = None
        for emp in tabela1['empresas']:
            if emp['nome'] == 'Real':
                real_data = emp
                break

        if not real_data:
            return {
                'meta_total': 0,
                'dias_uteis_total': 0,
                'dias_uteis_trabalhados': 0,
                'valor_mantido': 0,
                'valor_redistribuido': 0
            }

        # Meta de março da Real
        meta_marco = real_data['metas'].get('2025-03', 0)

        # Dias úteis
        dias_uteis_total = 19  # Conforme Excel
        dias_uteis_ate_26 = self._calcular_dias_uteis_ate_data(2025, 3, self.data_redistrib_real)

        # Valores
        valor_mantido = (meta_marco * dias_uteis_ate_26 / dias_uteis_total)
        valor_redistribuido = meta_marco - valor_mantido

        return {
            'meta_total': meta_marco,
            'dias_uteis_total': dias_uteis_total,
            'dias_uteis_trabalhados': dias_uteis_ate_26,
            'valor_mantido': valor_mantido,
            'valor_redistribuido': valor_redistribuido
        }