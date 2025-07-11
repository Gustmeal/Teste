from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from sqlalchemy import text
from app import db
from app.models.metas_redistribuicao import MetasPercentuaisDistribuicao, Metas, MetasPeriodoAvaliativo
import calendar


class RedistribuicaoCalculator:
    def __init__(self, edital_id, periodo_id):
        self.edital_id = edital_id
        self.periodo_id = periodo_id
        self.periodo_info = None
        self.metas_mensais = {}
        self.distribuicoes = {}
        self.metas_periodo = None
        self.meses_periodo = []
        self.dias_uteis_periodo = {}

    def calcular_redistribuicao(self, empresas_redistribuicao):
        """
        Calcula a redistribuição de metas baseado nas empresas que saem
        empresas_redistribuicao: {data: [lista_ids_empresas_que_saem]}
        """
        print("\n=== INICIANDO CÁLCULO DE REDISTRIBUIÇÃO ===")

        # 1. Carregar dados base
        self._carregar_dados_base()

        # 2. Identificar meses do período
        self._identificar_meses_periodo()

        # 3. Processar redistribuições
        resultado = self._processar_redistribuicoes(empresas_redistribuicao)

        return resultado

    def _carregar_dados_base(self):
        """Carrega os dados das tabelas auxiliares"""
        self._carregar_periodo()
        self._carregar_distribuicoes()
        self._carregar_meta_periodo()
        self._carregar_metas_mensais()

    def _carregar_periodo(self):
        """Carrega informações do período"""
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
            raise ValueError("Período não encontrado")

        self.periodo_info = {
            'id': result[0],
            'id_periodo': result[1],
            'dt_inicio': result[2],
            'dt_fim': result[3],
            'nu_edital': result[4],
            'ano_edital': result[5]
        }

    def _identificar_meses_periodo(self):
        """Identifica os meses do período avaliativo"""
        meses_portugues = {
            1: 'JAN', 2: 'FEV', 3: 'MAR', 4: 'ABR',
            5: 'MAI', 6: 'JUN', 7: 'JUL', 8: 'AGO',
            9: 'SET', 10: 'OUT', 11: 'NOV', 12: 'DEZ'
        }

        data_atual = self.periodo_info['dt_inicio'].replace(day=1)
        data_fim = self.periodo_info['dt_fim']

        self.meses_periodo = []
        while data_atual <= data_fim:
            self.meses_periodo.append({
                'ano': data_atual.year,
                'mes': data_atual.month,
                'ano_mes': data_atual.strftime('%Y-%m'),
                'competencia': data_atual.strftime('%Y-%m'),
                'nome_mes': meses_portugues[data_atual.month]
            })
            data_atual = data_atual + relativedelta(months=1)

        print(f"Meses do período: {[m['competencia'] for m in self.meses_periodo]}")

    def _carregar_metas_mensais(self):
        """Carrega metas mensais - primeiro das TB013, senão calcula do SISCOR"""
        # Tentar carregar da TB013
        sql = text("""
            SELECT COMPETENCIA, VR_MENSAL_SISCOR, QTDE_DIAS_UTEIS_MES
            FROM BDG.DCA_TB012_METAS
            WHERE ID_EDITAL = :edital_id
            AND ID_PERIODO = :periodo_id
            AND DELETED_AT IS NULL
            ORDER BY COMPETENCIA
        """)

        result = db.session.execute(sql, {
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_id
        })

        metas_tb013 = {}
        for row in result:
            metas_tb013[row[0]] = {
                'valor_siscor': Decimal(str(row[1])) if row[1] else Decimal('0'),
                'dias_uteis': row[2] or 0
            }

        # Para cada mês do período, usar TB013 ou calcular do SISCOR
        for mes in self.meses_periodo:
            competencia = mes['competencia']

            if competencia in metas_tb013:
                # Usar dados da TB013
                self.metas_mensais[competencia] = metas_tb013[competencia]
                self.dias_uteis_periodo[competencia] = {
                    'total': metas_tb013[competencia]['dias_uteis'],
                    'periodo': metas_tb013[competencia]['dias_uteis']
                }
            else:
                # Buscar do SISCOR
                meta_siscor = self._buscar_meta_siscor(mes['ano'], mes['mes'])
                dias_uteis = self._calcular_dias_uteis_mes(mes['ano'], mes['mes'])

                self.metas_mensais[competencia] = {
                    'valor_siscor': meta_siscor,
                    'dias_uteis': dias_uteis
                }
                self.dias_uteis_periodo[competencia] = {
                    'total': dias_uteis,
                    'periodo': dias_uteis
                }

        print(f"Metas carregadas para: {list(self.metas_mensais.keys())}")

    def _buscar_meta_siscor(self, ano, mes):
        """Busca meta SISCOR para um mês específico"""
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

        result = db.session.execute(sql, {'ano': ano, 'mes': mes}).fetchone()
        return Decimal(str(result[0])) if result and result[0] else Decimal('0')

    def _calcular_dias_uteis_mes(self, ano, mes):
        """Calcula dias úteis totais de um mês"""
        sql = text("""
            SELECT COUNT(*) as QTDE_DIAS_UTEIS
            FROM BDG.AUX_TB004_CALENDARIO
            WHERE ANO = :ano 
            AND MES = :mes
            AND E_DIA_UTIL = 1
        """)

        result = db.session.execute(sql, {'ano': ano, 'mes': mes}).fetchone()
        return result[0] if result and result[0] else 0

    def _carregar_distribuicoes(self):
        """Carrega distribuições da TB015 com informações das empresas"""
        sql = text("""
            SELECT DISTINCT
                mpd.DT_REFERENCIA,
                mpd.ID_EMPRESA,
                mpd.VR_SALDO_DEVEDOR_DISTRIBUIDO,
                mpd.PERCENTUAL_SALDO_DEVEDOR,
                emp.NO_EMPRESA_ABREVIADA,
                (SELECT COUNT(*) FROM BDG.DCA_TB005_DISTRIBUICAO dist 
                 WHERE dist.ID_EDITAL = mpd.ID_EDITAL 
                 AND dist.ID_PERIODO = mpd.ID_PERIODO 
                 AND dist.COD_EMPRESA_COBRANCA = mpd.ID_EMPRESA
                 AND dist.DT_REFERENCIA = mpd.DT_REFERENCIA) as QTDE_CONTRATOS
            FROM BDG.DCA_TB014_METAS_PERCENTUAIS_DISTRIBUICAO mpd
            JOIN BDG.DCA_TB002_EMPRESAS_PARTICIPANTES emp 
                ON mpd.ID_EMPRESA = emp.ID_EMPRESA
                AND mpd.ID_EDITAL = emp.ID_EDITAL
                AND mpd.ID_PERIODO = emp.ID_PERIODO
            WHERE mpd.ID_EDITAL = :edital_id
            AND mpd.ID_PERIODO = :periodo_id
            AND mpd.DELETED_AT IS NULL
            ORDER BY mpd.DT_REFERENCIA, emp.NO_EMPRESA_ABREVIADA
        """)

        result = db.session.execute(sql, {
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_id
        })

        for row in result:
            dt_ref = row[0].strftime('%Y-%m-%d')

            if dt_ref not in self.distribuicoes:
                self.distribuicoes[dt_ref] = {}

            self.distribuicoes[dt_ref][row[1]] = {
                'saldo_devedor': Decimal(str(row[2])) if row[2] else Decimal('0'),
                'percentual': Decimal(str(row[3])) if row[3] else Decimal('0'),
                'nome_empresa': row[4],
                'qtde_contratos': row[5] or 0
            }

        print(f"Distribuições carregadas para datas: {list(self.distribuicoes.keys())}")

    def _carregar_meta_periodo(self):
        """Carrega meta do período da TB014"""
        meta = MetasPeriodoAvaliativo.query.filter_by(
            ID_EDITAL=self.edital_id,
            ID_PERIODO=self.periodo_id,
            DELETED_AT=None
        ).order_by(MetasPeriodoAvaliativo.DT_REFERENCIA.desc()).first()

        if meta:
            self.metas_periodo = {
                'valor_global_siscor': Decimal(str(meta.VR_GLOBAL_SISCOR)) if meta.VR_GLOBAL_SISCOR else Decimal('0'),
                'dias_uteis_periodo': meta.QTDE_DIAS_UTEIS_PERIODO or 0,
                'incremento': Decimal(str(meta.INDICE_INCREMENTO_META)) if meta.INDICE_INCREMENTO_META else Decimal(
                    '1.00'),
                'meta_distribuir': Decimal(str(meta.VR_META_A_DISTRIBUIR)) if meta.VR_META_A_DISTRIBUIR else Decimal(
                    '0'),
                'valor_por_dia': Decimal(str(meta.VR_POR_DIA_UTIL)) if meta.VR_POR_DIA_UTIL else Decimal('0')
            }
        else:
            # Valores padrão
            total_siscor = sum(m['valor_siscor'] for m in self.metas_mensais.values())
            total_dias = sum(m['dias_uteis'] for m in self.metas_mensais.values())

            self.metas_periodo = {
                'valor_global_siscor': total_siscor,
                'dias_uteis_periodo': total_dias,
                'incremento': Decimal('1.00'),
                'meta_distribuir': total_siscor,
                'valor_por_dia': total_siscor / total_dias if total_dias > 0 else Decimal('0')
            }

    def _processar_redistribuicoes(self, empresas_redistribuicao):
        """Processa as redistribuições e gera a tabela final"""

        # Usar a última distribuição disponível como base
        data_inicial = max(self.distribuicoes.keys())
        distribuicao_atual = self.distribuicoes[data_inicial].copy()

        # Estrutura para armazenar metas calculadas
        metas_por_empresa = {}

        # Calcular total SD inicial
        total_sd_inicial = sum(emp['saldo_devedor'] for emp in distribuicao_atual.values())

        print(f"\nDistribuição inicial ({data_inicial}):")
        print(f"Total SD: R$ {float(total_sd_inicial):,.2f}")

        # Inicializar metas para todas as empresas
        for emp_id, dados in distribuicao_atual.items():
            metas_por_empresa[emp_id] = {
                'id_empresa': emp_id,
                'nome_empresa': dados['nome_empresa'],
                'saldo_devedor': float(dados['saldo_devedor']),
                'percentual': float(dados['percentual']),
                'qtde_contratos': dados['qtde_contratos'],
                'metas_mensais': {},
                'ativo': True
            }
            print(
                f"  {dados['nome_empresa']}: SD={float(dados['saldo_devedor']):,.2f}, %={float(dados['percentual']):.8f}%")

        # Processar cada mês
        for mes_info in self.meses_periodo:
            competencia = mes_info['competencia']
            ano = mes_info['ano']
            mes = mes_info['mes']

            print(f"\nProcessando {mes_info['nome_mes']}/{ano}...")

            # Verificar se há redistribuição neste mês
            redistribuicao_mes = None
            empresas_saindo_mes = []

            for data_redistrib, empresas_ids in empresas_redistribuicao.items():
                data_redistrib_obj = datetime.strptime(data_redistrib, '%Y-%m-%d').date()
                if data_redistrib_obj.year == ano and data_redistrib_obj.month == mes:
                    redistribuicao_mes = data_redistrib_obj
                    empresas_saindo_mes = [int(emp_id) for emp_id in empresas_ids]
                    print(f"  Redistribuição em {data_redistrib}: empresas {empresas_saindo_mes} saindo")
                    break

            # Obter meta do mês
            meta_mensal = self.metas_mensais.get(competencia, {})
            valor_meta = meta_mensal.get('valor_siscor', Decimal('0'))

            # Aplicar incremento
            valor_meta_incrementado = valor_meta * self.metas_periodo['incremento']

            print(f"  Meta SISCOR: R$ {float(valor_meta):,.2f}")
            print(f"  Meta com incremento: R$ {float(valor_meta_incrementado):,.2f}")

            if redistribuicao_mes and empresas_saindo_mes:
                # Mês com redistribuição
                self._calcular_mes_com_redistribuicao(
                    metas_por_empresa,
                    competencia,
                    valor_meta_incrementado,
                    distribuicao_atual,
                    redistribuicao_mes,
                    empresas_saindo_mes
                )

                # Atualizar distribuição para os próximos meses
                nova_distribuicao = {}
                novo_total = Decimal('0')

                # Somar SD das empresas que continuam
                for emp_id, dados in distribuicao_atual.items():
                    if emp_id not in empresas_saindo_mes:
                        novo_total += dados['saldo_devedor']

                print(f"  Novo total SD (sem empresas que saíram): R$ {float(novo_total):,.2f}")

                # Recalcular percentuais
                for emp_id, dados in distribuicao_atual.items():
                    if emp_id not in empresas_saindo_mes:
                        nova_distribuicao[emp_id] = {
                            'saldo_devedor': dados['saldo_devedor'],
                            'percentual': (dados['saldo_devedor'] / novo_total * 100).quantize(
                                Decimal('0.00000001'), rounding=ROUND_HALF_UP
                            ) if novo_total > 0 else Decimal('0'),
                            'nome_empresa': dados['nome_empresa'],
                            'qtde_contratos': dados['qtde_contratos']
                        }

                # Marcar empresas que saíram como inativas
                for emp_id in empresas_saindo_mes:
                    if emp_id in metas_por_empresa:
                        metas_por_empresa[emp_id]['ativo'] = False
                        metas_por_empresa[emp_id]['percentual'] = 0.0

                distribuicao_atual = nova_distribuicao
            else:
                # Mês sem redistribuição
                for emp_id, dados in distribuicao_atual.items():
                    percentual_decimal = dados['percentual'] / Decimal('100')
                    meta_empresa = (valor_meta_incrementado * percentual_decimal).quantize(
                        Decimal('0.01'), rounding=ROUND_HALF_UP
                    )

                    metas_por_empresa[emp_id]['metas_mensais'][competencia] = {
                        'meta_arrecadacao': float(meta_empresa),
                        'meta_acionamento': None,
                        'meta_liquidacao': None,
                        'meta_bonificacao': float((meta_empresa * Decimal('0.05')).quantize(
                            Decimal('0.01'), rounding=ROUND_HALF_UP
                        ))
                    }

                # Empresas inativas recebem zero
                for emp_id, dados in metas_por_empresa.items():
                    if not dados['ativo'] and competencia not in dados['metas_mensais']:
                        dados['metas_mensais'][competencia] = {
                            'meta_arrecadacao': 0.0,
                            'meta_acionamento': None,
                            'meta_liquidacao': None,
                            'meta_bonificacao': 0.0
                        }

        # Formatar resultado final
        return self._formatar_resultado_tabela(metas_por_empresa, total_sd_inicial)

    def _calcular_mes_com_redistribuicao(self, metas_por_empresa, competencia, valor_meta,
                                         distribuicao_atual, data_redistrib, empresas_saindo):
        """Calcula metas para um mês com redistribuição proporcional aos dias úteis"""

        ano = int(competencia.split('-')[0])
        mes = int(competencia.split('-')[1])

        # Obter dias úteis
        dias_uteis_total = self.dias_uteis_periodo[competencia]['total']
        dias_uteis_ate_saida = self._calcular_dias_uteis_ate_data(ano, mes, data_redistrib)
        dias_uteis_apos_saida = dias_uteis_total - dias_uteis_ate_saida

        print(f"  Dias úteis: total={dias_uteis_total}, até saída={dias_uteis_ate_saida}, após={dias_uteis_apos_saida}")

        # Calcular nova distribuição sem empresas que saem
        nova_distribuicao = {}
        novo_total = Decimal('0')

        for emp_id, dados in distribuicao_atual.items():
            if emp_id not in empresas_saindo:
                novo_total += dados['saldo_devedor']

        for emp_id, dados in distribuicao_atual.items():
            if emp_id not in empresas_saindo:
                nova_distribuicao[emp_id] = {
                    'percentual': (dados['saldo_devedor'] / novo_total * 100).quantize(
                        Decimal('0.00000001'), rounding=ROUND_HALF_UP
                    ) if novo_total > 0 else Decimal('0')
                }

        # Converter para Decimal para cálculos precisos
        dias_uteis_total_dec = Decimal(str(dias_uteis_total))
        dias_uteis_ate_dec = Decimal(str(dias_uteis_ate_saida))
        dias_uteis_apos_dec = Decimal(str(dias_uteis_apos_saida))

        # Calcular metas proporcionais
        for emp_id, dados in metas_por_empresa.items():
            if emp_id in empresas_saindo:
                # Empresa sai - recebe proporcional até a data de saída
                if emp_id in distribuicao_atual:
                    percentual_decimal = distribuicao_atual[emp_id]['percentual'] / Decimal('100')
                    meta_proporcional = (
                                valor_meta * percentual_decimal * dias_uteis_ate_dec / dias_uteis_total_dec).quantize(
                        Decimal('0.01'), rounding=ROUND_HALF_UP
                    )

                    print(f"  {dados['nome_empresa']} (saindo): R$ {float(meta_proporcional):,.2f}")

                    dados['metas_mensais'][competencia] = {
                        'meta_arrecadacao': float(meta_proporcional),
                        'meta_acionamento': None,
                        'meta_liquidacao': None,
                        'meta_bonificacao': float((meta_proporcional * Decimal('0.05')).quantize(
                            Decimal('0.01'), rounding=ROUND_HALF_UP
                        ))
                    }
            elif emp_id in distribuicao_atual:
                # Empresa continua - recebe parte com percentual antigo + parte com novo
                percentual_antes = distribuicao_atual[emp_id]['percentual'] / Decimal('100')
                percentual_depois = nova_distribuicao[emp_id]['percentual'] / Decimal('100')

                # Meta = (valor × %antes × dias_ate) + (valor × %depois × dias_apos)
                meta_parte1 = valor_meta * percentual_antes * dias_uteis_ate_dec / dias_uteis_total_dec
                meta_parte2 = valor_meta * percentual_depois * dias_uteis_apos_dec / dias_uteis_total_dec
                meta_total = (meta_parte1 + meta_parte2).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

                print(
                    f"  {dados['nome_empresa']}: R$ {float(meta_total):,.2f} (antes: {float(meta_parte1):,.2f} + depois: {float(meta_parte2):,.2f})")

                dados['metas_mensais'][competencia] = {
                    'meta_arrecadacao': float(meta_total),
                    'meta_acionamento': None,
                    'meta_liquidacao': None,
                    'meta_bonificacao': float((meta_total * Decimal('0.05')).quantize(
                        Decimal('0.01'), rounding=ROUND_HALF_UP
                    ))
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
        dt_fim = data

        result = db.session.execute(sql, {
            'ano': ano,
            'mes': mes,
            'dt_inicio': dt_inicio,
            'dt_fim': dt_fim
        }).fetchone()

        return result[0] if result and result[0] else 0

    def _formatar_resultado_tabela(self, metas_por_empresa, total_sd_inicial):
        """Formata o resultado no formato esperado para a tabela"""

        # Calcular metas estendidas
        metas_estendidas = {}
        for mes in self.meses_periodo:
            competencia = mes['competencia']
            meta_mensal = self.metas_mensais.get(competencia, {})
            valor_meta = meta_mensal.get('valor_siscor', Decimal('0'))
            metas_estendidas[competencia] = float(valor_meta * self.metas_periodo['incremento'])

        # Preparar empresas e metas detalhadas
        empresas = []
        metas_detalhadas = []

        # Ordenar empresas por nome
        empresas_ordenadas = sorted(metas_por_empresa.items(), key=lambda x: x[1]['nome_empresa'])

        for emp_id, dados in empresas_ordenadas:
            total_arrecadacao = Decimal('0')
            total_bonificacao = Decimal('0')

            # Calcular totais
            for competencia, meta in dados['metas_mensais'].items():
                total_arrecadacao += Decimal(str(meta['meta_arrecadacao']))
                total_bonificacao += Decimal(str(meta['meta_bonificacao']))

                # Adicionar ao detalhamento
                mes_info = next(m for m in self.meses_periodo if m['competencia'] == competencia)
                metas_detalhadas.append({
                    'id_empresa': emp_id,
                    'nome_empresa': dados['nome_empresa'],
                    'competencia': competencia,
                    'nome_mes': mes_info['nome_mes'],
                    'dias_uteis_total': self.dias_uteis_periodo[competencia]['total'],
                    'dias_uteis_periodo': self.dias_uteis_periodo[competencia]['periodo'],
                    'meta_siscor': float(self.metas_mensais.get(competencia, {}).get('valor_siscor', 0)),
                    'meta_periodo': float(self.metas_mensais.get(competencia, {}).get('valor_siscor', 0)),
                    'meta_estendida': metas_estendidas.get(competencia, 0),
                    'percentual': dados['percentual'] if dados['ativo'] else 0.0,
                    'meta_arrecadacao': meta['meta_arrecadacao'],
                    'meta_acionamento': meta['meta_acionamento'],
                    'meta_liquidacao': meta['meta_liquidacao'],
                    'meta_bonificacao': meta['meta_bonificacao']
                })

            # Adicionar empresa
            empresas.append({
                'id_empresa': emp_id,
                'nome_abreviado': dados['nome_empresa'],
                'nome': dados['nome_empresa'],
                'saldo_devedor': dados['saldo_devedor'],
                'percentual': dados['percentual'],
                'qtde_contratos': dados['qtde_contratos'],
                'condicao': 'ATIVA' if dados['ativo'] else 'DESCREDENCIADA NO PERÍODO',
                'metas_mensais': dados['metas_mensais'],
                'total_arrecadacao': float(total_arrecadacao),
                'total_acionamento': None,
                'total_liquidacao': None,
                'total_bonificacao': float(total_bonificacao)
            })

        # Retornar resultado
        return {
            'periodo_info': self.periodo_info,
            'meses': self.meses_periodo,
            'dias_uteis': self.dias_uteis_periodo,
            'metas_siscor': {k: float(v['valor_siscor']) for k, v in self.metas_mensais.items()},
            'metas_periodo': {k: float(v['valor_siscor']) for k, v in self.metas_mensais.items()},
            'incremento_meta': float(self.metas_periodo['incremento']),
            'metas_estendidas': metas_estendidas,
            'total_saldo_devedor': float(total_sd_inicial),
            'empresas': empresas,
            'metas_detalhadas': metas_detalhadas
        }