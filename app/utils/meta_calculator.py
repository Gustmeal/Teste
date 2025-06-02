# app/utils/meta_calculator.py
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from sqlalchemy import text
from app import db
from app.models.meta_avaliacao import MetaAvaliacao, MetaSemestral
import calendar


class MetaCalculator:
    def __init__(self, edital_id, periodo_id, fator_incremento=1.10):
        self.edital_id = edital_id
        self.periodo_id = periodo_id
        self.periodo_info = None
        self.meses_periodo = []
        self.dias_uteis_periodo = {}
        self.metas_siscor = {}
        self.metas_diarias = {}
        self.metas_periodo = {}
        self.metas_estendidas = {}
        self.distribuicoes = []
        self.total_saldo_devedor = Decimal('0')
        self.incremento_meta = Decimal(str(fator_incremento))

    def calcular_metas_completas(self):
        """Executa todo o processo de cálculo de metas"""
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

        # 5. Calcular metas diárias
        self._calcular_metas_diarias()

        # 6. Calcular metas do período avaliativo
        self._calcular_metas_periodo()

        # 7. Calcular metas estendidas (com incremento)
        self._calcular_metas_estendidas()

        # 8. Obter distribuições e calcular percentuais
        self._obter_distribuicoes()

        # 9. Calcular metas por empresa
        return self._calcular_metas_empresas()

    def _obter_periodo(self):
        """Obtém dados do período"""
        sql = text("""
            SELECT p.ID, p.ID_PERIODO, p.DT_INICIO, p.DT_FIM,
                   e.NU_EDITAL, e.ANO
            FROM DEV.DCA_TB001_PERIODO_AVALIACAO p
            JOIN DEV.DCA_TB008_EDITAIS e ON p.ID_EDITAL = e.ID
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
        # Verificar se houve redistribuição para ajustar o período
        sql_redistribuicao = text("""
            SELECT 
                MAX(DT_REFERENCIA) as DT_REDISTRIBUICAO
            FROM BDDASHBOARDBI.BDG.DCA_TB005_DISTRIBUICAO
            WHERE ID_EDITAL = :edital_id
            AND ID_PERIODO = :periodo_id
            AND COD_CRITERIO_SELECAO IN (
                SELECT DISTINCT COD_CRITERIO_SELECAO
                FROM BDDASHBOARDBI.BDG.DCA_TB005_DISTRIBUICAO
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                GROUP BY COD_CRITERIO_SELECAO, fkContratoSISCTR
                HAVING COUNT(DISTINCT COD_EMPRESA_COBRANCA) > 1
            )
        """)

        dt_redistribuicao = db.session.execute(sql_redistribuicao, {
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_info['id']
        }).scalar()

        meses = []
        data_fim = self.periodo_info['dt_fim']

        # Se houver redistribuição, começar do mês da redistribuição
        if dt_redistribuicao:
            data_atual = dt_redistribuicao.replace(day=1)
        else:
            data_atual = self.periodo_info['dt_inicio'].replace(day=1)

        # Mapeamento de nomes de meses em português
        meses_portugues = {
            1: 'JAN', 2: 'FEV', 3: 'MAR', 4: 'ABR',
            5: 'MAI', 6: 'JUN', 7: 'JUL', 8: 'AGO',
            9: 'SET', 10: 'OUT', 11: 'NOV', 12: 'DEZ'
        }

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
        for mes in self.meses_periodo:
            # Query para dias úteis totais do mês
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

            dias_uteis_total = result_total[0] if result_total and result_total[0] else 0

            # Query para dias úteis dentro do período avaliativo
            sql_periodo = text("""
                SELECT COUNT(*) as QTDE_DIAS_UTEIS
                FROM BDG.AUX_TB004_CALENDARIO
                WHERE ANO = :ano 
                AND MES = :mes
                AND E_DIA_UTIL = 1
                AND DT_REFERENCIA BETWEEN :dt_inicio AND :dt_fim
            """)

            # Ajustar datas para o mês específico
            primeiro_dia_mes = date(mes['ano'], mes['mes'], 1)
            ultimo_dia_mes = date(mes['ano'], mes['mes'], calendar.monthrange(mes['ano'], mes['mes'])[1])

            # Usar a maior data entre início do período e primeiro dia do mês
            dt_inicio_calc = max(self.periodo_info['dt_inicio'], primeiro_dia_mes)
            # Usar a menor data entre fim do período e último dia do mês
            dt_fim_calc = min(self.periodo_info['dt_fim'], ultimo_dia_mes)

            result_periodo = db.session.execute(sql_periodo, {
                'ano': mes['ano'],
                'mes': mes['mes'],
                'dt_inicio': dt_inicio_calc,
                'dt_fim': dt_fim_calc
            }).fetchone()

            dias_uteis_periodo = result_periodo[0] if result_periodo and result_periodo[0] else 0

            self.dias_uteis_periodo[mes['ano_mes']] = {
                'total': dias_uteis_total,
                'periodo': dias_uteis_periodo
            }

    def _obter_metas_siscor_todos_meses(self):
        """Obtém metas SISCOR de todos os meses"""
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
            self.metas_siscor[mes['ano_mes']] = meta_valor

    def _calcular_metas_diarias(self):
        """Calcula metas diárias para cada mês"""
        for mes in self.meses_periodo:
            ano_mes = mes['ano_mes']
            dias_uteis = self.dias_uteis_periodo[ano_mes]['total']
            meta_siscor = self.metas_siscor[ano_mes]

            if dias_uteis > 0:
                meta_diaria = meta_siscor / dias_uteis
                meta_diaria = meta_diaria.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            else:
                meta_diaria = Decimal('0')

            self.metas_diarias[ano_mes] = meta_diaria

    def _calcular_metas_periodo(self):
        """Calcula metas do período avaliativo (meta diária × dias úteis do período)"""
        for mes in self.meses_periodo:
            ano_mes = mes['ano_mes']
            dias_periodo = self.dias_uteis_periodo[ano_mes]['periodo']
            meta_diaria = self.metas_diarias[ano_mes]

            # Meta do período = meta diária × dias úteis do período
            meta_periodo = meta_diaria * dias_periodo
            meta_periodo = meta_periodo.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

            self.metas_periodo[ano_mes] = meta_periodo

    def _calcular_metas_estendidas(self):
        """Calcula metas estendidas (meta período × incremento)"""
        for mes in self.meses_periodo:
            ano_mes = mes['ano_mes']
            meta_periodo = self.metas_periodo[ano_mes]

            # Meta estendida = meta período × incremento (1,10)
            meta_estendida = meta_periodo * self.incremento_meta
            meta_estendida = meta_estendida.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

            self.metas_estendidas[ano_mes] = meta_estendida

    def _obter_distribuicoes(self):
        """Obtém distribuições e calcula percentuais baseado no saldo devedor dos contratos distribuídos"""

        # Query principal - considera todos os contratos, incluindo redistribuições
        sql = text("""
            WITH UltimaDistribuicao AS (
                -- Pegar apenas a última distribuição de cada contrato (em caso de redistribuição)
                SELECT 
                    fkContratoSISCTR,
                    COD_EMPRESA_COBRANCA,
                    VR_SD_DEVEDOR,
                    DT_REFERENCIA,
                    ROW_NUMBER() OVER (PARTITION BY fkContratoSISCTR ORDER BY DT_REFERENCIA DESC) as RN
                FROM BDDASHBOARDBI.BDG.DCA_TB005_DISTRIBUICAO
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND DELETED_AT IS NULL
            )
            SELECT 
                e.ID_EMPRESA,
                e.NO_EMPRESA_ABREVIADA,
                e.NO_EMPRESA,
                COALESCE(dist.SALDO_TOTAL, 0) as SALDO_EMPRESA,
                e.DS_CONDICAO,
                COALESCE(dist.QTDE_CONTRATOS, 0) as QTDE_CONTRATOS
            FROM DEV.DCA_TB002_EMPRESAS_PARTICIPANTES e
            LEFT JOIN (
                SELECT 
                    COD_EMPRESA_COBRANCA,
                    SUM(VR_SD_DEVEDOR) as SALDO_TOTAL,
                    COUNT(DISTINCT fkContratoSISCTR) as QTDE_CONTRATOS
                FROM UltimaDistribuicao
                WHERE RN = 1  -- Apenas a distribuição mais recente de cada contrato
                GROUP BY COD_EMPRESA_COBRANCA
            ) dist ON dist.COD_EMPRESA_COBRANCA = e.ID_EMPRESA
            WHERE e.ID_EDITAL = :edital_id
            AND e.ID_PERIODO = :periodo_id
            AND e.DELETED_AT IS NULL
            AND COALESCE(e.DS_CONDICAO, '') != 'DESCREDENCIADA'
            ORDER BY e.NO_EMPRESA_ABREVIADA
        """)

        result = db.session.execute(sql, {
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_info['id']
        })

        self.distribuicoes = []
        self.total_saldo_devedor = Decimal('0')

        for row in result:
            saldo = Decimal(str(row[3]))

            empresa_data = {
                'id_empresa': row[0],
                'nome_abreviado': row[1] or row[2][:30],
                'nome': row[2],
                'saldo': saldo,
                'condicao': row[4] if row[4] else 'PERMANECE',
                'qtde_contratos': row[5]
            }
            self.distribuicoes.append(empresa_data)
            self.total_saldo_devedor += saldo

        # Calcular percentuais
        for dist in self.distribuicoes:
            if self.total_saldo_devedor > 0:
                percentual = (dist['saldo'] / self.total_saldo_devedor * 100)
                dist['percentual'] = percentual.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)
            else:
                dist['percentual'] = Decimal('0')

    def _calcular_metas_empresas(self):
        """Calcula metas para cada empresa"""
        resultado = {
            'periodo_info': self.periodo_info,
            'meses': self.meses_periodo,
            'dias_uteis': self.dias_uteis_periodo,
            'metas_siscor': {k: float(v) for k, v in self.metas_siscor.items()},
            'metas_diarias': {k: float(v) for k, v in self.metas_diarias.items()},
            'metas_periodo': {k: float(v) for k, v in self.metas_periodo.items()},
            'incremento_meta': float(self.incremento_meta),
            'metas_estendidas': {k: float(v) for k, v in self.metas_estendidas.items()},
            'total_saldo_devedor': float(self.total_saldo_devedor),
            'empresas': [],
            'metas_detalhadas': []
        }

        # Calcular para cada empresa
        for idx, dist in enumerate(self.distribuicoes):
            empresa_metas = {
                'id_empresa': dist['id_empresa'],
                'nome_abreviado': dist['nome_abreviado'],
                'nome': dist['nome'],
                'saldo_devedor': float(dist['saldo']),
                'percentual': float(dist['percentual']),
                'metas_mensais': {}
            }

            total_arrecadacao = Decimal('0')
            total_bonificacao = Decimal('0')

            # Calcular metas para cada mês
            for mes in self.meses_periodo:
                ano_mes = mes['ano_mes']

                # Meta de arrecadação = Meta Estendida × Percentual da empresa
                meta_estendida = self.metas_estendidas[ano_mes]
                meta_arrecadacao = (meta_estendida * dist['percentual'] / 100).quantize(
                    Decimal('0.01'), rounding=ROUND_HALF_UP
                )

                # Meta de bonificação = 5% da meta de arrecadação
                meta_bonificacao = (meta_arrecadacao * Decimal('0.05')).quantize(
                    Decimal('0.01'), rounding=ROUND_HALF_UP
                )

                empresa_metas['metas_mensais'][ano_mes] = {
                    'meta_arrecadacao': float(meta_arrecadacao),
                    'meta_acionamento': None,  # Será null conforme solicitado
                    'meta_liquidacao': None,  # Será null conforme solicitado
                    'meta_bonificacao': float(meta_bonificacao)
                }

                total_arrecadacao += meta_arrecadacao
                total_bonificacao += meta_bonificacao

                # Adicionar ao detalhamento
                resultado['metas_detalhadas'].append({
                    'id_empresa': dist['id_empresa'],
                    'nome_empresa': dist['nome_abreviado'],
                    'competencia': ano_mes,
                    'nome_mes': mes['nome_mes'],
                    'dias_uteis_total': self.dias_uteis_periodo[ano_mes]['total'],
                    'dias_uteis_periodo': self.dias_uteis_periodo[ano_mes]['periodo'],
                    'meta_siscor': float(self.metas_siscor[ano_mes]),
                    'meta_diaria': float(self.metas_diarias[ano_mes]),
                    'meta_periodo': float(self.metas_periodo[ano_mes]),
                    'meta_estendida': float(self.metas_estendidas[ano_mes]),
                    'percentual': float(dist['percentual']),
                    'meta_arrecadacao': float(meta_arrecadacao),
                    'meta_acionamento': None,
                    'meta_liquidacao': None,
                    'meta_bonificacao': float(meta_bonificacao)
                })

            empresa_metas['total_arrecadacao'] = float(total_arrecadacao)
            empresa_metas['total_acionamento'] = None
            empresa_metas['total_liquidacao'] = None
            empresa_metas['total_bonificacao'] = float(total_bonificacao)

            resultado['empresas'].append(empresa_metas)

        return resultado

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
                    VR_META_ACIONAMENTO=None,  # NULL conforme solicitado
                    QTDE_META_LIQUIDACAO=None,  # NULL conforme solicitado
                    QTDE_META_BONIFICACAO=Decimal(str(meta['meta_bonificacao']))
                )
                db.session.add(nova_meta)

            # Salvar metas semestrais (totais por empresa)
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