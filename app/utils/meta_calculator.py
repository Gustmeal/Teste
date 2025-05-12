# app/utils/meta_calculator.py
from decimal import Decimal
from datetime import datetime
from sqlalchemy import text
from app import db
import calendar


class MetaCalculator:
    def __init__(self, edital_id, periodo_id):
        self.edital_id = edital_id
        self.periodo_id = periodo_id

    def calcular_todas_metas(self):
        """Calcula metas para todas as empresas do período"""
        periodo = self._obter_periodo()
        empresas = self._obter_empresas()
        meses = self._obter_meses_periodo(periodo['dt_inicio'], periodo['dt_fim'])

        metas_calculadas = []

        for mes in meses:
            ano, mes_num = mes.split('-')

            # Calcular para cada empresa
            for empresa in empresas:
                meta = self._calcular_meta_empresa(empresa, int(ano), int(mes_num))
                meta['competencia'] = mes
                metas_calculadas.append(meta)

        return metas_calculadas

    def _obter_periodo(self):
        """Obtém dados do período"""
        sql = text("""
            SELECT ID, ID_PERIODO, DT_INICIO, DT_FIM
            FROM DEV.DCA_TB001_PERIODO_AVALIACAO
            WHERE ID_EDITAL = :edital_id
            AND ID = :periodo_id
        """)

        result = db.session.execute(sql, {
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_id
        }).fetchone()

        return {
            'id': result[0],
            'id_periodo': result[1],
            'dt_inicio': result[2],
            'dt_fim': result[3]
        }

    def _obter_empresas(self):
        """Obtém empresas participantes"""
        # Primeiro, obter o ID_PERIODO correto
        periodo = self._obter_periodo()

        sql = text("""
            SELECT ID, ID_EMPRESA, NO_EMPRESA, NO_EMPRESA_ABREVIADA
            FROM DEV.DCA_TB002_EMPRESAS_PARTICIPANTES
            WHERE ID_EDITAL = :edital_id
            AND ID_PERIODO = :id_periodo
            AND DELETED_AT IS NULL
        """)

        results = db.session.execute(sql, {
            'edital_id': self.edital_id,
            'id_periodo': periodo['id_periodo']
        }).fetchall()

        return [{
            'id': r[0],
            'id_empresa': r[1],
            'nome': r[2],
            'nome_abreviado': r[3]
        } for r in results]

    def _obter_meses_periodo(self, dt_inicio, dt_fim):
        """Retorna lista de meses no formato YYYY-MM entre as datas"""
        meses = []
        atual = dt_inicio

        while atual <= dt_fim:
            meses.append(atual.strftime('%Y-%m'))
            # Avançar para próximo mês
            if atual.month == 12:
                atual = atual.replace(year=atual.year + 1, month=1, day=1)
            else:
                mes_prox = atual.month + 1
                ultimo_dia = calendar.monthrange(atual.year, mes_prox)[1]
                dia_prox = min(atual.day, ultimo_dia)
                atual = atual.replace(month=mes_prox, day=dia_prox)

        return meses

    def _calcular_dias_uteis(self, ano, mes):
        """Calcula dias úteis do mês consultando a tabela de calendário"""
        sql = text("""
            SELECT COUNT(*) as qtde_dias_uteis
            FROM BDG.AUX_TB004_CALENDARIO
            WHERE ANO = :ano 
            AND MES = :mes
            AND E_DIA_UTIL = 1
        """)

        result = db.session.execute(sql, {'ano': ano, 'mes': mes}).fetchone()
        return result[0] if result else 20  # Valor padrão se não encontrar

    def _obter_meta_siscor(self, ano, mes):
        """Obtém meta global do SISCOR para o mês"""
        sql = text("""
            SELECT SUM(VR_PREVISAO_ORCAMENTO) as meta_total
            FROM BDG.COR_TB002_REPROGRAMACAO_ORCAMENTARIA_SISCOR
            WHERE ID_NATUREZA = 3
            AND UNIDADE = 'SUPEC'
            AND DT_PREVISAO_ORCAMENTO/100 = :ano
            AND DT_PREVISAO_ORCAMENTO - (DT_PREVISAO_ORCAMENTO/100 * 100) = :mes
            AND ID_TIPO_FASE_ORC = (
                SELECT MAX(ID_TIPO_FASE_ORC)
                FROM BDG.COR_TB002_REPROGRAMACAO_ORCAMENTARIA_SISCOR
                WHERE DT_PREVISAO_ORCAMENTO/100 = :ano
            )
        """)

        result = db.session.execute(sql, {'ano': ano, 'mes': mes}).fetchone()
        return Decimal(str(result[0])) if result and result[0] else Decimal('1000000.00')

    def _calcular_percentual_participacao(self, empresa_id):
        """Calcula percentual de participação da empresa"""
        periodo = self._obter_periodo()

        # Total de saldo devedor distribuído
        sql_total = text("""
            SELECT SUM(VR_SD_DEVEDOR) as total
            FROM DEV.DCA_TB005_DISTRIBUICAO
            WHERE ID_EDITAL = :edital_id
            AND ID_PERIODO = :periodo_id
            AND DELETED_AT IS NULL
        """)

        result_total = db.session.execute(sql_total, {
            'edital_id': self.edital_id,
            'periodo_id': periodo['id_periodo']
        }).fetchone()

        total_geral = Decimal(str(result_total[0])) if result_total and result_total[0] else Decimal('0')

        # Saldo devedor da empresa
        sql_empresa = text("""
            SELECT SUM(VR_SD_DEVEDOR) as total
            FROM DEV.DCA_TB005_DISTRIBUICAO
            WHERE ID_EDITAL = :edital_id
            AND ID_PERIODO = :periodo_id
            AND COD_EMPRESA_COBRANCA = :empresa_id
            AND DELETED_AT IS NULL
        """)

        result_empresa = db.session.execute(sql_empresa, {
            'edital_id': self.edital_id,
            'periodo_id': periodo['id_periodo'],
            'empresa_id': empresa_id
        }).fetchone()

        total_empresa = Decimal(str(result_empresa[0])) if result_empresa and result_empresa[0] else Decimal('0')

        if total_geral > 0:
            return (total_empresa / total_geral * 100)
        else:
            return Decimal('0')

    def _calcular_meta_empresa(self, empresa, ano, mes):
        """Calcula meta específica de uma empresa"""
        dias_uteis = self._calcular_dias_uteis(ano, mes)
        meta_global = self._obter_meta_siscor(ano, mes)
        percentual = self._calcular_percentual_participacao(empresa['id_empresa'])

        # Calcular meta diária
        meta_diaria = meta_global / dias_uteis if dias_uteis > 0 else Decimal('0')

        # Calcular meta da empresa (aplicando percentual de participação)
        meta_arrecadacao = float(meta_global * percentual / 100)

        return {
            'empresa_id': empresa['id'],
            'empresa_nome': empresa['nome_abreviado'] or empresa['nome'],
            'dias_uteis': dias_uteis,
            'meta_global': float(meta_global),
            'meta_diaria': float(meta_diaria),
            'percentual_participacao': float(percentual),
            'meta_arrecadacao': meta_arrecadacao,
            'meta_acionamento': self._calcular_meta_acionamento(empresa['id_empresa'], f"{ano}-{mes:02d}"),
            'meta_liquidacao': self._calcular_meta_liquidacao(empresa['id_empresa'], f"{ano}-{mes:02d}"),
            'meta_bonificacao': self._calcular_meta_bonificacao(meta_arrecadacao)
        }

    def _calcular_meta_acionamento(self, empresa_id, ano_mes):
        """Calcula meta de acionamento baseada em histórico"""
        sql = text("""
            SELECT AVG(VR_ACIONAMENTO) as media_acionamentos
            FROM BDG.COM_TB062_REMUNERACAO_ESTIMADA
            WHERE CO_EMPRESA_COBRANCA = :empresa_id
            AND COMPETENCIA = :competencia
        """)

        result = db.session.execute(sql, {
            'empresa_id': empresa_id,
            'competencia': ano_mes.replace('-', '')
        }).fetchone()

        return float(result[0]) if result and result[0] else 10000.0

    def _calcular_meta_liquidacao(self, empresa_id, ano_mes):
        """Calcula meta de liquidação baseada em histórico"""
        ano, mes = ano_mes.split('-')

        sql = text("""
            SELECT COUNT(DISTINCT fkContratoSISCTR) as qtde_liquidacoes
            FROM BDG.COM_TB009_ACORDOS_LIQUIDADOS_VIGENTES
            WHERE COD_EMPRESA_COBRANCA = :empresa_id
            AND YEAR(DT_LIQUIDACAO) = :ano
            AND MONTH(DT_LIQUIDACAO) = :mes
        """)

        result = db.session.execute(sql, {
            'empresa_id': empresa_id,
            'ano': int(ano),
            'mes': int(mes)
        }).fetchone()

        return int(result[0]) if result and result[0] else 100

    def _calcular_meta_bonificacao(self, meta_arrecadacao):
        """Calcula meta de bonificação baseada na arrecadação"""
        # 5% da meta de arrecadação
        return float(meta_arrecadacao * 0.05)