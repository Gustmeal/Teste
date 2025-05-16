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
        if not periodo:
            raise ValueError("Período não encontrado")

        distribuicoes = self._calcular_distribuicoes()
        if not distribuicoes:
            raise ValueError("Nenhuma distribuição encontrada para este período")

        meses = self._obter_meses_periodo(periodo['dt_inicio'], periodo['dt_fim'])
        metas_calculadas = []

        for mes_ano in meses:
            ano, mes = mes_ano.split('-')

            # Calcular dados do mês
            dias_uteis = self._calcular_dias_uteis_mes(int(ano), int(mes))
            meta_siscor = self._obter_meta_siscor(int(ano), int(mes))
            meta_diaria = meta_siscor / dias_uteis if dias_uteis > 0 else Decimal('0')

            # Calcular meta para cada empresa
            for dist in distribuicoes:
                meta_arrecadacao = meta_siscor * dist['percentual'] / 100
                meta_acionamento = self._calcular_meta_acionamento(dist['id_empresa'], ano, mes)
                meta_liquidacao = self._calcular_meta_liquidacao(dist['id_empresa'], ano, mes)
                meta_bonificacao = meta_arrecadacao * Decimal('0.05')

                metas_calculadas.append({
                    'empresa_id': dist['id_empresa'],
                    'empresa_nome': dist['nome_abreviado'],
                    'competencia': mes_ano,
                    'dias_uteis': dias_uteis,
                    'meta_global': float(meta_siscor),
                    'meta_diaria': float(meta_diaria),
                    'percentual_participacao': float(dist['percentual']),
                    'meta_arrecadacao': float(meta_arrecadacao),
                    'meta_acionamento': float(meta_acionamento),
                    'meta_liquidacao': int(meta_liquidacao),
                    'meta_bonificacao': float(meta_bonificacao)
                })

        # Salvar metas semestrais
        self._salvar_metas_semestrais(distribuicoes, metas_calculadas)

        return metas_calculadas

    def _obter_periodo(self):
        """Obtém dados do período"""
        sql = text("""
            SELECT ID, ID_PERIODO, DT_INICIO, DT_FIM
            FROM DEV.DCA_TB001_PERIODO_AVALIACAO
            WHERE ID = :periodo_id
            AND ID_EDITAL = :edital_id
            AND DELETED_AT IS NULL
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
            'dt_fim': result[3]
        }

    def _calcular_distribuicoes(self):
        """Calcula distribuições e percentuais de participação"""
        periodo = self._obter_periodo()

        sql = text("""
            SELECT 
                e.ID_EMPRESA,
                e.NO_EMPRESA_ABREVIADA,
                e.NO_EMPRESA,
                SUM(d.VR_SD_DEVEDOR) as SALDO_EMPRESA
            FROM DEV.DCA_TB005_DISTRIBUICAO d
            JOIN DEV.DCA_TB002_EMPRESAS_PARTICIPANTES e 
                ON d.COD_EMPRESA_COBRANCA = e.ID_EMPRESA
                AND d.ID_EDITAL = e.ID_EDITAL 
                AND d.ID_PERIODO = e.ID_PERIODO
            WHERE d.ID_EDITAL = :edital_id
            AND d.ID_PERIODO = :periodo_id
            AND d.DELETED_AT IS NULL
            AND e.DELETED_AT IS NULL
            GROUP BY e.ID_EMPRESA, e.NO_EMPRESA_ABREVIADA, e.NO_EMPRESA
        """)

        result = db.session.execute(sql, {
            'edital_id': self.edital_id,
            'periodo_id': periodo['id_periodo']
        })

        distribuicoes = []
        total_saldo = Decimal('0')

        for row in result:
            empresa_data = {
                'id_empresa': row[0],
                'nome_abreviado': row[1] or row[2],
                'nome': row[2],
                'saldo': Decimal(str(row[3]))
            }
            distribuicoes.append(empresa_data)
            total_saldo += empresa_data['saldo']

        # Calcular percentuais
        for dist in distribuicoes:
            dist['percentual'] = (dist['saldo'] / total_saldo * 100) if total_saldo > 0 else Decimal('0')

        return distribuicoes

    def _obter_meses_periodo(self, dt_inicio, dt_fim):
        """Retorna lista de meses no formato YYYY-MM entre as datas"""
        meses = []
        atual = dt_inicio

        while atual <= dt_fim:
            meses.append(atual.strftime('%Y-%m'))
            if atual.month == 12:
                atual = atual.replace(year=atual.year + 1, month=1, day=1)
            else:
                ultimo_dia = calendar.monthrange(atual.year, atual.month + 1)[1]
                atual = atual.replace(month=atual.month + 1, day=min(atual.day, ultimo_dia))

        return meses

    def _calcular_dias_uteis_mes(self, ano, mes):
        """Calcula dias úteis do mês consultando a tabela de calendário"""
        sql = text("""
            SELECT COUNT(*) as QTDE_DIAS_UTEIS
            FROM BDG.AUX_TB004_CALENDARIO
            WHERE ANO = :ano 
            AND MES = :mes
            AND E_DIA_UTIL = 1
        """)

        result = db.session.execute(sql, {'ano': ano, 'mes': mes}).fetchone()
        return result[0] if result else 22

    def _obter_meta_siscor(self, ano, mes):
        """Obtém meta global do SISCOR para o mês"""
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

    def _calcular_meta_acionamento(self, empresa_id, ano, mes):
        """Calcula meta de acionamento baseada em histórico"""
        sql = text("""
            SELECT AVG(VR_ARRECADACAO_TOTAL) as MEDIA_ACIONAMENTOS
            FROM BDG.COM_TB062_REMUNERACAO_ESTIMADA
            WHERE CO_EMPRESA_COBRANCA = :empresa_id
            AND YEAR(DT_ARRECADACAO) = :ano
            AND MONTH(DT_ARRECADACAO) = :mes
        """)

        result = db.session.execute(sql, {
            'empresa_id': empresa_id,
            'ano': int(ano),
            'mes': int(mes)
        }).fetchone()

        if result and result[0]:
            return Decimal(str(result[0]))

        # Se não houver histórico, buscar média geral
        sql_geral = text("""
            SELECT AVG(VR_ARRECADACAO_TOTAL) as MEDIA_GERAL
            FROM BDG.COM_TB062_REMUNERACAO_ESTIMADA
            WHERE YEAR(DT_ARRECADACAO) = :ano
            AND MONTH(DT_ARRECADACAO) = :mes
        """)

        result_geral = db.session.execute(sql_geral, {'ano': int(ano), 'mes': int(mes)}).fetchone()
        return Decimal(str(result_geral[0])) if result_geral and result_geral[0] else Decimal('0')

    def _calcular_meta_liquidacao(self, empresa_id, ano, mes):
        """Calcula meta de liquidação baseada em histórico - CORRIGIDO"""
        sql = text("""
            SELECT COUNT(DISTINCT liq.fkContratoSISCTR) as QTDE_LIQUIDACOES
            FROM BDG.COM_TB009_ACORDOS_LIQUIDADOS_VIGENTES liq
            JOIN BDG.COM_TB011_EMPRESA_COBRANCA_ATUAL emp
                ON liq.fkContratoSISCTR = emp.fkContratoSISCTR
            WHERE emp.COD_EMPRESA_COBRANCA = :empresa_id
            AND YEAR(liq.DT_LIQUIDACAO) = :ano
            AND MONTH(liq.DT_LIQUIDACAO) = :mes
        """)

        result = db.session.execute(sql, {
            'empresa_id': empresa_id,
            'ano': int(ano),
            'mes': int(mes)
        }).fetchone()

        if result and result[0]:
            return int(result[0])

        # Se não houver histórico, estimar baseado na distribuição
        sql_dist = text("""
            SELECT COUNT(*) * 0.05 as META_ESTIMADA
            FROM DEV.DCA_TB005_DISTRIBUICAO
            WHERE COD_EMPRESA_COBRANCA = :empresa_id
            AND DELETED_AT IS NULL
        """)

        result_dist = db.session.execute(sql_dist, {'empresa_id': empresa_id}).fetchone()
        return int(result_dist[0]) if result_dist and result_dist[0] > 0 else 0

    def _salvar_metas_semestrais(self, distribuicoes, metas_calculadas):
        """Salva ou atualiza metas semestrais"""
        from app.models.meta_avaliacao import MetaSemestral

        for dist in distribuicoes:
            # Calcular total do período para a empresa
            meta_total_periodo = Decimal('0')
            for meta in metas_calculadas:
                if meta['empresa_id'] == dist['id_empresa']:
                    meta_total_periodo += Decimal(str(meta['meta_arrecadacao']))

            # Verificar se já existe
            meta_semestral = MetaSemestral.query.filter_by(
                ID_EDITAL=self.edital_id,
                ID_PERIODO=self.periodo_id,
                COD_EMPRESA_COBRANCA=dist['id_empresa'],
                DELETED_AT=None
            ).first()

            if not meta_semestral:
                meta_semestral = MetaSemestral(
                    ID_EDITAL=self.edital_id,
                    ID_PERIODO=self.periodo_id,
                    COD_EMPRESA_COBRANCA=dist['id_empresa']
                )
                db.session.add(meta_semestral)

            meta_semestral.NO_ABREVIADO_EMPRESA = dist['nome_abreviado']
            meta_semestral.VR_SD_DEVEDOR = dist['saldo']
            meta_semestral.PERC_EMPRESA = dist['percentual']
            meta_semestral.META_TOTAL_PERC = meta_total_periodo

        db.session.commit()