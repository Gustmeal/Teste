# app/utils/distribuicao_inicial.py

from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from sqlalchemy import text
from app import db
# Adicionado para buscar o objeto do período
from app.models.periodo import PeriodoAvaliacao


class DistribuicaoInicial:
    """
    Classe responsável pela distribuição inicial de metas.
    """

    def __init__(self, edital_id, periodo_id):
        # periodo_id aqui é a CHAVE PRIMÁRIA da tabela de períodos
        self.edital_id = edital_id
        self.periodo_pk_id = periodo_id  # Chave Primária (ex: 1, 2, 3...)

        # Busca o objeto do período para encontrar a chave de negócio
        periodo_obj = PeriodoAvaliacao.query.get(self.periodo_pk_id)
        if not periodo_obj:
            raise ValueError(f"Período de avaliação com a chave primária {self.periodo_pk_id} não foi encontrado.")

        # Chave de Negócio (ex: 5, 6, conforme a regra de negócio)
        self.periodo_business_id = periodo_obj.ID_PERIODO

        self.data_referencia = datetime.now().date()

    def calcular_distribuicao(self):
        """
        Calcula a distribuição inicial de metas para todas as empresas.
        """
        try:
            empresas_saldo = self._buscar_saldos_devedores()
            if not empresas_saldo or not empresas_saldo['empresas']:
                raise ValueError("Nenhuma empresa com saldo devedor foi encontrada para o cálculo.")

            valores_siscor = self._buscar_valores_siscor()
            if not valores_siscor:
                raise ValueError(
                    "Não foram encontrados valores de meta (SISCOR) para o edital e período selecionados. "
                    "Verifique se as metas foram cadastradas na tabela DEV.DCA_TB013_METAS."
                )

            periodo_info = self._buscar_info_periodo()
            resultado = self._calcular_metas_empresas(empresas_saldo, valores_siscor)
            resultado[
                'periodo_info'] = f"{periodo_info['id_periodo']} ({periodo_info['dt_inicio']} a {periodo_info['dt_fim']})"

            return resultado
        except Exception as e:
            raise Exception(f"Erro ao calcular distribuição: {str(e)}")

    def _buscar_saldos_devedores(self):
        """
        Busca saldo devedor das empresas no sistema de contratos.
        """
        sql = text("""
            DECLARE @SD DECIMAL(18,2)
            SET @SD = (
                SELECT SUM(VR_SD_DEVEDOR) 
                FROM BDG.COM_TB007_SITUACAO_CONTRATOS SIT
                INNER JOIN BDG.COM_TB011_EMPRESA_COBRANCA_ATUAL ASS
                    ON SIT.fkContratoSISCTR = ASS.fkContratoSISCTR
                WHERE SIT.fkSituacaoCredito = 1 
                AND ASS.COD_EMPRESA_COBRANCA NOT IN (407,422)
            )

            SELECT 
                ASS.COD_EMPRESA_COBRANCA as id_empresa,
                SUM(SIT.VR_SD_DEVEDOR) as saldo_devedor,
                (SUM(SIT.VR_SD_DEVEDOR)/@SD * 100) as percentual
            FROM BDG.COM_TB007_SITUACAO_CONTRATOS SIT
            INNER JOIN BDG.COM_TB011_EMPRESA_COBRANCA_ATUAL ASS
                ON SIT.fkContratoSISCTR = ASS.fkContratoSISCTR
            WHERE SIT.fkSituacaoCredito = 1
            AND ASS.COD_EMPRESA_COBRANCA NOT IN (407,422)
            GROUP BY ASS.COD_EMPRESA_COBRANCA
            ORDER BY ASS.COD_EMPRESA_COBRANCA
        """)
        result = db.session.execute(sql).fetchall()

        empresas = []
        total_sd = Decimal('0')
        for row in result:
            nome_empresa = self._buscar_nome_empresa(row.id_empresa)
            empresas.append({
                'id_empresa': row.id_empresa,
                'nome': nome_empresa,
                'saldo_devedor': Decimal(str(row.saldo_devedor)),
                'percentual': Decimal(str(row.percentual))
            })
            total_sd += Decimal(str(row.saldo_devedor))

        return {'empresas': empresas, 'total_saldo_devedor': total_sd}

    def _buscar_nome_empresa(self, cod_empresa):
        """
        Busca o nome da empresa usando a chave de negócio do período.
        """
        sql = text("""
            SELECT TOP 1 NO_EMPRESA_ABREVIADA
            FROM DEV.DCA_TB002_EMPRESAS_PARTICIPANTES
            WHERE ID_EMPRESA = :id_empresa
            AND ID_EDITAL = :edital_id
            AND ID_PERIODO = :periodo_id -- Usa a chave de negócio
            AND DELETED_AT IS NULL
        """)
        result = db.session.execute(sql, {
            'id_empresa': cod_empresa,
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_business_id  # CORRIGIDO
        }).fetchone()
        return result.NO_EMPRESA_ABREVIADA if result else f"Empresa {cod_empresa}"

    def _buscar_valores_siscor(self):
        """
        Busca valores SISCOR usando a chave de negócio do período.
        """
        sql = text("""
            SELECT 
                CAST(COMPETENCIA AS VARCHAR(10)) as competencia,
                VR_MENSAL_SISCOR,
                QTDE_DIAS_UTEIS_MES
            FROM DEV.DCA_TB013_METAS
            WHERE ID_EDITAL = :edital_id
            AND ID_PERIODO = :periodo_id -- Usa a chave de negócio
            AND DELETED_AT IS NULL
            AND DT_REFERENCIA = (
                SELECT MAX(DT_REFERENCIA)
                FROM DEV.DCA_TB013_METAS
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id -- Usa a chave de negócio
                AND DELETED_AT IS NULL
            )
            ORDER BY COMPETENCIA
        """)
        result = db.session.execute(sql, {
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_business_id  # CORRIGIDO
        })

        valores = []
        meses_nomes = {'01': 'JAN', '02': 'FEV', '03': 'MAR', '04': 'ABR', '05': 'MAI', '06': 'JUN', '07': 'JUL',
                       '08': 'AGO', '09': 'SET', '10': 'OUT', '11': 'NOV', '12': 'DEZ'}
        for row in result:
            competencia_str = str(row.competencia).strip()
            if len(competencia_str) == 6:
                ano, mes = competencia_str[:4], competencia_str[4:].zfill(2)
                competencia_formatada = f"{ano}-{mes}"
                nome_mes = meses_nomes.get(mes, mes)
            else:
                competencia_formatada = competencia_str
                mes = competencia_str.split('-')[1] if '-' in competencia_str else '01'
                nome_mes = meses_nomes.get(mes, mes)
            valores.append({
                'competencia': competencia_formatada, 'nome': nome_mes,
                'valor_siscor': Decimal(str(row.VR_MENSAL_SISCOR)) if row.VR_MENSAL_SISCOR else Decimal('0'),
                'dias_uteis': int(row.QTDE_DIAS_UTEIS_MES) if row.QTDE_DIAS_UTEIS_MES else 20
            })
        return valores

    def _buscar_info_periodo(self):
        """
        Busca informações do período usando a chave primária.
        """
        sql = text("""
            SELECT ID_PERIODO, DT_INICIO, DT_FIM
            FROM DEV.DCA_TB001_PERIODO_AVALIACAO
            WHERE ID = :periodo_id -- Usa a chave primária
            AND ID_EDITAL = :edital_id
            AND DELETED_AT IS NULL
        """)
        result = db.session.execute(sql, {
            'periodo_id': self.periodo_pk_id,  # CORRIGIDO
            'edital_id': self.edital_id
        }).fetchone()
        if result:
            return {'id_periodo': result.ID_PERIODO, 'dt_inicio': result.DT_INICIO.strftime('%d/%m/%Y'),
                    'dt_fim': result.DT_FIM.strftime('%d/%m/%Y')}
        return {'id_periodo': self.periodo_business_id, 'dt_inicio': '', 'dt_fim': ''}

    def _calcular_metas_empresas(self, dados_saldo, valores_siscor):
        """
        Calcula as metas mensais para cada empresa.
        """
        empresas, meses = dados_saldo['empresas'], valores_siscor
        empresas_resultado = []
        for empresa in empresas:
            empresa_metas = {'id_empresa': empresa['id_empresa'], 'nome': empresa['nome'],
                             'saldo_devedor': float(empresa['saldo_devedor']),
                             'percentual': float(empresa['percentual']), 'metas': {}, 'total': 0}
            total_empresa = Decimal('0')
            for mes in meses:
                valor_mes = (mes['valor_siscor'] * (empresa['percentual'] / Decimal('100'))).quantize(Decimal('0.01'),
                                                                                                      rounding=ROUND_HALF_UP)
                empresa_metas['metas'][mes['competencia']] = float(valor_mes)
                total_empresa += valor_mes
            empresa_metas['total'] = float(total_empresa)
            empresas_resultado.append(empresa_metas)

        meses_resultado, total_geral = [], Decimal('0')
        for mes in meses:
            total_mes = sum(Decimal(str(empresa['metas'][mes['competencia']])) for empresa in empresas_resultado)
            meses_resultado.append(
                {'competencia': mes['competencia'], 'nome': mes['nome'], 'valor_siscor': float(mes['valor_siscor']),
                 'total_mes': float(total_mes)})
            total_geral += total_mes

        return {'empresas': empresas_resultado, 'meses': meses_resultado,
                'total_saldo_devedor': float(dados_saldo['total_saldo_devedor']), 'total_geral': float(total_geral)}

    def salvar_distribuicao(self, dados_calculados=None):
        """
        Salva a distribuição calculada usando a chave de negócio do período.
        """
        try:
            if not dados_calculados:
                dados_calculados = self.calcular_distribuicao()
            for empresa in dados_calculados['empresas']:
                for competencia, valor_meta in empresa['metas'].items():
                    sql_insert = text("""
                        INSERT INTO DEV.DCA_TB018_METAS_REDISTRIBUIDAS_MENSAL
                        (ID_EDITAL, ID_PERIODO, DT_REFERENCIA, ID_EMPRESA, NO_EMPRESA_ABREVIADA, VR_SALDO_DEVEDOR_DISTRIBUIDO, PERCENTUAL_SALDO_DEVEDOR, MES_COMPETENCIA, VR_META_MES, CREATED_AT)
                        VALUES (:edital_id, :periodo_id, :dt_ref, :empresa_id, :nome_empresa, :saldo_devedor, :percentual, :mes_competencia, :valor_meta, :created_at)
                    """)
                    db.session.execute(sql_insert, {
                        'edital_id': self.edital_id,
                        'periodo_id': self.periodo_business_id,  # CORRIGIDO
                        'dt_ref': self.data_referencia,
                        'empresa_id': empresa['id_empresa'], 'nome_empresa': empresa['nome'],
                        'saldo_devedor': empresa['saldo_devedor'], 'percentual': empresa['percentual'],
                        'mes_competencia': competencia, 'valor_meta': valor_meta,
                        'created_at': datetime.now()
                    })
            db.session.commit()
            return True
        except Exception as e:
            db.session.rollback()
            raise Exception(f"Erro ao salvar distribuição: {str(e)}")