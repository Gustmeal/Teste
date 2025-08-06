

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

    def __init__(self, edital_id, periodo_id, incremento_meta=1.0):
        # periodo_id aqui é a CHAVE PRIMÁRIA da tabela de períodos
        self.edital_id = edital_id
        self.periodo_pk_id = periodo_id  # Chave Primária (ex: 1, 2, 3...)
        self.incremento_meta = Decimal(str(incremento_meta))  # Mantém o incremento para o cálculo

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
                    "Verifique se as metas foram cadastradas na tabela BDG.DCA_TB012_METAS."
                )

            periodo_info = self._buscar_info_periodo()
            resultado = self._calcular_metas_empresas(empresas_saldo, valores_siscor)
            resultado[
                'periodo_info'] = f"{periodo_info['id_periodo']} ({periodo_info['dt_inicio']} a {periodo_info['dt_fim']})"

            # Adiciona o incremento ao resultado para exibição na tela
            resultado['incremento_meta'] = float(self.incremento_meta)

            return resultado
        except Exception as e:
            raise Exception(f"Erro ao calcular distribuição: {str(e)}")

    def _buscar_saldos_devedores(self):
        """
        Busca saldo devedor das empresas no sistema de contratos,
        considerando apenas as empresas que não estão descredenciadas.
        """
        sql = text("""
            WITH EmpresasAtivas AS (
                SELECT ID_EMPRESA
                FROM BDG.DCA_TB002_EMPRESAS_PARTICIPANTES
                WHERE ID_EDITAL = :edital_id
                  AND ID_PERIODO = :periodo_id
                  AND DS_CONDICAO <> 'DESCREDENCIADA'
                  AND DELETED_AT IS NULL
            ),
            SaldoTotal AS (
                SELECT SUM(SIT.VR_SD_DEVEDOR) as TotalSD
                FROM BDG.COM_TB007_SITUACAO_CONTRATOS SIT
                INNER JOIN BDG.COM_TB011_EMPRESA_COBRANCA_ATUAL ASS
                    ON SIT.fkContratoSISCTR = ASS.fkContratoSISCTR
                WHERE SIT.fkSituacaoCredito = 1 
                  AND ASS.COD_EMPRESA_COBRANCA NOT IN (407, 422)
                  AND ASS.COD_EMPRESA_COBRANCA IN (SELECT ID_EMPRESA FROM EmpresasAtivas)
            )
            SELECT 
                ASS.COD_EMPRESA_COBRANCA as id_empresa,
                SUM(SIT.VR_SD_DEVEDOR) as saldo_devedor,
                (SUM(SIT.VR_SD_DEVEDOR) / NULLIF((SELECT TotalSD FROM SaldoTotal), 0) * 100) as percentual
            FROM BDG.COM_TB007_SITUACAO_CONTRATOS SIT
            INNER JOIN BDG.COM_TB011_EMPRESA_COBRANCA_ATUAL ASS
                ON SIT.fkContratoSISCTR = ASS.fkContratoSISCTR
            WHERE SIT.fkSituacaoCredito = 1
              AND ASS.COD_EMPRESA_COBRANCA NOT IN (407, 422)
              AND ASS.COD_EMPRESA_COBRANCA IN (SELECT ID_EMPRESA FROM EmpresasAtivas)
            GROUP BY ASS.COD_EMPRESA_COBRANCA
            HAVING SUM(SIT.VR_SD_DEVEDOR) > 0
            ORDER BY ASS.COD_EMPRESA_COBRANCA
        """)
        result = db.session.execute(sql, {
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_business_id
        }).fetchall()

        empresas = []
        total_sd = Decimal('0')
        for row in result:
            nome_empresa = self._buscar_nome_empresa(row.id_empresa)
            empresas.append({
                'id_empresa': row.id_empresa,
                'nome': nome_empresa,
                'saldo_devedor': Decimal(str(row.saldo_devedor)),
                'percentual': Decimal(str(row.percentual or '0'))
            })
            total_sd += Decimal(str(row.saldo_devedor))

        return {'empresas': empresas, 'total_saldo_devedor': total_sd}

    def _buscar_nome_empresa(self, cod_empresa):
        sql = text("""
            SELECT TOP 1 NO_EMPRESA_ABREVIADA
            FROM BDG.DCA_TB002_EMPRESAS_PARTICIPANTES
            WHERE ID_EMPRESA = :id_empresa AND ID_EDITAL = :edital_id AND ID_PERIODO = :periodo_id AND DELETED_AT IS NULL
        """)
        result = db.session.execute(sql, {
            'id_empresa': cod_empresa,
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_business_id
        }).fetchone()
        return result.NO_EMPRESA_ABREVIADA if result else f"Empresa {cod_empresa}"

    def _buscar_valores_siscor(self):
        sql = text("""
            SELECT 
                CAST(COMPETENCIA AS VARCHAR(10)) as competencia, VR_MENSAL_SISCOR, QTDE_DIAS_UTEIS_MES
            FROM BDG.DCA_TB012_METAS
            WHERE ID_EDITAL = :edital_id AND ID_PERIODO = :periodo_id AND DELETED_AT IS NULL
            AND DT_REFERENCIA = (
                SELECT MAX(DT_REFERENCIA) FROM BDG.DCA_TB012_METAS
                WHERE ID_EDITAL = :edital_id AND ID_PERIODO = :periodo_id AND DELETED_AT IS NULL
            )
            ORDER BY COMPETENCIA
        """)
        result = db.session.execute(sql, {
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_business_id
        })
        valores = []
        meses_nomes = {'01': 'JAN', '02': 'FEV', '03': 'MAR', '04': 'ABR', '05': 'MAI', '06': 'JUN', '07': 'JUL',
                       '08': 'AGO', '09': 'SET', '10': 'OUT', '11': 'NOV', '12': 'DEZ'}
        for row in result:
            competencia_str = str(row.competencia).strip()
            ano, mes = (competencia_str[:4], competencia_str[4:].zfill(2)) if len(competencia_str) == 6 else (
            competencia_str.split('-')[0], competencia_str.split('-')[1])
            valores.append({
                'competencia': f"{ano}-{mes}", 'nome': meses_nomes.get(mes, mes),
                'valor_siscor': Decimal(str(row.VR_MENSAL_SISCOR or '0')),
                'dias_uteis': int(row.QTDE_DIAS_UTEIS_MES or 20)
            })
        return valores

    def _buscar_info_periodo(self):
        sql = text("""
            SELECT ID_PERIODO, DT_INICIO, DT_FIM
            FROM BDG.DCA_TB001_PERIODO_AVALIACAO
            WHERE ID = :periodo_id AND ID_EDITAL = :edital_id AND DELETED_AT IS NULL
        """)
        result = db.session.execute(sql, {
            'periodo_id': self.periodo_pk_id,
            'edital_id': self.edital_id
        }).fetchone()
        if result:
            return {'id_periodo': result.ID_PERIODO, 'dt_inicio': result.DT_INICIO.strftime('%d/%m/%Y'),
                    'dt_fim': result.DT_FIM.strftime('%d/%m/%Y')}
        return {'id_periodo': self.periodo_business_id, 'dt_inicio': '', 'dt_fim': ''}

    def _calcular_metas_empresas(self, dados_saldo, valores_siscor):
        empresas, meses = dados_saldo['empresas'], valores_siscor
        empresas_resultado = []
        for empresa in empresas:
            empresa_metas = {'id_empresa': empresa['id_empresa'], 'nome': empresa['nome'],
                             'saldo_devedor': float(empresa['saldo_devedor']),
                             'percentual': float(empresa['percentual']), 'metas': {}, 'total': 0}
            total_empresa = Decimal('0')
            for mes in meses:
                valor_siscor_incrementado = mes['valor_siscor'] * self.incremento_meta
                valor_mes = (valor_siscor_incrementado * (empresa['percentual'] / Decimal('100'))).quantize(
                    Decimal('0.01'), rounding=ROUND_HALF_UP)
                empresa_metas['metas'][mes['competencia']] = float(valor_mes)
                total_empresa += valor_mes
            empresa_metas['total'] = float(total_empresa)
            empresas_resultado.append(empresa_metas)

        totais_siscor = {mes['competencia']: mes['valor_siscor'] for mes in meses}
        totais_meta_incrementada = {comp: val * self.incremento_meta for comp, val in totais_siscor.items()}
        total_geral_siscor = sum(totais_siscor.values())
        total_geral_incrementado = sum(totais_meta_incrementada.values())

        return {
            'empresas': empresas_resultado,
            'meses': meses,
            'totais_siscor': {k: float(v) for k, v in totais_siscor.items()},
            'totais_meta_incrementada': {k: float(v) for k, v in totais_meta_incrementada.items()},
            'total_saldo_devedor': float(dados_saldo['total_saldo_devedor']),
            'total_geral_siscor': float(total_geral_siscor),
            'total_geral_incrementado': float(total_geral_incrementado)
        }

    def salvar_distribuicao(self, dados_calculados=None):
        """
        Salva a distribuição calculada nas três tabelas:
        1. TB016_METAS_REDISTRIBUIDAS_MENSAL (compatibilidade)
        2. TB017_DISTRIBUICAO_SUMARIO (nova - resumo)
        3. TB019_DISTRIBUICAO_MENSAL (nova - detalhes)
        """
        try:
            if not dados_calculados:
                dados_calculados = self.calcular_distribuicao()

            # Para cada empresa, salvar nas três tabelas
            for empresa in dados_calculados['empresas']:
                # 1. Primeiro, inserir na tabela SUMARIO e pegar o ID gerado
                sql_sumario = text("""
                    INSERT INTO BDG.DCA_TB017_DISTRIBUICAO_SUMARIO
                    (ID_EDITAL, ID_PERIODO, DT_REFERENCIA, ID_EMPRESA, NO_EMPRESA_ABREVIADA, 
                     VR_SALDO_DEVEDOR_DISTRIBUIDO, PERCENTUAL_SALDO_DEVEDOR, CREATED_AT)
                    OUTPUT INSERTED.ID
                    VALUES (:edital_id, :periodo_id, :dt_ref, :empresa_id, :nome_empresa, 
                            :saldo_devedor, :percentual, :created_at)
                """)

                result_sumario = db.session.execute(sql_sumario, {
                    'edital_id': self.edital_id,
                    'periodo_id': self.periodo_business_id,
                    'dt_ref': self.data_referencia,
                    'empresa_id': empresa['id_empresa'],
                    'nome_empresa': empresa['nome'],
                    'saldo_devedor': empresa['saldo_devedor'],
                    'percentual': empresa['percentual'],
                    'created_at': datetime.now()
                })

                # Pegar o ID gerado
                id_sumario = result_sumario.fetchone()[0]

                # 2. Inserir os detalhes mensais na TB019
                for competencia, valor_meta in empresa['metas'].items():
                    # CORREÇÃO: Adicionadas as colunas ID_EDITAL e ID_PERIODO
                    sql_detalhe = text("""
                        INSERT INTO BDG.DCA_TB019_DISTRIBUICAO_MENSAL
                        (ID_DISTRIBUICAO_SUMARIO, ID_EDITAL, ID_PERIODO, ID_EMPRESA, MES_COMPETENCIA, VR_META_MES, CREATED_AT)
                        VALUES (:id_sumario, :id_edital, :id_periodo, :id_empresa, :mes_competencia, :valor_meta, :created_at)
                    """)

                    # CORREÇÃO: Adicionados os parâmetros id_edital e id_periodo
                    db.session.execute(sql_detalhe, {
                        'id_sumario': id_sumario,
                        'id_edital': self.edital_id,
                        'id_periodo': self.periodo_business_id,
                        'id_empresa': empresa['id_empresa'],
                        'mes_competencia': competencia,
                        'valor_meta': valor_meta,
                        'created_at': datetime.now()
                    })

                    # 3. Manter a inserção na tabela original TB018 para compatibilidade
                    sql_original = text("""
                        INSERT INTO BDG.DCA_TB018_METAS_REDISTRIBUIDAS_MENSAL
                        (ID_EDITAL, ID_PERIODO, DT_REFERENCIA, ID_EMPRESA, NO_EMPRESA_ABREVIADA, 
                         VR_SALDO_DEVEDOR_DISTRIBUIDO, PERCENTUAL_SALDO_DEVEDOR, MES_COMPETENCIA, 
                         VR_META_MES, CREATED_AT)
                        VALUES (:edital_id, :periodo_id, :dt_ref, :empresa_id, :nome_empresa, 
                                :saldo_devedor, :percentual, :mes_competencia, :valor_meta, :created_at)
                    """)

                    db.session.execute(sql_original, {
                        'edital_id': self.edital_id,
                        'periodo_id': self.periodo_business_id,
                        'dt_ref': self.data_referencia,
                        'empresa_id': empresa['id_empresa'],
                        'nome_empresa': empresa['nome'],
                        'saldo_devedor': empresa['saldo_devedor'],
                        'percentual': empresa['percentual'],
                        'mes_competencia': competencia,
                        'valor_meta': valor_meta,
                        'created_at': datetime.now()
                    })

            db.session.commit()
            return True

        except Exception as e:
            db.session.rollback()
            raise Exception(f"Erro ao salvar distribuição: {str(e)}")