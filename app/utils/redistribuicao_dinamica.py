# app/utils/redistribuicao_dinamica.py

from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, date
from sqlalchemy import text
from app import db
from app.models.periodo import PeriodoAvaliacao


class RedistribuicaoDinamica:
    def __init__(self, edital_id, periodo_id, empresa_saindo_id, data_descredenciamento, incremento_meta=1.0):
        self.edital_id = edital_id
        self.periodo_pk_id = periodo_id

        periodo_obj = PeriodoAvaliacao.query.get(self.periodo_pk_id)
        if not periodo_obj:
            raise ValueError(f"Período de avaliação com a chave primária {self.periodo_pk_id} não foi encontrado.")
        self.periodo_business_id = periodo_obj.ID_PERIODO

        # Garantir que o ID da empresa seja um inteiro para comparação
        self.empresa_saindo_id = int(empresa_saindo_id)
        self.data_descredenciamento = datetime.strptime(data_descredenciamento, '%Y-%m-%d').date()
        self.incremento_meta = Decimal(str(incremento_meta))

    def calcular_redistribuicao(self):
        """Calcula a redistribuição com base no estado atual da TB018."""

        empresas_map, meses_cabecalho = self._buscar_distribuicao_anterior()
        if not empresas_map or not meses_cabecalho:
            raise ValueError("Nenhuma distribuição inicial encontrada para este período. Não é possível redistribuir.")

        valores_siscor_lookup = self._buscar_valores_siscor_lookup()

        # Validação crucial para evitar o erro no frontend
        empresa_saindo = empresas_map.get(self.empresa_saindo_id)
        if not empresa_saindo:
            raise ValueError(
                f"A empresa com ID {self.empresa_saindo_id} não foi encontrada na última distribuição salva. Verifique os dados.")

        if empresa_saindo['percentual'] == 0:
            raise ValueError("Esta empresa já foi descredenciada e seu percentual já é zero.")

        empresas_ativas = [emp for id, emp in empresas_map.items() if
                           id != self.empresa_saindo_id and emp['percentual'] > 0]
        total_percentual_ativo = sum(emp['percentual'] for emp in empresas_ativas)

        if total_percentual_ativo == 0:
            raise ValueError("Não há outras empresas ativas para receber a redistribuição.")

        percentual_saindo = empresa_saindo['percentual']
        sd_saindo = empresa_saindo['saldo_devedor']
        for emp in empresas_ativas:
            proporcao = emp['percentual'] / total_percentual_ativo
            emp['novo_sd'] = emp['saldo_devedor'] + (sd_saindo * proporcao)
            emp['novo_percentual'] = emp['percentual'] + (percentual_saindo * proporcao)

        empresas_resultado = []
        empresas_resultado.append(
            self._processar_empresa_saindo(empresa_saindo, meses_cabecalho, valores_siscor_lookup))
        for emp in empresas_ativas:
            empresas_resultado.append(
                self._processar_empresa_ativa(emp, meses_cabecalho, empresa_saindo, valores_siscor_lookup,
                                              total_percentual_ativo))
        for id, emp in empresas_map.items():
            if id != self.empresa_saindo_id and emp['percentual'] == 0:
                empresas_resultado.append(self._processar_empresa_descredenciada(emp, meses_cabecalho))

        empresas_resultado.sort(key=lambda x: x['nome'])

        return {
            'meses': meses_cabecalho,
            'empresas': empresas_resultado,
            'empresa_saindo': {
                'id': empresa_saindo['id_empresa'], 'nome': empresa_saindo['nome'],
                'percentual_anterior': float(empresa_saindo['percentual']),
                'saldo_devedor': float(empresa_saindo['saldo_devedor'])
            }
        }

    def _buscar_distribuicao_anterior(self):
        sql = text("""
            SELECT m.ID_EMPRESA as id_empresa, m.NO_EMPRESA_ABREVIADA as nome,
                   m.VR_SALDO_DEVEDOR_DISTRIBUIDO as saldo_devedor,
                   m.PERCENTUAL_SALDO_DEVEDOR as percentual,
                   m.MES_COMPETENCIA as mes_competencia, m.VR_META_MES as valor_meta
            FROM BDG.DCA_TB018_METAS_REDISTRIBUIDAS_MENSAL m
            WHERE m.ID_EDITAL = :edital_id AND m.ID_PERIODO = :periodo_id
            AND m.DT_REFERENCIA = (
                SELECT MAX(DT_REFERENCIA) FROM BDG.DCA_TB018_METAS_REDISTRIBUIDAS_MENSAL
                WHERE ID_EDITAL = :edital_id AND ID_PERIODO = :periodo_id AND DELETED_AT IS NULL
            ) AND m.DELETED_AT IS NULL
            ORDER BY m.MES_COMPETENCIA, m.NO_EMPRESA_ABREVIADA
        """)
        result = db.session.execute(sql, {'edital_id': self.edital_id, 'periodo_id': self.periodo_business_id})

        empresas_map = {}
        meses_set = set()
        for row in result:
            id_empresa = int(row.id_empresa)
            if id_empresa not in empresas_map:
                empresas_map[id_empresa] = {'id_empresa': id_empresa, 'nome': row.nome,
                                            'saldo_devedor': Decimal(str(row.saldo_devedor)),
                                            'percentual': Decimal(str(row.percentual)), 'metas': {}}
            empresas_map[id_empresa]['metas'][row.mes_competencia] = Decimal(str(row.valor_meta))
            meses_set.add(row.mes_competencia)

        meses_nomes = {'01': 'JAN', '02': 'FEV', '03': 'MAR', '04': 'ABR', '05': 'MAI', '06': 'JUN', '07': 'JUL',
                       '08': 'AGO', '09': 'SET', '10': 'OUT', '11': 'NOV', '12': 'DEZ'}
        meses_cabecalho = [{'competencia': c, 'nome': meses_nomes.get(c.split('-')[1], c)} for c in
                           sorted(list(meses_set))]
        return empresas_map, meses_cabecalho

    def _buscar_valores_siscor_lookup(self):
        sql = text("""
            SELECT COMPETENCIA, VR_MENSAL_SISCOR, QTDE_DIAS_UTEIS_MES FROM BDG.DCA_TB012_METAS
            WHERE ID_EDITAL = :edital_id AND ID_PERIODO = :periodo_id AND DELETED_AT IS NULL
            AND DT_REFERENCIA = (SELECT MAX(DT_REFERENCIA) FROM BDG.DCA_TB012_METAS WHERE ID_EDITAL = :edital_id AND ID_PERIODO = :periodo_id AND DELETED_AT IS NULL)
        """)
        result = db.session.execute(sql, {'edital_id': self.edital_id, 'periodo_id': self.periodo_business_id})
        lookup = {}
        for row in result:
            comp_str = str(row.COMPETENCIA)
            comp_fmt = f"{comp_str[:4]}-{comp_str[4:]}" if len(comp_str) == 6 else comp_str
            lookup[comp_fmt] = {'valor': Decimal(str(row.VR_MENSAL_SISCOR)),
                                'dias_uteis': int(row.QTDE_DIAS_UTEIS_MES or 20)}
        return lookup

    def _calcular_dias_trabalhados(self, dias_uteis_mes):
        return int((self.data_descredenciamento.day / 30.0) * dias_uteis_mes)

    def _processar_empresa_saindo(self, empresa, meses_cabecalho, valores_siscor_lookup):
        mes_saida_num = self.data_descredenciamento.month
        empresa_result = {'id': empresa['id_empresa'], 'nome': empresa['nome'], 'saldo_devedor': 0,
                          'percentual_anterior': float(empresa['percentual']), 'percentual_novo': 0, 'destacar': True,
                          'meses': {}, 'total': Decimal('0')}
        for mes_info in meses_cabecalho:
            competencia = mes_info['competencia']
            mes_num = int(competencia.split('-')[1])
            valor = Decimal('0')
            if mes_num < mes_saida_num:
                valor = empresa['metas'].get(competencia, Decimal('0'))
            elif mes_num == mes_saida_num:
                valor_mes_completo = empresa['metas'].get(competencia, Decimal('0'))
                dias_uteis_mes = valores_siscor_lookup.get(competencia, {}).get('dias_uteis', 20)
                dias_trabalhados = self._calcular_dias_trabalhados(dias_uteis_mes)
                valor = (valor_mes_completo * Decimal(dias_trabalhados)) / Decimal(dias_uteis_mes)
            empresa_result['meses'][competencia] = float(valor.quantize(Decimal('0.01')))
            empresa_result['total'] += valor
        empresa_result['total'] = float(empresa_result['total'].quantize(Decimal('0.01')))
        return empresa_result

    def _processar_empresa_ativa(self, empresa, meses_cabecalho, empresa_saindo, valores_siscor_lookup,
                                 total_percentual_ativo):
        mes_saida_num = self.data_descredenciamento.month
        empresa_result = {'id': empresa['id_empresa'], 'nome': empresa['nome'],
                          'saldo_devedor': float(empresa['novo_sd']),
                          'percentual_anterior': float(empresa['percentual']),
                          'percentual_novo': float(empresa['novo_percentual']), 'destacar': False, 'meses': {},
                          'total': Decimal('0')}
        for mes_info in meses_cabecalho:
            competencia = mes_info['competencia']
            mes_num = int(competencia.split('-')[1])
            valor = Decimal('0')
            if mes_num < mes_saida_num:
                valor = empresa['metas'].get(competencia, Decimal('0'))
            elif mes_num == mes_saida_num:
                valor_original = empresa['metas'].get(competencia, Decimal('0'))
                valor_saindo_completo = empresa_saindo['metas'].get(competencia, Decimal('0'))
                dias_uteis_mes = valores_siscor_lookup.get(competencia, {}).get('dias_uteis', 20)
                dias_trabalhados_saindo = self._calcular_dias_trabalhados(dias_uteis_mes)
                valor_nao_trabalhado = valor_saindo_completo * (
                            Decimal(dias_uteis_mes - dias_trabalhados_saindo) / Decimal(dias_uteis_mes))
                proporcao = empresa['percentual'] / total_percentual_ativo
                valor = valor_original + (valor_nao_trabalhado * proporcao)
            else:
                siscor_info = valores_siscor_lookup.get(competencia)
                if siscor_info:
                    valor = siscor_info['valor'] * (empresa['novo_percentual'] / Decimal(100))
            empresa_result['meses'][competencia] = float(valor.quantize(Decimal('0.01')))
            empresa_result['total'] += valor
        empresa_result['total'] = float(empresa_result['total'].quantize(Decimal('0.01')))
        return empresa_result

    def _processar_empresa_descredenciada(self, empresa, meses_cabecalho):
        empresa_result = {'id': empresa['id_empresa'], 'nome': empresa['nome'], 'saldo_devedor': 0,
                          'percentual_anterior': 0, 'percentual_novo': 0, 'destacar': False, 'meses': {},
                          'total': Decimal('0')}
        for mes_info in meses_cabecalho:
            competencia = mes_info['competencia']
            valor = empresa['metas'].get(competencia, Decimal('0'))
            empresa_result['meses'][competencia] = float(valor)
            empresa_result['total'] += valor
        empresa_result['total'] = float(empresa_result['total'])
        return empresa_result

    def salvar_redistribuicao(self, resultado_calculado=None):
        """
        Salva a redistribuição nas três tabelas:
        1. TB016_METAS_REDISTRIBUIDAS_MENSAL (compatibilidade)
        2. TB017_DISTRIBUICAO_SUMARIO (nova - resumo)
        3. TB019_DISTRIBUICAO_MENSAL (nova - detalhes)
        """
        try:
            # Se não foi passado resultado, calcular
            if not resultado_calculado:
                resultado_calculado = self.calcular_redistribuicao()

            for empresa in resultado_calculado['empresas']:
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
                    'dt_ref': self.data_descredenciamento,  # Usando a data de descredenciamento
                    'empresa_id': empresa['id'],
                    'nome_empresa': empresa['nome'],
                    'saldo_devedor': empresa.get('saldo_devedor', 0),
                    'percentual': empresa.get('percentual_novo', 0),
                    'created_at': datetime.now()
                })

                # Pegar o ID gerado
                id_sumario = result_sumario.fetchone()[0]

                # 2. Inserir os detalhes mensais na TB019
                for competencia, valor in empresa['meses'].items():
                    sql_detalhe = text("""
                        INSERT INTO BDG.DCA_TB019_DISTRIBUICAO_MENSAL
                        (ID_DISTRIBUICAO_SUMARIO, MES_COMPETENCIA, VR_META_MES, CREATED_AT)
                        VALUES (:id_sumario, :mes_competencia, :valor_meta, :created_at)
                    """)

                    db.session.execute(sql_detalhe, {
                        'id_sumario': id_sumario,
                        'mes_competencia': competencia,
                        'valor_meta': valor,
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
                        'dt_ref': self.data_descredenciamento,
                        'empresa_id': empresa['id'],
                        'nome_empresa': empresa['nome'],
                        'saldo_devedor': empresa.get('saldo_devedor', 0),
                        'percentual': empresa.get('percentual_novo', 0),
                        'mes_competencia': competencia,
                        'valor_meta': valor,
                        'created_at': datetime.now()
                    })

            db.session.commit()
            return True

        except Exception as e:
            db.session.rollback()
            raise e