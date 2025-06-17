from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, date
from sqlalchemy import text
from app import db
from app.models.empresa_participante import EmpresaParticipante
from app.models.metas_redistribuicao import MetasPercentuaisDistribuicao


class RedistribuicaoDinamica:
    def __init__(self, edital_id, periodo_id, empresa_saindo_id, data_descredenciamento, incremento_meta=1.0):
        self.edital_id = edital_id
        self.periodo_id = periodo_id
        self.empresa_saindo_id = empresa_saindo_id
        self.data_descredenciamento = datetime.strptime(data_descredenciamento, '%Y-%m-%d').date()
        self.incremento_meta = Decimal(str(incremento_meta))

    def calcular_redistribuicao(self):
        """Calcula a redistribuição quando uma empresa sai"""
        # Buscar SEMPRE a última DT_REFERENCIA para redistribuição
        ultima_distribuicao = self._buscar_ultima_distribuicao()

        if not ultima_distribuicao:
            raise ValueError("Nenhuma distribuição encontrada para calcular redistribuição")

        # Identificar empresa que está saindo
        empresa_saindo = None
        empresas_continuam = []
        total_sd_continua = Decimal('0')

        for empresa in ultima_distribuicao:
            # Apenas empresas com percentual > 0 estão ativas
            if empresa['percentual'] > 0:
                if empresa['id_empresa'] == self.empresa_saindo_id:
                    empresa_saindo = empresa
                else:
                    empresas_continuam.append(empresa)
                    total_sd_continua += empresa['saldo_devedor']

        if not empresa_saindo:
            raise ValueError("Empresa selecionada não está ativa ou já foi redistribuída")

        # Percentual a ser redistribuído
        percentual_redistribuir = empresa_saindo['percentual']

        # Calcular novos percentuais
        novas_distribuicoes = []

        for empresa in empresas_continuam:
            # Proporção do SD desta empresa no total
            proporcao_sd = empresa['saldo_devedor'] / total_sd_continua

            # Parcela do percentual que esta empresa receberá
            percentual_adicional = percentual_redistribuir * proporcao_sd

            # Novo percentual = percentual atual + percentual adicional
            novo_percentual = (empresa['percentual'] + percentual_adicional).quantize(
                Decimal('0.00000001'), rounding=ROUND_HALF_UP
            )

            novas_distribuicoes.append({
                'id_empresa': empresa['id_empresa'],
                'nome_empresa': empresa['nome_empresa'],
                'saldo_devedor': empresa['saldo_devedor'],
                'percentual_anterior': empresa['percentual'],
                'percentual_novo': novo_percentual
            })

        # Validar que a soma dos percentuais é 100%
        soma_percentuais = sum(e['percentual_novo'] for e in novas_distribuicoes)

        # Ajustar pequenas diferenças de arredondamento
        if abs(soma_percentuais - Decimal('100')) < Decimal('0.0001'):
            # Ajustar no maior percentual
            maior = max(novas_distribuicoes, key=lambda x: x['percentual_novo'])
            maior['percentual_novo'] += (Decimal('100') - soma_percentuais)

        return {
            'empresa_saindo': {
                'id': empresa_saindo['id_empresa'],
                'nome': empresa_saindo['nome_empresa'],
                'saldo_devedor': float(empresa_saindo['saldo_devedor']),
                'percentual': float(empresa_saindo['percentual'])
            },
            'data_descredenciamento': self.data_descredenciamento.strftime('%Y-%m-%d'),
            'data_referencia_base': empresa_saindo['data_referencia'].strftime('%Y-%m-%d'),
            'novas_distribuicoes': [{
                'id_empresa': e['id_empresa'],
                'nome_empresa': e['nome_empresa'],
                'saldo_devedor': float(e['saldo_devedor']),
                'percentual_anterior': float(e['percentual_anterior']),
                'percentual_novo': float(e['percentual_novo'])
            } for e in novas_distribuicoes],
            'total_percentual_anterior': float(sum(e['percentual_anterior'] for e in novas_distribuicoes)),
            'total_percentual_novo': float(soma_percentuais)  # Usar a soma já calculada
        }
    def _buscar_ultima_distribuicao(self):
        """Busca SEMPRE a última distribuição válida (última DT_REFERENCIA)"""
        # Primeiro, buscar a última data
        sql_ultima_data = text("""
            SELECT MAX(DT_REFERENCIA)
            FROM DEV.DCA_TB015_METAS_PERCENTUAIS_DISTRIBUICAO
            WHERE ID_EDITAL = :edital_id
            AND ID_PERIODO = :periodo_id
            AND DELETED_AT IS NULL
        """)

        result_data = db.session.execute(sql_ultima_data, {
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_id
        }).fetchone()

        if not result_data or not result_data[0]:
            return None

        ultima_data = result_data[0]

        # Agora buscar TODOS os registros dessa data
        sql = text("""
            SELECT 
                mpd.ID_EMPRESA,
                emp.NO_EMPRESA_ABREVIADA,
                mpd.VR_SALDO_DEVEDOR_DISTRIBUIDO,
                mpd.PERCENTUAL_SALDO_DEVEDOR,
                mpd.DT_REFERENCIA
            FROM DEV.DCA_TB015_METAS_PERCENTUAIS_DISTRIBUICAO mpd
            JOIN DEV.DCA_TB002_EMPRESAS_PARTICIPANTES emp 
                ON mpd.ID_EMPRESA = emp.ID_EMPRESA
                AND mpd.ID_EDITAL = emp.ID_EDITAL
                AND mpd.ID_PERIODO = emp.ID_PERIODO
            WHERE mpd.ID_EDITAL = :edital_id
            AND mpd.ID_PERIODO = :periodo_id
            AND mpd.DT_REFERENCIA = :ultima_data
            AND mpd.DELETED_AT IS NULL
            ORDER BY emp.NO_EMPRESA_ABREVIADA
        """)

        result = db.session.execute(sql, {
            'edital_id': self.edital_id,
            'periodo_id': self.periodo_id,
            'ultima_data': ultima_data
        })

        distribuicao = []
        for row in result:
            distribuicao.append({
                'id_empresa': row[0],
                'nome_empresa': row[1],
                'saldo_devedor': Decimal(str(row[2])) if row[2] else Decimal('0'),
                'percentual': Decimal(str(row[3])) if row[3] else Decimal('0'),
                'data_referencia': row[4]
            })

        return distribuicao

    def salvar_redistribuicao(self, resultado):
        """Salva a nova distribuição no banco de dados"""
        try:
            # NÃO atualizar DS_CONDICAO - empresa já está descredenciada

            # Inserir nova distribuição na TB015
            # Pegar TODAS as empresas da última distribuição
            ultima_dist = self._buscar_ultima_distribuicao()

            # Criar mapa de empresas ativas na redistribuição
            empresas_redistribuidas = {dist['id_empresa']: dist for dist in resultado['novas_distribuicoes']}

            # Inserir registros para TODAS as empresas
            for empresa in ultima_dist:
                id_empresa = empresa['id_empresa']

                if id_empresa == self.empresa_saindo_id:
                    # Empresa que está saindo agora
                    nova_dist = MetasPercentuaisDistribuicao(
                        DT_REFERENCIA=self.data_descredenciamento,
                        ID_EDITAL=self.edital_id,
                        ID_PERIODO=self.periodo_id,
                        ID_EMPRESA=id_empresa,
                        VR_SALDO_DEVEDOR_DISTRIBUIDO=Decimal('0'),
                        PERCENTUAL_SALDO_DEVEDOR=Decimal('0')
                    )
                elif id_empresa in empresas_redistribuidas:
                    # Empresa que continua ativa
                    dist = empresas_redistribuidas[id_empresa]
                    nova_dist = MetasPercentuaisDistribuicao(
                        DT_REFERENCIA=self.data_descredenciamento,
                        ID_EDITAL=self.edital_id,
                        ID_PERIODO=self.periodo_id,
                        ID_EMPRESA=id_empresa,
                        VR_SALDO_DEVEDOR_DISTRIBUIDO=Decimal(str(dist['saldo_devedor'])),
                        PERCENTUAL_SALDO_DEVEDOR=Decimal(str(dist['percentual_novo']))
                    )
                else:
                    # Empresa já descredenciada anteriormente (percentual = 0)
                    nova_dist = MetasPercentuaisDistribuicao(
                        DT_REFERENCIA=self.data_descredenciamento,
                        ID_EDITAL=self.edital_id,
                        ID_PERIODO=self.periodo_id,
                        ID_EMPRESA=id_empresa,
                        VR_SALDO_DEVEDOR_DISTRIBUIDO=Decimal('0'),
                        PERCENTUAL_SALDO_DEVEDOR=Decimal('0')
                    )

                db.session.add(nova_dist)

            # Salvar incremento se necessário
            if self.incremento_meta != Decimal('1.00'):
                # Inserir novo registro na TB014 com a data de redistribuição
                sql_insert_periodo = text("""
                    INSERT INTO DEV.DCA_TB014_METAS_PERIODO_AVALIATIVO
                    (DT_REFERENCIA, ID_EDITAL, ID_PERIODO, VR_GLOBAL_SISCOR, 
                     QTDE_DIAS_UTEIS_PERIODO, INDICE_INCREMENTO_META, 
                     VR_META_A_DISTRIBUIR, VR_POR_DIA_UTIL, CREATED_AT)
                    SELECT 
                        :data_ref,
                        ID_EDITAL,
                        ID_PERIODO,
                        VR_GLOBAL_SISCOR,
                        QTDE_DIAS_UTEIS_PERIODO,
                        :incremento,
                        VR_GLOBAL_SISCOR * :incremento,
                        (VR_GLOBAL_SISCOR * :incremento) / QTDE_DIAS_UTEIS_PERIODO,
                        :created_at
                    FROM DEV.DCA_TB014_METAS_PERIODO_AVALIATIVO
                    WHERE ID_EDITAL = :edital_id
                    AND ID_PERIODO = :periodo_id
                    AND DT_REFERENCIA = (
                        SELECT MAX(DT_REFERENCIA)
                        FROM DEV.DCA_TB014_METAS_PERIODO_AVALIATIVO
                        WHERE ID_EDITAL = :edital_id
                        AND ID_PERIODO = :periodo_id
                        AND DELETED_AT IS NULL
                    )
                """)

                db.session.execute(sql_insert_periodo, {
                    'data_ref': self.data_descredenciamento,
                    'incremento': self.incremento_meta,
                    'edital_id': self.edital_id,
                    'periodo_id': self.periodo_id,
                    'created_at': datetime.now()
                })

            db.session.commit()
            return True

        except Exception as e:
            db.session.rollback()
            raise e