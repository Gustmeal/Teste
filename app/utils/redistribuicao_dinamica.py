from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, date
from sqlalchemy import text
from app import db
from app.utils.visualizador_redistribuicao import VisualizadorRedistribuicao


class RedistribuicaoDinamica:
    def __init__(self, edital_id, periodo_id, empresa_saindo_id, data_descredenciamento, incremento_meta=1.0):
        self.edital_id = edital_id
        self.periodo_id = periodo_id
        self.empresa_saindo_id = empresa_saindo_id
        self.data_descredenciamento = datetime.strptime(data_descredenciamento, '%Y-%m-%d').date()
        self.incremento_meta = Decimal(str(incremento_meta))

        # Datas conhecidas de saída
        self.datas_saida_conhecidas = {
            'Real': date(2025, 3, 26),
            'H.Costa': date(2025, 5, 16)
        }

    def calcular_redistribuicao(self):
        """Calcula a redistribuição quando uma empresa sai"""
        # Usar o visualizador para pegar a última tabela disponível
        visualizador = VisualizadorRedistribuicao(self.edital_id, self.periodo_id)
        dados_completos = visualizador.calcular_redistribuicao_completa()

        # Determinar qual tabela usar como base
        tabela_base = None

        # Sempre usar a última tabela disponível
        if 'tabela3' in dados_completos and dados_completos['tabela3']:
            tabela_base = dados_completos['tabela3']
            print("Usando tabela3 (após H.Costa)")
        elif 'tabela2' in dados_completos and dados_completos['tabela2']:
            tabela_base = dados_completos['tabela2']
            print("Usando tabela2 (após Real)")
        else:
            tabela_base = dados_completos['tabela1']
            print("Usando tabela1 (inicial)")

        if not tabela_base:
            raise ValueError("Nenhuma tabela de distribuição encontrada")

        # DEBUG - Ver empresas na tabela base
        print("\n=== EMPRESAS NA TABELA BASE ===")
        for emp in tabela_base['empresas']:
            print(f"ID: {emp['id']}, Nome: {emp['nome']}, Percentual: {emp['percentual']}")
            # DEBUG - Ver valores mensais
            print(f"  Valores mensais: {emp.get('metas', {})}")

        # Calcular a redistribuição com base na tabela selecionada
        return self._calcular_nova_distribuicao(tabela_base)

    def _calcular_nova_distribuicao(self, tabela_base):
        """Calcula a nova distribuição baseada na tabela atual"""
        # Pegar meses de dentro da tabela_base
        meses_info = tabela_base['meses']

        # Identificar empresa que está saindo
        empresa_saindo = None
        empresas_continuam = []
        empresas_descredenciadas = []

        # Pegar os dados das empresas da tabela base
        for empresa in tabela_base['empresas']:
            if empresa['id'] == self.empresa_saindo_id:
                empresa_saindo = empresa
                print(f"Empresa saindo encontrada: {empresa['nome']}")
            else:
                if empresa['percentual'] > 0:  # Empresas ativas
                    empresas_continuam.append(empresa)
                else:  # Empresas já descredenciadas
                    empresas_descredenciadas.append(empresa)
                    print(f"Empresa já descredenciada: {empresa['nome']} (percentual: {empresa['percentual']})")

        if not empresa_saindo:
            raise ValueError(f"Empresa selecionada (ID: {self.empresa_saindo_id}) não encontrada na distribuição")

        # Calcular novos percentuais
        percentual_saindo = Decimal(str(empresa_saindo['percentual']))
        soma_percentuais_continuam = sum(Decimal(str(e['percentual'])) for e in empresas_continuam)

        print(f"\nPercentual saindo: {percentual_saindo}")
        print(f"Soma percentuais que continuam: {soma_percentuais_continuam}")

        # Criar nova distribuição
        nova_distribuicao = {
            'meses': meses_info,
            'empresas': [],
            'empresa_saindo': {
                'id': empresa_saindo['id'],
                'nome': empresa_saindo['nome'],
                'percentual_anterior': float(percentual_saindo)
            }
        }

        # Calcular mês da saída
        mes_saida = self.data_descredenciamento.strftime('%Y-%m')
        print(f"\nMês da saída: {mes_saida}")

        # Processar TODAS as empresas
        for empresa in tabela_base['empresas']:
            empresa_data = {
                'id': empresa['id'],
                'nome': empresa['nome'],
                'saldo_devedor': empresa['saldo_devedor'],
                'percentual_anterior': empresa['percentual'],
                'meses': {}
            }

            # Verificar se é a empresa que está saindo agora
            if empresa['id'] == self.empresa_saindo_id:
                print(f"\nProcessando empresa que sai: {empresa['nome']}")

                # Empresa que sai - preservar valores até a saída
                for mes in meses_info:
                    competencia = mes['competencia']
                    valor_original = empresa['metas'].get(competencia, 0)

                    if competencia < mes_saida:
                        # Antes da saída - manter valor original
                        empresa_data['meses'][competencia] = valor_original
                    elif competencia == mes_saida:
                        # Mês da saída - calcular proporcional
                        valor_proporcional = self._calcular_valor_proporcional_saida(valor_original, mes)
                        empresa_data['meses'][competencia] = float(valor_proporcional)
                    else:
                        # Após saída = 0
                        empresa_data['meses'][competencia] = 0

                # Novo percentual será 0
                empresa_data['percentual_novo'] = 0
                empresa_data['destacar'] = True

            elif empresa['percentual'] > 0:
                # Empresas ativas que continuam
                print(f"\nProcessando empresa ativa: {empresa['nome']}")

                # Calcular novo percentual
                percentual_atual = Decimal(str(empresa['percentual']))
                fator_redistribuicao = percentual_atual / soma_percentuais_continuam if soma_percentuais_continuam > 0 else Decimal(
                    '0')
                percentual_novo = percentual_atual + (percentual_saindo * fator_redistribuicao)
                empresa_data['percentual_novo'] = float(percentual_novo)

                # Calcular valores mensais
                for mes in meses_info:
                    competencia = mes['competencia']
                    valor_original = empresa['metas'].get(competencia, 0)

                    if competencia < mes_saida:
                        # Antes da saída - manter valores originais
                        empresa_data['meses'][competencia] = valor_original
                    elif competencia == mes_saida:
                        # Mês da saída - calcular redistribuição
                        valor_redistribuido = self._calcular_valor_redistribuido_mes_saida(
                            valor_original, mes, percentual_atual, percentual_novo
                        )
                        empresa_data['meses'][competencia] = float(valor_redistribuido)
                    else:
                        # Após a saída - usar novo percentual
                        meta_estendida = Decimal(str(mes['meta_estendida']))
                        novo_valor = (meta_estendida * percentual_novo / 100).quantize(
                            Decimal('0.01'), rounding=ROUND_HALF_UP
                        )
                        empresa_data['meses'][competencia] = float(novo_valor)

            else:
                # Empresas já descredenciadas - aplicar regras específicas
                print(f"\nProcessando empresa já descredenciada: {empresa['nome']}")
                empresa_data['percentual_novo'] = 0

                # Determinar quando esta empresa saiu
                data_saida_empresa = self.datas_saida_conhecidas.get(empresa['nome'])

                if data_saida_empresa:
                    mes_saida_empresa = data_saida_empresa.strftime('%Y-%m')
                    print(f"  Data de saída conhecida: {data_saida_empresa} (mês: {mes_saida_empresa})")

                    for mes in meses_info:
                        competencia = mes['competencia']

                        # IMPORTANTE: Para empresas já descredenciadas, não pegar valores da tabela base
                        # pois eles podem estar incorretos. Vamos zerar após a data de saída.

                        if competencia < mes_saida_empresa:
                            # Antes da saída - manter valor original
                            empresa_data['meses'][competencia] = empresa['metas'].get(competencia, 0)
                        elif competencia == mes_saida_empresa:
                            # Mês da saída - valor proporcional (já deve vir da tabela base)
                            empresa_data['meses'][competencia] = empresa['metas'].get(competencia, 0)
                        else:
                            # CORREÇÃO: Após a saída - SEMPRE ZERO
                            empresa_data['meses'][competencia] = 0
                            print(f"    {competencia}: zerado (após saída)")
                else:
                    # Empresa descredenciada sem data conhecida - zerar tudo por segurança
                    print(f"  AVISO: Empresa {empresa['nome']} sem data de saída conhecida - zerando todos os valores")
                    for mes in meses_info:
                        empresa_data['meses'][mes['competencia']] = 0

            # Calcular total
            empresa_data['total'] = sum(empresa_data['meses'].values())
            nova_distribuicao['empresas'].append(empresa_data)

        # Validar soma = 100%
        soma_novos_percentuais = sum(
            Decimal(str(e['percentual_novo'])) for e in nova_distribuicao['empresas'] if e['percentual_novo'] > 0)
        print(f"\nSoma dos novos percentuais: {soma_novos_percentuais}")

        if abs(soma_novos_percentuais - Decimal('100')) > Decimal('0.01'):
            # Ajustar pequenas diferenças no maior percentual
            empresas_ativas = [e for e in nova_distribuicao['empresas'] if e['percentual_novo'] > 0]
            if empresas_ativas:
                maior = max(empresas_ativas, key=lambda x: x['percentual_novo'])
                diferenca = Decimal('100') - soma_novos_percentuais
                maior['percentual_novo'] = float(Decimal(str(maior['percentual_novo'])) + diferenca)
                print(f"Ajustando diferença de {diferenca} no percentual de {maior['nome']}")

        # Adicionar total de saldo devedor
        nova_distribuicao['total_saldo_devedor'] = sum(e['saldo_devedor'] for e in nova_distribuicao['empresas'])

        return nova_distribuicao

    def _calcular_valor_proporcional_saida(self, valor_original, mes_info):
        """Calcula valor proporcional para empresa que sai no mês"""
        if valor_original == 0:
            return Decimal('0')

        # Para data 09/06/2025, considerar 5 dias úteis de 20
        dias_trabalhados = 5
        dias_uteis_mes = mes_info['dias_uteis']

        if dias_uteis_mes > 0:
            valor_proporcional = (Decimal(str(valor_original)) * dias_trabalhados / dias_uteis_mes).quantize(
                Decimal('0.01'), rounding=ROUND_HALF_UP
            )
            return valor_proporcional
        return Decimal('0')

    def _calcular_valor_redistribuido_mes_saida(self, valor_original, mes_info, percentual_antigo, percentual_novo):
        """Calcula valor para empresas que continuam no mês da saída"""
        # Para simplificar, vamos usar os mesmos 5 dias / 20 dias
        dias_antes = 5
        dias_depois = 15
        dias_uteis_total = 20

        if dias_uteis_total > 0:
            # Parte do mês com percentual antigo
            meta_estendida = Decimal(str(mes_info['meta_estendida']))
            valor_parte1 = (meta_estendida * percentual_antigo / 100 * dias_antes / dias_uteis_total)

            # Parte do mês com percentual novo
            valor_parte2 = (meta_estendida * Decimal(str(percentual_novo)) / 100 * dias_depois / dias_uteis_total)

            valor_total = (valor_parte1 + valor_parte2).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            return valor_total

        return Decimal(str(valor_original))

    def salvar_redistribuicao(self):
        """Salva a redistribuição no banco de dados"""
        try:
            # 1. Atualizar status da empresa para DESCREDENCIADA NO PERÍODO
            sql_update_empresa = text("""
                UPDATE DEV.DCA_TB002_EMPRESAS_PARTICIPANTES
                SET DS_CONDICAO = 'DESCREDENCIADA NO PERÍODO',
                    UPDATED_AT = :updated_at
                WHERE ID_EMPRESA = :empresa_id
                AND ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
            """)

            db.session.execute(sql_update_empresa, {
                'empresa_id': self.empresa_saindo_id,
                'edital_id': self.edital_id,
                'periodo_id': self.periodo_id,
                'updated_at': datetime.now()
            })

            # 2. Calcular nova distribuição
            nova_distribuicao = self.calcular_redistribuicao()

            # 3. Inserir nova distribuição na TB015
            for empresa in nova_distribuicao['empresas']:
                sql_insert = text("""
                    INSERT INTO DEV.DCA_TB015_METAS_PERCENTUAIS_DISTRIBUICAO
                    (DT_REFERENCIA, ID_EDITAL, ID_PERIODO, ID_EMPRESA,
                     VR_SALDO_DEVEDOR_DISTRIBUIDO, PERCENTUAL_SALDO_DEVEDOR,
                     CREATED_AT)
                    VALUES (:data_ref, :edital_id, :periodo_id, :empresa_id,
                            :saldo_devedor, :percentual, :created_at)
                """)

                db.session.execute(sql_insert, {
                    'data_ref': self.data_descredenciamento,
                    'edital_id': self.edital_id,
                    'periodo_id': self.periodo_id,
                    'empresa_id': empresa['id'],
                    'saldo_devedor': empresa['saldo_devedor'] if empresa['percentual_novo'] > 0 else 0,
                    'percentual': empresa['percentual_novo'],
                    'created_at': datetime.now()
                })

            # 4. Atualizar TB014 se necessário (incremento)
            if self.incremento_meta != Decimal('1.00'):
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