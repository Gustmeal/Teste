# utils/siscalculo_calc.py
from app import db
from app.models.siscalculo import (
    SiscalculoDados,
    SiscalculoCalculos,
    IndicadorEconomico
)
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from decimal import Decimal
import calendar


class CalculadorSiscalculo:
    """Classe responsável pelos cálculos do SISCalculo"""

    # Constantes do sistema
    TAXA_JUROS_MENSAL = Decimal('0.01')  # 1% ao mês (JUROS SIMPLES)
    MULTA_ANTIGA = Decimal('10.00')  # 10% antes de 10/01/2003
    MULTA_NOVA = Decimal('2.00')  # 2% após 11/01/2003
    DATA_MUDANCA_MULTA = date(2003, 1, 10)
    PERCENTUAL_HONORARIOS = Decimal('10.00')  # 10%

    def __init__(self, dt_atualizacao, id_indice, usuario):
        self.dt_atualizacao = dt_atualizacao
        self.id_indice = id_indice
        self.usuario = usuario
        self.dados_processados = []

    def executar_calculos(self):
        """Executa os cálculos para todos os dados importados"""
        try:
            # Buscar dados importados
            dados = SiscalculoDados.query.filter_by(
                DT_ATUALIZACAO=self.dt_atualizacao
            ).order_by(SiscalculoDados.DT_VENCIMENTO).all()

            if not dados:
                return {
                    'sucesso': False,
                    'erro': 'Nenhum dado encontrado para processar'
                }

            # Limpar cálculos anteriores para esta data e índice
            SiscalculoCalculos.query.filter_by(
                DT_ATUALIZACAO=self.dt_atualizacao,
                ID_INDICE_ECONOMICO=self.id_indice
            ).delete()

            total_processado = Decimal('0')
            registros_calculados = 0

            # Processar cada cota
            for dado in dados:
                resultado = self.calcular_cota(dado)
                if resultado:
                    # Criar registro de cálculo
                    novo_calculo = SiscalculoCalculos(
                        IMOVEL=dado.IMOVEL,
                        DT_VENCIMENTO=dado.DT_VENCIMENTO,
                        VR_COTA=dado.VR_COTA,
                        DT_ATUALIZACAO=self.dt_atualizacao,
                        TEMPO_ATRASO=resultado['meses_atraso'],
                        PERC_ATUALIZACAO=resultado['percentual_atualizacao'],
                        ATM=resultado['valor_atualizacao'],
                        VR_JUROS=resultado['valor_juros'],
                        VR_MULTA=resultado['valor_multa'],
                        VR_DESCONTO=Decimal('0'),
                        VR_TOTAL=resultado['valor_total'],
                        ID_INDICE_ECONOMICO=self.id_indice,
                        USUARIO_CALCULO=self.usuario
                    )
                    db.session.add(novo_calculo)

                    total_processado += resultado['valor_total']
                    registros_calculados += 1
                    self.dados_processados.append(resultado)

            db.session.commit()

            return {
                'sucesso': True,
                'registros_calculados': registros_calculados,
                'valor_total': float(total_processado),
                'dados': self.dados_processados
            }

        except Exception as e:
            db.session.rollback()
            return {
                'sucesso': False,
                'erro': str(e)
            }

    def calcular_cota(self, dado):
        """Calcula uma cota individual"""
        try:
            # Calcular meses de atraso
            meses_atraso = self.calcular_meses_atraso(
                dado.DT_VENCIMENTO,
                self.dt_atualizacao
            )

            if meses_atraso <= 0:
                # Não há atraso, retornar valor original
                return {
                    'meses_atraso': 0,
                    'percentual_atualizacao': Decimal('0'),
                    'valor_atualizacao': Decimal('0'),
                    'valor_juros': Decimal('0'),
                    'valor_multa': Decimal('0'),
                    'valor_total': dado.VR_COTA
                }

            # Determinar percentual de multa baseado na data
            percentual_multa = self.obter_percentual_multa(dado.DT_VENCIMENTO)

            # Buscar fator de atualização do índice
            fator_acumulado = self.buscar_fator_indice(
                dado.DT_VENCIMENTO,
                self.dt_atualizacao
            )

            # Calcular valor atualizado
            valor_atualizado = dado.VR_COTA * (Decimal('1') + fator_acumulado)
            valor_atualizacao = valor_atualizado - dado.VR_COTA

            # Calcular juros simples
            valor_juros = valor_atualizado * (self.TAXA_JUROS_MENSAL * Decimal(str(meses_atraso)))

            # Calcular multa
            valor_multa = valor_atualizado * (percentual_multa / Decimal('100'))

            # Total
            valor_total = valor_atualizado + valor_juros + valor_multa

            return {
                'meses_atraso': meses_atraso,
                'percentual_atualizacao': fator_acumulado,
                'valor_atualizacao': valor_atualizacao,
                'valor_juros': valor_juros,
                'valor_multa': valor_multa,
                'valor_total': valor_total
            }

        except Exception as e:
            print(f"Erro ao calcular cota: {e}")
            return None

    def calcular_meses_atraso(self, dt_vencimento, dt_atualizacao):
        """Calcula o número de meses de atraso"""
        if dt_vencimento >= dt_atualizacao:
            return 0

        # Calcular diferença em meses
        delta = relativedelta(dt_atualizacao, dt_vencimento)
        meses = delta.years * 12 + delta.months

        # Se houver dias, considerar mês adicional
        if delta.days > 0:
            meses += 1

        return meses

    def obter_percentual_multa(self, dt_vencimento):
        """Retorna o percentual de multa baseado na data"""
        if dt_vencimento <= self.DATA_MUDANCA_MULTA:
            return self.MULTA_ANTIGA
        else:
            return self.MULTA_NOVA

    def buscar_fator_indice(self, dt_inicio, dt_fim):
        """Busca o fator acumulado do índice econômico"""
        try:
            # Formatar datas para o formato esperado (YYYYMMDD)
            dt_inicio_str = dt_inicio.strftime('%Y%m%d')
            dt_fim_str = dt_fim.strftime('%Y%m%d')

            # Query SQL para calcular fator acumulado
            from sqlalchemy import text

            sql = text("""
                WITH IndicesPeriodo AS (
                    SELECT 
                        chDTInicio,
                        numIndicadorEconomico,
                        CAST((1 + (numIndicadorEconomico / 100.0)) AS DECIMAL(18,8)) AS FatorMensal,
                        ROW_NUMBER() OVER (ORDER BY chDTInicio) AS NumMes
                    FROM DBPRDINDICADORECONOMICO.dbo.tblIndicadorEconomico
                    WHERE idTipoIndicadorEconomico = :id_tipo
                    AND chDTInicio >= :dt_inicio
                    AND chDTInicio <= :dt_fim
                    AND (:id_tipo != 2 OR (RIGHT(chDTInicio, 2) = '01' AND RIGHT(chDTFinal, 2) = '01'))
                ),
                CalculoAcumulado AS (
                    SELECT 
                        NumMes, 
                        FatorMensal,
                        FatorMensal AS FatorAcumulado
                    FROM IndicesPeriodo
                    WHERE NumMes = 1

                    UNION ALL

                    SELECT 
                        i.NumMes,
                        i.FatorMensal,
                        CAST(c.FatorAcumulado * i.FatorMensal AS DECIMAL(18,8)) AS FatorAcumulado
                    FROM IndicesPeriodo i
                    INNER JOIN CalculoAcumulado c ON i.NumMes = c.NumMes + 1
                )
                SELECT ISNULL(MAX(FatorAcumulado) - 1, 0) AS FatorAcumulado
                FROM CalculoAcumulado
                OPTION (MAXRECURSION 0)
            """)

            resultado = db.session.execute(sql, {
                'id_tipo': self.id_indice,
                'dt_inicio': dt_inicio_str,
                'dt_fim': dt_fim_str
            }).scalar()

            return Decimal(str(resultado)) if resultado else Decimal('0')

        except Exception as e:
            print(f"Erro ao buscar fator do índice: {e}")
            # Em caso de erro, usar uma aproximação simples
            return self.calcular_fator_aproximado(dt_inicio, dt_fim)

    def calcular_fator_aproximado(self, dt_inicio, dt_fim):
        """Calcula fator aproximado quando não há dados do índice"""
        # Mapear taxas aproximadas por tipo de índice
        taxas_mensais = {
            2: Decimal('0.0005'),  # TR - aproximadamente 0.05% ao mês
            5: Decimal('0.005'),  # INPC - aproximadamente 0.5% ao mês
            7: Decimal('0.006'),  # IGPM - aproximadamente 0.6% ao mês
            9: Decimal('0.004')  # IPCA - aproximadamente 0.4% ao mês
        }

        taxa = taxas_mensais.get(self.id_indice, Decimal('0.005'))
        meses = self.calcular_meses_atraso(dt_inicio, dt_fim)

        # Calcular fator composto
        fator = (Decimal('1') + taxa) ** meses - Decimal('1')
        return fator

    def gerar_resumo(self):
        """Gera resumo dos cálculos realizados"""
        if not self.dados_processados:
            return None

        total_cotas = sum(d.get('valor_original', 0) for d in self.dados_processados)
        total_atualizacao = sum(d['valor_atualizacao'] for d in self.dados_processados)
        total_juros = sum(d['valor_juros'] for d in self.dados_processados)
        total_multa = sum(d['valor_multa'] for d in self.dados_processados)
        total_geral = sum(d['valor_total'] for d in self.dados_processados)

        honorarios = total_geral * (self.PERCENTUAL_HONORARIOS / Decimal('100'))
        total_final = total_geral + honorarios

        return {
            'total_cotas': float(total_cotas),
            'total_atualizacao': float(total_atualizacao),
            'total_juros': float(total_juros),
            'total_multa': float(total_multa),
            'subtotal': float(total_geral),
            'honorarios': float(honorarios),
            'total_final': float(total_final),
            'quantidade_cotas': len(self.dados_processados)
        }


class ComparadorIndices:
    """Classe para comparar resultados entre diferentes índices"""

    def __init__(self, dt_atualizacao):
        self.dt_atualizacao = dt_atualizacao
        self.indices_permitidos = [2, 5, 7, 9]  # TR, INPC, IGPM, IPCA

    def executar_comparacao(self):
        """Executa cálculos para todos os índices e compara"""
        resultados = {}

        # Buscar dados base
        dados = SiscalculoDados.query.filter_by(
            DT_ATUALIZACAO=self.dt_atualizacao
        ).all()

        if not dados:
            return None

        # Calcular para cada índice
        for id_indice in self.indices_permitidos:
            calculador = CalculadorSiscalculo(
                self.dt_atualizacao,
                id_indice,
                'SISTEMA'
            )

            resultado = calculador.executar_calculos()
            if resultado['sucesso']:
                resumo = calculador.gerar_resumo()
                resultados[id_indice] = resumo

        # Identificar melhor e pior
        if resultados:
            melhor_indice = min(resultados.keys(),
                                key=lambda k: resultados[k]['total_final'])
            pior_indice = max(resultados.keys(),
                              key=lambda k: resultados[k]['total_final'])

            return {
                'resultados': resultados,
                'melhor_indice': melhor_indice,
                'pior_indice': pior_indice,
                'diferenca': resultados[pior_indice]['total_final'] -
                             resultados[melhor_indice]['total_final']
            }

        return None