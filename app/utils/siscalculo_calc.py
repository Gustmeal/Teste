# utils/siscalculo_calc.py
from app import db
from app.models.siscalculo import (
    SiscalculoDados,
    SiscalculoCalculos
)
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from decimal import Decimal
from sqlalchemy import text


def truncar(valor, casas=4):
    """
    Trunca um valor Decimal em N casas decimais (não arredonda)
    Exemplo: truncar(Decimal('1.23456'), 4) = Decimal('1.2345')
    """
    if valor is None:
        return Decimal('0')

    multiplicador = Decimal(10 ** casas)
    return Decimal(int(valor * multiplicador)) / multiplicador


def arredondar(valor, casas=2):
    """
    Arredonda um valor Decimal em N casas decimais
    Usa ROUND_HALF_UP (igual ao Excel)

    Exemplos:
    - arredondar(1.235, 2) = 1.24 (0.5 arredonda para cima)
    - arredondar(1.234, 2) = 1.23
    - arredondar(-19.625, 2) = -19.63
    """
    from decimal import ROUND_HALF_UP

    if valor is None:
        return Decimal('0')

    # Criar quantizador com o número de casas decimais
    if casas == 2:
        quantizador = Decimal('0.01')
    elif casas == 4:
        quantizador = Decimal('0.0001')
    elif casas == 8:
        quantizador = Decimal('0.00000001')
    else:
        quantizador = Decimal(10) ** (-casas)

    return valor.quantize(quantizador, rounding=ROUND_HALF_UP)


class CalculadorSiscalculo:
    """Classe responsável pelos cálculos do SISCalculo"""

    # Constantes do sistema
    TAXA_JUROS_MENSAL = Decimal('0.01')  # 1% ao mês (JUROS SIMPLES)
    MULTA_ANTIGA = Decimal('10.00')  # 10% antes de 10/01/2003
    MULTA_NOVA = Decimal('2.00')  # 2% após 11/01/2003
    DATA_MUDANCA_MULTA = date(2003, 1, 10)

    def __init__(self, dt_atualizacao, id_indice, usuario, perc_honorarios=Decimal('10.00'), imovel=None):
        self.dt_atualizacao = dt_atualizacao
        self.id_indice = id_indice
        self.usuario = usuario
        self.perc_honorarios = perc_honorarios  # NOVO: Percentual de honorários personalizável
        self.imovel = imovel
        self.dados_processados = []

    def executar_calculos(self):
        """Executa os cálculos para todas as parcelas importadas"""
        try:
            print("=" * 80)
            print("INICIANDO PROCESSAMENTO SISCALCULO")
            print(f"Data de Atualização: {self.dt_atualizacao}")
            print(f"ID Índice: {self.id_indice}")
            print(f"Usuário: {self.usuario}")
            print("=" * 80)

            # Buscar dados importados (cada registro = uma parcela)
            print("\n[1] Buscando parcelas importadas...")
            query = SiscalculoDados.query.filter_by(
                DT_ATUALIZACAO=self.dt_atualizacao
            )

            # ✅ NOVO: Filtrar por imóvel se informado
            if self.imovel:
                query = query.filter_by(IMOVEL=self.imovel)
                print(f"[1] Filtrando por imóvel: {self.imovel}")

            dados = query.order_by(
                SiscalculoDados.DT_VENCIMENTO,  # ✅ CORRETO - data primeiro
                SiscalculoDados.ID_TIPO  # Depois tipo
            ).all()

            print(f"[1] Total de parcelas encontradas: {len(dados)}")

            if not dados:
                print("[ERRO] Nenhum dado encontrado para processar!")
                return {
                    'sucesso': False,
                    'erro': 'Nenhum dado encontrado para processar'
                }

            imovel = dados[0].IMOVEL if dados else 'N/A'
            print(f"[1] Imóvel: {imovel}")

            # --- INÍCIO DA CORREÇÃO ---
            # A limpeza deve ser específica para a data de atualização, imóvel E índice.
            # Isso impede que cálculos de outras datas ou para outros índices do mesmo
            # imóvel sejam afetados ou se misturem nos resultados.
            print(
                f"\n[2] Limpando cálculos anteriores para DT: {self.dt_atualizacao}, Imóvel: {imovel}, Índice: {self.id_indice}...")
            registros_deletados = SiscalculoCalculos.query.filter_by(
                DT_ATUALIZACAO=self.dt_atualizacao,
                IMOVEL=imovel,
                ID_INDICE_ECONOMICO=self.id_indice
            ).delete()
            print(f"[2] Registros deletados: {registros_deletados}")
            # --- FIM DA CORREÇÃO ---

            db.session.commit()

            total_processado = Decimal('0')
            registros_calculados = 0

            print(f"\n[3] Processando {len(dados)} parcelas...")

            for idx, dado in enumerate(dados, 1):
                try:
                    print(f"\n[3.{idx}] Processando parcela {idx}/{len(dados)}: Vencimento {dado.DT_VENCIMENTO}")
                    resultado_parcela = self.calcular_parcela_completa(dado)
                    if not resultado_parcela:
                        print(f"  [AVISO] Erro ao calcular parcela")
                        continue

                    novo_calculo = SiscalculoCalculos(
                        IMOVEL=dado.IMOVEL or '',
                        DT_VENCIMENTO=dado.DT_VENCIMENTO,
                        VR_COTA=dado.VR_COTA,
                        DT_ATUALIZACAO=self.dt_atualizacao,
                        ID_INDICE_ECONOMICO=self.id_indice,
                        TEMPO_ATRASO=resultado_parcela['meses_atraso_total'],
                        PERC_ATUALIZACAO=resultado_parcela['percentual_total'],
                        ATM=resultado_parcela['atm'],
                        VR_JUROS=resultado_parcela['total_juros'],
                        VR_MULTA=resultado_parcela['total_multa'],
                        VR_DESCONTO=resultado_parcela['total_desconto'],
                        VR_TOTAL=resultado_parcela['valor_total'],
                        PERC_HONORARIOS=self.perc_honorarios,
                        ID_TIPO=dado.ID_TIPO
                    )
                    db.session.add(novo_calculo)
                    total_processado += resultado_parcela['valor_total']
                    registros_calculados += 1
                    print(f"  [RESULTADO] Total da parcela: R$ {resultado_parcela['valor_total']}")

                except Exception as e:
                    print(f"  [ERRO] Erro ao processar parcela {idx}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    continue

            print(f"\n[4] Realizando commit de {registros_calculados} registros...")
            try:
                db.session.commit()
                print("[4] Commit realizado com sucesso!")
            except Exception as e:
                print(f"[ERRO] Falha no commit: {str(e)}")
                db.session.rollback()
                return {'sucesso': False, 'erro': f'Erro ao salvar cálculos: {str(e)}'}

            print("\n" + "=" * 80)
            print("PROCESSAMENTO CONCLUÍDO")
            print(f"Total de parcelas calculadas: {registros_calculados}")
            print(f"Valor total processado: R$ {total_processado:,.4f}")
            print("=" * 80)

            return {
                'sucesso': True,
                'registros_processados': registros_calculados,
                'valor_total': float(total_processado),
                'erros': 0
            }

        except Exception as e:
            print("\n" + "=" * 80)
            print("[ERRO CRÍTICO]:")
            print(f"Erro: {str(e)}")
            import traceback
            traceback.print_exc()
            print("=" * 80)
            db.session.rollback()
            return {'sucesso': False, 'erro': f'Erro crítico: {str(e)}'}

    def calcular_parcela_completa(self, dado):
        """
        Calcula UMA parcela com TODAS as etapas de arredondamento

        CORREÇÕES:
        1. Se meses de atraso < 0 (futuro), zera todos os encargos
        2. Se meses de atraso == 0 (mês da atualização), zera todos os encargos
        3. Cálculo correto de meses considerando apenas ano e mês (ignora dia)
        """
        try:
            print(f"\n=== CALCULANDO PARCELA ===")
            print(f"Imóvel: {dado.IMOVEL}")
            print(f"Vencimento: {dado.DT_VENCIMENTO}")
            print(f"Atualização: {self.dt_atualizacao}")

            # 1. ARREDONDAR VR_COTA (2 casas decimais)
            vr_cota_arredondado = arredondar(dado.VR_COTA, 2)
            print(f"    VR_COTA arredondado: R$ {vr_cota_arredondado}")

            # 2. MESES DE ATRASO - Calcular diferença em meses
            delta = relativedelta(self.dt_atualizacao, dado.DT_VENCIMENTO)
            meses_atraso = delta.years * 12 + delta.months

            # ✅ CORREÇÃO: Se meses == 0 mas já passou do vencimento, conta 1 mês
            # Exemplo: 02/01/2018 → 01/02/2018 = 30 dias = 1 mês de atraso
            if meses_atraso == 0 and self.dt_atualizacao > dado.DT_VENCIMENTO:
                meses_atraso = 1

            print(f"    Meses de atraso: {meses_atraso}")

            # ✅ CORREÇÃO: Se meses <= 0, zerar TUDO
            if meses_atraso <= 0:
                print(f"    [AVISO] Meses de atraso <= 0: Zerando todos os encargos")
                return {
                    'meses_atraso_total': meses_atraso,
                    'percentual_total': Decimal('0'),
                    'atm': Decimal('0'),
                    'total_juros': Decimal('0'),
                    'total_multa': Decimal('0'),
                    'total_desconto': Decimal('0'),
                    'valor_total': vr_cota_arredondado
                }

            # 3. Buscar índices do período e calcular FATOR ACUMULADO
            indices_periodo = self._obter_indices_periodo(dado.DT_VENCIMENTO, self.dt_atualizacao)

            # Calcular fator acumulado (juros compostos)
            fator_acumulado = Decimal('1.0')
            for indice in indices_periodo:
                fator_mes = Decimal('1.0') + (indice / Decimal('100'))
                fator_acumulado = fator_acumulado * fator_mes

            # Percentual = (Fator - 1)
            percentual_correcao = (fator_acumulado - Decimal('1.0'))

            print(f"    Total de índices aplicados: {len(indices_periodo)}")
            print(f"    Fator acumulado: {fator_acumulado}")
            print(f"    Percentual de correção: {(percentual_correcao * Decimal('100')):.4f}%")

            # 4. VR_Atual = VR_COTA × Fator_Acumulado
            vr_atual_calculado = vr_cota_arredondado * fator_acumulado
            vr_atual_arredondado = arredondar(vr_atual_calculado, 2)

            # 5. ATM = VR_Atual - VR_COTA
            atm = vr_atual_arredondado - vr_cota_arredondado
            atm_arredondado = arredondar(atm, 2)

            print(f"    Valor Atualizado: R$ {vr_atual_arredondado}")
            print(f"    ATM: R$ {atm_arredondado}")

            # 6. JUROS = VR_Atual × (Taxa × Meses)
            valor_juros_calculado = vr_atual_arredondado * self.TAXA_JUROS_MENSAL * Decimal(str(meses_atraso))
            juros_arredondado = arredondar(valor_juros_calculado, 4)

            # 7. MULTA = VR_Atual × (Percentual_Multa / 100)
            if dado.DT_VENCIMENTO <= self.DATA_MUDANCA_MULTA:
                taxa_multa = self.MULTA_ANTIGA
            else:
                taxa_multa = self.MULTA_NOVA

            valor_multa_calculado = vr_atual_arredondado * taxa_multa / Decimal('100')
            multa_arredondada = arredondar(valor_multa_calculado, 2)

            # 8. Desconto
            valor_desconto = Decimal('0')

            # 9. SOMA TOTAL
            soma = vr_atual_arredondado + juros_arredondado + multa_arredondada - valor_desconto
            soma_final = arredondar(soma, 2)

            print(f"    Juros: R$ {juros_arredondado}")
            print(f"    Multa: R$ {multa_arredondada}")
            print(f"    SOMA: R$ {soma_final}")

            return {
                'meses_atraso_total': meses_atraso,
                'percentual_total': percentual_correcao,
                'atm': atm_arredondado,
                'total_juros': juros_arredondado,
                'total_multa': multa_arredondada,
                'total_desconto': valor_desconto,
                'valor_total': soma_final
            }

        except Exception as e:
            print(f"    [ERRO] Erro em calcular_parcela_completa: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    def _obter_indices_periodo(self, dt_inicio, dt_fim):
        """
        Busca TODOS os índices do período (mês a mês)
        Retorna lista com os percentuais

        Exemplo de retorno: [0.25, 0.57, 0.42, ...]

        IMPORTANTE: INCLUI o índice do mês da data de atualização

        Exemplo: Se data de atualização é 01/02/2018, INCLUI o índice de fevereiro/2018
        """
        try:
            # Primeiro dia do mês de início
            dt_inicio_mes = date(dt_inicio.year, dt_inicio.month, 1)

            # ✅ CORREÇÃO DEFINITIVA: Incluir o mês da data de atualização
            # Se dt_fim = 01/02/2018, pega até 01/02/2018 (INCLUI fevereiro)
            dt_fim_mes_atual = date(dt_fim.year, dt_fim.month, 1)

            dt_inicio_str = dt_inicio_mes.strftime('%Y%m%d')
            dt_fim_str = dt_fim_mes_atual.strftime('%Y%m%d')

            print(f"    [DEBUG ÍNDICES] Período: {dt_inicio_str} até {dt_fim_str}")

            # Query simples já que IPCA (id=9) tem apenas 1 registro por mês
            sql = text("""
                SELECT numIndicadorEconomico
                FROM [DBPRDINDICADORECONOMICO].[dbo].[tblIndicadorEconomico]
                WHERE idTipoIndicadorEconomico = :id_tipo
                    AND chDTInicio >= :dt_inicio
                    AND chDTInicio <= :dt_fim
                ORDER BY chDTInicio
            """)

            resultado = db.session.execute(
                sql,
                {
                    'id_tipo': self.id_indice,
                    'dt_inicio': dt_inicio_str,
                    'dt_fim': dt_fim_str
                }
            )

            # Retorna lista de Decimals
            indices = [Decimal(str(row[0])) for row in resultado.fetchall()]

            if len(indices) > 0:
                print(f"    [DEBUG] Total de índices retornados: {len(indices)}")
                print(f"    [DEBUG] Primeiros 3 índices: {indices[:3]}")
                print(f"    [DEBUG] Últimos 3 índices: {indices[-3:]}")
            else:
                print(f"    [AVISO] Nenhum índice encontrado para o período!")

            return indices

        except Exception as e:
            print(f"    [ERRO] Erro ao buscar índices do período: {e}")
            import traceback
            traceback.print_exc()
            return []

    def gerar_resumo(self):
        """Gera resumo dos cálculos processados"""
        if not self.dados_processados:
            return {}

        total_cotas = sum(d['valor_cota'] for d in self.dados_processados)
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