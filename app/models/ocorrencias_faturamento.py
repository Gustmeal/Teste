from app import db
from sqlalchemy import Column, Integer, String, Date, DateTime, Numeric, text, func
from datetime import datetime
import base64


class OcorrenciasFaturamento(db.Model):
    """Model para análise de ocorrências de faturamento"""
    __tablename__ = 'MOV_TB039_SMART_OCORRENCIAS_ANALISAR'
    __table_args__ = {'schema': 'BDG'}

    # Colunas - Chave primária composta incluindo justificativa
    NR_CONTRATO = Column('NR_CONTRATO', Numeric(23, 0), nullable=False, primary_key=True)
    nrOcorrencia = Column('nrOcorrencia', Integer, nullable=False, primary_key=True)
    dsJustificativa = Column('dsJustificativa', String(4000), nullable=True, primary_key=True)
    ID_FATURAMENTO = Column('ID_FATURAMENTO', Integer, nullable=True)
    MES_ANO_FATURAMENTO = Column('MES_ANO_FATURAMENTO', Integer, nullable=True)
    DT_JUSTIFICATIVA = Column('DT_JUSTIFICATIVA', Date, nullable=True)
    STATUS = Column('STATUS', String(100), nullable=True)
    ITEM_SERVICO = Column('ITEM_SERVICO', String(100), nullable=True)
    OBS = Column('OBS', String(100), nullable=True)
    DSC_ESTADO = Column('DSC_ESTADO', String(100), nullable=True)
    RESPONSAVEL = Column('RESPONSAVEL', String(100), nullable=True)

    def __repr__(self):
        return f'<OcorrenciasFaturamento {self.NR_CONTRATO}-{self.nrOcorrencia}>'

    def gerar_identificador(self):
        """Gera um identificador único base64 para a justificativa"""
        texto = self.dsJustificativa or 'NULL'
        return base64.urlsafe_b64encode(texto.encode('utf-8')).decode('utf-8')

    def formatar_mes_ano(self):
        """
        Formata MES_ANO_FATURAMENTO de 202505 (AAAAMM) para 05/2025 (MM/AAAA)
        """
        if self.MES_ANO_FATURAMENTO and self.MES_ANO_FATURAMENTO > 0:
            mes_ano_str = str(self.MES_ANO_FATURAMENTO).zfill(6)  # Garante 6 dígitos
            ano = mes_ano_str[:4]  # Primeiros 4 dígitos = ano
            mes = mes_ano_str[4:]  # Últimos 2 dígitos = mês
            return f"{mes}/{ano}"
        return '-'

    @classmethod
    def listar_sem_analise(cls):
        """
        Lista ocorrências sem análise (ID_FATURAMENTO NULL)
        MANTIDO PARA COMPATIBILIDADE - MAS NÃO SERÁ MAIS USADO
        """
        sql = text("""
            SELECT NR_CONTRATO, nrOcorrencia, dsJustificativa, ID_FATURAMENTO, 
                   MES_ANO_FATURAMENTO, DT_JUSTIFICATIVA, STATUS, ITEM_SERVICO, OBS,
                   DSC_ESTADO, RESPONSAVEL
            FROM BDG.MOV_TB039_SMART_OCORRENCIAS_ANALISAR
            WHERE ID_FATURAMENTO IS NULL
            ORDER BY NR_CONTRATO, nrOcorrencia
        """)

        resultado = db.session.execute(sql).fetchall()

        # Converter para objetos
        ocorrencias = []
        for row in resultado:
            obj = cls()
            obj.NR_CONTRATO = row[0]
            obj.nrOcorrencia = row[1]
            obj.dsJustificativa = row[2]
            obj.ID_FATURAMENTO = row[3]
            obj.MES_ANO_FATURAMENTO = row[4]
            obj.DT_JUSTIFICATIVA = row[5]
            obj.STATUS = row[6]
            obj.ITEM_SERVICO = row[7]
            obj.OBS = row[8]
            obj.DSC_ESTADO = row[9]
            obj.RESPONSAVEL = row[10]
            ocorrencias.append(obj)

        return ocorrencias

    @classmethod
    def listar_por_status(cls):
        """
        Lista ocorrências agrupadas por STATUS
        REGRA ESPECIAL: Status 'Faturar no Mês - Prévia' e 'Não Faturar no Mês - Prévia'
        aparecem na aba do status MESMO após análise (ID_FATURAMENTO preenchido)

        Retorna um dicionário onde a chave é o STATUS e o valor é uma lista de ocorrências
        """
        sql = text("""
                SELECT NR_CONTRATO, nrOcorrencia, dsJustificativa, ID_FATURAMENTO, 
                       MES_ANO_FATURAMENTO, DT_JUSTIFICATIVA, STATUS, ITEM_SERVICO, OBS,
                       DSC_ESTADO, RESPONSAVEL
                FROM BDG.MOV_TB039_SMART_OCORRENCIAS_ANALISAR
                WHERE (
                    ID_FATURAMENTO IS NULL
                    OR STATUS IN ('Faturar no Mês - Prévia', 'Não Faturar no Mês - Prévia')
                )
                ORDER BY STATUS, NR_CONTRATO, nrOcorrencia
            """)

        resultado = db.session.execute(sql).fetchall()

        # Agrupar por STATUS
        ocorrencias_por_status = {}
        for row in resultado:
            obj = cls()
            obj.NR_CONTRATO = row[0]
            obj.nrOcorrencia = row[1]
            obj.dsJustificativa = row[2]
            obj.ID_FATURAMENTO = row[3]
            obj.MES_ANO_FATURAMENTO = row[4]
            obj.DT_JUSTIFICATIVA = row[5]
            obj.STATUS = row[6]
            obj.ITEM_SERVICO = row[7]
            obj.OBS = row[8]
            obj.DSC_ESTADO = row[9]
            obj.RESPONSAVEL = row[10]

            # Agrupar por STATUS (se STATUS for NULL, usa 'Sem Status')
            status_chave = obj.STATUS if obj.STATUS else 'Sem Status'

            if status_chave not in ocorrencias_por_status:
                ocorrencias_por_status[status_chave] = []

            ocorrencias_por_status[status_chave].append(obj)

        return ocorrencias_por_status

    @classmethod
    def listar_analisadas(cls):
        """
        Lista ocorrências já analisadas (ID_FATURAMENTO preenchido)
        MAS QUE AINDA NÃO FORAM SINCRONIZADAS COM A TABELA SMART_FATURAMENTO

        INCLUI também os status 'Faturar no Mês - Prévia' e 'Não Faturar no Mês - Prévia'
        """
        sql = text("""
                    SELECT A.NR_CONTRATO, A.nrOcorrencia, A.dsJustificativa, A.ID_FATURAMENTO, 
                           A.MES_ANO_FATURAMENTO, A.DT_JUSTIFICATIVA, A.STATUS, A.ITEM_SERVICO, A.OBS,
                           A.DSC_ESTADO, A.RESPONSAVEL
                    FROM BDG.MOV_TB039_SMART_OCORRENCIAS_ANALISAR A
                    WHERE A.ID_FATURAMENTO IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 
                          FROM BDDASHBOARDBI.[BDG].[MOV_TB034_SMART_FATURAMENTO] B
                          WHERE B.NR_CONTRATO = A.NR_CONTRATO
                            AND B.nrOcorrencia = A.nrOcorrencia
                            AND B.ID_FATURAMENTO = A.ID_FATURAMENTO
                            AND (
                                (B.MES_ANO_FATURAMENTO = A.MES_ANO_FATURAMENTO)
                                OR (B.MES_ANO_FATURAMENTO IS NULL AND A.MES_ANO_FATURAMENTO IS NULL)
                            )
                      )
                    ORDER BY A.NR_CONTRATO, A.nrOcorrencia
                """)

        resultado = db.session.execute(sql).fetchall()

        # Converter para objetos
        ocorrencias = []
        for row in resultado:
            obj = cls()
            obj.NR_CONTRATO = row[0]
            obj.nrOcorrencia = row[1]
            obj.dsJustificativa = row[2]
            obj.ID_FATURAMENTO = row[3]
            obj.MES_ANO_FATURAMENTO = row[4]
            obj.DT_JUSTIFICATIVA = row[5]
            obj.STATUS = row[6]
            obj.ITEM_SERVICO = row[7]
            obj.OBS = row[8]
            obj.DSC_ESTADO = row[9]
            obj.RESPONSAVEL = row[10]
            ocorrencias.append(obj)

        return ocorrencias

    @classmethod
    def listar_analisadas(cls):
        """
        Lista ocorrências já analisadas (ID_FATURAMENTO preenchido)
        MAS QUE AINDA NÃO FORAM SINCRONIZADAS COM A TABELA SMART_FATURAMENTO
        """
        sql = text("""
                SELECT A.NR_CONTRATO, A.nrOcorrencia, A.dsJustificativa, A.ID_FATURAMENTO, 
                       A.MES_ANO_FATURAMENTO, A.DT_JUSTIFICATIVA, A.STATUS, A.ITEM_SERVICO, A.OBS,
                       A.DSC_ESTADO, A.RESPONSAVEL
                FROM BDG.MOV_TB039_SMART_OCORRENCIAS_ANALISAR A
                WHERE A.ID_FATURAMENTO IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 
                      FROM BDDASHBOARDBI.[BDG].[MOV_TB034_SMART_FATURAMENTO] B
                      WHERE B.NR_CONTRATO = A.NR_CONTRATO
                        AND B.nrOcorrencia = A.nrOcorrencia
                        AND B.ID_FATURAMENTO = A.ID_FATURAMENTO
                        AND (
                            (B.MES_ANO_FATURAMENTO = A.MES_ANO_FATURAMENTO)
                            OR (B.MES_ANO_FATURAMENTO IS NULL AND A.MES_ANO_FATURAMENTO IS NULL)
                        )
                  )
                ORDER BY A.NR_CONTRATO, A.nrOcorrencia
            """)

        resultado = db.session.execute(sql).fetchall()

        # Converter para objetos
        ocorrencias = []
        for row in resultado:
            obj = cls()
            obj.NR_CONTRATO = row[0]
            obj.nrOcorrencia = row[1]
            obj.dsJustificativa = row[2]
            obj.ID_FATURAMENTO = row[3]
            obj.MES_ANO_FATURAMENTO = row[4]
            obj.DT_JUSTIFICATIVA = row[5]
            obj.STATUS = row[6]
            obj.ITEM_SERVICO = row[7]
            obj.OBS = row[8]
            obj.DSC_ESTADO = row[9]
            obj.RESPONSAVEL = row[10]
            ocorrencias.append(obj)

        return ocorrencias

    @classmethod
    def buscar_por_identificador(cls, nr_contrato, nr_ocorrencia, identificador_b64):
        """Busca uma ocorrência específica pelo identificador base64"""
        try:
            # Decodificar o identificador
            justificativa_decodificada = base64.urlsafe_b64decode(identificador_b64.encode('utf-8')).decode('utf-8')

            # Se for 'NULL', buscar onde justificativa é NULL
            if justificativa_decodificada == 'NULL':
                return cls.query.filter_by(
                    NR_CONTRATO=nr_contrato,
                    nrOcorrencia=nr_ocorrencia,
                    dsJustificativa=None
                ).first()
            else:
                return cls.query.filter_by(
                    NR_CONTRATO=nr_contrato,
                    nrOcorrencia=nr_ocorrencia,
                    dsJustificativa=justificativa_decodificada
                ).first()
        except:
            return None

    @staticmethod
    def atualizar_faturamento_smart():
        """
        Executa o UPDATE para sincronizar os dados analisados com a tabela de faturamento
        Executa em 2 etapas: primeiro ID_FATURAMENTO = 1, depois ID_FATURAMENTO = 0
        """
        try:
            # ETAPA 1: Atualizar registros onde ID_FATURAMENTO = 1 (HOUVE FATURAMENTO)
            sql_etapa1 = text("""
                    UPDATE BDDASHBOARDBI.[BDG].[MOV_TB034_SMART_FATURAMENTO]
                    SET [ID_FATURAMENTO] = B.[ID_FATURAMENTO],
                        [MES_ANO_FATURAMENTO] = B.[MES_ANO_FATURAMENTO],
                        OBS = 'ANÁLISE SUMOV'
                    FROM BDDASHBOARDBI.[BDG].[MOV_TB034_SMART_FATURAMENTO] A
                    INNER JOIN BDDASHBOARDBI.[BDG].[MOV_TB039_SMART_OCORRENCIAS_ANALISAR] B
                        ON A.NR_CONTRATO = B.[NR_CONTRATO]
                        AND A.[nrOcorrencia] = B.[nrOcorrencia]
                    WHERE B.ID_FATURAMENTO = 1
                      AND A.ID_FATURAMENTO IS NULL
                """)

            result_etapa1 = db.session.execute(sql_etapa1)
            db.session.commit()
            linhas_etapa1 = result_etapa1.rowcount

            print(f"DEBUG ETAPA 1: {linhas_etapa1} registros atualizados (ID_FATURAMENTO = 1)")

            # ETAPA 2: Atualizar registros onde ID_FATURAMENTO = 0 (NÃO HOUVE FATURAMENTO)
            sql_etapa2 = text("""
                    UPDATE BDDASHBOARDBI.[BDG].[MOV_TB034_SMART_FATURAMENTO]
                    SET [ID_FATURAMENTO] = B.[ID_FATURAMENTO],
                        [MES_ANO_FATURAMENTO] = B.[MES_ANO_FATURAMENTO],
                        OBS = 'ANÁLISE SUMOV'
                    FROM BDDASHBOARDBI.[BDG].[MOV_TB034_SMART_FATURAMENTO] A
                    INNER JOIN BDDASHBOARDBI.[BDG].[MOV_TB039_SMART_OCORRENCIAS_ANALISAR] B
                        ON A.NR_CONTRATO = B.[NR_CONTRATO]
                        AND A.[nrOcorrencia] = B.[nrOcorrencia]
                    WHERE B.ID_FATURAMENTO = 0
                      AND A.ID_FATURAMENTO IS NULL
                """)

            result_etapa2 = db.session.execute(sql_etapa2)
            db.session.commit()
            linhas_etapa2 = result_etapa2.rowcount

            print(f"DEBUG ETAPA 2: {linhas_etapa2} registros atualizados (ID_FATURAMENTO = 0)")

            # Retornar total de linhas atualizadas
            total_linhas = linhas_etapa1 + linhas_etapa2
            print(f"DEBUG TOTAL: {total_linhas} registros atualizados no total")

            return total_linhas

        except Exception as e:
            db.session.rollback()
            print(f"DEBUG ERRO ao atualizar faturamento SMART: {str(e)}")
            import traceback
            traceback.print_exc()
            raise