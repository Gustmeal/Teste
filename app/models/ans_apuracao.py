"""
Model: app/models/ans_apuracao.py
Módulo ANS Glosas - Apuração de ocorrências ANS
"""

from app import db
from sqlalchemy import Column, Integer, String, Date, DateTime, text
from datetime import datetime, date


class AnsApuracao(db.Model):
    __tablename__ = 'MOV_TB045_ANS_APURACAO'
    __table_args__ = {'schema': 'BDG'}

    DT_APURACAO = Column(Date, primary_key=True, nullable=False)
    nrOcorrencia = Column(Integer, primary_key=True, nullable=False)
    GRUPO = Column('GRUPO', db.SmallInteger, nullable=False)
    DT_ABERTURA = Column(Date, nullable=True)
    QTDE_DIAS = Column(Integer, nullable=True)
    DT_EFETIVACAO = Column(Date, nullable=True)
    DT_ANDAMENTO = Column(Date, nullable=True)
    DT_JUSTIFICATIVA = Column(Date, nullable=True)
    DT_CANCELADO = Column(Date, nullable=True)
    DT_DEFERIDO = Column(Date, nullable=True)
    NO_PRAZO = Column(Integer, nullable=True)
    ADVERTENCIA = Column(Integer, nullable=True)
    DT_ADVERTENCIA = Column(Integer, nullable=True)
    REINCIDENCIA = Column(Integer, nullable=True)
    DT_REINCIDENCIA = Column(Integer, nullable=True)
    REITERACAO = Column(Integer, nullable=True)
    DT_REITERACAO = Column(Integer, nullable=True)
    JUST_ACEITA = Column(Integer, nullable=True)
    DSC_JUSTIFICATIVA = Column(String(4000), nullable=True)

    def __repr__(self):
        return f'<AnsApuracao {self.DT_APURACAO}-{self.nrOcorrencia}>'

    # ==================================================================
    # DATAS DE APURAÇÃO DISPONÍVEIS
    # ==================================================================

    @staticmethod
    def obter_datas_apuracao():
        """Retorna todas as datas de apuração distintas, ordenadas desc"""
        sql = text("""
            SELECT DISTINCT DT_APURACAO
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO
            ORDER BY DT_APURACAO DESC
        """)
        resultado = db.session.execute(sql).fetchall()
        return [row.DT_APURACAO for row in resultado]

    @staticmethod
    def obter_data_anterior(dt_apuracao):
        """
        Retorna a DT_APURACAO imediatamente anterior à informada.
        Usada para verificar advertência/reincidência do período anterior.
        """
        sql = text("""
            SELECT TOP 1 DT_APURACAO
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO
            WHERE DT_APURACAO < :dt_apuracao
            ORDER BY DT_APURACAO DESC
        """)
        row = db.session.execute(sql, {'dt_apuracao': dt_apuracao}).fetchone()
        return row.DT_APURACAO if row else None

    # ==================================================================
    # QUERIES BASE
    # ==================================================================

    @staticmethod
    def _query_base(dt_apuracao, filtro_just_aceita):
        if filtro_just_aceita == 'NULL':
            where_just = "AND A.JUST_ACEITA IS NULL"
        else:
            where_just = "AND A.JUST_ACEITA IS NOT NULL"

        sql = text(f"""
            SELECT 
                A.DT_APURACAO, A.nrOcorrencia, A.GRUPO,
                A.DT_ABERTURA, A.DT_ANDAMENTO, A.DT_JUSTIFICATIVA,
                A.DSC_JUSTIFICATIVA, A.JUST_ACEITA, A.QTDE_DIAS, A.NO_PRAZO,
                A.ADVERTENCIA, A.DT_ADVERTENCIA,
                A.REINCIDENCIA, A.DT_REINCIDENCIA,
                A.REITERACAO, A.DT_REITERACAO,
                B.itemServico, B.PRAZO
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO A
            OUTER APPLY (
                SELECT TOP 1 X.itemServico, X.PRAZO
                FROM BDDASHBOARDBI.BDG.MOV_TB043_ANS_ITENS_FATURAMENTO X
                WHERE X.GRUPO = A.GRUPO
            ) B
            WHERE A.DT_APURACAO = :dt_apuracao
              {where_just}
            ORDER BY A.GRUPO, A.nrOcorrencia
        """)
        return db.session.execute(sql, {'dt_apuracao': dt_apuracao}).fetchall()

    @staticmethod
    def _agrupar_por_grupo(resultados):
        agrupado = {}
        for row in resultados:
            grupo = row.GRUPO
            if grupo not in agrupado:
                agrupado[grupo] = []
            agrupado[grupo].append(row)
        return agrupado

    @staticmethod
    def listar_por_grupo(dt_apuracao):
        resultados = AnsApuracao._query_base(dt_apuracao, 'NULL')
        return AnsApuracao._agrupar_por_grupo(resultados)

    @staticmethod
    def listar_analisadas_por_grupo(dt_apuracao):
        resultados = AnsApuracao._query_base(dt_apuracao, 'NOT NULL')
        return AnsApuracao._agrupar_por_grupo(resultados)

    @staticmethod
    def listar_analisadas(dt_apuracao):
        return AnsApuracao._query_base(dt_apuracao, 'NOT NULL')

    @staticmethod
    def buscar_ocorrencia(dt_apuracao, nr_ocorrencia):
        sql = text("""
            SELECT 
                A.DT_APURACAO, A.nrOcorrencia, A.GRUPO,
                A.DT_ABERTURA, A.DT_ANDAMENTO, A.DT_JUSTIFICATIVA,
                A.DSC_JUSTIFICATIVA, A.JUST_ACEITA, A.QTDE_DIAS, A.NO_PRAZO,
                A.ADVERTENCIA, B.itemServico, B.PRAZO
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO A
            OUTER APPLY (
                SELECT TOP 1 X.itemServico, X.PRAZO
                FROM BDDASHBOARDBI.BDG.MOV_TB043_ANS_ITENS_FATURAMENTO X
                WHERE X.GRUPO = A.GRUPO
            ) B
            WHERE A.DT_APURACAO = :dt_apuracao AND A.nrOcorrencia = :nr_ocorrencia
        """)
        return db.session.execute(sql, {
            'dt_apuracao': dt_apuracao, 'nr_ocorrencia': nr_ocorrencia
        }).fetchone()

    # ==================================================================
    # SALVAR ANÁLISE
    # ==================================================================

    @staticmethod
    def salvar_analise(dt_apuracao, nr_ocorrencia, just_aceita):
        ocorrencia = AnsApuracao.buscar_ocorrencia(dt_apuracao, nr_ocorrencia)
        if not ocorrencia:
            return False, 'Ocorrência não encontrada.'

        dt_abertura = ocorrencia.DT_ABERTURA
        if not dt_abertura:
            return False, 'Data de abertura não encontrada.'

        if just_aceita == 1:
            dt_fim = ocorrencia.DT_JUSTIFICATIVA
            if not dt_fim:
                return False, 'Data de justificativa não encontrada.'
        else:
            dt_fim = ocorrencia.DT_APURACAO
            if not dt_fim:
                return False, 'Data de apuração não encontrada.'

        if isinstance(dt_abertura, datetime):
            dt_abertura = dt_abertura.date()
        if isinstance(dt_fim, datetime):
            dt_fim = dt_fim.date()

        qtde_dias = (dt_fim - dt_abertura).days + 1
        prazo = ocorrencia.PRAZO
        no_prazo = (1 if qtde_dias <= prazo else 0) if prazo is not None else None

        sql_update = text("""
            UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO
            SET JUST_ACEITA = :just_aceita, QTDE_DIAS = :qtde_dias, NO_PRAZO = :no_prazo
            WHERE DT_APURACAO = :dt_apuracao AND nrOcorrencia = :nr_ocorrencia
        """)
        db.session.execute(sql_update, {
            'just_aceita': just_aceita, 'qtde_dias': qtde_dias, 'no_prazo': no_prazo,
            'dt_apuracao': dt_apuracao, 'nr_ocorrencia': nr_ocorrencia
        })
        db.session.commit()
        return True, {'qtde_dias': qtde_dias, 'no_prazo': no_prazo, 'prazo': prazo}

    @staticmethod
    def salvar_analise_lote(dt_apuracao, analises):
        sucesso_count = 0
        erro_count = 0
        erros = []
        for item in analises:
            nr_ocorrencia = item.get('nr_ocorrencia')
            just_aceita = item.get('just_aceita')
            if nr_ocorrencia is None or just_aceita is None:
                erro_count += 1
                erros.append(f'Dados incompletos para ocorrência {nr_ocorrencia}')
                continue
            sucesso, resultado = AnsApuracao.salvar_analise(dt_apuracao, nr_ocorrencia, just_aceita)
            if sucesso:
                sucesso_count += 1
            else:
                erro_count += 1
                erros.append(f'Ocorrência {nr_ocorrencia}: {resultado}')
        return sucesso_count, erro_count, erros

    # ==================================================================
    # ESTATÍSTICAS E GRUPOS
    # ==================================================================

    @staticmethod
    def obter_estatisticas(dt_apuracao):
        sql = text("""
            SELECT 
                COUNT(*) AS total,
                SUM(CASE WHEN JUST_ACEITA IS NOT NULL THEN 1 ELSE 0 END) AS analisadas,
                SUM(CASE WHEN JUST_ACEITA IS NULL THEN 1 ELSE 0 END) AS pendentes,
                SUM(CASE WHEN JUST_ACEITA = 1 THEN 1 ELSE 0 END) AS aceitas,
                SUM(CASE WHEN JUST_ACEITA = 0 THEN 1 ELSE 0 END) AS rejeitadas,
                SUM(CASE WHEN NO_PRAZO = 1 THEN 1 ELSE 0 END) AS dentro_prazo,
                SUM(CASE WHEN NO_PRAZO = 0 THEN 1 ELSE 0 END) AS fora_prazo
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO
            WHERE DT_APURACAO = :dt_apuracao
        """)
        return db.session.execute(sql, {'dt_apuracao': dt_apuracao}).fetchone()

    @staticmethod
    def obter_todos_grupos(dt_apuracao):
        sql = text("""
            SELECT DISTINCT A.GRUPO, B.itemServico, B.PRAZO
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO A
            OUTER APPLY (
                SELECT TOP 1 X.itemServico, X.PRAZO
                FROM BDDASHBOARDBI.BDG.MOV_TB043_ANS_ITENS_FATURAMENTO X
                WHERE X.GRUPO = A.GRUPO
            ) B
            WHERE A.DT_APURACAO = :dt_apuracao
            ORDER BY A.GRUPO
        """)
        return db.session.execute(sql, {'dt_apuracao': dt_apuracao}).fetchall()

    # ==================================================================
    # ADVERTÊNCIA
    # ==================================================================

    @staticmethod
    def calcular_resumo_advertencia(dt_apuracao, grupo):
        """
        Resumo completo do grupo incluindo estados de advertência,
        reincidência e reiteração para controle dos botões.
        """
        sql = text("""
            SELECT 
                COUNT(*) AS total,
                SUM(CASE WHEN NO_PRAZO = 0 AND JUST_ACEITA = 0 THEN 1 ELSE 0 END) AS fora_prazo_sem_just,
                SUM(CASE WHEN NO_PRAZO = 0 THEN 1 ELSE 0 END) AS total_fora_prazo,
                SUM(CASE WHEN NO_PRAZO = 1 THEN 1 ELSE 0 END) AS total_dentro_prazo,
                SUM(CASE WHEN JUST_ACEITA = 1 THEN 1 ELSE 0 END) AS total_just_aceitas,
                SUM(CASE WHEN JUST_ACEITA = 0 THEN 1 ELSE 0 END) AS total_just_rejeitadas,
                SUM(CASE WHEN JUST_ACEITA IS NULL THEN 1 ELSE 0 END) AS pendentes,
                SUM(CASE WHEN ADVERTENCIA IS NOT NULL THEN 1 ELSE 0 END) AS adv_processadas,
                SUM(CASE WHEN ADVERTENCIA = 1 THEN 1 ELSE 0 END) AS adv_marcadas,
                SUM(CASE WHEN REINCIDENCIA IS NOT NULL THEN 1 ELSE 0 END) AS reinc_processadas,
                SUM(CASE WHEN REINCIDENCIA = 1 THEN 1 ELSE 0 END) AS reinc_marcadas,
                SUM(CASE WHEN REITERACAO IS NOT NULL THEN 1 ELSE 0 END) AS reit_processadas,
                SUM(CASE WHEN REITERACAO = 1 THEN 1 ELSE 0 END) AS reit_marcadas
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO
            WHERE DT_APURACAO = :dt_apuracao AND GRUPO = :grupo
        """)
        row = db.session.execute(sql, {'dt_apuracao': dt_apuracao, 'grupo': grupo}).fetchone()

        total = row.total or 0
        fora_prazo_sem_just = row.fora_prazo_sem_just or 0
        pendentes = row.pendentes or 0
        adv_processadas = row.adv_processadas or 0
        reinc_processadas = row.reinc_processadas or 0
        reit_processadas = row.reit_processadas or 0

        percentual = (fora_prazo_sem_just / total * 100) if total > 0 else 0

        return {
            'total': total,
            'pendentes': pendentes,
            'todas_analisadas': pendentes == 0,
            'total_fora_prazo': row.total_fora_prazo or 0,
            'total_dentro_prazo': row.total_dentro_prazo or 0,
            'total_just_aceitas': row.total_just_aceitas or 0,
            'total_just_rejeitadas': row.total_just_rejeitadas or 0,
            'fora_prazo_sem_just': fora_prazo_sem_just,
            'percentual': round(percentual, 2),
            'sera_advertido': percentual > 10,
            # Estados dos botões
            'advertencia_processada': adv_processadas == total and total > 0,
            'adv_marcadas': row.adv_marcadas or 0,
            'reincidencia_processada': reinc_processadas == total and total > 0,
            'reinc_marcadas': row.reinc_marcadas or 0,
            'reiteracao_processada': reit_processadas == total and total > 0,
            'reit_marcadas': row.reit_marcadas or 0,
        }

    @staticmethod
    def aplicar_advertencia_grupo(dt_apuracao, grupo):
        resumo = AnsApuracao.calcular_resumo_advertencia(dt_apuracao, grupo)
        if not resumo['todas_analisadas']:
            return False, 'Ainda existem ocorrências pendentes de análise neste grupo.'

        dt_int = int(str(dt_apuracao).replace('-', ''))

        if resumo['sera_advertido']:
            sql_advertir = text("""
                UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO
                SET ADVERTENCIA = CASE
                        WHEN NO_PRAZO = 0 AND JUST_ACEITA = 0 THEN 1 ELSE 0
                    END,
                    DT_ADVERTENCIA = CASE
                        WHEN NO_PRAZO = 0 AND JUST_ACEITA = 0 THEN :dt_int ELSE NULL
                    END
                WHERE DT_APURACAO = :dt_apuracao AND GRUPO = :grupo
            """)
        else:
            sql_advertir = text("""
                UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO
                SET ADVERTENCIA = 0, DT_ADVERTENCIA = NULL
                WHERE DT_APURACAO = :dt_apuracao AND GRUPO = :grupo
            """)

        db.session.execute(sql_advertir, {
            'dt_apuracao': dt_apuracao, 'grupo': grupo,
            'dt_int': dt_int if resumo['sera_advertido'] else None
        })
        db.session.commit()
        return True, resumo

    # ==================================================================
    # REINCIDÊNCIA
    # ==================================================================

    @staticmethod
    def aplicar_reincidencia_grupo(dt_apuracao, grupo):
        """
        Aplica reincidência para o grupo:

        1. Verifica se advertência já foi processada no grupo
        2. Busca a DT_APURACAO anterior
        3. Verifica se o mesmo critério >10% fora do prazo + just rejeitada se aplica
        4. Para cada ocorrência que está fora do prazo + just rejeitada no período ATUAL,
           verifica se a MESMA nrOcorrencia tinha ADVERTENCIA=1 no período ANTERIOR
        5. Se sim E >10%: REINCIDENCIA=1, DT_REINCIDENCIA = DT_APURACAO atual (como int)
        6. Demais: REINCIDENCIA=0, DT_REINCIDENCIA=NULL
        7. Se <=10%: REINCIDENCIA=0 em TODAS
        """
        resumo = AnsApuracao.calcular_resumo_advertencia(dt_apuracao, grupo)

        if not resumo['advertencia_processada']:
            return False, 'A advertência ainda não foi processada para este grupo.'

        dt_anterior = AnsApuracao.obter_data_anterior(dt_apuracao)
        if not dt_anterior:
            return False, 'Não foi encontrada uma data de apuração anterior para comparação.'

        dt_int = int(str(dt_apuracao).replace('-', ''))

        if resumo['sera_advertido']:
            # >10%: marcar REINCIDENCIA=1 nas que estão fora do prazo + just rejeitada
            # E que tinham ADVERTENCIA=1 na data anterior com mesmo nrOcorrencia
            sql = text("""
                UPDATE A
                SET A.REINCIDENCIA = CASE
                        WHEN A.NO_PRAZO = 0 AND A.JUST_ACEITA = 0
                             AND EXISTS (
                                 SELECT 1 FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO P
                                 WHERE P.DT_APURACAO = :dt_anterior
                                   AND P.GRUPO = :grupo
                                   AND P.nrOcorrencia = A.nrOcorrencia
                                   AND P.ADVERTENCIA = 1
                             )
                        THEN 1 ELSE 0
                    END,
                    A.DT_REINCIDENCIA = CASE
                        WHEN A.NO_PRAZO = 0 AND A.JUST_ACEITA = 0
                             AND EXISTS (
                                 SELECT 1 FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO P
                                 WHERE P.DT_APURACAO = :dt_anterior
                                   AND P.GRUPO = :grupo
                                   AND P.nrOcorrencia = A.nrOcorrencia
                                   AND P.ADVERTENCIA = 1
                             )
                        THEN :dt_int ELSE NULL
                    END
                FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO A
                WHERE A.DT_APURACAO = :dt_apuracao AND A.GRUPO = :grupo
            """)
        else:
            # <=10%: REINCIDENCIA=0 em todas
            sql = text("""
                UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO
                SET REINCIDENCIA = 0, DT_REINCIDENCIA = NULL
                WHERE DT_APURACAO = :dt_apuracao AND GRUPO = :grupo
            """)

        db.session.execute(sql, {
            'dt_apuracao': dt_apuracao, 'dt_anterior': dt_anterior,
            'grupo': grupo, 'dt_int': dt_int
        })
        db.session.commit()

        # Contar quantas reincidências foram marcadas
        sql_count = text("""
            SELECT SUM(CASE WHEN REINCIDENCIA = 1 THEN 1 ELSE 0 END) AS total_reinc
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO
            WHERE DT_APURACAO = :dt_apuracao AND GRUPO = :grupo
        """)
        row = db.session.execute(sql_count, {'dt_apuracao': dt_apuracao, 'grupo': grupo}).fetchone()

        resumo['reinc_marcadas_resultado'] = row.total_reinc or 0
        resumo['dt_anterior'] = str(dt_anterior)
        return True, resumo

    # ==================================================================
    # REITERAÇÃO
    # ==================================================================

    @staticmethod
    def aplicar_reiteracao_grupo(dt_apuracao, grupo):
        """
        Aplica reiteração para o grupo:

        Mesma lógica da reincidência, mas verifica REINCIDENCIA=1
        no período anterior (em vez de ADVERTENCIA=1).

        1. Verifica se reincidência já foi processada no grupo
        2. Busca DT_APURACAO anterior
        3. Para cada ocorrência fora do prazo + just rejeitada + >10%,
           verifica se a MESMA nrOcorrencia tinha REINCIDENCIA=1 no período anterior
        4. Se sim: REITERACAO=1, DT_REITERACAO = DT_APURACAO atual (como int)
        5. Demais: REITERACAO=0
        6. Se <=10%: REITERACAO=0 em TODAS
        """
        resumo = AnsApuracao.calcular_resumo_advertencia(dt_apuracao, grupo)

        if not resumo['reincidencia_processada']:
            return False, 'A reincidência ainda não foi processada para este grupo.'

        dt_anterior = AnsApuracao.obter_data_anterior(dt_apuracao)
        if not dt_anterior:
            return False, 'Não foi encontrada uma data de apuração anterior para comparação.'

        dt_int = int(str(dt_apuracao).replace('-', ''))

        if resumo['sera_advertido']:
            sql = text("""
                UPDATE A
                SET A.REITERACAO = CASE
                        WHEN A.NO_PRAZO = 0 AND A.JUST_ACEITA = 0
                             AND EXISTS (
                                 SELECT 1 FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO P
                                 WHERE P.DT_APURACAO = :dt_anterior
                                   AND P.GRUPO = :grupo
                                   AND P.nrOcorrencia = A.nrOcorrencia
                                   AND P.REINCIDENCIA = 1
                             )
                        THEN 1 ELSE 0
                    END,
                    A.DT_REITERACAO = CASE
                        WHEN A.NO_PRAZO = 0 AND A.JUST_ACEITA = 0
                             AND EXISTS (
                                 SELECT 1 FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO P
                                 WHERE P.DT_APURACAO = :dt_anterior
                                   AND P.GRUPO = :grupo
                                   AND P.nrOcorrencia = A.nrOcorrencia
                                   AND P.REINCIDENCIA = 1
                             )
                        THEN :dt_int ELSE NULL
                    END
                FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO A
                WHERE A.DT_APURACAO = :dt_apuracao AND A.GRUPO = :grupo
            """)
        else:
            sql = text("""
                UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO
                SET REITERACAO = 0, DT_REITERACAO = NULL
                WHERE DT_APURACAO = :dt_apuracao AND GRUPO = :grupo
            """)

        db.session.execute(sql, {
            'dt_apuracao': dt_apuracao, 'dt_anterior': dt_anterior,
            'grupo': grupo, 'dt_int': dt_int
        })
        db.session.commit()

        sql_count = text("""
            SELECT SUM(CASE WHEN REITERACAO = 1 THEN 1 ELSE 0 END) AS total_reit
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO
            WHERE DT_APURACAO = :dt_apuracao AND GRUPO = :grupo
        """)
        row = db.session.execute(sql_count, {'dt_apuracao': dt_apuracao, 'grupo': grupo}).fetchone()

        resumo['reit_marcadas_resultado'] = row.total_reit or 0
        resumo['dt_anterior'] = str(dt_anterior)
        return True, resumo


class AnsItensFaturamento(db.Model):
    __tablename__ = 'MOV_TB043_ANS_ITENS_FATURAMENTO'
    __table_args__ = {'schema': 'BDG'}

    itemServico = Column(String(150), primary_key=True, nullable=False)
    GRUPO = Column('GRUPO', db.SmallInteger, nullable=False)
    PRAZO = Column('PRAZO', db.SmallInteger, nullable=False)

    def __repr__(self):
        return f'<AnsItensFaturamento {self.itemServico} - Grupo {self.GRUPO}>'

    @staticmethod
    def listar_todos():
        return AnsItensFaturamento.query.order_by(AnsItensFaturamento.GRUPO).all()

    @staticmethod
    def buscar_por_grupo(grupo):
        return AnsItensFaturamento.query.filter_by(GRUPO=grupo).first()