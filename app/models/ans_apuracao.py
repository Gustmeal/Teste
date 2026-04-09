"""
Model: app/models/ans_apuracao.py
Módulo ANS Glosas
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
    DT_ADVERTENCIA = Column(Date, nullable=True)
    REINCIDENCIA = Column(Integer, nullable=True)
    DT_REINCIDENCIA = Column(Date, nullable=True)
    REITERACAO = Column(Integer, nullable=True)
    DT_REITERACAO = Column(Date, nullable=True)
    JUST_ACEITA = Column(Integer, nullable=True)
    DSC_JUSTIFICATIVA = Column(String(4000), nullable=True)

    def __repr__(self):
        return f'<AnsApuracao {self.DT_APURACAO}-{self.nrOcorrencia}>'

    # ==================================================================
    # DATAS DE APURAÇÃO
    # ==================================================================

    @staticmethod
    def obter_datas_apuracao():
        sql = text("SELECT DISTINCT DT_APURACAO FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO ORDER BY DT_APURACAO DESC")
        return [row.DT_APURACAO for row in db.session.execute(sql).fetchall()]

    @staticmethod
    def obter_data_anterior(dt_apuracao):
        sql = text("SELECT TOP 1 DT_APURACAO FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO WHERE DT_APURACAO < :dt ORDER BY DT_APURACAO DESC")
        row = db.session.execute(sql, {'dt': dt_apuracao}).fetchone()
        return row.DT_APURACAO if row else None

    # ==================================================================
    # QUERIES BASE
    # ==================================================================

    @staticmethod
    def _query_base(dt_apuracao, filtro_just_aceita):
        where_just = "AND A.JUST_ACEITA IS NULL" if filtro_just_aceita == 'NULL' else "AND A.JUST_ACEITA IS NOT NULL"
        sql = text(f"""
            SELECT A.DT_APURACAO, A.nrOcorrencia, A.GRUPO, A.DT_ABERTURA, A.DT_ANDAMENTO, A.DT_JUSTIFICATIVA,
                A.DT_EFETIVACAO, A.DT_DEFERIDO,
                A.DSC_JUSTIFICATIVA, A.JUST_ACEITA, A.QTDE_DIAS, A.NO_PRAZO,
                A.ADVERTENCIA, A.DT_ADVERTENCIA, A.REINCIDENCIA, A.DT_REINCIDENCIA,
                A.REITERACAO, A.DT_REITERACAO, B.itemServico, B.PRAZO
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO A
            OUTER APPLY (SELECT TOP 1 X.itemServico, X.PRAZO FROM BDDASHBOARDBI.BDG.MOV_TB043_ANS_ITENS_FATURAMENTO X WHERE X.GRUPO = A.GRUPO) B
            WHERE A.DT_APURACAO = :dt_apuracao {where_just}
            ORDER BY A.GRUPO, A.nrOcorrencia
        """)
        return db.session.execute(sql, {'dt_apuracao': dt_apuracao}).fetchall()

    @staticmethod
    def _agrupar_por_grupo(resultados):
        agrupado = {}
        for row in resultados:
            g = row.GRUPO
            if g not in agrupado:
                agrupado[g] = []
            agrupado[g].append(row)
        return agrupado

    @staticmethod
    def listar_por_grupo(dt_apuracao):
        return AnsApuracao._agrupar_por_grupo(AnsApuracao._query_base(dt_apuracao, 'NULL'))

    @staticmethod
    def listar_analisadas_por_grupo(dt_apuracao):
        return AnsApuracao._agrupar_por_grupo(AnsApuracao._query_base(dt_apuracao, 'NOT NULL'))

    @staticmethod
    def buscar_ocorrencia(dt_apuracao, nr_ocorrencia):
        sql = text("""
            SELECT A.DT_APURACAO, A.nrOcorrencia, A.GRUPO, A.DT_ABERTURA, A.DT_ANDAMENTO, A.DT_JUSTIFICATIVA,
                A.DSC_JUSTIFICATIVA, A.JUST_ACEITA, A.QTDE_DIAS, A.NO_PRAZO, A.ADVERTENCIA, B.itemServico, B.PRAZO
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO A
            OUTER APPLY (SELECT TOP 1 X.itemServico, X.PRAZO FROM BDDASHBOARDBI.BDG.MOV_TB043_ANS_ITENS_FATURAMENTO X WHERE X.GRUPO = A.GRUPO) B
            WHERE A.DT_APURACAO = :dt_apuracao AND A.nrOcorrencia = :nr_ocorrencia
        """)
        return db.session.execute(sql, {'dt_apuracao': dt_apuracao, 'nr_ocorrencia': nr_ocorrencia}).fetchone()

    # ==================================================================
    # SALVAR ANÁLISE
    # ==================================================================

    @staticmethod
    def salvar_analise(dt_apuracao, nr_ocorrencia, just_aceita):
        oc = AnsApuracao.buscar_ocorrencia(dt_apuracao, nr_ocorrencia)
        if not oc:
            return False, 'Ocorrência não encontrada.'
        dt_abertura = oc.DT_ABERTURA
        if not dt_abertura:
            return False, 'Data de abertura não encontrada.'
        if just_aceita == 1:
            dt_fim = oc.DT_JUSTIFICATIVA
            if not dt_fim:
                return False, 'Data de justificativa não encontrada.'
        else:
            dt_fim = oc.DT_APURACAO
            if not dt_fim:
                return False, 'Data de apuração não encontrada.'
        if isinstance(dt_abertura, datetime): dt_abertura = dt_abertura.date()
        if isinstance(dt_fim, datetime): dt_fim = dt_fim.date()
        qtde_dias = (dt_fim - dt_abertura).days + 1
        prazo = oc.PRAZO
        no_prazo = (1 if qtde_dias <= prazo else 0) if prazo is not None else None
        sql_update = text("UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO SET JUST_ACEITA=:ja, QTDE_DIAS=:qd, NO_PRAZO=:np WHERE DT_APURACAO=:dt AND nrOcorrencia=:nr")
        db.session.execute(sql_update, {'ja': just_aceita, 'qd': qtde_dias, 'np': no_prazo, 'dt': dt_apuracao, 'nr': nr_ocorrencia})
        db.session.commit()
        return True, {'qtde_dias': qtde_dias, 'no_prazo': no_prazo, 'prazo': prazo}

    @staticmethod
    def salvar_analise_lote(dt_apuracao, analises):
        sc, ec, errs = 0, 0, []
        for item in analises:
            nr, ja = item.get('nr_ocorrencia'), item.get('just_aceita')
            if nr is None or ja is None:
                ec += 1; errs.append(f'Incompleto: {nr}'); continue
            ok, res = AnsApuracao.salvar_analise(dt_apuracao, nr, ja)
            if ok: sc += 1
            else: ec += 1; errs.append(f'{nr}: {res}')
        return sc, ec, errs

    # ==================================================================
    # ESTATÍSTICAS E GRUPOS
    # ==================================================================

    @staticmethod
    def obter_estatisticas(dt_apuracao):
        sql = text("""
            SELECT COUNT(*) AS total,
                SUM(CASE WHEN JUST_ACEITA IS NOT NULL THEN 1 ELSE 0 END) AS analisadas,
                SUM(CASE WHEN JUST_ACEITA IS NULL THEN 1 ELSE 0 END) AS pendentes,
                SUM(CASE WHEN JUST_ACEITA = 1 THEN 1 ELSE 0 END) AS aceitas,
                SUM(CASE WHEN JUST_ACEITA = 0 THEN 1 ELSE 0 END) AS rejeitadas,
                SUM(CASE WHEN NO_PRAZO = 1 THEN 1 ELSE 0 END) AS dentro_prazo,
                SUM(CASE WHEN NO_PRAZO = 0 THEN 1 ELSE 0 END) AS fora_prazo
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO WHERE DT_APURACAO = :dt
        """)
        return db.session.execute(sql, {'dt': dt_apuracao}).fetchone()

    @staticmethod
    def obter_todos_grupos(dt_apuracao):
        sql = text("""
            SELECT DISTINCT A.GRUPO, B.itemServico, B.PRAZO, B.NO_GRUPO
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO A
            OUTER APPLY (SELECT TOP 1 X.itemServico, X.PRAZO, X.NO_GRUPO FROM BDDASHBOARDBI.BDG.MOV_TB043_ANS_ITENS_FATURAMENTO X WHERE X.GRUPO = A.GRUPO) B
            WHERE A.DT_APURACAO = :dt ORDER BY A.GRUPO
        """)
        return db.session.execute(sql, {'dt': dt_apuracao}).fetchall()

    # ==================================================================
    # RESUMO POR GRUPO
    # ==================================================================
    # O cálculo de percentual, pendentes, total etc. vem da TB045 — IGUAL ao original
    # Os contadores de penalidades (adv_marcadas, reinc_marcadas, reit_marcadas)
    # e os flags _processada vêm da TB049 (fonte de verdade dos badges)
    # ==================================================================

    @staticmethod
    def calcular_resumo_advertencia(dt_apuracao, grupo):
        # Dados da TB045 — SEM filtro, exatamente como o original
        sql_tb045 = text("""
            SELECT COUNT(*) AS total,
                SUM(CASE WHEN NO_PRAZO=0 AND JUST_ACEITA=0 THEN 1 ELSE 0 END) AS fora_prazo_sem_just,
                SUM(CASE WHEN NO_PRAZO=0 THEN 1 ELSE 0 END) AS total_fora_prazo,
                SUM(CASE WHEN NO_PRAZO=1 THEN 1 ELSE 0 END) AS total_dentro_prazo,
                SUM(CASE WHEN JUST_ACEITA=1 THEN 1 ELSE 0 END) AS total_just_aceitas,
                SUM(CASE WHEN JUST_ACEITA=0 THEN 1 ELSE 0 END) AS total_just_rejeitadas,
                SUM(CASE WHEN JUST_ACEITA IS NULL THEN 1 ELSE 0 END) AS pendentes
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO 
            WHERE DT_APURACAO=:dt AND GRUPO=:g
        """)
        r = db.session.execute(sql_tb045, {'dt': dt_apuracao, 'g': grupo}).fetchone()

        total = r.total or 0
        fpsj = r.fora_prazo_sem_just or 0
        pend = r.pendentes or 0
        pct = (fpsj / total * 100) if total > 0 else 0

        # Dados da TB049 — fonte de verdade dos badges do cabeçalho
        sql_tb049 = text("""
            SELECT PENALIDADE, QTDE_OCORRENCIAS
            FROM BDDASHBOARDBI.BDG.MOV_TB049_ANS_PENALIDADES_CONCLUIDAS
            WHERE DT_APURACAO=:dt AND GRUPO=:g
        """)
        penalidades = db.session.execute(sql_tb049, {'dt': dt_apuracao, 'g': grupo}).fetchall()

        adv_processada = False
        adv_marcadas = 0
        reinc_processada = False
        reinc_marcadas = 0
        reit_processada = False
        reit_marcadas = 0

        for p in penalidades:
            if p.PENALIDADE == 1:
                adv_processada = True
                adv_marcadas = p.QTDE_OCORRENCIAS or 0
            elif p.PENALIDADE == 2:
                reinc_processada = True
                reinc_marcadas = p.QTDE_OCORRENCIAS or 0
            elif p.PENALIDADE == 3:
                reit_processada = True
                reit_marcadas = p.QTDE_OCORRENCIAS or 0

        return {
            'total': total, 'pendentes': pend, 'todas_analisadas': pend == 0,
            'total_fora_prazo': r.total_fora_prazo or 0,
            'total_dentro_prazo': r.total_dentro_prazo or 0,
            'total_just_aceitas': r.total_just_aceitas or 0,
            'total_just_rejeitadas': r.total_just_rejeitadas or 0,
            'fora_prazo_sem_just': fpsj,
            'percentual': round(pct, 2),
            'sera_advertido': pct > 10,
            'advertencia_processada': adv_processada,
            'adv_marcadas': adv_marcadas,
            'reincidencia_processada': reinc_processada,
            'reinc_marcadas': reinc_marcadas,
            'reiteracao_processada': reit_processada,
            'reit_marcadas': reit_marcadas,
        }

    # ==================================================================
    # HELPER: REGISTRO NA TB049
    # ==================================================================

    @staticmethod
    def _registrar_penalidade_tb049(dt_apuracao, grupo, penalidade, qtde_ocorrencias):
        """
        Insere/atualiza registro na TB049.
        penalidade: 1=Advertência, 2=Reincidência, 3=Reiteração
        PK: DT_APURACAO + PENALIDADE + GRUPO
        """
        sql_del = text("""
            DELETE FROM BDDASHBOARDBI.BDG.MOV_TB049_ANS_PENALIDADES_CONCLUIDAS
            WHERE DT_APURACAO=:dt AND PENALIDADE=:p AND GRUPO=:g
        """)
        db.session.execute(sql_del, {'dt': dt_apuracao, 'p': penalidade, 'g': grupo})

        sql_ins = text("""
            INSERT INTO BDDASHBOARDBI.BDG.MOV_TB049_ANS_PENALIDADES_CONCLUIDAS
            (DT_APURACAO, PENALIDADE, GRUPO, QTDE_OCORRENCIAS, DT_APLICACAO)
            VALUES (:dt, :p, :g, :qtde, :dt)
        """)
        db.session.execute(sql_ins, {
            'dt': dt_apuracao, 'p': penalidade, 'g': grupo, 'qtde': qtde_ocorrencias
        })

    # ==================================================================
    # ADVERTÊNCIA
    # ==================================================================
    # Lógica IGUAL à original: UPDATE marca 1/0 conforme percentual > 10%
    # Diferença: antes do UPDATE, captura os nrOcorrencia que JÁ estavam
    # com ADVERTENCIA=1 (de meses anteriores). Depois do UPDATE, conta as
    # que ficaram com ADVERTENCIA=1 EXCLUINDO as que já existiam — assim
    # só as NOVAS desta rodada são gravadas na TB049.
    # ==================================================================

    @staticmethod
    def aplicar_advertencia_grupo(dt_apuracao, grupo):
        resumo = AnsApuracao.calcular_resumo_advertencia(dt_apuracao, grupo)

        # Capturar nrOcorrencias que JÁ estavam com ADVERTENCIA=1 ANTES do UPDATE
        sql_antes = text("""
            SELECT nrOcorrencia 
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO 
            WHERE DT_APURACAO=:dt AND GRUPO=:g AND ADVERTENCIA=1
        """)
        ja_advertidas_antes = {row.nrOcorrencia for row in
                               db.session.execute(sql_antes, {'dt': dt_apuracao, 'g': grupo}).fetchall()}

        # UPDATE — preserva DT_ADVERTENCIA existente, só preenche quando NULL
        if resumo['sera_advertido']:
            sql = text("""
                UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO
                SET ADVERTENCIA = CASE WHEN NO_PRAZO=0 AND JUST_ACEITA=0 THEN 1 ELSE 0 END,
                    DT_ADVERTENCIA = CASE 
                        WHEN NO_PRAZO=0 AND JUST_ACEITA=0 AND DT_ADVERTENCIA IS NOT NULL THEN DT_ADVERTENCIA
                        WHEN NO_PRAZO=0 AND JUST_ACEITA=0 THEN :dt 
                        ELSE NULL 
                    END
                WHERE DT_APURACAO=:dt AND GRUPO=:g
            """)
        else:
            sql = text("""
                UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO 
                SET ADVERTENCIA=0, DT_ADVERTENCIA=NULL 
                WHERE DT_APURACAO=:dt AND GRUPO=:g
                  AND ADVERTENCIA IS NULL
            """)
        db.session.execute(sql, {'dt': dt_apuracao, 'g': grupo})

        # Capturar nrOcorrencias com ADVERTENCIA=1 DEPOIS do UPDATE
        sql_depois = text("""
            SELECT nrOcorrencia 
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO 
            WHERE DT_APURACAO=:dt AND GRUPO=:g AND ADVERTENCIA=1
        """)
        advertidas_depois = {row.nrOcorrencia for row in
                             db.session.execute(sql_depois, {'dt': dt_apuracao, 'g': grupo}).fetchall()}

        # SÓ AS NOVAS = diferença entre depois e antes
        novas = advertidas_depois - ja_advertidas_antes
        qtde_novas = len(novas)

        # Registrar na TB049 apenas a quantidade de NOVAS
        AnsApuracao._registrar_penalidade_tb049(dt_apuracao, grupo, 1, qtde_novas)

        db.session.commit()

        resumo['adv_marcadas'] = qtde_novas
        return True, resumo

    # ==================================================================
    # REINCIDÊNCIA
    # ==================================================================

    @staticmethod
    def aplicar_reincidencia_grupo(dt_apuracao, grupo):
        resumo = AnsApuracao.calcular_resumo_advertencia(dt_apuracao, grupo)
        if not resumo['advertencia_processada']:
            return False, 'Advertência ainda não processada.'

        dt_ant = AnsApuracao.obter_data_anterior(dt_apuracao)
        if not dt_ant:
            # Sem histórico, zera tudo e registra 0 na TB049
            sql = text("""
                UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO 
                SET REINCIDENCIA=0, DT_REINCIDENCIA=NULL 
                WHERE DT_APURACAO=:dt AND GRUPO=:g
                  AND REINCIDENCIA IS NULL
            """)
            db.session.execute(sql, {'dt': dt_apuracao, 'g': grupo})
            AnsApuracao._registrar_penalidade_tb049(dt_apuracao, grupo, 2, 0)
            db.session.commit()
            resumo['reinc_marcadas_resultado'] = 0
            resumo['reinc_marcadas'] = 0
            resumo['dt_anterior'] = 'Sem período anterior'
            return True, resumo

        # Capturar nrOcorrencias que JÁ estavam com REINCIDENCIA=1 ANTES do UPDATE
        sql_antes = text("""
            SELECT nrOcorrencia 
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO 
            WHERE DT_APURACAO=:dt AND GRUPO=:g AND REINCIDENCIA=1
        """)
        ja_reincidentes_antes = {row.nrOcorrencia for row in
                                 db.session.execute(sql_antes, {'dt': dt_apuracao, 'g': grupo}).fetchall()}

        # UPDATE — preserva DT_REINCIDENCIA existente, só preenche quando NULL
        if resumo['sera_advertido']:
            sql = text("""
                UPDATE A SET
                    A.REINCIDENCIA = CASE WHEN A.NO_PRAZO=0 AND A.JUST_ACEITA=0
                        AND EXISTS (SELECT 1 FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO P WHERE P.DT_APURACAO=:da AND P.GRUPO=:g AND P.nrOcorrencia=A.nrOcorrencia AND P.ADVERTENCIA=1)
                        THEN 1 ELSE 0 END,
                    A.DT_REINCIDENCIA = CASE 
                        WHEN A.NO_PRAZO=0 AND A.JUST_ACEITA=0
                            AND EXISTS (SELECT 1 FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO P WHERE P.DT_APURACAO=:da AND P.GRUPO=:g AND P.nrOcorrencia=A.nrOcorrencia AND P.ADVERTENCIA=1)
                            AND A.DT_REINCIDENCIA IS NOT NULL THEN A.DT_REINCIDENCIA
                        WHEN A.NO_PRAZO=0 AND A.JUST_ACEITA=0
                            AND EXISTS (SELECT 1 FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO P WHERE P.DT_APURACAO=:da AND P.GRUPO=:g AND P.nrOcorrencia=A.nrOcorrencia AND P.ADVERTENCIA=1)
                            THEN :dt 
                        ELSE NULL 
                    END
                FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO A 
                WHERE A.DT_APURACAO=:dt AND A.GRUPO=:g
            """)
        else:
            sql = text("""
                UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO 
                SET REINCIDENCIA=0, DT_REINCIDENCIA=NULL 
                WHERE DT_APURACAO=:dt AND GRUPO=:g
                  AND REINCIDENCIA IS NULL
            """)
        db.session.execute(sql, {'dt': dt_apuracao, 'da': dt_ant, 'g': grupo})

        # Capturar DEPOIS do UPDATE
        sql_depois = text("""
            SELECT nrOcorrencia 
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO 
            WHERE DT_APURACAO=:dt AND GRUPO=:g AND REINCIDENCIA=1
        """)
        reincidentes_depois = {row.nrOcorrencia for row in
                               db.session.execute(sql_depois, {'dt': dt_apuracao, 'g': grupo}).fetchall()}

        # SÓ AS NOVAS
        novas = reincidentes_depois - ja_reincidentes_antes
        qtde_novas = len(novas)

        AnsApuracao._registrar_penalidade_tb049(dt_apuracao, grupo, 2, qtde_novas)
        db.session.commit()

        resumo['reinc_marcadas_resultado'] = qtde_novas
        resumo['reinc_marcadas'] = qtde_novas
        resumo['dt_anterior'] = str(dt_ant)
        return True, resumo

    # ==================================================================
    # REITERAÇÃO
    # ==================================================================

    @staticmethod
    def aplicar_reiteracao_grupo(dt_apuracao, grupo):
        resumo = AnsApuracao.calcular_resumo_advertencia(dt_apuracao, grupo)
        if not resumo['reincidencia_processada']:
            return False, 'Reincidência ainda não processada.'

        dt_ant = AnsApuracao.obter_data_anterior(dt_apuracao)
        if not dt_ant:
            sql = text("""
                    UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO 
                    SET REITERACAO=0, DT_REITERACAO=NULL 
                    WHERE DT_APURACAO=:dt AND GRUPO=:g
                """)
            db.session.execute(sql, {'dt': dt_apuracao, 'g': grupo})
            AnsApuracao._registrar_penalidade_tb049(dt_apuracao, grupo, 3, 0)
            db.session.commit()
            resumo['reit_marcadas_resultado'] = 0
            resumo['reit_marcadas'] = 0
            resumo['dt_anterior'] = 'Sem período anterior'
            return True, resumo

        # Capturar ANTES do UPDATE
        sql_antes = text("""
                SELECT nrOcorrencia 
                FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO 
                WHERE DT_APURACAO=:dt AND GRUPO=:g AND REITERACAO=1
            """)
        ja_reiteradas_antes = {row.nrOcorrencia for row in
                               db.session.execute(sql_antes, {'dt': dt_apuracao, 'g': grupo}).fetchall()}

        # UPDATE — lógica IGUAL ao original
        if resumo['sera_advertido']:
            sql = text("""
                    UPDATE A SET
                        A.REITERACAO = CASE WHEN A.NO_PRAZO=0 AND A.JUST_ACEITA=0
                            AND EXISTS (SELECT 1 FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO P WHERE P.DT_APURACAO=:da AND P.GRUPO=:g AND P.nrOcorrencia=A.nrOcorrencia AND P.REINCIDENCIA=1)
                            THEN 1 ELSE 0 END,
                        A.DT_REITERACAO = CASE WHEN A.NO_PRAZO=0 AND A.JUST_ACEITA=0
                            AND EXISTS (SELECT 1 FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO P WHERE P.DT_APURACAO=:da AND P.GRUPO=:g AND P.nrOcorrencia=A.nrOcorrencia AND P.REINCIDENCIA=1)
                            THEN :dt ELSE NULL END
                    FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO A 
                    WHERE A.DT_APURACAO=:dt AND A.GRUPO=:g
                """)
        else:
            sql = text("""
                    UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO 
                    SET REITERACAO=0, DT_REITERACAO=NULL 
                    WHERE DT_APURACAO=:dt AND GRUPO=:g
                """)
        db.session.execute(sql, {'dt': dt_apuracao, 'da': dt_ant, 'g': grupo})

        # Capturar DEPOIS do UPDATE
        sql_depois = text("""
                SELECT nrOcorrencia 
                FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO 
                WHERE DT_APURACAO=:dt AND GRUPO=:g AND REITERACAO=1
            """)
        reiteradas_depois = {row.nrOcorrencia for row in
                             db.session.execute(sql_depois, {'dt': dt_apuracao, 'g': grupo}).fetchall()}

        # SÓ AS NOVAS
        novas = reiteradas_depois - ja_reiteradas_antes
        qtde_novas = len(novas)

        AnsApuracao._registrar_penalidade_tb049(dt_apuracao, grupo, 3, qtde_novas)
        db.session.commit()

        resumo['reit_marcadas_resultado'] = qtde_novas
        resumo['reit_marcadas'] = qtde_novas
        resumo['dt_anterior'] = str(dt_ant)
        return True, resumo

    # ==================================================================
    # EDIÇÃO INDIVIDUAL
    # ==================================================================

    @staticmethod
    def editar_campo_individual(dt_apuracao, nr_ocorrencia, campo, valor):
        campos_validos = {'ADVERTENCIA': 'DT_ADVERTENCIA', 'REINCIDENCIA': 'DT_REINCIDENCIA', 'REITERACAO': 'DT_REITERACAO'}
        if campo not in campos_validos:
            return False, f'Campo inválido: {campo}'
        dt_campo = campos_validos[campo]
        dt_valor = dt_apuracao if valor == 1 else None
        sql = text(f"UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO SET [{campo}]=:valor, [{dt_campo}]=:dt_valor WHERE DT_APURACAO=:dt AND nrOcorrencia=:nr")
        db.session.execute(sql, {'valor': valor, 'dt_valor': dt_valor, 'dt': dt_apuracao, 'nr': nr_ocorrencia})
        db.session.commit()
        return True, f'{campo} da ocorrência {nr_ocorrencia} alterada para {"Sim" if valor == 1 else "Não"}.'

    # ==================================================================
    # JUSTIFICATIVA DA PRESTADORA (TB048)
    # ==================================================================

    @staticmethod
    def salvar_justificativa_prestadora(dt_apuracao, nr_ocorrencia, retorno_prest):
        """
        Salva o retorno da prestadora na TB048.
        Se já existir registro com a mesma PK (DT_APURACAO + nrOcorrencia),
        retorna erro informando que já foi incluído anteriormente.
        """
        sql_check = text("""
            SELECT COUNT(*) AS qtd 
            FROM BDDASHBOARDBI.BDG.MOV_TB048_ANS_JUSTIFICATIVA_PRESTADORA
            WHERE DT_APURACAO = :dt AND nrOcorrencia = :nr
        """)
        row = db.session.execute(sql_check, {'dt': dt_apuracao, 'nr': nr_ocorrencia}).fetchone()
        if row and row.qtd > 0:
            return False, f'Já existe uma justificativa cadastrada para a ocorrência {nr_ocorrencia} nesta apuração.'

        sql_insert = text("""
            INSERT INTO BDDASHBOARDBI.BDG.MOV_TB048_ANS_JUSTIFICATIVA_PRESTADORA
            (DT_APURACAO, nrOcorrencia, RETORNO_PREST)
            VALUES (:dt, :nr, :ret)
        """)
        db.session.execute(sql_insert, {
            'dt': dt_apuracao,
            'nr': nr_ocorrencia,
            'ret': retorno_prest
        })
        db.session.commit()
        return True, f'Retorno da prestadora salvo com sucesso para a ocorrência {nr_ocorrencia}.'

    # ==================================================================
    # CONCLUSÃO DA APURAÇÃO
    # ==================================================================

    @staticmethod
    def verificar_apuracao_completa(dt_apuracao, todos_grupos):
        """
        Verifica se TODOS os grupos tiveram as 3 etapas processadas
        (advertência, reincidência e reiteração) e todas as ocorrências analisadas.
        """
        status_grupos = {}
        pode_concluir = True
        total_adv = 0
        total_reinc = 0
        total_reit = 0
        total_ocs = 0

        for grp in todos_grupos:
            resumo = AnsApuracao.calcular_resumo_advertencia(dt_apuracao, grp.GRUPO)
            grupo_ok = (
                resumo['todas_analisadas']
                and resumo['advertencia_processada']
                and resumo['reincidencia_processada']
                and resumo['reiteracao_processada']
            )
            status_grupos[grp.GRUPO] = {
                'completo': grupo_ok,
                'analise_ok': resumo['todas_analisadas'],
                'advertencia_ok': resumo['advertencia_processada'],
                'reincidencia_ok': resumo['reincidencia_processada'],
                'reiteracao_ok': resumo['reiteracao_processada'],
                'adv_marcadas': resumo['adv_marcadas'],
                'reinc_marcadas': resumo['reinc_marcadas'],
                'reit_marcadas': resumo['reit_marcadas'],
                'total': resumo['total'],
            }
            if not grupo_ok:
                pode_concluir = False
            total_adv += resumo['adv_marcadas']
            total_reinc += resumo['reinc_marcadas']
            total_reit += resumo['reit_marcadas']
            total_ocs += resumo['total']

        return {
            'pode_concluir': pode_concluir,
            'status_grupos': status_grupos,
            'total_ocorrencias': total_ocs,
            'total_grupos': len(todos_grupos),
            'total_advertencias': total_adv,
            'total_reincidencias': total_reinc,
            'total_reiteracoes': total_reit,
        }

    @staticmethod
    def verificar_conclusao_existente(dt_apuracao):
        """Verifica se já existe registro de conclusão para esta data de apuração"""
        sql = text("SELECT * FROM BDDASHBOARDBI.BDG.MOV_TB046_ANS_CONCLUSAO WHERE DT_APURACAO = :dt")
        row = db.session.execute(sql, {'dt': dt_apuracao}).fetchone()
        return row

    @staticmethod
    def concluir_apuracao(dt_apuracao, usuario_id, usuario_nome, verificacao):
        """Registra a conclusão da apuração na tabela MOV_TB046_ANS_CONCLUSAO."""
        existente = AnsApuracao.verificar_conclusao_existente(dt_apuracao)
        if existente:
            return False, 'Esta apuração já foi concluída anteriormente.'

        sql = text("""
            INSERT INTO BDDASHBOARDBI.BDG.MOV_TB046_ANS_CONCLUSAO
            (DT_APURACAO, DT_CONCLUSAO, USUARIO_ID, USUARIO_NOME,
             TOTAL_OCORRENCIAS, TOTAL_GRUPOS, TOTAL_ADVERTENCIAS,
             TOTAL_REINCIDENCIAS, TOTAL_REITERACOES)
            VALUES (:dt, GETDATE(), :uid, :unome, :toc, :tgr, :tadv, :treinc, :treit)
        """)
        db.session.execute(sql, {
            'dt': dt_apuracao,
            'uid': usuario_id,
            'unome': usuario_nome,
            'toc': verificacao['total_ocorrencias'],
            'tgr': verificacao['total_grupos'],
            'tadv': verificacao['total_advertencias'],
            'treinc': verificacao['total_reincidencias'],
            'treit': verificacao['total_reiteracoes'],
        })
        db.session.commit()
        return True, 'Apuração concluída com sucesso!'


class AnsItensFaturamento(db.Model):
    __tablename__ = 'MOV_TB043_ANS_ITENS_FATURAMENTO'
    __table_args__ = {'schema': 'BDG'}
    itemServico = Column(String(150), primary_key=True, nullable=False)
    GRUPO = Column('GRUPO', db.SmallInteger, nullable=False)
    PRAZO = Column('PRAZO', db.SmallInteger, nullable=False)
    NO_GRUPO = Column('NO_GRUPO', String(200), nullable=True)

    @staticmethod
    def listar_todos():
        return AnsItensFaturamento.query.order_by(AnsItensFaturamento.GRUPO).all()