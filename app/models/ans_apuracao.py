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

        # UPDATE — mantém DT_REITERACAO sempre com a data mais nova
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

        # Capturar DEPOIS do UPDATE — TODAS com REITERACAO=1
        sql_depois = text("""
            SELECT nrOcorrencia 
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO 
            WHERE DT_APURACAO=:dt AND GRUPO=:g AND REITERACAO=1
        """)
        reiteradas_depois = {row.nrOcorrencia for row in
                             db.session.execute(sql_depois, {'dt': dt_apuracao, 'g': grupo}).fetchall()}

        # DIFERENÇA: na reiteração, salva TODAS na TB049 (não só as novas)
        qtde_total_reiteradas = len(reiteradas_depois)

        # Só as novas (para exibir na mensagem)
        novas = reiteradas_depois - ja_reiteradas_antes
        qtde_novas = len(novas)

        # Registrar na TB049 o TOTAL de reiteradas (todas com REITERACAO=1)
        AnsApuracao._registrar_penalidade_tb049(dt_apuracao, grupo, 3, qtde_total_reiteradas)
        db.session.commit()

        resumo['reit_marcadas_resultado'] = qtde_novas
        resumo['reit_marcadas'] = qtde_total_reiteradas
        resumo['dt_anterior'] = str(dt_ant)
        return True, resumo

    # ==================================================================
    # EDIÇÃO INDIVIDUAL
    # ==================================================================

    @staticmethod
    def editar_campo_individual(dt_apuracao, nr_ocorrencia, campo, valor):
        campos_validos = {'ADVERTENCIA': 'DT_ADVERTENCIA', 'REINCIDENCIA': 'DT_REINCIDENCIA',
                          'REITERACAO': 'DT_REITERACAO'}
        if campo not in campos_validos:
            return False, f'Campo inválido: {campo}'

        # Mapeia o campo para o código de penalidade da TB049
        # 1 = Advertência | 2 = Reincidência | 3 = Reiteração
        penalidade_por_campo = {'ADVERTENCIA': 1, 'REINCIDENCIA': 2, 'REITERACAO': 3}

        dt_campo = campos_validos[campo]
        dt_valor = dt_apuracao if valor == 1 else None

        # 1) Atualiza a ocorrência na TB045 (fonte de verdade das ocorrências)
        sql = text(
            f"UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO SET [{campo}]=:valor, [{dt_campo}]=:dt_valor WHERE DT_APURACAO=:dt AND nrOcorrencia=:nr")
        db.session.execute(sql, {'valor': valor, 'dt_valor': dt_valor, 'dt': dt_apuracao, 'nr': nr_ocorrencia})

        # 2) Descobre o GRUPO da ocorrência (a TB049 é por grupo, e a edição só manda nrOcorrencia)
        sql_grupo = text("""
                SELECT TOP 1 GRUPO
                FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO
                WHERE DT_APURACAO=:dt AND nrOcorrencia=:nr
            """)
        row_grupo = db.session.execute(sql_grupo, {'dt': dt_apuracao, 'nr': nr_ocorrencia}).fetchone()

        if row_grupo:
            grupo = row_grupo.GRUPO
            penalidade = penalidade_por_campo[campo]

            # 3) Só sincroniza o badge se a penalidade JÁ foi processada para esse grupo
            #    (ou seja, já existe linha na TB049). Edição manual não deve criar badge do nada.
            sql_existe = text("""
                    SELECT COUNT(*) AS qtd
                    FROM BDDASHBOARDBI.BDG.MOV_TB049_ANS_PENALIDADES_CONCLUIDAS
                    WHERE DT_APURACAO=:dt AND PENALIDADE=:p AND GRUPO=:g
                """)
            ja_processada = db.session.execute(
                sql_existe, {'dt': dt_apuracao, 'p': penalidade, 'g': grupo}
            ).fetchone().qtd > 0

            if ja_processada:
                # 4) Reconta só as penalidades DESTE mês: campo = 1 e data da penalidade = DT_APURACAO.
                #    Mesmo critério do aplicar_* (só as novas da rodada) e do Excel Exportar Penalidades.
                #    Antes contava todas com campo = 1, inclusive as herdadas de meses anteriores,
                #    e o total do grupo não batia com o Excel depois de uma edição manual.
                sql_conta = text(f"""
                        SELECT COUNT(*) AS qtd
                        FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO
                        WHERE DT_APURACAO=:dt AND GRUPO=:g AND [{campo}]=1
                          AND [{dt_campo}] = DT_APURACAO
                    """)
                nova_qtde = db.session.execute(
                    sql_conta, {'dt': dt_apuracao, 'g': grupo}
                ).fetchone().qtd or 0

                # 5) Regrava o total na TB049 (DELETE + INSERT na PK) — badge passa a refletir a realidade
                AnsApuracao._registrar_penalidade_tb049(dt_apuracao, grupo, penalidade, nova_qtde)

        db.session.commit()
        return True, f'{campo} da ocorrência {nr_ocorrencia} alterada para {"Sim" if valor == 1 else "Não"}.'

    # ==================================================================
    # JUSTIFICATIVA DA PRESTADORA (TB048)
    # ==================================================================

    # ==================================================================
    # JUSTIFICATIVA DA PRESTADORA (TB048)
    # ==================================================================

    @staticmethod
    def salvar_justificativa_prestadora(dt_apuracao, nr_ocorrencia, retorno_prest, manifestacao_geadi=None):
        """
        Salva o retorno da prestadora na TB048 (PK: DT_APURACAO + nrOcorrencia).

        Regras:
          - Não existe linha para a PK -> INSERT
          - Já existe linha para a PK  -> UPDATE (substitui os textos antigos pelos novos)

        manifestacao_geadi é opcional (VARCHAR 500) — pode vir None/vazio.
        """
        sql_check = text("""
                SELECT COUNT(*) AS qtd
                FROM BDDASHBOARDBI.BDG.MOV_TB048_ANS_JUSTIFICATIVA_PRESTADORA
                WHERE DT_APURACAO = :dt AND nrOcorrencia = :nr
            """)
        row = db.session.execute(sql_check, {'dt': dt_apuracao, 'nr': nr_ocorrencia}).fetchone()
        ja_existe = bool(row and row.qtd > 0)

        params = {
            'dt': dt_apuracao,
            'nr': nr_ocorrencia,
            'ret': retorno_prest,
            'manif': manifestacao_geadi
        }

        if ja_existe:
            # Mesma data + mesma ocorrência -> substitui os valores antigos pelos novos
            sql_update = text("""
                    UPDATE BDDASHBOARDBI.BDG.MOV_TB048_ANS_JUSTIFICATIVA_PRESTADORA
                    SET RETORNO_PREST = :ret,
                        MANIFESTACAO_GEADI = :manif
                    WHERE DT_APURACAO = :dt AND nrOcorrencia = :nr
                """)
            db.session.execute(sql_update, params)
            db.session.commit()
            return True, f'Retorno da prestadora atualizado com sucesso para a ocorrência {nr_ocorrencia}.'

        # Não existe -> cria
        sql_insert = text("""
                INSERT INTO BDDASHBOARDBI.BDG.MOV_TB048_ANS_JUSTIFICATIVA_PRESTADORA
                (DT_APURACAO, nrOcorrencia, RETORNO_PREST, MANIFESTACAO_GEADI)
                VALUES (:dt, :nr, :ret, :manif)
            """)
        db.session.execute(sql_insert, params)
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

    @staticmethod
    def salvar_dt_aplicacao(dt_apuracao, dt_aplicacao):
        """
        Upsert na TB053_DT_APLICACAO_GLOSA.
        PK: DT_APURACAO. O usuário informa só a DT_APLICACAO;
        a DT_APURACAO vem automática (mesma data do filtro da apuração).
        """
        sql = text("""
                MERGE BDDASHBOARDBI.BDG.MOV_TB053_DT_APLICACAO_GLOSA AS dest
                USING (SELECT :dt AS DT_APURACAO, :dt_aplic AS DT_APLICACAO) AS src
                    ON dest.DT_APURACAO = src.DT_APURACAO
                WHEN MATCHED THEN
                    UPDATE SET dest.DT_APLICACAO = src.DT_APLICACAO
                WHEN NOT MATCHED THEN
                    INSERT (DT_APURACAO, DT_APLICACAO)
                    VALUES (src.DT_APURACAO, src.DT_APLICACAO);
            """)
        db.session.execute(sql, {'dt': dt_apuracao, 'dt_aplic': dt_aplicacao})
        db.session.commit()
        return True, f'Data de aplicação salva com sucesso ({dt_aplicacao}).'
    @staticmethod
    def obter_dt_aplicacao(dt_apuracao):
        """
        Lê a Data de Aplicação da Glosa da TB053 (PK: DT_APURACAO).
        Retorna a data ou None se ainda não foi informada.
        Blindado: qualquer falha na leitura não derruba a página.
        """
        try:
            sql = text("""
                SELECT DT_APLICACAO
                FROM BDDASHBOARDBI.BDG.MOV_TB053_DT_APLICACAO_GLOSA
                WHERE DT_APURACAO = :dt
            """)
            row = db.session.execute(sql, {'dt': dt_apuracao}).fetchone()
            return row.DT_APLICACAO if row else None
        except Exception:
            import traceback
            traceback.print_exc()
            db.session.rollback()
            return None

    # ==================================================================
    # EXPORTAÇÃO — PENALIDADES DA APURAÇÃO (ADV / REINC / REIT) COM VALORES
    # ==================================================================

    @staticmethod
    def listar_penalidades_exportacao(dt_apuracao):
        """
        Retorna as ocorrências que receberam ADVERTENCIA, REINCIDENCIA ou
        REITERACAO NO MÊS DA APURAÇÃO (dados 100% da TB045), SEM as
        justificativas (não lê DSC_JUSTIFICATIVA, DT_JUSTIFICATIVA,
        JUST_ACEITA nem a TB048).

        Regra do mês: a TB045 de cada apuração pode trazer ocorrências que já
        estavam com o campo = 1 de meses anteriores (a data da penalidade é
        preservada pelo app). Por isso a penalidade só conta quando o campo = 1
        E a data da penalidade (DT_ADVERTENCIA / DT_REINCIDENCIA /
        DT_REITERACAO) é igual à DT_APURACAO filtrada — mesmo critério que o
        app usa para contar as NOVAS de cada rodada.

        Valores por ocorrência:
          - Advertência  -> sem valor financeiro
          - Reincidência -> 10% da tarifa
          - Reiteração   -> 25% da tarifa

        Tarifa: vem da TB061 (MOV_TB061_TARIFA_PRESTADOR) pela DT_APURACAO.
        Se não existir linha exatamente na data, usa a última tarifa anterior.
        """
        from decimal import Decimal, ROUND_HALF_UP

        # Tarifa do prestador (TB061) — nada fixo no código
        tarifa = AnsApuracao.obter_tarifa_vigente(dt_apuracao)
        if tarifa is None:
            raise ValueError('Não há tarifa cadastrada na TB061 (MOV_TB061_TARIFA_PRESTADOR) '
                             'para a apuração {} ou datas anteriores.'.format(dt_apuracao))

        # Percentuais da glosa (regra de negócio — backend)
        perc_reincidencia = '0.10'
        perc_reiteracao = '0.25'

        centavo = Decimal('0.01')
        zero = Decimal('0.00')
        base = Decimal(str(tarifa['vr_tarifa'])).quantize(centavo, rounding=ROUND_HALF_UP)
        perc_reinc = Decimal(perc_reincidencia)
        perc_reit = Decimal(perc_reiteracao)
        valor_reinc = (base * perc_reinc).quantize(centavo, rounding=ROUND_HALF_UP)
        valor_reit = (base * perc_reit).quantize(centavo, rounding=ROUND_HALF_UP)

        sql = text("""
            SELECT A.DT_APURACAO, A.nrOcorrencia, A.NR_CONTRATO,
                   A.GRUPO, B.NO_GRUPO, B.itemServico, B.PRAZO,
                   A.DT_ABERTURA, A.DT_ANDAMENTO, A.DT_EFETIVACAO, A.DT_DEFERIDO,
                   A.QTDE_DIAS, A.NO_PRAZO,
                   CASE WHEN A.ADVERTENCIA = 1 AND A.DT_ADVERTENCIA = A.DT_APURACAO
                        THEN 1 ELSE 0 END AS ADVERTENCIA,
                   A.DT_ADVERTENCIA,
                   CASE WHEN A.REINCIDENCIA = 1 AND A.DT_REINCIDENCIA = A.DT_APURACAO
                        THEN 1 ELSE 0 END AS REINCIDENCIA,
                   A.DT_REINCIDENCIA,
                   CASE WHEN A.REITERACAO = 1 AND A.DT_REITERACAO = A.DT_APURACAO
                        THEN 1 ELSE 0 END AS REITERACAO,
                   A.DT_REITERACAO
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO A
            OUTER APPLY (
                SELECT TOP 1 X.itemServico, X.PRAZO, X.NO_GRUPO
                FROM BDDASHBOARDBI.BDG.MOV_TB043_ANS_ITENS_FATURAMENTO X
                WHERE X.GRUPO = A.GRUPO
            ) B
            WHERE A.DT_APURACAO = :dt
              AND (
                    (A.ADVERTENCIA  = 1 AND A.DT_ADVERTENCIA  = A.DT_APURACAO)
                 OR (A.REINCIDENCIA = 1 AND A.DT_REINCIDENCIA = A.DT_APURACAO)
                 OR (A.REITERACAO   = 1 AND A.DT_REITERACAO   = A.DT_APURACAO)
              )
            ORDER BY A.GRUPO, A.nrOcorrencia
        """)
        rows = db.session.execute(sql, {'dt': dt_apuracao}).fetchall()

        registros = []
        por_grupo = {}
        totais = {
            'qt_ocorrencias': 0, 'qt_advertencia': 0, 'qt_reincidencia': 0, 'qt_reiteracao': 0,
            'vr_reincidencia': zero, 'vr_reiteracao': zero, 'vr_total': zero,
        }

        for row in rows:
            m = dict(row._mapping)
            adv = 1 if m.get('ADVERTENCIA') == 1 else 0
            reinc = 1 if m.get('REINCIDENCIA') == 1 else 0
            reit = 1 if m.get('REITERACAO') == 1 else 0

            vr_reinc = valor_reinc if reinc else zero
            vr_reit = valor_reit if reit else zero
            m['VR_REINCIDENCIA'] = vr_reinc
            m['VR_REITERACAO'] = vr_reit
            m['VR_TOTAL'] = vr_reinc + vr_reit
            registros.append(m)

            # Consolidação por grupo (as linhas já vêm ordenadas por GRUPO)
            g = por_grupo.get(m['GRUPO'])
            if g is None:
                g = {
                    'GRUPO': m['GRUPO'], 'NO_GRUPO': m.get('NO_GRUPO'),
                    'QT_OCORRENCIAS': 0, 'QT_ADVERTENCIA': 0,
                    'QT_REINCIDENCIA': 0, 'VR_REINCIDENCIA': zero,
                    'QT_REITERACAO': 0, 'VR_REITERACAO': zero, 'VR_TOTAL': zero,
                }
                por_grupo[m['GRUPO']] = g
            g['QT_OCORRENCIAS'] += 1
            g['QT_ADVERTENCIA'] += adv
            g['QT_REINCIDENCIA'] += reinc
            g['VR_REINCIDENCIA'] += vr_reinc
            g['QT_REITERACAO'] += reit
            g['VR_REITERACAO'] += vr_reit
            g['VR_TOTAL'] += m['VR_TOTAL']

            # Totais da apuração
            totais['qt_ocorrencias'] += 1
            totais['qt_advertencia'] += adv
            totais['qt_reincidencia'] += reinc
            totais['qt_reiteracao'] += reit
            totais['vr_reincidencia'] += vr_reinc
            totais['vr_reiteracao'] += vr_reit
            totais['vr_total'] += m['VR_TOTAL']

        return {
            'registros': registros,
            'por_grupo': list(por_grupo.values()),
            'totais': totais,
            'parametros': {
                'valor_base': base,
                'dt_tarifa': tarifa['dt_apuracao'],
                'perc_reincidencia': perc_reinc,
                'valor_reincidencia': valor_reinc,
                'perc_reiteracao': perc_reit,
                'valor_reiteracao': valor_reit,
            },
        }

    # ==================================================================
    # TARIFA DO PRESTADOR (TB061)
    # ==================================================================

    @staticmethod
    def obter_tarifa_vigente(dt_apuracao):
        """
        Tarifa da TB061 para a data de apuração. Se não houver linha exatamente
        nessa data, devolve a última tarifa anterior (a rotina repete a última).
        Retorna {'dt_apuracao': date, 'vr_tarifa': Decimal} ou None.
        """
        sql = text("""
            SELECT TOP 1 DT_APURACAO, VR_TARIFA
            FROM BDDASHBOARDBI.BDG.MOV_TB061_TARIFA_PRESTADOR
            WHERE DT_APURACAO <= :dt AND VR_TARIFA IS NOT NULL
            ORDER BY DT_APURACAO DESC
        """)
        row = db.session.execute(sql, {'dt': dt_apuracao}).fetchone()
        if not row:
            return None
        return {'dt_apuracao': row.DT_APURACAO, 'vr_tarifa': row.VR_TARIFA}

    @staticmethod
    def obter_ultima_tarifa():
        """
        Última posição da TB061 (maior DT_APURACAO).
        Retorna {'dt_apuracao': date, 'vr_tarifa': Decimal} ou None.
        """
        sql = text("""
            SELECT TOP 1 DT_APURACAO, VR_TARIFA
            FROM BDDASHBOARDBI.BDG.MOV_TB061_TARIFA_PRESTADOR
            ORDER BY DT_APURACAO DESC
        """)
        row = db.session.execute(sql).fetchone()
        if not row:
            return None
        return {'dt_apuracao': row.DT_APURACAO, 'vr_tarifa': row.VR_TARIFA}

    @staticmethod
    def alterar_ultima_tarifa(dt_apuracao, novo_valor):
        """
        Altera o VR_TARIFA da ÚLTIMA data da TB061.
        dt_apuracao = data que o usuário viu no modal; só grava se ela ainda for
        a última da tabela (evita alterar a linha errada se a rotina tiver
        inserido um mês novo enquanto o modal estava aberto).
        Retorna (ok, mensagem, info) — info: {'dt_apuracao', 'vr_anterior', 'vr_novo'}.
        """
        ultima = AnsApuracao.obter_ultima_tarifa()
        if ultima is None:
            return False, 'Não há tarifa cadastrada na TB061.', None

        dt_ultima = str(ultima['dt_apuracao'])[:10]
        if str(dt_apuracao)[:10] != dt_ultima:
            return False, ('A última data da tarifa mudou para {}. Feche e abra novamente '
                           'o "Alterar Tarifa".'.format(dt_ultima)), None

        sql = text("""
            UPDATE BDDASHBOARDBI.BDG.MOV_TB061_TARIFA_PRESTADOR
            SET VR_TARIFA = :vr
            WHERE DT_APURACAO = :dt
        """)
        db.session.execute(sql, {'vr': novo_valor, 'dt': dt_ultima})
        db.session.commit()

        info = {'dt_apuracao': dt_ultima, 'vr_anterior': ultima['vr_tarifa'], 'vr_novo': novo_valor}
        return True, 'Tarifa da data {} alterada com sucesso.'.format(dt_ultima), info

    # ==================================================================
    # HISTÓRICO DE OCORRÊNCIAS (todas as datas de apuração)
    # ==================================================================
    # Legenda única do histórico — o front só desenha o que vem daqui.
    # Ordem = prioridade da classificação do mês (a mais grave primeiro).
    LEGENDA_HISTORICO = [
        {'codigo': 'REITERADA', 'rotulo': 'Reiterada', 'sigla': 'RT', 'cor': '#5a2d82', 'icone': 'fa-exclamation-circle',
         'descricao': 'Reiteração = Sim no mês (já era reincidente na apuração anterior).'},
        {'codigo': 'REINCIDIDA', 'rotulo': 'Reincidida', 'sigla': 'RC', 'cor': '#fd7e14', 'icone': 'fa-redo',
         'descricao': 'Reincidência = Sim no mês (já era advertida na apuração anterior).'},
        {'codigo': 'ADVERTIDA', 'rotulo': 'Advertida', 'sigla': 'AD', 'cor': '#e74a3b', 'icone': 'fa-gavel',
         'descricao': 'Advertência = Sim no mês.'},
        {'codigo': 'RETIRADA', 'rotulo': 'Retirada manualmente', 'sigla': 'RM', 'cor': '#36b9cc', 'icone': 'fa-undo',
         'descricao': 'A penalidade do mês foi alterada para Não manualmente (com retorno da prestadora). '
                      'Não conta como penalidade no mês.'},
        {'codigo': 'SEM_PENALIDADE', 'rotulo': 'Sem penalidade', 'sigla': 'OK', 'cor': '#1cc88a', 'icone': 'fa-shield-alt',
         'descricao': 'Analisada no mês, sem advertência, reincidência ou reiteração do próprio mês '
                      '(marcações herdadas de meses anteriores não contam).'},
        {'codigo': 'PENDENTE', 'rotulo': 'Pendente de análise', 'sigla': 'PD', 'cor': '#f6c23e', 'icone': 'fa-hourglass-half',
         'descricao': 'Estava na apuração, mas a justificativa ainda não foi analisada (Sim/Não).'},
        {'codigo': 'NADA_NO_MES', 'rotulo': 'Nada no mês', 'sigla': '—', 'cor': '#d1d3e2', 'icone': 'fa-minus',
         'descricao': 'A ocorrência não constava nesta data de apuração.'},
    ]

    @staticmethod
    def _sim_nao(valor):
        """1 -> 'Sim', 0 -> 'Não', None -> None (front mostra '—')."""
        if valor is None:
            return None
        return 'Sim' if int(valor) == 1 else 'Não'

    @staticmethod
    def _data_iso_br(valor):
        """Converte date/datetime/str em (iso, br). Vazio -> (None, None)."""
        if valor is None or valor == '':
            return None, None
        if isinstance(valor, datetime):
            valor = valor.date()
        if isinstance(valor, date):
            return valor.strftime('%Y-%m-%d'), valor.strftime('%d/%m/%Y')
        txt = str(valor)[:10]
        try:
            d = datetime.strptime(txt, '%Y-%m-%d').date()
            return d.strftime('%Y-%m-%d'), d.strftime('%d/%m/%Y')
        except ValueError:
            return txt, txt

    @staticmethod
    def _so_data(valor):
        """date/datetime/str -> date (ou None)."""
        if valor is None or valor == '':
            return None
        if isinstance(valor, datetime):
            return valor.date()
        if isinstance(valor, date):
            return valor
        try:
            return datetime.strptime(str(valor)[:10], '%Y-%m-%d').date()
        except ValueError:
            return None

    @staticmethod
    def _penalidade_do_mes(flag, dt_penalidade, dt_apuracao):
        """
        A penalidade é DO MÊS quando o campo = 1 e a data dela não é anterior à apuração.
        Campo = 1 com data de mês anterior é marcação HERDADA: veio copiada pela rotina
        (ocorrências do mês anterior) ou a rotina regravou a data da 1ª vez. Ela não é
        uma penalidade nova naquele mês (mesmo critério da TB049 e do Excel).
        """
        if flag != 1:
            return False
        dp = AnsApuracao._so_data(dt_penalidade)
        da = AnsApuracao._so_data(dt_apuracao)
        if dp is None or da is None:
            return True
        return dp >= da

    @staticmethod
    def _penalidades_mes(row):
        """Retorna (adv_do_mes, reinc_do_mes, reit_do_mes) já descontando as herdadas."""
        return (
            AnsApuracao._penalidade_do_mes(row.ADVERTENCIA, row.DT_ADVERTENCIA, row.DT_APURACAO),
            AnsApuracao._penalidade_do_mes(row.REINCIDENCIA, row.DT_REINCIDENCIA, row.DT_APURACAO),
            AnsApuracao._penalidade_do_mes(row.REITERACAO, row.DT_REITERACAO, row.DT_APURACAO),
        )

    @staticmethod
    def _classificar_mes(row):
        """
        Status do mês. Prioridade:
          Reiteração do mês > Reincidência do mês > Advertência do mês >
          Retirada manualmente (tem retorno da prestadora na TB048) >
          Pendente (JUST_ACEITA vazio) > Sem penalidade.
        Marcações herdadas de meses anteriores NÃO classificam o mês: se a última
        reiteração foi retirada manualmente, a reincidência antiga que ficou na linha
        não faz o mês aparecer como "Reincidida".
        """
        adv, reinc, reit = AnsApuracao._penalidades_mes(row)
        if reit:
            return 'REITERADA'
        if reinc:
            return 'REINCIDIDA'
        if adv:
            return 'ADVERTIDA'
        if getattr(row, 'TEM_TB048', 0) == 1:
            return 'RETIRADA'
        if row.JUST_ACEITA is None:
            return 'PENDENTE'
        return 'SEM_PENALIDADE'

    @staticmethod
    def obter_historico_ocorrencias(nrs_ocorrencia):
        """
        Histórico completo (todas as DT_APURACAO da TB045) de uma ou mais ocorrências.

        Lógica:
          1) Busca na TB045 todas as linhas das ocorrências pedidas (qualquer data),
             com o grupo/item (TB043) e o retorno da prestadora (TB048) daquele mês.
          2) Monta a "régua" de datas: todas as DT_APURACAO existentes na TB045 desde
             a primeira aparição de qualquer ocorrência pedida até a apuração mais recente.
             Assim, os meses em que a ocorrência não constava aparecem como "Nada no mês".
          3) Cada mês recebe um status pela prioridade Reiteração > Reincidência >
             Advertência > Pendente > Sem penalidade.

        Retorna dict com: datas, ocorrencias (achadas), nao_encontradas, legenda, totais.
        """
        from sqlalchemy import bindparam

        nrs = []
        for n in nrs_ocorrencia or []:
            try:
                v = int(n)
            except (TypeError, ValueError):
                continue
            if v not in nrs:
                nrs.append(v)

        resultado = {'datas': [], 'ocorrencias': [], 'nao_encontradas': [],
                     'legenda': AnsApuracao.LEGENDA_HISTORICO, 'totais': {}}
        if not nrs:
            return resultado

        sql = text("""
            SELECT A.DT_APURACAO, A.nrOcorrencia, A.GRUPO,
                   A.DT_ABERTURA, A.DT_JUSTIFICATIVA, A.DT_EFETIVACAO, A.DT_DEFERIDO,
                   A.JUST_ACEITA, A.QTDE_DIAS, A.NO_PRAZO,
                   A.ADVERTENCIA, A.DT_ADVERTENCIA,
                   A.REINCIDENCIA, A.DT_REINCIDENCIA,
                   A.REITERACAO, A.DT_REITERACAO,
                   A.DSC_JUSTIFICATIVA,
                   B.itemServico, B.PRAZO, B.NO_GRUPO,
                   J.RETORNO_PREST, J.MANIFESTACAO_GEADI,
                   CASE WHEN J.nrOcorrencia IS NULL THEN 0 ELSE 1 END AS TEM_TB048
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO A
            OUTER APPLY (SELECT TOP 1 X.itemServico, X.PRAZO, X.NO_GRUPO
                         FROM BDDASHBOARDBI.BDG.MOV_TB043_ANS_ITENS_FATURAMENTO X
                         WHERE X.GRUPO = A.GRUPO) B
            LEFT JOIN BDDASHBOARDBI.BDG.MOV_TB048_ANS_JUSTIFICATIVA_PRESTADORA J
                   ON J.DT_APURACAO = A.DT_APURACAO AND J.nrOcorrencia = A.nrOcorrencia
            WHERE A.nrOcorrencia IN :nrs
            ORDER BY A.nrOcorrencia, A.DT_APURACAO
        """).bindparams(bindparam('nrs', expanding=True))
        linhas = db.session.execute(sql, {'nrs': nrs}).fetchall()

        if not linhas:
            resultado['nao_encontradas'] = nrs
            return resultado

        # Régua de datas: da primeira aparição até a apuração mais recente da TB045
        primeira = min(r.DT_APURACAO for r in linhas)
        sql_datas = text("""
            SELECT DISTINCT DT_APURACAO
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO
            WHERE DT_APURACAO >= :dt
            ORDER BY DT_APURACAO
        """)
        datas_iso = []
        for r in db.session.execute(sql_datas, {'dt': primeira}).fetchall():
            iso, br = AnsApuracao._data_iso_br(r.DT_APURACAO)
            datas_iso.append(iso)
            resultado['datas'].append({'iso': iso, 'br': br, 'mes': br[3:] if br else ''})

        # Se houver duas apurações no mesmo mês, o cabeçalho mostra a data completa
        meses_vistos = [d['mes'] for d in resultado['datas']]
        for d in resultado['datas']:
            if meses_vistos.count(d['mes']) > 1:
                d['mes'] = d['br']

        # Agrupa as linhas por ocorrência
        por_nr = {}
        for r in linhas:
            por_nr.setdefault(int(r.nrOcorrencia), []).append(r)

        totais_gerais = dict((item['codigo'], 0) for item in AnsApuracao.LEGENDA_HISTORICO)

        for nr in nrs:
            if nr not in por_nr:
                resultado['nao_encontradas'].append(nr)
                continue

            meses_por_data = {}
            for r in por_nr[nr]:
                iso, br = AnsApuracao._data_iso_br(r.DT_APURACAO)
                status = AnsApuracao._classificar_mes(r)
                adv_mes, reinc_mes, reit_mes = AnsApuracao._penalidades_mes(r)
                meses_por_data[iso] = {
                    'dt_apuracao': iso,
                    'dt_apuracao_br': br,
                    'status': status,
                    'grupo': r.GRUPO,
                    'no_grupo': r.NO_GRUPO or r.itemServico or 'Grupo {}'.format(r.GRUPO),
                    'prazo': r.PRAZO,
                    'qtde_dias': r.QTDE_DIAS,
                    'dt_abertura': AnsApuracao._data_iso_br(r.DT_ABERTURA)[1],
                    'dt_justificativa': AnsApuracao._data_iso_br(r.DT_JUSTIFICATIVA)[1],
                    'dt_efetivacao': AnsApuracao._data_iso_br(r.DT_EFETIVACAO)[1],
                    'dt_deferido': AnsApuracao._data_iso_br(r.DT_DEFERIDO)[1],
                    'just_aceita': AnsApuracao._sim_nao(r.JUST_ACEITA),
                    'no_prazo': AnsApuracao._sim_nao(r.NO_PRAZO),
                    'advertencia': AnsApuracao._sim_nao(r.ADVERTENCIA),
                    'dt_advertencia': AnsApuracao._data_iso_br(r.DT_ADVERTENCIA)[1],
                    'reincidencia': AnsApuracao._sim_nao(r.REINCIDENCIA),
                    'dt_reincidencia': AnsApuracao._data_iso_br(r.DT_REINCIDENCIA)[1],
                    'reiteracao': AnsApuracao._sim_nao(r.REITERACAO),
                    'dt_reiteracao': AnsApuracao._data_iso_br(r.DT_REITERACAO)[1],
                    # Campo = 1 mas de mês anterior (não conta no mês) — o front mostra "Sim (herdada)"
                    'adv_herdada': r.ADVERTENCIA == 1 and not adv_mes,
                    'reinc_herdada': r.REINCIDENCIA == 1 and not reinc_mes,
                    'reit_herdada': r.REITERACAO == 1 and not reit_mes,
                    'retirada_manual': getattr(r, 'TEM_TB048', 0) == 1,
                    'dsc_justificativa': r.DSC_JUSTIFICATIVA,
                    'retorno_prestadora': r.RETORNO_PREST,
                    'manifestacao_geadi': r.MANIFESTACAO_GEADI,
                }

            # Linha do tempo completa, preenchendo os meses sem registro
            meses = []
            contagem = dict((item['codigo'], 0) for item in AnsApuracao.LEGENDA_HISTORICO)
            for d in resultado['datas']:
                mes = meses_por_data.get(d['iso'])
                if mes is None:
                    mes = {'dt_apuracao': d['iso'], 'dt_apuracao_br': d['br'], 'status': 'NADA_NO_MES'}
                meses.append(mes)
                contagem[mes['status']] += 1
                totais_gerais[mes['status']] += 1

            presentes = [m for m in meses if m['status'] != 'NADA_NO_MES']
            ultimo = presentes[-1] if presentes else None
            resultado['ocorrencias'].append({
                'nr_ocorrencia': nr,
                'meses': meses,
                'contagem': contagem,
                'qtde_apuracoes': len(presentes),
                'primeira_apuracao': presentes[0]['dt_apuracao_br'] if presentes else None,
                'ultima_apuracao': ultimo['dt_apuracao_br'] if ultimo else None,
                'status_atual': ultimo['status'] if ultimo else None,
                'grupo_atual': ultimo['grupo'] if ultimo else None,
                'no_grupo_atual': ultimo['no_grupo'] if ultimo else None,
            })

        resultado['totais'] = totais_gerais
        return resultado


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