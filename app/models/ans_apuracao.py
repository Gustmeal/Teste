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
    DT_ADVERTENCIA = Column(Integer, nullable=True)
    REINCIDENCIA = Column(Integer, nullable=True)
    DT_REINCIDENCIA = Column(Integer, nullable=True)
    REITERACAO = Column(Integer, nullable=True)
    DT_REITERACAO = Column(Integer, nullable=True)
    JUST_ACEITA = Column(Integer, nullable=True)
    DSC_JUSTIFICATIVA = Column(String(4000), nullable=True)

    def __repr__(self):
        return f'<AnsApuracao {self.DT_APURACAO}-{self.nrOcorrencia}>'

    @staticmethod
    def obter_datas_apuracao():
        sql = text("SELECT DISTINCT DT_APURACAO FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO ORDER BY DT_APURACAO DESC")
        return [row.DT_APURACAO for row in db.session.execute(sql).fetchall()]

    @staticmethod
    def obter_data_anterior(dt_apuracao):
        sql = text("SELECT TOP 1 DT_APURACAO FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO WHERE DT_APURACAO < :dt ORDER BY DT_APURACAO DESC")
        row = db.session.execute(sql, {'dt': dt_apuracao}).fetchone()
        return row.DT_APURACAO if row else None

    @staticmethod
    def _query_base(dt_apuracao, filtro_just_aceita):
        where_just = "AND A.JUST_ACEITA IS NULL" if filtro_just_aceita == 'NULL' else "AND A.JUST_ACEITA IS NOT NULL"
        sql = text(f"""
            SELECT A.DT_APURACAO, A.nrOcorrencia, A.GRUPO, A.DT_ABERTURA, A.DT_ANDAMENTO, A.DT_JUSTIFICATIVA,
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

    @staticmethod
    def calcular_resumo_advertencia(dt_apuracao, grupo):
        sql = text("""
            SELECT COUNT(*) AS total,
                SUM(CASE WHEN NO_PRAZO=0 AND JUST_ACEITA=0 THEN 1 ELSE 0 END) AS fora_prazo_sem_just,
                SUM(CASE WHEN NO_PRAZO=0 THEN 1 ELSE 0 END) AS total_fora_prazo,
                SUM(CASE WHEN NO_PRAZO=1 THEN 1 ELSE 0 END) AS total_dentro_prazo,
                SUM(CASE WHEN JUST_ACEITA=1 THEN 1 ELSE 0 END) AS total_just_aceitas,
                SUM(CASE WHEN JUST_ACEITA=0 THEN 1 ELSE 0 END) AS total_just_rejeitadas,
                SUM(CASE WHEN JUST_ACEITA IS NULL THEN 1 ELSE 0 END) AS pendentes,
                SUM(CASE WHEN ADVERTENCIA IS NOT NULL THEN 1 ELSE 0 END) AS adv_proc,
                SUM(CASE WHEN ADVERTENCIA=1 THEN 1 ELSE 0 END) AS adv_marc,
                SUM(CASE WHEN REINCIDENCIA IS NOT NULL THEN 1 ELSE 0 END) AS reinc_proc,
                SUM(CASE WHEN REINCIDENCIA=1 THEN 1 ELSE 0 END) AS reinc_marc,
                SUM(CASE WHEN REITERACAO IS NOT NULL THEN 1 ELSE 0 END) AS reit_proc,
                SUM(CASE WHEN REITERACAO=1 THEN 1 ELSE 0 END) AS reit_marc
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO WHERE DT_APURACAO=:dt AND GRUPO=:g
        """)
        r = db.session.execute(sql, {'dt': dt_apuracao, 'g': grupo}).fetchone()
        total = r.total or 0
        fpsj = r.fora_prazo_sem_just or 0
        pend = r.pendentes or 0
        pct = (fpsj / total * 100) if total > 0 else 0
        return {
            'total': total, 'pendentes': pend, 'todas_analisadas': pend == 0,
            'total_fora_prazo': r.total_fora_prazo or 0, 'total_dentro_prazo': r.total_dentro_prazo or 0,
            'total_just_aceitas': r.total_just_aceitas or 0, 'total_just_rejeitadas': r.total_just_rejeitadas or 0,
            'fora_prazo_sem_just': fpsj, 'percentual': round(pct, 2), 'sera_advertido': pct > 10,
            'advertencia_processada': (r.adv_proc or 0) == total and total > 0,
            'adv_marcadas': r.adv_marc or 0,
            'reincidencia_processada': (r.reinc_proc or 0) == total and total > 0,
            'reinc_marcadas': r.reinc_marc or 0,
            'reiteracao_processada': (r.reit_proc or 0) == total and total > 0,
            'reit_marcadas': r.reit_marc or 0,
        }

    @staticmethod
    def aplicar_advertencia_grupo(dt_apuracao, grupo):
        resumo = AnsApuracao.calcular_resumo_advertencia(dt_apuracao, grupo)
        if not resumo['todas_analisadas']:
            return False, 'Ainda existem ocorrências pendentes.'
        dt_int = int(str(dt_apuracao).replace('-', ''))
        if resumo['sera_advertido']:
            sql = text("""
                UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO
                SET ADVERTENCIA = CASE WHEN NO_PRAZO=0 AND JUST_ACEITA=0 THEN 1 ELSE 0 END,
                    DT_ADVERTENCIA = CASE WHEN NO_PRAZO=0 AND JUST_ACEITA=0 THEN :di ELSE NULL END
                WHERE DT_APURACAO=:dt AND GRUPO=:g
            """)
        else:
            sql = text("UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO SET ADVERTENCIA=0, DT_ADVERTENCIA=NULL WHERE DT_APURACAO=:dt AND GRUPO=:g")
        db.session.execute(sql, {'dt': dt_apuracao, 'g': grupo, 'di': dt_int if resumo['sera_advertido'] else None})
        db.session.commit()
        return True, resumo

    @staticmethod
    def aplicar_reincidencia_grupo(dt_apuracao, grupo):
        resumo = AnsApuracao.calcular_resumo_advertencia(dt_apuracao, grupo)
        if not resumo['advertencia_processada']:
            return False, 'Advertência ainda não processada.'
        dt_ant = AnsApuracao.obter_data_anterior(dt_apuracao)
        if not dt_ant:
            return False, 'Não há data de apuração anterior.'
        dt_int = int(str(dt_apuracao).replace('-', ''))
        if resumo['sera_advertido']:
            sql = text("""
                UPDATE A SET
                    A.REINCIDENCIA = CASE WHEN A.NO_PRAZO=0 AND A.JUST_ACEITA=0
                        AND EXISTS (SELECT 1 FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO P WHERE P.DT_APURACAO=:da AND P.GRUPO=:g AND P.nrOcorrencia=A.nrOcorrencia AND P.ADVERTENCIA=1)
                        THEN 1 ELSE 0 END,
                    A.DT_REINCIDENCIA = CASE WHEN A.NO_PRAZO=0 AND A.JUST_ACEITA=0
                        AND EXISTS (SELECT 1 FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO P WHERE P.DT_APURACAO=:da AND P.GRUPO=:g AND P.nrOcorrencia=A.nrOcorrencia AND P.ADVERTENCIA=1)
                        THEN :di ELSE NULL END
                FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO A WHERE A.DT_APURACAO=:dt AND A.GRUPO=:g
            """)
        else:
            sql = text("UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO SET REINCIDENCIA=0, DT_REINCIDENCIA=NULL WHERE DT_APURACAO=:dt AND GRUPO=:g")
        db.session.execute(sql, {'dt': dt_apuracao, 'da': dt_ant, 'g': grupo, 'di': dt_int})
        db.session.commit()
        cnt = db.session.execute(text("SELECT SUM(CASE WHEN REINCIDENCIA=1 THEN 1 ELSE 0 END) AS t FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO WHERE DT_APURACAO=:dt AND GRUPO=:g"), {'dt': dt_apuracao, 'g': grupo}).fetchone()
        resumo['reinc_marcadas_resultado'] = cnt.t or 0
        resumo['dt_anterior'] = str(dt_ant)
        return True, resumo

    @staticmethod
    def aplicar_reiteracao_grupo(dt_apuracao, grupo):
        resumo = AnsApuracao.calcular_resumo_advertencia(dt_apuracao, grupo)
        if not resumo['reincidencia_processada']:
            return False, 'Reincidência ainda não processada.'
        dt_ant = AnsApuracao.obter_data_anterior(dt_apuracao)
        if not dt_ant:
            return False, 'Não há data de apuração anterior.'
        dt_int = int(str(dt_apuracao).replace('-', ''))
        if resumo['sera_advertido']:
            sql = text("""
                UPDATE A SET
                    A.REITERACAO = CASE WHEN A.NO_PRAZO=0 AND A.JUST_ACEITA=0
                        AND EXISTS (SELECT 1 FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO P WHERE P.DT_APURACAO=:da AND P.GRUPO=:g AND P.nrOcorrencia=A.nrOcorrencia AND P.REINCIDENCIA=1)
                        THEN 1 ELSE 0 END,
                    A.DT_REITERACAO = CASE WHEN A.NO_PRAZO=0 AND A.JUST_ACEITA=0
                        AND EXISTS (SELECT 1 FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO P WHERE P.DT_APURACAO=:da AND P.GRUPO=:g AND P.nrOcorrencia=A.nrOcorrencia AND P.REINCIDENCIA=1)
                        THEN :di ELSE NULL END
                FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO A WHERE A.DT_APURACAO=:dt AND A.GRUPO=:g
            """)
        else:
            sql = text("UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO SET REITERACAO=0, DT_REITERACAO=NULL WHERE DT_APURACAO=:dt AND GRUPO=:g")
        db.session.execute(sql, {'dt': dt_apuracao, 'da': dt_ant, 'g': grupo, 'di': dt_int})
        db.session.commit()
        cnt = db.session.execute(text("SELECT SUM(CASE WHEN REITERACAO=1 THEN 1 ELSE 0 END) AS t FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO WHERE DT_APURACAO=:dt AND GRUPO=:g"), {'dt': dt_apuracao, 'g': grupo}).fetchone()
        resumo['reit_marcadas_resultado'] = cnt.t or 0
        resumo['dt_anterior'] = str(dt_ant)
        return True, resumo

    # ==================================================================
    # EDIÇÃO INDIVIDUAL DE CAMPO (Advertência/Reincidência/Reiteração)
    # ==================================================================

    @staticmethod
    def editar_campo_individual(dt_apuracao, nr_ocorrencia, campo, valor):
        """
        Edita individualmente ADVERTENCIA, REINCIDENCIA ou REITERACAO.
        campo: 'ADVERTENCIA', 'REINCIDENCIA' ou 'REITERACAO'
        valor: 0 ou 1
        Também atualiza o campo DT_ correspondente:
          - Se valor=1: DT_campo = DT_APURACAO como int YYYYMMDD
          - Se valor=0: DT_campo = NULL
        """
        campos_validos = {
            'ADVERTENCIA': 'DT_ADVERTENCIA',
            'REINCIDENCIA': 'DT_REINCIDENCIA',
            'REITERACAO': 'DT_REITERACAO'
        }
        if campo not in campos_validos:
            return False, f'Campo inválido: {campo}'

        dt_campo = campos_validos[campo]
        dt_int = int(str(dt_apuracao).replace('-', '')) if valor == 1 else None

        sql = text(f"""
            UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO
            SET [{campo}] = :valor, [{dt_campo}] = :dt_int
            WHERE DT_APURACAO = :dt_apuracao AND nrOcorrencia = :nr_ocorrencia
        """)
        db.session.execute(sql, {
            'valor': valor, 'dt_int': dt_int,
            'dt_apuracao': dt_apuracao, 'nr_ocorrencia': nr_ocorrencia
        })
        db.session.commit()
        return True, f'{campo} da ocorrência {nr_ocorrencia} alterada para {"Sim" if valor == 1 else "Não"}.'


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