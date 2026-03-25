"""
Model: app/models/ans_apuracao.py
Módulo ANS Glosas - Apuração de ocorrências ANS
Tabelas: BDG.MOV_TB045_ANS_APURACAO e BDG.MOV_TB043_ANS_ITENS_FATURAMENTO

CORREÇÃO: Queries usam OUTER APPLY TOP 1 para evitar duplicatas
quando um GRUPO tem múltiplos itemServico na TB043.
"""

from app import db
from sqlalchemy import Column, Integer, String, Date, DateTime, text
from datetime import datetime, date


class AnsApuracao(db.Model):
    """Model para a tabela de apuração ANS"""
    __tablename__ = 'MOV_TB045_ANS_APURACAO'
    __table_args__ = {'schema': 'BDG'}

    # Chaves primárias compostas
    DT_APURACAO = Column(Date, primary_key=True, nullable=False)
    nrOcorrencia = Column(Integer, primary_key=True, nullable=False)

    # Demais colunas
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
    def listar_por_grupo(dt_apuracao='2025-12-31'):
        """
        Busca ocorrências agrupadas por GRUPO para a data de apuração informada.
        Retorna um dicionário {grupo: [lista de ocorrências]}
        Apenas ocorrências que ainda NÃO foram analisadas (JUST_ACEITA IS NULL)

        CORREÇÃO: Usa OUTER APPLY com TOP 1 para pegar apenas 1 registro
        da TB043 por GRUPO, evitando duplicatas quando existem múltiplos
        itemServico para o mesmo GRUPO.
        """
        sql = text("""
            SELECT 
                A.DT_APURACAO,
                A.nrOcorrencia,
                A.GRUPO,
                A.DT_ABERTURA,
                A.DT_ANDAMENTO,
                A.DT_JUSTIFICATIVA,
                A.DSC_JUSTIFICATIVA,
                A.JUST_ACEITA,
                A.QTDE_DIAS,
                A.NO_PRAZO,
                B.itemServico,
                B.PRAZO
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO A
            OUTER APPLY (
                SELECT TOP 1 X.itemServico, X.PRAZO
                FROM BDDASHBOARDBI.BDG.MOV_TB043_ANS_ITENS_FATURAMENTO X
                WHERE X.GRUPO = A.GRUPO
            ) B
            WHERE A.DT_APURACAO = :dt_apuracao
              AND A.JUST_ACEITA IS NULL
            ORDER BY A.GRUPO, A.nrOcorrencia
        """)

        resultado = db.session.execute(sql, {'dt_apuracao': dt_apuracao}).fetchall()

        # Agrupar por GRUPO
        ocorrencias_por_grupo = {}
        for row in resultado:
            grupo = row.GRUPO
            if grupo not in ocorrencias_por_grupo:
                ocorrencias_por_grupo[grupo] = []
            ocorrencias_por_grupo[grupo].append(row)

        return ocorrencias_por_grupo

    @staticmethod
    def listar_analisadas(dt_apuracao='2025-12-31'):
        """
        Busca ocorrências já analisadas (JUST_ACEITA IS NOT NULL)

        CORREÇÃO: Usa OUTER APPLY com TOP 1 para evitar duplicatas.
        """
        sql = text("""
            SELECT 
                A.DT_APURACAO,
                A.nrOcorrencia,
                A.GRUPO,
                A.DT_ABERTURA,
                A.DT_ANDAMENTO,
                A.DT_JUSTIFICATIVA,
                A.DSC_JUSTIFICATIVA,
                A.JUST_ACEITA,
                A.QTDE_DIAS,
                A.NO_PRAZO,
                B.itemServico,
                B.PRAZO
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO A
            OUTER APPLY (
                SELECT TOP 1 X.itemServico, X.PRAZO
                FROM BDDASHBOARDBI.BDG.MOV_TB043_ANS_ITENS_FATURAMENTO X
                WHERE X.GRUPO = A.GRUPO
            ) B
            WHERE A.DT_APURACAO = :dt_apuracao
              AND A.JUST_ACEITA IS NOT NULL
            ORDER BY A.GRUPO, A.nrOcorrencia
        """)

        resultado = db.session.execute(sql, {'dt_apuracao': dt_apuracao}).fetchall()
        return resultado

    @staticmethod
    def buscar_ocorrencia(dt_apuracao, nr_ocorrencia):
        """
        Busca uma ocorrência específica pelo DT_APURACAO e nrOcorrencia

        CORREÇÃO: Usa OUTER APPLY com TOP 1 para evitar duplicatas.
        """
        sql = text("""
            SELECT 
                A.DT_APURACAO,
                A.nrOcorrencia,
                A.GRUPO,
                A.DT_ABERTURA,
                A.DT_ANDAMENTO,
                A.DT_JUSTIFICATIVA,
                A.DSC_JUSTIFICATIVA,
                A.JUST_ACEITA,
                A.QTDE_DIAS,
                A.NO_PRAZO,
                B.itemServico,
                B.PRAZO
            FROM BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO A
            OUTER APPLY (
                SELECT TOP 1 X.itemServico, X.PRAZO
                FROM BDDASHBOARDBI.BDG.MOV_TB043_ANS_ITENS_FATURAMENTO X
                WHERE X.GRUPO = A.GRUPO
            ) B
            WHERE A.DT_APURACAO = :dt_apuracao
              AND A.nrOcorrencia = :nr_ocorrencia
        """)

        resultado = db.session.execute(sql, {
            'dt_apuracao': dt_apuracao,
            'nr_ocorrencia': nr_ocorrencia
        }).fetchone()

        return resultado

    @staticmethod
    def salvar_analise(dt_apuracao, nr_ocorrencia, just_aceita):
        """
        Salva a análise de uma ocorrência:
        1. Calcula QTDE_DIAS com base em JUST_ACEITA
        2. Compara com PRAZO da TB043 para definir NO_PRAZO
        3. Atualiza o registro na TB045

        Lógica de cálculo:
        - Se JUST_ACEITA = 1 (Sim): QTDE_DIAS = dias de DT_ABERTURA até DT_JUSTIFICATIVA
          (contando DT_ABERTURA como dia 1)
        - Se JUST_ACEITA = 0 (Não): QTDE_DIAS = dias de DT_ABERTURA até DT_APURACAO
          (contando DT_ABERTURA como dia 1)

        Lógica NO_PRAZO:
        - Cruza com TB043 pelo GRUPO
        - Se QTDE_DIAS > PRAZO → NO_PRAZO = 0 (fora do prazo)
        - Se QTDE_DIAS <= PRAZO → NO_PRAZO = 1 (dentro do prazo)
        """
        # Buscar dados da ocorrência
        ocorrencia = AnsApuracao.buscar_ocorrencia(dt_apuracao, nr_ocorrencia)
        if not ocorrencia:
            return False, 'Ocorrência não encontrada.'

        # Calcular QTDE_DIAS
        dt_abertura = ocorrencia.DT_ABERTURA
        if not dt_abertura:
            return False, 'Data de abertura não encontrada para esta ocorrência.'

        if just_aceita == 1:
            # Justificativa aceita: contar de DT_ABERTURA até DT_JUSTIFICATIVA
            dt_fim = ocorrencia.DT_JUSTIFICATIVA
            if not dt_fim:
                return False, 'Data de justificativa não encontrada para esta ocorrência.'
        else:
            # Justificativa não aceita: contar de DT_ABERTURA até DT_APURACAO
            dt_fim = ocorrencia.DT_APURACAO
            if not dt_fim:
                return False, 'Data de apuração não encontrada para esta ocorrência.'

        # Converter para date se necessário
        if isinstance(dt_abertura, datetime):
            dt_abertura = dt_abertura.date()
        if isinstance(dt_fim, datetime):
            dt_fim = dt_fim.date()

        # Calcular dias (DT_ABERTURA conta como dia 1)
        qtde_dias = (dt_fim - dt_abertura).days + 1

        # Buscar PRAZO da TB043 pelo GRUPO
        prazo = ocorrencia.PRAZO

        # Calcular NO_PRAZO
        if prazo is not None:
            no_prazo = 1 if qtde_dias <= prazo else 0
        else:
            no_prazo = None  # Sem prazo definido na TB043

        # Atualizar no banco
        sql_update = text("""
            UPDATE BDDASHBOARDBI.BDG.MOV_TB045_ANS_APURACAO
            SET JUST_ACEITA = :just_aceita,
                QTDE_DIAS = :qtde_dias,
                NO_PRAZO = :no_prazo
            WHERE DT_APURACAO = :dt_apuracao
              AND nrOcorrencia = :nr_ocorrencia
        """)

        db.session.execute(sql_update, {
            'just_aceita': just_aceita,
            'qtde_dias': qtde_dias,
            'no_prazo': no_prazo,
            'dt_apuracao': dt_apuracao,
            'nr_ocorrencia': nr_ocorrencia
        })
        db.session.commit()

        return True, {
            'qtde_dias': qtde_dias,
            'no_prazo': no_prazo,
            'prazo': prazo
        }

    @staticmethod
    def salvar_analise_lote(dt_apuracao, analises):
        """
        Salva múltiplas análises de uma vez (lote).
        analises = lista de dicts: [{'nr_ocorrencia': X, 'just_aceita': 0 ou 1}, ...]

        Retorna (sucesso_count, erro_count, erros)
        """
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

            sucesso, resultado = AnsApuracao.salvar_analise(
                dt_apuracao, nr_ocorrencia, just_aceita
            )

            if sucesso:
                sucesso_count += 1
            else:
                erro_count += 1
                erros.append(f'Ocorrência {nr_ocorrencia}: {resultado}')

        return sucesso_count, erro_count, erros

    @staticmethod
    def obter_estatisticas(dt_apuracao='2025-12-31'):
        """
        Retorna estatísticas da apuração ANS
        """
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

        resultado = db.session.execute(sql, {'dt_apuracao': dt_apuracao}).fetchone()
        return resultado


class AnsItensFaturamento(db.Model):
    """Model para a tabela auxiliar de itens de faturamento ANS"""
    __tablename__ = 'MOV_TB043_ANS_ITENS_FATURAMENTO'
    __table_args__ = {'schema': 'BDG'}

    itemServico = Column(String(150), primary_key=True, nullable=False)
    GRUPO = Column('GRUPO', db.SmallInteger, nullable=False)
    PRAZO = Column('PRAZO', db.SmallInteger, nullable=False)

    def __repr__(self):
        return f'<AnsItensFaturamento {self.itemServico} - Grupo {self.GRUPO}>'

    @staticmethod
    def listar_todos():
        """Retorna todos os itens de faturamento"""
        return AnsItensFaturamento.query.order_by(AnsItensFaturamento.GRUPO).all()

    @staticmethod
    def buscar_por_grupo(grupo):
        """Busca item de faturamento pelo grupo"""
        return AnsItensFaturamento.query.filter_by(GRUPO=grupo).first()