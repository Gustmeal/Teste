# app/models/teletrabalho.py
from datetime import datetime, timedelta
from app import db
import math
from sqlalchemy import func, text


class ConfigAreaTeletrabalho(db.Model):
    """Configuração personalizada da área"""
    __tablename__ = 'APK_TB007_CONFIG_AREA_TELETRABALHO'
    __table_args__ = {'schema': 'BDG'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    AREA = db.Column(db.String(50), nullable=False)
    TIPO_AREA = db.Column(db.String(20), nullable=False)
    QTD_TOTAL_PESSOAS = db.Column(db.Integer, nullable=False)
    PERCENTUAL_LIMITE = db.Column(db.Numeric(5, 2), default=30.00)
    GESTOR_ID = db.Column(db.Integer, db.ForeignKey('BDG.APK_TB002_USUARIOS.ID'))
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    gestor = db.relationship('Usuario', foreign_keys=[GESTOR_ID])

    @property
    def limite_pessoas_dia(self):
        """Calcula limite (arredonda para cima)"""
        limite = (self.QTD_TOTAL_PESSOAS * float(self.PERCENTUAL_LIMITE)) / 100.0
        return math.ceil(limite)

    def __repr__(self):
        return f'<ConfigAreaTeletrabalho {self.AREA} - {self.QTD_TOTAL_PESSOAS} pessoas>'

class Teletrabalho(db.Model):
    """Registro de teletrabalho"""
    __tablename__ = 'APK_TB006_TELETRABALHO'
    __table_args__ = {'schema': 'BDG'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    USUARIO_ID = db.Column(db.Integer, db.ForeignKey('BDG.APK_TB002_USUARIOS.ID'), nullable=False)
    DATA_TELETRABALHO = db.Column(db.Date, nullable=False)
    MES_REFERENCIA = db.Column(db.String(7), nullable=False)
    AREA = db.Column(db.String(50), nullable=False)
    TIPO_AREA = db.Column(db.String(20), nullable=False)
    STATUS = db.Column(db.String(20), default='APROVADO')
    TIPO_PERIODO = db.Column(db.String(30))
    OBSERVACAO = db.Column(db.Text)
    APROVADO_POR = db.Column(db.Integer, db.ForeignKey('BDG.APK_TB002_USUARIOS.ID'))
    APROVADO_EM = db.Column(db.DateTime)
    TIPO_MARCACAO = db.Column(db.String(20), default='TELETRABALHO')
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    usuario = db.relationship('Usuario', foreign_keys=[USUARIO_ID])
    aprovador = db.relationship('Usuario', foreign_keys=[APROVADO_POR])

    # ... resto dos métodos permanecem iguais ...

    def __repr__(self):
        return f'<Teletrabalho {self.ID} - Usuario {self.USUARIO_ID} - {self.DATA_TELETRABALHO}>'

    @staticmethod
    def contar_dias_mes(usuario_id, mes_referencia):
        """Conta quantos dias de teletrabalho o usuário já tem no mês"""
        return Teletrabalho.query.filter(
            Teletrabalho.USUARIO_ID == usuario_id,
            Teletrabalho.MES_REFERENCIA == mes_referencia,
            Teletrabalho.STATUS == 'APROVADO',
            Teletrabalho.DELETED_AT.is_(None)
        ).count()

    @staticmethod
    def contar_pessoas_dia(data, area, tipo_area):
        """Conta quantas pessoas já estão em teletrabalho naquele dia naquela área"""
        return Teletrabalho.query.filter(
            Teletrabalho.DATA_TELETRABALHO == data,
            Teletrabalho.AREA == area,
            Teletrabalho.TIPO_AREA == tipo_area,
            Teletrabalho.STATUS == 'APROVADO',
            Teletrabalho.DELETED_AT.is_(None)
        ).count()

    @staticmethod
    def validar_dias_alternados(usuario_id, data_nova, mes_referencia, excluir_id=None):
        """
        Valida intervalo mínimo entre dias alternados

        Args:
            usuario_id: ID do usuário
            data_nova: Data que será marcada
            mes_referencia: Mês no formato YYYYMM
            excluir_id: ID do teletrabalho a excluir da validação (usado em edição)

        Returns:
            tuple: (bool, str) - (válido, mensagem)
        """
        from datetime import timedelta

        query = Teletrabalho.query.filter(
            Teletrabalho.USUARIO_ID == usuario_id,
            Teletrabalho.MES_REFERENCIA == mes_referencia,
            Teletrabalho.TIPO_PERIODO == 'ALTERNADO',
            Teletrabalho.STATUS == 'APROVADO',
            Teletrabalho.DELETED_AT.is_(None)
        )

        # Se for edição, excluir o registro atual da validação
        if excluir_id:
            query = query.filter(Teletrabalho.ID != excluir_id)

        dias_marcados = [t.DATA_TELETRABALHO for t in query.all()]

        # Verificar intervalo mínimo de 1 dia útil entre cada data
        for dia_marcado in dias_marcados:
            # Calcular diferença em dias
            diferenca_dias = abs((data_nova - dia_marcado).days)

            # Contar dias úteis entre as datas
            data_inicio = min(data_nova, dia_marcado)
            data_fim = max(data_nova, dia_marcado)
            dias_uteis_entre = 0

            data_check = data_inicio + timedelta(days=1)
            while data_check < data_fim:
                if Feriado.eh_dia_util(data_check):
                    dias_uteis_entre += 1
                data_check += timedelta(days=1)

            # Validar intervalo mínimo
            if dias_uteis_entre < 1:
                data_marcada_str = dia_marcado.strftime("%d/%m/%Y")
                data_nova_str = data_nova.strftime("%d/%m/%Y")
                return False, f'Precisa haver pelo menos 1 dia útil entre {data_marcada_str} e {data_nova_str}'

        return True, 'OK'

    @staticmethod
    def validar_cinco_dias_corridos(usuario_id, datas, mes_referencia):
        """
        REGRA: 5 dias corridos = 5 dias consecutivos (Ex: Seg, Ter, Qua, Qui, Sex)
        """
        if len(datas) != 5:
            return False, "Devem ser exatamente 5 dias corridos"

        datas_ordenadas = sorted(datas)

        # Verificar se são consecutivas
        for i in range(len(datas_ordenadas) - 1):
            diferenca = (datas_ordenadas[i + 1] - datas_ordenadas[i]).days
            if diferenca != 1:
                return False, "Os 5 dias devem ser corridos (consecutivos sem intervalo)"

        return True, ""


class Feriado(db.Model):
    """Acessa tabela de feriados existente"""
    __tablename__ = 'AUX_TB003_FERIADOS'
    __table_args__ = {'schema': 'BDG', 'extend_existing': True}

    PK_FERIADO = db.Column(db.Integer, primary_key=True)
    ANO = db.Column(db.SmallInteger, nullable=False)
    MES = db.Column(db.SmallInteger, nullable=False)
    DIA = db.Column(db.SmallInteger, nullable=False)
    TP_FERIADO = db.Column(db.String(1))
    DS_FERIADO = db.Column(db.String(100), nullable=False)
    SG_UF = db.Column(db.String(2))

    def __repr__(self):
        return f'<Feriado {self.ANO}/{self.MES}/{self.DIA} - {self.DS_FERIADO}>'

    @staticmethod
    def eh_feriado(data):
        """
        Verifica se é feriado (DF ou nacional - SG_UF = NULL)
        """
        return Feriado.query.filter(
            Feriado.ANO == data.year,
            Feriado.MES == data.month,
            Feriado.DIA == data.day,
            db.or_(
                Feriado.SG_UF == 'DF',
                Feriado.SG_UF.is_(None),
                Feriado.SG_UF == ''
            )
        ).first() is not None

    @staticmethod
    def eh_dia_util(data):
        """
        REGRA: Dia útil = Segunda a Sexta, exceto feriados
        weekday(): 0=Seg, 1=Ter, 2=Qua, 3=Qui, 4=Sex, 5=Sáb, 6=Dom
        """
        dia_semana = data.weekday()

        # Sábado (5) ou Domingo (6) = NÃO ÚTIL
        if dia_semana >= 5:
            return False

        # Segunda a Sexta (0-4) MAS é feriado = NÃO ÚTIL
        if Feriado.eh_feriado(data):
            return False

        # Segunda a Sexta (0-4) e NÃO é feriado = ÚTIL
        return True

    @staticmethod
    def obter_feriado(data):
        """Retorna o feriado se existir"""
        return Feriado.query.filter(
            Feriado.ANO == data.year,
            Feriado.MES == data.month,
            Feriado.DIA == data.day,
            db.or_(
                Feriado.SG_UF == 'DF',
                Feriado.SG_UF.is_(None),
                Feriado.SG_UF == ''
            )
        ).first()

    @staticmethod
    def debug_dia(data):
        """
        Função de DEBUG para identificar problemas
        Retorna informações detalhadas sobre a data
        """
        dia_semana = data.weekday()
        dias_nome = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
        eh_feriado = Feriado.eh_feriado(data)
        feriado_obj = Feriado.obter_feriado(data)
        eh_util = Feriado.eh_dia_util(data)

        return {
            'data': data.strftime('%Y-%m-%d'),
            'dia_semana_num': dia_semana,
            'dia_semana_nome': dias_nome[dia_semana],
            'eh_feriado': eh_feriado,
            'feriado': feriado_obj.DS_FERIADO if feriado_obj else None,
            'eh_util': eh_util,
            'motivo_nao_util': 'Fim de semana' if dia_semana >= 5 else ('Feriado' if eh_feriado else None)
        }