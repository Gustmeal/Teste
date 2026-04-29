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
    def validar_tres_consecutivos(usuario_id, datas_novas, mes_referencia, excluir_ids=None):
        """
        Valida o modo TRES_CONSECUTIVOS.

        REGRA:
        - Os dias do mês precisam estar agrupados em até 2 blocos.
        - Cada bloco = exatamente 3 dias úteis consecutivos na MESMA semana ISO
          (ex.: SEG-TER-QUA, TER-QUA-QUI ou QUA-QUI-SEX).
        - Se houver 2 blocos, eles precisam estar em semanas ISO diferentes.
        - Total máximo no mês: 6 dias (2 blocos x 3).

        A validação considera o ESTADO FINAL do mês = dias já marcados + datas_novas.

        Args:
            usuario_id: ID do usuário
            datas_novas: lista de date() que o usuário quer marcar agora
            mes_referencia: mês no formato YYYYMM
            excluir_ids: IDs de registros de teletrabalho a ignorar (usado na edição)

        Returns:
            tuple (bool, str) - (válido, mensagem)
        """
        from datetime import timedelta

        if excluir_ids is None:
            excluir_ids = []

        # Buscar dias já marcados como TRES_CONSECUTIVOS no mês
        query = Teletrabalho.query.filter(
            Teletrabalho.USUARIO_ID == usuario_id,
            Teletrabalho.MES_REFERENCIA == mes_referencia,
            Teletrabalho.TIPO_PERIODO == 'TRES_CONSECUTIVOS',
            Teletrabalho.TIPO_MARCACAO == 'TELETRABALHO',
            Teletrabalho.STATUS == 'APROVADO',
            Teletrabalho.DELETED_AT.is_(None)
        )
        if excluir_ids:
            query = query.filter(~Teletrabalho.ID.in_(excluir_ids))

        dias_existentes = [t.DATA_TELETRABALHO for t in query.all()]

        # Unir com as datas novas e remover duplicatas
        todas_datas = sorted(set(dias_existentes + list(datas_novas)))

        if not todas_datas:
            return True, "OK"

        if len(todas_datas) > 6:
            return False, (
                f"Máximo de 6 dias no mês (2 blocos de 3 consecutivos). "
                f"Total informado: {len(todas_datas)}."
            )

        # Agrupar por semana ISO (ano, semana) — funciona corretamente em virada de ano
        blocos_por_semana = {}
        for d in todas_datas:
            ano_iso, semana_iso, _ = d.isocalendar()
            chave = (ano_iso, semana_iso)
            blocos_por_semana.setdefault(chave, []).append(d)

        # Máximo 2 semanas (= 2 blocos)
        if len(blocos_por_semana) > 2:
            return False, (
                f"Só são permitidos até 2 blocos de 3 dias consecutivos no mês "
                f"(em semanas diferentes). Foram detectadas {len(blocos_por_semana)} semanas."
            )

        # Cada bloco deve ter exatamente 3 dias úteis consecutivos
        for (ano_iso, semana_iso), dias in blocos_por_semana.items():
            if len(dias) > 3:
                return False, (
                    f"Cada bloco deve ter exatamente 3 dias úteis consecutivos. "
                    f"Semana {semana_iso}/{ano_iso}: {len(dias)} dia(s) marcado(s)."
                )

            dias_ord = sorted(dias)

            # Verificar que os 3 dias são úteis consecutivos (sem gap de dia útil)
            for i in range(len(dias_ord) - 1):
                d1 = dias_ord[i]
                d2 = dias_ord[i + 1]

                gap_uteis = 0
                d = d1 + timedelta(days=1)
                while d < d2:
                    if Feriado.eh_dia_util(d):
                        gap_uteis += 1
                    d += timedelta(days=1)

                if gap_uteis > 0:
                    return False, (
                        f"Os 3 dias da semana {semana_iso}/{ano_iso} precisam ser "
                        f"úteis consecutivos (sem intervalo entre eles)."
                    )

        return True, "OK"

    @staticmethod
    def validar_cinco_dias_corridos(usuario_id, datas, mes_referencia, excluir_ids=None):
        """
        REGRA: 5 dias corridos = 5 dias úteis consecutivos (ex.: Seg a Sex).
        Além disso, o usuário NÃO pode ter nenhum outro dia marcado no mês.
        """
        from datetime import timedelta

        if excluir_ids is None:
            excluir_ids = []

        if len(datas) != 5:
            return False, "Devem ser exatamente 5 dias corridos."

        datas_ordenadas = sorted(datas)

        # Verificar que são 5 dias úteis consecutivos (sem gap de dia útil)
        for i in range(len(datas_ordenadas) - 1):
            d1 = datas_ordenadas[i]
            d2 = datas_ordenadas[i + 1]

            gap_uteis = 0
            d = d1 + timedelta(days=1)
            while d < d2:
                if Feriado.eh_dia_util(d):
                    gap_uteis += 1
                d += timedelta(days=1)

            if gap_uteis > 0:
                return False, "Os 5 dias devem ser úteis consecutivos (sem intervalo)."

        # Não pode haver outros dias marcados no mês além desses 5
        query = Teletrabalho.query.filter(
            Teletrabalho.USUARIO_ID == usuario_id,
            Teletrabalho.MES_REFERENCIA == mes_referencia,
            Teletrabalho.TIPO_MARCACAO == 'TELETRABALHO',
            Teletrabalho.STATUS == 'APROVADO',
            Teletrabalho.DELETED_AT.is_(None)
        )
        if excluir_ids:
            query = query.filter(~Teletrabalho.ID.in_(excluir_ids))

        outros = [t.DATA_TELETRABALHO for t in query.all() if t.DATA_TELETRABALHO not in datas_ordenadas]
        if outros:
            return False, (
                "No modo '5 dias corridos' o usuário não pode ter outros dias "
                "marcados no mês além dos 5 consecutivos."
            )

        return True, "OK"


    @staticmethod
    def contar_pessoas_dia_sem_bloqueio(data, area, tipo_area):
        """Conta pessoas em teletrabalho (excluindo bloqueios e férias)"""
        return Teletrabalho.query.filter(
            Teletrabalho.DATA_TELETRABALHO == data,
            Teletrabalho.AREA == area,
            Teletrabalho.TIPO_AREA == tipo_area,
            Teletrabalho.TIPO_MARCACAO == 'TELETRABALHO',
            Teletrabalho.STATUS == 'APROVADO',
            Teletrabalho.DELETED_AT.is_(None)
        ).count()


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

class Calendario(db.Model):
    """
    Tabela calendário corporativa com classificação de dia útil.
    Fonte definitiva para verificar se um dia é útil ou não.
    Coluna DIA_UTIL: 1 = dia útil, 0 = não útil (feriado, fim de semana, ponto facultativo etc.)
    """
    __tablename__ = 'PAR_TB020_CALENDARIO'
    __table_args__ = {'schema': 'BDG', 'extend_existing': True}

    DIA = db.Column(db.Date, primary_key=True)
    DIA_SEMANA = db.Column(db.SmallInteger)
    DATA_ANO = db.Column(db.Integer)
    DATA_MES = db.Column(db.SmallInteger)
    DATA_DIA = db.Column(db.SmallInteger)
    DIA_FORMATADO = db.Column(db.String(10))
    DSC_MES = db.Column(db.String(9))
    DSC_SEMANA = db.Column(db.String(13))
    DIA_UTIL = db.Column(db.SmallInteger)
    DIAS_UTEIS_CORRIDOS = db.Column(db.SmallInteger)
    DIAS_UTEIS_PERIODO = db.Column(db.SmallInteger)

    def __repr__(self):
        return f'<Calendario {self.DIA} - Util={self.DIA_UTIL}>'

    @staticmethod
    def eh_dia_util(data):
        """
        Verifica se a data é dia útil usando a tabela PAR_TB020_CALENDARIO.
        Retorna True se DIA_UTIL = 1, False se DIA_UTIL = 0 ou não encontrado.
        """
        registro = Calendario.query.filter(Calendario.DIA == data).first()
        if registro is not None:
            return registro.DIA_UTIL == 1
        return Feriado.eh_dia_util(data)

    @staticmethod
    def carregar_dias_uteis_mes(ano, mes):
        """
        Carrega todos os dias do mês e retorna dois sets: úteis e não úteis.
        Usado pelo sorteio automático.
        """
        registros = Calendario.query.filter(
            Calendario.DATA_ANO == ano,
            Calendario.DATA_MES == mes
        ).all()

        dias_uteis = set()
        dias_nao_uteis = set()

        for reg in registros:
            if reg.DIA_UTIL == 1:
                dias_uteis.add(reg.DIA)
            else:
                dias_nao_uteis.add(reg.DIA)

        return dias_uteis, dias_nao_uteis

    @staticmethod
    def carregar_info_mes(ano, mes):
        """
        Carrega todos os dias do mês e retorna um dicionário completo.
        Faz UMA query para o mês inteiro, evitando N queries por dia.
        Usado pelas telas de visualização (calendário, visualização geral, etc.)

        Returns:
            dict {date: {'eh_util': bool, 'dsc_semana': str}}
        """
        registros = Calendario.query.filter(
            Calendario.DATA_ANO == ano,
            Calendario.DATA_MES == mes
        ).all()

        info = {}
        for reg in registros:
            info[reg.DIA] = {
                'eh_util': reg.DIA_UTIL == 1,
                'dsc_semana': (reg.DSC_SEMANA or '').strip()
            }

        return info

class BloqueioDia(db.Model):
    """Model para bloqueio de dias de teletrabalho"""
    __tablename__ = 'APK_TB008_BLOQUEIO_DIA_TELETRABALHO'
    __table_args__ = {'schema': 'BDG', 'extend_existing': True}

    ID = db.Column(db.Integer, primary_key=True)
    DATA_BLOQUEIO = db.Column(db.Date, nullable=False)
    MES_REFERENCIA = db.Column(db.String(6), nullable=False)
    AREA = db.Column(db.String(50), nullable=False)
    TIPO_AREA = db.Column(db.String(50), nullable=False, default='superintendencia')
    MOTIVO = db.Column(db.String(500))
    BLOQUEADO_POR = db.Column(db.Integer, db.ForeignKey('BDG.APK_TB002_USUARIOS.ID'), nullable=False)
    BLOQUEADO_EM = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    # Relacionamento com o usuário que bloqueou
    bloqueador = db.relationship('Usuario', foreign_keys=[BLOQUEADO_POR])

    def __repr__(self):
        return f'<BloqueioDia {self.DATA_BLOQUEIO} - {self.AREA}>'

    @staticmethod
    def dia_esta_bloqueado(data, area, tipo_area):
        """
        Verifica se um dia específico está bloqueado para uma área

        Args:
            data: Data a verificar
            area: Área (ex: SUPEC)
            tipo_area: Tipo da área (ex: superintendencia)

        Returns:
            tuple: (bool, BloqueioDia ou None)
        """
        bloqueio = BloqueioDia.query.filter(
            BloqueioDia.DATA_BLOQUEIO == data,
            BloqueioDia.AREA == area,
            BloqueioDia.TIPO_AREA == tipo_area,
            BloqueioDia.DELETED_AT.is_(None)
        ).first()

        return bloqueio is not None, bloqueio

    @staticmethod
    def listar_bloqueios_mes(mes_referencia, area, tipo_area):
        """
        Lista todos os bloqueios de um mês específico

        Args:
            mes_referencia: Mês no formato YYYYMM (ex: 202601)
            area: Área (ex: SUPEC)
            tipo_area: Tipo da área (ex: superintendencia)

        Returns:
            list: Lista de objetos BloqueioDia
        """
        return BloqueioDia.query.filter(
            BloqueioDia.MES_REFERENCIA == mes_referencia,
            BloqueioDia.AREA == area,
            BloqueioDia.TIPO_AREA == tipo_area,
            BloqueioDia.DELETED_AT.is_(None)
        ).order_by(BloqueioDia.DATA_BLOQUEIO).all()

    @staticmethod
    def listar_bloqueios_area(area, tipo_area=None):
        """
        Lista todos os bloqueios de uma área (não deletados)

        Args:
            area: Área (ex: SUPEC)
            tipo_area: Tipo da área (opcional)

        Returns:
            list: Lista de objetos BloqueioDia
        """
        query = BloqueioDia.query.filter(
            BloqueioDia.AREA == area,
            BloqueioDia.DELETED_AT.is_(None)
        )

        if tipo_area:
            query = query.filter(BloqueioDia.TIPO_AREA == tipo_area)

        return query.order_by(BloqueioDia.DATA_BLOQUEIO.desc()).all()

    @staticmethod
    def obter_bloqueio(data, area, tipo_area):
        """
        Retorna o bloqueio de um dia específico

        Args:
            data: Data a buscar
            area: Área (ex: SUPEC)
            tipo_area: Tipo da área (ex: superintendencia)

        Returns:
            BloqueioDia ou None
        """
        return BloqueioDia.query.filter(
            BloqueioDia.DATA_BLOQUEIO == data,
            BloqueioDia.AREA == area,
            BloqueioDia.TIPO_AREA == tipo_area,
            BloqueioDia.DELETED_AT.is_(None)
        ).first()
