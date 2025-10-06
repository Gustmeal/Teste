from datetime import datetime
from app import db
from sqlalchemy import func
import os


class Demanda(db.Model):
    """Modelo para gerenciar demandas de trabalho"""
    __tablename__ = 'DEM_TB001_DEMANDAS'
    __table_args__ = {'schema': 'DEV'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    TITULO = db.Column(db.String(200), nullable=False)
    DESCRICAO = db.Column(db.Text, nullable=True)
    RESPONSAVEL_ID = db.Column(db.Integer, db.ForeignKey('BDG.APK_TB002_USUARIOS.ID'), nullable=False)
    SOLICITANTE_ID = db.Column(db.Integer, db.ForeignKey('BDG.APK_TB002_USUARIOS.ID'), nullable=False)
    PRIORIDADE = db.Column(db.String(20), nullable=False, default='MEDIA')
    STATUS = db.Column(db.String(30), nullable=False, default='PENDENTE')
    PERCENTUAL_CONCLUSAO = db.Column(db.Integer, default=0)
    DT_CRIACAO = db.Column(db.DateTime, default=datetime.utcnow)
    DT_INICIO = db.Column(db.DateTime, nullable=True)
    DT_PRAZO = db.Column(db.DateTime, nullable=False)
    DT_CONCLUSAO = db.Column(db.DateTime, nullable=True)
    HORAS_ESTIMADAS = db.Column(db.Float, nullable=True)
    HORAS_TRABALHADAS = db.Column(db.Float, default=0)
    OBSERVACOES = db.Column(db.Text, nullable=True)
    TIPO_DEMANDA = db.Column(db.String(50), nullable=True)
    SISTEMA_RELACIONADO = db.Column(db.String(100), nullable=True)
    AREA = db.Column(db.String(100), nullable=True)
    VISIBILIDADE_AREA = db.Column(db.Boolean, default=False)
    ARQUIVO_ANEXO = db.Column(db.String(500), nullable=True)
    NOME_ARQUIVO = db.Column(db.String(255), nullable=True)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime, nullable=True)

    # Relacionamentos
    responsavel = db.relationship('Usuario', foreign_keys=[RESPONSAVEL_ID], backref='demandas_responsavel')
    solicitante = db.relationship('Usuario', foreign_keys=[SOLICITANTE_ID], backref='demandas_solicitante')
    anexos = db.relationship('DemandaAnexo', backref='demanda', cascade='all, delete-orphan')

    def pode_visualizar(self, usuario):
        """Verifica se o usuário pode visualizar a demanda"""
        # Admin e moderador veem tudo
        if usuario.PERFIL in ['admin', 'moderador']:
            return True

        # Responsável e solicitante sempre podem ver
        if usuario.ID in [self.RESPONSAVEL_ID, self.SOLICITANTE_ID]:
            return True

        # Se visibilidade por área está ativa, verificar se é da mesma área
        if self.VISIBILIDADE_AREA and self.AREA:
            # Buscar área do usuário
            if hasattr(usuario, 'empregado') and usuario.empregado:
                area_usuario = usuario.empregado.sgSuperintendencia
                if area_usuario == self.AREA:
                    return True

        return False

    def pode_editar(self, usuario):
        """Verifica se o usuário pode editar a demanda"""
        # Admin sempre pode
        if usuario.PERFIL == 'admin':
            return True

        # Moderador pode se for solicitante ou responsável
        if usuario.PERFIL == 'moderador':
            return usuario.ID in [self.RESPONSAVEL_ID, self.SOLICITANTE_ID]

        # Usuário comum só se for responsável
        return usuario.ID == self.RESPONSAVEL_ID

    @property
    def tempo_restante(self):
        """Calcula o tempo restante até o prazo"""
        if self.STATUS in ['CONCLUIDA', 'CANCELADA']:
            return None

        from datetime import datetime
        agora = datetime.now()

        if self.DT_PRAZO:
            diferenca = self.DT_PRAZO - agora
            return diferenca
        return None

    @property
    def esta_atrasada(self):
        """Verifica se a demanda está atrasada"""
        if self.STATUS in ['CONCLUIDA', 'CANCELADA']:
            return False

        from datetime import datetime
        return datetime.now() > self.DT_PRAZO if self.DT_PRAZO else False

    @property
    def cor_prioridade(self):
        """Retorna a cor baseada na prioridade"""
        cores = {
            'BAIXA': 'success',
            'MEDIA': 'warning',
            'ALTA': 'danger',
            'URGENTE': 'dark'
        }
        return cores.get(self.PRIORIDADE, 'secondary')

    @property
    def icone_status(self):
        """Retorna o ícone baseado no status"""
        icones = {
            'PENDENTE': 'fa-clock',
            'EM_ANDAMENTO': 'fa-spinner fa-spin',
            'PAUSADA': 'fa-pause',
            'CONCLUIDA': 'fa-check-circle',
            'CANCELADA': 'fa-times-circle'
        }
        return icones.get(self.STATUS, 'fa-question-circle')

    def __repr__(self):
        return f'<Demanda {self.ID}: {self.TITULO}>'


class DemandaHistorico(db.Model):
    """Histórico de alterações nas demandas"""
    __tablename__ = 'DEM_TB002_HISTORICO'
    __table_args__ = {'schema': 'DEV'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    DEMANDA_ID = db.Column(db.Integer, db.ForeignKey('DEV.DEM_TB001_DEMANDAS.ID'), nullable=False)
    USUARIO_ID = db.Column(db.Integer, db.ForeignKey('BDG.APK_TB002_USUARIOS.ID'), nullable=False)
    ACAO = db.Column(db.String(50), nullable=False)
    DESCRICAO = db.Column(db.Text, nullable=True)
    VALOR_ANTERIOR = db.Column(db.String(200), nullable=True)
    VALOR_NOVO = db.Column(db.String(200), nullable=True)
    DATA_HORA = db.Column(db.DateTime, default=datetime.utcnow)

    # Relacionamentos
    usuario = db.relationship('Usuario', backref='historico_demandas')

    def __repr__(self):
        return f'<DemandaHistorico {self.ID}: {self.ACAO}>'


class DemandaAnexo(db.Model):
    """Anexos das demandas"""
    __tablename__ = 'DEM_TB003_ANEXOS'
    __table_args__ = {'schema': 'DEV'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    DEMANDA_ID = db.Column(db.Integer, db.ForeignKey('DEV.DEM_TB001_DEMANDAS.ID'), nullable=False)
    NOME_ARQUIVO = db.Column(db.String(255), nullable=False)
    CAMINHO_ARQUIVO = db.Column(db.String(500), nullable=False)
    TAMANHO_KB = db.Column(db.Integer, nullable=True)
    TIPO_ARQUIVO = db.Column(db.String(50), nullable=True)
    USUARIO_ID = db.Column(db.Integer, db.ForeignKey('BDG.APK_TB002_USUARIOS.ID'), nullable=False)
    DT_UPLOAD = db.Column(db.DateTime, default=datetime.utcnow)

    # Relacionamentos
    usuario = db.relationship('Usuario', backref='anexos_demandas')

    @property
    def tamanho_formatado(self):
        """Retorna o tamanho formatado"""
        if not self.TAMANHO_KB:
            return 'N/A'

        if self.TAMANHO_KB < 1024:
            return f"{self.TAMANHO_KB} KB"
        else:
            return f"{self.TAMANHO_KB / 1024:.1f} MB"

    @property
    def icone(self):
        """Retorna o ícone baseado no tipo de arquivo"""
        if not self.TIPO_ARQUIVO:
            return 'fa-file'

        tipos = {
            'pdf': 'fa-file-pdf text-danger',
            'doc': 'fa-file-word text-primary',
            'docx': 'fa-file-word text-primary',
            'xls': 'fa-file-excel text-success',
            'xlsx': 'fa-file-excel text-success',
            'png': 'fa-file-image text-info',
            'jpg': 'fa-file-image text-info',
            'jpeg': 'fa-file-image text-info',
            'zip': 'fa-file-archive text-warning',
            'rar': 'fa-file-archive text-warning'
        }

        ext = self.NOME_ARQUIVO.split('.')[-1].lower() if '.' in self.NOME_ARQUIVO else ''
        return tipos.get(ext, 'fa-file')

    def __repr__(self):
        return f'<DemandaAnexo {self.ID}: {self.NOME_ARQUIVO}>'