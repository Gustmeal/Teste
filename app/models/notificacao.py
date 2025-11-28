# app/models/notificacao.py
from datetime import datetime
from app import db


class Notificacao(db.Model):
    """Modelo para notificações administrativas"""
    __tablename__ = 'APK_TB010_NOTIFICACOES'
    __table_args__ = {'schema': 'BDG'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    TITULO = db.Column(db.String(200), nullable=False)
    MENSAGEM = db.Column(db.Text, nullable=False)
    TIPO = db.Column(db.String(20), default='modal')
    PRIORIDADE = db.Column(db.String(20), default='normal')
    ATIVO = db.Column(db.Boolean, default=True)
    CRIADO_POR = db.Column(db.Integer, nullable=False)  # SEM FOREIGN KEY
    DT_INICIO = db.Column(db.DateTime, nullable=True)
    DT_FIM = db.Column(db.DateTime, nullable=True)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime, nullable=True)

    # Relacionamento SOMENTE com visualizações
    visualizacoes = db.relationship('NotificacaoVisualizacao', backref='notificacao', cascade='all, delete-orphan')

    def __repr__(self):
        return f'<Notificacao {self.ID} - {self.TITULO}>'

    @property
    def criador(self):
        """Busca o criador quando necessário (sem relacionamento automático)"""
        from app.models.usuario import Usuario
        return Usuario.query.get(self.CRIADO_POR)

    def foi_visualizada_por(self, usuario_id):
        """Verifica se a notificação foi visualizada por um usuário específico"""
        vis = NotificacaoVisualizacao.query.filter_by(
            NOTIFICACAO_ID=self.ID,
            USUARIO_ID=usuario_id
        ).first()
        return vis is not None

    @staticmethod
    def obter_ativas_nao_visualizadas(usuario_id):
        """Retorna notificações ativas que o usuário ainda não visualizou"""
        from sqlalchemy import or_

        agora = datetime.utcnow()

        print(f"DEBUG MODEL - Buscando notificações não visualizadas para usuario {usuario_id}")

        # Buscar IDs das notificações já visualizadas
        visualizadas_ids = db.session.query(NotificacaoVisualizacao.NOTIFICACAO_ID).filter(
            NotificacaoVisualizacao.USUARIO_ID == usuario_id
        ).all()

        # Converter para lista simples
        visualizadas_ids = [v[0] for v in visualizadas_ids]
        print(f"DEBUG MODEL - Notificações já visualizadas: {visualizadas_ids}")

        # Query principal - excluindo as já visualizadas
        query = Notificacao.query.filter(
            Notificacao.ATIVO == True,
            Notificacao.DELETED_AT == None,
            or_(
                Notificacao.DT_INICIO == None,
                Notificacao.DT_INICIO <= agora
            ),
            or_(
                Notificacao.DT_FIM == None,
                Notificacao.DT_FIM >= agora
            )
        )

        # Excluir as já visualizadas
        if visualizadas_ids:
            query = query.filter(~Notificacao.ID.in_(visualizadas_ids))

        resultado = query.order_by(Notificacao.CREATED_AT.desc()).all()

        print(f"DEBUG MODEL - Encontradas {len(resultado)} notificações não visualizadas")

        return resultado


class NotificacaoVisualizacao(db.Model):
    """Controle de visualizações de notificações por usuário"""
    __tablename__ = 'APK_TB011_NOTIFICACAO_VISUALIZACAO'
    __table_args__ = {'schema': 'BDG'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    NOTIFICACAO_ID = db.Column(db.Integer, db.ForeignKey('BDG.APK_TB010_NOTIFICACOES.ID'), nullable=False)
    USUARIO_ID = db.Column(db.Integer, nullable=False)  # SEM FOREIGN KEY
    VISUALIZADO_AT = db.Column(db.DateTime, default=datetime.utcnow)

    @property
    def usuario(self):
        """Busca o usuário quando necessário (sem relacionamento automático)"""
        from app.models.usuario import Usuario
        return Usuario.query.get(self.USUARIO_ID)

    def __repr__(self):
        return f'<NotificacaoVisualizacao Notif:{self.NOTIFICACAO_ID} User:{self.USUARIO_ID}>'