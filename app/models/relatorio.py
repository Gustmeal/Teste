from app import db
from datetime import datetime
from sqlalchemy import JSON


class RelatorioTemplate(db.Model):
    """Modelo para salvar templates de relatórios criados pelos usuários"""
    __tablename__ = 'REL_TB001_TEMPLATES'
    __table_args__ = {'schema': 'BDG'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    NOME = db.Column(db.String(100), nullable=False)
    DESCRICAO = db.Column(db.String(500))
    USUARIO_ID = db.Column(db.Integer, db.ForeignKey('SYS_TB_USUARIOS.ID'))
    CONFIGURACAO = db.Column(JSON, nullable=False)  # Estrutura do relatório
    PUBLICO = db.Column(db.Boolean, default=False)  # Se outros podem usar
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    def __repr__(self):
        return f'<RelatorioTemplate {self.NOME}>'


class RelatorioGerado(db.Model):
    """Histórico de relatórios gerados"""
    __tablename__ = 'REL_TB002_GERADOS'
    __table_args__ = {'schema': 'BDG'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    TEMPLATE_ID = db.Column(db.Integer, db.ForeignKey('BDG.REL_TB001_TEMPLATES.ID'))
    USUARIO_ID = db.Column(db.Integer, db.ForeignKey('SYS_TB_USUARIOS.ID'))
    NOME_ARQUIVO = db.Column(db.String(255))
    FORMATO = db.Column(db.String(10))  # pdf, excel, word
    PARAMETROS = db.Column(JSON)  # Filtros aplicados
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)

    template = db.relationship('RelatorioTemplate', backref='gerados')