# app/models/doc_catalogo.py
from datetime import datetime
from app import db


class Aplicativo(db.Model):
    """Aplicativos catalogados no módulo Catálogo de Tabelas e Dados."""
    __tablename__ = 'DOC_TB001_APLICATIVO'
    __table_args__ = {'schema': 'BDG'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    CHAVE = db.Column(db.String(50), unique=True, nullable=False)
    NO_APLICATIVO = db.Column(db.String(120), nullable=False)
    DS_APLICATIVO = db.Column(db.String(400), nullable=True)
    ICONE = db.Column(db.String(60), nullable=True)
    COR = db.Column(db.String(40), nullable=True)
    NU_ORDEM = db.Column(db.Integer, default=0)
    IN_ATIVO = db.Column(db.Boolean, default=True)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    subaplicativos = db.relationship(
        'Subaplicativo', backref='aplicativo', cascade='all, delete-orphan'
    )
    tabelas = db.relationship(
        'TabelaDoc', backref='aplicativo', cascade='all, delete-orphan'
    )

    def __repr__(self):
        return f'<Aplicativo {self.CHAVE}>'


class Subaplicativo(db.Model):
    """Subdivisão de um aplicativo (ex.: Editais, Períodos, Metas...)."""
    __tablename__ = 'DOC_TB002_SUBAPLICATIVO'
    __table_args__ = {'schema': 'BDG'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ID_APLICATIVO = db.Column(
        db.Integer, db.ForeignKey('BDG.DOC_TB001_APLICATIVO.ID'), nullable=False
    )
    NO_SUBAPLICATIVO = db.Column(db.String(120), nullable=False)
    DS_SUBAPLICATIVO = db.Column(db.String(400), nullable=True)
    NU_ORDEM = db.Column(db.Integer, default=0)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    def __repr__(self):
        return f'<Subaplicativo {self.NO_SUBAPLICATIVO}>'


class TabelaDoc(db.Model):
    """Documentação (parte 'humana') de cada tabela: objetivo, motivo, uso.
    A estrutura real (colunas/tipos/chaves) é lida ao vivo do banco.
    NO_BANCO: quando preenchido, a estrutura é lida desse banco (nome de 3
    partes); quando NULL, do banco padrão da aplicação."""
    __tablename__ = 'DOC_TB003_TABELA'
    __table_args__ = {'schema': 'BDG'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ID_APLICATIVO = db.Column(db.Integer, db.ForeignKey('BDG.DOC_TB001_APLICATIVO.ID'), nullable=False)
    ID_SUBAPLICATIVO = db.Column(db.Integer, db.ForeignKey('BDG.DOC_TB002_SUBAPLICATIVO.ID'), nullable=True)
    NO_BANCO = db.Column(db.String(60), nullable=True)   # NULL = banco padrão
    NO_SCHEMA = db.Column(db.String(20), nullable=False, default='BDG')
    NO_TABELA = db.Column(db.String(150), nullable=False)
    TP_CATEGORIA = db.Column(db.String(20), nullable=False)  # ENTRADA/SAIDA/PARAMETRO/CONSULTA
    DS_OBJETIVO = db.Column(db.String(800), nullable=True)
    DS_MOTIVO = db.Column(db.String(800), nullable=True)
    DS_SERVE_PARA = db.Column(db.String(800), nullable=True)
    DS_OBSERVACAO = db.Column(db.String(800), nullable=True)
    NU_ORDEM = db.Column(db.Integer, default=0)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    subaplicativo = db.relationship('Subaplicativo', backref='tabelas')

    def __repr__(self):
        return f'<TabelaDoc {self.NO_SCHEMA}.{self.NO_TABELA}>'