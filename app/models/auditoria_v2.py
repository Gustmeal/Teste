# -*- coding: utf-8 -*-
"""
Model: app/models/auditoria_v2.py
Auditoria V2 do Portal GEINC (captura automática).

Todas as tabelas são APPEND-ONLY: o sistema só faz INSERT nelas.
As tabelas são criadas automaticamente pelo db.create_all() do create_app().

    AUD_TB001_REQUISICAO       -> 1 linha por ação do usuário no portal (cada requisição HTTP)
    AUD_TB002_ALTERACAO        -> 1 linha por registro (linha de tabela) incluído/alterado/excluído
    AUD_TB003_ALTERACAO_CAMPO  -> 1 linha por campo alterado (só UPDATE), para busca rápida
    AUD_TB004_COMANDO_SQL      -> 1 linha por comando de escrita executado no banco
    AUD_TB005_EVENTO           -> eventos de negócio e segurança (login, logout, login falho, registrar_log...)
    AUD_TB006_APLICATIVO       -> mapeamento blueprint -> nome do aplicativo (editável na tela)
    AUD_TB007_CONFIG           -> parâmetros da auditoria (editáveis na tela)

Compatível com Python 3.9 e 3.12.
"""

from datetime import datetime

from sqlalchemy.dialects.mssql import DATETIME2, NVARCHAR

from app import db

# DATETIME2 no SQL Server (precisão de microssegundos); tipo genérico nos demais bancos
DATA_HORA = db.DateTime().with_variant(DATETIME2(), 'mssql')
# BIGINT IDENTITY no SQL Server (INTEGER só para permitir testes locais em SQLite)
ID_GRANDE = db.BigInteger().with_variant(db.Integer(), 'sqlite')
# Texto longo: sempre NVARCHAR(MAX) no SQL Server (nunca o antigo NTEXT)
TEXTO_LONGO = db.UnicodeText().with_variant(NVARCHAR(), 'mssql')


class AudRequisicao(db.Model):
    __tablename__ = 'AUD_TB001_REQUISICAO'
    __table_args__ = (
        db.Index('IX_AUD_TB001_DT', 'DT_INICIO'),
        db.Index('IX_AUD_TB001_USUARIO', 'USUARIO_ID', 'DT_INICIO'),
        db.Index('IX_AUD_TB001_APP', 'BLUEPRINT', 'DT_INICIO'),
        db.Index('IX_AUD_TB001_CORREL', 'ID_CORRELACAO'),
        {'schema': 'BDG'},
    )

    ID = db.Column(ID_GRANDE, primary_key=True, autoincrement=True)
    ID_CORRELACAO = db.Column(db.String(32), nullable=False)
    DT_INICIO = db.Column(DATA_HORA, nullable=False, default=datetime.now)
    DURACAO_MS = db.Column(db.Integer)
    USUARIO_ID = db.Column(db.Integer)
    USUARIO_NOME = db.Column(db.Unicode(150))
    USUARIO_EMAIL = db.Column(db.Unicode(150))
    USUARIO_PERFIL = db.Column(db.Unicode(30))
    USUARIO_AREA = db.Column(db.Unicode(30))
    USUARIO_CARGO = db.Column(db.Unicode(150))
    IP = db.Column(db.String(60))
    NAVEGADOR = db.Column(db.Unicode(500))
    METODO = db.Column(db.String(10))
    URL = db.Column(db.Unicode(2000))
    PAGINA_ORIGEM = db.Column(db.Unicode(2000))
    ROTA = db.Column(db.String(200))
    BLUEPRINT = db.Column(db.String(100))
    APLICATIVO = db.Column(db.Unicode(150))
    TIPO_ACAO = db.Column(db.String(30))
    STATUS_HTTP = db.Column(db.SmallInteger)
    AJAX = db.Column(db.Boolean)
    PARAMETROS = db.Column(TEXTO_LONGO)
    ARQUIVO_DOWNLOAD = db.Column(db.Unicode(400))
    TAMANHO_RESPOSTA = db.Column(db.BigInteger)
    QTDE_CONSULTAS_BANCO = db.Column(db.Integer)
    TEMPO_BANCO_MS = db.Column(db.Integer)
    QTDE_COMANDOS_ESCRITA = db.Column(db.Integer)
    QTDE_REGISTROS_ALTERADOS = db.Column(db.Integer)
    MENSAGENS = db.Column(TEXTO_LONGO)
    ERRO = db.Column(TEXTO_LONGO)


class AudAlteracao(db.Model):
    __tablename__ = 'AUD_TB002_ALTERACAO'
    __table_args__ = (
        db.Index('IX_AUD_TB002_DT', 'DT_ALTERACAO'),
        db.Index('IX_AUD_TB002_TABELA', 'TABELA', 'CHAVE_REGISTRO'),
        db.Index('IX_AUD_TB002_CORREL', 'ID_CORRELACAO'),
        db.Index('IX_AUD_TB002_USUARIO', 'USUARIO_ID', 'DT_ALTERACAO'),
        {'schema': 'BDG'},
    )

    ID = db.Column(ID_GRANDE, primary_key=True, autoincrement=True)
    COD_ALTERACAO = db.Column(db.String(32), nullable=False, unique=True)
    ID_CORRELACAO = db.Column(db.String(32), nullable=False)
    COD_COMANDO = db.Column(db.String(32))
    DT_ALTERACAO = db.Column(DATA_HORA, nullable=False, default=datetime.now)
    USUARIO_ID = db.Column(db.Integer)
    USUARIO_NOME = db.Column(db.Unicode(150))
    BLUEPRINT = db.Column(db.String(100))
    APLICATIVO = db.Column(db.Unicode(150))
    ORIGEM = db.Column(db.String(20))            # ORM / SQL
    OPERACAO = db.Column(db.String(10))          # INSERT / UPDATE / DELETE
    TABELA = db.Column(db.String(300))
    CHAVE_REGISTRO = db.Column(db.Unicode(500))
    CAMPOS_ALTERADOS = db.Column(db.Unicode(4000))
    DADOS_ANTES = db.Column(TEXTO_LONGO)
    DADOS_DEPOIS = db.Column(TEXTO_LONGO)


class AudAlteracaoCampo(db.Model):
    __tablename__ = 'AUD_TB003_ALTERACAO_CAMPO'
    __table_args__ = (
        db.Index('IX_AUD_TB003_COD', 'COD_ALTERACAO'),
        db.Index('IX_AUD_TB003_CAMPO', 'TABELA', 'CAMPO', 'DT_ALTERACAO'),
        {'schema': 'BDG'},
    )

    ID = db.Column(ID_GRANDE, primary_key=True, autoincrement=True)
    COD_ALTERACAO = db.Column(db.String(32), nullable=False)
    ID_CORRELACAO = db.Column(db.String(32), nullable=False)
    DT_ALTERACAO = db.Column(DATA_HORA, nullable=False, default=datetime.now)
    USUARIO_ID = db.Column(db.Integer)
    USUARIO_NOME = db.Column(db.Unicode(150))
    TABELA = db.Column(db.String(300))
    CHAVE_REGISTRO = db.Column(db.Unicode(500))
    CAMPO = db.Column(db.String(150))
    VALOR_ANTES = db.Column(db.Unicode(4000))
    VALOR_DEPOIS = db.Column(db.Unicode(4000))


class AudComandoSql(db.Model):
    __tablename__ = 'AUD_TB004_COMANDO_SQL'
    __table_args__ = (
        db.Index('IX_AUD_TB004_DT', 'DT_EXECUCAO'),
        db.Index('IX_AUD_TB004_CORREL', 'ID_CORRELACAO'),
        db.Index('IX_AUD_TB004_TABELA', 'TABELA', 'DT_EXECUCAO'),
        {'schema': 'BDG'},
    )

    ID = db.Column(ID_GRANDE, primary_key=True, autoincrement=True)
    COD_COMANDO = db.Column(db.String(32), nullable=False, unique=True)
    ID_CORRELACAO = db.Column(db.String(32), nullable=False)
    DT_EXECUCAO = db.Column(DATA_HORA, nullable=False, default=datetime.now)
    USUARIO_ID = db.Column(db.Integer)
    USUARIO_NOME = db.Column(db.Unicode(150))
    BLUEPRINT = db.Column(db.String(100))
    APLICATIVO = db.Column(db.Unicode(150))
    ORIGEM = db.Column(db.String(20))            # ORM / ORM_LOTE / SQL
    OPERACAO = db.Column(db.String(20))          # INSERT / UPDATE / DELETE / MERGE / PROCEDURE / DDL
    TABELA = db.Column(db.String(300))
    COMANDO = db.Column(TEXTO_LONGO)
    PARAMETROS = db.Column(TEXTO_LONGO)
    QTDE_LOTES = db.Column(db.Integer)
    LINHAS_AFETADAS = db.Column(db.Integer)
    DURACAO_MS = db.Column(db.Integer)
    SITUACAO = db.Column(db.String(20))          # CONFIRMADA / DESFEITA / ERRO
    DETALHAMENTO = db.Column(db.String(30))      # COMPLETO / PARCIAL / SEM_DETALHE / FALHOU
    ERRO = db.Column(db.Unicode(4000))


class AudEvento(db.Model):
    __tablename__ = 'AUD_TB005_EVENTO'
    __table_args__ = (
        db.Index('IX_AUD_TB005_DT', 'DT_EVENTO'),
        db.Index('IX_AUD_TB005_TIPO', 'TIPO', 'DT_EVENTO'),
        db.Index('IX_AUD_TB005_CORREL', 'ID_CORRELACAO'),
        {'schema': 'BDG'},
    )

    ID = db.Column(ID_GRANDE, primary_key=True, autoincrement=True)
    ID_CORRELACAO = db.Column(db.String(32), nullable=False)
    DT_EVENTO = db.Column(DATA_HORA, nullable=False, default=datetime.now)
    TIPO = db.Column(db.String(40), nullable=False)
    NIVEL = db.Column(db.String(10))             # INFO / ALERTA / CRITICO
    USUARIO_ID = db.Column(db.Integer)
    USUARIO_NOME = db.Column(db.Unicode(150))
    IP = db.Column(db.String(60))
    BLUEPRINT = db.Column(db.String(100))
    APLICATIVO = db.Column(db.Unicode(150))
    ENTIDADE = db.Column(db.Unicode(100))
    ENTIDADE_ID = db.Column(db.Unicode(100))
    DESCRICAO = db.Column(db.Unicode(4000))
    DETALHES = db.Column(TEXTO_LONGO)


class AudAplicativo(db.Model):
    __tablename__ = 'AUD_TB006_APLICATIVO'
    __table_args__ = {'schema': 'BDG'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    BLUEPRINT = db.Column(db.String(100), nullable=False, unique=True)
    NO_APLICATIVO = db.Column(db.Unicode(150), nullable=False)
    CATEGORIA = db.Column(db.Unicode(100))
    IN_AUDITAR = db.Column(db.Boolean, nullable=False, default=True)
    CREATED_AT = db.Column(DATA_HORA, default=datetime.now)
    UPDATED_AT = db.Column(DATA_HORA)


class AudConfig(db.Model):
    __tablename__ = 'AUD_TB007_CONFIG'
    __table_args__ = {'schema': 'BDG'}

    CHAVE = db.Column(db.String(60), primary_key=True)
    VALOR = db.Column(db.Unicode(4000))
    DESCRICAO = db.Column(db.Unicode(500))
    UPDATED_AT = db.Column(DATA_HORA)
    UPDATED_POR = db.Column(db.Unicode(150))
