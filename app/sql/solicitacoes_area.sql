/* =====================================================================
   SOLICITAÇÕES À ÁREA (GEINC) - criação das tabelas
   Arquivo: app/sql/solicitacoes_area.sql
   Banco:   BDDASHBOARDBI  /  Schema: BDG

   Executar UMA vez, antes de subir o aplicativo.

   SOL_TB001_SOLICITACOES  -> a solicitação (o que foi pedido, por quem,
                              área, prazos, status, entrega e homologação)
   SOL_TB002_ENVOLVIDOS    -> executores (quem fez) e observadores
   SOL_TB003_INTERACOES    -> linha do tempo: atualizações, comentários,
                              mudanças de status, marcos, impedimentos
                              (FL_INTERNA = 1 -> solicitante não vê)
   SOL_TB004_ITENS_ESCOPO  -> checklist do que precisa ser entregue
   SOL_TB005_ARQUIVOS      -> evidências da solicitação, anexos, termos
                              gerados e termos assinados (arquivo no banco)
   SOL_TB006_HISTORICO     -> histórico campo a campo das alterações
   ===================================================================== */

USE BDDASHBOARDBI;
GO

/* ---------------------------------------------------------------------
   SOL_TB001_SOLICITACOES
   --------------------------------------------------------------------- */
IF OBJECT_ID('BDG.SOL_TB001_SOLICITACOES', 'U') IS NULL
BEGIN
    CREATE TABLE BDG.SOL_TB001_SOLICITACOES (
        ID_SOLICITACAO              INT IDENTITY(1,1) NOT NULL,   -- *chave
        NU_PROTOCOLO                VARCHAR(20)     NOT NULL,     -- SOL-2026-0001
        TITULO                      NVARCHAR(200)   NOT NULL,
        DESCRICAO                   NVARCHAR(MAX)   NULL,
        TIPO_ENTREGA                VARCHAR(20)     NOT NULL,     -- APLICATIVO | PAINEL
        CATEGORIA                   VARCHAR(30)     NULL,         -- NOVO, MELHORIA, CORRECAO...
        PRIORIDADE                  VARCHAR(10)     NOT NULL DEFAULT 'MEDIA',
        IMPACTO                     VARCHAR(10)     NULL,
        COMPLEXIDADE                VARCHAR(5)      NULL,         -- P | M | G | GG
        CANAL_ORIGEM                VARCHAR(20)     NULL,         -- EMAIL, TEAMS, REUNIAO...
        NU_REFERENCIA               VARCHAR(60)     NULL,         -- nº SEI / memorando / chamado
        NO_SISTEMA                  NVARCHAR(150)   NULL,         -- nome do aplicativo/painel
        ETIQUETAS                   NVARCHAR(300)   NULL,         -- separadas por vírgula

        ID_USUARIO_SOLICITANTE      INT             NOT NULL,
        NO_SOLICITANTE              NVARCHAR(100)   NULL,
        EMAIL_SOLICITANTE           VARCHAR(100)    NULL,
        SG_SETOR                    VARCHAR(10)     NULL,         -- foto da área no momento
        SG_SUPERINTENDENCIA         VARCHAR(10)     NULL,
        SG_DIRETORIA                VARCHAR(10)     NULL,
        DS_CARGO_SOLICITANTE        NVARCHAR(100)   NULL,

        DT_SOLICITACAO              DATE            NOT NULL,
        DT_PRAZO_DESEJADO           DATE            NULL,
        DT_PRAZO_ACORDADO           DATE            NULL,
        DT_INICIO                   DATETIME        NULL,
        DT_ENTREGA                  DATETIME        NULL,         -- enviada para homologação
        DT_CONCLUSAO                DATETIME        NULL,
        HORAS_ESTIMADAS             DECIMAL(9,2)    NULL,

        STATUS                      VARCHAR(30)     NOT NULL DEFAULT 'RECEBIDA',
        PC_CONCLUSAO                INT             NOT NULL DEFAULT 0,

        DT_CIENCIA_SOLICITANTE      DATETIME        NULL,         -- solicitante confirmou o pedido

        RESUMO_ENTREGA              NVARCHAR(MAX)   NULL,         -- o que foi feito (vai no termo)
        ORIENTACOES_USO             NVARCHAR(MAX)   NULL,
        LINK_ACESSO                 NVARCHAR(300)   NULL,

        ST_HOMOLOGACAO_SOLICITANTE  VARCHAR(10)     NULL,         -- APROVADA | RECUSADA
        PARECER_SOLICITANTE         NVARCHAR(MAX)   NULL,
        DT_HOMOLOGACAO_SOLICITANTE  DATETIME        NULL,
        ID_USUARIO_HOMOLOGACAO      INT             NULL,

        JUSTIFICATIVA_CONCLUSAO     NVARCHAR(MAX)   NULL,
        ID_USUARIO_CONCLUSAO        INT             NULL,

        ID_USUARIO_CRIACAO          INT             NULL,
        ID_USUARIO_ATUALIZACAO      INT             NULL,
        CREATED_AT                  DATETIME        NOT NULL DEFAULT GETDATE(),
        UPDATED_AT                  DATETIME        NULL,
        DELETED_AT                  DATETIME        NULL,

        CONSTRAINT PK_SOL_TB001 PRIMARY KEY (ID_SOLICITACAO),
        CONSTRAINT UQ_SOL_TB001_PROTOCOLO UNIQUE (NU_PROTOCOLO)
    );

    CREATE INDEX IX_SOL_TB001_STATUS      ON BDG.SOL_TB001_SOLICITACOES (STATUS) WHERE DELETED_AT IS NULL;
    CREATE INDEX IX_SOL_TB001_SOLICITANTE ON BDG.SOL_TB001_SOLICITACOES (ID_USUARIO_SOLICITANTE);
    CREATE INDEX IX_SOL_TB001_DT          ON BDG.SOL_TB001_SOLICITACOES (DT_SOLICITACAO);
END
GO

/* ---------------------------------------------------------------------
   SOL_TB002_ENVOLVIDOS
   --------------------------------------------------------------------- */
IF OBJECT_ID('BDG.SOL_TB002_ENVOLVIDOS', 'U') IS NULL
BEGIN
    CREATE TABLE BDG.SOL_TB002_ENVOLVIDOS (
        ID_ENVOLVIDO        INT IDENTITY(1,1) NOT NULL,   -- *chave
        ID_SOLICITACAO      INT             NOT NULL,
        ID_USUARIO          INT             NOT NULL,
        NO_USUARIO          NVARCHAR(100)   NULL,
        PAPEL               VARCHAR(15)     NOT NULL,     -- EXECUTOR | OBSERVADOR
        DS_FUNCAO           NVARCHAR(60)    NULL,         -- ex.: Desenvolvimento, Testes, BI
        ID_USUARIO_CRIACAO  INT             NULL,
        CREATED_AT          DATETIME        NOT NULL DEFAULT GETDATE(),
        DELETED_AT          DATETIME        NULL,

        CONSTRAINT PK_SOL_TB002 PRIMARY KEY (ID_ENVOLVIDO),
        CONSTRAINT FK_SOL_TB002_SOL FOREIGN KEY (ID_SOLICITACAO)
            REFERENCES BDG.SOL_TB001_SOLICITACOES (ID_SOLICITACAO)
    );

    CREATE INDEX IX_SOL_TB002_SOL     ON BDG.SOL_TB002_ENVOLVIDOS (ID_SOLICITACAO);
    CREATE INDEX IX_SOL_TB002_USUARIO ON BDG.SOL_TB002_ENVOLVIDOS (ID_USUARIO, PAPEL);
END
GO

/* ---------------------------------------------------------------------
   SOL_TB003_INTERACOES
   TIPO: ATUALIZACAO, COMENTARIO, STATUS, MARCO, IMPEDIMENTO, ENTREGA,
         HOMOLOGACAO, SISTEMA
   --------------------------------------------------------------------- */
IF OBJECT_ID('BDG.SOL_TB003_INTERACOES', 'U') IS NULL
BEGIN
    CREATE TABLE BDG.SOL_TB003_INTERACOES (
        ID_INTERACAO        INT IDENTITY(1,1) NOT NULL,   -- *chave
        ID_SOLICITACAO      INT             NOT NULL,
        TIPO                VARCHAR(15)     NOT NULL,
        TEXTO               NVARCHAR(MAX)   NULL,
        FL_INTERNA          BIT             NOT NULL DEFAULT 0,
        PC_CONCLUSAO        INT             NULL,
        QT_HORAS            DECIMAL(9,2)    NULL,
        STATUS_ANTERIOR     VARCHAR(30)     NULL,
        STATUS_NOVO         VARCHAR(30)     NULL,
        ID_USUARIO          INT             NULL,
        NO_USUARIO          NVARCHAR(100)   NULL,
        CREATED_AT          DATETIME        NOT NULL DEFAULT GETDATE(),
        UPDATED_AT          DATETIME        NULL,
        DELETED_AT          DATETIME        NULL,

        CONSTRAINT PK_SOL_TB003 PRIMARY KEY (ID_INTERACAO),
        CONSTRAINT FK_SOL_TB003_SOL FOREIGN KEY (ID_SOLICITACAO)
            REFERENCES BDG.SOL_TB001_SOLICITACOES (ID_SOLICITACAO)
    );

    CREATE INDEX IX_SOL_TB003_SOL     ON BDG.SOL_TB003_INTERACOES (ID_SOLICITACAO, CREATED_AT);
    CREATE INDEX IX_SOL_TB003_USUARIO ON BDG.SOL_TB003_INTERACOES (ID_USUARIO);
END
GO

/* ---------------------------------------------------------------------
   SOL_TB004_ITENS_ESCOPO
   --------------------------------------------------------------------- */
IF OBJECT_ID('BDG.SOL_TB004_ITENS_ESCOPO', 'U') IS NULL
BEGIN
    CREATE TABLE BDG.SOL_TB004_ITENS_ESCOPO (
        ID_ITEM                 INT IDENTITY(1,1) NOT NULL,   -- *chave
        ID_SOLICITACAO          INT             NOT NULL,
        DESCRICAO               NVARCHAR(500)   NOT NULL,
        NU_ORDEM                INT             NOT NULL DEFAULT 0,
        FL_CONCLUIDO            BIT             NOT NULL DEFAULT 0,
        DT_CONCLUSAO            DATETIME        NULL,
        ID_USUARIO_CONCLUSAO    INT             NULL,
        NO_USUARIO_CONCLUSAO    NVARCHAR(100)   NULL,
        ID_USUARIO_CRIACAO      INT             NULL,
        CREATED_AT              DATETIME        NOT NULL DEFAULT GETDATE(),
        DELETED_AT              DATETIME        NULL,

        CONSTRAINT PK_SOL_TB004 PRIMARY KEY (ID_ITEM),
        CONSTRAINT FK_SOL_TB004_SOL FOREIGN KEY (ID_SOLICITACAO)
            REFERENCES BDG.SOL_TB001_SOLICITACOES (ID_SOLICITACAO)
    );

    CREATE INDEX IX_SOL_TB004_SOL ON BDG.SOL_TB004_ITENS_ESCOPO (ID_SOLICITACAO);
END
GO

/* ---------------------------------------------------------------------
   SOL_TB005_ARQUIVOS
   TIPO: EVIDENCIA (e-mail/print do pedido), ANEXO, TERMO_GERADO,
         TERMO_ASSINADO
   --------------------------------------------------------------------- */
IF OBJECT_ID('BDG.SOL_TB005_ARQUIVOS', 'U') IS NULL
BEGIN
    CREATE TABLE BDG.SOL_TB005_ARQUIVOS (
        ID_ARQUIVO          INT IDENTITY(1,1) NOT NULL,   -- *chave
        ID_SOLICITACAO      INT             NOT NULL,
        TIPO                VARCHAR(20)     NOT NULL,
        NO_ARQUIVO          NVARCHAR(255)   NOT NULL,
        DS_CONTENT_TYPE     VARCHAR(150)    NULL,
        QT_BYTES            INT             NULL,
        CONTEUDO            VARBINARY(MAX)  NOT NULL,
        HASH_SHA256         CHAR(64)        NULL,
        NU_VERSAO           INT             NULL,
        FL_INTERNO          BIT             NOT NULL DEFAULT 0,
        DS_OBSERVACAO       NVARCHAR(300)   NULL,
        ID_USUARIO          INT             NULL,
        NO_USUARIO          NVARCHAR(100)   NULL,
        CREATED_AT          DATETIME        NOT NULL DEFAULT GETDATE(),
        DELETED_AT          DATETIME        NULL,

        CONSTRAINT PK_SOL_TB005 PRIMARY KEY (ID_ARQUIVO),
        CONSTRAINT FK_SOL_TB005_SOL FOREIGN KEY (ID_SOLICITACAO)
            REFERENCES BDG.SOL_TB001_SOLICITACOES (ID_SOLICITACAO)
    );

    CREATE INDEX IX_SOL_TB005_SOL ON BDG.SOL_TB005_ARQUIVOS (ID_SOLICITACAO, TIPO);
END
GO

/* ---------------------------------------------------------------------
   SOL_TB006_HISTORICO
   --------------------------------------------------------------------- */
IF OBJECT_ID('BDG.SOL_TB006_HISTORICO', 'U') IS NULL
BEGIN
    CREATE TABLE BDG.SOL_TB006_HISTORICO (
        ID_HISTORICO        INT IDENTITY(1,1) NOT NULL,   -- *chave
        ID_SOLICITACAO      INT             NOT NULL,
        CAMPO               VARCHAR(60)     NOT NULL,
        VALOR_ANTERIOR      NVARCHAR(MAX)   NULL,
        VALOR_NOVO          NVARCHAR(MAX)   NULL,
        ID_USUARIO          INT             NULL,
        NO_USUARIO          NVARCHAR(100)   NULL,
        CREATED_AT          DATETIME        NOT NULL DEFAULT GETDATE(),

        CONSTRAINT PK_SOL_TB006 PRIMARY KEY (ID_HISTORICO),
        CONSTRAINT FK_SOL_TB006_SOL FOREIGN KEY (ID_SOLICITACAO)
            REFERENCES BDG.SOL_TB001_SOLICITACOES (ID_SOLICITACAO)
    );

    CREATE INDEX IX_SOL_TB006_SOL ON BDG.SOL_TB006_HISTORICO (ID_SOLICITACAO, CREATED_AT);
END
GO
