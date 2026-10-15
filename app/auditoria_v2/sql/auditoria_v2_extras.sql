/* =====================================================================
   Auditoria V2 - scripts OPCIONAIS (SQL Server)
   Arquivo: app/auditoria_v2/sql/auditoria_v2_extras.sql

   As tabelas AUD_TB001..AUD_TB007 são criadas AUTOMATICAMENTE pelo
   db.create_all() quando o portal sobe. Este arquivo NÃO é necessário
   para a auditoria funcionar; ele traz extras para o DBA:

     1) View que junta a auditoria antiga (APK_TB001) com a V2
     2) Procedure de limpeza por retenção (parâmetro DIAS_RETENCAO)
     3) Endurecimento: impedir UPDATE/DELETE nas tabelas de auditoria
   ===================================================================== */

USE BDDASHBOARDBI;
GO

/* ---------------------------------------------------------------------
   1) Linha do tempo única: legado + V2
   Obs.: a auditoria antiga gravava DATA em UTC (datetime.utcnow);
         aqui é convertida para horário de Brasília (UTC-3).
   --------------------------------------------------------------------- */
CREATE OR ALTER VIEW BDG.AUD_VW001_LINHA_DO_TEMPO AS
SELECT
    'LEGADO'                          AS ORIGEM,
    CAST(L.ID AS VARCHAR(32))         AS ID_REFERENCIA,
    DATEADD(HOUR, -3, L.DATA)         AS DT_ACAO,
    L.USUARIO_ID,
    L.USUARIO_NOME,
    L.ENTIDADE                        AS APLICATIVO,
    UPPER(L.ACAO)                     AS TIPO_ACAO,
    L.DESCRICAO,
    L.IP
FROM BDG.APK_TB001_AUDIT_LOG L
UNION ALL
SELECT
    'V2_ACAO',
    R.ID_CORRELACAO,
    R.DT_INICIO,
    R.USUARIO_ID,
    R.USUARIO_NOME,
    R.APLICATIVO,
    R.TIPO_ACAO,
    CONCAT(R.METODO, ' ', R.URL),
    R.IP
FROM BDG.AUD_TB001_REQUISICAO R
UNION ALL
SELECT
    'V2_EVENTO',
    E.ID_CORRELACAO,
    E.DT_EVENTO,
    E.USUARIO_ID,
    E.USUARIO_NOME,
    E.APLICATIVO,
    E.TIPO,
    E.DESCRICAO,
    E.IP
FROM BDG.AUD_TB005_EVENTO E;
GO

/* ---------------------------------------------------------------------
   2) Limpeza por retenção (lê DIAS_RETENCAO em AUD_TB007_CONFIG).
      DIAS_RETENCAO = 0  -> não apaga nada.
      Apaga em lotes de 5.000 para não travar o banco.
      Agendar no SQL Agent (ex.: todo domingo de madrugada).
   --------------------------------------------------------------------- */
CREATE OR ALTER PROCEDURE BDG.AUD_SP001_LIMPEZA
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @dias INT = TRY_CAST((SELECT VALOR FROM BDG.AUD_TB007_CONFIG WHERE CHAVE = 'DIAS_RETENCAO') AS INT);
    IF @dias IS NULL OR @dias <= 0
        RETURN;

    DECLARE @corte DATETIME2 = DATEADD(DAY, -@dias, SYSDATETIME());
    DECLARE @n INT = 1;

    WHILE @n > 0 BEGIN DELETE TOP (5000) FROM BDG.AUD_TB003_ALTERACAO_CAMPO WHERE DT_ALTERACAO < @corte; SET @n = @@ROWCOUNT; END
    SET @n = 1;
    WHILE @n > 0 BEGIN DELETE TOP (5000) FROM BDG.AUD_TB002_ALTERACAO      WHERE DT_ALTERACAO < @corte; SET @n = @@ROWCOUNT; END
    SET @n = 1;
    WHILE @n > 0 BEGIN DELETE TOP (5000) FROM BDG.AUD_TB004_COMANDO_SQL    WHERE DT_EXECUCAO  < @corte; SET @n = @@ROWCOUNT; END
    SET @n = 1;
    WHILE @n > 0 BEGIN DELETE TOP (5000) FROM BDG.AUD_TB005_EVENTO         WHERE DT_EVENTO    < @corte; SET @n = @@ROWCOUNT; END
    SET @n = 1;
    WHILE @n > 0 BEGIN DELETE TOP (5000) FROM BDG.AUD_TB001_REQUISICAO     WHERE DT_INICIO    < @corte; SET @n = @@ROWCOUNT; END
END;
GO

/* ---------------------------------------------------------------------
   3) Endurecimento (OPCIONAL - executar só com o DBA)
      Impede que a conta usada pelo portal altere ou apague a auditoria.
      Troque [DOMINIO\conta_do_servico] pela conta Windows que roda o portal.
      ATENÇÃO: a procedure de limpeza deve então rodar com outra conta (SQL Agent).
      AUD_TB006 e AUD_TB007 ficam de fora porque são editadas pela tela de configurações.
   --------------------------------------------------------------------- */
/*
DENY UPDATE, DELETE ON BDG.AUD_TB001_REQUISICAO      TO [DOMINIO\conta_do_servico];
DENY UPDATE, DELETE ON BDG.AUD_TB002_ALTERACAO       TO [DOMINIO\conta_do_servico];
DENY UPDATE, DELETE ON BDG.AUD_TB003_ALTERACAO_CAMPO TO [DOMINIO\conta_do_servico];
DENY UPDATE, DELETE ON BDG.AUD_TB004_COMANDO_SQL     TO [DOMINIO\conta_do_servico];
DENY UPDATE, DELETE ON BDG.AUD_TB005_EVENTO          TO [DOMINIO\conta_do_servico];
*/
