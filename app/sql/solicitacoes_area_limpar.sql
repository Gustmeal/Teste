/* =====================================================================
   SOLICITAÇÕES À ÁREA - LIMPAR TODOS OS DADOS (uso em teste)
   Arquivo: app/sql/solicitacoes_area_limpar.sql

   ATENÇÃO: apaga TODAS as solicitações, comentários, itens, arquivos e
   históricos do aplicativo e zera a numeração dos IDs. As tabelas
   continuam existindo. Não tem como desfazer.

   Ordem: primeiro as tabelas filhas (TB002..TB006), depois a TB001,
   por causa das chaves estrangeiras.
   ===================================================================== */

USE BDDASHBOARDBI;
GO

BEGIN TRY
    BEGIN TRANSACTION;

    DELETE FROM BDG.SOL_TB006_HISTORICO;
    DELETE FROM BDG.SOL_TB005_ARQUIVOS;
    DELETE FROM BDG.SOL_TB004_ITENS_ESCOPO;
    DELETE FROM BDG.SOL_TB003_INTERACOES;
    DELETE FROM BDG.SOL_TB002_ENVOLVIDOS;
    DELETE FROM BDG.SOL_TB001_SOLICITACOES;

    -- Zera os IDENTITY: o próximo registro volta a ser o ID 1
    DBCC CHECKIDENT ('BDG.SOL_TB006_HISTORICO',    RESEED, 0);
    DBCC CHECKIDENT ('BDG.SOL_TB005_ARQUIVOS',     RESEED, 0);
    DBCC CHECKIDENT ('BDG.SOL_TB004_ITENS_ESCOPO', RESEED, 0);
    DBCC CHECKIDENT ('BDG.SOL_TB003_INTERACOES',   RESEED, 0);
    DBCC CHECKIDENT ('BDG.SOL_TB002_ENVOLVIDOS',   RESEED, 0);
    DBCC CHECKIDENT ('BDG.SOL_TB001_SOLICITACOES', RESEED, 0);

    COMMIT TRANSACTION;
    PRINT 'Tabelas do Solicitações à Área limpas.';
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    PRINT 'Erro: ' + ERROR_MESSAGE() + ' - nada foi apagado.';
END CATCH;
GO

/* Conferência: tudo deve voltar 0 */
SELECT 'SOL_TB001_SOLICITACOES' AS TABELA, COUNT(*) AS QTD FROM BDG.SOL_TB001_SOLICITACOES
UNION ALL SELECT 'SOL_TB002_ENVOLVIDOS',   COUNT(*) FROM BDG.SOL_TB002_ENVOLVIDOS
UNION ALL SELECT 'SOL_TB003_INTERACOES',   COUNT(*) FROM BDG.SOL_TB003_INTERACOES
UNION ALL SELECT 'SOL_TB004_ITENS_ESCOPO', COUNT(*) FROM BDG.SOL_TB004_ITENS_ESCOPO
UNION ALL SELECT 'SOL_TB005_ARQUIVOS',     COUNT(*) FROM BDG.SOL_TB005_ARQUIVOS
UNION ALL SELECT 'SOL_TB006_HISTORICO',    COUNT(*) FROM BDG.SOL_TB006_HISTORICO;
GO
