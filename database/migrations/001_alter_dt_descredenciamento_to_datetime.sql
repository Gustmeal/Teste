/*
===============================================================================
SCRIPT DE MIGRAÇÃO DE BANCO DE DADOS
===============================================================================
Descrição: Alteração da coluna DT_DESCREDENCIAMENTO de DATE para DATETIME
Tabela: [BDDASHBOARDBI].[BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES]
Autor: Sistema
Data: 2024
===============================================================================
*/

USE BDDASHBOARDBI;
GO

-- Verificar se a coluna existe e seu tipo atual
DECLARE @ColumnExists BIT = 0;
DECLARE @CurrentDataType NVARCHAR(128);

SELECT 
    @ColumnExists = 1,
    @CurrentDataType = DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE 
    TABLE_SCHEMA = 'BDG' 
    AND TABLE_NAME = 'DCA_TB002_EMPRESAS_PARTICIPANTES'
    AND COLUMN_NAME = 'DT_DESCREDENCIAMENTO';

-- Exibir informações sobre o estado atual da coluna
IF @ColumnExists = 1
BEGIN
    PRINT 'INFORMAÇÃO: Coluna DT_DESCREDENCIAMENTO encontrada.';
    PRINT 'Tipo atual: ' + @CurrentDataType;
    
    -- Verificar se a coluna já é do tipo DATETIME
    IF @CurrentDataType = 'datetime'
    BEGIN
        PRINT 'AVISO: A coluna já está definida como DATETIME. Nenhuma alteração necessária.';
    END
    ELSE
    BEGIN
        PRINT 'INÍCIO: Alterando tipo da coluna de ' + @CurrentDataType + ' para DATETIME...';
        
        -- Alterar o tipo da coluna para DATETIME
        ALTER TABLE [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES]
        ALTER COLUMN [DT_DESCREDENCIAMENTO] DATETIME NULL;
        
        PRINT 'SUCESSO: Coluna DT_DESCREDENCIAMENTO alterada para DATETIME com sucesso.';
    END
END
ELSE
BEGIN
    PRINT 'INFORMAÇÃO: Coluna DT_DESCREDENCIAMENTO não existe. Criando nova coluna...';
    
    -- Adicionar a nova coluna como DATETIME
    ALTER TABLE [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES]
    ADD [DT_DESCREDENCIAMENTO] DATETIME NULL;
    
    PRINT 'SUCESSO: Coluna DT_DESCREDENCIAMENTO criada como DATETIME com sucesso.';
END

-- Verificação final do resultado
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE 
    TABLE_SCHEMA = 'BDG' 
    AND TABLE_NAME = 'DCA_TB002_EMPRESAS_PARTICIPANTES'
    AND COLUMN_NAME = 'DT_DESCREDENCIAMENTO';

PRINT '===============================================================================';
PRINT 'MIGRAÇÃO CONCLUÍDA: Verificação da estrutura da coluna acima.';
PRINT '===============================================================================';