/*
===============================================================================
SCRIPT DE VERIFICAÇÃO DA COLUNA DT_DESCREDENCIAMENTO
===============================================================================
Descrição: Script para verificar o status da coluna DT_DESCREDENCIAMENTO
Tabela: [BDDASHBOARDBI].[BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES]
Uso: Execute este script para verificar se a coluna existe e seu tipo
===============================================================================
*/

USE BDDASHBOARDBI;
GO

PRINT '===============================================================================';
PRINT 'VERIFICAÇÃO DA COLUNA DT_DESCREDENCIAMENTO';
PRINT '===============================================================================';

-- Verificar se a tabela existe
IF EXISTS (
    SELECT 1 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'BDG' 
    AND TABLE_NAME = 'DCA_TB002_EMPRESAS_PARTICIPANTES'
)
BEGIN
    PRINT 'TABELA: [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES] - ENCONTRADA';
    
    -- Verificar se a coluna existe
    IF EXISTS (
        SELECT 1 
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'BDG' 
        AND TABLE_NAME = 'DCA_TB002_EMPRESAS_PARTICIPANTES'
        AND COLUMN_NAME = 'DT_DESCREDENCIAMENTO'
    )
    BEGIN
        PRINT 'COLUNA: DT_DESCREDENCIAMENTO - ENCONTRADA';
        PRINT '';
        PRINT 'DETALHES DA COLUNA:';
        
        -- Mostrar detalhes da coluna
        SELECT 
            'DT_DESCREDENCIAMENTO' as COLUNA,
            DATA_TYPE as TIPO_DADOS,
            CASE 
                WHEN IS_NULLABLE = 'YES' THEN 'SIM'
                ELSE 'NÃO'
            END as PERMITE_NULL,
            COALESCE(COLUMN_DEFAULT, 'NULL') as VALOR_PADRAO
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE 
            TABLE_SCHEMA = 'BDG' 
            AND TABLE_NAME = 'DCA_TB002_EMPRESAS_PARTICIPANTES'
            AND COLUMN_NAME = 'DT_DESCREDENCIAMENTO';
        
        -- Verificar se está conforme os requisitos
        DECLARE @DataType NVARCHAR(128);
        SELECT @DataType = DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'BDG' 
        AND TABLE_NAME = 'DCA_TB002_EMPRESAS_PARTICIPANTES'
        AND COLUMN_NAME = 'DT_DESCREDENCIAMENTO';
        
        PRINT '';
        IF @DataType = 'datetime'
        BEGIN
            PRINT 'STATUS: ✓ CONFORME - Coluna está definida como DATETIME';
        END
        ELSE
        BEGIN
            PRINT 'STATUS: ✗ NÃO CONFORME - Coluna deveria ser DATETIME, mas está como ' + @DataType;
            PRINT 'AÇÃO REQUERIDA: Execute o script de migração 001_alter_dt_descredenciamento_to_datetime.sql';
        END
    END
    ELSE
    BEGIN
        PRINT 'COLUNA: DT_DESCREDENCIAMENTO - NÃO ENCONTRADA';
        PRINT 'STATUS: ✗ COLUNA AUSENTE';
        PRINT 'AÇÃO REQUERIDA: Execute o script de migração 001_alter_dt_descredenciamento_to_datetime.sql';
    END
END
ELSE
BEGIN
    PRINT 'ERRO: Tabela [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES] não encontrada';
    PRINT 'Verifique se você está conectado ao banco correto';
END

PRINT '===============================================================================';
PRINT 'VERIFICAÇÃO CONCLUÍDA';
PRINT '===============================================================================';