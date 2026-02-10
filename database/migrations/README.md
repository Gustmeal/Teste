# Migração da Coluna DT_DESCREDENCIAMENTO

## Descrição

Este diretório contém os scripts necessários para a alteração da coluna `DT_DESCREDENCIAMENTO` na tabela `[BDDASHBOARDBI].[BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES]`.

### Alteração Realizada

- **Coluna**: `DT_DESCREDENCIAMENTO`
- **Tipo anterior**: `DATE`
- **Tipo novo**: `DATETIME`
- **Valor padrão**: `NULL`
- **Permite valores nulos**: Sim

## Arquivos

### Scripts SQL

1. **`000_verify_dt_descredenciamento_column.sql`**
   - Script de verificação da coluna
   - Verifica se a coluna existe e seu tipo atual
   - Não faz alterações no banco

2. **`001_alter_dt_descredenciamento_to_datetime.sql`**
   - Script principal de migração
   - Altera o tipo da coluna de DATE para DATETIME
   - Inclui verificações de segurança
   - Cria a coluna se ela não existir

### Script Python

3. **`migrate_dt_descredenciamento.py`**
   - Script automatizado para execução da migração
   - Permite verificação sem alterações (`--verify-only`)
   - Executa os scripts SQL automaticamente
   - Fornece feedback detalhado do processo

## Como Usar

### Opção 1: Execução Manual dos Scripts SQL

1. **Verificação (opcional):**
   ```sql
   -- Execute no SQL Server Management Studio ou ferramenta similar
   -- Arquivo: 000_verify_dt_descredenciamento_column.sql
   ```

2. **Migração:**
   ```sql
   -- Execute no SQL Server Management Studio ou ferramenta similar
   -- Arquivo: 001_alter_dt_descredenciamento_to_datetime.sql
   ```

### Opção 2: Execução Automatizada com Python

1. **Apenas verificação:**
   ```bash
   python migrate_dt_descredenciamento.py --verify-only
   ```

2. **Migração completa:**
   ```bash
   python migrate_dt_descredenciamento.py
   ```

## Pré-requisitos

### Para Scripts SQL
- Acesso ao SQL Server com permissões para:
  - Consultar `INFORMATION_SCHEMA`
  - Executar `ALTER TABLE`
- Conexão com o banco `BDDASHBOARDBI`

### Para Script Python
- Python 3.x instalado
- Biblioteca `pyodbc` instalada
- Driver ODBC para SQL Server (ODBC Driver 18 for SQL Server)
- Acesso ao servidor `AMON` com autenticação Windows

## Verificações de Segurança

Os scripts incluem as seguintes verificações:

1. **Verificação de existência da tabela**
2. **Verificação de existência da coluna**
3. **Verificação do tipo atual da coluna**
4. **Prevenção de alterações desnecessárias**
5. **Feedback detalhado do processo**

## Impacto da Migração

### ✅ Impactos Positivos
- A coluna passará a suportar informações de hora além da data
- Maior precisão temporal para registros de descredenciamento
- Compatibilidade com o modelo SQLAlchemy atualizado

### ⚠️ Considerações
- **Dados existentes**: Mantidos intactos (valores NULL permanecem NULL)
- **Aplicação**: Requer atualização do modelo SQLAlchemy (já realizada)
- **Performance**: Impacto mínimo, pois a alteração é de tipo compatível

## Rollback

Se necessário, é possível reverter a alteração:

```sql
-- APENAS SE NECESSÁRIO - Reverter para DATE
ALTER TABLE [BDG].[DCA_TB002_EMPRESAS_PARTICIPANTES]
ALTER COLUMN [DT_DESCREDENCIAMENTO] DATE NULL;
```

⚠️ **Atenção**: O rollback pode causar perda de informações de hora se houver dados com horário específico.

## Status da Migração

- [x] Scripts SQL criados
- [x] Script Python de automação criado
- [x] Modelo SQLAlchemy atualizado
- [x] Documentação completa
- [ ] Migração executada em produção (pendente)

## Contato

Para dúvidas sobre esta migração, consulte a documentação do projeto ou entre em contato com a equipe de desenvolvimento.