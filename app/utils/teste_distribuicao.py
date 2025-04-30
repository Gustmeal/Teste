# Ajuste as importações conforme a estrutura real do seu projeto
from app import db  # Removi 'app' da importação
from flask import current_app  # Vamos usar current_app em vez de app diretamente
from sqlalchemy import text
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('teste_distribuicao')


def testar_etapa_especifica(edital_id=1, periodo_id=6):
    """
    Testa apenas a etapa específica que estava falhando.
    """
    try:
        logger.info(f"Iniciando teste com Edital: {edital_id}, Período: {periodo_id}")

        # Etapa 1: Limpar qualquer tabela temporária existente
        logger.info("1. Limpando tabela temporária anterior...")
        db.session.execute(
            text("IF OBJECT_ID('tempdb..##CPFsArrasteTest') IS NOT NULL DROP TABLE ##CPFsArrasteTest")
        )
        db.session.commit()

        # Etapa 2: Criar tabela temporária
        logger.info("2. Criando tabela temporária...")
        db.session.execute(
            text("""
                CREATE TABLE ##CPFsArrasteTest (
                    NR_CPF_CNPJ VARCHAR(20) PRIMARY KEY CLUSTERED
                )
            """)
        )
        db.session.commit()

        # Etapa 3: Inserir dados (parte que estava falhando)
        logger.info("3. Inserindo CPFs com múltiplos contratos...")
        db.session.execute(
            text("""
                INSERT INTO ##CPFsArrasteTest (NR_CPF_CNPJ)
                SELECT DISTINCT LTRIM(RTRIM(NR_CPF_CNPJ))
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] D
                WHERE NR_CPF_CNPJ IS NOT NULL
                GROUP BY LTRIM(RTRIM(NR_CPF_CNPJ))
                HAVING COUNT(*) > 1
            """)
        )
        db.session.commit()

        # Etapa 4: Contar resultados (separadamente)
        logger.info("4. Contando CPFs inseridos...")
        cpfs_multiplos = db.session.execute(
            text("SELECT COUNT(*) FROM ##CPFsArrasteTest")
        ).scalar() or 0

        logger.info(f"Encontrados {cpfs_multiplos} CPFs com múltiplos contratos")

        # Limpeza
        logger.info("5. Limpando tabela temporária...")
        db.session.execute(
            text("DROP TABLE ##CPFsArrasteTest")
        )
        db.session.commit()

        logger.info("Teste concluído com sucesso!")
        return True

    except Exception as e:
        db.session.rollback()
        logger.error(f"Erro durante o teste: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

        # Tentar limpar recursos mesmo em caso de erro
        try:
            db.session.execute(
                text("IF OBJECT_ID('tempdb..##CPFsArrasteTest') IS NOT NULL DROP TABLE ##CPFsArrasteTest"))
            db.session.commit()
        except:
            pass

        return False


def testar_movimento_para_arrastaveis(edital_id=1, periodo_id=6, limite=100):
    """
    Testa a movimentação de um pequeno número de contratos para arrastaveis.
    """
    try:
        logger.info(f"Iniciando teste de movimento com limite de {limite} contratos")

        # Preparar tabela temporária
        db.session.execute(text("IF OBJECT_ID('tempdb..##CPFsArrasteTest') IS NOT NULL DROP TABLE ##CPFsArrasteTest"))
        db.session.execute(
            text("""
                CREATE TABLE ##CPFsArrasteTest (
                    NR_CPF_CNPJ VARCHAR(20) PRIMARY KEY CLUSTERED
                )
            """)
        )

        # Inserir apenas alguns CPFs para teste
        db.session.execute(
            text("""
                INSERT INTO ##CPFsArrasteTest (NR_CPF_CNPJ)
                SELECT TOP(:limite) LTRIM(RTRIM(NR_CPF_CNPJ))
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS]
                WHERE NR_CPF_CNPJ IS NOT NULL
                GROUP BY LTRIM(RTRIM(NR_CPF_CNPJ))
            """),
            {"limite": limite}
        )
        db.session.commit()

        # Contar quantos contratos serão movidos
        contratos = db.session.execute(
            text("""
                SELECT COUNT(*)
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] D
                INNER JOIN ##CPFsArrasteTest M 
                    ON LTRIM(RTRIM(D.NR_CPF_CNPJ)) = M.NR_CPF_CNPJ
            """)
        ).scalar() or 0

        logger.info(f"Encontrados {contratos} contratos para processar")

        # Testar inserção na tabela de arrastaveis (sem realmente excluir da distribuíveis)
        inseridos = db.session.execute(
            text("""
                SELECT COUNT(*)
                FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] D
                INNER JOIN ##CPFsArrasteTest M 
                    ON LTRIM(RTRIM(D.NR_CPF_CNPJ)) = M.NR_CPF_CNPJ
                LEFT JOIN [DEV].[DCA_TB007_ARRASTAVEIS] A
                    ON D.FkContratoSISCTR = A.FkContratoSISCTR
                WHERE A.FkContratoSISCTR IS NULL
            """)
        ).scalar() or 0

        logger.info(f"Seriam inseridos {inseridos} contratos na tabela de arrastaveis")

        # Limpeza
        db.session.execute(text("DROP TABLE ##CPFsArrasteTest"))
        db.session.commit()

        return True

    except Exception as e:
        db.session.rollback()
        logger.error(f"Erro no teste de movimento: {str(e)}")

        # Limpar
        try:
            db.session.execute(
                text("IF OBJECT_ID('tempdb..##CPFsArrasteTest') IS NOT NULL DROP TABLE ##CPFsArrasteTest"))
            db.session.commit()
        except:
            pass

        return False


if __name__ == "__main__":
    # Importação da aplicação para criar o contexto
    # MÉTODO 1: Se você tem uma função create_app()
    try:
        from app import create_app

        app = create_app()
        with app.app_context():
            print("=== Iniciando teste da etapa específica ===")
            result1 = testar_etapa_especifica()

            print("\n=== Iniciando teste de movimento para arrastaveis ===")
            result2 = testar_movimento_para_arrastaveis(limite=10)

            if result1 and result2:
                print("\n✅ Todos os testes passaram!")
            else:
                print("\n❌ Alguns testes falharam. Verifique os logs.")
    except ImportError:
        # MÉTODO 2: Se você tem um padrão diferente de importação
        try:
            from app.factory import app  # Ajuste conforme seu projeto

            with app.app_context():
                print("=== Usando app.factory ===")
                testar_etapa_especifica()
        except ImportError:
            # MÉTODO 3: Último recurso - executar direto com o db (não recomendado)
            print("⚠️ ATENÇÃO: Não foi possível importar a aplicação Flask corretamente.")
            print("Execute este script a partir do shell do Flask:")
            print("\nNo terminal, execute:")
            print("  1. cd /caminho/do/projeto")
            print("  2. flask shell")
            print("  3. from app.utils.teste_distribuicao import testar_etapa_especifica")
            print("  4. testar_etapa_especifica()")