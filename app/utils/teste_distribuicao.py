from app import create_app, db
from sqlalchemy import text
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Importar a função a ser testada
from app.utils.distribuir_contratos import aplicar_regra_arrasto_sem_acordo


def testar_arrasto_sem_acordo():
    """
    Testa a função de arrasto sem modificar o banco de dados permanentemente.
    """
    print("Iniciando teste da função aplicar_regra_arrasto_sem_acordo...")

    # Parâmetros para o teste
    edital_id = 1  # Substitua pelo ID válido no seu ambiente
    periodo_id = 6  # Substitua pelo ID válido no seu ambiente

    # Verificar se existem dados disponíveis para teste
    contagem_distribuiveis = db.session.execute(
        text("SELECT COUNT(*) FROM [BDG].[DCA_TB006_DISTRIBUIVEIS]")
    ).scalar() or 0

    print(f"Contratos disponíveis para teste: {contagem_distribuiveis}")

    if contagem_distribuiveis == 0:
        print("ALERTA: Não há contratos na tabela de distribuíveis para testar")
        return

    try:
        # NÃO use db.session.begin() - isso já acontece automaticamente
        print("Iniciando teste com rollback ao final")

        # Criar um savepoint para poder reverter depois
        db.session.begin_nested()

        # Executar a função
        resultados = aplicar_regra_arrasto_sem_acordo(edital_id, periodo_id)

        # Exibir resultados
        print("\n=== RESULTADOS DO TESTE ===")
        print(f"Contratos movidos para arrastaveis: {resultados['inseridos_arrastaveis']}")
        print(f"CPFs processados: {resultados['cpfs_processados']}")
        print(f"Contratos distribuídos: {resultados['distribuidos']}")

        # Reverter todas as alterações
        print("\nRevertendo alterações (rollback da transação)...")
        db.session.rollback()
        print("Rollback concluído com sucesso")

    except Exception as e:
        # Garantir rollback em caso de erro
        db.session.rollback()
        print(f"ERRO NO TESTE: {str(e)}")
        import traceback
        print(traceback.format_exc())


if __name__ == "__main__":
    # Criar contexto da aplicação
    app = create_app()

    with app.app_context():
        testar_arrasto_sem_acordo()