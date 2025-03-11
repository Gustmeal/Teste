# teste_conexao.py
import pyodbc

try:
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 18 for SQL Server};'
        'SERVER=AMON;'
        'DATABASE=BDDASHBOARDBI;'
        'Trusted_Connection=yes;'
        'TrustServerCertificate=yes;'
    )
    cursor = conn.cursor()
    cursor.execute("SELECT SUSER_NAME()")  # Mostrará seu usuário do Windows
    print("Usuário conectado:", cursor.fetchone()[0])

    # Verificar se a tabela existe
    cursor.execute(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'DEV' AND TABLE_NAME = 'DCA_TB000_USUARIOS'")
    table_exists = cursor.fetchone()[0]

    if table_exists:
        print("Tabela de usuários existe.")

        # Listar usuários
        cursor.execute("SELECT ID, NOME, EMAIL, PERFIL, ATIVO FROM DEV.DCA_TB000_USUARIOS")
        users = cursor.fetchall()

        if users:
            print(f"Total de usuários: {len(users)}")
            for user in users:
                print(f"ID: {user[0]}, Nome: {user[1]}, Email: {user[2]}, Perfil: {user[3]}, Ativo: {user[4]}")
        else:
            print("Não há usuários cadastrados.")
    else:
        print("Tabela de usuários não existe.")

except Exception as e:
    print("Erro:", e)