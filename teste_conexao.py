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
except Exception as e:
    print("Erro:", e)