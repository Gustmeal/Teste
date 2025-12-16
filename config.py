import os
import secrets


class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or secrets.token_hex(32)

    # String de conexão
    SQLALCHEMY_DATABASE_URI = (
        "mssql+pyodbc://@AMON/BDDASHBOARDBI?"
        "driver=ODBC+Driver+17+for+SQL+Server"
        "&Trusted_Connection=yes"
        "&TrustServerCertificate=yes"
    )

    SQLALCHEMY_TRACK_MODIFICATIONS = False  # Otimização de performance

    # ===== CONFIGURAÇÕES DE POOL DE CONEXÕES =====
    # Testa conexões antes de usá-las para evitar conexões "stale"
    SQLALCHEMY_ENGINE_OPTIONS = {
        'pool_pre_ping': True,  # Testa conexão antes de usar
        'pool_recycle': 3600,  # Recicla conexões a cada 1 hora (3600 segundos)
        'pool_size': 10,  # Tamanho do pool (10 conexões simultâneas)
        'max_overflow': 20,  # Permite até 20 conexões extras em picos
        'pool_timeout': 30,  # Timeout de 30 segundos ao aguardar conexão
        'echo_pool': False,  # Não logar operações de pool (mude para True se quiser debug)
    }