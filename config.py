import os
import secrets


class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or secrets.token_hex(32)

    # String de conexão (ajuste o nome do banco se necessário)
    SQLALCHEMY_DATABASE_URI = (
        "mssql+pyodbc://@AMON/BDDASHBOARDBI?"
        "driver=ODBC+Driver+18+for+SQL+Server"
        "&Trusted_Connection=yes"
        "&TrustServerCertificate=yes"
    )

    SQLALCHEMY_TRACK_MODIFICATIONS = False  # Otimização de performance