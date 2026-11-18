#!/usr/bin/env python3
"""
Script de Migração para a Coluna DT_DESCREDENCIAMENTO
====================================================

Este script executa a migração da coluna DT_DESCREDENCIAMENTO 
de DATE para DATETIME na tabela DCA_TB002_EMPRESAS_PARTICIPANTES.

Uso:
    python migrate_dt_descredenciamento.py [--verify-only]

Opções:
    --verify-only    Apenas verifica o status da coluna sem fazer alterações
"""

import os
import sys
import argparse
import pyodbc
from pathlib import Path


def get_database_connection():
    """Estabelece conexão com o banco de dados."""
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 18 for SQL Server};'
            'SERVER=AMON;'
            'DATABASE=BDDASHBOARDBI;'
            'Trusted_Connection=yes;'
            'TrustServerCertificate=yes;'
        )
        return conn
    except Exception as e:
        print(f"Erro ao conectar com o banco de dados: {e}")
        return None


def execute_sql_file(conn, sql_file_path):
    """Executa um arquivo SQL."""
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as file:
            sql_content = file.read()
        
        cursor = conn.cursor()
        
        # Dividir o SQL em comandos individuais (por GO)
        commands = []
        current_command = []
        
        for line in sql_content.split('\n'):
            line = line.strip()
            if line.upper() == 'GO':
                if current_command:
                    commands.append('\n'.join(current_command))
                    current_command = []
            else:
                current_command.append(line)
        
        # Adicionar o último comando se existir
        if current_command:
            commands.append('\n'.join(current_command))
        
        # Executar cada comando
        for command in commands:
            command = command.strip()
            if command and not command.startswith('--'):
                cursor.execute(command)
                
                # Capturar mensagens do servidor
                while cursor.nextset():
                    pass
        
        conn.commit()
        print("Script SQL executado com sucesso.")
        return True
        
    except Exception as e:
        print(f"Erro ao executar script SQL: {e}")
        conn.rollback()
        return False


def verify_column_status(conn):
    """Verifica o status atual da coluna DT_DESCREDENCIAMENTO."""
    try:
        cursor = conn.cursor()
        
        # Verificar se a coluna existe e seu tipo
        cursor.execute("""
            SELECT 
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE 
                TABLE_SCHEMA = 'BDG' 
                AND TABLE_NAME = 'DCA_TB002_EMPRESAS_PARTICIPANTES'
                AND COLUMN_NAME = 'DT_DESCREDENCIAMENTO'
        """)
        
        result = cursor.fetchone()
        
        if result:
            data_type, is_nullable, default_value = result
            print(f"✓ Coluna DT_DESCREDENCIAMENTO encontrada:")
            print(f"  - Tipo: {data_type}")
            print(f"  - Permite NULL: {is_nullable}")
            print(f"  - Valor padrão: {default_value or 'NULL'}")
            
            if data_type.lower() == 'datetime':
                print("✓ Coluna está conforme os requisitos (DATETIME)")
                return True
            else:
                print(f"✗ Coluna deveria ser DATETIME, mas está como {data_type}")
                return False
        else:
            print("✗ Coluna DT_DESCREDENCIAMENTO não encontrada")
            return False
            
    except Exception as e:
        print(f"Erro ao verificar status da coluna: {e}")
        return False


def main():
    """Função principal."""
    parser = argparse.ArgumentParser(
        description='Script de migração para coluna DT_DESCREDENCIAMENTO'
    )
    parser.add_argument(
        '--verify-only', 
        action='store_true',
        help='Apenas verifica o status da coluna sem fazer alterações'
    )
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("MIGRAÇÃO DA COLUNA DT_DESCREDENCIAMENTO")
    print("=" * 80)
    
    # Conectar ao banco de dados
    print("Conectando ao banco de dados...")
    conn = get_database_connection()
    if not conn:
        print("Falha na conexão. Abortando.")
        return 1
    
    print("Conexão estabelecida com sucesso.")
    
    try:
        if args.verify_only:
            print("\n🔍 MODO VERIFICAÇÃO - Apenas verificando status da coluna")
            print("-" * 60)
            success = verify_column_status(conn)
            
        else:
            print("\n🔧 MODO MIGRAÇÃO - Executando alterações necessárias")
            print("-" * 60)
            
            # Primeiro verificar o status atual
            print("1. Verificando status atual da coluna...")
            verify_column_status(conn)
            
            # Executar o script de migração
            print("\n2. Executando script de migração...")
            script_dir = Path(__file__).parent
            migration_script = script_dir / "001_alter_dt_descredenciamento_to_datetime.sql"
            
            if not migration_script.exists():
                print(f"Erro: Script de migração não encontrado: {migration_script}")
                return 1
            
            success = execute_sql_file(conn, migration_script)
            
            if success:
                print("\n3. Verificando resultado da migração...")
                verify_column_status(conn)
        
        print("\n" + "=" * 80)
        if success:
            print("✓ OPERAÇÃO CONCLUÍDA COM SUCESSO")
        else:
            print("✗ OPERAÇÃO FALHOU")
        print("=" * 80)
        
        return 0 if success else 1
        
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())