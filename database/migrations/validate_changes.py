#!/usr/bin/env python3
"""
Validação Simples da Alteração do Modelo
========================================

Script para validar se a alteração da coluna DT_DESCREDENCIAMENTO
foi aplicada corretamente no arquivo do modelo.
"""

import os
import re

def validate_model_file():
    """Valida se o arquivo do modelo contém as alterações corretas."""
    
    print("=" * 60)
    print("VALIDAÇÃO DO ARQUIVO EMPRESA_PARTICIPANTE.PY")
    print("=" * 60)
    
    model_path = "/home/runner/work/Teste/Teste/app/models/empresa_participante.py"
    
    if not os.path.exists(model_path):
        print(f"✗ Arquivo não encontrado: {model_path}")
        return False
    
    with open(model_path, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # Verificar se a linha correta está presente
    datetime_pattern = r'DT_DESCREDENCIAMENTO\s*=\s*db\.Column\(db\.DateTime'
    date_pattern = r'DT_DESCREDENCIAMENTO\s*=\s*db\.Column\(db\.Date\b'
    
    if re.search(datetime_pattern, content):
        print("✓ Coluna DT_DESCREDENCIAMENTO definida como db.DateTime")
        
        # Verificar se não há mais referência ao tipo Date
        if re.search(date_pattern, content):
            print("✗ Ainda existe definição como db.Date (conflito)")
            return False
        else:
            print("✓ Nenhuma definição conflitante como db.Date encontrada")
            
        # Verificar nullable=True
        nullable_pattern = r'DT_DESCREDENCIAMENTO.*nullable\s*=\s*True'
        if re.search(nullable_pattern, content):
            print("✓ Coluna permite valores NULL (nullable=True)")
        else:
            print("? Verificação de nullable não foi possível determinar")
        
        return True
        
    elif re.search(date_pattern, content):
        print("✗ Coluna ainda definida como db.Date (requer correção)")
        return False
    else:
        print("✗ Coluna DT_DESCREDENCIAMENTO não encontrada")
        return False

def validate_sql_scripts():
    """Valida se os scripts SQL foram criados corretamente."""
    
    print("\n" + "=" * 60)
    print("VALIDAÇÃO DOS SCRIPTS SQL")
    print("=" * 60)
    
    migrations_dir = "/home/runner/work/Teste/Teste/database/migrations"
    
    required_files = [
        "000_verify_dt_descredenciamento_column.sql",
        "001_alter_dt_descredenciamento_to_datetime.sql",
        "migrate_dt_descredenciamento.py",
        "README.md"
    ]
    
    all_files_exist = True
    
    for filename in required_files:
        filepath = os.path.join(migrations_dir, filename)
        if os.path.exists(filepath):
            print(f"✓ {filename}")
            
            # Verificação específica para o script principal
            if filename == "001_alter_dt_descredenciamento_to_datetime.sql":
                with open(filepath, 'r', encoding='utf-8') as file:
                    content = file.read()
                    
                if "ALTER COLUMN" in content and "DATETIME" in content:
                    print(f"  ✓ Contém comando ALTER COLUMN para DATETIME")
                else:
                    print(f"  ✗ Script pode estar incompleto")
                    
        else:
            print(f"✗ {filename} - AUSENTE")
            all_files_exist = False
    
    return all_files_exist

def main():
    """Função principal da validação."""
    
    print("Iniciando validação das alterações...\n")
    
    # Teste 1: Validar arquivo do modelo
    model_valid = validate_model_file()
    
    # Teste 2: Validar scripts SQL
    scripts_valid = validate_sql_scripts()
    
    # Resultado final
    print("\n" + "=" * 60)
    print("RESULTADO DA VALIDAÇÃO")
    print("=" * 60)
    
    if model_valid and scripts_valid:
        print("✓ VALIDAÇÃO APROVADA")
        print("✓ Todas as alterações foram aplicadas corretamente")
        print("\nPróximos passos:")
        print("1. Execute o script de migração SQL no banco de dados")
        print("2. Teste a aplicação para confirmar funcionamento")
        return 0
    else:
        print("✗ VALIDAÇÃO FALHADA")
        if not model_valid:
            print("  - Modelo requer correção")
        if not scripts_valid:
            print("  - Scripts SQL requerem verificação")
        return 1

if __name__ == "__main__":
    exit(main())