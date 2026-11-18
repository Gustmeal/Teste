#!/usr/bin/env python3
"""
Teste de Validação do Modelo EmpresaParticipante
===============================================

Script para validar se a alteração da coluna DT_DESCREDENCIAMENTO
foi aplicada corretamente no modelo SQLAlchemy.
"""

import sys
import os
from datetime import datetime

# Adicionar o diretório raiz do projeto ao path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

try:
    from app.models.empresa_participante import EmpresaParticipante
    from sqlalchemy import DateTime, Date
    
    def test_dt_descredenciamento_column():
        """Testa se a coluna DT_DESCREDENCIAMENTO está definida corretamente."""
        
        print("=" * 60)
        print("TESTE DE VALIDAÇÃO DO MODELO EMPRESAPARTICIPANTE")
        print("=" * 60)
        
        # Verificar se a coluna existe no modelo
        if hasattr(EmpresaParticipante, 'DT_DESCREDENCIAMENTO'):
            print("✓ Coluna DT_DESCREDENCIAMENTO encontrada no modelo")
            
            # Obter informações sobre a coluna
            column = EmpresaParticipante.DT_DESCREDENCIAMENTO
            column_type = column.type
            
            print(f"  - Tipo da coluna: {type(column_type).__name__}")
            print(f"  - Permite NULL: {column.nullable}")
            print(f"  - Valor padrão: {column.default}")
            
            # Verificar se é do tipo DateTime
            if isinstance(column_type, DateTime):
                print("✓ Tipo correto: DateTime")
                return True
            elif isinstance(column_type, Date):
                print("✗ Tipo incorreto: Date (deveria ser DateTime)")
                return False
            else:
                print(f"✗ Tipo inesperado: {type(column_type).__name__}")
                return False
                
        else:
            print("✗ Coluna DT_DESCREDENCIAMENTO não encontrada no modelo")
            return False
    
    def test_model_structure():
        """Testa a estrutura geral do modelo."""
        
        print("\n" + "-" * 40)
        print("ESTRUTURA DO MODELO")
        print("-" * 40)
        
        # Listar todas as colunas do modelo
        columns = []
        for attr_name in dir(EmpresaParticipante):
            attr = getattr(EmpresaParticipante, attr_name)
            if hasattr(attr, 'type'):  # É uma coluna SQLAlchemy
                columns.append((attr_name, type(attr.type).__name__, attr.nullable))
        
        print("Colunas encontradas:")
        for name, type_name, nullable in columns:
            null_str = "NULL" if nullable else "NOT NULL"
            print(f"  - {name}: {type_name} ({null_str})")
        
        # Verificar colunas esperadas
        expected_columns = [
            'ID', 'ID_EDITAL', 'ID_PERIODO', 'ID_EMPRESA', 
            'NO_EMPRESA', 'NO_EMPRESA_ABREVIADA', 'DS_CONDICAO',
            'DT_DESCREDENCIAMENTO', 'CREATED_AT', 'UPDATED_AT', 'DELETED_AT'
        ]
        
        missing_columns = []
        for col in expected_columns:
            if not any(name == col for name, _, _ in columns):
                missing_columns.append(col)
        
        if missing_columns:
            print(f"\n✗ Colunas ausentes: {missing_columns}")
            return False
        else:
            print("\n✓ Todas as colunas esperadas estão presentes")
            return True
    
    def main():
        """Função principal do teste."""
        
        print("Iniciando testes de validação...\n")
        
        # Teste 1: Validar coluna DT_DESCREDENCIAMENTO
        test1_passed = test_dt_descredenciamento_column()
        
        # Teste 2: Validar estrutura do modelo
        test2_passed = test_model_structure()
        
        # Resultado final
        print("\n" + "=" * 60)
        print("RESULTADO DOS TESTES")
        print("=" * 60)
        
        if test1_passed and test2_passed:
            print("✓ TODOS OS TESTES PASSARAM")
            print("✓ Modelo EmpresaParticipante está conforme os requisitos")
            return 0
        else:
            print("✗ ALGUNS TESTES FALHARAM")
            if not test1_passed:
                print("  - Coluna DT_DESCREDENCIAMENTO requer correção")
            if not test2_passed:
                print("  - Estrutura do modelo requer verificação")
            return 1

except ImportError as e:
    print(f"Erro ao importar módulos: {e}")
    print("Certifique-se de que está executando do diretório correto")
    print("e que as dependências estão instaladas.")
    sys.exit(1)

if __name__ == "__main__":
    sys.exit(main())