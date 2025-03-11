from app import create_app, db
from app.models.usuario import Usuario
from werkzeug.security import generate_password_hash, check_password_hash

app = create_app()

with app.app_context():
    # Obter o usuário admin
    admin = Usuario.query.filter_by(EMAIL='admin@emgea.com').first()

    if admin:
        print(f"Usuário encontrado: {admin.NOME} ({admin.EMAIL})")
        print(f"Hash da senha: {admin.SENHA_HASH[:20]}...")

        # Testar verificação de senha
        test_senha = 'admin123'
        result = check_password_hash(admin.SENHA_HASH, test_senha)
        print(f"Verificação com senha 'admin123': {result}")

        # Redefinir a senha
        print("\nRedefinindo a senha para 'admin123'")
        admin.SENHA_HASH = generate_password_hash('admin123')
        db.session.commit()
        print("Senha redefinida com sucesso.")

        # Verificar novamente
        print("\nVerificando novamente com a nova senha:")
        admin = Usuario.query.filter_by(EMAIL='admin@emgea.com').first()
        result = check_password_hash(admin.SENHA_HASH, 'admin123')
        print(f"Verificação com senha 'admin123': {result}")
    else:
        print("Usuário não encontrado")