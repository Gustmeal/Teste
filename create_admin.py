from app import create_app, db
from app.models.usuario import Usuario

app = create_app()

with app.app_context():
    # Verificar se já existe usuário admin
    admin = Usuario.query.filter_by(EMAIL='admin@emgea.com').first()

    if not admin:
        # Criar usuário administrador
        admin = Usuario(
            NOME='Administrador',
            EMAIL='admin@emgea.com',
            PERFIL='admin'
        )
        admin.set_senha('admin123')  # Defina uma senha forte em ambiente de produção

        db.session.add(admin)
        db.session.commit()
        print("Usuário administrador criado com sucesso!")
    else:
        print("Usuário administrador já existe.")