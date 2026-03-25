from app import create_app, db
from app.models.usuario import Usuario
from werkzeug.security import generate_password_hash

app = create_app()
with app.app_context():
    usuarios = Usuario.query.all()
    resetados = 0
    for u in usuarios:
        u.SENHA_HASH = generate_password_hash('123456', method='pbkdf2:sha256')
        print(f"Senha resetada para: {u.NOME} ({u.EMAIL})")
        resetados += 1
    db.session.commit()
    print(f"\nTotal: {resetados} usuario(s) resetado(s).")