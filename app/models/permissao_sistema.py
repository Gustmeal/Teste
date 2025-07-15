from datetime import datetime
from app import db


class PermissaoSistema(db.Model):
    """Modelo para controle de permissões de acesso aos sistemas"""
    __tablename__ = 'APK_TB005_PERMISSOES_SISTEMA'
    __table_args__ = {'schema': 'DEV'}  # Schema DEV ao invés de BDG

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    USUARIO_ID = db.Column(db.Integer, db.ForeignKey('BDG.APK_TB002_USUARIOS.ID'), nullable=False)
    SISTEMA = db.Column(db.String(50), nullable=False)
    TEM_ACESSO = db.Column(db.Boolean, default=True)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    # Relacionamento com usuário que está em BDG
    usuario = db.relationship('Usuario', backref='permissoes_sistemas')

    # Sistemas disponíveis
    SISTEMAS_DISPONIVEIS = {
        'credenciamento': {
            'nome': 'Assessoria de Cobranças',
            'icone': 'fa-file-contract',
            'descricao': 'Sistema de credenciamento e avaliação'
        },
        'sumov': {
            'nome': 'SUMOV',
            'icone': 'fa-home',
            'descricao': 'Gerenciamento de contratos e imóveis'
        },
        'codigos_contabeis': {
            'nome': 'Códigos Contábeis',
            'icone': 'fa-calculator',
            'descricao': 'Gerenciamento de códigos contábeis PDG'
        },
        'converter': {
            'nome': 'Conversor de Documentos',
            'icone': 'fa-sync-alt',
            'descricao': 'Conversão de arquivos e documentos'
        },
        'vinculacao': {
            'nome': 'Vinculação',
            'icone': 'fa-link',
            'descricao': 'Sistema de vinculação'
        },
        'feedback': {
            'nome': 'Feedback',
            'icone': 'fa-comment-alt',
            'descricao': 'Sistema de feedback'
        },
        'chat': {
            'nome': 'Chat',
            'icone': 'fa-comments',
            'descricao': 'Sistema de chat'
        }
    }

    def __repr__(self):
        return f'<PermissaoSistema {self.USUARIO_ID} - {self.SISTEMA}: {self.TEM_ACESSO}>'

    @staticmethod
    def verificar_acesso(usuario_id, sistema):
        """Verifica se o usuário tem acesso ao sistema"""
        from app.models.usuario import Usuario

        # Buscar o usuário em BDG
        usuario = Usuario.query.get(usuario_id)

        # Admins e moderadores sempre têm acesso total
        if usuario and usuario.PERFIL in ['admin', 'moderador']:
            return True

        # Para usuários comuns, verificar na tabela de permissões em DEV
        permissao = PermissaoSistema.query.filter_by(
            USUARIO_ID=usuario_id,
            SISTEMA=sistema,
            DELETED_AT=None
        ).first()

        # Se não existe permissão específica, criar com acesso padrão (True)
        if not permissao:
            permissao = PermissaoSistema(
                USUARIO_ID=usuario_id,
                SISTEMA=sistema,
                TEM_ACESSO=True
            )
            db.session.add(permissao)
            try:
                db.session.commit()
            except:
                db.session.rollback()
                # Em caso de erro, assumir acesso permitido
                return True

        return permissao.TEM_ACESSO

    @staticmethod
    def criar_permissoes_padrao(usuario_id):
        """Cria permissões padrão para um novo usuário (todos os sistemas permitidos)"""
        for sistema in PermissaoSistema.SISTEMAS_DISPONIVEIS.keys():
            # Verificar se já existe
            existe = PermissaoSistema.query.filter_by(
                USUARIO_ID=usuario_id,
                SISTEMA=sistema
            ).first()

            if not existe:
                permissao = PermissaoSistema(
                    USUARIO_ID=usuario_id,
                    SISTEMA=sistema,
                    TEM_ACESSO=True
                )
                db.session.add(permissao)

        try:
            db.session.commit()
        except:
            db.session.rollback()

    @staticmethod
    def limpar_permissoes_usuario(usuario_id):
        """Remove todas as permissões de um usuário (soft delete)"""
        permissoes = PermissaoSistema.query.filter_by(
            USUARIO_ID=usuario_id,
            DELETED_AT=None
        ).all()

        for permissao in permissoes:
            permissao.DELETED_AT = datetime.utcnow()

        try:
            db.session.commit()
        except:
            db.session.rollback()