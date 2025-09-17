# Arquivo: app/models/permissao_sistema.py

from datetime import datetime
from app import db
from sqlalchemy import text


class PermissaoArea(db.Model):
    """Modelo para controle de permissões de acesso aos sistemas por área"""
    __tablename__ = 'APK_TB006_PERMISSOES_AREA'
    __table_args__ = {'schema': 'DEV'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    AREA = db.Column(db.String(50), nullable=False)
    TIPO_AREA = db.Column(db.String(20), nullable=False)
    SISTEMA = db.Column(db.String(50), nullable=False)
    TEM_ACESSO = db.Column(db.Boolean, default=True)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    @staticmethod
    def atualizar_permissoes_area(area, tipo_area, sistemas_permitidos):
        """Atualiza as permissões de uma área para todos os sistemas"""
        try:
            from app.models.permissao_sistema import PermissaoSistema

            # Marca todas as permissões existentes como deletadas
            permissoes_existentes = PermissaoArea.query.filter_by(
                AREA=area,
                TIPO_AREA=tipo_area,
                DELETED_AT=None
            ).all()

            for perm in permissoes_existentes:
                perm.DELETED_AT = datetime.utcnow()

            # Cria ou atualiza as novas permissões
            for sistema in PermissaoSistema.SISTEMAS_DISPONIVEIS.keys():
                tem_acesso = sistema in sistemas_permitidos

                perm_existente = PermissaoArea.query.filter_by(
                    AREA=area,
                    TIPO_AREA=tipo_area,
                    SISTEMA=sistema
                ).first()

                if perm_existente:
                    perm_existente.TEM_ACESSO = tem_acesso
                    perm_existente.DELETED_AT = None
                    perm_existente.UPDATED_AT = datetime.utcnow()
                else:
                    nova_perm = PermissaoArea(
                        AREA=area,
                        TIPO_AREA=tipo_area,
                        SISTEMA=sistema,
                        TEM_ACESSO=tem_acesso
                    )
                    db.session.add(nova_perm)

            db.session.commit()
            return True
        except Exception as e:
            db.session.rollback()
            print(f"Erro ao atualizar permissões de área: {str(e)}")
            return False


class PermissaoSistema(db.Model):
    """Modelo para controle de permissões de acesso aos sistemas"""
    __tablename__ = 'APK_TB005_PERMISSOES_SISTEMA'
    __table_args__ = {'schema': 'DEV'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    USUARIO_ID = db.Column(db.Integer, db.ForeignKey('BDG.APK_TB002_USUARIOS.ID'), nullable=False)
    SISTEMA = db.Column(db.String(50), nullable=False)
    TEM_ACESSO = db.Column(db.Boolean, default=True)
    PERMISSAO_INDIVIDUAL = db.Column(db.Boolean, default=False)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    DELETED_AT = db.Column(db.DateTime)

    usuario = db.relationship('Usuario', backref='permissoes_sistemas')

    # Lista completa de sistemas disponíveis
    SISTEMAS_DISPONIVEIS = {
        'codigos_contabeis': {
            'nome': 'Códigos Contábeis',
            'icone': 'fa-calculator',
            'descricao': 'Gerenciamento de códigos contábeis PDG',
            'categoria': 'Gestão Contábil'
        },
        'demonstrativo_sucor': {
            'nome': 'Demonstrativos SUCOR',
            'icone': 'fa-chart-bar',
            'descricao': 'Comparação de demonstrativos entre períodos',
            'categoria': 'Gestão Contábil'
        },
        'depositos_judiciais': {
            'nome': 'Depósitos Judiciais',
            'icone': 'fa-gavel',
            'descricao': 'Gestão de depósitos judiciais SUFIN',
            'categoria': 'Gestão Financeira'
        },
        'credenciamento': {
            'nome': 'Assessoria de Cobranças',
            'icone': 'fa-handshake',
            'descricao': 'Credenciamento e avaliação de empresas',
            'categoria': 'Gestão de Contratos'
        },
        'sumov': {
            'nome': 'SUMOV',
            'icone': 'fa-building',
            'descricao': 'Gestão de contratos e imóveis',
            'categoria': 'Gestão de Contratos'
        },
        'indicadores': {
            'nome': 'Indicadores',
            'icone': 'fa-chart-line',
            'descricao': 'Sistema de indicadores e metas',
            'categoria': 'Análise e Controle'
        },
        'pendencia_retencao': {
            'nome': 'Cobrados Vs Retidos',
            'icone': 'fa-exchange-alt',
            'descricao': 'Comparação de valores cobrados e retidos',
            'categoria': 'Análise e Controle'
        },
        'auditoria': {
            'nome': 'Auditoria',
            'icone': 'fa-history',
            'descricao': 'Logs e trilha de auditoria do sistema',
            'categoria': 'Análise e Controle'
        },
        'export': {
            'nome': 'Exportação de Dados',
            'icone': 'fa-download',
            'descricao': 'Exporte dados para Excel, PDF ou Word',
            'categoria': 'Ferramentas'
        },
        'relatorio': {
            'nome': 'Gerador de Relatórios',
            'icone': 'fa-file-invoice',
            'descricao': 'Crie relatórios personalizados',
            'categoria': 'Ferramentas'
        }
    }

    def __repr__(self):
        return f'<PermissaoSistema {self.USUARIO_ID} - {self.SISTEMA}: {self.TEM_ACESSO}>'

    @staticmethod
    def verificar_acesso(usuario_id, sistema):
        """Verifica se o usuário tem acesso ao sistema"""
        from app.models.usuario import Usuario

        usuario = Usuario.query.get(usuario_id)
        if not usuario:
            return False

        # Admins e moderadores sempre têm acesso total
        if usuario and usuario.PERFIL in ['admin', 'moderador']:
            return True

        # Para usuários comuns, verificar na tabela de permissões
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
                return True

        return permissao.TEM_ACESSO

    @staticmethod
    def verificar_acesso_com_area(usuario_id, sistema):
        """
        Verifica acesso considerando área do usuário
        """
        from app.models.usuario import Usuario

        usuario = Usuario.query.get(usuario_id)
        if not usuario:
            return False

        # Admins e moderadores sempre têm acesso total
        if usuario.PERFIL in ['admin', 'moderador']:
            return True

        # Verificar permissão individual primeiro
        permissao_individual = PermissaoSistema.query.filter_by(
            USUARIO_ID=usuario_id,
            SISTEMA=sistema,
            DELETED_AT=None
        ).first()

        if permissao_individual and permissao_individual.PERMISSAO_INDIVIDUAL:
            return permissao_individual.TEM_ACESSO

        # Verificar permissão da área
        empregado = usuario.empregado
        if empregado:
            for tipo_area, campo_area in [
                ('setor', empregado.sgSetor),
                ('superintendencia', empregado.sgSuperintendencia),
                ('diretoria', empregado.sgDiretoria)
            ]:
                if campo_area:
                    permissao_area = PermissaoArea.query.filter_by(
                        AREA=campo_area,
                        TIPO_AREA=tipo_area,
                        SISTEMA=sistema,
                        DELETED_AT=None
                    ).first()

                    if permissao_area:
                        return permissao_area.TEM_ACESSO

        # Padrão: permitir acesso
        return True

    @staticmethod
    def criar_permissoes_padrao(usuario_id):
        """Cria permissões padrão para um novo usuário"""
        for sistema in PermissaoSistema.SISTEMAS_DISPONIVEIS.keys():
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
        """Remove todas as permissões de um usuário"""
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