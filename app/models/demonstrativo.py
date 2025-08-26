from app import db
from sqlalchemy import text

class CodigoDemonstrativo(db.Model):
    """Tabela de códigos dos demonstrativos"""
    __tablename__ = 'COR_DEM_TB001_CODIGOS'
    __table_args__ = {'schema': 'BDG'}

    CO_DEMONSTRATIVO = db.Column(db.Integer, primary_key=True)
    NO_DEMONSTRATIVO = db.Column(db.String(50), nullable=False)

    def __repr__(self):
        return f'<CodigoDemonstrativo {self.CO_DEMONSTRATIVO} - {self.NO_DEMONSTRATIVO}>'

    @staticmethod
    def obter_todos():
        """Retorna todos os códigos de demonstrativos ordenados"""
        return CodigoDemonstrativo.query.order_by(CodigoDemonstrativo.CO_DEMONSTRATIVO).all()

class EstruturaDemonstrativo(db.Model):
    """Tabela de estrutura dos demonstrativos"""
    __tablename__ = 'COR_DEM_TB002_ESTRUTURA'
    __table_args__ = {'schema': 'BDG'}

    ORDEM = db.Column(db.Integer, primary_key=True)
    GRUPO = db.Column(db.String(255), nullable=False)
    SOMA = db.Column(db.String(255), nullable=True)
    FORMULA = db.Column(db.String(255), nullable=True)
    CO_DEMONSTRATIVO = db.Column(db.Integer, primary_key=True)  # ADICIONAR COMO CHAVE PRIMÁRIA COMPOSTA

    def __repr__(self):
        return f'<EstruturaDemonstrativo {self.CO_DEMONSTRATIVO} - {self.ORDEM} - {self.GRUPO}>'

    @staticmethod
    def obter_por_demonstrativo(co_demonstrativo):
        """Retorna estruturas de um demonstrativo específico"""
        return EstruturaDemonstrativo.query.filter_by(
            CO_DEMONSTRATIVO=co_demonstrativo
        ).order_by(EstruturaDemonstrativo.ORDEM).all()

    @staticmethod
    def obter_estruturas_agrupadas():
        """Retorna estruturas agrupadas por demonstrativo"""
        from sqlalchemy import text
        sql = text("""
            SELECT 
                e.ORDEM,
                e.GRUPO,
                e.CO_DEMONSTRATIVO,
                c.NO_DEMONSTRATIVO
            FROM BDG.COR_DEM_TB002_ESTRUTURA e
            INNER JOIN BDG.COR_DEM_TB001_CODIGOS c ON e.CO_DEMONSTRATIVO = c.CO_DEMONSTRATIVO
            ORDER BY e.CO_DEMONSTRATIVO, e.ORDEM
        """)
        return db.session.execute(sql).fetchall()

class ContaDemonstrativo(db.Model):
    """Tabela de vinculação entre contas e demonstrativos"""
    __tablename__ = 'COR_DEM_TB003_CONTA_DEMONSTRATIVO'
    __table_args__ = {'schema': 'BDG'}

    CO_CONTA = db.Column(db.String(50), primary_key=True)
    CO_BP_Gerencial = db.Column(db.Integer, nullable=True)
    CO_BP_Resumida = db.Column(db.Integer, nullable=True)
    CO_DRE_Gerencial = db.Column(db.Integer, nullable=True)
    CO_DRE_Resumida = db.Column(db.Integer, nullable=True)
    CO_DVA_Gerencial = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return f'<ContaDemonstrativo {self.CO_CONTA}>'

    @staticmethod
    def obter_contas_disponiveis():
        """Retorna lista de contas únicas disponíveis"""
        try:
            # Busca tanto contas já cadastradas quanto contas potenciais
            # Você pode adicionar aqui uma query para buscar contas de outras tabelas também
            contas = db.session.query(ContaDemonstrativo.CO_CONTA).distinct().all()
            return [conta[0] for conta in contas if conta[0]]
        except:
            return []

    @staticmethod
    def criar_ou_atualizar(co_conta, dados):
        """Cria nova conta ou atualiza se já existir"""
        conta_existente = ContaDemonstrativo.query.filter_by(CO_CONTA=co_conta).first()

        if conta_existente:
            # Atualizar conta existente
            for campo, valor in dados.items():
                setattr(conta_existente, campo, valor)
            return conta_existente, False  # False = não é nova
        else:
            # Criar nova conta
            nova_conta = ContaDemonstrativo(CO_CONTA=co_conta, **dados)
            db.session.add(nova_conta)
            return nova_conta, True  # True = é nova

    @staticmethod
    def obter_data_referencia_mais_recente():
        """Busca a DT_REFERENCIA mais recente da tabela COR_TB012_BALANCETE"""
        try:
            result = db.session.execute(
                text("SELECT MAX(DT_REFERENCIA) FROM BDG.COR_TB012_BALANCETE")
            ).scalar()
            return result
        except Exception as e:
            print(f"Erro ao buscar data mais recente: {str(e)}")
            return None