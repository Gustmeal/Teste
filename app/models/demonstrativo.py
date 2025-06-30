from app import db


class EstruturaDemonstrativo(db.Model):
    """Tabela de estrutura dos demonstrativos"""
    __tablename__ = 'COR_DEM_TB002_ESTRUTURA'
    __table_args__ = {'schema': 'BDG'}

    ORDEM = db.Column(db.Integer, primary_key=True)
    GRUPO = db.Column(db.String(255), nullable=False)
    SOMA = db.Column(db.String(255), nullable=True)
    FORMULA = db.Column(db.String(255), nullable=True)
    CO_DEMONSTRATIVO = db.Column(db.String(50), nullable=True)

    def __repr__(self):
        return f'<EstruturaDemonstrativo {self.ORDEM} - {self.GRUPO}>'


class ContaDemonstrativo(db.Model):
    """Tabela de vinculação entre contas e demonstrativos"""
    __tablename__ = 'COR_DEM_TB003_CONTA_DEMONSTRATIVO'
    __table_args__ = {'schema': 'DEV'}

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