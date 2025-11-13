# models/siscalculo.py
from app import db
from datetime import datetime
from decimal import Decimal


class ParamIndicesEconomicos(db.Model):
    """Tabela de parâmetros dos índices econômicos"""
    __tablename__ = 'PAR023_INDICES_ECONOMICOS'
    __table_args__ = {'schema': 'BDG'}

    ID_INDICE_ECONOMICO = db.Column(db.Integer, primary_key=True)
    DSC_INDICE_ECONOMICO = db.Column(db.String(100), nullable=False)

    def __repr__(self):
        return f'<IndiceEconomico {self.ID_INDICE_ECONOMICO}: {self.DSC_INDICE_ECONOMICO}>'

    @staticmethod
    def obter_indices_permitidos():
        """Retorna apenas os índices 2, 5, 7, 9"""
        return ParamIndicesEconomicos.query.filter(
            ParamIndicesEconomicos.ID_INDICE_ECONOMICO.in_([2, 5, 7, 9])
        ).order_by(ParamIndicesEconomicos.ID_INDICE_ECONOMICO).all()


class SiscalculoDados(db.Model):
    """Tabela para armazenar os dados importados do Excel"""
    __tablename__ = 'MOV_TB030_SISCALCULO_DADOS'
    __table_args__ = {'schema': 'BDG'}

    # Chave primária composta (não tem ID autoincrement)
    IMOVEL = db.Column(db.String(50), primary_key=True, nullable=False, default='')
    NOME_CONDOMINIO = db.Column(db.String(200), nullable=True)
    DT_VENCIMENTO = db.Column(db.Date, primary_key=True, nullable=False)
    DT_ATUALIZACAO = db.Column(db.Date, primary_key=True, nullable=False)

    # Dados
    VR_COTA = db.Column(db.Numeric(18, 2), nullable=False)

    def __repr__(self):
        return f'<SiscalculoDados {self.IMOVEL} - {self.DT_VENCIMENTO}>'

    @staticmethod
    def limpar_dados_temporarios(dt_atualizacao):
        """Remove dados anteriores para a mesma data de atualização"""
        try:
            SiscalculoDados.query.filter_by(
                DT_ATUALIZACAO=dt_atualizacao
            ).delete()
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            print(f"Erro ao limpar dados temporários: {e}")


class SiscalculoCalculos(db.Model):
    """Tabela para armazenar os cálculos realizados"""
    __tablename__ = 'MOV_TB031_SISCALCULO_CALCULOS'
    __table_args__ = {'schema': 'BDG'}

    # Chave primária composta: DT_ATUALIZACAO + ID_INDICE_ECONOMICO + DT_VENCIMENTO + IMOVEL
    DT_ATUALIZACAO = db.Column(db.Date, primary_key=True, nullable=False)
    ID_INDICE_ECONOMICO = db.Column(db.Integer, primary_key=True, nullable=False)
    DT_VENCIMENTO = db.Column(db.Date, primary_key=True, nullable=False)
    IMOVEL = db.Column(db.String(50), primary_key=True, nullable=False, default='')
    VR_COTA = db.Column(db.Numeric(18, 2), nullable=False)
    TEMPO_ATRASO = db.Column(db.Integer, nullable=True)
    PERC_ATUALIZACAO = db.Column(db.Numeric(18, 4), nullable=True)
    ATM = db.Column(db.Numeric(18, 2), nullable=True)
    VR_JUROS = db.Column(db.Numeric(18, 2), nullable=True)
    VR_MULTA = db.Column(db.Numeric(18, 2), nullable=True)
    VR_DESCONTO = db.Column(db.Numeric(18, 2), nullable=True)
    VR_TOTAL = db.Column(db.Numeric(18, 2), nullable=True)
    PERC_HONORARIOS = db.Column(db.Numeric(5, 2), nullable=True)  # ✅ NOVA COLUNA

    def __repr__(self):
        return f'<SiscalculoCalculo {self.DT_ATUALIZACAO} - Índice {self.ID_INDICE_ECONOMICO} - {self.IMOVEL}>'

    @staticmethod
    def obter_calculos_por_data(dt_atualizacao):
        """Busca cálculos por data de atualização"""
        return SiscalculoCalculos.query.filter_by(
            DT_ATUALIZACAO=dt_atualizacao
        ).order_by(
            SiscalculoCalculos.ID_INDICE_ECONOMICO,
            SiscalculoCalculos.DT_VENCIMENTO
        ).all()


class IndicadorEconomico(db.Model):
    """Tabela de indicadores econômicos do banco DBPRDINDICADORECONOMICO"""
    __tablename__ = 'tblIndicadorEconomico'
    __table_args__ = {'schema': 'dbo', 'info': {'bind_key': 'indicadores'}}

    # Como a tabela original não tem PK definida, usar chave composta
    idTipoIndicadorEconomico = db.Column(db.Integer, primary_key=True)
    chDTInicio = db.Column(db.String(8), primary_key=True)
    chDTFinal = db.Column(db.String(8))
    numIndicadorEconomico = db.Column(db.Numeric(18, 8))

    @staticmethod
    def obter_fator_acumulado(id_tipo, dt_inicio, dt_fim):
        """Calcula o fator acumulado para um período"""
        from sqlalchemy import text

        # Usar conexão específica se configurada
        sql = text("""
            WITH IndicesPeriodo AS (
                SELECT 
                    chDTInicio,
                    numIndicadorEconomico,
                    CAST((1 + (numIndicadorEconomico / 100.0)) AS DECIMAL(18,8)) AS FatorMensal,
                    ROW_NUMBER() OVER (ORDER BY chDTInicio) AS Sequencia
                FROM [DBPRDINDICADORECONOMICO].[dbo].[tblIndicadorEconomico]
                WHERE idTipoIndicadorEconomico = :id_tipo
                    AND chDTInicio >= :dt_inicio
                    AND chDTInicio <= :dt_fim
            )
            SELECT 
                EXP(SUM(LOG(FatorMensal))) AS FatorAcumulado
            FROM IndicesPeriodo
        """)

        resultado = db.session.execute(sql, {
            'id_tipo': id_tipo,
            'dt_inicio': dt_inicio,
            'dt_fim': dt_fim
        }).fetchone()

        return float(resultado[0]) if resultado and resultado[0] else 1.0