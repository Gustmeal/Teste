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

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    IMOVEL = db.Column(db.String(50), nullable=True)
    DT_VENCIMENTO = db.Column(db.Date, nullable=False)
    VR_COTA = db.Column(db.Numeric(18, 2), nullable=False)
    DT_ATUALIZACAO = db.Column(db.Date, nullable=False)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)
    USUARIO_CARGA = db.Column(db.String(100), nullable=True)

    def __repr__(self):
        return f'<SiscalculoDados {self.ID}: Vencimento {self.DT_VENCIMENTO}>'

    @staticmethod
    def limpar_dados_temporarios(dt_atualizacao):
        """Remove dados temporários para nova importação"""
        try:
            SiscalculoDados.query.filter_by(DT_ATUALIZACAO=dt_atualizacao).delete()
            db.session.commit()
            return True
        except Exception as e:
            db.session.rollback()
            return False


class SiscalculoCalculos(db.Model):
    """Tabela para armazenar os cálculos realizados"""
    __tablename__ = 'MOV_TB031_SISCALCULO_CALCULOS'
    __table_args__ = {'schema': 'BDG'}

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    IMOVEL = db.Column(db.String(50), nullable=True)
    DT_VENCIMENTO = db.Column(db.Date, nullable=False)
    VR_COTA = db.Column(db.Numeric(18, 2), nullable=False)
    DT_ATUALIZACAO = db.Column(db.Date, nullable=False)
    TEMPO_ATRASO = db.Column(db.Integer, nullable=False)  # Meses
    PERC_ATUALIZACAO = db.Column(db.Numeric(18, 8), nullable=True)
    ATM = db.Column(db.Numeric(18, 2), nullable=True)  # Valor Atualização Monetária
    VR_JUROS = db.Column(db.Numeric(18, 2), nullable=True)
    VR_MULTA = db.Column(db.Numeric(18, 2), nullable=True)
    VR_DESCONTO = db.Column(db.Numeric(18, 2), nullable=True, default=0)
    VR_TOTAL = db.Column(db.Numeric(18, 2), nullable=True)
    ID_INDICE_ECONOMICO = db.Column(db.Integer, nullable=False)
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    USUARIO_CALCULO = db.Column(db.String(100), nullable=True)

    def __repr__(self):
        return f'<SiscalculoCalculo {self.ID}: Índice {self.ID_INDICE_ECONOMICO}>'

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
                    ROW_NUMBER() OVER (ORDER BY chDTInicio) AS NumMes
                FROM DBPRDINDICADORECONOMICO.dbo.tblIndicadorEconomico
                WHERE idTipoIndicadorEconomico = :id_tipo
                AND chDTInicio >= :dt_inicio
                AND chDTInicio <= :dt_fim
                AND (:id_tipo != 2 OR (RIGHT(chDTInicio, 2) = '01' AND RIGHT(chDTFinal, 2) = '01'))
            ),
            CalculoAcumulado AS (
                SELECT 
                    NumMes, 
                    FatorMensal,
                    FatorMensal AS FatorAcumulado
                FROM IndicesPeriodo
                WHERE NumMes = 1

                UNION ALL

                SELECT 
                    i.NumMes,
                    i.FatorMensal,
                    CAST(c.FatorAcumulado * i.FatorMensal AS DECIMAL(18,8)) AS FatorAcumulado
                FROM IndicesPeriodo i
                INNER JOIN CalculoAcumulado c ON i.NumMes = c.NumMes + 1
            )
            SELECT ISNULL(MAX(FatorAcumulado) - 1, 0) AS FatorAcumulado
            FROM CalculoAcumulado
        """)

        try:
            result = db.session.execute(sql, {
                'id_tipo': id_tipo,
                'dt_inicio': dt_inicio.strftime('%Y%m%d'),
                'dt_fim': dt_fim.strftime('%Y%m%d')
            }).scalar()
            return Decimal(str(result)) if result else Decimal('0')
        except Exception as e:
            print(f"Erro ao calcular fator acumulado: {e}")
            return Decimal('0')