# app/models/diferenca_despesas_siscor.py
from app import db
from decimal import Decimal
from functools import lru_cache


class DiferencaDespesasSiscor(db.Model):
    """Modelo para a view MOV_VW002_DIFERENCA_DESPESAS_X_SISCOR"""
    __tablename__ = 'MOV_VW002_DIFERENCA_DESPESAS_X_SISCOR'
    __table_args__ = {
        'schema': 'BDG',
        'extend_existing': True
    }

    # Definir colunas explicitamente (mais rápido que autoload)
    DT_DESPESA = db.Column(db.String(6), primary_key=True, nullable=False)
    ID_ITEM = db.Column(db.Integer, primary_key=True, nullable=False)
    VR_SISCOR = db.Column(db.Numeric(18, 2))
    VR_DESPESA = db.Column(db.Numeric(18, 2))
    DIF = db.Column(db.Numeric(18, 2))

    __mapper_args__ = {
        'confirm_deleted_rows': False
    }

    def __repr__(self):
        return f'<DiferencaDespesasSiscor {self.DT_DESPESA} - Item {self.ID_ITEM}>'

    @staticmethod
    @lru_cache(maxsize=1)  # Cache do resultado
    def _meses_nomes():
        """Dicionário de meses (em cache)"""
        return {
            '01': 'Janeiro', '02': 'Fevereiro', '03': 'Março',
            '04': 'Abril', '05': 'Maio', '06': 'Junho',
            '07': 'Julho', '08': 'Agosto', '09': 'Setembro',
            '10': 'Outubro', '11': 'Novembro', '12': 'Dezembro'
        }

    @staticmethod
    def listar_datas_disponiveis_formatadas():
        """
        Lista datas disponíveis JÁ FORMATADAS em uma única query
        Retorna: [{'valor': '202508', 'label': 'Agosto de 2025'}, ...]
        """
        # Query otimizada que retorna apenas as datas únicas
        datas_raw = db.session.query(
            DiferencaDespesasSiscor.DT_DESPESA
        ).distinct().order_by(
            DiferencaDespesasSiscor.DT_DESPESA.desc()
        ).all()

        # Formatar todas de uma vez
        meses = DiferencaDespesasSiscor._meses_nomes()
        datas_formatadas = []

        for (dt,) in datas_raw:
            if len(dt) == 6:
                ano = dt[:4]
                mes = dt[4:6]
                label = f"{meses.get(mes, 'Mês Inválido')} de {ano}"
            else:
                label = dt

            datas_formatadas.append({
                'valor': dt,
                'label': label
            })

        return datas_formatadas

    @staticmethod
    def formatar_data_mesano(dt_despesa):
        """Converte formato 202508 para 'Agosto de 2025'"""
        meses = DiferencaDespesasSiscor._meses_nomes()

        if len(dt_despesa) == 6:
            ano = dt_despesa[:4]
            mes = dt_despesa[4:6]
            return f"{meses.get(mes, 'Mês Inválido')} de {ano}"

        return dt_despesa

    @staticmethod
    def buscar_por_filtros(datas=None, id_item=None):
        """
        Busca registros com filtros opcionais

        Args:
            datas: Lista de datas no formato AAAAMM, ou None para todas
            id_item: ID específico do item, ou None para todos
        """
        query = DiferencaDespesasSiscor.query

        # Filtro de datas
        if datas and len(datas) > 0:
            query = query.filter(DiferencaDespesasSiscor.DT_DESPESA.in_(datas))

        # Filtro de ID_ITEM
        if id_item is not None:
            query = query.filter(DiferencaDespesasSiscor.ID_ITEM == id_item)

        return query.order_by(
            DiferencaDespesasSiscor.DT_DESPESA.desc(),
            DiferencaDespesasSiscor.ID_ITEM.asc()
        ).all()

    @staticmethod
    def calcular_totais(registros):
        """Calcula totais de VR_SISCOR, VR_DESPESA e DIF"""
        total_siscor = sum(
            (r.VR_SISCOR or Decimal('0')) for r in registros
        )
        total_despesa = sum(
            (r.VR_DESPESA or Decimal('0')) for r in registros
        )
        total_dif = sum(
            (r.DIF or Decimal('0')) for r in registros
        )

        return {
            'total_siscor': float(total_siscor),
            'total_despesa': float(total_despesa),
            'total_dif': float(total_dif)
        }