from decimal import Decimal

from sqlalchemy import func, text

from app import db


class CotasBBFae2(db.Model):
    """BDG.FIN_TB026 — Cotas do Fundo BB FAE 2 (3 contas). *chave: DATA."""
    __tablename__ = 'FIN_TB026_COTAS_FUNDOS_INVESTIMENTOS_BB_FAE2'
    __table_args__ = {'schema': 'BDG', 'extend_existing': True}

    DATA = db.Column(db.Date, primary_key=True, nullable=False)
    VR_COTA = db.Column(db.Numeric(18, 8))
    IND_COTA = db.Column(db.Numeric(18, 8))
    VR_SD_BRUTO_BB_FAE2 = db.Column(db.Numeric(18, 2))
    VR_BLOQUEIO_JUDICIAL_BB_FAE2 = db.Column(db.Numeric(18, 2))
    VR_SD_LIQUIDO_BB_FAE2 = db.Column(db.Numeric(18, 2))
    VR_SD_BRUTO_BB_FAE2_CC = db.Column(db.Numeric(18, 2))
    VR_BLOQUEIO_JUDICIAL_BB_FAE2_CC = db.Column(db.Numeric(18, 2))
    VR_SD_LIQUIDO_BB_FAE2_CC = db.Column(db.Numeric(18, 2))
    VR_SD_BRUTO_BB_FAE2_1000004 = db.Column(db.Numeric(18, 2))
    VR_BLOQUEIO_JUDICIAL_BB_FAE2_1000004 = db.Column(db.Numeric(18, 2))
    VR_SD_LIQUIDO_BB_FAE2_1000004 = db.Column(db.Numeric(18, 2))


class CotasBBExclusivo(db.Model):
    """BDG.FIN_TB027 — Cotas do Fundo BB Exclusivo Emgea. *chave: DATA."""
    __tablename__ = 'FIN_TB027_COTAS_FUNDOS_INVESTIMENTOS_BB_EXCLUSIVO'
    __table_args__ = {'schema': 'BDG', 'extend_existing': True}

    DATA = db.Column(db.Date, primary_key=True, nullable=False)
    VR_COTA = db.Column(db.Numeric(18, 8))
    IND_COTA = db.Column(db.Numeric(18, 8))
    VR_SD_BRUTO_BB_EXCLUSIVO = db.Column(db.Numeric(18, 2))
    VR_BLOQUEIO_JUDICIAL_BB_EXCLUSIVO = db.Column(db.Numeric(18, 2))
    VR_SD_LIQUIDO_BB_EXCLUSIVO = db.Column(db.Numeric(18, 2))


class CotasCaixaXXI(db.Model):
    """BDG.FIN_TB028 — Cotas do Fundo Caixa Exclusivo XXI. *chave: DATA."""
    __tablename__ = 'FIN_TB028_COTAS_FUNDOS_INVESTIMENTOS_CAIXA_EXCLUSIVO_XXI'
    __table_args__ = {'schema': 'BDG', 'extend_existing': True}

    DATA = db.Column(db.Date, primary_key=True, nullable=False)
    VR_COTA = db.Column(db.Numeric(18, 8))
    IND_COTA = db.Column(db.Numeric(18, 8))
    VR_SD_BRUTO_CAIXA_EXCLUSIVO_XXI = db.Column(db.Numeric(18, 2))
    VR_SD_LIQUIDO_CAIXA_EXCLUSIVO_XXI = db.Column(db.Numeric(18, 2))


# =========================================================================
# CONFIGURAÇÃO DOS FUNDOS (o app funciona igual para os três)
# =========================================================================
FUNDOS = {
    'bb_fae2': {
        'label': 'BB Extramercado FAE 2',
        'tabela': 'FIN_TB026',
        'model': CotasBBFae2,
        'campos': [
            ('VR_SD_BRUTO_BB_FAE2', 'Saldo Bruto (191.166-X)'),
            ('VR_BLOQUEIO_JUDICIAL_BB_FAE2', 'Bloqueio Judicial (191.166-X)'),
            ('VR_SD_LIQUIDO_BB_FAE2', 'Saldo Líquido (191.166-X)'),
            ('VR_SD_BRUTO_BB_FAE2_CC', 'Saldo Bruto (192.166-5)'),
            ('VR_BLOQUEIO_JUDICIAL_BB_FAE2_CC', 'Bloqueio Judicial (192.166-5)'),
            ('VR_SD_LIQUIDO_BB_FAE2_CC', 'Saldo Líquido (192.166-5)'),
            ('VR_SD_BRUTO_BB_FAE2_1000004', 'Saldo Bruto (100.000-4)'),
            ('VR_BLOQUEIO_JUDICIAL_BB_FAE2_1000004', 'Bloqueio Judicial (100.000-4)'),
            ('VR_SD_LIQUIDO_BB_FAE2_1000004', 'Saldo Líquido (100.000-4)'),
        ],
    },
    'bb_exclusivo': {
        'label': 'BB Exclusivo Extramercado Emgea',
        'tabela': 'FIN_TB027',
        'model': CotasBBExclusivo,
        'campos': [
            ('VR_SD_BRUTO_BB_EXCLUSIVO', 'Saldo Bruto'),
            ('VR_BLOQUEIO_JUDICIAL_BB_EXCLUSIVO', 'Bloqueio Judicial'),
            ('VR_SD_LIQUIDO_BB_EXCLUSIVO', 'Saldo Líquido'),
        ],
    },
    'caixa_xxi': {
        'label': 'Caixa Extramercado Exclusivo XXI',
        'tabela': 'FIN_TB028',
        'model': CotasCaixaXXI,
        'campos': [
            ('VR_SD_BRUTO_CAIXA_EXCLUSIVO_XXI', 'Saldo Bruto'),
            ('VR_SD_LIQUIDO_CAIXA_EXCLUSIVO_XXI', 'Saldo Líquido'),
        ],
    },
}


def obter_ultimo_registro(model):
    """Último registro gravado (maior DATA). None se a tabela estiver vazia."""
    return model.query.order_by(model.DATA.desc()).first()


def obter_proxima_data(model):
    """Próxima DATA a preencher = MAX(DATA) + 1 dia. None se vazia."""
    from datetime import timedelta
    ultima = db.session.query(func.max(model.DATA)).scalar()
    return (ultima + timedelta(days=1)) if ultima else None


def e_dia_util(data):
    """
    Consulta a AUX_TB004_CALENDARIO. Retorna True/False, ou None se a data
    não existir no calendário. Query em text() para não depender do tipo
    (BIT/INT) da coluna E_DIA_UTIL.
    """
    valor = db.session.execute(text("""
        SELECT TOP 1 E_DIA_UTIL
        FROM [BDG].[AUX_TB004_CALENDARIO]
        WHERE DT_REFERENCIA = :dt
    """), {'dt': data}).scalar()
    if valor is None:
        return None
    try:
        return bool(int(valor))
    except (TypeError, ValueError):
        return bool(valor)


def calcular_ind_cota(vr_cota_atual, vr_cota_anterior):
    """
    IND_COTA = (VR_COTA do dia / VR_COTA do dia anterior) - 1.
    Retorna Decimal com 8 casas. 0 se não houver anterior ou se for zero.
    """
    if vr_cota_atual is None or vr_cota_anterior in (None, 0):
        return Decimal('0.00000000')
    atual = Decimal(str(vr_cota_atual))
    anterior = Decimal(str(vr_cota_anterior))
    if anterior == 0:
        return Decimal('0.00000000')
    return ((atual / anterior) - Decimal('1')).quantize(Decimal('0.00000001'))