# app/models/mes_instrumento.py
from app import db


class MesInstrumento(db.Model):
    """
    Modelo para a tabela BDG.FIN_TB002_MESES_INSTRUMENTO.

    Mapeia o código de mês dos contratos futuros (padrão CME/B3) para o
    nome do mês por extenso e seu número.

    Estrutura:
      - COD_MES : varchar    (ex: 'F', 'G', 'H', ...)
      - MES     : varchar    (ex: 'Janeiro', 'Fevereiro', ...)
      - NR_MES  : tinyint    (ex: 1, 2, 3, ...)
    """
    __tablename__ = 'FIN_TB002_MESES_INSTRUMENTO'
    __table_args__ = {'schema': 'BDG'}

    COD_MES = db.Column(db.String(1), primary_key=True, nullable=False)
    MES = db.Column(db.String(20), nullable=True)
    NR_MES = db.Column(db.SmallInteger, nullable=True)

    def __repr__(self):
        return f'<MesInstrumento {self.COD_MES}={self.MES} ({self.NR_MES})>'

    @staticmethod
    def carregar_mapa():
        """
        Retorna {COD_MES → NR_MES}.
        Ex: {'F': 1, 'G': 2, ..., 'Z': 12}
        Usado para converter letra CME → número do mês.
        """
        registros = MesInstrumento.query.all()
        return {
            (r.COD_MES or '').strip().upper(): r.NR_MES
            for r in registros
            if r.COD_MES and r.NR_MES
        }

    @staticmethod
    def carregar_mapa_invertido():
        """
        Retorna {NR_MES → COD_MES}.
        Ex: {1: 'F', 2: 'G', ..., 12: 'Z'}
        Usado para construir o INST_FINANC dos contratos virtuais
        (quando a B3 não emite contrato pra um mês específico, mas a
        EMGEA precisa dele na série de 105 meses).
        """
        registros = MesInstrumento.query.all()
        return {
            int(r.NR_MES): (r.COD_MES or '').strip().upper()
            for r in registros
            if r.COD_MES and r.NR_MES
        }