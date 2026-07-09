from app import db


class EstruturaBoletim(db.Model):
    """
    Tabela BDG.FIN_TB019_ESTRUTURA_BOLETIM.

    Estrutura pré-definida que vincula cada NATUREZA ao seu NU_LINHA fixo.
    É a fonte do NU_LINHA usado ao gravar na FIN_TB020: o nome da natureza
    extraído do Excel é comparado com a NATUREZA desta tabela para descobrir
    o NU_LINHA correspondente.

    Colunas mapeadas (conforme o SELECT informado):
      - NU_LINHA  int      *chave
      - NATUREZA  varchar

    OBS.: se a tabela tiver colunas adicionais para distinguir naturezas de
    mesmo nome (grupo/seção/item-pai), me avise que eu incluo aqui e no
    casamento — hoje o mapa é só por nome.
    """
    __tablename__ = 'FIN_TB019_ESTRUTURA_BOLETIM'
    __table_args__ = {'schema': 'BDG'}

    NU_LINHA = db.Column(db.Integer, primary_key=True, nullable=False)
    NATUREZA = db.Column(db.String(255), nullable=True)

    def __repr__(self):
        return f'<EstruturaBoletim L{self.NU_LINHA} {self.NATUREZA}>'

    @staticmethod
    def carregar_mapa_natureza_nu_linha():
        """
        Retorna {NATUREZA_normalizada -> NU_LINHA}.
        Normalização: trim + colapso de espaços + UPPER, para casar com o
        nome extraído do Excel independentemente de padding (CHAR/NCHAR),
        maiúsculas/minúsculas ou espaços duplicados.

        Se a mesma NATUREZA existir com mais de um NU_LINHA na FIN_TB019,
        prevalece o último lido (e isso indica que o casamento por nome é
        insuficiente — precisaria de grupo/pai).
        """
        import re
        mapa = {}
        for r in EstruturaBoletim.query.all():
            if r.NATUREZA is None or r.NU_LINHA is None:
                continue
            chave = re.sub(r'\s+', ' ', str(r.NATUREZA).strip()).upper()
            if chave:
                mapa[chave] = int(r.NU_LINHA)
        return mapa