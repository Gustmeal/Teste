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
        Retorna {NATUREZA_normalizada -> [NU_LINHA, ...]} ordenado por NU_LINHA.

        A lista (fila) permite que a MESMA natureza apareça mais de uma vez no
        Boletim — caso dos fundos que constam em APLICAÇÕES FINANCEIRAS e
        novamente em BLOQUEIOS JUDICIAIS. A 1ª ocorrência no Excel consome o
        1º NU_LINHA, a 2ª consome o 2º, e assim por diante.

        Normalização: remove pontos/espaços de recuo do início, colapsa espaços
        e passa para UPPER — aplicada aos DOIS lados da comparação.
        """
        import re
        mapa = {}
        registros = EstruturaBoletim.query.order_by(
            EstruturaBoletim.NU_LINHA
        ).all()
        for r in registros:
            if r.NATUREZA is None or r.NU_LINHA is None:
                continue
            chave = re.sub(r'^[.\s]+', '', str(r.NATUREZA))
            chave = re.sub(r'\s+', ' ', chave).strip().upper()
            if chave:
                mapa.setdefault(chave, []).append(int(r.NU_LINHA))
        return mapa