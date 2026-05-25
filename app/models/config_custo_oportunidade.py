# app/models/config_custo_oportunidade.py
from datetime import datetime
from app import db


class ConfigCustoOportunidade(db.Model):
    """
    Modelo para a tabela BDG.FIN_TB006_CONFIG_CUSTO_OPORTUNIDADE.

    Tabela chave/valor com configurações do módulo Custo de Oportunidade.

    Chaves usadas hoje:
      - ANO_MES_LIMITE : YYYYMM (ex: '203501' = janeiro/2035)
        Define até quando o bot deve pegar contratos DI1 na série mensal.
        Substitui o antigo HORIZONTE_MESES fixo de 105 meses.

    Estrutura:
      - CHAVE     varchar(50)  NOT NULL  *chave (PK)
      - VALOR     varchar(20)  NULL
      - DT_UPDATE datetime     NOT NULL DEFAULT GETDATE()
    """
    __tablename__ = 'FIN_TB006_CONFIG_CUSTO_OPORTUNIDADE'
    __table_args__ = {'schema': 'BDG'}

    CHAVE = db.Column(db.String(50), primary_key=True, nullable=False)
    VALOR = db.Column(db.String(20), nullable=True)
    DT_UPDATE = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self):
        return f'<Config {self.CHAVE}={self.VALOR}>'

    # ------------------------------------------------------------------
    # ANO_MES_LIMITE
    # ------------------------------------------------------------------
    @staticmethod
    def obter_ano_mes_limite():
        """
        Retorna o ANO_MES_LIMITE como int (ex: 203501) ou None se não
        estiver configurado.
        """
        reg = ConfigCustoOportunidade.query.filter_by(
            CHAVE='ANO_MES_LIMITE'
        ).first()
        if not reg or not reg.VALOR:
            return None
        try:
            return int(reg.VALOR.strip())
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def salvar_ano_mes_limite(valor_int):
        """
        Salva o ANO_MES_LIMITE (int, ex: 203501).
        Faz UPSERT: atualiza se já existir, insere se não.
        """
        reg = ConfigCustoOportunidade.query.filter_by(
            CHAVE='ANO_MES_LIMITE'
        ).first()

        if reg:
            reg.VALOR = f'{valor_int:06d}'
            reg.DT_UPDATE = datetime.utcnow()
        else:
            reg = ConfigCustoOportunidade(
                CHAVE='ANO_MES_LIMITE',
                VALOR=f'{valor_int:06d}',
                DT_UPDATE=datetime.utcnow()
            )
            db.session.add(reg)

        db.session.commit()
        return reg