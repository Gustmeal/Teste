"""
Model para Deliberação de Penalidades ANS
Tabela: BDDASHBOARDBI.BDG.MOV_TB037_DELIBERACAO_ANS
"""
from app import db
from datetime import datetime


class DeliberacaoANS(db.Model):
    """
    Model para salvar deliberações de penalidades ANS calculadas
    """
    __tablename__ = 'MOV_TB037_DELIBERACAO_ANS'
    __table_args__ = {'schema': 'BDG'}

    # Chave Primária
    NU_CONTRATO = db.Column(db.String(50), primary_key=True, nullable=False)

    # Dados Gerais
    DT_ESTOQUE = db.Column(db.Date, nullable=False)
    VR_ANS = db.Column(db.Numeric(18, 2), nullable=False)

    # Contrato s/nº
    QTD_MESES_SN = db.Column(db.Integer, nullable=True)
    VR_PENALIDADE_SN = db.Column(db.Numeric(18, 2), nullable=True)

    # Contrato 03/2014
    QTD_MESES_03_2014 = db.Column(db.Integer, nullable=True)
    VR_PENALIDADE_03_2014 = db.Column(db.Numeric(18, 2), nullable=True)

    # Contrato 13/2019
    QTD_MESES_13_2019 = db.Column(db.Integer, nullable=True)
    VR_PENALIDADE_13_2019 = db.Column(db.Numeric(18, 2), nullable=True)

    # Campos de Auditoria
    CREATED_AT = db.Column(db.DateTime, default=datetime.utcnow)
    UPDATED_AT = db.Column(db.DateTime, onupdate=datetime.utcnow)

    def __repr__(self):
        return f'<DeliberacaoANS {self.NU_CONTRATO}>'

    @staticmethod
    def buscar_por_contrato(nu_contrato):
        """Busca deliberação por número de contrato"""
        return DeliberacaoANS.query.filter_by(NU_CONTRATO=nu_contrato).first()

    def salvar(self):
        """Salva ou atualiza a deliberação no banco"""
        try:
            # Verificar se já existe
            existe = DeliberacaoANS.buscar_por_contrato(self.NU_CONTRATO)

            if existe:
                # Atualizar registro existente
                existe.DT_ESTOQUE = self.DT_ESTOQUE
                existe.VR_ANS = self.VR_ANS
                existe.QTD_MESES_SN = self.QTD_MESES_SN
                existe.VR_PENALIDADE_SN = self.VR_PENALIDADE_SN
                existe.QTD_MESES_03_2014 = self.QTD_MESES_03_2014
                existe.VR_PENALIDADE_03_2014 = self.VR_PENALIDADE_03_2014
                existe.QTD_MESES_13_2019 = self.QTD_MESES_13_2019
                existe.VR_PENALIDADE_13_2019 = self.VR_PENALIDADE_13_2019
                existe.UPDATED_AT = datetime.utcnow()
            else:
                # Adicionar novo registro
                db.session.add(self)

            db.session.commit()
            return True

        except Exception as e:
            db.session.rollback()
            print(f"[ERRO] Erro ao salvar deliberação ANS: {str(e)}")
            import traceback
            traceback.print_exc()
            return False