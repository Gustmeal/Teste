"""
Model para Penalidades ANS
Tabela: BDDASHBOARDBI.BDG.MOV_TB036_PENALIDADE_ANS
"""
from app import db


class PenalidadeANS(db.Model):
    """
    Model para gerenciar Penalidades ANS dos contratos

    Campos:
    - NU_CONTRATO (*chave): Identificador do contrato ANS (texto)
    - INI_VIGENCIA (*chave): Data de início da vigência
    - FIM_VIGENCIA (*chave): Data de fim da vigência
    - VR_TARIFA: Valor da tarifa ANS
    - PRAZO_DIAS: Prazo em dias
    """
    __tablename__ = 'MOV_TB036_PENALIDADE_ANS'
    __table_args__ = {'schema': 'BDG'}

    # Chave Primária Composta
    NU_CONTRATO = db.Column(db.String(50), primary_key=True, nullable=False)
    INI_VIGENCIA = db.Column(db.Date, primary_key=True, nullable=False)
    FIM_VIGENCIA = db.Column(db.Date, primary_key=True, nullable=False)

    # Campos de Dados
    VR_TARIFA = db.Column(db.Numeric(18, 2), nullable=False)
    PRAZO_DIAS = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return f'<PenalidadeANS {self.NU_CONTRATO} - {self.INI_VIGENCIA} a {self.FIM_VIGENCIA}>'

    @staticmethod
    def buscar_por_contrato(nu_contrato):
        """
        Busca todas as penalidades de um contrato específico

        Args:
            nu_contrato: Número do contrato

        Returns:
            Lista de PenalidadeANS
        """
        return PenalidadeANS.query.filter_by(
            NU_CONTRATO=nu_contrato
        ).order_by(
            PenalidadeANS.INI_VIGENCIA.desc()
        ).all()

    def salvar(self):
        """
        Salva ou atualiza a penalidade no banco de dados

        Returns:
            bool: True se sucesso, False se erro
        """
        try:
            # Verificar se já existe
            existe = PenalidadeANS.query.filter_by(
                NU_CONTRATO=self.NU_CONTRATO,
                INI_VIGENCIA=self.INI_VIGENCIA,
                FIM_VIGENCIA=self.FIM_VIGENCIA
            ).first()

            if existe:
                # Atualizar registro existente
                existe.VR_TARIFA = self.VR_TARIFA
                existe.PRAZO_DIAS = self.PRAZO_DIAS
            else:
                # Adicionar novo registro
                db.session.add(self)

            db.session.commit()
            return True

        except Exception as e:
            db.session.rollback()
            print(f"[ERRO] Erro ao salvar penalidade ANS: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def excluir(self):
        """
        Exclui permanentemente a penalidade (hard delete)

        Returns:
            bool: True se sucesso, False se erro
        """
        try:
            db.session.delete(self)
            db.session.commit()
            return True

        except Exception as e:
            db.session.rollback()
            print(f"[ERRO] Erro ao excluir penalidade ANS: {str(e)}")
            return False