from app import db
from datetime import datetime
from decimal import Decimal


class DespesasAnalitico(db.Model):
    """Modelo para a tabela MOV_TB004_DESPESAS_ANALITICO"""
    __tablename__ = 'MOV_TB004_DESPESAS_ANALITICO'
    __table_args__ = {'schema': 'BDG'}

    # Chaves primárias
    DT_REFERENCIA = db.Column(db.Date, primary_key=True, nullable=False)
    NR_CONTRATO = db.Column(db.String(23), primary_key=True, nullable=False)
    NR_OCORRENCIA = db.Column(db.String(23), primary_key=True, nullable=False)

    # Outros campos
    DSC_ITEM_SERVICO = db.Column(db.String(100))
    DT_LANCAMENTO_PAGAMENTO = db.Column(db.Date)
    VR_DESPESA = db.Column(db.Numeric(18, 2))
    estadoLancamento = db.Column(db.String(50))
    ID_ITEM_SISCOR = db.Column(db.Integer)
    ID_ITEM_SERVICO = db.Column(db.Integer)
    NR_CTR_ORIGINAL_TABELA = db.Column(db.String(23))
    DSC_TIPO_FORMA_PGTO = db.Column(db.String(50))
    NO_ORIGEM_REGISTRO = db.Column(db.String(10))

    def __repr__(self):
        return f'<DespesaAnalitico: {self.NR_CONTRATO} - {self.NR_OCORRENCIA}>'

    @staticmethod
    def obter_proximo_numero_ocorrencia(nr_contrato):
        """Obtém o próximo número de ocorrência global (não por contrato)"""
        # Busca a maior ocorrência de TODOS os contratos para pegar a sequência global
        ultima_ocorrencia = db.session.query(
            db.func.max(
                db.func.cast(
                    db.func.right(DespesasAnalitico.NR_OCORRENCIA, 6),
                    db.Integer
                )
            )
        ).filter(
            DespesasAnalitico.NO_ORIGEM_REGISTRO == 'SUMOV'
        ).scalar()

        if ultima_ocorrencia:
            proximo_numero = ultima_ocorrencia + 1
        else:
            # Se não houver nenhuma ocorrência SUMOV, busca o maior número geral
            maior_geral = db.session.query(
                db.func.max(
                    db.func.cast(
                        db.func.right(DespesasAnalitico.NR_OCORRENCIA, 6),
                        db.Integer
                    )
                )
            ).scalar()

            if maior_geral:
                proximo_numero = maior_geral + 1
            else:
                proximo_numero = 1  # Começa do 1 se não houver nenhum registro

        # Formata o número de ocorrência com zeros
        if proximo_numero < 10:
            numero_formatado = f"{nr_contrato}00000{proximo_numero}"
        elif proximo_numero < 100:
            numero_formatado = f"{nr_contrato}0000{proximo_numero}"
        elif proximo_numero < 1000:
            numero_formatado = f"{nr_contrato}000{proximo_numero}"
        elif proximo_numero < 10000:
            numero_formatado = f"{nr_contrato}00{proximo_numero}"
        elif proximo_numero < 100000:
            numero_formatado = f"{nr_contrato}0{proximo_numero}"
        else:
            numero_formatado = f"{nr_contrato}{proximo_numero}"

        return numero_formatado

    @staticmethod
    def listar_despesas_sumov():
        """Lista apenas as despesas com origem SUMOV"""
        return DespesasAnalitico.query.filter_by(
            NO_ORIGEM_REGISTRO='SUMOV'
        ).order_by(
            DespesasAnalitico.DT_REFERENCIA.desc(),
            DespesasAnalitico.NR_OCORRENCIA.desc()
        ).all()

    @staticmethod
    def verificar_registro_existente(nr_contrato, id_item_servico, dt_lancamento, vr_despesa):
        """
        Verifica se já existe um registro com os mesmos dados.
        Retorna:
            - None: se não existir registro duplicado
            - 'SUMOV': se existir registro com origem SUMOV
            - 'SISGEA': se existir registro com origem SISGEA
        """
        from datetime import date

        # Converter dt_lancamento para date se for datetime
        if isinstance(dt_lancamento, str):
            dt_lancamento = datetime.strptime(dt_lancamento, '%Y-%m-%d').date()
        elif hasattr(dt_lancamento, 'date'):
            dt_lancamento = dt_lancamento.date()

        # Converter valor para Decimal
        if isinstance(vr_despesa, str):
            vr_despesa = Decimal(vr_despesa.replace(',', '.'))
        elif not isinstance(vr_despesa, Decimal):
            vr_despesa = Decimal(str(vr_despesa))

        # Buscar registro duplicado
        registro = DespesasAnalitico.query.filter_by(
            NR_CONTRATO=nr_contrato,
            ID_ITEM_SERVICO=id_item_servico,
            DT_LANCAMENTO_PAGAMENTO=dt_lancamento,
            VR_DESPESA=vr_despesa
        ).first()

        if registro:
            return registro.NO_ORIGEM_REGISTRO

        return None


class OcorrenciasMovItemServico(db.Model):
    """Modelo para a tabela PAR_TB015_OCORRENCIAS_MOV_ITEM_SERVICO"""
    __tablename__ = 'PAR_TB007_OCORRENCIAS_MOV_ITEM_SERVICO'
    __table_args__ = {'schema': 'BDG'}

    ID_ITEM_SERVICO = db.Column(db.Integer, primary_key=True)
    DSC_ITEM_SERVICO = db.Column(db.String(200))
    ID_ITEM_SISCOR = db.Column(db.Integer)
    DSC_RESUMIDA_DESPESA = db.Column(db.String(100))

    def __repr__(self):
        return f'<ItemServico: {self.ID_ITEM_SERVICO} - {self.DSC_ITEM_SERVICO}>'

    @staticmethod
    def listar_itens_permitidos():
        """Lista apenas os itens de serviço permitidos"""
        itens_permitidos = [32, 33, 35, 36, 66, 69, 34, 37, 42, 43, 45, 68, 73]
        return OcorrenciasMovItemServico.query.filter(
            OcorrenciasMovItemServico.ID_ITEM_SERVICO.in_(itens_permitidos)
        ).order_by(OcorrenciasMovItemServico.DSC_ITEM_SERVICO).all()