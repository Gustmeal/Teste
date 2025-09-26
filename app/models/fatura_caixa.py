from app import db
from datetime import datetime
from sqlalchemy import text
from decimal import Decimal
import os
import shutil

class FaturaCaixa(db.Model):
    __tablename__ = 'SEG_TB002_FATURA_CAIXA'
    __table_args__ = {'schema': 'BDG'}

    ID = db.Column(db.Integer, primary_key=True)
    NUM_CONTRATO_TERC = db.Column(db.DECIMAL(23, 0))
    NR_CONTRATO = db.Column(db.Integer)
    SEQ_PREMIO = db.Column(db.Integer)
    COD_PRODUTO = db.Column(db.Integer)
    COD_SUBEST = db.Column(db.Integer)
    MIP_DIF = db.Column(db.Integer)
    IND_TP_PREMIO = db.Column(db.String(1))
    DTA_ULT_MOVTO = db.Column(db.Date)
    DTA_INI_REFERENCIA = db.Column(db.Date)
    DTA_FIM_REFERENCIA = db.Column(db.Date)
    VR_PREMIO = db.Column(db.DECIMAL(10, 2))
    IOF_MIP_DIF = db.Column(db.DECIMAL(10, 2))
    COD_EVENTO = db.Column(db.Integer)
    NUM_ORI_CONTRATO = db.Column(db.Integer)
    SEQ_PREMIO_ORI = db.Column(db.Integer)
    NUM_ENDOSSO = db.Column(db.Integer)
    DTA_CARGA = db.Column(db.DateTime, default=datetime.now)
    USUARIO_CARGA = db.Column(db.String(100))
    ARQUIVO_ORIGEM = db.Column(db.String(255))
    DELETED_AT = db.Column(db.DateTime, nullable=True)

    @staticmethod
    def buscar_todos():
        """Busca todos os registros não deletados"""
        return FaturaCaixa.query.filter_by(DELETED_AT=None).all()

    @staticmethod
    def buscar_por_contrato(nr_contrato):
        """Busca registros por número de contrato"""
        return FaturaCaixa.query.filter_by(
            NR_CONTRATO=nr_contrato,
            DELETED_AT=None
        ).all()

    @staticmethod
    def verificar_duplicata(num_contrato_terc, nr_contrato, seq_premio, data_referencia):
        """Verifica se já existe um registro com essas características"""
        return FaturaCaixa.query.filter_by(
            NUM_CONTRATO_TERC=num_contrato_terc,
            NR_CONTRATO=nr_contrato,
            SEQ_PREMIO=seq_premio,
            DTA_INI_REFERENCIA=data_referencia,
            DELETED_AT=None
        ).first()

    @staticmethod
    def contar_registros_arquivo(arquivo_origem):
        """Conta quantos registros já foram importados de um arquivo específico"""
        return FaturaCaixa.query.filter_by(
            ARQUIVO_ORIGEM=arquivo_origem,
            DELETED_AT=None
        ).count()

    @staticmethod
    def importar_dados_txt(arquivo_path, usuario):
        """Importa dados do arquivo TXT para a tabela - PRESERVANDO DADOS ANTERIORES"""
        registros_inseridos = 0
        registros_duplicados = 0
        nome_arquivo = arquivo_path.split('\\')[-1]

        # Verificar se este arquivo já foi processado
        ja_processado = FaturaCaixa.contar_registros_arquivo(nome_arquivo)
        if ja_processado > 0:
            return 0, f"Este arquivo ({nome_arquivo}) já foi processado anteriormente com {ja_processado} registros."

        try:
            with open(arquivo_path, 'r', encoding='latin-1') as arquivo:
                for linha in arquivo:
                    # Remove espaços e quebras de linha
                    linha = linha.strip()
                    if not linha:
                        continue

                    # Divide a linha por ponto e vírgula
                    campos = linha.split(';')

                    if len(campos) >= 16:
                        # Preparar dados para verificar duplicata
                        num_contrato_terc = Decimal(campos[0]) if campos[0] else None
                        nr_contrato = int(campos[1]) if campos[1] else None
                        seq_premio = int(campos[2]) if campos[2] else None

                        # Converter data de início de referência para verificar duplicata
                        data_ini_ref = None
                        try:
                            if campos[8]:
                                data_ini_ref = datetime.strptime(campos[8], '%Y-%m-%d').date()
                        except:
                            pass

                        # Verificar se já existe este registro (evitar duplicatas)
                        if num_contrato_terc and nr_contrato and seq_premio and data_ini_ref:
                            duplicata = FaturaCaixa.verificar_duplicata(
                                num_contrato_terc,
                                nr_contrato,
                                seq_premio,
                                data_ini_ref
                            )

                            if duplicata:
                                registros_duplicados += 1
                                continue  # Pula para o próximo registro

                        # Cria novo registro apenas se não for duplicata
                        novo_registro = FaturaCaixa()

                        # Mapeia os campos do arquivo para as colunas
                        novo_registro.NUM_CONTRATO_TERC = num_contrato_terc
                        novo_registro.NR_CONTRATO = nr_contrato
                        novo_registro.SEQ_PREMIO = seq_premio
                        novo_registro.COD_PRODUTO = int(campos[3]) if campos[3] else None
                        novo_registro.COD_SUBEST = int(campos[4]) if campos[4] else None
                        novo_registro.MIP_DIF = int(campos[5]) if campos[5] and campos[5] in ['1', '2'] else None
                        novo_registro.IND_TP_PREMIO = campos[6] if campos[6] else None

                        # Converte datas (formato YYYY-MM-DD)
                        try:
                            novo_registro.DTA_ULT_MOVTO = datetime.strptime(campos[7], '%Y-%m-%d').date() if campos[
                                7] else None
                        except:
                            novo_registro.DTA_ULT_MOVTO = None

                        novo_registro.DTA_INI_REFERENCIA = data_ini_ref

                        try:
                            novo_registro.DTA_FIM_REFERENCIA = datetime.strptime(campos[9], '%Y-%m-%d').date() if \
                            campos[9] else None
                        except:
                            novo_registro.DTA_FIM_REFERENCIA = None

                        # Converte valores decimais (substitui vírgula por ponto)
                        novo_registro.VR_PREMIO = Decimal(campos[10].replace(',', '.')) if campos[10] else 0
                        novo_registro.IOF_MIP_DIF = Decimal(campos[11].replace(',', '.')) if campos[11] else 0

                        novo_registro.COD_EVENTO = int(campos[12]) if campos[12] else None
                        novo_registro.NUM_ORI_CONTRATO = int(campos[13]) if campos[13] else None
                        novo_registro.SEQ_PREMIO_ORI = int(campos[14]) if campos[14] else None
                        novo_registro.NUM_ENDOSSO = int(campos[15]) if campos[15] else None

                        # Dados de controle
                        novo_registro.USUARIO_CARGA = usuario
                        novo_registro.ARQUIVO_ORIGEM = nome_arquivo
                        novo_registro.DTA_CARGA = datetime.now()

                        db.session.add(novo_registro)
                        registros_inseridos += 1

                db.session.commit()

                mensagem = f"{registros_inseridos} novos registros inseridos"
                if registros_duplicados > 0:
                    mensagem += f" ({registros_duplicados} registros já existentes foram ignorados)"

                return registros_inseridos, mensagem

        except Exception as e:
            db.session.rollback()
            return 0, str(e)

    @staticmethod
    def obter_estatisticas():
        """Retorna estatísticas dos dados carregados"""
        sql = text("""
            SELECT 
                COUNT(DISTINCT NR_CONTRATO) as total_contratos,
                COUNT(*) as total_registros,
                SUM(VR_PREMIO) as valor_total,
                MIN(DTA_INI_REFERENCIA) as data_mais_antiga,
                MAX(DTA_FIM_REFERENCIA) as data_mais_recente,
                COUNT(DISTINCT ARQUIVO_ORIGEM) as total_arquivos_processados
            FROM BDG.SEG_TB002_FATURA_CAIXA
            WHERE DELETED_AT IS NULL
        """)

        resultado = db.session.execute(sql).fetchone()

        return {
            'total_contratos': resultado[0] or 0,
            'total_registros': resultado[1] or 0,
            'valor_total': float(resultado[2] or 0),
            'data_mais_antiga': resultado[3],
            'data_mais_recente': resultado[4],
            'total_arquivos': resultado[5] or 0
        }


    @staticmethod
    def obter_referencias_arquivo_final():
        """Obtém as referências disponíveis na view SEG_TB001_ARQUIVO_FINAL_TI"""
        sql = text("""
            SELECT DISTINCT REFERENCIA
            FROM BDG.SEG_TB001_ARQUIVO_FINAL_TI
            WHERE REFERENCIA IS NOT NULL
            ORDER BY REFERENCIA DESC
        """)

        resultado = db.session.execute(sql).fetchall()
        return [r[0] for r in resultado]


    @staticmethod
    def obter_dados_arquivo_final(referencia):
        """Obtém os dados da view filtrados por referência"""
        sql = text("""
            SELECT 
                COD_SUBEST,
                COD_PRODUTO,
                NR_CONTRATO,
                NOME_TITULAR,
                PREMIO_MIP,
                IOF_MIP,
                PRM_MIP_ATRASO,
                IOF_MIP_ATRASO,
                PREMIO_DFI,
                IOF_DFI,
                PRM_DFI_ATRASO,
                IOF_DFI_ATRASO,
                REMUNER_MES,
                REFERENCIA,
                OBS,
                MIP_DIF
            FROM BDG.SEG_TB001_ARQUIVO_FINAL_TI
            WHERE REFERENCIA = :referencia
            ORDER BY NR_CONTRATO
        """)

        resultado = db.session.execute(sql, {'referencia': referencia}).fetchall()

        dados = []
        for row in resultado:
            dados.append({
                'COD_SUBEST': row[0],
                'COD_PRODUTO': row[1],
                'NR_CONTRATO': row[2],
                'NOME_TITULAR': row[3],
                'PREMIO_MIP': float(row[4]) if row[4] else 0,
                'IOF_MIP': float(row[5]) if row[5] else 0,
                'PRM_MIP_ATRASO': float(row[6]) if row[6] else 0,
                'IOF_MIP_ATRASO': float(row[7]) if row[7] else 0,
                'PREMIO_DFI': float(row[8]) if row[8] else 0,
                'IOF_DFI': float(row[9]) if row[9] else 0,
                'PRM_DFI_ATRASO': float(row[10]) if row[10] else 0,
                'IOF_DFI_ATRASO': float(row[11]) if row[11] else 0,
                'REMUNER_MES': float(row[12]) if row[12] else 0,
                'REFERENCIA': row[13],
                'OBS': row[14],
                'MIP_DIF': row[15]
            })

        return dados