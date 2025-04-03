from flask import Blueprint, render_template, redirect, url_for, request, send_file, flash
from flask_login import login_required, current_user
from app import db
from app.models.edital import Edital
from app.models.periodo import PeriodoAvaliacao
from app.models.empresa_participante import EmpresaParticipante
from app.models.empresa_responsavel import EmpresaResponsavel
from app.models.meta_avaliacao import MetaAvaliacao
from app.models.limite_distribuicao import LimiteDistribuicao
from app.utils.excel_export import export_to_excel
from app.utils.pdf_export import export_to_pdf
from datetime import datetime
import os
import tempfile

export_bp = Blueprint('export', __name__, url_prefix='/exportacao')


@export_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@export_bp.route('/')
@login_required
def index():
    # Configuração de sistemas e módulos disponíveis para exportação
    sistemas = [
        {
            'id': 'credenciamento',
            'nome': 'Assessoria de Cobranças',
            'modulos': [
                {'id': 'editais', 'nome': 'Editais'},
                {'id': 'periodos', 'nome': 'Períodos'},
                {'id': 'empresas', 'nome': 'Empresas Participantes'},
                {'id': 'metas', 'nome': 'Metas de Avaliação'},
                {'id': 'limites', 'nome': 'Limites de Distribuição'}
            ]
        }
        # Adicione outros sistemas conforme necessário
    ]

    return render_template('export/index.html', sistemas=sistemas)


@export_bp.route('/gerar', methods=['POST'])
@login_required
def gerar_relatorio():
    try:
        sistema = request.form.get('sistema')
        modulo = request.form.get('modulo')
        formato = request.form.get('formato')

        if not all([sistema, modulo, formato]):
            flash('Por favor, selecione todas as opções.', 'warning')
            return redirect(url_for('export.index'))

        # Obter dados com base no módulo selecionado
        dados, colunas, titulo = get_data_for_export(sistema, modulo)

        if not dados:
            flash('Não há dados disponíveis para exportação.', 'warning')
            return redirect(url_for('export.index'))

        # Gerar arquivo temporário
        temp_dir = tempfile.gettempdir()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{modulo}_{timestamp}"

        try:
            if formato == 'excel':
                filepath = os.path.join(temp_dir, f"{filename}.xlsx")
                export_to_excel(dados, colunas, titulo, filepath)
                mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                download_name = f"{titulo}.xlsx"
            elif formato == 'pdf':
                filepath = os.path.join(temp_dir, f"{filename}.pdf")
                export_to_pdf(dados, colunas, titulo, filepath)
                mimetype = 'application/pdf'
                download_name = f"{titulo}.pdf"
            else:
                flash('Formato não suportado.', 'danger')
                return redirect(url_for('export.index'))

            # Esta função "as_attachment=True" causa o download imediato
            # e "download_name" define o nome do arquivo baixado
            return send_file(filepath,
                             mimetype=mimetype,
                             as_attachment=True,
                             download_name=download_name)

        finally:
            # Tenta remover o arquivo temporário após o download, mas
            # de forma segura que não interrompa o fluxo
            try:
                if os.path.exists(filepath):
                    # Agenda a remoção do arquivo para ser feita depois
                    # do download completo (não bloqueante)
                    import threading
                    threading.Timer(30.0, lambda: os.remove(filepath) if os.path.exists(filepath) else None).start()
            except:
                # Ignora erros de remoção do arquivo
                pass

    except Exception as e:
        flash(f'Erro ao gerar relatório: {str(e)}', 'danger')
        return redirect(url_for('export.index'))

def get_data_for_export(sistema, modulo):
    """Obtém os dados para exportação com base no sistema e módulo selecionados"""
    if sistema == 'credenciamento':
        if modulo == 'editais':
            # Exportar editais
            editais = Edital.query.filter(Edital.DELETED_AT == None).all()
            dados = []
            for edital in editais:
                dados.append({
                    'ID': edital.ID,
                    'Número': edital.NU_EDITAL,
                    'Ano': edital.ANO,
                    'Descrição': edital.DESCRICAO,
                    'Data de Criação': edital.CREATED_AT.strftime('%d/%m/%Y %H:%M') if edital.CREATED_AT else '',
                    'Última Atualização': edital.UPDATED_AT.strftime('%d/%m/%Y %H:%M') if edital.UPDATED_AT else ''
                })
            colunas = ['ID', 'Número', 'Ano', 'Descrição', 'Data de Criação', 'Última Atualização']
            titulo = 'Lista de Editais'

        elif modulo == 'periodos':
            # Exportar períodos
            periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).all()
            dados = []
            for periodo in periodos:
                edital = Edital.query.get(periodo.ID_EDITAL)
                dados.append({
                    'ID': periodo.ID,
                    'ID Período': periodo.ID_PERIODO,
                    'Edital': f"{edital.NU_EDITAL}/{edital.ANO}" if edital else '',
                    'Data Início': periodo.DT_INICIO.strftime('%d/%m/%Y') if periodo.DT_INICIO else '',
                    'Data Fim': periodo.DT_FIM.strftime('%d/%m/%Y') if periodo.DT_FIM else '',
                    'Data de Criação': periodo.CREATED_AT.strftime('%d/%m/%Y %H:%M') if periodo.CREATED_AT else ''
                })
            colunas = ['ID', 'ID Período', 'Edital', 'Data Início', 'Data Fim', 'Data de Criação']
            titulo = 'Lista de Períodos'

        elif modulo == 'empresas':
            # Exportar empresas participantes
            empresas = db.session.query(
                EmpresaParticipante,
                PeriodoAvaliacao,
                Edital
            ).join(
                PeriodoAvaliacao,
                EmpresaParticipante.ID_PERIODO == PeriodoAvaliacao.ID_PERIODO
            ).join(
                Edital,
                EmpresaParticipante.ID_EDITAL == Edital.ID
            ).filter(
                EmpresaParticipante.DELETED_AT == None
            ).all()

            dados = []
            for empresa, periodo, edital in empresas:
                dados.append({
                    'ID': empresa.ID,
                    'ID Empresa': empresa.ID_EMPRESA,
                    'Nome Empresa': empresa.NO_EMPRESA,
                    'Nome Abreviado': empresa.NO_EMPRESA_ABREVIADA,
                    'Condição': empresa.DS_CONDICAO,
                    'Edital': f"{edital.NU_EDITAL}/{edital.ANO}",
                    'Período': periodo.ID_PERIODO,
                    'Data de Cadastro': empresa.CREATED_AT.strftime('%d/%m/%Y %H:%M') if empresa.CREATED_AT else ''
                })
            colunas = ['ID', 'ID Empresa', 'Nome Empresa', 'Nome Abreviado', 'Condição', 'Edital', 'Período',
                       'Data de Cadastro']
            titulo = 'Lista de Empresas Participantes'

        elif modulo == 'metas':
            # Exportar metas de avaliação
            metas = MetaAvaliacao.query.filter(MetaAvaliacao.DELETED_AT == None).all()
            dados = []
            for meta in metas:
                edital = Edital.query.get(meta.ID_EDITAL)
                periodo = PeriodoAvaliacao.query.filter_by(ID_PERIODO=meta.ID_PERIODO).first()
                dados.append({
                    'ID': meta.ID,
                    'Competência': meta.COMPETENCIA,
                    'Edital': f"{edital.NU_EDITAL}/{edital.ANO}" if edital else '',
                    'Período': periodo.ID_PERIODO if periodo else '',
                    'ID Empresa': meta.ID_EMPRESA,
                    'Meta Arrecadação': meta.META_ARRECADACAO,
                    'Meta Acionamento': meta.META_ACIONAMENTO,
                    'Meta Liquidação': meta.META_LIQUIDACAO,
                    'Meta Bonificação': meta.META_BONIFICACAO
                })
            colunas = ['ID', 'Competência', 'Edital', 'Período', 'ID Empresa',
                       'Meta Arrecadação', 'Meta Acionamento', 'Meta Liquidação', 'Meta Bonificação']
            titulo = 'Lista de Metas de Avaliação'

        elif modulo == 'limites':
            # Exportar limites de distribuição
            limites = LimiteDistribuicao.query.filter(LimiteDistribuicao.DELETED_AT == None).all()
            dados = []
            for limite in limites:
                edital = Edital.query.get(limite.ID_EDITAL)
                periodo = PeriodoAvaliacao.query.filter_by(ID_PERIODO=limite.ID_PERIODO).first()
                dados.append({
                    'ID': limite.ID,
                    'Edital': f"{edital.NU_EDITAL}/{edital.ANO}" if edital else '',
                    'Período': periodo.ID_PERIODO if periodo else '',
                    'ID Empresa': limite.ID_EMPRESA,
                    'Critério Seleção': limite.COD_CRITERIO_SELECAO,
                    'Quantidade Máxima': limite.QTDE_MAXIMA,
                    'Valor Máximo': limite.VALOR_MAXIMO,
                    'Percentual Final': limite.PERCENTUAL_FINAL
                })
            colunas = ['ID', 'Edital', 'Período', 'ID Empresa', 'Critério Seleção',
                       'Quantidade Máxima', 'Valor Máximo', 'Percentual Final']
            titulo = 'Lista de Limites de Distribuição'
        else:
            return [], [], ''
    else:
        return [], [], ''

    return dados, colunas, titulo