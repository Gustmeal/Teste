from flask import Blueprint, render_template, redirect, url_for, request, send_file, flash
from flask_login import login_required, current_user
from app import db
from app.models.edital import Edital
from app.models.periodo import PeriodoAvaliacao
from app.models.empresa_participante import EmpresaParticipante
from app.models.empresa_responsavel import EmpresaResponsavel
from app.models.meta_avaliacao import MetaAvaliacao
from app.models.limite_distribuicao import LimiteDistribuicao
from app.models.codigo_contabil import CodigoContabil
from app.models.vinculacao import ItemContaSucor, DescricaoItensSiscor
from app.models.criterio_selecao import CriterioSelecao
from app.utils.excel_export import export_to_excel
from app.utils.pdf_export import export_to_pdf
from datetime import datetime
import os
import tempfile
import io

export_bp = Blueprint('export', __name__, url_prefix='/exportacao')


@export_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@export_bp.route('/')
@login_required
def index():
    # Configuração de todos os sistemas e módulos disponíveis para exportação
    sistemas = [
        {
            'id': 'credenciamento',
            'nome': 'Assessoria de Cobranças',
            'modulos': [
                {'id': 'editais', 'nome': 'Editais'},
                {'id': 'periodos', 'nome': 'Períodos'},
                {'id': 'empresas', 'nome': 'Empresas Participantes'},
                {'id': 'metas', 'nome': 'Metas de Avaliação'},
                {'id': 'limites', 'nome': 'Limites de Distribuição'},
                {'id': 'criterios', 'nome': 'Critérios de Seleção'}
            ]
        },
        {
            'id': 'codigos_contabeis',
            'nome': 'Códigos Contábeis',
            'modulos': [
                {'id': 'codigos', 'nome': 'Códigos Contábeis'},
                {'id': 'vinculacoes', 'nome': 'Vinculações SISCOR'},
                {'id': 'itens_siscor', 'nome': 'Itens SISCOR'}
            ]
        },
        {
            'id': 'auditoria',
            'nome': 'Auditoria e Logs',
            'modulos': [
                {'id': 'logs_auditoria', 'nome': 'Logs de Auditoria'},
                {'id': 'usuarios', 'nome': 'Usuários do Sistema'}
            ]
        }
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
            flash('Não há dados disponíveis para exportação.', 'info')
            return redirect(url_for('export.index'))

        # Gerar arquivo baseado no formato
        if formato == 'excel':
            # Criar arquivo temporário
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
            filepath = temp_file.name
            temp_file.close()

            # Exportar para Excel
            export_to_excel(dados, colunas, titulo, filepath)

            # Ler arquivo para enviar
            with open(filepath, 'rb') as f:
                arquivo = io.BytesIO(f.read())

            # Remover arquivo temporário
            os.unlink(filepath)

            arquivo.seek(0)
            mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            download_name = f'{titulo}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'

        elif formato == 'pdf':
            # Criar arquivo temporário
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
            filepath = temp_file.name
            temp_file.close()

            # Exportar para PDF
            export_to_pdf(dados, colunas, titulo, filepath)

            # Ler arquivo para enviar
            with open(filepath, 'rb') as f:
                arquivo = io.BytesIO(f.read())

            # Remover arquivo temporário
            os.unlink(filepath)

            arquivo.seek(0)
            mimetype = 'application/pdf'
            download_name = f'{titulo}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf'

        elif formato == 'word':
            # Importar função de exportação Word
            from app.utils.word_export import export_to_word

            # Exportar para Word
            arquivo = export_to_word(dados, colunas, titulo)
            mimetype = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
            download_name = f'{titulo}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.docx'

        else:
            flash('Formato de exportação inválido.', 'danger')
            return redirect(url_for('export.index'))

        # Retornar arquivo para download
        return send_file(
            arquivo,
            mimetype=mimetype,
            as_attachment=True,
            download_name=download_name
        )

    except Exception as e:
        flash(f'Erro ao gerar relatório: {str(e)}', 'danger')
        return redirect(url_for('export.index'))


def get_data_for_export(sistema, modulo):
    """
    Retorna dados, colunas e título baseado no sistema e módulo selecionado
    """
    dados = []
    colunas = []
    titulo = ''

    # Sistema de Credenciamento (Assessoria de Cobranças)
    if sistema == 'credenciamento':
        if modulo == 'editais':
            # Buscar dados de editais
            editais = Edital.query.filter(Edital.DELETED_AT == None).all()
            colunas = ['Número', 'Ano', 'Data Início', 'Data Fim', 'Descrição']
            titulo = 'Editais de Credenciamento'

            for edital in editais:
                dados.append({
                    'Número': edital.NU_EDITAL,
                    'Ano': edital.ANO,
                    'Data Início': edital.DT_INICIO.strftime('%d/%m/%Y') if edital.DT_INICIO else '',
                    'Data Fim': edital.DT_FIM.strftime('%d/%m/%Y') if edital.DT_FIM else '',
                    'Descrição': edital.DESCRICAO or ''
                })

        elif modulo == 'periodos':
            # Buscar dados de períodos com seus editais
            from sqlalchemy.orm import joinedload
            periodos = PeriodoAvaliacao.query.options(
                joinedload(PeriodoAvaliacao.edital)
            ).filter(PeriodoAvaliacao.DELETED_AT == None).all()

            colunas = ['Edital', 'Período', 'Data Início', 'Data Fim', 'Dias Úteis']
            titulo = 'Períodos de Avaliação'

            for periodo in periodos:
                dados.append({
                    'Edital': f"{periodo.edital.NU_EDITAL}/{periodo.edital.ANO}" if periodo.edital else '',
                    'Período': periodo.ID_PERIODO,
                    'Data Início': periodo.DT_INICIO.strftime('%d/%m/%Y') if periodo.DT_INICIO else '',
                    'Data Fim': periodo.DT_FIM.strftime('%d/%m/%Y') if periodo.DT_FIM else '',
                    'Dias Úteis': periodo.DIAS_UTEIS or 0
                })

        elif modulo == 'empresas':
            # Buscar dados de empresas participantes
            empresas = db.session.query(
                EmpresaParticipante,
                EmpresaResponsavel,
                Edital,
                PeriodoAvaliacao
            ).join(
                EmpresaResponsavel,
                EmpresaParticipante.ID_EMPRESA == EmpresaResponsavel.pkEmpresaResponsavelCobranca
            ).join(
                Edital,
                EmpresaParticipante.ID_EDITAL == Edital.ID
            ).join(
                PeriodoAvaliacao,
                EmpresaParticipante.ID_PERIODO == PeriodoAvaliacao.ID
            ).filter(
                EmpresaParticipante.DELETED_AT == None
            ).all()

            colunas = ['Empresa', 'Edital', 'Período', 'Condição', 'Data Descredenciamento']
            titulo = 'Empresas Participantes'

            for emp_part, emp_resp, edital, periodo in empresas:
                dados.append({
                    'Empresa': emp_resp.NO_ABREVIADO_EMPRESA,
                    'Edital': f"{edital.NU_EDITAL}/{edital.ANO}",
                    'Período': periodo.ID_PERIODO,
                    'Condição': emp_part.DS_CONDICAO,
                    'Data Descredenciamento': emp_part.DT_DESCREDENCIAMENTO.strftime(
                        '%d/%m/%Y') if emp_part.DT_DESCREDENCIAMENTO else ''
                })

        elif modulo == 'metas':
            # Buscar metas de avaliação
            metas = db.session.query(
                MetaAvaliacao,
                EmpresaResponsavel,
                Edital,
                PeriodoAvaliacao
            ).join(
                EmpresaResponsavel,
                MetaAvaliacao.ID_EMPRESA == EmpresaResponsavel.pkEmpresaResponsavelCobranca
            ).join(
                Edital,
                MetaAvaliacao.ID_EDITAL == Edital.ID
            ).join(
                PeriodoAvaliacao,
                MetaAvaliacao.ID_PERIODO == PeriodoAvaliacao.ID
            ).filter(
                MetaAvaliacao.DELETED_AT == None
            ).all()

            colunas = ['Empresa', 'Edital', 'Período', 'Competência', 'Meta Arrecadação',
                       'Meta Acionamento', 'Meta Liquidação', 'Meta Bonificação']
            titulo = 'Metas de Avaliação'

            for meta, empresa, edital, periodo in metas:
                dados.append({
                    'Empresa': empresa.NO_ABREVIADO_EMPRESA,
                    'Edital': f"{edital.NU_EDITAL}/{edital.ANO}",
                    'Período': periodo.ID_PERIODO,
                    'Competência': meta.COMPETENCIA,
                    'Meta Arrecadação': f"R$ {meta.META_ARRECADACAO:,.2f}" if meta.META_ARRECADACAO else 'R$ 0,00',
                    'Meta Acionamento': f"{meta.META_ACIONAMENTO:.2f}%" if meta.META_ACIONAMENTO else '-',
                    'Meta Liquidação': f"{meta.META_LIQUIDACAO:.2f}%" if meta.META_LIQUIDACAO else '-',
                    'Meta Bonificação': f"R$ {meta.META_BONIFICACAO:,.2f}" if meta.META_BONIFICACAO else 'R$ 0,00'
                })

        elif modulo == 'limites':
            # Buscar limites de distribuição
            limites = db.session.query(
                LimiteDistribuicao,
                EmpresaResponsavel,
                CriterioSelecao,
                Edital,
                PeriodoAvaliacao
            ).join(
                EmpresaResponsavel,
                LimiteDistribuicao.ID_EMPRESA == EmpresaResponsavel.pkEmpresaResponsavelCobranca
            ).join(
                CriterioSelecao,
                LimiteDistribuicao.COD_CRITERIO_SELECAO == CriterioSelecao.COD
            ).join(
                Edital,
                LimiteDistribuicao.ID_EDITAL == Edital.ID
            ).join(
                PeriodoAvaliacao,
                LimiteDistribuicao.ID_PERIODO == PeriodoAvaliacao.ID
            ).filter(
                LimiteDistribuicao.DELETED_AT == None
            ).all()

            colunas = ['Empresa', 'Edital', 'Período', 'Critério', 'Qtde Máxima',
                       'Valor Máximo', 'Percentual Final']
            titulo = 'Limites de Distribuição'

            for limite, empresa, criterio, edital, periodo in limites:
                dados.append({
                    'Empresa': empresa.NO_ABREVIADO_EMPRESA,
                    'Edital': f"{edital.NU_EDITAL}/{edital.ANO}",
                    'Período': periodo.ID_PERIODO,
                    'Critério': criterio.DS_CRITERIO_SELECAO,
                    'Qtde Máxima': limite.QTDE_MAXIMA or 0,
                    'Valor Máximo': f"R$ {limite.VALOR_MAXIMO:,.2f}" if limite.VALOR_MAXIMO else 'R$ 0,00',
                    'Percentual Final': f"{limite.PERCENTUAL_FINAL:.2f}%" if limite.PERCENTUAL_FINAL else '0,00%'
                })

        elif modulo == 'criterios':
            # Buscar critérios de seleção
            criterios = CriterioSelecao.query.filter(
                CriterioSelecao.DELETED_AT == None
            ).order_by(CriterioSelecao.COD.asc()).all()

            colunas = ['Código', 'Descrição']
            titulo = 'Critérios de Seleção'

            for criterio in criterios:
                dados.append({
                    'Código': criterio.COD,
                    'Descrição': criterio.DS_CRITERIO_SELECAO
                })

    # Sistema de Códigos Contábeis
    elif sistema == 'codigos_contabeis':
        if modulo == 'codigos':
            # Buscar códigos contábeis
            codigos = CodigoContabil.query.order_by(
                CodigoContabil.ANO.desc(),
                CodigoContabil.CODIGO.asc()
            ).all()

            colunas = ['Código', 'Ano', 'Descrição', 'Quebra', 'Código Rubrica', 'Totalização']
            titulo = 'Códigos Contábeis PDG'

            for codigo in codigos:
                ind_tot = ''
                if codigo.IND_TOTALIZACAO == 1:
                    ind_tot = 'Soma'
                elif codigo.IND_TOTALIZACAO == 2:
                    ind_tot = 'Resultado'

                dados.append({
                    'Código': codigo.CODIGO,
                    'Ano': codigo.ANO,
                    'Descrição': codigo.DSC_CODIGO,
                    'Quebra': codigo.NO_QUEBRA or '',
                    'Código Rubrica': codigo.COD_RUBRICA or '',
                    'Totalização': ind_tot
                })

        elif modulo == 'vinculacoes':
            # Buscar vinculações
            vinculacoes = db.session.query(
                ItemContaSucor,
                DescricaoItensSiscor
            ).join(
                DescricaoItensSiscor,
                ItemContaSucor.ID_ITEM == DescricaoItensSiscor.ID_ITEM
            ).order_by(
                ItemContaSucor.ANO.desc(),
                ItemContaSucor.CODIGO.asc()
            ).all()

            colunas = ['Código Contábil', 'Ano', 'Item SISCOR', 'Descrição Item', 'Arquivo']
            titulo = 'Vinculações SISCOR'

            for vinc, item in vinculacoes:
                dados.append({
                    'Código Contábil': vinc.CODIGO,
                    'Ano': vinc.ANO,
                    'Item SISCOR': vinc.ID_ITEM,
                    'Descrição Item': item.DESCRICAO,
                    'Arquivo': vinc.NM_ARQUIVO or ''
                })

        elif modulo == 'itens_siscor':
            # Buscar descrições de itens SISCOR
            itens = DescricaoItensSiscor.query.order_by(
                DescricaoItensSiscor.ID_ITEM.asc()
            ).all()

            colunas = ['ID Item', 'Descrição']
            titulo = 'Itens SISCOR'

            for item in itens:
                dados.append({
                    'ID Item': item.ID_ITEM,
                    'Descrição': item.DESCRICAO
                })

    # Sistema de Auditoria
    elif sistema == 'auditoria':
        if modulo == 'logs_auditoria':
            # Buscar logs de auditoria
            from app.models.audit_log import AuditLog
            from app.models.usuario import Usuario

            logs = db.session.query(
                AuditLog,
                Usuario
            ).join(
                Usuario,
                AuditLog.USUARIO_ID == Usuario.ID
            ).order_by(
                AuditLog.DATA_HORA.desc()
            ).limit(1000).all()

            colunas = ['Data/Hora', 'Usuário', 'Ação', 'Tabela', 'Registro ID', 'Detalhes']
            titulo = 'Logs de Auditoria'

            for log, usuario in logs:
                dados.append({
                    'Data/Hora': log.DATA_HORA.strftime('%d/%m/%Y %H:%M:%S'),
                    'Usuário': usuario.NOME,
                    'Ação': log.ACAO,
                    'Tabela': log.TABELA,
                    'Registro ID': log.REGISTRO_ID or '',
                    'Detalhes': log.DETALHES or ''
                })

        elif modulo == 'usuarios':
            # Buscar usuários do sistema
            from app.models.usuario import Usuario

            usuarios = Usuario.query.filter(
                Usuario.DELETED_AT == None
            ).order_by(Usuario.NOME.asc()).all()

            colunas = ['Nome', 'Login', 'Email', 'Perfil', 'Ativo', 'Último Acesso']
            titulo = 'Usuários do Sistema'

            for usuario in usuarios:
                dados.append({
                    'Nome': usuario.NOME,
                    'Login': usuario.LOGIN,
                    'Email': usuario.EMAIL or '',
                    'Perfil': usuario.PERFIL.capitalize(),
                    'Ativo': 'Sim' if usuario.ATIVO else 'Não',
                    'Último Acesso': usuario.ULTIMO_ACESSO.strftime(
                        '%d/%m/%Y %H:%M') if usuario.ULTIMO_ACESSO else 'Nunca'
                })

    return dados, colunas, titulo