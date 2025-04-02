from flask import Blueprint, render_template, redirect, url_for, request, flash
from app.models.empresa_participante import EmpresaParticipante
from app.models.periodo import PeriodoAvaliacao
from app.models.edital import Edital
from app import db
from datetime import datetime
from flask_login import login_required, current_user
from app.utils.audit import registrar_log
import logging

# Configurar logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

empresa_bp = Blueprint('empresa', __name__, url_prefix='/credenciamento')


@empresa_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@empresa_bp.route('/periodos/<int:periodo_id>/empresas')
@login_required
def lista_empresas(periodo_id):
    periodo = PeriodoAvaliacao.query.get_or_404(periodo_id)
    empresas = EmpresaParticipante.query.filter_by(ID_PERIODO=periodo_id, DELETED_AT=None).all()
    return render_template('credenciamento/lista_empresas.html', periodo=periodo, empresas=empresas)


@empresa_bp.route('/periodos/<int:periodo_id>/empresas/nova', methods=['GET', 'POST'])
@login_required
def nova_empresa(periodo_id):
    periodo = PeriodoAvaliacao.query.get_or_404(periodo_id)
    edital = Edital.query.get(periodo.ID_EDITAL)

    # Obter todas as empresas existentes na tabela para a caixa de seleção
    empresas_existentes = db.session.query(
        EmpresaParticipante.ID_EMPRESA,
        EmpresaParticipante.NO_EMPRESA,
        EmpresaParticipante.NO_EMPRESA_ABREVIADA
    ).filter(
        EmpresaParticipante.DELETED_AT == None
    ).distinct(EmpresaParticipante.ID_EMPRESA).all()

    # Obter condições existentes para a caixa de seleção
    condicoes_existentes = db.session.query(
        EmpresaParticipante.DS_CONDICAO
    ).filter(
        EmpresaParticipante.DELETED_AT == None,
        EmpresaParticipante.DS_CONDICAO != None,
        EmpresaParticipante.DS_CONDICAO != ''
    ).distinct().all()
    condicoes = [cond[0] for cond in condicoes_existentes if cond[0]]

    if request.method == 'POST':
        try:
            # Capturar dados do formulário
            id_empresa = request.form['id_empresa']
            nome_empresa = request.form.get('nome_empresa', '')
            nome_abreviado = request.form.get('nome_abreviado', '')
            ds_condicao = request.form.get('ds_condicao', '')

            logger.debug(
                f"Dados do formulário: ID={id_empresa}, Nome={nome_empresa}, Abrev={nome_abreviado}, Cond={ds_condicao}")

            # Verificar se já existe empresa com este ID neste período
            empresa_existente = EmpresaParticipante.query.filter_by(
                ID_PERIODO=periodo_id,
                ID_EMPRESA=id_empresa,
                DELETED_AT=None
            ).first()

            if empresa_existente:
                flash(f'Empresa com ID {id_empresa} já cadastrada para este período.', 'danger')
                return render_template('credenciamento/form_empresa.html',
                                       periodo=periodo,
                                       edital=edital,
                                       empresas_existentes=empresas_existentes,
                                       condicoes=condicoes)

            # Criar e salvar a nova empresa participante
            nova_empresa = EmpresaParticipante(
                ID_EDITAL=edital.ID,
                ID_PERIODO=periodo_id,
                ID_EMPRESA=id_empresa,
                NO_EMPRESA=nome_empresa,
                NO_EMPRESA_ABREVIADA=nome_abreviado,
                DS_CONDICAO=ds_condicao
            )

            # Salvar no banco de dados via ORM
            db.session.add(nova_empresa)
            db.session.commit()
            logger.debug(f"Empresa salva via ORM com ID: {nova_empresa.ID}")

            # Registrar log de auditoria
            dados_novos = {
                'id_edital': edital.ID,
                'id_periodo': periodo_id,
                'id_empresa': id_empresa,
                'no_empresa': nome_empresa,
                'no_empresa_abreviada': nome_abreviado,
                'ds_condicao': ds_condicao
            }
            registrar_log(
                acao='criar',
                entidade='empresa',
                entidade_id=nova_empresa.ID,
                descricao=f'Cadastro da empresa {nome_empresa} no período {periodo.ID_PERIODO}',
                dados_novos=dados_novos
            )

            # Verificar se o registro foi salvo
            verificacao = EmpresaParticipante.query.filter_by(
                ID_EDITAL=edital.ID,
                ID_PERIODO=periodo_id,
                ID_EMPRESA=id_empresa,
                DELETED_AT=None
            ).first()

            if verificacao:
                logger.debug(f"SUCESSO! Empresa encontrada após salvamento: ID={verificacao.ID}")
                flash('Empresa cadastrada com sucesso!', 'success')
            else:
                logger.error("ERRO! Empresa não encontrada após tentativa de salvamento")
                flash('Falha ao salvar a empresa. Tente novamente.', 'danger')

            return redirect(url_for('empresa.lista_empresas', periodo_id=periodo_id))

        except Exception as e:
            db.session.rollback()
            logger.exception(f"ERRO DETALHADO: {str(e)}")
            flash(f'Erro: {str(e)}', 'danger')

    return render_template('credenciamento/form_empresa.html',
                           periodo=periodo,
                           edital=edital,
                           empresas_existentes=empresas_existentes,
                           condicoes=condicoes)


@empresa_bp.route('/empresas/excluir/<int:id>')
@login_required
def excluir_empresa(id):
    try:
        empresa = EmpresaParticipante.query.get_or_404(id)
        periodo_id = empresa.ID_PERIODO

        # Capturar dados para auditoria
        dados_antigos = {
            'no_empresa': empresa.NO_EMPRESA,
            'id_empresa': empresa.ID_EMPRESA,
            'deleted_at': None
        }

        empresa.DELETED_AT = datetime.utcnow()
        db.session.commit()

        # Registrar log de auditoria
        dados_novos = {
            'deleted_at': empresa.DELETED_AT.strftime('%Y-%m-%d %H:%M:%S')
        }
        registrar_log(
            acao='excluir',
            entidade='empresa',
            entidade_id=empresa.ID,
            descricao=f'Exclusão da empresa {empresa.NO_EMPRESA}',
            dados_antigos=dados_antigos,
            dados_novos=dados_novos
        )

        flash('Empresa removida com sucesso!', 'warning')
    except Exception as e:
        db.session.rollback()
        logger.exception(f"Erro ao excluir: {str(e)}")
        flash(f'Erro: {str(e)}', 'danger')

    return redirect(url_for('empresa.lista_empresas', periodo_id=periodo_id))