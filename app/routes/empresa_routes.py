from flask import Blueprint, render_template, redirect, url_for, request, flash
from app.models.empresa_participante import EmpresaParticipante
from app.models.empresa_responsavel import EmpresaResponsavel  # Modelo da tabela externa
from app.models.periodo import PeriodoAvaliacao
from app.models.edital import Edital
from app import db
from datetime import datetime
from flask_login import login_required
from app.utils.audit import registrar_log

empresa_bp = Blueprint('empresa', __name__, url_prefix='/credenciamento')


@empresa_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@empresa_bp.route('/periodos/<int:periodo_id>/empresas')
@login_required
def lista_empresas(periodo_id):
    periodo = PeriodoAvaliacao.query.get_or_404(periodo_id)

    # Join com EmpresaResponsavel para buscar informações completas
    empresas = db.session.query(
        EmpresaParticipante,
        EmpresaResponsavel
    ).outerjoin(
        EmpresaResponsavel,
        EmpresaParticipante.ID_EMPRESA == EmpresaResponsavel.pkEmpresaResponsavelCobranca
    ).filter(
        EmpresaParticipante.ID_PERIODO == periodo.ID_PERIODO,  # Usa o ID_PERIODO em vez do ID da tabela
        EmpresaParticipante.DELETED_AT == None
    ).all()

    return render_template('credenciamento/lista_empresas.html',
                           periodo=periodo,
                           empresas=empresas)


@empresa_bp.route('/periodos/<int:periodo_id>/empresas/nova', methods=['GET', 'POST'])
@login_required
def nova_empresa(periodo_id):
    periodo = PeriodoAvaliacao.query.get_or_404(periodo_id)
    edital = Edital.query.get(periodo.ID_EDITAL)

    # Obter todas as empresas disponíveis para o dropdown
    empresas_responsaveis = EmpresaResponsavel.query.order_by(EmpresaResponsavel.nmEmpresaResponsavelCobranca).all()

    # Lista de opções para o campo condição
    condicoes = ["NOVA", "PERMANECE","DESCREDENCIADA"]

    if request.method == 'POST':
        try:
            # Obter ID da empresa selecionada
            id_empresa = request.form['id_empresa']
            ds_condicao = request.form.get('ds_condicao', '')

            # Buscar empresa responsável
            empresa_responsavel = EmpresaResponsavel.query.get(id_empresa)
            if not empresa_responsavel:
                flash(f'Empresa com ID {id_empresa} não encontrada.', 'danger')
                return render_template('credenciamento/form_empresa.html',
                                       periodo=periodo,
                                       edital=edital,
                                       empresas=empresas_responsaveis,
                                       condicoes=condicoes)

            # Verificar se já existe empresa com este ID neste período
            empresa_existente = EmpresaParticipante.query.filter_by(
                ID_PERIODO=periodo.ID_PERIODO,  # Usa o ID_PERIODO em vez do ID da tabela
                ID_EMPRESA=id_empresa,
                DELETED_AT=None
            ).first()

            if empresa_existente:
                flash(f'Empresa já cadastrada para este período.', 'danger')
                return render_template('credenciamento/form_empresa.html',
                                       periodo=periodo,
                                       edital=edital,
                                       empresas=empresas_responsaveis,
                                       condicoes=condicoes)

            nova_empresa = EmpresaParticipante(
                ID_EDITAL=edital.ID,
                ID_PERIODO=periodo.ID_PERIODO,  # Usa o ID_PERIODO em vez do ID da tabela
                ID_EMPRESA=empresa_responsavel.pkEmpresaResponsavelCobranca,
                NO_EMPRESA=empresa_responsavel.nmEmpresaResponsavelCobranca,
                NO_EMPRESA_ABREVIADA=empresa_responsavel.NO_ABREVIADO_EMPRESA,
                DS_CONDICAO=ds_condicao
            )

            db.session.add(nova_empresa)
            db.session.commit()

            # Registrar log de auditoria
            dados_novos = {
                'id_edital': edital.ID,
                'id_periodo': periodo.ID_PERIODO,  # Usa o ID_PERIODO em vez do ID da tabela
                'id_empresa': empresa_responsavel.pkEmpresaResponsavelCobranca,
                'no_empresa': empresa_responsavel.nmEmpresaResponsavelCobranca,
                'no_empresa_abreviada': empresa_responsavel.NO_ABREVIADO_EMPRESA,
                'ds_condicao': ds_condicao
            }
            registrar_log(
                acao='criar',
                entidade='empresa',
                entidade_id=nova_empresa.ID,
                descricao=f'Cadastro da empresa {empresa_responsavel.nmEmpresaResponsavelCobranca} no período {periodo.ID_PERIODO}',
                dados_novos=dados_novos
            )

            flash('Empresa cadastrada com sucesso!', 'success')
            return redirect(url_for('empresa.lista_empresas', periodo_id=periodo_id))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')

    return render_template('credenciamento/form_empresa.html',
                           periodo=periodo,
                           edital=edital,
                           empresas=empresas_responsaveis,
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
        flash(f'Erro: {str(e)}', 'danger')

    return redirect(url_for('empresa.lista_empresas', periodo_id=periodo_id))