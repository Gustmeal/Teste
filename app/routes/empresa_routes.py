from flask import Blueprint, render_template, redirect, url_for, request, flash
from app.models.empresa_participante import EmpresaParticipante
from app.models.periodo import PeriodoAvaliacao
from app.models.edital import Edital
from app import db
from datetime import datetime
from flask_login import login_required
from app.utils.audit import registrar_log

empresa_bp = Blueprint('empresa', __name__)


@empresa_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@empresa_bp.route('/periodos/<int:periodo_id>/empresas')
@login_required
def lista_empresas(periodo_id):
    periodo = PeriodoAvaliacao.query.get_or_404(periodo_id)
    empresas = EmpresaParticipante.query.filter_by(ID_PERIODO=periodo_id, DELETED_AT=None).all()
    return render_template('lista_empresas.html', periodo=periodo, empresas=empresas)


@empresa_bp.route('/periodos/<int:periodo_id>/empresas/nova', methods=['GET', 'POST'])
@login_required
def nova_empresa(periodo_id):
    periodo = PeriodoAvaliacao.query.get_or_404(periodo_id)
    edital = Edital.query.get(periodo.ID_EDITAL)

    if request.method == 'POST':
        try:
            nome_empresa = request.form['nome_empresa']
            nome_abreviado = request.form['nome_abreviado']
            id_empresa = request.form['id_empresa']
            ds_condicao = request.form.get('ds_condicao', '')

            # Verificar se já existe empresa com este ID neste período
            empresa_existente = EmpresaParticipante.query.filter_by(
                ID_PERIODO=periodo_id,
                ID_EMPRESA=id_empresa,
                DELETED_AT=None
            ).first()

            if empresa_existente:
                flash(f'Empresa com ID {id_empresa} já cadastrada para este período.', 'danger')
                return render_template('form_empresa.html', periodo=periodo, edital=edital)

            nova_empresa = EmpresaParticipante(
                ID_EDITAL=edital.ID,
                ID_PERIODO=periodo_id,
                ID_EMPRESA=id_empresa,
                NO_EMPRESA=nome_empresa,
                NO_EMPRESA_ABREVIADA=nome_abreviado,
                DS_CONDICAO=ds_condicao
            )

            db.session.add(nova_empresa)
            db.session.commit()

            # Registrar log de auditoria
            dados_novos = {
                'id_edital': edital.ID,
                'id_periodo': periodo_id,
                'id_empresa': id_empresa,
                'no_empresa': nome_empresa
            }
            registrar_log(
                acao='criar',
                entidade='empresa',
                entidade_id=nova_empresa.ID,
                descricao=f'Cadastro da empresa {nome_empresa} no período {periodo.ID_PERIODO}',
                dados_novos=dados_novos
            )

            flash('Empresa cadastrada com sucesso!', 'success')
            return redirect(url_for('empresa.lista_empresas', periodo_id=periodo_id))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')

    return render_template('form_empresa.html', periodo=periodo, edital=edital)


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