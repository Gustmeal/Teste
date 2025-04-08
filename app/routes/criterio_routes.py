from flask import Blueprint, render_template, redirect, url_for, request, flash
from app.models.criterio_distribuicao import CriterioDistribuicao
from app import db
from datetime import datetime
from flask_login import login_required
from app.utils.audit import registrar_log
from app.auth.utils import admin_required

criterio_bp = Blueprint('criterio', __name__, url_prefix='/credenciamento')


@criterio_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@criterio_bp.route('/criterios')
@login_required
def lista_criterios():
    criterios = CriterioDistribuicao.query.filter(CriterioDistribuicao.DELETED_AT == None).all()
    return render_template('credenciamento/lista_criterios.html', criterios=criterios)


@criterio_bp.route('/criterios/novo', methods=['GET', 'POST'])
@login_required
@admin_required
def novo_criterio():
    if request.method == 'POST':
        try:
            cod = request.form['cod']
            descricao = request.form['descricao']

            # Verificar se já existe um critério com esse código
            criterio_existente = CriterioDistribuicao.query.filter_by(COD=cod, DELETED_AT=None).first()
            if criterio_existente:
                flash(f'Erro: O código {cod} já está sendo utilizado. Por favor, escolha outro código.', 'danger')
                return render_template('credenciamento/form_criterio.html')

            novo_criterio = CriterioDistribuicao(
                COD=cod,
                DS_CRITERIO_SELECAO=descricao
            )
            db.session.add(novo_criterio)
            db.session.commit()

            # Registrar log de auditoria
            dados = {
                'cod': novo_criterio.COD,
                'descricao': novo_criterio.DS_CRITERIO_SELECAO
            }
            registrar_log(
                acao='criar',
                entidade='criterio',
                entidade_id=novo_criterio.ID,
                descricao=f'Criação do critério de distribuição {novo_criterio.COD}',
                dados_novos=dados
            )

            flash('Critério de distribuição cadastrado com sucesso!', 'success')
            return redirect(url_for('criterio.lista_criterios'))
        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')
    return render_template('credenciamento/form_criterio.html')


@criterio_bp.route('/criterios/editar/<int:id>', methods=['GET', 'POST'])
@login_required
@admin_required
def editar_criterio(id):
    criterio = CriterioDistribuicao.query.get_or_404(id)
    if request.method == 'POST':
        try:
            # Capturar dados antigos para auditoria
            dados_antigos = {
                'cod': criterio.COD,
                'descricao': criterio.DS_CRITERIO_SELECAO
            }

            # Atualizar dados
            criterio.COD = request.form['cod']
            criterio.DS_CRITERIO_SELECAO = request.form['descricao']
            db.session.commit()

            # Registrar log de auditoria
            dados_novos = {
                'cod': criterio.COD,
                'descricao': criterio.DS_CRITERIO_SELECAO
            }
            registrar_log(
                acao='editar',
                entidade='criterio',
                entidade_id=criterio.ID,
                descricao=f'Edição do critério de distribuição {criterio.COD}',
                dados_antigos=dados_antigos,
                dados_novos=dados_novos
            )

            flash('Critério de distribuição atualizado!', 'success')
            return redirect(url_for('criterio.lista_criterios'))
        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')
    return render_template('credenciamento/form_criterio.html', criterio=criterio)


@criterio_bp.route('/criterios/excluir/<int:id>', methods=['GET', 'POST'])
@login_required
@admin_required
def excluir_criterio(id):
    try:
        criterio = CriterioDistribuicao.query.get_or_404(id)

        # Capturar dados para auditoria
        dados_antigos = {
            'cod': criterio.COD,
            'descricao': criterio.DS_CRITERIO_SELECAO,
            'deleted_at': None
        }

        criterio.DELETED_AT = datetime.utcnow()
        db.session.commit()

        # Registrar log de auditoria
        dados_novos = {
            'deleted_at': criterio.DELETED_AT.strftime('%Y-%m-%d %H:%M:%S')
        }
        registrar_log(
            acao='excluir',
            entidade='criterio',
            entidade_id=criterio.ID,
            descricao=f'Exclusão do critério de distribuição {criterio.COD}',
            dados_antigos=dados_antigos,
            dados_novos=dados_novos
        )

        flash('Critério de distribuição excluído com sucesso!', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Erro: {str(e)}', 'danger')
    return redirect(url_for('criterio.lista_criterios'))