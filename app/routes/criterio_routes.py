from flask import Blueprint, render_template, redirect, url_for, request, flash
from app.models.criterio_selecao import CriterioSelecao
from app import db
from datetime import datetime
from flask_login import login_required
from app.utils.audit import registrar_log

criterio_bp = Blueprint('criterio', __name__, url_prefix='/credenciamento')


@criterio_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@criterio_bp.route('/criterios')
@login_required
def lista_criterios():
    criterios = CriterioSelecao.query.filter(CriterioSelecao.DELETED_AT == None).all()
    return render_template('credenciamento/lista_criterios.html', criterios=criterios)


@criterio_bp.route('/criterios/novo', methods=['GET', 'POST'])
@login_required
def novo_criterio():
    if request.method == 'POST':
        try:
            codigo = int(request.form['codigo'])
            descricao = request.form['descricao']

            # Verificar se já existe um critério com este código
            criterio_existente = CriterioSelecao.query.filter_by(COD=codigo, DELETED_AT=None).first()
            if criterio_existente:
                flash(f'Erro: O código de critério {codigo} já está sendo utilizado. Por favor, escolha outro código.',
                      'danger')
                return render_template('credenciamento/form_criterio.html')

            novo_criterio = CriterioSelecao(
                COD=codigo,
                DS_CRITERIO_SELECAO=descricao
            )
            db.session.add(novo_criterio)
            db.session.commit()

            # Registrar log de auditoria
            dados = {
                'codigo': novo_criterio.COD,
                'descricao': novo_criterio.DS_CRITERIO_SELECAO
            }
            registrar_log(
                acao='criar',
                entidade='criterio',
                entidade_id=novo_criterio.ID,
                descricao=f'Criação do critério de seleção {novo_criterio.COD}',
                dados_novos=dados
            )

            flash('Critério cadastrado com sucesso!', 'success')
            return redirect(url_for('criterio.lista_criterios'))
        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')
    return render_template('credenciamento/form_criterio.html')


@criterio_bp.route('/criterios/editar/<int:id>', methods=['GET', 'POST'])
@login_required
def editar_criterio(id):
    criterio = CriterioSelecao.query.get_or_404(id)
    if request.method == 'POST':
        try:
            # Capturar dados antigos para auditoria
            dados_antigos = {
                'codigo': criterio.COD,
                'descricao': criterio.DS_CRITERIO_SELECAO
            }

            # Verificar se o código foi alterado e se já existe
            novo_codigo = int(request.form['codigo'])
            if novo_codigo != criterio.COD:
                codigo_existente = CriterioSelecao.query.filter(
                    CriterioSelecao.COD == novo_codigo,
                    CriterioSelecao.ID != id,
                    CriterioSelecao.DELETED_AT == None
                ).first()
                if codigo_existente:
                    flash(f'Erro: O código de critério {novo_codigo} já está sendo utilizado.', 'danger')
                    return render_template('credenciamento/form_criterio.html', criterio=criterio)

            # Atualizar dados
            criterio.COD = novo_codigo
            criterio.DS_CRITERIO_SELECAO = request.form['descricao']
            db.session.commit()

            # Registrar log de auditoria
            dados_novos = {
                'codigo': criterio.COD,
                'descricao': criterio.DS_CRITERIO_SELECAO
            }
            registrar_log(
                acao='editar',
                entidade='criterio',
                entidade_id=criterio.ID,
                descricao=f'Edição do critério de seleção {criterio.COD}',
                dados_antigos=dados_antigos,
                dados_novos=dados_novos
            )

            flash('Critério atualizado!', 'success')
            return redirect(url_for('criterio.lista_criterios'))
        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')
    return render_template('credenciamento/form_criterio.html', criterio=criterio)


@criterio_bp.route('/criterios/excluir/<int:id>')
@login_required
def excluir_criterio(id):
    try:
        criterio = CriterioSelecao.query.get_or_404(id)

        # Capturar dados para auditoria
        dados_antigos = {
            'codigo': criterio.COD,
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
            descricao=f'Exclusão do critério de seleção {criterio.COD}',
            dados_antigos=dados_antigos,
            dados_novos=dados_novos
        )

        flash('Critério excluído com sucesso!', 'warning')
    except Exception as e:
        db.session.rollback()
        flash(f'Erro: {str(e)}', 'danger')
    return redirect(url_for('criterio.lista_criterios'))