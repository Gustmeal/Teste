from flask import Blueprint, render_template, redirect, url_for, request, flash
from app.models.periodo import PeriodoAvaliacao
from app.models.edital import Edital
from app import db
from datetime import datetime
from flask_login import login_required

periodo_bp = Blueprint('periodo', __name__)

@periodo_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}

@periodo_bp.route('/periodos')
@login_required
def lista_periodos():
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).all()
    return render_template('lista_periodos.html', periodos=periodos)


@periodo_bp.route('/periodos/novo', methods=['GET', 'POST'])
@login_required
def novo_periodo():
    editais = Edital.query.filter(Edital.DELETED_AT == None).all()

    if request.method == 'POST':
        try:
            edital_id = int(request.form['edital'])
            edital = Edital.query.filter_by(NU_EDITAL=edital_id).first()

            if not edital:
                flash(f'Erro: Edital não encontrado.', 'danger')
                return render_template('form_periodo.html', editais=editais)

            # Validar datas
            dt_inicio = datetime.strptime(request.form['dt_inicio'], '%Y-%m-%d')
            dt_fim = datetime.strptime(request.form['dt_fim'], '%Y-%m-%d')

            if dt_inicio > dt_fim:
                flash('Erro: A data de início não pode ser posterior à data de término.', 'danger')
                return render_template('form_periodo.html', editais=editais)

            # Pegar o último ID_PERIODO e incrementar
            ultimo_periodo = PeriodoAvaliacao.query.order_by(PeriodoAvaliacao.ID_PERIODO.desc()).first()
            novo_id_periodo = 1
            if ultimo_periodo:
                novo_id_periodo = ultimo_periodo.ID_PERIODO + 1

            novo_periodo = PeriodoAvaliacao(
                ID_PERIODO=novo_id_periodo,
                ID_EDITAL=edital.ID,
                DT_INICIO=dt_inicio,
                DT_FIM=dt_fim
            )
            db.session.add(novo_periodo)
            db.session.commit()
            flash('Período cadastrado!', 'success')
            return redirect(url_for('periodo.lista_periodos'))
        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')

    return render_template('form_periodo.html', editais=editais)


@periodo_bp.route('/periodos/editar/<int:id>', methods=['GET', 'POST'])
@login_required
def editar_periodo(id):
    periodo = PeriodoAvaliacao.query.get_or_404(id)
    editais = Edital.query.filter(Edital.DELETED_AT == None).all()

    if request.method == 'POST':
        try:
            edital_id = int(request.form['edital'])
            edital = Edital.query.filter_by(NU_EDITAL=edital_id).first()

            if not edital:
                flash(f'Erro: Edital não encontrado.', 'danger')
                return render_template('form_periodo.html', periodo=periodo, editais=editais)

            # Validar datas
            dt_inicio = datetime.strptime(request.form['dt_inicio'], '%Y-%m-%d')
            dt_fim = datetime.strptime(request.form['dt_fim'], '%Y-%m-%d')

            if dt_inicio > dt_fim:
                flash('Erro: A data de início não pode ser posterior à data de término.', 'danger')
                return render_template('form_periodo.html', periodo=periodo, editais=editais)

            periodo.ID_EDITAL = edital.ID
            # Não alteramos o ID_PERIODO durante a edição
            periodo.DT_INICIO = dt_inicio
            periodo.DT_FIM = dt_fim
            db.session.commit()
            flash('Período atualizado!', 'success')
            return redirect(url_for('periodo.lista_periodos'))
        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')

    return render_template('form_periodo.html', periodo=periodo, editais=editais)


@periodo_bp.route('/periodos/excluir/<int:id>')
@login_required
def excluir_periodo(id):
    try:
        periodo = PeriodoAvaliacao.query.get_or_404(id)
        periodo.DELETED_AT = datetime.utcnow()
        db.session.commit()
        flash('Período arquivado!', 'warning')
    except Exception as e:
        db.session.rollback()
        flash(f'Erro: {str(e)}', 'danger')
    return redirect(url_for('periodo.lista_periodos'))