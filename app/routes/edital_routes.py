from flask import Blueprint, render_template, redirect, url_for, request, flash
from app.models.edital import Edital
from app import db
from datetime import datetime
from flask_login import login_required

edital_bp = Blueprint('edital', __name__)

@edital_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}

@edital_bp.route('/')
@login_required
def index():
    return render_template('index.html')

@edital_bp.route('/editais')
@login_required
def lista_editais():
    editais = Edital.query.filter(Edital.DELETED_AT == None).all()
    return render_template('lista_editais.html', editais=editais)

@edital_bp.route('/editais/novo', methods=['GET', 'POST'])
@login_required
def novo_edital():
    if request.method == 'POST':
        try:
            novo_edital = Edital(
                NU_EDITAL=request.form['nu_edital'],
                ANO=request.form['ano'],
                DESCRICAO=request.form['descricao']
            )
            db.session.add(novo_edital)
            db.session.commit()
            flash('Edital cadastrado com sucesso!', 'success')
            return redirect(url_for('edital.lista_editais'))
        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')
    return render_template('form_edital.html')

@edital_bp.route('/editais/editar/<int:id>', methods=['GET', 'POST'])
@login_required
def editar_edital(id):
    edital = Edital.query.get_or_404(id)
    if request.method == 'POST':
        try:
            edital.NU_EDITAL = request.form['nu_edital']
            edital.ANO = request.form['ano']
            edital.DESCRICAO = request.form['descricao']
            db.session.commit()
            flash('Edital atualizado!', 'success')
            return redirect(url_for('edital.lista_editais'))
        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')
    return render_template('form_edital.html', edital=edital)

@edital_bp.route('/editais/excluir/<int:id>')
@login_required
def excluir_edital(id):
    try:
        edital = Edital.query.get_or_404(id)
        edital.DELETED_AT = datetime.utcnow()
        db.session.commit()
        flash('Edital arquivado!', 'warning')
    except Exception as e:
        db.session.rollback()
        flash(f'Erro: {str(e)}', 'danger')
    return redirect(url_for('edital.lista_editais'))