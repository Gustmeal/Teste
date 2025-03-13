from flask import Blueprint, render_template, redirect, url_for, request, flash
from app.models.periodo import PeriodoAvaliacao
from app.models.edital import Edital
from app import db
from datetime import datetime
from flask_login import login_required
from app.utils.audit import registrar_log

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
    # Aqui está o problema potencial - editais deve ser carregado primeiro
    editais = Edital.query.filter(Edital.DELETED_AT == None).all()

    # Verificar se há editais cadastrados
    if not editais:
        flash('Não há editais cadastrados para associar a um período. Cadastre um edital primeiro.', 'warning')
        return redirect(url_for('edital.lista_editais'))

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

            # Registrar dados para auditoria
            dados_novos = {
                'id_periodo': novo_id_periodo,
                'id_edital': edital.ID,
                'dt_inicio': dt_inicio.strftime('%Y-%m-%d'),
                'dt_fim': dt_fim.strftime('%Y-%m-%d')
            }

            db.session.add(novo_periodo)
            db.session.commit()

            # Registrar log de auditoria
            registrar_log(
                acao='criar',
                entidade='periodo',
                entidade_id=novo_periodo.ID,
                descricao=f'Criação do período {novo_id_periodo} para o edital {edital.NU_EDITAL}/{edital.ANO}',
                dados_novos=dados_novos
            )

            flash('Período cadastrado!', 'success')
            return redirect(url_for('periodo.lista_periodos'))
        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')

    # IMPORTANTE: Verifique se este template existe no diretório correto
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

            # Capturar dados antigos para auditoria
            dados_antigos = {
                'id_periodo': periodo.ID_PERIODO,
                'id_edital': periodo.ID_EDITAL,
                'dt_inicio': periodo.DT_INICIO.strftime('%Y-%m-%d'),
                'dt_fim': periodo.DT_FIM.strftime('%Y-%m-%d')
            }

            periodo.ID_EDITAL = edital.ID
            # Não alteramos o ID_PERIODO durante a edição
            periodo.DT_INICIO = dt_inicio
            periodo.DT_FIM = dt_fim

            # Capturar dados novos para auditoria
            dados_novos = {
                'id_periodo': periodo.ID_PERIODO,
                'id_edital': periodo.ID_EDITAL,
                'dt_inicio': periodo.DT_INICIO.strftime('%Y-%m-%d'),
                'dt_fim': periodo.DT_FIM.strftime('%Y-%m-%d')
            }

            db.session.commit()

            # Registrar log de auditoria
            registrar_log(
                acao='editar',
                entidade='periodo',
                entidade_id=periodo.ID,
                descricao=f'Edição do período {periodo.ID_PERIODO}',
                dados_antigos=dados_antigos,
                dados_novos=dados_novos
            )

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

        # Capturar dados para auditoria
        dados_antigos = {
            'id_periodo': periodo.ID_PERIODO,
            'id_edital': periodo.ID_EDITAL,
            'dt_inicio': periodo.DT_INICIO.strftime('%Y-%m-%d'),
            'dt_fim': periodo.DT_FIM.strftime('%Y-%m-%d'),
            'deleted_at': None
        }

        periodo.DELETED_AT = datetime.utcnow()
        db.session.commit()

        # Registrar log de auditoria
        dados_novos = {
            'deleted_at': periodo.DELETED_AT.strftime('%Y-%m-%d %H:%M:%S')
        }

        registrar_log(
            acao='excluir',
            entidade='periodo',
            entidade_id=periodo.ID,
            descricao=f'Arquivamento do período {periodo.ID_PERIODO}',
            dados_antigos=dados_antigos,
            dados_novos=dados_novos
        )

        flash('Período arquivado!', 'warning')
    except Exception as e:
        db.session.rollback()
        flash(f'Erro: {str(e)}', 'danger')
    return redirect(url_for('periodo.lista_periodos'))