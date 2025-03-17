from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify
from app.models.edital import Edital
from app import db
from datetime import datetime
from flask_login import login_required, current_user
from app.utils.audit import registrar_log

edital_bp = Blueprint('edital', __name__)


@edital_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@edital_bp.route('/')
@login_required
def index():
    # Contar editais ativos
    editais = Edital.query.filter(Edital.DELETED_AT == None).count()

    # Contar períodos ativos
    from app.models.periodo import PeriodoAvaliacao
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).count()

    # Contar limites ativos
    from app.models.limite_distribuicao import LimiteDistribuicao
    limites = LimiteDistribuicao.query.filter(LimiteDistribuicao.DELETED_AT == None).count()

    # Contar metas ativas - NOVO
    from app.models.meta_avaliacao import MetaAvaliacao
    metas = MetaAvaliacao.query.filter(MetaAvaliacao.DELETED_AT == None).count()

    # Contar usuários ativos (apenas para administradores)
    usuarios = 0
    if current_user.perfil == 'admin':
        from app.models.usuario import Usuario
        usuarios = Usuario.query.filter(Usuario.DELETED_AT == None, Usuario.ATIVO == True).count()

    return render_template('index.html', editais=editais, periodos=periodos, limites=limites, metas=metas,
                           usuarios=usuarios)


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

            # Registrar log de auditoria
            dados = {
                'nu_edital': novo_edital.NU_EDITAL,
                'ano': novo_edital.ANO,
                'descricao': novo_edital.DESCRICAO
            }
            registrar_log(
                acao='criar',
                entidade='edital',
                entidade_id=novo_edital.ID,
                descricao=f'Criação do edital {novo_edital.NU_EDITAL}/{novo_edital.ANO}',
                dados_novos=dados
            )

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
            # Capturar dados antigos para auditoria
            dados_antigos = {
                'nu_edital': edital.NU_EDITAL,
                'ano': edital.ANO,
                'descricao': edital.DESCRICAO
            }

            # Atualizar dados
            edital.NU_EDITAL = request.form['nu_edital']
            edital.ANO = request.form['ano']
            edital.DESCRICAO = request.form['descricao']
            db.session.commit()

            # Registrar log de auditoria
            dados_novos = {
                'nu_edital': edital.NU_EDITAL,
                'ano': edital.ANO,
                'descricao': edital.DESCRICAO
            }
            registrar_log(
                acao='editar',
                entidade='edital',
                entidade_id=edital.ID,
                descricao=f'Edição do edital {edital.NU_EDITAL}/{edital.ANO}',
                dados_antigos=dados_antigos,
                dados_novos=dados_novos
            )

            flash('Edital atualizado!', 'success')
            return redirect(url_for('edital.lista_editais'))
        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')
    return render_template('form_edital.html', edital=edital)


@edital_bp.route('/editais/excluir/<int:id>', methods=['GET', 'POST'])
@login_required
def excluir_edital(id):
    try:
        edital = Edital.query.get_or_404(id)

        # Capturar dados para auditoria
        dados_antigos = {
            'nu_edital': edital.NU_EDITAL,
            'ano': edital.ANO,
            'descricao': edital.DESCRICAO,
            'deleted_at': None
        }

        edital.DELETED_AT = datetime.utcnow()
        db.session.commit()

        # Registrar log de auditoria
        dados_novos = {
            'deleted_at': edital.DELETED_AT.strftime('%Y-%m-%d %H:%M:%S')
        }
        registrar_log(
            acao='excluir',
            entidade='edital',
            entidade_id=edital.ID,
            descricao=f'Arquivamento do edital {edital.NU_EDITAL}/{edital.ANO}',
            dados_antigos=dados_antigos,
            dados_novos=dados_novos
        )

        # Se a requisição for AJAX, retorna JSON
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({
                'success': True,
                'message': f'Edital {edital.NU_EDITAL} arquivado com sucesso!'
            })

        # Caso contrário, retorna redirecionamento normal
        flash('Edital arquivado com sucesso!', 'success')
        return redirect(url_for('edital.lista_editais'))

    except Exception as e:
        db.session.rollback()

        # Se a requisição for AJAX, retorna JSON com erro
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({
                'success': False,
                'message': f'Erro ao arquivar: {str(e)}'
            })

        # Caso contrário, retorna o erro via flash
        flash(f'Erro: {str(e)}', 'danger')
        return redirect(url_for('edital.lista_editais'))