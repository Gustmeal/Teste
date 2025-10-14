from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify
from flask_login import login_required, current_user
from app import db
from app.models.linha_tempo import LinhaTempo
from app.utils.audit import registrar_log
from datetime import datetime
from sqlalchemy import text

linha_tempo_bp = Blueprint('linha_tempo', __name__, url_prefix='/analise-controle/linha-tempo')


@linha_tempo_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@linha_tempo_bp.route('/')
@login_required
def index():
    """Lista todos os eventos da linha do tempo ordenados por ano e item"""
    try:
        eventos = LinhaTempo.query.order_by(
            LinhaTempo.ANO.desc(),
            LinhaTempo.ITEM.asc()
        ).all()

        # Agrupar eventos por ano para melhor visualização
        eventos_por_ano = {}
        for evento in eventos:
            if evento.ANO not in eventos_por_ano:
                eventos_por_ano[evento.ANO] = []
            eventos_por_ano[evento.ANO].append(evento)

        return render_template('linha_tempo/index.html',
                               eventos=eventos,
                               eventos_por_ano=eventos_por_ano)
    except Exception as e:
        flash(f'Erro ao carregar eventos: {str(e)}', 'danger')
        return render_template('linha_tempo/index.html',
                               eventos=[],
                               eventos_por_ano={})


@linha_tempo_bp.route('/novo', methods=['GET', 'POST'])
@login_required
def novo_evento():
    """Cria um novo evento na linha do tempo"""
    if request.method == 'POST':
        try:
            ano = int(request.form.get('ano'))
            descricao = request.form.get('descricao', '').strip()

            # Validações
            if not descricao:
                flash('A descrição do evento é obrigatória!', 'warning')
                return redirect(url_for('linha_tempo.novo_evento'))

            if ano < 1900 or ano > 2100:
                flash('Ano inválido! Informe um ano entre 1900 e 2100.', 'warning')
                return redirect(url_for('linha_tempo.novo_evento'))

            if len(descricao) > 300:
                flash('A descrição deve ter no máximo 300 caracteres!', 'warning')
                return redirect(url_for('linha_tempo.novo_evento'))

            # Obter próximo item automaticamente
            proximo_item = LinhaTempo.obter_proximo_item(ano)

            # Criar novo evento
            novo_evento = LinhaTempo(
                ANO=ano,
                ITEM=proximo_item,
                DSC_EVENTO=descricao
            )

            db.session.add(novo_evento)
            db.session.commit()

            # Registrar log
            registrar_log(
                acao='criar',
                entidade='linha_tempo',
                entidade_id=f"{ano}-{proximo_item}",
                descricao=f'Evento criado: {ano} - Item {proximo_item}'
            )

            flash(f'Evento criado com sucesso! (Ano: {ano}, Item: {proximo_item})', 'success')
            return redirect(url_for('linha_tempo.index'))

        except ValueError:
            flash('Ano inválido! Informe um número válido.', 'danger')
            return redirect(url_for('linha_tempo.novo_evento'))
        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao criar evento: {str(e)}', 'danger')
            return redirect(url_for('linha_tempo.novo_evento'))

    # GET - Renderizar formulário
    ano_atual = datetime.now().year
    return render_template('linha_tempo/novo_evento.html', ano_atual=ano_atual)


@linha_tempo_bp.route('/editar/<int:ano>/<int:item>', methods=['GET', 'POST'])
@login_required
def editar_evento(ano, item):
    """Edita um evento existente"""
    evento = LinhaTempo.query.get_or_404((ano, item))

    if request.method == 'POST':
        try:
            descricao = request.form.get('descricao', '').strip()

            # Validações
            if not descricao:
                flash('A descrição do evento é obrigatória!', 'warning')
                return redirect(url_for('linha_tempo.editar_evento', ano=ano, item=item))

            if len(descricao) > 300:
                flash('A descrição deve ter no máximo 300 caracteres!', 'warning')
                return redirect(url_for('linha_tempo.editar_evento', ano=ano, item=item))

            # Atualizar descrição
            descricao_antiga = evento.DSC_EVENTO
            evento.DSC_EVENTO = descricao

            db.session.commit()

            # Registrar log
            registrar_log(
                acao='editar',
                entidade='linha_tempo',
                entidade_id=f"{ano}-{item}",
                descricao=f'Evento editado: {ano} - Item {item}',
                dados_antigos={'DSC_EVENTO': descricao_antiga},
                dados_novos={'DSC_EVENTO': descricao}
            )

            flash('Evento atualizado com sucesso!', 'success')
            return redirect(url_for('linha_tempo.index'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao editar evento: {str(e)}', 'danger')
            return redirect(url_for('linha_tempo.editar_evento', ano=ano, item=item))

    # GET - Renderizar formulário de edição
    return render_template('linha_tempo/editar_evento.html', evento=evento)


@linha_tempo_bp.route('/excluir/<int:ano>/<int:item>', methods=['POST'])
@login_required
def excluir_evento(ano, item):
    """Exclui um evento da linha do tempo"""
    try:
        evento = LinhaTempo.query.get_or_404((ano, item))
        descricao = evento.DSC_EVENTO

        db.session.delete(evento)
        db.session.commit()

        # Registrar log
        registrar_log(
            acao='excluir',
            entidade='linha_tempo',
            entidade_id=f"{ano}-{item}",
            descricao=f'Evento excluído: {ano} - Item {item} - {descricao}'
        )

        flash('Evento excluído com sucesso!', 'success')
        return redirect(url_for('linha_tempo.index'))

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao excluir evento: {str(e)}', 'danger')
        return redirect(url_for('linha_tempo.index'))


@linha_tempo_bp.route('/api/proximo-item/<int:ano>')
@login_required
def api_proximo_item(ano):
    """API para obter o próximo item de um ano (para usar via AJAX)"""
    try:
        proximo_item = LinhaTempo.obter_proximo_item(ano)
        return jsonify({
            'sucesso': True,
            'proximo_item': proximo_item
        })
    except Exception as e:
        return jsonify({
            'sucesso': False,
            'erro': str(e)
        }), 500