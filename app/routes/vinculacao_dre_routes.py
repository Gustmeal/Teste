from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify
from app.models.vinculacao_dre import VinculoDreItem, VisaoEconomicaSiscor
from app import db
from datetime import datetime
from flask_login import login_required
from app.utils.audit import registrar_log

vinculacao_dre_bp = Blueprint('vinculacao_dre', __name__, url_prefix='/codigos-contabeis/dre-item')


@vinculacao_dre_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@vinculacao_dre_bp.route('/')
@login_required
def index():
    """Dashboard principal das vinculações DRE vs Item Orçamentário"""
    total_vinculacoes = VinculoDreItem.query.count()

    # Contar itens distintos vinculados
    itens_vinculados = db.session.query(
        VinculoDreItem.NIVEL3_ITEM
    ).distinct().count()

    # Total de itens disponíveis
    itens_disponiveis = db.session.query(
        VisaoEconomicaSiscor.ID_ITEM
    ).distinct().count()

    # Últimas vinculações
    ultimas_vinculacoes = VinculoDreItem.query.order_by(
        VinculoDreItem.NIVEL3_ITEM.desc()
    ).limit(5).all()

    return render_template('codigos_contabeis/dre_item/index.html',
                           total_vinculacoes=total_vinculacoes,
                           itens_vinculados=itens_vinculados,
                           itens_disponiveis=itens_disponiveis,
                           ultimas_vinculacoes=ultimas_vinculacoes)


@vinculacao_dre_bp.route('/lista')
@login_required
def lista_vinculacoes():
    """Lista todas as vinculações DRE vs Item"""
    # Filtros
    nivel1_filtro = request.args.get('nivel1', '')
    nivel2_filtro = request.args.get('nivel2', '')
    item_filtro = request.args.get('item', '')

    query = VinculoDreItem.query

    if nivel1_filtro:
        query = query.filter(VinculoDreItem.NIVEL1 == nivel1_filtro)

    if nivel2_filtro:
        query = query.filter(VinculoDreItem.NIVEL2 == nivel2_filtro)

    if item_filtro:
        query = query.filter(VinculoDreItem.NIVEL3_ITEM == int(item_filtro))

    vinculacoes = query.order_by(VinculoDreItem.ORDEM.asc(), VinculoDreItem.NIVEL3_ITEM.asc()).all()

    # Obter opções para filtros
    nivel1_disponiveis = VinculoDreItem.obter_nivel1_distintos()
    nivel2_disponiveis = VinculoDreItem.obter_nivel2_distintos()

    return render_template('codigos_contabeis/dre_item/lista_vinculacoes.html',
                           vinculacoes=vinculacoes,
                           nivel1_disponiveis=nivel1_disponiveis,
                           nivel2_disponiveis=nivel2_disponiveis,
                           nivel1_filtro=nivel1_filtro,
                           nivel2_filtro=nivel2_filtro,
                           item_filtro=item_filtro)


@vinculacao_dre_bp.route('/nova', methods=['GET', 'POST'])
@login_required
def nova_vinculacao():
    """Formulário para nova vinculação DRE vs Item"""
    if request.method == 'POST':
        try:
            # Obter dados do formulário
            nivel3_item = int(request.form['nivel3_item'])
            nivel2 = request.form['nivel2'].strip()
            nivel1 = request.form['nivel1'].strip()

            # Verificar se já existe vinculação com esse NIVEL3_ITEM (chave primária)
            vinculacao_existente = VinculoDreItem.query.get(nivel3_item)
            if vinculacao_existente:
                flash(f'Já existe uma vinculação para o Item {nivel3_item}.', 'danger')
                return redirect(url_for('vinculacao_dre.nova_vinculacao'))

            # Obter a ordem correta baseada no NIVEL2 selecionado
            ordem = VinculoDreItem.obter_ordem_por_nivel2(nivel2)

            # Se não encontrar ordem para o NIVEL2, permitir que o usuário defina
            # ou usar um valor padrão
            if ordem is None:
                ordem = 0  # Valor padrão ou pode pedir ao usuário

            # Colunas sim/não (0 ou 1)
            receita_liquida = 1 if request.form.get('receita_liquida') == 'sim' else 0
            lucro_bruto = 1 if request.form.get('lucro_bruto') == 'sim' else 0
            despesas_administrativas = 1 if request.form.get('despesas_administrativas') == 'sim' else 0
            receitas_despesas = 1 if request.form.get('receitas_despesas') == 'sim' else 0
            lucro_antes_result_financ = 1 if request.form.get('lucro_antes_result_financ') == 'sim' else 0
            result_antes_tributos = 1 if request.form.get('result_antes_tributos') == 'sim' else 0
            lucro_liq_periodo = 1 if request.form.get('lucro_liq_periodo') == 'sim' else 0
            result_depois_jcp = 1 if request.form.get('result_depois_jcp') == 'sim' else 0

            # Sinal (1 para positivo, -1 para negativo)
            sinal = 1 if request.form.get('sinal') == 'positivo' else -1

            # Criar nova vinculação
            nova_vinculacao = VinculoDreItem(
                NIVEL3_ITEM=nivel3_item,
                ORDEM=ordem,
                NIVEL2=nivel2,
                NIVEL1=nivel1,
                RECEITA_LIQUIDA=receita_liquida,
                LUCRO_BRUTO=lucro_bruto,
                DESPESAS_ADMINISTRATIVAS=despesas_administrativas,
                RECEITAS_DESPESAS=receitas_despesas,
                LUCRO_ANTES_RESULT_FINANC=lucro_antes_result_financ,
                RESULT_ANTES_TRIBUTOS=result_antes_tributos,
                LUCRO_LIQ_PERIODO=lucro_liq_periodo,
                RESULT_DEPOIS_JCP=result_depois_jcp,
                SINAL=sinal
            )

            db.session.add(nova_vinculacao)
            db.session.commit()

            # Registrar log
            dados_novos = {
                'nivel3_item': nova_vinculacao.NIVEL3_ITEM,
                'ordem': nova_vinculacao.ORDEM,
                'nivel2': nova_vinculacao.NIVEL2,
                'nivel1': nova_vinculacao.NIVEL1,
                'sinal': 'Positivo' if nova_vinculacao.SINAL == 1 else 'Negativo'
            }

            registrar_log(
                acao='criar',
                entidade='vinculacao_dre_item',
                entidade_id=str(nova_vinculacao.NIVEL3_ITEM),
                descricao=f'Criação de vinculação DRE - Item {nivel3_item}',
                dados_novos=dados_novos
            )

            flash('Vinculação DRE vs Item criada com sucesso!', 'success')
            return redirect(url_for('vinculacao_dre.lista_vinculacoes'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao criar vinculação: {str(e)}', 'danger')
            return redirect(url_for('vinculacao_dre.nova_vinculacao'))

    # GET - Carregar dados para o formulário
    itens_siscor = VisaoEconomicaSiscor.obter_itens_distintos()
    nivel2_disponiveis = VinculoDreItem.obter_nivel2_distintos()
    nivel1_disponiveis = VinculoDreItem.obter_nivel1_distintos()

    return render_template('codigos_contabeis/dre_item/form_vinculacao.html',
                           itens_siscor=itens_siscor,
                           nivel2_disponiveis=nivel2_disponiveis,
                           nivel1_disponiveis=nivel1_disponiveis,
                           edit_mode=False)


@vinculacao_dre_bp.route('/editar/<int:nivel3_item>', methods=['GET', 'POST'])
@login_required
def editar_vinculacao(nivel3_item):
    """Editar vinculação existente"""
    vinculacao = VinculoDreItem.query.get_or_404(nivel3_item)

    if request.method == 'POST':
        try:
            dados_antigos = {
                'nivel3_item': vinculacao.NIVEL3_ITEM,
                'ordem': vinculacao.ORDEM,
                'nivel2': vinculacao.NIVEL2,
                'nivel1': vinculacao.NIVEL1,
                'sinal': 'Positivo' if vinculacao.SINAL == 1 else 'Negativo'
            }

            # Atualizar dados (NIVEL3_ITEM não pode ser alterado pois é PK)
            vinculacao.NIVEL2 = request.form['nivel2'].strip()
            vinculacao.NIVEL1 = request.form['nivel1'].strip()

            # Atualizar ordem baseada no novo NIVEL2
            ordem = VinculoDreItem.obter_ordem_por_nivel2(vinculacao.NIVEL2)
            if ordem is not None:
                vinculacao.ORDEM = ordem

            # Atualizar colunas sim/não
            vinculacao.RECEITA_LIQUIDA = 1 if request.form.get('receita_liquida') == 'sim' else 0
            vinculacao.LUCRO_BRUTO = 1 if request.form.get('lucro_bruto') == 'sim' else 0
            vinculacao.DESPESAS_ADMINISTRATIVAS = 1 if request.form.get('despesas_administrativas') == 'sim' else 0
            vinculacao.RECEITAS_DESPESAS = 1 if request.form.get('receitas_despesas') == 'sim' else 0
            vinculacao.LUCRO_ANTES_RESULT_FINANC = 1 if request.form.get('lucro_antes_result_financ') == 'sim' else 0
            vinculacao.RESULT_ANTES_TRIBUTOS = 1 if request.form.get('result_antes_tributos') == 'sim' else 0
            vinculacao.LUCRO_LIQ_PERIODO = 1 if request.form.get('lucro_liq_periodo') == 'sim' else 0
            vinculacao.RESULT_DEPOIS_JCP = 1 if request.form.get('result_depois_jcp') == 'sim' else 0

            # Atualizar sinal
            vinculacao.SINAL = 1 if request.form.get('sinal') == 'positivo' else -1

            dados_novos = {
                'nivel3_item': vinculacao.NIVEL3_ITEM,
                'ordem': vinculacao.ORDEM,
                'nivel2': vinculacao.NIVEL2,
                'nivel1': vinculacao.NIVEL1,
                'sinal': 'Positivo' if vinculacao.SINAL == 1 else 'Negativo'
            }

            db.session.commit()

            registrar_log(
                acao='editar',
                entidade='vinculacao_dre_item',
                entidade_id=str(vinculacao.NIVEL3_ITEM),
                descricao=f'Edição de vinculação DRE - Item {vinculacao.NIVEL3_ITEM}',
                dados_antigos=dados_antigos,
                dados_novos=dados_novos
            )

            flash('Vinculação atualizada com sucesso!', 'success')
            return redirect(url_for('vinculacao_dre.lista_vinculacoes'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao atualizar vinculação: {str(e)}', 'danger')

    # GET - Carregar dados
    itens_siscor = VisaoEconomicaSiscor.obter_itens_distintos()
    nivel2_disponiveis = VinculoDreItem.obter_nivel2_distintos()
    nivel1_disponiveis = VinculoDreItem.obter_nivel1_distintos()

    return render_template('codigos_contabeis/dre_item/form_vinculacao.html',
                           vinculacao=vinculacao,
                           itens_siscor=itens_siscor,
                           nivel2_disponiveis=nivel2_disponiveis,
                           nivel1_disponiveis=nivel1_disponiveis,
                           edit_mode=True)


@vinculacao_dre_bp.route('/excluir/<int:nivel3_item>', methods=['POST'])
@login_required
def excluir_vinculacao(nivel3_item):
    """Excluir vinculação"""
    try:
        vinculacao = VinculoDreItem.query.get_or_404(nivel3_item)

        dados_antigos = {
            'nivel3_item': vinculacao.NIVEL3_ITEM,
            'ordem': vinculacao.ORDEM,
            'nivel2': vinculacao.NIVEL2,
            'nivel1': vinculacao.NIVEL1
        }

        db.session.delete(vinculacao)
        db.session.commit()

        registrar_log(
            acao='remover',
            entidade='vinculacao_dre_item',
            entidade_id=str(nivel3_item),
            descricao=f'Remoção de vinculação DRE - Item {nivel3_item}',
            dados_antigos=dados_antigos
        )

        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'success': True, 'message': 'Vinculação removida com sucesso!'})

        flash('Vinculação removida com sucesso!', 'success')
        return redirect(url_for('vinculacao_dre.lista_vinculacoes'))

    except Exception as e:
        db.session.rollback()

        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'success': False, 'message': f'Erro ao remover vinculação: {str(e)}'})

        flash(f'Erro ao remover vinculação: {str(e)}', 'danger')
        return redirect(url_for('vinculacao_dre.lista_vinculacoes'))