from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify
from app.models.vinculacao import ItemContaSucor, DescricaoItensSiscor, CodigoContabilVinculacao
from app import db
from datetime import datetime
from flask_login import login_required
from app.utils.audit import registrar_log

vinculacao_bp = Blueprint('vinculacao', __name__, url_prefix='/codigos-contabeis')


@vinculacao_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@vinculacao_bp.route('/vinculacao')
@login_required
def index():
    """Dashboard principal das vinculações"""
    total_vinculacoes = ItemContaSucor.query.count()
    ano_atual = datetime.now().year
    vinculacoes_ano_atual = ItemContaSucor.query.filter_by(ANO=ano_atual).count()
    itens_siscor = DescricaoItensSiscor.query.count()
    codigos_disponiveis = len(CodigoContabilVinculacao.obter_codigos_ordenados())

    ultimas_vinculacoes_query = db.session.query(
        ItemContaSucor,
        DescricaoItensSiscor.DSC_ITEM_ORCAMENTO
    ).join(
        DescricaoItensSiscor, ItemContaSucor.ID_ITEM == DescricaoItensSiscor.ID_ITEM
    ).order_by(
        ItemContaSucor.ANO.desc(), ItemContaSucor.ID_ITEM.desc()
    ).limit(5).all()

    ultimas_vinculacoes = []
    for vinculacao, dsc_item_orcamento in ultimas_vinculacoes_query:
        vinculacao.dsc_item_orcamento = dsc_item_orcamento
        ultimas_vinculacoes.append(vinculacao)

    return render_template('codigos_contabeis/vinculacao/index.html',
                           total_vinculacoes=total_vinculacoes,
                           vinculacoes_ano_atual=vinculacoes_ano_atual,
                           ano_atual=ano_atual,
                           itens_siscor=itens_siscor,
                           codigos_disponiveis=codigos_disponiveis,
                           ultimas_vinculacoes=ultimas_vinculacoes)


@vinculacao_bp.route('/vinculacao/lista')
@login_required
def lista_vinculacoes():
    """Lista todas as vinculações"""
    ano_filtro = request.args.get('ano', type=int)
    codigo_filtro = request.args.get('codigo', '')
    arquivo_filtro = request.args.get('arquivo', '')
    id_item_filtro = request.args.get('id_item', '')

    query = db.session.query(
        ItemContaSucor,
        DescricaoItensSiscor.DSC_ITEM_ORCAMENTO
    ).join(
        DescricaoItensSiscor, ItemContaSucor.ID_ITEM == DescricaoItensSiscor.ID_ITEM
    )

    if ano_filtro:
        query = query.filter(ItemContaSucor.ANO == ano_filtro)
    if codigo_filtro:
        query = query.filter(ItemContaSucor.CODIGO == codigo_filtro)
    if arquivo_filtro:
        query = query.filter(ItemContaSucor.DSC_ARQUIVO == arquivo_filtro)
    if id_item_filtro:
        query = query.filter(ItemContaSucor.ID_ITEM == int(id_item_filtro))

    resultados = query.order_by(ItemContaSucor.ANO.desc(), ItemContaSucor.ID_ITEM.desc()).all()

    vinculacoes_completas = []
    for vinculacao, dsc_item_orcamento in resultados:
        vinculacao.dsc_item_orcamento = dsc_item_orcamento
        vinculacoes_completas.append(vinculacao)

    anos_disponiveis = [ano[0] for ano in
                        db.session.query(ItemContaSucor.ANO).distinct().order_by(ItemContaSucor.ANO.desc()).all()]
    codigos_disponiveis = [codigo[0] for codigo in db.session.query(ItemContaSucor.CODIGO).distinct().order_by(
        ItemContaSucor.CODIGO.asc()).all()]
    arquivos_disponiveis = [arquivo[0] for arquivo in db.session.query(ItemContaSucor.DSC_ARQUIVO).distinct().filter(
        ItemContaSucor.DSC_ARQUIVO.isnot(None)).order_by(ItemContaSucor.DSC_ARQUIVO.asc()).all()]

    return render_template('codigos_contabeis/vinculacao/lista_vinculacoes.html',
                           vinculacoes=vinculacoes_completas,
                           anos_disponiveis=anos_disponiveis,
                           codigos_disponiveis=codigos_disponiveis,
                           arquivos_disponiveis=arquivos_disponiveis,
                           ano_filtro=ano_filtro,
                           codigo_filtro=codigo_filtro,
                           arquivo_filtro=arquivo_filtro,
                           id_item_filtro=id_item_filtro)


@vinculacao_bp.route('/vinculacao/nova', methods=['GET', 'POST'])
@login_required
def nova_vinculacao():
    """Formulário para nova vinculação"""
    if request.method == 'POST':
        try:
            id_item_siscor = int(request.form['id_item_siscor'])
            codigo = request.form['codigo'].strip()
            dsc_arquivo = request.form.get('dsc_arquivo', '').strip() or None
            ano = int(request.form['ano'])
            arquivo7_str = request.form.get('arquivo7', '')
            arquivo7 = 1 if arquivo7_str == '1' else None

            chave_primaria = {'ID_ITEM': id_item_siscor, 'CODIGO': codigo, 'ANO': ano}
            vinculacao_existente = ItemContaSucor.query.get(chave_primaria)
            if vinculacao_existente:
                flash(f'Já existe uma vinculação para o Item {id_item_siscor}, Código {codigo} e Ano {ano}.', 'danger')
                return redirect(url_for('vinculacao.nova_vinculacao'))

            item_siscor = DescricaoItensSiscor.query.get(id_item_siscor)
            if not item_siscor:
                flash('Item SISCOR não encontrado.', 'danger')
                return redirect(url_for('vinculacao.nova_vinculacao'))

            nova_vinculacao = ItemContaSucor(
                ID_ITEM=id_item_siscor, CODIGO=codigo, ANO=ano,
                DSC_ARQUIVO=dsc_arquivo, ARQUIVO7=arquivo7
            )
            db.session.add(nova_vinculacao)
            db.session.commit()

            dados_novos = {
                'id_item': nova_vinculacao.ID_ITEM, 'codigo': nova_vinculacao.CODIGO, 'ano': nova_vinculacao.ANO,
                'dsc_arquivo': nova_vinculacao.DSC_ARQUIVO, 'arquivo7': nova_vinculacao.ARQUIVO7,
                'item_siscor_descricao': item_siscor.DSC_ITEM_ORCAMENTO
            }
            registrar_log(
                acao='criar', entidade='vinculacao',
                entidade_id=f"{nova_vinculacao.ID_ITEM}-{nova_vinculacao.CODIGO}-{nova_vinculacao.ANO}",
                descricao=f'Criação de vinculação {codigo} - {item_siscor.DSC_ITEM_ORCAMENTO}',
                dados_novos=dados_novos
            )
            flash('Vinculação criada com sucesso!', 'success')
            return redirect(url_for('vinculacao.lista_vinculacoes'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao criar vinculação: {str(e)}', 'danger')
            return redirect(url_for('vinculacao.nova_vinculacao'))

    itens_siscor = DescricaoItensSiscor.obter_itens_ordenados()
    codigos_contabeis = CodigoContabilVinculacao.obter_codigos_ordenados()
    arquivos_disponiveis = CodigoContabilVinculacao.obter_arquivos_distinct()
    ano_atual = datetime.now().year
    return render_template('codigos_contabeis/vinculacao/form_vinculacao.html',
                           itens_siscor=itens_siscor, codigos_contabeis=codigos_contabeis,
                           arquivos_disponiveis=arquivos_disponiveis, ano_atual=ano_atual)


@vinculacao_bp.route('/vinculacao/editar/<int:id_item>/<string:codigo>/<int:ano>', methods=['GET', 'POST'])
@login_required
def editar_vinculacao(id_item, codigo, ano):
    """Editar vinculação existente"""
    chave_primaria = {'ID_ITEM': id_item, 'CODIGO': codigo, 'ANO': ano}
    vinculacao = db.session.get(ItemContaSucor, chave_primaria)
    if not vinculacao:
        flash('Vinculação não encontrada.', 'danger')
        return redirect(url_for('vinculacao.lista_vinculacoes'))

    if request.method == 'POST':
        try:
            dados_antigos = {
                'id_item': vinculacao.ID_ITEM, 'codigo': vinculacao.CODIGO, 'ano': vinculacao.ANO,
                'dsc_arquivo': vinculacao.DSC_ARQUIVO, 'arquivo7': vinculacao.ARQUIVO7
            }
            vinculacao.DSC_ARQUIVO = request.form.get('dsc_arquivo', '').strip() or None
            arquivo7_str = request.form.get('arquivo7', '')
            vinculacao.ARQUIVO7 = 1 if arquivo7_str == '1' else None
            dados_novos = {
                'id_item': vinculacao.ID_ITEM, 'codigo': vinculacao.CODIGO, 'ano': vinculacao.ANO,
                'dsc_arquivo': vinculacao.DSC_ARQUIVO, 'arquivo7': vinculacao.ARQUIVO7
            }
            db.session.commit()
            registrar_log(
                acao='editar', entidade='vinculacao',
                entidade_id=f"{vinculacao.ID_ITEM}-{vinculacao.CODIGO}-{vinculacao.ANO}",
                descricao=f'Edição de vinculação {vinculacao.CODIGO}',
                dados_antigos=dados_antigos, dados_novos=dados_novos
            )
            flash('Vinculação atualizada com sucesso!', 'success')
            return redirect(url_for('vinculacao.lista_vinculacoes'))
        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao atualizar vinculação: {str(e)}', 'danger')

    itens_siscor = DescricaoItensSiscor.obter_itens_ordenados()
    codigos_contabeis = CodigoContabilVinculacao.obter_codigos_ordenados()
    arquivos_disponiveis = CodigoContabilVinculacao.obter_arquivos_distinct()
    return render_template('codigos_contabeis/vinculacao/form_vinculacao.html',
                           vinculacao=vinculacao, itens_siscor=itens_siscor,
                           codigos_contabeis=codigos_contabeis, arquivos_disponiveis=arquivos_disponiveis,
                           edit_mode=True)


@vinculacao_bp.route('/vinculacao/excluir/<int:id_item>/<string:codigo>/<int:ano>', methods=['POST'])
@login_required
def excluir_vinculacao(id_item, codigo, ano):
    """Remover vinculação (delete físico)"""
    try:
        chave_primaria = {'ID_ITEM': id_item, 'CODIGO': codigo, 'ANO': ano}
        vinculacao = db.session.get(ItemContaSucor, chave_primaria)
        if not vinculacao:
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({'success': False, 'message': 'Vinculação não encontrada.'}), 404
            flash('Vinculação não encontrada.', 'danger')
            return redirect(url_for('vinculacao.lista_vinculacoes'))

        dados_antigos = {
            'id_item': vinculacao.ID_ITEM, 'codigo': vinculacao.CODIGO, 'ano': vinculacao.ANO,
            'dsc_arquivo': vinculacao.DSC_ARQUIVO, 'arquivo7': vinculacao.ARQUIVO7
        }
        db.session.delete(vinculacao)
        db.session.commit()
        registrar_log(
            acao='remover_vinculacao', entidade='vinculacao',
            entidade_id=f"{id_item}-{codigo}-{ano}",
            descricao=f'Remoção de vinculação {codigo}', dados_antigos=dados_antigos
        )
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'success': True, 'message': 'Vinculação removida com sucesso!'})
        flash('Vinculação removida com sucesso!', 'success')
        return redirect(url_for('vinculacao.lista_vinculacoes'))
    except Exception as e:
        db.session.rollback()
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'success': False, 'message': f'Erro ao remover vinculação: {str(e)}'})
        flash(f'Erro ao remover vinculação: {str(e)}', 'danger')
        return redirect(url_for('vinculacao.lista_vinculacoes'))