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
    # Contar vinculações ativas
    total_vinculacoes = ItemContaSucor.query.count()

    # Contar por ano atual
    ano_atual = datetime.now().year
    vinculacoes_ano_atual = ItemContaSucor.query.filter(
        ItemContaSucor.ANO == ano_atual
    ).count()

    # Contar itens SISCOR disponíveis
    itens_siscor = len(DescricaoItensSiscor.obter_itens_ordenados())

    # Contar códigos contábeis disponíveis
    codigos_disponiveis = len(CodigoContabilVinculacao.obter_codigos_ordenados())

    # Últimas vinculações
    ultimas_vinculacoes = ItemContaSucor.query.order_by(
        ItemContaSucor.ID_ITEM.desc()
    ).limit(5).all()

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
    # Filtros
    ano_filtro = request.args.get('ano', type=int)
    codigo_filtro = request.args.get('codigo', '')
    arquivo_filtro = request.args.get('arquivo', '')

    query = ItemContaSucor.query

    if ano_filtro:
        query = query.filter(ItemContaSucor.ANO == ano_filtro)

    if codigo_filtro:
        query = query.filter(ItemContaSucor.CODIGO == codigo_filtro)

    if arquivo_filtro:
        query = query.filter(ItemContaSucor.DSC_ARQUIVO == arquivo_filtro)

    vinculacoes = query.order_by(ItemContaSucor.ANO.desc(), ItemContaSucor.ID_ITEM.desc()).all()

    # Buscar dados para vincular nas vinculações
    vinculacoes_completas = []
    for vinculacao in vinculacoes:
        # Buscar descrição do item SISCOR
        item_siscor = DescricaoItensSiscor.query.filter_by(ID_ITEM=vinculacao.ID_ITEM).first()

        vinculacao_completa = {
            'id_item': vinculacao.ID_ITEM,
            'codigo': vinculacao.CODIGO,
            'dsc_arquivo': vinculacao.DSC_ARQUIVO,
            'ano': vinculacao.ANO,
            'dsc_item_orcamento': item_siscor.DSC_ITEM_ORCAMENTO if item_siscor else 'Item não encontrado'
        }
        vinculacoes_completas.append(vinculacao_completa)

    # Obter dados para filtros
    anos_disponiveis = db.session.query(ItemContaSucor.ANO).distinct().order_by(
        ItemContaSucor.ANO.desc()
    ).all()
    anos_disponiveis = [ano[0] for ano in anos_disponiveis]

    codigos_disponiveis = db.session.query(ItemContaSucor.CODIGO).distinct().order_by(
        ItemContaSucor.CODIGO.asc()
    ).all()
    codigos_disponiveis = [codigo[0] for codigo in codigos_disponiveis]

    arquivos_disponiveis = db.session.query(ItemContaSucor.DSC_ARQUIVO).distinct().filter(
        ItemContaSucor.DSC_ARQUIVO.isnot(None)
    ).order_by(ItemContaSucor.DSC_ARQUIVO.asc()).all()
    arquivos_disponiveis = [arquivo[0] for arquivo in arquivos_disponiveis]

    return render_template('codigos_contabeis/vinculacao/lista_vinculacoes.html',
                           vinculacoes=vinculacoes_completas,
                           anos_disponiveis=anos_disponiveis,
                           codigos_disponiveis=codigos_disponiveis,
                           arquivos_disponiveis=arquivos_disponiveis,
                           ano_filtro=ano_filtro,
                           codigo_filtro=codigo_filtro,
                           arquivo_filtro=arquivo_filtro)


@vinculacao_bp.route('/vinculacao/nova', methods=['GET', 'POST'])
@login_required
def nova_vinculacao():
    """Formulário para nova vinculação"""
    if request.method == 'POST':
        try:
            # Obter dados do formulário
            id_item_siscor = int(request.form['id_item_siscor'])
            codigo = request.form['codigo'].strip()
            dsc_arquivo = request.form.get('dsc_arquivo', '').strip()
            ano = int(request.form['ano'])

            # Verificar se o item SISCOR existe
            item_siscor = DescricaoItensSiscor.query.filter_by(ID_ITEM=id_item_siscor).first()
            if not item_siscor:
                flash('Item SISCOR não encontrado.', 'danger')
                return render_template('codigos_contabeis/vinculacao/form_vinculacao.html',
                                       itens_siscor=DescricaoItensSiscor.obter_itens_ordenados(),
                                       codigos_contabeis=CodigoContabilVinculacao.obter_codigos_ordenados(),
                                       arquivos_disponiveis=CodigoContabilVinculacao.obter_arquivos_distinct())

            # Tratar DSC_ARQUIVO vazio
            if not dsc_arquivo:
                dsc_arquivo = None

            # Criar nova vinculação usando método customizado
            novo_id = ItemContaSucor.criar_vinculacao(codigo, dsc_arquivo, ano)

            db.session.commit()

            # Registrar log de auditoria
            dados_novos = {
                'id_item_gerado': novo_id,
                'item_siscor_selecionado': id_item_siscor,
                'codigo': codigo,
                'dsc_arquivo': dsc_arquivo,
                'ano': ano,
                'item_siscor_descricao': item_siscor.DSC_ITEM_ORCAMENTO
            }

            registrar_log(
                acao='criar',
                entidade='vinculacao',
                entidade_id=novo_id,
                descricao=f'Criação de vinculação {codigo} - {item_siscor.DSC_ITEM_ORCAMENTO}',
                dados_novos=dados_novos
            )

            flash('Vinculação criada com sucesso!', 'success')
            return redirect(url_for('vinculacao.lista_vinculacoes'))

        except ValueError as e:
            flash(f'Erro nos dados informados: {str(e)}', 'danger')
        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao criar vinculação: {str(e)}', 'danger')

    # GET - Mostrar formulário
    itens_siscor = DescricaoItensSiscor.obter_itens_ordenados()
    codigos_contabeis = CodigoContabilVinculacao.obter_codigos_ordenados()
    arquivos_disponiveis = CodigoContabilVinculacao.obter_arquivos_distinct()
    ano_atual = datetime.now().year

    return render_template('codigos_contabeis/vinculacao/form_vinculacao.html',
                           itens_siscor=itens_siscor,
                           codigos_contabeis=codigos_contabeis,
                           arquivos_disponiveis=arquivos_disponiveis,
                           ano_atual=ano_atual)

@vinculacao_bp.route('/vinculacao/editar/<int:id>', methods=['GET', 'POST'])
@login_required
def editar_vinculacao(id):
    """Editar vinculação existente"""
    vinculacao = ItemContaSucor.query.get_or_404(id)

    if request.method == 'POST':
        try:
            # Capturar dados antigos para auditoria
            dados_antigos = {
                'id_item': vinculacao.ID_ITEM,
                'codigo': vinculacao.CODIGO,
                'dsc_arquivo': vinculacao.DSC_ARQUIVO,
                'ano': vinculacao.ANO
            }

            # Obter novos dados
            id_item_siscor = int(request.form['id_item_siscor'])
            codigo = request.form['codigo'].strip()
            dsc_arquivo = request.form.get('dsc_arquivo', '').strip()
            ano = int(request.form['ano'])

            # Verificar se o item SISCOR existe
            item_siscor = DescricaoItensSiscor.query.filter_by(ID_ITEM=id_item_siscor).first()
            if not item_siscor:
                flash('Item SISCOR não encontrado.', 'danger')
                return render_template('codigos_contabeis/vinculacao/form_vinculacao.html',
                                       vinculacao=vinculacao,
                                       itens_siscor=DescricaoItensSiscor.obter_itens_ordenados(),
                                       codigos_contabeis=CodigoContabilVinculacao.obter_codigos_ordenados(),
                                       arquivos_disponiveis=CodigoContabilVinculacao.obter_arquivos_distinct())

            # Atualizar os dados (permite duplicações)
            vinculacao.CODIGO = codigo
            vinculacao.DSC_ARQUIVO = dsc_arquivo if dsc_arquivo else None
            vinculacao.ANO = ano

            # Dados novos para auditoria
            dados_novos = {
                'id_item': vinculacao.ID_ITEM,
                'item_siscor_selecionado': id_item_siscor,
                'codigo': vinculacao.CODIGO,
                'dsc_arquivo': vinculacao.DSC_ARQUIVO,
                'ano': vinculacao.ANO,
                'item_siscor_descricao': item_siscor.DSC_ITEM_ORCAMENTO
            }

            db.session.commit()

            # Registrar log de auditoria
            registrar_log(
                acao='editar',
                entidade='vinculacao',
                entidade_id=vinculacao.ID_ITEM,
                descricao=f'Edição de vinculação {vinculacao.CODIGO}',
                dados_antigos=dados_antigos,
                dados_novos=dados_novos
            )

            flash('Vinculação atualizada com sucesso!', 'success')
            return redirect(url_for('vinculacao.lista_vinculacoes'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao atualizar vinculação: {str(e)}', 'danger')

    # GET - Mostrar formulário de edição
    itens_siscor = DescricaoItensSiscor.obter_itens_ordenados()
    codigos_contabeis = CodigoContabilVinculacao.obter_codigos_ordenados()
    arquivos_disponiveis = CodigoContabilVinculacao.obter_arquivos_distinct()

    return render_template('codigos_contabeis/vinculacao/form_vinculacao.html',
                           vinculacao=vinculacao,
                           itens_siscor=itens_siscor,
                           codigos_contabeis=codigos_contabeis,
                           arquivos_disponiveis=arquivos_disponiveis)

@vinculacao_bp.route('/vinculacao/excluir/<int:id>', methods=['POST'])
@login_required
def excluir_vinculacao(id):
    """Excluir vinculação (delete físico)"""
    try:
        vinculacao = ItemContaSucor.query.get_or_404(id)

        # Capturar dados para auditoria
        dados_antigos = {
            'id_item': vinculacao.ID_ITEM,
            'codigo': vinculacao.CODIGO,
            'dsc_arquivo': vinculacao.DSC_ARQUIVO,
            'ano': vinculacao.ANO
        }

        db.session.delete(vinculacao)
        db.session.commit()

        # Registrar log de auditoria
        registrar_log(
            acao='excluir',
            entidade='vinculacao',
            entidade_id=id,
            descricao=f'Exclusão de vinculação {vinculacao.CODIGO}',
            dados_antigos=dados_antigos
        )

        # Se a requisição for AJAX, retorna JSON
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({
                'success': True,
                'message': f'Vinculação {dados_antigos["codigo"]} excluída com sucesso!'
            })

        flash('Vinculação excluída com sucesso!', 'success')
        return redirect(url_for('vinculacao.lista_vinculacoes'))

    except Exception as e:
        db.session.rollback()

        # Se a requisição for AJAX, retorna JSON com erro
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({
                'success': False,
                'message': f'Erro ao excluir: {str(e)}'
            })

        flash(f'Erro ao excluir vinculação: {str(e)}', 'danger')
        return redirect(url_for('vinculacao.lista_vinculacoes'))