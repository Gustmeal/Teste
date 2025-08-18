from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from app import db
from app.models.indicador import IndicadorFormula, CodigoIndicador, VariavelIndicador, IndicadorAno
from app.utils.audit import registrar_log
from datetime import datetime, date
from sqlalchemy import extract, func, not_
from calendar import monthrange
from decimal import Decimal
from app.models.indicador import MetaAnual


indicador_bp = Blueprint('indicador', __name__)


@indicador_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.now().year}


@indicador_bp.route('/indicadores')
@login_required
def index():
    """Página principal com os cards do sistema de indicadores"""
    return render_template('indicadores/index.html')


from sqlalchemy import distinct # Adicionar esta importação no início do arquivo

@indicador_bp.route('/indicadores/formulas')
@login_required
def formulas():
    """Página principal do sistema de fórmulas de indicadores com filtros."""
    # Capturar filtros da URL
    indicador_filtro = request.args.get('indicador', '')
    data_inicio_filtro = request.args.get('data_inicio', '')
    data_fim_filtro = request.args.get('data_fim', '')

    # Query base, excluindo registros 'BDG'
    query = IndicadorFormula.query.filter(
        not_(IndicadorFormula.RESPONSAVEL_INCLUSAO == 'BDG')
    )

    # Aplicar filtros conforme preenchidos
    if indicador_filtro:
        query = query.filter(IndicadorFormula.INDICADOR == indicador_filtro)
    if data_inicio_filtro:
        try:
            # Converte a data do filtro e aplica na query
            data_inicio = datetime.strptime(data_inicio_filtro, '%Y-%m-%d').date()
            query = query.filter(IndicadorFormula.DT_REFERENCIA >= data_inicio)
        except ValueError:
            flash('Data de início inválida. Use o formato AAAA-MM-DD.', 'warning')
    if data_fim_filtro:
        try:
            # Converte a data do filtro e aplica na query
            data_fim = datetime.strptime(data_fim_filtro, '%Y-%m-%d').date()
            query = query.filter(IndicadorFormula.DT_REFERENCIA <= data_fim)
        except ValueError:
            flash('Data de fim inválida. Use o formato AAAA-MM-DD.', 'warning')

    # Executar a query com ordenação
    registros = query.order_by(
        IndicadorFormula.DT_REFERENCIA.desc(),
        IndicadorFormula.INDICADOR
    ).all()

    # Agrupar registros para a visualização
    registros_agrupados = {}
    for reg in registros:
        chave = (reg.DT_REFERENCIA, reg.INDICADOR)
        if chave not in registros_agrupados:
            registros_agrupados[chave] = []
        registros_agrupados[chave].append(reg)

    # Buscar indicadores únicos para popular o dropdown de filtro
    indicadores_disponiveis = db.session.query(
        distinct(IndicadorFormula.INDICADOR)
    ).order_by(IndicadorFormula.INDICADOR).all()
    # Extrai o primeiro elemento de cada tupla retornada
    indicadores_disponiveis = [ind[0] for ind in indicadores_disponiveis]

    # Renderizar o template, passando os dados e os filtros atuais
    return render_template('indicadores/formulas.html',
                           registros_agrupados=registros_agrupados,
                           indicadores_disponiveis=indicadores_disponiveis,
                           indicador_filtro=indicador_filtro,
                           data_inicio_filtro=data_inicio_filtro,
                           data_fim_filtro=data_fim_filtro)


@indicador_bp.route('/indicadores/novo', methods=['GET', 'POST'])
@login_required
def novo():
    """Formulário para inclusão de novos indicadores"""
    if request.method == 'POST':
        try:
            # Capturar dados do formulário
            mes = int(request.form.get('mes'))
            ano = int(request.form.get('ano'))
            indicador_sg = request.form.get('indicador')

            # Calcular último dia do mês
            ultimo_dia = monthrange(ano, mes)[1]
            dt_referencia = date(ano, mes, ultimo_dia)

            # Buscar o código do indicador selecionado
            codigo_indicador = CodigoIndicador.query.filter_by(
                SG_INDICADOR=indicador_sg
            ).first()

            if not codigo_indicador:
                flash('Indicador não encontrado.', 'danger')
                return redirect(url_for('indicador.novo'))

            # Buscar todas as variáveis deste indicador
            variaveis = VariavelIndicador.query.filter_by(
                CO_INDICADOR=codigo_indicador.CO_INDICADOR
            ).order_by(VariavelIndicador.VARIAVEL).all()

            # Verificar se já existe registro para esta data/indicador
            registro_existente = IndicadorFormula.query.filter_by(
                DT_REFERENCIA=dt_referencia,
                INDICADOR=indicador_sg
            ).first()

            if registro_existente:
                flash('Já existe registro para este indicador nesta data.', 'warning')
                return redirect(url_for('indicador.formulas'))

            # Definir tamanhos máximos baseados no banco
            MAX_INDICADOR = 18
            MAX_NO_VARIAVEL = 50
            MAX_FONTE = 100
            MAX_RESPONSAVEL = 100

            # Inserir um registro para cada variáveis
            for var in variaveis:
                valor_str = request.form.get(f'valor_{var.VARIAVEL}', '0')
                valor = Decimal(valor_str.replace('.', '').replace(',', '.')) if valor_str else Decimal('0')

                # Truncar strings se necessário
                no_variavel = var.NO_VARIAVEL[:MAX_NO_VARIAVEL] if var.NO_VARIAVEL else ''
                fonte = var.FONTE[:MAX_FONTE] if var.FONTE else ''
                responsavel = current_user.nome[:MAX_RESPONSAVEL] if current_user.nome else ''

                novo_registro = IndicadorFormula(
                    DT_REFERENCIA=dt_referencia,
                    INDICADOR=indicador_sg[:MAX_INDICADOR],
                    VARIAVEL=var.VARIAVEL,
                    NO_VARIAVEL=no_variavel,
                    FONTE=fonte,
                    VR_VARIAVEL=valor,
                    RESPONSAVEL_INCLUSAO=responsavel
                )
                db.session.add(novo_registro)

            db.session.commit()

            # Registrar log
            registrar_log(
                acao='criar',
                entidade='indicador',
                entidade_id=None,
                descricao=f'Inclusão de indicador {indicador_sg} para {mes}/{ano}'
            )

            flash('Indicador incluído com sucesso!', 'success')
            return redirect(url_for('indicador.formulas'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao incluir indicador: {str(e)}', 'danger')
            return redirect(url_for('indicador.novo'))

    # GET - Carregar dados para o formulário
    indicadores = CodigoIndicador.query.order_by(CodigoIndicador.SG_INDICADOR).all()

    # Gerar lista de meses e anos para seleção
    ano_atual = datetime.now().year
    anos = list(range(ano_atual - 2, ano_atual + 2))
    meses = [
        (1, 'Janeiro'), (2, 'Fevereiro'), (3, 'Março'),
        (4, 'Abril'), (5, 'Maio'), (6, 'Junho'),
        (7, 'Julho'), (8, 'Agosto'), (9, 'Setembro'),
        (10, 'Outubro'), (11, 'Novembro'), (12, 'Dezembro')
    ]

    return render_template('indicadores/form.html',
                           indicadores=indicadores,
                           anos=anos,
                           meses=meses,
                           mes_atual=datetime.now().month,
                           ano_atual=ano_atual)


@indicador_bp.route('/indicadores/api/variaveis/<string:sg_indicador>')
@login_required
def get_variaveis(sg_indicador):
    """API para buscar variáveis de um indicador e verificar dados existentes."""
    # Buscar código do indicador
    codigo_indicador = CodigoIndicador.query.filter_by(
        SG_INDICADOR=sg_indicador
    ).first()

    if not codigo_indicador:
        return jsonify({'erro': 'Indicador não encontrado'}), 404

    # Obter mês e ano dos parâmetros da requisição
    mes = request.args.get('mes', type=int)
    ano = request.args.get('ano', type=int)

    # Buscar variáveis
    variaveis = VariavelIndicador.query.filter_by(
        CO_INDICADOR=codigo_indicador.CO_INDICADOR
    ).order_by(VariavelIndicador.VARIAVEL).all()

    # Verificar se já existem dados para este período
    dados_existentes = {}
    ja_existe = False
    if mes and ano:
        try:
            ultimo_dia = monthrange(ano, mes)[1]
            dt_referencia = date(ano, mes, ultimo_dia)

            registros_existentes = IndicadorFormula.query.filter_by(
                DT_REFERENCIA=dt_referencia,
                INDICADOR=sg_indicador
            ).all()

            if registros_existentes:
                ja_existe = True
                dados_existentes = {reg.VARIAVEL: reg.VR_VARIAVEL for reg in registros_existentes}
        except ValueError:
            # Ignora caso o mês/ano seja inválido
            pass

    # Formatar resposta
    variaveis_lista = []
    for var in variaveis:
        valor_existente = dados_existentes.get(var.VARIAVEL)
        variaveis_lista.append({
            'variavel': var.VARIAVEL,
            'nome': var.NO_VARIAVEL,
            'fonte': var.FONTE,
            'valor': f'{valor_existente:.2f}'.replace('.', ',') if valor_existente is not None else ''
        })

    return jsonify({
        'indicador': {
            'codigo': codigo_indicador.CO_INDICADOR,
            'sigla': codigo_indicador.SG_INDICADOR,
            'descricao': codigo_indicador.DSC_INDICADOR,
            'qtde_variaveis': codigo_indicador.QTDE_VARIAVEIS
        },
        'variaveis': variaveis_lista,
        'ja_existe': ja_existe
    })


@indicador_bp.route('/indicadores/editar/<string:dt_ref>/<string:indicador>')
@login_required
def editar(dt_ref, indicador):
    """Editar valores de um indicador"""
    try:
        # Converter string de data
        dt_referencia = datetime.strptime(dt_ref, '%Y-%m-%d').date()

        # Buscar registros
        registros = IndicadorFormula.query.filter_by(
            DT_REFERENCIA=dt_referencia,
            INDICADOR=indicador
        ).order_by(IndicadorFormula.VARIAVEL).all()

        if not registros:
            flash('Registros não encontrados.', 'warning')
            return redirect(url_for('indicador.formulas'))

        return render_template('indicadores/editar.html',
                               registros=registros,
                               dt_referencia=dt_referencia,
                               indicador=indicador)

    except Exception as e:
        flash(f'Erro ao carregar registros: {str(e)}', 'danger')
        return redirect(url_for('indicador.formulas'))


@indicador_bp.route('/indicadores/atualizar', methods=['POST'])
@login_required
def atualizar():
    """Atualizar valores de um indicador"""
    try:
        dt_ref = request.form.get('dt_referencia')
        indicador = request.form.get('indicador')

        # Converter string de data
        dt_referencia = datetime.strptime(dt_ref, '%Y-%m-%d').date()

        # Buscar registros existentes
        registros = IndicadorFormula.query.filter_by(
            DT_REFERENCIA=dt_referencia,
            INDICADOR=indicador
        ).all()

        # Definir tamanho máximo
        MAX_RESPONSAVEL = 100

        # Preparar nome do responsável
        nome_usuario = current_user.nome
        if len(nome_usuario) > MAX_RESPONSAVEL:
            primeiro_nome = nome_usuario.split()[0]
            if len(primeiro_nome) <= MAX_RESPONSAVEL:
                responsavel = primeiro_nome
            else:
                responsavel = nome_usuario[:MAX_RESPONSAVEL]
        else:
            responsavel = nome_usuario

        # Atualizar cada registro APENAS se o valor mudou
        valores_alterados = False
        for reg in registros:
            # Capturar valor original do formulário
            valor_original_str = request.form.get(f'valor_original_{reg.VARIAVEL}', '0')
            # CORREÇÃO: Remover pontos (separador de milhar) e depois substituir vírgula por ponto
            valor_original = Decimal(valor_original_str.replace('.', '').replace(',', '.'))

            # Capturar novo valor
            valor_novo_str = request.form.get(f'valor_{reg.VARIAVEL}', '0')
            # CORREÇÃO: Remover pontos (separador de milhar) e depois substituir vírgula por ponto
            valor_novo = Decimal(valor_novo_str.replace('.', '').replace(',', '.'))

            # Só atualizar se o valor mudou
            if valor_original != valor_novo:
                reg.VR_VARIAVEL = valor_novo
                reg.RESPONSAVEL_INCLUSAO = responsavel
                valores_alterados = True

        db.session.commit()

        # Registrar log
        registrar_log(
            acao='editar',
            entidade='indicador',
            entidade_id=None,
            descricao=f'Atualização de valores do indicador {indicador} para {dt_ref}'
        )

        if valores_alterados:
            flash('Valores atualizados com sucesso!', 'success')
        else:
            flash('Nenhum valor foi alterado.', 'info')

        return redirect(url_for('indicador.formulas'))

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao atualizar valores: {str(e)}', 'danger')
        return redirect(url_for('indicador.formulas'))


@indicador_bp.route('/indicadores/excluir/<string:dt_ref>/<string:indicador>')
@login_required
def excluir(dt_ref, indicador):
    """Excluir todos os registros de um indicador em uma data"""
    try:
        # Converter string de data
        dt_referencia = datetime.strptime(dt_ref, '%Y-%m-%d').date()

        # Buscar e excluir registros
        registros = IndicadorFormula.query.filter_by(
            DT_REFERENCIA=dt_referencia,
            INDICADOR=indicador
        ).all()

        if not registros:
            flash('Registros não encontrados.', 'warning')
            return redirect(url_for('indicador.formulas'))

        # Excluir todos os registros
        for reg in registros:
            db.session.delete(reg)

        db.session.commit()

        # Registrar log
        registrar_log(
            acao='excluir',
            entidade='indicador',
            entidade_id=None,
            descricao=f'Exclusão do indicador {indicador} para {dt_ref}'
        )

        flash('Indicador excluído com sucesso!', 'success')

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao excluir indicador: {str(e)}', 'danger')

    return redirect(url_for('indicador.formulas'))


# Rotas para Inclusão de Indicadores (antigo Itens)
@indicador_bp.route('/indicadores/itens')
@login_required
def itens():
    """Listar todos os itens de indicadores por ano"""
    itens = IndicadorAno.query.order_by(
        IndicadorAno.ANO.desc(),
        IndicadorAno.ORDEM
    ).all()

    # Buscar anos disponíveis para filtro
    anos_disponiveis = db.session.query(
        IndicadorAno.ANO
    ).distinct().order_by(IndicadorAno.ANO.desc()).all()
    anos_disponiveis = [ano[0] for ano in anos_disponiveis]

    return render_template('indicadores/itens.html',
                           itens=itens,
                           anos_disponiveis=anos_disponiveis)


@indicador_bp.route('/indicadores/itens/novo', methods=['GET', 'POST'])
@login_required
def novo_item():
    """Criar novo item de indicador"""
    if request.method == 'POST':
        try:
            # Capturar dados do formulário
            ano = request.form.get('ano')
            ordem = int(request.form.get('ordem'))

            # Verificar se já existe
            existe = IndicadorAno.query.filter_by(
                ANO=ano,
                ORDEM=ordem
            ).first()

            if existe:
                flash('Já existe um item com esta ordem neste ano.', 'warning')
                return redirect(url_for('indicador.novo_item'))

            # --- CORREÇÃO APLICADA AQUI ---
            # Captura e trata o valor da meta, removendo o separador de milhar.
            meta_str = request.form.get('meta')
            meta_valor = None
            if meta_str:
                meta_valor = Decimal(meta_str.replace('.', '').replace(',', '.'))
            # --- FIM DA CORREÇÃO ---

            # Criar novo item
            novo_item = IndicadorAno(
                ANO=ano,
                ORDEM=ordem,
                INDICADOR=request.form.get('indicador'),
                DSC_INDICADOR=request.form.get('dsc_indicador'),
                DIMENSAO=request.form.get('dimensao') or None,
                UNIDADE_MEDIDA=request.form.get('unidade_medida') or None,
                UNIDADE=request.form.get('unidade') or None,
                QT_MAIOR_MELHOR=request.form.get('qt_maior_melhor') == '1' if request.form.get(
                    'qt_maior_melhor') else None,
                DESTINACAO=request.form.get('destinacao') or None,
                META=meta_valor  # Usa a variável tratada
            )

            db.session.add(novo_item)
            db.session.commit()

            # Registrar log
            registrar_log(
                acao='criar',
                entidade='indicador_item',
                entidade_id=None,
                descricao=f'Criação de item de indicador: {ano} - Ordem {ordem}'
            )

            flash('Item criado com sucesso!', 'success')
            return redirect(url_for('indicador.itens'))

        except Exception as e:
            db.session.rollback()
            # Adicionado o tipo da exceção ao flash para facilitar a depuração
            flash(f'Erro ao criar item: [{type(e).__name__}] {str(e)}', 'danger')
            return redirect(url_for('indicador.novo_item'))

    # GET - Preparar dados para o formulário
    ano_atual = datetime.now().year
    anos = [str(ano) for ano in range(ano_atual - 2, ano_atual + 3)]

    # Buscar valores únicos para os selects
    dimensoes = db.session.query(IndicadorAno.DIMENSAO).filter(
        IndicadorAno.DIMENSAO.isnot(None)
    ).distinct().order_by(IndicadorAno.DIMENSAO).all()
    dimensoes = [d[0] for d in dimensoes if d[0]]

    unidades_medida = db.session.query(IndicadorAno.UNIDADE_MEDIDA).filter(
        IndicadorAno.UNIDADE_MEDIDA.isnot(None)
    ).distinct().order_by(IndicadorAno.UNIDADE_MEDIDA).all()
    unidades_medida = [u[0] for u in unidades_medida if u[0]]

    return render_template('indicadores/form_item.html',
                           anos=anos,
                           dimensoes=dimensoes,
                           unidades_medida=unidades_medida,
                           editando=False)

@indicador_bp.route('/indicadores/itens/editar/<string:ano>/<int:ordem>', methods=['GET', 'POST'])
@login_required
def editar_item(ano, ordem):
    """Editar item de indicador existente"""
    item = IndicadorAno.query.filter_by(ANO=ano, ORDEM=ordem).first_or_404()

    if request.method == 'POST':
        try:
            # Função auxiliar para tratar strings vazias
            def get_form_value(key):
                value = request.form.get(key)
                return value if value else None

            # Atualizar campos (ano e ordem não podem ser alterados)
            item.INDICADOR = get_form_value('indicador')
            item.DSC_INDICADOR = get_form_value('dsc_indicador')
            item.DIMENSAO = get_form_value('dimensao')
            item.UNIDADE_MEDIDA = get_form_value('unidade_medida')
            item.UNIDADE = get_form_value('unidade')
            item.QT_MAIOR_MELHOR = request.form.get('qt_maior_melhor') == '1' if request.form.get(
                'qt_maior_melhor') else None
            item.DESTINACAO = get_form_value('destinacao')

            # --- CORREÇÃO APLICADA AQUI ---
            # Captura e trata o valor da meta, removendo o separador de milhar.
            meta_str = request.form.get('meta')
            if meta_str:
                # Remove pontos (separador de milhar) e substitui a vírgula (decimal) por ponto
                item.META = Decimal(meta_str.replace('.', '').replace(',', '.'))
            else:
                item.META = None
            # --- FIM DA CORREÇÃO ---

            db.session.commit()

            # Registrar log
            registrar_log(
                acao='editar',
                entidade='indicador_item',
                entidade_id=None,
                descricao=f'Edição de item de indicador: {ano} - Ordem {ordem}'
            )

            flash('Item atualizado com sucesso!', 'success')
            return redirect(url_for('indicador.itens'))

        except Exception as e:
            db.session.rollback()
            # Adicionado o tipo da exceção ao flash para facilitar a depuração
            flash(f'Erro ao atualizar item: [{type(e).__name__}] {str(e)}', 'danger')

    # GET ou erro no POST
    ano_atual = datetime.now().year
    anos = [str(ano) for ano in range(ano_atual - 2, ano_atual + 3)]

    # Buscar valores únicos para os selects
    dimensoes = db.session.query(IndicadorAno.DIMENSAO).filter(
        IndicadorAno.DIMENSAO.isnot(None)
    ).distinct().order_by(IndicadorAno.DIMENSAO).all()
    dimensoes = [d[0] for d in dimensoes if d[0]]

    unidades_medida = db.session.query(IndicadorAno.UNIDADE_MEDIDA).filter(
        IndicadorAno.UNIDADE_MEDIDA.isnot(None)
    ).distinct().order_by(IndicadorAno.UNIDADE_MEDIDA).all()
    unidades_medida = [u[0] for u in unidades_medida if u[0]]

    return render_template('indicadores/form_item.html',
                           item=item,
                           anos=anos,
                           dimensoes=dimensoes,
                           unidades_medida=unidades_medida,
                           editando=True)

@indicador_bp.route('/indicadores/itens/excluir/<string:ano>/<int:ordem>')
@login_required
def excluir_item(ano, ordem):
    """Excluir item de indicador"""
    try:
        item = IndicadorAno.query.filter_by(ANO=ano, ORDEM=ordem).first_or_404()

        db.session.delete(item)
        db.session.commit()

        # Registrar log
        registrar_log(
            acao='excluir',
            entidade='indicador_item',
            entidade_id=None,
            descricao=f'Exclusão de item de indicador: {ano} - Ordem {ordem}'
        )

        flash('Item excluído com sucesso!', 'success')

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao excluir item: {str(e)}', 'danger')

    return redirect(url_for('indicador.itens'))


@indicador_bp.route('/indicadores/api/proxima-ordem')
@login_required
def proxima_ordem():
    """API para retornar a próxima ordem disponível para um ano"""
    ano = request.args.get('ano')

    if not ano:
        return jsonify({'erro': 'Ano não informado'}), 400

    # Buscar maior ordem do ano
    maior_ordem = db.session.query(func.max(IndicadorAno.ORDEM)).filter_by(ANO=ano).scalar()

    proxima = 1 if maior_ordem is None else maior_ordem + 1

    return jsonify({'proxima_ordem': proxima})


@indicador_bp.route('/indicadores/metas')
@login_required
def metas():
    """Listar todas as metas anuais"""
    # Filtros
    ano_filtro = request.args.get('ano')
    indicador_filtro = request.args.get('indicador')

    # Query base
    query = MetaAnual.query

    # Aplicar filtros
    if ano_filtro:
        query = query.filter(MetaAnual.ANO == ano_filtro)
    if indicador_filtro:
        query = query.filter(MetaAnual.SG_INDICADOR == indicador_filtro)

    # Executar query
    metas = query.order_by(
        MetaAnual.ANO.desc(),
        MetaAnual.SG_INDICADOR,
        MetaAnual.NO_VARIAVEL
    ).all()

    # Buscar valores únicos para os filtros
    anos_disponiveis = db.session.query(MetaAnual.ANO).distinct().order_by(MetaAnual.ANO.desc()).all()
    anos_disponiveis = [ano[0] for ano in anos_disponiveis]

    indicadores_disponiveis = db.session.query(MetaAnual.SG_INDICADOR).distinct().order_by(MetaAnual.SG_INDICADOR).all()
    indicadores_disponiveis = [ind[0] for ind in indicadores_disponiveis]

    return render_template('indicadores/metas.html',
                           metas=metas,
                           anos_disponiveis=anos_disponiveis,
                           indicadores_disponiveis=indicadores_disponiveis,
                           ano_filtro=ano_filtro,
                           indicador_filtro=indicador_filtro)


@indicador_bp.route('/indicadores/metas/novo', methods=['GET', 'POST'])
@login_required
def nova_meta():
    """Criar nova meta anual"""
    if request.method == 'POST':
        try:
            # Capturar dados do formulário
            ano = request.form.get('ano')
            sg_indicador = request.form.get('sg_indicador')
            no_variavel = request.form.get('no_variavel')
            vr_meta_str = request.form.get('vr_meta', '0')
            vr_meta = Decimal(vr_meta_str.replace(',', '.'))

            # VARIAVEL sempre será 1 conforme solicitado
            variavel = 1

            # Verificar se já existe
            existe = MetaAnual.query.filter_by(
                ANO=ano,
                SG_INDICADOR=sg_indicador,
                VARIAVEL=variavel
            ).first()

            if existe:
                flash('Já existe uma meta para este indicador neste ano.', 'warning')
                return redirect(url_for('indicador.nova_meta'))

            # Criar nova meta
            nova_meta = MetaAnual(
                ANO=ano,
                SG_INDICADOR=sg_indicador,
                VARIAVEL=variavel,
                NO_VARIAVEL=no_variavel,
                VR_META=vr_meta
            )

            db.session.add(nova_meta)
            db.session.commit()

            # Registrar log
            registrar_log(
                acao='criar',
                entidade='meta_anual',
                entidade_id=None,
                descricao=f'Criação de meta anual: {ano} - {sg_indicador}'
            )

            flash('Meta criada com sucesso!', 'success')
            return redirect(url_for('indicador.metas'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao criar meta: {str(e)}', 'danger')
            return redirect(url_for('indicador.nova_meta'))

    # GET - Preparar dados para o formulário
    ano_atual = datetime.now().year

    # Buscar indicadores disponíveis (DISTINCT) - da própria tabela de metas
    indicadores_disponiveis = db.session.query(
        MetaAnual.SG_INDICADOR
    ).distinct().order_by(MetaAnual.SG_INDICADOR).all()
    indicadores_disponiveis = [ind[0] for ind in indicadores_disponiveis]

    # Se não houver indicadores na tabela de metas, buscar da tabela de códigos
    if not indicadores_disponiveis:
        codigos_indicadores = CodigoIndicador.query.order_by(CodigoIndicador.SG_INDICADOR).all()
        indicadores_disponiveis = [cod.SG_INDICADOR for cod in codigos_indicadores]

    # Buscar TODAS as variáveis disponíveis (DISTINCT) - sem filtrar por indicador
    variaveis_disponiveis = db.session.query(
        MetaAnual.NO_VARIAVEL
    ).filter(
        MetaAnual.NO_VARIAVEL.isnot(None)
    ).distinct().order_by(MetaAnual.NO_VARIAVEL).all()
    variaveis_disponiveis = [var[0] for var in variaveis_disponiveis if var[0]]

    return render_template('indicadores/form_meta.html',
                           ano_atual=ano_atual,
                           indicadores_disponiveis=indicadores_disponiveis,
                           variaveis_disponiveis=variaveis_disponiveis,
                           editando=False)

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from app import db
from app.models.indicador import IndicadorFormula, CodigoIndicador, VariavelIndicador, IndicadorAno, MetaAnual
from app.utils.audit import registrar_log
from datetime import datetime, date
from sqlalchemy import func
from calendar import monthrange
from decimal import Decimal

# ... (outras rotas e código do blueprint) ...

@indicador_bp.route('/indicadores/metas/editar/<int:ano>/<path:sg_indicador>/<int:variavel>',
                    methods=['GET', 'POST'])
@login_required
def editar_meta(ano, sg_indicador, variavel):
    """Editar meta anual existente"""
    meta = MetaAnual.query.filter_by(
        ANO=ano,
        SG_INDICADOR=sg_indicador,
        VARIAVEL=variavel
    ).first_or_404()

    if request.method == 'POST':
        try:
            # Atualiza os campos que podem ser modificados
            meta.NO_VARIAVEL = request.form.get('no_variavel')
            vr_meta_str = request.form.get('vr_meta', '0')
            meta.VR_META = Decimal(vr_meta_str.replace(',', '.'))

            db.session.commit()

            registrar_log(
                acao='editar',
                entidade='meta_anual',
                entidade_id=None,
                descricao=f'Edição de meta anual: {ano} - {sg_indicador}'
            )

            flash('Meta atualizada com sucesso!', 'success')
            return redirect(url_for('indicador.metas'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao atualizar meta: {str(e)}', 'danger')

    # GET - Preparar dados para o formulário
    # Buscar indicadores e variáveis disponíveis para os selects
    indicadores_disponiveis = db.session.query(
        CodigoIndicador.SG_INDICADOR
    ).distinct().order_by(CodigoIndicador.SG_INDICADOR).all()
    indicadores_disponiveis = [ind[0] for ind in indicadores_disponiveis]

    variaveis_disponiveis = db.session.query(
        VariavelIndicador.NO_VARIAVEL
    ).filter(
        VariavelIndicador.NO_VARIAVEL.isnot(None)
    ).distinct().order_by(VariavelIndicador.NO_VARIAVEL).all()
    variaveis_disponiveis = [var[0] for var in variaveis_disponiveis if var[0]]

    return render_template('indicadores/form_meta.html',
                           meta=meta,
                           indicadores_disponiveis=indicadores_disponiveis,
                           variaveis_disponiveis=variaveis_disponiveis,
                           editando=True)


@indicador_bp.route('/indicadores/metas/excluir/<int:ano>/<path:sg_indicador>/<int:variavel>')
@login_required
def excluir_meta(ano, sg_indicador, variavel):
    """Excluir meta anual"""
    try:
        meta = MetaAnual.query.filter_by(
            ANO=ano,
            SG_INDICADOR=sg_indicador,
            VARIAVEL=variavel
        ).first_or_404()

        db.session.delete(meta)
        db.session.commit()

        registrar_log(
            acao='excluir',
            entidade='meta_anual',
            entidade_id=None,
            descricao=f'Exclusão de meta anual: {ano} - {sg_indicador}'
        )

        flash('Meta excluída com sucesso!', 'success')

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao excluir meta: {str(e)}', 'danger')

    return redirect(url_for('indicador.metas'))


@indicador_bp.route('/indicadores/api/variaveis-metas')
@login_required
def api_variaveis_metas():
    """API para buscar variáveis disponíveis para um indicador"""
    indicador = request.args.get('indicador')

    if not indicador:
        return jsonify({'variaveis': []})

    # Buscar variáveis distintas para o indicador
    variaveis = db.session.query(
        MetaAnual.NO_VARIAVEL
    ).filter(
        MetaAnual.SG_INDICADOR == indicador,
        MetaAnual.NO_VARIAVEL.isnot(None)
    ).distinct().order_by(MetaAnual.NO_VARIAVEL).all()

    variaveis_lista = [var[0] for var in variaveis if var[0]]

    # Se não houver variáveis para este indicador, buscar todas as disponíveis
    if not variaveis_lista:
        variaveis = db.session.query(
            MetaAnual.NO_VARIAVEL
        ).filter(
            MetaAnual.NO_VARIAVEL.isnot(None)
        ).distinct().order_by(MetaAnual.NO_VARIAVEL).all()

        variaveis_lista = [var[0] for var in variaveis if var[0]]

    return jsonify({'variaveis': variaveis_lista})