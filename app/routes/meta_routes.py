from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash
from app.models.meta_avaliacao import MetaAvaliacao
from app.models.edital import Edital
from app.models.periodo import PeriodoAvaliacao
from app.models.empresa_participante import EmpresaParticipante
from datetime import datetime
from flask_login import login_required, current_user
from app import db
from app.utils.audit import registrar_log

meta_bp = Blueprint('meta', __name__, url_prefix='/credenciamento')


@meta_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@meta_bp.route('/metas')
@login_required
def lista_metas():
    # Parâmetros para filtro
    edital_id = request.args.get('edital_id', type=int)
    periodo_id = request.args.get('periodo_id', type=int)
    empresa_id = request.args.get('empresa_id', type=int)
    competencia = request.args.get('competencia', type=str)

    # Consulta base
    query = db.session.query(MetaAvaliacao, EmpresaParticipante). \
        join(EmpresaParticipante, MetaAvaliacao.ID_EMPRESA == EmpresaParticipante.ID_EMPRESA). \
        filter(MetaAvaliacao.DELETED_AT == None, EmpresaParticipante.DELETED_AT == None)

    # Aplicar filtros se fornecidos
    if edital_id:
        query = query.filter(MetaAvaliacao.ID_EDITAL == edital_id)
    if periodo_id:
        query = query.filter(MetaAvaliacao.ID_PERIODO == periodo_id)
    if empresa_id:
        query = query.filter(EmpresaParticipante.ID == empresa_id)
    if competencia:
        query = query.filter(MetaAvaliacao.COMPETENCIA == competencia)

    # Executar a consulta
    results = query.order_by(MetaAvaliacao.COMPETENCIA).all()

    # Usar um dicionário para eliminar duplicatas com base numa chave única
    metas_dict = {}
    for meta, empresa in results:
        # Criar uma chave única para cada combinação edital-período-empresa-competência
        chave_unica = (meta.ID_EDITAL, meta.ID_PERIODO, meta.ID_EMPRESA, meta.COMPETENCIA)

        # Se a chave já existir, pular (evita duplicatas)
        if chave_unica in metas_dict:
            continue

        # Adicionar informações da empresa
        meta.empresa_nome = empresa.NO_EMPRESA
        meta.empresa_nome_abreviado = empresa.NO_EMPRESA_ABREVIADA

        # Adicionar ao dicionário
        metas_dict[chave_unica] = meta

    # Converter o dicionário de volta para lista
    metas = list(metas_dict.values())

    # Ordenar a lista de metas por competência
    metas.sort(key=lambda m: m.COMPETENCIA)

    # Obter dados para os filtros - todos os dados
    editais = Edital.query.filter(Edital.DELETED_AT == None).all()

    # Usar consultas específicas para obter apenas os dados disponíveis nas metas
    # Obter períodos únicos disponíveis
    periodos_query = db.session.query(PeriodoAvaliacao). \
        join(MetaAvaliacao, PeriodoAvaliacao.ID == MetaAvaliacao.ID_PERIODO). \
        filter(PeriodoAvaliacao.DELETED_AT == None, MetaAvaliacao.DELETED_AT == None). \
        distinct(PeriodoAvaliacao.ID)

    # Aplicar filtros para limitar os períodos conforme seleção de edital
    if edital_id:
        periodos_query = periodos_query.filter(MetaAvaliacao.ID_EDITAL == edital_id)

    periodos = periodos_query.all()

    # Obter todas as empresas para processamento
    todas_empresas = EmpresaParticipante.query.filter(EmpresaParticipante.DELETED_AT == None).all()

    # Criar um dicionário para eliminar duplicatas por nome abreviado
    empresas_unicas = {}
    for empresa in todas_empresas:
        nome_chave = empresa.NO_EMPRESA_ABREVIADA or empresa.NO_EMPRESA
        if nome_chave not in empresas_unicas:
            empresas_unicas[nome_chave] = empresa

    # Converter para lista ordenada
    empresas = sorted(empresas_unicas.values(), key=lambda e: e.NO_EMPRESA_ABREVIADA or e.NO_EMPRESA)

    # Gerar lista de competências únicas disponíveis
    competencias_query = db.session.query(MetaAvaliacao.COMPETENCIA). \
        filter(MetaAvaliacao.DELETED_AT == None). \
        distinct(MetaAvaliacao.COMPETENCIA)

    # Aplicar filtros para limitar as competências
    if edital_id:
        competencias_query = competencias_query.filter(MetaAvaliacao.ID_EDITAL == edital_id)
    if periodo_id:
        competencias_query = competencias_query.filter(MetaAvaliacao.ID_PERIODO == periodo_id)
    if empresa_id:
        competencias_query = competencias_query.join(EmpresaParticipante,
                                                     MetaAvaliacao.ID_EMPRESA == EmpresaParticipante.ID_EMPRESA). \
            filter(EmpresaParticipante.ID == empresa_id)

    competencias = [c[0] for c in competencias_query.all() if c[0]]
    competencias.sort()

    # Obter todos os dados (para uso com JavaScript)
    todos_editais = Edital.query.filter(Edital.DELETED_AT == None).all()
    todos_periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).all()

    # Obter todas as relações para construir a lógica dos filtros
    meta_relationships = db.session.query(
        MetaAvaliacao.ID_EDITAL,
        MetaAvaliacao.ID_PERIODO,
        MetaAvaliacao.ID_EMPRESA,
        MetaAvaliacao.COMPETENCIA,
        EmpresaParticipante.ID.label('empresa_id')
    ).join(
        EmpresaParticipante,
        MetaAvaliacao.ID_EMPRESA == EmpresaParticipante.ID_EMPRESA
    ).filter(
        MetaAvaliacao.DELETED_AT == None,
        EmpresaParticipante.DELETED_AT == None
    ).all()

    relationships = []
    for rel in meta_relationships:
        relationships.append({
            'edital_id': rel.ID_EDITAL,
            'periodo_id': rel.ID_PERIODO,
            'id_empresa': rel.ID_EMPRESA,
            'empresa_id': rel.empresa_id,
            'competencia': rel.COMPETENCIA
        })

    return render_template('credenciamento/lista_metas.html',
                           metas=metas,
                           editais=editais,
                           periodos=periodos,
                           empresas=empresas,
                           competencias=competencias,
                           filtro_edital_id=edital_id,
                           filtro_periodo_id=periodo_id,
                           filtro_empresa_id=empresa_id,
                           filtro_competencia=competencia,
                           todos_editais=todos_editais,
                           todos_periodos=todos_periodos,
                           todas_empresas=todas_empresas,
                           relationships=relationships)


@meta_bp.route('/metas/filtros')
@login_required
def metas_filtros():
    # Endpoint para fornecer dados de filtro via AJAX
    edital_id = request.args.get('edital_id', type=int)
    periodo_id = request.args.get('periodo_id', type=int)
    empresa_id = request.args.get('empresa_id', type=int)

    # Base query
    query = db.session.query(MetaAvaliacao).filter(MetaAvaliacao.DELETED_AT == None)

    # Aplicar filtros
    if edital_id:
        query = query.filter(MetaAvaliacao.ID_EDITAL == edital_id)
    if periodo_id:
        query = query.filter(MetaAvaliacao.ID_PERIODO == periodo_id)
    if empresa_id:
        query = query.join(EmpresaParticipante,
                           MetaAvaliacao.ID_EMPRESA == EmpresaParticipante.ID_EMPRESA). \
            filter(EmpresaParticipante.ID == empresa_id)

    # Obter valores únicos para cada filtro
    periodos_query = db.session.query(PeriodoAvaliacao). \
        join(MetaAvaliacao, PeriodoAvaliacao.ID == MetaAvaliacao.ID_PERIODO). \
        filter(MetaAvaliacao.ID.in_([m.ID for m in query])). \
        distinct(PeriodoAvaliacao.ID)

    # Obter todas as empresas para processamento
    todas_empresas_query = db.session.query(EmpresaParticipante). \
        join(MetaAvaliacao, EmpresaParticipante.ID_EMPRESA == MetaAvaliacao.ID_EMPRESA). \
        filter(MetaAvaliacao.ID.in_([m.ID for m in query]))

    # Eliminar duplicatas por nome abreviado
    empresas_unicas = {}
    for empresa in todas_empresas_query:
        nome_chave = empresa.NO_EMPRESA_ABREVIADA or empresa.NO_EMPRESA
        if nome_chave not in empresas_unicas:
            empresas_unicas[nome_chave] = empresa

    # Ordenar empresas
    empresas_lista = sorted(empresas_unicas.values(), key=lambda e: e.NO_EMPRESA_ABREVIADA or e.NO_EMPRESA)

    competencias_query = db.session.query(MetaAvaliacao.COMPETENCIA). \
        filter(MetaAvaliacao.ID.in_([m.ID for m in query])). \
        distinct(MetaAvaliacao.COMPETENCIA)

    # Construir resposta JSON
    periodos = [
        {'id': p.ID, 'nome': f"{p.ID_PERIODO} - {p.DT_INICIO.strftime('%d/%m/%Y')} a {p.DT_FIM.strftime('%d/%m/%Y')}"}
        for p in periodos_query]

    empresas = [{'id': e.ID,
                 'nome': e.NO_EMPRESA_ABREVIADA if e.NO_EMPRESA_ABREVIADA else e.NO_EMPRESA}
                for e in empresas_lista]

    competencias = [c[0] for c in competencias_query if c[0]]

    return jsonify({
        'periodos': periodos,
        'empresas': empresas,
        'competencias': competencias
    })


@meta_bp.route('/metas/nova', methods=['GET', 'POST'])
@login_required
def nova_meta():
    editais = Edital.query.filter(Edital.DELETED_AT == None).all()
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).all()
    empresas = EmpresaParticipante.query.filter(EmpresaParticipante.DELETED_AT == None).all()

    # Verificar se há editais e períodos cadastrados
    if not editais:
        flash('Não há editais cadastrados. Cadastre um edital primeiro.', 'warning')
        return redirect(url_for('edital.lista_editais'))

    if not periodos:
        flash('Não há períodos cadastrados. Cadastre um período primeiro.', 'warning')
        return redirect(url_for('periodo.lista_periodos'))

    if not empresas:
        flash('Não há empresas cadastradas. Cadastre uma empresa primeiro.', 'warning')
        return redirect(url_for('periodo.lista_periodos'))

    if request.method == 'POST':
        try:
            edital_id = int(request.form['edital_id'])
            periodo_id = int(request.form['periodo_id'])
            empresa_id = int(request.form['empresa_id'])
            competencia = request.form['competencia']

            # Valores opcionais
            meta_arrecadacao = request.form.get('meta_arrecadacao')
            if meta_arrecadacao:
                meta_arrecadacao = float(meta_arrecadacao)

            meta_acionamento = request.form.get('meta_acionamento')
            if meta_acionamento:
                meta_acionamento = float(meta_acionamento)

            meta_liquidacao = request.form.get('meta_liquidacao')
            if meta_liquidacao:
                meta_liquidacao = int(meta_liquidacao)

            meta_bonificacao = request.form.get('meta_bonificacao')
            if meta_bonificacao:
                meta_bonificacao = float(meta_bonificacao)

            # Buscar empresa correspondente para obter o ID_EMPRESA correto
            empresa = EmpresaParticipante.query.get_or_404(empresa_id)

            # Verificar se já existe meta para esta combinação
            meta_existente = MetaAvaliacao.query.filter_by(
                ID_EDITAL=edital_id,
                ID_PERIODO=periodo_id,
                ID_EMPRESA=empresa.ID_EMPRESA,
                COMPETENCIA=competencia,
                DELETED_AT=None
            ).first()

            if meta_existente:
                flash('Já existe uma meta cadastrada com estes critérios.', 'danger')
                return render_template('credenciamento/form_meta.html', editais=editais, periodos=periodos, empresas=empresas)

            nova_meta = MetaAvaliacao(
                ID_EDITAL=edital_id,
                ID_PERIODO=periodo_id,
                ID_EMPRESA=empresa.ID_EMPRESA,
                COMPETENCIA=competencia,
                META_ARRECADACAO=meta_arrecadacao,
                META_ACIONAMENTO=meta_acionamento,
                META_LIQUIDACAO=meta_liquidacao,
                META_BONIFICACAO=meta_bonificacao
            )

            db.session.add(nova_meta)
            db.session.commit()

            # Registrar log de auditoria
            dados_novos = {
                'id_edital': edital_id,
                'id_periodo': periodo_id,
                'id_empresa': empresa.ID_EMPRESA,
                'competencia': competencia,
                'meta_arrecadacao': meta_arrecadacao,
                'meta_acionamento': meta_acionamento,
                'meta_liquidacao': meta_liquidacao,
                'meta_bonificacao': meta_bonificacao
            }

            registrar_log(
                acao='criar',
                entidade='meta',
                entidade_id=nova_meta.ID,
                descricao=f'Cadastro de meta de avaliação para {competencia}',
                dados_novos=dados_novos
            )

            flash('Meta de avaliação cadastrada com sucesso!', 'success')
            return redirect(url_for('meta.lista_metas'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')

    return render_template('credenciamento/form_meta.html', editais=editais, periodos=periodos, empresas=empresas)


@meta_bp.route('/metas/editar/<int:id>', methods=['GET', 'POST'])
@login_required
def editar_meta(id):
    meta = MetaAvaliacao.query.get_or_404(id)
    editais = Edital.query.filter(Edital.DELETED_AT == None).all()
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).all()
    empresas = EmpresaParticipante.query.filter(EmpresaParticipante.DELETED_AT == None).all()

    if request.method == 'POST':
        try:
            # Capturar dados antigos para auditoria
            dados_antigos = {
                'id_edital': meta.ID_EDITAL,
                'id_periodo': meta.ID_PERIODO,
                'id_empresa': meta.ID_EMPRESA,
                'competencia': meta.COMPETENCIA,
                'meta_arrecadacao': meta.META_ARRECADACAO,
                'meta_acionamento': meta.META_ACIONAMENTO,
                'meta_liquidacao': meta.META_LIQUIDACAO,
                'meta_bonificacao': meta.META_BONIFICACAO
            }

            # Atualizar dados
            meta.ID_EDITAL = int(request.form['edital_id'])
            meta.ID_PERIODO = int(request.form['periodo_id'])

            # Buscar empresa correspondente para obter o ID_EMPRESA correto
            empresa_id = int(request.form['empresa_id'])
            empresa = EmpresaParticipante.query.get_or_404(empresa_id)
            meta.ID_EMPRESA = empresa.ID_EMPRESA

            meta.COMPETENCIA = request.form['competencia']

            # Valores opcionais
            meta_arrecadacao = request.form.get('meta_arrecadacao')
            if meta_arrecadacao:
                meta.META_ARRECADACAO = float(meta_arrecadacao)
            else:
                meta.META_ARRECADACAO = None

            meta_acionamento = request.form.get('meta_acionamento')
            if meta_acionamento:
                meta.META_ACIONAMENTO = float(meta_acionamento)
            else:
                meta.META_ACIONAMENTO = None

            meta_liquidacao = request.form.get('meta_liquidacao')
            if meta_liquidacao:
                meta.META_LIQUIDACAO = int(meta_liquidacao)
            else:
                meta.META_LIQUIDACAO = None

            meta_bonificacao = request.form.get('meta_bonificacao')
            if meta_bonificacao:
                meta.META_BONIFICACAO = float(meta_bonificacao)
            else:
                meta.META_BONIFICACAO = None

            db.session.commit()

            # Registrar log de auditoria
            dados_novos = {
                'id_edital': meta.ID_EDITAL,
                'id_periodo': meta.ID_PERIODO,
                'id_empresa': meta.ID_EMPRESA,
                'competencia': meta.COMPETENCIA,
                'meta_arrecadacao': meta.META_ARRECADACAO,
                'meta_acionamento': meta.META_ACIONAMENTO,
                'meta_liquidacao': meta.META_LIQUIDACAO,
                'meta_bonificacao': meta.META_BONIFICACAO
            }

            registrar_log(
                acao='editar',
                entidade='meta',
                entidade_id=meta.ID,
                descricao=f'Edição de meta de avaliação para {meta.COMPETENCIA}',
                dados_antigos=dados_antigos,
                dados_novos=dados_novos
            )

            flash('Meta de avaliação atualizada com sucesso!', 'success')
            return redirect(url_for('meta.lista_metas'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')

    return render_template('credenciamento/form_meta.html', meta=meta, editais=editais, periodos=periodos, empresas=empresas)


@meta_bp.route('/metas/excluir/<int:id>')
@login_required
def excluir_meta(id):
    try:
        meta = MetaAvaliacao.query.get_or_404(id)

        # Capturar dados para auditoria
        dados_antigos = {
            'id_edital': meta.ID_EDITAL,
            'id_periodo': meta.ID_PERIODO,
            'id_empresa': meta.ID_EMPRESA,
            'competencia': meta.COMPETENCIA,
            'meta_arrecadacao': meta.META_ARRECADACAO,
            'meta_acionamento': meta.META_ACIONAMENTO,
            'meta_liquidacao': meta.META_LIQUIDACAO,
            'meta_bonificacao': meta.META_BONIFICACAO,
            'deleted_at': None
        }

        meta.DELETED_AT = datetime.utcnow()
        db.session.commit()

        # Registrar log de auditoria
        dados_novos = {'deleted_at': meta.DELETED_AT.strftime('%Y-%m-%d %H:%M:%S')}

        registrar_log(
            acao='excluir',
            entidade='meta',
            entidade_id=meta.ID,
            descricao=f'Exclusão de meta de avaliação para {meta.COMPETENCIA}',
            dados_antigos=dados_antigos,
            dados_novos=dados_novos
        )

        flash('Meta de avaliação removida com sucesso!', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Erro: {str(e)}', 'danger')

    return redirect(url_for('meta.lista_metas'))