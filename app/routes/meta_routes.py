from flask import Blueprint, render_template, request, jsonify
from app.models.meta_avaliacao import MetaAvaliacao
from app.models.edital import Edital
from app.models.periodo import PeriodoAvaliacao
from app.models.empresa_participante import EmpresaParticipante
from datetime import datetime
from flask_login import login_required
from app import db

meta_bp = Blueprint('meta', __name__)


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

    # Consulta base - usar join para garantir que temos acesso à empresa
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

    # Processar resultados para incluir dados da empresa
    metas = []
    for meta, empresa in results:
        meta.empresa_nome = empresa.NO_EMPRESA
        meta.empresa_nome_abreviado = empresa.NO_EMPRESA_ABREVIADA
        metas.append(meta)

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

    return render_template('lista_metas.html',
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