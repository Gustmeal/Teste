from flask import Blueprint, render_template, redirect, url_for, request, flash
from app.models.limite_distribuicao import LimiteDistribuicao
from app.models.edital import Edital
from app.models.periodo import PeriodoAvaliacao
from app.models.empresa_participante import EmpresaParticipante
from app.models.criterio_selecao import CriterioSelecao
from app import db
from datetime import datetime
from flask_login import login_required
from app.utils.audit import registrar_log
from sqlalchemy import or_

limite_bp = Blueprint('limite', __name__, url_prefix='/credenciamento')


@limite_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@limite_bp.route('/limites')
@login_required
def lista_limites():
    # Obter parâmetros de filtro
    periodo_id = request.args.get('periodo_id', type=int)
    criterio_id = request.args.get('criterio_id', type=int)

    # Consulta base com join para obter a descrição do critério
    query = db.session.query(
        LimiteDistribuicao,
        CriterioSelecao.DS_CRITERIO_SELECAO
    ).outerjoin(
        CriterioSelecao,
        LimiteDistribuicao.COD_CRITERIO_SELECAO == CriterioSelecao.COD
    ).filter(
        LimiteDistribuicao.DELETED_AT == None,
        or_(CriterioSelecao.DELETED_AT == None, CriterioSelecao.DELETED_AT.is_(None))
    )

    # Aplicar filtros se fornecidos
    if periodo_id:
        query = query.filter(LimiteDistribuicao.ID_PERIODO == periodo_id)

    if criterio_id:
        query = query.filter(LimiteDistribuicao.COD_CRITERIO_SELECAO == criterio_id)

    # Executar consulta
    results = query.all()

    # Processar resultados para incluir dados da empresa e descrição do critério
    limites = []
    for limite, ds_criterio in results:
        # Buscar informações da empresa
        empresa = EmpresaParticipante.query.filter_by(ID_EMPRESA=limite.ID_EMPRESA, DELETED_AT=None).first()
        if empresa:
            limite.empresa_nome = empresa.NO_EMPRESA
            limite.empresa_nome_abreviado = empresa.NO_EMPRESA_ABREVIADA

        # Adicionar descrição do critério
        limite.criterio_descricao = ds_criterio if ds_criterio else f"Critério {limite.COD_CRITERIO_SELECAO}"

        limites.append(limite)

    # Obter todos os períodos para o filtro
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).all()

    # Obter todos os critérios para o filtro
    criterios = CriterioSelecao.query.filter(CriterioSelecao.DELETED_AT == None).all()

    return render_template(
        'credenciamento/lista_limites.html',
        limites=limites,
        periodos=periodos,
        criterios=criterios,
        filtro_periodo_id=periodo_id,
        filtro_criterio_id=criterio_id
    )


@limite_bp.route('/limites/novo', methods=['GET', 'POST'])
@login_required
def novo_limite():
    editais = Edital.query.filter(Edital.DELETED_AT == None).all()
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).all()
    empresas = EmpresaParticipante.query.filter(EmpresaParticipante.DELETED_AT == None).all()
    criterios = CriterioSelecao.query.filter(CriterioSelecao.DELETED_AT == None).all()

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
            cod_criterio = int(request.form['cod_criterio'])

            # Valores opcionais
            qtde_maxima = request.form.get('qtde_maxima')
            if qtde_maxima:
                qtde_maxima = int(qtde_maxima)

            valor_maximo = request.form.get('valor_maximo')
            if valor_maximo:
                valor_maximo = float(valor_maximo)

            percentual_final = request.form.get('percentual_final')
            if percentual_final:
                percentual_final = float(percentual_final)

            # Verificar se já existe limite para esta combinação
            limite_existente = LimiteDistribuicao.query.filter_by(
                ID_EDITAL=edital_id,
                ID_PERIODO=periodo_id,
                ID_EMPRESA=empresa_id,
                COD_CRITERIO_SELECAO=cod_criterio,
                DELETED_AT=None
            ).first()

            if limite_existente:
                flash('Já existe um limite cadastrado com estes critérios.', 'danger')
                return render_template('credenciamento/form_limite.html', editais=editais, periodos=periodos,
                                       empresas=empresas,
                                       criterios=criterios)

            novo_limite = LimiteDistribuicao(
                ID_EDITAL=edital_id,
                ID_PERIODO=periodo_id,
                ID_EMPRESA=empresa_id,
                COD_CRITERIO_SELECAO=cod_criterio,
                QTDE_MAXIMA=qtde_maxima,
                VALOR_MAXIMO=valor_maximo,
                PERCENTUAL_FINAL=percentual_final
            )

            db.session.add(novo_limite)
            db.session.commit()

            # Registrar log de auditoria
            dados_novos = {
                'id_edital': edital_id,
                'id_periodo': periodo_id,
                'id_empresa': empresa_id,
                'cod_criterio': cod_criterio,
                'qtde_maxima': qtde_maxima,
                'valor_maximo': valor_maximo,
                'percentual_final': percentual_final
            }

            registrar_log(
                acao='criar',
                entidade='limite',
                entidade_id=novo_limite.ID,
                descricao=f'Cadastro de limite de distribuição',
                dados_novos=dados_novos
            )

            flash('Limite de distribuição cadastrado com sucesso!', 'success')
            return redirect(url_for('limite.lista_limites'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')

    return render_template('credenciamento/form_limite.html', editais=editais, periodos=periodos, empresas=empresas,
                           criterios=criterios)


@limite_bp.route('/limites/editar/<int:id>', methods=['GET', 'POST'])
@login_required
def editar_limite(id):
    limite = LimiteDistribuicao.query.get_or_404(id)
    editais = Edital.query.filter(Edital.DELETED_AT == None).all()
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).all()
    empresas = EmpresaParticipante.query.filter(EmpresaParticipante.DELETED_AT == None).all()
    criterios = CriterioSelecao.query.filter(CriterioSelecao.DELETED_AT == None).all()

    if request.method == 'POST':
        try:
            # Capturar dados antigos para auditoria
            dados_antigos = {
                'id_edital': limite.ID_EDITAL,
                'id_periodo': limite.ID_PERIODO,
                'id_empresa': limite.ID_EMPRESA,
                'cod_criterio': limite.COD_CRITERIO_SELECAO,
                'qtde_maxima': limite.QTDE_MAXIMA,
                'valor_maximo': limite.VALOR_MAXIMO,
                'percentual_final': limite.PERCENTUAL_FINAL
            }

            # Atualizar dados
            limite.ID_EDITAL = int(request.form['edital_id'])
            limite.ID_PERIODO = int(request.form['periodo_id'])
            limite.ID_EMPRESA = int(request.form['empresa_id'])
            limite.COD_CRITERIO_SELECAO = int(request.form['cod_criterio'])

            # Valores opcionais
            qtde_maxima = request.form.get('qtde_maxima')
            if qtde_maxima:
                limite.QTDE_MAXIMA = int(qtde_maxima)
            else:
                limite.QTDE_MAXIMA = None

            valor_maximo = request.form.get('valor_maximo')
            if valor_maximo:
                limite.VALOR_MAXIMO = float(valor_maximo)
            else:
                limite.VALOR_MAXIMO = None

            percentual_final = request.form.get('percentual_final')
            if percentual_final:
                limite.PERCENTUAL_FINAL = float(percentual_final)
            else:
                limite.PERCENTUAL_FINAL = None

            limite.UPDATED_AT = datetime.utcnow()

            # Verificar se já existe limite para esta combinação (excluindo o próprio registro)
            limite_existente = LimiteDistribuicao.query.filter(
                LimiteDistribuicao.ID_EDITAL == limite.ID_EDITAL,
                LimiteDistribuicao.ID_PERIODO == limite.ID_PERIODO,
                LimiteDistribuicao.ID_EMPRESA == limite.ID_EMPRESA,
                LimiteDistribuicao.COD_CRITERIO_SELECAO == limite.COD_CRITERIO_SELECAO,
                LimiteDistribuicao.DELETED_AT == None,
                LimiteDistribuicao.ID != id
            ).first()

            if limite_existente:
                flash('Já existe um limite cadastrado com estes critérios.', 'danger')
                return render_template(
                    'credenciamento/form_limite.html',
                    limite=limite,
                    editais=editais,
                    periodos=periodos,
                    empresas=empresas,
                    criterios=criterios
                )

            # Dados novos para auditoria
            dados_novos = {
                'id_edital': limite.ID_EDITAL,
                'id_periodo': limite.ID_PERIODO,
                'id_empresa': limite.ID_EMPRESA,
                'cod_criterio': limite.COD_CRITERIO_SELECAO,
                'qtde_maxima': limite.QTDE_MAXIMA,
                'valor_maximo': limite.VALOR_MAXIMO,
                'percentual_final': limite.PERCENTUAL_FINAL
            }

            db.session.commit()

            # Registrar log de auditoria
            registrar_log(
                acao='editar',
                entidade='limite',
                entidade_id=limite.ID,
                descricao=f'Atualização de limite de distribuição',
                dados_antigos=dados_antigos,
                dados_novos=dados_novos
            )

            flash('Limite de distribuição atualizado com sucesso!', 'success')
            return redirect(url_for('limite.lista_limites'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')

    return render_template(
        'credenciamento/form_limite.html',
        limite=limite,
        editais=editais,
        periodos=periodos,
        empresas=empresas,
        criterios=criterios
    )


@limite_bp.route('/limites/excluir/<int:id>', methods=['POST'])
@login_required
def excluir_limite(id):
    limite = LimiteDistribuicao.query.get_or_404(id)

    try:
        # Capturar dados antigos para auditoria
        dados_antigos = {
            'id_edital': limite.ID_EDITAL,
            'id_periodo': limite.ID_PERIODO,
            'id_empresa': limite.ID_EMPRESA,
            'cod_criterio': limite.COD_CRITERIO_SELECAO,
            'qtde_maxima': limite.QTDE_MAXIMA,
            'valor_maximo': limite.VALOR_MAXIMO,
            'percentual_final': limite.PERCENTUAL_FINAL
        }

        # Soft delete - apenas marca como excluído
        limite.DELETED_AT = datetime.utcnow()
        db.session.commit()

        # Registrar log de auditoria
        registrar_log(
            acao='excluir',
            entidade='limite',
            entidade_id=limite.ID,
            descricao=f'Exclusão de limite de distribuição',
            dados_antigos=dados_antigos
        )

        flash('Limite de distribuição excluído com sucesso!', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao excluir limite de distribuição: {str(e)}', 'danger')

    return redirect(url_for('limite.lista_limites'))


@limite_bp.route('/limites/filtrar', methods=['GET'])
@login_required
def filtrar_limites():
    periodo_id = request.args.get('periodo_id', type=int)
    criterio_id = request.args.get('criterio_id', type=int)

    # Redirecionar para a lista com os parâmetros de filtro
    return redirect(url_for('limite.lista_limites', periodo_id=periodo_id, criterio_id=criterio_id))


@limite_bp.route('/limites/detalhe/<int:id>')
@login_required
def detalhe_limite(id):
    # Consulta para obter o limite e a descrição do critério
    resultado = db.session.query(
        LimiteDistribuicao,
        CriterioSelecao.DS_CRITERIO_SELECAO,
        Edital.NU_EDITAL,
        Edital.NU_ANO,
        PeriodoAvaliacao.DT_INICIO,
        PeriodoAvaliacao.DT_FIM,
        EmpresaParticipante.NO_EMPRESA
    ).outerjoin(
        CriterioSelecao,
        LimiteDistribuicao.COD_CRITERIO_SELECAO == CriterioSelecao.COD
    ).outerjoin(
        Edital,
        LimiteDistribuicao.ID_EDITAL == Edital.ID
    ).outerjoin(
        PeriodoAvaliacao,
        LimiteDistribuicao.ID_PERIODO == PeriodoAvaliacao.ID
    ).outerjoin(
        EmpresaParticipante,
        LimiteDistribuicao.ID_EMPRESA == EmpresaParticipante.ID_EMPRESA
    ).filter(
        LimiteDistribuicao.ID == id,
        LimiteDistribuicao.DELETED_AT == None
    ).first_or_404()

    limite, ds_criterio, nu_edital, nu_ano, dt_inicio, dt_fim, no_empresa = resultado

    return render_template(
        'credenciamento/detalhe_limite.html',
        limite=limite,
        ds_criterio=ds_criterio,
        nu_edital=nu_edital,
        nu_ano=nu_ano,
        dt_inicio=dt_inicio,
        dt_fim=dt_fim,
        no_empresa=no_empresa
    )