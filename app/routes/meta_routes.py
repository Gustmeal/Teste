# app/routes/meta_routes.py
from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash, session
from app.models.metas_redistribuicao import MetasPercentuaisDistribuicao, Metas, MetasPeriodoAvaliativo
from app.models.meta_avaliacao import MetaAvaliacao, MetaSemestral
from app.models.edital import Edital
from app.models.periodo import PeriodoAvaliacao
from app.models.empresa_participante import EmpresaParticipante
from app.utils.meta_calculator import MetaCalculator
from datetime import datetime
from flask_login import login_required, current_user
from app import db
from app.utils.audit import registrar_log
from sqlalchemy import text
from decimal import Decimal

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

    # Montar query base - usando os nomes corretos das colunas
    query = """
        SELECT 
            m.ID,
            m.ID_EDITAL,
            m.ID_PERIODO,
            m.ID_EMPRESA,
            m.COMPETENCIA,
            m.META_ARRECADACAO,
            m.META_ACIONAMENTO,
            m.META_LIQUIDACAO,
            m.META_BONIFICACAO,
            e.NO_EMPRESA,
            e.NO_EMPRESA_ABREVIADA,
            ed.DESCRICAO,
            ed.NU_EDITAL,
            ed.ANO,
            p.ID_PERIODO as NUM_PERIODO,
            p.DT_INICIO,
            p.DT_FIM
        FROM DEV.DCA_TB009_META_AVALIACAO m
        LEFT JOIN DEV.DCA_TB002_EMPRESAS_PARTICIPANTES e 
            ON m.ID_EMPRESA = e.ID_EMPRESA 
            AND m.ID_EDITAL = e.ID_EDITAL 
            AND m.ID_PERIODO = e.ID_PERIODO
        LEFT JOIN DEV.DCA_TB008_EDITAIS ed ON m.ID_EDITAL = ed.ID
        LEFT JOIN DEV.DCA_TB001_PERIODO_AVALIACAO p ON m.ID_PERIODO = p.ID
        WHERE m.DELETED_AT IS NULL
    """

    params = {}

    # Aplicar filtros
    if edital_id:
        query += " AND m.ID_EDITAL = :edital_id"
        params['edital_id'] = edital_id
    if periodo_id:
        query += " AND m.ID_PERIODO = :periodo_id"
        params['periodo_id'] = periodo_id
    if empresa_id:
        query += " AND m.ID_EMPRESA = :empresa_id"
        params['empresa_id'] = empresa_id
    if competencia:
        query += " AND m.COMPETENCIA = :competencia"
        params['competencia'] = competencia

    # Ordenar por maior edital e período primeiro
    query += " ORDER BY m.ID_EDITAL DESC, p.ID_PERIODO DESC, m.COMPETENCIA, e.NO_EMPRESA_ABREVIADA"

    result = db.session.execute(text(query), params)

    metas = []
    for row in result:
        meta = {
            'ID': row[0],
            'ID_EDITAL': row[1],
            'ID_PERIODO': row[2],
            'ID_EMPRESA': row[3],
            'COMPETENCIA': row[4],
            'META_ARRECADACAO': float(row[5]) if row[5] else 0,
            'META_ACIONAMENTO': float(row[6]) if row[6] else None,
            'META_LIQUIDACAO': float(row[7]) if row[7] else None,
            'META_BONIFICACAO': float(row[8]) if row[8] else 0,
            'NO_EMPRESA': row[9],
            'NO_EMPRESA_ABREVIADA': row[10],
            'DESCRICAO_EDITAL': row[11],
            'NU_EDITAL': row[12],
            'ANO_EDITAL': row[13],
            'NUM_PERIODO': row[14],
            'DT_INICIO': row[15],
            'DT_FIM': row[16]
        }
        metas.append(meta)

    # Buscar dados para os filtros - ordenar por maior ID
    editais = Edital.query.filter(Edital.DELETED_AT == None).order_by(Edital.ID.desc()).all()
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).order_by(
        PeriodoAvaliacao.ID_EDITAL.desc(),
        PeriodoAvaliacao.ID_PERIODO.desc()
    ).all()

    # Buscar empresas distintas
    sql_empresas = text("""
        SELECT DISTINCT ID_EMPRESA, NO_EMPRESA, NO_EMPRESA_ABREVIADA
        FROM DEV.DCA_TB002_EMPRESAS_PARTICIPANTES
        WHERE DELETED_AT IS NULL
        ORDER BY NO_EMPRESA_ABREVIADA
    """)
    result_empresas = db.session.execute(sql_empresas)
    empresas = []
    for row in result_empresas:
        empresas.append({
            'ID': row[0],
            'NO_EMPRESA': row[1],
            'NO_EMPRESA_ABREVIADA': row[2]
        })

    # Buscar competências distintas
    sql_comp = text("""
        SELECT DISTINCT COMPETENCIA 
        FROM DEV.DCA_TB009_META_AVALIACAO 
        WHERE DELETED_AT IS NULL 
        ORDER BY COMPETENCIA DESC
    """)
    competencias = [row[0] for row in db.session.execute(sql_comp)]

    return render_template('credenciamento/lista_metas.html',
                           metas=metas,
                           editais=editais,
                           periodos=periodos,
                           empresas=empresas,
                           competencias=competencias,
                           filtro_edital_id=edital_id,
                           filtro_periodo_id=periodo_id,
                           filtro_empresa_id=empresa_id,
                           filtro_competencia=competencia)


@meta_bp.route('/metas/visualizar-redistribuicao')
@login_required
def visualizar_redistribuicao():
    """Página para visualizar cálculo de redistribuição de metas"""
    editais = Edital.query.filter(Edital.DELETED_AT == None).order_by(Edital.ID.desc()).all()
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).order_by(
        PeriodoAvaliacao.ID_EDITAL.desc(),
        PeriodoAvaliacao.ID_PERIODO.desc()
    ).all()

    return render_template('credenciamento/visualizar_redistribuicao.html',
                           editais=editais,
                           periodos=periodos)


@meta_bp.route('/metas/visualizar-calculo')
@login_required
def visualizar_calculo():
    """API para retornar dados do cálculo de redistribuição"""
    try:
        edital_id = request.args.get('edital_id', type=int)
        periodo_id = request.args.get('periodo_id', type=int)

        if not edital_id or not periodo_id:
            return jsonify({'sucesso': False, 'erro': 'Parâmetros inválidos'})

        from app.utils.visualizador_redistribuicao import VisualizadorRedistribuicao

        visualizador = VisualizadorRedistribuicao(edital_id, periodo_id)
        dados = visualizador.calcular_redistribuicao_completa()

        return jsonify({
            'sucesso': True,
            'dados': dados
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'sucesso': False, 'erro': str(e)})


@meta_bp.route('/metas/redistribuicao')
@login_required
def redistribuicao_metas():
    """Página para redistribuição de metas"""
    editais = Edital.query.filter(Edital.DELETED_AT == None).order_by(Edital.ID.desc()).all()
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).order_by(
        PeriodoAvaliacao.ID_EDITAL.desc(),
        PeriodoAvaliacao.ID_PERIODO.desc()
    ).all()

    return render_template('credenciamento/form_redistribuicao.html',
                           editais=editais,
                           periodos=periodos)


@meta_bp.route('/metas/buscar-empresas-ativas')
@login_required
def buscar_empresas_ativas():
    """
    Busca empresas que podem ser descredenciadas.
    A fonte de dados agora é a TB018, para garantir consistência.
    """
    try:
        edital_id = request.args.get('edital_id', type=int)
        periodo_pk_id = request.args.get('periodo_id', type=int)

        if not edital_id or not periodo_pk_id:
            return jsonify({'sucesso': False, 'erro': 'Edital e Período são obrigatórios.'})

        # Buscar a chave de negócio do período
        periodo = PeriodoAvaliacao.query.get(periodo_pk_id)
        if not periodo:
            return jsonify({'sucesso': False, 'erro': f'Período com ID {periodo_pk_id} não encontrado.'})
        periodo_business_id = periodo.ID_PERIODO

        # CORREÇÃO: A query agora busca da TB018, a mesma fonte do cálculo principal.
        sql = text("""
            SELECT DISTINCT
                m.ID_EMPRESA,
                m.NO_EMPRESA_ABREVIADA,
                m.PERCENTUAL_SALDO_DEVEDOR
            FROM DEV.DCA_TB018_METAS_REDISTRIBUIDAS_MENSAL m
            WHERE m.ID_EDITAL = :edital_id
            AND m.ID_PERIODO = :periodo_id
            AND m.DELETED_AT IS NULL
            AND m.PERCENTUAL_SALDO_DEVEDOR > 0  -- Apenas empresas com percentual a ser redistribuído
            AND m.DT_REFERENCIA = (
                SELECT MAX(DT_REFERENCIA)
                FROM DEV.DCA_TB018_METAS_REDISTRIBUIDAS_MENSAL
                WHERE ID_EDITAL = :edital_id
                AND ID_PERIODO = :periodo_id
                AND DELETED_AT IS NULL
            )
            ORDER BY m.NO_EMPRESA_ABREVIADA
        """)

        result = db.session.execute(sql, {
            'edital_id': edital_id,
            'periodo_id': periodo_business_id
        }).fetchall()

        if not result:
            return jsonify({'sucesso': False, 'erro': 'Nenhuma empresa ativa encontrada na última distribuição salva.'})

        empresas = []
        for row in result:
            empresas.append({
                'id_empresa': row.ID_EMPRESA,
                'nome_empresa': row.NO_EMPRESA_ABREVIADA,
                'percentual': float(row.PERCENTUAL_SALDO_DEVEDOR)
            })

        return jsonify({'sucesso': True, 'empresas': empresas})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'sucesso': False, 'erro': str(e)})


@meta_bp.route('/metas/calcular-nova-redistribuicao', methods=['POST'])
@login_required
def calcular_nova_redistribuicao():
    """Calcula uma nova redistribuição de metas"""
    try:
        data = request.json
        from app.utils.redistribuicao_dinamica import RedistribuicaoDinamica

        redistribuidor = RedistribuicaoDinamica(
            edital_id=data['edital_id'],
            periodo_id=data['periodo_id'],
            empresa_saindo_id=data['empresa_id'],
            data_descredenciamento=data['data_descredenciamento'],
            incremento_meta=data.get('incremento_meta', 1.0)
        )
        resultado = redistribuidor.calcular_redistribuicao()
        return jsonify(resultado)

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'erro': str(e)})


@meta_bp.route('/metas/salvar-redistribuicao', methods=['POST'])
@login_required
def salvar_redistribuicao():
    """Salva a redistribuição calculada"""
    try:
        data = request.json
        from app.utils.redistribuicao_dinamica import RedistribuicaoDinamica

        redistribuidor = RedistribuicaoDinamica(
            edital_id=data['edital_id'],
            periodo_id=data['periodo_id'],
            empresa_saindo_id=data['empresa_id'],
            data_descredenciamento=data['data_descredenciamento'],
            incremento_meta=data.get('incremento_meta', 1.0)
        )
        # O cálculo é feito novamente dentro do método salvar para garantir consistência
        sucesso = redistribuidor.salvar_redistribuicao()

        if sucesso:
            registrar_log(
                acao='redistribuicao_metas', entidade='meta',
                entidade_id=f"{data['edital_id']}-{data['periodo_id']}",
                descricao=f"Redistribuição de metas - Empresa {data['empresa_id']} descredenciada em {data['data_descredenciamento']}",
                dados_novos=data
            )
            return jsonify({'sucesso': True, 'mensagem': 'Redistribuição salva com sucesso!'})
        else:
            return jsonify({'sucesso': False, 'erro': 'Erro desconhecido ao salvar redistribuição'})

    except Exception as e:
        db.session.rollback()
        import traceback
        traceback.print_exc()
        return jsonify({'sucesso': False, 'erro': str(e)})


@meta_bp.route('/metas/nova', methods=['GET', 'POST'])
@login_required
def nova_meta():
    """Página para cálculo automático de metas"""
    # Ordenar por maior edital e período
    editais = Edital.query.filter(Edital.DELETED_AT == None).order_by(Edital.ID.desc()).all()
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).order_by(
        PeriodoAvaliacao.ID_EDITAL.desc(),
        PeriodoAvaliacao.ID_PERIODO.desc()
    ).all()

    return render_template('credenciamento/form_meta.html',
                           editais=editais,
                           periodos=periodos)


# Localizar a função calcular_metas e garantir que ela pegue o fator_incremento do formulário
@meta_bp.route('/metas/calcular', methods=['POST'])
@login_required
def calcular_metas():
    """Calcula metas usando a classe MetaCalculator"""
    try:
        edital_id = int(request.form['edital_id'])
        periodo_id = int(request.form['periodo_id'])
        fator_incremento = float(request.form.get('fator_incremento', 1.00))

        # Usar MetaCalculator com fator_incremento
        calculator = MetaCalculator(edital_id, periodo_id, fator_incremento)
        metas_calculadas = calculator.calcular_metas_completas()

        # Obter período para retornar as datas
        periodo = PeriodoAvaliacao.query.get(periodo_id)

        return jsonify({
            'sucesso': True,
            'metas': metas_calculadas,
            'periodo': {
                'inicio': periodo.DT_INICIO.strftime('%d/%m/%Y'),
                'fim': periodo.DT_FIM.strftime('%d/%m/%Y')
            }
        })

    except ValueError as e:
        return jsonify({'erro': str(e)}), 404
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'erro': str(e)}), 500


@meta_bp.route('/metas/salvar-calculadas', methods=['POST'])
@login_required
def salvar_metas_calculadas():
    """Salva as metas calculadas após confirmação do usuário"""
    try:
        data = request.json
        metas_data = data['metas']
        edital_id = int(data['edital_id'])
        periodo_id = int(data['periodo_id'])

        # Criar calculador e salvar
        calculator = MetaCalculator(edital_id, periodo_id)
        calculator.salvar_metas(metas_data)

        # Registrar log
        registrar_log(
            acao='calcular_metas',
            entidade='meta',
            entidade_id=f"{edital_id}-{periodo_id}",
            descricao=f'Cálculo e salvamento de metas para edital {edital_id} período {periodo_id}',
            dados_novos={'edital_id': edital_id, 'periodo_id': periodo_id}
        )

        return jsonify({
            'sucesso': True,
            'mensagem': 'Metas salvas com sucesso!'
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'erro': str(e)}), 500


@meta_bp.route('/metas/excluir/<int:id>')
@login_required
def excluir_meta(id):
    try:
        meta = MetaAvaliacao.query.get_or_404(id)
        meta.DELETED_AT = datetime.utcnow()
        db.session.commit()

        registrar_log(
            acao='excluir',
            entidade='meta',
            entidade_id=meta.ID,
            descricao=f'Exclusão de meta de avaliação para {meta.COMPETENCIA}',
            dados_antigos={'deleted_at': None},
            dados_novos={'deleted_at': meta.DELETED_AT.strftime('%Y-%m-%d %H:%M:%S')}
        )

        flash('Meta de avaliação removida com sucesso!', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao excluir meta: {str(e)}', 'danger')

    return redirect(url_for('meta.lista_metas'))


@meta_bp.route('/metas/buscar-empresas-redistribuicao')
@login_required
def buscar_empresas_redistribuicao():
    """Busca empresas disponíveis para redistribuição agrupadas por data"""
    try:
        edital_id = request.args.get('edital_id', type=int)
        periodo_id = request.args.get('periodo_id', type=int)

        # Buscar todas as distribuições do período
        sql = text("""
            SELECT DISTINCT
                mpd.DT_REFERENCIA,
                mpd.ID_EMPRESA,
                mpd.NO_EMPRESA_ABREVIADA,
                mpd.VR_SALDO_DEVEDOR_DISTRIBUIDO,
                mpd.PERCENTUAL_SALDO_DEVEDOR
            FROM DEV.DCA_TB018_METAS_REDISTRIBUIDAS_MENSAL mpd
            WHERE mpd.ID_EDITAL = :edital_id
            AND mpd.ID_PERIODO = :periodo_id
            AND mpd.DELETED_AT IS NULL
            ORDER BY mpd.DT_REFERENCIA, mpd.NO_EMPRESA_ABREVIADA
        """)

        periodo = PeriodoAvaliacao.query.get(periodo_id)

        result = db.session.execute(sql, {
            'edital_id': edital_id,
            'periodo_id': periodo.ID_PERIODO
        })

        # Agrupar por data de referência
        empresas_por_data = {}

        for row in result:
            dt_ref = row.DT_REFERENCIA.strftime('%Y-%m-%d')

            if dt_ref not in empresas_por_data:
                empresas_por_data[dt_ref] = []

            empresas_por_data[dt_ref].append({
                'id_empresa': row.ID_EMPRESA,
                'nome_empresa': row.NO_EMPRESA_ABREVIADA,
                'saldo_devedor': float(row.VR_SALDO_DEVEDOR_DISTRIBUIDO) if row.VR_SALDO_DEVEDOR_DISTRIBUIDO else 0,
                'percentual': float(row.PERCENTUAL_SALDO_DEVEDOR) if row.PERCENTUAL_SALDO_DEVEDOR else 0
            })

        return jsonify({
            'sucesso': True,
            'empresas_por_data': empresas_por_data
        })

    except Exception as e:
        return jsonify({'sucesso': False, 'erro': str(e)})


@meta_bp.route('/metas/calcular-redistribuicao', methods=['POST'])
@login_required
def calcular_redistribuicao():
    """Calcula preview da redistribuição de metas"""
    try:
        data = request.json

        from app.utils.redistribuicao_dinamica import RedistribuicaoDinamica

        redistribuidor = RedistribuicaoDinamica(
            edital_id=int(data['edital_id']),
            periodo_id=int(data['periodo_id']),
            empresa_saindo_id=int(data['empresa_id']),
            data_descredenciamento=data['data_descredenciamento'],
            incremento_meta=float(data['incremento_meta'])
        )

        resultado = redistribuidor.calcular_redistribuicao()

        return jsonify(resultado)

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'erro': str(e)}), 500


@meta_bp.route('/metas/nova-distribuicao')
@login_required
def nova_meta_distribuicao():
    """Página para criar nova distribuição inicial de metas"""
    editais = Edital.query.filter(Edital.DELETED_AT == None).order_by(Edital.ID.desc()).all()
    # Não vamos mais passar os períodos aqui, eles serão carregados dinamicamente
    return render_template('credenciamento/form_nova_meta.html',
                           editais=editais)


@meta_bp.route('/metas/calcular-distribuicao-inicial', methods=['POST'])
@login_required
def calcular_distribuicao_inicial():
    """Calcula a distribuição inicial de metas"""
    try:
        data = request.json
        edital_id = int(data['edital_id'])
        periodo_id = int(data['periodo_id'])

        from app.utils.distribuicao_inicial import DistribuicaoInicial

        distribuidor = DistribuicaoInicial(edital_id, periodo_id)
        resultado = distribuidor.calcular_distribuicao()

        # Guardar na sessão para posterior salvamento
        session['distribuicao_calculada'] = resultado
        session['distribuicao_edital'] = edital_id
        session['distribuicao_periodo'] = periodo_id

        return jsonify({
            'sucesso': True,
            'resultado': resultado
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'sucesso': False, 'erro': str(e)})


@meta_bp.route('/metas/salvar-distribuicao-inicial', methods=['POST'])
@login_required
def salvar_distribuicao_inicial():
    """Salva a distribuição inicial calculada"""
    try:
        data = request.json
        edital_id = int(data['edital_id'])
        periodo_id = int(data['periodo_id'])

        # Verificar se os dados de sessão existem e correspondem à requisição
        if (session.get('distribuicao_edital') != edital_id or
                session.get('distribuicao_periodo') != periodo_id or
                'distribuicao_calculada' not in session):
            return jsonify({
                'sucesso': False,
                'erro': 'Dados de cálculo não encontrados na sessão. Por favor, calcule a distribuição novamente.'
            })

        # Verificar se já existe distribuição para este período
        periodo = PeriodoAvaliacao.query.get(periodo_id)
        sql_check = text("""
            SELECT COUNT(*) as total
            FROM DEV.DCA_TB018_METAS_REDISTRIBUIDAS_MENSAL
            WHERE ID_EDITAL = :edital_id
            AND ID_PERIODO = :periodo_id
            AND DELETED_AT IS NULL
        """)

        result = db.session.execute(sql_check, {
            'edital_id': edital_id,
            'periodo_id': periodo.ID_PERIODO
        }).fetchone()

        if result and result.total > 0:
            return jsonify({
                'sucesso': False,
                'erro': 'Já existe uma distribuição de metas para este período!'
            })

        # Buscar dados calculados da sessão
        dados_calculados = session.get('distribuicao_calculada')

        from app.utils.distribuicao_inicial import DistribuicaoInicial
        distribuidor = DistribuicaoInicial(edital_id, periodo_id)

        # Salvar distribuição
        if distribuidor.salvar_distribuicao(dados_calculados):
            # Limpar dados da sessão após o salvamento
            session.pop('distribuicao_calculada', None)
            session.pop('distribuicao_edital', None)
            session.pop('distribuicao_periodo', None)

            # Registrar log
            registrar_log(
                acao='criar_distribuicao_inicial',
                entidade='meta',
                entidade_id=f"{edital_id}-{periodo.ID_PERIODO}",
                descricao=f'Criação de distribuição inicial de metas para edital {edital_id} período {periodo.ID_PERIODO}',
                dados_novos={'edital_id': edital_id, 'periodo_id': periodo_id}
            )

            return jsonify({
                'sucesso': True,
                'mensagem': 'Distribuição salva com sucesso!'
            })
        else:
            return jsonify({
                'sucesso': False,
                'erro': 'Erro ao salvar distribuição'
            })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'sucesso': False, 'erro': str(e)})


# NOVA ROTA PARA BUSCAR PERÍODOS FILTRADOS
@meta_bp.route('/metas/buscar-periodos-por-edital')
@login_required
def buscar_periodos_por_edital():
    """Busca períodos de avaliação filtrados por edital."""
    try:
        edital_id = request.args.get('edital_id', type=int)
        if not edital_id:
            return jsonify([])

        # Query para buscar apenas os períodos do edital selecionado
        periodos = PeriodoAvaliacao.query.filter(
            PeriodoAvaliacao.ID_EDITAL == edital_id,
            PeriodoAvaliacao.DELETED_AT == None
        ).order_by(PeriodoAvaliacao.ID_PERIODO.desc()).all()

        # Formata os dados para o frontend
        periodos_json = [
            {
                'id': p.ID,
                'texto': f"Período {p.ID_PERIODO} - {p.DT_INICIO.strftime('%d/%m/%Y')} a {p.DT_FIM.strftime('%d/%m/%Y')}"
            } for p in periodos
        ]
        return jsonify(periodos_json)
    except Exception as e:
        return jsonify({'erro': str(e)}), 500