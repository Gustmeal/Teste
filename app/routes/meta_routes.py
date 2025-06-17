# app/routes/meta_routes.py
from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash
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
    """Busca empresas descredenciadas que ainda não tiveram metas redistribuídas"""
    try:
        edital_id = request.args.get('edital_id', type=int)
        periodo_id = request.args.get('periodo_id', type=int)

        # Buscar última distribuição disponível
        sql = text("""
            SELECT TOP 1 DT_REFERENCIA
            FROM DEV.DCA_TB015_METAS_PERCENTUAIS_DISTRIBUICAO
            WHERE ID_EDITAL = :edital_id
            AND ID_PERIODO = :periodo_id
            AND DELETED_AT IS NULL
            ORDER BY DT_REFERENCIA DESC
        """)

        result = db.session.execute(sql, {
            'edital_id': edital_id,
            'periodo_id': periodo_id
        }).fetchone()

        if not result:
            return jsonify({'sucesso': False, 'erro': 'Nenhuma distribuição encontrada'})

        ultima_data = result[0]

        # Buscar empresas DESCREDENCIADAS NO PERÍODO mas que ainda têm percentual > 0
        # (ou seja, foram descredenciadas mas ainda não tiveram metas redistribuídas)
        sql_empresas = text("""
            SELECT 
                mpd.ID_EMPRESA,
                emp.NO_EMPRESA_ABREVIADA,
                emp.DS_CONDICAO,
                mpd.VR_SALDO_DEVEDOR_DISTRIBUIDO,
                mpd.PERCENTUAL_SALDO_DEVEDOR
            FROM DEV.DCA_TB015_METAS_PERCENTUAIS_DISTRIBUICAO mpd
            JOIN DEV.DCA_TB002_EMPRESAS_PARTICIPANTES emp 
                ON mpd.ID_EMPRESA = emp.ID_EMPRESA
                AND mpd.ID_EDITAL = emp.ID_EDITAL
                AND mpd.ID_PERIODO = emp.ID_PERIODO
            WHERE mpd.ID_EDITAL = :edital_id
            AND mpd.ID_PERIODO = :periodo_id
            AND mpd.DT_REFERENCIA = :data_ref
            AND mpd.DELETED_AT IS NULL
            AND emp.DS_CONDICAO = 'DESCREDENCIADA NO PERÍODO'  -- Apenas descredenciadas
            AND mpd.PERCENTUAL_SALDO_DEVEDOR > 0  -- Que ainda não foram redistribuídas
            ORDER BY emp.NO_EMPRESA_ABREVIADA
        """)

        result_empresas = db.session.execute(sql_empresas, {
            'edital_id': edital_id,
            'periodo_id': periodo_id,
            'data_ref': ultima_data
        })

        empresas = []
        for row in result_empresas:
            empresas.append({
                'id_empresa': row[0],
                'nome_empresa': row[1],
                'condicao': row[2],
                'saldo_devedor': float(row[3]) if row[3] else 0,
                'percentual': float(row[4]) if row[4] else 0
            })

        return jsonify({
            'sucesso': True,
            'empresas': empresas,
            'data_referencia': ultima_data.strftime('%Y-%m-%d')
        })

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
            incremento_meta=data['incremento_meta']
        )

        resultado = redistribuidor.calcular_redistribuicao()

        return jsonify({
            'sucesso': True,
            'resultado': resultado
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'sucesso': False, 'erro': str(e)})


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
            incremento_meta=data['incremento_meta']
        )

        sucesso = redistribuidor.salvar_redistribuicao(data['resultado'])

        if sucesso:
            # Registrar log
            registrar_log(
                acao='redistribuicao_metas',
                entidade='meta',
                entidade_id=f"{data['edital_id']}-{data['periodo_id']}",
                descricao=f'Redistribuição de metas - Empresa {data["empresa_id"]} descredenciada em {data["data_descredenciamento"]}',
                dados_novos=data
            )

            return jsonify({
                'sucesso': True,
                'mensagem': 'Redistribuição salva com sucesso!'
            })
        else:
            return jsonify({
                'sucesso': False,
                'erro': 'Erro ao salvar redistribuição'
            })

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
        periodo = PeriodoAvaliacao.query.filter_by(ID=periodo_id).first()

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
                emp.NO_EMPRESA_ABREVIADA,
                mpd.VR_SALDO_DEVEDOR_DISTRIBUIDO,
                mpd.PERCENTUAL_SALDO_DEVEDOR
            FROM DEV.DCA_TB015_METAS_PERCENTUAIS_DISTRIBUICAO mpd
            JOIN DEV.DCA_TB002_EMPRESAS_PARTICIPANTES emp 
                ON mpd.ID_EMPRESA = emp.ID_EMPRESA
                AND mpd.ID_EDITAL = emp.ID_EDITAL
                AND mpd.ID_PERIODO = emp.ID_PERIODO
            WHERE mpd.ID_EDITAL = :edital_id
            AND mpd.ID_PERIODO = :periodo_id
            AND mpd.DELETED_AT IS NULL
            ORDER BY mpd.DT_REFERENCIA, emp.NO_EMPRESA_ABREVIADA
        """)

        result = db.session.execute(sql, {
            'edital_id': edital_id,
            'periodo_id': periodo_id
        })

        # Agrupar por data de referência
        empresas_por_data = {}

        for row in result:
            dt_ref = row[0].strftime('%Y-%m-%d')

            if dt_ref not in empresas_por_data:
                empresas_por_data[dt_ref] = []

            empresas_por_data[dt_ref].append({
                'id_empresa': row[1],
                'nome_empresa': row[2],
                'saldo_devedor': float(row[3]) if row[3] else 0,
                'percentual': float(row[4]) if row[4] else 0
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
    """Calcula a redistribuição de metas"""
    try:
        data = request.json
        edital_id = data['edital_id']
        periodo_id = data['periodo_id']
        empresas_redistribuicao = data['empresas_redistribuicao']  # {data: [lista_empresas]}

        from app.utils.redistribuicao_calculator import RedistribuicaoCalculator

        calculator = RedistribuicaoCalculator(edital_id, periodo_id)
        resultado = calculator.calcular_redistribuicao(empresas_redistribuicao)

        return jsonify({
            'sucesso': True,
            'resultado': resultado
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'sucesso': False, 'erro': str(e)})