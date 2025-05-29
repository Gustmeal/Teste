# app/routes/meta_routes.py
from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash
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


@meta_bp.route('/metas/calcular', methods=['POST'])
@login_required
def calcular_metas():
    """Calcula metas usando a classe MetaCalculator"""
    try:
        edital_id = int(request.form['edital_id'])
        periodo_id = int(request.form['periodo_id'])
        fator_incremento = float(request.form.get('fator_incremento', 1.00))

        # Usar MetaCalculator
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