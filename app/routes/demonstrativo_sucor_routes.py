from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
from app.models.demonstrativo_sucor import DemonstrativoSucorMeses, JustificativaSucor
from app import db
from flask_login import login_required, current_user
from datetime import datetime, date
from sqlalchemy import distinct, and_
from decimal import Decimal
import logging
from sqlalchemy import distinct, and_, text

demonstrativo_sucor_bp = Blueprint('demonstrativo_sucor', __name__, url_prefix='/demonstrativos-sucor')

# Mapeamento de meses
MESES = {
    1: 'JAN', 2: 'FEV', 3: 'MAR', 4: 'ABR',
    5: 'MAI', 6: 'JUN', 7: 'JUL', 8: 'AGO',
    9: 'SET', 10: 'OUT', 11: 'NOV', 12: 'DEZ'
}

MESES_NOMES = {
    1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
    5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
    9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
}


@demonstrativo_sucor_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@demonstrativo_sucor_bp.route('/')
@login_required
def index():
    """Página principal dos demonstrativos SUCOR"""
    # Buscar tipos de demonstrativos únicos
    tipos_demonstrativos = db.session.query(
        distinct(DemonstrativoSucorMeses.NO_DEMONSTRATIVO)
    ).order_by(DemonstrativoSucorMeses.NO_DEMONSTRATIVO).all()

    # Buscar anos disponíveis
    anos_disponiveis = db.session.query(
        distinct(DemonstrativoSucorMeses.ANO)
    ).order_by(DemonstrativoSucorMeses.ANO.desc()).all()

    return render_template(
        'demonstrativos_sucor/index.html',
        tipos_demonstrativos=[t[0] for t in tipos_demonstrativos],
        anos_disponiveis=[a[0] for a in anos_disponiveis],
        meses=MESES_NOMES
    )


@demonstrativo_sucor_bp.route('/comparar', methods=['POST'])
@login_required
def comparar_demonstrativos():
    """Compara demonstrativos entre dois períodos e calcula variações"""
    try:
        # Obter parâmetros do formulário
        tipo_demonstrativo = request.form.get('tipo_demonstrativo')
        ano_periodo1 = int(request.form.get('ano_periodo1'))
        mes_periodo1 = int(request.form.get('mes_periodo1'))
        ano_periodo2 = int(request.form.get('ano_periodo2'))
        mes_periodo2 = int(request.form.get('mes_periodo2'))

        mes_coluna1 = MESES[mes_periodo1]
        mes_coluna2 = MESES[mes_periodo2]

        # Query SQL com colchetes nas colunas de mês para evitar conflito com palavras reservadas
        sql = text("""
            WITH base_demonstrativos AS (
                SELECT DISTINCT 
                    CO_DEMONSTRATIVO, 
                    ORDEM, 
                    GRUPO
                FROM BDG.COR_DEM_TB005_DEMONSTRATIVOS_MESES
                WHERE NO_DEMONSTRATIVO = :tipo_demonstrativo
            ),
            periodo1 AS (
                SELECT 
                    CO_DEMONSTRATIVO,
                    ORDEM,
                    [""" + mes_coluna1 + """] as valor_periodo1
                FROM BDG.COR_DEM_TB005_DEMONSTRATIVOS_MESES
                WHERE NO_DEMONSTRATIVO = :tipo_demonstrativo
                AND ANO = :ano_periodo1
            ),
            periodo2 AS (
                SELECT 
                    CO_DEMONSTRATIVO,
                    ORDEM,
                    [""" + mes_coluna2 + """] as valor_periodo2
                FROM BDG.COR_DEM_TB005_DEMONSTRATIVOS_MESES
                WHERE NO_DEMONSTRATIVO = :tipo_demonstrativo
                AND ANO = :ano_periodo2
            )
            SELECT 
                b.CO_DEMONSTRATIVO,
                b.ORDEM,
                b.GRUPO,
                COALESCE(p1.valor_periodo1, 0) as valor_periodo1,
                COALESCE(p2.valor_periodo2, 0) as valor_periodo2,
                COALESCE(p2.valor_periodo2, 0) - COALESCE(p1.valor_periodo1, 0) as variacao
            FROM base_demonstrativos b
            LEFT JOIN periodo1 p1 ON b.CO_DEMONSTRATIVO = p1.CO_DEMONSTRATIVO AND b.ORDEM = p1.ORDEM
            LEFT JOIN periodo2 p2 ON b.CO_DEMONSTRATIVO = p2.CO_DEMONSTRATIVO AND b.ORDEM = p2.ORDEM
            ORDER BY b.ORDEM
        """)

        resultados = db.session.execute(sql, {
            'tipo_demonstrativo': tipo_demonstrativo,
            'ano_periodo1': ano_periodo1,
            'ano_periodo2': ano_periodo2
        }).fetchall()

        # Preparar dados para comparação
        dados_comparacao = []

        for row in resultados:
            # Buscar justificativa se existir
            dt_ref = date(ano_periodo2, mes_periodo2, 1)
            justificativa = JustificativaSucor.query.filter(
                JustificativaSucor.DT_REFERENCIA == dt_ref,
                JustificativaSucor.CO_DEMONSTRATIVO == row.CO_DEMONSTRATIVO,
                JustificativaSucor.ORDEM == row.ORDEM
            ).first()

            dados_comparacao.append({
                'co_demonstrativo': row.CO_DEMONSTRATIVO,
                'ordem': row.ORDEM,
                'grupo': row.GRUPO,
                'valor_periodo1': float(row.valor_periodo1 or 0),
                'valor_periodo2': float(row.valor_periodo2 or 0),
                'variacao': float(row.variacao or 0),
                'variacao_justificada': float(justificativa.VARIACAO_DEM) if justificativa else float(row.variacao or 0),
                'tem_justificativa': justificativa is not None,
                'descricao_justificativa': justificativa.DESCRICAO if justificativa else None
            })

        # Preparar dados do período para exibição
        periodo1_str = f"{MESES_NOMES[mes_periodo1]}/{ano_periodo1}"
        periodo2_str = f"{MESES_NOMES[mes_periodo2]}/{ano_periodo2}"

        return render_template(
            'demonstrativos_sucor/comparacao.html',
            dados_comparacao=dados_comparacao,
            tipo_demonstrativo=tipo_demonstrativo,
            periodo1=periodo1_str,
            periodo2=periodo2_str,
            periodo1_data={'ano': ano_periodo1, 'mes': mes_periodo1},
            periodo2_data={'ano': ano_periodo2, 'mes': mes_periodo2}
        )

    except Exception as e:
        logging.error(f"Erro ao comparar demonstrativos SUCOR: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        flash(f'Erro ao comparar demonstrativos: {str(e)}', 'danger')
        return redirect(url_for('demonstrativo_sucor.index'))


@demonstrativo_sucor_bp.route('/justificativa/<int:co_demonstrativo>/<int:ordem>', methods=['GET'])
@login_required
def obter_justificativa(co_demonstrativo, ordem):
    """Obtém dados de justificativa para edição"""
    try:
        ano = request.args.get('ano', type=int)
        mes = request.args.get('mes', type=int)

        dt_ref = date(ano, mes, 1)

        # Buscar justificativa existente
        justificativa = JustificativaSucor.query.filter(
            JustificativaSucor.DT_REFERENCIA == dt_ref,
            JustificativaSucor.CO_DEMONSTRATIVO == co_demonstrativo,
            JustificativaSucor.ORDEM == ordem
        ).first()

        if justificativa:
            return jsonify({
                'success': True,
                'descricao': justificativa.DESCRICAO,
                'variacao': float(justificativa.VARIACAO_DEM)
            })
        else:
            return jsonify({
                'success': True,
                'descricao': '',
                'variacao': None
            })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })


@demonstrativo_sucor_bp.route('/salvar_justificativa', methods=['POST'])
@login_required
def salvar_justificativa():
    """Salva ou atualiza uma justificativa"""
    try:
        data = request.get_json()

        co_demonstrativo = data.get('co_demonstrativo')
        ordem = data.get('ordem')
        ano = data.get('ano')
        mes = data.get('mes')
        descricao = data.get('descricao')
        variacao = data.get('variacao')

        dt_ref = date(ano, mes, 1)

        # Verificar se já existe justificativa
        justificativa = JustificativaSucor.query.filter(
            JustificativaSucor.DT_REFERENCIA == dt_ref,
            JustificativaSucor.CO_DEMONSTRATIVO == co_demonstrativo,
            JustificativaSucor.ORDEM == ordem
        ).first()

        if justificativa:
            # Atualizar existente
            justificativa.DESCRICAO = descricao
            justificativa.VARIACAO_DEM = Decimal(str(variacao))
        else:
            # Criar nova
            justificativa = JustificativaSucor(
                DT_REFERENCIA=dt_ref,
                CO_DEMONSTRATIVO=co_demonstrativo,
                ORDEM=ordem,
                DESCRICAO=descricao,
                VARIACAO_DEM=Decimal(str(variacao))
            )
            db.session.add(justificativa)

        db.session.commit()

        return jsonify({
            'success': True,
            'message': 'Justificativa salva com sucesso!'
        })

    except Exception as e:
        db.session.rollback()
        logging.error(f"Erro ao salvar justificativa SUCOR: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        })


@demonstrativo_sucor_bp.route('/excluir_justificativa', methods=['POST'])
@login_required
def excluir_justificativa():
    """Exclui uma justificativa"""
    try:
        data = request.get_json()

        co_demonstrativo = data.get('co_demonstrativo')
        ordem = data.get('ordem')
        ano = data.get('ano')
        mes = data.get('mes')

        dt_ref = date(ano, mes, 1)

        # Buscar e excluir justificativa
        justificativa = JustificativaSucor.query.filter(
            JustificativaSucor.DT_REFERENCIA == dt_ref,
            JustificativaSucor.CO_DEMONSTRATIVO == co_demonstrativo,
            JustificativaSucor.ORDEM == ordem
        ).first()

        if justificativa:
            db.session.delete(justificativa)
            db.session.commit()

            return jsonify({
                'success': True,
                'message': 'Justificativa excluída com sucesso!'
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Justificativa não encontrada'
            })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'error': str(e)
        })