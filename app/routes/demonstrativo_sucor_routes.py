from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
from app.models.demonstrativo_sucor import DemonstrativoSucorMeses, JustificativaSucor
from app import db
from flask_login import login_required, current_user
from datetime import datetime, date
from sqlalchemy import distinct, and_
from decimal import Decimal
import logging

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

        # Buscar demonstrativos do tipo selecionado
        demonstrativos_periodo1 = DemonstrativoSucorMeses.query.filter(
            DemonstrativoSucorMeses.NO_DEMONSTRATIVO == tipo_demonstrativo,
            DemonstrativoSucorMeses.ANO == ano_periodo1
        ).order_by(DemonstrativoSucorMeses.ORDEM).all()

        demonstrativos_periodo2 = DemonstrativoSucorMeses.query.filter(
            DemonstrativoSucorMeses.NO_DEMONSTRATIVO == tipo_demonstrativo,
            DemonstrativoSucorMeses.ANO == ano_periodo2
        ).order_by(DemonstrativoSucorMeses.ORDEM).all()

        # Preparar dados para comparação
        dados_comparacao = []

        # Criar dicionário para facilitar comparação
        dict_periodo2 = {d.CO_DEMONSTRATIVO: d for d in demonstrativos_periodo2}

        for demo1 in demonstrativos_periodo1:
            demo2 = dict_periodo2.get(demo1.CO_DEMONSTRATIVO)

            if demo2:
                valor1 = float(demo1.get_valor_mes(mes_periodo1) or 0)
                valor2 = float(demo2.get_valor_mes(mes_periodo2) or 0)
                variacao = valor2 - valor1

                # Buscar justificativa se existir
                dt_ref = date(ano_periodo2, mes_periodo2, 1)
                justificativa = JustificativaSucor.query.filter(
                    JustificativaSucor.DT_REFERENCIA == dt_ref,
                    JustificativaSucor.CO_DEMONSTRATIVO == demo1.CO_DEMONSTRATIVO,
                    JustificativaSucor.ORDEM == demo1.ORDEM
                ).first()

                dados_comparacao.append({
                    'co_demonstrativo': demo1.CO_DEMONSTRATIVO,
                    'ordem': demo1.ORDEM,
                    'grupo': demo1.GRUPO,
                    'valor_periodo1': valor1,
                    'valor_periodo2': valor2,
                    'variacao': variacao,
                    'variacao_justificada': float(justificativa.VARIACAO_DEM) if justificativa else variacao,
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