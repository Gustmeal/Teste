from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from app import db
from app.models.depositos_judiciais import DepositosSufin, Area, CentroResultado
from app.utils.audit import registrar_log
from datetime import datetime
from sqlalchemy import func

depositos_judiciais_bp = Blueprint('depositos_judiciais', __name__, url_prefix='/depositos-judiciais')


@depositos_judiciais_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@depositos_judiciais_bp.route('/')
@login_required
def index():
    """Página principal do sistema de Depósitos Judiciais"""
    return render_template('depositos_judiciais/index.html')


@depositos_judiciais_bp.route('/inclusao', methods=['GET', 'POST'])
@login_required
def inclusao():
    """Página de inclusão de novos depósitos"""
    if request.method == 'POST':
        try:
            # Buscar o próximo NU_LINHA
            ultimo_nu_linha = db.session.query(func.max(DepositosSufin.NU_LINHA)).scalar()
            proximo_nu_linha = (ultimo_nu_linha or 0) + 1

            # Criar novo registro
            novo_deposito = DepositosSufin()
            novo_deposito.NU_LINHA = proximo_nu_linha
            novo_deposito.LANCAMENTO_RM = request.form.get('lancamento_rm')
            novo_deposito.DT_LANCAMENTO_DJ = datetime.strptime(request.form.get('dt_lancamento_dj'), '%Y-%m-%d')
            novo_deposito.VR_RATEIO = float(request.form.get('vr_rateio').replace(',', '.'))
            novo_deposito.MEMO_SUFIN = request.form.get('memo_sufin')

            # Lógica para DT_MEMO e ID_IDENTIFICADO
            dt_memo = request.form.get('dt_memo')
            if dt_memo:
                novo_deposito.DT_MEMO = datetime.strptime(dt_memo, '%Y-%m-%d')
                novo_deposito.ID_IDENTIFICADO = True
            else:
                novo_deposito.DT_MEMO = None
                novo_deposito.ID_IDENTIFICADO = False

            # DT_IDENTIFICACAO (opcional)
            dt_identificacao = request.form.get('dt_identificacao')
            if dt_identificacao:
                novo_deposito.DT_IDENTIFICACAO = datetime.strptime(dt_identificacao, '%Y-%m-%d')

            # ID_AREA do dropdown
            novo_deposito.ID_AREA = int(request.form.get('id_area')) if request.form.get('id_area') else None

            # ID_AREA_2 sempre NULL
            novo_deposito.ID_AREA_2 = None

            # ID_CENTRO do dropdown
            novo_deposito.ID_CENTRO = int(request.form.get('id_centro')) if request.form.get('id_centro') else None

            # ID_AJUSTE_RM sempre 0
            novo_deposito.ID_AJUSTE_RM = False

            # DT_AJUSTE_RM (opcional)
            dt_ajuste_rm = request.form.get('dt_ajuste_rm')
            if dt_ajuste_rm:
                novo_deposito.DT_AJUSTE_RM = datetime.strptime(dt_ajuste_rm, '%Y-%m-%d')

            # NU_CONTRATO
            nu_contrato = request.form.get('nu_contrato')
            if nu_contrato:
                novo_deposito.NU_CONTRATO = int(nu_contrato)

            # NU_CONTRATO_2 sempre NULL
            novo_deposito.NU_CONTRATO_2 = None

            # Eventos contábeis
            evento_anterior = request.form.get('evento_contabil_anterior')
            if evento_anterior:
                novo_deposito.EVENTO_CONTABIL_ANTERIOR = int(evento_anterior)

            evento_atual = request.form.get('evento_contabil_atual')
            if evento_atual:
                novo_deposito.EVENTO_CONTABIL_ATUAL = int(evento_atual)

            # Observação
            novo_deposito.OBS = request.form.get('obs')

            # IC_APROPRIADO sempre NULL
            novo_deposito.IC_APROPRIADO = None

            # DT_SISCOR (opcional)
            dt_siscor = request.form.get('dt_siscor')
            if dt_siscor:
                novo_deposito.DT_SISCOR = datetime.strptime(dt_siscor, '%Y-%m-%d')

            # IC_INCLUIDO_ACERTO sempre NULL
            novo_deposito.IC_INCLUIDO_ACERTO = None

            # Salvar no banco
            db.session.add(novo_deposito)
            db.session.commit()

            # Registrar log
            registrar_log(
                'depositos_judiciais',
                'create',
                f'Novo depósito incluído - NU_LINHA: {proximo_nu_linha}',
                {'nu_linha': proximo_nu_linha}
            )

            flash('Depósito judicial incluído com sucesso!', 'success')
            return redirect(url_for('depositos_judiciais.index'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao incluir depósito: {str(e)}', 'danger')
            return redirect(url_for('depositos_judiciais.inclusao'))

    # GET - Buscar dados para os dropdowns
    areas = Area.query.order_by(Area.NO_AREA).all()
    centros = CentroResultado.query.order_by(CentroResultado.NO_CARTEIRA).all()

    return render_template('depositos_judiciais/inclusao.html',
                           areas=areas,
                           centros=centros)


@depositos_judiciais_bp.route('/api/areas')
@login_required
def api_areas():
    """API para retornar lista de áreas"""
    areas = Area.query.order_by(Area.NO_AREA).all()
    return jsonify([{
        'id': area.ID_AREA,
        'nome': area.NO_AREA
    } for area in areas])


@depositos_judiciais_bp.route('/api/centros')
@login_required
def api_centros():
    """API para retornar lista de centros de resultado"""
    centros = CentroResultado.query.order_by(CentroResultado.NO_CARTEIRA).all()
    return jsonify([{
        'id': centro.ID_CENTRO,
        'nome': centro.NO_CARTEIRA
    } for centro in centros])