from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from app import db
from app.models.depositos_judiciais import DepositosSufin, Area, CentroResultado, ProcessosJudiciais
from app.utils.audit import registrar_log
from datetime import datetime
from sqlalchemy import func, or_, and_
from decimal import Decimal

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

            # Tratar valor de rateio
            vr_rateio_str = request.form.get('vr_rateio')
            vr_rateio_str = vr_rateio_str.replace('.', '').replace(',', '.')
            novo_deposito.VR_RATEIO = float(vr_rateio_str)

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

            # NOVA PARTE: Incluir o processo judicial se foi informado
            nr_processo = request.form.get('nr_processo')
            if nr_processo:
                novo_processo = ProcessosJudiciais()
                novo_processo.NU_LINHA = proximo_nu_linha  # Mesmo NU_LINHA
                novo_processo.NR_PROCESSO = nr_processo
                db.session.add(novo_processo)

            db.session.commit()

            # Registrar log
            registrar_log(
                'depositos_judiciais',
                'create',
                f'Novo depósito incluído - NU_LINHA: {proximo_nu_linha}',
                {'nu_linha': proximo_nu_linha, 'nr_processo': nr_processo if nr_processo else 'Não informado'}
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


@depositos_judiciais_bp.route('/edicao')
@login_required
def edicao():
    """Listagem de depósitos para edição com filtros"""
    # Pegar filtros da query string
    filtro_contrato = request.args.get('nu_contrato', '').strip()
    filtro_carteira = request.args.get('id_centro', '')
    filtro_valor = request.args.get('vr_rateio', '').strip()
    filtro_dt_identificacao = request.args.get('dt_identificacao', '')

    # Query base com JOIN
    query = db.session.query(
        DepositosSufin,
        ProcessosJudiciais.NR_PROCESSO,
        Area.NO_AREA,
        CentroResultado.NO_CARTEIRA
    ).outerjoin(
        ProcessosJudiciais,
        DepositosSufin.NU_LINHA == ProcessosJudiciais.NU_LINHA
    ).outerjoin(
        Area,
        DepositosSufin.ID_AREA == Area.ID_AREA
    ).outerjoin(
        CentroResultado,
        DepositosSufin.ID_CENTRO == CentroResultado.ID_CENTRO
    ).filter(
        or_(DepositosSufin.ID_CENTRO != 8, DepositosSufin.ID_CENTRO.is_(None))
    )  # EXCLUIR REGISTROS COM ID_CENTRO = 8

    # Aplicar filtros
    if filtro_contrato:
        query = query.filter(DepositosSufin.NU_CONTRATO == int(filtro_contrato))

    if filtro_carteira:
        query = query.filter(DepositosSufin.ID_CENTRO == int(filtro_carteira))

    if filtro_valor:
        try:
            filtro_valor_limpo = filtro_valor.replace('.', '').replace(',', '.')
            valor_decimal = Decimal(filtro_valor_limpo)
            query = query.filter(DepositosSufin.VR_RATEIO == valor_decimal)
        except:
            pass

    if filtro_dt_identificacao:
        try:
            data_filtro = datetime.strptime(filtro_dt_identificacao, '%Y-%m-%d')
            query = query.filter(DepositosSufin.DT_IDENTIFICACAO == data_filtro)
        except:
            pass

    # Executar query
    depositos = query.order_by(DepositosSufin.NU_LINHA.desc()).all()

    # Buscar centros para o filtro
    centros = CentroResultado.query.order_by(CentroResultado.NO_CARTEIRA).all()

    return render_template('depositos_judiciais/edicao.html',
                           depositos=depositos,
                           centros=centros,
                           filtros={
                               'nu_contrato': filtro_contrato,
                               'id_centro': filtro_carteira,
                               'vr_rateio': filtro_valor,
                               'dt_identificacao': filtro_dt_identificacao
                           })


@depositos_judiciais_bp.route('/editar/<int:nu_linha>', methods=['GET', 'POST'])
@login_required
def editar(nu_linha):
    """Editar um depósito específico"""
    # Buscar o depósito com os relacionamentos
    deposito = db.session.query(
        DepositosSufin,
        ProcessosJudiciais.NR_PROCESSO
    ).outerjoin(
        ProcessosJudiciais,
        DepositosSufin.NU_LINHA == ProcessosJudiciais.NU_LINHA
    ).filter(
        DepositosSufin.NU_LINHA == nu_linha
    ).first()

    if not deposito:
        flash('Depósito não encontrado.', 'danger')
        return redirect(url_for('depositos_judiciais.edicao'))

    deposito_obj = deposito[0]
    nr_processo = deposito[1]

    if request.method == 'POST':
        try:
            # Guardar valores antigos
            centro_antigo = deposito_obj.ID_CENTRO

            # Pegar novo centro
            novo_centro = int(request.form.get('id_centro')) if request.form.get('id_centro') else None

            # NOVA PARTE: Atualizar ou criar processo judicial
            novo_nr_processo = request.form.get('nr_processo')
            if novo_nr_processo:
                # Verificar se já existe um processo para este NU_LINHA
                processo_existente = ProcessosJudiciais.query.filter_by(NU_LINHA=nu_linha).first()
                if processo_existente:
                    processo_existente.NR_PROCESSO = novo_nr_processo
                else:
                    novo_processo = ProcessosJudiciais()
                    novo_processo.NU_LINHA = nu_linha
                    novo_processo.NR_PROCESSO = novo_nr_processo
                    db.session.add(novo_processo)
            else:
                # Se o campo foi limpo, remover o processo
                processo_existente = ProcessosJudiciais.query.filter_by(NU_LINHA=nu_linha).first()
                if processo_existente:
                    db.session.delete(processo_existente)

            # Verificar se mudou o centro e se não é Institucional
            if novo_centro != centro_antigo and novo_centro is not None:
                # Buscar o nome da nova carteira
                nova_carteira = CentroResultado.query.filter_by(ID_CENTRO=novo_centro).first()

                # Verificar se NÃO é Institucional (nome diferente E ID diferente de 6)
                if nova_carteira and nova_carteira.NO_CARTEIRA != 'Institucional' and novo_centro != 6:
                    # PROCESSO ESPECIAL: NÃO ALTERA O ORIGINAL, CRIA ESTORNO E NOVA

                    # 1. Buscar próximo NU_LINHA para estorno
                    ultimo_nu_linha = db.session.query(func.max(DepositosSufin.NU_LINHA)).scalar()
                    proximo_nu_linha_estorno = (ultimo_nu_linha or 0) + 1

                    # 2. Criar linha de ESTORNO (valor negativo, centro 8)
                    linha_estorno = DepositosSufin()
                    linha_estorno.NU_LINHA = proximo_nu_linha_estorno
                    linha_estorno.LANCAMENTO_RM = deposito_obj.LANCAMENTO_RM
                    linha_estorno.DT_LANCAMENTO_DJ = deposito_obj.DT_LANCAMENTO_DJ
                    linha_estorno.VR_RATEIO = -abs(deposito_obj.VR_RATEIO)  # VALOR NEGATIVO
                    linha_estorno.MEMO_SUFIN = deposito_obj.MEMO_SUFIN
                    linha_estorno.DT_MEMO = deposito_obj.DT_MEMO
                    linha_estorno.ID_IDENTIFICADO = deposito_obj.ID_IDENTIFICADO
                    linha_estorno.DT_IDENTIFICACAO = deposito_obj.DT_IDENTIFICACAO
                    linha_estorno.ID_AREA = deposito_obj.ID_AREA
                    linha_estorno.ID_AREA_2 = None
                    linha_estorno.ID_CENTRO = 8  # CENTRO 8 para estornos
                    linha_estorno.ID_AJUSTE_RM = deposito_obj.ID_AJUSTE_RM
                    linha_estorno.DT_AJUSTE_RM = deposito_obj.DT_AJUSTE_RM
                    linha_estorno.NU_CONTRATO = deposito_obj.NU_CONTRATO
                    linha_estorno.NU_CONTRATO_2 = None
                    linha_estorno.EVENTO_CONTABIL_ANTERIOR = deposito_obj.EVENTO_CONTABIL_ANTERIOR
                    linha_estorno.EVENTO_CONTABIL_ATUAL = deposito_obj.EVENTO_CONTABIL_ATUAL
                    linha_estorno.OBS = f"ESTORNO - REF. LINHA {nu_linha}"
                    linha_estorno.IC_APROPRIADO = deposito_obj.IC_APROPRIADO
                    linha_estorno.DT_SISCOR = deposito_obj.DT_SISCOR
                    linha_estorno.IC_INCLUIDO_ACERTO = deposito_obj.IC_INCLUIDO_ACERTO

                    db.session.add(linha_estorno)

                    # 3. Buscar próximo NU_LINHA para nova linha
                    proximo_nu_linha_nova = proximo_nu_linha_estorno + 1

                    # 4. Criar NOVA linha com as alterações
                    nova_linha = DepositosSufin()
                    nova_linha.NU_LINHA = proximo_nu_linha_nova
                    nova_linha.LANCAMENTO_RM = request.form.get('lancamento_rm')
                    nova_linha.DT_LANCAMENTO_DJ = datetime.strptime(request.form.get('dt_lancamento_dj'), '%Y-%m-%d')

                    # Tratar valor de rateio
                    vr_rateio_str = request.form.get('vr_rateio')
                    vr_rateio_str = vr_rateio_str.replace('.', '').replace(',', '.')
                    nova_linha.VR_RATEIO = float(vr_rateio_str)

                    nova_linha.MEMO_SUFIN = request.form.get('memo_sufin')

                    # Lógica para DT_MEMO e ID_IDENTIFICADO
                    dt_memo = request.form.get('dt_memo')
                    if dt_memo:
                        nova_linha.DT_MEMO = datetime.strptime(dt_memo, '%Y-%m-%d')
                        nova_linha.ID_IDENTIFICADO = True
                    else:
                        nova_linha.DT_MEMO = None
                        nova_linha.ID_IDENTIFICADO = False

                    # DT_IDENTIFICACAO
                    dt_identificacao = request.form.get('dt_identificacao')
                    if dt_identificacao:
                        nova_linha.DT_IDENTIFICACAO = datetime.strptime(dt_identificacao, '%Y-%m-%d')
                    else:
                        nova_linha.DT_IDENTIFICACAO = None

                    nova_linha.ID_AREA = int(request.form.get('id_area')) if request.form.get('id_area') else None
                    nova_linha.ID_AREA_2 = None
                    nova_linha.ID_CENTRO = novo_centro  # NOVO CENTRO
                    nova_linha.ID_AJUSTE_RM = False

                    dt_ajuste_rm = request.form.get('dt_ajuste_rm')
                    if dt_ajuste_rm:
                        nova_linha.DT_AJUSTE_RM = datetime.strptime(dt_ajuste_rm, '%Y-%m-%d')
                    else:
                        nova_linha.DT_AJUSTE_RM = None

                    nu_contrato = request.form.get('nu_contrato')
                    if nu_contrato:
                        nova_linha.NU_CONTRATO = int(nu_contrato)
                    else:
                        nova_linha.NU_CONTRATO = None

                    nova_linha.NU_CONTRATO_2 = None

                    evento_anterior = request.form.get('evento_contabil_anterior')
                    if evento_anterior:
                        nova_linha.EVENTO_CONTABIL_ANTERIOR = int(evento_anterior)
                    else:
                        nova_linha.EVENTO_CONTABIL_ANTERIOR = None

                    evento_atual = request.form.get('evento_contabil_atual')
                    if evento_atual:
                        nova_linha.EVENTO_CONTABIL_ATUAL = int(evento_atual)
                    else:
                        nova_linha.EVENTO_CONTABIL_ATUAL = None

                    nova_linha.OBS = request.form.get('obs')
                    nova_linha.IC_APROPRIADO = None

                    # DT_SISCOR - NULL a menos que preenchida
                    dt_siscor = request.form.get('dt_siscor')
                    if dt_siscor:
                        nova_linha.DT_SISCOR = datetime.strptime(dt_siscor, '%Y-%m-%d')
                    else:
                        nova_linha.DT_SISCOR = None

                    nova_linha.IC_INCLUIDO_ACERTO = None

                    db.session.add(nova_linha)

                    # NOVA PARTE: Se houver processo judicial, criar cópia para a nova linha
                    if novo_nr_processo:
                        novo_processo_nova_linha = ProcessosJudiciais()
                        novo_processo_nova_linha.NU_LINHA = proximo_nu_linha_nova
                        novo_processo_nova_linha.NR_PROCESSO = novo_nr_processo
                        db.session.add(novo_processo_nova_linha)

                    # NÃO ALTERAR O REGISTRO ORIGINAL!
                    db.session.commit()

                    flash('Depósito judicial processado com sucesso! Criadas linha de estorno e nova linha.', 'success')
                    return redirect(url_for('depositos_judiciais.edicao'))

                else:
                    # É Institucional ou não mudou significativamente - EDIÇÃO NORMAL
                    # Atualizar todos os campos normalmente
                    deposito_obj.LANCAMENTO_RM = request.form.get('lancamento_rm')
                    deposito_obj.DT_LANCAMENTO_DJ = datetime.strptime(request.form.get('dt_lancamento_dj'), '%Y-%m-%d')

                    # Tratar valor de rateio
                    vr_rateio_str = request.form.get('vr_rateio')
                    vr_rateio_str = vr_rateio_str.replace('.', '').replace(',', '.')
                    deposito_obj.VR_RATEIO = float(vr_rateio_str)

                    deposito_obj.MEMO_SUFIN = request.form.get('memo_sufin')

                    dt_memo = request.form.get('dt_memo')
                    if dt_memo:
                        deposito_obj.DT_MEMO = datetime.strptime(dt_memo, '%Y-%m-%d')
                        deposito_obj.ID_IDENTIFICADO = True
                    else:
                        deposito_obj.DT_MEMO = None
                        deposito_obj.ID_IDENTIFICADO = False

                    dt_identificacao = request.form.get('dt_identificacao')
                    if dt_identificacao:
                        deposito_obj.DT_IDENTIFICACAO = datetime.strptime(dt_identificacao, '%Y-%m-%d')
                    else:
                        deposito_obj.DT_IDENTIFICACAO = None

                    deposito_obj.ID_AREA = int(request.form.get('id_area')) if request.form.get('id_area') else None
                    deposito_obj.ID_CENTRO = novo_centro

                    dt_ajuste_rm = request.form.get('dt_ajuste_rm')
                    if dt_ajuste_rm:
                        deposito_obj.DT_AJUSTE_RM = datetime.strptime(dt_ajuste_rm, '%Y-%m-%d')
                    else:
                        deposito_obj.DT_AJUSTE_RM = None

                    nu_contrato = request.form.get('nu_contrato')
                    if nu_contrato:
                        deposito_obj.NU_CONTRATO = int(nu_contrato)
                    else:
                        deposito_obj.NU_CONTRATO = None

                    evento_anterior = request.form.get('evento_contabil_anterior')
                    if evento_anterior:
                        deposito_obj.EVENTO_CONTABIL_ANTERIOR = int(evento_anterior)
                    else:
                        deposito_obj.EVENTO_CONTABIL_ANTERIOR = None

                    evento_atual = request.form.get('evento_contabil_atual')
                    if evento_atual:
                        deposito_obj.EVENTO_CONTABIL_ATUAL = int(evento_atual)
                    else:
                        deposito_obj.EVENTO_CONTABIL_ATUAL = None

                    deposito_obj.OBS = request.form.get('obs')

                    dt_siscor = request.form.get('dt_siscor')
                    if dt_siscor:
                        deposito_obj.DT_SISCOR = datetime.strptime(dt_siscor, '%Y-%m-%d')
                    else:
                        deposito_obj.DT_SISCOR = None

                    db.session.commit()

                    flash('Depósito judicial atualizado com sucesso!', 'success')
                    return redirect(url_for('depositos_judiciais.edicao'))

            else:
                # Não mudou centro ou é NULL - EDIÇÃO NORMAL
                deposito_obj.LANCAMENTO_RM = request.form.get('lancamento_rm')
                deposito_obj.DT_LANCAMENTO_DJ = datetime.strptime(request.form.get('dt_lancamento_dj'), '%Y-%m-%d')

                # Tratar valor de rateio
                vr_rateio_str = request.form.get('vr_rateio')
                vr_rateio_str = vr_rateio_str.replace('.', '').replace(',', '.')
                deposito_obj.VR_RATEIO = float(vr_rateio_str)

                deposito_obj.MEMO_SUFIN = request.form.get('memo_sufin')

                dt_memo = request.form.get('dt_memo')
                if dt_memo:
                    deposito_obj.DT_MEMO = datetime.strptime(dt_memo, '%Y-%m-%d')
                    deposito_obj.ID_IDENTIFICADO = True
                else:
                    deposito_obj.DT_MEMO = None
                    deposito_obj.ID_IDENTIFICADO = False

                dt_identificacao = request.form.get('dt_identificacao')
                if dt_identificacao:
                    deposito_obj.DT_IDENTIFICACAO = datetime.strptime(dt_identificacao, '%Y-%m-%d')
                else:
                    deposito_obj.DT_IDENTIFICACAO = None

                deposito_obj.ID_AREA = int(request.form.get('id_area')) if request.form.get('id_area') else None
                deposito_obj.ID_CENTRO = novo_centro

                dt_ajuste_rm = request.form.get('dt_ajuste_rm')
                if dt_ajuste_rm:
                    deposito_obj.DT_AJUSTE_RM = datetime.strptime(dt_ajuste_rm, '%Y-%m-%d')
                else:
                    deposito_obj.DT_AJUSTE_RM = None

                nu_contrato = request.form.get('nu_contrato')
                if nu_contrato:
                    deposito_obj.NU_CONTRATO = int(nu_contrato)
                else:
                    deposito_obj.NU_CONTRATO = None

                evento_anterior = request.form.get('evento_contabil_anterior')
                if evento_anterior:
                    deposito_obj.EVENTO_CONTABIL_ANTERIOR = int(evento_anterior)
                else:
                    deposito_obj.EVENTO_CONTABIL_ANTERIOR = None

                evento_atual = request.form.get('evento_contabil_atual')
                if evento_atual:
                    deposito_obj.EVENTO_CONTABIL_ATUAL = int(evento_atual)
                else:
                    deposito_obj.EVENTO_CONTABIL_ATUAL = None

                deposito_obj.OBS = request.form.get('obs')

                dt_siscor = request.form.get('dt_siscor')
                if dt_siscor:
                    deposito_obj.DT_SISCOR = datetime.strptime(dt_siscor, '%Y-%m-%d')
                else:
                    deposito_obj.DT_SISCOR = None

                db.session.commit()

                flash('Depósito judicial atualizado com sucesso!', 'success')
                return redirect(url_for('depositos_judiciais.edicao'))

            # Registrar log
            registrar_log(
                'depositos_judiciais',
                'update',
                f'Depósito editado - NU_LINHA: {nu_linha}',
                {'nu_linha': nu_linha}
            )

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao atualizar depósito: {str(e)}', 'danger')
            return redirect(url_for('depositos_judiciais.editar', nu_linha=nu_linha))

    # GET - Buscar dados para os dropdowns
    areas = Area.query.order_by(Area.NO_AREA).all()
    centros = CentroResultado.query.order_by(CentroResultado.NO_CARTEIRA).all()

    return render_template('depositos_judiciais/editar.html',
                           deposito=deposito_obj,
                           nr_processo=nr_processo,
                           areas=areas,
                           centros=centros)