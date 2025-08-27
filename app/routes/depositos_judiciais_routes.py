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

            # Criar registro de processo judicial se informado
            nr_processo = request.form.get('nr_processo')
            if nr_processo and nr_processo.strip():
                novo_processo = ProcessosJudiciais()
                novo_processo.NU_LINHA = proximo_nu_linha
                novo_processo.NR_PROCESSO = nr_processo.strip()
                db.session.add(novo_processo)

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

                    # 2. Criar ESTORNO (cópia com valor negativo)
                    estorno = DepositosSufin()
                    estorno.NU_LINHA = proximo_nu_linha_estorno
                    estorno.LANCAMENTO_RM = deposito_obj.LANCAMENTO_RM
                    estorno.DT_LANCAMENTO_DJ = deposito_obj.DT_LANCAMENTO_DJ
                    estorno.VR_RATEIO = -abs(deposito_obj.VR_RATEIO) if deposito_obj.VR_RATEIO else 0
                    estorno.MEMO_SUFIN = deposito_obj.MEMO_SUFIN
                    estorno.DT_MEMO = deposito_obj.DT_MEMO
                    estorno.ID_IDENTIFICADO = deposito_obj.ID_IDENTIFICADO
                    estorno.DT_IDENTIFICACAO = deposito_obj.DT_IDENTIFICACAO
                    estorno.ID_AREA = deposito_obj.ID_AREA
                    estorno.ID_AREA_2 = deposito_obj.ID_AREA_2
                    estorno.ID_CENTRO = deposito_obj.ID_CENTRO  # MANTÉM O CENTRO ORIGINAL
                    estorno.ID_AJUSTE_RM = deposito_obj.ID_AJUSTE_RM
                    estorno.DT_AJUSTE_RM = deposito_obj.DT_AJUSTE_RM
                    estorno.NU_CONTRATO = deposito_obj.NU_CONTRATO
                    estorno.NU_CONTRATO_2 = deposito_obj.NU_CONTRATO_2
                    estorno.EVENTO_CONTABIL_ANTERIOR = deposito_obj.EVENTO_CONTABIL_ANTERIOR
                    estorno.EVENTO_CONTABIL_ATUAL = deposito_obj.EVENTO_CONTABIL_ATUAL
                    estorno.OBS = f"ESTORNO REF. LINHA {deposito_obj.NU_LINHA}"
                    estorno.IC_APROPRIADO = deposito_obj.IC_APROPRIADO
                    estorno.DT_SISCOR = deposito_obj.DT_SISCOR
                    estorno.IC_INCLUIDO_ACERTO = deposito_obj.IC_INCLUIDO_ACERTO

                    db.session.add(estorno)

                    # Copiar processo judicial para estorno se existir
                    if nr_processo:
                        processo_estorno = ProcessosJudiciais()
                        processo_estorno.NU_LINHA = proximo_nu_linha_estorno
                        processo_estorno.NR_PROCESSO = nr_processo
                        db.session.add(processo_estorno)

                    # 3. Buscar próximo NU_LINHA para nova linha
                    proximo_nu_linha_nova = proximo_nu_linha_estorno + 1

                    # 4. Criar NOVA linha com as alterações
                    nova_linha = DepositosSufin()
                    nova_linha.NU_LINHA = proximo_nu_linha_nova
                    nova_linha.LANCAMENTO_RM = request.form.get('lancamento_rm')
                    nova_linha.DT_LANCAMENTO_DJ = datetime.strptime(request.form.get('dt_lancamento_dj'), '%Y-%m-%d')
                    nova_linha.VR_RATEIO = float(request.form.get('vr_rateio').replace(',', '.'))
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

                    # Criar processo judicial para nova linha se informado
                    nr_processo_form = request.form.get('nr_processo')
                    if nr_processo_form and nr_processo_form.strip():
                        processo_novo = ProcessosJudiciais()
                        processo_novo.NU_LINHA = proximo_nu_linha_nova
                        processo_novo.NR_PROCESSO = nr_processo_form.strip()
                        db.session.add(processo_novo)

                    # NÃO ALTERAR O REGISTRO ORIGINAL!

                    db.session.commit()

                    flash('Depósito judicial processado com sucesso! Criadas linha de estorno e nova linha.', 'success')
                    return redirect(url_for('depositos_judiciais.edicao'))

                else:
                    # É Institucional ou não mudou significativamente - EDIÇÃO NORMAL
                    # Atualizar todos os campos normalmente
                    deposito_obj.LANCAMENTO_RM = request.form.get('lancamento_rm')
                    deposito_obj.DT_LANCAMENTO_DJ = datetime.strptime(request.form.get('dt_lancamento_dj'), '%Y-%m-%d')
                    deposito_obj.VR_RATEIO = float(request.form.get('vr_rateio').replace(',', '.'))
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

                    # Atualizar ou criar registro de processo judicial
                    nr_processo_form = request.form.get('nr_processo')
                    if nr_processo_form and nr_processo_form.strip():
                        # Verificar se já existe registro de processo
                        processo_existente = ProcessosJudiciais.query.filter_by(NU_LINHA=nu_linha).first()

                        if processo_existente:
                            # Atualizar existente
                            processo_existente.NR_PROCESSO = nr_processo_form.strip()
                        else:
                            # Criar novo
                            novo_processo = ProcessosJudiciais()
                            novo_processo.NU_LINHA = nu_linha
                            novo_processo.NR_PROCESSO = nr_processo_form.strip()
                            db.session.add(novo_processo)
                    else:
                        # Se o campo foi limpo, remover registro de processo se existir
                        processo_existente = ProcessosJudiciais.query.filter_by(NU_LINHA=nu_linha).first()
                        if processo_existente:
                            db.session.delete(processo_existente)

                    db.session.commit()

                    flash('Depósito judicial atualizado com sucesso!', 'success')
                    return redirect(url_for('depositos_judiciais.edicao'))
            else:
                # Não mudou centro - EDIÇÃO NORMAL de todos os campos
                deposito_obj.LANCAMENTO_RM = request.form.get('lancamento_rm')
                deposito_obj.DT_LANCAMENTO_DJ = datetime.strptime(request.form.get('dt_lancamento_dj'), '%Y-%m-%d')
                deposito_obj.VR_RATEIO = float(request.form.get('vr_rateio').replace(',', '.'))
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

                # Atualizar ou criar registro de processo judicial
                nr_processo_form = request.form.get('nr_processo')
                if nr_processo_form and nr_processo_form.strip():
                    # Verificar se já existe registro de processo
                    processo_existente = ProcessosJudiciais.query.filter_by(NU_LINHA=nu_linha).first()

                    if processo_existente:
                        # Atualizar existente
                        processo_existente.NR_PROCESSO = nr_processo_form.strip()
                    else:
                        # Criar novo
                        novo_processo = ProcessosJudiciais()
                        novo_processo.NU_LINHA = nu_linha
                        novo_processo.NR_PROCESSO = nr_processo_form.strip()
                        db.session.add(novo_processo)
                else:
                    # Se o campo foi limpo, remover registro de processo se existir
                    processo_existente = ProcessosJudiciais.query.filter_by(NU_LINHA=nu_linha).first()
                    if processo_existente:
                        db.session.delete(processo_existente)

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