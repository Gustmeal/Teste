from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from app import db
from app.models.depositos_judiciais import DepositosSufin, Area, CentroResultado, ProcessosJudiciais
from app.utils.audit import registrar_log
from datetime import datetime
from sqlalchemy import func, or_, and_, extract
from decimal import Decimal
from sqlalchemy import text


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

            # Corrigir conversão do valor rateio
            # Corrigir conversão do valor rateio (permite valores negativos)
            vr_rateio = request.form.get('vr_rateio')
            # Detectar se é negativo
            is_negativo = vr_rateio.startswith('-')
            # Remove pontos de milhar, mantém vírgula e sinal negativo
            vr_rateio_limpo = vr_rateio.replace('.', '').replace(',', '.')
            if vr_rateio_limpo and vr_rateio_limpo != '-':
                novo_deposito.VR_RATEIO = Decimal(vr_rateio_limpo)

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

            # ID_CENTRO do dropdown - OBRIGATÓRIO
            id_centro = request.form.get('id_centro')
            if not id_centro:
                raise ValueError("Centro de Resultado é obrigatório")
            novo_deposito.ID_CENTRO = int(id_centro)

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

            # Eventos contábeis (Preenchimento automático)
            # EVENTO_CONTABIL_ANTERIOR sempre 22607
            novo_deposito.EVENTO_CONTABIL_ANTERIOR = 22607

            # EVENTO_CONTABIL_ATUAL baseado no ID_CENTRO
            id_centro_int = int(id_centro) # id_centro já foi validado e convertido para int
            mapeamento_eventos = {
                1: 22611,
                2: 22612,
                3: 22610,
                4: 22611,
                5: 22613,
                6: 22607,
                7: 22710
            }
            novo_deposito.EVENTO_CONTABIL_ATUAL = mapeamento_eventos.get(id_centro_int)

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

            # Verificar qual botão foi clicado
            if 'salvar_e_sair' in request.form:
                flash('Depósito judicial incluído com sucesso!', 'success')
                return redirect(url_for('depositos_judiciais.index'))
            else:  # salvar_e_continuar
                flash('Depósito judicial incluído com sucesso! Pronto para incluir outro.', 'success')
                return redirect(url_for('depositos_judiciais.inclusao'))

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
    """Página de edição de depósitos judiciais com filtros"""
    try:
        # Capturar filtros da query string
        filtros = {
            'nu_contrato': request.args.get('nu_contrato', '').strip(),
            'id_centro': request.args.get('id_centro', ''),
            'vr_rateio': request.args.get('vr_rateio', ''),
            'dt_identificacao': request.args.get('dt_identificacao', ''),
            'mes_identificacao': request.args.get('mes_identificacao', ''),
            'ano_identificacao': request.args.get('ano_identificacao', ''),
            'dt_siscor': request.args.get('dt_siscor', ''),
            'dt_siscor_status': request.args.get('dt_siscor_status', ''),
            'status': request.args.get('status', '')
        }

        # Query base usando os modelos corretos
        query = db.session.query(
            DepositosSufin,
            ProcessosJudiciais.NR_PROCESSO,
            Area.NO_AREA,
            CentroResultado.NO_CARTEIRA
        ).outerjoin(
            ProcessosJudiciais,
            DepositosSufin.NU_LINHA == ProcessosJudiciais.NU_LINHA
        ).outerjoin(
            CentroResultado,
            DepositosSufin.ID_CENTRO == CentroResultado.ID_CENTRO
        ).outerjoin(
            Area,
            DepositosSufin.ID_AREA == Area.ID_AREA
        )

        # Aplicar filtros
        if filtros['nu_contrato']:
            query = query.filter(DepositosSufin.NU_CONTRATO.like(f"%{filtros['nu_contrato']}%"))

        if filtros['id_centro']:
            query = query.filter(DepositosSufin.ID_CENTRO == filtros['id_centro'])

        if filtros['vr_rateio']:
            try:
                valor = float(filtros['vr_rateio'].replace(',', '.'))
                # Buscar tanto o valor positivo quanto o negativo
                query = query.filter(
                    or_(
                        DepositosSufin.VR_RATEIO == valor,
                        DepositosSufin.VR_RATEIO == -valor
                    )
                )
            except ValueError:
                pass

        # Filtro de STATUS
        if filtros['status']:
            if filtros['status'] == 'em_andamento':
                query = query.filter(DepositosSufin.STATUS == 'Em andamento')
            elif filtros['status'] == 'concluido':
                query = query.filter(DepositosSufin.STATUS == 'Concluído')
            elif filtros['status'] == 'sem_status':
                query = query.filter(
                    or_(
                        DepositosSufin.STATUS.is_(None),
                        DepositosSufin.STATUS == ''
                    )
                )

        # Filtros de data de identificação
        if filtros['dt_identificacao']:
            try:
                data = datetime.strptime(filtros['dt_identificacao'], '%Y-%m-%d')
                query = query.filter(DepositosSufin.DT_IDENTIFICACAO == data)
            except ValueError:
                pass

        if filtros['mes_identificacao']:
            try:
                mes = int(filtros['mes_identificacao'])
                query = query.filter(extract('month', DepositosSufin.DT_IDENTIFICACAO) == mes)
            except ValueError:
                pass

        if filtros['ano_identificacao']:
            try:
                ano = int(filtros['ano_identificacao'])
                query = query.filter(extract('year', DepositosSufin.DT_IDENTIFICACAO) == ano)
            except ValueError:
                pass

        # Filtros de DT_SISCOR
        if filtros['dt_siscor']:
            try:
                data = datetime.strptime(filtros['dt_siscor'], '%Y-%m-%d')
                query = query.filter(DepositosSufin.DT_SISCOR == data)
            except ValueError:
                pass

        if filtros['dt_siscor_status']:
            if filtros['dt_siscor_status'] == 'preenchido':
                query = query.filter(DepositosSufin.DT_SISCOR.isnot(None))
            elif filtros['dt_siscor_status'] == 'vazio':
                query = query.filter(DepositosSufin.DT_SISCOR.is_(None))

        # Ordenar por NU_LINHA decrescente
        resultados = query.order_by(DepositosSufin.NU_LINHA.desc()).all()

        # Preparar dados para o template COM LÓGICA DE CORES
        depositos_lista = []
        for resultado in resultados:
            deposito_obj = resultado[0]
            nr_processo = resultado[1]
            no_area = resultado[2]
            no_carteira = resultado[3]

            # LÓGICA DE MARCAÇÃO COLORIDA
            cor_marcacao = None  # Padrão: sem cor

            # REGRA 1: Vermelho se está Em andamento (PRIORIDADE MÁXIMA)
            if deposito_obj.STATUS == 'Em andamento':
                cor_marcacao = 'vermelho'

            # REGRA 2: Laranja se mês/ano de DT_IDENTIFICACAO diferente de DT_LANCAMENTO_DJ
            elif (deposito_obj.DT_IDENTIFICACAO and deposito_obj.DT_LANCAMENTO_DJ):
                mes_ident = deposito_obj.DT_IDENTIFICACAO.month
                ano_ident = deposito_obj.DT_IDENTIFICACAO.year
                mes_lanc = deposito_obj.DT_LANCAMENTO_DJ.month
                ano_lanc = deposito_obj.DT_LANCAMENTO_DJ.year

                if mes_ident != mes_lanc or ano_ident != ano_lanc:
                    cor_marcacao = 'laranja'

            # REGRA 3: Amarelo se MEMO_SUFIN não nulo E DT_AJUSTE_RM e DT_IDENTIFICACAO nulos
            if cor_marcacao is None:  # Só verifica se ainda não tem cor
                if (deposito_obj.MEMO_SUFIN and
                        deposito_obj.DT_AJUSTE_RM is None and
                        deposito_obj.DT_IDENTIFICACAO is None):
                    cor_marcacao = 'amarelo'

            depositos_lista.append({
                'deposito': deposito_obj,
                'nr_processo': nr_processo,
                'no_area': no_area,
                'no_carteira': no_carteira,
                'cor_marcacao': cor_marcacao
            })

        # Buscar centros para o select
        centros = CentroResultado.query.order_by(CentroResultado.NO_CARTEIRA).all()

        return render_template(
            'depositos_judiciais/edicao.html',
            depositos_lista=depositos_lista,
            centros=centros,
            filtros=filtros
        )

    except Exception as e:
        flash(f'Erro ao consultar depósitos: {str(e)}', 'danger')
        return redirect(url_for('depositos_judiciais.index'))


@depositos_judiciais_bp.route('/api/marcar-em-andamento/<int:nu_linha>', methods=['POST'])
@login_required
def marcar_em_andamento(nu_linha):
    """
    API para marcar um depósito como 'Em andamento'
    Salva automaticamente sem precisar clicar em Salvar
    """
    try:
        from app.models.usuario import Usuario, Empregado

        deposito = DepositosSufin.query.filter_by(NU_LINHA=nu_linha).first()

        if not deposito:
            return jsonify({
                'success': False,
                'message': 'Depósito não encontrado'
            }), 404

        # Verificar se já está em andamento por outro usuário
        if deposito.STATUS == 'Em andamento':
            return jsonify({
                'success': False,
                'message': f'Este contrato já está sendo editado por outro usuário da área {deposito.AREA_STATUS}',
                'area': deposito.AREA_STATUS
            }), 409

        # Obter área do usuário logado - CORRIGIDO
        area_usuario = None

        # Buscar o usuário completo do banco de dados
        usuario = Usuario.query.get(current_user.id)

        if usuario and usuario.FK_PESSOA:
            # Buscar o empregado pela FK_PESSOA
            empregado = Empregado.query.filter_by(pkPessoa=usuario.FK_PESSOA).first()

            if empregado and empregado.sgSuperintendencia:
                area_usuario = empregado.sgSuperintendencia

        # Marcar como em andamento
        deposito.STATUS = 'Em andamento'
        deposito.AREA_STATUS = area_usuario

        db.session.commit()

        # Registrar log
        registrar_log(
            'depositos_judiciais',
            'update',
            f'Depósito NU_LINHA {nu_linha} marcado como Em andamento',
            {'nu_linha': nu_linha, 'area': area_usuario}
        )

        return jsonify({
            'success': True,
            'message': 'Contrato marcado como Em andamento',
            'status': 'Em andamento',
            'area': area_usuario,
            'usuario': usuario.NOME
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Erro ao marcar contrato: {str(e)}'
        }), 500


@depositos_judiciais_bp.route('/api/verificar-status/<int:nu_linha>', methods=['GET'])
@login_required
def verificar_status(nu_linha):
    """
    API para verificar o status atual de um depósito
    Retorna se está em andamento e por qual usuário/área
    """
    try:
        from app.models.usuario import Usuario, Empregado

        deposito = DepositosSufin.query.filter_by(NU_LINHA=nu_linha).first()

        if not deposito:
            return jsonify({
                'success': False,
                'message': 'Depósito não encontrado'
            }), 404

        # Verificar se o usuário atual é o mesmo que está editando
        area_usuario_atual = None
        usuario_atual = Usuario.query.get(current_user.id)

        if usuario_atual and usuario_atual.FK_PESSOA:
            empregado_atual = Empregado.query.filter_by(pkPessoa=usuario_atual.FK_PESSOA).first()
            if empregado_atual and empregado_atual.sgSuperintendencia:
                area_usuario_atual = empregado_atual.sgSuperintendencia

        # Verificar se é a mesma área que está editando
        eh_mesmo_usuario = (deposito.STATUS == 'Em andamento' and
                            deposito.AREA_STATUS == area_usuario_atual)

        return jsonify({
            'success': True,
            'status': deposito.STATUS,
            'area_status': deposito.AREA_STATUS,
            'em_andamento': deposito.STATUS == 'Em andamento',
            'eh_mesmo_usuario': eh_mesmo_usuario  # NOVO
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Erro ao verificar status: {str(e)}'
        }), 500

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

            # Pegar novo centro - OBRIGATÓRIO
            id_centro_str = request.form.get('id_centro')
            if not id_centro_str:
                raise ValueError("Centro de Resultado é obrigatório")
            novo_centro = int(id_centro_str)

            # Buscar informações da carteira antiga e nova
            carteira_antiga = CentroResultado.query.filter_by(
                ID_CENTRO=centro_antigo).first() if centro_antigo else None
            nova_carteira = CentroResultado.query.filter_by(ID_CENTRO=novo_centro).first()

            # Determinar se a carteira antiga ou nova é Institucional
            eh_institucional_antigo = (
                carteira_antiga and carteira_antiga.NO_CARTEIRA == 'Institucional') or centro_antigo == 6
            eh_institucional_novo = (nova_carteira and nova_carteira.NO_CARTEIRA == 'Institucional') or novo_centro == 6

            # Se mudou o centro E a nova carteira NÃO é Institucional E a antiga também NÃO era Institucional
            if novo_centro != centro_antigo and not eh_institucional_novo and not eh_institucional_antigo:
                # PROCESSO ESPECIAL: NÃO ALTERA O ORIGINAL, CRIA ESTORNO E NOVA

                # 1. Buscar próximo NU_LINHA para estorno
                ultimo_nu_linha = db.session.query(func.max(DepositosSufin.NU_LINHA)).scalar()
                proximo_nu_linha_estorno = (ultimo_nu_linha or 0) + 1

                # 2. Criar linha de ESTORNO (valor negativo) na carteira ANTIGA
                estorno = DepositosSufin()
                estorno.NU_LINHA = proximo_nu_linha_estorno
                estorno.LANCAMENTO_RM = request.form.get('lancamento_rm')
                estorno.DT_LANCAMENTO_DJ = datetime.strptime(request.form.get('dt_lancamento_dj'), '%Y-%m-%d')

                # Valor NEGATIVO para estorno
                vr_rateio = request.form.get('vr_rateio')
                vr_rateio_numeros = ''.join(filter(str.isdigit, vr_rateio))
                if vr_rateio_numeros:
                    estorno.VR_RATEIO = -abs(Decimal(vr_rateio_numeros) / 100)

                estorno.MEMO_SUFIN = request.form.get('memo_sufin')

                dt_memo = request.form.get('dt_memo')
                if dt_memo:
                    estorno.DT_MEMO = datetime.strptime(dt_memo, '%Y-%m-%d')
                    estorno.ID_IDENTIFICADO = True
                else:
                    estorno.DT_MEMO = None
                    estorno.ID_IDENTIFICADO = False

                dt_identificacao = request.form.get('dt_identificacao')
                if dt_identificacao:
                    estorno.DT_IDENTIFICACAO = datetime.strptime(dt_identificacao, '%Y-%m-%d')
                else:
                    estorno.DT_IDENTIFICACAO = None

                estorno.ID_AREA = int(request.form.get('id_area')) if request.form.get('id_area') else None
                estorno.ID_CENTRO = centro_antigo  # Carteira ANTIGA

                dt_ajuste_rm = request.form.get('dt_ajuste_rm')
                if dt_ajuste_rm:
                    estorno.DT_AJUSTE_RM = datetime.strptime(dt_ajuste_rm, '%Y-%m-%d')
                    estorno.ID_AJUSTE_RM = True
                else:
                    estorno.DT_AJUSTE_RM = None
                    estorno.ID_AJUSTE_RM = False

                nu_contrato = request.form.get('nu_contrato')
                if nu_contrato:
                    estorno.NU_CONTRATO = int(nu_contrato)
                else:
                    estorno.NU_CONTRATO = None

                estorno.NU_CONTRATO_2 = None

                                # Eventos contábeis (Preenchimento automático)
                # EVENTO_CONTABIL_ANTERIOR sempre 22607
                estorno.EVENTO_CONTABIL_ANTERIOR = 22607

                # EVENTO_CONTABIL_ATUAL baseado no ID_CENTRO (Carteira Antiga)
                id_centro_antigo_int = centro_antigo
                mapeamento_eventos = {
                    1: 22611, 2: 22612, 3: 22610, 4: 22611, 5: 22613, 6: 22607, 7: 22710
                }
                estorno.EVENTO_CONTABIL_ATUAL = mapeamento_eventos.get(id_centro_antigo_int)

                estorno.OBS = request.form.get('obs')
                estorno.IC_APROPRIADO = None

                dt_siscor = request.form.get('dt_siscor')
                if dt_siscor:
                    estorno.DT_SISCOR = datetime.strptime(dt_siscor, '%Y-%m-%d')
                else:
                    estorno.DT_SISCOR = None

                estorno.IC_INCLUIDO_ACERTO = None
                estorno.STATUS = None
                estorno.AREA_STATUS = None

                db.session.add(estorno)

                # Criar processo judicial para estorno se informado
                nr_processo_form = request.form.get('nr_processo')
                if nr_processo_form and nr_processo_form.strip():
                    processo_estorno = ProcessosJudiciais()
                    processo_estorno.NU_LINHA = proximo_nu_linha_estorno
                    processo_estorno.NR_PROCESSO = nr_processo_form.strip()
                    db.session.add(processo_estorno)

                # 3. Buscar próximo NU_LINHA para nova linha
                proximo_nu_linha_nova = proximo_nu_linha_estorno + 1

                # 4. Criar NOVA linha (valor positivo) na carteira NOVA
                nova_linha = DepositosSufin()
                nova_linha.NU_LINHA = proximo_nu_linha_nova
                nova_linha.LANCAMENTO_RM = request.form.get('lancamento_rm')
                nova_linha.DT_LANCAMENTO_DJ = datetime.strptime(request.form.get('dt_lancamento_dj'), '%Y-%m-%d')

                # Valor POSITIVO
                if vr_rateio_numeros:
                    nova_linha.VR_RATEIO = abs(Decimal(vr_rateio_numeros) / 100)

                nova_linha.MEMO_SUFIN = request.form.get('memo_sufin')

                if dt_memo:
                    nova_linha.DT_MEMO = datetime.strptime(dt_memo, '%Y-%m-%d')
                    nova_linha.ID_IDENTIFICADO = True
                else:
                    nova_linha.DT_MEMO = None
                    nova_linha.ID_IDENTIFICADO = False

                if dt_identificacao:
                    nova_linha.DT_IDENTIFICACAO = datetime.strptime(dt_identificacao, '%Y-%m-%d')
                else:
                    nova_linha.DT_IDENTIFICACAO = None

                nova_linha.ID_AREA = int(request.form.get('id_area')) if request.form.get('id_area') else None
                nova_linha.ID_CENTRO = novo_centro  # Carteira NOVA

                if dt_ajuste_rm:
                    nova_linha.DT_AJUSTE_RM = datetime.strptime(dt_ajuste_rm, '%Y-%m-%d')
                    nova_linha.ID_AJUSTE_RM = True
                else:
                    nova_linha.DT_AJUSTE_RM = None
                    nova_linha.ID_AJUSTE_RM = False

                if nu_contrato:
                    nova_linha.NU_CONTRATO = int(nu_contrato)
                else:
                    nova_linha.NU_CONTRATO = None

                nova_linha.NU_CONTRATO_2 = None

                                # Eventos contábeis (Preenchimento automático)
                # EVENTO_CONTABIL_ANTERIOR sempre 22607
                nova_linha.EVENTO_CONTABIL_ANTERIOR = 22607

                # EVENTO_CONTABIL_ATUAL baseado no ID_CENTRO (Carteira Nova)
                id_centro_novo_int = novo_centro
                mapeamento_eventos = {
                    1: 22611, 2: 22612, 3: 22610, 4: 22611, 5: 22613, 6: 22607, 7: 22710
                }
                nova_linha.EVENTO_CONTABIL_ATUAL = mapeamento_eventos.get(id_centro_novo_int)

                nova_linha.OBS = request.form.get('obs')
                nova_linha.IC_APROPRIADO = None

                if dt_siscor:
                    nova_linha.DT_SISCOR = datetime.strptime(dt_siscor, '%Y-%m-%d')
                else:
                    nova_linha.DT_SISCOR = None

                nova_linha.IC_INCLUIDO_ACERTO = None
                nova_linha.STATUS = None
                nova_linha.AREA_STATUS = None

                db.session.add(nova_linha)

                # Criar processo judicial para nova linha se informado
                if nr_processo_form and nr_processo_form.strip():
                    processo_novo = ProcessosJudiciais()
                    processo_novo.NU_LINHA = proximo_nu_linha_nova
                    processo_novo.NR_PROCESSO = nr_processo_form.strip()
                    db.session.add(processo_novo)

                # NÃO ALTERAR O REGISTRO ORIGINAL!

                db.session.commit()

                # Registrar log
                registrar_log(
                    'depositos_judiciais',
                    'update',
                    f'Mudança de carteira - NU_LINHA: {nu_linha} - Criadas linhas {proximo_nu_linha_estorno} (estorno) e {proximo_nu_linha_nova} (nova)',
                    {'nu_linha_original': nu_linha, 'estorno': proximo_nu_linha_estorno, 'nova': proximo_nu_linha_nova}
                )

                flash('Depósito judicial processado com sucesso! Criadas linha de estorno e nova linha.', 'success')
                return redirect(url_for('depositos_judiciais.edicao'))

            else:
                # Não mudou centro OU é Institucional - EDIÇÃO NORMAL
                deposito_obj.LANCAMENTO_RM = request.form.get('lancamento_rm')
                deposito_obj.DT_LANCAMENTO_DJ = datetime.strptime(request.form.get('dt_lancamento_dj'), '%Y-%m-%d')

                # Corrigir conversão do valor rateio (permite valores negativos)
                vr_rateio = request.form.get('vr_rateio')
                # Remove pontos de milhar, mantém vírgula e sinal negativo
                vr_rateio_limpo = vr_rateio.replace('.', '').replace(',', '.')
                if vr_rateio_limpo and vr_rateio_limpo != '-':
                    deposito_obj.VR_RATEIO = Decimal(vr_rateio_limpo)

                deposito_obj.MEMO_SUFIN = request.form.get('memo_sufin')

                dt_memo = request.form.get('dt_memo')
                if dt_memo:
                    deposito_obj.DT_MEMO = datetime.strptime(dt_memo, '%Y-%m-%d')
                    deposito_obj.ID_IDENTIFICADO = True
                else:
                    deposito_obj.DT_MEMO = None
                    deposito_obj.ID_IDENTIFICADO = False

                # MODIFICAÇÃO AQUI: Data de identificação muda status para Concluído
                dt_identificacao = request.form.get('dt_identificacao')
                if dt_identificacao:
                    deposito_obj.DT_IDENTIFICACAO = datetime.strptime(dt_identificacao, '%Y-%m-%d')
                    # NOVA LÓGICA: Quando coloca data de identificação, muda status para Concluído
                    deposito_obj.STATUS = 'Concluído'
                else:
                    deposito_obj.DT_IDENTIFICACAO = None

                deposito_obj.ID_AREA = int(request.form.get('id_area')) if request.form.get('id_area') else None
                deposito_obj.ID_CENTRO = novo_centro

                dt_ajuste_rm = request.form.get('dt_ajuste_rm')
                if dt_ajuste_rm:
                    deposito_obj.DT_AJUSTE_RM = datetime.strptime(dt_ajuste_rm, '%Y-%m-%d')
                    deposito_obj.ID_AJUSTE_RM = True
                else:
                    deposito_obj.DT_AJUSTE_RM = None
                    deposito_obj.ID_AJUSTE_RM = False

                nu_contrato = request.form.get('nu_contrato')
                if nu_contrato:
                    deposito_obj.NU_CONTRATO = int(nu_contrato)
                else:
                    deposito_obj.NU_CONTRATO = None

                deposito_obj.NU_CONTRATO_2 = None

                                # Eventos contábeis (Preenchimento automático)
                # EVENTO_CONTABIL_ANTERIOR sempre 22607
                deposito_obj.EVENTO_CONTABIL_ANTERIOR = 22607

                # EVENTO_CONTABIL_ATUAL baseado no ID_CENTRO (Carteira Atual)
                id_centro_atual_int = novo_centro
                mapeamento_eventos = {
                    1: 22611, 2: 22612, 3: 22610, 4: 22611, 5: 22613, 6: 22607, 7: 22710
                }
                deposito_obj.EVENTO_CONTABIL_ATUAL = mapeamento_eventos.get(id_centro_atual_int)

                deposito_obj.OBS = request.form.get('obs')
                deposito_obj.IC_APROPRIADO = None

                dt_siscor = request.form.get('dt_siscor')
                if dt_siscor:
                    deposito_obj.DT_SISCOR = datetime.strptime(dt_siscor, '%Y-%m-%d')
                else:
                    deposito_obj.DT_SISCOR = None

                deposito_obj.IC_INCLUIDO_ACERTO = None

                # Atualizar processo judicial
                processo = ProcessosJudiciais.query.filter_by(NU_LINHA=nu_linha).first()
                nr_processo_form = request.form.get('nr_processo')

                if nr_processo_form and nr_processo_form.strip():
                    if processo:
                        processo.NR_PROCESSO = nr_processo_form.strip()
                    else:
                        novo_processo = ProcessosJudiciais()
                        novo_processo.NU_LINHA = nu_linha
                        novo_processo.NR_PROCESSO = nr_processo_form.strip()
                        db.session.add(novo_processo)
                else:
                    if processo:
                        db.session.delete(processo)

                db.session.commit()

                # Registrar log
                registrar_log(
                    'depositos_judiciais',
                    'update',
                    f'Depósito editado - NU_LINHA: {nu_linha}',
                    {'nu_linha': nu_linha}
                )

                flash('Depósito judicial atualizado com sucesso!', 'success')
                return redirect(url_for('depositos_judiciais.edicao'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao editar depósito: {str(e)}', 'danger')
            return redirect(url_for('depositos_judiciais.editar', nu_linha=nu_linha))

    # GET - Buscar dados para os dropdowns
    areas = Area.query.order_by(Area.NO_AREA).all()
    centros = CentroResultado.query.order_by(CentroResultado.NO_CARTEIRA).all()

    return render_template('depositos_judiciais/editar.html',
                           deposito=deposito_obj,
                           nr_processo=nr_processo,
                           areas=areas,
                           centros=centros)


@depositos_judiciais_bp.route('/executar-scripts-relatorio', methods=['POST'])
@login_required
def executar_scripts_relatorio():
    """
    Executa os scripts SQL para atualizar relatórios
    Scripts baseados nos arquivos:
    - DPJ_TB007_DJ_RELATORIO.sql
    - DPJ_TB008_COMPARATIVO_SISCOR.sql
    """
    import time

    tempo_inicio = time.time()

    try:
        # =====================================================
        # SCRIPT 1: DPJ_TB007_DJ_RELATORIO
        # =====================================================
        sql_relatorio = """
        DELETE FROM [BDDASHBOARDBI].[BDG].[DPJ_TB007_DJ_RELATORIO];

        INSERT INTO [BDDASHBOARDBI].[BDG].[DPJ_TB007_DJ_RELATORIO] 
        SELECT 
            DPJ.NU_LINHA,
            [DT_LANCAMENTO_DJ],
            ANO_SISCOR = YEAR([DT_SISCOR]),
            [DSC_MES] MES_SISCOR,
            [VR_RATEIO],
            [MEMO_SUFIN],
            [DT_IDENTIFICACAO],
            CT.[NO_CARTEIRA],
            [DT_AJUSTE_RM],
            [NU_CONTRATO],
            [NR_PROCESSO],
            [OBS],
            [IC_APROPRIADO],
            [IC_INCLUIDO_ACERTO],
            DT_SISCOR
        FROM [BDDASHBOARDBI].[BDG].[DPJ_TB004_DEPOSITOS_SUFIN] DPJ 
        INNER JOIN [BDDASHBOARDBI].bdg.DPJ_TB002_CENTRO_RESULTADO CT
            ON DPJ.ID_CENTRO = CT.ID_CENTRO
        LEFT JOIN [BDDASHBOARDBI].[BDG].[DPJ_TB006_PROCESSOS_JUDICIAIS] JU
            ON DPJ.NU_LINHA = JU.NU_LINHA
        LEFT JOIN [BDDASHBOARDBI].BDG.PAR_TB020_CALENDARIO CAL
            ON CAL.DIA = DPJ.DT_SISCOR
        ORDER BY ABS(VR_RATEIO);
        """

        # =====================================================
        # SCRIPT 2: DPJ_TB008_COMPARATIVO_SISCOR
        # =====================================================
        sql_comparativo = """
        DELETE FROM [BDDASHBOARDBI].[BDG].[DPJ_TB008_COMPARATIVO_SISCOR];

        INSERT INTO [BDDASHBOARDBI].BDG.[DPJ_TB008_COMPARATIVO_SISCOR]
        SELECT 
            COR.UNIDADE,
            COR.DT_EXECUCAO_ORCAMENTO DT_SISCOR,
            COR.VLR VR_SISCOR,
            ISNULL(DJ.VR,0) VR_BASE_DJ,
            DIFERENCA = COR.VLR - ISNULL(DJ.VR,0)
        FROM 
        (
            SELECT 
                UNIDADE = CASE  WHEN ID_ITEM IN (1464) THEN 'Indenização-Seguro'
                                WHEN UNIDADE IN ('SUCRE') THEN 'Sucre-DJ'
                                WHEN UNIDADE IN ('SUPEJ') THEN 'Supej'
                                WHEN UNIDADE IN ('SUPEC') THEN 'Supec'
                                WHEN UNIDADE IN ('SUMOV') THEN 'Sumov'
                                ELSE UNIDADE END,
                [DT_EXECUCAO_ORCAMENTO],
                SUM([VR_EXECUCAO_ORCAMENTO]) VLR
            FROM [BDDASHBOARDBI].[BDG].[COR_TB001_EXECUCAO_ORCAMENTARIA_SISCOR]
            WHERE ID_ITEM IN (1432,1473,1471,1470,1472,1464)   --SUPEJ(1471), SUCRE (1470) , SUPEC (1472) , SUMOV (1473)
                AND [ID_NATUREZA] = 3
                AND [VR_EXECUCAO_ORCAMENTO] <> 0
                AND [UNIDADE] NOT IN ('INSTIT')
            GROUP BY
                CASE    WHEN ID_ITEM IN (1464) THEN 'Indenização-Seguro'
                        WHEN UNIDADE IN ('SUCRE') THEN 'Sucre-DJ'
                        WHEN UNIDADE IN ('SUPEJ') THEN 'Supej'
                        WHEN UNIDADE IN ('SUPEC') THEN 'Supec'
                        WHEN UNIDADE IN ('SUMOV') THEN 'Sumov'
                        ELSE UNIDADE END,
                [DT_EXECUCAO_ORCAMENTO]
        ) COR
        LEFT JOIN 
        (
            SELECT 
                UNIDADE = CASE  WHEN ID_CENTRO IN (2) THEN 'Supec'
                                WHEN ID_CENTRO IN (3) THEN 'Sucre-DJ'
                                WHEN ID_CENTRO IN (1,4) THEN 'Supej'
                                WHEN ID_CENTRO IN (5) THEN 'Sumov'
                                WHEN ID_CENTRO IN (7) THEN 'Indenização-Seguro'
                                ELSE NULL END,
                DT_IDENT_SISCOR = (SUBSTRING(CONVERT(VARCHAR(4),[DT_SISCOR]),1,4)+SUBSTRING(CONVERT(VARCHAR(10),[DT_SISCOR]),6,2)),
                SUM(VR_RATEIO) VR
            FROM [BDDASHBOARDBI].BDG.[DPJ_TB004_DEPOSITOS_SUFIN]
            WHERE DT_SISCOR IS NOT NULL AND ID_CENTRO IN (1,2,3,4,5,7)
            GROUP BY
                CASE    WHEN ID_CENTRO IN (2) THEN 'Supec'
                        WHEN ID_CENTRO IN (3) THEN 'Sucre-DJ'
                        WHEN ID_CENTRO IN (1,4) THEN 'Supej'
                        WHEN ID_CENTRO IN (5) THEN 'Sumov'
                        WHEN ID_CENTRO IN (7) THEN 'Indenização-Seguro'
                        ELSE NULL END,
                (SUBSTRING(CONVERT(VARCHAR(4),[DT_SISCOR]),1,4)+SUBSTRING(CONVERT(VARCHAR(10),[DT_SISCOR]),6,2))
        ) DJ
            ON COR.DT_EXECUCAO_ORCAMENTO = DJ.DT_IDENT_SISCOR
            AND COR.UNIDADE = DJ.UNIDADE
        ORDER BY COR.UNIDADE, COR.DT_EXECUCAO_ORCAMENTO;
        """

        # =====================================================
        # SCRIPT 3: DPJ_TB009_ALERTAS_SUFIN
        # =====================================================
        sql_alertas = """
        DELETE [BDDASHBOARDBI].BDG.[DPJ_TB009_ALERTAS_SUFIN];

        INSERT INTO [BDDASHBOARDBI].BDG.[DPJ_TB009_ALERTAS_SUFIN]
        SELECT 
            DJ.NU_LINHA,
            DJ.DT_LANCAMENTO_DJ,
            CT.NO_CENTRO_RESULTADO,
            DJ.DT_AJUSTE_RM,
            DJ.NU_CONTRATO,
            DJ.VR_RATEIO,
            DJ.DT_SISCOR,
            ALERTA = CASE   WHEN DJ.DT_LANCAMENTO_DJ IS NULL THEN 'Dt de Lançamento DJ não informada'
                            WHEN ABS(DJ.VR_RATEIO) IN (50258.25,123131.63) THEN 'Lançado 2 vezes no Siscor'
                            ELSE 'Indício de Duplidade' END
        FROM [BDDASHBOARDBI].BDG.[DPJ_TB004_DEPOSITOS_SUFIN] DJ
        INNER JOIN [BDDASHBOARDBI].BDG.[DPJ_TB002_CENTRO_RESULTADO] CT
            ON DJ.ID_CENTRO = CT.ID_CENTRO
        INNER JOIN 
        (
            SELECT 
                ISNULL(DT_LANCAMENTO_DJ,'') DT_LANCAMENTO_DJ,
                ABS([VR_RATEIO]) QUEBRA_VR,
                SUM([VR_RATEIO]) VR
            FROM [BDDASHBOARDBI].BDG.[DPJ_TB004_DEPOSITOS_SUFIN]
            WHERE ABS([VR_RATEIO]) NOT IN (1120.09,27408.86,41447.54,585.87)
            GROUP BY 
                DT_LANCAMENTO_DJ,
                ABS([VR_RATEIO])
            HAVING ABS([VR_RATEIO]) - SUM([VR_RATEIO]) <> 0
        ) ALERTA
            ON ABS(DJ.VR_RATEIO) = ALERTA.QUEBRA_VR
            AND ISNULL(DJ.DT_LANCAMENTO_DJ,'') = ALERTA.DT_LANCAMENTO_DJ
        ORDER BY ABS(DJ.VR_RATEIO);

        -----
        INSERT INTO [BDDASHBOARDBI].BDG.[DPJ_TB009_ALERTAS_SUFIN]
        SELECT 
            DJ.[NU_LINHA],
            DJ.[DT_LANCAMENTO_DJ],
            CT.[NO_CENTRO_RESULTADO],
            DJ.[DT_AJUSTE_RM],
            DJ.[NU_CONTRATO],
            DJ.[VR_RATEIO],
            DJ.[DT_SISCOR],
            [ALERTA]= 'Áreas diferentes/mesmo contrato'
        FROM [BDDASHBOARDBI].BDG.[DPJ_TB004_DEPOSITOS_SUFIN] DJ
        INNER JOIN BDDASHBOARDBI.BDG.[DPJ_TB002_CENTRO_RESULTADO] CT
            ON DJ.ID_CENTRO = CT.ID_CENTRO
        INNER JOIN 
        (
            SELECT 
                NU_CONTRATO,
                ID_CENTRO,
                sum(DJ.VR_RATEIO) vr
            FROM [BDDASHBOARDBI].BDG.[DPJ_TB004_DEPOSITOS_SUFIN] DJ
            INNER JOIN [BDDASHBOARDBI].[BDG].[AUX_VW001_CONTRATOS_CPF] CTR
                ON DJ.NU_CONTRATO = CTR.NR_CONTRATO
            WHERE 
                NU_CONTRATO IS NOT NULL
                AND NU_CONTRATO NOT IN (101560100203)
                AND NU_CONTRATO <> 0
                AND 
                (
                    (DJ.ID_CENTRO = 2 AND [CARTEIRA] <> 'Comercial PF')
                    OR (DJ.ID_CENTRO = 3 AND [CARTEIRA] NOT IN ('Habitação PF','Imóveis'))
                    OR (DJ.ID_CENTRO = 4 AND [CARTEIRA] NOT LIKE '%PJ')
                )
            GROUP BY NU_CONTRATO, ID_CENTRO
            HAVING SUM(DJ.VR_RATEIO) > 0
        ) CTR
            ON DJ.NU_CONTRATO = CTR.NU_CONTRATO;

        ------AQUI
        INSERT INTO [BDDASHBOARDBI].BDG.[DPJ_TB009_ALERTAS_SUFIN]
        SELECT 
            DJ.[NU_LINHA],
            DJ.[DT_LANCAMENTO_DJ],
            CT.[NO_CENTRO_RESULTADO],
            DJ.[DT_AJUSTE_RM],
            DJ.[NU_CONTRATO],
            DJ.[VR_RATEIO],
            DJ.[DT_SISCOR],
            [ALERTA]= 'Contrato não é EMGEA'
        FROM [BDDASHBOARDBI].BDG.[DPJ_TB004_DEPOSITOS_SUFIN] DJ
        INNER JOIN BDDASHBOARDBI.BDG.[DPJ_TB002_CENTRO_RESULTADO] CT
            ON DJ.ID_CENTRO = CT.ID_CENTRO
        WHERE OBS LIKE '%CONTR%EMGEA%'
            AND DJ.NU_CONTRATO NOT IN (455552166963);

        ----
        INSERT INTO [BDDASHBOARDBI].BDG.[DPJ_TB009_ALERTAS_SUFIN]
        SELECT 
            DJ.[NU_LINHA],
            DJ.[DT_LANCAMENTO_DJ],
            CT.[NO_CENTRO_RESULTADO],
            DJ.[DT_AJUSTE_RM],
            DJ.[NU_CONTRATO],
            DJ.[VR_RATEIO],
            DJ.[DT_SISCOR],
            [ALERTA]= 'Não Apropriado no Siscor'
        FROM [BDDASHBOARDBI].BDG.[DPJ_TB004_DEPOSITOS_SUFIN] DJ
        INNER JOIN BDDASHBOARDBI.BDG.[DPJ_TB002_CENTRO_RESULTADO] CT
            ON DJ.ID_CENTRO = CT.ID_CENTRO
        WHERE DJ.ID_CENTRO NOT IN (6)
            AND DJ.[DT_SISCOR] IS NULL;

        DELETE FROM [BDDASHBOARDBI].BDG.[DPJ_TB009_ALERTAS_SUFIN]
        WHERE NU_LINHA IN (151,101,102,103,1789,3380,3379,3450,229,
        230,231,232,233,234,235,236,237,238,687,688,2272,2273,2494,2495,716,717,1446,1447,3546,3547,1362,3793,3096,3792,3728,3743,3789,
        3841);

        DELETE FROM [BDDASHBOARDBI].BDG.[DPJ_TB009_ALERTAS_SUFIN] 
        WHERE [NU_CONTRATO] IN (SELECT [NU_CONTRATO] FROM [BDDASHBOARDBI].BDG.[DPJ_TB004_DEPOSITOS_SUFIN] WHERE ID_CENTRO = 8);
        """

        # Executar scripts
        db.session.execute(text(sql_relatorio))
        db.session.execute(text(sql_comparativo))
        db.session.execute(text(sql_alertas))
        db.session.commit()

        # Registrar log
        registrar_log(
            'depositos_judiciais',
            'execute_scripts',
            'Scripts de relatório executados com sucesso',
            {}
        )

        tempo_execucao = round(time.time() - tempo_inicio, 2)

        return jsonify({
            'success': True,
            'mensagem': 'Scripts executados com sucesso',
            'tempo_execucao': tempo_execucao
        })

    except Exception as e:
        db.session.rollback()

        # Registrar erro no log
        registrar_log(
            'depositos_judiciais',
            'execute_scripts_error',
            f'Erro ao executar scripts: {str(e)}',
            {}
        )

        return jsonify({
            'success': False,
            'erro': str(e)
        }), 500


@depositos_judiciais_bp.route('/verificar-contratos-invalidos', methods=['POST'])
@login_required
def verificar_contratos_invalidos():
    """Verifica contratos que não existem na tabela de contratos CPF"""
    try:
        # Subquery para verificar contratos que existem em AUX_VW001_CONTRATOS_CPF
        from sqlalchemy import exists

        # Query usando ORM
        query = db.session.query(
            DepositosSufin.NU_LINHA,
            DepositosSufin.NU_CONTRATO,
            DepositosSufin.ID_CENTRO,
            DepositosSufin.OBS,
            CentroResultado.NO_CARTEIRA
        ).outerjoin(
            CentroResultado,
            DepositosSufin.ID_CENTRO == CentroResultado.ID_CENTRO
        ).filter(
            DepositosSufin.NU_CONTRATO.isnot(None),
            DepositosSufin.NU_CONTRATO > 0,
            DepositosSufin.OBS.is_(None)
        ).order_by(
            DepositosSufin.NU_LINHA.desc()
        )

        # Executar query
        resultados = query.all()

        # Filtrar apenas os que não existem em AUX_VW001_CONTRATOS_CPF
        contratos_invalidos = []
        for resultado in resultados:
            # Verificar se existe na tabela de contratos
            sql_check = text("""
                SELECT COUNT(*) 
                FROM BDG.AUX_VW001_CONTRATOS_CPF 
                WHERE NR_CONTRATO = :contrato
            """)

            count = db.session.execute(sql_check, {'contrato': resultado.NU_CONTRATO}).scalar()

            if count == 0:  # Não existe na tabela de contratos
                contratos_invalidos.append({
                    'nu_linha': resultado.NU_LINHA,
                    'nu_contrato': resultado.NU_CONTRATO,
                    'id_centro': resultado.ID_CENTRO,
                    'obs': resultado.OBS,
                    'no_carteira': resultado.NO_CARTEIRA
                })

        # Registrar log
        registrar_log(
            'depositos_judiciais',
            'verify_contracts',
            f'Verificação de contratos executada - {len(contratos_invalidos)} contratos não encontrados',
            {'total': len(contratos_invalidos)}
        )

        return jsonify({
            'success': True,
            'contratos': contratos_invalidos,
            'total': len(contratos_invalidos)
        })

    except Exception as e:
        # Registrar erro no log
        registrar_log(
            'depositos_judiciais',
            'verify_contracts_error',
            f'Erro ao verificar contratos: {str(e)}',
            {}
        )

        return jsonify({
            'success': False,
            'erro': str(e)
        }), 500

@depositos_judiciais_bp.route('/contratos-invalidos')
@login_required
def contratos_invalidos():
    """Página para visualizar contratos inválidos"""
    return render_template('depositos_judiciais/contratos_invalidos.html')


@depositos_judiciais_bp.route('/ratear/<int:nu_linha>', methods=['GET', 'POST'])
@login_required
def ratear(nu_linha):
    """
    Tela e processamento de rateio de valor de um depósito judicial.

    LÓGICA:
    1. GET: Exibe a tela com os campos pré-preenchidos (Lançamento RM, Data Lançamento DJ, Memo SUFIN, Data Memo)
    2. POST: Processa o rateio:
       - Cria um NOVO registro com o valor informado
       - DIMINUI o VR_RATEIO do registro original
       - Se o VR_RATEIO original ficar <= 0, fica com 0 (zero)
    """

    # Buscar o depósito original
    deposito_original = db.session.query(
        DepositosSufin,
        CentroResultado.NO_CARTEIRA
    ).outerjoin(
        CentroResultado,
        DepositosSufin.ID_CENTRO == CentroResultado.ID_CENTRO
    ).filter(
        DepositosSufin.NU_LINHA == nu_linha
    ).first()

    if not deposito_original:
        flash('Depósito não encontrado.', 'danger')
        return redirect(url_for('depositos_judiciais.edicao'))

    deposito_obj = deposito_original[0]
    carteira_original = deposito_original[1]

    # Validar se o depósito tem valor positivo para ratear
    if not deposito_obj.VR_RATEIO or deposito_obj.VR_RATEIO <= 0:
        flash('Este depósito não possui valor disponível para ratear.', 'warning')
        return redirect(url_for('depositos_judiciais.editar', nu_linha=nu_linha))

    if request.method == 'POST':
        try:
            # ============================================
            # ETAPA 1: VALIDAR E COLETAR DADOS DO FORMULÁRIO
            # ============================================

            # Valor a ratear - OBRIGATÓRIO
            vr_rateio_str = request.form.get('vr_rateio')
            if not vr_rateio_str or vr_rateio_str == '0,00':
                raise ValueError("Valor a ratear é obrigatório")

            # Converter valor monetário BR para decimal
            vr_rateio_limpo = vr_rateio_str.replace('.', '').replace(',', '.')
            vr_rateio_novo = float(vr_rateio_limpo)

            # Validar se o valor não ultrapassa o disponível
            if vr_rateio_novo > float(deposito_obj.VR_RATEIO):
                raise ValueError(
                    f"Valor informado ({vr_rateio_novo}) ultrapassa o valor disponível ({deposito_obj.VR_RATEIO})")

            # Centro de Resultado - OBRIGATÓRIO
            id_centro_str = request.form.get('id_centro')
            if not id_centro_str:
                raise ValueError("Centro de Resultado é obrigatório")
            id_centro_novo = int(id_centro_str)

            # ============================================
            # ETAPA 2: BUSCAR PRÓXIMO NU_LINHA
            # ============================================
            ultimo_nu_linha = db.session.query(func.max(DepositosSufin.NU_LINHA)).scalar()
            proximo_nu_linha = (ultimo_nu_linha or 0) + 1

            # ============================================
            # ETAPA 3: CRIAR NOVO REGISTRO COM VALOR RATEADO
            # ============================================
            novo_deposito = DepositosSufin()
            novo_deposito.NU_LINHA = proximo_nu_linha

            # CAMPOS PRÉ-PREENCHIDOS (da tela)
            novo_deposito.LANCAMENTO_RM = deposito_obj.LANCAMENTO_RM
            novo_deposito.DT_LANCAMENTO_DJ = deposito_obj.DT_LANCAMENTO_DJ
            novo_deposito.MEMO_SUFIN = deposito_obj.MEMO_SUFIN
            novo_deposito.DT_MEMO = deposito_obj.DT_MEMO

            # VALOR DO RATEIO (novo valor)
            novo_deposito.VR_RATEIO = vr_rateio_novo

            # CAMPOS PREENCHIDOS PELO USUÁRIO

            # Data Identificação
            dt_identificacao = request.form.get('dt_identificacao')
            if dt_identificacao:
                novo_deposito.DT_IDENTIFICACAO = datetime.strptime(dt_identificacao, '%Y-%m-%d')
                novo_deposito.ID_IDENTIFICADO = True
            else:
                novo_deposito.DT_IDENTIFICACAO = None
                novo_deposito.ID_IDENTIFICADO = False

            # Área
            id_area = request.form.get('id_area')
            novo_deposito.ID_AREA = int(id_area) if id_area else None
            novo_deposito.ID_AREA_2 = None

            # Centro de Resultado
            novo_deposito.ID_CENTRO = id_centro_novo

            # ID_AJUSTE_RM sempre False
            novo_deposito.ID_AJUSTE_RM = False

            # Data Ajuste RM
            dt_ajuste_rm = request.form.get('dt_ajuste_rm')
            if dt_ajuste_rm:
                novo_deposito.DT_AJUSTE_RM = datetime.strptime(dt_ajuste_rm, '%Y-%m-%d')
            else:
                novo_deposito.DT_AJUSTE_RM = None

            # Contrato
            nu_contrato = request.form.get('nu_contrato')
            if nu_contrato:
                novo_deposito.NU_CONTRATO = int(nu_contrato)
            else:
                novo_deposito.NU_CONTRATO = None

            novo_deposito.NU_CONTRATO_2 = None

            # Eventos Contábeis
            evento_anterior = request.form.get('evento_contabil_anterior')
            if evento_anterior:
                novo_deposito.EVENTO_CONTABIL_ANTERIOR = int(evento_anterior)
            else:
                novo_deposito.EVENTO_CONTABIL_ANTERIOR = None

            evento_atual = request.form.get('evento_contabil_atual')
            if evento_atual:
                novo_deposito.EVENTO_CONTABIL_ATUAL = int(evento_atual)
            else:
                novo_deposito.EVENTO_CONTABIL_ATUAL = None

            # Observação
            novo_deposito.OBS = request.form.get('obs')

            # IC_APROPRIADO sempre None
            novo_deposito.IC_APROPRIADO = None

            # Data SISCOR
            dt_siscor = request.form.get('dt_siscor')
            if dt_siscor:
                novo_deposito.DT_SISCOR = datetime.strptime(dt_siscor, '%Y-%m-%d')
            else:
                novo_deposito.DT_SISCOR = None

            # IC_INCLUIDO_ACERTO sempre None
            novo_deposito.IC_INCLUIDO_ACERTO = None

            # Adicionar novo depósito ao banco
            db.session.add(novo_deposito)

            # ============================================
            # ETAPA 4: CRIAR PROCESSO JUDICIAL SE INFORMADO
            # ============================================
            nr_processo = request.form.get('nr_processo')
            if nr_processo and nr_processo.strip():
                novo_processo = ProcessosJudiciais()
                novo_processo.NU_LINHA = proximo_nu_linha
                novo_processo.NR_PROCESSO = nr_processo.strip()
                db.session.add(novo_processo)

            # ============================================
            # ETAPA 5: DIMINUIR VALOR DO DEPÓSITO ORIGINAL
            # ============================================
            valor_original_antigo = float(deposito_obj.VR_RATEIO)
            valor_original_novo = valor_original_antigo - vr_rateio_novo

            # Se o valor ficar negativo ou zero, garantir que fique exatamente 0
            if valor_original_novo <= 0:
                deposito_obj.VR_RATEIO = 0
            else:
                deposito_obj.VR_RATEIO = valor_original_novo

            # ============================================
            # ETAPA 6: COMMIT NO BANCO DE DADOS
            # ============================================
            db.session.commit()

            # ============================================
            # ETAPA 7: REGISTRAR LOG DE AUDITORIA
            # ============================================
            registrar_log(
                'depositos_judiciais',
                'rateio',
                f'Rateio realizado - NU_LINHA original: {nu_linha}, novo NU_LINHA: {proximo_nu_linha}, valor rateado: R$ {vr_rateio_novo:.2f}',
                {
                    'nu_linha_original': nu_linha,
                    'nu_linha_novo': proximo_nu_linha,
                    'valor_rateado': vr_rateio_novo,
                    'valor_original_antigo': valor_original_antigo,
                    'valor_original_novo': float(deposito_obj.VR_RATEIO)
                }
            )

            # Mensagem de sucesso
            flash(
                f'Rateio realizado com sucesso! '
                f'Novo depósito criado (Nº Linha: {proximo_nu_linha}) com valor R$ {vr_rateio_novo:.2f}. '
                f'Depósito original agora tem valor R$ {float(deposito_obj.VR_RATEIO):.2f}.',
                'success'
            )

            return redirect(url_for('depositos_judiciais.edicao'))

        except ValueError as ve:
            db.session.rollback()
            flash(f'Erro de validação: {str(ve)}', 'danger')
            return redirect(url_for('depositos_judiciais.ratear', nu_linha=nu_linha))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao processar rateio: {str(e)}', 'danger')
            return redirect(url_for('depositos_judiciais.ratear', nu_linha=nu_linha))

    # GET - Buscar dados para os dropdowns
    areas = Area.query.order_by(Area.NO_AREA).all()
    centros = CentroResultado.query.order_by(CentroResultado.NO_CARTEIRA).all()

    return render_template('depositos_judiciais/ratear.html',
                           deposito_original=deposito_obj,
                           carteira_original=carteira_original,
                           areas=areas,
                           centros=centros)


@depositos_judiciais_bp.route('/ratear-multiplo/<int:nu_linha>', methods=['GET', 'POST'])
@login_required
def ratear_multiplo(nu_linha):
    """
    Ratear o valor de um depósito para MÚLTIPLOS contratos
    O usuário só consegue finalizar quando distribuir TODO o valor
    """
    # Buscar o depósito original
    deposito_obj = DepositosSufin.query.filter_by(NU_LINHA=nu_linha).first()

    if not deposito_obj:
        flash('Depósito não encontrado.', 'danger')
        return redirect(url_for('depositos_judiciais.edicao'))

    # Buscar carteira original
    carteira_original = CentroResultado.query.filter_by(ID_CENTRO=deposito_obj.ID_CENTRO).first()
    carteira_original = carteira_original.NO_CARTEIRA if carteira_original else 'Não informada'

    if request.method == 'POST':
        try:
            import json

            # Receber JSON com todas as linhas de rateio
            rateios_json = request.form.get('rateios_data')
            if not rateios_json:
                raise ValueError("Nenhum rateio informado")

            rateios = json.loads(rateios_json)

            if not rateios or len(rateios) == 0:
                raise ValueError("É necessário informar pelo menos um rateio")

            # Validar valor total
            valor_original = float(deposito_obj.VR_RATEIO)
            valor_total_rateado = sum(float(r['vr_rateio'].replace('.', '').replace(',', '.')) for r in rateios)

            if abs(valor_total_rateado - valor_original) > 0.01:  # Tolerância de 1 centavo
                raise ValueError(
                    f"A soma dos rateios (R$ {valor_total_rateado:.2f}) deve ser igual ao valor original (R$ {valor_original:.2f})"
                )

            # Buscar próximo NU_LINHA disponível
            ultimo_nu_linha = db.session.query(func.max(DepositosSufin.NU_LINHA)).scalar()
            proximo_nu_linha = (ultimo_nu_linha or 0) + 1

            # Criar cada linha de rateio
            linhas_criadas = []
            for idx, rateio in enumerate(rateios):
                novo_deposito = DepositosSufin()
                novo_deposito.NU_LINHA = proximo_nu_linha + idx

                # Copiar campos do original
                novo_deposito.LANCAMENTO_RM = deposito_obj.LANCAMENTO_RM
                novo_deposito.DT_LANCAMENTO_DJ = deposito_obj.DT_LANCAMENTO_DJ
                novo_deposito.MEMO_SUFIN = deposito_obj.MEMO_SUFIN
                novo_deposito.DT_MEMO = deposito_obj.DT_MEMO
                novo_deposito.ID_IDENTIFICADO = deposito_obj.ID_IDENTIFICADO

                # Campos específicos do rateio
                vr_rateio_str = rateio['vr_rateio'].replace('.', '').replace(',', '.')
                novo_deposito.VR_RATEIO = float(vr_rateio_str)

                # Área
                id_area = rateio.get('id_area')
                if id_area:
                    novo_deposito.ID_AREA = int(id_area)

                # Centro
                id_centro = rateio.get('id_centro')
                if id_centro:
                    novo_deposito.ID_CENTRO = int(id_centro)

                # Data identificação
                dt_identificacao = rateio.get('dt_identificacao')
                if dt_identificacao:
                    novo_deposito.DT_IDENTIFICACAO = datetime.strptime(dt_identificacao, '%Y-%m-%d')

                # Data ajuste RM
                dt_ajuste_rm = rateio.get('dt_ajuste_rm')
                if dt_ajuste_rm:
                    novo_deposito.DT_AJUSTE_RM = datetime.strptime(dt_ajuste_rm, '%Y-%m-%d')
                    novo_deposito.ID_AJUSTE_RM = True
                else:
                    novo_deposito.ID_AJUSTE_RM = False

                # Contrato
                nu_contrato = rateio.get('nu_contrato')
                if nu_contrato:
                    novo_deposito.NU_CONTRATO = int(nu_contrato)

                # Eventos contábeis
                evento_anterior = rateio.get('evento_contabil_anterior')
                if evento_anterior:
                    novo_deposito.EVENTO_CONTABIL_ANTERIOR = int(evento_anterior)

                evento_atual = rateio.get('evento_contabil_atual')
                if evento_atual:
                    novo_deposito.EVENTO_CONTABIL_ATUAL = int(evento_atual)

                # Observação
                novo_deposito.OBS = rateio.get('obs')

                # Data SISCOR
                dt_siscor = rateio.get('dt_siscor')
                if dt_siscor:
                    novo_deposito.DT_SISCOR = datetime.strptime(dt_siscor, '%Y-%m-%d')

                # Campos que sempre são None/False
                novo_deposito.NU_CONTRATO_2 = None
                novo_deposito.IC_APROPRIADO = None
                novo_deposito.IC_INCLUIDO_ACERTO = None
                novo_deposito.STATUS = None
                novo_deposito.AREA_STATUS = None

                db.session.add(novo_deposito)
                linhas_criadas.append(proximo_nu_linha + idx)

                # Criar processo judicial se informado
                nr_processo = rateio.get('nr_processo')
                if nr_processo and nr_processo.strip():
                    novo_processo = ProcessosJudiciais()
                    novo_processo.NU_LINHA = proximo_nu_linha + idx
                    novo_processo.NR_PROCESSO = nr_processo.strip()
                    db.session.add(novo_processo)

            # ZERAR o depósito original
            deposito_obj.VR_RATEIO = 0

            db.session.commit()

            # Registrar log
            registrar_log(
                'depositos_judiciais',
                'rateio_multiplo',
                f'Rateio múltiplo realizado - NU_LINHA original: {nu_linha}, criadas {len(linhas_criadas)} linhas',
                {
                    'nu_linha_original': nu_linha,
                    'linhas_criadas': linhas_criadas,
                    'quantidade': len(linhas_criadas),
                    'valor_total': valor_total_rateado
                }
            )

            flash(
                f'Rateio múltiplo realizado com sucesso! Criadas {len(linhas_criadas)} novas linhas. '
                f'Depósito original zerado.',
                'success'
            )
            return redirect(url_for('depositos_judiciais.edicao'))

        except ValueError as ve:
            db.session.rollback()
            flash(f'Erro de validação: {str(ve)}', 'danger')

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao processar rateio múltiplo: {str(e)}', 'danger')

    # GET - Buscar dados para os dropdowns
    areas = Area.query.order_by(Area.NO_AREA).all()
    centros = CentroResultado.query.order_by(CentroResultado.NO_CARTEIRA).all()

    return render_template('depositos_judiciais/ratear_multiplo.html',
                           deposito_original=deposito_obj,
                           carteira_original=carteira_original,
                           areas=areas,
                           centros=centros)