from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from app import db
from app.models.depositos_judiciais import DepositosSufin, Area, CentroResultado, ProcessosJudiciais
from app.utils.audit import registrar_log
from datetime import datetime
from sqlalchemy import func, or_, and_, extract
from decimal import Decimal
from sqlalchemy import text
from app.models.depositos_judiciais import DepositosSufin, DepositosSufinExclusao, Area, CentroResultado, ProcessosJudiciais


depositos_judiciais_bp = Blueprint('depositos_judiciais', __name__, url_prefix='/depositos-judiciais')

def obter_proximo_nu_linha():
    """
    Obtém o próximo NU_LINHA disponível verificando AMBAS as tabelas
    para evitar conflitos de chave primária.
    """
    max_depositos = db.session.query(func.max(DepositosSufin.NU_LINHA)).scalar() or 0
    max_processos = db.session.query(func.max(ProcessosJudiciais.NU_LINHA)).scalar() or 0
    return max(max_depositos, max_processos) + 1

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
    """
    Página de inclusão de novos depósitos judiciais.

    LÓGICA:
    - A inclusão é SEMPRE feita na carteira "Institucional" (não pode ser alterada).
    - Campos não exibidos no formulário (Data Identificação, Data Ajuste RM,
      Data SISCOR, Número Contrato, Número do Processo) ficam NULL no banco.
    - Antes de salvar, valida se já existe outro depósito com o mesmo Lançamento RM.
    - EVENTO_CONTABIL_ANTERIOR e EVENTO_CONTABIL_ATUAL ficam fixos em 22607
      (código contábil da carteira Institucional / Pendência de Depósito Judicial).
      Esses valores são mostrados na tela (readonly) e gravados no backend.
    """

    EVENTO_INSTITUCIONAL_CODIGO = 22607
    EVENTO_INSTITUCIONAL_DESCRICAO = 'PENDÊNCIA DE DEPÓSITO JUDICIAL'

    centro_institucional = CentroResultado.query.filter(
        CentroResultado.NO_CARTEIRA == 'Institucional'
    ).first()

    if not centro_institucional:
        flash('Carteira "Institucional" não encontrada no cadastro. '
              'Contate o administrador do sistema.', 'danger')
        return redirect(url_for('depositos_judiciais.index'))

    if request.method == 'POST':
        try:
            lancamento_rm = (request.form.get('lancamento_rm') or '').strip()
            if not lancamento_rm:
                flash('O campo Lançamento RM é obrigatório.', 'danger')
                return redirect(url_for('depositos_judiciais.inclusao'))

            existente = DepositosSufin.query.filter(
                func.rtrim(DepositosSufin.LANCAMENTO_RM) == lancamento_rm
            ).first()

            if existente:
                flash(
                    f'Depósito não incluído: já existe um registro com o Lançamento RM '
                    f'"{lancamento_rm}" (Nº Linha: {existente.NU_LINHA}).',
                    'warning'
                )
                return redirect(url_for('depositos_judiciais.inclusao'))

            proximo_nu_linha = obter_proximo_nu_linha()

            novo_deposito = DepositosSufin()
            novo_deposito.NU_LINHA = proximo_nu_linha
            novo_deposito.LANCAMENTO_RM = lancamento_rm

            dt_lancamento_dj = request.form.get('dt_lancamento_dj')
            if not dt_lancamento_dj:
                raise ValueError("Data Lançamento DJ é obrigatória")
            novo_deposito.DT_LANCAMENTO_DJ = datetime.strptime(dt_lancamento_dj, '%Y-%m-%d')

            vr_rateio = request.form.get('vr_rateio') or ''
            vr_rateio_limpo = vr_rateio.replace('.', '').replace(',', '.')
            if vr_rateio_limpo and vr_rateio_limpo not in ('-', ''):
                novo_deposito.VR_RATEIO = Decimal(vr_rateio_limpo)

            novo_deposito.MEMO_SUFIN = request.form.get('memo_sufin')

            dt_memo = request.form.get('dt_memo')
            if dt_memo:
                novo_deposito.DT_MEMO = datetime.strptime(dt_memo, '%Y-%m-%d')
                novo_deposito.ID_IDENTIFICADO = True
            else:
                novo_deposito.DT_MEMO = None
                novo_deposito.ID_IDENTIFICADO = False

            novo_deposito.DT_IDENTIFICACAO = None
            novo_deposito.DT_AJUSTE_RM = None
            novo_deposito.DT_SISCOR = None
            novo_deposito.NU_CONTRATO = None
            novo_deposito.NU_CONTRATO_2 = None
            novo_deposito.ID_AJUSTE_RM = False

            novo_deposito.ID_AREA = int(request.form.get('id_area')) if request.form.get('id_area') else None
            novo_deposito.ID_AREA_2 = None

            novo_deposito.ID_CENTRO = centro_institucional.ID_CENTRO

            novo_deposito.EVENTO_CONTABIL_ANTERIOR = EVENTO_INSTITUCIONAL_CODIGO
            novo_deposito.EVENTO_CONTABIL_ATUAL = EVENTO_INSTITUCIONAL_CODIGO

            novo_deposito.OBS = request.form.get('obs')

            novo_deposito.IC_APROPRIADO = None
            novo_deposito.IC_INCLUIDO_ACERTO = None

            db.session.add(novo_deposito)
            db.session.commit()

            registrar_log(
                'depositos_judiciais',
                'create',
                f'Novo depósito incluído - NU_LINHA: {proximo_nu_linha}',
                {'nu_linha': proximo_nu_linha}
            )

            if 'salvar_e_sair' in request.form:
                flash('Depósito judicial incluído com sucesso!', 'success')
                return redirect(url_for('depositos_judiciais.index'))
            else:
                flash('Depósito judicial incluído com sucesso! Pronto para incluir outro.', 'success')
                return redirect(url_for('depositos_judiciais.inclusao'))

        except ValueError as ve:
            db.session.rollback()
            flash(f'Erro de validação: {str(ve)}', 'danger')
            return redirect(url_for('depositos_judiciais.inclusao'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao incluir depósito: {str(e)}', 'danger')
            return redirect(url_for('depositos_judiciais.inclusao'))

    # GET - Buscar dados para os dropdowns e tabela de últimos incluídos
    areas = Area.query.order_by(Area.NO_AREA).all()

    ultimos_incluidos = (
        DepositosSufin.query
        .order_by(DepositosSufin.NU_LINHA.desc())
        .limit(10)
        .all()
    )

    areas_dict = {area.ID_AREA: area.NO_AREA for area in areas}

    return render_template('depositos_judiciais/inclusao.html',
                           areas=areas,
                           centro_institucional=centro_institucional,
                           evento_contabil_codigo=EVENTO_INSTITUCIONAL_CODIGO,
                           evento_contabil_descricao=EVENTO_INSTITUCIONAL_DESCRICAO,
                           ultimos_incluidos=ultimos_incluidos,
                           areas_dict=areas_dict)


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
            'nr_processo': request.args.get('nr_processo', '').strip(),
            'id_centro': request.args.get('id_centro', ''),
            'vr_rateio': request.args.get('vr_rateio', ''),
            'dt_identificacao': request.args.get('dt_identificacao', ''),
            'mes_identificacao': request.args.get('mes_identificacao', ''),
            'ano_identificacao': request.args.get('ano_identificacao', ''),
            'dt_lancamento': request.args.get('dt_lancamento', ''),
            'mes_lancamento': request.args.get('mes_lancamento', ''),
            'ano_lancamento': request.args.get('ano_lancamento', ''),
            'dt_lancamento_inicio': request.args.get('dt_lancamento_inicio', ''),
            'dt_lancamento_fim': request.args.get('dt_lancamento_fim', ''),
            'dt_siscor': request.args.get('dt_siscor', ''),
            'dt_siscor_status': request.args.get('dt_siscor_status', ''),
            'status': request.args.get('status', '')
        }

        # NOVO: Dicionário só com os filtros realmente preenchidos,
        # usado para propagar via URL sem poluir com parâmetros vazios
        filtros_ativos = {k: v for k, v in filtros.items() if v}

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

        if filtros['nr_processo']:
            query = query.filter(ProcessosJudiciais.NR_PROCESSO.like(f"%{filtros['nr_processo']}%"))

        if filtros['id_centro']:
            query = query.filter(DepositosSufin.ID_CENTRO == filtros['id_centro'])

        if filtros['vr_rateio']:
            try:
                valor = float(filtros['vr_rateio'].replace(',', '.'))
                query = query.filter(
                    or_(
                        DepositosSufin.VR_RATEIO == valor,
                        DepositosSufin.VR_RATEIO == -valor
                    )
                )
            except ValueError:
                pass

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

        # Filtros de data de LANÇAMENTO DJ
        if filtros['dt_lancamento']:
            try:
                data = datetime.strptime(filtros['dt_lancamento'], '%Y-%m-%d')
                query = query.filter(DepositosSufin.DT_LANCAMENTO_DJ == data)
            except ValueError:
                pass

        if filtros['mes_lancamento']:
            try:
                mes = int(filtros['mes_lancamento'])
                query = query.filter(extract('month', DepositosSufin.DT_LANCAMENTO_DJ) == mes)
            except ValueError:
                pass

        if filtros['ano_lancamento']:
            try:
                ano = int(filtros['ano_lancamento'])
                query = query.filter(extract('year', DepositosSufin.DT_LANCAMENTO_DJ) == ano)
            except ValueError:
                pass

        if filtros['dt_lancamento_inicio']:
            try:
                data_inicio = datetime.strptime(filtros['dt_lancamento_inicio'], '%Y-%m-%d')
                query = query.filter(DepositosSufin.DT_LANCAMENTO_DJ >= data_inicio)
            except ValueError:
                pass

        if filtros['dt_lancamento_fim']:
            try:
                data_fim = datetime.strptime(filtros['dt_lancamento_fim'], '%Y-%m-%d')
                query = query.filter(DepositosSufin.DT_LANCAMENTO_DJ <= data_fim)
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

            cor_marcacao = None

            if deposito_obj.STATUS == 'Em andamento':
                cor_marcacao = 'vermelho'
            elif (deposito_obj.DT_IDENTIFICACAO and deposito_obj.DT_LANCAMENTO_DJ):
                mes_ident = deposito_obj.DT_IDENTIFICACAO.month
                ano_ident = deposito_obj.DT_IDENTIFICACAO.year
                mes_lanc = deposito_obj.DT_LANCAMENTO_DJ.month
                ano_lanc = deposito_obj.DT_LANCAMENTO_DJ.year
                if mes_ident != mes_lanc or ano_ident != ano_lanc:
                    cor_marcacao = 'laranja'

            if cor_marcacao is None:
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

        centros = CentroResultado.query.order_by(CentroResultado.NO_CARTEIRA).all()

        return render_template(
            'depositos_judiciais/edicao.html',
            depositos_lista=depositos_lista,
            centros=centros,
            filtros=filtros,
            filtros_ativos=filtros_ativos  # NOVO: usado para propagar filtros
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

    # NOVO: Capturar filtros da URL para propagar no redirect
    # (assim os filtros aplicados na /edicao são preservados após salvar)
    filtros_ativos = {k: v for k, v in request.args.items() if v}

    # Mapeamento dos eventos contábeis por ID_CENTRO
    EVENTOS_POR_CENTRO = {
        1: {'codigo': 22611, 'descricao': 'LEVANTAMENTO DE DEPÓSITO JUDICIAL PJ'},
        2: {'codigo': 22612, 'descricao': 'LEVANTAMENTO DE DEPÓSITO JUD. COMERCIAL'},
        3: {'codigo': 22610, 'descricao': 'LEVANTAMENTO DE DEPÓSITO JUDICIAL PF'},
        4: {'codigo': 22611, 'descricao': 'LEVANTAMENTO DE DEPÓSITO JUDICIAL PJ'},
        5: {'codigo': 22613, 'descricao': 'LEVANTAMENTO DE DEPÓSITO JUD. IMÓVEIS'},
        6: {'codigo': 22607, 'descricao': 'PENDÊNCIA DE DEPÓSITO JUDICIAL'},
        7: {'codigo': 22710, 'descricao': 'EVENTO CONTÁBIL 22710'}
    }

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
        # NOVO: redireciona preservando filtros
        return redirect(url_for('depositos_judiciais.edicao', **filtros_ativos))

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

            eh_institucional_antigo = (
                carteira_antiga and carteira_antiga.NO_CARTEIRA == 'Institucional') or centro_antigo == 6
            eh_institucional_novo = (nova_carteira and nova_carteira.NO_CARTEIRA == 'Institucional') or novo_centro == 6

            if novo_centro != centro_antigo and not eh_institucional_antigo:
                # PROCESSO ESPECIAL: NÃO ALTERA O ORIGINAL, CRIA ESTORNO E NOVA
                proximo_nu_linha_estorno = obter_proximo_nu_linha()

                # Criar linha de ESTORNO (valor negativo) na carteira ANTIGA
                estorno = DepositosSufin()
                estorno.NU_LINHA = proximo_nu_linha_estorno
                estorno.LANCAMENTO_RM = request.form.get('lancamento_rm')
                estorno.DT_LANCAMENTO_DJ = datetime.strptime(request.form.get('dt_lancamento_dj'), '%Y-%m-%d')

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
                estorno.EVENTO_CONTABIL_ANTERIOR = 22607
                evento_antigo = EVENTOS_POR_CENTRO.get(centro_antigo)
                estorno.EVENTO_CONTABIL_ATUAL = evento_antigo['codigo'] if evento_antigo else None
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

                nr_processo_form = request.form.get('nr_processo')
                if nr_processo_form and nr_processo_form.strip():
                    processo_estorno = ProcessosJudiciais()
                    processo_estorno.NU_LINHA = proximo_nu_linha_estorno
                    processo_estorno.NR_PROCESSO = nr_processo_form.strip()
                    db.session.add(processo_estorno)

                proximo_nu_linha_nova = proximo_nu_linha_estorno + 1

                # Criar NOVA linha (valor positivo) na carteira NOVA
                nova_linha = DepositosSufin()
                nova_linha.NU_LINHA = proximo_nu_linha_nova
                nova_linha.LANCAMENTO_RM = request.form.get('lancamento_rm')
                nova_linha.DT_LANCAMENTO_DJ = datetime.strptime(request.form.get('dt_lancamento_dj'), '%Y-%m-%d')

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
                nova_linha.ID_CENTRO = novo_centro

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
                nova_linha.EVENTO_CONTABIL_ANTERIOR = 22607
                evento_novo = EVENTOS_POR_CENTRO.get(novo_centro)
                nova_linha.EVENTO_CONTABIL_ATUAL = evento_novo['codigo'] if evento_novo else None
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

                if nr_processo_form and nr_processo_form.strip():
                    processo_novo = ProcessosJudiciais()
                    processo_novo.NU_LINHA = proximo_nu_linha_nova
                    processo_novo.NR_PROCESSO = nr_processo_form.strip()
                    db.session.add(processo_novo)

                db.session.commit()

                registrar_log(
                    'depositos_judiciais',
                    'update',
                    f'Mudança de carteira - NU_LINHA: {nu_linha} - Criadas linhas {proximo_nu_linha_estorno} (estorno) e {proximo_nu_linha_nova} (nova)',
                    {'nu_linha_original': nu_linha, 'estorno': proximo_nu_linha_estorno, 'nova': proximo_nu_linha_nova}
                )

                flash('Depósito judicial processado com sucesso! Criadas linha de estorno e nova linha.', 'success')
                # NOVO: redirect com filtros preservados
                return redirect(url_for('depositos_judiciais.edicao', **filtros_ativos))

            else:
                # Não mudou centro OU é Institucional - EDIÇÃO NORMAL
                deposito_obj.LANCAMENTO_RM = request.form.get('lancamento_rm')
                deposito_obj.DT_LANCAMENTO_DJ = datetime.strptime(request.form.get('dt_lancamento_dj'), '%Y-%m-%d')

                vr_rateio = request.form.get('vr_rateio')
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

                dt_identificacao = request.form.get('dt_identificacao')
                if dt_identificacao:
                    deposito_obj.DT_IDENTIFICACAO = datetime.strptime(dt_identificacao, '%Y-%m-%d')
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
                deposito_obj.EVENTO_CONTABIL_ANTERIOR = 22607
                evento_atual = EVENTOS_POR_CENTRO.get(novo_centro)
                deposito_obj.EVENTO_CONTABIL_ATUAL = evento_atual['codigo'] if evento_atual else None
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

                registrar_log(
                    'depositos_judiciais',
                    'update',
                    f'Depósito editado - NU_LINHA: {nu_linha}',
                    {'nu_linha': nu_linha}
                )

                flash('Depósito judicial atualizado com sucesso!', 'success')
                # NOVO: redirect com filtros preservados
                return redirect(url_for('depositos_judiciais.edicao', **filtros_ativos))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao editar depósito: {str(e)}', 'danger')
            # NOVO: redirect com filtros preservados também em caso de erro
            return redirect(url_for('depositos_judiciais.editar', nu_linha=nu_linha, **filtros_ativos))

    # GET - Buscar dados para os dropdowns
    areas = Area.query.order_by(Area.NO_AREA).all()
    centros = CentroResultado.query.order_by(CentroResultado.NO_CARTEIRA).all()

    # Verificar se o depósito é institucional para exibir botão Excluir
    carteira = CentroResultado.query.filter_by(ID_CENTRO=deposito_obj.ID_CENTRO).first()
    is_institucional = carteira is not None and carteira.NO_CARTEIRA == 'Institucional'

    return render_template('depositos_judiciais/editar.html',
                           deposito=deposito_obj,
                           nr_processo=nr_processo,
                           areas=areas,
                           centros=centros,
                           eventos_por_centro=EVENTOS_POR_CENTRO,
                           evento_anterior_codigo=22607,
                           evento_anterior_descricao='PENDÊNCIA DE DEPÓSITO JUDICIAL',
                           filtros_ativos=filtros_ativos,
                           is_institucional=is_institucional)

@depositos_judiciais_bp.route('/excluir/<int:nu_linha>', methods=['POST'])
@login_required
def excluir(nu_linha):
    """
    Transfere um depósito institucional da tabela principal
    para DPJ_TB010_DEPOSITOS_SUFIN_EXCLUSAO e o remove fisicamente da original.

    LÓGICA:
    - Só permite exclusão se o depósito pertencer à carteira Institucional.
    - Em uma única transação: INSERT na TB010 + DELETE na TB004.
    - Registra log de auditoria.
    """
    try:
        deposito = DepositosSufin.query.filter_by(NU_LINHA=nu_linha).first()

        if not deposito:
            return jsonify({'success': False, 'erro': 'Depósito não encontrado.'}), 404

        dados = request.get_json() or {}
        obs_exclusao = (dados.get('obs') or '').strip()
        if not obs_exclusao:
            return jsonify({'success': False, 'erro': 'O motivo da exclusão é obrigatório.'}), 400

        # Segurança: confirmar que é institucional antes de excluir
        carteira = CentroResultado.query.filter_by(ID_CENTRO=deposito.ID_CENTRO).first()
        if not carteira or carteira.NO_CARTEIRA != 'Institucional':
            return jsonify({
                'success': False,
                'erro': 'Exclusão permitida somente para depósitos da carteira Institucional.'
            }), 403

        # Copiar para a tabela de exclusão
        excluido = DepositosSufinExclusao()
        excluido.NU_LINHA                 = deposito.NU_LINHA
        excluido.LANCAMENTO_RM            = deposito.LANCAMENTO_RM
        excluido.DT_LANCAMENTO_DJ         = deposito.DT_LANCAMENTO_DJ
        excluido.VR_RATEIO                = deposito.VR_RATEIO
        excluido.MEMO_SUFIN               = deposito.MEMO_SUFIN
        excluido.DT_MEMO                  = deposito.DT_MEMO
        excluido.ID_IDENTIFICADO          = deposito.ID_IDENTIFICADO
        excluido.DT_IDENTIFICACAO         = deposito.DT_IDENTIFICACAO
        excluido.ID_AREA                  = deposito.ID_AREA
        excluido.ID_AREA_2                = deposito.ID_AREA_2
        excluido.ID_CENTRO                = deposito.ID_CENTRO
        excluido.ID_AJUSTE_RM             = deposito.ID_AJUSTE_RM
        excluido.DT_AJUSTE_RM             = deposito.DT_AJUSTE_RM
        excluido.NU_CONTRATO              = deposito.NU_CONTRATO
        excluido.NU_CONTRATO_2            = deposito.NU_CONTRATO_2
        excluido.EVENTO_CONTABIL_ANTERIOR = deposito.EVENTO_CONTABIL_ANTERIOR
        excluido.EVENTO_CONTABIL_ATUAL    = deposito.EVENTO_CONTABIL_ATUAL
        excluido.OBS                      = obs_exclusao
        excluido.IC_APROPRIADO            = deposito.IC_APROPRIADO
        excluido.DT_SISCOR                = deposito.DT_SISCOR
        excluido.IC_INCLUIDO_ACERTO       = deposito.IC_INCLUIDO_ACERTO
        excluido.STATUS                   = deposito.STATUS
        excluido.AREA_STATUS              = deposito.AREA_STATUS

        db.session.add(excluido)
        db.session.delete(deposito)
        db.session.commit()

        registrar_log(
            'depositos_judiciais',
            'delete',
            f'Depósito institucional excluído - NU_LINHA: {nu_linha}',
            {'nu_linha': nu_linha}
        )

        return jsonify({'success': True, 'mensagem': f'Depósito Nº {nu_linha} excluído com sucesso.'})

    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'erro': str(e)}), 500


@depositos_judiciais_bp.route('/executar-scripts-relatorio', methods=['POST'])
@login_required
def executar_scripts_relatorio():
    """
    Executa os scripts SQL para atualizar relatórios
    Scripts baseados nos arquivos:
    - DPJ_TB007_DJ_RELATORIO.sql
    - DPJ_TB008_COMPARATIVO_SISCOR.sql
    - DPJ_TB009_ALERTAS_SUFIN.sql

    LÓGICA:
    1. Deleta registros existentes de cada tabela de relatório
    2. Insere novos dados processados com JOINs e cálculos
    3. Aplica regras de negócio e exclusões específicas
    4. Retorna tempo de execução e status

    REGRA DE PRIORIDADE DOS ALERTAS (DPJ_TB009):
    - ALERTA 1: Indícios de Duplicidade (inserido primeiro, sem filtro de deduplicação).
    - ALERTA 2: Áreas diferentes/mesmo contrato (sem filtro de deduplicação).
    - ALERTA 3: Contrato não é EMGEA (sem filtro de deduplicação).
    - ALERTA 4: Não Apropriado no Siscor — usa NOT IN para excluir NU_LINHAs
      já presentes na tabela (inseridos pelos alertas anteriores).
    - Após todos os inserts, dois DELETEs removem exceções manuais e
      contratos vinculados ao ID_CENTRO = 8.
    """
    import time
    import traceback

    tempo_inicio = time.time()

    try:
        print(f"[{datetime.now()}] Iniciando execução dos scripts de relatório...")

        # =====================================================
        # SCRIPT 1: DPJ_TB007_DJ_RELATORIO
        # =====================================================
        print(f"[{datetime.now()}] Executando SCRIPT 1: DPJ_TB007_DJ_RELATORIO")

        try:
            sql_delete_relatorio = text("""
                DELETE FROM [BDDASHBOARDBI].[BDG].[DPJ_TB007_DJ_RELATORIO]
            """)
            db.session.execute(sql_delete_relatorio)
            db.session.commit()
            print(f"[{datetime.now()}] DELETE da tabela DPJ_TB007 executado com sucesso")
        except Exception as e:
            print(f"[{datetime.now()}] ERRO no DELETE DPJ_TB007: {str(e)}")
            raise Exception(f"Erro ao deletar DPJ_TB007: {str(e)}")

        try:
            sql_insert_relatorio = text("""
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
                ORDER BY ABS(VR_RATEIO)
            """)
            db.session.execute(sql_insert_relatorio)
            db.session.commit()
            print(f"[{datetime.now()}] INSERT na tabela DPJ_TB007 executado com sucesso")
        except Exception as e:
            print(f"[{datetime.now()}] ERRO no INSERT DPJ_TB007: {str(e)}")
            raise Exception(f"Erro ao inserir em DPJ_TB007: {str(e)}")

        # =====================================================
        # SCRIPT 2: DPJ_TB008_COMPARATIVO_SISCOR
        # =====================================================
        print(f"[{datetime.now()}] Executando SCRIPT 2: DPJ_TB008_COMPARATIVO_SISCOR")

        try:
            sql_delete_comparativo = text("""
                DELETE FROM [BDDASHBOARDBI].[BDG].[DPJ_TB008_COMPARATIVO_SISCOR]
            """)
            db.session.execute(sql_delete_comparativo)
            db.session.commit()
            print(f"[{datetime.now()}] DELETE da tabela DPJ_TB008 executado com sucesso")
        except Exception as e:
            print(f"[{datetime.now()}] ERRO no DELETE DPJ_TB008: {str(e)}")
            raise Exception(f"Erro ao deletar DPJ_TB008: {str(e)}")

        try:
            sql_insert_comparativo = text("""
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
                    WHERE ID_ITEM IN (1432,1473,1471,1470,1472,1464)
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
                ORDER BY COR.UNIDADE, COR.DT_EXECUCAO_ORCAMENTO
            """)
            db.session.execute(sql_insert_comparativo)
            db.session.commit()
            print(f"[{datetime.now()}] INSERT na tabela DPJ_TB008 executado com sucesso")
        except Exception as e:
            print(f"[{datetime.now()}] ERRO no INSERT DPJ_TB008: {str(e)}")
            raise Exception(f"Erro ao inserir em DPJ_TB008: {str(e)}")

        # =====================================================
        # SCRIPT 3: DPJ_TB009_ALERTAS_SUFIN
        # =====================================================
        print(f"[{datetime.now()}] Executando SCRIPT 3: DPJ_TB009_ALERTAS_SUFIN")

        # 3.1 - DELETAR ALERTAS EXISTENTES
        try:
            sql_delete_alertas = text("""
                DELETE FROM [BDDASHBOARDBI].BDG.[DPJ_TB009_ALERTAS_SUFIN]
            """)
            db.session.execute(sql_delete_alertas)
            db.session.commit()
            print(f"[{datetime.now()}] DELETE da tabela DPJ_TB009 executado com sucesso")
        except Exception as e:
            print(f"[{datetime.now()}] ERRO no DELETE DPJ_TB009: {str(e)}")
            raise Exception(f"Erro ao deletar DPJ_TB009: {str(e)}")

        # 3.2 - ALERTA 1: Indícios de Duplicidade
        try:
            sql_alerta_1 = text("""
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
                ORDER BY ABS(DJ.VR_RATEIO)
            """)
            db.session.execute(sql_alerta_1)
            db.session.commit()
            print(f"[{datetime.now()}] ALERTA 1 (Duplicidade) inserido com sucesso")
        except Exception as e:
            print(f"[{datetime.now()}] ERRO no ALERTA 1: {str(e)}")
            raise Exception(f"Erro ao inserir ALERTA 1: {str(e)}")

        # 3.3 - ALERTA 2: Áreas diferentes/mesmo contrato
        try:
            sql_alerta_2 = text("""
                INSERT INTO [BDDASHBOARDBI].BDG.[DPJ_TB009_ALERTAS_SUFIN]
                SELECT 
                    DJ.[NU_LINHA],
                    DJ.[DT_LANCAMENTO_DJ],
                    CT.[NO_CENTRO_RESULTADO],
                    DJ.[DT_AJUSTE_RM],
                    DJ.[NU_CONTRATO],
                    DJ.[VR_RATEIO],
                    DJ.[DT_SISCOR],
                    [ALERTA] = 'Áreas diferentes/mesmo contrato'
                FROM [BDDASHBOARDBI].BDG.[DPJ_TB004_DEPOSITOS_SUFIN] DJ
                INNER JOIN BDDASHBOARDBI.BDG.[DPJ_TB002_CENTRO_RESULTADO] CT
                    ON DJ.ID_CENTRO = CT.ID_CENTRO
                INNER JOIN 
                (
                    SELECT 
                        NU_CONTRATO,
                        ID_CENTRO,
                        SUM(DJ.VR_RATEIO) vr
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
                    ON DJ.NU_CONTRATO = CTR.NU_CONTRATO
            """)
            db.session.execute(sql_alerta_2)
            db.session.commit()
            print(f"[{datetime.now()}] ALERTA 2 (Áreas diferentes) inserido com sucesso")
        except Exception as e:
            print(f"[{datetime.now()}] ERRO no ALERTA 2: {str(e)}")
            raise Exception(f"Erro ao inserir ALERTA 2: {str(e)}")

        # 3.4 - ALERTA 3: Contrato não é EMGEA
        try:
            sql_alerta_3 = text("""
                INSERT INTO [BDDASHBOARDBI].BDG.[DPJ_TB009_ALERTAS_SUFIN]
                SELECT 
                    DJ.[NU_LINHA],
                    DJ.[DT_LANCAMENTO_DJ],
                    CT.[NO_CENTRO_RESULTADO],
                    DJ.[DT_AJUSTE_RM],
                    DJ.[NU_CONTRATO],
                    DJ.[VR_RATEIO],
                    DJ.[DT_SISCOR],
                    [ALERTA] = 'Contrato não é EMGEA'
                FROM [BDDASHBOARDBI].BDG.[DPJ_TB004_DEPOSITOS_SUFIN] DJ
                INNER JOIN BDDASHBOARDBI.BDG.[DPJ_TB002_CENTRO_RESULTADO] CT
                    ON DJ.ID_CENTRO = CT.ID_CENTRO
                WHERE OBS LIKE '%CONTR%EMGEA%'
                    AND DJ.NU_CONTRATO NOT IN (455552166963)
            """)
            db.session.execute(sql_alerta_3)
            db.session.commit()
            print(f"[{datetime.now()}] ALERTA 3 (Contrato não EMGEA) inserido com sucesso")
        except Exception as e:
            print(f"[{datetime.now()}] ERRO no ALERTA 3: {str(e)}")
            raise Exception(f"Erro ao inserir ALERTA 3: {str(e)}")

        # 3.5 - ALERTA 4: Não Apropriado no Siscor
        # NOT IN exclui NU_LINHAs já inseridos pelos alertas anteriores
        try:
            sql_alerta_4 = text("""
                INSERT INTO [BDDASHBOARDBI].BDG.[DPJ_TB009_ALERTAS_SUFIN]
                SELECT 
                    DJ.[NU_LINHA],
                    DJ.[DT_LANCAMENTO_DJ],
                    CT.[NO_CENTRO_RESULTADO],
                    DJ.[DT_AJUSTE_RM],
                    DJ.[NU_CONTRATO],
                    DJ.[VR_RATEIO],
                    DJ.[DT_SISCOR],
                    [ALERTA] = 'Não Apropriado no Siscor'
                FROM [BDDASHBOARDBI].BDG.[DPJ_TB004_DEPOSITOS_SUFIN] DJ
                INNER JOIN BDDASHBOARDBI.BDG.[DPJ_TB002_CENTRO_RESULTADO] CT
                    ON DJ.ID_CENTRO = CT.ID_CENTRO
                WHERE DJ.ID_CENTRO NOT IN (6)
                    AND DJ.[DT_SISCOR] IS NULL
                    AND DJ.[NU_LINHA] NOT IN (
                        SELECT [NU_LINHA] FROM [BDDASHBOARDBI].BDG.[DPJ_TB009_ALERTAS_SUFIN]
                    )
            """)
            db.session.execute(sql_alerta_4)
            db.session.commit()
            print(f"[{datetime.now()}] ALERTA 4 (Não apropriado Siscor) inserido com sucesso")
        except Exception as e:
            print(f"[{datetime.now()}] ERRO no ALERTA 4: {str(e)}")
            raise Exception(f"Erro ao inserir ALERTA 4: {str(e)}")

        # 3.6 - EXCLUSÕES: Remover alertas específicos
        try:
            sql_exclusoes_1 = text("""
                DELETE FROM [BDDASHBOARDBI].BDG.[DPJ_TB009_ALERTAS_SUFIN]
                WHERE NU_LINHA IN (
                    151,101,102,103,1789,3380,3379,3450,229,
                    230,231,232,233,234,235,236,237,238,687,688,2272,2273,2494,2495,716,717,
                    1446,1447,3546,3547,1362,3793,3096,3792,3728,3743,3789,3841,
                    1348,1350,1917,1918,3868,3867,3866,3865,3864,3896,3895
                )
            """)
            db.session.execute(sql_exclusoes_1)
            db.session.commit()
            print(f"[{datetime.now()}] EXCLUSÃO 1 de alertas executada com sucesso")
        except Exception as e:
            print(f"[{datetime.now()}] ERRO na EXCLUSÃO 1: {str(e)}")
            raise Exception(f"Erro na EXCLUSÃO 1: {str(e)}")

        try:
            sql_exclusoes_2 = text("""
                DELETE FROM [BDDASHBOARDBI].BDG.[DPJ_TB009_ALERTAS_SUFIN]
                WHERE [NU_CONTRATO] IN (
                    SELECT [NU_CONTRATO] FROM [BDDASHBOARDBI].BDG.[DPJ_TB004_DEPOSITOS_SUFIN]
                    WHERE ID_CENTRO = 8
                )
            """)
            db.session.execute(sql_exclusoes_2)
            db.session.commit()
            print(f"[{datetime.now()}] EXCLUSÃO 2 de alertas executada com sucesso")
        except Exception as e:
            print(f"[{datetime.now()}] ERRO na EXCLUSÃO 2: {str(e)}")
            raise Exception(f"Erro na EXCLUSÃO 2: {str(e)}")

        # Registrar log de sucesso
        registrar_log(
            'depositos_judiciais',
            'execute_scripts',
            'Scripts de relatório executados com sucesso',
            {}
        )

        tempo_execucao = round(time.time() - tempo_inicio, 2)
        print(f"[{datetime.now()}] Scripts executados com sucesso! Tempo: {tempo_execucao}s")

        return jsonify({
            'success': True,
            'mensagem': 'Scripts executados com sucesso',
            'tempo_execucao': tempo_execucao
        })

    except Exception as e:
        db.session.rollback()

        erro_completo = traceback.format_exc()
        print(f"[{datetime.now()}] ERRO GERAL: {erro_completo}")

        registrar_log(
            'depositos_judiciais',
            'execute_scripts_error',
            f'Erro ao executar scripts: {str(e)}',
            {'erro_completo': erro_completo}
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

@depositos_judiciais_bp.route('/excluidos')
@login_required
def excluidos():
    """Página para visualizar depósitos institucionais excluídos (DPJ_TB010)"""
    registros = (
        DepositosSufinExclusao.query
        .order_by(DepositosSufinExclusao.NU_LINHA.desc())
        .all()
    )
    return render_template('depositos_judiciais/excluidos.html', registros=registros)

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
            proximo_nu_linha = obter_proximo_nu_linha()

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
    Ratear o valor de um depósito para MÚLTIPLOS contratos.
    O usuário só consegue finalizar quando distribuir TODO o valor.

    REGRAS:
    - Cada linha tem seu próprio Lançamento RM (manual no formulário).
      Se vier vazio, usa o Lançamento RM do depósito original como fallback.
    - Se a Data de Identificação for preenchida em uma linha, essa linha
      nasce com STATUS = 'Concluído ' (mesma regra da função editar()).
    - Após criar as novas linhas, o depósito original é EXCLUÍDO
      PERMANENTEMENTE (delete físico), junto com seu ProcessosJudiciais
      vinculado (se existir), para não deixar registro órfão.
    """
    # Mapeamento dos eventos contábeis por ID_CENTRO
    # (mesmo mapa usado na função editar(); enviado ao template para
    #  pré-preencher o Evento Contábil Atual quando o Centro é selecionado)
    EVENTOS_POR_CENTRO = {
        1: {'codigo': 22611, 'descricao': 'LEVANTAMENTO DE DEPÓSITO JUDICIAL PJ'},
        2: {'codigo': 22612, 'descricao': 'LEVANTAMENTO DE DEPÓSITO JUD. COMERCIAL'},
        3: {'codigo': 22610, 'descricao': 'LEVANTAMENTO DE DEPÓSITO JUDICIAL PF'},
        4: {'codigo': 22611, 'descricao': 'LEVANTAMENTO DE DEPÓSITO JUDICIAL PJ'},
        5: {'codigo': 22613, 'descricao': 'LEVANTAMENTO DE DEPÓSITO JUD. IMÓVEIS'},
        6: {'codigo': 22607, 'descricao': 'PENDÊNCIA DE DEPÓSITO JUDICIAL'},
        7: {'codigo': 22710, 'descricao': 'EVENTO CONTÁBIL 22710'}
    }

    # Buscar o depósito original
    deposito_obj = DepositosSufin.query.filter_by(NU_LINHA=nu_linha).first()

    if not deposito_obj:
        flash('Depósito não encontrado.', 'danger')
        return redirect(url_for('depositos_judiciais.edicao'))

    # Guardar o NU_LINHA original em variável local
    nu_linha_original = deposito_obj.NU_LINHA

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

            # Validar valor total (Decimal para bater com o tipo da coluna Numeric(18,2))
            valor_original = Decimal(str(deposito_obj.VR_RATEIO or 0))
            valor_total_rateado = sum(
                Decimal(r['vr_rateio'].replace('.', '').replace(',', '.')) for r in rateios
            )

            if abs(valor_total_rateado - valor_original) > Decimal('0.01'):  # Tolerância de 1 centavo
                raise ValueError(
                    f"A soma dos rateios (R$ {valor_total_rateado:.2f}) deve ser igual ao valor original (R$ {valor_original:.2f})"
                )

            # Buscar próximo NU_LINHA disponível (verifica ambas as tabelas)
            proximo_nu_linha = obter_proximo_nu_linha()

            # Lançamento RM original como fallback caso alguma linha venha vazia
            lancamento_rm_original = (deposito_obj.LANCAMENTO_RM or '').strip()

            # Criar cada linha de rateio
            linhas_criadas = []
            for idx, rateio in enumerate(rateios):
                novo_deposito = DepositosSufin()
                novo_deposito.NU_LINHA = proximo_nu_linha + idx

                # Lançamento RM vem do formulário (manual por linha)
                # Fallback: se vier vazio, usa o do depósito original
                lancamento_rm_linha = (rateio.get('lancamento_rm') or '').strip()
                novo_deposito.LANCAMENTO_RM = lancamento_rm_linha if lancamento_rm_linha else lancamento_rm_original

                # Copiar demais campos básicos do original
                novo_deposito.DT_LANCAMENTO_DJ = deposito_obj.DT_LANCAMENTO_DJ
                novo_deposito.MEMO_SUFIN = deposito_obj.MEMO_SUFIN
                novo_deposito.DT_MEMO = deposito_obj.DT_MEMO
                novo_deposito.ID_IDENTIFICADO = deposito_obj.ID_IDENTIFICADO

                # Valor do rateio (Decimal para bater com Numeric(18,2))
                vr_rateio_str = rateio['vr_rateio'].replace('.', '').replace(',', '.')
                novo_deposito.VR_RATEIO = Decimal(vr_rateio_str)

                # Área
                id_area = rateio.get('id_area')
                if id_area:
                    novo_deposito.ID_AREA = int(id_area)

                # Centro
                id_centro = rateio.get('id_centro')
                if id_centro:
                    novo_deposito.ID_CENTRO = int(id_centro)

                # Data identificação
                # REGRA: se a data foi preenchida, o registro nasce como 'Concluído'
                # (mesma regra usada na função editar() do ramo de edição normal)
                dt_identificacao = rateio.get('dt_identificacao')
                if dt_identificacao:
                    novo_deposito.DT_IDENTIFICACAO = datetime.strptime(dt_identificacao, '%Y-%m-%d')
                    novo_deposito.STATUS = 'Concluído'
                else:
                    novo_deposito.DT_IDENTIFICACAO = None
                    novo_deposito.STATUS = None

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
                # AREA_STATUS é usada apenas para identificar quem está editando
                # (status 'Em andamento'). Em 'Concluído' não faz sentido preencher.
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

            # ============================================
            # EXCLUIR PERMANENTEMENTE O DEPÓSITO ORIGINAL
            # ============================================
            # Materializa os inserts das novas linhas ANTES de excluir o original.
            # Assim, se houver erro nos inserts, ele estoura aqui e o rollback
            # desfaz tudo de forma consistente (não exclui o original "à toa").
            db.session.flush()

            # 1) Excluir processo judicial vinculado ao NU_LINHA original (se existir).
            #    Mesma PK do depósito, então sem isso ficaria órfão.
            processo_original = ProcessosJudiciais.query.filter_by(NU_LINHA=nu_linha_original).first()
            if processo_original is not None:
                db.session.delete(processo_original)

            # 2) Excluir o depósito original permanentemente
            db.session.delete(deposito_obj)

            db.session.commit()

            # Registrar log
            registrar_log(
                'depositos_judiciais',
                'rateio_multiplo',
                f'Rateio múltiplo realizado - NU_LINHA original {nu_linha_original} excluído, '
                f'criadas {len(linhas_criadas)} linhas',
                {
                    'nu_linha_original_excluido': nu_linha_original,
                    'linhas_criadas': linhas_criadas,
                    'quantidade': len(linhas_criadas),
                    'valor_total': float(valor_total_rateado),
                    'processo_original_excluido': processo_original is not None
                }
            )

            flash(
                f'Rateio múltiplo realizado com sucesso! Criadas {len(linhas_criadas)} novas linhas. '
                f'Depósito original (Nº Linha: {nu_linha_original}) excluído permanentemente.',
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
                           centros=centros,
                           eventos_por_centro=EVENTOS_POR_CENTRO)