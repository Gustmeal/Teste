from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from app import db
from app.models.depositos_judiciais import DepositosSufin, Area, CentroResultado, ProcessosJudiciais
from app.utils.audit import registrar_log
from datetime import datetime
from sqlalchemy import func, or_, and_
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
            vr_rateio = request.form.get('vr_rateio')
            # Remove pontos de milhar e substitui vírgula por ponto
            vr_rateio_limpo = vr_rateio.replace('.', '').replace(',', '.')
            novo_deposito.VR_RATEIO = float(vr_rateio_limpo)

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
    """Listagem de depósitos para edição com filtros"""
    # Pegar filtros da query string
    filtro_contrato = request.args.get('nu_contrato', '').strip()
    filtro_carteira = request.args.get('id_centro', '')
    filtro_valor = request.args.get('vr_rateio', '').strip()
    filtro_dt_identificacao = request.args.get('dt_identificacao', '')
    filtro_dt_siscor = request.args.get('dt_siscor', '')
    filtro_dt_siscor_status = request.args.get('dt_siscor_status', '')

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
            # Remove pontos de milhar e substitui vírgula por ponto
            valor_limpo = filtro_valor.replace('.', '').replace(',', '.')
            valor_decimal = Decimal(valor_limpo)
            query = query.filter(DepositosSufin.VR_RATEIO == valor_decimal)
        except:
            pass

    if filtro_dt_identificacao:
        try:
            data_filtro = datetime.strptime(filtro_dt_identificacao, '%Y-%m-%d')
            query = query.filter(DepositosSufin.DT_IDENTIFICACAO == data_filtro)
        except:
            pass

    # Filtro de DT_SISCOR por data específica
    if filtro_dt_siscor:
        try:
            data_siscor = datetime.strptime(filtro_dt_siscor, '%Y-%m-%d')
            query = query.filter(DepositosSufin.DT_SISCOR == data_siscor)
        except:
            pass

    # Filtro de DT_SISCOR por status (preenchido ou nulo)
    if filtro_dt_siscor_status == 'nulo':
        query = query.filter(DepositosSufin.DT_SISCOR.is_(None))
    elif filtro_dt_siscor_status == 'preenchido':
        query = query.filter(DepositosSufin.DT_SISCOR.isnot(None))

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
                               'dt_identificacao': filtro_dt_identificacao,
                               'dt_siscor': filtro_dt_siscor,
                               'dt_siscor_status': filtro_dt_siscor_status
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

                # Corrigir conversão do valor rateio
                vr_rateio = request.form.get('vr_rateio')
                vr_rateio_limpo = vr_rateio.replace('.', '').replace(',', '.')
                nova_linha.VR_RATEIO = float(vr_rateio_limpo)

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
                # Não mudou centro OU é Institucional - EDIÇÃO NORMAL
                deposito_obj.LANCAMENTO_RM = request.form.get('lancamento_rm')
                deposito_obj.DT_LANCAMENTO_DJ = datetime.strptime(request.form.get('dt_lancamento_dj'), '%Y-%m-%d')

                # Corrigir conversão do valor rateio
                vr_rateio = request.form.get('vr_rateio')
                vr_rateio_limpo = vr_rateio.replace('.', '').replace(',', '.')
                deposito_obj.VR_RATEIO = float(vr_rateio_limpo)

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


@depositos_judiciais_bp.route('/executar-scripts-relatorio', methods=['POST'])
@login_required
def executar_scripts_relatorio():
    """Executa os scripts SQL para atualizar relatórios"""
    import time

    tempo_inicio = time.time()

    try:
        # Script 1: DPJ_TB007_DJ_RELATORIO
        sql_relatorio = """
        DELETE FROM [dbo].[DPJ_TB007_DJ_RELATORIO];

        INSERT INTO [dbo].[DPJ_TB007_DJ_RELATORIO] 
        SELECT 
            DPJ.NU_LINHA
            ,[DT_LANCAMENTO_DJ] 
            ,ANO_SISCOR = YEAR([DT_SISCOR])
            ,[DSC_MES] MES_SISCOR
            ,[VR_RATEIO]
            ,[MEMO_SUFIN]
            ,[DT_IDENTIFICACAO]
            ,CT.[NO_CARTEIRA]
            ,[DT_AJUSTE_RM]
            ,[NU_CONTRATO]
            ,[NR_PROCESSO]
            ,[OBS]
            ,[IC_APROPRIADO]
            ,[IC_INCLUIDO_ACERTO]
            ,DT_SISCOR
        FROM [dbo].[DPJ_TB004_DEPOSITOS_SUFIN] DPJ 
        INNER JOIN [dbo].[DPJ_TB002_CENTRO_RESULTADO] CT
            ON DPJ.ID_CENTRO = CT.ID_CENTRO
        LEFT JOIN [dbo].[DPJ_TB006_PROCESSOS_JUDICIAIS] JU
            ON DPJ.NU_LINHA = JU.NU_LINHA
        LEFT JOIN [BDG].[PAR_TB020_CALENDARIO] CAL
            ON CAL.DIA = DPJ.DT_SISCOR
        ORDER BY ABS(VR_RATEIO);
        """

        # Script 2: DPJ_TB008_COMPARATIVO_SISCOR e DPJ_TB009_ALERTAS_SUFIN
        sql_comparativo = """
        -- Comparativo SISCOR
        DELETE FROM [dbo].[DPJ_TB008_COMPARATIVO_SISCOR];

        INSERT INTO [dbo].[DPJ_TB008_COMPARATIVO_SISCOR]
        SELECT 
            COR.UNIDADE,
            COR.DT_EXECUCAO_ORCAMENTO DT_SISCOR,
            COR.VLR VR_SISCOR,
            ISNULL(DJ.VR,0) VR_BASE_DJ,
            DIFERENCA = COR.VLR - ISNULL(DJ.VR,0)
        FROM 
        (
            SELECT 
                UNIDADE = CASE WHEN ID_ITEM IN (1464) THEN 'Indenização-Seguro'
                            WHEN UNIDADE IN ('SUCRE') THEN 'Sucre-DJ'
                            WHEN UNIDADE IN ('SUPEJ') THEN 'Supej'
                            WHEN UNIDADE IN ('SUPEC') THEN 'Supec'
                            WHEN UNIDADE IN ('SUMOV') THEN 'Sumov'
                            ELSE UNIDADE END
                ,[DT_EXECUCAO_ORCAMENTO]
                ,SUM([VR_EXECUCAO_ORCAMENTO]) VLR
            FROM [BDG].[COR_TB001_EXECUCAO_ORCAMENTARIA_SISCOR]
            WHERE ID_ITEM IN (1432,1473,1471,1470,1472,1464)
                AND [ID_NATUREZA] = 3
                AND [VR_EXECUCAO_ORCAMENTO] <> 0
                AND [UNIDADE] NOT IN ('INSTIT')
            GROUP BY
                CASE WHEN ID_ITEM IN (1464) THEN 'Indenização-Seguro'
                    WHEN UNIDADE IN ('SUCRE') THEN 'Sucre-DJ'
                    WHEN UNIDADE IN ('SUPEJ') THEN 'Supej'
                    WHEN UNIDADE IN ('SUPEC') THEN 'Supec'
                    WHEN UNIDADE IN ('SUMOV') THEN 'Sumov'
                    ELSE UNIDADE END
                ,[DT_EXECUCAO_ORCAMENTO]
        ) COR
        LEFT JOIN 
        (
            SELECT 
                UNIDADE = CASE WHEN ID_CENTRO IN (2) THEN 'Supec'
                            WHEN ID_CENTRO IN (3) THEN 'Sucre-DJ'
                            WHEN ID_CENTRO IN (1,4) THEN 'Supej'
                            WHEN ID_CENTRO IN (5) THEN 'Sumov'
                            WHEN ID_CENTRO IN (7) THEN 'Indenização-Seguro'
                            ELSE NULL END,
                DT_IDENT_SISCOR = (SUBSTRING(CONVERT(VARCHAR(4),[DT_SISCOR]),1,4)+SUBSTRING(CONVERT(VARCHAR(10),[DT_SISCOR]),6,2)),
                SUM(VR_RATEIO) VR
            FROM [dbo].[DPJ_TB004_DEPOSITOS_SUFIN]
            WHERE DT_SISCOR IS NOT NULL AND ID_CENTRO IN (1,2,3,4,5,7)
            GROUP BY
                CASE WHEN ID_CENTRO IN (2) THEN 'Supec'
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

        -- Alertas SUFIN
        DELETE FROM [dbo].[DPJ_TB009_ALERTAS_SUFIN];

        -- Alerta 1: Duplicidades
        INSERT INTO [dbo].[DPJ_TB009_ALERTAS_SUFIN]
        SELECT 
            DJ.NU_LINHA,
            DJ.DT_LANCAMENTO_DJ,
            CT.NO_CENTRO_RESULTADO,
            DJ.DT_AJUSTE_RM,
            DJ.NU_CONTRATO,
            DJ.VR_RATEIO,
            DJ.DT_SISCOR,
            ALERTA = CASE WHEN DJ.DT_LANCAMENTO_DJ IS NULL THEN 'Dt de Lançamento DJ não informada'
                        WHEN ABS(DJ.VR_RATEIO) IN (50258.25,123131.63) THEN 'Lançado 2 vezes no Siscor'
                        ELSE 'Indício de Duplidade' END
        FROM [dbo].[DPJ_TB004_DEPOSITOS_SUFIN] DJ
        INNER JOIN [dbo].[DPJ_TB002_CENTRO_RESULTADO] CT
            ON DJ.ID_CENTRO = CT.ID_CENTRO
        INNER JOIN 
        (
            SELECT 
                ISNULL(DT_LANCAMENTO_DJ,'') DT_LANCAMENTO_DJ
                ,ABS([VR_RATEIO]) QUEBRA_VR
                ,SUM([VR_RATEIO]) VR
            FROM [dbo].[DPJ_TB004_DEPOSITOS_SUFIN]
            WHERE ABS([VR_RATEIO]) NOT IN (1120.09,27408.86,41447.54,585.87)
            GROUP BY DT_LANCAMENTO_DJ, ABS([VR_RATEIO])
            HAVING ABS([VR_RATEIO]) - SUM([VR_RATEIO]) <> 0
        ) ALERTA
        ON ABS(DJ.VR_RATEIO) = ALERTA.QUEBRA_VR
            AND ISNULL(DJ.DT_LANCAMENTO_DJ,'') = ALERTA.DT_LANCAMENTO_DJ
        ORDER BY ABS(DJ.VR_RATEIO);

        -- Alerta 2: Áreas diferentes/mesmo contrato
        INSERT INTO [dbo].[DPJ_TB009_ALERTAS_SUFIN]
        SELECT 
            DJ.[NU_LINHA],
            DJ.[DT_LANCAMENTO_DJ],
            CT.[NO_CENTRO_RESULTADO],
            DJ.[DT_AJUSTE_RM],
            DJ.[NU_CONTRATO],
            DJ.[VR_RATEIO],
            DJ.[DT_SISCOR],
            [ALERTA]= 'Áreas diferentes/mesmo contrato'
        FROM [dbo].[DPJ_TB004_DEPOSITOS_SUFIN] DJ
        INNER JOIN [dbo].[DPJ_TB002_CENTRO_RESULTADO] CT
            ON DJ.ID_CENTRO = CT.ID_CENTRO
        INNER JOIN 
        (
            SELECT 
                NU_CONTRATO,
                ID_CENTRO,
                sum(DJ.VR_RATEIO) vr
            FROM [dbo].[DPJ_TB004_DEPOSITOS_SUFIN] DJ
            INNER JOIN [BDG].[AUX_VW001_CONTRATOS_CPF] CTR
                ON DJ.NU_CONTRATO = CTR.NR_CONTRATO
            WHERE NU_CONTRATO IS NOT NULL
                AND NU_CONTRATO NOT IN (101560100203)
                AND NU_CONTRATO <> 0
                AND (
                    (DJ.ID_CENTRO = 2 AND [CARTEIRA] <> 'Comercial PF')
                    OR (DJ.ID_CENTRO = 3 AND [CARTEIRA] NOT IN ('Habitação PF','Imóveis'))
                    OR (DJ.ID_CENTRO = 4 AND [CARTEIRA] NOT LIKE '%PJ')
                )
            GROUP BY NU_CONTRATO, ID_CENTRO
            HAVING SUM(DJ.VR_RATEIO) > 0
        ) CTR
        ON DJ.NU_CONTRATO = CTR.NU_CONTRATO;

        -- Alerta 3: Contrato não é EMGEA
        INSERT INTO [dbo].[DPJ_TB009_ALERTAS_SUFIN]
        SELECT 
            DJ.[NU_LINHA],
            DJ.[DT_LANCAMENTO_DJ],
            CT.[NO_CENTRO_RESULTADO],
            DJ.[DT_AJUSTE_RM],
            DJ.[NU_CONTRATO],
            DJ.[VR_RATEIO],
            DJ.[DT_SISCOR],
            [ALERTA]= 'Contrato não é EMGEA'
        FROM [dbo].[DPJ_TB004_DEPOSITOS_SUFIN] DJ
        INNER JOIN [dbo].[DPJ_TB002_CENTRO_RESULTADO] CT
            ON DJ.ID_CENTRO = CT.ID_CENTRO
        WHERE OBS LIKE '%CONTR%EMGEA%'
            AND DJ.NU_CONTRATO NOT IN (455552166963);

        -- Alerta 4: Não Apropriado no Siscor
        INSERT INTO [dbo].[DPJ_TB009_ALERTAS_SUFIN]
        SELECT 
            DJ.[NU_LINHA],
            DJ.[DT_LANCAMENTO_DJ],
            CT.[NO_CENTRO_RESULTADO],
            DJ.[DT_AJUSTE_RM],
            DJ.[NU_CONTRATO],
            DJ.[VR_RATEIO],
            DJ.[DT_SISCOR],
            [ALERTA]= 'Não Apropriado no Siscor'
        FROM [dbo].[DPJ_TB004_DEPOSITOS_SUFIN] DJ
        INNER JOIN [dbo].[DPJ_TB002_CENTRO_RESULTADO] CT
            ON DJ.ID_CENTRO = CT.ID_CENTRO
        WHERE DJ.ID_CENTRO NOT IN (6)
            AND DJ.[DT_SISCOR] IS NULL;

        -- Exclusões específicas
        DELETE FROM [dbo].[DPJ_TB009_ALERTAS_SUFIN]
        WHERE NU_LINHA IN (151,101,102,103,1789,3380,3379,3450,229,
                         230,231,232,233,234,235,236,237,238,687,688,2272,2273,2494,2495,716,717,1446,1447,3546,3547);

        DELETE FROM [dbo].[DPJ_TB009_ALERTAS_SUFIN] 
        WHERE [NU_CONTRATO] IN (SELECT [NU_CONTRATO] FROM [dbo].[DPJ_TB004_DEPOSITOS_SUFIN] WHERE ID_CENTRO = 8);
        """

        # Executar scripts
        db.session.execute(text(sql_relatorio))
        db.session.execute(text(sql_comparativo))
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