from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify
from flask_login import login_required, current_user
from app import db
from app.models.relacao_imovel_contrato import RelacaoImovelContratoParcelamento
from app.utils.audit import registrar_log
from datetime import datetime
from sqlalchemy import text
from app.models.despesas_analitico import DespesasAnalitico, OcorrenciasMovItemServico
from decimal import Decimal
from app.models.evidencias_sumov import EvidenciasSumov
from datetime import datetime, date

sumov_bp = Blueprint('sumov', __name__, url_prefix='/sumov')


@sumov_bp.context_processor
def inject_datetime():
    """Injeta datetime e ano atual no contexto de todos os templates do SUMOV"""
    return {
        'datetime': datetime,
        'current_year': datetime.utcnow().year
    }
@sumov_bp.route('/')
@login_required
def index():
    """Dashboard principal do SUMOV"""
    # Estatísticas
    total_vinculacoes = RelacaoImovelContratoParcelamento.query.filter_by(
        DELETED_AT=None
    ).count()

    # Vinculações do mês atual
    inicio_mes = datetime.now().replace(day=1, hour=0, minute=0, second=0)
    vinculacoes_mes = RelacaoImovelContratoParcelamento.query.filter(
        RelacaoImovelContratoParcelamento.CREATED_AT >= inicio_mes,
        RelacaoImovelContratoParcelamento.DELETED_AT == None
    ).count()

    # Últimas vinculações
    ultimas_vinculacoes = RelacaoImovelContratoParcelamento.query.filter_by(
        DELETED_AT=None
    ).order_by(
        RelacaoImovelContratoParcelamento.CREATED_AT.desc()
    ).limit(5).all()

    return render_template('sumov/index.html',
                           total_vinculacoes=total_vinculacoes,
                           vinculacoes_mes=vinculacoes_mes,
                           ultimas_vinculacoes=ultimas_vinculacoes)


@sumov_bp.route('/relacao-imovel-contrato')
@login_required
def relacao_imovel_contrato():
    """Dashboard do sistema de relação imóvel/contrato"""
    # Estatísticas específicas
    total_contratos = db.session.query(
        RelacaoImovelContratoParcelamento.NR_CONTRATO
    ).filter_by(DELETED_AT=None).distinct().count()

    total_imoveis = db.session.query(
        RelacaoImovelContratoParcelamento.NR_IMOVEL
    ).filter_by(DELETED_AT=None).distinct().count()

    total_vinculacoes = RelacaoImovelContratoParcelamento.query.filter_by(
        DELETED_AT=None
    ).count()

    return render_template('sumov/relacao_imovel_contrato/index.html',
                           total_contratos=total_contratos,
                           total_imoveis=total_imoveis,
                           total_vinculacoes=total_vinculacoes)


@sumov_bp.route('/relacao-imovel-contrato/lista')
@login_required
def lista_vinculacoes():
    """Lista todas as vinculações"""
    vinculacoes = RelacaoImovelContratoParcelamento.listar_vinculacoes_ativas()

    return render_template('sumov/relacao_imovel_contrato/lista.html',
                           vinculacoes=vinculacoes)


@sumov_bp.route('/relacao-imovel-contrato/nova', methods=['GET', 'POST'])
@login_required
def nova_vinculacao():
    """Criar nova vinculação entre contrato e imóvel"""
    if request.method == 'POST':
        try:
            nr_contrato = request.form.get('nr_contrato', '').strip()
            nr_imovel = request.form.get('nr_imovel', '').strip()

            # Validações
            if not nr_contrato or not nr_imovel:
                flash('Por favor, preencha todos os campos.', 'danger')
                return redirect(url_for('sumov.nova_vinculacao'))

            # Validar formato (12 dígitos)
            if len(nr_contrato) != 12 or not nr_contrato.isdigit():
                flash('Número do contrato deve ter 12 dígitos.', 'danger')
                return redirect(url_for('sumov.nova_vinculacao'))

            if len(nr_imovel) != 12 or not nr_imovel.isdigit():
                flash('Número do imóvel deve ter 12 dígitos.', 'danger')
                return redirect(url_for('sumov.nova_vinculacao'))

            # Verificar se já existe vinculação
            if RelacaoImovelContratoParcelamento.verificar_vinculacao_existente(nr_contrato, nr_imovel):
                flash('Esta vinculação já existe no sistema.', 'warning')
                return redirect(url_for('sumov.nova_vinculacao'))

            # Criar nova vinculação
            nova_relacao = RelacaoImovelContratoParcelamento(
                NR_CONTRATO=nr_contrato,
                NR_IMOVEL=nr_imovel
            )

            db.session.add(nova_relacao)
            db.session.commit()

            # Registrar log de auditoria com informação do usuário
            registrar_log(
                acao='criar',
                entidade='relacao_imovel_contrato',
                entidade_id=f"{nr_contrato}-{nr_imovel}",
                descricao=f'Nova vinculação criada: Contrato {nr_contrato} - Imóvel {nr_imovel}'
            )

            flash('Vinculação criada com sucesso!', 'success')
            return redirect(url_for('sumov.lista_vinculacoes'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao criar vinculação: {str(e)}', 'danger')
            return redirect(url_for('sumov.nova_vinculacao'))

    return render_template('sumov/relacao_imovel_contrato/nova.html')


@sumov_bp.route('/relacao-imovel-contrato/excluir/<contrato>/<imovel>', methods=['POST'])
@login_required
def excluir_vinculacao(contrato, imovel):
    """Excluir vinculação (soft delete)"""
    try:
        vinculacao = RelacaoImovelContratoParcelamento.query.filter_by(
            NR_CONTRATO=contrato,
            NR_IMOVEL=imovel,
            DELETED_AT=None
        ).first()

        if not vinculacao:
            flash('Vinculação não encontrada.', 'warning')
            return redirect(url_for('sumov.lista_vinculacoes'))

        # Soft delete - apenas marca a data de exclusão
        vinculacao.DELETED_AT = datetime.utcnow()

        db.session.commit()

        # Registrar log de auditoria com informação do usuário
        registrar_log(
            acao='excluir',
            entidade='relacao_imovel_contrato',
            entidade_id=f"{contrato}-{imovel}",
            descricao=f'Vinculação excluída: Contrato {contrato} - Imóvel {imovel}'
        )

        flash('Vinculação excluída com sucesso!', 'success')

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao excluir vinculação: {str(e)}', 'danger')

    return redirect(url_for('sumov.lista_vinculacoes'))


@sumov_bp.route('/relacao-imovel-contrato/buscar-vinculacoes', methods=['GET'])
@login_required
def buscar_vinculacoes():
    """Busca vinculações por contrato ou imóvel"""
    termo = request.args.get('termo', '').strip()
    tipo = request.args.get('tipo', 'contrato')  # 'contrato' ou 'imovel'

    if not termo:
        return jsonify({'vinculacoes': []})

    query = RelacaoImovelContratoParcelamento.query.filter_by(DELETED_AT=None)

    if tipo == 'contrato':
        query = query.filter(RelacaoImovelContratoParcelamento.NR_CONTRATO.like(f'%{termo}%'))
    else:
        query = query.filter(RelacaoImovelContratoParcelamento.NR_IMOVEL.like(f'%{termo}%'))

    vinculacoes = query.limit(10).all()

    resultado = []
    for v in vinculacoes:
        resultado.append({
            'nr_contrato': v.NR_CONTRATO,
            'nr_imovel': v.NR_IMOVEL,
            'data_criacao': v.CREATED_AT.strftime('%d/%m/%Y %H:%M') if v.CREATED_AT else ''
        })

    return jsonify({'vinculacoes': resultado})


@sumov_bp.route('/despesas-pagamentos')
@login_required
def despesas_pagamentos():
    """Dashboard do sistema de despesas e pagamentos"""
    total_registros = DespesasAnalitico.query.filter_by(
        NO_ORIGEM_REGISTRO='SUMOV'
    ).count()

    # Total do mês atual
    inicio_mes = datetime.now().replace(day=1, hour=0, minute=0, second=0)
    registros_mes = DespesasAnalitico.query.filter(
        DespesasAnalitico.DT_REFERENCIA >= inicio_mes,
        DespesasAnalitico.NO_ORIGEM_REGISTRO == 'SUMOV'
    ).count()

    # Valor total de despesas
    valor_total = db.session.query(
        db.func.sum(DespesasAnalitico.VR_DESPESA)
    ).filter_by(NO_ORIGEM_REGISTRO='SUMOV').scalar() or 0

    return render_template('sumov/despesas_pagamentos/index.html',
                           total_registros=total_registros,
                           registros_mes=registros_mes,
                           valor_total=valor_total)


@sumov_bp.route('/despesas-pagamentos/lista')
@login_required
def lista_despesas():
    """Lista todas as despesas registradas pelo SUMOV"""
    despesas = DespesasAnalitico.listar_despesas_sumov()
    return render_template('sumov/despesas_pagamentos/lista.html',
                           despesas=despesas)


@sumov_bp.route('/despesas-pagamentos/verificar-duplicata', methods=['POST'])
@login_required
def verificar_duplicata_despesa():
    """Verifica se já existe registro com os mesmos dados"""
    try:
        data = request.get_json()

        nr_contrato = data.get('nr_contrato', '').strip()
        id_item_servico = int(data.get('id_item_servico'))
        dt_lancamento = data.get('dt_lancamento')
        vr_despesa = data.get('vr_despesa', '0').replace(',', '.')

        origem = DespesasAnalitico.verificar_registro_existente(
            nr_contrato,
            id_item_servico,
            dt_lancamento,
            vr_despesa
        )

        return jsonify({
            'existe': origem is not None,
            'origem': origem
        })

    except Exception as e:
        return jsonify({
            'erro': str(e)
        }), 400


@sumov_bp.route('/despesas-pagamentos/nova', methods=['GET', 'POST'])
@login_required
def nova_despesa():
    """Criar novo registro de pagamento de despesa"""
    if request.method == 'POST':
        try:
            # Captura os dados do formulário
            nr_contrato = request.form.get('nr_contrato', '').strip()
            id_item_servico = int(request.form.get('id_item_servico'))
            dt_lancamento = datetime.strptime(request.form.get('dt_lancamento_pagamento'), '%Y-%m-%d')
            vr_despesa = Decimal(request.form.get('vr_despesa', '0').replace(',', '.'))
            dsc_tipo_forma_pgto = request.form.get('dsc_tipo_forma_pgto')
            confirmar_sisgea = request.form.get('confirmar_sisgea') == 'true'

            # Validações
            if not nr_contrato:
                flash('Por favor, informe o número do contrato.', 'danger')
                return redirect(url_for('sumov.nova_despesa'))

            # Verificar duplicata ANTES de prosseguir
            origem_existente = DespesasAnalitico.verificar_registro_existente(
                nr_contrato,
                id_item_servico,
                dt_lancamento,
                vr_despesa
            )

            # Se já existe registro SUMOV, bloqueia totalmente
            if origem_existente == 'SUMOV':
                flash(
                    'Este registro já foi inserido anteriormente pelo SUMOV com os mesmos dados (Contrato, Item de Serviço, Data de Pagamento e Valor).',
                    'danger')
                return redirect(url_for('sumov.nova_despesa'))

            # Se existe registro SISGEA e usuário não confirmou, retorna para confirmação
            if origem_existente == 'SISGEA' and not confirmar_sisgea:
                # Isso nunca deve acontecer porque o JavaScript trata isso
                # Mas deixamos como segurança
                flash('Este contrato já possui registro no SISGEA. Por favor, confirme se deseja continuar.', 'warning')
                return redirect(url_for('sumov.nova_despesa'))

            # Busca o item de serviço selecionado
            item_servico = OcorrenciasMovItemServico.query.get(id_item_servico)
            if not item_servico:
                flash('Item de serviço inválido.', 'danger')
                return redirect(url_for('sumov.nova_despesa'))

            # Gera o número de ocorrência
            nr_ocorrencia = DespesasAnalitico.obter_proximo_numero_ocorrencia(nr_contrato)

            # NOVA LÓGICA: Determina o ID_ITEM_SISCOR baseado no ID_ITEM_SERVICO
            if id_item_servico in [32, 33, 66]:
                id_item_siscor = 1491
            elif id_item_servico in [35, 36, 69]:
                id_item_siscor = 1490
            else:
                id_item_siscor = 1159

            # Cria o novo registro
            nova_despesa = DespesasAnalitico(
                DT_REFERENCIA=datetime.now().date(),
                NR_CONTRATO=nr_contrato,
                NR_OCORRENCIA=nr_ocorrencia,
                DSC_ITEM_SERVICO=item_servico.DSC_ITEM_SERVICO,
                DT_LANCAMENTO_PAGAMENTO=dt_lancamento.date(),
                VR_DESPESA=vr_despesa,
                estadoLancamento='Pagamento Efetuado',
                ID_ITEM_SISCOR=id_item_siscor,  # Agora usa a variável determinada pela lógica
                ID_ITEM_SERVICO=id_item_servico,
                NR_CTR_ORIGINAL_TABELA=nr_contrato,
                DSC_TIPO_FORMA_PGTO=dsc_tipo_forma_pgto,
                NO_ORIGEM_REGISTRO='SUMOV'
            )

            db.session.add(nova_despesa)
            db.session.commit()

            # Registrar log
            msg_log = f'Novo pagamento registrado: Contrato {nr_contrato} - Ocorrência {nr_ocorrencia} - Valor R$ {vr_despesa} - ID_ITEM_SISCOR: {id_item_siscor}'
            if origem_existente == 'SISGEA':
                msg_log += ' (Confirmado apesar de existir no SISGEA)'

            registrar_log(
                acao='criar',
                entidade='despesa_pagamento',
                entidade_id=nr_ocorrencia,
                descricao=msg_log
            )

            flash('Pagamento registrado com sucesso!', 'success')
            return redirect(url_for('sumov.lista_despesas'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao registrar pagamento: {str(e)}', 'danger')
            return redirect(url_for('sumov.nova_despesa'))

    # GET - Carrega os itens de serviço permitidos
    itens_servico = OcorrenciasMovItemServico.listar_itens_permitidos()

    return render_template('sumov/despesas_pagamentos/nova.html',
                           itens_servico=itens_servico)

@sumov_bp.route('/evidencias')
@login_required
def evidencias_index():
    """Dashboard do sistema de Evidências SUMOV"""
    # Estatísticas diretas do banco
    total_evidencias = EvidenciasSumov.query.count()

    # Evidências do mês atual - usando DATE
    from datetime import datetime
    hoje = datetime.now()
    primeiro_dia_mes = date(hoje.year, hoje.month, 1)

    evidencias_mes = EvidenciasSumov.query.filter(
        db.func.year(EvidenciasSumov.MESANO) == hoje.year,
        db.func.month(EvidenciasSumov.MESANO) == hoje.month
    ).count()

    # Valor total das evidências
    valor_total = db.session.query(
        db.func.sum(EvidenciasSumov.VALOR)
    ).scalar() or 0

    # Últimas 5 evidências cadastradas (por ID decrescente)
    ultimas_evidencias = EvidenciasSumov.query.order_by(
        EvidenciasSumov.ID.desc()
    ).limit(5).all()

    return render_template('sumov/evidencias/index.html',
                           total_evidencias=total_evidencias,
                           evidencias_mes=evidencias_mes,
                           valor_total=valor_total,
                           ultimas_evidencias=ultimas_evidencias)


@sumov_bp.route('/evidencias/lista')
@login_required
def evidencias_lista():
    """Lista todas as evidências cadastradas no banco"""
    # Buscar todas as evidências diretamente do banco
    evidencias = EvidenciasSumov.listar_todas()

    return render_template('sumov/evidencias/lista.html',
                           evidencias=evidencias)


@sumov_bp.route('/evidencias/nova', methods=['GET', 'POST'])
@login_required
def evidencias_nova():
    """Criar nova evidência no banco"""
    if request.method == 'POST':
        try:
            # Captura dados do formulário
            nr_contrato = request.form.get('nr_contrato', '').strip()
            mesano = request.form.get('mesano', '').strip()
            valor_str = request.form.get('valor', '0').strip()
            descricao = request.form.get('descricao', '').strip()
            id_item = request.form.get('id_item', '').strip()

            # Validações dos campos obrigatórios
            if not mesano:
                flash('Por favor, informe o mês/ano de referência.', 'danger')
                return redirect(url_for('sumov.evidencias_nova'))

            if not descricao:
                flash('Por favor, informe a descrição.', 'danger')
                return redirect(url_for('sumov.evidencias_nova'))

            # Converter valor - remover formatação brasileira
            try:
                # Remove pontos de milhar e troca vírgula por ponto
                valor_str = valor_str.replace('.', '').replace(',', '.')
                valor = Decimal(valor_str) if valor_str else Decimal('0')

                # MODIFICAÇÃO: Aceita valores negativos, apenas impede zero
                if valor == 0:
                    flash('Por favor, informe um valor diferente de zero.', 'danger')
                    return redirect(url_for('sumov.evidencias_nova'))
            except:
                flash('Valor inválido. Use o formato: 1.234,56', 'danger')
                return redirect(url_for('sumov.evidencias_nova'))

            # Converter MESANO de MM/YYYY para date
            data_mesano = None
            if '/' in mesano:
                partes = mesano.split('/')
                if len(partes) == 2:
                    try:
                        mes = int(partes[0])
                        ano = int(partes[1])
                        # Criar data com primeiro dia do mês
                        data_mesano = date(ano, mes, 1)
                    except:
                        flash('Mês/Ano inválido. Use o formato MM/YYYY', 'danger')
                        return redirect(url_for('sumov.evidencias_nova'))

            # Converter NR_CONTRATO para Decimal ou None
            nr_contrato_decimal = None
            if nr_contrato and nr_contrato.replace(' ', '').isdigit():
                nr_contrato_decimal = Decimal(nr_contrato.replace(' ', ''))

            # Converter ID_ITEM para int ou None
            id_item_int = None
            if id_item and id_item.isdigit():
                id_item_int = int(id_item)

            # Criar nova evidência diretamente no banco
            nova_evidencia = EvidenciasSumov(
                NR_CONTRATO=nr_contrato_decimal,
                MESANO=data_mesano,
                VALOR=valor,
                DESCRICAO=descricao[:150],  # Limitar a 150 caracteres
                ID_ITEM=id_item_int
            )

            db.session.add(nova_evidencia)
            db.session.commit()

            # Registrar log de auditoria
            registrar_log(
                acao='criar',
                entidade='evidencia_sumov',
                entidade_id=nova_evidencia.ID,
                descricao=f'Nova evidência criada: {mesano} - Valor R$ {valor}'
            )

            flash('Evidência cadastrada com sucesso!', 'success')
            return redirect(url_for('sumov.evidencias_lista'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao cadastrar evidência: {str(e)}', 'danger')
            return redirect(url_for('sumov.evidencias_nova'))

    # GET - Exibe o formulário
    return render_template('sumov/evidencias/nova.html')


@sumov_bp.route('/evidencias/editar/<int:id>', methods=['GET', 'POST'])
@login_required
def evidencias_editar(id):
    """Editar evidência existente no banco"""
    # Buscar evidência diretamente do banco
    evidencia = EvidenciasSumov.buscar_por_id(id)
    if not evidencia:
        flash('Evidência não encontrada.', 'danger')
        return redirect(url_for('sumov.evidencias_lista'))

    if request.method == 'POST':
        try:
            # Capturar dados antigos para auditoria
            dados_antigos = {
                'nr_contrato': str(int(evidencia.NR_CONTRATO)) if evidencia.NR_CONTRATO else None,
                'mesano': evidencia.formatar_mesano(),
                'valor': float(evidencia.VALOR) if evidencia.VALOR else 0,
                'descricao': evidencia.DESCRICAO,
                'id_item': evidencia.ID_ITEM
            }

            # Capturar novos dados
            nr_contrato = request.form.get('nr_contrato', '').strip()
            mesano = request.form.get('mesano', '').strip()
            valor_str = request.form.get('valor', '0').strip()
            descricao_nova = request.form.get('descricao', '').strip()
            id_item = request.form.get('id_item', '').strip()

            # Validações
            if not mesano:
                flash('Por favor, informe o mês/ano de referência.', 'danger')
                return render_template('sumov/evidencias/editar.html', evidencia=evidencia)

            if not descricao_nova:
                flash('Por favor, informe a descrição.', 'danger')
                return render_template('sumov/evidencias/editar.html', evidencia=evidencia)

            # Converter valor - remover formatação brasileira
            try:
                # Remove pontos de milhar e troca vírgula por ponto
                valor_str = valor_str.replace('.', '').replace(',', '.')
                valor_novo = Decimal(valor_str) if valor_str else Decimal('0')

                # MODIFICAÇÃO: Aceita valores negativos, apenas impede zero
                if valor_novo == 0:
                    flash('Por favor, informe um valor diferente de zero.', 'danger')
                    return render_template('sumov/evidencias/editar.html', evidencia=evidencia)
            except:
                flash('Valor inválido. Use o formato: 1.234,56', 'danger')
                return render_template('sumov/evidencias/editar.html', evidencia=evidencia)

            # Converter MESANO de MM/YYYY para date
            if '/' in mesano:
                partes = mesano.split('/')
                if len(partes) == 2:
                    try:
                        mes = int(partes[0])
                        ano = int(partes[1])
                        evidencia.MESANO = date(ano, mes, 1)
                    except:
                        flash('Mês/Ano inválido. Use o formato MM/YYYY', 'danger')
                        return render_template('sumov/evidencias/editar.html', evidencia=evidencia)

            # Converter NR_CONTRATO para Decimal ou None
            if nr_contrato and nr_contrato.replace(' ', '').isdigit():
                evidencia.NR_CONTRATO = Decimal(nr_contrato.replace(' ', ''))
            else:
                evidencia.NR_CONTRATO = None

            # Converter ID_ITEM para int ou None
            if id_item and id_item.isdigit():
                evidencia.ID_ITEM = int(id_item)
            else:
                evidencia.ID_ITEM = None

            # Atualizar outros campos
            evidencia.VALOR = valor_novo
            evidencia.DESCRICAO = descricao_nova[:150]  # Limitar a 150 caracteres

            # Salvar alterações no banco
            db.session.commit()

            # Registrar log de auditoria
            dados_novos = {
                'nr_contrato': str(int(evidencia.NR_CONTRATO)) if evidencia.NR_CONTRATO else None,
                'mesano': evidencia.formatar_mesano(),
                'valor': float(evidencia.VALOR) if evidencia.VALOR else 0,
                'descricao': evidencia.DESCRICAO,
                'id_item': evidencia.ID_ITEM
            }

            registrar_log(
                acao='editar',
                entidade='evidencia_sumov',
                entidade_id=evidencia.ID,
                descricao=f'Evidência editada: ID {evidencia.ID}',
                dados_antigos=dados_antigos,
                dados_novos=dados_novos
            )

            flash('Evidência atualizada com sucesso!', 'success')
            return redirect(url_for('sumov.evidencias_lista'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao atualizar evidência: {str(e)}', 'danger')
            return render_template('sumov/evidencias/editar.html', evidencia=evidencia)

    return render_template('sumov/evidencias/editar.html', evidencia=evidencia)

@sumov_bp.route('/evidencias/excluir/<int:id>')
@login_required
def evidencias_excluir(id):
    """Excluir evidência do banco (delete real)"""
    try:
        # Buscar evidência no banco
        evidencia = EvidenciasSumov.buscar_por_id(id)
        if not evidencia:
            flash('Evidência não encontrada.', 'danger')
            return redirect(url_for('sumov.evidencias_lista'))

        # Guardar informações para o log antes de deletar
        info_evidencia = f"ID {evidencia.ID} - {evidencia.formatar_mesano()} - R$ {evidencia.VALOR}"

        # Deletar do banco (delete real)
        db.session.delete(evidencia)
        db.session.commit()

        # Registrar log
        registrar_log(
            acao='excluir',
            entidade='evidencia_sumov',
            entidade_id=id,
            descricao=f'Evidência excluída: {info_evidencia}'
        )

        flash('Evidência excluída com sucesso!', 'success')

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao excluir evidência: {str(e)}', 'danger')

    return redirect(url_for('sumov.evidencias_lista'))


@sumov_bp.route('/despesas-pagamentos/analise', methods=['GET'])
@login_required
def analise_pagamentos():
    """Página de análise de pagamentos por mês/ano e item de serviço"""
    # Buscar todos os itens de serviço permitidos
    itens_servico = OcorrenciasMovItemServico.listar_itens_permitidos()

    # Buscar todas as datas distintas de DT_LANCAMENTO_PAGAMENTO para montar o select de mês/ano
    datas_disponiveis = db.session.query(
        db.func.year(DespesasAnalitico.DT_LANCAMENTO_PAGAMENTO).label('ano'),
        db.func.month(DespesasAnalitico.DT_LANCAMENTO_PAGAMENTO).label('mes')
    ).filter(
        DespesasAnalitico.DT_LANCAMENTO_PAGAMENTO.isnot(None)
    ).distinct().order_by(
        db.text('ano DESC, mes DESC')
    ).all()

    # Formatar datas para exibição
    meses_anos = []
    meses_nomes = {
        1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
        5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
        9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
    }

    for data in datas_disponiveis:
        meses_anos.append({
            'ano': data.ano,
            'mes': data.mes,
            'label': f"{meses_nomes[data.mes]} de {data.ano}"
        })

    return render_template('sumov/despesas_pagamentos/analise_pagamentos.html',
                           itens_servico=itens_servico,
                           meses_anos=meses_anos)


@sumov_bp.route('/despesas-pagamentos/analise/buscar', methods=['POST'])
@login_required
def buscar_analise_pagamentos():
    """Busca dados de pagamentos filtrados por mês/ano e itens de serviço"""
    try:
        data = request.get_json()

        mes = int(data.get('mes'))
        ano = int(data.get('ano'))
        itens_selecionados = data.get('itens', [])

        if not itens_selecionados:
            return jsonify({'erro': 'Selecione pelo menos um item de serviço'}), 400

        # Converter itens para inteiros
        itens_selecionados = [int(item) for item in itens_selecionados]

        # Buscar registros SUMOV usando DT_LANCAMENTO_PAGAMENTO
        registros_sumov = DespesasAnalitico.query.filter(
            db.func.year(DespesasAnalitico.DT_LANCAMENTO_PAGAMENTO) == ano,
            db.func.month(DespesasAnalitico.DT_LANCAMENTO_PAGAMENTO) == mes,
            DespesasAnalitico.ID_ITEM_SERVICO.in_(itens_selecionados),
            DespesasAnalitico.NO_ORIGEM_REGISTRO == 'SUMOV'
        ).all()

        # Buscar registros SISGEA usando DT_LANCAMENTO_PAGAMENTO
        registros_sisgea = DespesasAnalitico.query.filter(
            db.func.year(DespesasAnalitico.DT_LANCAMENTO_PAGAMENTO) == ano,
            db.func.month(DespesasAnalitico.DT_LANCAMENTO_PAGAMENTO) == mes,
            DespesasAnalitico.ID_ITEM_SERVICO.in_(itens_selecionados),
            DespesasAnalitico.NO_ORIGEM_REGISTRO == 'SISGEA'
        ).all()

        # Calcular estatísticas SUMOV
        total_sumov = Decimal('0')
        contratos_unicos_sumov = set()
        itens_agrupados_sumov = {}

        for registro in registros_sumov:
            valor = registro.VR_DESPESA if registro.VR_DESPESA else Decimal('0')
            total_sumov += valor
            contratos_unicos_sumov.add(registro.NR_CONTRATO)

            # Agrupar por item de serviço
            item = registro.DSC_ITEM_SERVICO or 'Sem descrição'
            if item not in itens_agrupados_sumov:
                itens_agrupados_sumov[item] = {'quantidade': 0, 'valor_total': Decimal('0')}
            itens_agrupados_sumov[item]['quantidade'] += 1
            itens_agrupados_sumov[item]['valor_total'] += valor

        # Calcular estatísticas SISGEA
        total_sisgea = Decimal('0')
        contratos_unicos_sisgea = set()
        itens_agrupados_sisgea = {}

        for registro in registros_sisgea:
            valor = registro.VR_DESPESA if registro.VR_DESPESA else Decimal('0')
            total_sisgea += valor
            contratos_unicos_sisgea.add(registro.NR_CONTRATO)

            # Agrupar por item de serviço
            item = registro.DSC_ITEM_SERVICO or 'Sem descrição'
            if item not in itens_agrupados_sisgea:
                itens_agrupados_sisgea[item] = {'quantidade': 0, 'valor_total': Decimal('0')}
            itens_agrupados_sisgea[item]['quantidade'] += 1
            itens_agrupados_sisgea[item]['valor_total'] += valor

        # Formatar itens agrupados SUMOV
        itens_formatados_sumov = []
        for item, dados in itens_agrupados_sumov.items():
            itens_formatados_sumov.append({
                'item': item,
                'quantidade': dados['quantidade'],
                'valor_total': float(dados['valor_total']),
                'valor_total_formatado': f"R$ {dados['valor_total']:,.2f}".replace(",", "X").replace(".", ",").replace(
                    "X", ".")
            })

        # Formatar itens agrupados SISGEA
        itens_formatados_sisgea = []
        for item, dados in itens_agrupados_sisgea.items():
            itens_formatados_sisgea.append({
                'item': item,
                'quantidade': dados['quantidade'],
                'valor_total': float(dados['valor_total']),
                'valor_total_formatado': f"R$ {dados['valor_total']:,.2f}".replace(",", "X").replace(".", ",").replace(
                    "X", ".")
            })

        return jsonify({
            'sumov': {
                'total': float(total_sumov),
                'total_formatado': f"R$ {total_sumov:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                'quantidade_registros': len(registros_sumov),
                'quantidade_contratos': len(contratos_unicos_sumov),
                'itens': itens_formatados_sumov
            },
            'sisgea': {
                'total': float(total_sisgea),
                'total_formatado': f"R$ {total_sisgea:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                'quantidade_registros': len(registros_sisgea),
                'quantidade_contratos': len(contratos_unicos_sisgea),
                'itens': itens_formatados_sisgea
            }
        })

    except Exception as e:
        return jsonify({'erro': str(e)}), 500


# ==================== DELIBERAÇÃO DE PAGAMENTO ====================

@sumov_bp.route('/deliberacao-pagamento')
@login_required
def deliberacao_pagamento():
    """Dashboard do sistema de Deliberação de Pagamento"""
    return render_template('sumov/deliberacao_pagamento/index.html')


@sumov_bp.route('/deliberacao-pagamento/nova', methods=['GET', 'POST'])
@login_required
def deliberacao_pagamento_nova():
    """Criar nova Deliberação de Pagamento"""
    if request.method == 'POST':
        try:
            # Captura dados do formulário
            contrato = request.form.get('contrato', '').strip()
            matricula = request.form.get('matricula', '').strip()
            dt_arrematacao = request.form.get('dt_arrematacao', '').strip()
            vr_divida = request.form.get('vr_divida', '0').strip().replace('.', '').replace(',', '.')

            # Validações básicas
            if not contrato:
                flash('Por favor, informe o Contrato/Imóvel.', 'danger')
                return redirect(url_for('sumov.deliberacao_pagamento_nova'))

            # Aqui você pode processar os dados ou gerar o Word
            # Por enquanto, apenas confirma o recebimento
            flash(f'Deliberação de Pagamento para contrato {contrato} registrada com sucesso!', 'success')
            return redirect(url_for('sumov.deliberacao_pagamento'))

        except Exception as e:
            flash(f'Erro ao processar deliberação: {str(e)}', 'danger')
            return redirect(url_for('sumov.deliberacao_pagamento_nova'))

    # GET - Carregar página do formulário
    return render_template('sumov/deliberacao_pagamento/nova.html')


@sumov_bp.route('/deliberacao-pagamento/buscar-dados-contrato', methods=['POST'])
@login_required
def buscar_dados_contrato():
    """Busca dados do contrato nas tabelas do banco"""
    try:
        data = request.get_json()
        contrato = data.get('contrato', '').strip()

        if not contrato:
            return jsonify({'success': False, 'message': 'Contrato não informado'})

        # ===== BUSCA 1: Data de Entrada no Estoque =====
        sql_estoque = text("""
            SELECT TOP 1 
                [DT_ENTRADA_ESTOQUE],
                [NR_CONTRATO],
                [VR_AVALIACAO],
                [DSC_STATUS_IMOVEL]
            FROM [BDDASHBOARDBI].[BDG].[MOV_TB012_IMOVEIS_NAO_USO_ESTOQUE]
            WHERE [NR_CONTRATO] = :contrato
        """)

        result_estoque = db.session.execute(sql_estoque, {'contrato': contrato}).fetchone()

        # ===== BUSCA 2: Período de Prescrição =====
        sql_prescricao = text("""
            SELECT TOP 1
                [PERIODO_PRESCRICAO],
                [DT_VENCIMENTO],
                [VR_COTA]
            FROM [BDDASHBOARDBI].[BDG].[MOV_TB032_SISCALCULO_PRESCRICOES]
            WHERE [IMOVEL] = :contrato
            ORDER BY [DT_VENCIMENTO] DESC
        """)

        result_prescricao = db.session.execute(sql_prescricao, {'contrato': contrato}).fetchone()

        # ===== BUSCA 3: Valor Inicial (Soma dos VR_COTA) =====
        sql_valor_inicial = text("""
            SELECT 
                SUM([VR_COTA]) as VALOR_INICIAL,
                COUNT(*) as QTD_PARCELAS,
                MAX([NOME_CONDOMINIO]) as NOME_CONDOMINIO
            FROM [BDDASHBOARDBI].[BDG].[MOV_TB030_SISCALCULO_DADOS]
            WHERE [IMOVEL] = :contrato
        """)

        result_valor_inicial = db.session.execute(sql_valor_inicial, {'contrato': contrato}).fetchone()

        # ===== BUSCA 4: Valores Calculados por Índice (com Honorários) =====
        sql_indices = text("""
            SELECT 
                C.ID_INDICE_ECONOMICO,
                I.DSC_INDICE_ECONOMICO,
                SUM(C.VR_TOTAL) as VALOR_SEM_HONORARIOS,
                MAX(C.PERC_HONORARIOS) as PERC_HONORARIOS,
                SUM(C.VR_TOTAL) + (SUM(C.VR_TOTAL) * MAX(C.PERC_HONORARIOS) / 100.0) as VALOR_COM_HONORARIOS,
                MAX(C.DT_ATUALIZACAO) as DT_ATUALIZACAO,
                COUNT(*) as QTD_PARCELAS
            FROM [BDDASHBOARDBI].[BDG].[MOV_TB031_SISCALCULO_CALCULOS] C
            INNER JOIN [BDDASHBOARDBI].[BDG].[PAR023_INDICES_ECONOMICOS] I 
                ON C.ID_INDICE_ECONOMICO = I.ID_INDICE_ECONOMICO
            WHERE C.IMOVEL = :contrato
            GROUP BY C.ID_INDICE_ECONOMICO, I.DSC_INDICE_ECONOMICO
            ORDER BY C.ID_INDICE_ECONOMICO
        """)

        result_indices = db.session.execute(sql_indices, {'contrato': contrato}).fetchall()

        # ===== Montar lista de índices para o select =====
        indices_disponiveis = []
        for idx in result_indices:
            indices_disponiveis.append({
                'id_indice': idx[0],
                'nome_indice': idx[1],
                'valor_sem_honorarios': float(idx[2]) if idx[2] else 0,
                'perc_honorarios': float(idx[3]) if idx[3] else 0,
                'valor_com_honorarios': float(idx[4]) if idx[4] else 0,
                'dt_atualizacao': idx[5].strftime('%d/%m/%Y') if idx[5] else None,
                'qtd_parcelas': idx[6]
            })

        # ===== Montar resposta =====
        dados = {
            'success': True,
            'dt_entrada_estoque': result_estoque[0].strftime('%Y-%m-%d') if result_estoque and result_estoque[
                0] else None,
            'vr_avaliacao': float(result_estoque[2]) if result_estoque and result_estoque[2] else None,
            'status_imovel': result_estoque[3] if result_estoque and result_estoque[3] else None,
            'periodo_prescricao': result_prescricao[0] if result_prescricao and result_prescricao[0] else None,
            'valor_inicial': float(result_valor_inicial[0]) if result_valor_inicial and result_valor_inicial[0] else 0,
            'qtd_parcelas_inicial': result_valor_inicial[1] if result_valor_inicial else 0,
            'nome_condominio': result_valor_inicial[2] if result_valor_inicial else None,
            'indices_disponiveis': indices_disponiveis
        }

        if not result_estoque and not result_prescricao and not result_valor_inicial:
            dados['message'] = 'Nenhum dado encontrado para este contrato nas tabelas consultadas.'

        return jsonify(dados)

    except Exception as e:
        import traceback
        print(f"Erro ao buscar dados do contrato: {str(e)}")
        traceback.print_exc()
        return jsonify({'success': False, 'message': f'Erro ao buscar dados: {str(e)}'})