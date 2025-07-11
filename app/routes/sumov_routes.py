from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify
from flask_login import login_required, current_user
from app import db
from app.models.relacao_imovel_contrato import RelacaoImovelContratoParcelamento
from app.utils.audit import registrar_log
from datetime import datetime
from sqlalchemy import text
from app.models.despesas_analitico import DespesasAnalitico, OcorrenciasMovItemServico
from decimal import Decimal

sumov_bp = Blueprint('sumov', __name__, url_prefix='/sumov')


@sumov_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


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

            # Validações
            if not nr_contrato:
                flash('Por favor, informe o número do contrato.', 'danger')
                return redirect(url_for('sumov.nova_despesa'))

            # Busca o item de serviço selecionado
            item_servico = OcorrenciasMovItemServico.query.get(id_item_servico)
            if not item_servico:
                flash('Item de serviço inválido.', 'danger')
                return redirect(url_for('sumov.nova_despesa'))

            # Gera o número de ocorrência
            nr_ocorrencia = DespesasAnalitico.obter_proximo_numero_ocorrencia(nr_contrato)

            # Cria o novo registro
            nova_despesa = DespesasAnalitico(
                DT_REFERENCIA=datetime.now().date(),
                NR_CONTRATO=nr_contrato,
                NR_OCORRENCIA=nr_ocorrencia,
                DSC_ITEM_SERVICO=item_servico.DSC_ITEM_SERVICO,
                DT_LANCAMENTO_PAGAMENTO=dt_lancamento.date(),
                VR_DESPESA=vr_despesa,
                estadoLancamento='Pagamento Efetuado',
                ID_ITEM_SISCOR=1410,
                ID_ITEM_SERVICO=id_item_servico,
                NR_CTR_ORIGINAL_TABELA=nr_contrato,
                DSC_TIPO_FORMA_PGTO=dsc_tipo_forma_pgto,
                NO_ORIGEM_REGISTRO='SUMOV'
            )

            db.session.add(nova_despesa)
            db.session.commit()

            # Registrar log
            registrar_log(
                acao='criar',
                entidade='despesa_pagamento',
                entidade_id=nr_ocorrencia,
                descricao=f'Novo pagamento registrado: Contrato {nr_contrato} - Ocorrência {nr_ocorrencia} - Valor R$ {vr_despesa}'
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