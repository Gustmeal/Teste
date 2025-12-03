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
from app.models.deliberacao_pagamento import DeliberacaoPagamento
from flask import send_file

sumov_bp = Blueprint('sumov', __name__, url_prefix='/sumov')

@sumov_bp.context_processor
def inject_current_year():
    from datetime import datetime
    return {
        'current_year': datetime.utcnow().year,
        'data_hoje': datetime.now().strftime('%d/%m/%Y')  # ADICIONE ESTA LINHA
    }

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
    """Dashboard do sistema de Deliberação de Pagamento com lista de deliberações"""
    try:
        from datetime import datetime

        # Buscar todas as deliberações salvas
        deliberacoes = DeliberacaoPagamento.query.filter(
            DeliberacaoPagamento.DELETED_AT.is_(None)
        ).order_by(
            DeliberacaoPagamento.CREATED_AT.desc()
        ).all()

        return render_template('sumov/deliberacao_pagamento/index.html',
                               deliberacoes=deliberacoes,
                               data_hoje=datetime.now().strftime('%d/%m/%Y'))
    except Exception as e:
        from datetime import datetime
        flash(f'Erro ao carregar deliberações: {str(e)}', 'danger')
        return render_template('sumov/deliberacao_pagamento/index.html',
                               deliberacoes=[],
                               data_hoje=datetime.now().strftime('%d/%m/%Y'))


@sumov_bp.route('/deliberacao-pagamento/nova', methods=['GET', 'POST'])
@login_required
def deliberacao_pagamento_nova():
    """Criar nova Deliberação de Pagamento seguindo estrutura do Word"""
    if request.method == 'POST':
        try:
            from decimal import Decimal, InvalidOperation

            # ===== CAPTURA DADOS DO FORMULÁRIO =====
            contrato = request.form.get('contrato', '').strip()
            matricula = request.form.get('matricula', '').strip() or None
            dt_arrematacao_str = request.form.get('dt_arrematacao', '').strip()
            indice_selecionado_idx = request.form.get('indice_economico', '').strip()
            dt_registro_str = request.form.get('dt_registro', '').strip()

            # NOVOS CAMPOS
            vr_divida_condominio_2_str = request.form.get('vr_divida_condominio_2', '').strip()
            dt_periodo_inicio_str = request.form.get('dt_periodo_cobranca_inicio', '').strip()
            dt_periodo_fim_str = request.form.get('dt_periodo_cobranca_fim', '').strip()

            # Campos da parte final
            gravame_matricula = request.form.get('gravame_matricula', '').strip() or None
            acoes_negociais_adm = request.form.get('acoes_negociais_administrativas', '').strip() or None
            nr_processos = request.form.get('nr_processos_judiciais', '').strip() or None
            vara_processo = request.form.get('vara_processo', '').strip() or None
            fase_processo = request.form.get('fase_processo', '').strip() or None
            relatorio_assessoria = request.form.get('relatorio_assessoria_juridica', '').strip() or None
            penalidade_ans = request.form.get('penalidade_ans_caixa', '').strip() or None
            prejuizo_financeiro = request.form.get('prejuizo_financeiro_caixa', '').strip() or None
            consideracoes_analista = request.form.get('consideracoes_analista', '').strip() or None
            consideracoes_gestor = request.form.get('consideracoes_gestor', '').strip() or None
            consideracoes_gestor_sumov = request.form.get('consideracoes_gestor_sumov', '').strip() or None
            tipo_pagamento_venda = request.form.get('tipo_pagamento_venda', '').strip() or None

            # Validação básica obrigatória
            if not contrato:
                flash('Por favor, informe o Contrato/Imóvel.', 'danger')
                return redirect(url_for('sumov.deliberacao_pagamento_nova'))

            if not indice_selecionado_idx:
                flash('Por favor, selecione o Índice Econômico.', 'danger')
                return redirect(url_for('sumov.deliberacao_pagamento_nova'))

            if not dt_registro_str:
                flash('Por favor, informe a Data do Registro.', 'danger')
                return redirect(url_for('sumov.deliberacao_pagamento_nova'))

            # Converter datas
            dt_arrematacao = None
            if dt_arrematacao_str:
                try:
                    dt_arrematacao = datetime.strptime(dt_arrematacao_str, '%Y-%m-%d').date()
                except ValueError:
                    flash('Data de arrematação inválida.', 'danger')
                    return redirect(url_for('sumov.deliberacao_pagamento_nova'))

            dt_registro = None
            if dt_registro_str:
                try:
                    dt_registro = datetime.strptime(dt_registro_str, '%Y-%m-%d').date()
                except ValueError:
                    flash('Data do registro inválida.', 'danger')
                    return redirect(url_for('sumov.deliberacao_pagamento_nova'))

            # NOVOS: Converter datas do período de cobrança
            dt_periodo_cobranca_inicio = None
            if dt_periodo_inicio_str:
                try:
                    dt_periodo_cobranca_inicio = datetime.strptime(dt_periodo_inicio_str, '%Y-%m-%d').date()
                except ValueError:
                    flash('Data de início do período de cobrança inválida.', 'danger')
                    return redirect(url_for('sumov.deliberacao_pagamento_nova'))

            dt_periodo_cobranca_fim = None
            if dt_periodo_fim_str:
                try:
                    dt_periodo_cobranca_fim = datetime.strptime(dt_periodo_fim_str, '%Y-%m-%d').date()
                except ValueError:
                    flash('Data de fim do período de cobrança inválida.', 'danger')
                    return redirect(url_for('sumov.deliberacao_pagamento_nova'))

            # NOVO: Converter valor manual
            vr_divida_condominio_2 = None
            if vr_divida_condominio_2_str:
                try:
                    # Remover formatação brasileira e converter
                    valor_limpo = vr_divida_condominio_2_str.replace('R$', '').replace('.', '').replace(',',
                                                                                                        '.').strip()
                    if valor_limpo:
                        vr_divida_condominio_2 = Decimal(valor_limpo)
                except (ValueError, InvalidOperation):
                    flash('Valor da Dívida (Manual) inválido.', 'danger')
                    return redirect(url_for('sumov.deliberacao_pagamento_nova'))

            # ===== BUSCA 1: Data de Entrada no Estoque =====
            sql_estoque = text("""
                SELECT TOP 1 
                    [DT_ENTRADA_ESTOQUE]
                FROM [BDDASHBOARDBI].[BDG].[MOV_TB012_IMOVEIS_NAO_USO_ESTOQUE]
                WHERE [NR_CONTRATO] = :contrato
            """)
            result_estoque = db.session.execute(sql_estoque, {'contrato': contrato}).fetchone()
            dt_entrada_estoque = result_estoque[0] if result_estoque else None

            # ===== BUSCA 2: Período de Prescrição =====
            sql_prescricao = text("""
                SELECT TOP 1
                    [PERIODO_PRESCRICAO]
                FROM [BDDASHBOARDBI].[BDG].[MOV_TB032_SISCALCULO_PRESCRICOES]
                WHERE [IMOVEL] = :contrato
                ORDER BY [DT_VENCIMENTO] DESC
            """)
            result_prescricao = db.session.execute(sql_prescricao, {'contrato': contrato}).fetchone()
            periodo_prescrito = result_prescricao[0] if result_prescricao else None

            # ===== BUSCA 3: Valor Inicial (DA ÚLTIMA ATUALIZAÇÃO) =====
            sql_valor_inicial = text("""
                SELECT 
                    SUM([VR_COTA]) as VALOR_INICIAL,
                    MAX([NOME_CONDOMINIO]) as NOME_CONDOMINIO
                FROM [BDDASHBOARDBI].[BDG].[MOV_TB030_SISCALCULO_DADOS]
                WHERE [IMOVEL] = :contrato
                AND [DT_ATUALIZACAO] = (
                    SELECT MAX([DT_ATUALIZACAO])
                    FROM [BDDASHBOARDBI].[BDG].[MOV_TB030_SISCALCULO_DADOS]
                    WHERE [IMOVEL] = :contrato
                )
            """)
            result_valor = db.session.execute(sql_valor_inicial, {'contrato': contrato}).fetchone()

            vr_divida_inicial = result_valor[0] if result_valor and result_valor[0] else Decimal('0')
            nome_condominio = result_valor[1] if result_valor and result_valor[1] else None

            # ===== BUSCA 4: Valor de Débito EXCLUÍDOS as Cotas Prescritas (DE UM ÚNICO ÍNDICE) =====
            sql_valor_prescrito = text("""
                SELECT 
                    SUM([VR_COTA]) as VALOR_PRESCRITO
                FROM [BDDASHBOARDBI].[BDG].[MOV_TB032_SISCALCULO_PRESCRICOES]
                WHERE [IMOVEL] = :contrato
                AND [ID_INDICE_ECONOMICO] = (
                    SELECT MIN([ID_INDICE_ECONOMICO])
                    FROM [BDDASHBOARDBI].[BDG].[MOV_TB032_SISCALCULO_PRESCRICOES]
                    WHERE [IMOVEL] = :contrato
                    AND [DT_ATUALIZACAO] = (
                        SELECT MAX([DT_ATUALIZACAO])
                        FROM [BDDASHBOARDBI].[BDG].[MOV_TB032_SISCALCULO_PRESCRICOES]
                        WHERE [IMOVEL] = :contrato
                    )
                )
                AND [DT_ATUALIZACAO] = (
                    SELECT MAX([DT_ATUALIZACAO])
                    FROM [BDDASHBOARDBI].[BDG].[MOV_TB032_SISCALCULO_PRESCRICOES]
                    WHERE [IMOVEL] = :contrato
                )
            """)
            result_prescrito = db.session.execute(sql_valor_prescrito, {'contrato': contrato}).fetchone()
            vr_debito_excluido_prescritas = result_prescrito[0] if result_prescrito and result_prescrito[
                0] else Decimal('0')

            # ===== BUSCA 5: Cálculos do SISCalculo =====
            sql_calculos = text("""
                SELECT 
                    ID_INDICE_ECONOMICO,
                    SUM(VR_TOTAL) AS VR_TOTAL_SEM_HONORARIOS,
                    MAX(PERC_HONORARIOS) AS PERC_HONORARIOS,
                    SUM(VR_TOTAL) * MAX(PERC_HONORARIOS) / 100.0 AS VR_HONORARIOS,
                    SUM(VR_TOTAL) + (SUM(VR_TOTAL) * MAX(PERC_HONORARIOS) / 100.0) AS VR_TOTAL_COM_HONORARIOS
                FROM [BDDASHBOARDBI].[BDG].[MOV_TB031_SISCALCULO_CALCULOS]
                WHERE [IMOVEL] = :contrato
                AND [DT_ATUALIZACAO] = (
                    SELECT MAX([DT_ATUALIZACAO])
                    FROM [BDDASHBOARDBI].[BDG].[MOV_TB031_SISCALCULO_CALCULOS]
                    WHERE [IMOVEL] = :contrato
                )
                GROUP BY ID_INDICE_ECONOMICO
                ORDER BY ID_INDICE_ECONOMICO
            """)
            result_calculos = db.session.execute(sql_calculos, {'contrato': contrato}).fetchall()

            if not result_calculos:
                flash('Nenhum cálculo encontrado no SISCalculo para este contrato. Processe os cálculos primeiro.',
                      'warning')
                return redirect(url_for('sumov.deliberacao_pagamento_nova'))

            # Pegar o índice selecionado
            try:
                idx = int(indice_selecionado_idx)
                calculo = result_calculos[idx]
            except (ValueError, IndexError):
                flash('Índice econômico inválido.', 'danger')
                return redirect(url_for('sumov.deliberacao_pagamento_nova'))

            id_indice = calculo[0]
            vr_divida_calculada = calculo[1]
            perc_honorarios_emgea = calculo[2]
            vr_honorarios = calculo[3]
            vr_debito_calculado_emgea = calculo[4]

            # Buscar nome do índice
            from app.models.siscalculo import ParamIndicesEconomicos
            indice_obj = ParamIndicesEconomicos.query.get(id_indice)
            indice_debito_emgea = indice_obj.DSC_INDICE_ECONOMICO if indice_obj else f"Índice {id_indice}"

            # ===== BUSCA 6: Valor de Avaliação e Data do Laudo (SEMPRE DA ÚLTIMA DT_REFERENCIA) =====
            sql_avaliacao = text("""
                SELECT TOP 1
                    [VR_LAUDO_AVALIACAO],
                    [DT_LAUDO]
                FROM [BDDASHBOARDBI].[BDG].[MOV_TB001_IMOVEIS_NAO_USO_STATUS]
                WHERE [NR_CONTRATO] = :contrato
                ORDER BY [DT_REFERENCIA] DESC
            """)
            result_avaliacao = db.session.execute(sql_avaliacao, {'contrato': contrato}).fetchone()

            vr_avaliacao = result_avaliacao[0] if result_avaliacao and result_avaliacao[0] else None
            dt_laudo = result_avaliacao[1] if result_avaliacao and result_avaliacao[1] else None

            # ===== BUSCA 7: Status do Imóvel =====
            sql_status = text("""
                SELECT TOP 1
                    [DSC_SITUACAO]
                FROM [BDDASHBOARDBI].[BDG].[MOV_TB020_IMOVEIS_RM_TOTVS]
                WHERE [IMOVEL] = :contrato
            """)
            result_status = db.session.execute(sql_status, {'contrato': contrato}).fetchone()
            status_imovel = result_status[0] if result_status else None

            # ===== BUSCA 8: Dados de Venda =====
            sql_venda = text("""
                SELECT TOP 1
                    [VR_VENDA],
                    [DT_VENDA],
                    [NO_COMPRADOR]
                FROM [BDDASHBOARDBI].[BDG].[MOV_TB023_VENDA_IMOVEIS_RM_TOTVS]
                WHERE [NU_IMOVEL] = :contrato
                ORDER BY [DT_VENDA] DESC
            """)
            result_venda = db.session.execute(sql_venda, {'contrato': contrato}).fetchone()

            vr_venda = result_venda[0] if result_venda and result_venda[0] else None
            dt_venda = result_venda[1] if result_venda and result_venda[1] else None
            nome_comprador = result_venda[2] if result_venda and result_venda[2] else None

            # ===== BUSCA 9: Débitos Pagos =====
            sql_sisdex = text("""
                SELECT SUM([VR_PAGO]) as TOTAL_SISDEX
                FROM [BDDASHBOARDBI].[BDG].[MOV_TB017_DESPESAS_MANUTENCAO]
                WHERE [NU_CONTRATO] = :contrato
            """)
            result_sisdex = db.session.execute(sql_sisdex, {'contrato': contrato}).fetchone()
            vr_debitos_sisdex = result_sisdex[0] if result_sisdex and result_sisdex[0] else Decimal('0')

            sql_sisgea = text("""
                SELECT SUM([VR_DESPESA]) as TOTAL_SISGEA
                FROM [BDDASHBOARDBI].[BDG].[MOV_TB004_DESPESAS_ANALITICO]
                WHERE [NR_CONTRATO] = :contrato
            """)
            result_sisgea = db.session.execute(sql_sisgea, {'contrato': contrato}).fetchone()
            vr_debitos_sisgea = result_sisgea[0] if result_sisgea and result_sisgea[0] else Decimal('0')

            vr_debitos_total = vr_debitos_sisdex + vr_debitos_sisgea

            # ===== VERIFICAR SE JÁ EXISTE REGISTRO =====
            deliberacao_existente = DeliberacaoPagamento.buscar_por_contrato(contrato)

            if deliberacao_existente:
                # ATUALIZAR registro existente
                deliberacao_existente.COLABORADOR_ANALISOU = current_user.nome
                deliberacao_existente.DT_ANALISE = datetime.now().date()
                deliberacao_existente.MATRICULA_CAIXA_EMGEA = matricula
                deliberacao_existente.DT_ARREMATACAO_AQUISICAO = dt_arrematacao
                deliberacao_existente.DT_ENTRADA_ESTOQUE = dt_entrada_estoque
                deliberacao_existente.PERIODO_PRESCRITO = periodo_prescrito
                deliberacao_existente.DT_PERIODO_COBRANCA_INICIO = dt_periodo_cobranca_inicio
                deliberacao_existente.DT_PERIODO_COBRANCA_FIM = dt_periodo_cobranca_fim
                deliberacao_existente.VR_DIVIDA_CONDOMINIO_1 = vr_divida_inicial
                deliberacao_existente.VR_DIVIDA_CONDOMINIO_2 = vr_divida_condominio_2
                deliberacao_existente.VR_DEBITO_EXCLUIDO_PRESCRITAS = vr_debito_excluido_prescritas
                deliberacao_existente.VR_DEBITO_CALCULADO_EMGEA = vr_debito_calculado_emgea
                deliberacao_existente.INDICE_DEBITO_EMGEA = indice_debito_emgea
                deliberacao_existente.PERC_HONORARIOS_EMGEA = perc_honorarios_emgea
                deliberacao_existente.VR_HONORARIOS_EMGEA = vr_honorarios
                deliberacao_existente.DT_CALCULO_EMGEA = datetime.now().date()
                deliberacao_existente.VR_AVALIACAO = vr_avaliacao
                deliberacao_existente.DT_LAUDO = dt_laudo
                deliberacao_existente.STATUS_IMOVEL = status_imovel
                deliberacao_existente.VR_VENDA = vr_venda
                deliberacao_existente.DT_VENDA = dt_venda
                deliberacao_existente.NOME_COMPRADOR = nome_comprador
                deliberacao_existente.DT_REGISTRO = dt_registro
                deliberacao_existente.VR_DEBITOS_SISDEX = vr_debitos_sisdex
                deliberacao_existente.VR_DEBITOS_SISGEA = vr_debitos_sisgea
                deliberacao_existente.VR_DEBITOS_TOTAL = vr_debitos_total
                deliberacao_existente.GRAVAME_MATRICULA = gravame_matricula
                deliberacao_existente.ACOES_NEGOCIAIS_ADMINISTRATIVAS = acoes_negociais_adm
                deliberacao_existente.NR_PROCESSOS_JUDICIAIS = nr_processos
                deliberacao_existente.VARA_PROCESSO = vara_processo
                deliberacao_existente.FASE_PROCESSO = fase_processo
                deliberacao_existente.RELATORIO_ASSESSORIA_JURIDICA = relatorio_assessoria
                deliberacao_existente.PENALIDADE_ANS_CAIXA = penalidade_ans
                deliberacao_existente.PREJUIZO_FINANCEIRO_CAIXA = prejuizo_financeiro
                deliberacao_existente.CONSIDERACOES_ANALISTA_GEADI = consideracoes_analista
                deliberacao_existente.CONSIDERACOES_GESTOR_GEADI = consideracoes_gestor
                deliberacao_existente.CONSIDERACOES_GESTOR_SUMOV = consideracoes_gestor_sumov
                deliberacao_existente.TIPO_PAGAMENTO_VENDA = tipo_pagamento_venda
                deliberacao_existente.USUARIO_ATUALIZACAO = current_user.nome
                deliberacao_existente.UPDATED_AT = datetime.utcnow()

                deliberacao = deliberacao_existente
                acao_log = 'editar'
                mensagem = 'Deliberação de Pagamento atualizada com sucesso!'
            else:
                # CRIAR novo registro
                deliberacao = DeliberacaoPagamento(
                    NU_CONTRATO=contrato,
                    COLABORADOR_ANALISOU=current_user.nome,
                    DT_ANALISE=datetime.now().date(),
                    MATRICULA_CAIXA_EMGEA=matricula,
                    DT_ARREMATACAO_AQUISICAO=dt_arrematacao,
                    DT_ENTRADA_ESTOQUE=dt_entrada_estoque,
                    PERIODO_PRESCRITO=periodo_prescrito,
                    DT_PERIODO_COBRANCA_INICIO=dt_periodo_cobranca_inicio,
                    DT_PERIODO_COBRANCA_FIM=dt_periodo_cobranca_fim,
                    VR_DIVIDA_CONDOMINIO_1=vr_divida_inicial,
                    VR_DIVIDA_CONDOMINIO_2=vr_divida_condominio_2,
                    VR_DEBITO_EXCLUIDO_PRESCRITAS=vr_debito_excluido_prescritas,
                    VR_DEBITO_CALCULADO_EMGEA=vr_debito_calculado_emgea,
                    INDICE_DEBITO_EMGEA=indice_debito_emgea,
                    PERC_HONORARIOS_EMGEA=perc_honorarios_emgea,
                    VR_HONORARIOS_EMGEA=vr_honorarios,
                    DT_CALCULO_EMGEA=datetime.now().date(),
                    VR_AVALIACAO=vr_avaliacao,
                    DT_LAUDO=dt_laudo,
                    STATUS_IMOVEL=status_imovel,
                    VR_VENDA=vr_venda,
                    DT_VENDA=dt_venda,
                    NOME_COMPRADOR=nome_comprador,
                    DT_REGISTRO=dt_registro,
                    VR_DEBITOS_SISDEX=vr_debitos_sisdex,
                    VR_DEBITOS_SISGEA=vr_debitos_sisgea,
                    VR_DEBITOS_TOTAL=vr_debitos_total,
                    GRAVAME_MATRICULA=gravame_matricula,
                    ACOES_NEGOCIAIS_ADMINISTRATIVAS=acoes_negociais_adm,
                    NR_PROCESSOS_JUDICIAIS=nr_processos,
                    VARA_PROCESSO=vara_processo,
                    FASE_PROCESSO=fase_processo,
                    RELATORIO_ASSESSORIA_JURIDICA=relatorio_assessoria,
                    PENALIDADE_ANS_CAIXA=penalidade_ans,
                    PREJUIZO_FINANCEIRO_CAIXA=prejuizo_financeiro,
                    CONSIDERACOES_ANALISTA_GEADI=consideracoes_analista,
                    CONSIDERACOES_GESTOR_GEADI=consideracoes_gestor,
                    CONSIDERACOES_GESTOR_SUMOV=consideracoes_gestor_sumov,
                    TIPO_PAGAMENTO_VENDA=tipo_pagamento_venda,
                    STATUS_DOCUMENTO='RASCUNHO',
                    USUARIO_CRIACAO=current_user.nome,
                    CREATED_AT=datetime.utcnow()
                )

                acao_log = 'criar'
                mensagem = 'Deliberação de Pagamento criada com sucesso!'

            # ===== SALVAR NO BANCO =====
            if deliberacao.salvar():
                registrar_log(
                    acao=acao_log,
                    entidade='deliberacao_pagamento',
                    entidade_id=contrato,
                    descricao=f'Deliberação de Pagamento para contrato {contrato}: {indice_debito_emgea}'
                )

                flash(mensagem, 'success')
                return redirect(url_for('sumov.deliberacao_pagamento'))
            else:
                flash('Erro ao salvar deliberação no banco de dados.', 'danger')
                return redirect(url_for('sumov.deliberacao_pagamento_nova'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao processar deliberação: {str(e)}', 'danger')
            import traceback
            traceback.print_exc()
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
                [DT_ENTRADA_ESTOQUE]
            FROM [BDDASHBOARDBI].[BDG].[MOV_TB012_IMOVEIS_NAO_USO_ESTOQUE]
            WHERE [NR_CONTRATO] = :contrato
        """)
        result_estoque = db.session.execute(sql_estoque, {'contrato': contrato}).fetchone()
        dt_entrada_estoque = result_estoque[0].strftime('%Y-%m-%d') if result_estoque and result_estoque[0] else None

        # ===== BUSCA 2: Período de Prescrição =====
        sql_prescricao = text("""
            SELECT TOP 1
                [PERIODO_PRESCRICAO]
            FROM [BDDASHBOARDBI].[BDG].[MOV_TB032_SISCALCULO_PRESCRICOES]
            WHERE [IMOVEL] = :contrato
            ORDER BY [DT_VENCIMENTO] DESC
        """)
        result_prescricao = db.session.execute(sql_prescricao, {'contrato': contrato}).fetchone()
        periodo_prescricao = result_prescricao[0] if result_prescricao else None

        # ===== BUSCA 2.5: Período da Cobrança dos Débitos (MIN e MAX DT_VENCIMENTO) =====
        sql_periodo_cobranca = text("""
            SELECT 
                MIN([DT_VENCIMENTO]) as DT_INICIO,
                MAX([DT_VENCIMENTO]) as DT_FIM
            FROM [BDDASHBOARDBI].[BDG].[MOV_TB030_SISCALCULO_DADOS]
            WHERE [IMOVEL] = :contrato
            AND [DT_ATUALIZACAO] = (
                SELECT MAX([DT_ATUALIZACAO])
                FROM [BDDASHBOARDBI].[BDG].[MOV_TB030_SISCALCULO_DADOS]
                WHERE [IMOVEL] = :contrato
            )
        """)
        result_periodo_cobranca = db.session.execute(sql_periodo_cobranca, {'contrato': contrato}).fetchone()

        dt_periodo_cobranca_inicio = None
        dt_periodo_cobranca_fim = None
        periodo_cobranca_encontrado = False

        if result_periodo_cobranca and result_periodo_cobranca[0] and result_periodo_cobranca[1]:
            dt_periodo_cobranca_inicio = result_periodo_cobranca[0].strftime('%Y-%m-%d')
            dt_periodo_cobranca_fim = result_periodo_cobranca[1].strftime('%Y-%m-%d')
            periodo_cobranca_encontrado = True

        # ===== BUSCA 3: Valor Inicial (Soma dos VR_COTA - DA ÚLTIMA ATUALIZAÇÃO) =====
        sql_valor_inicial = text("""
            SELECT 
                SUM([VR_COTA]) as VALOR_INICIAL,
                COUNT(*) as QTD_PARCELAS,
                MAX([NOME_CONDOMINIO]) as NOME_CONDOMINIO
            FROM [BDDASHBOARDBI].[BDG].[MOV_TB030_SISCALCULO_DADOS]
            WHERE [IMOVEL] = :contrato
            AND [DT_ATUALIZACAO] = (
                SELECT MAX([DT_ATUALIZACAO])
                FROM [BDDASHBOARDBI].[BDG].[MOV_TB030_SISCALCULO_DADOS]
                WHERE [IMOVEL] = :contrato
            )
        """)
        result_valor = db.session.execute(sql_valor_inicial, {'contrato': contrato}).fetchone()

        valor_inicial = float(result_valor[0]) if result_valor and result_valor[0] else 0
        qtd_parcelas_inicial = result_valor[1] if result_valor and result_valor[1] else 0
        nome_condominio = result_valor[2] if result_valor and result_valor[2] else None

        # ===== BUSCA 4: Valor de Débito EXCLUÍDOS as Cotas Prescritas (DE UM ÚNICO ÍNDICE) =====
        sql_valor_prescrito = text("""
            SELECT 
                SUM([VR_COTA]) as VALOR_PRESCRITO,
                COUNT(*) as QTD_PRESCRITO
            FROM [BDDASHBOARDBI].[BDG].[MOV_TB032_SISCALCULO_PRESCRICOES]
            WHERE [IMOVEL] = :contrato
            AND [ID_INDICE_ECONOMICO] = (
                SELECT MIN([ID_INDICE_ECONOMICO])
                FROM [BDDASHBOARDBI].[BDG].[MOV_TB032_SISCALCULO_PRESCRICOES]
                WHERE [IMOVEL] = :contrato
                AND [DT_ATUALIZACAO] = (
                    SELECT MAX([DT_ATUALIZACAO])
                    FROM [BDDASHBOARDBI].[BDG].[MOV_TB032_SISCALCULO_PRESCRICOES]
                    WHERE [IMOVEL] = :contrato
                )
            )
            AND [DT_ATUALIZACAO] = (
                SELECT MAX([DT_ATUALIZACAO])
                FROM [BDDASHBOARDBI].[BDG].[MOV_TB032_SISCALCULO_PRESCRICOES]
                WHERE [IMOVEL] = :contrato
            )
        """)
        result_prescrito = db.session.execute(sql_valor_prescrito, {'contrato': contrato}).fetchone()

        valor_prescrito = float(result_prescrito[0]) if result_prescrito and result_prescrito[0] else 0
        qtd_prescrito = result_prescrito[1] if result_prescrito and result_prescrito[1] else 0

        # ===== BUSCA 5: Índices disponíveis no SISCalculo COM TOTAIS POR TIPO =====
        sql_indices = text("""
            SELECT 
                C.ID_INDICE_ECONOMICO,
                SUM(C.VR_TOTAL - (C.VR_TOTAL * C.PERC_HONORARIOS / 100.0)) as VR_DIVIDA,
                MAX(C.PERC_HONORARIOS) as PERC_HONORARIOS,
                SUM(C.VR_TOTAL * C.PERC_HONORARIOS / 100.0) as VR_HONORARIOS,
                SUM(C.VR_TOTAL) as VR_TOTAL_COM_HONORARIOS
            FROM [BDDASHBOARDBI].[BDG].[MOV_TB031_SISCALCULO_CALCULOS] C
            WHERE C.[IMOVEL] = :contrato
            AND C.[DT_ATUALIZACAO] = (
                SELECT MAX([DT_ATUALIZACAO])
                FROM [BDDASHBOARDBI].[BDG].[MOV_TB031_SISCALCULO_CALCULOS]
                WHERE [IMOVEL] = :contrato
            )
            GROUP BY C.ID_INDICE_ECONOMICO
            ORDER BY C.ID_INDICE_ECONOMICO
        """)
        result_indices = db.session.execute(sql_indices, {'contrato': contrato}).fetchall()

        indices_disponiveis = []
        if result_indices:
            from app.models.siscalculo import ParamIndicesEconomicos, SiscalculoCalculos, TipoParcela

            for idx, row in enumerate(result_indices):
                id_indice = row[0]
                vr_total_sem_honorarios = float(row[1]) if row[1] else 0
                perc_honorarios = float(row[2]) if row[2] else 0
                vr_honorarios = float(row[3]) if row[3] else 0
                vr_total_com_honorarios = float(row[4]) if row[4] else 0

                indice_obj = ParamIndicesEconomicos.query.get(id_indice)
                nome_indice = indice_obj.DSC_INDICE_ECONOMICO if indice_obj else f"Índice {id_indice}"

                # ✅ NOVO: Buscar totais por tipo de parcela para este índice específico
                totais_por_tipo = db.session.query(
                    SiscalculoCalculos.ID_TIPO,
                    TipoParcela.DSC_TIPO,
                    db.func.count(SiscalculoCalculos.DT_VENCIMENTO).label('quantidade'),
                    db.func.sum(SiscalculoCalculos.VR_TOTAL).label('valor_total')
                ).join(
                    TipoParcela,
                    SiscalculoCalculos.ID_TIPO == TipoParcela.ID_TIPO
                ).filter(
                    SiscalculoCalculos.IMOVEL == contrato,
                    SiscalculoCalculos.ID_INDICE_ECONOMICO == id_indice,
                    SiscalculoCalculos.DT_ATUALIZACAO == db.session.query(
                        db.func.max(SiscalculoCalculos.DT_ATUALIZACAO)
                    ).filter(
                        SiscalculoCalculos.IMOVEL == contrato
                    ).scalar_subquery()
                ).group_by(
                    SiscalculoCalculos.ID_TIPO,
                    TipoParcela.DSC_TIPO
                ).order_by(
                    SiscalculoCalculos.ID_TIPO
                ).all()

                # Converter para lista de dicionários
                tipos_parcela_lista = [
                    {
                        'id_tipo': t.ID_TIPO,
                        'descricao': t.DSC_TIPO,
                        'quantidade': t.quantidade,
                        'valor_total': float(t.valor_total) if t.valor_total else 0
                    }
                    for t in totais_por_tipo
                ]

                indices_disponiveis.append({
                    'id_indice': id_indice,
                    'nome_indice': nome_indice,
                    'valor_divida': vr_total_sem_honorarios,
                    'valor_honorarios': vr_honorarios,
                    'perc_honorarios': perc_honorarios,
                    'valor_com_honorarios': vr_total_com_honorarios,
                    'totais_por_tipo': tipos_parcela_lista  # ✅ NOVO CAMPO
                })

        # ===== BUSCA 6: Valor de Avaliação e Data do Laudo =====
        sql_avaliacao = text("""
            SELECT TOP 1
                [VR_LAUDO_AVALIACAO],
                [DT_LAUDO]
            FROM [BDDASHBOARDBI].[BDG].[MOV_TB001_IMOVEIS_NAO_USO_STATUS]
            WHERE [NR_CONTRATO] = :contrato
            ORDER BY [DT_REFERENCIA] DESC
        """)
        result_avaliacao = db.session.execute(sql_avaliacao, {'contrato': contrato}).fetchone()

        vr_avaliacao = float(result_avaliacao[0]) if result_avaliacao and result_avaliacao[0] else None
        dt_laudo = result_avaliacao[1].strftime('%Y-%m-%d') if result_avaliacao and result_avaliacao[1] else None

        # ===== BUSCA 7: Status do Imóvel =====
        sql_status = text("""
            SELECT TOP 1
                [DSC_SITUACAO]
            FROM [BDDASHBOARDBI].[BDG].[MOV_TB020_IMOVEIS_RM_TOTVS]
            WHERE [IMOVEL] = :contrato
        """)
        result_status = db.session.execute(sql_status, {'contrato': contrato}).fetchone()
        status_imovel = result_status[0] if result_status else None

        # ===== BUSCA 8: Dados de Venda + TIPO DE PAGAMENTO =====
        sql_venda = text("""
            SELECT TOP 1
                [VR_VENDA],
                [DT_VENDA],
                [NO_COMPRADOR],
                [VR_SD_DEVEDOR]
            FROM [BDDASHBOARDBI].[BDG].[MOV_TB023_VENDA_IMOVEIS_RM_TOTVS]
            WHERE [NU_IMOVEL] = :contrato
            ORDER BY [DT_VENDA] DESC
        """)
        result_venda = db.session.execute(sql_venda, {'contrato': contrato}).fetchone()

        vr_venda = float(result_venda[0]) if result_venda and result_venda[0] else None
        dt_venda = result_venda[1].strftime('%Y-%m-%d') if result_venda and result_venda[1] else None
        nome_comprador = result_venda[2] if result_venda and result_venda[2] else None

        # Determinar tipo de pagamento
        tipo_pagamento_venda = None
        if result_venda and result_venda[3] is not None:
            vr_sd_devedor = float(result_venda[3])
            if vr_sd_devedor > 0:
                tipo_pagamento_venda = "PARCELADO"
            else:
                tipo_pagamento_venda = "A_VISTA"

        # ===== BUSCA 9: Processos Judiciais =====
        sql_processos = text("""
            SELECT [nrProcessoCnj]
            FROM [BDEMGEAODS].[SISJUD].[tblProcessoCredito]
            WHERE [nrContrato] = :contrato
        """)
        result_processos = db.session.execute(sql_processos, {'contrato': contrato}).fetchall()

        if not result_processos:
            try:
                sql_processos_alt = text("""
                    SELECT [nrProcessoCnj]
                    FROM [BDEMGEAODS].[SISJUD].[tblProcessoCredito]
                    WHERE CAST([fkContratoSISCTR] AS VARCHAR(50)) = :contrato
                """)
                result_processos = db.session.execute(sql_processos_alt, {'contrato': contrato}).fetchall()
            except:
                result_processos = []

        processos_judiciais = []
        if result_processos:
            for processo in result_processos:
                nr_processo = processo[0]

                sql_detalhes = text("""
                    SELECT 
                        [dsComarca],
                        [dsLocal],
                        [dsFaseProcessual]
                    FROM [BDEMGEAODS].[SISJUD].[tblProcessoEmgea]
                    WHERE [nrProcessoCnj] = :nr_processo
                """)
                result_detalhes = db.session.execute(sql_detalhes, {'nr_processo': nr_processo}).fetchone()

                if result_detalhes:
                    comarca = result_detalhes[0] if result_detalhes[0] else ''
                    local = result_detalhes[1] if result_detalhes[1] else ''
                    fase = result_detalhes[2] if result_detalhes[2] else ''

                    vara = f"{comarca}-{local}" if comarca and local else comarca or local or 'Não informado'

                    processos_judiciais.append({
                        'numero': nr_processo,
                        'vara': vara,
                        'fase': fase
                    })

        # ===== BUSCA 10: Débitos Pagos - SISDEX =====
        sql_sisdex = text("""
            SELECT SUM([VR_PAGO]) as TOTAL_SISDEX
            FROM [BDDASHBOARDBI].[BDG].[MOV_TB017_DESPESAS_MANUTENCAO]
            WHERE [NU_CONTRATO] = :contrato
        """)
        result_sisdex = db.session.execute(sql_sisdex, {'contrato': contrato}).fetchone()
        vr_sisdex = float(result_sisdex[0]) if result_sisdex and result_sisdex[0] else 0

        # ===== BUSCA 11: Débitos Pagos - SISGEA =====
        sql_sisgea = text("""
            SELECT SUM([VR_DESPESA]) as TOTAL_SISGEA
            FROM [BDDASHBOARDBI].[BDG].[MOV_TB004_DESPESAS_ANALITICO]
            WHERE [NR_CONTRATO] = :contrato
        """)
        result_sisgea = db.session.execute(sql_sisgea, {'contrato': contrato}).fetchone()
        vr_sisgea = float(result_sisgea[0]) if result_sisgea and result_sisgea[0] else 0

        vr_total_debitos = vr_sisdex + vr_sisgea

        # ===== RETORNAR TODOS OS DADOS =====
        return jsonify({
            'success': True,
            'dt_entrada_estoque': dt_entrada_estoque,
            'periodo_prescricao': periodo_prescricao,
            'dt_periodo_cobranca_inicio': dt_periodo_cobranca_inicio,
            'dt_periodo_cobranca_fim': dt_periodo_cobranca_fim,
            'periodo_cobranca_encontrado': periodo_cobranca_encontrado,
            'valor_inicial': valor_inicial,
            'qtd_parcelas_inicial': qtd_parcelas_inicial,
            'nome_condominio': nome_condominio,
            'valor_prescrito': valor_prescrito,
            'qtd_parcelas_prescrito': qtd_prescrito,
            'indices_disponiveis': indices_disponiveis,
            'vr_avaliacao': vr_avaliacao,
            'dt_laudo': dt_laudo,
            'status_imovel': status_imovel,
            'vr_venda': vr_venda,
            'dt_venda': dt_venda,
            'nome_comprador': nome_comprador,
            'tipo_pagamento_venda': tipo_pagamento_venda,
            'processos_judiciais': processos_judiciais,
            'vr_debitos_sisdex': vr_sisdex,
            'vr_debitos_sisgea': vr_sisgea,
            'vr_debitos_total': vr_total_debitos
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': f'Erro ao buscar dados: {str(e)}'
        }), 500


@sumov_bp.route('/deliberacao-pagamento/editar/<contrato>')
@login_required
def deliberacao_pagamento_editar(contrato):
    """Editar uma Deliberação de Pagamento existente"""
    try:
        deliberacao = DeliberacaoPagamento.buscar_por_contrato(contrato)

        if not deliberacao:
            flash('Deliberação não encontrada.', 'warning')
            return redirect(url_for('sumov.deliberacao_pagamento'))

        # ✅ NOVO: Buscar totais por tipo de parcela
        # Busca o índice diretamente do SISCalculo (não precisa estar salvo na deliberação)
        totais_por_tipo = []

        try:
            from app.models.siscalculo import SiscalculoCalculos, TipoParcela

            # Primeiro, buscar qual índice foi usado para este contrato
            indice_usado = db.session.query(
                SiscalculoCalculos.ID_INDICE_ECONOMICO
            ).filter(
                SiscalculoCalculos.IMOVEL == contrato,
                SiscalculoCalculos.DT_ATUALIZACAO == db.session.query(
                    db.func.max(SiscalculoCalculos.DT_ATUALIZACAO)
                ).filter(
                    SiscalculoCalculos.IMOVEL == contrato
                ).scalar_subquery()
            ).first()

            # Se encontrou o índice, buscar os totais por tipo
            if indice_usado:
                id_indice = indice_usado[0]

                totais_query = db.session.query(
                    SiscalculoCalculos.ID_TIPO,
                    TipoParcela.DSC_TIPO,
                    db.func.count(SiscalculoCalculos.DT_VENCIMENTO).label('quantidade'),
                    db.func.sum(SiscalculoCalculos.VR_TOTAL).label('valor_total')
                ).join(
                    TipoParcela,
                    SiscalculoCalculos.ID_TIPO == TipoParcela.ID_TIPO
                ).filter(
                    SiscalculoCalculos.IMOVEL == contrato,
                    SiscalculoCalculos.ID_INDICE_ECONOMICO == id_indice,
                    SiscalculoCalculos.DT_ATUALIZACAO == db.session.query(
                        db.func.max(SiscalculoCalculos.DT_ATUALIZACAO)
                    ).filter(
                        SiscalculoCalculos.IMOVEL == contrato
                    ).scalar_subquery()
                ).group_by(
                    SiscalculoCalculos.ID_TIPO,
                    TipoParcela.DSC_TIPO
                ).order_by(
                    SiscalculoCalculos.ID_TIPO
                ).all()

                totais_por_tipo = [
                    {
                        'id_tipo': t.ID_TIPO,
                        'descricao': t.DSC_TIPO,
                        'quantidade': t.quantidade,
                        'valor_total': float(t.valor_total) if t.valor_total else 0
                    }
                    for t in totais_query
                ]
        except Exception as e:
            # Se der erro ao buscar tipos de parcela, apenas não mostra (não quebra a página)
            print(f"Aviso: Não foi possível buscar tipos de parcela: {e}")
            totais_por_tipo = []

        return render_template(
            'sumov/deliberacao_pagamento/editar.html',
            deliberacao=deliberacao,
            totais_por_tipo=totais_por_tipo
        )

    except Exception as e:
        flash(f'Erro ao carregar deliberação: {str(e)}', 'danger')
        return redirect(url_for('sumov.deliberacao_pagamento'))


@sumov_bp.route('/deliberacao-pagamento/excluir/<contrato>', methods=['POST'])
@login_required
def deliberacao_pagamento_excluir(contrato):
    """Excluir (soft delete) uma Deliberação de Pagamento"""
    try:
        deliberacao = DeliberacaoPagamento.buscar_por_contrato(contrato)

        if not deliberacao:
            flash('Deliberação não encontrada.', 'warning')
            return redirect(url_for('sumov.deliberacao_pagamento'))

        # Soft delete
        deliberacao.DELETED_AT = datetime.utcnow()
        deliberacao.USUARIO_ATUALIZACAO = current_user.nome
        deliberacao.UPDATED_AT = datetime.utcnow()

        if deliberacao.salvar():
            registrar_log(
                acao='excluir',
                entidade='deliberacao_pagamento',
                entidade_id=contrato,
                descricao=f'Deliberação de Pagamento excluída: {contrato}'
            )
            flash('Deliberação excluída com sucesso!', 'success')
        else:
            flash('Erro ao excluir deliberação.', 'danger')

        return redirect(url_for('sumov.deliberacao_pagamento'))

    except Exception as e:
        flash(f'Erro ao excluir deliberação: {str(e)}', 'danger')
        return redirect(url_for('sumov.deliberacao_pagamento'))


@sumov_bp.route('/deliberacao-pagamento/pdf/<contrato>')
@login_required
def deliberacao_pagamento_pdf(contrato):
    """Gerar PDF da Deliberação de Pagamento"""
    try:
        from io import BytesIO
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import cm
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT, TA_RIGHT
        from reportlab.pdfgen import canvas
        from datetime import datetime
        from decimal import Decimal
        import html

        # Buscar deliberação
        deliberacao = DeliberacaoPagamento.buscar_por_contrato(contrato)

        if not deliberacao:
            flash('Deliberação não encontrada.', 'warning')
            return redirect(url_for('sumov.deliberacao_pagamento'))

        # Criar buffer
        buffer = BytesIO()

        # Criar documento
        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            rightMargin=2 * cm,
            leftMargin=2 * cm,
            topMargin=2.5 * cm,
            bottomMargin=2 * cm
        )

        # Estilos
        styles = getSampleStyleSheet()

        style_titulo = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=16,
            textColor=colors.HexColor('#1a5490'),
            spaceAfter=20,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        )

        style_subtitulo = ParagraphStyle(
            'CustomSubtitle',
            parent=styles['Heading2'],
            fontSize=12,
            textColor=colors.HexColor('#1a5490'),
            spaceAfter=12,
            spaceBefore=15,
            fontName='Helvetica-Bold'
        )

        style_normal = ParagraphStyle(
            'CustomNormal',
            parent=styles['Normal'],
            fontSize=10,
            alignment=TA_JUSTIFY,
            spaceAfter=8,
            fontName='Helvetica'
        )

        style_label = ParagraphStyle(
            'CustomLabel',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.HexColor('#666666'),
            spaceAfter=2,
            fontName='Helvetica-Bold'
        )

        style_valor = ParagraphStyle(
            'CustomValor',
            parent=styles['Normal'],
            fontSize=10,
            spaceAfter=10,
            fontName='Helvetica'
        )

        # Helper functions
        def escape_text(text):
            if not text:
                return '-'
            return html.escape(str(text))

        def formatar_data(data):
            if not data:
                return '-'
            if isinstance(data, str):
                return data
            return data.strftime('%d/%m/%Y')

        def formatar_moeda(valor):
            if not valor:
                return 'R$ 0,00'
            return 'R$ {:,.2f}'.format(float(valor)).replace(',', 'X').replace('.', ',').replace('X', '.')

        def formatar_percentual(valor):
            if not valor:
                return '0,00%'
            return '{:,.2f}%'.format(float(valor)).replace('.', ',')

        # Elementos do PDF
        elements = []

        # ===== CABEÇALHO =====
        elements.append(Paragraph('DELIBERAÇÃO DE PAGAMENTO', style_titulo))
        elements.append(Paragraph(f'Contrato/Imóvel: {escape_text(deliberacao.NU_CONTRATO)}', style_normal))
        elements.append(Spacer(1, 0.5 * cm))

        # ===== SEÇÃO 1: IDENTIFICAÇÃO =====
        elements.append(Paragraph('1. IDENTIFICAÇÃO', style_subtitulo))

        dados_identificacao = [
            ['Colaborador que Analisou:', escape_text(deliberacao.COLABORADOR_ANALISOU)],
            ['Data da Análise:', formatar_data(deliberacao.DT_ANALISE)],
            ['Matrícula (Caixa/Emgea):', escape_text(deliberacao.MATRICULA_CAIXA_EMGEA)],
            ['Data da Arrematação/Aquisição:', formatar_data(deliberacao.DT_ARREMATACAO_AQUISICAO)],
            ['Data de Entrada no Estoque:', formatar_data(deliberacao.DT_ENTRADA_ESTOQUE)]
        ]

        tabela_identificacao = Table(dados_identificacao, colWidths=[7 * cm, 10 * cm])
        tabela_identificacao.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#E8F4F8')),
            ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#1a5490')),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        elements.append(tabela_identificacao)
        elements.append(Spacer(1, 0.5 * cm))

        # ===== SEÇÃO 2: COBRANÇA E DÍVIDA =====
        elements.append(Paragraph('2. COBRANÇA E DÍVIDA', style_subtitulo))

        dados_cobranca = [
            ['Período Prescrito:', escape_text(deliberacao.PERIODO_PRESCRITO)],
        ]

        # NOVO: Adicionar período de cobrança
        if deliberacao.DT_PERIODO_COBRANCA_INICIO and deliberacao.DT_PERIODO_COBRANCA_FIM:
            periodo_cobranca_texto = f"{formatar_data(deliberacao.DT_PERIODO_COBRANCA_INICIO)} até {formatar_data(deliberacao.DT_PERIODO_COBRANCA_FIM)}"
            dados_cobranca.append(['Período da Cobrança dos Débitos:', periodo_cobranca_texto])

        dados_cobranca.extend([
            ['Valor Inicial da Dívida:', formatar_moeda(deliberacao.VR_DIVIDA_CONDOMINIO_1)],
        ])

        # NOVO: Adicionar valor manual se existir
        if deliberacao.VR_DIVIDA_CONDOMINIO_2:
            dados_cobranca.append(['Valor da Dívida (Manual):', formatar_moeda(deliberacao.VR_DIVIDA_CONDOMINIO_2)])

        dados_cobranca.extend([
            ['Valor Excluídos Cotas Prescritas:', formatar_moeda(deliberacao.VR_DEBITO_EXCLUIDO_PRESCRITAS)],
            ['Índice Econômico:', escape_text(deliberacao.INDICE_DEBITO_EMGEA)],
            ['Percentual de Honorários:', formatar_percentual(deliberacao.PERC_HONORARIOS_EMGEA)],
            ['Valor dos Honorários:', formatar_moeda(deliberacao.VR_HONORARIOS_EMGEA)],
            ['VALOR TOTAL (com Honorários):', formatar_moeda(deliberacao.VR_DEBITO_CALCULADO_EMGEA)]
        ])

        tabela_cobranca = Table(dados_cobranca, colWidths=[7 * cm, 10 * cm])
        tabela_cobranca.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#E8F4F8')),
            ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#1a5490')),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#D4EDDA')),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, -1), (-1, -1), 10),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        elements.append(tabela_cobranca)
        elements.append(Spacer(1, 0.5 * cm))

        # ✅ NOVO: Adicionar tabela de tipos de parcela (se houver dados)
        # Busca o índice diretamente do SISCalculo
        try:
            from app.models.siscalculo import SiscalculoCalculos, TipoParcela

            # Primeiro, buscar qual índice foi usado para este contrato
            indice_usado = db.session.query(
                SiscalculoCalculos.ID_INDICE_ECONOMICO
            ).filter(
                SiscalculoCalculos.IMOVEL == contrato,
                SiscalculoCalculos.DT_ATUALIZACAO == db.session.query(
                    db.func.max(SiscalculoCalculos.DT_ATUALIZACAO)
                ).filter(
                    SiscalculoCalculos.IMOVEL == contrato
                ).scalar_subquery()
            ).first()

            # Se encontrou o índice, buscar os totais por tipo
            if indice_usado:
                id_indice = indice_usado[0]

                totais_query = db.session.query(
                    SiscalculoCalculos.ID_TIPO,
                    TipoParcela.DSC_TIPO,
                    db.func.count(SiscalculoCalculos.DT_VENCIMENTO).label('quantidade'),
                    db.func.sum(SiscalculoCalculos.VR_TOTAL).label('valor_total')
                ).join(
                    TipoParcela,
                    SiscalculoCalculos.ID_TIPO == TipoParcela.ID_TIPO
                ).filter(
                    SiscalculoCalculos.IMOVEL == contrato,
                    SiscalculoCalculos.ID_INDICE_ECONOMICO == id_indice,
                    SiscalculoCalculos.DT_ATUALIZACAO == db.session.query(
                        db.func.max(SiscalculoCalculos.DT_ATUALIZACAO)
                    ).filter(
                        SiscalculoCalculos.IMOVEL == contrato
                    ).scalar_subquery()
                ).group_by(
                    SiscalculoCalculos.ID_TIPO,
                    TipoParcela.DSC_TIPO
                ).order_by(
                    SiscalculoCalculos.ID_TIPO
                ).all()

                if totais_query:
                    # Adicionar subtítulo
                    elements.append(Spacer(1, 0.3 * cm))
                    elements.append(Paragraph('2.1 RESUMO POR TIPO DE PARCELA', style_subtitulo))

                    # Criar dados da tabela
                    dados_tipos = [['ID', 'Descrição do Tipo', 'Quantidade', 'Valor Total']]

                    total_qtd = 0
                    total_valor = Decimal('0')

                    for t in totais_query:
                        valor = Decimal(str(t.valor_total)) if t.valor_total else Decimal('0')
                        total_qtd += t.quantidade
                        total_valor += valor

                        dados_tipos.append([
                            str(t.ID_TIPO),
                            escape_text(t.DSC_TIPO),
                            str(t.quantidade),
                            formatar_moeda(valor)
                        ])

                    # Adicionar linha de totais
                    dados_tipos.append([
                        '',
                        'TOTAL GERAL:',
                        str(total_qtd),
                        formatar_moeda(total_valor)
                    ])

                    # Criar tabela
                    tabela_tipos = Table(dados_tipos, colWidths=[2 * cm, 8 * cm, 3 * cm, 4 * cm])
                    tabela_tipos.setStyle(TableStyle([
                        # Cabeçalho
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#28a745')),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, 0), 10),

                        # Corpo da tabela
                        ('BACKGROUND', (0, 1), (-1, -2), colors.white),
                        ('TEXTCOLOR', (0, 1), (-1, -2), colors.black),
                        ('ALIGN', (0, 1), (0, -2), 'CENTER'),  # ID centralizado
                        ('ALIGN', (1, 1), (1, -2), 'LEFT'),  # Descrição à esquerda
                        ('ALIGN', (2, 1), (2, -2), 'CENTER'),  # Quantidade centralizada
                        ('ALIGN', (3, 1), (3, -2), 'RIGHT'),  # Valor à direita
                        ('FONTNAME', (0, 1), (-1, -2), 'Helvetica'),
                        ('FONTSIZE', (0, 1), (-1, -2), 9),

                        # Linha de totais (última linha)
                        ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#D4EDDA')),
                        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, -1), (-1, -1), 10),
                        ('ALIGN', (1, -1), (1, -1), 'RIGHT'),
                        ('ALIGN', (2, -1), (2, -1), 'CENTER'),
                        ('ALIGN', (3, -1), (3, -1), 'RIGHT'),

                        # Bordas e padding
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                        ('LEFTPADDING', (0, 0), (-1, -1), 6),
                        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
                        ('TOPPADDING', (0, 0), (-1, -1), 6),
                        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                    ]))

                    elements.append(tabela_tipos)
                    elements.append(Spacer(1, 0.5 * cm))
        except Exception as e:
            # Se der erro ao buscar tipos de parcela, apenas não adiciona no PDF (não quebra)
            print(f"Aviso: Não foi possível adicionar tipos de parcela ao PDF: {e}")

        # ===== SEÇÃO 3: AVALIAÇÃO E VENDA =====
        elements.append(Paragraph('3. AVALIAÇÃO E VENDA', style_subtitulo))

        dados_avaliacao = [
            ['Valor de Avaliação:', formatar_moeda(deliberacao.VR_AVALIACAO)],
            ['Data do Laudo:', formatar_data(deliberacao.DT_LAUDO)],
            ['Status do Imóvel:', escape_text(deliberacao.STATUS_IMOVEL)],
        ]

        if deliberacao.STATUS_IMOVEL and 'VENDIDO' in deliberacao.STATUS_IMOVEL.upper():
            dados_avaliacao.extend([
                ['Valor de Venda:', formatar_moeda(deliberacao.VR_VENDA)],
                ['Data da Venda:', formatar_data(deliberacao.DT_VENDA)],
                ['Nome do Adquirente:', escape_text(deliberacao.NOME_COMPRADOR)],
            ])

            tipo_pagamento_texto = '-'
            if deliberacao.TIPO_PAGAMENTO_VENDA:
                if deliberacao.TIPO_PAGAMENTO_VENDA == 'A_VISTA':
                    tipo_pagamento_texto = 'À Vista'
                elif deliberacao.TIPO_PAGAMENTO_VENDA == 'PARCELADO':
                    tipo_pagamento_texto = 'Parcelado'

            dados_avaliacao.append(['Tipo de Pagamento:', tipo_pagamento_texto])

        dados_avaliacao.append(['Data do Registro:', formatar_data(deliberacao.DT_REGISTRO)])

        tabela_avaliacao = Table(dados_avaliacao, colWidths=[7 * cm, 10 * cm])
        tabela_avaliacao.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#E8F4F8')),
            ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#1a5490')),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        elements.append(tabela_avaliacao)
        elements.append(Spacer(1, 0.5 * cm))

        # ===== SEÇÃO 4: AÇÕES E PROCESSOS JUDICIAIS =====
        elements.append(Paragraph('4. AÇÕES E PROCESSOS JUDICIAIS', style_subtitulo))

        if deliberacao.GRAVAME_MATRICULA:
            elements.append(Paragraph('<b>Gravame na Matrícula:</b>', style_label))
            elements.append(Paragraph(escape_text(deliberacao.GRAVAME_MATRICULA), style_valor))

        if deliberacao.ACOES_NEGOCIAIS_ADMINISTRATIVAS:
            elements.append(Paragraph('<b>Ações Negociais Administrativas:</b>', style_label))
            elements.append(Paragraph(escape_text(deliberacao.ACOES_NEGOCIAIS_ADMINISTRATIVAS), style_valor))

        if deliberacao.NR_PROCESSOS_JUDICIAIS:
            elements.append(Paragraph('<b>Nº da Ação Judicial:</b>', style_label))
            elements.append(Paragraph(escape_text(deliberacao.NR_PROCESSOS_JUDICIAIS), style_valor))

        if deliberacao.VARA_PROCESSO:
            elements.append(Paragraph('<b>Vara do Processo:</b>', style_label))
            elements.append(Paragraph(escape_text(deliberacao.VARA_PROCESSO), style_valor))

        if deliberacao.FASE_PROCESSO:
            elements.append(Paragraph('<b>Fase do Processo:</b>', style_label))
            elements.append(Paragraph(escape_text(deliberacao.FASE_PROCESSO), style_valor))

        if deliberacao.RELATORIO_ASSESSORIA_JURIDICA:
            elements.append(Paragraph('<b>Relatório da Assessoria Jurídica:</b>', style_label))
            elements.append(Paragraph(escape_text(deliberacao.RELATORIO_ASSESSORIA_JURIDICA), style_valor))

        elements.append(Spacer(1, 0.3 * cm))

        # ===== SEÇÃO 5: DÉBITOS E PENALIDADES =====
        elements.append(Paragraph('5. DÉBITOS E PENALIDADES', style_subtitulo))

        dados_debitos = [
            ['Débitos Sisdex:', formatar_moeda(deliberacao.VR_DEBITOS_SISDEX)],
            ['Débitos Sisgea:', formatar_moeda(deliberacao.VR_DEBITOS_SISGEA)],
            ['TOTAL de Débitos:', formatar_moeda(deliberacao.VR_DEBITOS_TOTAL)]
        ]

        tabela_debitos = Table(dados_debitos, colWidths=[7 * cm, 10 * cm])
        tabela_debitos.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#E8F4F8')),
            ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#1a5490')),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#FFF3CD')),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        elements.append(tabela_debitos)
        elements.append(Spacer(1, 0.3 * cm))

        if deliberacao.PENALIDADE_ANS_CAIXA:
            elements.append(Paragraph('<b>Penalidade de ANS - CAIXA:</b>', style_label))
            elements.append(Paragraph(escape_text(deliberacao.PENALIDADE_ANS_CAIXA), style_valor))

        if deliberacao.PREJUIZO_FINANCEIRO_CAIXA:
            elements.append(Paragraph('<b>Prejuízo Financeiro - CAIXA:</b>', style_label))
            elements.append(Paragraph(escape_text(deliberacao.PREJUIZO_FINANCEIRO_CAIXA), style_valor))

        elements.append(Spacer(1, 0.3 * cm))

        # ===== SEÇÃO 6: CONSIDERAÇÕES FINAIS =====
        elements.append(Paragraph('6. CONSIDERAÇÕES FINAIS', style_subtitulo))

        if deliberacao.CONSIDERACOES_ANALISTA_GEADI:
            elements.append(Paragraph('<b>Considerações da Analista GEADI:</b>', style_label))
            elements.append(Paragraph(escape_text(deliberacao.CONSIDERACOES_ANALISTA_GEADI), style_valor))
            elements.append(Spacer(1, 0.3 * cm))

        if deliberacao.CONSIDERACOES_GESTOR_GEADI:
            elements.append(Paragraph('<b>Considerações Finais do Gestor da GEADI:</b>', style_label))
            elements.append(Paragraph(escape_text(deliberacao.CONSIDERACOES_GESTOR_GEADI), style_valor))
            elements.append(Spacer(1, 0.3 * cm))

        if deliberacao.CONSIDERACOES_GESTOR_SUMOV:
            elements.append(Paragraph('<b>Considerações Finais do Gestor da SUMOV:</b>', style_label))
            elements.append(Paragraph(escape_text(deliberacao.CONSIDERACOES_GESTOR_SUMOV), style_valor))
            elements.append(Spacer(1, 0.3 * cm))

        # ===== RODAPÉ =====
        elements.append(Spacer(1, 1 * cm))
        elements.append(Paragraph(
            f'<i>Documento gerado em {datetime.now().strftime("%d/%m/%Y às %H:%M")}</i>',
            ParagraphStyle('Rodape', parent=styles['Normal'], fontSize=8, textColor=colors.grey, alignment=TA_CENTER)
        ))

        # Construir PDF
        doc.build(elements)

        # Retornar PDF
        buffer.seek(0)
        return send_file(
            buffer,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=f'Deliberacao_Pagamento_{contrato}.pdf'
        )

    except Exception as e:
        import traceback
        traceback.print_exc()
        flash(f'Erro ao gerar PDF: {str(e)}', 'danger')
        return redirect(url_for('sumov.deliberacao_pagamento'))


# ==================== DIFERENÇA DESPESAS X SISCOR ====================

from app.models.diferenca_despesas_siscor import DiferencaDespesasSiscor


@sumov_bp.route('/despesas-pagamentos/diferenca-siscor')
@login_required
def diferenca_despesas_siscor():
    """Página de análise de diferenças entre Despesas e SISCOR"""

    # Buscar datas já formatadas em uma única operação
    datas_formatadas = DiferencaDespesasSiscor.listar_datas_disponiveis_formatadas()

    return render_template('sumov/despesas_pagamentos/diferenca_siscor.html',
                           datas_disponiveis=datas_formatadas)


@sumov_bp.route('/despesas-pagamentos/diferenca-siscor/buscar', methods=['POST'])
@login_required
def buscar_diferenca_siscor():
    """API para buscar diferenças com filtros"""
    try:
        data = request.get_json()

        # Obter filtros do request
        datas_selecionadas = data.get('datas', [])
        id_item = data.get('id_item', None)

        # Converter id_item para int se fornecido
        if id_item:
            try:
                id_item = int(id_item)
            except (ValueError, TypeError):
                id_item = None

        # Buscar registros com filtros
        registros = DiferencaDespesasSiscor.buscar_por_filtros(
            datas=datas_selecionadas if len(datas_selecionadas) > 0 else None,
            id_item=id_item
        )

        # Calcular totais
        totais = DiferencaDespesasSiscor.calcular_totais(registros)

        # Formatar registros para JSON
        registros_formatados = []
        for r in registros:
            registros_formatados.append({
                'dt_despesa': r.DT_DESPESA,
                'dt_despesa_formatada': DiferencaDespesasSiscor.formatar_data_mesano(r.DT_DESPESA),
                'id_item': r.ID_ITEM,
                'vr_siscor': float(r.VR_SISCOR) if r.VR_SISCOR else 0,
                'vr_despesa': float(r.VR_DESPESA) if r.VR_DESPESA else 0,
                'dif': float(r.DIF) if r.DIF else 0
            })

        # Retornar dados
        return jsonify({
            'success': True,
            'registros': registros_formatados,
            'totais': totais,
            'quantidade': len(registros_formatados)
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'erro': str(e)
        }), 500