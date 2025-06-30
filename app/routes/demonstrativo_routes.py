from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify
from app.models.demonstrativo import EstruturaDemonstrativo, ContaDemonstrativo
from app import db
from flask_login import login_required
from app.utils.audit import registrar_log
from datetime import datetime

demonstrativo_bp = Blueprint('demonstrativo', __name__, url_prefix='/codigos-contabeis/demonstrativos')


@demonstrativo_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@demonstrativo_bp.route('/')
@login_required
def index():
    """Dashboard principal dos demonstrativos"""
    total_vinculacoes = ContaDemonstrativo.query.count()
    total_estruturas = EstruturaDemonstrativo.query.count()

    # Últimas vinculações modificadas
    ultimas_vinculacoes = ContaDemonstrativo.query.limit(5).all()

    return render_template('codigos_contabeis/demonstrativos/index.html',
                           total_vinculacoes=total_vinculacoes,
                           total_estruturas=total_estruturas,
                           ultimas_vinculacoes=ultimas_vinculacoes)


@demonstrativo_bp.route('/lista')
@login_required
def lista_demonstrativos():
    """Lista todas as vinculações de demonstrativos"""
    demonstrativos = ContaDemonstrativo.query.all()

    # Buscar estruturas para mostrar os nomes
    estruturas = {e.ORDEM: e.GRUPO for e in EstruturaDemonstrativo.query.all()}

    return render_template('codigos_contabeis/demonstrativos/lista_demonstrativos.html',
                           demonstrativos=demonstrativos,
                           estruturas=estruturas)


@demonstrativo_bp.route('/novo', methods=['GET', 'POST'])
@login_required
def novo_demonstrativo():
    """Criar ou atualizar vinculação de demonstrativo"""
    if request.method == 'POST':
        try:
            co_conta = request.form.get('co_conta', '').strip()

            if not co_conta:
                flash('Conta é obrigatória!', 'danger')
                return redirect(url_for('demonstrativo.novo_demonstrativo'))

            # Verificar se já existe para decidir se é criação ou atualização
            conta_existente = ContaDemonstrativo.query.filter_by(CO_CONTA=co_conta).first()

            # Capturar dados antigos se existir
            dados_antigos = None
            if conta_existente:
                dados_antigos = {
                    'co_conta': conta_existente.CO_CONTA,
                    'CO_BP_Gerencial': conta_existente.CO_BP_Gerencial,
                    'CO_BP_Resumida': conta_existente.CO_BP_Resumida,
                    'CO_DRE_Gerencial': conta_existente.CO_DRE_Gerencial,
                    'CO_DRE_Resumida': conta_existente.CO_DRE_Resumida,
                    'CO_DVA_Gerencial': conta_existente.CO_DVA_Gerencial
                }

            # Processar dados do formulário
            campos = ['CO_BP_Gerencial', 'CO_BP_Resumida', 'CO_DRE_Gerencial',
                      'CO_DRE_Resumida', 'CO_DVA_Gerencial']

            dados_novos = {}
            for campo in campos:
                valor = request.form.get(campo.lower())
                if valor and valor != 'nenhum':
                    dados_novos[campo] = int(valor)
                else:
                    dados_novos[campo] = None

            # Criar ou atualizar
            conta, is_nova = ContaDemonstrativo.criar_ou_atualizar(co_conta, dados_novos)

            db.session.commit()

            # Registrar log apropriado
            if is_nova:
                registrar_log(
                    acao='criar',
                    entidade='conta_demonstrativo',
                    entidade_id=co_conta,
                    descricao=f'Criação de vinculação demonstrativo para conta {co_conta}',
                    dados_novos={'co_conta': co_conta, **dados_novos}
                )
                flash('Vinculação criada com sucesso!', 'success')
            else:
                registrar_log(
                    acao='editar',
                    entidade='conta_demonstrativo',
                    entidade_id=co_conta,
                    descricao=f'Atualização de vinculação demonstrativo para conta {co_conta}',
                    dados_antigos=dados_antigos,
                    dados_novos={'co_conta': co_conta, **dados_novos}
                )
                flash('Vinculação atualizada com sucesso!', 'success')

            return redirect(url_for('demonstrativo.lista_demonstrativos'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao processar vinculação: {str(e)}', 'danger')

    # GET - Buscar estruturas disponíveis
    estruturas = EstruturaDemonstrativo.query.order_by(EstruturaDemonstrativo.ORDEM).all()

    return render_template('codigos_contabeis/demonstrativos/form_demonstrativo.html',
                           estruturas=estruturas)


@demonstrativo_bp.route('/editar/<co_conta>', methods=['GET', 'POST'])
@login_required
def editar_demonstrativo(co_conta):
    """Editar vinculação existente"""
    demonstrativo = ContaDemonstrativo.query.filter_by(CO_CONTA=co_conta).first_or_404()

    if request.method == 'POST':
        try:
            # Capturar dados antigos
            dados_antigos = {
                'co_conta': demonstrativo.CO_CONTA,
                'CO_BP_Gerencial': demonstrativo.CO_BP_Gerencial,
                'CO_BP_Resumida': demonstrativo.CO_BP_Resumida,
                'CO_DRE_Gerencial': demonstrativo.CO_DRE_Gerencial,
                'CO_DRE_Resumida': demonstrativo.CO_DRE_Resumida,
                'CO_DVA_Gerencial': demonstrativo.CO_DVA_Gerencial
            }

            # Atualizar campos
            campos = ['CO_BP_Gerencial', 'CO_BP_Resumida', 'CO_DRE_Gerencial',
                      'CO_DRE_Resumida', 'CO_DVA_Gerencial']

            dados_novos = {'co_conta': co_conta}

            for campo in campos:
                valor = request.form.get(campo.lower())
                if valor and valor != 'nenhum':
                    setattr(demonstrativo, campo, int(valor))
                    dados_novos[campo] = int(valor)
                else:
                    setattr(demonstrativo, campo, None)
                    dados_novos[campo] = None

            db.session.commit()

            # Registrar log
            registrar_log(
                acao='editar',
                entidade='conta_demonstrativo',
                entidade_id=co_conta,
                descricao=f'Edição de vinculação demonstrativo para conta {co_conta}',
                dados_antigos=dados_antigos,
                dados_novos=dados_novos
            )

            flash('Vinculação atualizada com sucesso!', 'success')
            return redirect(url_for('demonstrativo.lista_demonstrativos'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao atualizar: {str(e)}', 'danger')

    # GET
    estruturas = EstruturaDemonstrativo.query.order_by(EstruturaDemonstrativo.ORDEM).all()

    return render_template('codigos_contabeis/demonstrativos/form_demonstrativo.html',
                           demonstrativo=demonstrativo,
                           estruturas=estruturas)


@demonstrativo_bp.route('/excluir/<co_conta>', methods=['POST'])
@login_required
def excluir_demonstrativo(co_conta):
    """Excluir vinculações (limpar campos, mas manter a conta)"""
    try:
        demonstrativo = ContaDemonstrativo.query.filter_by(CO_CONTA=co_conta).first_or_404()

        # Capturar dados antigos para auditoria
        dados_antigos = {
            'co_conta': demonstrativo.CO_CONTA,
            'CO_BP_Gerencial': demonstrativo.CO_BP_Gerencial,
            'CO_BP_Resumida': demonstrativo.CO_BP_Resumida,
            'CO_DRE_Gerencial': demonstrativo.CO_DRE_Gerencial,
            'CO_DRE_Resumida': demonstrativo.CO_DRE_Resumida,
            'CO_DVA_Gerencial': demonstrativo.CO_DVA_Gerencial
        }

        # Limpar as vinculações (definir como NULL)
        demonstrativo.CO_BP_Gerencial = None
        demonstrativo.CO_BP_Resumida = None
        demonstrativo.CO_DRE_Gerencial = None
        demonstrativo.CO_DRE_Resumida = None
        demonstrativo.CO_DVA_Gerencial = None

        db.session.commit()

        # Dados novos para auditoria (todos NULL)
        dados_novos = {
            'co_conta': demonstrativo.CO_CONTA,
            'CO_BP_Gerencial': None,
            'CO_BP_Resumida': None,
            'CO_DRE_Gerencial': None,
            'CO_DRE_Resumida': None,
            'CO_DVA_Gerencial': None
        }

        # Registrar log
        registrar_log(
            acao='limpar_vinculacoes',
            entidade='conta_demonstrativo',
            entidade_id=co_conta,
            descricao=f'Remoção de todas as vinculações da conta {co_conta}',
            dados_antigos=dados_antigos,
            dados_novos=dados_novos
        )

        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({
                'success': True,
                'message': f'Vinculações da conta {co_conta} removidas com sucesso!'
            })

        flash('Vinculações removidas com sucesso!', 'success')
        return redirect(url_for('demonstrativo.lista_demonstrativos'))

    except Exception as e:
        db.session.rollback()

        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({
                'success': False,
                'message': f'Erro ao remover vinculações: {str(e)}'
            })

        flash(f'Erro ao remover vinculações: {str(e)}', 'danger')
        return redirect(url_for('demonstrativo.lista_demonstrativos'))

@demonstrativo_bp.route('/api/buscar-contas', methods=['GET'])
@login_required
def buscar_contas():
    """API para buscar contas com autocomplete"""
    try:
        termo = request.args.get('q', '').strip()

        # Aqui você pode buscar contas de múltiplas fontes
        # Por exemplo, de uma tabela de contas gerais, além das já cadastradas

        # Por enquanto, vamos buscar das contas já cadastradas
        query = db.session.query(ContaDemonstrativo.CO_CONTA).distinct()

        if termo:
            query = query.filter(ContaDemonstrativo.CO_CONTA.like(f'%{termo}%'))

        contas = query.limit(20).all()

        # Se precisar, adicione aqui busca de outras tabelas de contas
        # Por exemplo:
        # outras_contas = db.session.query(OutraTabela.conta).filter(...)
        # contas_total = set([c[0] for c in contas] + [c[0] for c in outras_contas])

        return jsonify({
            'results': [{'id': c[0], 'text': c[0]} for c in contas]
        })

    except Exception as e:
        return jsonify({
            'error': str(e),
            'results': []
        })


@demonstrativo_bp.route('/api/verificar-conta/<co_conta>', methods=['GET'])
@login_required
def verificar_conta(co_conta):
    """Verifica se a conta já existe e retorna seus dados"""
    try:
        conta = ContaDemonstrativo.query.filter_by(CO_CONTA=co_conta).first()

        if conta:
            return jsonify({
                'existe': True,
                'dados': {
                    'CO_BP_Gerencial': conta.CO_BP_Gerencial,
                    'CO_BP_Resumida': conta.CO_BP_Resumida,
                    'CO_DRE_Gerencial': conta.CO_DRE_Gerencial,
                    'CO_DRE_Resumida': conta.CO_DRE_Resumida,
                    'CO_DVA_Gerencial': conta.CO_DVA_Gerencial
                }
            })
        else:
            return jsonify({'existe': False})

    except Exception as e:
        return jsonify({'erro': str(e)})