from flask import Blueprint, render_template, request, jsonify, send_file, flash, redirect, url_for
from flask_login import login_required, current_user
from app import db
from app.models.relatorio import RelatorioTemplate, RelatorioGerado
from app.utils.relatorio_builder import RelatorioBuilder
from app.utils.audit import registrar_log
import json
from datetime import datetime

relatorio_bp = Blueprint('relatorio', __name__, url_prefix='/relatorios')


@relatorio_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@relatorio_bp.route('/')
@login_required
def index():
    """Lista de relatórios disponíveis"""
    # Templates do usuário
    meus_templates = RelatorioTemplate.query.filter_by(
        USUARIO_ID=current_user.id,
        DELETED_AT=None
    ).order_by(RelatorioTemplate.CREATED_AT.desc()).all()

    # Templates públicos
    templates_publicos = RelatorioTemplate.query.filter(
        RelatorioTemplate.PUBLICO == True,
        RelatorioTemplate.DELETED_AT == None,
        RelatorioTemplate.USUARIO_ID != current_user.id
    ).order_by(RelatorioTemplate.CREATED_AT.desc()).all()

    # Histórico de relatórios gerados
    historico = db.session.query(RelatorioGerado, RelatorioTemplate).join(
        RelatorioTemplate
    ).filter(
        RelatorioGerado.USUARIO_ID == current_user.id
    ).order_by(
        RelatorioGerado.CREATED_AT.desc()
    ).limit(10).all()

    return render_template('relatorios/index.html',
                           meus_templates=meus_templates,
                           templates_publicos=templates_publicos,
                           historico=historico)


@relatorio_bp.route('/novo')
@login_required
def novo():
    """Interface para criar novo relatório"""
    return render_template('relatorios/form_relatorio.html')


@relatorio_bp.route('/editar/<int:id>')
@login_required
def editar(id):
    """Editar template existente"""
    template = RelatorioTemplate.query.get_or_404(id)

    # Verificar permissão
    if template.USUARIO_ID != current_user.id:
        flash('Você não tem permissão para editar este template.', 'danger')
        return redirect(url_for('relatorio.index'))

    return render_template('relatorios/form_relatorio.html',
                           template=template,
                           config=json.dumps(template.CONFIGURACAO))


@relatorio_bp.route('/api/fontes-dados')
@login_required
def get_fontes_dados():
    """Retorna as fontes de dados disponíveis"""
    fontes = {
        'empresas': {
            'nome': 'Empresas',
            'tabela': 'DCA_TB002_EMPRESAS_PARTICIPANTES',
            'campos': [
                {'id': 'ID_EMPRESA', 'nome': 'Código', 'tipo': 'numero'},
                {'id': 'NOME', 'nome': 'Nome', 'tipo': 'texto'},
                {'id': 'DS_CONDICAO', 'nome': 'Condição', 'tipo': 'texto'},
                {'id': 'DT_INICIO', 'nome': 'Data Início', 'tipo': 'data'},
                {'id': 'DT_FIM', 'nome': 'Data Fim', 'tipo': 'data'}
            ]
        },
        'distribuicao': {
            'nome': 'Distribuição',
            'tabela': 'DCA_TB005_DISTRIBUICAO',
            'campos': [
                {'id': 'fkContratoSISCTR', 'nome': 'Contrato', 'tipo': 'numero'},
                {'id': 'COD_EMPRESA_COBRANCA', 'nome': 'Empresa', 'tipo': 'numero'},
                {'id': 'NR_CPF_CNPJ', 'nome': 'CPF/CNPJ', 'tipo': 'texto'},
                {'id': 'VR_SD_DEVEDOR', 'nome': 'Saldo Devedor', 'tipo': 'moeda'}
            ]
        },
        'metas': {
            'nome': 'Metas',
            'tabela': 'DCA_TB012_METAS_AVALIACAO',
            'campos': [
                {'id': 'MES_COMPETENCIA', 'nome': 'Competência', 'tipo': 'data'},
                {'id': 'VLR_META', 'nome': 'Valor Meta', 'tipo': 'moeda'},
                {'id': 'VLR_DISTRIBUIDO', 'nome': 'Valor Distribuído', 'tipo': 'moeda'},
                {'id': 'PERCENTUAL_META', 'nome': 'Percentual Meta', 'tipo': 'percentual'}
            ]
        },
        'contratos_distribuiveis': {
            'nome': 'Contratos Distribuíveis',
            'tabela': 'DCA_TB006_DISTRIBUIVEIS',
            'campos': [
                {'id': 'fkContratoSISCTR', 'nome': 'Contrato', 'tipo': 'numero'},
                {'id': 'NR_CPF_CNPJ', 'nome': 'CPF/CNPJ', 'tipo': 'texto'},
                {'id': 'VR_SD_DEVEDOR', 'nome': 'Saldo Devedor', 'tipo': 'moeda'}
            ]
        }
    }
    return jsonify(fontes)


@relatorio_bp.route('/api/salvar-template', methods=['POST'])
@login_required
def salvar_template():
    """Salva configuração do template"""
    try:
        data = request.json
        template_id = data.get('id')

        if template_id:
            # Editar existente
            template = RelatorioTemplate.query.get_or_404(template_id)
            if template.USUARIO_ID != current_user.id:
                return jsonify({'erro': 'Sem permissão'}), 403

            template.NOME = data['nome']
            template.DESCRICAO = data.get('descricao', '')
            template.CONFIGURACAO = data['configuracao']
            template.PUBLICO = data.get('publico', False)
            template.UPDATED_AT = datetime.utcnow()
        else:
            # Criar novo
            template = RelatorioTemplate(
                NOME=data['nome'],
                DESCRICAO=data.get('descricao', ''),
                USUARIO_ID=current_user.id,
                CONFIGURACAO=data['configuracao'],
                PUBLICO=data.get('publico', False)
            )
            db.session.add(template)

        db.session.commit()

        registrar_log(
            acao='criar' if not template_id else 'editar',
            entidade='relatorio_template',
            entidade_id=template.ID,
            descricao=f'Template de relatório: {template.NOME}'
        )

        return jsonify({
            'sucesso': True,
            'id': template.ID,
            'mensagem': 'Template salvo com sucesso!'
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'erro': str(e)}), 500


@relatorio_bp.route('/api/preview', methods=['POST'])
@login_required
def preview_relatorio():
    """Gera preview do relatório"""
    try:
        config = request.json
        builder = RelatorioBuilder()

        # Gerar dados do relatório
        dados = builder.processar_configuracao(config)

        return jsonify({
            'sucesso': True,
            'dados': dados,
            'total_registros': len(dados.get('registros', []))
        })

    except Exception as e:
        return jsonify({'erro': str(e)}), 500


@relatorio_bp.route('/gerar/<int:template_id>', methods=['POST'])
@login_required
def gerar_relatorio(template_id):
    """Gera relatório baseado no template"""
    try:
        template = RelatorioTemplate.query.get_or_404(template_id)

        # Verificar permissão
        if not template.PUBLICO and template.USUARIO_ID != current_user.id:
            flash('Sem permissão para usar este template.', 'danger')
            return redirect(url_for('relatorio.index'))

        # Parâmetros do formulário
        formato = request.form.get('formato', 'pdf')
        filtros = json.loads(request.form.get('filtros', '{}'))

        # Gerar relatório
        builder = RelatorioBuilder()
        arquivo = builder.gerar(
            configuracao=template.CONFIGURACAO,
            formato=formato,
            filtros=filtros,
            titulo=template.NOME
        )

        # Salvar histórico
        historico = RelatorioGerado(
            TEMPLATE_ID=template_id,
            USUARIO_ID=current_user.id,
            NOME_ARQUIVO=arquivo['nome'],
            FORMATO=formato,
            PARAMETROS=filtros
        )
        db.session.add(historico)
        db.session.commit()

        # Registrar log
        registrar_log(
            acao='gerar',
            entidade='relatorio',
            entidade_id=historico.ID,
            descricao=f'Relatório gerado: {template.NOME}'
        )

        return send_file(
            arquivo['caminho'],
            as_attachment=True,
            download_name=arquivo['nome'],
            mimetype=arquivo['mimetype']
        )

    except Exception as e:
        flash(f'Erro ao gerar relatório: {str(e)}', 'danger')
        return redirect(url_for('relatorio.index'))


@relatorio_bp.route('/excluir/<int:id>', methods=['POST'])
@login_required
def excluir_template(id):
    """Excluir template (soft delete)"""
    template = RelatorioTemplate.query.get_or_404(id)

    if template.USUARIO_ID != current_user.id:
        flash('Sem permissão para excluir este template.', 'danger')
        return redirect(url_for('relatorio.index'))

    template.DELETED_AT = datetime.utcnow()
    db.session.commit()

    registrar_log(
        acao='excluir',
        entidade='relatorio_template',
        entidade_id=template.ID,
        descricao=f'Template excluído: {template.NOME}'
    )

    flash('Template excluído com sucesso!', 'success')
    return redirect(url_for('relatorio.index'))