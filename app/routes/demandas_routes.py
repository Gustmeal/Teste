from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_file, abort
from flask_login import login_required, current_user
from app import db
from app.models.demanda import Demanda, DemandaHistorico, DemandaAnexo
from app.models.usuario import Usuario, Empregado
from app.utils.audit import registrar_log
from datetime import datetime, timedelta
from sqlalchemy import or_, and_, func
from werkzeug.utils import secure_filename
import os
import uuid

demandas_bp = Blueprint('demandas', __name__, url_prefix='/demandas')

# Configurações de upload
UPLOAD_FOLDER = 'app/static/uploads/demandas'
ALLOWED_EXTENSIONS = {'pdf', 'doc', 'docx', 'xls', 'xlsx', 'png', 'jpg', 'jpeg', 'zip', 'rar'}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def criar_pasta_upload():
    """Cria a pasta de upload se não existir"""
    if not os.path.exists(UPLOAD_FOLDER):
        os.makedirs(UPLOAD_FOLDER)


def verificar_permissao_criacao():
    """Verifica se o usuário pode criar demandas"""
    return current_user.perfil in ['admin', 'moderador']  # Corrigido para minúsculo


@demandas_bp.route('/')
@login_required
def index():
    """Página principal das demandas"""
    # Verificar permissões
    pode_criar = verificar_permissao_criacao()

    # Estatísticas baseadas no perfil do usuário
    if current_user.perfil in ['admin', 'moderador']:  # Corrigido para minúsculo
        # Admin e moderador veem todas
        minhas_demandas = Demanda.query.filter(
            Demanda.DELETED_AT.is_(None),
            Demanda.STATUS != 'CONCLUIDA'
        ).count()

        demandas_atrasadas = Demanda.query.filter(
            Demanda.DELETED_AT.is_(None),
            Demanda.STATUS.notin_(['CONCLUIDA', 'CANCELADA']),
            Demanda.DT_PRAZO < datetime.now()
        ).count()

        demandas_urgentes = Demanda.query.filter(
            Demanda.DELETED_AT.is_(None),
            Demanda.STATUS != 'CONCLUIDA',
            Demanda.PRIORIDADE.in_(['ALTA', 'URGENTE'])
        ).count()

        # Demandas recentes - todas
        demandas_recentes = Demanda.query.filter(
            Demanda.DELETED_AT.is_(None)
        ).order_by(Demanda.DT_CRIACAO.desc()).limit(5).all()
    else:
        # Usuário comum vê apenas suas demandas ou da sua área
        area_usuario = None
        if hasattr(current_user, 'empregado') and current_user.empregado:
            area_usuario = current_user.empregado.get('area')

        # Query base para usuários comuns
        query_base = Demanda.query.filter(
            Demanda.DELETED_AT.is_(None),
            or_(
                Demanda.RESPONSAVEL_ID == current_user.id,
                Demanda.SOLICITANTE_ID == current_user.id,
                and_(
                    Demanda.VISIBILIDADE_AREA == True,
                    Demanda.AREA == area_usuario
                ) if area_usuario else False
            )
        )

        minhas_demandas = query_base.filter(
            Demanda.STATUS != 'CONCLUIDA'
        ).count()

        demandas_atrasadas = query_base.filter(
            Demanda.STATUS.notin_(['CONCLUIDA', 'CANCELADA']),
            Demanda.DT_PRAZO < datetime.now()
        ).count()

        demandas_urgentes = query_base.filter(
            Demanda.STATUS != 'CONCLUIDA',
            Demanda.PRIORIDADE.in_(['ALTA', 'URGENTE'])
        ).count()

        # Demandas recentes visíveis para o usuário
        demandas_recentes = []
        todas_demandas = Demanda.query.filter(
            Demanda.DELETED_AT.is_(None)
        ).order_by(Demanda.DT_CRIACAO.desc()).limit(20).all()

        for demanda in todas_demandas:
            if demanda.pode_visualizar(current_user):
                demandas_recentes.append(demanda)
                if len(demandas_recentes) >= 5:
                    break

    return render_template('demandas/index.html',
                           minhas_demandas=minhas_demandas,
                           demandas_atrasadas=demandas_atrasadas,
                           demandas_urgentes=demandas_urgentes,
                           demandas_recentes=demandas_recentes,
                           pode_criar=pode_criar)


@demandas_bp.route('/listar')
@login_required
def listar():
    """Lista demandas com filtros e permissões"""
    # Capturar filtros
    status = request.args.get('status', '')
    prioridade = request.args.get('prioridade', '')
    responsavel_id = request.args.get('responsavel_id', '')
    area = request.args.get('area', '')
    tipo_visualizacao = request.args.get('view', 'lista')

    # Query base
    query = Demanda.query.filter(Demanda.DELETED_AT.is_(None))

    # Aplicar filtros de permissão se não for admin/moderador
    if current_user.perfil not in ['admin', 'moderador']:  # Corrigido para minúsculo
        area_usuario = None
        if hasattr(current_user, 'empregado') and current_user.empregado:
            area_usuario = current_user.empregado.get('area')  # Corrigido para usar get()

        query = query.filter(
            or_(
                Demanda.RESPONSAVEL_ID == current_user.id,
                Demanda.SOLICITANTE_ID == current_user.id,
                and_(
                    Demanda.VISIBILIDADE_AREA == True,
                    Demanda.AREA == area_usuario
                ) if area_usuario else False
            )
        )

    # Aplicar outros filtros
    if status:
        query = query.filter(Demanda.STATUS == status)
    if prioridade:
        query = query.filter(Demanda.PRIORIDADE == prioridade)
    if responsavel_id:
        query = query.filter(Demanda.RESPONSAVEL_ID == responsavel_id)
    if area:
        query = query.filter(Demanda.AREA == area)

    # Ordenação
    query = query.order_by(
        Demanda.PRIORIDADE.desc(),
        Demanda.DT_PRAZO.asc(),
        Demanda.DT_CRIACAO.desc()
    )

    demandas = query.all()
    usuarios = Usuario.query.filter(Usuario.ATIVO == True).all()

    # Buscar lista de áreas únicas
    areas = db.session.query(Empregado.sgSuperintendencia).distinct().filter(
        Empregado.sgSuperintendencia.isnot(None)
    ).all()
    areas = [a[0] for a in areas]

    pode_criar = verificar_permissao_criacao()

    if tipo_visualizacao == 'kanban':
        # Organizar para visualização kanban
        kanban = {
            'PENDENTE': [],
            'EM_ANDAMENTO': [],
            'PAUSADA': [],
            'CONCLUIDA': [],
            'CANCELADA': []
        }

        for demanda in demandas:
            if demanda.STATUS in kanban:
                kanban[demanda.STATUS].append(demanda)

        return render_template('demandas/kanban.html',
                               kanban=kanban,
                               usuarios=usuarios,
                               areas=areas,
                               filtro_status=status,
                               filtro_prioridade=prioridade,
                               filtro_responsavel=responsavel_id,
                               filtro_area=area,
                               pode_criar=pode_criar)
    else:
        return render_template('demandas/lista.html',
                               demandas=demandas,
                               usuarios=usuarios,
                               areas=areas,
                               filtro_status=status,
                               filtro_prioridade=prioridade,
                               filtro_responsavel=responsavel_id,
                               filtro_area=area,
                               pode_criar=pode_criar)


@demandas_bp.route('/nova', methods=['GET', 'POST'])
@login_required
def nova():
    """Criar nova demanda - apenas admin e moderador"""
    # Verificar permissão
    if not verificar_permissao_criacao():
        flash('Você não tem permissão para criar demandas.', 'danger')
        return redirect(url_for('demandas.listar'))

    if request.method == 'POST':
        try:
            criar_pasta_upload()

            # Calcular data de prazo
            prazo_dias = int(request.form.get('prazo_dias', 1))
            dt_prazo = datetime.now() + timedelta(days=prazo_dias)

            # Verificar se foi selecionado responsável ou área
            responsavel_id = request.form.get('responsavel_id')
            area = request.form.get('area')
            visibilidade_area = request.form.get('visibilidade_area') == 'on'

            if not responsavel_id and not area:
                flash('Você deve selecionar um responsável ou uma área.', 'warning')
                return redirect(url_for('demandas.nova'))

            # Se selecionou área mas não responsável, pegar o primeiro da área
            if area and not responsavel_id:
                usuario_area = db.session.query(Usuario).join(
                    Empregado, Usuario.FK_PESSOA == Empregado.pkPessoa
                ).filter(
                    Empregado.sgSuperintendencia == area,
                    Usuario.ATIVO == True
                ).first()

                if usuario_area:
                    responsavel_id = usuario_area.ID
                else:
                    flash('Não foi encontrado nenhum usuário ativo nesta área.', 'warning')
                    return redirect(url_for('demandas.nova'))

            # Criar demanda
            demanda = Demanda(
                TITULO=request.form['titulo'],
                DESCRICAO=request.form.get('descricao'),
                RESPONSAVEL_ID=int(responsavel_id),
                SOLICITANTE_ID=current_user.id,
                PRIORIDADE=request.form['prioridade'],
                STATUS='PENDENTE',
                DT_PRAZO=dt_prazo,
                HORAS_ESTIMADAS=float(request.form.get('horas_estimadas', 0)) if request.form.get(
                    'horas_estimadas') else None,
                TIPO_DEMANDA=request.form.get('tipo_demanda'),
                SISTEMA_RELACIONADO=request.form.get('sistema_relacionado'),
                AREA=area,
                VISIBILIDADE_AREA=visibilidade_area
            )

            # Processar arquivo anexo se houver
            if 'arquivo' in request.files:
                arquivo = request.files['arquivo']
                if arquivo and arquivo.filename and allowed_file(arquivo.filename):
                    # Gerar nome único
                    filename = secure_filename(arquivo.filename)
                    unique_filename = f"{uuid.uuid4()}_{filename}"
                    filepath = os.path.join(UPLOAD_FOLDER, unique_filename)

                    # Salvar arquivo
                    arquivo.save(filepath)

                    # Salvar referência no banco
                    demanda.ARQUIVO_ANEXO = unique_filename
                    demanda.NOME_ARQUIVO = filename

            db.session.add(demanda)
            db.session.commit()

            # Se houver múltiplos arquivos, processar
            if 'arquivos_adicionais' in request.files:
                arquivos = request.files.getlist('arquivos_adicionais')
                for arquivo in arquivos:
                    if arquivo and arquivo.filename and allowed_file(arquivo.filename):
                        filename = secure_filename(arquivo.filename)
                        unique_filename = f"{uuid.uuid4()}_{filename}"
                        filepath = os.path.join(UPLOAD_FOLDER, unique_filename)
                        arquivo.save(filepath)

                        # Criar registro de anexo
                        anexo = DemandaAnexo(
                            DEMANDA_ID=demanda.ID,
                            NOME_ARQUIVO=filename,
                            CAMINHO_ARQUIVO=unique_filename,
                            TAMANHO_KB=os.path.getsize(filepath) // 1024,
                            TIPO_ARQUIVO=filename.rsplit('.', 1)[1].lower() if '.' in filename else None,
                            USUARIO_ID=current_user.id
                        )
                        db.session.add(anexo)

            db.session.commit()

            # Registrar no histórico
            historico = DemandaHistorico(
                DEMANDA_ID=demanda.ID,
                USUARIO_ID=current_user.id,
                ACAO='CRIACAO',
                DESCRICAO=f'Demanda criada por {current_user.nome}'
            )
            db.session.add(historico)
            db.session.commit()

            # Registrar log de auditoria
            registrar_log(
                acao='criar',
                entidade='demanda',
                entidade_id=demanda.ID,
                descricao=f'Nova demanda criada: {demanda.TITULO}'
            )

            flash('Demanda criada com sucesso!', 'success')
            return redirect(url_for('demandas.visualizar', id=demanda.ID))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao criar demanda: {str(e)}', 'danger')

    # Buscar usuários e áreas
    usuarios = Usuario.query.filter(Usuario.ATIVO == True).all()
    areas = db.session.query(Empregado.sgSuperintendencia).distinct().filter(
        Empregado.sgSuperintendencia.isnot(None)
    ).all()
    areas = [a[0] for a in areas]

    return render_template('demandas/form.html',
                           usuarios=usuarios,
                           areas=areas,
                           demanda=None)


@demandas_bp.route('/editar/<int:id>', methods=['GET', 'POST'])
@login_required
def editar(id):
    """Editar demanda existente"""
    demanda = Demanda.query.get_or_404(id)

    # Verificar permissão (responsável, solicitante ou admin)
    if not (current_user.id == demanda.RESPONSAVEL_ID or
            current_user.id == demanda.SOLICITANTE_ID or
            current_user.perfil == 'admin'):
        flash('Você não tem permissão para editar esta demanda.', 'danger')
        return redirect(url_for('demandas.listar'))

    if request.method == 'POST':
        try:
            # Guardar valores antigos para o histórico
            dados_antigos = {
                'titulo': demanda.TITULO,
                'status': demanda.STATUS,
                'prioridade': demanda.PRIORIDADE,
                'responsavel': demanda.RESPONSAVEL_ID
            }

            # Atualizar demanda
            demanda.TITULO = request.form['titulo']
            demanda.DESCRICAO = request.form.get('descricao')
            demanda.RESPONSAVEL_ID = int(request.form['responsavel_id'])
            demanda.PRIORIDADE = request.form['prioridade']
            demanda.HORAS_ESTIMADAS = float(request.form.get('horas_estimadas', 0)) if request.form.get(
                'horas_estimadas') else None
            demanda.TIPO_DEMANDA = request.form.get('tipo_demanda')
            demanda.SISTEMA_RELACIONADO = request.form.get('sistema_relacionado')
            demanda.UPDATED_AT = datetime.utcnow()

            # Se mudou a data do prazo
            if request.form.get('dt_prazo'):
                demanda.DT_PRAZO = datetime.strptime(request.form['dt_prazo'], '%Y-%m-%d')

            db.session.commit()

            # Registrar mudanças significativas no histórico
            if dados_antigos['status'] != demanda.STATUS:
                historico = DemandaHistorico(
                    DEMANDA_ID=demanda.ID,
                    USUARIO_ID=current_user.id,
                    ACAO='ATUALIZACAO_STATUS',
                    DESCRICAO=f'Status alterado',
                    VALOR_ANTERIOR=dados_antigos['status'],
                    VALOR_NOVO=demanda.STATUS
                )
                db.session.add(historico)

            db.session.commit()

            # Registrar log
            registrar_log(
                acao='editar',
                entidade='demanda',
                entidade_id=demanda.ID,
                descricao=f'Demanda editada: {demanda.TITULO}',
                dados_antigos=dados_antigos
            )

            flash('Demanda atualizada com sucesso!', 'success')
            return redirect(url_for('demandas.visualizar', id=demanda.ID))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao atualizar demanda: {str(e)}', 'danger')

    usuarios = Usuario.query.filter(Usuario.ATIVO == True).all()
    return render_template('demandas/form.html', usuarios=usuarios, demanda=demanda)


@demandas_bp.route('/visualizar/<int:id>')
@login_required
def visualizar(id):
    """Visualizar detalhes da demanda com verificação de permissão"""
    demanda = Demanda.query.get_or_404(id)

    # Verificar permissão de visualização
    if not demanda.pode_visualizar(current_user):
        flash('Você não tem permissão para visualizar esta demanda.', 'danger')
        return redirect(url_for('demandas.listar'))

    historico = DemandaHistorico.query.filter_by(DEMANDA_ID=id).order_by(DemandaHistorico.DATA_HORA.desc()).all()
    pode_editar = demanda.pode_editar(current_user)

    return render_template('demandas/visualizar.html',
                           demanda=demanda,
                           historico=historico,
                           pode_editar=pode_editar)


@demandas_bp.route('/atualizar-status/<int:id>', methods=['POST'])
@login_required
def atualizar_status(id):
    """Atualiza o status da demanda"""
    try:
        demanda = Demanda.query.get_or_404(id)
        novo_status = request.form.get('status')

        # Validar status
        status_validos = ['PENDENTE', 'EM_ANDAMENTO', 'PAUSADA', 'CONCLUIDA', 'CANCELADA']
        if novo_status not in status_validos:
            flash('Status inválido.', 'danger')
            return redirect(url_for('demandas.visualizar', id=id))

        # Guardar status anterior
        status_anterior = demanda.STATUS

        # Atualizar status
        demanda.STATUS = novo_status

        # Se está iniciando, registrar data de início
        if novo_status == 'EM_ANDAMENTO' and not demanda.DT_INICIO:
            demanda.DT_INICIO = datetime.utcnow()

        # Se está concluindo, registrar data de conclusão
        if novo_status == 'CONCLUIDA':
            demanda.DT_CONCLUSAO = datetime.utcnow()
            demanda.PERCENTUAL_CONCLUSAO = 100

        demanda.UPDATED_AT = datetime.utcnow()

        # Registrar no histórico
        historico = DemandaHistorico(
            DEMANDA_ID=demanda.ID,
            USUARIO_ID=current_user.id,
            ACAO='ATUALIZACAO_STATUS',
            DESCRICAO=f'Status alterado por {current_user.nome}',
            VALOR_ANTERIOR=status_anterior,
            VALOR_NOVO=novo_status
        )

        db.session.add(historico)
        db.session.commit()

        flash('Status atualizado com sucesso!', 'success')
        return redirect(url_for('demandas.visualizar', id=id))

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao atualizar status: {str(e)}', 'danger')
        return redirect(url_for('demandas.visualizar', id=id))


@demandas_bp.route('/atualizar-progresso/<int:id>', methods=['POST'])
@login_required
def atualizar_progresso(id):
    """Atualiza o percentual de conclusão e horas trabalhadas"""
    try:
        demanda = Demanda.query.get_or_404(id)

        percentual_anterior = demanda.PERCENTUAL_CONCLUSAO
        horas_anterior = demanda.HORAS_TRABALHADAS

        # Atualizar valores
        demanda.PERCENTUAL_CONCLUSAO = int(request.form.get('percentual', 0))
        demanda.HORAS_TRABALHADAS = float(request.form.get('horas_trabalhadas', 0))
        demanda.UPDATED_AT = datetime.utcnow()

        # Se chegou a 100%, marcar como concluída
        if demanda.PERCENTUAL_CONCLUSAO == 100 and demanda.STATUS != 'CONCLUIDA':
            demanda.STATUS = 'CONCLUIDA'
            demanda.DT_CONCLUSAO = datetime.utcnow()

        # Registrar no histórico
        descricao = f'Progresso atualizado: {percentual_anterior}% → {demanda.PERCENTUAL_CONCLUSAO}%'
        if horas_anterior != demanda.HORAS_TRABALHADAS:
            descricao += f' | Horas: {horas_anterior}h → {demanda.HORAS_TRABALHADAS}h'

        historico = DemandaHistorico(
            DEMANDA_ID=demanda.ID,
            USUARIO_ID=current_user.id,
            ACAO='ATUALIZACAO_PERCENTUAL',
            DESCRICAO=descricao,
            VALOR_ANTERIOR=str(percentual_anterior),
            VALOR_NOVO=str(demanda.PERCENTUAL_CONCLUSAO)
        )

        db.session.add(historico)
        db.session.commit()

        flash('Progresso atualizado com sucesso!', 'success')
        return redirect(url_for('demandas.visualizar', id=id))

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao atualizar progresso: {str(e)}', 'danger')
        return redirect(url_for('demandas.visualizar', id=id))


@demandas_bp.route('/adicionar-comentario/<int:id>', methods=['POST'])
@login_required
def adicionar_comentario(id):
    """Adiciona um comentário/observação à demanda"""
    try:
        demanda = Demanda.query.get_or_404(id)
        comentario = request.form.get('comentario')

        if not comentario:
            flash('Comentário não pode estar vazio.', 'warning')
            return redirect(url_for('demandas.visualizar', id=id))

        # Registrar no histórico como comentário
        historico = DemandaHistorico(
            DEMANDA_ID=demanda.ID,
            USUARIO_ID=current_user.id,
            ACAO='COMENTARIO',
            DESCRICAO=comentario
        )

        db.session.add(historico)
        db.session.commit()

        flash('Comentário adicionado com sucesso!', 'success')
        return redirect(url_for('demandas.visualizar', id=id))

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao adicionar comentário: {str(e)}', 'danger')
        return redirect(url_for('demandas.visualizar', id=id))


@demandas_bp.route('/dashboard')
@login_required
def dashboard():
    """Dashboard com estatísticas e gráficos"""
    # Estatísticas gerais
    total_demandas = Demanda.query.filter(Demanda.DELETED_AT.is_(None)).count()
    demandas_abertas = Demanda.query.filter(
        Demanda.DELETED_AT.is_(None),
        Demanda.STATUS.notin_(['CONCLUIDA', 'CANCELADA'])
    ).count()

    # Demandas por status
    demandas_por_status = db.session.query(
        Demanda.STATUS,
        func.count(Demanda.ID)
    ).filter(
        Demanda.DELETED_AT.is_(None)
    ).group_by(Demanda.STATUS).all()

    # Demandas por prioridade
    demandas_por_prioridade = db.session.query(
        Demanda.PRIORIDADE,
        func.count(Demanda.ID)
    ).filter(
        Demanda.DELETED_AT.is_(None),
        Demanda.STATUS.notin_(['CONCLUIDA', 'CANCELADA'])
    ).group_by(Demanda.PRIORIDADE).all()

    # Top 5 responsáveis com mais demandas
    top_responsaveis = db.session.query(
        Usuario.NOME,
        func.count(Demanda.ID).label('total')
    ).join(
        Demanda, Demanda.RESPONSAVEL_ID == Usuario.ID
    ).filter(
        Demanda.DELETED_AT.is_(None),
        Demanda.STATUS.notin_(['CONCLUIDA', 'CANCELADA'])
    ).group_by(Usuario.NOME).order_by(func.count(Demanda.ID).desc()).limit(5).all()

    return render_template('demandas/dashboard.html',
                           total_demandas=total_demandas,
                           demandas_abertas=demandas_abertas,
                           demandas_por_status=demandas_por_status,
                           demandas_por_prioridade=demandas_por_prioridade,
                           top_responsaveis=top_responsaveis)


@demandas_bp.route('/download/<int:demanda_id>/<int:anexo_id>')
@login_required
def download_anexo(demanda_id, anexo_id):
    """Download de arquivo anexo"""
    demanda = Demanda.query.get_or_404(demanda_id)

    # Verificar permissão
    if not demanda.pode_visualizar(current_user):
        abort(403)

    anexo = DemandaAnexo.query.get_or_404(anexo_id)

    if anexo.DEMANDA_ID != demanda_id:
        abort(404)

    filepath = os.path.join(UPLOAD_FOLDER, anexo.CAMINHO_ARQUIVO)

    if not os.path.exists(filepath):
        flash('Arquivo não encontrado.', 'danger')
        return redirect(url_for('demandas.visualizar', id=demanda_id))

    return send_file(filepath, as_attachment=True, download_name=anexo.NOME_ARQUIVO)


@demandas_bp.route('/excluir/<int:id>', methods=['POST'])
@login_required
def excluir(id):
    """Exclui (soft delete) uma demanda"""
    try:
        demanda = Demanda.query.get_or_404(id)

        # Verificar permissão
        if not (current_user.id == demanda.SOLICITANTE_ID or current_user.perfil == 'admin'):
            flash('Você não tem permissão para excluir esta demanda.', 'danger')
            return redirect(url_for('demandas.listar'))

        # Soft delete
        demanda.DELETED_AT = datetime.utcnow()

        # Registrar log
        registrar_log(
            acao='excluir',
            entidade='demanda',
            entidade_id=demanda.ID,
            descricao=f'Demanda excluída: {demanda.TITULO}'
        )

        db.session.commit()
        flash('Demanda excluída com sucesso!', 'success')
        return redirect(url_for('demandas.listar'))

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao excluir demanda: {str(e)}', 'danger')
        return redirect(url_for('demandas.listar'))