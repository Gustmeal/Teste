# app/routes/teletrabalho_routes.py
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from app import db
from app.models.teletrabalho import Teletrabalho, Feriado
from app.models.usuario import Usuario, Empregado
from app.utils.audit import registrar_log
from datetime import datetime, timedelta, date
from sqlalchemy import and_, or_, func, text, extract
import calendar
import math
from app.models.teletrabalho import ConfigAreaTeletrabalho

teletrabalho_bp = Blueprint('teletrabalho', __name__, url_prefix='/teletrabalho')


def eh_gestor_ou_admin(usuario):
    """Verifica se o usuário é gestor, admin ou moderador"""
    if usuario.PERFIL in ['admin', 'moderador']:
        return True

    if usuario.empregado and usuario.empregado.dsCargo:
        if 'GERENTE' in usuario.empregado.dsCargo.upper():
            return True

    return False


def calcular_limite_area(area, tipo_area, percentual=30.0):
    """
    Calcula limite baseado em ConfigAreaTeletrabalho (se existir)
    ou conta pessoas reais na área
    """
    from app.models.teletrabalho import ConfigAreaTeletrabalho

    # Buscar configuração salva
    config = ConfigAreaTeletrabalho.query.filter_by(
        AREA=area,
        TIPO_AREA=tipo_area,
        DELETED_AT=None
    ).first()

    if config:
        # Usar configuração personalizada
        qtd = config.QTD_TOTAL_PESSOAS
        percentual = float(config.PERCENTUAL_LIMITE)
    else:
        # Contar pessoas reais na área
        if tipo_area == 'setor':
            qtd = Empregado.query.filter(
                Empregado.sgSetor == area,
                Empregado.fkStatus == 1
            ).count()
        elif tipo_area == 'superintendencia':
            qtd = Empregado.query.filter(
                Empregado.sgSuperintendencia == area,
                Empregado.fkStatus == 1
            ).count()
        else:
            qtd = Empregado.query.filter(
                Empregado.sgDiretoria == area,
                Empregado.fkStatus == 1
            ).count()

    limite = (qtd * percentual) / 100.0
    return math.ceil(limite), qtd

def obter_areas_disponiveis():
    """Retorna lista de áreas disponíveis"""
    areas = []

    superintendencias = db.session.query(
        Empregado.sgSuperintendencia,
        func.count(Empregado.pkPessoa).label('qtd')
    ).filter(
        Empregado.sgSuperintendencia.isnot(None),
        Empregado.sgSuperintendencia != '',
        Empregado.fkStatus == 1
    ).group_by(Empregado.sgSuperintendencia).all()

    for sup, qtd in superintendencias:
        areas.append({
            'area': sup,
            'tipo': 'superintendencia',
            'qtd_pessoas': qtd
        })

    return areas


def gerar_opcoes_periodo():
    """Gera opções de ano e mês para seleção (ano atual até +2 anos)"""
    hoje = date.today()
    ano_atual = hoje.year

    periodos = []

    for ano in range(ano_atual, ano_atual + 3):  # 2025, 2026, 2027
        for mes in range(1, 13):
            # Só incluir meses futuros do ano atual
            if ano == ano_atual and mes < hoje.month:
                continue

            data = date(ano, mes, 1)
            mes_ref = data.strftime('%Y%m')
            meses_pt = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
                        'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
            nome_mes = f"{meses_pt[mes - 1]}/{ano}"

            periodos.append({
                'mes_ref': mes_ref,
                'nome': nome_mes,
                'ano': ano,
                'mes': mes
            })

    return periodos


@teletrabalho_bp.route('/')
@login_required
def index():
    """Página principal do sistema de teletrabalho"""
    try:
        usuario = Usuario.query.get(current_user.id)
        empregado = usuario.empregado

        if current_user.perfil == 'admin':
            if not empregado:
                areas = obter_areas_disponiveis()
                periodos = gerar_opcoes_periodo()

                return render_template('teletrabalho/index.html',
                                       area='Admin (selecione área)',
                                       tipo_area='superintendencia',
                                       limite=0,
                                       total_pessoas=0,
                                       eh_gestor=True,
                                       areas=areas,
                                       meses_disponiveis=periodos[:12],
                                       admin_sem_empregado=True)

            area = empregado.sgSuperintendencia
            tipo_area = 'superintendencia'
            limite, total_pessoas = calcular_limite_area(area, tipo_area)
            areas = obter_areas_disponiveis()
        else:
            if not empregado:
                flash('Usuário sem vínculo com empregado. Entre em contato com o RH.', 'warning')
                return redirect(url_for('main.geinc_index'))

            area = empregado.sgSuperintendencia
            tipo_area = 'superintendencia'
            limite, total_pessoas = calcular_limite_area(area, tipo_area)
            areas = None

        periodos = gerar_opcoes_periodo()

        return render_template('teletrabalho/index.html',
                               area=area,
                               tipo_area=tipo_area,
                               limite=limite,
                               total_pessoas=total_pessoas,
                               eh_gestor=eh_gestor_ou_admin(usuario),
                               areas=areas,
                               meses_disponiveis=periodos[:12],
                               admin_sem_empregado=False)

    except Exception as e:
        flash(f'Erro ao carregar página: {str(e)}', 'danger')
        return redirect(url_for('main.geinc_index'))


@teletrabalho_bp.route('/calendario/<mes_referencia>')
@login_required
def calendario(mes_referencia):
    """Exibe o calendário do mês selecionado"""
    try:
        usuario = Usuario.query.get(current_user.id)
        empregado = usuario.empregado

        area_param = request.args.get('area')
        tipo_area_param = request.args.get('tipo_area')

        if current_user.perfil == 'admin' and area_param:
            area = area_param
            tipo_area = tipo_area_param
        else:
            if current_user.perfil == 'admin' and not empregado:
                flash('Selecione uma área para visualizar o calendário.', 'warning')
                return redirect(url_for('teletrabalho.index'))

            if not empregado:
                flash('Usuário sem vínculo com empregado. Entre em contato com o RH.', 'warning')
                return redirect(url_for('main.geinc_index'))

            area = empregado.sgSuperintendencia
            tipo_area = 'superintendencia'

        limite, total_pessoas = calcular_limite_area(area, tipo_area)

        ano = int(mes_referencia[:4])
        mes = int(mes_referencia[4:])

        meses_pt = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
                    'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
        nome_mes = f"{meses_pt[mes - 1]} de {ano}"

        primeiro_dia = date(ano, mes, 1)
        ultimo_dia = date(ano, mes, calendar.monthrange(ano, mes)[1])

        # CORREÇÃO CRÍTICA: Calcular dia da semana
        # Python: 0=Segunda, 1=Terça, ..., 6=Domingo
        # Grid: 0=Domingo, 1=Segunda, ..., 6=Sábado
        # Fórmula: (weekday + 1) % 7
        primeiro_dia_weekday = primeiro_dia.weekday()
        primeiro_dia_grid = (primeiro_dia_weekday + 1) % 7

        teletrabalhos = Teletrabalho.query.filter(
            Teletrabalho.MES_REFERENCIA == mes_referencia,
            Teletrabalho.AREA == area,
            Teletrabalho.TIPO_AREA == tipo_area,
            Teletrabalho.STATUS == 'APROVADO',
            Teletrabalho.DELETED_AT.is_(None)
        ).all()

        dias_ocupados = {}
        for tele in teletrabalhos:
            data_str = tele.DATA_TELETRABALHO.strftime('%Y-%m-%d')
            if data_str not in dias_ocupados:
                dias_ocupados[data_str] = []
            dias_ocupados[data_str].append({
                'usuario': tele.usuario.NOME,
                'usuario_id': tele.USUARIO_ID,
                'id': tele.ID
            })

        dias_calendario = []
        data_atual = primeiro_dia

        while data_atual <= ultimo_dia:
            data_str = data_atual.strftime('%Y-%m-%d')
            eh_util = Feriado.eh_dia_util(data_atual)
            feriado = Feriado.obter_feriado(data_atual)
            qtd_pessoas = len(dias_ocupados.get(data_str, []))

            dias_calendario.append({
                'data': data_str,
                'dia': data_atual.day,
                'eh_util': eh_util,
                'feriado': feriado.DS_FERIADO if feriado else None,
                'qtd_pessoas': qtd_pessoas,
                'limite': limite,
                'lotado': qtd_pessoas >= limite,
                'pessoas': dias_ocupados.get(data_str, [])
            })

            data_atual += timedelta(days=1)

        meus_dias = Teletrabalho.query.filter(
            Teletrabalho.USUARIO_ID == current_user.id,
            Teletrabalho.MES_REFERENCIA == mes_referencia,
            Teletrabalho.STATUS == 'APROVADO',
            Teletrabalho.DELETED_AT.is_(None)
        ).all()

        meus_dias_datas = [d.DATA_TELETRABALHO.strftime('%Y-%m-%d') for d in meus_dias]

        # Opções de período para navegação
        periodos = gerar_opcoes_periodo()

        return render_template('teletrabalho/calendario.html',
                               dias=dias_calendario,
                               mes_referencia=mes_referencia,
                               nome_mes=nome_mes,
                               limite=limite,
                               total_pessoas=total_pessoas,
                               area=area,
                               tipo_area=tipo_area,
                               meus_dias=meus_dias_datas,
                               eh_gestor=eh_gestor_ou_admin(usuario),
                               pode_solicitar=(empregado is not None),
                               primeiro_dia_grid=primeiro_dia_grid,
                               periodos_disponiveis=periodos)

    except Exception as e:
        flash(f'Erro ao carregar calendário: {str(e)}', 'danger')
        return redirect(url_for('teletrabalho.index'))


@teletrabalho_bp.route('/visualizacao-geral')
@login_required
def visualizacao_geral():
    """Visualização geral de todos os teletrabalhos"""
    try:
        # Filtros
        mes_ref = request.args.get('mes_ref')
        area_filtro = request.args.get('area')

        # Período padrão: mês atual
        if not mes_ref:
            hoje = date.today()
            mes_ref = hoje.strftime('%Y%m')

        ano = int(mes_ref[:4])
        mes = int(mes_ref[4:])

        primeiro_dia = date(ano, mes, 1)
        ultimo_dia = date(ano, mes, calendar.monthrange(ano, mes)[1])

        meses_pt = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
                    'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
        nome_mes = f"{meses_pt[mes - 1]} de {ano}"

        # Buscar teletrabalhos
        query = Teletrabalho.query.filter(
            Teletrabalho.MES_REFERENCIA == mes_ref,
            Teletrabalho.STATUS == 'APROVADO',
            Teletrabalho.DELETED_AT.is_(None)
        )

        if area_filtro:
            query = query.filter(Teletrabalho.AREA == area_filtro)

        teletrabalhos = query.order_by(Teletrabalho.DATA_TELETRABALHO).all()

        # Organizar por dia
        dias_organizados = {}
        data_atual = primeiro_dia

        while data_atual <= ultimo_dia:
            data_str = data_atual.strftime('%Y-%m-%d')
            eh_util = Feriado.eh_dia_util(data_atual)
            feriado = Feriado.obter_feriado(data_atual)

            dias_semana = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
            dia_semana_nome = dias_semana[data_atual.weekday()]

            dias_organizados[data_str] = {
                'data': data_atual,
                'dia': data_atual.day,
                'dia_semana': dia_semana_nome,
                'eh_util': eh_util,
                'feriado': feriado.DS_FERIADO if feriado else None,
                'pessoas': []
            }

            data_atual += timedelta(days=1)

        # Adicionar pessoas aos dias
        for tele in teletrabalhos:
            data_str = tele.DATA_TELETRABALHO.strftime('%Y-%m-%d')
            if data_str in dias_organizados:
                dias_organizados[data_str]['pessoas'].append({
                    'nome': tele.usuario.NOME,
                    'area': tele.AREA,
                    'tipo_periodo': tele.TIPO_PERIODO,
                    'observacao': tele.OBSERVACAO
                })

        # Estatísticas
        total_dias_solicitados = len(teletrabalhos)
        total_pessoas_unicas = len(set([t.USUARIO_ID for t in teletrabalhos]))

        # Áreas e períodos
        areas = obter_areas_disponiveis()
        periodos = gerar_opcoes_periodo()

        return render_template('teletrabalho/visualizacao_geral.html',
                               dias_organizados=dias_organizados,
                               nome_mes=nome_mes,
                               mes_ref=mes_ref,
                               area_filtro=area_filtro,
                               areas=areas,
                               periodos=periodos,
                               total_dias=total_dias_solicitados,
                               total_pessoas=total_pessoas_unicas)

    except Exception as e:
        flash(f'Erro ao carregar visualização: {str(e)}', 'danger')
        return redirect(url_for('teletrabalho.index'))


@teletrabalho_bp.route('/solicitar', methods=['POST'])
@login_required
def solicitar():
    """Solicita dias de teletrabalho"""
    try:
        usuario = Usuario.query.get(current_user.id)
        empregado = usuario.empregado

        if not empregado:
            flash('Você precisa estar vinculado a um empregado para solicitar teletrabalho.', 'danger')
            return redirect(url_for('teletrabalho.index'))

        datas_str = request.form.getlist('datas[]')
        mes_referencia = request.form.get('mes_referencia')
        tipo_periodo = request.form.get('tipo_periodo')
        observacao = request.form.get('observacao', '')

        area = empregado.sgSuperintendencia
        tipo_area = 'superintendencia'

        limite, total_pessoas = calcular_limite_area(area, tipo_area)

        qtd_dias_mes = Teletrabalho.contar_dias_mes(current_user.id, mes_referencia)

        if qtd_dias_mes >= 5:
            flash('Você já atingiu o limite de 5 dias de teletrabalho neste mês.', 'warning')
            return redirect(url_for('teletrabalho.calendario', mes_referencia=mes_referencia))

        if len(datas_str) + qtd_dias_mes > 5:
            flash(f'Você só pode marcar mais {5 - qtd_dias_mes} dia(s) neste mês.', 'warning')
            return redirect(url_for('teletrabalho.calendario', mes_referencia=mes_referencia))

        datas = [datetime.strptime(d, '%Y-%m-%d').date() for d in datas_str]

        if tipo_periodo == 'CINCO_DIAS_CORRIDOS':
            if len(datas) != 5:
                flash('Para o tipo "5 dias corridos" você deve selecionar exatamente 5 dias.', 'warning')
                return redirect(url_for('teletrabalho.calendario', mes_referencia=mes_referencia))

            valido, msg = Teletrabalho.validar_cinco_dias_corridos(current_user.id, datas, mes_referencia)
            if not valido:
                flash(msg, 'warning')
                return redirect(url_for('teletrabalho.calendario', mes_referencia=mes_referencia))

        elif tipo_periodo == 'ALTERNADO':
            for data in datas:
                valido, msg = Teletrabalho.validar_dias_alternados(current_user.id, data, mes_referencia)
                if not valido:
                    flash(f'Data {data.strftime("%d/%m/%Y")}: {msg}', 'warning')
                    return redirect(url_for('teletrabalho.calendario', mes_referencia=mes_referencia))

        for data in datas:
            if not Feriado.eh_dia_util(data):
                feriado = Feriado.obter_feriado(data)
                motivo = feriado.DS_FERIADO if feriado else "fim de semana"
                flash(f'A data {data.strftime("%d/%m/%Y")} não é dia útil ({motivo}).', 'warning')
                return redirect(url_for('teletrabalho.calendario', mes_referencia=mes_referencia))

            qtd_atual = Teletrabalho.contar_pessoas_dia(data, area, tipo_area)
            if qtd_atual >= limite:
                flash(f'Data {data.strftime("%d/%m/%Y")}: Limite de {limite} pessoas atingido.', 'warning')
                return redirect(url_for('teletrabalho.calendario', mes_referencia=mes_referencia))

        for data in datas:
            teletrabalho = Teletrabalho(
                USUARIO_ID=current_user.id,
                DATA_TELETRABALHO=data,
                MES_REFERENCIA=mes_referencia,
                AREA=area,
                TIPO_AREA=tipo_area,
                STATUS='APROVADO',
                TIPO_PERIODO=tipo_periodo,
                OBSERVACAO=observacao,
                APROVADO_POR=current_user.id,
                APROVADO_EM=datetime.utcnow()
            )
            db.session.add(teletrabalho)

        registrar_log(
            acao='criar',
            entidade='teletrabalho',
            entidade_id=0,
            descricao=f'Solicitação de {len(datas)} dia(s) de teletrabalho - {tipo_periodo}'
        )

        db.session.commit()
        flash(f'{len(datas)} dia(s) de teletrabalho solicitado(s) com sucesso!', 'success')
        return redirect(url_for('teletrabalho.calendario', mes_referencia=mes_referencia))

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao solicitar teletrabalho: {str(e)}', 'danger')
        return redirect(url_for('teletrabalho.index'))


@teletrabalho_bp.route('/cancelar/<int:id>', methods=['POST'])
@login_required
def cancelar(id):
    """Cancela um dia de teletrabalho"""
    try:
        teletrabalho = Teletrabalho.query.get_or_404(id)

        if teletrabalho.USUARIO_ID != current_user.id and not eh_gestor_ou_admin(Usuario.query.get(current_user.id)):
            flash('Você não tem permissão para cancelar este teletrabalho.', 'danger')
            return redirect(url_for('teletrabalho.index'))

        teletrabalho.DELETED_AT = datetime.utcnow()

        registrar_log(
            acao='excluir',
            entidade='teletrabalho',
            entidade_id=id,
            descricao=f'Cancelamento de teletrabalho - {teletrabalho.DATA_TELETRABALHO}'
        )

        db.session.commit()
        flash('Teletrabalho cancelado com sucesso!', 'success')

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao cancelar teletrabalho: {str(e)}', 'danger')

    return redirect(request.referrer or url_for('teletrabalho.index'))


@teletrabalho_bp.route('/relatorio')
@login_required
def relatorio():
    """Relatório de teletrabalho (apenas admin)"""
    try:
        if current_user.perfil != 'admin':
            flash('Você não tem permissão para acessar esta página.', 'danger')
            return redirect(url_for('teletrabalho.index'))

        mes_ref = request.args.get('mes_ref')
        area_filtro = request.args.get('area')

        query = db.session.query(
            Teletrabalho,
            Usuario
        ).join(
            Usuario, Teletrabalho.USUARIO_ID == Usuario.ID
        ).filter(
            Teletrabalho.DELETED_AT.is_(None)
        )

        if mes_ref:
            query = query.filter(Teletrabalho.MES_REFERENCIA == mes_ref)

        if area_filtro:
            query = query.filter(Teletrabalho.AREA == area_filtro)

        resultados = query.order_by(
            Teletrabalho.DATA_TELETRABALHO.desc()
        ).all()

        areas = obter_areas_disponiveis()
        periodos = gerar_opcoes_periodo()

        total_solicitacoes = len(resultados)
        total_aprovadas = sum(1 for t, u in resultados if t.STATUS == 'APROVADO')

        return render_template('teletrabalho/relatorio.html',
                               resultados=resultados,
                               areas=areas,
                               periodos=periodos,
                               total_solicitacoes=total_solicitacoes,
                               total_aprovadas=total_aprovadas,
                               mes_ref=mes_ref,
                               area_filtro=area_filtro)

    except Exception as e:
        flash(f'Erro ao gerar relatório: {str(e)}', 'danger')
        return redirect(url_for('teletrabalho.index'))


# Rotas Admin continuam as mesmas...
@teletrabalho_bp.route('/admin/cadastrar', methods=['GET'])
@login_required
def admin_cadastrar():
    """Formulário para admin/moderador/gerente cadastrar teletrabalho"""
    usuario = Usuario.query.get(current_user.id)

    # CORREÇÃO: Permitir admin, moderador E gerentes
    if not eh_gestor_ou_admin(usuario):
        flash('Acesso negado. Apenas administradores, moderadores e gerentes.', 'danger')
        return redirect(url_for('teletrabalho.index'))

    try:
        usuarios = db.session.query(
            Usuario.ID,
            Usuario.NOME,
            Usuario.EMAIL,
            Empregado.sgSuperintendencia,
            Empregado.dsCargo
        ).join(
            Empregado, Usuario.FK_PESSOA == Empregado.pkPessoa
        ).filter(
            Usuario.ATIVO == True,
            Usuario.DELETED_AT.is_(None),
            Empregado.fkStatus == 1,
            Empregado.sgSuperintendencia.isnot(None)
        ).order_by(Usuario.NOME).all()

        lista_usuarios = []
        for u in usuarios:
            lista_usuarios.append({
                'id': u.ID,
                'nome': u.NOME,
                'email': u.EMAIL,
                'area': u.sgSuperintendencia,
                'cargo': u.dsCargo
            })

        periodos = gerar_opcoes_periodo()

        return render_template('teletrabalho/admin_cadastrar.html',
                               usuarios=lista_usuarios,
                               meses_disponiveis=periodos)

    except Exception as e:
        flash(f'Erro ao carregar página: {str(e)}', 'danger')
        return redirect(url_for('teletrabalho.index'))


@teletrabalho_bp.route('/admin/obter_calendario/<int:usuario_id>/<mes_referencia>')
@login_required
def admin_obter_calendario(usuario_id, mes_referencia):
    """API para obter calendário do usuário selecionado"""
    usuario = Usuario.query.get(current_user.id)

    # CORREÇÃO: Permitir admin, moderador E gerentes
    if not eh_gestor_ou_admin(usuario):
        return jsonify({'erro': 'Acesso negado'}), 403

    try:
        usuario_selecionado = Usuario.query.get_or_404(usuario_id)
        empregado = usuario_selecionado.empregado

        if not empregado:
            return jsonify({'erro': 'Usuário sem vínculo com empregado'}), 400

        area = empregado.sgSuperintendencia
        tipo_area = 'superintendencia'

        limite, total_pessoas = calcular_limite_area(area, tipo_area)

        ano = int(mes_referencia[:4])
        mes = int(mes_referencia[4:])

        primeiro_dia = date(ano, mes, 1)
        ultimo_dia = date(ano, mes, calendar.monthrange(ano, mes)[1])

        primeiro_dia_grid = (primeiro_dia.weekday() + 1) % 7

        teletrabalhos = Teletrabalho.query.filter(
            Teletrabalho.MES_REFERENCIA == mes_referencia,
            Teletrabalho.AREA == area,
            Teletrabalho.TIPO_AREA == tipo_area,
            Teletrabalho.STATUS == 'APROVADO',
            Teletrabalho.DELETED_AT.is_(None)
        ).all()

        dias_ocupados = {}
        for tele in teletrabalhos:
            data_str = tele.DATA_TELETRABALHO.strftime('%Y-%m-%d')
            if data_str not in dias_ocupados:
                dias_ocupados[data_str] = {'qtd': 0, 'usuarios': []}
            dias_ocupados[data_str]['qtd'] += 1
            dias_ocupados[data_str]['usuarios'].append({
                'nome': tele.usuario.NOME,
                'eh_usuario_selecionado': tele.USUARIO_ID == usuario_id
            })

        dias_usuario = Teletrabalho.query.filter(
            Teletrabalho.USUARIO_ID == usuario_id,
            Teletrabalho.MES_REFERENCIA == mes_referencia,
            Teletrabalho.STATUS == 'APROVADO',
            Teletrabalho.DELETED_AT.is_(None)
        ).all()

        dias_usuario_marcados = [d.DATA_TELETRABALHO.strftime('%Y-%m-%d') for d in dias_usuario]

        dias_calendario = []
        data_atual = primeiro_dia

        while data_atual <= ultimo_dia:
            data_str = data_atual.strftime('%Y-%m-%d')
            eh_util = Feriado.eh_dia_util(data_atual)
            feriado = Feriado.obter_feriado(data_atual)
            qtd_pessoas = dias_ocupados.get(data_str, {}).get('qtd', 0)

            dias_calendario.append({
                'data': data_str,
                'dia': data_atual.day,
                'eh_util': eh_util,
                'feriado': feriado.DS_FERIADO if feriado else None,
                'qtd_pessoas': qtd_pessoas,
                'limite': limite,
                'lotado': qtd_pessoas >= limite,
                'ja_marcado': data_str in dias_usuario_marcados,
                'usuarios': dias_ocupados.get(data_str, {}).get('usuarios', [])
            })

            data_atual += timedelta(days=1)

        return jsonify({
            'dias': dias_calendario,
            'area': area,
            'limite': limite,
            'total_pessoas': total_pessoas,
            'dias_marcados': len(dias_usuario_marcados),
            'primeiro_dia_grid': primeiro_dia_grid
        })

    except Exception as e:
        return jsonify({'erro': str(e)}), 500


@teletrabalho_bp.route('/admin/cadastrar', methods=['POST'])
@login_required
def admin_cadastrar_post():
    """Processa cadastro de teletrabalho pelo admin/moderador/gerente"""
    usuario_logado = Usuario.query.get(current_user.id)

    # CORREÇÃO: Permitir admin, moderador E gerentes
    if not eh_gestor_ou_admin(usuario_logado):
        flash('Acesso negado. Apenas administradores, moderadores e gerentes.', 'danger')
        return redirect(url_for('teletrabalho.index'))

    try:
        usuario_id = int(request.form.get('usuario_id'))
        datas_str = request.form.getlist('datas[]')
        mes_referencia = request.form.get('mes_referencia')
        tipo_periodo = request.form.get('tipo_periodo')
        observacao = request.form.get('observacao', '')

        usuario = Usuario.query.get_or_404(usuario_id)
        empregado = usuario.empregado

        if not empregado:
            flash('Usuário sem vínculo com empregado.', 'danger')
            return redirect(url_for('teletrabalho.admin_cadastrar'))

        area = empregado.sgSuperintendencia
        tipo_area = 'superintendencia'

        limite, total_pessoas = calcular_limite_area(area, tipo_area)

        qtd_dias_mes = Teletrabalho.contar_dias_mes(usuario_id, mes_referencia)

        if qtd_dias_mes >= 5:
            flash(f'{usuario.NOME} já atingiu o limite de 5 dias neste mês.', 'warning')
            return redirect(url_for('teletrabalho.admin_cadastrar'))

        if len(datas_str) + qtd_dias_mes > 5:
            flash(f'{usuario.NOME} só pode marcar mais {5 - qtd_dias_mes} dia(s) neste mês.', 'warning')
            return redirect(url_for('teletrabalho.admin_cadastrar'))

        datas = [datetime.strptime(d, '%Y-%m-%d').date() for d in datas_str]

        if tipo_periodo == 'CINCO_DIAS_CORRIDOS':
            if len(datas) != 5:
                flash('Para "5 dias corridos" selecione exatamente 5 dias.', 'warning')
                return redirect(url_for('teletrabalho.admin_cadastrar'))

            valido, msg = Teletrabalho.validar_cinco_dias_corridos(usuario_id, datas, mes_referencia)
            if not valido:
                flash(msg, 'warning')
                return redirect(url_for('teletrabalho.admin_cadastrar'))

        elif tipo_periodo == 'ALTERNADO':
            for data in datas:
                valido, msg = Teletrabalho.validar_dias_alternados(usuario_id, data, mes_referencia)
                if not valido:
                    flash(f'Data {data.strftime("%d/%m/%Y")}: {msg}', 'warning')
                    return redirect(url_for('teletrabalho.admin_cadastrar'))

        for data in datas:
            if not Feriado.eh_dia_util(data):
                feriado = Feriado.obter_feriado(data)
                motivo = feriado.DS_FERIADO if feriado else "fim de semana"
                flash(f'{data.strftime("%d/%m/%Y")} não é dia útil ({motivo}).', 'warning')
                return redirect(url_for('teletrabalho.admin_cadastrar'))

            qtd_atual = Teletrabalho.contar_pessoas_dia(data, area, tipo_area)
            if qtd_atual >= limite:
                flash(f'{data.strftime("%d/%m/%Y")}: Limite de {limite} pessoas atingido.', 'warning')
                return redirect(url_for('teletrabalho.admin_cadastrar'))

        # Identificar tipo de usuário que está cadastrando
        tipo_cadastrador = 'ADMIN' if usuario_logado.PERFIL == 'admin' else (
            'MODERADOR' if usuario_logado.PERFIL == 'moderador' else 'GERENTE'
        )

        for data in datas:
            teletrabalho = Teletrabalho(
                USUARIO_ID=usuario_id,
                DATA_TELETRABALHO=data,
                MES_REFERENCIA=mes_referencia,
                AREA=area,
                TIPO_AREA=tipo_area,
                STATUS='APROVADO',
                TIPO_PERIODO=tipo_periodo,
                OBSERVACAO=f'[{tipo_cadastrador}] {observacao}' if observacao else f'[{tipo_cadastrador}] Cadastrado por {tipo_cadastrador.lower()}',
                APROVADO_POR=current_user.id,
                APROVADO_EM=datetime.utcnow()
            )
            db.session.add(teletrabalho)

        registrar_log(
            acao='criar',
            entidade='teletrabalho',
            entidade_id=0,
            descricao=f'{tipo_cadastrador} cadastrou {len(datas)} dia(s) de teletrabalho para {usuario.NOME}'
        )

        db.session.commit()
        flash(f'{len(datas)} dia(s) cadastrado(s) para {usuario.NOME} com sucesso!', 'success')
        return redirect(url_for('teletrabalho.admin_cadastrar'))

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao cadastrar: {str(e)}', 'danger')
        return redirect(url_for('teletrabalho.admin_cadastrar'))


@teletrabalho_bp.route('/config/area', methods=['GET', 'POST'])
@login_required
def config_area():
    """Configuração da área - criar/editar"""
    usuario = Usuario.query.get(current_user.id)

    if not eh_gestor_ou_admin(usuario):
        flash('Acesso negado. Apenas gestores podem configurar.', 'danger')
        return redirect(url_for('teletrabalho.index'))

    try:
        empregado = usuario.empregado

        # Admin pode escolher área, gerente só sua área
        if current_user.perfil == 'admin':
            area_param = request.args.get('area')
            tipo_area_param = request.args.get('tipo_area', 'superintendencia')

            if area_param:
                area = area_param
                tipo_area = tipo_area_param
            elif empregado:
                area = empregado.sgSuperintendencia
                tipo_area = 'superintendencia'
            else:
                # Admin sem empregado - mostrar seletor
                areas = obter_areas_disponiveis()
                return render_template('teletrabalho/config_area.html',
                                       config=None,
                                       area=None,
                                       tipo_area=None,
                                       areas_disponiveis=areas,
                                       mostrar_seletor=True)
        else:
            # Gerente/Moderador - só sua área
            if not empregado:
                flash('Usuário sem vínculo com empregado.', 'danger')
                return redirect(url_for('teletrabalho.index'))

            area = empregado.sgSuperintendencia
            tipo_area = 'superintendencia'

        if request.method == 'POST':
            qtd_pessoas = int(request.form.get('qtd_pessoas'))
            percentual = float(request.form.get('percentual', 30.0))

            if qtd_pessoas < 1:
                flash('Quantidade deve ser maior que zero.', 'warning')
                return redirect(url_for('teletrabalho.config_area', area=area, tipo_area=tipo_area))

            if percentual < 1 or percentual > 100:
                flash('Percentual deve estar entre 1 e 100.', 'warning')
                return redirect(url_for('teletrabalho.config_area', area=area, tipo_area=tipo_area))

            # Buscar config existente
            from app.models.teletrabalho import ConfigAreaTeletrabalho

            config = ConfigAreaTeletrabalho.query.filter_by(
                AREA=area,
                TIPO_AREA=tipo_area,
                DELETED_AT=None
            ).first()

            if config:
                # Atualizar
                config.QTD_TOTAL_PESSOAS = qtd_pessoas
                config.PERCENTUAL_LIMITE = percentual
                config.UPDATED_AT = datetime.utcnow()
                acao = 'atualizada'
            else:
                # Criar
                config = ConfigAreaTeletrabalho(
                    AREA=area,
                    TIPO_AREA=tipo_area,
                    QTD_TOTAL_PESSOAS=qtd_pessoas,
                    PERCENTUAL_LIMITE=percentual,
                    GESTOR_ID=current_user.id
                )
                db.session.add(config)
                acao = 'criada'

            registrar_log(
                acao='editar' if acao == 'atualizada' else 'criar',
                entidade='config_area_teletrabalho',
                entidade_id=config.ID if config.ID else 0,
                descricao=f'Configuração da área {area} {acao}: {qtd_pessoas} pessoas, {percentual}%'
            )

            db.session.commit()
            flash(f'Configuração {acao} com sucesso!', 'success')
            return redirect(url_for('teletrabalho.config_area', area=area, tipo_area=tipo_area))

        # GET - buscar config
        from app.models.teletrabalho import ConfigAreaTeletrabalho

        config = ConfigAreaTeletrabalho.query.filter_by(
            AREA=area,
            TIPO_AREA=tipo_area,
            DELETED_AT=None
        ).first()

        # Contar pessoas reais na área
        qtd_real = Empregado.query.filter(
            Empregado.sgSuperintendencia == area,
            Empregado.fkStatus == 1
        ).count()

        return render_template('teletrabalho/config_area.html',
                               config=config,
                               area=area,
                               tipo_area=tipo_area,
                               qtd_real=qtd_real,
                               areas_disponiveis=None,
                               mostrar_seletor=False)

    except Exception as e:
        db.session.rollback()
        flash(f'Erro: {str(e)}', 'danger')
        return redirect(url_for('teletrabalho.index'))


@teletrabalho_bp.route('/config/listar')
@login_required
def config_listar():
    """Lista todas as configurações de áreas"""
    if current_user.perfil != 'admin':
        flash('Acesso negado.', 'danger')
        return redirect(url_for('teletrabalho.index'))

    try:
        from app.models.teletrabalho import ConfigAreaTeletrabalho

        configs = ConfigAreaTeletrabalho.query.filter(
            ConfigAreaTeletrabalho.DELETED_AT.is_(None)
        ).order_by(ConfigAreaTeletrabalho.AREA).all()

        # Buscar áreas sem config
        areas_com_config = [c.AREA for c in configs]

        todas_areas = db.session.query(
            Empregado.sgSuperintendencia,
            func.count(Empregado.pkPessoa).label('qtd')
        ).filter(
            Empregado.sgSuperintendencia.isnot(None),
            Empregado.sgSuperintendencia != '',
            Empregado.fkStatus == 1
        ).group_by(Empregado.sgSuperintendencia).all()

        areas_sem_config = [
            {'area': area, 'qtd': qtd}
            for area, qtd in todas_areas
            if area not in areas_com_config
        ]

        return render_template('teletrabalho/config_listar.html',
                               configs=configs,
                               areas_sem_config=areas_sem_config)

    except Exception as e:
        flash(f'Erro: {str(e)}', 'danger')
        return redirect(url_for('teletrabalho.index'))


@teletrabalho_bp.route('/config/excluir/<int:id>', methods=['POST'])
@login_required
def config_excluir(id):
    """Excluir configuração de área"""
    if current_user.perfil != 'admin':
        flash('Acesso negado.', 'danger')
        return redirect(url_for('teletrabalho.index'))

    try:
        from app.models.teletrabalho import ConfigAreaTeletrabalho

        config = ConfigAreaTeletrabalho.query.get_or_404(id)
        config.DELETED_AT = datetime.utcnow()

        registrar_log(
            acao='excluir',
            entidade='config_area_teletrabalho',
            entidade_id=id,
            descricao=f'Configuração da área {config.AREA} excluída'
        )

        db.session.commit()
        flash('Configuração excluída.', 'success')

    except Exception as e:
        db.session.rollback()
        flash(f'Erro: {str(e)}', 'danger')

    return redirect(url_for('teletrabalho.config_listar'))