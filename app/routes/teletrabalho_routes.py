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


def verificar_nivel_acesso_teletrabalho(usuario):
    """
    Determina o nível de acesso do usuário no sistema de teletrabalho

    PRIORIDADE:
    1. GERENTE (cargo) → retorna 'gerente' (vê apenas sua área)
    2. ADMIN (perfil) → retorna 'admin' (vê tudo)
    3. MODERADOR (perfil, não gerente) → retorna 'moderador' (vê tudo)
    4. USUÁRIO → retorna 'usuario' (vê apenas próprios dados)

    IMPORTANTE: Gerente prevalece sobre moderador!
    Se alguém é gerente E moderador, será tratado como gerente.
    """
    # 1. PRIORIDADE: Verificar se é GERENTE (pelo cargo)
    if usuario.empregado and usuario.empregado.dsCargo:
        if 'GERENTE' in usuario.empregado.dsCargo.upper():
            return 'gerente'

    # 2. Se não é gerente, verificar perfil ADMIN
    if usuario.PERFIL == 'admin':
        return 'admin'

    # 3. Se não é gerente nem admin, verificar MODERADOR
    if usuario.PERFIL == 'moderador':
        return 'moderador'

    # 4. Usuário comum
    return 'usuario'


def eh_gestor_ou_admin(usuario):
    """Verifica se o usuário é gestor, admin ou moderador"""
    nivel = verificar_nivel_acesso_teletrabalho(usuario)
    return nivel in ['gerente', 'admin', 'moderador']


def pode_ver_todas_areas(usuario):
    """Verifica se o usuário pode ver todas as áreas (admin ou moderador não-gerente)"""
    nivel = verificar_nivel_acesso_teletrabalho(usuario)
    return nivel in ['admin', 'moderador']

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
        usuario = Usuario.query.get(current_user.id)

        # Verificar permissão
        if teletrabalho.USUARIO_ID != current_user.id and not eh_gestor_ou_admin(usuario):
            flash('Você não tem permissão para cancelar este teletrabalho.', 'danger')
            return redirect(url_for('teletrabalho.index'))

        # ✅ VALIDAÇÃO: Gerente só pode excluir da sua área
        nivel_acesso = verificar_nivel_acesso_teletrabalho(usuario)

        if nivel_acesso == 'gerente':
            if not usuario.empregado:
                flash('Usuário sem vínculo de empregado.', 'danger')
                return redirect(url_for('teletrabalho.index'))

            area_gerente = usuario.empregado.sgSuperintendencia

            if teletrabalho.AREA != area_gerente:
                flash('Você só pode cancelar teletrabalhos da sua área.', 'danger')
                return redirect(url_for('teletrabalho.index'))

        # Guardar informações para o log
        data_cancelada = teletrabalho.DATA_TELETRABALHO
        usuario_nome = teletrabalho.usuario.NOME if teletrabalho.usuario else 'Usuário desconhecido'

        # ✅ SOFT DELETE
        teletrabalho.DELETED_AT = datetime.utcnow()

        # ✅ COMMIT PRIMEIRO (antes do log)
        db.session.commit()

        # ✅ LOG DEPOIS DO COMMIT (para evitar rollback)
        try:
            registrar_log(
                acao='excluir',
                entidade='teletrabalho',
                entidade_id=id,
                descricao=f'{nivel_acesso.upper()} cancelou teletrabalho de {usuario_nome} em {data_cancelada.strftime("%d/%m/%Y")}'
            )
        except Exception as log_error:
            # Se falhar o log, não importa - o dado já foi salvo
            print(f"[AVISO] Erro ao registrar log (não afeta a operação): {log_error}")

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

    if not eh_gestor_ou_admin(usuario):
        flash('Acesso negado. Apenas administradores, moderadores e gerentes.', 'danger')
        return redirect(url_for('teletrabalho.index'))

    try:
        # Query base
        query = db.session.query(
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
        )

        # CORREÇÃO: Verificar nível de acesso
        # GERENTE (mesmo se moderador) vê apenas sua área
        # ADMIN e MODERADOR (não gerente) veem tudo
        if not pode_ver_todas_areas(usuario):
            # É gerente (mesmo se for moderador também)
            empregado_logado = usuario.empregado
            if empregado_logado and empregado_logado.sgSuperintendencia:
                query = query.filter(Empregado.sgSuperintendencia == empregado_logado.sgSuperintendencia)
            else:
                flash('Você não está vinculado a nenhuma superintendência.', 'warning')
                return redirect(url_for('teletrabalho.index'))

        usuarios = query.order_by(Usuario.NOME).all()

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


@teletrabalho_bp.route('/admin/sortear', methods=['GET'])
@login_required
def admin_sortear_form():
    """Formulário para sortear teletrabalho automaticamente"""
    usuario = Usuario.query.get(current_user.id)

    if not eh_gestor_ou_admin(usuario):
        flash('Acesso negado. Apenas gestores e admins.', 'danger')
        return redirect(url_for('teletrabalho.index'))

    try:
        # CORREÇÃO: Gerente vê apenas sua área, Admin/Moderador não-gerente veem tudo
        if pode_ver_todas_areas(usuario):
            # Admin ou Moderador (não gerente) - vê todas
            areas = db.session.query(
                Empregado.sgSuperintendencia,
                func.count(Empregado.pkPessoa).label('qtd')
            ).filter(
                Empregado.sgSuperintendencia.isnot(None),
                Empregado.fkStatus == 1
            ).group_by(Empregado.sgSuperintendencia).all()
        else:
            # Gerente (mesmo se for moderador também) - vê apenas sua área
            if usuario.empregado and usuario.empregado.sgSuperintendencia:
                area = usuario.empregado.sgSuperintendencia
                qtd = Empregado.query.filter(
                    Empregado.sgSuperintendencia == area,
                    Empregado.fkStatus == 1
                ).count()
                areas = [(area, qtd)]
            else:
                flash('Você não está vinculado a nenhuma área.', 'warning')
                return redirect(url_for('teletrabalho.index'))

        areas_disponiveis = [{'area': a[0], 'qtd': a[1]} for a in areas]
        periodos = gerar_opcoes_periodo()

        return render_template('teletrabalho/admin_sortear.html',
                               areas=areas_disponiveis,
                               periodos=periodos)

    except Exception as e:
        flash(f'Erro: {str(e)}', 'danger')
        return redirect(url_for('teletrabalho.index'))

@teletrabalho_bp.route('/admin/sortear', methods=['POST'])
@login_required
def admin_sortear_processar():
    """Processa o sorteio automático de teletrabalho"""
    usuario = Usuario.query.get(current_user.id)

    if not eh_gestor_ou_admin(usuario):
        flash('Acesso negado.', 'danger')
        return redirect(url_for('teletrabalho.index'))

    try:
        area = request.form.get('area')
        mes_referencia = request.form.get('mes_referencia')
        qtd_dias = int(request.form.get('qtd_dias', 3))

        if not area or not mes_referencia:
            flash('Preencha todos os campos.', 'warning')
            return redirect(url_for('teletrabalho.admin_sortear_form'))

        # Executar sorteio
        resultado = sortear_teletrabalho_automatico(area, mes_referencia, qtd_dias, usuario.ID)

        if resultado['sucesso']:
            flash(
                f"✅ Sorteio concluído! {resultado['total_sorteados']} pessoas com {qtd_dias} dias cada. Total: {resultado['total_dias']} dias.",
                'success')

            registrar_log(
                acao='criar',
                entidade='sorteio_teletrabalho',
                entidade_id=0,
                descricao=f'Sorteio automático: {area} - {mes_referencia} - {resultado["total_sorteados"]} pessoas'
            )
        else:
            flash(f"❌ Erro no sorteio: {resultado['erro']}", 'danger')

        return redirect(url_for('teletrabalho.admin_sortear_form'))

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao processar sorteio: {str(e)}', 'danger')
        return redirect(url_for('teletrabalho.admin_sortear_form'))


def sortear_teletrabalho_automatico(area, mes_referencia, qtd_dias_por_pessoa, admin_id):
    """
    Sorteia teletrabalho com regras específicas por cargo

    REGRAS DEFINITIVAS:
    - NÃO-GERENTES: 5 dias alternados (SEG-QUA-SEX + TER-QUI em semanas diferentes)
    - GERENTES: 1-2 dias no mês (1 por semana, semanas diferentes)
    - NUNCA dias consecutivos
    - Respeita limite de 30% por dia
    - NÃO sorteia em dias de férias
    """
    import random
    from datetime import datetime, timedelta

    try:
        ano = int(mes_referencia[:4])
        mes = int(mes_referencia[4:])

        # 1. BUSCAR E SEPARAR PESSOAS POR CARGO
        usuarios_query = db.session.query(
            Usuario.ID,
            Usuario.NOME,
            Empregado.dsCargo
        ).join(
            Empregado, Usuario.FK_PESSOA == Empregado.pkPessoa
        ).filter(
            Usuario.ATIVO == True,
            Usuario.DELETED_AT.is_(None),
            Empregado.fkStatus == 1,
            Empregado.sgSuperintendencia == area,
            ~Empregado.dsCargo.ilike('%estagiário%'),
            ~Empregado.dsCargo.ilike('%estagiaria%'),
            ~Empregado.dsCargo.ilike('%superintendente%')
        ).all()

        if not usuarios_query:
            return {'sucesso': False, 'erro': 'Nenhuma pessoa elegível'}

        # ✅ BUSCAR FÉRIAS DE TODOS OS USUÁRIOS DA ÁREA NESTE MÊS
        print(f"[SORTEIO] Buscando férias do mês {mes_referencia} na área {area}...")

        ferias_mes = {}
        ferias_query = Teletrabalho.query.filter(
            Teletrabalho.MES_REFERENCIA == mes_referencia,
            Teletrabalho.AREA == area,
            Teletrabalho.TIPO_MARCACAO == 'FERIAS',
            Teletrabalho.DELETED_AT.is_(None)
        ).all()

        for f in ferias_query:
            if f.USUARIO_ID not in ferias_mes:
                ferias_mes[f.USUARIO_ID] = []
            ferias_mes[f.USUARIO_ID].append(f.DATA_TELETRABALHO)

        print(f"[SORTEIO] Encontradas {len(ferias_query)} férias marcadas para {len(ferias_mes)} pessoas")

        # Separar gerentes de não-gerentes
        gerentes = []
        nao_gerentes = []

        for u in usuarios_query:
            if u.dsCargo and 'GERENTE' in u.dsCargo.upper():
                gerentes.append(u)
            else:
                nao_gerentes.append(u)

        # 2. CALCULAR LIMITE DIÁRIO
        limite_diario, _ = calcular_limite_area(area, 'superintendencia')

        # 3. ORGANIZAR DIAS DO MÊS POR SEMANA
        primeiro_dia = date(ano, mes, 1)
        ultimo_dia = date(ano, mes, calendar.monthrange(ano, mes)[1])

        # Estrutura: {numero_semana: {dia_semana: [datas]}}
        semanas = {}
        data_atual = primeiro_dia

        while data_atual <= ultimo_dia:
            if Feriado.eh_dia_util(data_atual):
                num_semana = data_atual.isocalendar()[1]
                dia_semana = data_atual.weekday()  # 0=SEG, 1=TER, 2=QUA, 3=QUI, 4=SEX

                if num_semana not in semanas:
                    semanas[num_semana] = {0: [], 1: [], 2: [], 3: [], 4: []}

                semanas[num_semana][dia_semana].append(data_atual)

            data_atual += timedelta(days=1)

        if len(semanas) < 2:
            return {'sucesso': False, 'erro': 'Mês precisa ter pelo menos 2 semanas com dias úteis'}

        # 4. CONTROLE DE OCUPAÇÃO
        ocupacao_por_dia = {}
        data_atual = primeiro_dia
        while data_atual <= ultimo_dia:
            if Feriado.eh_dia_util(data_atual):
                ocupacao_por_dia[data_atual] = 0
            data_atual += timedelta(days=1)

        dias_por_usuario = {}
        avisos_ferias = []

        # 5. SORTEAR PARA NÃO-GERENTES (5 dias: 3+2 alternados)
        random.shuffle(nao_gerentes)

        for usuario in nao_gerentes:
            # ✅ PEGAR FÉRIAS DESTE USUÁRIO
            dias_ferias_usuario = ferias_mes.get(usuario.ID, [])

            # ✅ CALCULAR QUANTOS DIAS ÚTEIS DISPONÍVEIS (excluindo férias)
            dias_uteis_disponiveis = sum(
                1 for d in ocupacao_por_dia.keys()
                if d not in dias_ferias_usuario
            )

            # ✅ AJUSTAR QUANTIDADE ESPERADA
            if dias_uteis_disponiveis < 5:
                qtd_esperada = min(dias_uteis_disponiveis, 5)
                avisos_ferias.append(
                    f"{usuario.NOME}: {qtd_esperada} dias (férias: {len(dias_ferias_usuario)} dias)"
                )
            else:
                qtd_esperada = 5

            if qtd_esperada == 0:
                avisos_ferias.append(f"{usuario.NOME}: NENHUM dia (mês todo de férias)")
                continue

            # ✅ SORTEAR EXCLUINDO DIAS DE FÉRIAS
            dias_sorteados = sortear_5_dias_alternados(
                semanas,
                ocupacao_por_dia,
                limite_diario,
                excluir_datas=dias_ferias_usuario  # ✅ NOVO PARÂMETRO
            )

            if len(dias_sorteados) >= qtd_esperada:
                dias_por_usuario[usuario.ID] = dias_sorteados[:qtd_esperada]
                for dia in dias_sorteados[:qtd_esperada]:
                    ocupacao_por_dia[dia] += 1
            else:
                return {
                    'sucesso': False,
                    'erro': f'Não conseguiu sortear {qtd_esperada} dias para {usuario.NOME} (tem {len(dias_ferias_usuario)} dias de férias)'
                }

        # 6. SORTEAR PARA GERENTES (1-2 dias, semanas diferentes)
        random.shuffle(gerentes)

        for usuario in gerentes:
            # ✅ PEGAR FÉRIAS DESTE GERENTE
            dias_ferias_usuario = ferias_mes.get(usuario.ID, [])

            # ✅ CALCULAR DIAS DISPONÍVEIS
            dias_uteis_disponiveis = sum(
                1 for d in ocupacao_por_dia.keys()
                if d not in dias_ferias_usuario
            )

            # ✅ AJUSTAR QUANTIDADE (gerentes podem ter 1 ou 2 dias)
            if dias_uteis_disponiveis < 2:
                qtd_dias_gerente = max(1, dias_uteis_disponiveis)
                if qtd_dias_gerente > 0:
                    avisos_ferias.append(
                        f"{usuario.NOME} (Gerente): {qtd_dias_gerente} dia (férias: {len(dias_ferias_usuario)} dias)"
                    )
            else:
                qtd_dias_gerente = 2

            if qtd_dias_gerente == 0:
                avisos_ferias.append(f"{usuario.NOME} (Gerente): NENHUM dia (mês todo de férias)")
                continue

            # ✅ SORTEAR EXCLUINDO DIAS DE FÉRIAS
            dias_sorteados = sortear_dias_gerente(
                semanas,
                ocupacao_por_dia,
                limite_diario,
                qtd_dias_gerente,
                excluir_datas=dias_ferias_usuario  # ✅ NOVO PARÂMETRO
            )

            if len(dias_sorteados) >= qtd_dias_gerente:
                dias_por_usuario[usuario.ID] = dias_sorteados[:qtd_dias_gerente]
                for dia in dias_sorteados[:qtd_dias_gerente]:
                    ocupacao_por_dia[dia] += 1
            else:
                return {
                    'sucesso': False,
                    'erro': f'Não conseguiu sortear dias para gerente {usuario.NOME} (tem {len(dias_ferias_usuario)} dias de férias)'
                }

        # 7. SALVAR NO BANCO
        total_dias_cadastrados = 0

        for usuario_id, dias in dias_por_usuario.items():
            for dia in dias:
                teletrabalho = Teletrabalho(
                    USUARIO_ID=usuario_id,
                    DATA_TELETRABALHO=dia,
                    MES_REFERENCIA=mes_referencia,
                    AREA=area,
                    TIPO_AREA='superintendencia',
                    STATUS='APROVADO',
                    TIPO_PERIODO='ALTERNADO',
                    TIPO_MARCACAO='TELETRABALHO',  # ✅ IMPORTANTE: Marcar como TELETRABALHO
                    OBSERVACAO=f'[SORTEIO AUTO] {datetime.now().strftime("%d/%m/%Y %H:%M")}',
                    APROVADO_POR=admin_id,
                    APROVADO_EM=datetime.utcnow()
                )
                db.session.add(teletrabalho)
                total_dias_cadastrados += 1

        db.session.commit()

        # ✅ INCLUIR AVISOS DE FÉRIAS NO RESULTADO
        return {
            'sucesso': True,
            'total_sorteados': len(dias_por_usuario),
            'total_dias': total_dias_cadastrados,
            'gerentes': len(gerentes),
            'nao_gerentes': len(nao_gerentes),
            'avisos_ferias': avisos_ferias  # ✅ NOVO
        }

    except Exception as e:
        db.session.rollback()
        print(f"[SORTEIO] ERRO: {e}")
        import traceback
        traceback.print_exc()
        return {'sucesso': False, 'erro': str(e)}


def sortear_5_dias_alternados(semanas, ocupacao, limite, excluir_datas=None):
    """
    Sorteia 5 dias alternados para não-gerentes
    Padrão: SEG-QUA-SEX (semana 1) + TER-QUI (semana 2)

    :param excluir_datas: Lista de datas (férias) que devem ser excluídas do sorteio
    """
    import random

    if excluir_datas is None:
        excluir_datas = []

    # Padrões possíveis
    padroes = [
        {'semana1': [0, 2, 4], 'semana2': [1, 3]},  # SEG-QUA-SEX + TER-QUI
        {'semana1': [1, 3], 'semana2': [0, 2, 4]}  # TER-QUI + SEG-QUA-SEX
    ]

    random.shuffle(padroes)

    for padrao in padroes:
        dias_sorteados = []
        semanas_usadas = []

        # Sortear semana 1
        semanas_disponiveis = [s for s in semanas.keys() if s not in semanas_usadas]
        random.shuffle(semanas_disponiveis)

        for num_semana in semanas_disponiveis:
            dias_semana1 = []
            valido = True

            for dia_semana in padrao['semana1']:
                if dia_semana in semanas[num_semana] and semanas[num_semana][dia_semana]:
                    candidatos = [
                        d for d in semanas[num_semana][dia_semana]
                        if ocupacao.get(d, 0) < limite
                           and d not in excluir_datas  # ✅ EXCLUIR FÉRIAS
                    ]
                    if candidatos:
                        dias_semana1.append(candidatos[0])
                    else:
                        valido = False
                        break
                else:
                    valido = False
                    break

            if valido and len(dias_semana1) == len(padrao['semana1']):
                dias_sorteados.extend(dias_semana1)
                semanas_usadas.append(num_semana)
                break

        if len(dias_sorteados) != len(padrao['semana1']):
            continue

        # Sortear semana 2
        semanas_disponiveis = [s for s in semanas.keys() if s not in semanas_usadas]
        random.shuffle(semanas_disponiveis)

        for num_semana in semanas_disponiveis:
            dias_semana2 = []
            valido = True

            for dia_semana in padrao['semana2']:
                if dia_semana in semanas[num_semana] and semanas[num_semana][dia_semana]:
                    candidatos = [
                        d for d in semanas[num_semana][dia_semana]
                        if ocupacao.get(d, 0) < limite
                           and d not in excluir_datas  # ✅ EXCLUIR FÉRIAS
                    ]
                    if candidatos:
                        dias_semana2.append(candidatos[0])
                    else:
                        valido = False
                        break
                else:
                    valido = False
                    break

            if valido and len(dias_semana2) == len(padrao['semana2']):
                dias_sorteados.extend(dias_semana2)
                semanas_usadas.append(num_semana)
                break

        # Verificar se conseguiu 5 dias
        if len(dias_sorteados) == 5:
            dias_ordenados = sorted(dias_sorteados)
            tem_consecutivo = False
            for i in range(len(dias_ordenados) - 1):
                if (dias_ordenados[i + 1] - dias_ordenados[i]).days == 1:
                    tem_consecutivo = True
                    break

            if not tem_consecutivo:
                return dias_sorteados

    return []


def sortear_dias_gerente(semanas, ocupacao, limite, qtd_dias, excluir_datas=None):
    """
    Sorteia 1 ou 2 dias para gerentes
    Regra: 1 dia por semana, semanas diferentes

    :param excluir_datas: Lista de datas (férias) que devem ser excluídas do sorteio
    """
    import random

    if excluir_datas is None:
        excluir_datas = []

    semanas_disponiveis = list(semanas.keys())
    random.shuffle(semanas_disponiveis)

    dias_sorteados = []
    semanas_usadas = []

    tentativas = 0
    max_tentativas = 50

    while len(dias_sorteados) < qtd_dias and tentativas < max_tentativas:
        tentativas += 1

        semanas_candidatas = [s for s in semanas_disponiveis if s not in semanas_usadas]
        if not semanas_candidatas:
            break

        num_semana = random.choice(semanas_candidatas)

        dias_semana_disponiveis = []
        for dia_semana in [0, 1, 2, 3, 4]:  # SEG a SEX
            if dia_semana in semanas[num_semana] and semanas[num_semana][dia_semana]:
                for data in semanas[num_semana][dia_semana]:
                    if (ocupacao.get(data, 0) < limite
                            and data not in excluir_datas  # ✅ EXCLUIR FÉRIAS
                            and data not in dias_sorteados):
                        dias_semana_disponiveis.append(data)

        if dias_semana_disponiveis:
            dia_escolhido = random.choice(dias_semana_disponiveis)

            # Validar não-consecutivo
            eh_valido = True
            for dia_ja in dias_sorteados:
                if abs((dia_escolhido - dia_ja).days) == 1:
                    eh_valido = False
                    break

            if eh_valido:
                dias_sorteados.append(dia_escolhido)
                semanas_usadas.append(num_semana)

    return dias_sorteados

@teletrabalho_bp.route('/visualizacao-geral')
@login_required
def visualizacao_geral():
    """Visualização consolidada de teletrabalho da área"""
    try:
        usuario = Usuario.query.get(current_user.id)

        # Pegar parâmetros
        mes_ref = request.args.get('mes_ref')
        area_filtro = request.args.get('area')

        # Se não passou mês, usa o atual
        if not mes_ref:
            hoje = date.today()
            mes_ref = f"{hoje.year}{hoje.month:02d}"

        # DEFINIR ÁREA baseado no nível de acesso
        nivel_acesso = verificar_nivel_acesso_teletrabalho(usuario)

        if nivel_acesso == 'gerente':
            # Gerente vê apenas sua área
            if usuario.empregado and usuario.empregado.sgSuperintendencia:
                area = usuario.empregado.sgSuperintendencia
                pode_trocar_area = False
            else:
                flash('Você não está vinculado a nenhuma área.', 'warning')
                return redirect(url_for('teletrabalho.index'))

        elif nivel_acesso in ['admin', 'moderador']:
            # Admin/Moderador podem filtrar
            if area_filtro:
                area = area_filtro
            else:
                # Pegar primeira área disponível
                primeira_area = db.session.query(Empregado.sgSuperintendencia).filter(
                    Empregado.sgSuperintendencia.isnot(None),
                    Empregado.fkStatus == 1
                ).distinct().first()

                area = primeira_area[0] if primeira_area else None

            pode_trocar_area = True

        else:
            # Usuário comum vê sua própria área
            if usuario.empregado and usuario.empregado.sgSuperintendencia:
                area = usuario.empregado.sgSuperintendencia
                pode_trocar_area = False
            else:
                flash('Você não está vinculado a nenhuma área.', 'warning')
                return redirect(url_for('teletrabalho.index'))

        if not area:
            flash('Nenhuma área disponível.', 'warning')
            return redirect(url_for('teletrabalho.index'))

        # BUSCAR DADOS DO MÊS
        ano = int(mes_ref[:4])
        mes = int(mes_ref[4:])

        # Buscar todos os teletrabalhos aprovados da área neste mês
        teletrabalhos = Teletrabalho.query.join(
            Usuario, Teletrabalho.USUARIO_ID == Usuario.ID
        ).filter(
            Teletrabalho.MES_REFERENCIA == mes_ref,
            Teletrabalho.AREA == area,
            Teletrabalho.STATUS == 'APROVADO',
            Teletrabalho.DELETED_AT.is_(None)
        ).all()

        # ORGANIZAR POR DIA
        primeiro_dia = date(ano, mes, 1)
        ultimo_dia = date(ano, mes, calendar.monthrange(ano, mes)[1])

        dias_organizados = {}
        data_atual = primeiro_dia

        while data_atual <= ultimo_dia:
            dia_str = data_atual.strftime('%Y-%m-%d')

            # Informações do dia
            eh_util = Feriado.eh_dia_util(data_atual)
            feriado = Feriado.obter_feriado(data_atual)
            dia_semana = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo'][data_atual.weekday()]

            # Buscar pessoas neste dia
            pessoas_dia = []
            for t in teletrabalhos:
                if t.DATA_TELETRABALHO == data_atual:
                    usuario_tele = Usuario.query.get(t.USUARIO_ID)
                    if usuario_tele:
                        pessoas_dia.append({
                            'nome': usuario_tele.NOME,
                            'tipo_periodo': t.TIPO_PERIODO,
                            'observacao': t.OBSERVACAO
                        })

            dias_organizados[dia_str] = {
                'data': data_atual,
                'dia_semana': dia_semana,
                'eh_util': eh_util,
                'feriado': feriado.DS_FERIADO if feriado else None,
                'qtd_pessoas': len(pessoas_dia),
                'pessoas': pessoas_dia
            }

            data_atual += timedelta(days=1)

        # CALCULAR ESTATÍSTICAS
        total_dias_com_pessoas = sum(1 for d in dias_organizados.values() if d['qtd_pessoas'] > 0)
        pessoas_unicas = set()
        for t in teletrabalhos:
            pessoas_unicas.add(t.USUARIO_ID)

        # BUSCAR TODAS AS ÁREAS (para filtro de admin/moderador)
        if pode_trocar_area:
            areas_disponiveis = db.session.query(
                Empregado.sgSuperintendencia,
                func.count(Empregado.pkPessoa).label('qtd')
            ).filter(
                Empregado.sgSuperintendencia.isnot(None),
                Empregado.fkStatus == 1
            ).group_by(Empregado.sgSuperintendencia).all()
        else:
            areas_disponiveis = []

        # PERÍODOS DISPONÍVEIS
        periodos = gerar_opcoes_periodo()

        # Nome do mês
        meses_nomes = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
                       'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
        nome_mes = f"{meses_nomes[mes - 1]} de {ano}"

        return render_template('teletrabalho/visualizacao_geral.html',
                               dias_organizados=dias_organizados,
                               area=area,
                               mes_referencia=mes_ref,
                               nome_mes=nome_mes,
                               total_dias_com_pessoas=total_dias_com_pessoas,
                               total_pessoas_unicas=len(pessoas_unicas),
                               total_agendamentos=len(teletrabalhos),
                               pode_trocar_area=pode_trocar_area,
                               areas_disponiveis=areas_disponiveis,
                               periodos=periodos)

    except Exception as e:
        flash(f'Erro ao carregar visualização: {str(e)}', 'danger')
        return redirect(url_for('teletrabalho.index'))


@teletrabalho_bp.route('/calendario-visual')
@login_required
def calendario_visual():
    """Calendário visual com drag-and-drop para gestores"""
    try:
        usuario = Usuario.query.get(current_user.id)
        empregado = usuario.empregado

        # Obter filtros
        mes_ref = request.args.get('mes_ref')
        area_filtro = request.args.get('area')
        tipo_area_filtro = request.args.get('tipo_area', 'superintendencia')

        # Período padrão: mês atual
        if not mes_ref:
            hoje = date.today()
            mes_ref = hoje.strftime('%Y%m')

        # Área padrão
        if not area_filtro:
            if empregado:
                area_filtro = empregado.sgSuperintendencia
                tipo_area_filtro = 'superintendencia'
            elif current_user.perfil == 'admin':
                # Admin sem empregado - pegar primeira área disponível
                primeira_area = db.session.query(
                    Empregado.sgSuperintendencia
                ).filter(
                    Empregado.sgSuperintendencia.isnot(None),
                    Empregado.fkStatus == 1
                ).first()

                if primeira_area:
                    area_filtro = primeira_area[0]
                    tipo_area_filtro = 'superintendencia'

        if not area_filtro:
            flash('Área não identificada.', 'warning')
            return redirect(url_for('teletrabalho.index'))

        ano = int(mes_ref[:4])
        mes = int(mes_ref[4:])

        meses_pt = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
                    'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
        nome_mes = f"{meses_pt[mes - 1]} de {ano}"

        primeiro_dia = date(ano, mes, 1)
        ultimo_dia = date(ano, mes, calendar.monthrange(ano, mes)[1])
        primeiro_dia_grid = (primeiro_dia.weekday() + 1) % 7

        # Buscar teletrabalhos da área
        teletrabalhos = Teletrabalho.query.filter(
            Teletrabalho.MES_REFERENCIA == mes_ref,
            Teletrabalho.AREA == area_filtro,
            Teletrabalho.TIPO_AREA == tipo_area_filtro,
            Teletrabalho.STATUS == 'APROVADO',
            Teletrabalho.DELETED_AT.is_(None)
        ).all()

        # Organizar estrutura de dias do mês
        dias_organizados = {}
        data_atual = primeiro_dia

        while data_atual <= ultimo_dia:
            data_str = data_atual.strftime('%Y-%m-%d')
            dias_organizados[data_str] = {
                'data': data_atual,
                'dia': data_atual.day,
                'eh_util': Feriado.eh_dia_util(data_atual),
                'feriado': Feriado.obter_feriado(data_atual),
                'pessoas': []
            }
            data_atual += timedelta(days=1)

        # Adicionar pessoas aos dias
        for tele in teletrabalhos:
            data_str = tele.DATA_TELETRABALHO.strftime('%Y-%m-%d')
            if data_str in dias_organizados:
                nome_completo = tele.usuario.NOME if tele.usuario.NOME else 'Sem Nome'
                partes_nome = nome_completo.split()

                dias_organizados[data_str]['pessoas'].append({
                    'id': tele.ID,
                    'nome_completo': nome_completo,
                    'nome_primeiro': partes_nome[0] if partes_nome else '',
                    'nome_segundo': partes_nome[1] if len(partes_nome) > 1 else '',
                    'usuario_id': tele.USUARIO_ID,
                    'tipo_periodo': tele.TIPO_PERIODO,
                    'observacao': tele.OBSERVACAO
                })

        # Processar nomes para evitar duplicatas no mesmo dia
        for data_str, info in dias_organizados.items():
            if info['pessoas']:
                # Contar quantas vezes cada primeiro nome aparece
                contagem_nomes = {}
                for pessoa in info['pessoas']:
                    primeiro = pessoa['nome_primeiro']
                    contagem_nomes[primeiro] = contagem_nomes.get(primeiro, 0) + 1

                # Se nome repetido, adicionar inicial do sobrenome
                for pessoa in info['pessoas']:
                    primeiro = pessoa['nome_primeiro']
                    if contagem_nomes[primeiro] > 1 and pessoa['nome_segundo']:
                        pessoa['nome_exibicao'] = f"{primeiro} {pessoa['nome_segundo'][0]}."
                    else:
                        pessoa['nome_exibicao'] = primeiro

        # Calcular limite de pessoas por dia (30% da área)
        limite, total_pessoas = calcular_limite_area(area_filtro, tipo_area_filtro)

        # Dados para os filtros
        areas = obter_areas_disponiveis()
        periodos = gerar_opcoes_periodo()

        # ✅ CORREÇÃO CRÍTICA: Verificar corretamente se é gestor
        # A função eh_gestor_ou_admin() usa verificar_nivel_acesso_teletrabalho()
        # que PRIORIZA GERENTE sobre moderador
        eh_gestor = eh_gestor_ou_admin(usuario)

        return render_template('teletrabalho/calendario_visual.html',
                               dias_organizados=dias_organizados,
                               nome_mes=nome_mes,
                               mes_ref=mes_ref,
                               area=area_filtro,
                               tipo_area=tipo_area_filtro,
                               limite=limite,
                               total_pessoas=total_pessoas,
                               areas=areas,
                               periodos=periodos,
                               primeiro_dia_grid=primeiro_dia_grid,
                               eh_gestor=eh_gestor)  # ✅ Passa corretamente

    except Exception as e:
        flash(f'Erro ao carregar calendário: {str(e)}', 'danger')
        return redirect(url_for('teletrabalho.index'))

@teletrabalho_bp.route('/exportar-excel/<mes_ref>')
@login_required
def exportar_excel(mes_ref):
    """Exporta calendário para Excel com nomes abreviados"""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
        from flask import send_file
        import io

        area = request.args.get('area')
        tipo_area = request.args.get('tipo_area', 'superintendencia')

        ano = int(mes_ref[:4])
        mes = int(mes_ref[4:])

        meses_pt = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
                    'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
        nome_mes = f"{meses_pt[mes - 1]} {ano}"

        primeiro_dia = date(ano, mes, 1)
        ultimo_dia = date(ano, mes, calendar.monthrange(ano, mes)[1])

        # Buscar dados
        teletrabalhos = Teletrabalho.query.filter(
            Teletrabalho.MES_REFERENCIA == mes_ref,
            Teletrabalho.AREA == area,
            Teletrabalho.TIPO_AREA == tipo_area,
            Teletrabalho.STATUS == 'APROVADO',
            Teletrabalho.DELETED_AT.is_(None)
        ).all()

        # Organizar por dia com nomes abreviados
        dias_dados = {}
        data_atual = primeiro_dia

        while data_atual <= ultimo_dia:
            data_str = data_atual.strftime('%Y-%m-%d')
            dias_dados[data_str] = []
            data_atual += timedelta(days=1)

        # Adicionar pessoas
        for tele in teletrabalhos:
            data_str = tele.DATA_TELETRABALHO.strftime('%Y-%m-%d')
            if data_str in dias_dados:
                nome_completo = tele.usuario.NOME
                partes = nome_completo.split()

                if len(partes) >= 2:
                    nome_abreviado = f"{partes[0]} {partes[1][0]}."
                else:
                    nome_abreviado = partes[0] if partes else nome_completo

                dias_dados[data_str].append(nome_abreviado)

        # Processar nomes duplicados por dia
        for data_str, nomes in dias_dados.items():
            if len(nomes) > 1:
                # Contar primeiros nomes
                contagem = {}
                for nome in nomes:
                    primeiro = nome.split()[0]
                    contagem[primeiro] = contagem.get(primeiro, 0) + 1

                # Se houver duplicatas, já está com inicial do segundo nome
                # Então não precisa fazer nada, já vem como "João S."

        # Criar workbook
        wb = Workbook()
        ws = wb.active
        ws.title = f"Teletrabalho {nome_mes}"

        # Estilos
        header_fill = PatternFill(start_color="667eea", end_color="667eea", fill_type="solid")
        header_font = Font(bold=True, color="FFFFFF", size=12)
        center_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
        border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )

        # Cabeçalho principal
        ws.merge_cells('A1:G1')
        ws['A1'] = f"Calendário de Teletrabalho - {nome_mes}"
        ws['A1'].font = Font(bold=True, size=14, color="FFFFFF")
        ws['A1'].fill = PatternFill(start_color="667eea", end_color="667eea", fill_type="solid")
        ws['A1'].alignment = center_align

        ws.merge_cells('A2:G2')
        ws['A2'] = f"Área: {area}"
        ws['A2'].font = Font(size=11, bold=True)
        ws['A2'].fill = PatternFill(start_color="e3f2fd", end_color="e3f2fd", fill_type="solid")
        ws['A2'].alignment = center_align

        # Cabeçalho dos dias
        dias_semana = ['DOMINGO', 'SEGUNDA', 'TERÇA', 'QUARTA', 'QUINTA', 'SEXTA', 'SÁBADO']
        row = 4
        for col, dia in enumerate(dias_semana, start=1):
            cell = ws.cell(row=row, column=col, value=dia)
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = center_align
            cell.border = border
            ws.column_dimensions[chr(64 + col)].width = 18

        # Dados do calendário
        row = 5
        col = (primeiro_dia.weekday() + 1) % 7 + 1  # Posição inicial

        data_atual = primeiro_dia
        while data_atual <= ultimo_dia:
            if col > 7:
                col = 1
                row += 1

            data_str = data_atual.strftime('%Y-%m-%d')
            pessoas = dias_dados.get(data_str, [])

            # Conteúdo da célula
            conteudo = f"{data_atual.day}"
            if pessoas:
                conteudo += "\n\n" + "\n".join(pessoas)

            cell = ws.cell(row=row, column=col, value=conteudo)
            cell.alignment = Alignment(horizontal="center", vertical="top", wrap_text=True)
            cell.border = border
            cell.font = Font(size=10)

            # Número do dia em negrito
            if pessoas:
                cell.font = Font(size=10, bold=True)

            # Cor de fundo
            if not Feriado.eh_dia_util(data_atual):
                cell.fill = PatternFill(start_color="e0e0e0", end_color="e0e0e0", fill_type="solid")
                cell.font = Font(size=10, color="666666")
            elif pessoas:
                # Verde claro se tem pessoas
                cell.fill = PatternFill(start_color="d4edda", end_color="d4edda", fill_type="solid")
                cell.font = Font(size=10, bold=True, color="155724")
            else:
                # Branco se vazio
                cell.fill = PatternFill(start_color="ffffff", end_color="ffffff", fill_type="solid")

            col += 1
            data_atual += timedelta(days=1)

        # Ajustar altura das linhas
        for r in range(5, row + 1):
            ws.row_dimensions[r].height = 80

        # Rodapé com legenda
        row += 2
        ws.merge_cells(f'A{row}:G{row}')
        ws[f'A{row}'] = "Legenda: Verde = Com teletrabalho | Cinza = Fim de semana/Feriado"
        ws[f'A{row}'].font = Font(size=9, italic=True)
        ws[f'A{row}'].alignment = Alignment(horizontal="center")

        # Salvar em memória
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'Teletrabalho_{area}_{nome_mes.replace(" ", "_")}.xlsx'
        )

    except Exception as e:
        flash(f'Erro ao exportar: {str(e)}', 'danger')
        return redirect(url_for('teletrabalho.calendario_visual'))


@teletrabalho_bp.route('/mover-pessoa', methods=['POST'])
@login_required
def mover_pessoa():
    """Move pessoa de um dia para outro via drag-and-drop"""
    usuario = Usuario.query.get(current_user.id)

    # Apenas gestores podem mover
    if not eh_gestor_ou_admin(usuario):
        return jsonify({'erro': 'Sem permissão'}), 403

    try:
        data = request.get_json()
        teletrabalho_id = int(data.get('teletrabalho_id'))
        data_nova_str = data.get('data_nova')

        # Buscar teletrabalho
        teletrabalho = Teletrabalho.query.get_or_404(teletrabalho_id)

        # Validação de área para gerente
        nivel_acesso = verificar_nivel_acesso_teletrabalho(usuario)

        if nivel_acesso == 'gerente':
            if not usuario.empregado:
                return jsonify({'erro': 'Usuário sem vínculo de empregado'}), 403

            area_gerente = usuario.empregado.sgSuperintendencia

            if teletrabalho.AREA != area_gerente:
                return jsonify({'erro': 'Você só pode mover pessoas da sua área'}), 403

        data_antiga = teletrabalho.DATA_TELETRABALHO
        data_nova = datetime.strptime(data_nova_str, '%Y-%m-%d').date()

        # Validar se mudou
        if data_antiga == data_nova:
            return jsonify({'erro': 'Mesma data'}), 400

        # Validar dia útil
        if not Feriado.eh_dia_util(data_nova):
            feriado = Feriado.obter_feriado(data_nova)
            motivo = feriado.DS_FERIADO if feriado else "fim de semana"
            return jsonify({'erro': f'Data não é dia útil ({motivo})'}), 400

        # Validar mês
        mes_referencia = teletrabalho.MES_REFERENCIA
        if data_nova.strftime('%Y%m') != mes_referencia:
            return jsonify({'erro': 'Não pode mover para outro mês'}), 400

        # Validar limite de pessoas no dia
        area = teletrabalho.AREA
        tipo_area = teletrabalho.TIPO_AREA
        limite, _ = calcular_limite_area(area, tipo_area)

        qtd_no_dia = Teletrabalho.contar_pessoas_dia(data_nova, area, tipo_area)
        if qtd_no_dia >= limite:
            return jsonify({'erro': f'Dia lotado ({qtd_no_dia}/{limite})'}), 400

        # Validar 5 dias corridos (se for o tipo)
        if teletrabalho.TIPO_PERIODO == 'CINCO_DIAS_CORRIDOS':
            outros_dias = Teletrabalho.query.filter(
                Teletrabalho.USUARIO_ID == teletrabalho.USUARIO_ID,
                Teletrabalho.MES_REFERENCIA == mes_referencia,
                Teletrabalho.TIPO_PERIODO == 'CINCO_DIAS_CORRIDOS',
                Teletrabalho.STATUS == 'APROVADO',
                Teletrabalho.DELETED_AT.is_(None),
                Teletrabalho.ID != teletrabalho_id
            ).all()

            if len(outros_dias) == 4:
                todas_datas = [d.DATA_TELETRABALHO for d in outros_dias]
                todas_datas.append(data_nova)
                todas_datas.sort()

                consecutivos = True
                for i in range(len(todas_datas) - 1):
                    dias_entre = 0
                    data_check = todas_datas[i]
                    while data_check < todas_datas[i + 1]:
                        data_check += timedelta(days=1)
                        if Feriado.eh_dia_util(data_check):
                            dias_entre += 1

                    if dias_entre > 1:
                        consecutivos = False
                        break

                if not consecutivos:
                    return jsonify({'erro': '5 dias corridos devem ser consecutivos'}), 400

        # Validar dias alternados
        elif teletrabalho.TIPO_PERIODO == 'ALTERNADO':
            valido, msg = Teletrabalho.validar_dias_alternados(
                teletrabalho.USUARIO_ID,
                data_nova,
                mes_referencia,
                excluir_id=teletrabalho_id
            )
            if not valido:
                return jsonify({'erro': msg}), 400

        # ✅ ATUALIZAR DATA
        data_antiga_str = data_antiga.strftime('%d/%m/%Y')
        data_nova_str_br = data_nova.strftime('%d/%m/%Y')

        teletrabalho.DATA_TELETRABALHO = data_nova
        teletrabalho.UPDATED_AT = datetime.utcnow()

        # ✅ COMMIT PRIMEIRO
        db.session.commit()

        # ✅ LOG DE AUDITORIA DEPOIS DO COMMIT (para evitar rollback)
        try:
            registrar_log(
                acao='editar',
                entidade='teletrabalho',
                entidade_id=teletrabalho_id,
                descricao=f'{nivel_acesso.upper()} moveu {teletrabalho.usuario.NOME} de {data_antiga_str} para {data_nova_str_br}'
            )
        except Exception as log_error:
            # Se falhar o log, não importa - o dado já foi salvo
            print(f"[AVISO] Erro ao registrar log (não afeta a operação): {log_error}")

        return jsonify({
            'sucesso': True,
            'mensagem': f'Movido para {data_nova_str_br}'
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'erro': f'Erro: {str(e)}'}), 500


@teletrabalho_bp.route('/ferias', methods=['GET'])
@login_required
def marcar_ferias():
    """Página para marcar férias"""
    try:
        usuario = Usuario.query.get(current_user.id)
        empregado = usuario.empregado

        if not empregado:
            flash('Você precisa estar vinculado a um empregado.', 'warning')
            return redirect(url_for('teletrabalho.index'))

        periodos = gerar_opcoes_periodo()

        # Buscar férias já marcadas
        ferias_marcadas = Teletrabalho.query.filter(
            Teletrabalho.USUARIO_ID == current_user.id,
            Teletrabalho.TIPO_MARCACAO == 'FERIAS',
            Teletrabalho.DELETED_AT.is_(None)
        ).order_by(Teletrabalho.DATA_TELETRABALHO).all()

        return render_template('teletrabalho/marcar_ferias.html',
                               periodos=periodos,
                               ferias_marcadas=ferias_marcadas)

    except Exception as e:
        flash(f'Erro: {str(e)}', 'danger')
        return redirect(url_for('teletrabalho.index'))


@teletrabalho_bp.route('/ferias/calendario/<mes_referencia>', methods=['GET'])
@login_required
def ferias_calendario(mes_referencia):
    """Retorna dados do calendário para marcação de férias via AJAX"""
    try:
        usuario = Usuario.query.get(current_user.id)
        empregado = usuario.empregado

        if not empregado:
            return jsonify({'erro': 'Sem vínculo com empregado'}), 403

        ano = int(mes_referencia[:4])
        mes = int(mes_referencia[4:])

        primeiro_dia = date(ano, mes, 1)
        ultimo_dia = date(ano, mes, calendar.monthrange(ano, mes)[1])

        # Buscar férias já marcadas
        ferias_marcadas = Teletrabalho.query.filter(
            Teletrabalho.USUARIO_ID == current_user.id,
            Teletrabalho.MES_REFERENCIA == mes_referencia,
            Teletrabalho.TIPO_MARCACAO == 'FERIAS',
            Teletrabalho.DELETED_AT.is_(None)
        ).all()

        datas_ferias = [f.DATA_TELETRABALHO.strftime('%Y-%m-%d') for f in ferias_marcadas]

        # Buscar teletrabalhos já marcados
        teletrabalhos = Teletrabalho.query.filter(
            Teletrabalho.USUARIO_ID == current_user.id,
            Teletrabalho.MES_REFERENCIA == mes_referencia,
            Teletrabalho.TIPO_MARCACAO == 'TELETRABALHO',
            Teletrabalho.DELETED_AT.is_(None)
        ).all()

        datas_teletrabalho = [t.DATA_TELETRABALHO.strftime('%Y-%m-%d') for t in teletrabalhos]

        # Montar dias
        dias = []
        data_atual = primeiro_dia

        while data_atual <= ultimo_dia:
            data_str = data_atual.strftime('%Y-%m-%d')
            feriado_obj = Feriado.obter_feriado(data_atual)

            dias.append({
                'data': data_str,
                'dia': data_atual.day,
                'eh_util': Feriado.eh_dia_util(data_atual),
                'ja_marcado_ferias': data_str in datas_ferias,
                'ja_marcado_teletrabalho': data_str in datas_teletrabalho,
                'feriado': feriado_obj.DS_FERIADO if feriado_obj else None
            })

            data_atual += timedelta(days=1)

        return jsonify({
            'sucesso': True,
            'dias': dias,
            'primeiro_dia_grid': (primeiro_dia.weekday() + 1) % 7
        })

    except Exception as e:
        return jsonify({'erro': str(e)}), 500


@teletrabalho_bp.route('/ferias/salvar', methods=['POST'])
@login_required
def salvar_ferias():
    """Salva marcação de férias"""
    try:
        usuario = Usuario.query.get(current_user.id)
        empregado = usuario.empregado

        if not empregado:
            return jsonify({'erro': 'Sem vínculo com empregado'}), 403

        data = request.get_json()
        datas_str = data.get('datas', [])

        if not datas_str:
            return jsonify({'erro': 'Selecione pelo menos um dia'}), 400

        area = empregado.sgSuperintendencia
        tipo_area = 'superintendencia'

        datas = [datetime.strptime(d, '%Y-%m-%d').date() for d in datas_str]

        # Validar se já tem teletrabalho marcado
        teletrabalhos_conflito = Teletrabalho.query.filter(
            Teletrabalho.USUARIO_ID == current_user.id,
            Teletrabalho.DATA_TELETRABALHO.in_(datas),
            Teletrabalho.TIPO_MARCACAO == 'TELETRABALHO',
            Teletrabalho.DELETED_AT.is_(None)
        ).all()

        if teletrabalhos_conflito:
            datas_conflito = [t.DATA_TELETRABALHO.strftime('%d/%m/%Y') for t in teletrabalhos_conflito]
            return jsonify({
                'erro': f'Você já tem teletrabalho marcado em: {", ".join(datas_conflito)}'
            }), 400

        # Salvar férias
        for data_ferias in datas:
            # Verificar se já existe
            existe = Teletrabalho.query.filter(
                Teletrabalho.USUARIO_ID == current_user.id,
                Teletrabalho.DATA_TELETRABALHO == data_ferias,
                Teletrabalho.TIPO_MARCACAO == 'FERIAS',
                Teletrabalho.DELETED_AT.is_(None)
            ).first()

            if not existe:
                ferias = Teletrabalho(
                    USUARIO_ID=current_user.id,
                    DATA_TELETRABALHO=data_ferias,
                    MES_REFERENCIA=data_ferias.strftime('%Y%m'),
                    AREA=area,
                    TIPO_AREA=tipo_area,
                    STATUS='APROVADO',
                    TIPO_PERIODO='FERIAS',
                    TIPO_MARCACAO='FERIAS',  # ✅ IMPORTANTE
                    OBSERVACAO='Férias marcadas pelo usuário',
                    APROVADO_POR=current_user.id,
                    APROVADO_EM=datetime.utcnow()
                )
                db.session.add(ferias)

        db.session.commit()

        return jsonify({
            'sucesso': True,
            'mensagem': f'{len(datas)} dia(s) de férias marcado(s)!'
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'erro': str(e)}), 500


@teletrabalho_bp.route('/ferias/excluir/<int:id>', methods=['POST'])
@login_required
def excluir_ferias(id):
    """Exclui marcação de férias"""
    try:
        ferias = Teletrabalho.query.get_or_404(id)

        if ferias.USUARIO_ID != current_user.id:
            return jsonify({'erro': 'Sem permissão'}), 403

        if ferias.TIPO_MARCACAO != 'FERIAS':
            return jsonify({'erro': 'Não é registro de férias'}), 400

        ferias.DELETED_AT = datetime.utcnow()
        db.session.commit()

        return jsonify({
            'sucesso': True,
            'mensagem': 'Férias removidas!'
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'erro': str(e)}), 500


@teletrabalho_bp.route('/admin/marcar-ferias', methods=['GET'])
@login_required
def admin_marcar_ferias():
    """Página para gestor marcar férias de usuários"""
    usuario = Usuario.query.get(current_user.id)

    if not eh_gestor_ou_admin(usuario):
        flash('Acesso negado.', 'danger')
        return redirect(url_for('teletrabalho.index'))

    try:
        # Buscar usuários da área (similar ao admin_cadastrar)
        nivel_acesso = verificar_nivel_acesso_teletrabalho(usuario)

        if nivel_acesso in ['admin', 'moderador']:
            # Admin/Moderador vê todos
            usuarios_query = db.session.query(
                Usuario.ID,
                Usuario.NOME,
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

        else:  # Gerente
            if not usuario.empregado:
                flash('Você não está vinculado a nenhuma área.', 'warning')
                return redirect(url_for('teletrabalho.index'))

            area_gerente = usuario.empregado.sgSuperintendencia

            usuarios_query = db.session.query(
                Usuario.ID,
                Usuario.NOME,
                Empregado.sgSuperintendencia,
                Empregado.dsCargo
            ).join(
                Empregado, Usuario.FK_PESSOA == Empregado.pkPessoa
            ).filter(
                Usuario.ATIVO == True,
                Usuario.DELETED_AT.is_(None),
                Empregado.fkStatus == 1,
                Empregado.sgSuperintendencia == area_gerente
            ).order_by(Usuario.NOME).all()

        usuarios = [
            {
                'id': u.ID,
                'nome': u.NOME,
                'area': u.sgSuperintendencia,
                'cargo': u.dsCargo
            }
            for u in usuarios_query
        ]

        periodos = gerar_opcoes_periodo()

        return render_template('teletrabalho/admin_marcar_ferias.html',
                               usuarios=usuarios,
                               periodos=periodos)

    except Exception as e:
        flash(f'Erro: {str(e)}', 'danger')
        return redirect(url_for('teletrabalho.index'))


@teletrabalho_bp.route('/admin/marcar-ferias/calendario/<int:usuario_id>/<mes_referencia>', methods=['GET'])
@login_required
def admin_ferias_calendario(usuario_id, mes_referencia):
    """Retorna dados do calendário para gestor marcar férias de usuário"""
    usuario_logado = Usuario.query.get(current_user.id)

    if not eh_gestor_ou_admin(usuario_logado):
        return jsonify({'erro': 'Sem permissão'}), 403

    try:
        # Buscar usuário alvo
        usuario_alvo = Usuario.query.get_or_404(usuario_id)
        empregado = usuario_alvo.empregado

        if not empregado:
            return jsonify({'erro': 'Usuário sem vínculo com empregado'}), 404

        # Validar permissão de área (gerente só pode marcar da sua área)
        nivel_acesso = verificar_nivel_acesso_teletrabalho(usuario_logado)

        if nivel_acesso == 'gerente':
            if not usuario_logado.empregado:
                return jsonify({'erro': 'Sem vínculo com empregado'}), 403

            area_gerente = usuario_logado.empregado.sgSuperintendencia

            if empregado.sgSuperintendencia != area_gerente:
                return jsonify({'erro': 'Você só pode marcar férias de usuários da sua área'}), 403

        ano = int(mes_referencia[:4])
        mes = int(mes_referencia[4:])

        primeiro_dia = date(ano, mes, 1)
        ultimo_dia = date(ano, mes, calendar.monthrange(ano, mes)[1])

        # Buscar férias já marcadas
        ferias_marcadas = Teletrabalho.query.filter(
            Teletrabalho.USUARIO_ID == usuario_id,
            Teletrabalho.MES_REFERENCIA == mes_referencia,
            Teletrabalho.TIPO_MARCACAO == 'FERIAS',
            Teletrabalho.DELETED_AT.is_(None)
        ).all()

        datas_ferias = [f.DATA_TELETRABALHO.strftime('%Y-%m-%d') for f in ferias_marcadas]

        # Buscar teletrabalhos
        teletrabalhos = Teletrabalho.query.filter(
            Teletrabalho.USUARIO_ID == usuario_id,
            Teletrabalho.MES_REFERENCIA == mes_referencia,
            Teletrabalho.TIPO_MARCACAO == 'TELETRABALHO',
            Teletrabalho.DELETED_AT.is_(None)
        ).all()

        datas_teletrabalho = [t.DATA_TELETRABALHO.strftime('%Y-%m-%d') for t in teletrabalhos]

        # Montar dias
        dias = []
        data_atual = primeiro_dia

        while data_atual <= ultimo_dia:
            data_str = data_atual.strftime('%Y-%m-%d')
            feriado_obj = Feriado.obter_feriado(data_atual)

            dias.append({
                'data': data_str,
                'dia': data_atual.day,
                'eh_util': Feriado.eh_dia_util(data_atual),
                'ja_marcado_ferias': data_str in datas_ferias,
                'ja_marcado_teletrabalho': data_str in datas_teletrabalho,
                'feriado': feriado_obj.DS_FERIADO if feriado_obj else None
            })

            data_atual += timedelta(days=1)

        return jsonify({
            'sucesso': True,
            'dias': dias,
            'primeiro_dia_grid': (primeiro_dia.weekday() + 1) % 7,
            'usuario_nome': usuario_alvo.NOME,
            'area': empregado.sgSuperintendencia
        })

    except Exception as e:
        return jsonify({'erro': str(e)}), 500


@teletrabalho_bp.route('/admin/marcar-ferias/salvar', methods=['POST'])
@login_required
def admin_salvar_ferias():
    """Gestor salva férias de usuário"""
    usuario_logado = Usuario.query.get(current_user.id)

    if not eh_gestor_ou_admin(usuario_logado):
        return jsonify({'erro': 'Sem permissão'}), 403

    try:
        data = request.get_json()
        usuario_id = int(data.get('usuario_id'))
        datas_str = data.get('datas', [])

        if not datas_str:
            return jsonify({'erro': 'Selecione pelo menos um dia'}), 400

        # Buscar usuário alvo
        usuario_alvo = Usuario.query.get_or_404(usuario_id)
        empregado = usuario_alvo.empregado

        if not empregado:
            return jsonify({'erro': 'Usuário sem vínculo com empregado'}), 404

        # Validar permissão de área
        nivel_acesso = verificar_nivel_acesso_teletrabalho(usuario_logado)

        if nivel_acesso == 'gerente':
            if not usuario_logado.empregado:
                return jsonify({'erro': 'Sem vínculo com empregado'}), 403

            area_gerente = usuario_logado.empregado.sgSuperintendencia

            if empregado.sgSuperintendencia != area_gerente:
                return jsonify({'erro': 'Você só pode marcar férias de usuários da sua área'}), 403

        area = empregado.sgSuperintendencia
        tipo_area = 'superintendencia'

        datas = [datetime.strptime(d, '%Y-%m-%d').date() for d in datas_str]

        # Validar conflito com teletrabalho
        teletrabalhos_conflito = Teletrabalho.query.filter(
            Teletrabalho.USUARIO_ID == usuario_id,
            Teletrabalho.DATA_TELETRABALHO.in_(datas),
            Teletrabalho.TIPO_MARCACAO == 'TELETRABALHO',
            Teletrabalho.DELETED_AT.is_(None)
        ).all()

        if teletrabalhos_conflito:
            datas_conflito = [t.DATA_TELETRABALHO.strftime('%d/%m/%Y') for t in teletrabalhos_conflito]
            return jsonify({
                'erro': f'Usuário já tem teletrabalho marcado em: {", ".join(datas_conflito)}'
            }), 400

        # Salvar férias
        for data_ferias in datas:
            existe = Teletrabalho.query.filter(
                Teletrabalho.USUARIO_ID == usuario_id,
                Teletrabalho.DATA_TELETRABALHO == data_ferias,
                Teletrabalho.TIPO_MARCACAO == 'FERIAS',
                Teletrabalho.DELETED_AT.is_(None)
            ).first()

            if not existe:
                ferias = Teletrabalho(
                    USUARIO_ID=usuario_id,
                    DATA_TELETRABALHO=data_ferias,
                    MES_REFERENCIA=data_ferias.strftime('%Y%m'),
                    AREA=area,
                    TIPO_AREA=tipo_area,
                    STATUS='APROVADO',
                    TIPO_PERIODO='FERIAS',
                    TIPO_MARCACAO='FERIAS',
                    OBSERVACAO=f'[{nivel_acesso.upper()}] Férias marcadas por {usuario_logado.NOME}',
                    APROVADO_POR=current_user.id,
                    APROVADO_EM=datetime.utcnow()
                )
                db.session.add(ferias)

        db.session.commit()

        try:
            registrar_log(
                acao='criar',
                entidade='ferias',
                entidade_id=0,
                descricao=f'{nivel_acesso.upper()} marcou {len(datas)} dia(s) de férias para {usuario_alvo.NOME}'
            )
        except:
            pass

        return jsonify({
            'sucesso': True,
            'mensagem': f'{len(datas)} dia(s) de férias marcado(s) para {usuario_alvo.NOME}!'
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'erro': str(e)}), 500