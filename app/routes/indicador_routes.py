from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from app import db
from app.models.indicador import IndicadorFormula, CodigoIndicador, VariavelIndicador
from app.utils.audit import registrar_log
from datetime import datetime, date
from sqlalchemy import extract, func, not_
from calendar import monthrange
from decimal import Decimal

indicador_bp = Blueprint('indicador', __name__)


@indicador_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.now().year}


@indicador_bp.route('/indicadores')
@login_required
def index():
    """Página principal do sistema de indicadores"""
    # Buscar todos os registros, exceto os de 'BDG', ordenados por data e indicador
    registros = IndicadorFormula.query.filter(
        not_(IndicadorFormula.RESPONSAVEL_INCLUSAO == 'BDG')
    ).order_by(
        IndicadorFormula.DT_REFERENCIA.desc(),
        IndicadorFormula.INDICADOR,
        IndicadorFormula.VARIAVEL
    ).all()

    # Agrupar registros por data e indicador para melhor visualização
    registros_agrupados = {}
    for reg in registros:
        chave = (reg.DT_REFERENCIA, reg.INDICADOR)
        if chave not in registros_agrupados:
            registros_agrupados[chave] = []
        registros_agrupados[chave].append(reg)

    return render_template('indicadores/index.html',
                           registros_agrupados=registros_agrupados)


@indicador_bp.route('/indicadores/novo', methods=['GET', 'POST'])
@login_required
def novo():
    """Formulário para inclusão de novos indicadores"""
    if request.method == 'POST':
        try:
            # Capturar dados do formulário
            mes = int(request.form.get('mes'))
            ano = int(request.form.get('ano'))
            indicador_sg = request.form.get('indicador')

            # Calcular último dia do mês
            ultimo_dia = monthrange(ano, mes)[1]
            dt_referencia = date(ano, mes, ultimo_dia)

            # Buscar o código do indicador selecionado
            codigo_indicador = CodigoIndicador.query.filter_by(
                SG_INDICADOR=indicador_sg
            ).first()

            if not codigo_indicador:
                flash('Indicador não encontrado.', 'danger')
                return redirect(url_for('indicador.novo'))

            # Buscar todas as variáveis deste indicador
            variaveis = VariavelIndicador.query.filter_by(
                CO_INDICADOR=codigo_indicador.CO_INDICADOR
            ).order_by(VariavelIndicador.VARIAVEL).all()

            # Verificar se já existe registro para esta data/indicador
            registro_existente = IndicadorFormula.query.filter_by(
                DT_REFERENCIA=dt_referencia,
                INDICADOR=indicador_sg
            ).first()

            if registro_existente:
                flash('Já existe registro para este indicador nesta data.', 'warning')
                return redirect(url_for('indicador.index'))

            # Definir tamanhos máximos baseados no banco
            MAX_INDICADOR = 18
            MAX_NO_VARIAVEL = 50  # Ajustado para um tamanho menor
            MAX_FONTE = 100  # Ajustado para um tamanho menor
            MAX_RESPONSAVEL = 100

            # Inserir um registro para cada variável
            for var in variaveis:
                valor_str = request.form.get(f'valor_{var.VARIAVEL}', '0')
                valor = Decimal(valor_str.replace(',', '.')) if valor_str else Decimal('0')

                # Truncar strings se necessário
                no_variavel = var.NO_VARIAVEL[:MAX_NO_VARIAVEL] if var.NO_VARIAVEL else ''
                fonte = var.FONTE[:MAX_FONTE] if var.FONTE else ''
                responsavel = current_user.nome[:MAX_RESPONSAVEL] if current_user.nome else ''

                novo_registro = IndicadorFormula(
                    DT_REFERENCIA=dt_referencia,
                    INDICADOR=indicador_sg[:MAX_INDICADOR],
                    VARIAVEL=var.VARIAVEL,
                    NO_VARIAVEL=no_variavel,
                    FONTE=fonte,
                    VR_VARIAVEL=valor,
                    RESPONSAVEL_INCLUSAO=responsavel
                )
                db.session.add(novo_registro)

            db.session.commit()

            # Registrar log
            registrar_log(
                acao='criar',
                entidade='indicador',
                entidade_id=None,
                descricao=f'Inclusão de indicador {indicador_sg} para {mes}/{ano}'
            )

            flash('Indicador incluído com sucesso!', 'success')
            return redirect(url_for('indicador.index'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao incluir indicador: {str(e)}', 'danger')
            return redirect(url_for('indicador.novo'))

    # GET - Carregar dados para o formulário
    indicadores = CodigoIndicador.query.order_by(CodigoIndicador.SG_INDICADOR).all()

    # Gerar lista de meses e anos para seleção
    ano_atual = datetime.now().year
    anos = list(range(ano_atual - 2, ano_atual + 2))
    meses = [
        (1, 'Janeiro'), (2, 'Fevereiro'), (3, 'Março'),
        (4, 'Abril'), (5, 'Maio'), (6, 'Junho'),
        (7, 'Julho'), (8, 'Agosto'), (9, 'Setembro'),
        (10, 'Outubro'), (11, 'Novembro'), (12, 'Dezembro')
    ]

    return render_template('indicadores/form.html',
                           indicadores=indicadores,
                           anos=anos,
                           meses=meses,
                           mes_atual=datetime.now().month,
                           ano_atual=ano_atual)


@indicador_bp.route('/indicadores/api/variaveis/<string:sg_indicador>')
@login_required
def get_variaveis(sg_indicador):
    """API para buscar variáveis de um indicador e verificar dados existentes."""
    # Buscar código do indicador
    codigo_indicador = CodigoIndicador.query.filter_by(
        SG_INDICADOR=sg_indicador
    ).first()

    if not codigo_indicador:
        return jsonify({'erro': 'Indicador não encontrado'}), 404

    # Obter mês e ano dos parâmetros da requisição
    mes = request.args.get('mes', type=int)
    ano = request.args.get('ano', type=int)

    # Buscar variáveis
    variaveis = VariavelIndicador.query.filter_by(
        CO_INDICADOR=codigo_indicador.CO_INDICADOR
    ).order_by(VariavelIndicador.VARIAVEL).all()

    # Verificar se já existem dados para este período
    dados_existentes = {}
    ja_existe = False
    if mes and ano:
        try:
            ultimo_dia = monthrange(ano, mes)[1]
            dt_referencia = date(ano, mes, ultimo_dia)

            registros_existentes = IndicadorFormula.query.filter_by(
                DT_REFERENCIA=dt_referencia,
                INDICADOR=sg_indicador
            ).all()

            if registros_existentes:
                ja_existe = True
                dados_existentes = {reg.VARIAVEL: reg.VR_VARIAVEL for reg in registros_existentes}
        except ValueError:
            # Ignora caso o mês/ano seja inválido
            pass

    # Formatar resposta
    variaveis_lista = []
    for var in variaveis:
        valor_existente = dados_existentes.get(var.VARIAVEL)
        variaveis_lista.append({
            'variavel': var.VARIAVEL,
            'nome': var.NO_VARIAVEL,
            'fonte': var.FONTE,
            'valor': f'{valor_existente:.2f}'.replace('.', ',') if valor_existente is not None else ''
        })

    return jsonify({
        'indicador': {
            'codigo': codigo_indicador.CO_INDICADOR,
            'sigla': codigo_indicador.SG_INDICADOR,
            'descricao': codigo_indicador.DSC_INDICADOR,
            'qtde_variaveis': codigo_indicador.QTDE_VARIAVEIS
        },
        'variaveis': variaveis_lista,
        'ja_existe': ja_existe
    })


@indicador_bp.route('/indicadores/editar/<string:dt_ref>/<string:indicador>')
@login_required
def editar(dt_ref, indicador):
    """Editar valores de um indicador"""
    try:
        # Converter string de data
        dt_referencia = datetime.strptime(dt_ref, '%Y-%m-%d').date()

        # Buscar registros
        registros = IndicadorFormula.query.filter_by(
            DT_REFERENCIA=dt_referencia,
            INDICADOR=indicador
        ).order_by(IndicadorFormula.VARIAVEL).all()

        if not registros:
            flash('Registros não encontrados.', 'warning')
            return redirect(url_for('indicador.index'))

        return render_template('indicadores/editar.html',
                               registros=registros,
                               dt_referencia=dt_referencia,
                               indicador=indicador)

    except Exception as e:
        flash(f'Erro ao carregar registros: {str(e)}', 'danger')
        return redirect(url_for('indicador.index'))


@indicador_bp.route('/indicadores/atualizar', methods=['POST'])
@login_required
def atualizar():
    """Atualizar valores de um indicador"""
    try:
        dt_ref = request.form.get('dt_referencia')
        indicador = request.form.get('indicador')

        # Converter string de data
        dt_referencia = datetime.strptime(dt_ref, '%Y-%m-%d').date()

        # Buscar registros existentes
        registros = IndicadorFormula.query.filter_by(
            DT_REFERENCIA=dt_referencia,
            INDICADOR=indicador
        ).all()

        # Definir tamanho máximo
        MAX_RESPONSAVEL = 100  # Limite da coluna no banco

        # Preparar nome do responsável
        nome_usuario = current_user.nome
        if len(nome_usuario) > MAX_RESPONSAVEL:
            primeiro_nome = nome_usuario.split()[0]
            if len(primeiro_nome) <= MAX_RESPONSAVEL:
                responsavel = primeiro_nome
            else:
                responsavel = nome_usuario[:MAX_RESPONSAVEL]
        else:
            responsavel = nome_usuario

        # Atualizar cada registro APENAS se o valor mudou
        valores_alterados = False
        for reg in registros:
            # Capturar valor original do formulário
            valor_original_str = request.form.get(f'valor_original_{reg.VARIAVEL}', '0')
            valor_original = Decimal(valor_original_str.replace(',', '.'))

            # Capturar novo valor
            valor_novo_str = request.form.get(f'valor_{reg.VARIAVEL}', '0')
            valor_novo = Decimal(valor_novo_str.replace(',', '.'))

            # Só atualizar se o valor mudou
            if valor_original != valor_novo:
                reg.VR_VARIAVEL = valor_novo
                reg.RESPONSAVEL_INCLUSAO = responsavel
                valores_alterados = True
            # Se o valor não mudou, mantém o RESPONSAVEL_INCLUSAO original

        db.session.commit()

        # Registrar log
        registrar_log(
            acao='editar',
            entidade='indicador',
            entidade_id=None,
            descricao=f'Atualização de valores do indicador {indicador} para {dt_ref}'
        )

        if valores_alterados:
            flash('Valores atualizados com sucesso!', 'success')
        else:
            flash('Nenhum valor foi alterado.', 'info')

        return redirect(url_for('indicador.index'))

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao atualizar valores: {str(e)}', 'danger')
        return redirect(url_for('indicador.index'))

@indicador_bp.route('/indicadores/excluir/<string:dt_ref>/<string:indicador>')
@login_required
def excluir(dt_ref, indicador):
    """Excluir todos os registros de um indicador em uma data"""
    try:
        # Converter string de data
        dt_referencia = datetime.strptime(dt_ref, '%Y-%m-%d').date()

        # Buscar e excluir registros
        registros = IndicadorFormula.query.filter_by(
            DT_REFERENCIA=dt_referencia,
            INDICADOR=indicador
        ).all()

        if not registros:
            flash('Registros não encontrados.', 'warning')
            return redirect(url_for('indicador.index'))

        # Excluir todos os registros
        for reg in registros:
            db.session.delete(reg)

        db.session.commit()

        # Registrar log
        registrar_log(
            acao='excluir',
            entidade='indicador',
            entidade_id=None,
            descricao=f'Exclusão do indicador {indicador} para {dt_ref}'
        )

        flash('Indicador excluído com sucesso!', 'success')

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao excluir indicador: {str(e)}', 'danger')

    return redirect(url_for('indicador.index'))