from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify
from app.models.codigo_contabil import CodigoContabil
from app import db
from datetime import datetime
from flask_login import login_required
from app.utils.audit import registrar_log
import re

codigo_contabil_bp = Blueprint('codigo_contabil', __name__, url_prefix='/codigos-contabeis')


@codigo_contabil_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@codigo_contabil_bp.route('/')
@login_required
def index():
    """Dashboard principal dos códigos contábeis"""
    return render_template('codigos_contabeis/index.html')


@codigo_contabil_bp.route('/codigos')
@login_required
def lista_codigos():
    """Lista todos os códigos contábeis"""
    # Filtros
    ano_filtro = request.args.get('ano', type=int)
    quebra_filtro = request.args.get('quebra', '')

    query = CodigoContabil.query

    if ano_filtro:
        query = query.filter(CodigoContabil.ANO == ano_filtro)

    if quebra_filtro:
        query = query.filter(CodigoContabil.NO_QUEBRA == quebra_filtro)

    codigos = query.order_by(CodigoContabil.ANO.desc(), CodigoContabil.CODIGO.asc()).all()

    # Obter anos e quebras disponíveis para filtros
    anos_disponiveis = db.session.query(CodigoContabil.ANO).distinct().order_by(
        CodigoContabil.ANO.desc()
    ).all()
    anos_disponiveis = [ano[0] for ano in anos_disponiveis]

    quebras_disponiveis = CodigoContabil.obter_quebras_disponiveis()

    return render_template('codigos_contabeis/lista_codigos.html',
                           codigos=codigos,
                           anos_disponiveis=anos_disponiveis,
                           quebras_disponiveis=quebras_disponiveis,
                           ano_filtro=ano_filtro,
                           quebra_filtro=quebra_filtro)


@codigo_contabil_bp.route('/codigos/novo', methods=['GET', 'POST'])
@login_required
def novo_codigo():
    """Formulário para novo código contábil"""
    if request.method == 'POST':
        try:
            # Obter dados do formulário
            codigo = request.form['codigo'].strip()
            dsc_codigo = request.form['dsc_codigo'].strip()
            no_quebra = request.form.get('no_quebra', '').strip()
            ind_totalizacao = request.form.get('ind_totalizacao')
            ano = int(request.form['ano'])

            # Validar formato do código
            if not re.match(r'^\d{1,3}(\.\d{3})*(\.\d{1,3})?$', codigo):
                flash('Formato do código inválido. Use o formato: 1.100.000.000', 'danger')
                return render_template('codigos_contabeis/form_codigo.html',
                                       quebras_disponiveis=CodigoContabil.obter_quebras_disponiveis())

            # Verificar se código já existe para o mesmo ano
            codigo_existente = CodigoContabil.query.filter_by(
                CODIGO=codigo,
                ANO=ano
            ).first()

            if codigo_existente:
                flash(f'Código {codigo} já existe para o ano {ano}.', 'danger')
                return render_template('codigos_contabeis/form_codigo.html',
                                       quebras_disponiveis=CodigoContabil.obter_quebras_disponiveis())

            # Gerar COD_RUBRICA automaticamente
            cod_rubrica = CodigoContabil.gerar_cod_rubrica(codigo)

            # Tratar IND_TOTALIZACAO
            if ind_totalizacao == 'nenhum' or not ind_totalizacao:
                ind_totalizacao = None
            else:
                ind_totalizacao = int(ind_totalizacao)

            # Tratar NO_QUEBRA vazio
            if not no_quebra:
                no_quebra = None

            # Criar novo código
            novo_codigo = CodigoContabil(
                CODIGO=codigo,
                ANO=ano,
                DSC_CODIGO=dsc_codigo,
                NU_ORDEM=None,  # NULL por enquanto
                NO_QUEBRA=no_quebra,
                COD_RUBRICA=cod_rubrica,
                IND_TOTALIZACAO=ind_totalizacao
            )

            db.session.add(novo_codigo)
            db.session.commit()

            # Registrar log de auditoria
            dados_novos = {
                'codigo': codigo,
                'ano': ano,
                'dsc_codigo': dsc_codigo,
                'no_quebra': no_quebra,
                'cod_rubrica': cod_rubrica,
                'ind_totalizacao': ind_totalizacao
            }

            registrar_log(
                acao='criar',
                entidade='codigo_contabil',
                entidade_id=f"{codigo}-{ano}",
                descricao=f'Criação do código contábil {codigo} - {dsc_codigo}',
                dados_novos=dados_novos
            )

            flash('Código contábil cadastrado com sucesso!', 'success')
            return redirect(url_for('codigo_contabil.lista_codigos'))

        except ValueError as e:
            flash(f'Erro nos dados informados: {str(e)}', 'danger')
        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao cadastrar código: {str(e)}', 'danger')

    # GET - Mostrar formulário
    quebras_disponiveis = CodigoContabil.obter_quebras_disponiveis()
    ano_atual = datetime.now().year

    return render_template('codigos_contabeis/form_codigo.html',
                           quebras_disponiveis=quebras_disponiveis,
                           ano_atual=ano_atual)


@codigo_contabil_bp.route('/codigos/editar/<codigo>/<int:ano>', methods=['GET', 'POST'])
@login_required
def editar_codigo(codigo, ano):
    """Editar código contábil existente"""
    codigo_obj = CodigoContabil.query.filter_by(CODIGO=codigo, ANO=ano).first_or_404()

    if request.method == 'POST':
        try:
            # Capturar dados antigos para auditoria
            dados_antigos = {
                'codigo': codigo_obj.CODIGO,
                'ano': codigo_obj.ANO,
                'dsc_codigo': codigo_obj.DSC_CODIGO,
                'no_quebra': codigo_obj.NO_QUEBRA,
                'cod_rubrica': codigo_obj.COD_RUBRICA,
                'ind_totalizacao': codigo_obj.IND_TOTALIZACAO
            }

            # Obter novos dados
            novo_codigo = request.form['codigo'].strip()
            dsc_codigo = request.form['dsc_codigo'].strip()
            no_quebra = request.form.get('no_quebra', '').strip()
            ind_totalizacao = request.form.get('ind_totalizacao')
            novo_ano = int(request.form['ano'])

            # Validar formato do código
            if not re.match(r'^\d{1,3}(\.\d{3})*(\.\d{1,3})?$', novo_codigo):
                flash('Formato do código inválido. Use o formato: 1.100.000.000', 'danger')
                return render_template('codigos_contabeis/form_codigo.html',
                                       codigo=codigo_obj,
                                       quebras_disponiveis=CodigoContabil.obter_quebras_disponiveis())

            # Se mudou código ou ano, verificar se já existe
            if (novo_codigo != codigo_obj.CODIGO or novo_ano != codigo_obj.ANO):
                codigo_existente = CodigoContabil.query.filter_by(
                    CODIGO=novo_codigo,
                    ANO=novo_ano
                ).first()

                if codigo_existente:
                    flash(f'Código {novo_codigo} já existe para o ano {novo_ano}.', 'danger')
                    return render_template('codigos_contabeis/form_codigo.html',
                                           codigo=codigo_obj,
                                           quebras_disponiveis=CodigoContabil.obter_quebras_disponiveis())

            # Se mudou a chave primária, precisa deletar o antigo e criar novo
            if (novo_codigo != codigo_obj.CODIGO or novo_ano != codigo_obj.ANO):
                # Deletar registro antigo
                db.session.delete(codigo_obj)

                # Criar novo registro
                codigo_obj = CodigoContabil(
                    CODIGO=novo_codigo,
                    ANO=novo_ano,
                    DSC_CODIGO=dsc_codigo,
                    NU_ORDEM=None,
                    NO_QUEBRA=no_quebra if no_quebra else None,
                    COD_RUBRICA=CodigoContabil.gerar_cod_rubrica(novo_codigo),
                    IND_TOTALIZACAO=int(ind_totalizacao) if ind_totalizacao and ind_totalizacao != 'nenhum' else None
                )
                db.session.add(codigo_obj)
            else:
                # Apenas atualizar campos
                codigo_obj.DSC_CODIGO = dsc_codigo
                codigo_obj.NO_QUEBRA = no_quebra if no_quebra else None
                codigo_obj.COD_RUBRICA = CodigoContabil.gerar_cod_rubrica(novo_codigo)
                codigo_obj.IND_TOTALIZACAO = int(
                    ind_totalizacao) if ind_totalizacao and ind_totalizacao != 'nenhum' else None

            # Dados novos para auditoria
            dados_novos = {
                'codigo': codigo_obj.CODIGO,
                'ano': codigo_obj.ANO,
                'dsc_codigo': codigo_obj.DSC_CODIGO,
                'no_quebra': codigo_obj.NO_QUEBRA,
                'cod_rubrica': codigo_obj.COD_RUBRICA,
                'ind_totalizacao': codigo_obj.IND_TOTALIZACAO
            }

            db.session.commit()

            # Registrar log de auditoria
            registrar_log(
                acao='editar',
                entidade='codigo_contabil',
                entidade_id=f"{codigo_obj.CODIGO}-{codigo_obj.ANO}",
                descricao=f'Edição do código contábil {codigo_obj.CODIGO}',
                dados_antigos=dados_antigos,
                dados_novos=dados_novos
            )

            flash('Código contábil atualizado com sucesso!', 'success')
            return redirect(url_for('codigo_contabil.lista_codigos'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao atualizar código: {str(e)}', 'danger')

    # GET - Mostrar formulário de edição
    quebras_disponiveis = CodigoContabil.obter_quebras_disponiveis()

    return render_template('codigos_contabeis/form_codigo.html',
                           codigo=codigo_obj,
                           quebras_disponiveis=quebras_disponiveis)


@codigo_contabil_bp.route('/codigos/excluir/<codigo>/<int:ano>', methods=['POST'])
@login_required
def excluir_codigo(codigo, ano):
    """Excluir código contábil (delete físico)"""
    try:
        codigo_obj = CodigoContabil.query.filter_by(CODIGO=codigo, ANO=ano).first_or_404()

        # Capturar dados para auditoria
        dados_antigos = {
            'codigo': codigo_obj.CODIGO,
            'ano': codigo_obj.ANO,
            'dsc_codigo': codigo_obj.DSC_CODIGO
        }

        db.session.delete(codigo_obj)
        db.session.commit()

        # Registrar log de auditoria
        registrar_log(
            acao='excluir',
            entidade='codigo_contabil',
            entidade_id=f"{codigo}-{ano}",
            descricao=f'Exclusão do código contábil {codigo}',
            dados_antigos=dados_antigos
        )

        # Se a requisição for AJAX, retorna JSON
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({
                'success': True,
                'message': f'Código {codigo} excluído com sucesso!'
            })

        flash('Código contábil excluído com sucesso!', 'success')
        return redirect(url_for('codigo_contabil.lista_codigos'))

    except Exception as e:
        db.session.rollback()

        # Se a requisição for AJAX, retorna JSON com erro
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({
                'success': False,
                'message': f'Erro ao excluir: {str(e)}'
            })

        flash(f'Erro ao excluir código: {str(e)}', 'danger')
        return redirect(url_for('codigo_contabil.lista_codigos'))


@codigo_contabil_bp.route('/api/gerar-rubrica', methods=['POST'])
@login_required
def gerar_rubrica():
    """API para gerar COD_RUBRICA automaticamente baseado no CODIGO"""
    try:
        data = request.json
        codigo = data.get('codigo', '')

        if not codigo:
            return jsonify({'success': False, 'message': 'Código não informado'})

        cod_rubrica = CodigoContabil.gerar_cod_rubrica(codigo)

        return jsonify({
            'success': True,
            'cod_rubrica': cod_rubrica
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Erro ao gerar rubrica: {str(e)}'
        })