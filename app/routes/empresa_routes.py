from flask import Blueprint, render_template, redirect, url_for, request, flash
from app.models.empresa_participante import EmpresaParticipante
from app.models.empresa_responsavel import EmpresaResponsavel  # Modelo da tabela externa
from app.models.periodo import PeriodoAvaliacao
from app.models.edital import Edital
from app import db
from datetime import datetime
from flask_login import login_required
from app.utils.audit import registrar_log

empresa_bp = Blueprint('empresa', __name__, url_prefix='/credenciamento')


@empresa_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@empresa_bp.route('/periodos/<int:periodo_id>/empresas')
@login_required
def lista_empresas(periodo_id):
    periodo = PeriodoAvaliacao.query.get_or_404(periodo_id)

    # Join com EmpresaResponsavel para buscar informações completas
    empresas = db.session.query(
        EmpresaParticipante,
        EmpresaResponsavel
    ).outerjoin(
        EmpresaResponsavel,
        EmpresaParticipante.ID_EMPRESA == EmpresaResponsavel.pkEmpresaResponsavelCobranca
    ).filter(
        EmpresaParticipante.ID_PERIODO == periodo.ID_PERIODO,  # Usa o ID_PERIODO em vez do ID da tabela
        EmpresaParticipante.DELETED_AT == None
    ).all()

    return render_template('credenciamento/lista_empresas.html',
                           periodo=periodo,
                           empresas=empresas)


@empresa_bp.route('/periodos/<int:periodo_id>/empresas/nova', methods=['GET', 'POST'])
@login_required
def nova_empresa(periodo_id):
    periodo = PeriodoAvaliacao.query.get_or_404(periodo_id)
    edital = Edital.query.get(periodo.ID_EDITAL)

    # Obter todas as empresas disponíveis para o dropdown
    try:
        empresas_responsaveis = EmpresaResponsavel.query.order_by(EmpresaResponsavel.nmEmpresaResponsavelCobranca).all()

        # Para debug - adicione este código temporariamente
        print(f"Total de empresas encontradas: {len(empresas_responsaveis)}")
        for empresa in empresas_responsaveis[:5]:  # Mostrar apenas as 5 primeiras para não encher o log
            print(f"Empresa: {empresa.pkEmpresaResponsavelCobranca} - {empresa.nmEmpresaResponsavelCobranca}")

        if not empresas_responsaveis:
            print("AVISO: Nenhuma empresa responsável encontrada na tabela!")
            # Tente verificar se a tabela existe e tem dados
            from sqlalchemy import text
            with db.engine.connect() as connection:
                result = connection.execute(text("SELECT COUNT(*) FROM BDG.PAR_TB002_EMPRESA_RESPONSAVEL_COBRANCA"))
                count = result.scalar()
                print(f"Total de registros na tabela: {count}")
    except Exception as e:
        print(f"ERRO ao buscar empresas responsáveis: {str(e)}")
        empresas_responsaveis = []
        flash(f"Erro ao carregar empresas responsáveis: {str(e)}", "danger")

    # Lista de opções para o campo condição
    condicoes = ["NOVA", "PERMANECE", "DESCREDENCIADA", "DESCREDENCIADA NO PERÍODO"]

    if request.method == 'POST':
        try:
            # Obter dados do formulário
            id_empresa = request.form['id_empresa']
            ds_condicao = request.form.get('ds_condicao', '')
            dt_descredenciamento = request.form.get('dt_descredenciamento', '')

            # Validação da data de descredenciamento
            data_descredenciamento = None
            if ds_condicao == 'DESCREDENCIADA NO PERÍODO':
                if not dt_descredenciamento:
                    flash(
                        'Para empresas com condição "DESCREDENCIADA NO PERÍODO" é obrigatório informar a data de descredenciamento.',
                        'danger')
                    return render_template('credenciamento/form_empresa.html',
                                           periodo=periodo,
                                           edital=edital,
                                           empresas=empresas_responsaveis,
                                           condicoes=condicoes)

                # Converter string para date
                try:
                    data_descredenciamento = datetime.strptime(dt_descredenciamento, '%Y-%m-%d').date()

                    # Validar se a data está dentro do período
                    if not (periodo.DT_INICIO <= data_descredenciamento <= periodo.DT_FIM):
                        flash(
                            f'A data de descredenciamento deve estar entre {periodo.DT_INICIO.strftime("%d/%m/%Y")} e {periodo.DT_FIM.strftime("%d/%m/%Y")}.',
                            'danger')
                        return render_template('credenciamento/form_empresa.html',
                                               periodo=periodo,
                                               edital=edital,
                                               empresas=empresas_responsaveis,
                                               condicoes=condicoes)
                except ValueError:
                    flash('Data de descredenciamento inválida.', 'danger')
                    return render_template('credenciamento/form_empresa.html',
                                           periodo=periodo,
                                           edital=edital,
                                           empresas=empresas_responsaveis,
                                           condicoes=condicoes)

            # Buscar empresa responsável
            empresa_responsavel = EmpresaResponsavel.query.get(id_empresa)
            if not empresa_responsavel:
                flash(f'Empresa com ID {id_empresa} não encontrada.', 'danger')
                return render_template('credenciamento/form_empresa.html',
                                       periodo=periodo,
                                       edital=edital,
                                       empresas=empresas_responsaveis,
                                       condicoes=condicoes)

            # Verificar se já existe empresa com este ID neste período
            empresa_existente = EmpresaParticipante.query.filter_by(
                ID_PERIODO=periodo.ID_PERIODO,  # Usa o ID_PERIODO em vez do ID da tabela
                ID_EMPRESA=id_empresa,
                DELETED_AT=None
            ).first()

            if empresa_existente:
                flash(f'Empresa já cadastrada para este período.', 'danger')
                return render_template('credenciamento/form_empresa.html',
                                       periodo=periodo,
                                       edital=edital,
                                       empresas=empresas_responsaveis,
                                       condicoes=condicoes)

            nova_empresa = EmpresaParticipante(
                ID_EDITAL=edital.ID,
                ID_PERIODO=periodo.ID_PERIODO,  # Usa o ID_PERIODO em vez do ID da tabela
                ID_EMPRESA=empresa_responsavel.pkEmpresaResponsavelCobranca,
                NO_EMPRESA=empresa_responsavel.nmEmpresaResponsavelCobranca,
                NO_EMPRESA_ABREVIADA=empresa_responsavel.NO_ABREVIADO_EMPRESA,
                DS_CONDICAO=ds_condicao,
                DT_DESCREDENCIAMENTO=data_descredenciamento  # Nova coluna
            )

            db.session.add(nova_empresa)
            db.session.commit()

            # Registrar log de auditoria
            dados_novos = {
                'id_edital': edital.ID,
                'id_periodo': periodo.ID_PERIODO,  # Usa o ID_PERIODO em vez do ID da tabela
                'id_empresa': empresa_responsavel.pkEmpresaResponsavelCobranca,
                'no_empresa': empresa_responsavel.nmEmpresaResponsavelCobranca,
                'no_empresa_abreviada': empresa_responsavel.NO_ABREVIADO_EMPRESA,
                'ds_condicao': ds_condicao,
                'dt_descredenciamento': data_descredenciamento.strftime('%Y-%m-%d') if data_descredenciamento else None
            }
            registrar_log(
                acao='criar',
                entidade='empresa',
                entidade_id=nova_empresa.ID,
                descricao=f'Cadastro da empresa {empresa_responsavel.nmEmpresaResponsavelCobranca} no período {periodo.ID_PERIODO}',
                dados_novos=dados_novos
            )

            # Mensagem de sucesso com informação sobre descredenciamento se aplicável
            if data_descredenciamento:
                flash(
                    f'Empresa cadastrada com sucesso! Data de descredenciamento: {data_descredenciamento.strftime("%d/%m/%Y")}',
                    'success')
            else:
                flash('Empresa cadastrada com sucesso!', 'success')

            return redirect(url_for('empresa.lista_empresas', periodo_id=periodo_id))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')

    return render_template('credenciamento/form_empresa.html',
                           periodo=periodo,
                           edital=edital,
                           empresas=empresas_responsaveis,
                           condicoes=condicoes)


@empresa_bp.route('/empresas/editar/<int:id>', methods=['GET', 'POST'])
@login_required
def editar_empresa(id):
    empresa = EmpresaParticipante.query.get_or_404(id)
    periodo = PeriodoAvaliacao.query.filter_by(ID_PERIODO=empresa.ID_PERIODO).first_or_404()
    edital = Edital.query.get(periodo.ID_EDITAL)

    # Lista de opções para o campo condição
    condicoes = ["NOVA", "PERMANECE", "DESCREDENCIADA", "DESCREDENCIADA NO PERÍODO"]

    if request.method == 'POST':
        try:
            # Capturar dados antigos para auditoria
            dados_antigos = {
                'id_empresa': empresa.ID_EMPRESA,
                'no_empresa': empresa.NO_EMPRESA,
                'no_empresa_abreviada': empresa.NO_EMPRESA_ABREVIADA,
                'ds_condicao': empresa.DS_CONDICAO,
                'dt_descredenciamento': empresa.DT_DESCREDENCIAMENTO.strftime(
                    '%Y-%m-%d') if empresa.DT_DESCREDENCIAMENTO else None
            }

            # Obter dados do formulário
            nova_condicao = request.form.get('ds_condicao', '')
            dt_descredenciamento = request.form.get('dt_descredenciamento', '')

            # Validação da data de descredenciamento
            data_descredenciamento = None
            if nova_condicao == 'DESCREDENCIADA NO PERÍODO':
                if not dt_descredenciamento:
                    flash(
                        'Para empresas com condição "DESCREDENCIADA NO PERÍODO" é obrigatório informar a data de descredenciamento.',
                        'danger')
                    return render_template('credenciamento/form_empresa_editar.html',
                                           empresa=empresa,
                                           periodo=periodo,
                                           edital=edital,
                                           condicoes=condicoes)

                # Converter string para date
                try:
                    data_descredenciamento = datetime.strptime(dt_descredenciamento, '%Y-%m-%d').date()

                    # Validar se a data está dentro do período
                    if not (periodo.DT_INICIO <= data_descredenciamento <= periodo.DT_FIM):
                        flash(
                            f'A data de descredenciamento deve estar entre {periodo.DT_INICIO.strftime("%d/%m/%Y")} e {periodo.DT_FIM.strftime("%d/%m/%Y")}.',
                            'danger')
                        return render_template('credenciamento/form_empresa_editar.html',
                                               empresa=empresa,
                                               periodo=periodo,
                                               edital=edital,
                                               condicoes=condicoes)
                except ValueError:
                    flash('Data de descredenciamento inválida.', 'danger')
                    return render_template('credenciamento/form_empresa_editar.html',
                                           empresa=empresa,
                                           periodo=periodo,
                                           edital=edital,
                                           condicoes=condicoes)

            # Atualizar campos da empresa
            empresa.DS_CONDICAO = nova_condicao
            empresa.DT_DESCREDENCIAMENTO = data_descredenciamento

            # Dados para auditoria
            dados_novos = {
                'ds_condicao': nova_condicao,
                'dt_descredenciamento': data_descredenciamento.strftime('%Y-%m-%d') if data_descredenciamento else None
            }

            db.session.commit()

            # Registrar log de auditoria
            registrar_log(
                acao='editar',
                entidade='empresa',
                entidade_id=empresa.ID,
                descricao=f'Alteração da condição da empresa {empresa.NO_EMPRESA}',
                dados_antigos=dados_antigos,
                dados_novos=dados_novos
            )

            # Mensagem de sucesso com informação sobre descredenciamento se aplicável
            if data_descredenciamento:
                flash(
                    f'Empresa atualizada com sucesso! Data de descredenciamento: {data_descredenciamento.strftime("%d/%m/%Y")}',
                    'success')
            else:
                flash('Empresa atualizada com sucesso!', 'success')

            return redirect(url_for('empresa.lista_empresas', periodo_id=periodo.ID))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')

    return render_template('credenciamento/form_empresa_editar.html',
                           empresa=empresa,
                           periodo=periodo,
                           edital=edital,
                           condicoes=condicoes)


@empresa_bp.route('/empresas/excluir/<int:id>')
@login_required
def excluir_empresa(id):
    try:
        empresa = EmpresaParticipante.query.get_or_404(id)
        periodo_id = empresa.ID_PERIODO

        # Capturar dados para auditoria
        dados_antigos = {
            'no_empresa': empresa.NO_EMPRESA,
            'id_empresa': empresa.ID_EMPRESA,
            'ds_condicao': empresa.DS_CONDICAO,
            'dt_descredenciamento': empresa.DT_DESCREDENCIAMENTO.strftime(
                '%Y-%m-%d') if empresa.DT_DESCREDENCIAMENTO else None,
            'deleted_at': None
        }

        empresa.DELETED_AT = datetime.utcnow()
        db.session.commit()

        # Registrar log de auditoria
        dados_novos = {
            'deleted_at': empresa.DELETED_AT.strftime('%Y-%m-%d %H:%M:%S')
        }
        registrar_log(
            acao='excluir',
            entidade='empresa',
            entidade_id=empresa.ID,
            descricao=f'Exclusão da empresa {empresa.NO_EMPRESA}',
            dados_antigos=dados_antigos,
            dados_novos=dados_novos
        )

        flash('Empresa removida com sucesso!', 'warning')
    except Exception as e:
        db.session.rollback()
        flash(f'Erro: {str(e)}', 'danger')

    return redirect(url_for('empresa.lista_empresas', periodo_id=periodo_id))


@empresa_bp.route('/teste-empresas')
@login_required
def teste_empresas():
    try:
        from flask import jsonify
        empresas = EmpresaResponsavel.query.limit(10).all()
        resultado = [
            {
                'id': empresa.pkEmpresaResponsavelCobranca,
                'nome': empresa.nmEmpresaResponsavelCobranca,
                'abreviado': empresa.NO_ABREVIADO_EMPRESA
            }
            for empresa in empresas
        ]
        return jsonify({
            'sucesso': True,
            'mensagem': f'Encontradas {len(empresas)} empresas',
            'empresas': resultado
        })
    except Exception as e:
        return jsonify({
            'sucesso': False,
            'mensagem': f'Erro: {str(e)}',
            'erro_detalhado': repr(e)
        })