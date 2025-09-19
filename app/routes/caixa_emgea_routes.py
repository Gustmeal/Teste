from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from app import db
from app.models.caixa_emgea import CaixaEmgea
from app.models.pendencia_retencao import (
    PenOcorrencias, PenCarteiras, PenObservacoes, PenStatusOcorrencia
)
from app.utils.audit import registrar_log
from datetime import datetime
from decimal import Decimal

caixa_emgea_bp = Blueprint('caixa_emgea', __name__, url_prefix='/caixa-emgea')


@caixa_emgea_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@caixa_emgea_bp.route('/')
@login_required
def index():
    """Lista todos os registros de Caixa EMGEA"""
    registros = CaixaEmgea.obter_todos_ativos()

    # Buscar descrições para exibir na lista
    ocorrencias = {o.ID_OCORRENCIA: o.DSC_OCORRENCIA for o in PenOcorrencias.query.all()}
    status_list = {s.ID_STATUS: s.DSC_STATUS for s in PenStatusOcorrencia.query.all()}
    carteiras = {c.ID_CARTEIRA: c.DSC_CARTEIRA for c in PenCarteiras.query.all()}

    return render_template('caixa_emgea/index.html',
                           registros=registros,
                           ocorrencias=ocorrencias,
                           status_list=status_list,
                           carteiras=carteiras)


@caixa_emgea_bp.route('/novo', methods=['GET', 'POST'])
@login_required
def novo():
    """Formulário para novo registro"""
    if request.method == 'POST':
        try:
            # Capturar dados do formulário
            nu_contrato = request.form.get('nu_contrato', '').strip()
            valor = request.form.get('valor', '').strip()

            # Converter para os tipos corretos se não estiverem vazios
            nu_contrato_decimal = Decimal(nu_contrato) if nu_contrato else None
            valor_decimal = Decimal(valor) if valor else None

            # Verificar duplicidade se ambos foram informados
            if nu_contrato_decimal and valor_decimal:
                if CaixaEmgea.verificar_duplicidade(nu_contrato_decimal, valor_decimal):
                    confirmar = request.form.get('confirmar_duplicidade')
                    if not confirmar:
                        flash('Atenção: Já existe um registro com este Número de Contrato e Valor. '
                              'Marque a opção para confirmar a inclusão mesmo assim.', 'warning')
                        return render_template('caixa_emgea/form.html',
                                               dados=request.form,
                                               duplicidade=True,
                                               ocorrencias=PenOcorrencias.query.all(),
                                               status_list=PenStatusOcorrencia.query.all(),
                                               observacoes=PenObservacoes.query.all(),
                                               carteiras=PenCarteiras.query.all())

            # Criar novo registro
            novo_registro = CaixaEmgea(
                ID_OCORRENCIA=int(request.form.get('id_ocorrencia')) if request.form.get('id_ocorrencia') else None,
                NU_OFICIO=int(request.form.get('nu_oficio')) if request.form.get('nu_oficio') else None,
                DT_OFICIO=int(request.form.get('dt_oficio')) if request.form.get('dt_oficio') else None,
                EMITENTE=request.form.get('emitente', '').strip() or None,
                ID_STATUS=int(request.form.get('id_status')) if request.form.get('id_status') else None,
                NU_CONTRATO=nu_contrato_decimal,
                NU_PROCESSO=request.form.get('nu_processo', '').strip() or None,
                VALOR=valor_decimal,
                VR_REAL=Decimal(request.form.get('vr_real')) if request.form.get('vr_real') else None,
                DT_PAGTO=datetime.strptime(request.form.get('dt_pagto'), '%Y-%m-%d').date() if request.form.get(
                    'dt_pagto') else None,
                ID_OBSERVACAO=int(request.form.get('id_observacao')) if request.form.get('id_observacao') else None,
                ID_ESPECIFICACAO=int(request.form.get('id_especificacao')) if request.form.get(
                    'id_especificacao') else None,
                ID_CARTEIRA=int(request.form.get('id_carteira')) if request.form.get('id_carteira') else None,
                NR_TICKET=int(request.form.get('nr_ticket')) if request.form.get('nr_ticket') else None,
                DSC_DOCUMENTO=request.form.get('dsc_documento', '').strip() or None,
                VR_ISS=Decimal(request.form.get('vr_iss')) if request.form.get('vr_iss') else None,
                USUARIO_CRIACAO=current_user.nome  # CORRIGIDO: mudado de NOME para nome
            )

            db.session.add(novo_registro)
            db.session.commit()

            # Registrar log de auditoria
            registrar_log(
                acao='criar',
                entidade='caixa_emgea',
                entidade_id=novo_registro.ID,
                descricao=f'Novo registro Caixa EMGEA - Contrato: {nu_contrato or "N/A"}',
                dados_novos={'contrato': str(nu_contrato_decimal) if nu_contrato_decimal else None}
            )

            flash('Registro criado com sucesso!', 'success')
            return redirect(url_for('caixa_emgea.index'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao criar registro: {str(e)}', 'danger')

    # GET - Exibir formulário
    return render_template('caixa_emgea/form.html',
                           ocorrencias=PenOcorrencias.query.all(),
                           status_list=PenStatusOcorrencia.query.all(),
                           observacoes=PenObservacoes.query.all(),
                           carteiras=PenCarteiras.query.all())


@caixa_emgea_bp.route('/editar/<int:id>', methods=['GET', 'POST'])
@login_required
def editar(id):
    """Editar registro existente"""
    registro = CaixaEmgea.query.get_or_404(id)

    if request.method == 'POST':
        try:
            # Capturar dados do formulário
            nu_contrato = request.form.get('nu_contrato', '').strip()
            valor = request.form.get('valor', '').strip()

            # Converter para os tipos corretos
            nu_contrato_decimal = Decimal(nu_contrato) if nu_contrato else None
            valor_decimal = Decimal(valor) if valor else None

            # Verificar duplicidade
            if nu_contrato_decimal and valor_decimal:
                if CaixaEmgea.verificar_duplicidade(nu_contrato_decimal, valor_decimal, id):
                    confirmar = request.form.get('confirmar_duplicidade')
                    if not confirmar:
                        flash('Atenção: Já existe outro registro com este Número de Contrato e Valor. '
                              'Marque a opção para confirmar a alteração mesmo assim.', 'warning')
                        return render_template('caixa_emgea/form.html',
                                               registro=registro,
                                               dados=request.form,
                                               duplicidade=True,
                                               ocorrencias=PenOcorrencias.query.all(),
                                               status_list=PenStatusOcorrencia.query.all(),
                                               observacoes=PenObservacoes.query.all(),
                                               carteiras=PenCarteiras.query.all())

            # Atualizar registro
            registro.ID_OCORRENCIA = int(request.form.get('id_ocorrencia')) if request.form.get(
                'id_ocorrencia') else None
            registro.NU_OFICIO = int(request.form.get('nu_oficio')) if request.form.get('nu_oficio') else None
            registro.DT_OFICIO = int(request.form.get('dt_oficio')) if request.form.get('dt_oficio') else None
            registro.EMITENTE = request.form.get('emitente', '').strip() or None
            registro.ID_STATUS = int(request.form.get('id_status')) if request.form.get('id_status') else None
            registro.NU_CONTRATO = nu_contrato_decimal
            registro.NU_PROCESSO = request.form.get('nu_processo', '').strip() or None
            registro.VALOR = valor_decimal
            registro.VR_REAL = Decimal(request.form.get('vr_real')) if request.form.get('vr_real') else None
            registro.DT_PAGTO = datetime.strptime(request.form.get('dt_pagto'), '%Y-%m-%d').date() if request.form.get(
                'dt_pagto') else None
            registro.ID_OBSERVACAO = int(request.form.get('id_observacao')) if request.form.get(
                'id_observacao') else None
            registro.ID_ESPECIFICACAO = int(request.form.get('id_especificacao')) if request.form.get(
                'id_especificacao') else None
            registro.ID_CARTEIRA = int(request.form.get('id_carteira')) if request.form.get('id_carteira') else None
            registro.NR_TICKET = int(request.form.get('nr_ticket')) if request.form.get('nr_ticket') else None
            registro.DSC_DOCUMENTO = request.form.get('dsc_documento', '').strip() or None
            registro.VR_ISS = Decimal(request.form.get('vr_iss')) if request.form.get('vr_iss') else None
            registro.USUARIO_ALTERACAO = current_user.nome  # CORRIGIDO: mudado de NOME para nome
            registro.UPDATED_AT = datetime.utcnow()

            db.session.commit()

            # Registrar log
            registrar_log(
                acao='editar',
                entidade='caixa_emgea',
                entidade_id=registro.ID,
                descricao=f'Registro Caixa EMGEA editado - ID: {id}'
            )

            flash('Registro atualizado com sucesso!', 'success')
            return redirect(url_for('caixa_emgea.index'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao atualizar registro: {str(e)}', 'danger')

    # GET - Exibir formulário
    return render_template('caixa_emgea/form.html',
                           registro=registro,
                           ocorrencias=PenOcorrencias.query.all(),
                           status_list=PenStatusOcorrencia.query.all(),
                           observacoes=PenObservacoes.query.all(),
                           carteiras=PenCarteiras.query.all())


@caixa_emgea_bp.route('/excluir/<int:id>', methods=['POST'])
@login_required
def excluir(id):
    """Excluir registro (soft delete)"""
    try:
        registro = CaixaEmgea.query.get_or_404(id)
        registro.soft_delete(current_user.nome)  # CORRIGIDO: mudado de NOME para nome

        registrar_log(
            acao='excluir',
            entidade='caixa_emgea',
            entidade_id=id,
            descricao=f'Registro Caixa EMGEA excluído - ID: {id}'
        )

        flash('Registro excluído com sucesso!', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao excluir registro: {str(e)}', 'danger')

    return redirect(url_for('caixa_emgea.index'))