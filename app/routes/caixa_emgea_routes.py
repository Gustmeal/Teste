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
from sqlalchemy import text

caixa_emgea_bp = Blueprint('caixa_emgea', __name__, url_prefix='/caixa-emgea')


@caixa_emgea_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


def criar_observacao(descricao, id_detalhamento=None):
    """
    Cria uma observação na tabela PEN_TB005_OBSERVACOES e retorna o ID gerado

    LÓGICA:
    1. Busca o próximo ID disponível (MAX + 1)
    2. Insere com ID manual porque a coluna não tem IDENTITY no banco
    3. Retorna o ID gerado
    """
    # PASSO 1: Obter próximo ID disponível
    sql_max = text("""
        SELECT ISNULL(MAX(ID_OBSERVACAO), 0) + 1 AS PROXIMO_ID 
        FROM [BDG].[PEN_TB005_OBSERVACOES]
    """)

    result_max = db.session.execute(sql_max)
    proximo_id = result_max.fetchone()[0]

    # PASSO 2: Inserir com ID manual
    sql_insert = text("""
        INSERT INTO [BDG].[PEN_TB005_OBSERVACOES] 
        (ID_OBSERVACAO, ID_DETALHAMENTO, DSC_OBSERVACAO, ULTIMA_ATUALIZACAO)
        VALUES (:id_observacao, :id_detalhamento, :descricao, :data_atualizacao)
    """)

    db.session.execute(sql_insert, {
        'id_observacao': proximo_id,
        'id_detalhamento': id_detalhamento,
        'descricao': descricao,
        'data_atualizacao': datetime.utcnow()
    })

    return proximo_id


def criar_especificacao(descricao, id_detalhamento=None):
    """
    Cria uma especificação na tabela PEN_TB006_ESPECIFICACAO_FALHA e retorna o ID gerado

    LÓGICA:
    1. Busca o próximo ID disponível (MAX + 1)
    2. Insere com ID manual porque a coluna não tem IDENTITY no banco
    3. Retorna o ID gerado
    """
    # PASSO 1: Obter próximo ID disponível
    sql_max = text("""
        SELECT ISNULL(MAX(ID_ESPECIFICACAO), 0) + 1 AS PROXIMO_ID 
        FROM [BDG].[PEN_TB006_ESPECIFICACAO_FALHA]
    """)

    result_max = db.session.execute(sql_max)
    proximo_id = result_max.fetchone()[0]

    # PASSO 2: Inserir com ID manual
    sql_insert = text("""
        INSERT INTO [BDG].[PEN_TB006_ESPECIFICACAO_FALHA] 
        (ID_ESPECIFICACAO, ID_DETALHAMENTO, DSC_ESPECIFICACAO, ULTIMA_ATUALIZACAO)
        VALUES (:id_especificacao, :id_detalhamento, :descricao, :data_atualizacao)
    """)

    db.session.execute(sql_insert, {
        'id_especificacao': proximo_id,
        'id_detalhamento': id_detalhamento,
        'descricao': descricao,
        'data_atualizacao': datetime.utcnow()
    })

    return proximo_id


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
    """
    Formulário para novo registro
    LÓGICA:
    1. Obter próximo ID_DETALHAMENTO (último + 1)
    2. Verificar duplicidade (mesmo NU_CONTRATO + VR_FALHA)
       - Se duplicado: INDICIO_DUPLIC = TRUE (pede confirmação)
       - Se não duplicado: INDICIO_DUPLIC = NULL
    3. ID_ACAO sempre = 0
    4. Se houver observação: inserir em PEN_TB005 e pegar ID
    5. Se houver especificação: inserir em PEN_TB006 e pegar ID
    6. Inserir registro principal com os IDs gerados
    """
    if request.method == 'POST':
        try:
            # PASSO 1: Obter próximo ID_DETALHAMENTO
            proximo_id = CaixaEmgea.obter_proximo_id()

            # Capturar dados do formulário
            nu_contrato = request.form.get('nu_contrato', '').strip()
            vr_falha = request.form.get('vr_falha', '').strip()

            # Converter para os tipos corretos
            nu_contrato_decimal = Decimal(nu_contrato) if nu_contrato else None
            vr_falha_decimal = Decimal(vr_falha) if vr_falha else None

            # PASSO 2: Verificar duplicidade e definir INDICIO_DUPLIC
            indicio_duplic = None  # Padrão: NULL

            if nu_contrato_decimal and vr_falha_decimal:
                if CaixaEmgea.verificar_duplicidade(nu_contrato_decimal, vr_falha_decimal):
                    # DETECTOU DUPLICIDADE
                    indicio_duplic = True  # Marca como TRUE

                    confirmar = request.form.get('confirmar_duplicidade')
                    if not confirmar:
                        # Usuário não confirmou - mostrar alerta
                        flash('Atenção: Já existe um registro com este Número de Contrato e Valor de Falha. '
                              'Marque a opção para confirmar a inclusão mesmo assim.', 'warning')

                        devedores = CaixaEmgea.obter_devedores_distintos()
                        if not devedores:
                            devedores = ['CAIXA', 'EMGEA']

                        return render_template('caixa_emgea/form.html',
                                               ocorrencias=PenOcorrencias.query.all(),
                                               status_list=PenStatusOcorrencia.query.all(),
                                               carteiras=PenCarteiras.query.all(),
                                               devedores=devedores,
                                               dados=request.form,
                                               duplicidade=True)
                else:
                    # NÃO tem duplicidade
                    indicio_duplic = None  # Mantém NULL

            # PASSO 3: Processar OBSERVAÇÃO (se houver)
            id_observacao = None
            observacao_texto = request.form.get('observacao_texto', '').strip()
            if observacao_texto:
                id_observacao = criar_observacao(observacao_texto, proximo_id)

            # PASSO 4: Processar ESPECIFICAÇÃO (se houver)
            id_especificacao = None
            especificacao_texto = request.form.get('especificacao_texto', '').strip()
            if especificacao_texto:
                id_especificacao = criar_especificacao(especificacao_texto, proximo_id)

            # PASSO 5: Criar registro principal
            novo_registro = CaixaEmgea()

            # Definir ID manualmente
            novo_registro.ID_DETALHAMENTO = proximo_id

            # Campos básicos
            novo_registro.NU_CONTRATO = nu_contrato_decimal
            novo_registro.VR_FALHA = vr_falha_decimal
            novo_registro.ID_OCORRENCIA = int(request.form.get('id_ocorrencia')) if request.form.get(
                'id_ocorrencia') else None
            novo_registro.ID_STATUS = int(request.form.get('id_status')) if request.form.get('id_status') else None
            novo_registro.ID_CARTEIRA = int(request.form.get('id_carteira')) if request.form.get(
                'id_carteira') else None
            novo_registro.NU_OFICIO = int(request.form.get('nu_oficio')) if request.form.get('nu_oficio') else None

            # NOVOS CAMPOS
            novo_registro.NU_MEMORANDO = request.form.get('nu_memorando', '').strip() or None
            novo_registro.NR_PROCESSO_SEI = request.form.get('nr_processo_sei', '').strip() or None

            # DT_DOCUMENTO
            dt_documento_str = request.form.get('dt_documento', '').strip()
            if dt_documento_str:
                novo_registro.DT_DOCUMENTO = datetime.strptime(dt_documento_str, '%Y-%m-%d').date()

            # DEVEDOR e flags
            novo_registro.DEVEDOR = request.form.get('devedor', '').strip() or None
            novo_registro.INDICIO_DUPLIC = indicio_duplic
            novo_registro.ID_ACAO = 0  # SEMPRE 0

            # IC_CONDENACAO
            ic_condenacao = request.form.get('ic_condenacao', '').strip()
            if ic_condenacao == '1':
                novo_registro.IC_CONDENACAO = True
            elif ic_condenacao == '0':
                novo_registro.IC_CONDENACAO = False
            else:
                novo_registro.IC_CONDENACAO = None

            # VR_REAL (copia VR_FALHA com sinal correto)
            # Se DEVEDOR = EMGEA: negativo | Se DEVEDOR = CAIXA: positivo
            devedor_temp = request.form.get('devedor', '').strip()
            if devedor_temp == 'EMGEA' and vr_falha_decimal:
                novo_registro.VR_REAL = -abs(vr_falha_decimal)  # Garante negativo
            else:
                novo_registro.VR_REAL = vr_falha_decimal  # Mantém positivo

            # DT_PAGTO
            dt_pagto_str = request.form.get('dt_pagto', '').strip()
            if dt_pagto_str:
                novo_registro.DT_PAGTO = datetime.strptime(dt_pagto_str, '%Y-%m-%d').date()

            # IDs das tabelas auxiliares
            novo_registro.ID_OBSERVACAO = id_observacao
            novo_registro.ID_ESPECIFICACAO = id_especificacao

            # Outros campos
            novo_registro.NR_TICKET = int(request.form.get('nr_ticket')) if request.form.get('nr_ticket') else None
            novo_registro.DSC_DOCUMENTO = request.form.get('dsc_documento', '').strip() or None
            novo_registro.VR_ISS = Decimal(request.form.get('vr_iss')) if request.form.get('vr_iss') else None

            # Auditoria
            novo_registro.USUARIO_CRIACAO = current_user.nome

            db.session.add(novo_registro)
            db.session.commit()

            # Registrar log
            registrar_log(
                acao='criar',
                entidade='caixa_emgea',
                entidade_id=novo_registro.ID_DETALHAMENTO,
                descricao=f'Criação de registro Caixa EMGEA - Contrato: {novo_registro.NU_CONTRATO}',
                dados_novos={
                    'id_detalhamento': proximo_id,
                    'nu_contrato': str(novo_registro.NU_CONTRATO),
                    'vr_falha': str(novo_registro.VR_FALHA),
                    'devedor': novo_registro.DEVEDOR,
                    'indicio_duplic': indicio_duplic,
                    'id_acao': 0,
                    'id_observacao': id_observacao,
                    'id_especificacao': id_especificacao,
                    'nu_memorando': novo_registro.NU_MEMORANDO,
                    'nr_processo_sei': novo_registro.NR_PROCESSO_SEI
                }
            )

            flash('Registro criado com sucesso!', 'success')
            return redirect(url_for('caixa_emgea.index'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao criar registro: {str(e)}', 'danger')

    # GET - Exibir formulário
    # Buscar devedores distintos (CAIXA e EMGEA)
    devedores = CaixaEmgea.obter_devedores_distintos()
    if not devedores:
        devedores = ['CAIXA', 'EMGEA']  # Valores padrão

    return render_template('caixa_emgea/form.html',
                           ocorrencias=PenOcorrencias.query.all(),
                           status_list=PenStatusOcorrencia.query.all(),
                           carteiras=PenCarteiras.query.all(),
                           devedores=devedores)


@caixa_emgea_bp.route('/editar/<int:id>', methods=['GET', 'POST'])
@login_required
def editar(id):
    """
    Editar registro existente
    LÓGICA:
    - Verifica duplicidade para outros registros
    - Atualiza INDICIO_DUPLIC automaticamente
    - ID_ACAO mantém como 0
    """
    registro = CaixaEmgea.query.get_or_404(id)

    if request.method == 'POST':
        try:
            # Capturar dados
            nu_contrato = request.form.get('nu_contrato', '').strip()
            vr_falha = request.form.get('vr_falha', '').strip()
            nu_contrato_decimal = Decimal(nu_contrato) if nu_contrato else None
            vr_falha_decimal = Decimal(vr_falha) if vr_falha else None

            # Verificar duplicidade e definir INDICIO_DUPLIC
            indicio_duplic = None  # Padrão: NULL

            if nu_contrato_decimal and vr_falha_decimal:
                # Verificar duplicidade com OUTROS registros (excluindo o atual)
                if CaixaEmgea.verificar_duplicidade(nu_contrato_decimal, vr_falha_decimal, id):
                    # DETECTOU DUPLICIDADE com outro registro
                    indicio_duplic = True

                    confirmar = request.form.get('confirmar_duplicidade')
                    if not confirmar:
                        flash('Atenção: Já existe outro registro com este Número de Contrato e Valor de Falha. '
                              'Marque a opção para confirmar a alteração mesmo assim.', 'warning')

                        devedores = CaixaEmgea.obter_devedores_distintos() or ['CAIXA', 'EMGEA']

                        # Buscar observação e especificação atuais
                        observacao_atual = None
                        especificacao_atual = None

                        if registro.ID_OBSERVACAO:
                            obs = PenObservacoes.query.get(registro.ID_OBSERVACAO)
                            if obs:
                                observacao_atual = obs.DSC_OBSERVACAO

                        if registro.ID_ESPECIFICACAO:
                            result = db.session.execute(
                                text(
                                    "SELECT DSC_ESPECIFICACAO FROM BDG.PEN_TB006_ESPECIFICACAO_FALHA WHERE ID_ESPECIFICACAO = :id"),
                                {'id': registro.ID_ESPECIFICACAO}
                            )
                            row = result.fetchone()
                            if row:
                                especificacao_atual = row[0]

                        return render_template('caixa_emgea/form.html',
                                               registro=registro,
                                               ocorrencias=PenOcorrencias.query.all(),
                                               status_list=PenStatusOcorrencia.query.all(),
                                               carteiras=PenCarteiras.query.all(),
                                               devedores=devedores,
                                               observacao_atual=observacao_atual,
                                               especificacao_atual=especificacao_atual,
                                               dados=request.form,
                                               duplicidade=True,
                                               edit_mode=True)
                else:
                    # NÃO tem duplicidade
                    indicio_duplic = None

            # Dados antigos para log
            dados_antigos = {
                'nu_contrato': str(registro.NU_CONTRATO) if registro.NU_CONTRATO else None,
                'vr_falha': str(registro.VR_FALHA) if registro.VR_FALHA else None,
                'devedor': registro.DEVEDOR,
                'nu_memorando': registro.NU_MEMORANDO,
                'nr_processo_sei': registro.NR_PROCESSO_SEI
            }

            # Processar OBSERVAÇÃO (se houver nova)
            observacao_texto = request.form.get('observacao_texto', '').strip()
            if observacao_texto:
                # Criar nova observação e atualizar o ID
                registro.ID_OBSERVACAO = criar_observacao(observacao_texto, registro.ID_DETALHAMENTO)

            # Processar ESPECIFICAÇÃO (se houver nova)
            especificacao_texto = request.form.get('especificacao_texto', '').strip()
            if especificacao_texto:
                # Criar nova especificação e atualizar o ID
                registro.ID_ESPECIFICACAO = criar_especificacao(especificacao_texto, registro.ID_DETALHAMENTO)

            # Atualizar campos
            registro.NU_CONTRATO = nu_contrato_decimal
            registro.VR_FALHA = vr_falha_decimal

            # VR_REAL (copia VR_FALHA com sinal correto)
            # Se DEVEDOR = EMGEA: negativo | Se DEVEDOR = CAIXA: positivo
            devedor_temp = request.form.get('devedor', '').strip()
            if devedor_temp == 'EMGEA' and vr_falha_decimal:
                registro.VR_REAL = -abs(vr_falha_decimal)
            else:
                registro.VR_REAL = vr_falha_decimal

            registro.ID_OCORRENCIA = int(request.form.get('id_ocorrencia')) if request.form.get(
                'id_ocorrencia') else None
            registro.ID_STATUS = int(request.form.get('id_status')) if request.form.get('id_status') else None
            registro.ID_CARTEIRA = int(request.form.get('id_carteira')) if request.form.get('id_carteira') else None
            registro.NU_OFICIO = int(request.form.get('nu_oficio')) if request.form.get('nu_oficio') else None

            # NOVOS CAMPOS
            registro.NU_MEMORANDO = request.form.get('nu_memorando', '').strip() or None
            registro.NR_PROCESSO_SEI = request.form.get('nr_processo_sei', '').strip() or None

            # DT_DOCUMENTO
            dt_documento_str = request.form.get('dt_documento', '').strip()
            if dt_documento_str:
                registro.DT_DOCUMENTO = datetime.strptime(dt_documento_str, '%Y-%m-%d').date()
            else:
                registro.DT_DOCUMENTO = None

            registro.DEVEDOR = request.form.get('devedor', '').strip() or None
            registro.INDICIO_DUPLIC = indicio_duplic

            # IC_CONDENACAO
            ic_condenacao = request.form.get('ic_condenacao', '').strip()
            if ic_condenacao == '1':
                registro.IC_CONDENACAO = True
            elif ic_condenacao == '0':
                registro.IC_CONDENACAO = False
            else:
                registro.IC_CONDENACAO = None

            # DT_PAGTO
            dt_pagto_str = request.form.get('dt_pagto', '').strip()
            if dt_pagto_str:
                registro.DT_PAGTO = datetime.strptime(dt_pagto_str, '%Y-%m-%d').date()
            else:
                registro.DT_PAGTO = None

            registro.NR_TICKET = int(request.form.get('nr_ticket')) if request.form.get('nr_ticket') else None
            registro.DSC_DOCUMENTO = request.form.get('dsc_documento', '').strip() or None
            registro.VR_ISS = Decimal(request.form.get('vr_iss')) if request.form.get('vr_iss') else None

            # Auditoria
            registro.USUARIO_ALTERACAO = current_user.nome
            registro.UPDATED_AT = datetime.utcnow()

            db.session.commit()

            # Registrar log
            registrar_log(
                acao='editar',
                entidade='caixa_emgea',
                entidade_id=registro.ID_DETALHAMENTO,
                descricao=f'Edição de registro Caixa EMGEA - Contrato: {registro.NU_CONTRATO}',
                dados_antigos=dados_antigos,
                dados_novos={
                    'nu_contrato': str(registro.NU_CONTRATO),
                    'vr_falha': str(registro.VR_FALHA),
                    'devedor': registro.DEVEDOR,
                    'indicio_duplic': indicio_duplic,
                    'nu_memorando': registro.NU_MEMORANDO,
                    'nr_processo_sei': registro.NR_PROCESSO_SEI
                }
            )

            flash('Registro atualizado com sucesso!', 'success')
            return redirect(url_for('caixa_emgea.index'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao atualizar registro: {str(e)}', 'danger')

    # GET - Exibir formulário
    devedores = CaixaEmgea.obter_devedores_distintos() or ['CAIXA', 'EMGEA']

    # Buscar observação e especificação atuais (se existirem)
    observacao_atual = None
    especificacao_atual = None

    if registro.ID_OBSERVACAO:
        obs = PenObservacoes.query.get(registro.ID_OBSERVACAO)
        if obs:
            observacao_atual = obs.DSC_OBSERVACAO

    if registro.ID_ESPECIFICACAO:
        result = db.session.execute(
            text("SELECT DSC_ESPECIFICACAO FROM BDG.PEN_TB006_ESPECIFICACAO_FALHA WHERE ID_ESPECIFICACAO = :id"),
            {'id': registro.ID_ESPECIFICACAO}
        )
        row = result.fetchone()
        if row:
            especificacao_atual = row[0]

    return render_template('caixa_emgea/form.html',
                           registro=registro,
                           ocorrencias=PenOcorrencias.query.all(),
                           status_list=PenStatusOcorrencia.query.all(),
                           carteiras=PenCarteiras.query.all(),
                           devedores=devedores,
                           observacao_atual=observacao_atual,
                           especificacao_atual=especificacao_atual,
                           edit_mode=True)


@caixa_emgea_bp.route('/excluir/<int:id>', methods=['POST'])
@login_required
def excluir(id):
    """Soft delete de registro"""
    try:
        registro = CaixaEmgea.query.get_or_404(id)

        dados_antigos = {
            'id_detalhamento': registro.ID_DETALHAMENTO,
            'nu_contrato': str(registro.NU_CONTRATO) if registro.NU_CONTRATO else None,
            'vr_falha': str(registro.VR_FALHA) if registro.VR_FALHA else None,
            'devedor': registro.DEVEDOR
        }

        usuario = current_user.nome
        registro.soft_delete(usuario)

        registrar_log(
            acao='excluir',
            entidade='caixa_emgea',
            entidade_id=registro.ID_DETALHAMENTO,
            descricao=f'Exclusão de registro Caixa EMGEA - Contrato: {registro.NU_CONTRATO}',
            dados_antigos=dados_antigos
        )

        return jsonify({'success': True, 'message': 'Registro excluído com sucesso!'})

    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': f'Erro ao excluir registro: {str(e)}'}), 500