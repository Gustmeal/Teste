from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify, send_file
from app.models.criterio_distribuicao import CriterioDistribuicao
from app.models.contrato_distribuivel import ContratoDistribuivel, ContratoArrastavel, Distribuicao
from app.models.periodo import PeriodoAvaliacao
from app.models.edital import Edital
from app.models.empresa_participante import EmpresaParticipante
from app.models.limite_distribuicao import LimiteDistribuicao
from app import db
from datetime import datetime
from flask_login import login_required, current_user
from app.utils.audit import registrar_log
from app.auth.utils import admin_required
import pandas as pd
import tempfile
import os
import io

distribuicao_bp = Blueprint('distribuicao', __name__, url_prefix='/credenciamento')


@distribuicao_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@distribuicao_bp.route('/distribuicao')
@login_required
@admin_required
def index():
    # Obter períodos ativos
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).all()

    # Obter critérios de distribuição
    criterios = CriterioDistribuicao.query.filter(CriterioDistribuicao.DELETED_AT == None).all()

    # Obter empresas participantes
    empresas = EmpresaParticipante.query.filter(EmpresaParticipante.DELETED_AT == None).all()

    # Obter histórico de distribuições (mais recentes primeiro)
    distribuicoes = db.session.query(
        db.func.count(Distribuicao.ID).label('total_contratos'),
        Distribuicao.DT_REFERENCIA,
        Distribuicao.ID_EDITAL,
        Distribuicao.ID_PERIODO,
        Distribuicao.COD_CRITERIO_SELECAO,
        CriterioDistribuicao.DS_CRITERIO_SELECAO,
        Edital.NU_EDITAL
    ).join(
        CriterioDistribuicao,
        Distribuicao.COD_CRITERIO_SELECAO == CriterioDistribuicao.COD,
        isouter=True
    ).join(
        Edital,
        Distribuicao.ID_EDITAL == Edital.ID
    ).filter(
        Distribuicao.DELETED_AT == None
    ).group_by(
        Distribuicao.DT_REFERENCIA,
        Distribuicao.ID_EDITAL,
        Distribuicao.ID_PERIODO,
        Distribuicao.COD_CRITERIO_SELECAO,
        CriterioDistribuicao.DS_CRITERIO_SELECAO,
        Edital.NU_EDITAL
    ).order_by(
        Distribuicao.DT_REFERENCIA.desc()
    ).limit(10).all()

    return render_template(
        'distribuicao/index.html',
        periodos=periodos,
        criterios=criterios,
        empresas=empresas,
        distribuicoes=distribuicoes
    )


@distribuicao_bp.route('/distribuicao/nova', methods=['GET', 'POST'])
@login_required
@admin_required
def nova_distribuicao():
    if request.method == 'POST':
        try:
            periodo_id = int(request.form['periodo_id'])
            criterio_id = int(request.form['criterio_id'])

            # Verificar se o período e critério existem
            periodo = PeriodoAvaliacao.query.get_or_404(periodo_id)
            criterio = CriterioDistribuicao.query.filter_by(COD=criterio_id).first()

            if not criterio:
                flash(f'Critério de distribuição com código {criterio_id} não encontrado.', 'danger')
                return redirect(url_for('distribuicao.nova_distribuicao'))

            # Aqui implementaremos o processamento de distribuição
            # Para esta fase inicial, vamos apenas simular o processamento

            # Limpar tabelas temporárias (distribuíveis, arrastaveis)
            db.session.query(ContratoDistribuivel).delete()
            db.session.query(ContratoArrastavel).delete()

            # Registrar dados para auditoria
            dados_processo = {
                'periodo_id': periodo_id,
                'criterio_id': criterio_id,
                'data_processamento': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            registrar_log(
                acao='criar',
                entidade='distribuicao',
                entidade_id=0,  # ID temporário
                descricao=f'Início do processamento de distribuição para o período {periodo.ID_PERIODO}',
                dados_novos=dados_processo
            )

            # Simulação: Adicionar alguns registros ao histórico de distribuição
            dt_referencia = datetime.now()

            # Em uma implementação real, o processo seria mais complexo seguindo as regras do documento
            # Esta é apenas uma simulação para demonstrar a interface
            for _ in range(5):  # Adiciona 5 registros de exemplo
                distribuicao = Distribuicao(
                    DT_REFERENCIA=dt_referencia,
                    ID_EDITAL=periodo.ID_EDITAL,
                    ID_PERIODO=periodo.ID,
                    FkContratoSISCTR=12345,  # Número de exemplo
                    COD_CRITERIO_SELECAO=criterio_id,
                    COD_EMPRESA_COBRANCA=101,  # Código de empresa de exemplo
                    NR_CPF_CNPJ=12345678901,
                    VR_SD_DEVEDOR=1000.00
                )
                db.session.add(distribuicao)

            db.session.commit()

            flash('Processamento de distribuição iniciado com sucesso.', 'success')
            return redirect(url_for('distribuicao.index'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro durante o processamento: {str(e)}', 'danger')

    # Obter períodos ativos
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).all()

    # Obter critérios de distribuição
    criterios = CriterioDistribuicao.query.filter(CriterioDistribuicao.DELETED_AT == None).all()

    return render_template(
        'distribuicao/form_distribuicao.html',
        periodos=periodos,
        criterios=criterios
    )


@distribuicao_bp.route('/distribuicao/redistribuicao', methods=['GET', 'POST'])
@login_required
@admin_required
def redistribuicao():
    if request.method == 'POST':
        try:
            periodo_id = int(request.form['periodo_id'])
            criterio_id = int(request.form['criterio_id'])
            empresa_id = int(request.form['empresa_id'])

            # Verificar se o período, critério e empresa existem
            periodo = PeriodoAvaliacao.query.get_or_404(periodo_id)
            criterio = CriterioDistribuicao.query.filter_by(COD=criterio_id).first()
            empresa = EmpresaParticipante.query.filter_by(ID_EMPRESA=empresa_id).first()

            if not criterio:
                flash(f'Critério de distribuição com código {criterio_id} não encontrado.', 'danger')
                return redirect(url_for('distribuicao.redistribuicao'))

            if not empresa:
                flash(f'Empresa com ID {empresa_id} não encontrada.', 'danger')
                return redirect(url_for('distribuicao.redistribuicao'))

            # Aqui implementaremos o processamento de redistribuição
            # Para esta fase inicial, vamos apenas simular o processamento

            # Limpar tabelas temporárias (distribuíveis, arrastaveis)
            db.session.query(ContratoDistribuivel).delete()
            db.session.query(ContratoArrastavel).delete()

            # Registrar dados para auditoria
            dados_processo = {
                'periodo_id': periodo_id,
                'criterio_id': criterio_id,
                'empresa_id': empresa_id,
                'data_processamento': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            registrar_log(
                acao='criar',
                entidade='redistribuicao',
                entidade_id=0,  # ID temporário
                descricao=f'Início do processamento de redistribuição para o período {periodo.ID_PERIODO}',
                dados_novos=dados_processo
            )

            # Simulação: Adicionar alguns registros ao histórico de distribuição
            dt_referencia = datetime.now()

            # Em uma implementação real, o processo seria mais complexo seguindo as regras do documento
            # Esta é apenas uma simulação para demonstrar a interface
            for _ in range(5):  # Adiciona 5 registros de exemplo
                distribuicao = Distribuicao(
                    DT_REFERENCIA=dt_referencia,
                    ID_EDITAL=periodo.ID_EDITAL,
                    ID_PERIODO=periodo.ID,
                    FkContratoSISCTR=12345,  # Número de exemplo
                    COD_CRITERIO_SELECAO=criterio_id,
                    COD_EMPRESA_COBRANCA=101,  # Código de empresa de exemplo
                    NR_CPF_CNPJ=12345678901,
                    VR_SD_DEVEDOR=1000.00
                )
                db.session.add(distribuicao)

            db.session.commit()

            flash('Processamento de redistribuição iniciado com sucesso.', 'success')
            return redirect(url_for('distribuicao.index'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro durante o processamento: {str(e)}', 'danger')

    # Obter períodos ativos
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).all()

    # Obter critérios de distribuição
    criterios = CriterioDistribuicao.query.filter(CriterioDistribuicao.DELETED_AT == None).all()

    # Obter empresas participantes
    empresas = EmpresaParticipante.query.filter(EmpresaParticipante.DELETED_AT == None).all()

    return render_template(
        'distribuicao/form_redistribuicao.html',
        periodos=periodos,
        criterios=criterios,
        empresas=empresas
    )


@distribuicao_bp.route('/distribuicao/visualizar/<string:data_ref>/<int:periodo_id>/<int:criterio_id>')
@login_required
def visualizar_distribuicao(data_ref, periodo_id, criterio_id):
    try:
        # Converter a data_ref para datetime
        data_ref_dt = datetime.strptime(data_ref, '%Y-%m-%d')

        # Buscar distribuições
        distribuicoes = Distribuicao.query.filter(
            Distribuicao.DT_REFERENCIA == data_ref_dt,
            Distribuicao.ID_PERIODO == periodo_id,
            Distribuicao.COD_CRITERIO_SELECAO == criterio_id,
            Distribuicao.DELETED_AT == None
        ).all()

        if not distribuicoes:
            flash('Nenhuma distribuição encontrada com os parâmetros informados.', 'warning')
            return redirect(url_for('distribuicao.index'))

        # Agrupar por empresa para mostrar totais
        estatisticas = db.session.query(
            Distribuicao.COD_EMPRESA_COBRANCA,
            db.func.count(Distribuicao.ID).label('total_contratos'),
            db.func.sum(Distribuicao.VR_SD_DEVEDOR).label('valor_total')
        ).filter(
            Distribuicao.DT_REFERENCIA == data_ref_dt,
            Distribuicao.ID_PERIODO == periodo_id,
            Distribuicao.COD_CRITERIO_SELECAO == criterio_id,
            Distribuicao.DELETED_AT == None
        ).group_by(
            Distribuicao.COD_EMPRESA_COBRANCA
        ).all()

        # Buscar informações do período e critério
        periodo = PeriodoAvaliacao.query.get(periodo_id)
        criterio = CriterioDistribuicao.query.filter_by(COD=criterio_id).first()

        return render_template(
            'distribuicao/visualizar.html',
            distribuicoes=distribuicoes[:100],  # Limitando para não sobrecarregar a página
            estatisticas=estatisticas,
            periodo=periodo,
            criterio=criterio,
            data_ref=data_ref_dt,
            total_registros=len(distribuicoes)
        )

    except Exception as e:
        flash(f'Erro ao visualizar distribuição: {str(e)}', 'danger')
        return redirect(url_for('distribuicao.index'))


@distribuicao_bp.route('/distribuicao/exportar/<string:data_ref>/<int:periodo_id>/<int:criterio_id>')
@login_required
def exportar_distribuicao(data_ref, periodo_id, criterio_id):
    try:
        # Converter a data_ref para datetime
        data_ref_dt = datetime.strptime(data_ref, '%Y-%m-%d')

        # Buscar distribuições
        distribuicoes = Distribuicao.query.filter(
            Distribuicao.DT_REFERENCIA == data_ref_dt,
            Distribuicao.ID_PERIODO == periodo_id,
            Distribuicao.COD_CRITERIO_SELECAO == criterio_id,
            Distribuicao.DELETED_AT == None
        ).all()

        if not distribuicoes:
            flash('Nenhuma distribuição encontrada com os parâmetros informados.', 'warning')
            return redirect(url_for('distribuicao.index'))

        # Criar arquivo TXT para exportação
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.txt')

        # Escrever cabeçalho
        temp_file.write(b"fkContratoSISCTR;NOVA_EMPRESA\n")

        # Escrever dados
        for dist in distribuicoes:
            line = f"{dist.FkContratoSISCTR};{dist.COD_EMPRESA_COBRANCA}\n"
            temp_file.write(line.encode('utf-8'))

        temp_file.close()

        # Formatar data para o nome do arquivo
        data_formatada = data_ref_dt.strftime('%Y%m%d')
        filename = f"DISTRIBUICAO_COBRANCA_{data_formatada}_TI.TXT"

        return send_file(
            temp_file.name,
            as_attachment=True,
            download_name=filename,
            mimetype='text/plain'
        )

    except Exception as e:
        flash(f'Erro ao exportar distribuição: {str(e)}', 'danger')
        return redirect(url_for('distribuicao.index'))