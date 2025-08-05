from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
from flask_login import login_required, current_user
from app import db
from app.models.pendencia_retencao import (
    PenDetalhamento, AexAnalitico, PenRelacionaVlrRetido,
    PenCarteiras, PenOcorrencias, AexConsolidado, PenStatusOcorrencia
)
from app.utils.audit import registrar_log
from datetime import datetime
from sqlalchemy import or_, and_, text
from decimal import Decimal

pendencia_retencao_bp = Blueprint('pendencia_retencao', __name__, url_prefix='/pendencia-retencao')


@pendencia_retencao_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@pendencia_retencao_bp.route('/')
@login_required
def index():
    """Página inicial do sistema Pendência Vs Retenção"""
    return render_template('pendencia_retencao/index.html')


@pendencia_retencao_bp.route('/consultar', methods=['GET', 'POST'])
@login_required
def consultar():
    """Consulta de pendências e retenções por contrato"""
    # Verificar se veio número de contrato por GET
    nu_contrato_param = request.args.get('nu_contrato', '')

    if request.method == 'POST':
        nu_contrato = request.form.get('nu_contrato', '').strip()

        if not nu_contrato:
            flash('Por favor, informe o número do contrato.', 'warning')
            return redirect(url_for('pendencia_retencao.consultar'))

        try:
            # Converter string para decimal para buscar na tabela de pendências
            try:
                nu_contrato_decimal = Decimal(nu_contrato)
            except:
                flash('Número de contrato inválido.', 'danger')
                return redirect(url_for('pendencia_retencao.consultar'))

            # Buscar TODAS as pendências do contrato (mudado de first() para all())
            pendencias = db.session.query(
                PenDetalhamento,
                PenCarteiras.DSC_CARTEIRA,
                PenOcorrencias.DSC_OCORRENCIA
            ).outerjoin(
                PenCarteiras,
                PenDetalhamento.ID_CARTEIRA == PenCarteiras.ID_CARTEIRA
            ).outerjoin(
                PenOcorrencias,
                PenDetalhamento.ID_OCORRENCIA == PenOcorrencias.ID_OCORRENCIA
            ).filter(
                PenDetalhamento.NU_CONTRATO == nu_contrato_decimal
            ).all()

            if not pendencias:
                flash(f'Nenhuma pendência encontrada para o contrato {nu_contrato}.', 'info')
                return redirect(url_for('pendencia_retencao.consultar'))

            # Para buscar na tabela analítico, usar o número como string
            nu_contrato_str = str(int(nu_contrato_decimal))

            # Buscar registros analíticos
            analiticos = AexAnalitico.query.filter(
                AexAnalitico.NU_CONTRATO == nu_contrato_str
            ).all()

            # Buscar todas as vinculações existentes para todas as pendências encontradas
            ids_pendencias = [p.PenDetalhamento.ID_DETALHAMENTO for p in pendencias]
            vinculacoes_existentes = PenRelacionaVlrRetido.query.filter(
                PenRelacionaVlrRetido.ID_PENDENCIA.in_(ids_pendencias)
            ).all()

            # Criar dicionário de vinculações por pendência
            vinculacoes_por_pendencia = {}
            for v in vinculacoes_existentes:
                if v.ID_PENDENCIA not in vinculacoes_por_pendencia:
                    vinculacoes_por_pendencia[v.ID_PENDENCIA] = []
                vinculacoes_por_pendencia[v.ID_PENDENCIA].append(v.ID_ARREC_EXT_SISTEMA)

            return render_template(
                'pendencia_retencao/resultado_consulta.html',
                pendencias=pendencias,
                analiticos=analiticos,
                vinculacoes_por_pendencia=vinculacoes_por_pendencia
            )

        except Exception as e:
            flash(f'Erro ao consultar dados: {str(e)}', 'danger')
            return redirect(url_for('pendencia_retencao.consultar'))

    return render_template('pendencia_retencao/consultar.html')


@pendencia_retencao_bp.route('/listar-contratos')
@login_required
def listar_contratos():
    """Listar contratos disponíveis para seleção"""
    try:
        # Buscar contratos conforme query fornecida
        contratos = db.session.query(
            PenDetalhamento,
            PenCarteiras.DSC_CARTEIRA,
            PenOcorrencias.DSC_OCORRENCIA,
            PenStatusOcorrencia.DSC_STATUS
        ).join(
            PenCarteiras,
            PenDetalhamento.ID_CARTEIRA == PenCarteiras.ID_CARTEIRA
        ).join(
            PenStatusOcorrencia,
            PenDetalhamento.ID_STATUS == PenStatusOcorrencia.ID_STATUS
        ).outerjoin(
            PenOcorrencias,
            PenDetalhamento.ID_OCORRENCIA == PenOcorrencias.ID_OCORRENCIA
        ).filter(
            PenDetalhamento.VR_REAL_FALHA < 0,
            PenDetalhamento.IC_EXCLUIR == None
        ).order_by(
            PenDetalhamento.NU_CONTRATO.desc()
        ).all()

        return render_template(
            'pendencia_retencao/listar_contratos.html',
            contratos=contratos
        )

    except Exception as e:
        flash(f'Erro ao listar contratos: {str(e)}', 'danger')
        return redirect(url_for('pendencia_retencao.index'))


@pendencia_retencao_bp.route('/consultar-contrato/<nu_contrato>')
@login_required
def consultar_contrato_direto(nu_contrato):
    """Redireciona para consulta com o contrato já preenchido"""
    return redirect(url_for('pendencia_retencao.consultar', nu_contrato=nu_contrato))


@pendencia_retencao_bp.route('/salvar-vinculacao', methods=['POST'])
@login_required
def salvar_vinculacao():
    """Salvar vinculação entre pendências e retenções"""
    try:
        data = request.get_json()

        ids_pendencias = data.get('ids_pendencias', [])
        ids_analiticos = data.get('ids_analiticos', [])
        observacao = data.get('observacao', '').strip()

        if not ids_pendencias or not ids_analiticos:
            return jsonify({
                'success': False,
                'message': 'Selecione ao menos uma pendência e um registro analítico.'
            }), 400

        contador = 0
        # Criar vinculação para cada combinação de pendência x analítico
        for id_pendencia in ids_pendencias:
            for id_analitico in ids_analiticos:
                # Verificar se já existe
                existe = PenRelacionaVlrRetido.query.filter_by(
                    ID_PENDENCIA=id_pendencia,
                    ID_ARREC_EXT_SISTEMA=id_analitico
                ).first()

                if not existe:
                    nova_vinculacao = PenRelacionaVlrRetido(
                        ID_PENDENCIA=id_pendencia,
                        ID_ARREC_EXT_SISTEMA=id_analitico,
                        OBS=observacao,
                        NO_RSPONSAVEL=current_user.nome,
                        DT_ANALISE=datetime.now()
                    )
                    db.session.add(nova_vinculacao)
                    contador += 1

        db.session.commit()

        registrar_log(
            acao='criar',
            entidade='pendencia_retencao',
            entidade_id=f"{','.join(map(str, ids_pendencias))}",
            descricao=f'Vinculação de {contador} registros',
            dados_novos={
                'ids_pendencias': ids_pendencias,
                'ids_analiticos': ids_analiticos,
                'observacao': observacao
            }
        )

        return jsonify({
            'success': True,
            'message': f'{contador} vinculações salvas com sucesso!'
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Erro ao salvar vinculação: {str(e)}'
        }), 500


@pendencia_retencao_bp.route('/listar-vinculacoes')
@login_required
def listar_vinculacoes():
    """Listar todas as vinculações realizadas"""
    try:
        vinculacoes = db.session.query(
            PenRelacionaVlrRetido,
            PenDetalhamento.NU_CONTRATO,
            PenDetalhamento.NU_PROCESSO,
            AexAnalitico.VALOR,
            AexAnalitico.NO_ARQUIVO,
            AexAnalitico.DT_REPASSE
        ).join(
            PenDetalhamento,
            PenRelacionaVlrRetido.ID_PENDENCIA == PenDetalhamento.ID_DETALHAMENTO
        ).join(
            AexAnalitico,
            PenRelacionaVlrRetido.ID_ARREC_EXT_SISTEMA == AexAnalitico.ID
        ).order_by(
            PenRelacionaVlrRetido.DT_ANALISE.desc()
        ).all()

        return render_template(
            'pendencia_retencao/listar_vinculacoes.html',
            vinculacoes=vinculacoes
        )

    except Exception as e:
        flash(f'Erro ao listar vinculações: {str(e)}', 'danger')
        return redirect(url_for('pendencia_retencao.index'))