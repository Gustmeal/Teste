from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
from flask_login import login_required, current_user
from app import db
from app.models.pendencia_retencao import (
    PenDetalhamento, AexAnalitico, PenRelacionaVlrRetido,
    PenCarteiras, PenOcorrencias, AexConsolidado, PenStatusOcorrencia, PenOficios
)
from app.utils.audit import registrar_log
from datetime import datetime
from sqlalchemy import or_, and_, text, func
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
                PenOcorrencias.DSC_OCORRENCIA,
                PenStatusOcorrencia.DSC_STATUS,
                PenOficios.DT_OFICIO
            ).outerjoin(
                PenCarteiras,
                PenDetalhamento.ID_CARTEIRA == PenCarteiras.ID_CARTEIRA
            ).outerjoin(
                PenOcorrencias,
                PenDetalhamento.ID_OCORRENCIA == PenOcorrencias.ID_OCORRENCIA
            ).outerjoin(
                PenStatusOcorrencia,
                PenDetalhamento.ID_STATUS == PenStatusOcorrencia.ID_STATUS
            ).outerjoin(
                PenOficios,
                PenDetalhamento.NU_OFICIO == PenOficios.NU_OFICIO
            ).filter(
                PenDetalhamento.NU_CONTRATO == nu_contrato_decimal
            ).all()

            # Para buscar na tabela analítico, usar o número como string
            nu_contrato_str = str(int(nu_contrato_decimal))

            # Buscar registros analíticos
            analiticos = AexAnalitico.query.filter(
                AexAnalitico.NU_CONTRATO == nu_contrato_str
            ).all()

            # Buscar todas as vinculações existentes para todas as pendências encontradas
            ids_pendencias = [p.PenDetalhamento.ID_DETALHAMENTO for p in pendencias]

            # Inicializar o dicionário vazio
            vinculacoes_por_pendencia = {}
            pendencias_com_vinculacao = set()  # Novo: set para pendências que já têm vinculação

            if ids_pendencias:  # Só buscar se houver pendências
                vinculacoes_existentes = PenRelacionaVlrRetido.query.filter(
                    PenRelacionaVlrRetido.ID_PENDENCIA.in_(ids_pendencias)
                ).all()

                # Criar dicionário de vinculações por pendência
                for v in vinculacoes_existentes:
                    if v.ID_PENDENCIA not in vinculacoes_por_pendencia:
                        vinculacoes_por_pendencia[v.ID_PENDENCIA] = []
                    vinculacoes_por_pendencia[v.ID_PENDENCIA].append(v.ID_ARREC_EXT_SISTEMA)

                    # Adicionar ao set de pendências com vinculação
                    pendencias_com_vinculacao.add(v.ID_PENDENCIA)

            return render_template(
                'pendencia_retencao/resultado_consulta.html',
                pendencias=pendencias,
                analiticos=analiticos,
                vinculacoes_por_pendencia=vinculacoes_por_pendencia,
                pendencias_com_vinculacao=pendencias_com_vinculacao,  # Novo parâmetro
                nu_contrato=nu_contrato
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
        # Pegar filtro de carteira da query string
        carteira_filtro = request.args.get('carteira', '')

        # Query base
        query = db.session.query(
            PenDetalhamento,
            PenCarteiras.DSC_CARTEIRA,
            PenOcorrencias.DSC_OCORRENCIA,
            PenStatusOcorrencia.DSC_STATUS,
            PenOficios.DT_OFICIO
        ).join(
            PenCarteiras,
            PenDetalhamento.ID_CARTEIRA == PenCarteiras.ID_CARTEIRA
        ).join(
            PenStatusOcorrencia,
            PenDetalhamento.ID_STATUS == PenStatusOcorrencia.ID_STATUS
        ).outerjoin(
            PenOcorrencias,
            PenDetalhamento.ID_OCORRENCIA == PenOcorrencias.ID_OCORRENCIA
        ).outerjoin(
            PenOficios,
            PenDetalhamento.NU_OFICIO == PenOficios.NU_OFICIO
        ).filter(
            PenDetalhamento.VR_REAL_FALHA < 0,
            PenDetalhamento.IC_EXCLUIR == None
        )

        # Aplicar filtro de carteira se fornecido
        if carteira_filtro:
            query = query.filter(PenCarteiras.DSC_CARTEIRA.like(f'%{carteira_filtro}%'))

        contratos = query.order_by(
            PenDetalhamento.NU_CONTRATO.desc()
        ).all()

        # Buscar lista de carteiras únicas para o filtro
        carteiras_unicas = db.session.query(
            PenCarteiras.DSC_CARTEIRA
        ).join(
            PenDetalhamento,
            PenDetalhamento.ID_CARTEIRA == PenCarteiras.ID_CARTEIRA
        ).filter(
            PenDetalhamento.VR_REAL_FALHA < 0,
            PenDetalhamento.IC_EXCLUIR == None
        ).distinct().order_by(
            PenCarteiras.DSC_CARTEIRA
        ).all()

        # Buscar contratos com vinculações (código anterior)
        contratos_com_vinculacao = []

        vinculacoes = db.session.query(
            PenDetalhamento.NU_CONTRATO,
            AexAnalitico.NU_CONTRATO
        ).select_from(
            PenRelacionaVlrRetido
        ).outerjoin(
            PenDetalhamento,
            PenRelacionaVlrRetido.ID_PENDENCIA == PenDetalhamento.ID_DETALHAMENTO
        ).outerjoin(
            AexAnalitico,
            PenRelacionaVlrRetido.ID_ARREC_EXT_SISTEMA == AexAnalitico.ID
        ).distinct().all()

        for v in vinculacoes:
            if v[0]:
                contratos_com_vinculacao.append(v[0])
            elif v[1]:
                try:
                    contrato_decimal = Decimal(v[1])
                    contratos_com_vinculacao.append(contrato_decimal)
                except:
                    pass

        return render_template(
            'pendencia_retencao/listar_contratos.html',
            contratos=contratos,
            contratos_com_vinculacao=contratos_com_vinculacao,
            carteiras_unicas=[c[0] for c in carteiras_unicas],
            carteira_selecionada=carteira_filtro
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
        apenas_observacao = data.get('apenas_observacao', False)

        contador = 0

        # Se for apenas observação (sem analíticos disponíveis)
        if apenas_observacao:
            if ids_pendencias:
                # Se há pendências selecionadas, salvar cada uma com ID_ARREC_EXT_SISTEMA = NULL
                for id_pendencia in ids_pendencias:
                    sql_insert = text("""
                        INSERT INTO BDG.PEN_TB010_RELACIONA_VLR_RETIDO 
                        (ID_PENDENCIA, ID_ARREC_EXT_SISTEMA, OBS, NO_RSPONSAVEL, DT_ANALISE)
                        VALUES (:id_pend, NULL, :obs, :responsavel, :dt_analise)
                    """)

                    db.session.execute(sql_insert, {
                        'id_pend': id_pendencia,
                        'obs': observacao,
                        'responsavel': current_user.nome,
                        'dt_analise': datetime.now()
                    })
                    contador += 1
            else:
                # Se não há pendências nem analíticos, salvar apenas observação
                sql_insert = text("""
                    INSERT INTO BDG.PEN_TB010_RELACIONA_VLR_RETIDO 
                    (ID_PENDENCIA, ID_ARREC_EXT_SISTEMA, OBS, NO_RSPONSAVEL, DT_ANALISE)
                    VALUES (NULL, NULL, :obs, :responsavel, :dt_analise)
                """)

                db.session.execute(sql_insert, {
                    'obs': observacao,
                    'responsavel': current_user.nome,
                    'dt_analise': datetime.now()
                })
                contador = 1

            db.session.commit()

            registrar_log(
                acao='criar',
                entidade='pendencia_retencao',
                entidade_id=f"{','.join(map(str, ids_pendencias)) if ids_pendencias else 'APENAS_OBSERVACAO'}",
                descricao=f'Observação registrada sem valores retidos - {contador} registros',
                dados_novos={
                    'ids_pendencias': ids_pendencias,
                    'observacao': observacao
                }
            )

            return jsonify({
                'success': True,
                'message': f'{contador} registro(s) salvos com sucesso (sem valores retidos)!'
            })

        # Resto do código para quando há analíticos disponíveis
        if not ids_analiticos:
            return jsonify({
                'success': False,
                'message': 'Selecione ao menos um registro analítico.'
            }), 400

        if ids_pendencias:
            # Se houver pendências, criar vinculação normal
            for id_pendencia in ids_pendencias:
                for id_analitico in ids_analiticos:
                    # Verificar se já existe
                    sql_check = text("""
                        SELECT COUNT(*) FROM BDG.PEN_TB010_RELACIONA_VLR_RETIDO 
                        WHERE ID_PENDENCIA = :id_pend AND ID_ARREC_EXT_SISTEMA = :id_arrec
                    """)

                    result = db.session.execute(sql_check, {
                        'id_pend': id_pendencia,
                        'id_arrec': id_analitico
                    }).scalar()

                    if result == 0:
                        sql_insert = text("""
                            INSERT INTO BDG.PEN_TB010_RELACIONA_VLR_RETIDO 
                            (ID_PENDENCIA, ID_ARREC_EXT_SISTEMA, OBS, NO_RSPONSAVEL, DT_ANALISE)
                            VALUES (:id_pend, :id_arrec, :obs, :responsavel, :dt_analise)
                        """)

                        db.session.execute(sql_insert, {
                            'id_pend': id_pendencia,
                            'id_arrec': id_analitico,
                            'obs': observacao,
                            'responsavel': current_user.nome,
                            'dt_analise': datetime.now()
                        })
                        contador += 1
        else:
            # Se não houver pendências, criar vinculação com ID_PENDENCIA = NULL
            for id_analitico in ids_analiticos:
                sql_check = text("""
                    SELECT COUNT(*) FROM BDG.PEN_TB010_RELACIONA_VLR_RETIDO 
                    WHERE ID_PENDENCIA IS NULL AND ID_ARREC_EXT_SISTEMA = :id_arrec
                """)

                result = db.session.execute(sql_check, {'id_arrec': id_analitico}).scalar()

                if result == 0:
                    sql_insert = text("""
                        INSERT INTO BDG.PEN_TB010_RELACIONA_VLR_RETIDO 
                        (ID_PENDENCIA, ID_ARREC_EXT_SISTEMA, OBS, NO_RSPONSAVEL, DT_ANALISE)
                        VALUES (NULL, :id_arrec, :obs, :responsavel, :dt_analise)
                    """)

                    db.session.execute(sql_insert, {
                        'id_arrec': id_analitico,
                        'obs': observacao,
                        'responsavel': current_user.nome,
                        'dt_analise': datetime.now()
                    })
                    contador += 1

        db.session.commit()

        registrar_log(
            acao='criar',
            entidade='pendencia_retencao',
            entidade_id=f"{','.join(map(str, ids_pendencias)) if ids_pendencias else 'SEM_PENDENCIA'}",
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
        # Buscar todas as vinculações com informações relacionadas
        vinculacoes = db.session.query(
            PenRelacionaVlrRetido,
            PenDetalhamento,
            AexAnalitico,
            PenCarteiras.DSC_CARTEIRA,
            PenOcorrencias.DSC_OCORRENCIA,
            PenStatusOcorrencia.DSC_STATUS
        ).outerjoin(  # Mudado para outerjoin para pegar vinculações sem pendência
            PenDetalhamento,
            PenRelacionaVlrRetido.ID_PENDENCIA == PenDetalhamento.ID_DETALHAMENTO
        ).join(
            AexAnalitico,
            PenRelacionaVlrRetido.ID_ARREC_EXT_SISTEMA == AexAnalitico.ID
        ).outerjoin(
            PenCarteiras,
            PenDetalhamento.ID_CARTEIRA == PenCarteiras.ID_CARTEIRA
        ).outerjoin(
            PenOcorrencias,
            PenDetalhamento.ID_OCORRENCIA == PenOcorrencias.ID_OCORRENCIA
        ).outerjoin(
            PenStatusOcorrencia,
            PenDetalhamento.ID_STATUS == PenStatusOcorrencia.ID_STATUS
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


@pendencia_retencao_bp.route('/excluir-vinculacao', methods=['POST'])
@login_required
def excluir_vinculacao():
    """Excluir uma vinculação específica"""
    try:
        data = request.get_json()
        id_pendencia = data.get('id_pendencia')
        id_arrec_ext_sistema = data.get('id_arrec_ext_sistema')

        if not id_arrec_ext_sistema:
            return jsonify({
                'success': False,
                'message': 'ID do analítico inválido.'
            }), 400

        # Modificado para permitir ID_PENDENCIA null
        if id_pendencia:
            vinculacao = PenRelacionaVlrRetido.query.filter_by(
                ID_PENDENCIA=id_pendencia,
                ID_ARREC_EXT_SISTEMA=id_arrec_ext_sistema
            ).first()
        else:
            vinculacao = PenRelacionaVlrRetido.query.filter(
                PenRelacionaVlrRetido.ID_PENDENCIA == None,
                PenRelacionaVlrRetido.ID_ARREC_EXT_SISTEMA == id_arrec_ext_sistema
            ).first()

        if not vinculacao:
            return jsonify({
                'success': False,
                'message': 'Vinculação não encontrada.'
            }), 404

        registrar_log(
            acao='excluir',
            entidade='pendencia_retencao',
            entidade_id=f"{id_pendencia or 'NULL'}-{id_arrec_ext_sistema}",
            descricao='Exclusão de vinculação',
            dados_antigos={
                'id_pendencia': id_pendencia,
                'id_arrec_ext_sistema': id_arrec_ext_sistema,
                'observacao': vinculacao.OBS,
                'responsavel': vinculacao.NO_RSPONSAVEL
            }
        )

        db.session.delete(vinculacao)
        db.session.commit()

        return jsonify({
            'success': True,
            'message': 'Vinculação excluída com sucesso!'
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Erro ao excluir vinculação: {str(e)}'
        }), 500

