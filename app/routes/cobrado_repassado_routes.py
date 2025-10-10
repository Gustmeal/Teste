from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from app import db
from app.models.pendencia_retencao import (
    PenDetalhamento, AexAnalitico, PenRelacionaVlrRepassado,
    PenCarteiras, PenOcorrencias, PenStatusOcorrencia, PenOficios
)
from decimal import Decimal
from datetime import datetime
from sqlalchemy import text
from app.utils.audit import registrar_log

cobrado_repassado_bp = Blueprint('cobrado_repassado', __name__, url_prefix='/cobrado-repassado')


@cobrado_repassado_bp.route('/')
@login_required
def index():
    """Página inicial do módulo Cobrado vs Repassado"""
    return render_template('cobrado_repassado/index.html')


@cobrado_repassado_bp.route('/consultar', methods=['GET', 'POST'])
@login_required
def consultar():
    """Consultar pendências e analíticos por número de contrato"""
    if request.method == 'POST':
        nu_contrato = request.form.get('nu_contrato', '').strip()

        if not nu_contrato:
            flash('Por favor, informe o número do contrato.', 'warning')
            return redirect(url_for('cobrado_repassado.consultar'))

        try:
            # Converter string para decimal para buscar na tabela de pendências
            try:
                nu_contrato_decimal = Decimal(nu_contrato)
            except:
                flash('Número de contrato inválido.', 'danger')
                return redirect(url_for('cobrado_repassado.consultar'))

            # Buscar pendências na nova tabela PEN_TB013
            # MUDANÇA: Removido filtro VR_REAL_FALHA > 0 pois não existe mais
            # MUDANÇA: Usando VR_FALHA > 0 para filtrar pendências com valores cobrados da Caixa
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
                PenDetalhamento.NU_CONTRATO == nu_contrato_decimal,
                PenDetalhamento.VR_FALHA > 0  # MUDANÇA: usar VR_FALHA ao invés de VR_REAL_FALHA
            ).all()

            # Para buscar na tabela analítico, usar o número como string
            nu_contrato_str = str(int(nu_contrato_decimal))

            # Buscar registros analíticos com VALOR > 0
            analiticos = AexAnalitico.query.filter(
                AexAnalitico.NU_CONTRATO == nu_contrato_str,
                AexAnalitico.VALOR > 0
            ).all()

            # Buscar todas as vinculações existentes
            ids_pendencias = [p.PenDetalhamento.ID_DETALHAMENTO for p in pendencias]

            vinculacoes_por_pendencia = {}
            pendencias_com_vinculacao = set()

            if ids_pendencias:
                vinculacoes_existentes = PenRelacionaVlrRepassado.query.filter(
                    PenRelacionaVlrRepassado.ID_PENDENCIA.in_(ids_pendencias)
                ).all()

                for v in vinculacoes_existentes:
                    if v.ID_PENDENCIA not in vinculacoes_por_pendencia:
                        vinculacoes_por_pendencia[v.ID_PENDENCIA] = []
                    vinculacoes_por_pendencia[v.ID_PENDENCIA].append(v.ID_ARREC_EXT_SISTEMA)
                    pendencias_com_vinculacao.add(v.ID_PENDENCIA)

            return render_template(
                'cobrado_repassado/resultado_consulta.html',
                pendencias=pendencias,
                analiticos=analiticos,
                vinculacoes_por_pendencia=vinculacoes_por_pendencia,
                pendencias_com_vinculacao=pendencias_com_vinculacao,
                nu_contrato=nu_contrato
            )

        except Exception as e:
            flash(f'Erro ao consultar dados: {str(e)}', 'danger')
            return redirect(url_for('cobrado_repassado.consultar'))

    return render_template('cobrado_repassado/consultar.html')


@cobrado_repassado_bp.route('/listar-contratos')
@login_required
def listar_contratos():
    """Listar contratos disponíveis para seleção"""
    try:
        # Pegar filtro de carteira da query string
        carteira_filtro = request.args.get('carteira', '')

        # Query base na nova tabela PEN_TB013
        # MUDANÇA: Removido filtro VR_REAL_FALHA > 0 e IC_EXCLUIR == None
        # Agora usa VR_FALHA > 0 para pegar contratos com pendências
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
            PenDetalhamento.VR_FALHA > 0  # MUDANÇA: usar VR_FALHA
        )

        if carteira_filtro:
            query = query.filter(PenCarteiras.DSC_CARTEIRA.like(f'%{carteira_filtro}%'))

        contratos = query.order_by(
            PenDetalhamento.NU_CONTRATO.desc()
        ).all()

        # Buscar lista de carteiras únicas
        carteiras_unicas = db.session.query(
            PenCarteiras.DSC_CARTEIRA
        ).join(
            PenDetalhamento,
            PenDetalhamento.ID_CARTEIRA == PenCarteiras.ID_CARTEIRA
        ).filter(
            PenDetalhamento.VR_FALHA > 0  # MUDANÇA: usar VR_FALHA
        ).distinct().order_by(
            PenCarteiras.DSC_CARTEIRA
        ).all()

        # Buscar contratos com vinculações
        contratos_com_vinculacao = []

        vinculacoes = db.session.query(
            PenDetalhamento.NU_CONTRATO,
            AexAnalitico.NU_CONTRATO
        ).select_from(
            PenRelacionaVlrRepassado
        ).outerjoin(
            PenDetalhamento,
            PenRelacionaVlrRepassado.ID_PENDENCIA == PenDetalhamento.ID_DETALHAMENTO
        ).outerjoin(
            AexAnalitico,
            PenRelacionaVlrRepassado.ID_ARREC_EXT_SISTEMA == AexAnalitico.ID
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
            'cobrado_repassado/listar_contratos.html',
            contratos=contratos,
            contratos_com_vinculacao=contratos_com_vinculacao,
            carteiras_unicas=[c[0] for c in carteiras_unicas],
            carteira_selecionada=carteira_filtro
        )

    except Exception as e:
        flash(f'Erro ao listar contratos: {str(e)}', 'danger')
        return redirect(url_for('cobrado_repassado.index'))


@cobrado_repassado_bp.route('/consultar-contrato/<nu_contrato>')
@login_required
def consultar_contrato_direto(nu_contrato):
    """Redireciona para consulta com o contrato já preenchido"""
    return redirect(url_for('cobrado_repassado.consultar', nu_contrato=nu_contrato))


@cobrado_repassado_bp.route('/salvar-vinculacao', methods=['POST'])
@login_required
def salvar_vinculacao():
    """Salvar vinculação entre valores cobrados e repassados"""
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
                for id_pendencia in ids_pendencias:
                    sql_insert = text("""
                        INSERT INTO BDG.PEN_TB011_RELACIONA_VLR_REPASSADO 
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
                sql_insert = text("""
                    INSERT INTO BDG.PEN_TB011_RELACIONA_VLR_REPASSADO 
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
                entidade='cobrado_repassado',
                entidade_id=f"{','.join(map(str, ids_pendencias)) if ids_pendencias else 'APENAS_OBSERVACAO'}",
                descricao=f'Observação registrada sem valores repassados - {contador} registros',
                dados_novos={
                    'ids_pendencias': ids_pendencias,
                    'observacao': observacao
                }
            )

            return jsonify({
                'success': True,
                'message': f'{contador} registro(s) salvos com sucesso (sem valores repassados)!'
            })

        # Resto do código para quando há analíticos
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
                        SELECT COUNT(*) FROM BDG.PEN_TB011_RELACIONA_VLR_REPASSADO 
                        WHERE ID_PENDENCIA = :id_pend AND ID_ARREC_EXT_SISTEMA = :id_arrec
                    """)

                    result = db.session.execute(sql_check, {
                        'id_pend': id_pendencia,
                        'id_arrec': id_analitico
                    }).scalar()

                    if result == 0:
                        sql_insert = text("""
                            INSERT INTO BDG.PEN_TB011_RELACIONA_VLR_REPASSADO 
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
                    SELECT COUNT(*) FROM BDG.PEN_TB011_RELACIONA_VLR_REPASSADO 
                    WHERE ID_PENDENCIA IS NULL AND ID_ARREC_EXT_SISTEMA = :id_arrec
                """)

                result = db.session.execute(sql_check, {'id_arrec': id_analitico}).scalar()

                if result == 0:
                    sql_insert = text("""
                        INSERT INTO BDG.PEN_TB011_RELACIONA_VLR_REPASSADO 
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
            entidade='cobrado_repassado',
            entidade_id=f"{','.join(map(str, ids_pendencias)) if ids_pendencias else 'SEM_PENDENCIAS'}",
            descricao=f'Vinculação de valores cobrados e repassados - {contador} registros',
            dados_novos={
                'ids_pendencias': ids_pendencias,
                'ids_analiticos': ids_analiticos,
                'observacao': observacao
            }
        )

        return jsonify({
            'success': True,
            'message': f'{contador} vinculação(ões) salva(s) com sucesso!'
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Erro ao salvar vinculação: {str(e)}'
        }), 500


@cobrado_repassado_bp.route('/listar-vinculacoes')
@login_required
def listar_vinculacoes():
    """Listar todas as vinculações realizadas"""
    try:
        # Buscar todas as vinculações com informações relacionadas
        vinculacoes = db.session.query(
            PenRelacionaVlrRepassado,
            PenDetalhamento,
            AexAnalitico,
            PenCarteiras.DSC_CARTEIRA,
            PenOcorrencias.DSC_OCORRENCIA,
            PenStatusOcorrencia.DSC_STATUS,
            PenOficios.NU_OFICIO,
            PenOficios.DT_OFICIO
        ).outerjoin(
            PenDetalhamento,
            PenRelacionaVlrRepassado.ID_PENDENCIA == PenDetalhamento.ID_DETALHAMENTO
        ).join(
            AexAnalitico,
            PenRelacionaVlrRepassado.ID_ARREC_EXT_SISTEMA == AexAnalitico.ID
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
        ).order_by(
            PenRelacionaVlrRepassado.DT_ANALISE.desc()
        ).all()

        return render_template(
            'cobrado_repassado/listar_vinculacoes.html',
            vinculacoes=vinculacoes
        )

    except Exception as e:
        flash(f'Erro ao listar vinculações: {str(e)}', 'danger')
        return redirect(url_for('cobrado_repassado.index'))


@cobrado_repassado_bp.route('/excluir-vinculacao', methods=['POST'])
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
            vinculacao = PenRelacionaVlrRepassado.query.filter_by(
                ID_PENDENCIA=id_pendencia,
                ID_ARREC_EXT_SISTEMA=id_arrec_ext_sistema
            ).first()
        else:
            vinculacao = PenRelacionaVlrRepassado.query.filter(
                PenRelacionaVlrRepassado.ID_PENDENCIA == None,
                PenRelacionaVlrRepassado.ID_ARREC_EXT_SISTEMA == id_arrec_ext_sistema
            ).first()

        if not vinculacao:
            return jsonify({
                'success': False,
                'message': 'Vinculação não encontrada.'
            }), 404

        db.session.delete(vinculacao)
        db.session.commit()

        registrar_log(
            acao='excluir',
            entidade='cobrado_repassado',
            entidade_id=f"{id_pendencia}_{id_arrec_ext_sistema}",
            descricao=f'Exclusão de vinculação entre pendência {id_pendencia} e analítico {id_arrec_ext_sistema}'
        )

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