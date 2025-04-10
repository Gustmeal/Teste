from flask import Blueprint, render_template, redirect, url_for, request, flash
from app.models.limite_distribuicao import LimiteDistribuicao
from app.models.edital import Edital
from app.models.periodo import PeriodoAvaliacao
from app.models.empresa_participante import EmpresaParticipante
from app.models.criterio_selecao import CriterioSelecao
from app.models.audit_log import AuditLog
from app import db
from datetime import datetime
from flask_login import login_required, current_user
from app.utils.audit import registrar_log
from sqlalchemy import or_, func, text
import pandas as pd
import numpy as np

limite_bp = Blueprint('limite', __name__, url_prefix='/credenciamento')


@limite_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


def truncate_decimal(value, decimal_places=2):
    """Trunca o valor para o número especificado de casas decimais sem arredondamento."""
    factor = 10 ** decimal_places
    return int(value * factor) / factor


@limite_bp.route('/limites')
@login_required
def lista_limites():
    # Obter parâmetros de filtro
    periodo_id = request.args.get('periodo_id', type=int)
    criterio_id = request.args.get('criterio_id', type=int)

    # Consulta base com join para obter a descrição do critério
    query = db.session.query(
        LimiteDistribuicao,
        CriterioSelecao.DS_CRITERIO_SELECAO
    ).outerjoin(
        CriterioSelecao,
        LimiteDistribuicao.COD_CRITERIO_SELECAO == CriterioSelecao.COD
    ).filter(
        LimiteDistribuicao.DELETED_AT == None,
        or_(CriterioSelecao.DELETED_AT == None, CriterioSelecao.DELETED_AT.is_(None))
    )

    # Aplicar filtros se fornecidos
    if periodo_id:
        query = query.filter(LimiteDistribuicao.ID_PERIODO == periodo_id)

    if criterio_id:
        query = query.filter(LimiteDistribuicao.COD_CRITERIO_SELECAO == criterio_id)

    # Executar consulta
    results = query.all()

    # Processar resultados para incluir dados da empresa e descrição do critério
    limites = []
    for limite, ds_criterio in results:
        # Para cada limite, buscar todas as ocorrências da empresa (não apenas uma)
        empresas = EmpresaParticipante.query.filter_by(ID_EMPRESA=limite.ID_EMPRESA).all()

        empresa_nome = None
        empresa_nome_abreviado = None

        # Se encontrou alguma empresa, usar os dados dela
        if empresas:
            for empresa in empresas:
                if empresa.NO_EMPRESA:
                    empresa_nome = empresa.NO_EMPRESA
                    empresa_nome_abreviado = empresa.NO_EMPRESA_ABREVIADA
                    break

        # Se não encontrou na tabela de participantes, buscar na tabela complementar (se existir)
        if not empresa_nome:
            try:
                # Opcional: Tentar buscar em outra tabela se disponível
                from app.models.empresa_responsavel import EmpresaResponsavel
                empresa_resp = EmpresaResponsavel.query.filter_by(
                    pkEmpresaResponsavelCobranca=limite.ID_EMPRESA
                ).first()

                if empresa_resp:
                    empresa_nome = empresa_resp.nmEmpresaResponsavelCobranca
                    empresa_nome_abreviado = empresa_resp.NO_ABREVIADO_EMPRESA
            except:
                # Em caso de erro ou tabela não disponível, usar ID como nome
                empresa_nome = f"Empresa ID {limite.ID_EMPRESA}"
                empresa_nome_abreviado = f"ID {limite.ID_EMPRESA}"

        # Armazenar informações da empresa no objeto limite
        limite.empresa_nome = empresa_nome or f"Empresa ID {limite.ID_EMPRESA}"
        limite.empresa_nome_abreviado = empresa_nome_abreviado

        # Adicionar descrição do critério
        limite.criterio_descricao = ds_criterio if ds_criterio else f"Critério {limite.COD_CRITERIO_SELECAO}"

        limites.append(limite)

    # Obter todos os períodos para o filtro
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).all()

    # Obter todos os critérios para o filtro
    criterios = CriterioSelecao.query.filter(CriterioSelecao.DELETED_AT == None).all()

    return render_template(
        'credenciamento/lista_limites.html',
        limites=limites,
        periodos=periodos,
        criterios=criterios,
        filtro_periodo_id=periodo_id,
        filtro_criterio_id=criterio_id
    )


def selecionar_contratos():
    """
    Seleciona o universo de contratos que serão distribuídos e os armazena na tabela DCA_TB006_DISTRIBUIVEIS.
    Usa as tabelas do Banco de Dados Gerencial (BDG).
    Retorna a quantidade de contratos selecionados.
    """
    try:
        # Usar uma conexão direta para executar o SQL
        with db.engine.connect() as connection:
            try:
                # Primeiro, limpar a tabela de distribuíveis
                truncate_sql = text("TRUNCATE TABLE [DEV].[DCA_TB006_DISTRIBUIVEIS]")
                connection.execute(truncate_sql)


                # Em seguida, inserir os contratos selecionados
                insert_sql = text("""
                INSERT INTO [DEV].[DCA_TB006_DISTRIBUIVEIS]
                SELECT
                    ECA.fkContratoSISCTR
                    , CON.NR_CPF_CNPJ
                    , SIT.VR_SD_DEVEDOR
                    , CREATED_AT = GETDATE()
                    , UPDATED_AT = NULL
                    , DELETED_AT = NULL
                FROM 
                    [BDG].[COM_TB011_EMPRESA_COBRANCA_ATUAL] AS ECA
                
                    INNER JOIN [BDG].[COM_TB001_CONTRATO] AS CON
                        ON ECA.fkContratoSISCTR = CON.fkContratoSISCTR
                
                    INNER JOIN [BDG].[COM_TB007_SITUACAO_CONTRATOS] AS SIT
                        ON ECA.fkContratoSISCTR = SIT.fkContratoSISCTR
                
                    LEFT JOIN [BDG].[COM_TB013_SUSPENSO_DECISAO_JUDICIAL] AS SDJ
                        ON ECA.fkContratoSISCTR = SDJ.fkContratoSISCTR
                WHERE
                    SIT.[fkSituacaoCredito] = 1
                    AND SDJ.fkContratoSISCTR IS NULL""")

                connection.execute(insert_sql)


                # Contar quantos contratos foram selecionados
                count_sql = text("SELECT COUNT(*) FROM [DEV].[DCA_TB006_DISTRIBUIVEIS] WHERE DELETED_AT IS NULL")
                result = connection.execute(count_sql)
                num_contratos = result.scalar()


                return num_contratos

            except Exception as e:
                print(f"Erro durante a execução das consultas SQL: {str(e)}")
                # Log mais detalhado caso ocorra erro
                import traceback
                print(traceback.format_exc())
                raise

    except Exception as e:
        print(f"Erro na seleção de contratos: {str(e)}")
        db.session.rollback()
        return 0


@limite_bp.route('/limites/analise')
@login_required
def analise_limites():
    try:
        # Primeiro, selecionar os contratos distribuíveis
        num_contratos = selecionar_contratos()
        print(f"Contratos selecionados: {num_contratos}")

        # Obter o edital mais recente
        ultimo_edital = Edital.query.filter(Edital.DELETED_AT == None).order_by(Edital.ID.desc()).first()

        if not ultimo_edital:
            flash('Não foram encontrados editais cadastrados.', 'warning')
            return redirect(url_for('edital.lista_editais'))

        # Obter o período mais recente
        ultimo_periodo = PeriodoAvaliacao.query.filter(
            PeriodoAvaliacao.DELETED_AT == None,
            PeriodoAvaliacao.ID_EDITAL == ultimo_edital.ID
        ).order_by(PeriodoAvaliacao.ID_PERIODO.desc()).first()

        if not ultimo_periodo:
            flash('Não foram encontrados períodos para o edital mais recente.', 'warning')
            return redirect(url_for('periodo.lista_periodos'))

        # Buscar empresas participantes do último período
        empresas = EmpresaParticipante.query.filter(
            EmpresaParticipante.ID_EDITAL == ultimo_edital.ID,
            EmpresaParticipante.ID_PERIODO == ultimo_periodo.ID_PERIODO,
            EmpresaParticipante.DELETED_AT == None
        ).all()

        if not empresas:
            flash('Não foram encontradas empresas participantes para o período atual.', 'warning')
            return redirect(url_for('empresa.lista_empresas', periodo_id=ultimo_periodo.ID))

        # Analisar condições das empresas
        todas_permanece = all(empresa.DS_CONDICAO == 'PERMANECE' for empresa in empresas)
        todas_novas = all(empresa.DS_CONDICAO == 'NOVA' for empresa in empresas)
        alguma_permanece = any(empresa.DS_CONDICAO == 'PERMANECE' for empresa in empresas)

        # Período anterior para cálculos
        periodo_anterior = None
        if ultimo_periodo.ID_PERIODO > 1:
            periodo_anterior = PeriodoAvaliacao.query.filter(
                PeriodoAvaliacao.ID_EDITAL == ultimo_edital.ID,
                PeriodoAvaliacao.ID_PERIODO < ultimo_periodo.ID_PERIODO,
                PeriodoAvaliacao.DELETED_AT == None
            ).order_by(PeriodoAvaliacao.ID_PERIODO.desc()).first()

        # Resultados do cálculo
        resultados_calculo = []

        # Se todas as empresas são PERMANECE, aplicar o cálculo 3.3.1
        if todas_permanece:
            # Verificar se há período anterior para cálculos
            if periodo_anterior:
                try:
                    # Criar conexão direta com o banco para obter os dados de arrecadação
                    with db.engine.connect() as connection:
                        # Consulta para obter arrecadação das empresas no período anterior
                        sql = text("""
                        SELECT 
                            EP.ID_EMPRESA,
                            EP.NO_EMPRESA,
                            EP.NO_EMPRESA_ABREVIADA,
                            EP.DS_CONDICAO,
                            -- Buscar dados de arrecadação da tabela real
                            COALESCE(REE.VR_ARRECADACAO, 0) AS VR_ARRECADACAO
                        FROM 
                            DEV.DCA_TB002_EMPRESAS_PARTICIPANTES AS EP
                        LEFT JOIN (
                            SELECT 
                                REE.CO_EMPRESA_COBRANCA,
                                SUM(REE.VR_ARRECADACAO_TOTAL) AS VR_ARRECADACAO
                            FROM 
                                BDG.COM_TB062_REMUNERACAO_ESTIMADA AS REE
                            WHERE 
                                REE.DT_ARRECADACAO BETWEEN :data_inicio AND :data_fim
                            GROUP BY 
                                REE.CO_EMPRESA_COBRANCA
                        ) AS REE ON EP.ID_EMPRESA = REE.CO_EMPRESA_COBRANCA
                        WHERE 
                            EP.ID_EDITAL = :id_edital 
                            AND EP.ID_PERIODO = :id_periodo 
                            AND EP.DS_CONDICAO = 'PERMANECE'
                            AND EP.DELETED_AT IS NULL
                        ORDER BY
                            VR_ARRECADACAO DESC
                        """).bindparams(
                            id_edital=ultimo_edital.ID,
                            id_periodo=ultimo_periodo.ID_PERIODO,
                            data_inicio=periodo_anterior.DT_INICIO,
                            data_fim=periodo_anterior.DT_FIM
                        )

                        # Executar a consulta
                        result = connection.execute(sql)
                        rows = result.fetchall()

                        if not rows:
                            flash('Não foram encontrados dados de arrecadação para o período anterior.', 'warning')
                            return redirect(url_for('limite.lista_limites'))

                        # Processar dados da arrecadação real
                        dados_arrecadacao = []
                        for row in rows:
                            dados_arrecadacao.append({
                                'id_empresa': row[0],
                                'nome': row[1],
                                'nome_abreviado': row[2] or (row[1][0] if row[1] else ''),
                                'situacao': row[3],
                                'arrecadacao': float(row[4]) if row[4] else 0.0
                            })

                        # Ordenar por arrecadação (maior para menor)
                        dados_arrecadacao.sort(key=lambda x: x['arrecadacao'], reverse=True)

                        # Calcular total de arrecadação
                        total_arrecadacao = sum(item['arrecadacao'] for item in dados_arrecadacao)

                        # Calcular percentuais (truncados, sem arredondamento)
                        dados_processados = []
                        for idx, item in enumerate(dados_arrecadacao):
                            # Calcular percentual bruto e truncar para duas casas decimais
                            pct_arrecadacao = truncate_decimal((item['arrecadacao'] / total_arrecadacao) * 100)

                            # Adicionar dados processados
                            dados_processados.append({
                                'idx': idx + 1,
                                'id_empresa': item['id_empresa'],
                                'empresa': item['nome_abreviado'],
                                'situacao': item['situacao'],
                                'arrecadacao': item['arrecadacao'],
                                'pct_arrecadacao': pct_arrecadacao,
                                'ajuste': 0.00,  # Será atualizado depois
                                'pct_final': pct_arrecadacao  # Será atualizado depois
                            })

                        # Calcular a soma dos percentuais truncados
                        soma_percentuais = sum(item['pct_arrecadacao'] for item in dados_processados)

                        # Calcular quanto falta para chegar a 100%
                        diferenca = truncate_decimal(100.00 - soma_percentuais)

                        # Aplicar ajuste de 0.01% às empresas com maior arrecadação até chegar exatamente a 100%
                        if diferenca > 0:
                            # Quantas empresas precisam ser ajustadas inicialmente (um "ciclo" completo de empresas)
                            ajuste_por_empresa = 0.01
                            num_ciclos_completos = int(diferenca / (ajuste_por_empresa * len(dados_processados)))
                            ajustes_restantes = int(
                                (diferenca % (ajuste_por_empresa * len(dados_processados))) / ajuste_por_empresa)

                            # Aplicar ajustes para ciclos completos (todas as empresas recebem ajuste)
                            if num_ciclos_completos > 0:
                                for i in range(len(dados_processados)):
                                    dados_processados[i]['ajuste'] = num_ciclos_completos * ajuste_por_empresa
                                    dados_processados[i]['pct_final'] = truncate_decimal(
                                        dados_processados[i]['pct_arrecadacao'] + dados_processados[i]['ajuste'])

                            # Aplicar ajustes restantes (apenas algumas empresas recebem ajuste adicional)
                            for i in range(ajustes_restantes):
                                # O índice será sempre menor que o número de empresas
                                indice = i % len(dados_processados)
                                dados_processados[indice]['ajuste'] += ajuste_por_empresa
                                dados_processados[indice]['pct_final'] = truncate_decimal(
                                    dados_processados[indice]['pct_arrecadacao'] + dados_processados[indice]['ajuste'])

                            # Recalcular totais
                            total_pct_arrecadacao = sum(item['pct_arrecadacao'] for item in dados_processados)
                            total_ajuste = sum(item['ajuste'] for item in dados_processados)
                            total_pct_final = sum(item['pct_final'] for item in dados_processados)

                            # Verificar se o total final é exatamente 100%
                            if total_pct_final != 100.00:
                                # Se ainda não for exatamente 100%, ajustar a primeira empresa
                                diferenca_restante = 100.00 - total_pct_final
                                dados_processados[0]['pct_final'] = truncate_decimal(
                                    dados_processados[0]['pct_final'] + diferenca_restante)
                                dados_processados[0]['ajuste'] = truncate_decimal(
                                    dados_processados[0]['ajuste'] + diferenca_restante)

                                # Recalcular totais finais
                                total_ajuste = sum(item['ajuste'] for item in dados_processados)
                                total_pct_final = sum(item['pct_final'] for item in dados_processados)

                        # Adicionar linha de total
                        dados_processados.append({
                            'idx': 'TOTAL',
                            'id_empresa': '',
                            'empresa': 'TOTAL',
                            'situacao': '',
                            'arrecadacao': total_arrecadacao,
                            'pct_arrecadacao': soma_percentuais,
                            'ajuste': total_ajuste,
                            'pct_final': total_pct_final
                        })

                        resultados_calculo = dados_processados

                except Exception as e:
                    flash(f'Erro ao processar cálculo: {str(e)}', 'danger')
                    print(f"Erro detalhado: {e}")
                    import traceback
                    print(traceback.format_exc())
                    return redirect(url_for('limite.lista_limites'))
            else:
                flash('Não foi encontrado período anterior para realizar os cálculos.', 'warning')
        elif todas_novas:
            # Cálculo 3.3.2 será implementado posteriormente
            pass
        elif alguma_permanece:
            # Cálculo 3.3.3 será implementado posteriormente
            pass

        # Renderizar o template com os resultados da análise
        return render_template(
            'credenciamento/analise_limite.html',
            edital=ultimo_edital,
            periodo=ultimo_periodo,
            periodo_anterior=periodo_anterior,
            empresas=empresas,
            todas_permanece=todas_permanece,
            todas_novas=todas_novas,
            alguma_permanece=alguma_permanece,
            resultados_calculo=resultados_calculo,
            num_contratos=num_contratos  # Passar o número de contratos para o template
        )

    except Exception as e:
        flash(f'Erro durante a análise: {str(e)}', 'danger')
        import traceback
        print(traceback.format_exc())
        return redirect(url_for('limite.lista_limites'))


@limite_bp.route('/limites/salvar', methods=['POST'])
@login_required
def salvar_limites():
    try:
        # Obter dados do formulário
        edital_id = request.form.get('edital_id', type=int)
        periodo_id = request.form.get('periodo_id', type=int)
        data_apuracao = datetime.now()  # Data atual como data de apuração

        # Obter dados das empresas e seus percentuais calculados
        empresas_data = request.form.getlist('empresa_id[]')
        percentuais = request.form.getlist('percentual_final[]')
        arrecadacoes = request.form.getlist('arrecadacao[]')

        # Verificar se os arrays têm o mesmo tamanho
        if len(empresas_data) != len(percentuais) or len(empresas_data) != len(arrecadacoes):
            flash('Erro: Dados inconsistentes. Por favor, tente novamente.', 'danger')
            return redirect(url_for('limite.analise_limites'))

        # Verificar se há dados
        if not empresas_data:
            flash('Erro: Nenhum dado recebido para salvamento.', 'danger')
            return redirect(url_for('limite.analise_limites'))

        # Critério fixo conforme orientação
        cod_criterio = 7  # "Distribuição Percentual Global"

        # Excluir limites existentes com mesmo critério para este edital/período/empresas
        for empresa_id in empresas_data:
            limite_existente = LimiteDistribuicao.query.filter_by(
                ID_EDITAL=edital_id,
                ID_PERIODO=periodo_id,
                ID_EMPRESA=empresa_id,
                COD_CRITERIO_SELECAO=cod_criterio,
                DELETED_AT=None
            ).first()

            if limite_existente:
                limite_existente.DELETED_AT = datetime.utcnow()

                # Registrar log de exclusão do limite existente
                registrar_log(
                    acao='excluir',
                    entidade='limite',
                    entidade_id=limite_existente.ID,
                    descricao=f'Exclusão automática de limite existente para atualização',
                    dados_antigos={
                        'id_edital': limite_existente.ID_EDITAL,
                        'id_periodo': limite_existente.ID_PERIODO,
                        'id_empresa': limite_existente.ID_EMPRESA,
                        'cod_criterio': limite_existente.COD_CRITERIO_SELECAO,
                        'percentual_final': limite_existente.PERCENTUAL_FINAL
                    }
                )

        # Commit para confirmar as exclusões
        db.session.commit()

        # Criar novos registros de limite
        limites_criados = 0
        for i in range(len(empresas_data)):
            # Criar novo limite com os dados do cálculo
            novo_limite = LimiteDistribuicao(
                ID_EDITAL=edital_id,
                ID_PERIODO=periodo_id,
                ID_EMPRESA=int(empresas_data[i]),
                COD_CRITERIO_SELECAO=cod_criterio,
                QTDE_MAXIMA=None,  # NULL conforme orientação
                VALOR_MAXIMO=None,  # NULL conforme orientação
                PERCENTUAL_FINAL=float(percentuais[i])
            )

            db.session.add(novo_limite)
            limites_criados += 1

            # Registrar log de auditoria para cada limite criado
            registrar_log(
                acao='criar',
                entidade='limite',
                entidade_id=0,  # Será atualizado após o commit
                descricao=f'Criação automática de limite de distribuição por cálculo',
                dados_novos={
                    'id_edital': edital_id,
                    'id_periodo': periodo_id,
                    'id_empresa': int(empresas_data[i]),
                    'cod_criterio': cod_criterio,
                    'percentual_final': float(percentuais[i]),
                    'arrecadacao_referencia': float(arrecadacoes[i]) if arrecadacoes[i] else 0
                }
            )

        # Commit para salvar todos os limites
        db.session.commit()

        # Atualizar IDs dos logs após o commit (opcional, mas ideal para manter rastreabilidade completa)
        for i in range(len(empresas_data)):
            limite = LimiteDistribuicao.query.filter_by(
                ID_EDITAL=edital_id,
                ID_PERIODO=periodo_id,
                ID_EMPRESA=int(empresas_data[i]),
                COD_CRITERIO_SELECAO=cod_criterio,
                DELETED_AT=None
            ).first()

            if limite:
                # Atualizar o log com o ID correto
                log = AuditLog.query.filter_by(
                    ENTIDADE='limite',
                    ACAO='criar',
                    ENTIDADE_ID=0,
                    USUARIO_ID=current_user.id
                ).order_by(AuditLog.DATA.desc()).first()

                if log:
                    log.ENTIDADE_ID = limite.ID

        # Commit final para atualizar os logs
        db.session.commit()

        flash(f'Limites de distribuição salvos com sucesso! Total: {limites_criados} registros.', 'success')
        return redirect(url_for('limite.lista_limites'))

    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao salvar limites: {str(e)}', 'danger')
        print(f"Erro detalhado ao salvar limites: {e}")
        return redirect(url_for('limite.analise_limites'))


@limite_bp.route('/limites/novo', methods=['GET', 'POST'])
@login_required
def novo_limite():
    editais = Edital.query.filter(Edital.DELETED_AT == None).all()
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).all()
    empresas = EmpresaParticipante.query.filter(EmpresaParticipante.DELETED_AT == None).all()
    criterios = CriterioSelecao.query.filter(CriterioSelecao.DELETED_AT == None).all()

    # Verificar se há editais e períodos cadastrados
    if not editais:
        flash('Não há editais cadastrados. Cadastre um edital primeiro.', 'warning')
        return redirect(url_for('edital.lista_editais'))

    if not periodos:
        flash('Não há períodos cadastrados. Cadastre um período primeiro.', 'warning')
        return redirect(url_for('periodo.lista_periodos'))

    if not empresas:
        flash('Não há empresas cadastradas. Cadastre uma empresa primeiro.', 'warning')
        return redirect(url_for('periodo.lista_periodos'))

    if request.method == 'POST':
        try:
            edital_id = int(request.form['edital_id'])
            periodo_id = int(request.form['periodo_id'])
            empresa_id = int(request.form['empresa_id'])
            cod_criterio = int(request.form['cod_criterio'])

            # Valores opcionais
            qtde_maxima = request.form.get('qtde_maxima')
            if qtde_maxima:
                qtde_maxima = int(qtde_maxima)

            valor_maximo = request.form.get('valor_maximo')
            if valor_maximo:
                valor_maximo = float(valor_maximo)

            percentual_final = request.form.get('percentual_final')
            if percentual_final:
                percentual_final = float(percentual_final)

            # Verificar se já existe limite para esta combinação
            limite_existente = LimiteDistribuicao.query.filter_by(
                ID_EDITAL=edital_id,
                ID_PERIODO=periodo_id,
                ID_EMPRESA=empresa_id,
                COD_CRITERIO_SELECAO=cod_criterio,
                DELETED_AT=None
            ).first()

            if limite_existente:
                flash('Já existe um limite cadastrado com estes critérios.', 'danger')
                return render_template('credenciamento/form_limite.html', editais=editais, periodos=periodos,
                                       empresas=empresas,
                                       criterios=criterios)

            novo_limite = LimiteDistribuicao(
                ID_EDITAL=edital_id,
                ID_PERIODO=periodo_id,
                ID_EMPRESA=empresa_id,
                COD_CRITERIO_SELECAO=cod_criterio,
                QTDE_MAXIMA=qtde_maxima,
                VALOR_MAXIMO=valor_maximo,
                PERCENTUAL_FINAL=percentual_final
            )

            db.session.add(novo_limite)
            db.session.commit()

            # Registrar log de auditoria
            dados_novos = {
                'id_edital': edital_id,
                'id_periodo': periodo_id,
                'id_empresa': empresa_id,
                'cod_criterio': cod_criterio,
                'qtde_maxima': qtde_maxima,
                'valor_maximo': valor_maximo,
                'percentual_final': percentual_final
            }

            registrar_log(
                acao='criar',
                entidade='limite',
                entidade_id=novo_limite.ID,
                descricao=f'Cadastro de limite de distribuição',
                dados_novos=dados_novos
            )

            flash('Limite de distribuição cadastrado com sucesso!', 'success')
            return redirect(url_for('limite.lista_limites'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')

    return render_template('credenciamento/form_limite.html', editais=editais, periodos=periodos, empresas=empresas,
                           criterios=criterios)


@limite_bp.route('/limites/editar/<int:id>', methods=['GET', 'POST'])
@login_required
def editar_limite(id):
    limite = LimiteDistribuicao.query.get_or_404(id)
    editais = Edital.query.filter(Edital.DELETED_AT == None).all()
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).all()
    empresas = EmpresaParticipante.query.filter(EmpresaParticipante.DELETED_AT == None).all()
    criterios = CriterioSelecao.query.filter(CriterioSelecao.DELETED_AT == None).all()

    if request.method == 'POST':
        try:
            # Capturar dados antigos para auditoria
            dados_antigos = {
                'id_edital': limite.ID_EDITAL,
                'id_periodo': limite.ID_PERIODO,
                'id_empresa': limite.ID_EMPRESA,
                'cod_criterio': limite.COD_CRITERIO_SELECAO,
                'qtde_maxima': limite.QTDE_MAXIMA,
                'valor_maximo': limite.VALOR_MAXIMO,
                'percentual_final': limite.PERCENTUAL_FINAL
            }

            # Atualizar dados
            limite.ID_EDITAL = int(request.form['edital_id'])
            limite.ID_PERIODO = int(request.form['periodo_id'])
            limite.ID_EMPRESA = int(request.form['empresa_id'])
            limite.COD_CRITERIO_SELECAO = int(request.form['cod_criterio'])

            # Valores opcionais
            qtde_maxima = request.form.get('qtde_maxima')
            if qtde_maxima:
                limite.QTDE_MAXIMA = int(qtde_maxima)
            else:
                limite.QTDE_MAXIMA = None

            valor_maximo = request.form.get('valor_maximo')
            if valor_maximo:
                limite.VALOR_MAXIMO = float(valor_maximo)
            else:
                limite.VALOR_MAXIMO = None

            percentual_final = request.form.get('percentual_final')
            if percentual_final:
                limite.PERCENTUAL_FINAL = float(percentual_final)
            else:
                limite.PERCENTUAL_FINAL = None

            limite.UPDATED_AT = datetime.utcnow()

            # Verificar se já existe limite para esta combinação (excluindo o próprio registro)
            limite_existente = LimiteDistribuicao.query.filter(
                LimiteDistribuicao.ID_EDITAL == limite.ID_EDITAL,
                LimiteDistribuicao.ID_PERIODO == limite.ID_PERIODO,
                LimiteDistribuicao.ID_EMPRESA == limite.ID_EMPRESA,
                LimiteDistribuicao.COD_CRITERIO_SELECAO == limite.COD_CRITERIO_SELECAO,
                LimiteDistribuicao.DELETED_AT == None,
                LimiteDistribuicao.ID != id
            ).first()

            if limite_existente:
                flash('Já existe um limite cadastrado com estes critérios.', 'danger')
                return render_template(
                    'credenciamento/form_limite.html',
                    limite=limite,
                    editais=editais,
                    periodos=periodos,
                    empresas=empresas,
                    criterios=criterios
                )

            # Dados novos para auditoria
            dados_novos = {
                'id_edital': limite.ID_EDITAL,
                'id_periodo': limite.ID_PERIODO,
                'id_empresa': limite.ID_EMPRESA,
                'cod_criterio': limite.COD_CRITERIO_SELECAO,
                'qtde_maxima': limite.QTDE_MAXIMA,
                'valor_maximo': limite.VALOR_MAXIMO,
                'percentual_final': limite.PERCENTUAL_FINAL
            }

            db.session.commit()

            # Registrar log de auditoria
            registrar_log(
                acao='editar',
                entidade='limite',
                entidade_id=limite.ID,
                descricao=f'Atualização de limite de distribuição',
                dados_antigos=dados_antigos,
                dados_novos=dados_novos
            )

            flash('Limite de distribuição atualizado com sucesso!', 'success')
            return redirect(url_for('limite.lista_limites'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro: {str(e)}', 'danger')

    return render_template(
        'credenciamento/form_limite.html',
        limite=limite,
        editais=editais,
        periodos=periodos,
        empresas=empresas,
        criterios=criterios)

@limite_bp.route('/limites/excluir/<int:id>', methods=['POST'])
@login_required
def excluir_limite(id):
    limite = LimiteDistribuicao.query.get_or_404(id)

    try:
        # Capturar dados antigos para auditoria
        dados_antigos = {
            'id_edital': limite.ID_EDITAL,
            'id_periodo': limite.ID_PERIODO,
            'id_empresa': limite.ID_EMPRESA,
            'cod_criterio': limite.COD_CRITERIO_SELECAO,
            'qtde_maxima': limite.QTDE_MAXIMA,
            'valor_maximo': limite.VALOR_MAXIMO,
            'percentual_final': limite.PERCENTUAL_FINAL
        }

        # Soft delete - apenas marca como excluído
        limite.DELETED_AT = datetime.utcnow()
        db.session.commit()

        # Registrar log de auditoria
        registrar_log(
            acao='excluir',
            entidade='limite',
            entidade_id=limite.ID,
            descricao=f'Exclusão de limite de distribuição',
            dados_antigos=dados_antigos
        )

        flash('Limite de distribuição excluído com sucesso!', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Erro ao excluir limite de distribuição: {str(e)}', 'danger')

    return redirect(url_for('limite.lista_limites'))


@limite_bp.route('/limites/filtrar', methods=['GET'])
@login_required
def filtrar_limites():
    periodo_id = request.args.get('periodo_id', type=int)
    criterio_id = request.args.get('criterio_id', type=int)

    # Redirecionar para a lista com os parâmetros de filtro
    return redirect(url_for('limite.lista_limites', periodo_id=periodo_id, criterio_id=criterio_id))


@limite_bp.route('/limites/detalhe/<int:id>')
@login_required
def detalhe_limite(id):
    # Consulta para obter o limite e a descrição do critério
    resultado = db.session.query(
        LimiteDistribuicao,
        CriterioSelecao.DS_CRITERIO_SELECAO,
        Edital.NU_EDITAL,
        Edital.ANO,
        PeriodoAvaliacao.DT_INICIO,
        PeriodoAvaliacao.DT_FIM,
        EmpresaParticipante.NO_EMPRESA
    ).outerjoin(
        CriterioSelecao,
        LimiteDistribuicao.COD_CRITERIO_SELECAO == CriterioSelecao.COD
    ).outerjoin(
        Edital,
        LimiteDistribuicao.ID_EDITAL == Edital.ID
    ).outerjoin(
        PeriodoAvaliacao,
        LimiteDistribuicao.ID_PERIODO == PeriodoAvaliacao.ID
    ).outerjoin(
        EmpresaParticipante,
        LimiteDistribuicao.ID_EMPRESA == EmpresaParticipante.ID_EMPRESA
    ).filter(
        LimiteDistribuicao.ID == id,
        LimiteDistribuicao.DELETED_AT == None
    ).first_or_404()

    limite, ds_criterio, nu_edital, nu_ano, dt_inicio, dt_fim, no_empresa = resultado

    return render_template(
        'credenciamento/detalhe_limite.html',
        limite=limite,
        ds_criterio=ds_criterio,
        nu_edital=nu_edital,
        nu_ano=nu_ano,
        dt_inicio=dt_inicio,
        dt_fim=dt_fim,
        no_empresa=no_empresa
    )