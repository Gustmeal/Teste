from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify
from app.models.demonstrativo import EstruturaDemonstrativo, ContaDemonstrativo, CodigoDemonstrativo
from app import db
from flask_login import login_required
from app.utils.audit import registrar_log
from datetime import datetime
from sqlalchemy import text

demonstrativo_bp = Blueprint('demonstrativo', __name__, url_prefix='/codigos-contabeis/demonstrativos')


@demonstrativo_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@demonstrativo_bp.route('/')
@login_required
def index():
    """Dashboard principal dos demonstrativos"""
    total_vinculacoes = ContaDemonstrativo.query.count()
    total_estruturas = EstruturaDemonstrativo.query.count()

    # Últimas vinculações modificadas
    ultimas_vinculacoes = ContaDemonstrativo.query.limit(5).all()

    return render_template('codigos_contabeis/demonstrativos/index.html',
                           total_vinculacoes=total_vinculacoes,
                           total_estruturas=total_estruturas,
                           ultimas_vinculacoes=ultimas_vinculacoes)


@demonstrativo_bp.route('/lista')
@login_required
def lista_demonstrativos():
    """Lista todas as vinculações de demonstrativos"""
    demonstrativos = ContaDemonstrativo.query.all()

    # Buscar estruturas para mostrar os nomes
    estruturas = {e.ORDEM: e.GRUPO for e in EstruturaDemonstrativo.query.all()}

    return render_template('codigos_contabeis/demonstrativos/lista_demonstrativos.html',
                           demonstrativos=demonstrativos,
                           estruturas=estruturas)


@demonstrativo_bp.route('/executar-rotina', methods=['GET', 'POST'])
@login_required
def executar_rotina():
    """Página para executar rotina de demonstrativos"""
    if request.method == 'GET':
        # Buscar data mais recente - com fallback
        try:
            data_mais_recente = ContaDemonstrativo.obter_data_referencia_mais_recente()
        except:
            # Se der erro, usar data atual como fallback
            data_mais_recente = datetime.now().date()

        # Lista de demonstrativos disponíveis
        demonstrativos_disponiveis = [
            {'valor': 'BP_Gerencial', 'nome': 'BP Gerencial'},
            {'valor': 'BP_Resumida', 'nome': 'BP Resumida'},
            {'valor': 'DRE_Gerencial', 'nome': 'DRE Gerencial'},
            {'valor': 'DRE_Resumida', 'nome': 'DRE Resumida'},
            {'valor': 'DVA_Gerencial', 'nome': 'DVA Gerencial'}
        ]

        return render_template('codigos_contabeis/demonstrativos/executar_rotina.html',
                               data_mais_recente=data_mais_recente,
                               demonstrativos_disponiveis=demonstrativos_disponiveis)

    # POST - Executar a rotina
    try:
        dt_referencia = request.form.get('dt_referencia')
        demonstrativos_selecionados = request.form.getlist('demonstrativos')

        if not dt_referencia:
            flash('Data de referência é obrigatória!', 'danger')
            return redirect(url_for('demonstrativo.executar_rotina'))

        if not demonstrativos_selecionados:
            flash('Selecione pelo menos um demonstrativo!', 'danger')
            return redirect(url_for('demonstrativo.executar_rotina'))

        # Log simples no console
        print(f"[{datetime.now()}] Iniciando rotina de demonstrativos")
        print(f"Data: {dt_referencia}")
        print(f"Demonstrativos: {', '.join(demonstrativos_selecionados)}")

        # Executar o script SQL principal
        resultados = executar_script_demonstrativos(dt_referencia, demonstrativos_selecionados)

        print(f"[{datetime.now()}] Rotina finalizada - {resultados['total_processados']} registros processados")

        flash(f'Rotina executada com sucesso! {resultados["total_processados"]} registros processados.', 'success')
        return redirect(url_for('demonstrativo.lista_demonstrativos'))

    except Exception as e:
        print(f"[{datetime.now()}] Erro na rotina: {str(e)}")
        db.session.rollback()
        flash(f'Erro ao executar rotina: {str(e)}', 'danger')
        return redirect(url_for('demonstrativo.executar_rotina'))


def executar_script_demonstrativos(dt_referencia, demonstrativos):
    """Executa o script SQL dos demonstrativos"""
    try:
        registros_processados = 0

        # Primeiro, deletar registros existentes para a data
        sql_delete = text("""
            DELETE FROM BDG.COR_DEM_TB004_DEMONSTRATIVOS 
            WHERE DT_REFERENCIA = :dt_referencia
        """)
        db.session.execute(sql_delete, {'dt_referencia': dt_referencia})

        # Mapear demonstrativos para CO_DEMONSTRATIVO
        mapa_demonstrativos = {
            'BP_Resumida': 2,
            'BP_Gerencial': 1,
            'DRE_Resumida': 4,
            'DRE_Gerencial': 3,
            'DVA_Gerencial': 5
        }

        # Executar script para cada demonstrativo selecionado
        for demonstrativo in demonstrativos:
            co_demonstrativo = mapa_demonstrativos.get(demonstrativo)

            if co_demonstrativo == 2:  # BP Resumida
                registros_processados += executar_bp_resumida(dt_referencia)
            elif co_demonstrativo == 1:  # BP Gerencial
                registros_processados += executar_bp_gerencial(dt_referencia)
            elif co_demonstrativo == 4:  # DRE Resumida
                registros_processados += executar_dre_resumida(dt_referencia)
            elif co_demonstrativo == 3:  # DRE Gerencial
                registros_processados += executar_dre_gerencial(dt_referencia)
            elif co_demonstrativo == 5:  # DVA Gerencial
                registros_processados += executar_dva_gerencial(dt_referencia)

        db.session.commit()

        return {
            'success': True,
            'total_processados': registros_processados
        }

    except Exception as e:
        db.session.rollback()
        raise e


def executar_bp_resumida(dt_referencia):
    """Executa a rotina do BP Resumida"""
    # Script do BP Resumida
    sql = text("""
        INSERT INTO BDG.COR_DEM_TB004_DEMONSTRATIVOS
        SELECT
            :dt_referencia as DT_REFERENCIA,
            A.CO_DEMONSTRATIVO,
            A.NO_DEMONSTRATIVO,
            B.ORDEM,
            B.GRUPO,
            B.SOMA,
            VR_SALDO = SUM(
                CASE
                    WHEN C.CO_CONTA LIKE '4%' THEN -D.VR_SALDO_ATUAL
                    ELSE D.VR_SALDO_ATUAL
                END
            )
        FROM BDG.COR_DEM_TB001_CODIGOS AS A
        INNER JOIN BDG.COR_DEM_TB002_ESTRUTURA AS B ON A.CO_DEMONSTRATIVO = B.CO_DEMONSTRATIVO
        LEFT JOIN BDG.COR_DEM_TB003_CONTA_DEMONSTRATIVO AS C ON C.CO_BP_Resumida = B.ORDEM
        LEFT JOIN BDG.COR_TB012_BALANCETE D ON C.CO_CONTA = D.CO_CONTA AND D.DT_REFERENCIA = :dt_referencia
        WHERE B.CO_DEMONSTRATIVO = 2
        GROUP BY A.CO_DEMONSTRATIVO, A.NO_DEMONSTRATIVO, B.ORDEM, B.GRUPO, B.SOMA
        ORDER BY B.ORDEM
    """)

    result = db.session.execute(sql, {'dt_referencia': dt_referencia})
    count = result.rowcount

    # Executar cálculos específicos
    calculos = [
        """UPDATE BDG.COR_DEM_TB004_DEMONSTRATIVOS
           SET VR = (SELECT SUM(VR) FROM BDG.COR_DEM_TB004_DEMONSTRATIVOS 
                     WHERE CO_DEMONSTRATIVO = 2 AND ORDEM < 11 AND DT_REFERENCIA = :dt_referencia)
           WHERE CO_DEMONSTRATIVO = 2 AND ORDEM = 11 AND DT_REFERENCIA = :dt_referencia""",

        """UPDATE BDG.COR_DEM_TB004_DEMONSTRATIVOS
           SET VR = (SELECT SUM(VR) FROM BDG.COR_DEM_TB004_DEMONSTRATIVOS 
                     WHERE CO_DEMONSTRATIVO = 2 AND ORDEM BETWEEN 12 AND 20 AND DT_REFERENCIA = :dt_referencia)
           WHERE CO_DEMONSTRATIVO = 2 AND ORDEM = 21 AND DT_REFERENCIA = :dt_referencia""",

        """UPDATE BDG.COR_DEM_TB004_DEMONSTRATIVOS
           SET VR = (SELECT SUM(VR) FROM BDG.COR_DEM_TB004_DEMONSTRATIVOS 
                     WHERE CO_DEMONSTRATIVO = 2 AND ORDEM IN (21, 22) AND DT_REFERENCIA = :dt_referencia)
           WHERE CO_DEMONSTRATIVO = 2 AND ORDEM = 23 AND DT_REFERENCIA = :dt_referencia"""
    ]

    for calc in calculos:
        db.session.execute(text(calc), {'dt_referencia': dt_referencia})

    return count


def executar_bp_gerencial(dt_referencia):
    """Executa a rotina do BP Gerencial"""
    # Script do BP Gerencial
    sql = text("""
        INSERT INTO BDG.COR_DEM_TB004_DEMONSTRATIVOS
        SELECT
            :dt_referencia as DT_REFERENCIA,
            A.CO_DEMONSTRATIVO,
            A.NO_DEMONSTRATIVO,
            B.ORDEM,
            B.GRUPO,
            B.SOMA,
            VR_SALDO = SUM(
                CASE
                    WHEN C.CO_CONTA LIKE '4%' THEN ISNULL(-D.VR_SALDO_ATUAL,0)
                    ELSE ISNULL(D.VR_SALDO_ATUAL,0)
                END
            )
        FROM BDG.COR_DEM_TB001_CODIGOS AS A
        INNER JOIN BDG.COR_DEM_TB002_ESTRUTURA AS B ON A.CO_DEMONSTRATIVO = B.CO_DEMONSTRATIVO
        LEFT JOIN BDG.COR_DEM_TB003_CONTA_DEMONSTRATIVO AS C ON C.CO_BP_Gerencial = B.ORDEM
        LEFT JOIN BDG.COR_TB012_BALANCETE D ON C.CO_CONTA = D.CO_CONTA AND D.DT_REFERENCIA = :dt_referencia
        WHERE B.CO_DEMONSTRATIVO = 1
        GROUP BY A.CO_DEMONSTRATIVO, A.NO_DEMONSTRATIVO, B.ORDEM, B.GRUPO, B.SOMA
        ORDER BY B.ORDEM
    """)

    result = db.session.execute(sql, {'dt_referencia': dt_referencia})
    count = result.rowcount

    # Executar todos os cálculos do BP Gerencial (são muitos)
    # Aqui estão alguns exemplos, você deve adicionar todos do arquivo Word
    calculos_bp_gerencial = [
        # Tributos a Recuperar
        """UPDATE BDG.COR_DEM_TB004_DEMONSTRATIVOS
           SET VR = (SELECT SUM(VR) FROM BDG.COR_DEM_TB004_DEMONSTRATIVOS 
                     WHERE CO_DEMONSTRATIVO = 1 AND ORDEM IN (13,14) AND DT_REFERENCIA = :dt_referencia)
           WHERE CO_DEMONSTRATIVO = 1 AND ORDEM = 12 AND DT_REFERENCIA = :dt_referencia""",

        # Caixa
        """UPDATE BDG.COR_DEM_TB004_DEMONSTRATIVOS
           SET VR = (SELECT SUM(VR) FROM BDG.COR_DEM_TB004_DEMONSTRATIVOS 
                     WHERE CO_DEMONSTRATIVO = 1 AND ORDEM IN (4) AND DT_REFERENCIA = :dt_referencia)
           WHERE CO_DEMONSTRATIVO = 1 AND ORDEM = 3 AND DT_REFERENCIA = :dt_referencia""",

        # Adicionar todos os outros cálculos do arquivo Word...
    ]

    for calc in calculos_bp_gerencial:
        db.session.execute(text(calc), {'dt_referencia': dt_referencia})

    return count


def executar_dre_resumida(dt_referencia):
    """Executa a rotina do DRE Resumida"""
    sql = text("""
        INSERT INTO BDG.COR_DEM_TB004_DEMONSTRATIVOS
        SELECT
            :dt_referencia as DT_REFERENCIA,
            A.CO_DEMONSTRATIVO,
            A.NO_DEMONSTRATIVO,
            B.ORDEM,
            B.GRUPO,
            B.SOMA,
            VR = SUM(
                CASE
                    WHEN B.ORDEM IN (2, 4, 8, 9, 10, 13, 16, 23) THEN -D.VR_MOVIMENTACAO
                    WHEN D.CO_CONTA LIKE '419.08%' THEN -D.VR_MOVIMENTACAO
                    WHEN D.CO_CONTA LIKE '418.02%' THEN -D.VR_MOVIMENTACAO
                    WHEN D.CO_CONTA LIKE '419.03%' THEN -D.VR_MOVIMENTACAO
                    WHEN D.CO_CONTA LIKE '431.0%' THEN -D.VR_MOVIMENTACAO
                    WHEN D.CO_CONTA LIKE '419.10%' THEN -D.VR_MOVIMENTACAO
                    WHEN D.CO_CONTA LIKE '454.%' THEN -D.VR_MOVIMENTACAO
                    ELSE D.VR_MOVIMENTACAO
                END
            )
        FROM BDG.COR_DEM_TB001_CODIGOS AS A
        INNER JOIN BDG.COR_DEM_TB002_ESTRUTURA AS B ON A.CO_DEMONSTRATIVO = B.CO_DEMONSTRATIVO
        LEFT JOIN BDG.COR_DEM_TB003_CONTA_DEMONSTRATIVO AS C ON C.CO_DRE_Resumida = B.ORDEM
        LEFT JOIN BDG.COR_TB012_BALANCETE D ON C.CO_CONTA = D.CO_CONTA AND D.DT_REFERENCIA = :dt_referencia
        WHERE B.CO_DEMONSTRATIVO = 4
        GROUP BY A.CO_DEMONSTRATIVO, A.NO_DEMONSTRATIVO, B.ORDEM, B.GRUPO, B.SOMA
        ORDER BY B.ORDEM
    """)

    result = db.session.execute(sql, {'dt_referencia': dt_referencia})
    count = result.rowcount

    # Executar cálculos do DRE Resumida
    # Adicionar todos os cálculos do arquivo Word...

    return count


def executar_dre_gerencial(dt_referencia):
    """Executa a rotina do DRE Gerencial"""
    sql = text("""
        INSERT INTO BDG.COR_DEM_TB004_DEMONSTRATIVOS
        SELECT
            :dt_referencia as DT_REFERENCIA,
            A.CO_DEMONSTRATIVO,
            A.NO_DEMONSTRATIVO,
            B.ORDEM,
            B.GRUPO,
            B.SOMA,
            VR_SALDO = SUM(
                CASE
                    WHEN C.CO_CONTA LIKE '4%' THEN -D.VR_MOVIMENTACAO
                    ELSE D.VR_MOVIMENTACAO
                END
            )
        FROM BDG.COR_DEM_TB001_CODIGOS AS A
        INNER JOIN BDG.COR_DEM_TB002_ESTRUTURA AS B ON A.CO_DEMONSTRATIVO = B.CO_DEMONSTRATIVO
        LEFT JOIN BDG.COR_DEM_TB003_CONTA_DEMONSTRATIVO AS C ON C.CO_DRE_Gerencial = B.ORDEM
        LEFT JOIN BDG.COR_TB012_BALANCETE AS D ON C.CO_CONTA = D.CO_CONTA AND D.DT_REFERENCIA = :dt_referencia
        WHERE B.CO_DEMONSTRATIVO = 3
        GROUP BY A.CO_DEMONSTRATIVO, A.NO_DEMONSTRATIVO, B.ORDEM, B.GRUPO, B.SOMA
        ORDER BY B.ORDEM
    """)

    result = db.session.execute(sql, {'dt_referencia': dt_referencia})
    count = result.rowcount

    # Executar cálculos do DRE Gerencial
    # Adicionar todos os cálculos do arquivo Word...

    return count


def executar_dva_gerencial(dt_referencia):
    """Executa a rotina do DVA Gerencial"""
    sql = text("""
        INSERT INTO BDG.COR_DEM_TB004_DEMONSTRATIVOS
        SELECT
            :dt_referencia as DT_REFERENCIA,
            A.CO_DEMONSTRATIVO,
            A.NO_DEMONSTRATIVO,
            B.ORDEM,
            B.GRUPO,
            B.SOMA,
            VR_SALDO = SUM(
                CASE
                    WHEN C.CO_CONTA LIKE '4%' THEN -D.VR_MOVIMENTACAO
                    ELSE D.VR_MOVIMENTACAO
                END
            )
        FROM BDG.COR_DEM_TB001_CODIGOS AS A
        INNER JOIN BDG.COR_DEM_TB002_ESTRUTURA AS B ON A.CO_DEMONSTRATIVO = B.CO_DEMONSTRATIVO
        LEFT JOIN BDG.COR_DEM_TB003_CONTA_DEMONSTRATIVO AS C ON C.CO_DVA_Gerencial = B.ORDEM
        LEFT JOIN BDG.COR_TB012_BALANCETE D ON C.CO_CONTA = D.CO_CONTA AND D.DT_REFERENCIA = :dt_referencia
        WHERE B.CO_DEMONSTRATIVO = 5
        GROUP BY A.CO_DEMONSTRATIVO, A.NO_DEMONSTRATIVO, B.ORDEM, B.GRUPO, B.SOMA
        ORDER BY B.ORDEM
    """)

    result = db.session.execute(sql, {'dt_referencia': dt_referencia})
    count = result.rowcount

    # Inversão de sinal das Linhas 17, 18, 19, 21, 22, 24, 25, 27
    sql_inversao = text("""
        UPDATE BDG.COR_DEM_TB004_DEMONSTRATIVOS
        SET VR = VR * (-1)
        WHERE CO_DEMONSTRATIVO = 5 
        AND ORDEM IN (17, 18, 19, 21, 22, 24, 25, 27) 
        AND DT_REFERENCIA = :dt_referencia
    """)
    db.session.execute(sql_inversao, {'dt_referencia': dt_referencia})

    # Executar cálculos do DVA
    # Adicionar todos os cálculos do arquivo Word...

    return count


@demonstrativo_bp.route('/novo', methods=['GET', 'POST'])
@login_required
def novo_demonstrativo():
    """Criar ou atualizar vinculação de demonstrativo"""
    if request.method == 'POST':
        try:
            co_conta = request.form.get('co_conta', '').strip()

            if not co_conta:
                flash('Conta é obrigatória!', 'danger')
                return redirect(url_for('demonstrativo.novo_demonstrativo'))

            # Verificar se já existe para decidir se é criação ou atualização
            conta_existente = ContaDemonstrativo.query.filter_by(CO_CONTA=co_conta).first()

            # Capturar dados antigos se existir
            dados_antigos = None
            if conta_existente:
                dados_antigos = {
                    'co_conta': conta_existente.CO_CONTA,
                    'CO_BP_Gerencial': conta_existente.CO_BP_Gerencial,
                    'CO_BP_Resumida': conta_existente.CO_BP_Resumida,
                    'CO_DRE_Gerencial': conta_existente.CO_DRE_Gerencial,
                    'CO_DRE_Resumida': conta_existente.CO_DRE_Resumida,
                    'CO_DVA_Gerencial': conta_existente.CO_DVA_Gerencial
                }

            # Processar dados do formulário
            campos = ['CO_BP_Gerencial', 'CO_BP_Resumida', 'CO_DRE_Gerencial',
                      'CO_DRE_Resumida', 'CO_DVA_Gerencial']

            dados_novos = {}
            for campo in campos:
                valor = request.form.get(campo.lower())
                if valor and valor != 'nenhum':
                    dados_novos[campo] = int(valor)
                else:
                    dados_novos[campo] = None

            # Criar ou atualizar
            conta, is_nova = ContaDemonstrativo.criar_ou_atualizar(co_conta, dados_novos)

            db.session.commit()

            # Registrar log apropriado
            if is_nova:
                registrar_log(
                    acao='criar',
                    entidade='conta_demonstrativo',
                    entidade_id=co_conta,
                    descricao=f'Criação de vinculação demonstrativo para conta {co_conta}',
                    dados_novos={'co_conta': co_conta, **dados_novos}
                )
                flash('Vinculação criada com sucesso!', 'success')
            else:
                registrar_log(
                    acao='editar',
                    entidade='conta_demonstrativo',
                    entidade_id=co_conta,
                    descricao=f'Atualização de vinculação demonstrativo para conta {co_conta}',
                    dados_antigos=dados_antigos,
                    dados_novos={'co_conta': co_conta, **dados_novos}
                )
                flash('Vinculação atualizada com sucesso!', 'success')

            return redirect(url_for('demonstrativo.lista_demonstrativos'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao processar vinculação: {str(e)}', 'danger')

    # GET - Buscar estruturas disponíveis AGRUPADAS POR DEMONSTRATIVO
    estruturas_agrupadas = EstruturaDemonstrativo.obter_estruturas_agrupadas()
    codigos_demonstrativos = CodigoDemonstrativo.obter_todos()

    # Organizar estruturas por demonstrativo
    estruturas_por_demonstrativo = {}
    for estrutura in estruturas_agrupadas:
        co_demo = estrutura.CO_DEMONSTRATIVO
        if co_demo not in estruturas_por_demonstrativo:
            estruturas_por_demonstrativo[co_demo] = {
                'nome': estrutura.NO_DEMONSTRATIVO,
                'estruturas': []
            }
        estruturas_por_demonstrativo[co_demo]['estruturas'].append({
            'ordem': estrutura.ORDEM,
            'grupo': estrutura.GRUPO
        })

    return render_template('codigos_contabeis/demonstrativos/form_demonstrativo.html',
                           estruturas_por_demonstrativo=estruturas_por_demonstrativo,
                           codigos_demonstrativos=codigos_demonstrativos)



@demonstrativo_bp.route('/editar/<co_conta>', methods=['GET', 'POST'])
@login_required
def editar_demonstrativo(co_conta):
    """Editar vinculação existente"""
    demonstrativo = ContaDemonstrativo.query.filter_by(CO_CONTA=co_conta).first_or_404()

    if request.method == 'POST':
        try:
            # Capturar dados antigos
            dados_antigos = {
                'co_conta': demonstrativo.CO_CONTA,
                'CO_BP_Gerencial': demonstrativo.CO_BP_Gerencial,
                'CO_BP_Resumida': demonstrativo.CO_BP_Resumida,
                'CO_DRE_Gerencial': demonstrativo.CO_DRE_Gerencial,
                'CO_DRE_Resumida': demonstrativo.CO_DRE_Resumida,
                'CO_DVA_Gerencial': demonstrativo.CO_DVA_Gerencial
            }

            # Atualizar campos
            campos = ['CO_BP_Gerencial', 'CO_BP_Resumida', 'CO_DRE_Gerencial',
                      'CO_DRE_Resumida', 'CO_DVA_Gerencial']

            dados_novos = {'co_conta': co_conta}

            for campo in campos:
                valor = request.form.get(campo.lower())
                if valor and valor != 'nenhum':
                    setattr(demonstrativo, campo, int(valor))
                    dados_novos[campo] = int(valor)
                else:
                    setattr(demonstrativo, campo, None)
                    dados_novos[campo] = None

            db.session.commit()

            # Registrar log
            registrar_log(
                acao='editar',
                entidade='conta_demonstrativo',
                entidade_id=co_conta,
                descricao=f'Edição de vinculação demonstrativo para conta {co_conta}',
                dados_antigos=dados_antigos,
                dados_novos=dados_novos
            )

            flash('Vinculação atualizada com sucesso!', 'success')
            return redirect(url_for('demonstrativo.lista_demonstrativos'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erro ao atualizar: {str(e)}', 'danger')

    # GET - Buscar estruturas agrupadas
    estruturas_agrupadas = EstruturaDemonstrativo.obter_estruturas_agrupadas()
    codigos_demonstrativos = CodigoDemonstrativo.obter_todos()

    # Organizar estruturas por demonstrativo
    estruturas_por_demonstrativo = {}
    for estrutura in estruturas_agrupadas:
        co_demo = estrutura.CO_DEMONSTRATIVO
        if co_demo not in estruturas_por_demonstrativo:
            estruturas_por_demonstrativo[co_demo] = {
                'nome': estrutura.NO_DEMONSTRATIVO,
                'estruturas': []
            }
        estruturas_por_demonstrativo[co_demo]['estruturas'].append({
            'ordem': estrutura.ORDEM,
            'grupo': estrutura.GRUPO
        })

    return render_template('codigos_contabeis/demonstrativos/form_demonstrativo.html',
                           demonstrativo=demonstrativo,
                           estruturas_por_demonstrativo=estruturas_por_demonstrativo,
                           codigos_demonstrativos=codigos_demonstrativos)

    # GET - Buscar estruturas organizadas por CO_DEMONSTRATIVO
    estruturas_por_demonstrativo = {}

    demonstrativos_map = {
        1: 'BP_Gerencial',
        2: 'BP_Resumida',
        3: 'DRE_Gerencial',
        4: 'DRE_Resumida',
        5: 'DVA_Gerencial'
    }

    for co_dem, nome_dem in demonstrativos_map.items():
        estruturas = EstruturaDemonstrativo.query.filter_by(
            CO_DEMONSTRATIVO=str(co_dem)
        ).order_by(EstruturaDemonstrativo.ORDEM).all()
        estruturas_por_demonstrativo[nome_dem] = estruturas

    return render_template('codigos_contabeis/demonstrativos/form_demonstrativo.html',
                           demonstrativo=demonstrativo,
                           estruturas_por_demonstrativo=estruturas_por_demonstrativo,
                           demonstrativos_map=demonstrativos_map)


@demonstrativo_bp.route('/api/estruturas/<int:co_demonstrativo>', methods=['GET'])
@login_required
def obter_estruturas_demonstrativo(co_demonstrativo):
    """API para obter estruturas de um demonstrativo específico"""
    try:
        estruturas = EstruturaDemonstrativo.obter_por_demonstrativo(co_demonstrativo)
        return jsonify({
            'success': True,
            'estruturas': [{
                'ordem': e.ORDEM,
                'grupo': e.GRUPO
            } for e in estruturas]
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })


@demonstrativo_bp.route('/excluir/<co_conta>', methods=['POST'])
@login_required
def excluir_demonstrativo(co_conta):
    """Excluir vinculações (limpar campos, mas manter a conta)"""
    try:
        demonstrativo = ContaDemonstrativo.query.filter_by(CO_CONTA=co_conta).first_or_404()

        # Capturar dados antigos para auditoria
        dados_antigos = {
            'co_conta': demonstrativo.CO_CONTA,
            'CO_BP_Gerencial': demonstrativo.CO_BP_Gerencial,
            'CO_BP_Resumida': demonstrativo.CO_BP_Resumida,
            'CO_DRE_Gerencial': demonstrativo.CO_DRE_Gerencial,
            'CO_DRE_Resumida': demonstrativo.CO_DRE_Resumida,
            'CO_DVA_Gerencial': demonstrativo.CO_DVA_Gerencial
        }

        # Limpar as vinculações (definir como NULL)
        demonstrativo.CO_BP_Gerencial = None
        demonstrativo.CO_BP_Resumida = None
        demonstrativo.CO_DRE_Gerencial = None
        demonstrativo.CO_DRE_Resumida = None
        demonstrativo.CO_DVA_Gerencial = None

        db.session.commit()

        # Dados novos para auditoria (todos NULL)
        dados_novos = {
            'co_conta': demonstrativo.CO_CONTA,
            'CO_BP_Gerencial': None,
            'CO_BP_Resumida': None,
            'CO_DRE_Gerencial': None,
            'CO_DRE_Resumida': None,
            'CO_DVA_Gerencial': None
        }

        # Registrar log
        registrar_log(
            acao='limpar_vinculacoes',
            entidade='conta_demonstrativo',
            entidade_id=co_conta,
            descricao=f'Remoção de todas as vinculações da conta {co_conta}',
            dados_antigos=dados_antigos,
            dados_novos=dados_novos
        )

        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({
                'success': True,
                'message': f'Vinculações da conta {co_conta} removidas com sucesso!'
            })

        flash('Vinculações removidas com sucesso!', 'success')
        return redirect(url_for('demonstrativo.lista_demonstrativos'))

    except Exception as e:
        db.session.rollback()

        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({
                'success': False,
                'message': f'Erro ao remover vinculações: {str(e)}'
            })

        flash(f'Erro ao remover vinculações: {str(e)}', 'danger')
        return redirect(url_for('demonstrativo.lista_demonstrativos'))


@demonstrativo_bp.route('/api/buscar-contas', methods=['GET'])
@login_required
def buscar_contas():
    """API para buscar contas com autocomplete"""
    try:
        termo = request.args.get('q', '').strip()

        # Aqui você pode buscar contas de múltiplas fontes
        # Por exemplo, de uma tabela de contas gerais, além das já cadastradas

        # Por enquanto, vamos buscar das contas já cadastradas
        query = db.session.query(ContaDemonstrativo.CO_CONTA).distinct()

        if termo:
            query = query.filter(ContaDemonstrativo.CO_CONTA.like(f'%{termo}%'))

        contas = query.limit(20).all()

        # Se precisar, adicione aqui busca de outras tabelas de contas
        # Por exemplo:
        # outras_contas = db.session.query(OutraTabela.conta).filter(...)
        # contas_total = set([c[0] for c in contas] + [c[0] for c in outras_contas])

        return jsonify({
            'results': [{'id': c[0], 'text': c[0]} for c in contas]
        })

    except Exception as e:
        return jsonify({
            'error': str(e),
            'results': []
        })


@demonstrativo_bp.route('/api/verificar-conta/<co_conta>', methods=['GET'])
@login_required
def verificar_conta(co_conta):
    """Verifica se a conta já existe e retorna seus dados"""
    try:
        conta = ContaDemonstrativo.query.filter_by(CO_CONTA=co_conta).first()

        if conta:
            return jsonify({
                'existe': True,
                'dados': {
                    'CO_BP_Gerencial': conta.CO_BP_Gerencial,
                    'CO_BP_Resumida': conta.CO_BP_Resumida,
                    'CO_DRE_Gerencial': conta.CO_DRE_Gerencial,
                    'CO_DRE_Resumida': conta.CO_DRE_Resumida,
                    'CO_DVA_Gerencial': conta.CO_DVA_Gerencial
                }
            })
        else:
            return jsonify({'existe': False})

    except Exception as e:
        return jsonify({'erro': str(e)})


@demonstrativo_bp.route('/api/data-mais-recente', methods=['GET'])
@login_required
def obter_data_mais_recente():
    """API para obter a data mais recente"""
    try:
        data = ContaDemonstrativo.obter_data_referencia_mais_recente()
        return jsonify({
            'success': True,
            'data': data.strftime('%Y-%m-%d') if data else None
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })