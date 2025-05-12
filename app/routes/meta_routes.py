from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash
from app.models.meta_avaliacao import MetaAvaliacao
from app.models.edital import Edital
from app.models.periodo import PeriodoAvaliacao
from app.models.empresa_participante import EmpresaParticipante
from datetime import datetime
from flask_login import login_required, current_user
from app import db
from app.utils.audit import registrar_log
from sqlalchemy import text
from decimal import Decimal
import calendar

meta_bp = Blueprint('meta', __name__, url_prefix='/credenciamento')


@meta_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@meta_bp.route('/metas')
@login_required
def lista_metas():
    # Parâmetros para filtro
    edital_id = request.args.get('edital_id', type=int)
    periodo_id = request.args.get('periodo_id', type=int)
    empresa_id = request.args.get('empresa_id', type=int)
    competencia = request.args.get('competencia', type=str)

    # Consulta base
    query = db.session.query(MetaAvaliacao, EmpresaParticipante). \
        join(EmpresaParticipante, MetaAvaliacao.ID_EMPRESA == EmpresaParticipante.ID_EMPRESA). \
        filter(MetaAvaliacao.DELETED_AT == None, EmpresaParticipante.DELETED_AT == None)

    # Aplicar filtros se fornecidos
    if edital_id:
        query = query.filter(MetaAvaliacao.ID_EDITAL == edital_id)
    if periodo_id:
        query = query.filter(MetaAvaliacao.ID_PERIODO == periodo_id)
    if empresa_id:
        query = query.filter(EmpresaParticipante.ID == empresa_id)
    if competencia:
        query = query.filter(MetaAvaliacao.COMPETENCIA == competencia)

    # Executar a consulta
    results = query.order_by(MetaAvaliacao.COMPETENCIA).all()

    # Usar um dicionário para eliminar duplicatas com base numa chave única
    metas_dict = {}
    for meta, empresa in results:
        # Criar uma chave única para cada combinação edital-período-empresa-competência
        chave_unica = (meta.ID_EDITAL, meta.ID_PERIODO, meta.ID_EMPRESA, meta.COMPETENCIA)

        # Se a chave já existir, pular (evita duplicatas)
        if chave_unica in metas_dict:
            continue

        # Adicionar informações da empresa
        meta.empresa_nome = empresa.NO_EMPRESA
        meta.empresa_nome_abreviado = empresa.NO_EMPRESA_ABREVIADA

        # Adicionar ao dicionário
        metas_dict[chave_unica] = meta

    # Converter o dicionário de volta para lista
    metas = list(metas_dict.values())

    # Ordenar a lista de metas por competência
    metas.sort(key=lambda m: m.COMPETENCIA)

    # Obter dados para os filtros - todos os dados
    editais = Edital.query.filter(Edital.DELETED_AT == None).all()

    # Usar consultas específicas para obter apenas os dados disponíveis nas metas
    # Obter períodos únicos disponíveis
    periodos_query = db.session.query(PeriodoAvaliacao). \
        join(MetaAvaliacao, PeriodoAvaliacao.ID == MetaAvaliacao.ID_PERIODO). \
        filter(PeriodoAvaliacao.DELETED_AT == None, MetaAvaliacao.DELETED_AT == None). \
        distinct(PeriodoAvaliacao.ID)

    # Aplicar filtros para limitar os períodos conforme seleção de edital
    if edital_id:
        periodos_query = periodos_query.filter(MetaAvaliacao.ID_EDITAL == edital_id)

    periodos = periodos_query.all()

    # Obter todas as empresas para processamento
    todas_empresas = EmpresaParticipante.query.filter(EmpresaParticipante.DELETED_AT == None).all()

    # Criar um dicionário para eliminar duplicatas por nome abreviado
    empresas_unicas = {}
    for empresa in todas_empresas:
        nome_chave = empresa.NO_EMPRESA_ABREVIADA or empresa.NO_EMPRESA
        if nome_chave not in empresas_unicas:
            empresas_unicas[nome_chave] = empresa

    # Converter para lista ordenada
    empresas = sorted(empresas_unicas.values(), key=lambda e: e.NO_EMPRESA_ABREVIADA or e.NO_EMPRESA)

    # Gerar lista de competências únicas disponíveis
    competencias_query = db.session.query(MetaAvaliacao.COMPETENCIA). \
        filter(MetaAvaliacao.DELETED_AT == None). \
        distinct(MetaAvaliacao.COMPETENCIA)

    # Aplicar filtros para limitar as competências
    if edital_id:
        competencias_query = competencias_query.filter(MetaAvaliacao.ID_EDITAL == edital_id)
    if periodo_id:
        competencias_query = competencias_query.filter(MetaAvaliacao.ID_PERIODO == periodo_id)
    if empresa_id:
        competencias_query = competencias_query.join(EmpresaParticipante,
                                                     MetaAvaliacao.ID_EMPRESA == EmpresaParticipante.ID_EMPRESA). \
            filter(EmpresaParticipante.ID == empresa_id)

    competencias = [c[0] for c in competencias_query.all() if c[0]]
    competencias.sort()

    # Obter todos os dados (para uso com JavaScript)
    todos_editais = Edital.query.filter(Edital.DELETED_AT == None).all()
    todos_periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).all()

    # Obter todas as relações para construir a lógica dos filtros
    meta_relationships = db.session.query(
        MetaAvaliacao.ID_EDITAL,
        MetaAvaliacao.ID_PERIODO,
        MetaAvaliacao.ID_EMPRESA,
        MetaAvaliacao.COMPETENCIA,
        EmpresaParticipante.ID.label('empresa_id')
    ).join(
        EmpresaParticipante,
        MetaAvaliacao.ID_EMPRESA == EmpresaParticipante.ID_EMPRESA
    ).filter(
        MetaAvaliacao.DELETED_AT == None,
        EmpresaParticipante.DELETED_AT == None
    ).all()

    relationships = []
    for rel in meta_relationships:
        relationships.append({
            'edital_id': rel.ID_EDITAL,
            'periodo_id': rel.ID_PERIODO,
            'id_empresa': rel.ID_EMPRESA,
            'empresa_id': rel.empresa_id,
            'competencia': rel.COMPETENCIA
        })

    return render_template('credenciamento/lista_metas.html',
                           metas=metas,
                           editais=editais,
                           periodos=periodos,
                           empresas=empresas,
                           competencias=competencias,
                           filtro_edital_id=edital_id,
                           filtro_periodo_id=periodo_id,
                           filtro_empresa_id=empresa_id,
                           filtro_competencia=competencia,
                           todos_editais=todos_editais,
                           todos_periodos=todos_periodos,
                           todas_empresas=todas_empresas,
                           relationships=relationships)


@meta_bp.route('/metas/nova', methods=['GET', 'POST'])
@login_required
def nova_meta():
    """Página para cálculo automático de metas"""
    editais = Edital.query.filter(Edital.DELETED_AT == None).all()
    periodos = PeriodoAvaliacao.query.filter(PeriodoAvaliacao.DELETED_AT == None).all()

    # Verificar se há editais e períodos cadastrados
    if not editais:
        flash('Não há editais cadastrados. Cadastre um edital primeiro.', 'warning')
        return redirect(url_for('edital.lista_editais'))

    if not periodos:
        flash('Não há períodos cadastrados. Cadastre um período primeiro.', 'warning')
        return redirect(url_for('periodo.lista_periodos'))

    # Renderizar apenas a página de cálculo automático
    return render_template('credenciamento/form_meta.html',
                           editais=editais,
                           periodos=periodos)


@meta_bp.route('/metas/editar/<int:id>', methods=['GET', 'POST'])
@login_required
def editar_meta(id):
    # Redirecionar para a lista já que não há mais edição manual
    flash('Use o cálculo automático para criar ou atualizar metas.', 'info')
    return redirect(url_for('meta.lista_metas'))


@meta_bp.route('/metas/excluir/<int:id>')
@login_required
def excluir_meta(id):
    try:
        meta = MetaAvaliacao.query.get_or_404(id)

        # Capturar dados para auditoria
        dados_antigos = {
            'id_edital': meta.ID_EDITAL,
            'id_periodo': meta.ID_PERIODO,
            'id_empresa': meta.ID_EMPRESA,
            'competencia': meta.COMPETENCIA,
            'meta_arrecadacao': float(meta.META_ARRECADACAO) if meta.META_ARRECADACAO else None,
            'meta_acionamento': float(meta.META_ACIONAMENTO) if meta.META_ACIONAMENTO else None,
            'meta_liquidacao': float(meta.META_LIQUIDACAO) if meta.META_LIQUIDACAO else None,
            'meta_bonificacao': float(meta.META_BONIFICACAO) if meta.META_BONIFICACAO else None,
            'deleted_at': None
        }

        meta.DELETED_AT = datetime.utcnow()
        db.session.commit()

        # Registrar log de auditoria
        dados_novos = {'deleted_at': meta.DELETED_AT.strftime('%Y-%m-%d %H:%M:%S')}

        registrar_log(
            acao='excluir',
            entidade='meta',
            entidade_id=meta.ID,
            descricao=f'Exclusão de meta de avaliação para {meta.COMPETENCIA}',
            dados_antigos=dados_antigos,
            dados_novos=dados_novos
        )

        flash('Meta de avaliação removida com sucesso!', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Erro: {str(e)}', 'danger')

    return redirect(url_for('meta.lista_metas'))


@meta_bp.route('/metas/calcular', methods=['POST'])
@login_required
def calcular_metas():
    try:
        edital_id = int(request.form['edital_id'])
        periodo_id = int(request.form['periodo_id'])

        print(f"DEBUG: Calculando metas para edital {edital_id} e período {periodo_id}")

        # Obter período avaliativo
        periodo = PeriodoAvaliacao.query.filter_by(
            ID_EDITAL=edital_id,
            ID=periodo_id
        ).first()

        if not periodo:
            print("DEBUG: Período não encontrado")
            return jsonify({'erro': 'Período não encontrado'}), 404

        print(f"DEBUG: Período encontrado: {periodo.DT_INICIO} a {periodo.DT_FIM}")
        print(f"DEBUG: ID_PERIODO do período: {periodo.ID_PERIODO}")

        # CORREÇÃO: Buscar empresas pelo ID_PERIODO correto
        empresas = EmpresaParticipante.query.filter_by(
            ID_EDITAL=edital_id,
            ID_PERIODO=periodo.ID_PERIODO,  # Usar ID_PERIODO da tabela
            DELETED_AT=None
        ).all()

        print(f"DEBUG: Encontradas {len(empresas)} empresas")

        if not empresas:
            return jsonify({'erro': 'Nenhuma empresa encontrada para este período'}), 404

        # Calcular metas
        metas_calculadas = []
        meses_periodo = obter_meses_periodo(periodo.DT_INICIO, periodo.DT_FIM)

        print(f"DEBUG: Meses do período: {meses_periodo}")

        for ano_mes in meses_periodo:
            for empresa in empresas:
                try:
                    ano, mes = ano_mes.split('-')

                    # Calcular dados
                    dias_uteis = calcular_dias_uteis_mes(int(ano), int(mes))
                    meta_global_siscor = obter_meta_siscor(int(ano), int(mes))
                    saldo_empresa = obter_saldo_devedor_empresa(edital_id, periodo_id, empresa.ID_EMPRESA)
                    total_saldo_devedor = obter_total_saldo_devedor(edital_id, periodo_id)

                    print(f"DEBUG: Empresa {empresa.NO_EMPRESA} - Mês {ano_mes}")
                    print(f"  Dias úteis: {dias_uteis}")
                    print(f"  Meta SISCOR: {meta_global_siscor}")
                    print(f"  Saldo empresa: {saldo_empresa}")
                    print(f"  Total saldo: {total_saldo_devedor}")

                    percentual_participacao = (
                                saldo_empresa / total_saldo_devedor * 100) if total_saldo_devedor > 0 else 0
                    meta_empresa = meta_global_siscor * percentual_participacao / 100

                    # Outras metas
                    meta_acionamento = calcular_meta_acionamento(empresa.ID_EMPRESA, ano_mes)
                    meta_liquidacao = calcular_meta_liquidacao(empresa.ID_EMPRESA, ano_mes)
                    meta_bonificacao = calcular_meta_bonificacao(meta_empresa)

                    metas_calculadas.append({
                        'empresa_id': empresa.ID,
                        'empresa_nome': empresa.NO_EMPRESA_ABREVIADA or empresa.NO_EMPRESA,
                        'competencia': ano_mes,
                        'dias_uteis': dias_uteis,
                        'meta_global': float(meta_global_siscor),
                        'percentual_participacao': float(percentual_participacao),
                        'meta_arrecadacao': float(meta_empresa),
                        'meta_acionamento': float(meta_acionamento),
                        'meta_liquidacao': int(meta_liquidacao),
                        'meta_bonificacao': float(meta_bonificacao)
                    })

                except Exception as e:
                    print(f"ERRO ao calcular meta para {empresa.NO_EMPRESA}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    continue

        print(f"DEBUG: Total de metas calculadas: {len(metas_calculadas)}")

        return jsonify({
            'sucesso': True,
            'metas': metas_calculadas,
            'periodo': {
                'inicio': periodo.DT_INICIO.strftime('%d/%m/%Y'),
                'fim': periodo.DT_FIM.strftime('%d/%m/%Y')
            }
        })

    except Exception as e:
        print(f"ERRO GERAL: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'erro': str(e)}), 500


# Funções auxiliares
def obter_meses_periodo(dt_inicio, dt_fim):
    """Retorna lista de meses no formato YYYY-MM entre as datas"""
    meses = []
    atual = dt_inicio

    while atual <= dt_fim:
        meses.append(atual.strftime('%Y-%m'))
        # Avançar para próximo mês
        if atual.month == 12:
            atual = atual.replace(year=atual.year + 1, month=1, day=1)
        else:
            mes_prox = atual.month + 1
            # Garantir que o dia seja válido para o próximo mês
            ultimo_dia = calendar.monthrange(atual.year, mes_prox)[1]
            dia_prox = min(atual.day, ultimo_dia)
            atual = atual.replace(month=mes_prox, day=dia_prox)

    return meses


def calcular_dias_uteis_mes(ano, mes):
    """Calcula dias úteis do mês consultando a tabela de calendário"""
    sql = text("""
        SELECT COUNT(*) as qtde_dias_uteis
        FROM BDG.AUX_TB004_CALENDARIO
        WHERE ANO = :ano 
        AND MES = :mes
        AND E_DIA_UTIL = 1
    """)

    try:
        result = db.session.execute(sql, {'ano': ano, 'mes': mes}).fetchone()
        return result[0] if result else 20  # Valor padrão se não encontrar
    except Exception as e:
        print(f"ERRO ao buscar dias úteis: {str(e)}")
        return 20  # Valor padrão


def obter_meta_siscor(ano, mes):
    """Obtém meta global do SISCOR para o mês"""
    sql = text("""
        SELECT SUM(VR_PREVISAO_ORCAMENTO) as meta_total
        FROM BDG.COR_TB002_REPROGRAMACAO_ORCAMENTARIA_SISCOR
        WHERE ID_NATUREZA = 3
        AND UNIDADE = 'SUPEC'
        AND DT_PREVISAO_ORCAMENTO/100 = :ano
        AND DT_PREVISAO_ORCAMENTO - (DT_PREVISAO_ORCAMENTO/100 * 100) = :mes
        AND ID_TIPO_FASE_ORC = (
            SELECT MAX(ID_TIPO_FASE_ORC)
            FROM BDG.COR_TB002_REPROGRAMACAO_ORCAMENTARIA_SISCOR
            WHERE DT_PREVISAO_ORCAMENTO/100 = :ano
        )
    """)

    try:
        result = db.session.execute(sql, {'ano': ano, 'mes': mes}).fetchone()
        if result and result[0]:
            return Decimal(str(result[0]))
        else:
            print(f"AVISO: Nenhuma meta SISCOR encontrada para {ano}/{mes}, usando valor padrão")
            return Decimal('1000000.00')  # 1 milhão como padrão
    except Exception as e:
        print(f"ERRO ao buscar meta SISCOR: {str(e)}")
        return Decimal('1000000.00')  # Valor padrão em caso de erro


def obter_total_saldo_devedor(edital_id, periodo_id):
    """Obtém total de saldo devedor distribuído"""
    # Primeiro, obter o ID_PERIODO real
    periodo = PeriodoAvaliacao.query.filter_by(
        ID_EDITAL=edital_id,
        ID=periodo_id
    ).first()

    if not periodo:
        return Decimal('0')

    sql = text("""
        SELECT SUM(VR_SD_DEVEDOR) as total
        FROM DEV.DCA_TB005_DISTRIBUICAO
        WHERE ID_EDITAL = :edital_id
        AND ID_PERIODO = :periodo_id
        AND DELETED_AT IS NULL
    """)

    try:
        result = db.session.execute(sql, {
            'edital_id': edital_id,
            'periodo_id': periodo.ID_PERIODO
        }).fetchone()
        return Decimal(str(result[0])) if result and result[0] else Decimal('0')
    except Exception as e:
        print(f"ERRO ao buscar total saldo devedor: {str(e)}")
        return Decimal('0')


def obter_saldo_devedor_empresa(edital_id, periodo_id, empresa_id):
    """Obtém saldo devedor da empresa"""
    # Primeiro, obter o ID_PERIODO real
    periodo = PeriodoAvaliacao.query.filter_by(
        ID_EDITAL=edital_id,
        ID=periodo_id
    ).first()

    if not periodo:
        return Decimal('0')

    sql = text("""
        SELECT SUM(VR_SD_DEVEDOR) as total
        FROM DEV.DCA_TB005_DISTRIBUICAO
        WHERE ID_EDITAL = :edital_id
        AND ID_PERIODO = :periodo_id
        AND COD_EMPRESA_COBRANCA = :empresa_id
        AND DELETED_AT IS NULL
    """)

    try:
        result = db.session.execute(sql, {
            'edital_id': edital_id,
            'periodo_id': periodo.ID_PERIODO,
            'empresa_id': empresa_id
        }).fetchone()

        return Decimal(str(result[0])) if result and result[0] else Decimal('0')
    except Exception as e:
        print(f"ERRO ao buscar saldo devedor da empresa: {str(e)}")
        return Decimal('0')


def calcular_meta_acionamento(empresa_id, ano_mes):
    """Calcula meta de acionamento baseada em histórico"""
    # Buscar histórico de acionamentos
    sql = text("""
        SELECT AVG(VR_ACIONAMENTO) as media_acionamentos
        FROM BDG.COM_TB062_REMUNERACAO_ESTIMADA
        WHERE CO_EMPRESA_COBRANCA = :empresa_id
        AND COMPETENCIA = :competencia
    """)

    try:
        result = db.session.execute(sql, {
            'empresa_id': empresa_id,
            'competencia': ano_mes.replace('-', '')
        }).fetchone()

        if result and result[0]:
            return Decimal(str(result[0]))
        else:
            # Se não houver histórico, buscar média geral
            sql_geral = text("""
                SELECT AVG(VR_ACIONAMENTO) as media_geral
                FROM BDG.COM_TB062_REMUNERACAO_ESTIMADA
                WHERE COMPETENCIA = :competencia
            """)
            result = db.session.execute(sql_geral, {
                'competencia': ano_mes.replace('-', '')
            }).fetchone()

            return Decimal(str(result[0])) if result and result[0] else Decimal('10000.00')
    except Exception as e:
        print(f"ERRO ao calcular meta acionamento: {str(e)}")
        return Decimal('10000.00')  # Valor padrão


def calcular_meta_liquidacao(empresa_id, ano_mes):
    """Calcula meta de liquidação baseada em histórico"""
    ano, mes = ano_mes.split('-')

    sql = text("""
        SELECT COUNT(DISTINCT fkContratoSISCTR) as qtde_liquidacoes
        FROM BDG.COM_TB009_ACORDOS_LIQUIDADOS_VIGENTES  
        WHERE COD_EMPRESA_COBRANCA = :empresa_id
        AND YEAR(DT_LIQUIDACAO) = :ano
        AND MONTH(DT_LIQUIDACAO) = :mes
    """)

    try:
        result = db.session.execute(sql, {
            'empresa_id': empresa_id,
            'ano': int(ano),
            'mes': int(mes)
        }).fetchone()

        if result and result[0]:
            return result[0]
        else:
            # Se não houver histórico, calcular proporcionalmente
            sql_proporcional = text("""
                SELECT COUNT(*) * 0.1 as meta_estimada
                FROM DEV.DCA_TB005_DISTRIBUICAO
                WHERE COD_EMPRESA_COBRANCA = :empresa_id
                AND DELETED_AT IS NULL
            """)
            result = db.session.execute(sql_proporcional, {'empresa_id': empresa_id}).fetchone()
            return int(result[0]) if result and result[0] else 100
    except Exception as e:
        print(f"ERRO ao calcular meta liquidação: {str(e)}")
        return 100  # Valor padrão


def calcular_meta_bonificacao(meta_arrecadacao):
    """Calcula meta de bonificação baseada na arrecadação"""
    # 5% da meta de arrecadação como bonificação
    return Decimal(str(meta_arrecadacao)) * Decimal('0.05')


@meta_bp.route('/metas/salvar-calculadas', methods=['POST'])
@login_required
def salvar_metas_calculadas():
    """Salva as metas calculadas após confirmação do usuário"""
    try:
        metas_data = request.json['metas']
        edital_id = int(request.json['edital_id'])
        periodo_id = int(request.json['periodo_id'])

        # Obter o período real
        periodo = PeriodoAvaliacao.query.filter_by(
            ID_EDITAL=edital_id,
            ID=periodo_id
        ).first()

        if not periodo:
            return jsonify({'erro': 'Período não encontrado'}), 404

        metas_salvas = []

        for meta_data in metas_data:
            # Buscar empresa para obter o ID_EMPRESA correto
            empresa = EmpresaParticipante.query.get_or_404(meta_data['empresa_id'])

            # Verificar se já existe meta para esta combinação
            meta_existente = MetaAvaliacao.query.filter_by(
                ID_EDITAL=edital_id,
                ID_PERIODO=periodo.ID_PERIODO,  # Usar ID_PERIODO do período
                ID_EMPRESA=empresa.ID_EMPRESA,
                COMPETENCIA=meta_data['competencia'],
                DELETED_AT=None
            ).first()

            if meta_existente:
                # Atualizar meta existente
                meta_existente.META_ARRECADACAO = meta_data['meta_arrecadacao']
                meta_existente.META_ACIONAMENTO = meta_data['meta_acionamento']
                meta_existente.META_LIQUIDACAO = meta_data['meta_liquidacao']
                meta_existente.META_BONIFICACAO = meta_data['meta_bonificacao']
                meta_existente.UPDATED_AT = datetime.utcnow()
            else:
                # Criar nova meta
                nova_meta = MetaAvaliacao(
                    ID_EDITAL=edital_id,
                    ID_PERIODO=periodo.ID_PERIODO,  # Usar ID_PERIODO do período
                    ID_EMPRESA=empresa.ID_EMPRESA,
                    COMPETENCIA=meta_data['competencia'],
                    META_ARRECADACAO=meta_data['meta_arrecadacao'],
                    META_ACIONAMENTO=meta_data['meta_acionamento'],
                    META_LIQUIDACAO=meta_data['meta_liquidacao'],
                    META_BONIFICACAO=meta_data['meta_bonificacao']
                )
                db.session.add(nova_meta)

            metas_salvas.append(meta_data['competencia'])

        db.session.commit()

        # Registrar log de auditoria
        registrar_log(
            acao='calcular_metas',
            entidade='meta',
            entidade_id=f"{edital_id}-{periodo_id}",
            descricao=f'Cálculo e salvamento de metas para o período {periodo.ID_PERIODO}',
            dados_novos={'metas_salvas': metas_salvas}
        )

        return jsonify({
            'sucesso': True,
            'mensagem': f'{len(metas_salvas)} metas salvas com sucesso'
        })

    except Exception as e:
        db.session.rollback()
        print(f"ERRO ao salvar metas: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'erro': str(e)}), 500