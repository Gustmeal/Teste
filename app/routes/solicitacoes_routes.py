# app/routes/solicitacoes_routes.py
"""
Aplicativo "Solicitações à Área" (Portal GEINC).

Objetivo: registrar TUDO o que as pessoas pedem para a área fazer
(aplicativos e painéis), quem pediu, de que área, o andamento, quem
executou e a homologação final — protegendo a área com um histórico
completo e um Termo de Entrega e Homologação assinado.

Papéis (calculados por solicitação em _papel):
  GESTOR      admin/moderador: cria, edita, atualiza, muda status, gera
              termo, conclui (homologada / não homologada), exclui.
  EQUIPE      executor da solicitação: vê tudo (inclusive registros
              internos), comenta, lança atualizações/horas e marca itens
              do escopo. Não altera os dados da solicitação.
  SOLICITANTE quem pediu: vê o andamento PÚBLICO, comenta, confirma a
              ciência do pedido, homologa (aprova/recusa) gerando o termo
              e anexa o termo assinado. Não vê registros internos e não
              altera nada.
  OBSERVADOR  acompanha como o solicitante, mas só comenta.

Tabelas (script em app/sql/solicitacoes_area.sql):
  BDG.SOL_TB001_SOLICITACOES, SOL_TB002_ENVOLVIDOS, SOL_TB003_INTERACOES,
  SOL_TB004_ITENS_ESCOPO, SOL_TB005_ARQUIVOS, SOL_TB006_HISTORICO

Todas as consultas usam text() parametrizado. Compatível com Python 3.9 e 3.12.
"""
import hashlib
import json
from collections import OrderedDict, defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from io import BytesIO

from flask import (Blueprint, abort, flash, g, jsonify, redirect, render_template, request,
                   send_file, url_for)
from flask_login import current_user, login_required
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from sqlalchemy import text

from app import db
from app.auth.decorators import sistema_requerido
from app.utils import solicitacoes_documento as termo
from app.utils.audit import registrar_log

solicitacoes_bp = Blueprint('solicitacoes', __name__, url_prefix='/solicitacoes')

SISTEMA = 'solicitacoes_area'

TB_SOL = 'BDG.SOL_TB001_SOLICITACOES'
TB_ENV = 'BDG.SOL_TB002_ENVOLVIDOS'
TB_INT = 'BDG.SOL_TB003_INTERACOES'
TB_ITEM = 'BDG.SOL_TB004_ITENS_ESCOPO'
TB_ARQ = 'BDG.SOL_TB005_ARQUIVOS'
TB_HIST = 'BDG.SOL_TB006_HISTORICO'
TB_USU = 'BDG.APK_TB002_USUARIOS'
TB_EMP = 'BDG.PES_TB001_EMPREGADOS'
TB_CAL = '[BDDASHBOARDBI].BDG.PAR_TB020_CALENDARIO'

# Área que executa as solicitações: só empregados dela aparecem em "Quem vai fazer / fez".
# Vale se a sigla bater com o setor, a superintendência ou a diretoria do empregado.
AREA_EXECUTORA = 'GEINC'

TAMANHO_MAX_ARQUIVO = 15 * 1024 * 1024  # 15 MB
EXTENSOES_PERMITIDAS = {'pdf', 'doc', 'docx', 'xls', 'xlsx', 'csv', 'png', 'jpg', 'jpeg', 'gif',
                        'msg', 'eml', 'txt', 'zip', 'pptx', 'ppt', 'odt', 'p7s'}

# =========================================================================
# CATÁLOGOS (domínios do aplicativo — o front recebe tudo daqui)
# =========================================================================
STATUS = OrderedDict([
    ('RECEBIDA', {'rotulo': 'Recebida', 'cor': '#6c757d', 'icone': 'fa-inbox', 'grupo': 'ABERTA'}),
    ('EM_ANALISE', {'rotulo': 'Em análise', 'cor': '#0dcaf0', 'icone': 'fa-search', 'grupo': 'ABERTA'}),
    ('PLANEJADA', {'rotulo': 'Planejada', 'cor': '#6610f2', 'icone': 'fa-calendar-check', 'grupo': 'ABERTA'}),
    ('EM_DESENVOLVIMENTO', {'rotulo': 'Em desenvolvimento', 'cor': '#4e73df', 'icone': 'fa-code', 'grupo': 'ABERTA'}),
    ('EM_TESTES', {'rotulo': 'Em testes', 'cor': '#fd7e14', 'icone': 'fa-vial', 'grupo': 'ABERTA'}),
    ('AGUARDANDO_HOMOLOGACAO', {'rotulo': 'Aguardando homologação', 'cor': '#f6c23e', 'icone': 'fa-file-signature', 'grupo': 'ABERTA'}),
    ('SUSPENSA', {'rotulo': 'Suspensa', 'cor': '#858796', 'icone': 'fa-pause-circle', 'grupo': 'ABERTA'}),
    ('CONCLUIDA_HOMOLOGADA', {'rotulo': 'Concluída e homologada', 'cor': '#1cc88a', 'icone': 'fa-check-double', 'grupo': 'FINALIZADA'}),
    ('CONCLUIDA_NAO_HOMOLOGADA', {'rotulo': 'Concluída e não homologada', 'cor': '#e74a3b', 'icone': 'fa-times-circle', 'grupo': 'FINALIZADA'}),
    ('CANCELADA', {'rotulo': 'Cancelada', 'cor': '#343a40', 'icone': 'fa-ban', 'grupo': 'FINALIZADA'}),
])
STATUS_FINAIS = ('CONCLUIDA_HOMOLOGADA', 'CONCLUIDA_NAO_HOMOLOGADA', 'CANCELADA')
STATUS_CONCLUIDOS = ('CONCLUIDA_HOMOLOGADA', 'CONCLUIDA_NAO_HOMOLOGADA')
# Status que o gestor pode escolher manualmente (os demais têm fluxo próprio)
STATUS_MANUAIS = ('RECEBIDA', 'EM_ANALISE', 'PLANEJADA', 'EM_DESENVOLVIMENTO', 'EM_TESTES', 'SUSPENSA', 'CANCELADA')
# Status que exigem justificativa ao serem escolhidos
STATUS_COM_JUSTIFICATIVA = ('SUSPENSA', 'CANCELADA')
# Etapas do "stepper" da tela de detalhe
ETAPAS = [
    ('RECEBIDA', 'Recebida'), ('EM_ANALISE', 'Análise'), ('PLANEJADA', 'Planejada'),
    ('EM_DESENVOLVIMENTO', 'Desenvolvimento'), ('EM_TESTES', 'Testes'),
    ('AGUARDANDO_HOMOLOGACAO', 'Homologação'), ('CONCLUIDA', 'Concluída'),
]

TIPOS_ENTREGA = OrderedDict([
    ('APLICATIVO', {'rotulo': 'Aplicativo', 'icone': 'fa-laptop-code', 'cor': '#4e73df',
                    'descricao': 'Sistema / módulo com telas, cadastros e regras de negócio'}),
    ('PAINEL', {'rotulo': 'Painel', 'icone': 'fa-chart-pie', 'cor': '#36b9cc',
                'descricao': 'Dashboard / painel de indicadores (BI)'}),
])

CATEGORIAS = OrderedDict([
    ('NOVO', 'Novo desenvolvimento'), ('MELHORIA', 'Melhoria / evolução'),
    ('CORRECAO', 'Correção de erro'), ('AJUSTE', 'Ajuste de dados / regra'),
    ('RELATORIO', 'Relatório / extração'), ('AUTOMACAO', 'Automação de processo'),
    ('INTEGRACAO', 'Integração de sistemas'), ('OUTROS', 'Outros'),
])

PRIORIDADES = OrderedDict([
    ('BAIXA', {'rotulo': 'Baixa', 'cor': '#1cc88a', 'peso': 1}),
    ('MEDIA', {'rotulo': 'Média', 'cor': '#36b9cc', 'peso': 2}),
    ('ALTA', {'rotulo': 'Alta', 'cor': '#f6c23e', 'peso': 3}),
    ('CRITICA', {'rotulo': 'Crítica', 'cor': '#e74a3b', 'peso': 4}),
])

IMPACTOS = OrderedDict([('BAIXO', 'Baixo'), ('MEDIO', 'Médio'), ('ALTO', 'Alto')])

COMPLEXIDADES = OrderedDict([
    ('P', 'P — até 1 semana'), ('M', 'M — até 1 mês'),
    ('G', 'G — até 3 meses'), ('GG', 'GG — mais de 3 meses'),
])

CANAIS = OrderedDict([
    ('EMAIL', {'rotulo': 'E-mail', 'icone': 'fa-envelope'}),
    ('TEAMS', {'rotulo': 'Teams', 'icone': 'fa-comments'}),
    ('REUNIAO', {'rotulo': 'Reunião', 'icone': 'fa-users'}),
    ('SEI', {'rotulo': 'Processo SEI', 'icone': 'fa-file-alt'}),
    ('PRESENCIAL', {'rotulo': 'Presencial', 'icone': 'fa-user'}),
    ('TELEFONE', {'rotulo': 'Telefone', 'icone': 'fa-phone'}),
    ('PORTAL', {'rotulo': 'Portal GEINC', 'icone': 'fa-globe'}),
])

TIPOS_INTERACAO = OrderedDict([
    ('ATUALIZACAO', {'rotulo': 'Atualização', 'icone': 'fa-sync-alt', 'cor': '#4e73df'}),
    ('COMENTARIO', {'rotulo': 'Comentário', 'icone': 'fa-comment-dots', 'cor': '#36b9cc'}),
    ('MARCO', {'rotulo': 'Marco atingido', 'icone': 'fa-flag-checkered', 'cor': '#1cc88a'}),
    ('IMPEDIMENTO', {'rotulo': 'Impedimento', 'icone': 'fa-exclamation-triangle', 'cor': '#e74a3b'}),
    ('STATUS', {'rotulo': 'Mudança de status', 'icone': 'fa-exchange-alt', 'cor': '#6610f2'}),
    ('ENTREGA', {'rotulo': 'Entrega', 'icone': 'fa-box-open', 'cor': '#fd7e14'}),
    ('HOMOLOGACAO', {'rotulo': 'Homologação', 'icone': 'fa-file-signature', 'cor': '#f6c23e'}),
    ('SISTEMA', {'rotulo': 'Registro do sistema', 'icone': 'fa-robot', 'cor': '#858796'}),
])
# Tipos que cada papel pode lançar pela caixa de atualização
TIPOS_POR_PAPEL = {
    'GESTOR': ('ATUALIZACAO', 'COMENTARIO', 'MARCO', 'IMPEDIMENTO'),
    'EQUIPE': ('ATUALIZACAO', 'COMENTARIO', 'MARCO', 'IMPEDIMENTO'),
    'SOLICITANTE': ('COMENTARIO',),
    'OBSERVADOR': ('COMENTARIO',),
}

TIPOS_ARQUIVO = OrderedDict([
    ('EVIDENCIA', {'rotulo': 'Evidência do pedido', 'icone': 'fa-envelope-open-text', 'cor': '#4e73df'}),
    ('ANEXO', {'rotulo': 'Anexo', 'icone': 'fa-paperclip', 'cor': '#858796'}),
    ('TERMO_GERADO', {'rotulo': 'Termo gerado', 'icone': 'fa-file-pdf', 'cor': '#e74a3b'}),
    ('TERMO_ASSINADO', {'rotulo': 'Termo assinado', 'icone': 'fa-file-signature', 'cor': '#1cc88a'}),
])
ARQUIVOS_POR_PAPEL = {
    'GESTOR': ('EVIDENCIA', 'ANEXO', 'TERMO_ASSINADO'),
    'EQUIPE': ('ANEXO',),
    'SOLICITANTE': ('ANEXO', 'TERMO_ASSINADO'),
    'OBSERVADOR': (),
}

PAPEIS = {
    'GESTOR': {'rotulo': 'Gestor', 'icone': 'fa-user-shield', 'cor': '#4e73df'},
    'EQUIPE': {'rotulo': 'Equipe executora', 'icone': 'fa-user-cog', 'cor': '#6610f2'},
    'SOLICITANTE': {'rotulo': 'Solicitante', 'icone': 'fa-user-tag', 'cor': '#1cc88a'},
    'OBSERVADOR': {'rotulo': 'Observador', 'icone': 'fa-eye', 'cor': '#858796'},
}

CAMPOS_HISTORICO = OrderedDict([
    ('TITULO', 'Título'), ('DESCRICAO', 'Descrição'), ('TIPO_ENTREGA', 'Tipo de entrega'),
    ('CATEGORIA', 'Categoria'), ('PRIORIDADE', 'Prioridade'), ('IMPACTO', 'Impacto'),
    ('COMPLEXIDADE', 'Complexidade'), ('CANAL_ORIGEM', 'Canal de origem'),
    ('NU_REFERENCIA', 'Referência'), ('NO_SISTEMA', 'Aplicativo / painel'), ('ETIQUETAS', 'Etiquetas'),
    ('ID_USUARIO_SOLICITANTE', 'Solicitante'), ('DT_SOLICITACAO', 'Data da solicitação'),
    ('DT_PRAZO_DESEJADO', 'Prazo desejado'), ('DT_PRAZO_ACORDADO', 'Prazo acordado'),
    ('HORAS_ESTIMADAS', 'Horas estimadas'),
])


def _catalogos():
    """Tudo que o front precisa para montar selects, cores e rótulos."""
    return {
        'status': STATUS, 'status_manuais': STATUS_MANUAIS, 'status_finais': STATUS_FINAIS,
        'status_com_justificativa': STATUS_COM_JUSTIFICATIVA,
        'tipos_entrega': TIPOS_ENTREGA, 'categorias': CATEGORIAS, 'prioridades': PRIORIDADES,
        'impactos': IMPACTOS, 'complexidades': COMPLEXIDADES, 'canais': CANAIS,
        'tipos_interacao': TIPOS_INTERACAO, 'tipos_arquivo': TIPOS_ARQUIVO, 'papeis': PAPEIS,
    }


# =========================================================================
# UTILITÁRIOS
# =========================================================================
def _rows(sql, params=None):
    return [dict(r) for r in db.session.execute(text(sql), params or {}).mappings().all()]


def _row(sql, params=None):
    r = db.session.execute(text(sql), params or {}).mappings().first()
    return dict(r) if r else None


def _gestor():
    return getattr(current_user, 'perfil', None) in ('admin', 'moderador')


def _membro_area():
    """
    True se o usuário logado é empregado da área executora (AREA_EXECUTORA = GEINC),
    pelo setor, superintendência ou diretoria do cadastro de empregados.
    Membros da área podem REGISTRAR solicitações e EDITAR as que criaram ou em que
    são executores, sem precisar ser moderador. Dashboard, exportação, status,
    entrega, conclusão e exclusão continuam só para administrador/moderador.
    Guardado em g para consultar o banco uma vez por requisição.
    """
    if not hasattr(g, 'sol_membro_area'):
        try:
            u = _dados_usuario(_uid()) or {}
            siglas = [(u.get(c) or '').strip().upper() for c in ('SG_SETOR', 'SG_SUPERINTENDENCIA', 'SG_DIRETORIA')]
            g.sol_membro_area = AREA_EXECUTORA.upper() in siglas
        except Exception:
            db.session.rollback()
            g.sol_membro_area = False
    return g.sol_membro_area


def _pode_criar():
    """Administrador, moderador ou qualquer empregado da área executora."""
    return _gestor() or _membro_area()


def _pode_editar(sol, papel):
    """Gestor edita tudo; membro da área edita as que registrou ou em que é executor."""
    if papel == 'GESTOR':
        return True
    return _membro_area() and (sol.get('ID_USUARIO_CRIACAO') == _uid() or papel == 'EQUIPE')


def _uid():
    return int(current_user.id)


def _unome():
    return getattr(current_user, 'nome', None) or ''


def _limpo(valor, tamanho=None):
    if valor is None:
        return None
    texto = str(valor).strip()
    if not texto:
        return None
    return texto[:tamanho] if tamanho else texto


def _parse_data(valor):
    texto = _limpo(valor)
    if not texto:
        return None
    for fmt in ('%Y-%m-%d', '%Y%m%d', '%d/%m/%Y'):
        try:
            return datetime.strptime(texto, fmt).date()
        except ValueError:
            continue
    return None


def _parse_decimal(valor):
    texto = _limpo(valor)
    if not texto:
        return None
    texto = texto.replace('.', '').replace(',', '.') if ',' in texto else texto
    try:
        numero = Decimal(texto)
    except InvalidOperation:
        return None
    return numero if numero >= 0 else None


def _parse_int(valor, minimo=None, maximo=None):
    try:
        numero = int(str(valor).strip())
    except (TypeError, ValueError):
        return None
    if minimo is not None and numero < minimo:
        numero = minimo
    if maximo is not None and numero > maximo:
        numero = maximo
    return numero


def _como_data(valor):
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor
    if isinstance(valor, str):
        return _parse_data(valor[:10])
    return None


def _fmt_data(valor, com_hora=False):
    if not valor:
        return ''
    if isinstance(valor, datetime):
        return valor.strftime('%d/%m/%Y %H:%M' if com_hora else '%d/%m/%Y')
    if isinstance(valor, date):
        return valor.strftime('%d/%m/%Y')
    return str(valor)


def _ok(**extra):
    dados = {'sucesso': True}
    dados.update(extra)
    return jsonify(dados)


def _erro(mensagem, codigo=400):
    return jsonify({'sucesso': False, 'mensagem': mensagem}), codigo


def _cargo_usuario_logado():
    emp = getattr(current_user, 'empregado', None)
    if isinstance(emp, dict):
        return emp.get('cargo') or ''
    return ''


# =========================================================================
# USUÁRIOS E ÁREA (a área vem SEMPRE do cadastro de empregados)
# =========================================================================
SQL_USUARIOS = """
    SELECT U.ID, U.NOME, U.EMAIL, U.PERFIL,
           E.sgSetor AS SG_SETOR, E.sgSuperintendencia AS SG_SUPERINTENDENCIA,
           E.sgDiretoria AS SG_DIRETORIA, E.dsCargo AS DS_CARGO
    FROM """ + TB_USU + """ U
    OUTER APPLY (
        SELECT TOP 1 EE.sgSetor, EE.sgSuperintendencia, EE.sgDiretoria, EE.dsCargo
        FROM """ + TB_EMP + """ EE
        WHERE EE.pkPessoa = U.FK_PESSOA
        ORDER BY EE.DT_REFERENCIA DESC
    ) E
"""


def _usuarios_ativos():
    return _rows(SQL_USUARIOS + " WHERE U.ATIVO = 1 AND U.DELETED_AT IS NULL ORDER BY U.NOME")


def _executores_area(manter_ids=None):
    """
    Usuários ativos da área executora (AREA_EXECUTORA), para "Quem vai fazer / fez".
    manter_ids: executores já gravados na solicitação que devem continuar na lista
    mesmo que tenham mudado de área (para a edição não perder ninguém).
    """
    manter = set(int(i) for i in (manter_ids or []))
    sigla = AREA_EXECUTORA.upper()
    lista = []
    for u in _usuarios_ativos():
        siglas = [(u.get(c) or '').strip().upper() for c in ('SG_SETOR', 'SG_SUPERINTENDENCIA', 'SG_DIRETORIA')]
        if sigla in siglas or u['ID'] in manter:
            lista.append(u)
    return lista


def _dados_usuario(usuario_id):
    if not usuario_id:
        return None
    return _row(SQL_USUARIOS + " WHERE U.ID = :id", {'id': usuario_id})


# =========================================================================
# CARGA DA SOLICITAÇÃO E PERMISSÕES
# =========================================================================
def _carregar(id_solicitacao):
    return _row("SELECT * FROM " + TB_SOL + " WHERE ID_SOLICITACAO = :id AND DELETED_AT IS NULL",
                {'id': id_solicitacao})


def _envolvidos(id_solicitacao):
    return _rows("""
        SELECT ID_ENVOLVIDO, ID_USUARIO, NO_USUARIO, PAPEL, DS_FUNCAO, CREATED_AT
        FROM """ + TB_ENV + """
        WHERE ID_SOLICITACAO = :id AND DELETED_AT IS NULL
        ORDER BY PAPEL, NO_USUARIO
    """, {'id': id_solicitacao})


def _papel(sol, envolvidos):
    if _gestor():
        return 'GESTOR'
    uid = _uid()
    if any(e['ID_USUARIO'] == uid and e['PAPEL'] == 'EXECUTOR' for e in envolvidos):
        return 'EQUIPE'
    if sol.get('ID_USUARIO_CRIACAO') == uid and _membro_area():
        return 'EQUIPE'   # membro da área que registrou a solicitação
    if sol['ID_USUARIO_SOLICITANTE'] == uid or any(
            e['ID_USUARIO'] == uid and e['PAPEL'] == 'SOLICITANTE' for e in envolvidos):
        return 'SOLICITANTE'
    if any(e['ID_USUARIO'] == uid and e['PAPEL'] == 'OBSERVADOR' for e in envolvidos):
        return 'OBSERVADOR'
    return None


def _cosolicitantes_com_area(envolvidos):
    """Outros solicitantes (PAPEL = 'SOLICITANTE') com cargo e área atuais do cadastro de empregados."""
    ids = [e['ID_USUARIO'] for e in envolvidos if e['PAPEL'] == 'SOLICITANTE']
    if not ids:
        return []
    dados = {u['ID']: u for u in _rows(SQL_USUARIOS + " WHERE U.ID IN ({})".format(','.join(str(int(i)) for i in ids)))}
    lista = []
    for e in envolvidos:
        if e['PAPEL'] != 'SOLICITANTE':
            continue
        u = dados.get(e['ID_USUARIO']) or {}
        lista.append({
            'id': e['ID_USUARIO'], 'nome': e['NO_USUARIO'], 'iniciais': _iniciais(e['NO_USUARIO']),
            'cargo': u.get('DS_CARGO') or '', 'email': u.get('EMAIL') or '',
            'area': ' / '.join([x for x in [u.get('SG_DIRETORIA'), u.get('SG_SUPERINTENDENCIA'), u.get('SG_SETOR')] if x]) or '—',
        })
    return lista


def _ve_interno(papel):
    return papel in ('GESTOR', 'EQUIPE')


def _acesso(id_solicitacao):
    """Carrega solicitação + envolvidos + papel; aborta 404/403 se não puder ver."""
    sol = _carregar(id_solicitacao)
    if not sol:
        abort(404)
    env = _envolvidos(id_solicitacao)
    papel = _papel(sol, env)
    if not papel:
        abort(403)
    return sol, env, papel


def _enriquecer(sol, hoje=None):
    """Acrescenta rótulos, cores e indicadores de prazo calculados no back end."""
    hoje = hoje or date.today()
    st = STATUS.get(sol.get('STATUS')) or STATUS['RECEBIDA']
    sol['status_rotulo'] = st['rotulo']
    sol['status_cor'] = st['cor']
    sol['status_icone'] = st['icone']
    sol['finalizada'] = sol.get('STATUS') in STATUS_FINAIS
    tipo = TIPOS_ENTREGA.get(sol.get('TIPO_ENTREGA')) or {}
    sol['tipo_rotulo'] = tipo.get('rotulo', sol.get('TIPO_ENTREGA'))
    sol['tipo_icone'] = tipo.get('icone', 'fa-question')
    sol['tipo_cor'] = tipo.get('cor', '#858796')
    pr = PRIORIDADES.get(sol.get('PRIORIDADE')) or PRIORIDADES['MEDIA']
    sol['prioridade_rotulo'] = pr['rotulo']
    sol['prioridade_cor'] = pr['cor']
    sol['categoria_rotulo'] = CATEGORIAS.get(sol.get('CATEGORIA'), '')
    canal = CANAIS.get(sol.get('CANAL_ORIGEM')) or {}
    sol['canal_rotulo'] = canal.get('rotulo', '')
    sol['canal_icone'] = canal.get('icone', 'fa-question')
    sol['impacto_rotulo'] = IMPACTOS.get(sol.get('IMPACTO'), '')
    sol['complexidade_rotulo'] = COMPLEXIDADES.get(sol.get('COMPLEXIDADE'), '')
    sol['area'] = sol.get('SG_SUPERINTENDENCIA') or sol.get('SG_SETOR') or sol.get('SG_DIRETORIA') or '—'
    sol['area_completa'] = ' / '.join([x for x in [sol.get('SG_DIRETORIA'), sol.get('SG_SUPERINTENDENCIA'),
                                                   sol.get('SG_SETOR')] if x]) or '—'
    sol['etiquetas_lista'] = [e.strip() for e in (sol.get('ETIQUETAS') or '').split(',') if e.strip()]

    prazo = _como_data(sol.get('DT_PRAZO_ACORDADO')) or _como_data(sol.get('DT_PRAZO_DESEJADO'))
    sol['prazo_ref'] = prazo
    sol['prazo_tipo'] = 'acordado' if sol.get('DT_PRAZO_ACORDADO') else ('desejado' if prazo else '')
    inicio = _como_data(sol.get('DT_SOLICITACAO')) or hoje
    fim = _como_data(sol.get('DT_CONCLUSAO')) if sol['finalizada'] else hoje
    sol['dias_em_aberto'] = max(0, ((fim or hoje) - inicio).days)
    sol['dias_restantes'] = None
    sol['atrasada'] = False
    sol['situacao_prazo'] = 'Sem prazo definido'
    sol['situacao_prazo_cor'] = '#858796'
    if prazo:
        entrega = _como_data(sol.get('DT_ENTREGA'))
        if sol['finalizada'] or entrega:
            referencia = entrega or _como_data(sol.get('DT_CONCLUSAO')) or hoje
            if referencia <= prazo:
                sol['situacao_prazo'], sol['situacao_prazo_cor'] = 'Entregue no prazo', '#1cc88a'
            else:
                sol['situacao_prazo'] = 'Entregue com {} dia(s) de atraso'.format((referencia - prazo).days)
                sol['situacao_prazo_cor'] = '#e74a3b'
        else:
            dias = (prazo - hoje).days
            sol['dias_restantes'] = dias
            if dias < 0:
                sol['atrasada'] = True
                sol['situacao_prazo'] = 'Atrasada há {} dia(s)'.format(-dias)
                sol['situacao_prazo_cor'] = '#e74a3b'
            elif dias <= 5:
                sol['situacao_prazo'] = 'Vence em {} dia(s)'.format(dias) if dias else 'Vence hoje'
                sol['situacao_prazo_cor'] = '#f6c23e'
            else:
                sol['situacao_prazo'] = 'No prazo ({} dias restantes)'.format(dias)
                sol['situacao_prazo_cor'] = '#1cc88a'
    entrega = _como_data(sol.get('DT_ENTREGA'))
    sol['dias_ate_entrega'] = (entrega - inicio).days if entrega else None
    return sol


# =========================================================================
# GRAVAÇÕES AUXILIARES
# =========================================================================
def _interacao(id_solicitacao, tipo, texto, interna=False, pc=None, horas=None,
               st_ant=None, st_novo=None):
    db.session.execute(text("""
        INSERT INTO """ + TB_INT + """
            (ID_SOLICITACAO, TIPO, TEXTO, FL_INTERNA, PC_CONCLUSAO, QT_HORAS,
             STATUS_ANTERIOR, STATUS_NOVO, ID_USUARIO, NO_USUARIO, CREATED_AT)
        VALUES (:id, :tipo, :texto, :interna, :pc, :horas, :sa, :sn, :uid, :unome, GETDATE())
    """), {'id': id_solicitacao, 'tipo': tipo, 'texto': texto, 'interna': 1 if interna else 0,
           'pc': pc, 'horas': horas, 'sa': st_ant, 'sn': st_novo, 'uid': _uid(), 'unome': _unome()})


def _historico(id_solicitacao, campo, anterior, novo):
    db.session.execute(text("""
        INSERT INTO """ + TB_HIST + """
            (ID_SOLICITACAO, CAMPO, VALOR_ANTERIOR, VALOR_NOVO, ID_USUARIO, NO_USUARIO, CREATED_AT)
        VALUES (:id, :campo, :ant, :novo, :uid, :unome, GETDATE())
    """), {'id': id_solicitacao, 'campo': campo, 'ant': anterior, 'novo': novo,
           'uid': _uid(), 'unome': _unome()})


def _gerar_protocolo(ano):
    prefixo = 'SOL-{}-'.format(ano)
    ultimo = db.session.execute(text("""
        SELECT MAX(CAST(SUBSTRING(NU_PROTOCOLO, 10, 10) AS INT))
        FROM """ + TB_SOL + """ WITH (UPDLOCK, HOLDLOCK)
        WHERE NU_PROTOCOLO LIKE :prefixo
    """), {'prefixo': prefixo + '%'}).scalar() or 0
    return '{}{:04d}'.format(prefixo, int(ultimo) + 1)


def _gravar_arquivo(id_solicitacao, tipo, nome, content_type, conteudo, interno=False,
                    observacao=None, hash_hex=None, versao=None):
    hash_hex = hash_hex or hashlib.sha256(conteudo).hexdigest()
    return db.session.execute(text("""
        INSERT INTO """ + TB_ARQ + """
            (ID_SOLICITACAO, TIPO, NO_ARQUIVO, DS_CONTENT_TYPE, QT_BYTES, CONTEUDO, HASH_SHA256,
             NU_VERSAO, FL_INTERNO, DS_OBSERVACAO, ID_USUARIO, NO_USUARIO, CREATED_AT)
        OUTPUT INSERTED.ID_ARQUIVO
        VALUES (:id, :tipo, :nome, :ct, :qt, :conteudo, :hash, :versao, :interno, :obs, :uid, :unome, GETDATE())
    """), {'id': id_solicitacao, 'tipo': tipo, 'nome': nome[:255], 'ct': (content_type or '')[:150],
           'qt': len(conteudo), 'conteudo': conteudo, 'hash': hash_hex, 'versao': versao,
           'interno': 1 if interno else 0, 'obs': _limpo(observacao, 300),
           'uid': _uid(), 'unome': _unome()}).scalar()


def _ler_upload(arquivo):
    """Valida e lê um arquivo enviado. Retorna (nome, content_type, bytes) ou levanta ValueError."""
    nome = (arquivo.filename or '').replace('\\', '/').split('/')[-1].strip()
    if not nome:
        raise ValueError('Arquivo sem nome.')
    extensao = nome.rsplit('.', 1)[-1].lower() if '.' in nome else ''
    if extensao not in EXTENSOES_PERMITIDAS:
        raise ValueError('Extensão ".{}" não permitida.'.format(extensao))
    conteudo = arquivo.read()
    if not conteudo:
        raise ValueError('O arquivo "{}" está vazio.'.format(nome))
    if len(conteudo) > TAMANHO_MAX_ARQUIVO:
        raise ValueError('O arquivo "{}" passa de 15 MB.'.format(nome))
    return nome, arquivo.mimetype or 'application/octet-stream', conteudo


PAPEIS_ENVOLVIDO = {'EXECUTOR': 'executor', 'OBSERVADOR': 'observador', 'SOLICITANTE': 'solicitante'}


def _sincronizar_envolvidos(id_solicitacao, executores, observadores, cosolicitantes=None):
    """
    executores: lista de (id_usuario, funcao); observadores e cosolicitantes: listas de id_usuario.
    Co-solicitantes (outras pessoas que também pediram) ficam com PAPEL = 'SOLICITANTE';
    o solicitante principal continua na própria SOL_TB001.
    Mantém quem continua, desativa quem saiu e inclui os novos.
    Retorna lista de textos com as mudanças (para o histórico).
    """
    atuais = _envolvidos(id_solicitacao)
    mudancas = []
    desejado = {}
    for uid, funcao in executores:
        desejado[(uid, 'EXECUTOR')] = funcao
    for uid in (cosolicitantes or []):
        desejado[(uid, 'SOLICITANTE')] = None
    ja = set(k[0] for k in desejado)
    for uid in observadores:
        if uid not in ja:
            desejado[(uid, 'OBSERVADOR')] = None

    for env in atuais:
        chave = (env['ID_USUARIO'], env['PAPEL'])
        if chave not in desejado:
            db.session.execute(text("UPDATE " + TB_ENV + " SET DELETED_AT = GETDATE() WHERE ID_ENVOLVIDO = :id"),
                               {'id': env['ID_ENVOLVIDO']})
            mudancas.append('{} removido(a) como {}'.format(env['NO_USUARIO'], PAPEIS_ENVOLVIDO.get(env['PAPEL'], env['PAPEL'].lower())))
        else:
            nova_funcao = desejado.pop(chave)
            if env['PAPEL'] == 'EXECUTOR' and (nova_funcao or None) != (env['DS_FUNCAO'] or None):
                db.session.execute(text("UPDATE " + TB_ENV + " SET DS_FUNCAO = :f WHERE ID_ENVOLVIDO = :id"),
                                   {'f': nova_funcao, 'id': env['ID_ENVOLVIDO']})

    if desejado:
        nomes = {u['ID']: u['NOME'] for u in _rows(
            "SELECT ID, NOME FROM " + TB_USU + " WHERE ID IN ({})".format(
                ','.join(str(int(k[0])) for k in desejado)))}
        for (uid, papel), funcao in desejado.items():
            if uid not in nomes:
                continue
            db.session.execute(text("""
                INSERT INTO """ + TB_ENV + """
                    (ID_SOLICITACAO, ID_USUARIO, NO_USUARIO, PAPEL, DS_FUNCAO, ID_USUARIO_CRIACAO, CREATED_AT)
                VALUES (:id, :uid, :nome, :papel, :funcao, :criador, GETDATE())
            """), {'id': id_solicitacao, 'uid': uid, 'nome': nomes[uid], 'papel': papel,
                   'funcao': funcao, 'criador': _uid()})
            mudancas.append('{} incluído(a) como {}'.format(nomes[uid], PAPEIS_ENVOLVIDO.get(papel, papel.lower())))
    return mudancas


def _dados_formulario():
    """Lê e valida o formulário. Retorna (dados, executores, observadores, cosolicitantes, erros)."""
    f = request.form
    erros = []
    dados = {
        'TITULO': _limpo(f.get('titulo'), 200),
        'DESCRICAO': _limpo(f.get('descricao')),
        'TIPO_ENTREGA': _limpo(f.get('tipo_entrega')),
        'CATEGORIA': _limpo(f.get('categoria')),
        'PRIORIDADE': _limpo(f.get('prioridade')) or 'MEDIA',
        'IMPACTO': _limpo(f.get('impacto')),
        'COMPLEXIDADE': _limpo(f.get('complexidade')),
        'CANAL_ORIGEM': _limpo(f.get('canal_origem')),
        'NU_REFERENCIA': _limpo(f.get('nu_referencia'), 60),
        'NO_SISTEMA': _limpo(f.get('no_sistema'), 150),
        'ETIQUETAS': None,
        'ID_USUARIO_SOLICITANTE': _parse_int(f.get('solicitante_id')),
        'DT_SOLICITACAO': _parse_data(f.get('dt_solicitacao')),
        'DT_PRAZO_DESEJADO': _parse_data(f.get('dt_prazo_desejado')),
        'DT_PRAZO_ACORDADO': _parse_data(f.get('dt_prazo_acordado')),
        'HORAS_ESTIMADAS': _parse_decimal(f.get('horas_estimadas')),
    }
    etiquetas = [e.strip()[:40] for e in (f.get('etiquetas') or '').split(',') if e.strip()]
    dados['ETIQUETAS'] = ', '.join(OrderedDict.fromkeys(etiquetas))[:300] or None

    if not dados['TITULO']:
        erros.append('Informe o título da solicitação.')
    if dados['TIPO_ENTREGA'] not in TIPOS_ENTREGA:
        erros.append('Escolha se a entrega é um Aplicativo ou um Painel.')
    if dados['CATEGORIA'] and dados['CATEGORIA'] not in CATEGORIAS:
        dados['CATEGORIA'] = None
    if dados['PRIORIDADE'] not in PRIORIDADES:
        dados['PRIORIDADE'] = 'MEDIA'
    if dados['IMPACTO'] and dados['IMPACTO'] not in IMPACTOS:
        dados['IMPACTO'] = None
    if dados['COMPLEXIDADE'] and dados['COMPLEXIDADE'] not in COMPLEXIDADES:
        dados['COMPLEXIDADE'] = None
    if dados['CANAL_ORIGEM'] and dados['CANAL_ORIGEM'] not in CANAIS:
        dados['CANAL_ORIGEM'] = None
    if not dados['ID_USUARIO_SOLICITANTE']:
        erros.append('Selecione o usuário que fez a solicitação.')
    if not dados['DT_SOLICITACAO']:
        erros.append('Informe a data da solicitação.')
    for campo, rotulo in (('DT_PRAZO_DESEJADO', 'desejado'), ('DT_PRAZO_ACORDADO', 'acordado')):
        if dados[campo] and dados['DT_SOLICITACAO'] and dados[campo] < dados['DT_SOLICITACAO']:
            erros.append('O prazo {} não pode ser anterior à data da solicitação.'.format(rotulo))

    executores = []
    vistos = set()
    ids = request.form.getlist('executor_id')
    funcoes = request.form.getlist('executor_funcao')
    for i, valor in enumerate(ids):
        uid = _parse_int(valor)
        if uid and uid not in vistos:
            vistos.add(uid)
            executores.append((uid, _limpo(funcoes[i] if i < len(funcoes) else None, 60)))
    # Outros solicitantes: não repetem o principal (que fica na SOL_TB001)
    cosolicitantes = []
    for valor in request.form.getlist('cosolicitante_id'):
        uid = _parse_int(valor)
        if uid and uid != dados['ID_USUARIO_SOLICITANTE'] and uid not in cosolicitantes:
            cosolicitantes.append(uid)
    observadores = []
    for valor in request.form.getlist('observador_id'):
        uid = _parse_int(valor)
        if uid and uid not in vistos and uid != dados['ID_USUARIO_SOLICITANTE'] and uid not in cosolicitantes:
            vistos.add(uid)
            observadores.append(uid)
    return dados, executores, observadores, cosolicitantes, erros


# =========================================================================
# LISTA / KANBAN
# =========================================================================
def _filtros_lista():
    a = request.args
    return {
        'q': _limpo(a.get('q'), 100) or '',
        'status': _limpo(a.get('status')) or '',
        'situacao': _limpo(a.get('situacao')) or '',
        'tipo': _limpo(a.get('tipo')) or '',
        'prioridade': _limpo(a.get('prioridade')) or '',
        'area': _limpo(a.get('area')) or '',
        'executor': _parse_int(a.get('executor')) or '',
        'solicitante': _parse_int(a.get('solicitante')) or '',
        'ano': _parse_int(a.get('ano')) or '',
        'visao': a.get('visao') if a.get('visao') in ('lista', 'kanban') else 'lista',
    }


def _consultar_lista(filtros, papel_global_gestor):
    where = ['S.DELETED_AT IS NULL']
    params = {}
    if not papel_global_gestor:
        where.append("""(S.ID_USUARIO_SOLICITANTE = :uid OR S.ID_USUARIO_CRIACAO = :uid OR EXISTS (
            SELECT 1 FROM """ + TB_ENV + """ EV WHERE EV.ID_SOLICITACAO = S.ID_SOLICITACAO
            AND EV.ID_USUARIO = :uid AND EV.DELETED_AT IS NULL))""")
        params['uid'] = _uid()
    if filtros['q']:
        where.append("""(S.TITULO LIKE :q OR S.NU_PROTOCOLO LIKE :q OR S.DESCRICAO LIKE :q
                        OR S.NO_SISTEMA LIKE :q OR S.ETIQUETAS LIKE :q OR S.NO_SOLICITANTE LIKE :q
                        OR S.NU_REFERENCIA LIKE :q)""")
        params['q'] = '%{}%'.format(filtros['q'])
    if filtros['status'] in STATUS:
        where.append('S.STATUS = :status')
        params['status'] = filtros['status']
    if filtros['situacao'] == 'abertas':
        where.append("S.STATUS NOT IN ('CONCLUIDA_HOMOLOGADA','CONCLUIDA_NAO_HOMOLOGADA','CANCELADA')")
    elif filtros['situacao'] == 'finalizadas':
        where.append("S.STATUS IN ('CONCLUIDA_HOMOLOGADA','CONCLUIDA_NAO_HOMOLOGADA','CANCELADA')")
    elif filtros['situacao'] == 'atrasadas':
        where.append("""S.STATUS NOT IN ('CONCLUIDA_HOMOLOGADA','CONCLUIDA_NAO_HOMOLOGADA','CANCELADA')
                        AND S.DT_ENTREGA IS NULL
                        AND ISNULL(S.DT_PRAZO_ACORDADO, S.DT_PRAZO_DESEJADO) < CAST(GETDATE() AS DATE)""")
    if filtros['tipo'] in TIPOS_ENTREGA:
        where.append('S.TIPO_ENTREGA = :tipo')
        params['tipo'] = filtros['tipo']
    if filtros['prioridade'] in PRIORIDADES:
        where.append('S.PRIORIDADE = :prioridade')
        params['prioridade'] = filtros['prioridade']
    if filtros['area']:
        where.append('S.SG_SUPERINTENDENCIA = :area')
        params['area'] = filtros['area']
    if filtros['executor']:
        where.append("""EXISTS (SELECT 1 FROM """ + TB_ENV + """ EX WHERE EX.ID_SOLICITACAO = S.ID_SOLICITACAO
                        AND EX.ID_USUARIO = :executor AND EX.PAPEL = 'EXECUTOR' AND EX.DELETED_AT IS NULL)""")
        params['executor'] = filtros['executor']
    if filtros['solicitante']:
        where.append('S.ID_USUARIO_SOLICITANTE = :solicitante')
        params['solicitante'] = filtros['solicitante']
    if filtros['ano']:
        where.append('YEAR(S.DT_SOLICITACAO) = :ano')
        params['ano'] = filtros['ano']

    lista = _rows("""
        SELECT S.ID_SOLICITACAO, S.NU_PROTOCOLO, S.TITULO, S.TIPO_ENTREGA, S.CATEGORIA, S.PRIORIDADE,
               S.STATUS, S.PC_CONCLUSAO, S.NO_SOLICITANTE, S.ID_USUARIO_SOLICITANTE,
               S.SG_SETOR, S.SG_SUPERINTENDENCIA, S.SG_DIRETORIA, S.NO_SISTEMA, S.ETIQUETAS,
               S.CANAL_ORIGEM, S.NU_REFERENCIA, S.IMPACTO, S.COMPLEXIDADE,
               S.DT_SOLICITACAO, S.DT_PRAZO_DESEJADO, S.DT_PRAZO_ACORDADO, S.DT_ENTREGA, S.DT_CONCLUSAO,
               S.DT_CIENCIA_SOLICITANTE, S.ST_HOMOLOGACAO_SOLICITANTE, S.CREATED_AT, S.UPDATED_AT,
               (SELECT COUNT(*) FROM """ + TB_INT + """ I WHERE I.ID_SOLICITACAO = S.ID_SOLICITACAO
                   AND I.DELETED_AT IS NULL AND I.TIPO = 'COMENTARIO' AND I.FL_INTERNA = 0) AS QT_COMENTARIOS,
               (SELECT COUNT(*) FROM """ + TB_ITEM + """ IT WHERE IT.ID_SOLICITACAO = S.ID_SOLICITACAO
                   AND IT.DELETED_AT IS NULL) AS QT_ITENS,
               (SELECT COUNT(*) FROM """ + TB_ITEM + """ IT WHERE IT.ID_SOLICITACAO = S.ID_SOLICITACAO
                   AND IT.DELETED_AT IS NULL AND IT.FL_CONCLUIDO = 1) AS QT_ITENS_OK,
               (SELECT MAX(I.CREATED_AT) FROM """ + TB_INT + """ I WHERE I.ID_SOLICITACAO = S.ID_SOLICITACAO
                   AND I.DELETED_AT IS NULL) AS DT_ULTIMA_INTERACAO
        FROM """ + TB_SOL + """ S
        WHERE """ + ' AND '.join(where) + """
        ORDER BY S.DT_SOLICITACAO DESC, S.ID_SOLICITACAO DESC
    """, params)

    hoje = date.today()
    executores = defaultdict(list)
    cosolicitantes = defaultdict(list)
    if lista:
        ids = ','.join(str(int(s['ID_SOLICITACAO'])) for s in lista)
        for env in _rows("""
            SELECT ID_SOLICITACAO, ID_USUARIO, NO_USUARIO, DS_FUNCAO, PAPEL FROM """ + TB_ENV + """
            WHERE PAPEL IN ('EXECUTOR', 'SOLICITANTE') AND DELETED_AT IS NULL AND ID_SOLICITACAO IN (""" + ids + """)
            ORDER BY NO_USUARIO"""):
            (executores if env['PAPEL'] == 'EXECUTOR' else cosolicitantes)[env['ID_SOLICITACAO']].append(env)
    for s in lista:
        _enriquecer(s, hoje)
        s['executores'] = executores.get(s['ID_SOLICITACAO'], [])
        s['cosolicitantes'] = cosolicitantes.get(s['ID_SOLICITACAO'], [])
        s['ids_solicitantes'] = [s['ID_USUARIO_SOLICITANTE']] + [c['ID_USUARIO'] for c in s['cosolicitantes']]
        s['iniciais'] = [_iniciais(e['NO_USUARIO']) for e in s['executores']]
    return lista


def _iniciais(nome):
    """Iniciais para o avatar: primeiro e último nome (ignora 'da', 'de', 'dos'...)."""
    partes = (nome or '').split()
    if not partes:
        return '?'
    resto = [p for p in partes[1:] if len(p) > 2]
    return (partes[0][0] + (resto[-1][0] if resto else '')).upper()


@solicitacoes_bp.route('/')
@login_required
@sistema_requerido(SISTEMA)
def index():
    filtros = _filtros_lista()
    gestor = _gestor()
    try:
        lista = _consultar_lista(filtros, gestor)
    except Exception as e:  # tabela ainda não criada, por exemplo
        db.session.rollback()
        flash('Não foi possível carregar as solicitações: {}'.format(e), 'danger')
        lista = []

    resumo = {
        'total': len(lista),
        'abertas': sum(1 for s in lista if not s['finalizada']),
        'atrasadas': sum(1 for s in lista if s['atrasada']),
        'aguardando': sum(1 for s in lista if s['STATUS'] == 'AGUARDANDO_HOMOLOGACAO'),
        'homologadas': sum(1 for s in lista if s['STATUS'] == 'CONCLUIDA_HOMOLOGADA'),
        'nao_homologadas': sum(1 for s in lista if s['STATUS'] == 'CONCLUIDA_NAO_HOMOLOGADA'),
    }

    # Pendências do usuário logado como solicitante
    uid = _uid()
    pendencias = {
        'homologar': [s for s in lista if uid in s['ids_solicitantes']
                      and s['STATUS'] == 'AGUARDANDO_HOMOLOGACAO' and not s['ST_HOMOLOGACAO_SOLICITANTE']],
        'ciencia': [s for s in lista if uid in s['ids_solicitantes']
                    and not s['DT_CIENCIA_SOLICITANTE'] and not s['finalizada']],
    }

    # Kanban: uma coluna por status aberto + coluna das finalizadas
    colunas = OrderedDict()
    for chave, st in STATUS.items():
        if st['grupo'] == 'ABERTA':
            colunas[chave] = {'chave': chave, 'rotulo': st['rotulo'], 'cor': st['cor'],
                              'icone': st['icone'], 'itens': []}
    colunas['FINALIZADAS'] = {'chave': 'FINALIZADAS', 'rotulo': 'Finalizadas', 'cor': '#1cc88a',
                              'icone': 'fa-flag-checkered', 'itens': []}
    for s in lista:
        destino = 'FINALIZADAS' if s['finalizada'] else s['STATUS']
        if destino in colunas:
            colunas[destino]['itens'].append(s)

    try:
        areas = [r['SG'] for r in _rows("""
            SELECT DISTINCT SG_SUPERINTENDENCIA AS SG FROM """ + TB_SOL + """
            WHERE DELETED_AT IS NULL AND SG_SUPERINTENDENCIA IS NOT NULL ORDER BY 1""")] if gestor else []
        executores_opcoes = _rows("""
            SELECT DISTINCT ID_USUARIO, NO_USUARIO FROM """ + TB_ENV + """
            WHERE PAPEL = 'EXECUTOR' AND DELETED_AT IS NULL ORDER BY NO_USUARIO""") if gestor else []
        anos = [r['ANO'] for r in _rows("""
            SELECT DISTINCT YEAR(DT_SOLICITACAO) AS ANO FROM """ + TB_SOL + """
            WHERE DELETED_AT IS NULL ORDER BY 1 DESC""")]
    except Exception:
        db.session.rollback()
        areas, executores_opcoes, anos = [], [], []

    return render_template('solicitacoes/index.html', lista=lista, resumo=resumo, filtros=filtros,
                           colunas=list(colunas.values()), pendencias=pendencias, gestor=gestor,
                           pode_criar=_pode_criar(),
                           areas=areas, executores_opcoes=executores_opcoes, anos=anos,
                           cat=_catalogos())


@solicitacoes_bp.route('/exportar')
@login_required
@sistema_requerido(SISTEMA)
def exportar():
    if not _gestor():
        abort(403)
    filtros = _filtros_lista()
    lista = _consultar_lista(filtros, True)

    wb = Workbook()
    ws = wb.active
    ws.title = 'Solicitações'
    cab = ['Protocolo', 'Título', 'Tipo', 'Categoria', 'Prioridade', 'Status', '% Conclusão',
           'Solicitante(s)', 'Diretoria', 'Superintendência', 'Setor', 'Executores', 'Aplicativo/Painel',
           'Referência', 'Canal', 'Data solicitação', 'Prazo desejado', 'Prazo acordado',
           'Entrega', 'Conclusão', 'Situação do prazo', 'Dias em aberto', 'Homologação solicitante',
           'Etiquetas']
    ws.append(cab)
    for s in lista:
        ws.append([
            s['NU_PROTOCOLO'], s['TITULO'], s['tipo_rotulo'], s['categoria_rotulo'], s['prioridade_rotulo'],
            s['status_rotulo'], s['PC_CONCLUSAO'],
            ', '.join([s['NO_SOLICITANTE'] or ''] + [c['NO_USUARIO'] for c in s['cosolicitantes']]), s['SG_DIRETORIA'],
            s['SG_SUPERINTENDENCIA'], s['SG_SETOR'],
            ', '.join(e['NO_USUARIO'] for e in s['executores']), s['NO_SISTEMA'], s['NU_REFERENCIA'],
            s['canal_rotulo'], _como_data(s['DT_SOLICITACAO']), _como_data(s['DT_PRAZO_DESEJADO']),
            _como_data(s['DT_PRAZO_ACORDADO']), s['DT_ENTREGA'], s['DT_CONCLUSAO'], s['situacao_prazo'],
            s['dias_em_aberto'], s['ST_HOMOLOGACAO_SOLICITANTE'], s['ETIQUETAS'],
        ])
    fill = PatternFill('solid', fgColor='224ABE')
    for i, _ in enumerate(cab, start=1):
        c = ws.cell(row=1, column=i)
        c.font = Font(bold=True, color='FFFFFF')
        c.fill = fill
        c.alignment = Alignment(vertical='center', wrap_text=True)
        ws.column_dimensions[get_column_letter(i)].width = 18
    ws.column_dimensions['B'].width = 45
    for linha in ws.iter_rows(min_row=2):
        for c in linha:
            if isinstance(c.value, datetime):
                c.number_format = 'DD/MM/YYYY HH:MM'
            elif isinstance(c.value, date):
                c.number_format = 'DD/MM/YYYY'
    ws.freeze_panes = 'A2'
    ws.auto_filter.ref = ws.dimensions

    saida = BytesIO()
    wb.save(saida)
    saida.seek(0)
    registrar_log('exportar', 'solicitacoes_area', 0, 'Exportou {} solicitações para Excel'.format(len(lista)))
    return send_file(saida, as_attachment=True,
                     download_name='solicitacoes_{}.xlsx'.format(datetime.now().strftime('%Y%m%d_%H%M')),
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


# =========================================================================
# CRIAR / EDITAR (somente gestor)
# =========================================================================
@solicitacoes_bp.route('/api/usuarios/<int:usuario_id>/area')
@login_required
@sistema_requerido(SISTEMA)
def api_area_usuario(usuario_id):
    if not _pode_criar():
        return _erro('Acesso restrito.', 403)
    u = _dados_usuario(usuario_id)
    if not u:
        return _erro('Usuário não encontrado.', 404)
    return _ok(usuario={
        'id': u['ID'], 'nome': u['NOME'], 'email': u['EMAIL'], 'cargo': u['DS_CARGO'] or '',
        'setor': u['SG_SETOR'] or '', 'superintendencia': u['SG_SUPERINTENDENCIA'] or '',
        'diretoria': u['SG_DIRETORIA'] or '',
        'tem_area': bool(u['SG_SETOR'] or u['SG_SUPERINTENDENCIA'] or u['SG_DIRETORIA']),
    }, historico=_rows("""
        SELECT TOP 5 ID_SOLICITACAO, NU_PROTOCOLO, TITULO, STATUS, DT_SOLICITACAO
        FROM """ + TB_SOL + """
        WHERE ID_USUARIO_SOLICITANTE = :id AND DELETED_AT IS NULL
        ORDER BY DT_SOLICITACAO DESC, ID_SOLICITACAO DESC""", {'id': usuario_id}))


def _render_form(sol=None, envolvidos=None, form=None):
    atuais = [e['ID_USUARIO'] for e in (envolvidos or []) if e['PAPEL'] == 'EXECUTOR']
    usuarios = _usuarios_ativos()
    return render_template('solicitacoes/form.html', sol=sol, envolvidos=envolvidos or [],
                           usuarios=usuarios, grupos_gerencia=_agrupar_por_gerencia(usuarios),
                           executores_opcoes=_executores_area(atuais),
                           area_executora=AREA_EXECUTORA, cat=_catalogos(), form=form,
                           hoje=date.today().strftime('%Y-%m-%d'))


def _agrupar_por_gerencia(usuarios):
    """
    Agrupa os usuários pela gerência (setor do cadastro de empregados) para os
    dropdowns de observadores e de outros solicitantes. Quem não tem vínculo no
    cadastro fica no grupo "Sem gerência no cadastro", no fim.
    """
    grupos = OrderedDict()
    for u in usuarios:
        setor = (u.get('SG_SETOR') or '').strip()
        chave = setor or '~SEM'
        if chave not in grupos:
            sup = (u.get('SG_SUPERINTENDENCIA') or '').strip()
            grupos[chave] = {
                'chave': chave,
                'setor': setor or 'Sem gerência no cadastro',
                'superintendencia': sup if setor else '',
                'usuarios': [],
            }
        grupos[chave]['usuarios'].append(u)
    return sorted(grupos.values(), key=lambda g: (g['chave'] == '~SEM', g['superintendencia'], g['setor']))


def _validar_executores(executores, envolvidos_atuais=None):
    """Garante no back end que só entram executores da área executora (ou os que já estavam)."""
    atuais = [e['ID_USUARIO'] for e in (envolvidos_atuais or []) if e['PAPEL'] == 'EXECUTOR']
    permitidos = set(u['ID'] for u in _executores_area(atuais))
    fora = [uid for uid, _ in executores if uid not in permitidos]
    if fora:
        return ['Só podem ser executores usuários da {}.'.format(AREA_EXECUTORA)]
    return []


@solicitacoes_bp.route('/nova', methods=['GET', 'POST'])
@login_required
@sistema_requerido(SISTEMA)
def nova():
    if not _pode_criar():
        flash('Somente administradores, moderadores e empregados da {} registram solicitações.'.format(AREA_EXECUTORA), 'warning')
        return redirect(url_for('solicitacoes.index'))
    if request.method == 'GET':
        return _render_form()

    dados, executores, observadores, cosolicitantes, erros = _dados_formulario()
    erros += _validar_executores(executores)
    solicitante = _dados_usuario(dados['ID_USUARIO_SOLICITANTE']) if dados['ID_USUARIO_SOLICITANTE'] else None
    if dados['ID_USUARIO_SOLICITANTE'] and not solicitante:
        erros.append('Usuário solicitante não encontrado.')
    arquivos = [a for a in request.files.getlist('evidencias') if a and a.filename]
    lidos = []
    for a in arquivos:
        try:
            lidos.append(_ler_upload(a))
        except ValueError as e:
            erros.append(str(e))
    if erros:
        for e in erros:
            flash(e, 'danger')
        return _render_form(form=request.form)

    try:
        protocolo = _gerar_protocolo(dados['DT_SOLICITACAO'].year)
        params = dict(dados)
        params.update({
            'NU_PROTOCOLO': protocolo,
            'NO_SOLICITANTE': solicitante['NOME'], 'EMAIL_SOLICITANTE': solicitante['EMAIL'],
            'SG_SETOR': solicitante['SG_SETOR'], 'SG_SUPERINTENDENCIA': solicitante['SG_SUPERINTENDENCIA'],
            'SG_DIRETORIA': solicitante['SG_DIRETORIA'], 'DS_CARGO_SOLICITANTE': solicitante['DS_CARGO'],
            'UID': _uid(),
        })
        novo_id = db.session.execute(text("""
            INSERT INTO """ + TB_SOL + """
                (NU_PROTOCOLO, TITULO, DESCRICAO, TIPO_ENTREGA, CATEGORIA, PRIORIDADE, IMPACTO, COMPLEXIDADE,
                 CANAL_ORIGEM, NU_REFERENCIA, NO_SISTEMA, ETIQUETAS,
                 ID_USUARIO_SOLICITANTE, NO_SOLICITANTE, EMAIL_SOLICITANTE, SG_SETOR, SG_SUPERINTENDENCIA,
                 SG_DIRETORIA, DS_CARGO_SOLICITANTE,
                 DT_SOLICITACAO, DT_PRAZO_DESEJADO, DT_PRAZO_ACORDADO, HORAS_ESTIMADAS,
                 STATUS, PC_CONCLUSAO, ID_USUARIO_CRIACAO, CREATED_AT)
            OUTPUT INSERTED.ID_SOLICITACAO
            VALUES
                (:NU_PROTOCOLO, :TITULO, :DESCRICAO, :TIPO_ENTREGA, :CATEGORIA, :PRIORIDADE, :IMPACTO, :COMPLEXIDADE,
                 :CANAL_ORIGEM, :NU_REFERENCIA, :NO_SISTEMA, :ETIQUETAS,
                 :ID_USUARIO_SOLICITANTE, :NO_SOLICITANTE, :EMAIL_SOLICITANTE, :SG_SETOR, :SG_SUPERINTENDENCIA,
                 :SG_DIRETORIA, :DS_CARGO_SOLICITANTE,
                 :DT_SOLICITACAO, :DT_PRAZO_DESEJADO, :DT_PRAZO_ACORDADO, :HORAS_ESTIMADAS,
                 'RECEBIDA', 0, :UID, GETDATE())
        """), params).scalar()

        _sincronizar_envolvidos(novo_id, executores, observadores, cosolicitantes)

        ordem = 0
        for linha in (request.form.get('escopo') or '').splitlines():
            item = _limpo(linha.lstrip('-•* \t'), 500)
            if item:
                ordem += 1
                db.session.execute(text("""
                    INSERT INTO """ + TB_ITEM + """ (ID_SOLICITACAO, DESCRICAO, NU_ORDEM, ID_USUARIO_CRIACAO, CREATED_AT)
                    VALUES (:id, :d, :o, :uid, GETDATE())"""), {'id': novo_id, 'd': item, 'o': ordem, 'uid': _uid()})

        interno_evid = request.form.get('evidencia_interna') == '1'
        for nome, ct, conteudo in lidos:
            _gravar_arquivo(novo_id, 'EVIDENCIA', nome, ct, conteudo, interno=interno_evid,
                            observacao='Evidência registrada na abertura')

        pedido_por = '{} ({})'.format(solicitante['NOME'], solicitante['SG_SUPERINTENDENCIA'] or solicitante['SG_SETOR'] or 'área não informada')
        if cosolicitantes:
            outros = [u['NOME'] for u in _rows("SELECT NOME FROM " + TB_USU + " WHERE ID IN ({}) ORDER BY NOME".format(
                ','.join(str(int(i)) for i in cosolicitantes)))]
            if outros:
                pedido_por += ', junto com ' + ', '.join(outros)
        _interacao(novo_id, 'SISTEMA', 'Solicitação registrada por {} a pedido de {}.'.format(_unome(), pedido_por),
                   st_novo='RECEBIDA')
        db.session.commit()
        registrar_log('criar', 'solicitacoes_area', novo_id,
                      'Registrou a solicitação {} - {}'.format(protocolo, dados['TITULO']))
        flash('Solicitação {} registrada com sucesso.'.format(protocolo), 'success')
        return redirect(url_for('solicitacoes.detalhe', id_solicitacao=novo_id))
    except Exception as e:
        db.session.rollback()
        flash('Erro ao registrar a solicitação: {}'.format(e), 'danger')
        return _render_form(form=request.form)


@solicitacoes_bp.route('/<int:id_solicitacao>/editar', methods=['GET', 'POST'])
@login_required
@sistema_requerido(SISTEMA)
def editar(id_solicitacao):
    sol, env, papel = _acesso(id_solicitacao)
    if not _pode_editar(sol, papel):
        abort(403)
    if request.method == 'GET':
        return _render_form(sol=sol, envolvidos=env)

    dados, executores, observadores, cosolicitantes, erros = _dados_formulario()
    erros += _validar_executores(executores, env)
    solicitante = None
    if dados['ID_USUARIO_SOLICITANTE'] and dados['ID_USUARIO_SOLICITANTE'] != sol['ID_USUARIO_SOLICITANTE']:
        solicitante = _dados_usuario(dados['ID_USUARIO_SOLICITANTE'])
        if not solicitante:
            erros.append('Usuário solicitante não encontrado.')
    if erros:
        for e in erros:
            flash(e, 'danger')
        return _render_form(sol=sol, envolvidos=env, form=request.form)

    try:
        alterados = []
        for campo, rotulo in CAMPOS_HISTORICO.items():
            antes, depois = sol.get(campo), dados.get(campo)
            antes_cmp = _como_data(antes) if isinstance(antes, (date, datetime)) else antes
            if isinstance(antes_cmp, Decimal) or isinstance(depois, Decimal):
                igual = (antes_cmp is None and depois is None) or (
                    antes_cmp is not None and depois is not None and Decimal(str(antes_cmp)) == Decimal(str(depois)))
            else:
                igual = (antes_cmp or None) == (depois or None)
            if not igual:
                alterados.append(rotulo)
                if campo == 'ID_USUARIO_SOLICITANTE':
                    _historico(id_solicitacao, rotulo, sol.get('NO_SOLICITANTE'),
                               solicitante['NOME'] if solicitante else None)
                else:
                    _historico(id_solicitacao, rotulo,
                               _fmt_data(antes) if isinstance(antes, (date, datetime)) else (None if antes is None else str(antes)),
                               _fmt_data(depois) if isinstance(depois, date) else (None if depois is None else str(depois)))

        params = dict(dados)
        params.update({'id': id_solicitacao, 'uid': _uid()})
        set_solicitante = ''
        if solicitante:
            set_solicitante = """, NO_SOLICITANTE = :ns, EMAIL_SOLICITANTE = :es, SG_SETOR = :sset,
                SG_SUPERINTENDENCIA = :ssup, SG_DIRETORIA = :sdir, DS_CARGO_SOLICITANTE = :scargo,
                DT_CIENCIA_SOLICITANTE = NULL"""
            params.update({'ns': solicitante['NOME'], 'es': solicitante['EMAIL'], 'sset': solicitante['SG_SETOR'],
                           'ssup': solicitante['SG_SUPERINTENDENCIA'], 'sdir': solicitante['SG_DIRETORIA'],
                           'scargo': solicitante['DS_CARGO']})
        db.session.execute(text("""
            UPDATE """ + TB_SOL + """ SET
                TITULO = :TITULO, DESCRICAO = :DESCRICAO, TIPO_ENTREGA = :TIPO_ENTREGA, CATEGORIA = :CATEGORIA,
                PRIORIDADE = :PRIORIDADE, IMPACTO = :IMPACTO, COMPLEXIDADE = :COMPLEXIDADE,
                CANAL_ORIGEM = :CANAL_ORIGEM, NU_REFERENCIA = :NU_REFERENCIA, NO_SISTEMA = :NO_SISTEMA,
                ETIQUETAS = :ETIQUETAS, ID_USUARIO_SOLICITANTE = :ID_USUARIO_SOLICITANTE,
                DT_SOLICITACAO = :DT_SOLICITACAO, DT_PRAZO_DESEJADO = :DT_PRAZO_DESEJADO,
                DT_PRAZO_ACORDADO = :DT_PRAZO_ACORDADO, HORAS_ESTIMADAS = :HORAS_ESTIMADAS,
                ID_USUARIO_ATUALIZACAO = :uid, UPDATED_AT = GETDATE()""" + set_solicitante + """
            WHERE ID_SOLICITACAO = :id
        """), params)

        mudancas = _sincronizar_envolvidos(id_solicitacao, executores, observadores, cosolicitantes)
        for m in mudancas:
            _historico(id_solicitacao, 'Envolvidos', None, m)

        if alterados or mudancas:
            partes = []
            if alterados:
                partes.append('campos alterados: ' + ', '.join(alterados))
            if mudancas:
                partes.append('; '.join(mudancas))
            _interacao(id_solicitacao, 'SISTEMA', 'Dados da solicitação atualizados ({}).'.format(' | '.join(partes)),
                       interna=True)
            # Prazo acordado mexe com o solicitante: avisa na linha do tempo pública
            if 'Prazo acordado' in alterados and dados['DT_PRAZO_ACORDADO']:
                _interacao(id_solicitacao, 'SISTEMA', 'Prazo acordado definido para {}.'.format(
                    _fmt_data(dados['DT_PRAZO_ACORDADO'])))
        db.session.commit()
        registrar_log('editar', 'solicitacoes_area', id_solicitacao,
                      'Editou a solicitação {}'.format(sol['NU_PROTOCOLO']))
        flash('Solicitação atualizada.', 'success')
        return redirect(url_for('solicitacoes.detalhe', id_solicitacao=id_solicitacao))
    except Exception as e:
        db.session.rollback()
        flash('Erro ao salvar: {}'.format(e), 'danger')
        return _render_form(sol=sol, envolvidos=env, form=request.form)


@solicitacoes_bp.route('/<int:id_solicitacao>/excluir', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def excluir(id_solicitacao):
    sol, env, papel = _acesso(id_solicitacao)
    if papel != 'GESTOR':
        return _erro('Somente administradores e moderadores podem excluir.', 403)
    motivo = _limpo((request.get_json(silent=True) or {}).get('motivo'), 1000)
    if not motivo:
        return _erro('Informe o motivo da exclusão.')
    try:
        db.session.execute(text("UPDATE " + TB_SOL + " SET DELETED_AT = GETDATE(), ID_USUARIO_ATUALIZACAO = :uid "
                                "WHERE ID_SOLICITACAO = :id"), {'id': id_solicitacao, 'uid': _uid()})
        _interacao(id_solicitacao, 'SISTEMA', 'Solicitação excluída. Motivo: {}'.format(motivo), interna=True)
        db.session.commit()
        registrar_log('excluir', 'solicitacoes_area', id_solicitacao,
                      'Excluiu a solicitação {} - motivo: {}'.format(sol['NU_PROTOCOLO'], motivo))
        return _ok(redirecionar=url_for('solicitacoes.index'))
    except Exception as e:
        db.session.rollback()
        return _erro('Erro ao excluir: {}'.format(e), 500)


# =========================================================================
# DETALHE
# =========================================================================
@solicitacoes_bp.route('/<int:id_solicitacao>')
@login_required
@sistema_requerido(SISTEMA)
def detalhe(id_solicitacao):
    sol, env, papel = _acesso(id_solicitacao)
    ve_interno = _ve_interno(papel)
    _enriquecer(sol)

    filtro_int = '' if ve_interno else ' AND FL_INTERNA = 0'
    interacoes = _rows("""
        SELECT ID_INTERACAO, TIPO, TEXTO, FL_INTERNA, PC_CONCLUSAO, QT_HORAS, STATUS_ANTERIOR, STATUS_NOVO,
               ID_USUARIO, NO_USUARIO, CREATED_AT
        FROM """ + TB_INT + """
        WHERE ID_SOLICITACAO = :id AND DELETED_AT IS NULL""" + filtro_int + """
        ORDER BY CREATED_AT DESC, ID_INTERACAO DESC
    """, {'id': id_solicitacao})
    for i in interacoes:
        ti = TIPOS_INTERACAO.get(i['TIPO']) or TIPOS_INTERACAO['SISTEMA']
        i['rotulo'], i['icone'], i['cor'] = ti['rotulo'], ti['icone'], ti['cor']
        i['status_anterior_rotulo'] = (STATUS.get(i['STATUS_ANTERIOR']) or {}).get('rotulo')
        i['status_novo_rotulo'] = (STATUS.get(i['STATUS_NOVO']) or {}).get('rotulo')
        i['status_novo_cor'] = (STATUS.get(i['STATUS_NOVO']) or {}).get('cor')
        i['iniciais'] = _iniciais(i['NO_USUARIO'])
        i['pode_excluir'] = papel == 'GESTOR' or (i['ID_USUARIO'] == _uid() and i['TIPO'] in ('COMENTARIO', 'ATUALIZACAO'))

    itens = _rows("""
        SELECT ID_ITEM, DESCRICAO, NU_ORDEM, FL_CONCLUIDO, DT_CONCLUSAO, NO_USUARIO_CONCLUSAO
        FROM """ + TB_ITEM + """ WHERE ID_SOLICITACAO = :id AND DELETED_AT IS NULL
        ORDER BY NU_ORDEM, ID_ITEM""", {'id': id_solicitacao})

    filtro_arq = '' if ve_interno else ' AND FL_INTERNO = 0'
    arquivos = _rows("""
        SELECT ID_ARQUIVO, TIPO, NO_ARQUIVO, DS_CONTENT_TYPE, QT_BYTES, HASH_SHA256, NU_VERSAO, FL_INTERNO,
               DS_OBSERVACAO, NO_USUARIO, CREATED_AT
        FROM """ + TB_ARQ + """
        WHERE ID_SOLICITACAO = :id AND DELETED_AT IS NULL""" + filtro_arq + """
        ORDER BY CREATED_AT DESC""", {'id': id_solicitacao})
    for a in arquivos:
        ta = TIPOS_ARQUIVO.get(a['TIPO']) or TIPOS_ARQUIVO['ANEXO']
        a['rotulo'], a['icone'], a['cor'] = ta['rotulo'], ta['icone'], ta['cor']
        tamanho = a['QT_BYTES'] or 0
        a['tamanho'] = '{:.1f} MB'.format(tamanho / 1048576.0) if tamanho >= 1048576 else '{:.0f} KB'.format(max(1, tamanho / 1024.0))

    historico = []
    horas = {'total': Decimal('0'), 'por_pessoa': []}
    if ve_interno:
        historico = _rows("""
            SELECT CAMPO, VALOR_ANTERIOR, VALOR_NOVO, NO_USUARIO, CREATED_AT FROM """ + TB_HIST + """
            WHERE ID_SOLICITACAO = :id ORDER BY CREATED_AT DESC, ID_HISTORICO DESC""", {'id': id_solicitacao})
        horas['por_pessoa'] = _rows("""
            SELECT NO_USUARIO, SUM(QT_HORAS) AS HORAS, COUNT(*) AS REGISTROS FROM """ + TB_INT + """
            WHERE ID_SOLICITACAO = :id AND DELETED_AT IS NULL AND QT_HORAS IS NOT NULL
            GROUP BY NO_USUARIO ORDER BY SUM(QT_HORAS) DESC""", {'id': id_solicitacao})
        horas['total'] = sum((h['HORAS'] or Decimal('0')) for h in horas['por_pessoa'])

    executores = [e for e in env if e['PAPEL'] == 'EXECUTOR']
    observadores = [e for e in env if e['PAPEL'] == 'OBSERVADOR']
    cosolicitantes = _cosolicitantes_com_area(env)
    for e in env:
        e['iniciais'] = _iniciais(e['NO_USUARIO'])

    # Stepper
    if sol['STATUS'] in STATUS_CONCLUIDOS:
        indice_atual = len(ETAPAS) - 1
    else:
        indice_atual = next((i for i, (k, _) in enumerate(ETAPAS) if k == sol['STATUS']), -1)
    etapas = [{'chave': k, 'rotulo': r, 'feita': i < indice_atual or sol['STATUS'] in STATUS_CONCLUIDOS,
               'atual': i == indice_atual and sol['STATUS'] not in STATUS_CONCLUIDOS} for i, (k, r) in enumerate(ETAPAS)]

    termos_gerados = [a for a in arquivos if a['TIPO'] == 'TERMO_GERADO']
    termos_assinados = [a for a in arquivos if a['TIPO'] == 'TERMO_ASSINADO']
    itens_ok = sum(1 for i in itens if i['FL_CONCLUIDO'])
    stats = {
        'comentarios': sum(1 for i in interacoes if i['TIPO'] == 'COMENTARIO'),
        'atualizacoes': sum(1 for i in interacoes if i['TIPO'] in ('ATUALIZACAO', 'MARCO', 'IMPEDIMENTO')),
        'impedimentos': sum(1 for i in interacoes if i['TIPO'] == 'IMPEDIMENTO'),
        'itens_ok': itens_ok, 'itens_total': len(itens),
        'pc_itens': int(round(100.0 * itens_ok / len(itens))) if itens else None,
        'arquivos': len(arquivos),
    }

    permissoes = {
        'editar': _pode_editar(sol, papel),
        'mudar_status': papel == 'GESTOR',
        'atualizar': papel in ('GESTOR', 'EQUIPE'),
        'comentar': True,
        'tipos_interacao': [(k, TIPOS_INTERACAO[k]) for k in TIPOS_POR_PAPEL.get(papel, ())],
        'marcar_interna': ve_interno,
        'gerir_itens': papel == 'GESTOR',
        'marcar_itens': papel in ('GESTOR', 'EQUIPE'),
        'tipos_arquivo': [(k, TIPOS_ARQUIVO[k]) for k in ARQUIVOS_POR_PAPEL.get(papel, ())],
        'entregar': papel == 'GESTOR' and not sol['finalizada'],
        'concluir': papel == 'GESTOR' and not sol['finalizada'],
        'reabrir': papel == 'GESTOR' and sol['finalizada'],
        'ciencia': papel == 'SOLICITANTE' and not sol['DT_CIENCIA_SOLICITANTE'] and not sol['finalizada'],
        'homologar': papel == 'SOLICITANTE' and sol['STATUS'] == 'AGUARDANDO_HOMOLOGACAO',
        'baixar_termo': bool(sol['DT_ENTREGA']) or papel == 'GESTOR',
        'excluir': papel == 'GESTOR',
    }
    status_manuais = [(k, STATUS[k]) for k in STATUS_MANUAIS if k != sol['STATUS']]

    return render_template('solicitacoes/detalhe.html', sol=sol, papel=papel, papel_info=PAPEIS[papel],
                           ve_interno=ve_interno, interacoes=interacoes, itens=itens, arquivos=arquivos,
                           historico=historico, horas=horas, executores=executores, observadores=observadores,
                           cosolicitantes=cosolicitantes,
                           etapas=etapas, termos_gerados=termos_gerados, termos_assinados=termos_assinados,
                           stats=stats, permissoes=permissoes, status_manuais=status_manuais, cat=_catalogos())


# =========================================================================
# INTERAÇÕES (atualizações e comentários)
# =========================================================================
@solicitacoes_bp.route('/<int:id_solicitacao>/interacoes', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def nova_interacao(id_solicitacao):
    sol, env, papel = _acesso(id_solicitacao)
    dados = request.get_json(silent=True) or {}
    tipo = dados.get('tipo') or 'COMENTARIO'
    if tipo not in TIPOS_POR_PAPEL.get(papel, ()):
        return _erro('Você não pode lançar este tipo de registro.', 403)
    texto_int = _limpo(dados.get('texto'), 8000)
    if not texto_int:
        return _erro('Escreva o texto do registro.')
    interna = bool(dados.get('interna')) and _ve_interno(papel)

    pc = None
    horas = None
    if papel in ('GESTOR', 'EQUIPE'):
        pc = _parse_int(dados.get('pc'), 0, 100) if str(dados.get('pc') or '').strip() != '' else None
        horas = _parse_decimal(dados.get('horas'))
        if horas is not None and horas > 999:
            return _erro('Quantidade de horas inválida.')
    if sol['STATUS'] in STATUS_FINAIS and tipo != 'COMENTARIO':
        return _erro('Solicitação finalizada: só é possível comentar. Reabra para lançar atualizações.')

    try:
        _interacao(id_solicitacao, tipo, texto_int, interna=interna, pc=pc, horas=horas)
        campos = ['UPDATED_AT = GETDATE()']
        params = {'id': id_solicitacao}
        if pc is not None and papel in ('GESTOR', 'EQUIPE'):
            campos.append('PC_CONCLUSAO = :pc')
            params['pc'] = pc
            if pc != sol['PC_CONCLUSAO']:
                _historico(id_solicitacao, '% de conclusão', str(sol['PC_CONCLUSAO']), str(pc))
        db.session.execute(text("UPDATE " + TB_SOL + " SET " + ', '.join(campos) + " WHERE ID_SOLICITACAO = :id"),
                           params)
        db.session.commit()
        registrar_log('criar', 'solicitacoes_area', id_solicitacao,
                      '{} na solicitação {}'.format(TIPOS_INTERACAO[tipo]['rotulo'], sol['NU_PROTOCOLO']))
        return _ok()
    except Exception as e:
        db.session.rollback()
        return _erro('Erro ao gravar: {}'.format(e), 500)


@solicitacoes_bp.route('/interacoes/<int:id_interacao>/excluir', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def excluir_interacao(id_interacao):
    reg = _row("SELECT ID_SOLICITACAO, ID_USUARIO, TIPO FROM " + TB_INT +
               " WHERE ID_INTERACAO = :id AND DELETED_AT IS NULL", {'id': id_interacao})
    if not reg:
        return _erro('Registro não encontrado.', 404)
    sol, env, papel = _acesso(reg['ID_SOLICITACAO'])
    if not (papel == 'GESTOR' or (reg['ID_USUARIO'] == _uid() and reg['TIPO'] in ('COMENTARIO', 'ATUALIZACAO'))):
        return _erro('Você não pode excluir este registro.', 403)
    try:
        db.session.execute(text("UPDATE " + TB_INT + " SET DELETED_AT = GETDATE() WHERE ID_INTERACAO = :id"),
                           {'id': id_interacao})
        _historico(reg['ID_SOLICITACAO'], 'Linha do tempo', 'Registro #{} ({})'.format(id_interacao, reg['TIPO']),
                   'Excluído')
        db.session.commit()
        registrar_log('excluir', 'solicitacoes_area', reg['ID_SOLICITACAO'],
                      'Excluiu registro #{} da solicitação {}'.format(id_interacao, sol['NU_PROTOCOLO']))
        return _ok()
    except Exception as e:
        db.session.rollback()
        return _erro('Erro ao excluir: {}'.format(e), 500)


# =========================================================================
# STATUS / CIÊNCIA / ENTREGA / HOMOLOGAÇÃO / CONCLUSÃO
# =========================================================================
@solicitacoes_bp.route('/<int:id_solicitacao>/status', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def mudar_status(id_solicitacao):
    sol, env, papel = _acesso(id_solicitacao)
    if papel != 'GESTOR':
        return _erro('Somente administradores e moderadores mudam o status.', 403)
    dados = request.get_json(silent=True) or {}
    novo = dados.get('status')
    texto_mot = _limpo(dados.get('texto'), 4000)
    interna = bool(dados.get('interna'))
    if novo not in STATUS_MANUAIS:
        return _erro('Status inválido para mudança manual.')
    if novo == sol['STATUS']:
        return _erro('A solicitação já está neste status.')
    reabrindo = sol['STATUS'] in STATUS_FINAIS
    if (novo in STATUS_COM_JUSTIFICATIVA or reabrindo) and not texto_mot:
        return _erro('Informe a justificativa para {}.'.format(
            'reabrir' if reabrindo else STATUS[novo]['rotulo'].lower()))

    try:
        campos = ['STATUS = :novo', 'UPDATED_AT = GETDATE()', 'ID_USUARIO_ATUALIZACAO = :uid']
        if novo == 'EM_DESENVOLVIMENTO' and not sol['DT_INICIO']:
            campos.append('DT_INICIO = GETDATE()')
        if novo == 'CANCELADA':
            campos += ['DT_CONCLUSAO = GETDATE()', 'ID_USUARIO_CONCLUSAO = :uid', 'JUSTIFICATIVA_CONCLUSAO = :texto']
        if reabrindo:
            campos += ['DT_CONCLUSAO = NULL', 'ID_USUARIO_CONCLUSAO = NULL', 'JUSTIFICATIVA_CONCLUSAO = NULL',
                       'DT_ENTREGA = NULL', 'ST_HOMOLOGACAO_SOLICITANTE = NULL', 'PARECER_SOLICITANTE = NULL',
                       'DT_HOMOLOGACAO_SOLICITANTE = NULL', 'ID_USUARIO_HOMOLOGACAO = NULL']
        params = {'novo': novo, 'uid': _uid(), 'id': id_solicitacao}
        if novo == 'CANCELADA':
            params['texto'] = texto_mot
        db.session.execute(text("UPDATE " + TB_SOL + " SET " + ', '.join(campos) + " WHERE ID_SOLICITACAO = :id"),
                           params)
        prefixo = 'Solicitação reaberta' if reabrindo else 'Status alterado'
        msg = '{}: {} → {}.'.format(prefixo, STATUS[sol['STATUS']]['rotulo'], STATUS[novo]['rotulo'])
        if texto_mot:
            msg += ' ' + texto_mot
        _interacao(id_solicitacao, 'STATUS', msg, interna=interna and not reabrindo and novo != 'CANCELADA',
                   st_ant=sol['STATUS'], st_novo=novo)
        _historico(id_solicitacao, 'Status', STATUS[sol['STATUS']]['rotulo'], STATUS[novo]['rotulo'])
        db.session.commit()
        registrar_log('editar', 'solicitacoes_area', id_solicitacao,
                      '{} {}: {} -> {}'.format(prefixo, sol['NU_PROTOCOLO'], sol['STATUS'], novo))
        return _ok()
    except Exception as e:
        db.session.rollback()
        return _erro('Erro ao mudar o status: {}'.format(e), 500)


@solicitacoes_bp.route('/<int:id_solicitacao>/ciencia', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def ciencia(id_solicitacao):
    sol, env, papel = _acesso(id_solicitacao)
    if papel != 'SOLICITANTE':
        return _erro('Somente o solicitante confirma a ciência do pedido.', 403)
    if sol['DT_CIENCIA_SOLICITANTE']:
        return _erro('A ciência já foi registrada.')
    obs = _limpo((request.get_json(silent=True) or {}).get('texto'), 2000)
    try:
        db.session.execute(text("UPDATE " + TB_SOL + " SET DT_CIENCIA_SOLICITANTE = GETDATE(), UPDATED_AT = GETDATE() "
                                "WHERE ID_SOLICITACAO = :id"), {'id': id_solicitacao})
        msg = '{} confirmou que a solicitação registrada corresponde ao que foi pedido.'.format(_unome())
        if obs:
            msg += ' Observação: ' + obs
        _interacao(id_solicitacao, 'SISTEMA', msg)
        db.session.commit()
        registrar_log('editar', 'solicitacoes_area', id_solicitacao,
                      'Ciência do solicitante na solicitação {}'.format(sol['NU_PROTOCOLO']))
        return _ok()
    except Exception as e:
        db.session.rollback()
        return _erro('Erro ao registrar ciência: {}'.format(e), 500)


@solicitacoes_bp.route('/<int:id_solicitacao>/entrega', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def registrar_entrega(id_solicitacao):
    sol, env, papel = _acesso(id_solicitacao)
    if papel != 'GESTOR':
        return _erro('Somente administradores e moderadores registram a entrega.', 403)
    if sol['STATUS'] in STATUS_FINAIS:
        return _erro('Solicitação finalizada. Reabra antes de registrar nova entrega.')
    dados = request.get_json(silent=True) or {}
    resumo = _limpo(dados.get('resumo'), 20000)
    orientacoes = _limpo(dados.get('orientacoes'), 10000)
    link = _limpo(dados.get('link'), 300)
    if not resumo:
        return _erro('Descreva o que foi feito: esse texto vai no termo de homologação.')
    try:
        nova_rodada = sol['STATUS'] != 'AGUARDANDO_HOMOLOGACAO'
        db.session.execute(text("""
            UPDATE """ + TB_SOL + """ SET
                RESUMO_ENTREGA = :resumo, ORIENTACOES_USO = :orient, LINK_ACESSO = :link,
                STATUS = 'AGUARDANDO_HOMOLOGACAO', PC_CONCLUSAO = 100,
                DT_ENTREGA = CASE WHEN :nova = 1 OR DT_ENTREGA IS NULL THEN GETDATE() ELSE DT_ENTREGA END,
                DT_INICIO = ISNULL(DT_INICIO, GETDATE()),
                ST_HOMOLOGACAO_SOLICITANTE = NULL, PARECER_SOLICITANTE = NULL,
                DT_HOMOLOGACAO_SOLICITANTE = NULL, ID_USUARIO_HOMOLOGACAO = NULL,
                UPDATED_AT = GETDATE(), ID_USUARIO_ATUALIZACAO = :uid
            WHERE ID_SOLICITACAO = :id
        """), {'resumo': resumo, 'orient': orientacoes, 'link': link, 'nova': 1 if nova_rodada else 0,
               'uid': _uid(), 'id': id_solicitacao})
        if nova_rodada:
            _interacao(id_solicitacao, 'STATUS', 'Entrega registrada e enviada para homologação do solicitante.',
                       st_ant=sol['STATUS'], st_novo='AGUARDANDO_HOMOLOGACAO')
            _historico(id_solicitacao, 'Status', STATUS[sol['STATUS']]['rotulo'],
                       STATUS['AGUARDANDO_HOMOLOGACAO']['rotulo'])
        _interacao(id_solicitacao, 'ENTREGA', resumo)
        if (sol['RESUMO_ENTREGA'] or '') != resumo:
            _historico(id_solicitacao, 'Resumo da entrega', sol['RESUMO_ENTREGA'], resumo)
        db.session.commit()
        aviso = _guardar_termo(id_solicitacao)
        registrar_log('editar', 'solicitacoes_area', id_solicitacao,
                      'Registrou a entrega da solicitação {}'.format(sol['NU_PROTOCOLO']))
        return _ok(aviso=aviso)
    except Exception as e:
        db.session.rollback()
        return _erro('Erro ao registrar a entrega: {}'.format(e), 500)


@solicitacoes_bp.route('/<int:id_solicitacao>/homologar', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def homologar(id_solicitacao):
    sol, env, papel = _acesso(id_solicitacao)
    if papel != 'SOLICITANTE':
        return _erro('Somente o solicitante homologa a entrega.', 403)
    if sol['STATUS'] != 'AGUARDANDO_HOMOLOGACAO':
        return _erro('A solicitação não está aguardando homologação.')
    dados = request.get_json(silent=True) or {}
    decisao = dados.get('decisao')
    parecer = _limpo(dados.get('parecer'), 8000)
    if decisao not in ('APROVADA', 'RECUSADA'):
        return _erro('Escolha se a entrega atende ou não atende ao pedido.')
    if decisao == 'RECUSADA' and not parecer:
        return _erro('Explique o que não atende ao pedido.')
    try:
        db.session.execute(text("""
            UPDATE """ + TB_SOL + """ SET ST_HOMOLOGACAO_SOLICITANTE = :d, PARECER_SOLICITANTE = :p,
                DT_HOMOLOGACAO_SOLICITANTE = GETDATE(), ID_USUARIO_HOMOLOGACAO = :uid, UPDATED_AT = GETDATE()
            WHERE ID_SOLICITACAO = :id"""), {'d': decisao, 'p': parecer, 'uid': _uid(), 'id': id_solicitacao})
        msg = '{} {} a entrega.'.format(_unome(), 'APROVOU' if decisao == 'APROVADA' else 'RECUSOU')
        if parecer:
            msg += ' Parecer: ' + parecer
        _interacao(id_solicitacao, 'HOMOLOGACAO', msg)
        _historico(id_solicitacao, 'Homologação do solicitante', sol['ST_HOMOLOGACAO_SOLICITANTE'], decisao)
        db.session.commit()
        aviso = _guardar_termo(id_solicitacao)
        registrar_log('editar', 'solicitacoes_area', id_solicitacao,
                      'Homologação {} pelo solicitante em {}'.format(decisao, sol['NU_PROTOCOLO']))
        return _ok(aviso=aviso,
                   download=url_for('solicitacoes.baixar_termo', id_solicitacao=id_solicitacao, formato='pdf'))
    except Exception as e:
        db.session.rollback()
        return _erro('Erro ao registrar a homologação: {}'.format(e), 500)


@solicitacoes_bp.route('/<int:id_solicitacao>/concluir', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def concluir(id_solicitacao):
    sol, env, papel = _acesso(id_solicitacao)
    if papel != 'GESTOR':
        return _erro('Somente administradores e moderadores concluem a solicitação.', 403)
    if sol['STATUS'] in STATUS_FINAIS:
        return _erro('Solicitação já finalizada.')
    dados = request.get_json(silent=True) or {}
    resultado = dados.get('resultado')
    justificativa = _limpo(dados.get('justificativa'), 8000)
    if resultado not in STATUS_CONCLUIDOS:
        return _erro('Escolha concluída e homologada ou concluída e não homologada.')
    if resultado == 'CONCLUIDA_NAO_HOMOLOGADA' and not justificativa:
        return _erro('Informe a justificativa da conclusão sem homologação.')
    try:
        db.session.execute(text("""
            UPDATE """ + TB_SOL + """ SET STATUS = :r, DT_CONCLUSAO = GETDATE(), ID_USUARIO_CONCLUSAO = :uid,
                JUSTIFICATIVA_CONCLUSAO = :j, DT_ENTREGA = ISNULL(DT_ENTREGA, GETDATE()),
                PC_CONCLUSAO = CASE WHEN :r = 'CONCLUIDA_HOMOLOGADA' THEN 100 ELSE PC_CONCLUSAO END,
                UPDATED_AT = GETDATE(), ID_USUARIO_ATUALIZACAO = :uid
            WHERE ID_SOLICITACAO = :id"""), {'r': resultado, 'uid': _uid(), 'j': justificativa, 'id': id_solicitacao})
        msg = 'Solicitação {}.'.format(STATUS[resultado]['rotulo'].lower())
        if justificativa:
            msg += ' ' + justificativa
        _interacao(id_solicitacao, 'STATUS', msg, st_ant=sol['STATUS'], st_novo=resultado)
        _historico(id_solicitacao, 'Status', STATUS[sol['STATUS']]['rotulo'], STATUS[resultado]['rotulo'])
        db.session.commit()
        registrar_log('editar', 'solicitacoes_area', id_solicitacao,
                      'Concluiu a solicitação {} como {}'.format(sol['NU_PROTOCOLO'], resultado))
        return _ok()
    except Exception as e:
        db.session.rollback()
        return _erro('Erro ao concluir: {}'.format(e), 500)


# =========================================================================
# ESCOPO (checklist)
# =========================================================================
@solicitacoes_bp.route('/<int:id_solicitacao>/itens', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def novo_item(id_solicitacao):
    sol, env, papel = _acesso(id_solicitacao)
    if papel != 'GESTOR':
        return _erro('Somente administradores e moderadores alteram o escopo.', 403)
    descricoes = [_limpo(l.lstrip('-•* \t'), 500) for l in ((request.get_json(silent=True) or {}).get('descricao') or '').splitlines()]
    descricoes = [d for d in descricoes if d]
    if not descricoes:
        return _erro('Descreva o item do escopo.')
    try:
        ordem = db.session.execute(text("SELECT ISNULL(MAX(NU_ORDEM), 0) FROM " + TB_ITEM +
                                        " WHERE ID_SOLICITACAO = :id"), {'id': id_solicitacao}).scalar() or 0
        for d in descricoes:
            ordem += 1
            db.session.execute(text("""
                INSERT INTO """ + TB_ITEM + """ (ID_SOLICITACAO, DESCRICAO, NU_ORDEM, ID_USUARIO_CRIACAO, CREATED_AT)
                VALUES (:id, :d, :o, :uid, GETDATE())"""), {'id': id_solicitacao, 'd': d, 'o': ordem, 'uid': _uid()})
            _historico(id_solicitacao, 'Escopo', None, 'Item incluído: ' + d)
        db.session.commit()
        return _ok()
    except Exception as e:
        db.session.rollback()
        return _erro('Erro ao incluir item: {}'.format(e), 500)


@solicitacoes_bp.route('/itens/<int:id_item>/alternar', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def alternar_item(id_item):
    item = _row("SELECT * FROM " + TB_ITEM + " WHERE ID_ITEM = :id AND DELETED_AT IS NULL", {'id': id_item})
    if not item:
        return _erro('Item não encontrado.', 404)
    sol, env, papel = _acesso(item['ID_SOLICITACAO'])
    if papel not in ('GESTOR', 'EQUIPE'):
        return _erro('Você não pode marcar itens do escopo.', 403)
    if sol['STATUS'] in STATUS_FINAIS:
        return _erro('Solicitação finalizada.')
    novo = 0 if item['FL_CONCLUIDO'] else 1
    try:
        db.session.execute(text("""
            UPDATE """ + TB_ITEM + """ SET FL_CONCLUIDO = :n,
                DT_CONCLUSAO = CASE WHEN :n = 1 THEN GETDATE() ELSE NULL END,
                ID_USUARIO_CONCLUSAO = CASE WHEN :n = 1 THEN :uid ELSE NULL END,
                NO_USUARIO_CONCLUSAO = CASE WHEN :n = 1 THEN :unome ELSE NULL END
            WHERE ID_ITEM = :id"""), {'n': novo, 'uid': _uid(), 'unome': _unome(), 'id': id_item})
        if novo:
            _interacao(item['ID_SOLICITACAO'], 'MARCO', 'Item do escopo concluído: {}'.format(item['DESCRICAO']))
        else:
            _historico(item['ID_SOLICITACAO'], 'Escopo', 'Concluído: ' + item['DESCRICAO'], 'Reaberto')
        db.session.commit()
        return _ok(concluido=bool(novo))
    except Exception as e:
        db.session.rollback()
        return _erro('Erro ao atualizar item: {}'.format(e), 500)


@solicitacoes_bp.route('/itens/<int:id_item>/excluir', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def excluir_item(id_item):
    item = _row("SELECT * FROM " + TB_ITEM + " WHERE ID_ITEM = :id AND DELETED_AT IS NULL", {'id': id_item})
    if not item:
        return _erro('Item não encontrado.', 404)
    sol, env, papel = _acesso(item['ID_SOLICITACAO'])
    if papel != 'GESTOR':
        return _erro('Somente administradores e moderadores alteram o escopo.', 403)
    try:
        db.session.execute(text("UPDATE " + TB_ITEM + " SET DELETED_AT = GETDATE() WHERE ID_ITEM = :id"), {'id': id_item})
        _historico(item['ID_SOLICITACAO'], 'Escopo', item['DESCRICAO'], 'Item excluído')
        db.session.commit()
        return _ok()
    except Exception as e:
        db.session.rollback()
        return _erro('Erro ao excluir item: {}'.format(e), 500)


# =========================================================================
# ARQUIVOS
# =========================================================================
@solicitacoes_bp.route('/<int:id_solicitacao>/arquivos', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def enviar_arquivo(id_solicitacao):
    sol, env, papel = _acesso(id_solicitacao)
    tipo = request.form.get('tipo') or 'ANEXO'
    if tipo not in ARQUIVOS_POR_PAPEL.get(papel, ()):
        return _erro('Você não pode enviar este tipo de arquivo.', 403)
    arquivos = [a for a in request.files.getlist('arquivos') if a and a.filename]
    if not arquivos:
        return _erro('Selecione ao menos um arquivo.')
    interno = request.form.get('interno') == '1' and _ve_interno(papel) and tipo != 'TERMO_ASSINADO'
    obs = request.form.get('observacao')
    try:
        lidos = [_ler_upload(a) for a in arquivos]
    except ValueError as e:
        return _erro(str(e))
    try:
        for nome, ct, conteudo in lidos:
            _gravar_arquivo(id_solicitacao, tipo, nome, ct, conteudo, interno=interno, observacao=obs)
        nomes = ', '.join(l[0] for l in lidos)
        _interacao(id_solicitacao, 'HOMOLOGACAO' if tipo == 'TERMO_ASSINADO' else 'SISTEMA',
                   '{} anexado(s): {}'.format(TIPOS_ARQUIVO[tipo]['rotulo'], nomes), interna=interno)
        db.session.commit()
        registrar_log('criar', 'solicitacoes_area', id_solicitacao,
                      'Anexou {} na solicitação {}: {}'.format(tipo, sol['NU_PROTOCOLO'], nomes))
        return _ok()
    except Exception as e:
        db.session.rollback()
        return _erro('Erro ao gravar o arquivo: {}'.format(e), 500)


@solicitacoes_bp.route('/arquivos/<int:id_arquivo>')
@login_required
@sistema_requerido(SISTEMA)
def baixar_arquivo(id_arquivo):
    arq = _row("SELECT * FROM " + TB_ARQ + " WHERE ID_ARQUIVO = :id AND DELETED_AT IS NULL", {'id': id_arquivo})
    if not arq:
        abort(404)
    sol, env, papel = _acesso(arq['ID_SOLICITACAO'])
    if arq['FL_INTERNO'] and not _ve_interno(papel):
        abort(403)
    return send_file(BytesIO(bytes(arq['CONTEUDO'])), as_attachment=True, download_name=arq['NO_ARQUIVO'],
                     mimetype=arq['DS_CONTENT_TYPE'] or 'application/octet-stream')


@solicitacoes_bp.route('/arquivos/<int:id_arquivo>/excluir', methods=['POST'])
@login_required
@sistema_requerido(SISTEMA)
def excluir_arquivo(id_arquivo):
    arq = _row("SELECT ID_SOLICITACAO, NO_ARQUIVO, TIPO FROM " + TB_ARQ +
               " WHERE ID_ARQUIVO = :id AND DELETED_AT IS NULL", {'id': id_arquivo})
    if not arq:
        return _erro('Arquivo não encontrado.', 404)
    sol, env, papel = _acesso(arq['ID_SOLICITACAO'])
    if papel != 'GESTOR':
        return _erro('Somente administradores e moderadores excluem arquivos.', 403)
    try:
        db.session.execute(text("UPDATE " + TB_ARQ + " SET DELETED_AT = GETDATE() WHERE ID_ARQUIVO = :id"),
                           {'id': id_arquivo})
        _historico(arq['ID_SOLICITACAO'], 'Arquivos', '{} ({})'.format(arq['NO_ARQUIVO'], arq['TIPO']), 'Excluído')
        db.session.commit()
        return _ok()
    except Exception as e:
        db.session.rollback()
        return _erro('Erro ao excluir: {}'.format(e), 500)


# =========================================================================
# TERMO DE ENTREGA E HOMOLOGAÇÃO
# =========================================================================
def _dados_termo(id_solicitacao):
    sol = _carregar(id_solicitacao)
    _enriquecer(sol)
    env = _envolvidos(id_solicitacao)
    itens = _rows("SELECT DESCRICAO, FL_CONCLUIDO FROM " + TB_ITEM +
                  " WHERE ID_SOLICITACAO = :id AND DELETED_AT IS NULL ORDER BY NU_ORDEM, ID_ITEM", {'id': id_solicitacao})
    linha_tempo = _rows("""
        SELECT TIPO, TEXTO, NO_USUARIO, CREATED_AT FROM """ + TB_INT + """
        WHERE ID_SOLICITACAO = :id AND DELETED_AT IS NULL AND FL_INTERNA = 0
          AND TIPO IN ('ATUALIZACAO','MARCO','STATUS','ENTREGA','HOMOLOGACAO','SISTEMA')
        ORDER BY CREATED_AT, ID_INTERACAO""", {'id': id_solicitacao})
    entrega = _row("""
        SELECT TOP 1 I.ID_USUARIO, I.NO_USUARIO FROM """ + TB_INT + """ I
        WHERE I.ID_SOLICITACAO = :id AND I.TIPO = 'ENTREGA' AND I.DELETED_AT IS NULL
        ORDER BY I.CREATED_AT DESC""", {'id': id_solicitacao})
    responsavel = _dados_usuario(entrega['ID_USUARIO']) if entrega else None

    dados = {
        'sol': sol,
        'rotulos': {'tipo': sol['tipo_rotulo'], 'categoria': sol['categoria_rotulo'],
                    'prioridade': sol['prioridade_rotulo'], 'canal': sol['canal_rotulo']},
        'orgao': 'EMGEA – Empresa Gestora de Ativos',
        'area_executora': AREA_EXECUTORA,
        'executores': [{'nome': e['NO_USUARIO'], 'funcao': e['DS_FUNCAO']} for e in env if e['PAPEL'] == 'EXECUTOR'],
        'outros_solicitantes': _cosolicitantes_com_area(env),
        'itens': [{'descricao': i['DESCRICAO'], 'concluido': i['FL_CONCLUIDO']} for i in itens],
        'linha_tempo': [{'data': r['CREATED_AT'],
                         'texto': '[{}] {}'.format(TIPOS_INTERACAO.get(r['TIPO'], {}).get('rotulo', r['TIPO']), r['TEXTO'] or ''),
                         'usuario': r['NO_USUARIO']} for r in linha_tempo],
        'indicadores': {'dias_ate_entrega': sol['dias_ate_entrega'], 'situacao_prazo': sol['situacao_prazo']},
        'responsavel_nome': responsavel['NOME'] if responsavel else None,
        'responsavel_cargo': responsavel['DS_CARGO'] if responsavel else None,
        'gerado_por': {'nome': _unome(), 'cargo': _cargo_usuario_logado()},
        'gerado_em': datetime.now(),
    }
    hash_hex, codigo = termo.codigo_verificacao(dados)
    dados['codigo'] = codigo
    dados['hash'] = hash_hex
    return dados


def _obter_termo_pdf(id_solicitacao):
    """
    Retorna (nome, bytes) do termo em PDF. Se o conteúdo não mudou desde a
    última via gerada, devolve a MESMA via guardada (mesmo código de
    verificação); se mudou, gera e guarda uma nova versão.
    """
    dados = _dados_termo(id_solicitacao)
    sol = dados['sol']
    ultima = _row("""
        SELECT TOP 1 ID_ARQUIVO, NO_ARQUIVO, CONTEUDO, HASH_SHA256, NU_VERSAO FROM """ + TB_ARQ + """
        WHERE ID_SOLICITACAO = :id AND TIPO = 'TERMO_GERADO' AND DELETED_AT IS NULL
        ORDER BY NU_VERSAO DESC, ID_ARQUIVO DESC""", {'id': id_solicitacao})
    if ultima and ultima['HASH_SHA256'] == dados['hash']:
        return ultima['NO_ARQUIVO'], bytes(ultima['CONTEUDO'])
    versao = (ultima['NU_VERSAO'] or 0) + 1 if ultima else 1
    blocos = termo.montar_blocos(dados)
    conteudo = termo.gerar_pdf(blocos, termo.texto_rodape(dados))
    nome = 'Termo_Homologacao_{}_v{}.pdf'.format(sol['NU_PROTOCOLO'], versao)
    _gravar_arquivo(id_solicitacao, 'TERMO_GERADO', nome, 'application/pdf', conteudo,
                    observacao='Código de verificação {}'.format(dados['codigo']), hash_hex=dados['hash'],
                    versao=versao)
    _interacao(id_solicitacao, 'SISTEMA', 'Termo de entrega e homologação gerado (versão {}, código {}).'.format(
        versao, dados['codigo']))
    return nome, conteudo


def _guardar_termo(id_solicitacao):
    """Gera/guarda a via oficial do termo. Se falhar, a operação principal já está gravada: só avisa."""
    try:
        _obter_termo_pdf(id_solicitacao)
        db.session.commit()
        return None
    except Exception as e:
        db.session.rollback()
        return 'Registro gravado, mas o termo não pôde ser gerado agora ({}). Use o botão "Baixar termo".'.format(e)


@solicitacoes_bp.route('/<int:id_solicitacao>/termo')
@login_required
@sistema_requerido(SISTEMA)
def baixar_termo(id_solicitacao):
    sol, env, papel = _acesso(id_solicitacao)
    if not sol['DT_ENTREGA'] and papel != 'GESTOR':
        abort(403)
    formato = request.args.get('formato', 'pdf')
    try:
        if formato == 'docx':
            dados = _dados_termo(id_solicitacao)
            conteudo = termo.gerar_docx(termo.montar_blocos(dados), termo.texto_rodape(dados))
            nome = 'Termo_Homologacao_{}.docx'.format(sol['NU_PROTOCOLO'])
            mimetype = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        else:
            nome, conteudo = _obter_termo_pdf(id_solicitacao)
            db.session.commit()
            mimetype = 'application/pdf'
        registrar_log('exportar', 'solicitacoes_area', id_solicitacao,
                      'Baixou o termo ({}) da solicitação {}'.format(formato, sol['NU_PROTOCOLO']))
        return send_file(BytesIO(conteudo), as_attachment=True, download_name=nome, mimetype=mimetype)
    except Exception as e:
        db.session.rollback()
        flash('Erro ao gerar o termo: {}'.format(e), 'danger')
        return redirect(url_for('solicitacoes.detalhe', id_solicitacao=id_solicitacao))


@solicitacoes_bp.route('/modelo-termo')
@login_required
@sistema_requerido(SISTEMA)
def modelo_termo():
    formato = request.args.get('formato', 'docx')
    dados = {'modelo': True, 'orgao': 'EMGEA – Empresa Gestora de Ativos', 'area_executora': AREA_EXECUTORA}
    blocos = termo.montar_blocos(dados)
    rodape = termo.texto_rodape(dados)
    if formato == 'pdf':
        return send_file(BytesIO(termo.gerar_pdf(blocos, rodape)), as_attachment=True,
                         download_name='Modelo_Termo_Entrega_Homologacao.pdf', mimetype='application/pdf')
    return send_file(BytesIO(termo.gerar_docx(blocos, rodape)), as_attachment=True,
                     download_name='Modelo_Termo_Entrega_Homologacao.docx',
                     mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document')


@solicitacoes_bp.route('/verificar', methods=['GET'])
@login_required
@sistema_requerido(SISTEMA)
def verificar_termo():
    """Confere um código de verificação impresso no termo."""
    codigo = (_limpo(request.args.get('codigo'), 30) or '').upper()
    resultado = None
    if codigo:
        resultado = _row("""
            SELECT TOP 1 A.ID_SOLICITACAO, A.NU_VERSAO, A.CREATED_AT, A.NO_USUARIO, S.NU_PROTOCOLO, S.TITULO,
                   S.STATUS
            FROM """ + TB_ARQ + """ A
            JOIN """ + TB_SOL + """ S ON S.ID_SOLICITACAO = A.ID_SOLICITACAO
            WHERE A.TIPO = 'TERMO_GERADO' AND A.DELETED_AT IS NULL AND S.DELETED_AT IS NULL
              AND A.DS_OBSERVACAO = :obs
            ORDER BY A.ID_ARQUIVO DESC""", {'obs': 'Código de verificação {}'.format(codigo)})
        if resultado:
            resultado['status_rotulo'] = STATUS.get(resultado['STATUS'], {}).get('rotulo')
            resultado['status_cor'] = STATUS.get(resultado['STATUS'], {}).get('cor')
            ultima = db.session.execute(text("SELECT MAX(NU_VERSAO) FROM " + TB_ARQ + " WHERE ID_SOLICITACAO = :id "
                                             "AND TIPO = 'TERMO_GERADO' AND DELETED_AT IS NULL"),
                                        {'id': resultado['ID_SOLICITACAO']}).scalar()
            resultado['vigente'] = ultima == resultado['NU_VERSAO']
    return jsonify({'sucesso': True, 'encontrado': bool(resultado), 'resultado': resultado and {
        'protocolo': resultado['NU_PROTOCOLO'], 'titulo': resultado['TITULO'], 'versao': resultado['NU_VERSAO'],
        'gerado_em': _fmt_data(resultado['CREATED_AT'], True), 'gerado_por': resultado['NO_USUARIO'],
        'status': resultado['status_rotulo'], 'status_cor': resultado['status_cor'],
        'vigente': resultado['vigente'],
        'url': url_for('solicitacoes.detalhe', id_solicitacao=resultado['ID_SOLICITACAO'])}})


# =========================================================================
# DASHBOARD (somente gestor)
# =========================================================================
def _dias_uteis(inicio, fim, uteis):
    if not inicio or not fim or uteis is None:
        return None
    if fim < inicio:
        return 0
    return sum(1 for d in uteis if inicio < d <= fim)


@solicitacoes_bp.route('/dashboard')
@login_required
@sistema_requerido(SISTEMA)
def dashboard():
    if not _gestor():
        flash('O dashboard é restrito a administradores e moderadores.', 'warning')
        return redirect(url_for('solicitacoes.index'))

    ano = _parse_int(request.args.get('ano')) or ''
    area = _limpo(request.args.get('area')) or ''
    tipo = _limpo(request.args.get('tipo')) or ''
    where = ['S.DELETED_AT IS NULL']
    params = {}
    if ano:
        where.append('YEAR(S.DT_SOLICITACAO) = :ano')
        params['ano'] = ano
    if area:
        where.append('S.SG_SUPERINTENDENCIA = :area')
        params['area'] = area
    if tipo in TIPOS_ENTREGA:
        where.append('S.TIPO_ENTREGA = :tipo')
        params['tipo'] = tipo

    try:
        sols = _rows("SELECT S.* FROM " + TB_SOL + " S WHERE " + ' AND '.join(where), params)
        anos = [r['ANO'] for r in _rows("SELECT DISTINCT YEAR(DT_SOLICITACAO) AS ANO FROM " + TB_SOL +
                                         " WHERE DELETED_AT IS NULL ORDER BY 1 DESC")]
        areas = [r['SG'] for r in _rows("SELECT DISTINCT SG_SUPERINTENDENCIA AS SG FROM " + TB_SOL +
                                         " WHERE DELETED_AT IS NULL AND SG_SUPERINTENDENCIA IS NOT NULL ORDER BY 1")]
    except Exception as e:
        db.session.rollback()
        flash('Não foi possível carregar o dashboard: {}'.format(e), 'danger')
        sols, anos, areas = [], [], []

    hoje = date.today()
    for s in sols:
        _enriquecer(s, hoje)
    por_id = {s['ID_SOLICITACAO']: s for s in sols}
    ids_csv = ','.join(str(int(i)) for i in por_id) or '0'

    envolvidos = _rows("SELECT ID_SOLICITACAO, ID_USUARIO, NO_USUARIO, PAPEL FROM " + TB_ENV +
                       " WHERE DELETED_AT IS NULL AND PAPEL = 'EXECUTOR' AND ID_SOLICITACAO IN (" + ids_csv + ")") if sols else []
    atividade = _rows("""
        SELECT ID_USUARIO, NO_USUARIO, ID_SOLICITACAO, COUNT(*) AS REGISTROS, SUM(ISNULL(QT_HORAS, 0)) AS HORAS,
               MAX(CREATED_AT) AS ULTIMO
        FROM """ + TB_INT + """
        WHERE DELETED_AT IS NULL AND TIPO IN ('ATUALIZACAO','MARCO','IMPEDIMENTO','ENTREGA','STATUS')
          AND ID_SOLICITACAO IN (""" + ids_csv + """)
        GROUP BY ID_USUARIO, NO_USUARIO, ID_SOLICITACAO""") if sols else []

    # Dias úteis (calendário oficial). Se a tabela não responder, o indicador fica de fora.
    uteis = None
    datas = [_como_data(s['DT_SOLICITACAO']) for s in sols if s['DT_SOLICITACAO']]
    if datas:
        try:
            uteis = [_como_data(r['DIA']) for r in _rows(
                "SELECT DIA FROM " + TB_CAL + " WHERE DIA_UTIL = 1 AND DIA BETWEEN :i AND :f",
                {'i': min(datas), 'f': hoje})]
        except Exception:
            db.session.rollback()
            uteis = None

    # ----- KPIs
    finalizadas = [s for s in sols if s['finalizada']]
    concluidas = [s for s in sols if s['STATUS'] in STATUS_CONCLUIDOS]
    homologadas = [s for s in sols if s['STATUS'] == 'CONCLUIDA_HOMOLOGADA']
    com_prazo_entregue = [s for s in sols if s['prazo_ref'] and (s['DT_ENTREGA'] or s['STATUS'] in STATUS_CONCLUIDOS)]
    no_prazo = [s for s in com_prazo_entregue if s['situacao_prazo'] == 'Entregue no prazo']
    ids_com_prazo = set(s['ID_SOLICITACAO'] for s in com_prazo_entregue)
    ids_no_prazo = set(s['ID_SOLICITACAO'] for s in no_prazo)
    lead = [((_como_data(s['DT_CONCLUSAO']) - _como_data(s['DT_SOLICITACAO'])).days) for s in concluidas
            if s['DT_CONCLUSAO'] and s['DT_SOLICITACAO']]
    lead_uteis = [_dias_uteis(_como_data(s['DT_SOLICITACAO']), _como_data(s['DT_CONCLUSAO']), uteis)
                  for s in concluidas if s['DT_CONCLUSAO']] if uteis is not None else []
    horas_total = sum(float(a['HORAS'] or 0) for a in atividade)
    kpis = {
        'total': len(sols),
        'abertas': len(sols) - len(finalizadas),
        'atrasadas': sum(1 for s in sols if s['atrasada']),
        'aguardando': sum(1 for s in sols if s['STATUS'] == 'AGUARDANDO_HOMOLOGACAO'),
        'homologadas': len(homologadas),
        'nao_homologadas': sum(1 for s in sols if s['STATUS'] == 'CONCLUIDA_NAO_HOMOLOGADA'),
        'canceladas': sum(1 for s in sols if s['STATUS'] == 'CANCELADA'),
        'taxa_homologacao': round(100.0 * len(homologadas) / len(concluidas), 1) if concluidas else None,
        'taxa_prazo': round(100.0 * len(no_prazo) / len(com_prazo_entregue), 1) if com_prazo_entregue else None,
        'lead_medio': round(sum(lead) / float(len(lead)), 1) if lead else None,
        'lead_uteis': round(sum(lead_uteis) / float(len(lead_uteis)), 1) if lead_uteis else None,
        'horas': round(horas_total, 1),
        'aplicativos': sum(1 for s in sols if s['TIPO_ENTREGA'] == 'APLICATIVO'),
        'paineis': sum(1 for s in sols if s['TIPO_ENTREGA'] == 'PAINEL'),
        'sem_ciencia': sum(1 for s in sols if not s['DT_CIENCIA_SOLICITANTE'] and not s['finalizada']),
    }

    # ----- Gráficos
    g_status = {'rotulos': [], 'valores': [], 'cores': []}
    for chave, st in STATUS.items():
        qt = sum(1 for s in sols if s['STATUS'] == chave)
        if qt:
            g_status['rotulos'].append(st['rotulo'])
            g_status['valores'].append(qt)
            g_status['cores'].append(st['cor'])

    g_tipo = {'rotulos': [v['rotulo'] for v in TIPOS_ENTREGA.values()],
              'valores': [sum(1 for s in sols if s['TIPO_ENTREGA'] == k) for k in TIPOS_ENTREGA],
              'cores': [v['cor'] for v in TIPOS_ENTREGA.values()]}

    cont_area = defaultdict(lambda: {'abertas': 0, 'finalizadas': 0})
    for s in sols:
        cont_area[s['area']]['finalizadas' if s['finalizada'] else 'abertas'] += 1
    areas_ord = sorted(cont_area.items(), key=lambda x: -(x[1]['abertas'] + x[1]['finalizadas']))
    g_area = {'rotulos': [a for a, _ in areas_ord], 'abertas': [v['abertas'] for _, v in areas_ord],
              'finalizadas': [v['finalizadas'] for _, v in areas_ord]}

    g_prioridade = {'rotulos': [v['rotulo'] for v in PRIORIDADES.values()],
                    'valores': [sum(1 for s in sols if s['PRIORIDADE'] == k and not s['finalizada']) for k in PRIORIDADES],
                    'cores': [v['cor'] for v in PRIORIDADES.values()]}

    g_categoria = {'rotulos': [], 'valores': []}
    for k, rot in CATEGORIAS.items():
        qt = sum(1 for s in sols if s['CATEGORIA'] == k)
        if qt:
            g_categoria['rotulos'].append(rot)
            g_categoria['valores'].append(qt)

    # Evolução mensal: ano filtrado (12 meses) ou últimos 12 meses
    if ano:
        meses = [(ano, m) for m in range(1, 13)]
    else:
        meses = []
        a, m = hoje.year, hoje.month
        for _ in range(12):
            meses.insert(0, (a, m))
            m -= 1
            if m == 0:
                a, m = a - 1, 12
    nomes_mes = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez']
    g_mensal = {'rotulos': ['{}/{}'.format(nomes_mes[m - 1], str(a)[2:]) for a, m in meses],
                'abertas': [], 'concluidas': [], 'homologadas': []}
    for a, m in meses:
        g_mensal['abertas'].append(sum(1 for s in sols if s['DT_SOLICITACAO'] and
                                       _como_data(s['DT_SOLICITACAO']).year == a and _como_data(s['DT_SOLICITACAO']).month == m))
        concl_mes = [s for s in concluidas if s['DT_CONCLUSAO'] and s['DT_CONCLUSAO'].year == a and s['DT_CONCLUSAO'].month == m]
        g_mensal['concluidas'].append(len(concl_mes))
        g_mensal['homologadas'].append(sum(1 for s in concl_mes if s['STATUS'] == 'CONCLUIDA_HOMOLOGADA'))

    # Envelhecimento das abertas
    faixas = [('Até 15 dias', 0, 15), ('16 a 30 dias', 16, 30), ('31 a 60 dias', 31, 60),
              ('61 a 90 dias', 61, 90), ('Mais de 90 dias', 91, 10 ** 6)]
    abertas = [s for s in sols if not s['finalizada']]
    g_aging = {'rotulos': [f[0] for f in faixas],
               'valores': [sum(1 for s in abertas if f[1] <= s['dias_em_aberto'] <= f[2]) for f in faixas]}

    # ----- Por pessoa (executores)
    pessoas = {}

    def _pessoa(uid, nome):
        if uid not in pessoas:
            pessoas[uid] = {'id': uid, 'nome': nome, 'iniciais': _iniciais(nome), 'total': 0, 'abertas': 0,
                            'homologadas': 0, 'nao_homologadas': 0, 'canceladas': 0, 'atrasadas': 0,
                            'aguardando': 0, 'aplicativos': 0, 'paineis': 0, 'horas': 0.0, 'registros': 0,
                            'ultimo': None, 'entregues_prazo': 0, 'entregues_com_prazo': 0, 'solicitacoes': []}
        return pessoas[uid]

    for e in envolvidos:
        s = por_id.get(e['ID_SOLICITACAO'])
        if not s:
            continue
        p = _pessoa(e['ID_USUARIO'], e['NO_USUARIO'])
        p['total'] += 1
        p['abertas'] += 0 if s['finalizada'] else 1
        p['homologadas'] += 1 if s['STATUS'] == 'CONCLUIDA_HOMOLOGADA' else 0
        p['nao_homologadas'] += 1 if s['STATUS'] == 'CONCLUIDA_NAO_HOMOLOGADA' else 0
        p['canceladas'] += 1 if s['STATUS'] == 'CANCELADA' else 0
        p['atrasadas'] += 1 if s['atrasada'] else 0
        p['aguardando'] += 1 if s['STATUS'] == 'AGUARDANDO_HOMOLOGACAO' else 0
        p['aplicativos'] += 1 if s['TIPO_ENTREGA'] == 'APLICATIVO' else 0
        p['paineis'] += 1 if s['TIPO_ENTREGA'] == 'PAINEL' else 0
        if s['ID_SOLICITACAO'] in ids_com_prazo:
            p['entregues_com_prazo'] += 1
            p['entregues_prazo'] += 1 if s['ID_SOLICITACAO'] in ids_no_prazo else 0
        p['solicitacoes'].append({'id': s['ID_SOLICITACAO'], 'protocolo': s['NU_PROTOCOLO'], 'titulo': s['TITULO'],
                                  'status': s['status_rotulo'], 'cor': s['status_cor'], 'tipo': s['tipo_rotulo'],
                                  'area': s['area'], 'pc': s['PC_CONCLUSAO'], 'atrasada': s['atrasada'],
                                  'url': url_for('solicitacoes.detalhe', id_solicitacao=s['ID_SOLICITACAO'])})
    for a in atividade:
        if a['ID_USUARIO'] is None:
            continue
        p = _pessoa(a['ID_USUARIO'], a['NO_USUARIO'])
        p['horas'] += float(a['HORAS'] or 0)
        p['registros'] += int(a['REGISTROS'] or 0)
        if a['ULTIMO'] and (p['ultimo'] is None or a['ULTIMO'] > p['ultimo']):
            p['ultimo'] = a['ULTIMO']
    lista_pessoas = sorted(pessoas.values(), key=lambda p: (-p['total'], -p['registros'], p['nome'] or ''))
    for p in lista_pessoas:
        concl = p['homologadas'] + p['nao_homologadas']
        p['taxa_homologacao'] = round(100.0 * p['homologadas'] / concl, 1) if concl else None
        p['taxa_prazo'] = round(100.0 * p['entregues_prazo'] / p['entregues_com_prazo'], 1) if p['entregues_com_prazo'] else None
        p['horas'] = round(p['horas'], 1)
        p['ultimo_fmt'] = _fmt_data(p['ultimo'], True)
        p['ultimo'] = None

    g_pessoas = {'rotulos': [p['nome'] for p in lista_pessoas],
                 'homologadas': [p['homologadas'] for p in lista_pessoas],
                 'nao_homologadas': [p['nao_homologadas'] for p in lista_pessoas],
                 'abertas': [p['abertas'] for p in lista_pessoas],
                 'horas': [p['horas'] for p in lista_pessoas]}

    # Quem mais pede
    solicitantes = defaultdict(lambda: {'nome': '', 'area': '', 'total': 0, 'abertas': 0, 'homologadas': 0})
    for s in sols:
        r = solicitantes[s['ID_USUARIO_SOLICITANTE']]
        r['nome'], r['area'] = s['NO_SOLICITANTE'], s['area']
        r['total'] += 1
        r['abertas'] += 0 if s['finalizada'] else 1
        r['homologadas'] += 1 if s['STATUS'] == 'CONCLUIDA_HOMOLOGADA' else 0
    top_solicitantes = sorted(solicitantes.values(), key=lambda r: -r['total'])[:10]

    # Atenção: atrasadas e aguardando homologação há mais tempo
    atencao = sorted([s for s in sols if s['atrasada'] or s['STATUS'] == 'AGUARDANDO_HOMOLOGACAO'],
                     key=lambda s: (0 if s['atrasada'] else 1, s['dias_restantes'] if s['dias_restantes'] is not None else 0))[:12]

    graficos = {'status': g_status, 'tipo': g_tipo, 'area': g_area, 'prioridade': g_prioridade,
                'categoria': g_categoria, 'mensal': g_mensal, 'aging': g_aging, 'pessoas': g_pessoas}
    pessoas_json = {str(p['id']): {'nome': p['nome'], 'solicitacoes': p['solicitacoes']} for p in lista_pessoas}

    return render_template('solicitacoes/dashboard.html', kpis=kpis, graficos=graficos, pessoas=lista_pessoas,
                           pessoas_json=pessoas_json, top_solicitantes=top_solicitantes, atencao=atencao,
                           anos=anos, areas=areas, filtros={'ano': ano, 'area': area, 'tipo': tipo},
                           tem_dias_uteis=uteis is not None, cat=_catalogos())


# =========================================================================
# TUTORIAIS (somente leitura — não alteram dados)
# =========================================================================
ETAPAS_TUTORIAL = {
    'admin': [
        {'numero': 1, 'titulo_curto': 'Registrar o pedido'},
        {'numero': 2, 'titulo_curto': 'Lista e quadro'},
        {'numero': 3, 'titulo_curto': 'Atualizar o andamento'},
        {'numero': 4, 'titulo_curto': 'Registrar a entrega'},
        {'numero': 5, 'titulo_curto': 'Homologação e termo'},
        {'numero': 6, 'titulo_curto': 'Concluir'},
        {'numero': 7, 'titulo_curto': 'Dashboard'},
    ],
    'usuario': [
        {'numero': 1, 'titulo_curto': 'Encontrar seus pedidos'},
        {'numero': 2, 'titulo_curto': 'Confirmar o pedido'},
        {'numero': 3, 'titulo_curto': 'Acompanhar'},
        {'numero': 4, 'titulo_curto': 'Comentar'},
        {'numero': 5, 'titulo_curto': 'Homologar'},
        {'numero': 6, 'titulo_curto': 'Assinar e anexar'},
    ],
}


@solicitacoes_bp.route('/tutorial')
@login_required
@sistema_requerido(SISTEMA)
def tutorial():
    """Tutorial do aplicativo: versão de administradores/moderadores e versão de usuários."""
    gestor = _gestor()
    registra = _pode_criar()
    perfil = request.args.get('perfil')
    if perfil not in ('admin', 'usuario') or (perfil == 'admin' and not registra):
        perfil = 'admin' if registra else 'usuario'
    modelo = 'solicitacoes/tutorial_admin.html' if perfil == 'admin' else 'solicitacoes/tutorial_usuario.html'
    return render_template(modelo, perfil=perfil, gestor=gestor, registra=registra,
                           etapas=ETAPAS_TUTORIAL[perfil], cat=_catalogos())
