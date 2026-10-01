# -*- coding: utf-8 -*-
"""
Controle de presença dos usuários no Portal GEINC.

Cada aba aberta do portal envia um "sinal" periódico. Aqui ficam:
- o registro do sinal (MERGE de uma linha por aba);
- o registro de saída (aba fechada);
- a montagem dos dados do painel (status, recomendação de atualização).

Compatível com Python 3.9 e 3.12.
"""

import re
import traceback

from flask import current_app
from sqlalchemy import text

from app import db


# =====================================================================
# CONFIGURAÇÕES (enviadas ao front pelo backend)
# =====================================================================

TB_PRESENCA = 'BDG.APK_TB012_PRESENCA_USUARIO'

INTERVALO_SINAL_SEG = 60        # De quanto em quanto tempo cada aba manda sinal
INTERVALO_PAINEL_SEG = 20       # De quanto em quanto tempo o painel se atualiza
LIMITE_OCIOSO_SEG = 5 * 60      # Sem interação por mais que isso = ocioso
LIMITE_OFFLINE_SEG = 150        # Sem sinal por mais que isso = saiu / sem sinal
JANELA_CONSULTA_MIN = 30        # Painel mostra quem deu sinal nos últimos X minutos
LIMPEZA_HORAS = 24              # Linhas mais antigas que isso são apagadas

PADRAO_ID_ABA = re.compile(r'^[A-Za-z0-9\-]{8,40}$')

STATUS = {
    'ATIVO': {
        'rotulo': 'Ativo agora',
        'descricao': 'Mexeu no portal nos últimos {} minutos'.format(LIMITE_OCIOSO_SEG // 60),
        'cor': 'bg-success',
        'icone': 'fa-circle',
        'ordem': 1,
    },
    'OCIOSO': {
        'rotulo': 'Ocioso',
        'descricao': 'Portal aberto na tela, mas sem interação há mais de {} minutos'.format(LIMITE_OCIOSO_SEG // 60),
        'cor': 'bg-warning text-dark',
        'icone': 'fa-moon',
        'ordem': 2,
    },
    'SEGUNDO_PLANO': {
        'rotulo': 'Portal em segundo plano',
        'descricao': 'Portal aberto, mas o usuário está em outra aba ou programa',
        'cor': 'bg-info text-dark',
        'icone': 'fa-window-minimize',
        'ordem': 3,
    },
    'OFFLINE': {
        'rotulo': 'Saiu / sem sinal',
        'descricao': 'Fechou o portal ou parou de enviar sinal',
        'cor': 'bg-secondary',
        'icone': 'fa-power-off',
        'ordem': 4,
    },
}

# Blueprints que não estão no catálogo de sistemas
NOMES_EXTRAS = {
    'main': 'Página inicial do GEINC',
    'auth': 'Usuários e Acesso',
    'presenca': 'Usuários Online',
    'audit': 'Auditoria',
}


# =====================================================================
# AUXILIARES
# =====================================================================

def resolver_modulo(rota, script_root=''):
    """Descobre o nome do módulo a partir da rota, usando o url_map do Flask."""
    if not rota:
        return 'Não identificado'

    caminho = rota.split('?', 1)[0]
    if script_root and caminho.startswith(script_root):
        caminho = caminho[len(script_root):] or '/'

    try:
        adaptador = current_app.url_map.bind('localhost')
        endpoint, _argumentos = adaptador.match(caminho, method='GET')
    except Exception:
        return 'Não identificado'

    blueprint = endpoint.split('.', 1)[0] if '.' in endpoint else endpoint

    if blueprint in NOMES_EXTRAS:
        return NOMES_EXTRAS[blueprint]

    try:
        from app.models.permissao_sistema import PermissaoSistema
        info = PermissaoSistema.SISTEMAS_DISPONIVEIS.get(blueprint)
        if info and info.get('nome'):
            return info['nome']
    except Exception:
        pass

    return blueprint.replace('_', ' ').title()


def _formatar_duracao(segundos):
    if segundos is None:
        return '-'
    segundos = max(0, int(segundos))
    if segundos < 60:
        return 'menos de 1 min'
    minutos = segundos // 60
    if minutos < 60:
        return '{} min'.format(minutos)
    return '{}h{:02d}min'.format(minutos // 60, minutos % 60)


def _formatar_data(valor):
    return valor.strftime('%d/%m/%Y %H:%M:%S') if valor else '-'


def _status_aba(linha):
    seg_sem_sinal = linha.SEG_SEM_SINAL if linha.SEG_SEM_SINAL is not None else 999999
    if linha.FECHADA or seg_sem_sinal > LIMITE_OFFLINE_SEG:
        return 'OFFLINE'
    if not linha.ABA_VISIVEL:
        return 'SEGUNDO_PLANO'
    if (linha.SEG_INATIVO or 0) >= LIMITE_OCIOSO_SEG:
        return 'OCIOSO'
    return 'ATIVO'


def _buscar_areas(ids_usuarios):
    """Retorna {USUARIO_ID: superintendência} usando o vínculo com o empregado."""
    resultado = {}
    if not ids_usuarios:
        return resultado
    try:
        from app.models.usuario import Usuario
        for usuario in Usuario.query.filter(Usuario.ID.in_(list(ids_usuarios))).all():
            area = None
            empregado = getattr(usuario, 'empregado', None)
            if empregado:
                area = getattr(empregado, 'sgSuperintendencia', None)
            resultado[usuario.ID] = area or '-'
    except Exception:
        traceback.print_exc()
    return resultado


def _limpar_registros_antigos():
    try:
        db.session.execute(
            text("DELETE FROM " + TB_PRESENCA + " WHERE DT_ULTIMO_SINAL < DATEADD(HOUR, :horas, GETDATE())"),
            {'horas': -LIMPEZA_HORAS}
        )
        db.session.commit()
    except Exception:
        db.session.rollback()
        traceback.print_exc()


# =====================================================================
# REGISTRO DE SINAL / SAÍDA
# =====================================================================

def registrar_sinal(usuario_id, usuario_nome, dados, ip=None, script_root=''):
    """Grava/atualiza a linha da aba (uma linha por aba, via MERGE)."""
    id_aba = str(dados.get('id_aba') or '').strip()
    if not PADRAO_ID_ABA.match(id_aba):
        return False

    rota = str(dados.get('rota') or '')[:300]
    titulo = str(dados.get('titulo') or '').strip()[:200]
    visivel = 1 if dados.get('visivel') in (True, 1, '1', 'true') else 0

    try:
        seg_sem_interacao = int(dados.get('seg_sem_interacao') or 0)
    except (TypeError, ValueError):
        seg_sem_interacao = 0
    seg_sem_interacao = max(0, min(seg_sem_interacao, 86400))

    modulo = resolver_modulo(rota, script_root)[:100]

    sql = text("""
        MERGE """ + TB_PRESENCA + """ WITH (HOLDLOCK) AS destino
        USING (SELECT :id_aba AS ID_ABA) AS origem
           ON destino.ID_ABA = origem.ID_ABA
        WHEN MATCHED THEN
            UPDATE SET
                USUARIO_ID          = :uid,
                USUARIO_NOME        = :unome,
                MODULO              = :modulo,
                PAGINA_TITULO       = :titulo,
                ROTA                = :rota,
                ABA_VISIVEL         = :visivel,
                FECHADA             = 0,
                IP                  = :ip,
                DT_ULTIMO_SINAL     = GETDATE(),
                DT_ULTIMA_INTERACAO = DATEADD(SECOND, :seg_neg, GETDATE())
        WHEN NOT MATCHED THEN
            INSERT (ID_ABA, USUARIO_ID, USUARIO_NOME, MODULO, PAGINA_TITULO, ROTA,
                    ABA_VISIVEL, FECHADA, IP, DT_ENTRADA, DT_ULTIMO_SINAL, DT_ULTIMA_INTERACAO)
            VALUES (:id_aba, :uid, :unome, :modulo, :titulo, :rota,
                    :visivel, 0, :ip, GETDATE(), GETDATE(), DATEADD(SECOND, :seg_neg, GETDATE()));
    """)

    try:
        db.session.execute(sql, {
            'id_aba': id_aba,
            'uid': usuario_id,
            'unome': (usuario_nome or '')[:100],
            'modulo': modulo,
            'titulo': titulo,
            'rota': rota,
            'visivel': visivel,
            'ip': (ip or '')[:50],
            'seg_neg': -seg_sem_interacao,
        })
        db.session.commit()
        return True
    except Exception:
        db.session.rollback()
        traceback.print_exc()
        return False


def registrar_saida(usuario_id, dados):
    """Marca a aba como fechada (chamado pelo navigator.sendBeacon ao sair da página)."""
    id_aba = str(dados.get('id_aba') or '').strip()
    if not PADRAO_ID_ABA.match(id_aba):
        return False
    try:
        db.session.execute(text("""
            UPDATE """ + TB_PRESENCA + """
               SET FECHADA = 1,
                   DT_ULTIMO_SINAL = GETDATE()
             WHERE ID_ABA = :id_aba
               AND USUARIO_ID = :uid
        """), {'id_aba': id_aba, 'uid': usuario_id})
        db.session.commit()
        return True
    except Exception:
        db.session.rollback()
        traceback.print_exc()
        return False


# =====================================================================
# DADOS DO PAINEL
# =====================================================================

def consultar_painel(usuario_atual_id):
    _limpar_registros_antigos()

    agora = db.session.execute(text("SELECT GETDATE()")).scalar()

    linhas = db.session.execute(text("""
        SELECT ID_ABA, USUARIO_ID, USUARIO_NOME, MODULO, PAGINA_TITULO, ROTA,
               ABA_VISIVEL, FECHADA, DT_ENTRADA, DT_ULTIMO_SINAL, DT_ULTIMA_INTERACAO,
               DATEDIFF(SECOND, DT_ULTIMO_SINAL, GETDATE())     AS SEG_SEM_SINAL,
               DATEDIFF(SECOND, DT_ULTIMA_INTERACAO, GETDATE()) AS SEG_INATIVO
          FROM """ + TB_PRESENCA + """
         WHERE DT_ULTIMO_SINAL >= DATEADD(MINUTE, :janela_neg, GETDATE())
    """), {'janela_neg': -JANELA_CONSULTA_MIN}).fetchall()

    # Agrupa as abas por usuário
    agrupado = {}
    for linha in linhas:
        status = _status_aba(linha)
        aba = {
            'status': status,
            'ordem': STATUS[status]['ordem'],
            'modulo': linha.MODULO or 'Não identificado',
            'pagina': linha.PAGINA_TITULO or '',
            'seg_inativo': max(0, linha.SEG_INATIVO or 0),
            'seg_sem_sinal': max(0, linha.SEG_SEM_SINAL or 0),
            'dt_entrada': linha.DT_ENTRADA,
        }
        usuario = agrupado.setdefault(linha.USUARIO_ID, {
            'usuario_id': linha.USUARIO_ID,
            'nome': linha.USUARIO_NOME,
            'abas': [],
        })
        usuario['abas'].append(aba)

    areas = _buscar_areas(agrupado.keys())

    usuarios = []
    por_status = dict((chave, 0) for chave in STATUS)
    nomes_ativos = []

    for usuario_id, usuario in agrupado.items():
        abas = sorted(usuario['abas'], key=lambda a: (a['ordem'], a['seg_inativo']))
        abas_abertas = [a for a in abas if a['status'] != 'OFFLINE']
        principal = abas[0]
        status = principal['status']

        if status == 'OFFLINE':
            tempo_rotulo = 'Sem sinal há'
            tempo = _formatar_duracao(min(a['seg_sem_sinal'] for a in abas))
        else:
            tempo_rotulo = 'Inativo há'
            tempo = _formatar_duracao(min(a['seg_inativo'] for a in abas_abertas))

        outras_abas = []
        for aba in abas_abertas[1:]:
            outras_abas.append({
                'status': aba['status'],
                'modulo': aba['modulo'],
                'pagina': aba['pagina'],
                'inativo_ha': _formatar_duracao(aba['seg_inativo']),
            })

        eh_voce = (usuario_id == usuario_atual_id)
        if not eh_voce:
            por_status[status] += 1
            if status == 'ATIVO':
                nomes_ativos.append(usuario['nome'])

        usuarios.append({
            'usuario_id': usuario_id,
            'nome': usuario['nome'],
            'area': areas.get(usuario_id, '-'),
            'eh_voce': eh_voce,
            'status': status,
            'ordem': STATUS[status]['ordem'],
            'modulo_atual': principal['modulo'],
            'pagina_atual': principal['pagina'],
            'tempo_rotulo': tempo_rotulo,
            'tempo': tempo,
            'entrou_em': _formatar_data(min(a['dt_entrada'] for a in abas if a['dt_entrada'])
                                        if any(a['dt_entrada'] for a in abas) else None),
            'qtd_abas_abertas': len(abas_abertas),
            'outras_abas': outras_abas,
        })

    usuarios.sort(key=lambda u: (u['ordem'], (u['nome'] or '').lower()))

    # Recomendação de atualização (você mesmo não entra na conta)
    qtd_ativos = por_status['ATIVO']
    qtd_abertos_parados = por_status['OCIOSO'] + por_status['SEGUNDO_PLANO']

    if qtd_ativos:
        exibidos = ', '.join(nomes_ativos[:5])
        if len(nomes_ativos) > 5:
            exibidos += ' e mais {}'.format(len(nomes_ativos) - 5)
        recomendacao = {
            'nivel': 'danger',
            'icone': 'fa-hand-paper',
            'titulo': 'Aguarde para atualizar',
            'mensagem': '{} usuário(s) trabalhando agora no portal: {}.'.format(qtd_ativos, exibidos),
        }
    elif qtd_abertos_parados:
        recomendacao = {
            'nivel': 'warning',
            'icone': 'fa-exclamation-triangle',
            'titulo': 'Atualização com cautela',
            'mensagem': ('Ninguém está interagindo agora, mas {} usuário(s) estão com o portal aberto. '
                         'Formulários não salvos podem ser perdidos.').format(qtd_abertos_parados),
        }
    else:
        recomendacao = {
            'nivel': 'success',
            'icone': 'fa-check-circle',
            'titulo': 'Pode atualizar',
            'mensagem': 'Nenhum outro usuário está com o portal aberto neste momento.',
        }

    return {
        'success': True,
        'atualizado_em': _formatar_data(agora),
        'status': STATUS,
        'resumo': {
            'por_status': por_status,
            'total_abertos': qtd_ativos + qtd_abertos_parados,
        },
        'recomendacao': recomendacao,
        'usuarios': usuarios,
        'config': {
            'intervalo_painel_seg': INTERVALO_PAINEL_SEG,
            'janela_min': JANELA_CONSULTA_MIN,
        },
    }