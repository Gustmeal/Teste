import re
from decimal import Decimal, InvalidOperation

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required
from sqlalchemy import text

from app import db
from app.utils.audit import registrar_log

composicao_fundos_bp = Blueprint(
    'composicao_fundos', __name__, url_prefix='/composicao-fundos'
)

_TB = '[BDG].[FIN_TB032_COMPOSICAO_FUNDOS_INVESTIMENTOS]'


def _parse_decimal(s):
    """Aceita valor com sinal, formato BR ou simples. None se vazio/inválido."""
    s = (s or '').strip()
    if s == '':
        return None
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def _norm_anomes(s):
    """'2026-05' ou '202605' -> '202605'. '' se inválido."""
    d = re.sub(r'\D', '', str(s or ''))
    return d[:6] if len(d) >= 6 else ''


def _fmt_anomes(s):
    s = str(s or '')
    return f"{s[4:6]}/{s[0:4]}" if len(s) >= 6 else s


def _fmt_vr(v):
    if v is None:
        return ''
    d = Decimal(str(v))
    inteiro, _, dec = f"{abs(d):.2f}".partition('.')
    inteiro = re.sub(r'(?<=\d)(?=(?:\d{3})+$)', '.', inteiro)
    return ('-' if d < 0 else '') + f"{inteiro},{dec}"


def _carregar_param(tabela, id_col, dsc_col):
    sql = text(f"SELECT {id_col}, {dsc_col} FROM [BDG].[{tabela}] ORDER BY {dsc_col}")
    return [{'id': r[0], 'dsc': (r[1] or '').strip()}
            for r in db.session.execute(sql).fetchall()]


@composicao_fundos_bp.route('/')
@login_required
def index():
    """Entrada/edição da composição dos fundos (FIN_TB032)."""
    fundos = _carregar_param('PAR_TB028_FUNDOS_INVESTIMENTOS', 'ID_FUNDO', 'DSC_FUNDO')
    ativos = _carregar_param('PAR_TB029_ATIVOS_FI', 'ID_ATIVO', 'DSC_ATIVO')
    itens = _carregar_param('PAR_TB030_ITENS_ATIVOS_FI', 'ID_ITEM', 'DSC_ITEM')

    f_anomes = _norm_anomes(request.args.get('anomes'))
    f_fundo = (request.args.get('fundo') or '').strip()

    cond, params = [], {}
    if f_anomes:
        cond.append("c.ANO_MES = :p_am"); params['p_am'] = f_anomes
    if f_fundo:
        cond.append("c.ID_FUNDO = :p_f"); params['p_f'] = f_fundo
    where = ("WHERE " + " AND ".join(cond)) if cond else ""

    rows = db.session.execute(text(f"""
        SELECT c.ANO_MES, c.ID_FUNDO, f.DSC_FUNDO, c.ID_ATIVO, a.DSC_ATIVO,
               c.ID_ITEM, i.DSC_ITEM, c.VR_ATUAL
        FROM {_TB} c
        LEFT JOIN [BDG].[PAR_TB028_FUNDOS_INVESTIMENTOS] f ON f.ID_FUNDO = c.ID_FUNDO
        LEFT JOIN [BDG].[PAR_TB029_ATIVOS_FI] a ON a.ID_ATIVO = c.ID_ATIVO
        LEFT JOIN [BDG].[PAR_TB030_ITENS_ATIVOS_FI] i ON i.ID_ITEM = c.ID_ITEM
        {where}
        ORDER BY c.ANO_MES DESC, f.DSC_FUNDO, a.DSC_ATIVO, i.DSC_ITEM
    """), params).fetchall()

    lista = []
    for r in rows:
        lista.append({
            'anomes': r[0], 'anomes_fmt': _fmt_anomes(r[0]),
            'id_fundo': r[1], 'dsc_fundo': (r[2] or ''),
            'id_ativo': r[3], 'dsc_ativo': (r[4] or ''),
            'id_item': r[5], 'dsc_item': (r[6] or ''),
            'vr': r[7], 'vr_str': ('' if r[7] is None else str(r[7])), 'vr_fmt': _fmt_vr(r[7]),
        })

    return render_template(
        'composicao_fundos/index.html',
        fundos=fundos, ativos=ativos, itens=itens, lista=lista,
        filtros={'anomes': f_anomes, 'fundo': f_fundo},
    )


@composicao_fundos_bp.route('/incluir', methods=['POST'])
@login_required
def incluir():
    anomes = _norm_anomes(request.form.get('ANO_MES'))
    id_fundo = (request.form.get('ID_FUNDO') or '').strip()
    id_ativo = (request.form.get('ID_ATIVO') or '').strip()
    id_item = (request.form.get('ID_ITEM') or '').strip()
    vr = _parse_decimal(request.form.get('VR_ATUAL'))

    if not anomes:
        return jsonify({'success': False, 'message': 'Informe a competência (Ano/Mês).'}), 400
    if not (id_fundo and id_ativo and id_item):
        return jsonify({'success': False, 'message': 'Selecione Fundo, Ativo e Item.'}), 400
    if vr is None:
        return jsonify({'success': False, 'message': 'Informe o VR_ATUAL (com o sinal, se negativo).'}), 400

    try:
        existe = db.session.execute(text(f"""
            SELECT 1 FROM {_TB}
            WHERE ANO_MES = :am AND ID_FUNDO = :fu AND ID_ATIVO = :at AND ID_ITEM = :it
        """), {'am': anomes, 'fu': id_fundo, 'at': id_ativo, 'it': id_item}).first()
        if existe:
            return jsonify({'success': False,
                            'message': 'Já existe um lançamento para essa combinação — use Editar.'}), 400

        db.session.execute(text(f"""
            INSERT INTO {_TB} (ANO_MES, ID_FUNDO, ID_ATIVO, ID_ITEM, VR_ATUAL)
            VALUES (:am, :fu, :at, :it, :vr)
        """), {'am': anomes, 'fu': id_fundo, 'at': id_ativo, 'it': id_item, 'vr': vr})
        db.session.commit()

        registrar_log(
            acao='inclusao', entidade='composicao_fundos', entidade_id=None,
            descricao=f'Composição {_fmt_anomes(anomes)} — fundo {id_fundo}/ativo {id_ativo}/item {id_item}',
            dados_novos={'ANO_MES': anomes, 'ID_FUNDO': id_fundo, 'ID_ATIVO': id_ativo,
                         'ID_ITEM': id_item, 'VR_ATUAL': str(vr)},
        )
        return jsonify({'success': True, 'message': 'Lançamento incluído.'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': f'Erro ao incluir: {str(e)}'}), 500


@composicao_fundos_bp.route('/editar', methods=['POST'])
@login_required
def editar():
    # novos valores
    anomes = _norm_anomes(request.form.get('ANO_MES'))
    id_fundo = (request.form.get('ID_FUNDO') or '').strip()
    id_ativo = (request.form.get('ID_ATIVO') or '').strip()
    id_item = (request.form.get('ID_ITEM') or '').strip()
    vr = _parse_decimal(request.form.get('VR_ATUAL'))

    # chave original (identifica a linha)
    o = {
        'am': _norm_anomes(request.form.get('o_ANO_MES')),
        'fu': (request.form.get('o_ID_FUNDO') or '').strip(),
        'at': (request.form.get('o_ID_ATIVO') or '').strip(),
        'it': (request.form.get('o_ID_ITEM') or '').strip(),
    }
    if not (anomes and id_fundo and id_ativo and id_item) or vr is None:
        return jsonify({'success': False, 'message': 'Preencha todos os campos (com o sinal do VR).'}), 400

    try:
        result = db.session.execute(text(f"""
            UPDATE {_TB}
            SET ANO_MES = :am, ID_FUNDO = :fu, ID_ATIVO = :at, ID_ITEM = :it, VR_ATUAL = :vr
            WHERE ANO_MES = :o_am AND ID_FUNDO = :o_fu
              AND ID_ATIVO = :o_at AND ID_ITEM = :o_it
        """), {'am': anomes, 'fu': id_fundo, 'at': id_ativo, 'it': id_item, 'vr': vr,
               'o_am': o['am'], 'o_fu': o['fu'], 'o_at': o['at'], 'o_it': o['it']})
        db.session.commit()
        if result.rowcount == 0:
            return jsonify({'success': False, 'message': 'Linha não encontrada (pode ter sido alterada).'}), 404

        registrar_log(
            acao='atualizacao', entidade='composicao_fundos', entidade_id=None,
            descricao=f'Edição composição {_fmt_anomes(anomes)}',
            dados_novos={'ANO_MES': anomes, 'ID_FUNDO': id_fundo, 'ID_ATIVO': id_ativo,
                         'ID_ITEM': id_item, 'VR_ATUAL': str(vr)},
        )
        return jsonify({'success': True, 'message': 'Lançamento atualizado.'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': f'Erro ao editar: {str(e)}'}), 500


@composicao_fundos_bp.route('/novo-item', methods=['POST'])
@login_required
def novo_item():
    """Cria um novo item na PAR_TB030 (ID_ITEM é IDENTITY: gerado pelo banco)."""
    dsc = (request.form.get('DSC_ITEM') or '').strip()
    if not dsc:
        return jsonify({'success': False, 'message': 'Informe a descrição do item.'}), 400
    try:
        # Insere só a descrição e recupera o ID gerado pelo IDENTITY.
        novo_id = db.session.execute(text("""
            INSERT INTO [BDG].[PAR_TB030_ITENS_ATIVOS_FI] (DSC_ITEM)
            OUTPUT INSERTED.ID_ITEM
            VALUES (:dsc)
        """), {'dsc': dsc}).scalar()
        db.session.commit()

        registrar_log(
            acao='inclusao', entidade='par_itens_ativos_fi', entidade_id=novo_id,
            descricao=f'Novo item de ativo FI: {dsc}',
            dados_novos={'ID_ITEM': novo_id, 'DSC_ITEM': dsc},
        )
        return jsonify({'success': True, 'message': f'Item "{dsc}" criado (ID {novo_id}).'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': f'Erro ao criar item: {str(e)}'}), 500