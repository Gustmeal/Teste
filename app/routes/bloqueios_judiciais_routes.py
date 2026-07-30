from datetime import datetime
from decimal import Decimal, InvalidOperation

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required
from sqlalchemy import text

from app import db
from app.utils.audit import registrar_log

bloqueios_judiciais_bp = Blueprint(
    'bloqueios_judiciais', __name__, url_prefix='/bloqueios-judiciais'
)

_TB = '[BDG].[FIN_TB022_BLOQUEIOS_JUDICIAIS]'


def _parse_data(s):
    s = (s or '').strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, '%Y-%m-%d').date()
    except ValueError:
        return None


def _parse_decimal(s):
    s = (s or '').strip()
    if not s:
        return None
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def _fmt_vr(v):
    if v is None:
        return ''
    inteiro, _, dec = f"{abs(Decimal(str(v))):.2f}".partition('.')
    import re
    inteiro = re.sub(r'(?<=\d)(?=(?:\d{3})+$)', '.', inteiro)
    sinal = '-' if Decimal(str(v)) < 0 else ''
    return f"{sinal}R$ {inteiro},{dec}"


def _carregar_contas():
    sql = text("""
        SELECT ID_CONTA, DSC_CONTA
        FROM [BDG].[PAR_TB027_CONTAS_EMGEA]
        ORDER BY DSC_CONTA
    """)
    return [{'id': r[0], 'dsc': (r[1] or '').strip()}
            for r in db.session.execute(sql).fetchall()]


@bloqueios_judiciais_bp.route('/')
@login_required
def index():
    """Entrada e edição dos Bloqueios Judiciais (FIN_TB022)."""
    f_processo = (request.args.get('processo') or '').strip()
    f_autor = (request.args.get('autor') or '').strip()
    f_conta = (request.args.get('conta') or '').strip()
    f_situacao = (request.args.get('situacao') or 'todos').strip()

    condicoes, params = [], {}
    if f_processo:
        condicoes.append("PROCESSO LIKE :p_proc"); params['p_proc'] = f'%{f_processo}%'
    if f_autor:
        condicoes.append("AUTOR LIKE :p_autor"); params['p_autor'] = f'%{f_autor}%'
    if f_conta:
        condicoes.append("CONTA = :p_conta"); params['p_conta'] = f_conta
    if f_situacao == 'bloqueado':
        condicoes.append("DT_DESBLOQUEIO IS NULL")
    elif f_situacao == 'desbloqueado':
        condicoes.append("DT_DESBLOQUEIO IS NOT NULL")
    where = ("WHERE " + " AND ".join(condicoes)) if condicoes else ""

    sql = text(f"""
        SELECT DT_DEPOSITO, VR_BLOQUEADO, CONTA, PROCESSO, VARA, AUTOR, DT_DESBLOQUEIO
        FROM {_TB} {where}
        ORDER BY DT_DEPOSITO DESC, PROCESSO
    """)
    rows = db.session.execute(sql, params).fetchall()

    lista = []
    for r in rows:
        lista.append({
            'dt_deposito': r[0], 'dt_deposito_iso': r[0].strftime('%Y-%m-%d') if r[0] else '',
            'vr': r[1], 'vr_str': ('' if r[1] is None else str(r[1])), 'vr_fmt': _fmt_vr(r[1]),
            'conta': (r[2] or ''), 'processo': (r[3] or ''),
            'vara': (r[4] or ''), 'autor': (r[5] or ''),
            'dt_desbloqueio': r[6], 'dt_desbloqueio_iso': r[6].strftime('%Y-%m-%d') if r[6] else '',
        })

    return render_template(
        'bloqueios_judiciais/index.html',
        contas=_carregar_contas(), lista=lista,
        filtros={'processo': f_processo, 'autor': f_autor,
                 'conta': f_conta, 'situacao': f_situacao},
    )


@bloqueios_judiciais_bp.route('/incluir', methods=['POST'])
@login_required
def incluir():
    dt_dep = _parse_data(request.form.get('DT_DEPOSITO'))
    vr = _parse_decimal(request.form.get('VR_BLOQUEADO'))
    conta = (request.form.get('CONTA') or '').strip()
    processo = (request.form.get('PROCESSO') or '').strip()
    vara = (request.form.get('VARA') or '').strip()
    autor = (request.form.get('AUTOR') or '').strip()

    if not dt_dep:
        return jsonify({'success': False, 'message': 'Informe a data do depósito.'}), 400
    if vr is None:
        return jsonify({'success': False, 'message': 'Informe o valor bloqueado.'}), 400
    if not conta:
        return jsonify({'success': False, 'message': 'Selecione a conta.'}), 400
    if not processo:
        return jsonify({'success': False, 'message': 'Informe o processo.'}), 400

    try:
        db.session.execute(text(f"""
            INSERT INTO {_TB}
                (DT_DEPOSITO, VR_BLOQUEADO, CONTA, PROCESSO, VARA, AUTOR, DT_DESBLOQUEIO)
            VALUES (:dt_dep, :vr, :conta, :processo, :vara, :autor, NULL)
        """), {'dt_dep': dt_dep, 'vr': vr, 'conta': conta,
               'processo': processo, 'vara': vara, 'autor': autor})
        db.session.commit()

        registrar_log(
            acao='inclusao', entidade='bloqueios_judiciais', entidade_id=None,
            descricao=f'Novo bloqueio — processo {processo}',
            dados_novos={'DT_DEPOSITO': dt_dep.strftime('%Y-%m-%d'), 'VR_BLOQUEADO': str(vr),
                         'CONTA': conta, 'PROCESSO': processo, 'VARA': vara, 'AUTOR': autor},
        )
        return jsonify({'success': True, 'message': f'Bloqueio do processo {processo} incluído.'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': f'Erro ao incluir: {str(e)}'}), 500


@bloqueios_judiciais_bp.route('/editar', methods=['POST'])
@login_required
def editar():
    # Novos valores
    dt_dep = _parse_data(request.form.get('DT_DEPOSITO'))
    vr = _parse_decimal(request.form.get('VR_BLOQUEADO'))
    conta = (request.form.get('CONTA') or '').strip()
    processo = (request.form.get('PROCESSO') or '').strip()
    vara = (request.form.get('VARA') or '').strip()
    autor = (request.form.get('AUTOR') or '').strip()
    dt_desb = _parse_data(request.form.get('DT_DESBLOQUEIO'))  # pode ser None

    if not dt_dep or vr is None or not conta or not processo:
        return jsonify({'success': False,
                        'message': 'Data do depósito, valor, conta e processo são obrigatórios.'}), 400

    # Valores ORIGINAIS (identificam a linha) — comparação null-safe por string
    o_dep = _parse_data(request.form.get('o_DT_DEPOSITO'))
    o_vr = _parse_decimal(request.form.get('o_VR_BLOQUEADO'))
    o_desb = _parse_data(request.form.get('o_DT_DESBLOQUEIO'))
    params = {
        'dt_dep': dt_dep, 'vr': vr, 'conta': conta, 'processo': processo,
        'vara': vara, 'autor': autor, 'dt_desb': dt_desb,
        'o_dep': o_dep.strftime('%Y%m%d') if o_dep else '',
        'o_vr': o_vr,
        'o_conta': (request.form.get('o_CONTA') or '').strip(),
        'o_processo': (request.form.get('o_PROCESSO') or '').strip(),
        'o_vara': (request.form.get('o_VARA') or '').strip(),
        'o_autor': (request.form.get('o_AUTOR') or '').strip(),
        'o_desb': o_desb.strftime('%Y%m%d') if o_desb else '',
    }

    try:
        result = db.session.execute(text(f"""
            UPDATE {_TB}
            SET DT_DEPOSITO = :dt_dep, VR_BLOQUEADO = :vr, CONTA = :conta,
                PROCESSO = :processo, VARA = :vara, AUTOR = :autor,
                DT_DESBLOQUEIO = :dt_desb
            WHERE CONVERT(varchar(8), DT_DEPOSITO, 112) = :o_dep
              AND VR_BLOQUEADO = :o_vr
              AND ISNULL(CONTA, '') = :o_conta
              AND ISNULL(PROCESSO, '') = :o_processo
              AND ISNULL(VARA, '') = :o_vara
              AND ISNULL(AUTOR, '') = :o_autor
              AND ISNULL(CONVERT(varchar(8), DT_DESBLOQUEIO, 112), '') = :o_desb
        """), params)
        db.session.commit()

        if result.rowcount == 0:
            return jsonify({'success': False,
                            'message': 'Nenhuma linha correspondente encontrada (a linha pode ter sido alterada).'}), 404

        registrar_log(
            acao='atualizacao', entidade='bloqueios_judiciais', entidade_id=None,
            descricao=f'Edição de bloqueio — processo {processo}',
            dados_novos={'DT_DEPOSITO': dt_dep.strftime('%Y-%m-%d'), 'VR_BLOQUEADO': str(vr),
                         'CONTA': conta, 'PROCESSO': processo, 'VARA': vara, 'AUTOR': autor,
                         'DT_DESBLOQUEIO': dt_desb.strftime('%Y-%m-%d') if dt_desb else None},
        )
        aviso = '' if result.rowcount == 1 else f' ({result.rowcount} linhas idênticas atualizadas)'
        return jsonify({'success': True, 'message': f'Bloqueio atualizado.{aviso}'})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': f'Erro ao editar: {str(e)}'}), 500