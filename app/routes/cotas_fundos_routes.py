from datetime import timedelta
from decimal import Decimal, InvalidOperation

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required

from app import db
from app.models.cotas_fundos import (
    FUNDOS, obter_ultimo_registro, obter_proxima_data,
    e_dia_util, calcular_ind_cota,
)
from app.utils.audit import registrar_log

cotas_fundos_bp = Blueprint(
    'cotas_fundos', __name__, url_prefix='/cotas-fundos'
)

# Em qual tipo de dia o preenchimento é AUTOMÁTICO (repete o dia anterior e
# zera o IND_COTA). 'nao_util' = fim de semana/feriado (padrão, pois a cota
# não se move e o índice dá zero). Troque para 'util' se a regra for outra.
AUTO_PREENCHER_EM = 'nao_util'


def _to_decimal(valor):
    """Converte string do formulário em Decimal. None se vazio/inválido."""
    if valor is None:
        return None
    txt = str(valor).strip().replace('.', '').replace(',', '.') \
        if ',' in str(valor) else str(valor).strip()
    if txt == '':
        return None
    try:
        return Decimal(txt)
    except (InvalidOperation, ValueError):
        return None


def _auto_preencher(data):
    """
    True somente quando a próxima DATA NÃO é dia útil (fim de semana/feriado).
    Nesse caso os valores são copiados do dia anterior e o IND_COTA vai zerado,
    pois a cota não se move. Em dia útil o usuário digita o VR_COTA.
    Data fora do calendário: exige digitação (retorna False).
    """
    util = e_dia_util(data)
    if util is None:
        return False
    return not util


def _montar_contexto_fundo(chave, cfg):
    """Monta o estado atual de um fundo para a tela."""
    model = cfg['model']
    ultimo = obter_ultimo_registro(model)
    proxima = obter_proxima_data(model)

    dados_anteriores = {}
    if ultimo is not None:
        for (attr, _label) in cfg['campos']:
            valor = getattr(ultimo, attr, None)
            dados_anteriores[attr] = float(valor) if valor is not None else None

    util = e_dia_util(proxima) if proxima else None
    return {
        'chave': chave,
        'label': cfg['label'],
        'tabela': cfg['tabela'],
        'campos': cfg['campos'],
        'proxima_data': proxima,
        'data_anterior': ultimo.DATA if ultimo else None,
        'vr_cota_anterior': float(ultimo.VR_COTA) if (ultimo and ultimo.VR_COTA is not None) else None,
        'dados_anteriores': dados_anteriores,
        'e_dia_util': util,
        'auto': _auto_preencher(proxima) if proxima else False,
        'vazia': ultimo is None,
    }


@cotas_fundos_bp.route('/')
@login_required
def index():
    """Tela de entrada de dados das cotas dos fundos (FIN_TB026/027/028)."""
    fundos = [_montar_contexto_fundo(chave, cfg) for chave, cfg in FUNDOS.items()]
    return render_template('cotas_fundos/index.html', fundos=fundos)


@cotas_fundos_bp.route('/salvar/<chave>', methods=['POST'])
@login_required
def salvar(chave):
    """Grava a linha da próxima DATA do fundo informado."""
    cfg = FUNDOS.get(chave)
    if not cfg:
        return jsonify({'success': False, 'message': 'Fundo inválido.'}), 400

    model = cfg['model']
    ultimo = obter_ultimo_registro(model)
    proxima = obter_proxima_data(model)

    if proxima is None:
        return jsonify({
            'success': False,
            'message': (f'A tabela {cfg["tabela"]} está vazia. Cadastre a primeira '
                        f'linha (data inicial) diretamente no banco para iniciar a série.')
        }), 400

    try:
        if _auto_preencher(proxima):
            # Dia de repetição: copia tudo do dia anterior e zera o IND_COTA.
            registro = model(DATA=proxima)
            registro.VR_COTA = ultimo.VR_COTA
            registro.IND_COTA = Decimal('0.00000000')
            for (attr, _label) in cfg['campos']:
                setattr(registro, attr, getattr(ultimo, attr, None))
            origem = 'automático (repetiu o dia anterior)'
        else:
            vr_cota = _to_decimal(request.form.get('VR_COTA'))
            if vr_cota is None:
                return jsonify({'success': False,
                                'message': 'Informe o VR_COTA.'}), 400
            registro = model(DATA=proxima)
            registro.VR_COTA = vr_cota
            registro.IND_COTA = calcular_ind_cota(
                vr_cota, ultimo.VR_COTA if ultimo else None
            )
            for (attr, _label) in cfg['campos']:
                setattr(registro, attr, _to_decimal(request.form.get(attr)))
            origem = 'manual'

        db.session.add(registro)
        db.session.commit()

        registrar_log(
            acao='inclusao',
            entidade='cotas_fundos',
            entidade_id=None,
            descricao=f'Cotas {cfg["label"]} — {proxima.strftime("%d/%m/%Y")} ({origem})',
            dados_novos={
                'tabela': cfg['tabela'],
                'DATA': proxima.strftime('%Y-%m-%d'),
                'VR_COTA': str(registro.VR_COTA),
                'IND_COTA': str(registro.IND_COTA),
            },
        )

        return jsonify({
            'success': True,
            'message': (f'{cfg["label"]}: {proxima.strftime("%d/%m/%Y")} gravado '
                        f'({origem}). IND_COTA = {registro.IND_COTA}.'),
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': f'Erro ao gravar: {str(e)}'}), 500


@cotas_fundos_bp.route('/preencher-automaticos/<chave>', methods=['POST'])
@login_required
def preencher_automaticos(chave):
    """
    Avança automaticamente todas as datas seguidas que são de repetição
    (não úteis), parando na primeira que exige digitação.
    """
    cfg = FUNDOS.get(chave)
    if not cfg:
        return jsonify({'success': False, 'message': 'Fundo inválido.'}), 400

    model = cfg['model']
    criados = []
    try:
        for _ in range(60):  # trava de segurança
            ultimo = obter_ultimo_registro(model)
            proxima = obter_proxima_data(model)
            if proxima is None or not _auto_preencher(proxima):
                break
            registro = model(DATA=proxima)
            registro.VR_COTA = ultimo.VR_COTA
            registro.IND_COTA = Decimal('0.00000000')
            for (attr, _label) in cfg['campos']:
                setattr(registro, attr, getattr(ultimo, attr, None))
            db.session.add(registro)
            db.session.flush()
            criados.append(proxima.strftime('%d/%m/%Y'))
        db.session.commit()

        if not criados:
            return jsonify({'success': True,
                            'message': 'Nenhum dia automático pendente.'})

        registrar_log(
            acao='inclusao',
            entidade='cotas_fundos',
            entidade_id=None,
            descricao=f'Cotas {cfg["label"]} — {len(criados)} dia(s) automático(s)',
            dados_novos={'tabela': cfg['tabela'], 'datas': criados},
        )
        return jsonify({
            'success': True,
            'message': f'{len(criados)} dia(s) preenchido(s): ' + ', '.join(criados) + '.',
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'message': f'Erro: {str(e)}'}), 500