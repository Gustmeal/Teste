"""
Motor de textos do Relatório de Gestão Financeira.

Regra dos valores: os valores monetários (R$) e percentuais (%) de cada texto
são contados em ordem de leitura — INCLUINDO os entre parênteses. O n-ésimo
valor (contagem GLOBAL, contínua entre os blocos) é substituído pelo VR da
FIN_TB023 cujo ID == n. Sem ID correspondente, o valor do modelo é mantido.

Regra do mês: o único trecho de data que muda é o mês de referência, vindo do
POSICAO ('AAAAMM'). Os placeholders {MES_REF}/{MES_REF_CAP}/{ANO_REF} do
template recebem esse valor.

O VR é decimal(18,2); aqui ele é reformatado no padrão do token original
(moeda 'R$'/sinal, ou '%').

Compatível com Python 3.9 e 3.12.
"""
import re
from decimal import Decimal

# Moeda BR (sinal opcional) OU percentual BR.
_PADRAO_VALOR = re.compile(
    r'-?R\$\s?\d[\d.]*,\d+'   # moeda: -R$ 216,45 / R$ 3.158,94
    r'|\d[\d.]*,\d+\s?%'     # percentual com decimais: 21,8%
    r'|\d+\s?%'              # percentual inteiro: 5%
)

_MESES_PT = {
    1: 'janeiro', 2: 'fevereiro', 3: 'março', 4: 'abril',
    5: 'maio', 6: 'junho', 7: 'julho', 8: 'agosto',
    9: 'setembro', 10: 'outubro', 11: 'novembro', 12: 'dezembro',
}


def partes_posicao(posicao):
    """'AAAAMM' -> (ano, mes_nome, mes_nome_cap). Ex.: '202605' -> ('2026','maio','Maio')."""
    p = str(posicao or '').strip()
    if len(p) < 6 or not p[:6].isdigit():
        return '', '', ''
    ano = p[:4]
    nome = _MESES_PT.get(int(p[4:6]), '')
    return ano, nome, (nome.capitalize() if nome else '')


def _fmt_br(valor, casas=2):
    """Formata número no padrão BR (milhar '.', decimal ',')."""
    d = Decimal(str(valor))
    neg = d < 0
    inteiro, _, dec = f"{abs(d):.{casas}f}".partition('.')
    inteiro = re.sub(r'(?<=\d)(?=(?:\d{3})+$)', '.', inteiro)
    corpo = f"{inteiro},{dec}" if casas else inteiro
    return ('-' if neg else '') + corpo


def _formatar_moeda(vr):
    """Ex.: 3158.94 -> 'R$ 3.158,94' ; -216.45 -> '-R$ 216,45'."""
    d = Decimal(str(vr))
    return ('-' if d < 0 else '') + 'R$ ' + _fmt_br(abs(d), 2)


def _formatar_percentual(vr):
    """Ex.: 21.80 -> '21,8%' ; 1.07 -> '1,07%' ; 100.00 -> '100%'."""
    d = Decimal(str(vr))
    corpo = _fmt_br(abs(d), 2)
    if ',' in corpo:
        corpo = corpo.rstrip('0').rstrip(',')
    return ('-' if d < 0 else '') + corpo + '%'


def substituir_valores_bloco(texto, mapa_id_vr, contador_inicial=0):
    """
    Substitui os valores de UM bloco usando a contagem GLOBAL.
    Retorna (texto_substituido, novo_contador).
    """
    if not texto:
        return texto, contador_inicial
    estado = {'n': contador_inicial}

    def _repl(m):
        estado['n'] += 1
        vr = mapa_id_vr.get(estado['n'])
        if vr is None:
            return m.group()
        return _formatar_percentual(vr) if '%' in m.group() else _formatar_moeda(vr)

    return _PADRAO_VALOR.sub(_repl, texto), estado['n']


def renderizar_pagina(estrutura, mapa_id_vr, mes_ref, mes_ref_cap, ano_ref):
    """
    Percorre a estrutura da página. Nos itens de texto aplica o mês de
    referência (placeholders) e substitui os valores contados (contagem GLOBAL
    e contínua). Itens 'grafico'/'tabela' viram espaços reservados.
    """
    itens = []
    contador = 0
    for item in estrutura:
        if item.get('tipo') == 'texto':
            texto = (item.get('texto', '')
                     .replace('{MES_REF_CAP}', mes_ref_cap or '')
                     .replace('{MES_REF}', mes_ref or '')
                     .replace('{ANO_REF}', ano_ref or ''))
            texto, contador = substituir_valores_bloco(texto, mapa_id_vr, contador)
            itens.append({'tipo': 'texto', 'conteudo': texto})
        else:
            itens.append({'tipo': item.get('tipo', 'texto'),
                          'titulo': item.get('titulo', ''),
                          'chave': item.get('chave', '')})
    return itens