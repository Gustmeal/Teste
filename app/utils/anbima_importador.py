"""
Importador dos Índices ANBIMA (IMA) — opera por baixo, via POST direto na
página de resultado (ima-carteira.asp), sem navegador. Mesmo espírito da
importação automática da B3.

Fluxo real do site (confirmado no HTML das páginas):
  - O formulário de consulta envia um POST para 'ima-carteira.asp' com:
      Indice   = 'irf-m 1'  (índice escolhido no print)
      DataRef  = 'DDMMAAAA' (ex.: '24072026')
      Consulta = 'Ambos'    (Totais & Carteira)
      Tipo     = '1'
  - A resposta é o HTML com a tabela cujos cabeçalhos são
    'Variação Diária/no Mês/no Ano/12 Meses/24 Meses (%)'.
"""
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import unicodedata

import requests

URL_RESULTADO = 'https://www.anbima.com.br/informacoes/ima/ima-carteira.asp'

# Índice consultado (a FIN_TB030 guarda um conjunto por dia). Troque aqui se
# a área quiser outro índice como referência.
INDICE_PADRAO = 'irf-m 1'

_HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/124.0 Safari/537.36'),
    'Referer': 'https://www.anbima.com.br/pt_br/informar/ima-resultados-diarios.htm',
    'Content-Type': 'application/x-www-form-urlencoded',
}


def _to_decimal(txt):
    """'1,0725' / '-0,03' -> Decimal. None se não for número."""
    if txt is None:
        return None
    t = str(txt).strip().replace('%', '').replace('.', '').replace(',', '.')
    if t in ('', '-', '--'):
        return None
    try:
        return Decimal(t)
    except (InvalidOperation, ValueError):
        return None


def _linhas_da_tabela(html):
    """Extrai as linhas (listas de células, texto puro) das <table> do HTML."""
    linhas = []
    for tr in re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.I | re.S):
        celulas = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', tr, re.I | re.S)
        if not celulas:
            continue
        limpa = []
        for c in celulas:
            txt = re.sub(r'<[^>]+>', ' ', c)
            txt = re.sub(r'&nbsp;', ' ', txt)
            txt = re.sub(r'\s+', ' ', txt).strip()
            limpa.append(txt)
        linhas.append(limpa)
    return linhas


def importar_indice_anbima(data_ref, indice=INDICE_PADRAO):
    """
    Consulta a ANBIMA e devolve as 5 variações + DIA, mapeando por CABEÇALHO
    (não por ordem). Casa cada coluna da tabela pelo título:
      'Variação Diária (%)', 'Variação no Mês (%)', 'Variação no Ano (%)',
      'Variação 12 Meses (%)', 'Variação 24 Meses (%)'.
    """
    if not isinstance(data_ref, date):
        raise Exception('Data de referência inválida.')

    payload = {
        'Indice': indice,
        'DataRef': data_ref.strftime('%d%m%Y'),   # DDMMAAAA
        'Dt_Ref': data_ref.strftime('%d/%m/%Y'),
        'Consulta': 'Ambos',
        'Tipo': '1',
        'Idioma': 'PT',
        'Pai': 'ima_carteira',
    }

    try:
        resp = requests.post(URL_RESULTADO, data=payload,
                             headers=_HEADERS, timeout=30, verify=True)
        resp.raise_for_status()
    except requests.exceptions.SSLError:
        raise Exception('Falha de SSL ao acessar a ANBIMA (proxy corporativo?). '
                        'Verifique o certificado/rede.')
    except requests.exceptions.RequestException as e:
        raise Exception(f'Não foi possível acessar a ANBIMA: {e}')

    resp.encoding = resp.apparent_encoding or 'latin-1'
    html = resp.text
    linhas = _linhas_da_tabela(html)

    # Normaliza texto p/ comparação (sem acento/pontuação/espaço, minúsculo)
    def _norm(s):
        s = unicodedata.normalize('NFKD', str(s or ''))
        s = ''.join(c for c in s if not unicodedata.combining(c))
        return re.sub(r'[^a-z0-9]', '', s.lower())

    # Título de cada coluna que queremos -> chave do retorno
    ALVOS = [
        ('VARIACAO_DIARIA_PERC',  _norm('Variação Diária (%)')),      # variacaodiaria
        ('VARIACAO_MENSAL_PERC',  _norm('Variação no Mês (%)')),      # variacaonomes
        ('VARIACAO_ANUAL_PERC',   _norm('Variação no Ano (%)')),      # variacaonoano
        ('VARIACAO_12MESES_PERC', _norm('Variação 12 Meses (%)')),    # variacao12meses
        ('VARIACAO_24MESES_PERC', _norm('Variação 24 Meses (%)')),    # variacao24meses
    ]

    # 1) Localiza a linha de cabeçalho e o índice de cada coluna alvo
    col_idx = {}
    header_row_i = None
    for i, cels in enumerate(linhas):
        celns = [_norm(c) for c in cels]
        achou = {}
        for chave, alvo in ALVOS:
            for j, cn in enumerate(celns):
                if cn == alvo:
                    achou[chave] = j
                    break
        # cabeçalho válido = achou pelo menos as 5 colunas de variação
        if len(achou) >= 5:
            col_idx = achou
            header_row_i = i
            break

    if not col_idx or header_row_i is None:
        raise Exception('Não encontrei o cabeçalho das colunas de variação na '
                        'resposta da ANBIMA (layout pode ter mudado).')

    # 2) Nas linhas ABAIXO do cabeçalho, pega a do índice pedido (ex.: IRF-M 1)
    alvo_indice = _norm(indice)
    j_diaria = col_idx['VARIACAO_DIARIA_PERC']

    def _tem_dados(cels):
        return (j_diaria < len(cels) and
                _to_decimal(cels[j_diaria]) is not None)

    linha_dados = None
    # 2a) linha que casa o índice E tem número na Variação Diária
    for cels in linhas[header_row_i + 1:]:
        if _tem_dados(cels) and any(alvo_indice in _norm(c) for c in cels):
            linha_dados = cels
            break
    # 2b) fallback: 1ª linha com números nas colunas de variação
    if linha_dados is None:
        for cels in linhas[header_row_i + 1:]:
            if _tem_dados(cels):
                linha_dados = cels
                break

    if linha_dados is None:
        raise Exception('Cabeçalho localizado, mas não achei a linha de dados com '
                        f'as variações do índice {indice} (pode não haver '
                        'divulgação nesse dia).')

    # 3) Lê cada coluna pelo índice do cabeçalho
    resultado = {'DIA': data_ref}
    for chave, j in col_idx.items():
        resultado[chave] = _to_decimal(linha_dados[j]) if j < len(linha_dados) else None

    if resultado.get('VARIACAO_DIARIA_PERC') is None:
        # Diagnóstico: mostra o que foi encontrado para ajustarmos o mapeamento
        cab = linhas[header_row_i]
        amostra_cab = ' | '.join(f'[{k}]={cab[v] if v < len(cab) else "?"}'
                                 for k, v in col_idx.items())
        celula = (linha_dados[col_idx["VARIACAO_DIARIA_PERC"]]
                  if col_idx["VARIACAO_DIARIA_PERC"] < len(linha_dados) else '(fora do range)')
        linha_txt = ' | '.join(linha_dados[:14])
        raise Exception(
            'Variação Diária veio vazia. '
            f'Célula lida = {celula!r}. '
            f'Colunas mapeadas: {amostra_cab}. '
            f'Linha do índice: {linha_txt}'
        )
    return resultado