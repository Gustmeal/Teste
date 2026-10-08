# app/utils/analise_financeira_pf_nota.py
"""
Nota Técnica (Memorando) da Análise Financeira PF em Word.

Fonte: view BDG.FIN_VW037_ANALISE_FINANCEIRA_PF_NOTA
  ID          -> ordem do fragmento no documento
  NU_CONTRATO -> contrato da nota
  TEXTO       -> fragmento de texto com '...' no lugar do valor
  INFORMACAO  -> valor que entra no lugar do '...' (NULL = fica pendente)

Mesma ideia do Relatório de Gestão: o texto é todo da view e o aplicativo
só troca o placeholder pelo valor e formata. Quando a view muda (positivo,
negativo reversível, negativo irreversível...), a nota muda sozinha.

Regras de montagem:
  1. Parágrafos: um fragmento que começa com espaço, letra minúscula ou '('
     CONTINUA o parágrafo anterior (ex.: 'e ...% nos 6 meses', ' de ...%');
     os demais abrem parágrafo novo. Se o fragmento que continua começa com
     '(' e o anterior termina num ponto solto (' .'), esse ponto sai. Quebra de linha dentro do TEXTO vira
     quebra de linha no Word.
  2. Placeholder '...' (ou '---'):
       'R$ ...'   -> moeda BR com 2 casas  (negativo: -R$ 176.108,94, sem quebrar linha)
       '...%'     -> percentual BR, até 2 casas, sem zeros à direita (14,33)
       'Jul/2026' -> mês/ano em minúsculo (jul/2026), como no modelo
       número     -> inteiro sem casas (4, 18) ou BR com até 2 casas
     INFORMACAO com vários valores separados por '|' preenche os '...' do
     fragmento na ordem. Sem valor, o '...' fica e é DESTACADO em amarelo
     no Word (assim como 'xxx' e 'xx/xx/xxxx'), para completar à mão.
  3. Formatação igual aos modelos AF_*.docx: A4, Calibri 13,5, preto,
     'Assunto:' com o restante em negrito, itens '*' como marcadores,
     parágrafos numerados justificados, nomes da assinatura em negrito.
  4. Ao final entra a assinatura do usuário logado (nome + cargo).

Resumo (view BDG.FIN_VW038_ANALISE_FINANCEIRA_PF_RESUMOS), mesmas colunas:
  - fragmento SEM valor e SEM '...'          -> título de seção (ex.: 'Resumo VPL')
  - fragmento com '...' e INFORMACAO         -> o valor entra no lugar do '...'
  - fragmento SEM '...' e COM INFORMACAO      -> o valor vai na coluna "Valor"
    (número = moeda R$; texto como '242/2026' entra como veio)
  - '...' sem valor fica destacado (pendente), como na nota.

Compatível com Python 3.9 e 3.12.
"""
import re
from decimal import Decimal, InvalidOperation
from io import BytesIO

from sqlalchemy import text
from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_COLOR_INDEX
from docx.oxml.ns import qn
from docx.shared import Pt, RGBColor, Emu

from app import db

VW_NOTA = '[BDG].[FIN_VW037_ANALISE_FINANCEIRA_PF_NOTA]'
VW_RESUMO = '[BDG].[FIN_VW038_ANALISE_FINANCEIRA_PF_RESUMOS]'

# Layout dos modelos AF_*.docx
FONTE = 'Calibri'
TAMANHO = Pt(13.5)
MARGEM_LATERAL = Emu(1080135)     # ~3,0 cm
MARGEM_VERTICAL = Emu(899795)     # ~2,5 cm
LARGURA_A4 = Emu(7560310)
ALTURA_A4 = Emu(10692130)
RECUO_MARCADOR = Emu(685800)      # 0,75"
RECUO_PENDURADO = Emu(-228600)    # -0,25"

# Placeholder = exatamente 3 pontos (ou 3 traços): em 'R$ ....' o 4º ponto é o final da frase
_PLACEHOLDER = re.compile(r'(R\$\s*)?(\.{3}|-{3})')
_PENDENTE = re.compile(r'\.{3}|-{3}|\bx{2}/x{2}/x{2,4}\b|\bx{3,}\b', re.IGNORECASE)
_MES_ANO = re.compile(r'^[A-Za-zÀ-ú]{3}/\d{4}$')
_NOME_ASSINATURA = re.compile(r'^[A-ZÀ-Ý\s\.]+$')


# =========================================================================
# LEITURA
# =========================================================================
def carregar_textos(nu_contrato):
    """Fragmentos da nota do contrato, na ordem do ID."""
    rows = db.session.execute(text(f"""
        SELECT [ID], [TEXTO], [INFORMACAO]
        FROM {VW_NOTA}
        WHERE [NU_CONTRATO] = :nu
        ORDER BY [ID]
    """), {'nu': nu_contrato}).fetchall()
    return [{'ID': r[0], 'TEXTO': r[1], 'INFORMACAO': r[2]} for r in rows]


# =========================================================================
# FORMATAÇÃO DOS VALORES
# =========================================================================
def _br(valor, casas):
    inteiro, _, dec = f'{abs(valor):.{casas}f}'.partition('.')
    inteiro = re.sub(r'(?<=\d)(?=(?:\d{3})+$)', '.', inteiro)
    return f'{inteiro},{dec}' if casas else inteiro


def _decimal(valor):
    txt = str(valor).strip()
    if ',' in txt and '.' in txt:
        txt = txt.replace('.', '').replace(',', '.')
    elif ',' in txt:
        txt = txt.replace(',', '.')
    try:
        return Decimal(txt)
    except InvalidOperation:
        return None


def _valores_informacao(informacao):
    """'20.40' -> ['20.40'] ; '239/2026|16/09/2026' -> ['239/2026', '16/09/2026'] ; NULL -> []"""
    if informacao is None:
        return []
    txt = str(informacao).strip()
    if not txt or txt.upper() == 'NULL':
        return []
    return [v.strip() for v in txt.split('|')]


def _formatar(valor, moeda, percentual):
    """Formata um valor da INFORMACAO conforme o contexto do placeholder."""
    if _MES_ANO.match(valor):
        return valor.lower()
    numero = _decimal(valor)
    if numero is None:
        return valor                              # texto livre: entra como veio
    if moeda:
        # hífen inseparável (U+2011) + espaço inseparável: o valor não quebra de linha
        return ('\u2011' if numero < 0 else '') + 'R$\u00a0' + _br(numero, 2)
    if percentual or numero != numero.to_integral_value():
        corpo = _br(numero, 2)
        if ',' in corpo:
            corpo = corpo.rstrip('0').rstrip(',')
        return ('-' if numero < 0 else '') + corpo
    return ('-' if numero < 0 else '') + _br(numero, 0)


def preencher(texto, informacao):
    """Troca os '...' do fragmento pelos valores da INFORMACAO, na ordem."""
    valores = _valores_informacao(informacao)
    if not texto or not valores:
        return texto or ''
    fila = list(valores)

    def _troca(m):
        if not fila:
            return m.group(0)                     # sem valor: placeholder fica (pendente)
        valor = fila.pop(0)
        moeda = m.group(1) is not None
        percentual = texto[m.end():].lstrip().startswith('%')
        return _formatar(valor, moeda, percentual)

    return _PLACEHOLDER.sub(_troca, texto)


# =========================================================================
# MONTAGEM DOS PARÁGRAFOS
# =========================================================================
def _continua_paragrafo(texto_bruto):
    """Fragmento que começa com espaço, minúscula ou '(' continua o anterior."""
    if not texto_bruto:
        return False
    primeiro = texto_bruto[0]
    return primeiro.isspace() or primeiro.islower() or primeiro == '('


def _limpar(texto):
    linhas = []
    for linha in texto.split('\n'):
        linha = re.sub(r'[ \t\u00a0]+', ' ', linha).strip()
        # tira espaço antes de pontuação, mas não antes de um '...' pendente
        linha = re.sub(r'\s+([,;:)%]|\.(?!\.\.))', r'\1', linha)
        linha = re.sub(r'\(\s+', '(', linha)
        linha = re.sub(r'R\$ (?=[-\d.])', 'R$\u00a0', linha)   # 'R$' não fica sozinho no fim da linha
        linhas.append(linha)
    return '\n'.join(linhas)


def montar_paragrafos(registros):
    """
    Junta os fragmentos em parágrafos e classifica cada um:
      'marcador'   -> começa com '*'
      'numerado'   -> começa com 'N.'
      'assunto'    -> começa com 'Assunto:'
      'nome'       -> nome em maiúsculas (assinatura)
      'texto'      -> demais
    Retorna [{'tipo', 'texto'}].
    """
    brutos = []
    for r in registros:
        bruto = (r['TEXTO'] or '').replace('\r\n', '\n').replace('\r', '\n')
        if not bruto.strip() or bruto.strip().upper() == 'NULL':
            continue
        preenchido = preencher(bruto, r['INFORMACAO'])
        if brutos and _continua_paragrafo(bruto):
            anterior = brutos[-1]
            # '... do valor de avaliação .' + '(desconto de 40%).' -> sem o ponto solto antes do '('
            fim = anterior.rstrip()
            if bruto.lstrip().startswith('(') and fim.endswith('.') and fim[:-1].endswith((' ', '\u00a0')):
                anterior = fim[:-1]
            brutos[-1] = anterior + ' ' + preenchido
        else:
            brutos.append(preenchido)

    paragrafos = []
    for bruto in brutos:
        texto = _limpar(bruto)
        if texto.startswith('*'):
            tipo, texto = 'marcador', texto.lstrip('*').strip()
        elif re.match(r'^\d+\.\s', texto):
            tipo = 'numerado'
        elif texto.lower().startswith('assunto:'):
            tipo = 'assunto'
        elif _NOME_ASSINATURA.match(texto) and len(texto) >= 8:
            tipo = 'nome'
        else:
            tipo = 'texto'
        paragrafos.append({'tipo': tipo, 'texto': texto})
    return paragrafos


# =========================================================================
# WORD
# =========================================================================
def _fonte_run(run, negrito=False):
    run.font.name = FONTE
    run.font.size = TAMANHO
    run.font.color.rgb = RGBColor(0, 0, 0)
    run.bold = negrito
    r_pr = run._element.get_or_add_rPr()
    r_fonts = r_pr.find(qn('w:rFonts'))
    if r_fonts is None:
        r_fonts = r_pr.makeelement(qn('w:rFonts'), {})
        r_pr.append(r_fonts)
    for atributo in ('w:ascii', 'w:hAnsi', 'w:cs', 'w:eastAsia'):
        r_fonts.set(qn(atributo), FONTE)


def _escrever(paragrafo, texto, negrito=False, negrito_palavras=()):
    """
    Escreve o texto em runs: trechos pendentes ('...', 'xxx', 'xx/xx/xxxx')
    com marca-texto amarelo; palavras de negrito_palavras em negrito;
    '\\n' vira quebra de linha.
    """
    linhas = texto.split('\n')
    for i, linha in enumerate(linhas):
        if i > 0:
            paragrafo.add_run().add_break()
        pos = 0
        for m in _PENDENTE.finditer(linha):
            _escrever_trecho(paragrafo, linha[pos:m.start()], negrito, negrito_palavras)
            run = paragrafo.add_run(m.group(0))
            _fonte_run(run, negrito)
            run.font.highlight_color = WD_COLOR_INDEX.YELLOW
            pos = m.end()
        _escrever_trecho(paragrafo, linha[pos:], negrito, negrito_palavras)


def _escrever_trecho(paragrafo, trecho, negrito, negrito_palavras):
    if not trecho:
        return
    if not negrito_palavras:
        _fonte_run(paragrafo.add_run(trecho), negrito)
        return
    padrao = re.compile(r'\b(' + '|'.join(re.escape(p) for p in negrito_palavras) + r')\b')
    pos = 0
    for m in padrao.finditer(trecho):
        if m.start() > pos:
            _fonte_run(paragrafo.add_run(trecho[pos:m.start()]), negrito)
        _fonte_run(paragrafo.add_run(m.group(0)), True)
        pos = m.end()
    if pos < len(trecho):
        _fonte_run(paragrafo.add_run(trecho[pos:]), negrito)


def _novo_paragrafo(doc, alinhamento=None):
    p = doc.add_paragraph()
    pf = p.paragraph_format
    pf.space_before = Pt(0)
    pf.space_after = Pt(0)
    if alinhamento is not None:
        p.alignment = alinhamento
    return p


def _linha_em_branco(doc):
    _fonte_run(_novo_paragrafo(doc).add_run('\u00a0'))


def gerar_docx(paragrafos, assinante_nome=None, assinante_cargo=None):
    """Monta o .docx no layout dos modelos e devolve BytesIO."""
    doc = Document()

    secao = doc.sections[0]
    secao.page_width = LARGURA_A4
    secao.page_height = ALTURA_A4
    secao.left_margin = secao.right_margin = MARGEM_LATERAL
    secao.top_margin = secao.bottom_margin = MARGEM_VERTICAL

    estilo = doc.styles['Normal']
    estilo.font.name = FONTE
    estilo.font.size = TAMANHO
    estilo.paragraph_format.space_before = Pt(0)
    estilo.paragraph_format.space_after = Pt(0)

    total = len(paragrafos)
    for i, par in enumerate(paragrafos):
        tipo, texto = par['tipo'], par['texto']
        proximo = paragrafos[i + 1] if i + 1 < total else None

        if tipo == 'marcador':
            p = _novo_paragrafo(doc, WD_ALIGN_PARAGRAPH.JUSTIFY)
            p.paragraph_format.left_indent = RECUO_MARCADOR
            p.paragraph_format.first_line_indent = RECUO_PENDURADO
            _fonte_run(p.add_run('•\t'))
            _escrever(p, texto)
        elif tipo == 'assunto':
            p = _novo_paragrafo(doc)
            rotulo, _, resto = texto.partition(':')
            _fonte_run(p.add_run(rotulo + ':\u00a0'))
            _escrever(p, resto.strip(), negrito=True)
        elif tipo == 'numerado':
            p = _novo_paragrafo(doc, WD_ALIGN_PARAGRAPH.JUSTIFY)
            # Como no modelo: 'positivo'/'negativo' em negrito só no item que anuncia o resultado (termina em ':')
            destaque = ('positivo', 'negativo') if texto.rstrip().endswith(':') else ()
            _escrever(p, texto, negrito_palavras=destaque)
        elif tipo == 'nome':
            p = _novo_paragrafo(doc)
            _escrever(p, texto, negrito=True)
        else:
            p = _novo_paragrafo(doc)
            _escrever(p, texto)

        # Linha em branco entre blocos, como nos modelos:
        #  - não separa marcadores entre si;
        #  - o resultado (linha do contrato) fica colado no item que o anuncia;
        #  - o nome da assinatura fica colado no cargo.
        if proximo is None:
            continue
        if tipo == 'marcador' and proximo['tipo'] == 'marcador':
            continue
        if tipo == 'nome':
            continue
        _linha_em_branco(doc)

    # Assinatura do usuário que gerou a nota
    if assinante_nome:
        _linha_em_branco(doc)
        p = _novo_paragrafo(doc)
        _fonte_run(p.add_run(assinante_nome.upper()), True)
        if assinante_cargo:
            p.add_run().add_break()
            _fonte_run(p.add_run(assinante_cargo))

    saida = BytesIO()
    doc.save(saida)
    saida.seek(0)
    return saida


# =========================================================================
# DADOS DO CONTRATO (MEMO_GEFIN e GERENTE da FIN_TB035)
# =========================================================================
_LINHA_MEMO_GEFIN = re.compile(r'Memorando\s+SEI.*Gefin', re.IGNORECASE)


def aplicar_dados_contrato(registros, memo_gefin=None, gerente=None):
    """
    Completa os textos da view (nota FIN_VW037 ou resumo FIN_VW038) com os
    dados preenchidos no modal e gravados na FIN_TB035:

      - MEMO_GEFIN ('762/2026'): entra no '...' da linha
        'Memorando SEI nº .../Gefin/Sufin/Difin' quando a view ainda não
        trouxer INFORMACAO -> 'Memorando SEI nº 762/2026/Gefin/Sufin/Difin'.
      - GERENTE: substitui o nome da linha imediatamente anterior a 'Gerente'
        (assinatura da gerente), em maiúsculas.

    Se a view já trouxer o valor (INFORMACAO preenchida), ela prevalece.
    Devolve uma nova lista; os registros originais não são alterados.
    """
    novos = [dict(r) for r in registros]
    memo = str(memo_gefin or '').strip()
    nome = str(gerente or '').strip()

    if memo:
        for r in novos:
            texto = r.get('TEXTO') or ''
            sem_info = not _valores_informacao(r.get('INFORMACAO'))
            if sem_info and _LINHA_MEMO_GEFIN.search(texto) and _PLACEHOLDER.search(texto):
                r['INFORMACAO'] = memo
                break

    if nome:
        for i, r in enumerate(novos):
            if i > 0 and (r.get('TEXTO') or '').strip().lower() == 'gerente':
                novos[i - 1]['TEXTO'] = nome.upper()
                novos[i - 1]['INFORMACAO'] = None
                break

    return novos


# =========================================================================
# RESUMO (FIN_VW038)
# =========================================================================
def carregar_resumo(lista_nu):
    """Linhas do resumo dos contratos informados, por contrato e ID."""
    if not lista_nu:
        return {}
    params = {}
    marcadores = []
    for i, nu in enumerate(lista_nu):
        params[f'nu{i}'] = nu
        marcadores.append(f':nu{i}')
    rows = db.session.execute(text(f"""
        SELECT [NU_CONTRATO], [ID], [TEXTO], [INFORMACAO]
        FROM {VW_RESUMO}
        WHERE [NU_CONTRATO] IN ({', '.join(marcadores)})
        ORDER BY [NU_CONTRATO], [ID]
    """), params).fetchall()

    por_contrato = {}
    for r in rows:
        chave = str(int(Decimal(str(r[0]))))
        por_contrato.setdefault(chave, []).append({'ID': r[1], 'TEXTO': r[2], 'INFORMACAO': r[3]})
    return por_contrato


def _valor_resumo(informacao):
    """Valor da coluna 'Valor': número vira moeda; texto entra como veio."""
    valores = _valores_informacao(informacao)
    if not valores:
        return None, None
    valor = valores[0]
    numero = _decimal(valor) if not _MES_ANO.match(valor) and '/' not in valor else None
    if numero is None:
        return valor, None
    return _formatar(valor, moeda=True, percentual=False).replace('\u2011', '-'), numero


_MOEDA_FINAL = re.compile(r'(-?\s*R\$[\s\u00a0]*-?[\d.]+,\d{2})[\s.;]*$')
_CTR = re.compile(r'\(\s*CTR\s*:?\s*\d+\s*\)', re.IGNORECASE)


def _rotulo_limpo(texto, nome=None):
    """
    Tira do rótulo o nome do mutuário e o '(CTR:...)' (já aparecem no
    cabeçalho do bloco) e os separadores que sobram:
    'Fluxo de Caixa Simulado - CID DA CUNHA (CTR:802258000147) - VPL:'
      -> 'Fluxo de Caixa Simulado - VPL'
    """
    t = _CTR.sub('', texto)
    if nome:
        t = re.sub(re.escape(nome.strip()), '', t, flags=re.IGNORECASE)
    t = re.sub(r'\s*-\s*(-\s*)+', ' - ', t)          # '- -' -> '-'
    t = re.sub(r'\s{2,}', ' ', t)
    return t.strip(' -:;\u00a0')


def _cabecalho(linha):
    """Classifica uma linha do cabeçalho do resumo (antes da 1ª seção)."""
    txt = linha.strip()
    if txt.lower().startswith('memorando'):
        return {'rotulo': 'Memorando', 'texto': txt}
    if txt.startswith('À') or txt.lower().startswith('ao '):
        return {'rotulo': 'Destinatário', 'texto': re.sub(r'^À\s*\(ao\)\s*', '', txt)}
    if txt.lower().startswith('assunto:'):
        return {'rotulo': 'Assunto', 'texto': txt.split(':', 1)[1].strip()}
    return {'rotulo': '', 'texto': txt}


def montar_resumo(registros, nome=None):
    """
    Organiza as linhas da view FIN_VW038 em:
      {'cabecalho': [{'rotulo', 'texto', 'pendente'}],
       'secoes':    [{'titulo', 'itens': [{'rotulo', 'valor', 'numero',
                                           'negativo', 'pendente'}]}]}

    - Linhas antes da 1ª seção = cabeçalho (Memorando, Destinatário, Assunto);
      quebras de linha do TEXTO viram linhas separadas.
    - Fragmento sem '...' e sem INFORMACAO = título de seção.
    - Demais = itens: o '...' recebe a INFORMACAO e o valor em R$ que fica no
      fim do texto vai para a coluna Valor; o rótulo perde o nome/contrato.
    - '...' sem valor marca o item/linha como pendente.
    """
    cabecalho = []
    secoes = []

    for r in registros:
        bruto = (r['TEXTO'] or '').replace('\r\n', '\n').replace('\r', '\n')
        if not bruto.strip() or bruto.strip().upper() == 'NULL':
            continue
        valores = _valores_informacao(r['INFORMACAO'])
        tem_placeholder = bool(_PLACEHOLDER.search(bruto))
        texto = _limpar(preencher(bruto, r['INFORMACAO']) if tem_placeholder else bruto)
        texto = texto.replace('\u2011', '-').replace('\u00a0', ' ')

        # Título de seção
        if not tem_placeholder and not valores and len(texto) <= 80 and '\n' not in texto:
            secoes.append({'titulo': texto, 'itens': []})
            continue

        # Cabeçalho (antes da primeira seção)
        if not secoes:
            for linha in texto.split('\n'):
                if linha.strip():
                    item = _cabecalho(linha)
                    item['pendente'] = bool(_PENDENTE.search(linha))
                    cabecalho.append(item)
            continue

        # Item com valor
        numero = None
        if valores:
            v = valores[0]
            if not _MES_ANO.match(v) and '/' not in v:
                numero = _decimal(v)

        valor = None
        rotulo = texto
        m = _MOEDA_FINAL.search(texto)
        if m:
            valor = re.sub(r'\s+', ' ', m.group(1)).replace('R$-', '-R$ ').strip()
            rotulo = texto[:m.start()]
        elif valores:
            valor, numero = _valor_resumo(r['INFORMACAO'])
            valor = valor.replace('\u00a0', ' ') if valor else valor

        secoes[-1]['itens'].append({
            'rotulo': _rotulo_limpo(rotulo, nome) or rotulo.strip(),
            'valor': valor,
            'numero': numero,
            'negativo': numero is not None and numero < 0,
            'pendente': bool(_PENDENTE.search(texto)),
        })

    return {'cabecalho': cabecalho, 'secoes': secoes}


def gerar_excel_resumo(blocos):
    """
    blocos: [{'nu_contrato', 'no_mutuario', 'resumo': montar_resumo()}]
    Aba 'Resumo' com um bloco por contrato e, se houver mais de um, uma aba
    por contrato. Layout de cada bloco:
      faixa azul com mutuário e contrato
      cabeçalho (Memorando / Destinatário / Assunto) em rótulo + texto
      cada seção com faixa clara e itens 'rótulo | valor em R$'
    """
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

    azul = '224ABE'
    cinza_txt = '5A5C69'
    fundo_secao = PatternFill('solid', fgColor='EEF2FF')
    fundo_cab = PatternFill('solid', fgColor='F8F9FC')
    amarelo = PatternFill('solid', fgColor='FFF59D')
    linha_fina = Side(style='thin', color='E3E6F0')
    borda = Border(bottom=linha_fina)
    fmt_moeda = '"R$" #,##0.00;[Red]-"R$" #,##0.00'
    quebra = Alignment(wrap_text=True, vertical='top')

    def _altura(texto, largura=95):
        linhas = sum(max(1, -(-len(p) // largura)) for p in str(texto).split('\n'))
        return max(18, 16 * linhas)

    def _bloco(ws, lin, bloco):
        res = bloco['resumo']

        # Faixa do contrato
        ws.merge_cells(start_row=lin, start_column=1, end_row=lin, end_column=3)
        c = ws.cell(row=lin, column=1, value=f"{bloco['no_mutuario']}  ·  Contrato {bloco['nu_contrato']}")
        c.font = Font(bold=True, color='FFFFFF', size=12)
        c.fill = PatternFill('solid', fgColor=azul)
        c.alignment = Alignment(vertical='center', indent=1)
        ws.row_dimensions[lin].height = 24
        lin += 1

        if not res['cabecalho'] and not res['secoes']:
            ws.cell(row=lin, column=1, value='Resumo ainda não disponível na FIN_VW038.').font = Font(italic=True, color=cinza_txt)
            return lin + 2

        # Cabeçalho do memorando
        for item in res['cabecalho']:
            r1 = ws.cell(row=lin, column=1, value=item['rotulo'])
            r1.font = Font(bold=True, color=cinza_txt)
            r1.fill = fundo_cab
            r1.alignment = Alignment(vertical='top', indent=1)
            ws.merge_cells(start_row=lin, start_column=2, end_row=lin, end_column=3)
            r2 = ws.cell(row=lin, column=2, value=item['texto'])
            r2.alignment = quebra
            r2.fill = amarelo if item['pendente'] else fundo_cab
            ws.row_dimensions[lin].height = _altura(item['texto'], 80)
            lin += 1
        lin += 1

        # Seções
        for secao in res['secoes']:
            ws.merge_cells(start_row=lin, start_column=1, end_row=lin, end_column=3)
            t = ws.cell(row=lin, column=1, value=secao['titulo'])
            t.font = Font(bold=True, color=azul, size=11)
            t.fill = fundo_secao
            t.alignment = Alignment(vertical='center', indent=1)
            ws.row_dimensions[lin].height = 20
            lin += 1
            for item in secao['itens']:
                ws.merge_cells(start_row=lin, start_column=1, end_row=lin, end_column=2)
                a = ws.cell(row=lin, column=1, value=item['rotulo'])
                a.alignment = Alignment(wrap_text=True, vertical='center', indent=2)
                if item['pendente']:
                    a.fill = amarelo
                v = ws.cell(row=lin, column=3)
                if item['numero'] is not None:
                    v.value = float(item['numero'])
                    v.number_format = fmt_moeda
                    v.font = Font(bold=True, color='B91C1C' if item['negativo'] else '047857')
                elif item['valor']:
                    v.value = item['valor']
                    v.font = Font(bold=True)
                v.alignment = Alignment(horizontal='right', vertical='center')
                for col in (1, 2, 3):
                    ws.cell(row=lin, column=col).border = borda
                ws.row_dimensions[lin].height = 20
                lin += 1
            lin += 1
        return lin + 1

    def _preparar(ws, titulo):
        ws.sheet_view.showGridLines = False
        # Impressão: cabe na largura de uma página
        ws.sheet_properties.pageSetUpPr.fitToPage = True
        ws.page_setup.fitToWidth = 1
        ws.page_setup.fitToHeight = 0
        ws.page_setup.paperSize = ws.PAPERSIZE_A4
        ws.column_dimensions['A'].width = 18
        ws.column_dimensions['B'].width = 80
        ws.column_dimensions['C'].width = 22
        ws.merge_cells('A1:C1')
        t = ws.cell(row=1, column=1, value=titulo)
        t.font = Font(bold=True, size=14, color=azul)
        ws.row_dimensions[1].height = 24
        return 3

    wb = Workbook()
    ws = wb.active
    ws.title = 'Resumo'
    lin = _preparar(ws, 'Análise Financeira PF - Resumo')
    for bloco in blocos:
        lin = _bloco(ws, lin, bloco)

    if len(blocos) > 1:
        usados = {'Resumo'}
        for bloco in blocos:
            nome = f"Resumo {bloco['nu_contrato']}"[:31]
            k = 2
            while nome in usados:
                nome = f"Resumo {bloco['nu_contrato']}"[:28] + f'_{k}'
                k += 1
            usados.add(nome)
            wsc = wb.create_sheet(nome)
            _bloco(wsc, _preparar(wsc, f"Resumo - {bloco['no_mutuario']}"), bloco)

    saida = BytesIO()
    wb.save(saida)
    saida.seek(0)
    return saida