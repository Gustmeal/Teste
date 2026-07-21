"""
Templates de texto das Considerações (base fixa, vinda do .docx do relatório).
Cada '{N}' é um ponto local substituído pelo VR da FIN_TB025 cujo ID_VR = N
(por subitem). Subitens sem valores (option b) ficam com texto fixo.
A chave do dicionário é EXATAMENTE o SUBITEM gravado na FIN_TB025.
"""

CONSIDERACOES_TEMPLATES = {
    "1) Carteira de Créditos Comerciais":
        "1) Carteira de Créditos Comerciais (R$ {1} milhão): queda de {2}% "
        "(R$ {3} milhão) em relação ao mesmo mês de 2025 e queda de {4}% "
        "(R$ {5} milhão) frente ao mês anterior.",

    "2) Carteira de Créditos Imobiliários PF":
        "2) Carteira de Créditos Imobiliários PF (R$ {1} milhões): aumento de "
        "{2}% (R$ {3} milhão) em relação ao mesmo mês do ano passado e queda de "
        "{4}% (R$ {5} milhão) frente ao mês anterior. Registra-se o recebimento "
        "de R$ {6} milhão como Levantamento de Depósito Judicial.",

    "3) Carteira de Créditos Imobiliários PJ":
        "3) Carteira de Créditos Imobiliários PJ (R$ {1} milhão): aumento de "
        "{2}% (R$ {3} milhão) em relação ao mesmo mês de 2025 e aumento de {4}% "
        "(R$ {5} milhão) frente ao mês anterior. Registra-se o recebimento de "
        "R$ {6} milhão como Levantamento de Depósito Judicial.",

    # Option (b): texto fixo (sem ponto local)
    "4) Carteira de Créditos do Setor Público:":
        "4) Carteira de Créditos do Setor Público: Não houve realização da "
        "rubrica neste mês.",

    "5) Alienação de Imóveis Não de Uso":
        "5) Alienação de Imóveis Não de Uso (R$ {1} milhão): queda de {2}% "
        "(R$ {3} milhão) em relação ao mesmo mês do ano passado e queda de {4}% "
        "(R$ {5} milhão) frente ao mês anterior. Registra-se o recebimento de "
        "R$ {6} milhão como Levantamento de Depósito Judicial.",

    # idvr 4 é NULL na tabela (sem {4}); "120ª e 124ª" e "maio" ficam fixos
    "6) Novação FCVS/Monetização":
        "6) Novação FCVS/Monetização CVS (R$ {1} milhões): queda de {2}% "
        "(R$ {3} milhões) em relação ao mesmo mês do ano passado. O valor de "
        "maio corresponde ao recebimento das 120ª e 124ª Novações de Dívidas. "
        "Comparado ao mês anterior, registra-se aumento de {5}% (R$ {6} milhão).",

    # ATENÇÃO à ordem: {5} (%) vem ANTES de {4} (R$) — no .docx é "21,6% ou R$ 6,00"
    "7) Receitas Financeiras Líquidas":
        "7) Receitas Financeiras Líquidas (R$ {1} milhões): aumento de {2}% "
        "(R$ {3} milhões) frente ao mesmo mês do ano anterior, em razão do maior "
        "volume das Receitas dos Fundos de Investimentos ({5}% ou R$ {4} "
        "milhões). Comparado a abril de 2026 as Receitas Financeiras apresentam "
        "queda de {6}% (R$ {7} milhão). Registra-se o recebimento de R$ {8} "
        "milhões com Títulos CVS - Juros e Amortização.",

    # Option (b): texto fixo
    "8) Crédito Tributário":
        "8) Crédito Tributário: Não houve realização da rubrica neste mês.",

    "9) Outros Ingressos":
        "9) Outros Ingressos (R$ {1} milhão): queda de {2}% (R$ {3} milhão) em "
        "relação ao mesmo mês de 2025. Registra-se aumento de {4}% (R$ {5} "
        "milhão) frente ao mês anterior.",

# ---- SAÍDAS ----
    "1) Serviço da Dívida (FGTS e outros):":
        "1) Serviço da Dívida (FGTS e outros): Desde a Liquidação do contrato "
        "450.169 junto ao FGTS, em setembro de 2025, esta rubrica não registra "
        "realização.",

    # OBS: no ITEM=SAIDAS há duas linhas com ID_VR=2 (33.43 e 11.62).
    # Aqui {2}=33,43% (aumento) e {3}=11,62 (R$). Ver nota no fim.
    "2) Tributos/Encargos":
        "2) Tributos/Encargos (R$ {1} milhões): aumento de {2}% (R$ {3} "
        "milhões) em relação ao mesmo mês do ano passado, principalmente em "
        "razão do maior recolhimento de IRRF sobre aplicações financeiras "
        "(R$ {3} milhões), tendo em vista apropriação no mês de maio de IRRF "
        "incidente sobre a base de rendimentos acumulados (come-cotas nos meses "
        "de maio e novembro de cada ano). Comparado ao mês anterior, registra-se "
        "aumento de {4}% (R$ {5} milhões).",

    "3) Serviços de Terceiros - Operacionais":
        "3) Serviços de Terceiros - Operacionais (R$ {1} milhão): queda de {2}% "
        "(R$ {3} milhão) frente ao mesmo mês de 2025 e queda de {4}% (R$ {5} "
        "milhão) frente ao mês anterior.",

    "4) Serviços de Terceiros - Demais":
        "4) Serviços de Terceiros - Demais (R$ {1} milhões): queda de {2}% "
        "(R$ {3} milhão) frente ao mesmo período no ano anterior. Comparado ao "
        "mês passado, registra-se aumento de {4}% (R$ {5} milhão).",

    # idvr 5 é "não houve variação" -> texto fixo no fim, sem {5}
    "5) Prêmio de Seguros/FCVS":
        "5) Prêmio de Seguros/FCVS (R$ {1} milhão): queda de {2}% (R$ {3} "
        "milhão) frente ao mesmo período no ano anterior. Comparado ao mês "
        "passado, não houve variação significativa.",

    "6) Outros Dispêndios Correntes - Operacionais":
        "6) Outros Dispêndios Correntes - Operacionais (R$ {1} milhões): aumento "
        "de {2}% (R$ {3} milhão) frente ao mesmo período no ano anterior, "
        "principalmente em razão do maior desembolso com Despesas Judiciais - "
        "Condenações, em {4}% (R$ {5} milhão). Comparado ao mês passado, houve "
        "aumento de {6}% (R$ {7} milhão).",

    "7) Despesas Administrativas e de Pessoal":
        "7) Despesas Administrativas e de Pessoal (R$ {1} milhões): queda de "
        "{2}% (R$ {3} milhão) frente ao mesmo mês do ano anterior, "
        "principalmente em razão do menor desembolso com Pessoal, Encargos e "
        "Benefícios, em {4}% (R$ {5} milhão). Comparado ao mês passado, houve "
        "aumento de {6}% (R$ {7} milhão).",

    # Option (b): texto fixo
    "8) Investimentos":
        "8) Investimentos: Não houve realização da rubrica neste mês.",

    "9) Dividendos/JCP/PLR/RVA":
        "9) Dividendos/JCP/PLR/RVA (R$ {1} milhões): aumento de R$ {2} milhões "
        "frente ao mesmo mês do ano anterior, devido ao pagamento de PLR "
        "(exercício 2025) em maio/26.",

    "10) Outras Saídas":
        "10) Outras Saídas (R$ {1} milhão): aumento de {2}%, representando uma "
        "diferença de R$ {3} milhões em relação ao mesmo mês do ano anterior. "
        "Queda de {4}% no valor de R$ {5} milhões frente ao mês anterior.",
}