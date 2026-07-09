"""
Estrutura (template) das páginas do Relatório de Gestão Financeira.

Cada item tem um 'tipo':
  - 'texto'   : parágrafo. Os valores contados são substituídos pela FIN_TB023
                e o mês de referência entra via placeholders {MES_REF} /
                {MES_REF_CAP} / {ANO_REF}.
  - 'grafico' : espaço reservado (faremos depois).
  - 'tabela'  : espaço reservado (faremos depois).

A ORDEM da lista É a ordem da contagem global dos valores.
"""

SUMARIO_EXECUTIVO = [
    {'tipo': 'texto', 'texto':
        'Em {MES_REF} o saldo das disponibilidades alcançou R$ 3.158,94 milhões, '
        'representando aumento de 21,8% (R$ 565,49 milhões) em relação ao mesmo '
        'mês do ano anterior e aumento de 0,5% (R$ 14,92 milhões) frente a abril '
        'de 2026.'},

    {'tipo': 'texto', 'texto':
        'Os Ingressos alcançaram R$ 71,69 milhões, apresentando queda de 75,1% '
        '(-R$ 216,45 milhões) frente ao mesmo mês do ano anterior, principalmente '
        'em razão do menor recebimento de Novações de Dívidas (-R$ 223,06 milhões). '
        'Em relação a abril de 2026, os ingressos registraram aumento de 1,3% '
        '(R$ 0,91 milhão). Em termos de composição, os destaques foram as Receitas '
        'Financeiras Líquidas representando 51,1% dos ingressos (R$ 36,62 milhões), '
        'as Novações FCVS com 38,1% (R$ 27,32 milhões) e as Carteiras de Créditos '
        'Operacionais com 10,3% (R$ 7,39 milhões).'},

    {'tipo': 'grafico', 'titulo': 'Ingressos (Fonte: Sistema RM Fluxus)'},

    {'tipo': 'texto', 'texto':
        'Já as Saídas alcançaram R$ 56,78 milhões, apresentando queda de 62,7% '
        '(-R$ 95,24 milhões) em relação à {MES_REF} do ano passado, devido ao fim '
        'do passivo junto ao FGTS, liquidado em set/2025. Em relação ao mês '
        'anterior, as saídas registraram queda de 78,1% (-R$ 202,84 milhões), '
        'devido ao pagamento de Dividendos/JCP realizado em abril. Quanto a '
        'composição das Saídas, os destaques foram: 81,7% (R$ 46,40 milhões) com '
        'Tributos/Encargos; 5,6% (R$ 3,19 milhões) com PLR; 5,1% (R$ 2,89 milhões) '
        'com Despesas Administrativas e de Pessoal; 5,1% (R$ 2,88 milhões) com '
        'Serviços de Terceiros; 3,9% (R$ 2,89 milhões) com Outros Dispêndios '
        'Correntes (Operacionais) e 0,3% (R$ 0,18 milhão) com Prêmios e Seguros.'},

    {'tipo': 'grafico', 'titulo': 'Saídas (Fonte: Sistema RM Fluxus)'},

    {'tipo': 'texto', 'texto':
        'Quanto a aplicação das Disponibilidades, segue o quadro com a '
        'Rentabilidade Acumulada dos Fundos de Investimentos em 2026:'},

    {'tipo': 'tabela', 'titulo': 'Rentabilidade Acumulada dos Fundos de Investimentos'},

    {'tipo': 'texto', 'texto':
        'No mês, a rentabilidade do Fundo BB Exclusivo Extramercado Emgea foi de '
        '1,07%, registrando uma performance anual de 99,54% do IRF-M 1 (benchmark) '
        'e 95,51% da TMS.'},

    {'tipo': 'texto', 'texto':
        'Com base nas projeções de fluxo de caixa de curto e médio prazos, no '
        'tocante ao risco de liquidez, a Emgea permanecerá no Cenário de '
        'Normalidade, levando em conta a disponibilidade de recursos financeiros '
        'suficientes para o cumprimento integral das obrigações previstas até '
        'dezembro de 2027.'},
]