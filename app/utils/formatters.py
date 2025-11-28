def format_currency(value, decimal_places=2, currency_symbol="R$ "):
    """
    Formata um valor como moeda com o padrão brasileiro (ponto para milhar, vírgula para decimal)
    Suporta valores negativos corretamente
    Ex: R$ 1.234,56 ou R$ -1.234,56
    """
    if value is None:
        return "-"

    # Converter para float
    value = float(value)

    # Detectar se é negativo
    is_negative = value < 0

    # Trabalhar com valor absoluto
    value = abs(value)

    # Separar parte inteira e decimal
    int_part = int(value)
    decimal_part = round((value - int_part) * (10 ** decimal_places))

    # Formatar parte inteira com separador de milhar
    formatted_int = ""
    int_str = str(int_part)

    # Adicionar pontos como separadores de milhar
    for i, digit in enumerate(reversed(int_str)):
        if i > 0 and i % 3 == 0:
            formatted_int = "." + formatted_int
        formatted_int = digit + formatted_int

    # Formatar parte decimal
    decimal_str = str(decimal_part).zfill(decimal_places)

    # Retornar valor formatado com sinal negativo se necessário
    if is_negative:
        return f"{currency_symbol}-{formatted_int},{decimal_str}"
    else:
        return f"{currency_symbol}{formatted_int},{decimal_str}"

def format_number(value):
    """
    Formata números inteiros com separador de milhar
    Ex: 1.234.567
    """
    if value is None:
        return "-"

    # Formatar inteiro com separador de milhar
    int_value = int(value)
    formatted = ""
    value_str = str(int_value)

    for i, digit in enumerate(reversed(value_str)):
        if i > 0 and i % 3 == 0:
            formatted = "." + formatted
        formatted = digit + formatted

    return formatted