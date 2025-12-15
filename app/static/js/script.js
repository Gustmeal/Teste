// Função de filtro para tabelas
function setupTableFilter(inputId, tableId) {
    const searchInput = document.getElementById(inputId);
    const table = document.getElementById(tableId);

    if (!searchInput || !table) return;

    searchInput.addEventListener('keyup', function() {
        const searchTerm = searchInput.value.toLowerCase().trim();
        const rows = table.getElementsByTagName('tbody')[0].getElementsByTagName('tr');

        for (let i = 0; i < rows.length; i++) {
            if (rows[i].classList.contains('no-result-row')) continue;

            const rowText = rows[i].textContent.toLowerCase();
            if (rowText.indexOf(searchTerm) > -1) {
                rows[i].style.display = '';
                rows[i].classList.add('filtered-in');
                rows[i].classList.remove('filtered-out');
            } else {
                rows[i].style.display = 'none';
                rows[i].classList.add('filtered-out');
                rows[i].classList.remove('filtered-in');
            }
        }

        // Verificar se há resultados
        checkNoResults(table, rows);
    });
}

// Verifica se há resultados na tabela após filtragem
function checkNoResults(table, rows) {
    const tbody = table.getElementsByTagName('tbody')[0];
    let visibleRows = 0;

    for (let i = 0; i < rows.length; i++) {
        if (rows[i].style.display !== 'none' && !rows[i].classList.contains('no-result-row')) {
            visibleRows++;
        }
    }

    // Remover mensagem existente se houver
    const existingMessage = tbody.querySelector('.no-result-row');
    if (existingMessage) {
        tbody.removeChild(existingMessage);
    }

    // Adicionar mensagem se não houver resultados
    if (visibleRows === 0) {
        const noResultRow = document.createElement('tr');
        noResultRow.className = 'no-result-row';

        const cell = document.createElement('td');
        cell.colSpan = table.rows[0].cells.length;
        cell.className = 'text-center py-4 text-muted';
        cell.innerHTML = '<i class="fas fa-search fa-2x mb-3"></i><p>Nenhum resultado encontrado</p>';

        noResultRow.appendChild(cell);
        tbody.appendChild(noResultRow);
    }
}

// Funções para manipulação do tema
function setTheme(themeName) {
    localStorage.setItem('theme', themeName);
    document.documentElement.setAttribute('data-theme', themeName);
    updateThemeIcon(themeName);
}

function toggleTheme() {
    if (localStorage.getItem('theme') === 'dark') {
        setTheme('light');
    } else {
        setTheme('dark');
    }
}

function updateThemeIcon(themeName) {
    const themeIcon = document.getElementById('themeIcon');
    if (themeIcon) {
        if (themeName === 'dark') {
            themeIcon.className = 'fas fa-sun';
        } else {
            themeIcon.className = 'fas fa-moon';
        }
    }
}

// Inicializar tema baseado na preferência armazenada
function initTheme() {
    if (localStorage.getItem('theme') === 'dark') {
        setTheme('dark');
    } else {
        setTheme('light');
    }
}

// Inicializar os filtros quando o DOM estiver carregado
document.addEventListener('DOMContentLoaded', function() {
    setupTableFilter('editaisSearch', 'editaisTable');
    setupTableFilter('periodosSearch', 'periodosTable');

    // Inicializa o tema
    initTheme();

    // Configura o botão de alternar tema
    const themeToggle = document.getElementById('themeToggle');
    if (themeToggle) {
        themeToggle.addEventListener('click', toggleTheme);
    }
});
// Inicializar tooltips do Bootstrap
document.addEventListener('DOMContentLoaded', function() {
  // Procura por todos os elementos com atributo data-bs-toggle="tooltip"
  var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));

  // Inicializa os tooltips do Bootstrap
  var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
    return new bootstrap.Tooltip(tooltipTriggerEl);
  });

  // Obter data atual formatada para impressão
  var now = new Date();
  var formattedDate = now.toLocaleDateString('pt-BR') + ' ' + now.toLocaleTimeString('pt-BR');

  // Adicionar ao body para uso no CSS de impressão
  document.body.setAttribute('data-print-date', formattedDate);

  // Destacar linhas na tabela quando forem referenciadas por URL
  if (window.location.hash) {
    const id = window.location.hash.substring(1);
    const row = document.getElementById(id);

    if (row) {
      row.classList.add('highlight-row');

      // Rolar até a linha destacada
      setTimeout(function() {
        row.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }, 500);

      // Remover destaque após alguns segundos
      setTimeout(function() {
        row.classList.remove('highlight-row');
      }, 3000);
    }
  }

  // Adicionar feedback ao clicar em botões
  const actionButtons = document.querySelectorAll('.btn:not([data-bs-toggle="modal"])');
  actionButtons.forEach(button => {
    button.addEventListener('click', function() {
      // Não adicionar efeito para botões que abrem modais ou são de cancelamento
      if (!this.classList.contains('btn-secondary')) {
        const originalText = this.innerHTML;
        const isIcon = this.querySelector('i');

        if (isIcon && this.textContent.trim() === '') {
          // Botão só com ícone
          const originalIcon = this.innerHTML;
          this.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';

          // Restaurar ícone original após um tempo
          setTimeout(() => {
            this.innerHTML = originalIcon;
          }, 500);
        } else if (!this.closest('form')) {
          // Botão com texto (não em formulário)
          this.disabled = true;
          const width = this.offsetWidth;
          this.style.width = width + 'px';
          this.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i> Processando...';

          // Restaurar texto original após um tempo
          setTimeout(() => {
            this.innerHTML = originalText;
            this.disabled = false;
            this.style.width = '';
          }, 500);
        }
      }
    });
  });
});
// ============================================
// CALCULADORA GLOBAL
// ============================================

let calculatorCurrentValue = '0';
let calculatorExpression = '';

// Abre o modal da calculadora
document.addEventListener('DOMContentLoaded', function() {
    const calculatorToggle = document.getElementById('calculatorToggle');
    if (calculatorToggle) {
        calculatorToggle.addEventListener('click', function() {
            const calculatorModal = new bootstrap.Modal(document.getElementById('calculatorModal'));
            calculatorModal.show();
        });
    }
});

// Adiciona número ou operador
function calculatorAppend(value) {
    const resultElement = document.getElementById('calcResult');
    const expressionElement = document.getElementById('calcExpression');

    // Se for o primeiro dígito ou após calcular
    if (calculatorCurrentValue === '0' && value !== '.') {
        calculatorCurrentValue = value;
    } else if (calculatorCurrentValue === '0' && value === '.') {
        calculatorCurrentValue = '0.';
    } else {
        calculatorCurrentValue += value;
    }

    resultElement.textContent = calculatorCurrentValue;
}

// Limpa tudo
function calculatorClear() {
    calculatorCurrentValue = '0';
    calculatorExpression = '';
    document.getElementById('calcResult').textContent = '0';
    document.getElementById('calcExpression').textContent = '';
}

// Deleta último caractere
function calculatorDelete() {
    const resultElement = document.getElementById('calcResult');

    if (calculatorCurrentValue.length > 1) {
        calculatorCurrentValue = calculatorCurrentValue.slice(0, -1);
    } else {
        calculatorCurrentValue = '0';
    }

    resultElement.textContent = calculatorCurrentValue;
}

// Calcula o resultado
function calculatorCalculate() {
    const resultElement = document.getElementById('calcResult');
    const expressionElement = document.getElementById('calcExpression');

    try {
        // Substitui × e ÷ pelos operadores corretos
        let expression = calculatorCurrentValue
            .replace(/×/g, '*')
            .replace(/÷/g, '/')
            .replace(/−/g, '-');

        // Salva a expressão para exibir
        calculatorExpression = calculatorCurrentValue;
        expressionElement.textContent = calculatorExpression + ' =';

        // Calcula o resultado
        let result = eval(expression);

        // Formata o resultado (máximo 10 casas decimais)
        if (result % 1 !== 0) {
            result = parseFloat(result.toFixed(10));
        }

        calculatorCurrentValue = result.toString();
        resultElement.textContent = calculatorCurrentValue;

    } catch (error) {
        resultElement.textContent = 'Erro';
        calculatorCurrentValue = '0';
        setTimeout(() => {
            resultElement.textContent = '0';
        }, 1500);
    }
}

// Suporte para teclado
document.addEventListener('keydown', function(event) {
    const calculatorModal = document.getElementById('calculatorModal');

    // Só funciona se o modal estiver aberto
    if (!calculatorModal.classList.contains('show')) return;

    const key = event.key;

    // Números e operadores
    if (/[0-9+\-*/.%]/.test(key)) {
        event.preventDefault();
        calculatorAppend(key);
    }
    // Enter = calcular
    else if (key === 'Enter') {
        event.preventDefault();
        calculatorCalculate();
    }
    // Backspace = deletar
    else if (key === 'Backspace') {
        event.preventDefault();
        calculatorDelete();
    }
    // Escape = limpar
    else if (key === 'Escape') {
        event.preventDefault();
        calculatorClear();
    }
});