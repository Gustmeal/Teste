// Animações e interações dinâmicas
document.addEventListener('DOMContentLoaded', function() {
    // Adicionar classe fade-in ao conteúdo principal
    const mainContent = document.querySelector('.main-content');
    if (mainContent) {
        mainContent.classList.add('fade-in');
    }

    // Efeito de entrada para cards
    const cards = document.querySelectorAll('.card');
    cards.forEach((card, index) => {
        setTimeout(() => {
            card.classList.add('slide-in');
        }, 100 * index);
    });

    // Adicionar efeito ripple a todos os botões
    const buttons = document.querySelectorAll('.btn');
    buttons.forEach(button => {
        button.classList.add('ripple');
    });

    // Destacar linha da tabela quando clicada
    const tableRows = document.querySelectorAll('tbody tr');
    tableRows.forEach(row => {
        row.addEventListener('click', function(e) {
            // Não adicionar a classe se clicar em um botão ou link
            if (e.target.closest('a, button')) return;

            // Remover a classe de todas as linhas
            tableRows.forEach(r => r.classList.remove('row-selected'));
            // Adicionar a classe à linha clicada
            this.classList.add('row-selected');
        });
    });

    // Adicionar animação para mensagens flash
    const alerts = document.querySelectorAll('.alert');
    alerts.forEach(alert => {
        // Remover automaticamente alertas após 5 segundos
        setTimeout(() => {
            if (alert) {
                fadeOut(alert);
            }
        }, 5000);
    });

    // Animação para formulários
    const forms = document.querySelectorAll('form:not([data-no-loading])');
    forms.forEach(form => {
        form.addEventListener('submit', function() {
            const submitButton = this.querySelector('button[type="submit"]');
            if (submitButton) {
                const btnText = submitButton.querySelector('.btn-text');
                const spinner = submitButton.querySelector('.spinner');

                if (btnText && spinner) {
                    btnText.classList.add('d-none');
                    spinner.classList.remove('d-none');
                } else {
                    submitButton.innerHTML = '<span class="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span> Processando...';
                }

                submitButton.disabled = true;
            }
            // Não retorna false para que o form seja enviado normalmente
        });
    });

    // Adicionar gradiente aos botões principais
    const mainButtons = document.querySelectorAll('.btn-success, .btn-primary, .btn-access');
    mainButtons.forEach(button => {
        button.classList.add('btn-gradient');
    });

    // Adicionar efeito skeleton loader para tabelas que estão carregando
    simulateTableLoading();

    // Iniciar animação para contadores
    animateCounters();
});

// Função para simular carregamento da tabela
function simulateTableLoading() {
    const tables = document.querySelectorAll('.table-responsive[data-loading="true"]');
    tables.forEach(tableContainer => {
        // Esconder a tabela temporariamente
        const table = tableContainer.querySelector('table');
        if (table) {
            table.style.display = 'none';

            // Criar e adicionar esqueletos de carregamento
            const loadingContainer = document.createElement('div');
            loadingContainer.className = 'p-3';

            // Adicionar algumas linhas de esqueleto
            for (let i = 0; i < 5; i++) {
                const row = document.createElement('div');
                row.className = 'skeleton-loader mb-3';
                loadingContainer.appendChild(row);
            }

            tableContainer.appendChild(loadingContainer);

            // Remover o esqueleto e mostrar a tabela após 1.5 segundos
            setTimeout(() => {
                loadingContainer.remove();
                table.style.display = '';

                // Animar linhas da tabela
                const rows = table.querySelectorAll('tbody tr');
                rows.forEach((row, index) => {
                    row.style.opacity = '0';
                    row.style.transform = 'translateY(20px)';

                    setTimeout(() => {
                        row.style.transition = 'all 0.3s ease';
                        row.style.opacity = '1';
                        row.style.transform = 'translateY(0)';
                    }, 100 * index);
                });
            }, 1500);
        }
    });
}

// Função para fade out com animação
function fadeOut(element) {
    element.style.opacity = '1';

    (function fade() {
        if ((element.style.opacity -= 0.1) < 0) {
            element.style.display = 'none';
            element.classList.add('d-none');
        } else {
            requestAnimationFrame(fade);
        }
    })();
}

// Função para mostrar um toast de notificação
function showToast(message, type = 'success') {
    // Criar elemento toast
    const toast = document.createElement('div');
    toast.className = `toast align-items-center text-white bg-${type} border-0 position-fixed bottom-0 end-0 m-3`;
    toast.setAttribute('role', 'alert');
    toast.setAttribute('aria-live', 'assertive');
    toast.setAttribute('aria-atomic', 'true');

    // Conteúdo do toast
    toast.innerHTML = `
        <div class="d-flex">
            <div class="toast-body">
                ${message}
            </div>
            <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button>
        </div>
    `;

    // Adicionar ao corpo do documento
    document.body.appendChild(toast);

    // Inicializar o toast
    const bsToast = new bootstrap.Toast(toast, {
        animation: true,
        autohide: true,
        delay: 3000
    });

    // Mostrar o toast
    bsToast.show();

    // Remover quando escondido
    toast.addEventListener('hidden.bs.toast', function() {
        document.body.removeChild(toast);
    });
}

// Função para animar contadores
function animateCounters() {
    const counters = document.querySelectorAll('.counter');

    counters.forEach(counter => {
        const target = parseInt(counter.getAttribute('data-target'));
        const duration = 1500; // ms
        const steps = 50;
        const stepValue = target / steps;
        const stepTime = duration / steps;
        let current = 0;

        const updateCounter = () => {
            current += stepValue;
            if (current > target) current = target;
            counter.textContent = Math.floor(current);

            if (current < target) {
                setTimeout(updateCounter, stepTime);
            }
        };

        // Verificar se o contador está visível antes de animar
        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    setTimeout(updateCounter, 300);
                    observer.unobserve(entry.target);
                }
            });
        });

        observer.observe(counter);
    });
}

// Expor funções globalmente
window.showToast = showToast;
window.fadeOut = fadeOut;
window.animateCounters = animateCounters;