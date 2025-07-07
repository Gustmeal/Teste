/**
 * Sistema automático de detecção de operações longas para exibição de tela de carregamento
 * Mostra a tela de carregamento apenas se uma operação demorar mais de 2.1 segundos
 */
(function() {
    // Estado do sistema
    let operationCounter = 0;
    let isLoadingVisible = false;
    let loadingTimeout = null;
    let loadingElement = null;
    let isNavigatingAway = false;

    // Criar overlay de carregamento
    function createLoadingOverlay() {
        if (document.getElementById('overlayLoading')) {
            return document.getElementById('overlayLoading');
        }

        const overlay = document.createElement('div');
        overlay.id = 'overlayLoading';
        overlay.className = 'overlay-loading';

        const content = document.createElement('div');
        content.className = 'loading-content';

        const spinner = document.createElement('div');
        spinner.className = 'loading-spinner';

        const text = document.createElement('div');
        text.className = 'loading-text';
        text.textContent = 'Processando, por favor aguarde...';

        content.appendChild(spinner);
        content.appendChild(text);
        overlay.appendChild(content);
        document.body.appendChild(overlay);

        return overlay;
    }

    // Mostrar overlay de carregamento
    function showLoading() {
        if (isLoadingVisible) return;

        if (!loadingElement) {
            loadingElement = createLoadingOverlay();
        }

        isLoadingVisible = true;
        loadingElement.classList.add('active');
    }

    // Esconder overlay de carregamento
    function hideLoading() {
        if (!isLoadingVisible) return;

        isLoadingVisible = false;
        if (loadingElement) {
            loadingElement.classList.remove('active');
        }

        if (loadingTimeout) {
            clearTimeout(loadingTimeout);
            loadingTimeout = null;
        }
    }

    // Resetar estado completamente
    function resetLoadingState() {
        operationCounter = 0;
        isNavigatingAway = false;
        hideLoading();
    }

    // Registrar início de operação
    function operationStarted() {
        operationCounter++;

        // Limpar timeout anterior se existir
        if (loadingTimeout) {
            clearTimeout(loadingTimeout);
        }

        // Definir novo timeout para mostrar loading após 1 segundo
        loadingTimeout = setTimeout(() => {
            if (operationCounter > 0 && !isNavigatingAway) {
                showLoading();
            }
        }, 1000);
    }

    // Registrar fim de operação
    function operationEnded() {
        operationCounter = Math.max(0, operationCounter - 1);

        if (operationCounter === 0) {
            hideLoading();
        }
    }

    // Interceptar todos os cliques
    document.addEventListener('click', (event) => {
        // Verificar se o clique foi em um botão, link ou elemento clicável
        const element = event.target.closest('button, a, [role="button"], [type="submit"]');

        if (element) {
            // Ignorar elementos que claramente não iniciam operações longas
            if (element.classList.contains('btn-close') ||
                element.getAttribute('data-bs-dismiss') ||
                element.classList.contains('btn-secondary') ||
                element.classList.contains('dropdown-toggle') ||
                element.getAttribute('data-bs-toggle') === 'dropdown' ||
                element.getAttribute('data-bs-toggle') === 'modal' ||
                element.getAttribute('data-bs-toggle') === 'tab' ||
                element.getAttribute('data-bs-toggle') === 'tooltip' ||
                element.classList.contains('page-link') && element.closest('.pagination')) {
                return;
            }

            // Verificar se há um href que não é # ou javascript:void(0)
            const href = element.getAttribute('href');
            if (href && href !== '#' && !href.startsWith('javascript:')) {
                operationStarted();
            }

            // Se for botão de formulário
            if (element.getAttribute('type') === 'submit') {
                operationStarted();
            }
        }
    });

    // Interceptar submissões de formulário
    document.addEventListener('submit', () => {
        operationStarted();
    });

    // Interceptar mudanças de página
    window.addEventListener('beforeunload', () => {
        isNavigatingAway = true;
    });

    // Detectar quando a página é mostrada (incluindo do cache)
    window.addEventListener('pageshow', (event) => {
        // Se a página foi carregada do cache do navegador
        if (event.persisted) {
            resetLoadingState();
        }
    });

    // Detectar mudanças de visibilidade da página
    document.addEventListener('visibilitychange', () => {
        if (!document.hidden) {
            // Quando a página volta a ficar visível, resetar se necessário
            if (isNavigatingAway) {
                resetLoadingState();
            }
        }
    });

    // Interceptar Fetch API
    const originalFetch = window.fetch;
    window.fetch = function(...args) {
        operationStarted();

        return originalFetch.apply(this, args)
            .then(response => {
                operationEnded();
                return response;
            })
            .catch(error => {
                operationEnded();
                throw error;
            });
    };

    // Interceptar XMLHttpRequest
    const originalOpen = XMLHttpRequest.prototype.open;
    XMLHttpRequest.prototype.open = function() {
        const xhr = this;
        xhr.addEventListener('loadstart', () => {
            operationStarted();
        });

        xhr.addEventListener('loadend', () => {
            operationEnded();
        });

        originalOpen.apply(xhr, arguments);
    };

    // Esconder loading quando a página terminar de carregar
    window.addEventListener('load', () => {
        resetLoadingState();
    });

    // Expor funções globalmente se necessário
    window.autoLoadingManager = {
        operationStarted,
        operationEnded,
        showLoading,
        hideLoading,
        resetLoadingState
    };
})();