/**
 * Sistema automático de detecção de operações longas para exibição de tela de carregamento
 * Mostra a tela de carregamento apenas se uma operação demorar mais de 1 segundo
 */
(function() {
    // Estado do sistema
    let operationCounter = 0;
    let isLoadingVisible = false;
    let loadingTimeout = null;
    let loadingElement = null;

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
        if (!isLoadingVisible && operationCounter === 0) return;

        isLoadingVisible = false;
        if (loadingElement) {
            loadingElement.classList.remove('active');
        }

        if (loadingTimeout) {
            clearTimeout(loadingTimeout);
            loadingTimeout = null;
        }
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
            if (operationCounter > 0) {
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
        const element = event.target.closest('button, a, [role="button"], [type="submit"]');

        if (element) {
            if (element.classList.contains('js-ignore-loading') ||
                element.classList.contains('btn-close') ||
                element.getAttribute('data-bs-dismiss') ||
                element.classList.contains('btn-secondary') ||
                element.classList.contains('dropdown-toggle') ||
                element.getAttribute('data-bs-toggle') === 'dropdown' ||
                element.getAttribute('data-bs-toggle') === 'modal' ||
                element.getAttribute('data-bs-toggle') === 'tab' ||
                element.getAttribute('data-bs-toggle') === 'tooltip' ||
                element.classList.contains('page-link') && element.closest('.pagination') ||
                element.hasAttribute('download')) {
                return;
            }

            const href = element.getAttribute('href');
            if (href && href !== '#' && !href.startsWith('javascript:')) {
                operationStarted();
            }

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
        operationStarted();
    });

    // Interceptar Fetch API
    const originalFetch = window.fetch;
    window.fetch = function(...args) {
        // Verificar o header X-Silent-Request sem consumir o body
        // Checamos diretamente no objeto de opções (segundo argumento)
        var options = args[1] || {};
        var headers = options.headers || {};
        var isSilent = false;

        if (headers instanceof Headers) {
            isSilent = headers.has('X-Silent-Request');
        } else if (typeof headers === 'object') {
            // Verificar no objeto plain
            isSilent = ('X-Silent-Request' in headers);
        }

        if (isSilent) {
            // Se for silenciosa, executa o fetch original diretamente
            // SEM criar new Request() para não consumir o body (FormData)
            return originalFetch.apply(this, args);
        }

        // Para todas as outras requisições, o comportamento normal continua
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

    // Lida com o ciclo de vida da página para corrigir o bug do botão "voltar"
    window.addEventListener('pageshow', (event) => {
        if (event.persisted) {
            operationCounter = 0;
            hideLoading();
        }
    });

    // Esconder loading quando a página terminar de carregar pela primeira vez
    window.addEventListener('load', () => {
        operationEnded();
    });

    // Expor funções globalmente se necessário
    window.autoLoadingManager = {
        operationStarted,
        operationEnded,
        showLoading,
        hideLoading
    };
})();