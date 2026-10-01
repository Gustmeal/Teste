/**
 * Sinal de presença do Portal GEINC.
 * Envia a cada X segundos: rota, título, se a aba está visível e há quanto tempo não há interação.
 * Mouse/teclado NÃO geram requisição: só atualizam uma variável local.
 */
(function () {
    'use strict';

    var script = document.currentScript;
    if (!script || !window.fetch) { return; }

    var URL_SINAL = script.getAttribute('data-url-sinal');
    var URL_SAIDA = script.getAttribute('data-url-saida');
    var INTERVALO_MS = (parseInt(script.getAttribute('data-intervalo'), 10) || 60) * 1000;
    var REATIVACAO_MS = 60000;      // parado há mais de 1 min e voltou -> avisa na hora
    var ENVIO_MINIMO_MS = 15000;    // nunca envia mais de 1 sinal extra a cada 15s

    function gerarId() {
        if (window.crypto && typeof window.crypto.randomUUID === 'function') {
            return window.crypto.randomUUID();
        }
        return 'aba-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 12);
    }

    function obterIdAba() {
        var chave = 'geinc_presenca_id_aba';
        try {
            var id = window.sessionStorage.getItem(chave);
            if (!id) {
                id = gerarId();
                window.sessionStorage.setItem(chave, id);
            }
            return id;
        } catch (e) {
            if (!window.__geincIdAba) { window.__geincIdAba = gerarId(); }
            return window.__geincIdAba;
        }
    }

    var ID_ABA = obterIdAba();
    var ultimaInteracao = Date.now();
    var ultimoEnvio = 0;

    function segundosSemInteracao() {
        return Math.max(0, Math.round((Date.now() - ultimaInteracao) / 1000));
    }

    function enviarSinal() {
        ultimoEnvio = Date.now();
        fetch(URL_SINAL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-Silent-Request': 'true' },
            credentials: 'same-origin',
            keepalive: true,
            body: JSON.stringify({
                id_aba: ID_ABA,
                rota: window.location.pathname + window.location.search,
                titulo: document.title || '',
                visivel: document.visibilityState === 'visible',
                seg_sem_interacao: segundosSemInteracao()
            })
        }).catch(function () { /* silencioso: servidor pode estar reiniciando no deploy */ });
    }

    function registrarInteracao() {
        var agora = Date.now();
        var estavaParado = (agora - ultimaInteracao) > REATIVACAO_MS;
        ultimaInteracao = agora;
        if (estavaParado && (agora - ultimoEnvio) > ENVIO_MINIMO_MS) {
            enviarSinal();
        }
    }

    ['mousemove', 'mousedown', 'keydown', 'scroll', 'wheel', 'touchstart'].forEach(function (evento) {
        document.addEventListener(evento, registrarInteracao, { passive: true, capture: true });
    });

    document.addEventListener('visibilitychange', function () {
        if (document.visibilityState === 'visible') { ultimaInteracao = Date.now(); }
        enviarSinal();
    });

    window.addEventListener('pagehide', function () {
        try {
            var corpo = new Blob([JSON.stringify({ id_aba: ID_ABA })], { type: 'text/plain' });
            navigator.sendBeacon(URL_SAIDA, corpo);
        } catch (e) { /* ignora */ }
    });

    window.addEventListener('pageshow', function (evento) {
        if (evento.persisted) { ultimaInteracao = Date.now(); enviarSinal(); }
    });

    enviarSinal();
    setInterval(enviarSinal, INTERVALO_MS);
})();