/*
 * app/static/js/filtro-tabela.js
 *
 * Filtros de tabela usados nas vinculações do FCO.
 * - Filtro de situação (vinculadas / sem vínculo) pelo atributo data-situacao da linha.
 * - Busca geral em todas as colunas.
 * - Um filtro por coluna, na linha de filtros do cabeçalho (tr.cc-filtros-coluna):
 *     <input  data-coluna="0" data-modo="texto">  -> contém o texto digitado
 *     <select data-coluna="2" data-modo="exato">  -> valor igual (opções montadas
 *                                                   com os valores que estão na tabela)
 *   O valor de cada célula é o atributo data-filtro do <td> (ou o texto do <td>).
 * Não usa dados fixos: as opções dos selects vêm das próprias linhas renderizadas
 * pelo back end.
 */
(function (global) {
    'use strict';

    function normalizar(texto) {
        return String(texto == null ? '' : texto)
            .toLowerCase()
            .normalize('NFD').replace(/[̀-ͯ]/g, '')
            .trim();
    }

    function valorCelula(tr, indice) {
        const td = tr.children[indice];
        if (!td) { return ''; }
        const v = td.getAttribute('data-filtro');
        return v !== null ? v : td.textContent.replace(/\s+/g, ' ').trim();
    }

    function ordenarTextos(a, b) {
        return a.localeCompare(b, 'pt-BR', {numeric: true, sensitivity: 'base'});
    }

    function iniciar(cfg) {
        const tabela = document.getElementById(cfg.tabela);
        if (!tabela) { return; }
        const linhas = Array.prototype.slice.call(tabela.querySelectorAll('tbody tr[data-busca]'));
        const campoTexto = cfg.texto ? document.getElementById(cfg.texto) : null;
        const campoSituacao = cfg.situacao ? document.getElementById(cfg.situacao) : null;
        const contador = cfg.contador ? document.getElementById(cfg.contador) : null;
        const btnLimpar = cfg.limpar ? document.getElementById(cfg.limpar) : null;
        const filtrosColuna = Array.prototype.slice.call(tabela.querySelectorAll('.cc-filtros-coluna [data-coluna]'));

        // Monta as opções dos selects com os valores existentes na coluna
        filtrosColuna.forEach(function (campo) {
            if (campo.tagName !== 'SELECT') { return; }
            const indice = parseInt(campo.getAttribute('data-coluna'), 10);
            const valores = [];
            linhas.forEach(function (tr) {
                const v = valorCelula(tr, indice);
                if (valores.indexOf(v) === -1) { valores.push(v); }
            });
            valores.sort(ordenarTextos);
            valores.forEach(function (v) {
                const op = document.createElement('option');
                op.value = v;
                op.textContent = v === '' ? '(vazio)' : v;
                campo.appendChild(op);
            });
        });

        function filtrar() {
            const termo = campoTexto ? normalizar(campoTexto.value) : '';
            const situacao = campoSituacao ? campoSituacao.value : '';
            const ativos = filtrosColuna
                .map(function (c) {
                    return {
                        indice: parseInt(c.getAttribute('data-coluna'), 10),
                        modo: c.getAttribute('data-modo') || 'texto',
                        valor: c.tagName === 'SELECT' ? c.value : normalizar(c.value),
                        usado: c.tagName === 'SELECT' ? c.value !== '__todos__' : normalizar(c.value) !== ''
                    };
                })
                .filter(function (f) { return f.usado; });

            let visiveis = 0;
            linhas.forEach(function (tr) {
                let mostrar = true;
                if (situacao && tr.getAttribute('data-situacao') !== situacao) { mostrar = false; }
                if (mostrar && termo && normalizar(tr.getAttribute('data-busca')).indexOf(termo) === -1) { mostrar = false; }
                if (mostrar) {
                    for (let i = 0; i < ativos.length; i++) {
                        const f = ativos[i];
                        const celula = valorCelula(tr, f.indice);
                        if (f.modo === 'exato') {
                            if (celula !== f.valor) { mostrar = false; break; }
                        } else if (normalizar(celula).indexOf(f.valor) === -1) {
                            mostrar = false; break;
                        }
                    }
                }
                tr.style.display = mostrar ? '' : 'none';
                if (mostrar) { visiveis++; }
            });
            if (contador) { contador.textContent = visiveis.toLocaleString('pt-BR'); }
        }

        if (campoTexto) { campoTexto.addEventListener('input', filtrar); }
        if (campoSituacao) { campoSituacao.addEventListener('change', filtrar); }
        filtrosColuna.forEach(function (c) {
            c.addEventListener(c.tagName === 'SELECT' ? 'change' : 'input', filtrar);
        });
        if (btnLimpar) {
            btnLimpar.addEventListener('click', function (e) {
                e.preventDefault();
                e.stopImmediatePropagation();
                if (campoTexto) { campoTexto.value = ''; }
                if (campoSituacao) { campoSituacao.value = ''; }
                filtrosColuna.forEach(function (c) { c.value = c.tagName === 'SELECT' ? '__todos__' : ''; });
                filtrar();
            });
        }
        filtrar();
        return {filtrar: filtrar};
    }

    global.CCFiltroTabela = {iniciar: iniciar};
})(window);
