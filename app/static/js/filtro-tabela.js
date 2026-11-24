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
 * - Classificação por coluna (como a setinha do Power BI): <th data-ordenar="N"> na
 *   primeira linha do cabeçalho. 1º clique crescente, 2º decrescente, 3º volta à
 *   ordem original. Números são comparados como número.
 *   data-padrao="80" no select: já abre filtrado nesse valor (igual a "80" ou que
 *   comece com "80 - ") e o botão Limpar volta para ele. O valor vem do back end.
 * Não usa dados fixos: as opções dos selects vêm das próprias linhas renderizadas
 * pelo back end.
 */
(function (global) {
    'use strict';

    function normalizar(texto) {
        return String(texto == null ? '' : texto)
            .toLowerCase()
            .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
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
            aplicarPadrao(campo);
        });

        // Seleciona o valor padrão do filtro (se existir na tabela)
        function aplicarPadrao(campo) {
            const padrao = campo.getAttribute('data-padrao');
            if (campo.tagName !== 'SELECT' || !padrao) { return false; }
            const ops = Array.prototype.slice.call(campo.options);
            const achou = ops.find(function (o) { return o.value === padrao || o.value.indexOf(padrao + ' - ') === 0; });
            if (achou) { campo.value = achou.value; return true; }
            return false;
        }

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

        // ---------------- Classificação por coluna ----------------
        const tbody = tabela.querySelector('tbody');
        linhas.forEach(function (tr, i) { tr.setAttribute('data-ordem-original', i); });
        const titulos = Array.prototype.slice.call(tabela.querySelectorAll('thead tr:first-child th[data-ordenar]'));
        const estadoOrdem = {coluna: null, dir: 0};   // dir: 1 crescente, -1 decrescente, 0 original

        function numero(texto) {
            const limpo = String(texto).trim().replace(/\./g, '').replace(',', '.');
            return /^-?\d+(\.\d+)?$/.test(limpo) ? parseFloat(limpo) : null;
        }
        function comparar(a, b) {
            const na = numero(a), nb = numero(b);
            if (na !== null && nb !== null) { return na - nb; }
            if (a === '' && b !== '') { return 1; }      // vazios sempre no fim
            if (b === '' && a !== '') { return -1; }
            return ordenarTextos(a, b);
        }
        function desenharSetas() {
            titulos.forEach(function (th) {
                const icone = th.querySelector('.cc-seta-ordem');
                const ativo = estadoOrdem.coluna === th.getAttribute('data-ordenar') && estadoOrdem.dir !== 0;
                icone.className = 'fas cc-seta-ordem ms-1 ' + (ativo ? (estadoOrdem.dir === 1 ? 'fa-sort-up' : 'fa-sort-down') : 'fa-sort');
                icone.style.opacity = ativo ? '1' : '0.35';
            });
        }
        function ordenar() {
            const indice = parseInt(estadoOrdem.coluna, 10);
            const copia = linhas.slice();
            copia.sort(function (x, y) {
                if (estadoOrdem.dir === 0 || isNaN(indice)) {
                    return parseInt(x.getAttribute('data-ordem-original'), 10) - parseInt(y.getAttribute('data-ordem-original'), 10);
                }
                const r = comparar(valorCelula(x, indice), valorCelula(y, indice));
                if (r !== 0) { return r * estadoOrdem.dir; }
                return parseInt(x.getAttribute('data-ordem-original'), 10) - parseInt(y.getAttribute('data-ordem-original'), 10);
            });
            copia.forEach(function (tr) { tbody.appendChild(tr); });
            desenharSetas();
        }
        titulos.forEach(function (th) {
            th.style.cursor = 'pointer';
            th.style.userSelect = 'none';
            th.title = 'Clique para classificar';
            const icone = document.createElement('i');
            icone.className = 'fas fa-sort cc-seta-ordem ms-1';
            th.appendChild(icone);
            th.addEventListener('click', function () {
                const col = th.getAttribute('data-ordenar');
                if (estadoOrdem.coluna !== col) {
                    estadoOrdem.coluna = col; estadoOrdem.dir = 1;
                } else {
                    estadoOrdem.dir = estadoOrdem.dir === 1 ? -1 : (estadoOrdem.dir === -1 ? 0 : 1);
                }
                ordenar();
            });
        });
        desenharSetas();

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
                if (campoSituacao) { campoSituacao.value = campoSituacao.getAttribute('data-padrao') || ''; }
                filtrosColuna.forEach(function (c) {
                    c.value = c.tagName === 'SELECT' ? '__todos__' : '';
                    aplicarPadrao(c);
                });
                filtrar();
            });
        }
        filtrar();
        return {filtrar: filtrar};
    }

    /*
     * Barra de rolagem lateral em cima da tabela, sincronizada com a de baixo.
     * idTopo: div fina com overflow-x (contém uma div filha); idCorpo: div que rola a tabela.
     */
    function scrollTopo(idTopo, idCorpo) {
        const topo = document.getElementById(idTopo);
        const corpo = document.getElementById(idCorpo);
        if (!topo || !corpo) { return; }
        const recheio = topo.firstElementChild;
        function ajustar() {
            recheio.style.width = corpo.scrollWidth + 'px';
            topo.style.display = corpo.scrollWidth > corpo.clientWidth ? '' : 'none';
        }
        let sincronizando = false;
        topo.addEventListener('scroll', function () {
            if (sincronizando) { sincronizando = false; return; }
            sincronizando = true;
            corpo.scrollLeft = topo.scrollLeft;
        });
        corpo.addEventListener('scroll', function () {
            if (sincronizando) { sincronizando = false; return; }
            sincronizando = true;
            topo.scrollLeft = corpo.scrollLeft;
        });
        window.addEventListener('resize', ajustar);
        ajustar();
        setTimeout(ajustar, 300);
    }

    global.CCFiltroTabela = {iniciar: iniciar, scrollTopo: scrollTopo};
})(window);
