# -*- coding: utf-8 -*-
"""
run.py - Ponto de entrada do Portal GEINC.
Compatível com Python 3.9 e 3.12.
"""

from app import create_app
from app.utils.log_seguro import configurar_logging, log_info, log_erro

# Configura o logging ANTES de criar a aplicação
configurar_logging()

app = create_app()

if __name__ == '__main__':
    try:
        # Servidor de produção multi-thread (não depende de console)
        from waitress import serve

        log_info("Iniciando Portal GEINC com Waitress na porta 5001")
        serve(
            app,
            host='0.0.0.0',
            port=5001,
            threads=12,
            channel_timeout=900,      # processos longos (distribuição) não caem
            connection_limit=200,
            ident='PortalGEINC'
        )
    except ImportError:
        # Fallback caso o waitress não esteja instalado (firewall corporativo)
        log_erro("Waitress não encontrado. Usando servidor Werkzeug em modo threaded.")
        app.run(host='0.0.0.0', port=5001, debug=False, threaded=True)