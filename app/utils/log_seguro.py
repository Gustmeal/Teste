# -*- coding: utf-8 -*-
"""
app/utils/log_seguro.py

Logging centralizado e à prova de falhas de I/O do Portal GEINC.
Resolve o erro intermitente "[Errno 22] Invalid argument" causado por:
  - abertura repetida de FileHandler dentro de funções (vazamento de handles);
  - escrita em sys.stdout quando não existe console válido;
  - caminho relativo do arquivo de log dependente do diretório de trabalho.

Compatível com Python 3.9 e 3.12.
"""

import logging
import os
import sys
import threading
from logging.handlers import RotatingFileHandler

# Impede que falhas internas do logging virem exceção na aplicação
logging.raiseExceptions = False

NOME_LOGGER = 'portal_geinc'

_LOCK = threading.Lock()
_CONFIGURADO = False


def _stdout_utilizavel():
    """
    Verifica se sys.stdout realmente aceita escrita.
    Em serviço Windows / pythonw / console fechado, esta escrita falha
    e é exatamente aí que nasce o [Errno 22] Invalid argument.
    """
    stream = getattr(sys, 'stdout', None)
    if stream is None:
        return False
    try:
        stream.write('')
        stream.flush()
        return True
    except Exception:
        return False


def configurar_logging(pasta_base=None, nivel=logging.INFO):
    """
    Configura o logger da aplicação UMA ÚNICA VEZ por processo.
    Deve ser chamada no create_app() ou no run.py.
    """
    global _CONFIGURADO

    with _LOCK:
        logger = logging.getLogger(NOME_LOGGER)

        if _CONFIGURADO:
            return logger

        # Raiz do projeto: app/utils/log_seguro.py -> app/utils -> app -> raiz
        if pasta_base is None:
            pasta_base = os.path.dirname(
                os.path.dirname(
                    os.path.dirname(os.path.abspath(__file__))
                )
            )

        pasta_logs = os.path.join(pasta_base, 'logs')
        try:
            os.makedirs(pasta_logs, exist_ok=True)
        except OSError:
            pasta_logs = pasta_base

        logger.setLevel(nivel)
        logger.propagate = False

        # Remove qualquer handler herdado de configurações antigas
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass

        formato = logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
        )

        # Handler de arquivo com caminho ABSOLUTO, rotação e delay=True
        # (delay=True só abre o arquivo na primeira escrita real)
        try:
            handler_arquivo = RotatingFileHandler(
                os.path.join(pasta_logs, 'portal_geinc.log'),
                maxBytes=5 * 1024 * 1024,
                backupCount=5,
                encoding='utf-8',
                delay=True
            )
            handler_arquivo.setFormatter(formato)
            logger.addHandler(handler_arquivo)
        except OSError:
            pass

        # Console apenas se houver console de verdade
        if _stdout_utilizavel():
            handler_console = logging.StreamHandler(sys.stdout)
            handler_console.setFormatter(formato)
            logger.addHandler(handler_console)

        # Garante que o logger nunca fique sem handler
        if not logger.handlers:
            logger.addHandler(logging.NullHandler())

        _CONFIGURADO = True
        return logger


def obter_logger():
    """Retorna o logger da aplicação, configurando-o se necessário."""
    if not _CONFIGURADO:
        return configurar_logging()
    return logging.getLogger(NOME_LOGGER)


def _emitir(nivel, mensagem):
    """
    Emite a mensagem blindando contra qualquer falha de I/O.
    É esta blindagem que impede o [Errno 22] de abortar a distribuição.
    """
    try:
        obter_logger().log(nivel, mensagem)
    except (OSError, IOError, ValueError):
        pass
    except Exception:
        pass


def log_info(mensagem):
    _emitir(logging.INFO, mensagem)


def log_erro(mensagem):
    _emitir(logging.ERROR, mensagem)


def log_alerta(mensagem):
    _emitir(logging.WARNING, mensagem)


def log_debug(mensagem):
    _emitir(logging.DEBUG, mensagem)


def log_print(*args, **kwargs):
    """
    Substituto direto do print(). Permite trocar 'print(' por 'log_print('
    em todo o projeto sem quebrar nada e sem risco de OSError.
    """
    separador = kwargs.get('sep', ' ')
    try:
        mensagem = separador.join(str(a) for a in args)
    except Exception:
        mensagem = '<mensagem nao serializavel>'
    _emitir(logging.INFO, mensagem)


def log_excecao(prefixo, excecao):
    """Registra a exceção com o traceback completo."""
    import traceback
    _emitir(logging.ERROR, '{0}: {1}'.format(prefixo, repr(excecao)))
    try:
        _emitir(logging.ERROR, traceback.format_exc())
    except Exception:
        pass