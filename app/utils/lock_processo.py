# -*- coding: utf-8 -*-
"""
app/utils/lock_processo.py

Lock de aplicação baseado em sp_getapplock (SQL Server).
Garante execução exclusiva de processos que usam tabelas temporárias
globais como [BDG].[DCA_TB006_DISTRIBUIVEIS] e [BDG].[DCA_TB007_ARRASTAVEIS].

Compatível com Python 3.9 e 3.12.
"""

from contextlib import contextmanager
from sqlalchemy import text

from app import db
from app.utils.log_seguro import log_info, log_erro


@contextmanager
def lock_exclusivo(nome_recurso, timeout_ms=0):
    """
    Uso:
        with lock_exclusivo('DCA_DISTRIBUICAO_CONTRATOS') as obtido:
            if not obtido:
                flash('Processo já em execução por outro usuário.', 'warning')
                return redirect(...)
            # ... executa o processo ...

    timeout_ms = 0  -> não espera, retorna False imediatamente se ocupado.
    """
    conexao = db.engine.connect()
    transacao = conexao.begin()
    obtido = False

    try:
        sql_lock = text("""
            DECLARE @retorno INT;
            EXEC @retorno = sp_getapplock
                 @Resource   = :recurso,
                 @LockMode   = 'Exclusive',
                 @LockOwner  = 'Transaction',
                 @LockTimeout = :timeout;
            SELECT @retorno AS RETORNO;
        """)

        retorno = conexao.execute(
            sql_lock,
            {"recurso": nome_recurso, "timeout": int(timeout_ms)}
        ).scalar()

        obtido = retorno is not None and int(retorno) >= 0

        if obtido:
            log_info("Lock obtido para o recurso: {0}".format(nome_recurso))
        else:
            log_info("Lock NEGADO para o recurso: {0} (em uso)".format(nome_recurso))

        yield obtido

    except Exception as e:
        log_erro("Erro ao obter lock '{0}': {1}".format(nome_recurso, repr(e)))
        # Em caso de falha do lock, libera a execução para não travar o sistema
        obtido = True
        yield obtido

    finally:
        # O commit/rollback da transação libera o applock automaticamente
        try:
            transacao.commit()
        except Exception:
            try:
                transacao.rollback()
            except Exception:
                pass
        try:
            conexao.close()
        except Exception:
            pass