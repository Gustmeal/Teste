# -*- coding: utf-8 -*-
"""
app/auditoria_v2/gravador.py
Gravação ASSÍNCRONA da Auditoria V2.

Por que assíncrona: a tela do usuário NUNCA espera a auditoria. Cada requisição só
coloca um pacote numa fila em memória (microssegundos). Uma thread em segundo plano
junta os pacotes e grava em lote (um INSERT em lote por tabela, a cada ~1 segundo).

Robustez:
  - Se o banco falhar, tenta de novo; persistindo a falha, salva os pacotes em
    logs/auditoria_v2_pendente.jsonl. Na próxima inicialização esse arquivo é
    reprocessado automaticamente (nada se perde).
  - Ao encerrar o processo (atexit), a fila é descarregada.

Compatível com Python 3.9 e 3.12.
"""

import atexit
import json
import os
import queue
import threading
import time
from datetime import datetime

from sqlalchemy import text

from app.auditoria_v2.util import MARCA_SQL

_TAMANHO_FILA = 200000
_LOTE_MAXIMO = 400
_ESPERA_SEGUNDOS = 1.0

COLUNAS = {
    'requisicoes': ('BDG.AUD_TB001_REQUISICAO', [
        'ID_CORRELACAO', 'DT_INICIO', 'DURACAO_MS', 'USUARIO_ID', 'USUARIO_NOME', 'USUARIO_EMAIL',
        'USUARIO_PERFIL', 'USUARIO_AREA', 'USUARIO_CARGO', 'IP', 'NAVEGADOR', 'METODO', 'URL',
        'PAGINA_ORIGEM', 'ROTA', 'BLUEPRINT', 'APLICATIVO', 'TIPO_ACAO', 'STATUS_HTTP', 'AJAX',
        'PARAMETROS', 'ARQUIVO_DOWNLOAD', 'TAMANHO_RESPOSTA', 'QTDE_CONSULTAS_BANCO', 'TEMPO_BANCO_MS',
        'QTDE_COMANDOS_ESCRITA', 'QTDE_REGISTROS_ALTERADOS', 'MENSAGENS', 'ERRO']),
    'comandos': ('BDG.AUD_TB004_COMANDO_SQL', [
        'COD_COMANDO', 'ID_CORRELACAO', 'DT_EXECUCAO', 'USUARIO_ID', 'USUARIO_NOME', 'BLUEPRINT',
        'APLICATIVO', 'ORIGEM', 'OPERACAO', 'TABELA', 'COMANDO', 'PARAMETROS', 'QTDE_LOTES',
        'LINHAS_AFETADAS', 'DURACAO_MS', 'SITUACAO', 'DETALHAMENTO', 'ERRO']),
    'alteracoes': ('BDG.AUD_TB002_ALTERACAO', [
        'COD_ALTERACAO', 'ID_CORRELACAO', 'COD_COMANDO', 'DT_ALTERACAO', 'USUARIO_ID', 'USUARIO_NOME',
        'BLUEPRINT', 'APLICATIVO', 'ORIGEM', 'OPERACAO', 'TABELA', 'CHAVE_REGISTRO',
        'CAMPOS_ALTERADOS', 'DADOS_ANTES', 'DADOS_DEPOIS']),
    'campos': ('BDG.AUD_TB003_ALTERACAO_CAMPO', [
        'COD_ALTERACAO', 'ID_CORRELACAO', 'DT_ALTERACAO', 'USUARIO_ID', 'USUARIO_NOME', 'TABELA',
        'CHAVE_REGISTRO', 'CAMPO', 'VALOR_ANTES', 'VALOR_DEPOIS']),
    'eventos': ('BDG.AUD_TB005_EVENTO', [
        'ID_CORRELACAO', 'DT_EVENTO', 'TIPO', 'NIVEL', 'USUARIO_ID', 'USUARIO_NOME', 'IP', 'BLUEPRINT',
        'APLICATIVO', 'ENTIDADE', 'ENTIDADE_ID', 'DESCRICAO', 'DETALHES']),
}

# Ordem de gravação (pais antes de filhos, para leitura consistente)
ORDEM = ('requisicoes', 'comandos', 'alteracoes', 'campos', 'eventos')


def _sql_insert(tabela, colunas):
    return text(MARCA_SQL + ' INSERT INTO {} ({}) VALUES ({})'.format(
        tabela, ', '.join(colunas), ', '.join(':' + c for c in colunas)))


_SQLS = dict((nome, _sql_insert(tab, cols)) for nome, (tab, cols) in COLUNAS.items())


def pacote_vazio():
    return dict((nome, []) for nome in ORDEM)


class _Gravador(object):

    def __init__(self):
        self.fila = queue.Queue(maxsize=_TAMANHO_FILA)
        self.engine = None
        self.arquivo_pendente = None
        self.thread = None
        self._parar = threading.Event()
        self._lock_arquivo = threading.Lock()

    # ------------------------------------------------------------------
    def iniciar(self, engine, pasta_logs):
        if self.thread is not None:
            return
        self.engine = engine
        try:
            os.makedirs(pasta_logs, exist_ok=True)
        except OSError:
            pass
        self.arquivo_pendente = os.path.join(pasta_logs, 'auditoria_v2_pendente.jsonl')
        self._reprocessar_pendentes()
        self.thread = threading.Thread(target=self._loop, name='AuditoriaV2-Gravador', daemon=True)
        self.thread.start()
        atexit.register(self.encerrar)

    def enfileirar(self, pacote):
        if not pacote or not any(pacote.get(n) for n in ORDEM):
            return
        try:
            self.fila.put_nowait(pacote)
        except queue.Full:
            self._salvar_em_arquivo([pacote])

    # ------------------------------------------------------------------
    def _loop(self):
        while not self._parar.is_set():
            lote = self._coletar_lote()
            if lote:
                self._gravar(lote)

    def _coletar_lote(self):
        lote = []
        try:
            lote.append(self.fila.get(timeout=_ESPERA_SEGUNDOS))
        except queue.Empty:
            return lote
        limite = time.time() + 0.5
        while len(lote) < _LOTE_MAXIMO and time.time() < limite:
            try:
                lote.append(self.fila.get(timeout=0.05))
            except queue.Empty:
                break
        return lote

    def _gravar(self, lote):
        juntos = pacote_vazio()
        for pacote in lote:
            for nome in ORDEM:
                itens = pacote.get(nome)
                if itens:
                    juntos[nome].extend(itens)
        for tentativa in (1, 2):
            try:
                with self.engine.begin() as conn:
                    for nome in ORDEM:
                        linhas = juntos[nome]
                        if not linhas:
                            continue
                        colunas = COLUNAS[nome][1]
                        normalizadas = [dict((c, linha.get(c)) for c in colunas) for linha in linhas]
                        for inicio in range(0, len(normalizadas), 500):
                            conn.execute(_SQLS[nome], normalizadas[inicio:inicio + 500])
                return
            except Exception as erro:
                if tentativa == 2:
                    self._log_erro('Falha ao gravar auditoria V2 no banco: ' + repr(erro))
                    self._salvar_em_arquivo(lote)
                else:
                    time.sleep(1.0)

    # ------------------------------------------------------------------
    def _salvar_em_arquivo(self, pacotes):
        if not self.arquivo_pendente:
            return
        with self._lock_arquivo:
            try:
                with open(self.arquivo_pendente, 'a', encoding='utf-8') as arq:
                    for pacote in pacotes:
                        arq.write(json.dumps(pacote, ensure_ascii=False, default=_json_padrao) + '\n')
            except Exception:
                pass

    def _reprocessar_pendentes(self):
        """Regrava no banco o que ficou em arquivo por falha anterior."""
        caminho = self.arquivo_pendente
        if not caminho or not os.path.exists(caminho):
            return
        processando = caminho + '.processando'
        try:
            os.replace(caminho, processando)
        except OSError:
            return
        pacotes = []
        try:
            with open(processando, 'r', encoding='utf-8') as arq:
                for linha in arq:
                    linha = linha.strip()
                    if linha:
                        try:
                            pacotes.append(_restaurar_datas(json.loads(linha)))
                        except Exception:
                            pass
        except Exception:
            return
        for inicio in range(0, len(pacotes), _LOTE_MAXIMO):
            self._gravar(pacotes[inicio:inicio + _LOTE_MAXIMO])
        try:
            os.remove(processando)
        except OSError:
            pass

    def encerrar(self):
        """Descarrega a fila ao encerrar o processo."""
        self._parar.set()
        restante = []
        while True:
            try:
                restante.append(self.fila.get_nowait())
            except queue.Empty:
                break
        if restante and self.engine is not None:
            for inicio in range(0, len(restante), _LOTE_MAXIMO):
                self._gravar(restante[inicio:inicio + _LOTE_MAXIMO])

    @staticmethod
    def _log_erro(mensagem):
        try:
            from app.utils.log_seguro import log_erro
            log_erro(mensagem)
        except Exception:
            pass


_CAMPOS_DATA = ('DT_INICIO', 'DT_EXECUCAO', 'DT_ALTERACAO', 'DT_EVENTO')


def _json_padrao(valor):
    if isinstance(valor, datetime):
        return valor.strftime('%Y-%m-%dT%H:%M:%S.%f')
    return str(valor)


def _restaurar_datas(pacote):
    for nome in ORDEM:
        for linha in pacote.get(nome) or []:
            for campo in _CAMPOS_DATA:
                valor = linha.get(campo)
                if isinstance(valor, str):
                    try:
                        linha[campo] = datetime.strptime(valor, '%Y-%m-%dT%H:%M:%S.%f')
                    except ValueError:
                        pass
    return pacote


gravador = _Gravador()
