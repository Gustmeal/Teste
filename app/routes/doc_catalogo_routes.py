# app/routes/doc_catalogo_routes.py
from functools import wraps

from flask import Blueprint, render_template, redirect, url_for, flash
from flask_login import login_required, current_user
from sqlalchemy import text, bindparam

from app import db
from app.models.doc_catalogo import Aplicativo, Subaplicativo, TabelaDoc

catalogo_dados_bp = Blueprint(
    'catalogo_dados', __name__, url_prefix='/catalogo-dados'
)


def admin_ou_moderador_required(f):
    """Restringe o acesso a admin e moderador. Caso contrário, volta à home.
    (Para deixar SÓ admin: troque a tupla por ('admin',).)"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated or current_user.perfil not in ('admin', 'moderador'):
            flash('Acesso restrito a administradores e moderadores.', 'danger')
            return redirect(url_for('main.geinc_index'))
        return f(*args, **kwargs)
    return wrapper


def _formatar_tipo(row):
    """Monta um tipo legível a partir do INFORMATION_SCHEMA: VARCHAR(100),
    NUMERIC(18,2), etc."""
    tipo = (row.DATA_TYPE or '').upper()
    if row.CHARACTER_MAXIMUM_LENGTH is not None:
        tam = 'MAX' if row.CHARACTER_MAXIMUM_LENGTH == -1 else row.CHARACTER_MAXIMUM_LENGTH
        return f'{tipo}({tam})'
    if tipo in ('NUMERIC', 'DECIMAL') and row.NUMERIC_PRECISION is not None:
        return f'{tipo}({row.NUMERIC_PRECISION},{row.NUMERIC_SCALE or 0})'
    return tipo


def _colunas_reais(banco, schema, nomes_tabelas):
    """Lê a estrutura REAL das tabelas direto do SQL Server.
    banco: nome do banco (ex.: 'BDDASHBOARDBI') ou None para o banco padrão.
    Retorna (estrutura, existentes). Nada é fixo: tabela inexistente/sem
    permissão simplesmente não retorna."""
    if not nomes_tabelas:
        return {}, set()

    import re
    nomes = list(nomes_tabelas)

    # Prefixo do banco (nome de 3 partes). Valida para evitar injeção de SQL.
    prefixo = ''
    if banco:
        if not re.match(r'^[A-Za-z0-9_]+$', banco):
            return {}, set()  # nome de banco inválido -> trata como não localizada
        prefixo = f'[{banco}].'

    sql_cols = text(f"""
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE,
               CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
               IS_NULLABLE, ORDINAL_POSITION
        FROM {prefixo}INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :schema
          AND TABLE_NAME IN :tabelas
        ORDER BY TABLE_NAME, ORDINAL_POSITION
    """).bindparams(bindparam('tabelas', expanding=True))

    sql_pk = text(f"""
        SELECT ku.TABLE_NAME, ku.COLUMN_NAME
        FROM {prefixo}INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN {prefixo}INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
          ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
         AND tc.TABLE_SCHEMA   = ku.TABLE_SCHEMA
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
          AND tc.TABLE_SCHEMA = :schema
          AND ku.TABLE_NAME IN :tabelas
    """).bindparams(bindparam('tabelas', expanding=True))

    params = {'schema': schema, 'tabelas': nomes}

    pk_map = {}
    for row in db.session.execute(sql_pk, params):
        pk_map.setdefault(row.TABLE_NAME, set()).add(row.COLUMN_NAME)

    estrutura = {}
    for row in db.session.execute(sql_cols, params):
        estrutura.setdefault(row.TABLE_NAME, []).append({
            'nome': row.COLUMN_NAME,
            'tipo': _formatar_tipo(row),
            'nulo': (row.IS_NULLABLE == 'YES'),
            'chave': row.COLUMN_NAME in pk_map.get(row.TABLE_NAME, set()),
        })

    return estrutura, set(estrutura.keys())


@catalogo_dados_bp.route('/')
@login_required
@admin_ou_moderador_required
def index():
    """Lista os aplicativos catalogados (tudo vindo do banco)."""
    aplicativos = (
        Aplicativo.query
        .filter(Aplicativo.IN_ATIVO == True, Aplicativo.DELETED_AT.is_(None))
        .order_by(Aplicativo.NU_ORDEM.asc(), Aplicativo.NO_APLICATIVO.asc())
        .all()
    )
    return render_template('catalogo_dados/index.html', aplicativos=aplicativos)


@catalogo_dados_bp.route('/<chave>')
@login_required
@admin_ou_moderador_required
def aplicativo(chave):
    """Detalhe do aplicativo: agrupa tabelas por subaplicativo e anexa a
    estrutura REAL de cada tabela lida na hora do SQL Server (por banco)."""
    app_obj = (
        Aplicativo.query
        .filter(Aplicativo.CHAVE == chave, Aplicativo.DELETED_AT.is_(None))
        .first_or_404()
    )

    tabelas = (
        TabelaDoc.query
        .filter(TabelaDoc.ID_APLICATIVO == app_obj.ID, TabelaDoc.DELETED_AT.is_(None))
        .order_by(TabelaDoc.NU_ORDEM.asc(), TabelaDoc.NO_TABELA.asc())
        .all()
    )

    # Descobre a estrutura real agrupando por (banco, schema) — uma consulta
    # por banco/schema, evitando N+1 mesmo com tabelas de bancos diferentes.
    por_grupo = {}
    for t in tabelas:
        por_grupo.setdefault((t.NO_BANCO or None, t.NO_SCHEMA or 'BDG'), set()).add(t.NO_TABELA)

    estrutura = {}
    existentes = set()
    for (banco, schema), nomes in por_grupo.items():
        est, exist = _colunas_reais(banco, schema, nomes)
        for nome, cols in est.items():
            estrutura[(banco, schema, nome)] = cols
        for nome in exist:
            existentes.add((banco, schema, nome))

    # Anexa colunas reais, flag de existência e nome completo em cada tabela.
    for t in tabelas:
        chave_tab = (t.NO_BANCO or None, t.NO_SCHEMA or 'BDG', t.NO_TABELA)
        t.colunas = estrutura.get(chave_tab, [])
        t.existe = chave_tab in existentes
        partes = [p for p in (t.NO_BANCO, t.NO_SCHEMA or 'BDG', t.NO_TABELA) if p]
        t.nome_completo = '.'.join(partes)

    # Agrupa por subaplicativo, na ordem cadastrada.
    subaplicativos = (
        Subaplicativo.query
        .filter(Subaplicativo.ID_APLICATIVO == app_obj.ID,
                Subaplicativo.DELETED_AT.is_(None))
        .order_by(Subaplicativo.NU_ORDEM.asc(), Subaplicativo.NO_SUBAPLICATIVO.asc())
        .all()
    )

    grupos = []
    for sub in subaplicativos:
        itens = [t for t in tabelas if t.ID_SUBAPLICATIVO == sub.ID]
        if itens:
            grupos.append({'sub': sub, 'tabelas': itens})

    soltas = [t for t in tabelas if not t.ID_SUBAPLICATIVO]
    if soltas:
        grupos.append({'sub': None, 'tabelas': soltas})

    return render_template('catalogo_dados/aplicativo.html', app_obj=app_obj, grupos=grupos)