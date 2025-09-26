from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for, send_file
from flask_login import login_required, current_user
from app.models.fatura_caixa import FaturaCaixa
from app import db
from datetime import datetime
import os
import glob
from app.utils.audit import registrar_log
from io import BytesIO
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

fatura_caixa_bp = Blueprint('fatura_caixa', __name__, url_prefix='/fatura-caixa')


@fatura_caixa_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}


@fatura_caixa_bp.route('/')
@login_required
def index():
    """Página principal do Fatura Caixa"""
    # Buscar estatísticas completas
    stats = FaturaCaixa.obter_estatisticas()

    # Buscar última carga
    ultima_carga = FaturaCaixa.query.filter_by(DELETED_AT=None).order_by(
        FaturaCaixa.DTA_CARGA.desc()
    ).first()

    return render_template('fatura_caixa/index.html',
                           stats=stats,
                           ultima_carga=ultima_carga)


@fatura_caixa_bp.route('/carga')
@login_required
def carga():
    """Página de carga de dados"""
    # Buscar histórico de arquivos já processados
    arquivos_processados = db.session.query(
        FaturaCaixa.ARQUIVO_ORIGEM,
        FaturaCaixa.DTA_CARGA,
        FaturaCaixa.USUARIO_CARGA,
        db.func.count(FaturaCaixa.ID).label('total_registros')
    ).filter(
        FaturaCaixa.DELETED_AT == None
    ).group_by(
        FaturaCaixa.ARQUIVO_ORIGEM,
        FaturaCaixa.DTA_CARGA,
        FaturaCaixa.USUARIO_CARGA
    ).order_by(
        FaturaCaixa.DTA_CARGA.desc()
    ).limit(10).all()

    return render_template('fatura_caixa/carga.html',
                           historico=arquivos_processados)


@fatura_caixa_bp.route('/processar-carga', methods=['POST'])
@login_required
def processar_carga():
    """Processa a carga do arquivo TXT - ACUMULANDO DADOS"""
    try:
        # Caminho base para buscar os arquivos
        caminho_base = r"\\Compartilhadas\UNIDADES\SUCRE\GEINC\AUTORIZAR_VALIDAR\Contrato Imobiliário\Seguro\FORA SFH\Caixa Seguradora\2025_CAIXA\092025"

        # Verifica se o caminho existe
        if not os.path.exists(caminho_base):
            return jsonify({
                'success': False,
                'message': 'Caminho de rede não acessível. Verifique a conexão com a rede.'
            }), 400

        # Buscar arquivos TXT que começam com o padrão
        padrao_arquivo = os.path.join(caminho_base, "CNT.GEA.MZ.BFC2.PREMIOS.EMGEA.D*")
        arquivos_encontrados = glob.glob(padrao_arquivo)

        if not arquivos_encontrados:
            return jsonify({
                'success': False,
                'message': 'Nenhum arquivo encontrado no caminho especificado.'
            }), 404

        # Pegar o arquivo mais recente (com base na data no nome)
        arquivo_mais_recente = max(arquivos_encontrados)
        nome_arquivo = os.path.basename(arquivo_mais_recente)

        # Verificar se este arquivo já foi processado
        ja_processado = FaturaCaixa.contar_registros_arquivo(nome_arquivo)
        if ja_processado > 0:
            return jsonify({
                'success': False,
                'message': f'O arquivo {nome_arquivo} já foi processado anteriormente com {ja_processado} registros.',
                'tipo': 'warning'
            }), 400

        # Importar dados do arquivo - DADOS ANTERIORES SÃO PRESERVADOS
        registros_inseridos, mensagem = FaturaCaixa.importar_dados_txt(
            arquivo_mais_recente,
            current_user.nome
        )

        if registros_inseridos == 0 and "já foi processado" not in mensagem:
            # Se for erro real (não arquivo duplicado)
            registrar_log(
                acao='erro_carga',
                entidade='fatura_caixa',
                entidade_id=None,
                descricao=f'Erro ao carregar arquivo: {mensagem}',
                dados_novos={'arquivo': nome_arquivo, 'erro': mensagem}
            )

            return jsonify({
                'success': False,
                'message': f'Erro ao processar arquivo: {mensagem}'
            }), 500

        # Registrar sucesso no log
        if registros_inseridos > 0:
            registrar_log(
                acao='carga',
                entidade='fatura_caixa',
                entidade_id=None,
                descricao=f'Carga realizada com sucesso',
                dados_novos={
                    'arquivo': nome_arquivo,
                    'registros_novos': registros_inseridos,
                    'usuario': current_user.nome
                }
            )

            # Buscar estatísticas atualizadas
            stats = FaturaCaixa.obter_estatisticas()

            return jsonify({
                'success': True,
                'message': f'Arquivo {nome_arquivo} processado com sucesso! {mensagem}',
                'arquivo': nome_arquivo,
                'registros_novos': registros_inseridos,
                'total_acumulado': stats['total_registros']
            })
        else:
            return jsonify({
                'success': False,
                'message': mensagem,
                'tipo': 'info'
            }), 400

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Erro inesperado: {str(e)}'
        }), 500


@fatura_caixa_bp.route('/consulta')
@login_required
def consulta():
    """Página de consulta de dados"""
    # Filtros
    nr_contrato = request.args.get('nr_contrato')
    data_inicio = request.args.get('data_inicio')
    data_fim = request.args.get('data_fim')
    arquivo_origem = request.args.get('arquivo_origem')

    # Query base
    query = FaturaCaixa.query.filter_by(DELETED_AT=None)

    # Aplicar filtros
    if nr_contrato:
        query = query.filter_by(NR_CONTRATO=nr_contrato)

    if arquivo_origem:
        query = query.filter_by(ARQUIVO_ORIGEM=arquivo_origem)

    if data_inicio:
        query = query.filter(FaturaCaixa.DTA_CARGA >= datetime.strptime(data_inicio, '%Y-%m-%d'))

    if data_fim:
        query = query.filter(FaturaCaixa.DTA_CARGA <= datetime.strptime(data_fim + ' 23:59:59', '%Y-%m-%d %H:%M:%S'))

    # Ordenar por data de carga descendente
    query = query.order_by(FaturaCaixa.DTA_CARGA.desc())

    # Executar query com paginação
    page = request.args.get('page', 1, type=int)
    per_page = 50
    registros = query.paginate(page=page, per_page=per_page, error_out=False)

    # Buscar lista de arquivos para filtro
    arquivos_distintos = db.session.query(
        FaturaCaixa.ARQUIVO_ORIGEM
    ).filter(
        FaturaCaixa.DELETED_AT == None
    ).distinct().all()

    return render_template('fatura_caixa/consulta.html',
                           registros=registros,
                           arquivos_distintos=[a[0] for a in arquivos_distintos],
                           filtros={
                               'nr_contrato': nr_contrato,
                               'data_inicio': data_inicio,
                               'data_fim': data_fim,
                               'arquivo_origem': arquivo_origem
                           })


@fatura_caixa_bp.route('/exportar')
@login_required
def exportar():
    """Exporta dados para Excel"""
    try:
        import pandas as pd
        import openpyxl

        # Aplicar os mesmos filtros da consulta
        nr_contrato = request.args.get('nr_contrato')
        data_inicio = request.args.get('data_inicio')
        data_fim = request.args.get('data_fim')
        arquivo_origem = request.args.get('arquivo_origem')

        # Query base
        query = FaturaCaixa.query.filter_by(DELETED_AT=None)

        # Aplicar filtros
        if nr_contrato:
            query = query.filter_by(NR_CONTRATO=nr_contrato)

        if arquivo_origem:
            query = query.filter_by(ARQUIVO_ORIGEM=arquivo_origem)

        if data_inicio:
            query = query.filter(FaturaCaixa.DTA_CARGA >= datetime.strptime(data_inicio, '%Y-%m-%d'))

        if data_fim:
            query = query.filter(
                FaturaCaixa.DTA_CARGA <= datetime.strptime(data_fim + ' 23:59:59', '%Y-%m-%d %H:%M:%S'))

        # Buscar dados
        registros = query.order_by(FaturaCaixa.DTA_CARGA.desc()).all()

        # Converter para DataFrame
        dados = []
        for r in registros:
            dados.append({
                'Contrato Terceiro': str(r.NUM_CONTRATO_TERC) if r.NUM_CONTRATO_TERC else '',
                'Nº Contrato': r.NR_CONTRATO,
                'Seq Prêmio': r.SEQ_PREMIO,
                'Cód Produto': r.COD_PRODUTO,
                'Cód Subest': r.COD_SUBEST,
                'MIP/DIF': 'MIP' if r.MIP_DIF == 1 else 'DIF' if r.MIP_DIF == 2 else '',
                'Tipo Prêmio': r.IND_TP_PREMIO,
                'Última Movimentação': r.DTA_ULT_MOVTO.strftime('%d/%m/%Y') if r.DTA_ULT_MOVTO else '',
                'Início Referência': r.DTA_INI_REFERENCIA.strftime('%d/%m/%Y') if r.DTA_INI_REFERENCIA else '',
                'Fim Referência': r.DTA_FIM_REFERENCIA.strftime('%d/%m/%Y') if r.DTA_FIM_REFERENCIA else '',
                'Valor Prêmio': float(r.VR_PREMIO) if r.VR_PREMIO else 0,
                'IOF MIP/DIF': float(r.IOF_MIP_DIF) if r.IOF_MIP_DIF else 0,
                'Cód Evento': r.COD_EVENTO,
                'Contrato Original': r.NUM_ORI_CONTRATO,
                'Seq Prêmio Original': r.SEQ_PREMIO_ORI,
                'Nº Endosso': r.NUM_ENDOSSO,
                'Arquivo Origem': r.ARQUIVO_ORIGEM,
                'Data Carga': r.DTA_CARGA.strftime('%d/%m/%Y %H:%M') if r.DTA_CARGA else '',
                'Usuário Carga': r.USUARIO_CARGA
            })

        df = pd.DataFrame(dados)

        # Criar arquivo Excel
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Fatura Caixa', index=False)

            # Ajustar largura das colunas
            worksheet = writer.sheets['Fatura Caixa']
            for idx, column in enumerate(df.columns):
                column_width = max(df[column].astype(str).map(len).max(), len(column))
                column_letter = chr(65 + idx) if idx < 26 else 'A' + chr(65 + idx - 26)
                worksheet.column_dimensions[column_letter].width = min(column_width + 2, 50)

        output.seek(0)

        # Registrar no log
        registrar_log(
            acao='exportar',
            entidade='fatura_caixa',
            entidade_id=None,
            descricao=f'Exportação realizada - {len(registros)} registros',
            dados_novos={'total_registros': len(registros), 'filtros': {
                'nr_contrato': nr_contrato,
                'arquivo_origem': arquivo_origem,
                'data_inicio': data_inicio,
                'data_fim': data_fim
            }}
        )

        nome_arquivo = f'fatura_caixa_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=nome_arquivo
        )

    except ImportError:
        flash('É necessário instalar a biblioteca pandas e openpyxl para exportar dados: pip install pandas openpyxl',
              'warning')
        return redirect(url_for('fatura_caixa.consulta'))
    except Exception as e:
        flash(f'Erro ao exportar dados: {str(e)}', 'danger')
        return redirect(url_for('fatura_caixa.consulta'))


@fatura_caixa_bp.route('/exportar-arquivo-final')
@login_required
def exportar_arquivo_final():
    """Página para exportar arquivo final TI"""
    # Buscar referências disponíveis
    referencias = FaturaCaixa.obter_referencias_arquivo_final()

    return render_template('fatura_caixa/exportar_arquivo_final.html',
                           referencias=referencias)


@fatura_caixa_bp.route('/gerar-arquivo-final', methods=['POST'])
@login_required
def gerar_arquivo_final():
    """Gera o arquivo Excel com os dados filtrados"""
    try:
        import pandas as pd

        data = request.get_json()
        referencia = data.get('referencia')
        salvar_automatico = data.get('salvar_automatico', False)
        abrir_chamado = data.get('abrir_chamado', False)

        if not referencia:
            return jsonify({
                'success': False,
                'message': 'Selecione uma referência'
            }), 400

        # Buscar dados
        dados = FaturaCaixa.obter_dados_arquivo_final(referencia)

        if not dados:
            return jsonify({
                'success': False,
                'message': 'Nenhum dado encontrado para esta referência'
            }), 404

        # Criar DataFrame
        df = pd.DataFrame(dados)

        # Criar arquivo Excel
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Dados_Caixa', index=False)

            # Formatar colunas
            worksheet = writer.sheets['Dados_Caixa']
            for idx, column in enumerate(df.columns):
                column_width = max(df[column].astype(str).map(len).max(), len(column))
                column_letter = chr(65 + idx) if idx < 26 else 'A' + chr(65 + idx - 26)
                worksheet.column_dimensions[column_letter].width = min(column_width + 2, 50)

        output.seek(0)

        # Nome do arquivo
        nome_arquivo = f'Caixa_Seguradora_{referencia.replace("/", "_")}.xlsx'

        if salvar_automatico:
            # Caminho de destino
            caminho_destino = r'A:\Supec\Geinc\Public_Geinc\Portal-GEINC\Seguros_Norrana'

            # Verificar se o caminho existe
            if not os.path.exists(caminho_destino):
                # Tentar criar o diretório
                try:
                    os.makedirs(caminho_destino, exist_ok=True)
                except:
                    return jsonify({
                        'success': False,
                        'message': f'Não foi possível acessar o caminho: {caminho_destino}'
                    }), 500

            # Salvar arquivo
            caminho_completo = os.path.join(caminho_destino, nome_arquivo)
            with open(caminho_completo, 'wb') as f:
                f.write(output.read())

            output.seek(0)  # Resetar posição para possível download

            # Se deve abrir chamado automaticamente
            if abrir_chamado:
                try:
                    resultado_chamado = abrir_chamado_automatico(caminho_completo)
                    if resultado_chamado['success']:
                        return jsonify({
                            'success': True,
                            'message': f'Arquivo salvo e chamado aberto com sucesso! Número: {resultado_chamado["numero_chamado"]}',
                            'arquivo': nome_arquivo,
                            'caminho': caminho_completo,
                            'numero_chamado': resultado_chamado['numero_chamado']
                        })
                    else:
                        return jsonify({
                            'success': True,
                            'message': f'Arquivo salvo, mas erro ao abrir chamado: {resultado_chamado["error"]}',
                            'arquivo': nome_arquivo,
                            'caminho': caminho_completo
                        })
                except Exception as e:
                    return jsonify({
                        'success': True,
                        'message': f'Arquivo salvo, mas erro ao abrir chamado: {str(e)}',
                        'arquivo': nome_arquivo,
                        'caminho': caminho_completo
                    })

            return jsonify({
                'success': True,
                'message': f'Arquivo salvo com sucesso em: {caminho_completo}',
                'arquivo': nome_arquivo,
                'caminho': caminho_completo
            })

        else:
            # Apenas retornar o arquivo para download
            return send_file(
                output,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=nome_arquivo
            )

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Erro ao gerar arquivo: {str(e)}'
        }), 500


def abrir_chamado_automatico(caminho_arquivo):
    """Abre chamado automaticamente no sistema SISADE"""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager

        # Configurar Chrome
        chrome_options = Options()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        # chrome_options.add_argument('--headless')  # Descomente para rodar sem interface

        # O webdriver-manager baixa automaticamente o driver correto
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)

        try:
            # Acessar a página
            driver.get('http://intranet/sistemas/sisade/view/chamadoins.aspx')

            # Aguardar página carregar
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "ctl00_cphMasterPage_WcChamado1_dtvChamado_ddlAreaNegocial"))
            )

            # Selecionar Área: Tecnologia
            select_area = Select(
                driver.find_element(By.ID, "ctl00_cphMasterPage_WcChamado1_dtvChamado_ddlAreaNegocial"))
            select_area.select_by_value("1")  # 1 = Tecnologia

            time.sleep(2)  # Aguardar carregamento do dropdown dependente

            # Selecionar Tipo de Chamado Nível 1: Sistemas
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "ctl00_cphMasterPage_WcChamado1_dtvChamado_ddlTipoChamadoNivel1"))
            )
            select_nivel1 = Select(
                driver.find_element(By.ID, "ctl00_cphMasterPage_WcChamado1_dtvChamado_ddlTipoChamadoNivel1"))
            select_nivel1.select_by_value("552")  # 552 = Sistemas

            time.sleep(2)  # Aguardar carregamento do dropdown dependente

            # Selecionar Tipo de Chamado Nível 2: Extração/Importação de Dados
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "ctl00_cphMasterPage_WcChamado1_dtvChamado_ddlTipoChamadoNivel2"))
            )
            select_nivel2 = Select(
                driver.find_element(By.ID, "ctl00_cphMasterPage_WcChamado1_dtvChamado_ddlTipoChamadoNivel2"))
            select_nivel2.select_by_value("555")  # 555 = Extração/Importação de Dados

            time.sleep(1)

            # Preencher descrição
            descricao = driver.find_element(By.ID, "ctl00_cphMasterPage_WcChamado1_dtvChamado_txbDsChamado")
            descricao.clear()
            descricao.send_keys("Prezados, importar para o SISSEG arquivo da Caixa Seguradora.")

            # Adicionar arquivo
            input_arquivo = driver.find_element(By.ID,
                                                "ctl00_cphMasterPage_WcChamado1_dtvChamado_wcUploadArquivo2_frmUploadArquivo_AnexosReaberturaChamado_IpFile")
            input_arquivo.send_keys(caminho_arquivo)

            # Clicar no botão Adicionar
            btn_adicionar = driver.find_element(By.ID,
                                                "ctl00_cphMasterPage_WcChamado1_dtvChamado_wcUploadArquivo2_frmUploadArquivo_AnexosReaberturaChamado_btnAdd")
            driver.execute_script("arguments[0].click();", btn_adicionar)

            time.sleep(2)  # Aguardar arquivo ser adicionado

            # Clicar em Salvar
            btn_salvar = driver.find_element(By.ID, "ctl00_cphMasterPage_WcChamado1_dtvChamado_btnChamadoINS")
            driver.execute_script("arguments[0].click();", btn_salvar)

            time.sleep(5)  # Aguardar processamento

            # Tentar capturar o número do chamado (se disponível)
            try:
                # Verificar se há mensagem de sucesso ou número do chamado
                # A página pode redirecionar ou mostrar uma mensagem
                numero_chamado = "Chamado aberto com sucesso"

                # Tentar capturar URL ou texto com número do chamado
                if "chamadocon" in driver.current_url.lower():
                    # Se redirecionou para página de confirmação
                    numero_chamado = f"Chamado aberto - Verifique no SISADE"
            except:
                numero_chamado = "Chamado aberto"

            driver.quit()

            return {
                'success': True,
                'numero_chamado': numero_chamado
            }

        except Exception as e:
            driver.quit()
            return {
                'success': False,
                'error': str(e)
            }

    except ImportError as e:
        return {
            'success': False,
            'error': 'Selenium ou webdriver-manager não instalado. Execute: pip install selenium webdriver-manager'
        }
    except Exception as e:
        return {
            'success': False,
            'error': f'Erro ao inicializar navegador: {str(e)}'
        }