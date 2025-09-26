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

            # Verificar/criar diretório
            if not os.path.exists(caminho_destino):
                os.makedirs(caminho_destino, exist_ok=True)

            # Salvar arquivo
            caminho_completo = os.path.join(caminho_destino, nome_arquivo)
            with open(caminho_completo, 'wb') as f:
                f.write(output.read())

            output.seek(0)

            # Se deve abrir chamado automaticamente
            if abrir_chamado:
                try:
                    resultado_chamado = abrir_chamado_automatico(caminho_completo)

                    if resultado_chamado['success']:
                        return jsonify({
                            'success': True,
                            'message': f'Arquivo salvo e chamado aberto com sucesso!',
                            'arquivo': nome_arquivo,
                            'caminho': caminho_completo,
                            'numero_chamado': resultado_chamado['numero_chamado'],
                            'automacao_completa': True
                        })
                    else:
                        # Automação falhou, mas arquivo foi salvo
                        return jsonify({
                            'success': True,
                            'message': f'Arquivo salvo! Erro na automação: {resultado_chamado["error"]}. Abra o chamado manualmente.',
                            'arquivo': nome_arquivo,
                            'caminho': caminho_completo,
                            'automacao_completa': False,
                            'instrucoes_manuais': True
                        })
                except Exception as e:
                    # Erro na automação, mas arquivo foi salvo
                    return jsonify({
                        'success': True,
                        'message': f'Arquivo salvo! Erro na automação. Abra o chamado manualmente.',
                        'arquivo': nome_arquivo,
                        'caminho': caminho_completo,
                        'automacao_completa': False,
                        'instrucoes_manuais': True,
                        'erro_tecnico': str(e)
                    })

            # Apenas salvar sem automação
            return jsonify({
                'success': True,
                'message': f'Arquivo salvo com sucesso!',
                'arquivo': nome_arquivo,
                'caminho': caminho_completo
            })

        else:
            # Retornar para download
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
    """Abre chamado automaticamente no sistema SISADE - COM DRIVER TEMPORÁRIO"""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import Select, WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        import time
        import os
        import tempfile
        import requests
        import zipfile
        import shutil

        # Função auxiliar para baixar o ChromeDriver
        def baixar_chromedriver_temporario():
            """Baixa o ChromeDriver temporariamente"""
            # Criar diretório temporário
            temp_dir = tempfile.mkdtemp()
            driver_path = os.path.join(temp_dir, 'chromedriver.exe')

            try:
                # Tentar obter versão do Chrome instalado (versão simplificada)
                # Usar versão estável conhecida se não conseguir detectar
                versao_chrome = "120"  # Versão padrão estável

                # URL do ChromeDriver
                # Primeiro, obter a versão exata disponível
                try:
                    response = requests.get(
                        f"https://chromedriver.storage.googleapis.com/LATEST_RELEASE_{versao_chrome}",
                        timeout=10
                    )
                    versao_completa = response.text.strip()
                except:
                    # Usar versão conhecida estável se falhar
                    versao_completa = "120.0.6099.109"

                # Baixar o ChromeDriver
                url_download = f"https://chromedriver.storage.googleapis.com/{versao_completa}/chromedriver_win32.zip"

                print(f"Baixando ChromeDriver versão {versao_completa}...")
                response = requests.get(url_download, timeout=30)

                # Salvar e extrair
                zip_path = os.path.join(temp_dir, 'chromedriver.zip')
                with open(zip_path, 'wb') as f:
                    f.write(response.content)

                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)

                # Verificar se existe chromedriver.exe
                if os.path.exists(driver_path):
                    return driver_path, temp_dir

                # Às vezes o arquivo está em subpasta
                chromedriver_path = os.path.join(temp_dir, 'chromedriver-win32', 'chromedriver.exe')
                if os.path.exists(chromedriver_path):
                    shutil.move(chromedriver_path, driver_path)
                    return driver_path, temp_dir

                return None, temp_dir

            except Exception as e:
                print(f"Erro ao baixar ChromeDriver: {e}")
                return None, temp_dir

        # Configurar Chrome
        chrome_options = Options()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--disable-web-security')
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        # chrome_options.add_argument('--headless')  # Descomente para rodar sem interface

        driver = None
        temp_dir = None

        try:
            # Primeiro, tentar usar ChromeDriver se já existir no sistema
            try:
                driver = webdriver.Chrome(options=chrome_options)
            except:
                # Se não encontrou, baixar temporariamente
                print("ChromeDriver não encontrado. Baixando temporariamente...")
                driver_path, temp_dir = baixar_chromedriver_temporario()

                if driver_path and os.path.exists(driver_path):
                    service = Service(driver_path)
                    driver = webdriver.Chrome(service=service, options=chrome_options)
                else:
                    return {
                        'success': False,
                        'error': 'Não foi possível configurar o ChromeDriver. Verifique a conexão com a internet.'
                    }

            # === INÍCIO DA AUTOMAÇÃO ===

            # Acessar a página
            driver.get('http://intranet/sistemas/sisade/view/chamadoins.aspx')

            # Aguardar página carregar
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "ctl00_cphMasterPage_WcChamado1_dtvChamado_ddlAreaNegocial"))
            )

            time.sleep(2)

            # 1. Selecionar Área: Tecnologia
            try:
                select_area = Select(
                    driver.find_element(By.ID, "ctl00_cphMasterPage_WcChamado1_dtvChamado_ddlAreaNegocial"))
                select_area.select_by_value("1")  # 1 = Tecnologia
                time.sleep(3)  # Aguardar carregar próximo dropdown
            except Exception as e:
                raise Exception(f"Erro ao selecionar área: {str(e)}")

            # 2. Selecionar Tipo de Chamado Nível 1: Sistemas
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (By.ID, "ctl00_cphMasterPage_WcChamado1_dtvChamado_ddlTipoChamadoNivel1"))
                )
                select_nivel1 = Select(
                    driver.find_element(By.ID, "ctl00_cphMasterPage_WcChamado1_dtvChamado_ddlTipoChamadoNivel1"))
                select_nivel1.select_by_value("552")  # 552 = Sistemas
                time.sleep(3)  # Aguardar carregar próximo dropdown
            except Exception as e:
                raise Exception(f"Erro ao selecionar Tipo Nível 1: {str(e)}")

            # 3. Selecionar Tipo de Chamado Nível 2: Extração/Importação de Dados
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (By.ID, "ctl00_cphMasterPage_WcChamado1_dtvChamado_ddlTipoChamadoNivel2"))
                )
                select_nivel2 = Select(
                    driver.find_element(By.ID, "ctl00_cphMasterPage_WcChamado1_dtvChamado_ddlTipoChamadoNivel2"))
                select_nivel2.select_by_value("555")  # 555 = Extração/Importação de Dados
                time.sleep(2)
            except Exception as e:
                raise Exception(f"Erro ao selecionar Tipo Nível 2: {str(e)}")

            # 4. Preencher descrição
            try:
                descricao = driver.find_element(By.ID, "ctl00_cphMasterPage_WcChamado1_dtvChamado_txbDsChamado")
                descricao.clear()
                descricao.send_keys("Prezados, importar para o SISSEG arquivo da Caixa Seguradora.")
            except Exception as e:
                raise Exception(f"Erro ao preencher descrição: {str(e)}")

            # 5. Adicionar arquivo
            try:
                input_arquivo = driver.find_element(By.ID,
                                                    "ctl00_cphMasterPage_WcChamado1_dtvChamado_wcUploadArquivo2_frmUploadArquivo_AnexosReaberturaChamado_IpFile")
                input_arquivo.send_keys(caminho_arquivo)
                time.sleep(2)

                # Clicar no botão Adicionar
                btn_adicionar = driver.find_element(By.ID,
                                                    "ctl00_cphMasterPage_WcChamado1_dtvChamado_wcUploadArquivo2_frmUploadArquivo_AnexosReaberturaChamado_btnAdd")
                driver.execute_script("arguments[0].click();", btn_adicionar)
                time.sleep(3)
            except Exception as e:
                raise Exception(f"Erro ao anexar arquivo: {str(e)}")

            # 6. Salvar chamado
            try:
                btn_salvar = driver.find_element(By.ID, "ctl00_cphMasterPage_WcChamado1_dtvChamado_btnChamadoINS")
                driver.execute_script("arguments[0].click();", btn_salvar)
                time.sleep(5)
            except Exception as e:
                raise Exception(f"Erro ao salvar chamado: {str(e)}")

            # Verificar sucesso
            numero_chamado = "Chamado criado com sucesso"
            try:
                if "sucesso" in driver.page_source.lower() or "chamadocon" in driver.current_url.lower():
                    numero_chamado = "Chamado criado com sucesso - Verifique no SISADE"
            except:
                pass

            driver.quit()

            # Limpar diretório temporário se foi criado
            if temp_dir and os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                except:
                    pass  # Não é crítico se falhar a limpeza

            return {
                'success': True,
                'numero_chamado': numero_chamado
            }

        except Exception as e:
            if driver:
                driver.quit()

            # Limpar diretório temporário
            if temp_dir and os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                except:
                    pass

            return {
                'success': False,
                'error': f'Erro durante automação: {str(e)}'
            }

    except ImportError as e:
        return {
            'success': False,
            'error': 'Selenium não instalado. Execute: pip install selenium requests'
        }
    except Exception as e:
        return {
            'success': False,
            'error': f'Erro geral: {str(e)}'
        }