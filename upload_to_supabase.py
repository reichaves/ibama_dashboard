import pandas as pd
from supabase import create_client, Client
import time
import os
import sys
import zipfile
import requests
from io import BytesIO
import urllib3
import ssl
from urllib.request import urlopen, Request
import subprocess
import json
import numpy as np
from datetime import datetime
import re

print("🌳 Iniciando processo de upload FINAL CORRIGIDO...")

# --- 1. Configuração de variáveis de ambiente ---
def get_env_var(key: str, default: str = None) -> str:
    """Obtém variável de ambiente com fallback."""
    value = os.getenv(key, default)
    if not value:
        raise ValueError(f"Variável de ambiente {key} não encontrada!")
    return value

# Configurações
SUPABASE_URL = get_env_var("SUPABASE_URL")
SUPABASE_KEY = get_env_var("SUPABASE_KEY")
IBAMA_ZIP_URL = get_env_var(
    "IBAMA_ZIP_URL", 
    "https://dadosabertos.ibama.gov.br/dados/SIFISC/auto_infracao/auto_infracao/auto_infracao_csv.zip"
)

print(f"Configurações carregadas:")
print(f"  - Supabase URL: {SUPABASE_URL[:50]}...")
print(f"  - IBAMA ZIP URL: {IBAMA_ZIP_URL}")

# --- 2. Schema Real do Supabase (baseado no arquivo fornecido) ---
def get_real_supabase_columns(supabase: Client) -> set:
    """Obtém colunas reais da tabela no Supabase."""
    print("🔍 Obtendo schema real do Supabase...")
    
    try:
        # Tenta buscar um registro para ver as colunas reais
        result = supabase.table('ibama_infracao').select('*').limit(1).execute()
        
        if result.data and len(result.data) > 0:
            actual_columns = set(result.data[0].keys())
            print(f"✅ Schema obtido via SELECT: {len(actual_columns)} colunas")
            
            # Remove colunas do sistema
            system_columns = {'id', 'created_at', 'updated_at'}
            data_columns = actual_columns - system_columns
            
            print(f"📋 Colunas de dados: {len(data_columns)}")
            
            # Log das primeiras colunas para verificação
            sorted_cols = sorted(list(data_columns))
            print("📋 Primeiras 10 colunas encontradas:")
            for i, col in enumerate(sorted_cols[:10], 1):
                print(f"  {i:2d}. {col}")
            
            return data_columns
            
        else:
            print("⚠️ Tabela vazia, usando schema conhecido...")
            return get_fallback_schema()
            
    except Exception as e:
        print(f"❌ Erro ao obter schema: {e}")
        return get_fallback_schema()

def get_fallback_schema() -> set:
    """Schema de fallback baseado na estrutura conhecida."""
    print("📋 Usando schema de fallback...")
    
    # Schema baseado na estrutura típica do IBAMA
    known_columns = {
        'SEQ_AUTO_INFRACAO', 'DES_STATUS_FORMULARIO', 'DS_SIT_AUTO_AIE',
        'SIT_CANCELADO', 'NUM_AUTO_INFRACAO', 'SER_AUTO_INFRACAO',
        'CD_ORIGINAL_AUTO_INFRACAO', 'TIPO_AUTO', 'TIPO_MULTA',
        'VAL_AUTO_INFRACAO', 'FUNDAMENTACAO_MULTA', 'PATRIMONIO_APURACAO',
        'GRAVIDADE_INFRACAO', 'CD_NIVEL_GRAVIDADE', 'MOTIVACAO_CONDUTA',
        'EFEITO_MEIO_AMBIENTE', 'EFEITO_SAUDE_PUBLICA', 'PASSIVEL_RECUPERACAO',
        'UNID_ARRECADACAO', 'DES_AUTO_INFRACAO', 'DAT_HORA_AUTO_INFRACAO',
        'FORMA_ENTREGA', 'DAT_CIENCIA_AUTUACAO', 'DT_FATO_INFRACIONAL',
        'DT_INICIO_ATO_INEQUIVOCO', 'DT_FIM_ATO_INEQUIVOCO', 'COD_MUNICIPIO',
        'MUNICIPIO', 'UF', 'NUM_PROCESSO', 'NU_PROCESSO_FORMATADO',
        'COD_INFRACAO', 'DES_INFRACAO', 'TIPO_INFRACAO',
        'CD_RECEITA_AUTO_INFRACAO', 'DES_RECEITA', 'TP_PESSOA_INFRATOR',
        'NUM_PESSOA_INFRATOR', 'NOME_INFRATOR', 'CPF_CNPJ_INFRATOR',
        'QT_AREA', 'INFRACAO_AREA', 'DES_OUTROS_TIPO_AREA',
        'CLASSIFICACAO_AREA', 'DS_FATOR_AJUSTE', 'NUM_LONGITUDE_AUTO',
        'NUM_LATITUDE_AUTO', 'DS_WKT', 'DES_LOCAL_INFRACAO',
        'DS_REFERENCIA_ACAO_FISCALIZATORIA', 'UNIDADE_CONSERVACAO',
        'ID_SICAFI_BIOMAS_ATINGIDOS_INFRACAO', 'DS_BIOMAS_ATINGIDOS',
        'SEQ_NOTIFICACAO', 'SEQ_ACAO_FISCALIZATORIA', 'CD_ACAO_FISCALIZATORIA',
        'UNID_CONTROLE', 'TIPO_ACAO', 'OPERACAO', 'SEQ_ORDEM_FISCALIZACAO',
        'ORDEM_FISCALIZACAO', 'UNID_ORDENADORA', 'SEQ_SOLICITACAO_RECURSO',
        'SOLICITACAO_RECURSO', 'OPERACAO_SOL_RECURSO', 'DT_LANCAMENTO',
        'TP_ULT_ALTERACAO', 'DT_ULT_ALTERACAO', 'JUSTIFICATIVA_ALTERACAO',
        'WKT_GE_AREA_AUTUADA', 'DT_ULT_ALTER_GEOM', 'TP_ORIGEM_GE_AREA_AUTUADA',
        'DS_ERRO_GE_AREA_AUTUADA', 'ST_AUTO_MIGRADO_AIE',
        'DS_ENQUADRAMENTO_ADMINISTRATIVO', 'DS_ENQUADRAMENTO_NAO_ADMINISTRATIVO',
        'DS_ENQUADRAMENTO_COMPLEMENTAR', 'CD_TERMOS_APREENSAO',
        'CD_TERMOS_EMBARGOS', 'TP_ORIGEM_REGISTRO_AUTO',
        'ULTIMA_ATUALIZACAO_RELATORIO'
    }
    
    print(f"📋 Schema de fallback: {len(known_columns)} colunas")
    return known_columns

# --- 3. Download robusto (mantido) ---
def download_with_multiple_methods(url):
    """Tenta múltiplos métodos para baixar o arquivo."""
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    methods = [
        ("requests_no_ssl", lambda: download_with_requests_no_ssl(url)),
        ("urllib_no_ssl", lambda: download_with_urllib_no_ssl(url)),
        ("wget", lambda: download_with_wget(url)),
        ("curl", lambda: download_with_curl(url)),
        ("requests_http", lambda: download_with_requests_http(url)),
    ]
    
    for method_name, method_func in methods:
        print(f"🔄 Tentando método: {method_name}")
        try:
            content = method_func()
            if content and len(content) > 1000:
                print(f"✅ Sucesso com {method_name}! Tamanho: {len(content):,} bytes")
                return content
            else:
                print(f"⚠️ {method_name}: Conteúdo muito pequeno")
        except Exception as e:
            print(f"❌ {method_name} falhou: {str(e)[:100]}...")
    
    raise Exception("❌ Todos os métodos de download falharam!")

def download_with_requests_no_ssl(url):
    session = requests.Session()
    session.verify = False
    response = session.get(url, timeout=300, 
                          headers={'User-Agent': 'Mozilla/5.0 (compatible; IBAMA-Bot/1.0)'})
    response.raise_for_status()
    return response.content

def download_with_urllib_no_ssl(url):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    request = Request(url, headers={'User-Agent': 'Mozilla/5.0 (compatible; IBAMA-Bot/1.0)'})
    with urlopen(request, timeout=300, context=ssl_context) as response:
        return response.read()

def download_with_wget(url):
    try:
        result = subprocess.run([
            'wget', '--no-check-certificate', '--timeout=300', 
            '--user-agent=Mozilla/5.0 (compatible; IBAMA-Bot/1.0)',
            '-O', '-', url
        ], capture_output=True, check=True, timeout=320)
        return result.stdout
    except:
        raise Exception("wget não disponível ou falhou")

def download_with_curl(url):
    try:
        result = subprocess.run([
            'curl', '-k', '--max-time', '300',
            '--user-agent', 'Mozilla/5.0 (compatible; IBAMA-Bot/1.0)',
            '-L', url
        ], capture_output=True, check=True, timeout=320)
        return result.stdout
    except:
        raise Exception("curl não disponível ou falhou")

def download_with_requests_http(url):
    http_url = url.replace('https://', 'http://')
    if http_url == url:
        raise Exception("URL já é HTTP")
    
    session = requests.Session()
    response = session.get(http_url, timeout=300,
                          headers={'User-Agent': 'Mozilla/5.0 (compatible; IBAMA-Bot/1.0)'})
    response.raise_for_status()
    return response.content

# --- 4. Processamento com sincronização de schema ---
def make_json_serializable(obj):
    """Converte objetos para tipos serializáveis em JSON."""
    if pd.isna(obj):
        return None
    elif isinstance(obj, (pd.Timestamp, datetime)):
        return obj.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(obj) else None
    elif isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        if np.isnan(obj):
            return None
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, bytes):
        return obj.decode('utf-8', errors='ignore')
    elif isinstance(obj, str):
        cleaned = str(obj).strip()
        return cleaned if cleaned else None
    else:
        return str(obj) if obj is not None else None

def sync_dataframe_with_supabase(df: pd.DataFrame, supabase_columns: set) -> pd.DataFrame:
    """Sincroniza DataFrame com colunas reais do Supabase."""
    print("🔄 Sincronizando DataFrame com Supabase...")
    
    df_clean = df.copy()
    
    # Verifica correspondências EXATAS (case-sensitive)
    csv_columns = set(df_clean.columns)
    
    # Encontra correspondências exatas
    exact_matches = csv_columns & supabase_columns
    
    # Encontra colunas do CSV que não existem no Supabase
    extra_columns = csv_columns - supabase_columns
    
    # Encontra colunas do Supabase que não estão no CSV
    missing_columns = supabase_columns - csv_columns
    
    print(f"  📊 Colunas no CSV: {len(csv_columns)}")
    print(f"  🏛️ Colunas no Supabase: {len(supabase_columns)}")
    print(f"  ✅ Correspondências exatas: {len(exact_matches)}")
    print(f"  ❌ Extras no CSV: {len(extra_columns)}")
    print(f"  ⚠️ Ausentes no CSV: {len(missing_columns)}")
    
    # Verifica se CD_RECEITA_AUTO_INFRACAO está sendo encontrada
    if 'CD_RECEITA_AUTO_INFRACAO' in exact_matches:
        print(f"  ✅ CD_RECEITA_AUTO_INFRACAO: correspondência exata encontrada")
    elif 'CD_RECEITA_AUTO_INFRACAO' in extra_columns:
        print(f"  ❌ CD_RECEITA_AUTO_INFRACAO: existe no CSV mas NÃO no Supabase")
    elif 'CD_RECEITA_AUTO_INFRACAO' in missing_columns:
        print(f"  ❌ CD_RECEITA_AUTO_INFRACAO: existe no Supabase mas NÃO no CSV")
    else:
        print(f"  ❓ CD_RECEITA_AUTO_INFRACAO: não encontrada em lugar nenhum")
    
    # Log de colunas problemáticas (primeiras 10)
    if extra_columns:
        print(f"\n🗑️ Colunas que serão removidas (primeiras 10):")
        for i, col in enumerate(sorted(extra_columns)[:10], 1):
            print(f"  {i:2d}. {col}")
        if len(extra_columns) > 10:
            print(f"  ... e mais {len(extra_columns) - 10} colunas")
    
    # Mantém apenas colunas que existem no Supabase
    columns_to_keep = list(exact_matches)
    df_synced = df_clean[columns_to_keep].copy()
    
    print(f"\n✅ DataFrame sincronizado: {len(df_synced)} registros, {len(df_synced.columns)} colunas")
    
    # Processa cada coluna para garantir compatibilidade
    for col in df_synced.columns:
        df_synced[col] = df_synced[col].apply(make_json_serializable)
        
        # Tratamento especial para colunas numéricas conhecidas
        numeric_columns = {
            'CD_RECEITA_AUTO_INFRACAO', 'SEQ_AUTO_INFRACAO', 'COD_MUNICIPIO', 
            'COD_INFRACAO', 'NUM_PROCESSO', 'NUM_PESSOA_INFRATOR',
            'SEQ_NOTIFICACAO', 'SEQ_ACAO_FISCALIZATORIA', 'SEQ_ORDEM_FISCALIZACAO',
            'SEQ_SOLICITACAO_RECURSO', 'SOLICITACAO_RECURSO'
        }
        
        if col in numeric_columns:
            try:
                df_synced[col] = pd.to_numeric(df_synced[col], errors='coerce')
                df_synced[col] = df_synced[col].where(pd.notna(df_synced[col]), None)
            except:
                print(f"    ⚠️ Erro na conversão numérica de {col}")
    
    # Valida colunas essenciais
    essential_columns = {'NUM_AUTO_INFRACAO', 'UF', 'TIPO_INFRACAO'}
    missing_essential = essential_columns - set(df_synced.columns)
    
    if missing_essential:
        raise ValueError(f"❌ Colunas essenciais ausentes: {missing_essential}")
    else:
        print(f"✅ Todas as colunas essenciais presentes")
    
    return df_synced

def read_csv_robust(zip_file, csv_file):
    """Lê um arquivo CSV de forma robusta."""
    encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
    separators = [';', ',', '\t']
    
    for encoding in encodings:
        for sep in separators:
            try:
                with zip_file.open(csv_file) as csv_data:
                    df = pd.read_csv(csv_data, encoding=encoding, sep=sep, low_memory=False)
                    if len(df.columns) > 5 and len(df) > 0:
                        return df
            except Exception:
                continue
    return None

# --- 5. Processamento principal ---
def download_and_process_data(supabase_columns: set):
    """Download e processa os dados do IBAMA."""
    print("📥 Baixando dados do IBAMA...")
    
    try:
        # Download
        content = download_with_multiple_methods(IBAMA_ZIP_URL)
        
        print("📦 Processando arquivo ZIP...")
        
        # Processa ZIP
        with zipfile.ZipFile(BytesIO(content)) as zip_file:
            file_list = zip_file.namelist()
            csv_files = [f for f in file_list if f.endswith('.csv')]
            
            if not csv_files:
                raise ValueError("Nenhum arquivo CSV encontrado no ZIP")
            
            print(f"📄 Total de arquivos CSV: {len(csv_files)}")
            
            # Busca arquivos 2024-2026
            target_files = [f for f in csv_files if any(year in f for year in ['2024', '2025', '2026'])]

            if target_files:
                print(f"🎯 Arquivos encontrados (2024-2026): {target_files}")
                files_to_process = target_files
            else:
                print("⚠️ Arquivos 2024-2026 não encontrados. Usando os mais recentes...")
                files_to_process = sorted(csv_files, reverse=True)[:5]
            
            # Processa arquivos
            all_dataframes = []
            
            for csv_file in files_to_process:
                print(f"⚙️ Processando: {csv_file}")
                
                try:
                    df_temp = read_csv_robust(zip_file, csv_file)
                    
                    if df_temp is not None and len(df_temp) > 0:
                        print(f"    ✅ {len(df_temp):,} registros, {len(df_temp.columns)} colunas")
                        all_dataframes.append(df_temp)
                    else:
                        print(f"    ⚠️ Arquivo vazio: {csv_file}")
                        
                except Exception as e:
                    print(f"    ❌ Erro: {str(e)[:100]}...")
                    continue
            
            if not all_dataframes:
                raise ValueError("Nenhum arquivo válido processado")
            
            # Combina DataFrames
            print(f"🔄 Combinando {len(all_dataframes)} arquivos...")
            df = pd.concat(all_dataframes, ignore_index=True, sort=False)
            print(f"📊 Dados combinados: {len(df):,} registros")
        
        # Sincroniza com Supabase
        df_synced = sync_dataframe_with_supabase(df, supabase_columns)
        
        return df_synced
        
    except Exception as e:
        print(f"❌ Erro no processamento: {e}")
        raise

# --- 6. Upload otimizado ---
def safe_upload_batch(supabase: Client, table_name: str, data_batch: list, batch_index: int):
    """Upload seguro de um lote com debug detalhado."""
    try:
        # Limpa registros
        cleaned_batch = []
        for record in data_batch:
            cleaned_record = {}
            for key, value in record.items():
                if key and key.strip():
                    if pd.isna(value):
                        cleaned_record[key] = None
                    elif isinstance(value, (list, dict)):
                        cleaned_record[key] = json.dumps(value) if value else None
                    else:
                        cleaned_record[key] = value
            cleaned_batch.append(cleaned_record)
        
        # Upload
        response = supabase.table(table_name).insert(cleaned_batch).execute()
        
        if hasattr(response, 'error') and response.error:
            raise Exception(f"Erro da API: {response.error}")
        
        return True, len(cleaned_batch)
        
    except Exception as e:
        error_msg = str(e)
        
        # Debug detalhado para primeiros erros
        if batch_index <= 5:
            print(f"\n🔍 DEBUG - Lote {batch_index}:")
            print(f"  Tamanho: {len(data_batch)} registros")
            print(f"  Colunas: {len(data_batch[0].keys())} colunas")
            print(f"  Erro: {error_msg[:400]}...")
            
            # Mostra as primeiras 10 colunas do registro problemático
            if data_batch:
                first_record_keys = list(data_batch[0].keys())[:10]
                print(f"  Primeiras colunas: {first_record_keys}")
            
            # Tenta identificar coluna específica no erro
            if "could not find" in error_msg.lower():
                import re
                match = re.search(r"could not find the '([^']+)'", error_msg, re.IGNORECASE)
                if match:
                    problematic_column = match.group(1)
                    print(f"  🚨 Coluna problemática: {problematic_column}")
                    
                    # Verifica se a coluna existe nos dados
                    if data_batch and problematic_column in data_batch[0]:
                        print(f"  📋 Coluna EXISTS nos dados - problema pode ser no schema do Supabase")
                    else:
                        print(f"  ❌ Coluna NOT EXISTS nos dados")
        
        return False, error_msg

# --- 7. Execução principal ---
try:
    # Conecta ao Supabase
    print("🔗 Conectando ao Supabase...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    table_name = "ibama_infracao"
    
    # Obtém schema real do Supabase
    supabase_columns = get_real_supabase_columns(supabase)
    print(f"✅ Schema do Supabase carregado: {len(supabase_columns)} colunas")
    
    # Download e processamento
    df = download_and_process_data(supabase_columns)
    
    if df.empty:
        print("❌ Nenhum dado processado após sincronização.")
        sys.exit(1)
    
    print(f"✅ Dados sincronizados: {len(df):,} registros, {len(df.columns)} colunas")
    
    # Teste de serialização final
    print("🔍 Teste final de compatibilidade...")
    try:
        test_record = df.iloc[0].to_dict()
        json.dumps(test_record, default=str)
        print("  ✅ Serialização OK")
    except Exception as e:
        print(f"  ❌ Erro na serialização: {e}")
        raise
    
    # Limpa tabela
    print(f"🧹 Limpando tabela '{table_name}'...")
    try:
        delete_result = supabase.table(table_name).delete().neq('id', -1).execute()
        print("  ✅ Tabela limpa")
    except Exception as e:
        print(f"  ⚠️ Aviso na limpeza: {e}")
        print("  ⚠️ Continuando com upload...")
    
    # Upload final em lotes pequenos
    chunk_size = 25  # Lotes bem pequenos para debug completo
    total_chunks = (len(df) // chunk_size) + 1
    print(f"🚀 Upload FINAL: {len(df):,} registros em {total_chunks} lotes de {chunk_size}")
    
    successful_uploads = 0
    failed_uploads = 0
    errors_log = []
    
    for i in range(0, len(df), chunk_size):
        chunk_index = i // chunk_size + 1
        print(f"  📤 Lote {chunk_index}/{total_chunks}...", end=" ")
        
        chunk = df[i:i + chunk_size]
        data_to_insert = chunk.to_dict(orient='records')
        
        success, result = safe_upload_batch(supabase, table_name, data_to_insert, chunk_index)
        
        if success:
            successful_uploads += result
            print(f"✅ {result} registros")
        else:
            failed_uploads += len(data_to_insert)
            errors_log.append(f"Lote {chunk_index}: {result}")
            print(f"❌ Erro")
            
            # Para após alguns erros para análise detalhada
            if chunk_index >= 10:
                print(f"\n🛑 Parando após {chunk_index} tentativas para análise completa")
                break
        
        time.sleep(0.05)  # Pausa mínima
    
    # Relatório final detalhado
    print(f"\n{'='*70}")
    print(f"📊 RELATÓRIO FINAL DO UPLOAD:")
    print(f"  📥 Total de registros: {len(df):,}")
    print(f"  ✅ Upload bem-sucedido: {successful_uploads:,}")
    print(f"  ❌ Upload com falha: {failed_uploads:,}")
    print(f"  🎯 Colunas sincronizadas: {len(df.columns)}")
    
    if len(df) > 0:
        success_rate = (successful_uploads / len(df)) * 100
        print(f"  📈 Taxa de sucesso: {success_rate:.1f}%")
    
    # Status final baseado na taxa de sucesso
    if successful_uploads > len(df) * 0.9:  # 90% ou mais
        print(f"\n🎉 UPLOAD CONCLUÍDO COM SUCESSO!")
        print(f"🎉 {successful_uploads:,} registros carregados no Supabase")
        status_code = 0
        
    elif successful_uploads > len(df) * 0.5:  # 50% ou mais
        print(f"\n⚠️ UPLOAD PARCIALMENTE CONCLUÍDO")
        print(f"⚠️ {successful_uploads:,} de {len(df):,} registros carregados")
        status_code = 0
        
    elif successful_uploads > 0:
        print(f"\n⚠️ UPLOAD COM PROBLEMAS")
        print(f"⚠️ Apenas {successful_uploads:,} registros foram carregados")
        status_code = 1
        
    else:
        print(f"\n❌ UPLOAD FALHOU COMPLETAMENTE")
        print(f"❌ Nenhum registro foi carregado no Supabase")
        status_code = 1
    
    # Log de erros (primeiros 5)
    if errors_log:
        print(f"\n🚨 PRIMEIROS ERROS ENCONTRADOS:")
        for i, error in enumerate(errors_log[:5], 1):
            print(f"  {i}. {error}")
        
        if len(errors_log) > 5:
            print(f"  ... e mais {len(errors_log) - 5} erros")
    
    # Resumo da sincronização de schema
    print(f"\n📋 RESUMO DA SINCRONIZAÇÃO:")
    print(f"  🏛️ Colunas no Supabase: {len(supabase_columns)}")
    print(f"  📄 Colunas no CSV: informação processada")
    print(f"  ✅ Colunas sincronizadas: {len(df.columns)}")
    
    # Verifica se a coluna problemática foi resolvida
    if 'CD_RECEITA_AUTO_INFRACAO' in df.columns:
        print(f"  ✅ CD_RECEITA_AUTO_INFRACAO: incluída no upload")
    else:
        print(f"  ❌ CD_RECEITA_AUTO_INFRACAO: removida (não existe no Supabase)")
    
    print(f"{'='*70}")
    
    # Instruções finais
    if status_code == 0:
        print(f"\n💡 PRÓXIMOS PASSOS:")
        print(f"  1. Verifique os dados no Supabase Dashboard")
        print(f"  2. Teste o aplicativo IBAMA Dashboard")
        print(f"  3. Execute consultas de validação se necessário")
        
        if failed_uploads > 0:
            print(f"\n💡 PARA RESOLVER REGISTROS COM FALHA:")
            print(f"  1. Analise os erros detalhados acima")
            print(f"  2. Execute novamente com chunk_size menor")
            print(f"  3. Considere upload dos registros faltantes separadamente")
    else:
        print(f"\n💡 PARA RESOLVER OS PROBLEMAS:")
        print(f"  1. Analise os erros detalhados acima")
        print(f"  2. Verifique se a tabela 'ibama_infracao' existe no Supabase")
        print(f"  3. Confirme se o schema da tabela está correto")
        print(f"  4. Verifique permissões de escrita na tabela")
        print(f"  5. Execute o script check_schema.py para diagnóstico")
    
    sys.exit(status_code)

except KeyboardInterrupt:
    print(f"\n⏸️ Upload interrompido pelo usuário")
    sys.exit(1)
    
except Exception as e:
    print(f"\n💥 ERRO CRÍTICO:")
    print(f"💥 {str(e)}")
    
    # Debug adicional
    print(f"\n🔍 INFORMAÇÕES DE DEBUG:")
    print(f"  📡 Supabase URL: {SUPABASE_URL[:50]}...")
    print(f"  🔑 Supabase Key: {'***' if SUPABASE_KEY else 'NÃO DEFINIDA'}")
    print(f"  📊 Tabela: {table_name}")
    
    # Stack trace para debug técnico
    print(f"\n📋 STACK TRACE COMPLETO:")
    import traceback
    traceback.print_exc()
    
    print(f"\n💡 SUGESTÕES PARA RESOLVER:")
    print(f"  1. Verifique se as variáveis de ambiente estão corretas")
    print(f"  2. Confirme se a tabela 'ibama_infracao' existe no Supabase")
    print(f"  3. Teste a conexão com o Supabase manualmente")
    print(f"  4. Execute check_schema.py para diagnóstico detalhado")
    
    sys.exit(1)
