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
from urllib.request import urlopen
from urllib.error import URLError

print("Iniciando processo de upload para o Supabase...")

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

# --- 2. Download e processamento dos dados ---
def download_and_process_data():
    """Download e processa os dados do IBAMA."""
    print("Baixando dados do IBAMA...")
    
    try:
        # Configurações para contornar problemas de SSL
        # Método 1: Tentar com requests e configurações de SSL relaxadas
        session = requests.Session()
        session.verify = False  # Desabilita verificação SSL
        
        # Suprimir avisos de SSL não verificado
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        print(f"Tentando download da URL: {IBAMA_ZIP_URL}")
        
        try:
            # Primeira tentativa: requests com SSL desabilitado
            response = session.get(IBAMA_ZIP_URL, timeout=300, verify=False)
            response.raise_for_status()
            content = response.content
            print(f"✅ Download via requests bem-sucedido. Tamanho: {len(content)} bytes")
            
        except Exception as e1:
            print(f"⚠️ Falha no requests: {e1}")
            print("Tentando método alternativo com urllib...")
            
            # Método 2: urllib com contexto SSL personalizado
            try:
                # Cria contexto SSL que aceita certificados auto-assinados
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                
                with urlopen(IBAMA_ZIP_URL, timeout=300, context=ssl_context) as response:
                    content = response.read()
                    
                print(f"✅ Download via urllib bem-sucedido. Tamanho: {len(content)} bytes")
                
            except Exception as e2:
                print(f"❌ Falha no urllib: {e2}")
                
                # Método 3: Tentar URL HTTP ao invés de HTTPS
                http_url = IBAMA_ZIP_URL.replace('https://', 'http://')
                if http_url != IBAMA_ZIP_URL:
                    print(f"Tentando URL HTTP: {http_url}")
                    try:
                        response = session.get(http_url, timeout=300)
                        response.raise_for_status()
                        content = response.content
                        print(f"✅ Download via HTTP bem-sucedido. Tamanho: {len(content)} bytes")
                    except Exception as e3:
                        print(f"❌ Falha no HTTP: {e3}")
                        raise Exception(f"Todos os métodos de download falharam: requests({e1}), urllib({e2}), http({e3})")
                else:
                    raise Exception(f"Métodos de download falharam: requests({e1}), urllib({e2})")
        
        # Processa o conteúdo baixado
        print("Processando arquivo ZIP...")
        
        # Extrai o ZIP em memória
        with zipfile.ZipFile(BytesIO(content)) as zip_file:
            # Lista arquivos no ZIP
            file_list = zip_file.namelist()
            csv_files = [f for f in file_list if f.endswith('.csv')]
            
            if not csv_files:
                raise ValueError("Nenhum arquivo CSV encontrado no ZIP")
            
            print(f"Arquivos CSV encontrados: {csv_files}")
            
            # Processa o primeiro arquivo CSV (ou combina múltiplos se necessário)
            csv_file = csv_files[0]
            print(f"Processando arquivo: {csv_file}")
            
            # Lê o CSV
            with zip_file.open(csv_file) as csv_data:
                df = pd.read_csv(csv_data, encoding='utf-8', sep=';', low_memory=False)
                
        print(f"Dados carregados. Shape: {df.shape}")
        print(f"Colunas: {list(df.columns)}")
        
        # Filtra dados dos últimos 3 anos (2024-2026)
        if 'DAT_HORA_AUTO_INFRACAO' in df.columns:
            # Converte a coluna de data
            df['DAT_HORA_AUTO_INFRACAO'] = pd.to_datetime(df['DAT_HORA_AUTO_INFRACAO'], errors='coerce')

            # Filtra pelos anos 2024, 2025 e 2026
            df = df[df['DAT_HORA_AUTO_INFRACAO'].dt.year.isin([2024, 2025, 2026])]
            print(f"Dados filtrados (2024-2026). Shape final: {df.shape}")
        
        # Remove valores NaN que podem causar problemas no JSON
        df = df.fillna('')
        
        return df
        
    except Exception as e:
        print(f"❌ Erro ao baixar/processar dados: {e}")
        raise

# --- 3. Processa os dados ---
df = download_and_process_data()

if df.empty:
    print("❌ Nenhum dado foi processado. Encerrando.")
    sys.exit(1)

print(f"Dados processados com sucesso. Total de {len(df)} registros.")

# --- 4. Configurar o cliente do Supabase ---
print("Conectando ao Supabase...")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
table_name = "ibama_infracao"

# --- 5. Limpar a tabela existente ---
print(f"Limpando a tabela '{table_name}' no Supabase...")
try:
    # Deleta todas as linhas da tabela
    delete_response = supabase.table(table_name).delete().neq('id', -1).execute()
    print("  Tabela limpa com sucesso.")
except Exception as e:
    print(f"❌ Erro ao limpar a tabela: {e}")
    raise

# --- 6. Upload dos dados em lotes ---
chunk_size = 500
total_chunks = (len(df) // chunk_size) + 1
print(f"Iniciando upload de {len(df)} registros em {total_chunks} lotes de {chunk_size}...")

successful_uploads = 0
failed_uploads = 0

for i in range(0, len(df), chunk_size):
    chunk_index = i // chunk_size + 1
    print(f"  Processando lote {chunk_index}/{total_chunks}...")
    
    chunk = df[i:i + chunk_size]
    data_to_insert = chunk.to_dict(orient='records')
    
    try:
        # Executa a inserção
        response = supabase.table(table_name).insert(data_to_insert).execute()
        
        # Verifica se houve erro
        if hasattr(response, 'error') and response.error:
            raise Exception(f"Erro da API do Supabase: {response.error.message}")
        
        successful_uploads += len(data_to_insert)
        print(f"  ✅ Lote {chunk_index} enviado com sucesso. {len(data_to_insert)} registros inseridos.")
        
        # Pausa para não sobrecarregar a API
        time.sleep(1)
        
    except Exception as e:
        failed_uploads += len(data_to_insert)
        print(f"  ❌ Falha no lote {chunk_index}: {e}")
        
        # Para uploads críticos, você pode querer interromper aqui
        # raise
        continue

# --- 7. Relatório final ---
print(f"\n{'='*50}")
print(f"RELATÓRIO FINAL:")
print(f"  Total de registros processados: {len(df)}")
print(f"  Uploads bem-sucedidos: {successful_uploads}")
print(f"  Uploads falharam: {failed_uploads}")
print(f"  Taxa de sucesso: {(successful_uploads/len(df))*100:.1f}%")

if failed_uploads == 0:
    print("✅ Upload para o Supabase concluído com sucesso!")
    sys.exit(0)
else:
    print("⚠️  Upload concluído com algumas falhas.")
    sys.exit(1 if failed_uploads > successful_uploads else 0)
