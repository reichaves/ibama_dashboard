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

print("🌳 IBAMA Upload SIMPLIFICADO - Apenas Colunas Existentes v3.1...")

# --- 1. Configuração ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
IBAMA_ZIP_URL = os.getenv("IBAMA_ZIP_URL", 
    "https://dadosabertos.ibama.gov.br/dados/SIFISC/auto_infracao/auto_infracao/auto_infracao_csv.zip")

# --- 2. DEFINIÇÃO MANUAL DAS COLUNAS PRINCIPAIS ---
# Baseado no CSV verificado, usando apenas as colunas mais importantes
ESSENTIAL_COLUMNS = [
    'NUM_AUTO_INFRACAO',           # ID principal
    'DAT_HORA_AUTO_INFRACAO',      # Data
    'UF',                          # Estado
    'MUNICIPIO',                   # Cidade
    'TIPO_INFRACAO',               # Tipo (Flora, Fauna, etc)
    'VAL_AUTO_INFRACAO',           # Valor da multa
    'NOME_INFRATOR',               # Nome do infrator
    'CPF_CNPJ_INFRATOR',           # Documento
    'DES_AUTO_INFRACAO',           # Descrição
    'GRAVIDADE_INFRACAO',          # Gravidade
    'DES_STATUS_FORMULARIO',       # Status
    'UNID_ARRECADACAO',           # Unidade
    'COD_MUNICIPIO',               # Código município
    'NUM_LATITUDE_AUTO',           # Latitude
    'NUM_LONGITUDE_AUTO',          # Longitude
    'TIPO_AUTO',                   # Tipo de auto
    'DES_INFRACAO',                # Descrição infração
    'TP_PESSOA_INFRATOR',          # Tipo pessoa
    'MOTIVACAO_CONDUTA',           # Motivação
    'EFEITO_MEIO_AMBIENTE',        # Efeito ambiental
]

def test_supabase_columns(supabase_client):
    """Testa quais colunas realmente existem no Supabase."""
    print("🔍 Testando colunas existentes no Supabase...")
    
    working_columns = []
    
    for col in ESSENTIAL_COLUMNS:
        try:
            # Testa se consegue fazer SELECT na coluna
            query = f'"{col}"'
            result = supabase_client.table('ibama_infracao').select(query).limit(1).execute()
            working_columns.append(col)
            print(f"  ✅ {col}")
            
        except Exception as e:
            print(f"  ❌ {col} - {str(e)[:50]}...")
    
    print(f"✅ Colunas funcionais: {len(working_columns)} de {len(ESSENTIAL_COLUMNS)}")
    return working_columns

# --- 3. Download (mesmo código) ---
def download_with_multiple_methods(url):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    print(f"🔄 Baixando de: {url}")
    session = requests.Session()
    session.verify = False
    response = session.get(url, timeout=300, 
                          headers={'User-Agent': 'Mozilla/5.0'})
    response.raise_for_status()
    return response.content

def read_csv_robust(zip_file, csv_file):
    encodings = ['utf-8', 'latin1', 'cp1252']
    separators = [';', ',']
    
    for encoding in encodings:
        for sep in separators:
            try:
                with zip_file.open(csv_file) as csv_data:
                    df = pd.read_csv(csv_data, encoding=encoding, sep=sep, low_memory=False)
                    if len(df.columns) > 5 and len(df) > 0:
                        return df
            except:
                continue
    return None

def process_ibama_data():
    """Processa dados do IBAMA."""
    print("📥 Baixando dados do IBAMA...")
    
    content = download_with_multiple_methods(IBAMA_ZIP_URL)
    
    print("📦 Processando ZIP...")
    with zipfile.ZipFile(BytesIO(content)) as zip_file:
        csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
        
        # Foca em 2024-2026
        target_files = [f for f in csv_files if any(year in f for year in ['2024', '2025', '2026'])]

        if not target_files:
            target_files = sorted(csv_files, reverse=True)[:3]
        
        print(f"📄 Processando {len(target_files)} arquivos")
        
        all_dataframes = []
        for csv_file in target_files:
            df_temp = read_csv_robust(zip_file, csv_file)
            if df_temp is not None:
                print(f"  ✅ {csv_file}: {len(df_temp):,} registros")
                all_dataframes.append(df_temp)
        
        df = pd.concat(all_dataframes, ignore_index=True, sort=False)
        print(f"📊 Total combinado: {len(df):,} registros")
        
    return df

# --- 4. Processamento simplificado ---
def clean_data_simple(df, working_columns):
    """Limpeza simplificada focada apenas nas colunas funcionais."""
    print(f"🧹 Limpeza simplificada...")
    
    # Filtra apenas colunas que funcionam
    available_cols = [col for col in working_columns if col in df.columns]
    df_filtered = df[available_cols].copy()
    
    print(f"📊 Colunas utilizadas: {len(available_cols)}")
    print(f"📊 Registros: {len(df_filtered):,}")
    
    # Limpeza básica
    for col in df_filtered.columns:
        df_filtered[col] = df_filtered[col].apply(lambda x: None if pd.isna(x) else str(x).strip() if x != '' else None)
    
    return df_filtered

def upload_simple(df_clean, supabase_client):
    """Upload simplificado."""
    print("🚀 Iniciando upload simplificado...")
    
    table_name = "ibama_infracao"
    
    # Limpa tabela
    try:
        supabase_client.table(table_name).delete().neq('NUM_AUTO_INFRACAO', 'IMPOSSIBLE_VALUE').execute()
        print("✅ Tabela limpa")
    except Exception as e:
        print(f"⚠️ Aviso ao limpar: {e}")
    
    # Upload em lotes pequenos
    chunk_size = 100  # Bem pequeno para máxima compatibilidade
    successful = 0
    failed = 0
    
    for i in range(0, len(df_clean), chunk_size):
        chunk_num = (i // chunk_size) + 1
        total_chunks = (len(df_clean) // chunk_size) + 1
        
        chunk = df_clean[i:i + chunk_size]
        
        try:
            data_to_insert = chunk.to_dict(orient='records')
            
            # Limpa dados especiais
            for record in data_to_insert:
                for key, value in record.items():
                    if isinstance(value, (np.integer, np.int64)):
                        record[key] = int(value)
                    elif isinstance(value, (np.floating, np.float64)):
                        record[key] = float(value) if not np.isnan(value) else None
                    elif pd.isna(value):
                        record[key] = None
            
            # Upload
            response = supabase_client.table(table_name).insert(data_to_insert).execute()
            
            successful += len(data_to_insert)
            print(f"  📤 Lote {chunk_num}/{total_chunks}: ✅ {len(data_to_insert)} registros")
            
            time.sleep(0.2)  # Pausa pequena
            
        except Exception as e:
            failed += len(chunk)
            print(f"  📤 Lote {chunk_num}/{total_chunks}: ❌ {str(e)[:60]}...")
            
            # Para no primeiro erro para debug
            if chunk_num == 1:
                print(f"🔍 Primeiro lote falhou - dados:")
                print(f"    Colunas: {list(chunk.columns)}")
                print(f"    Amostra: {chunk.iloc[0].to_dict()}")
                break
    
    return successful, failed

# --- 5. Execução principal ---
def main():
    try:
        # 1. Conecta Supabase
        print("🔗 Conectando ao Supabase...")
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # 2. Testa colunas existentes
        working_columns = test_supabase_columns(supabase)
        
        if len(working_columns) < 5:
            print("❌ Muito poucas colunas funcionais - verifique a tabela Supabase")
            return 1
        
        # 3. Processa dados do IBAMA
        df = process_ibama_data()
        
        # 4. Limpa dados
        df_clean = clean_data_simple(df, working_columns)
        
        if df_clean.empty:
            print("❌ Nenhum dado válido após limpeza")
            return 1
        
        # 5. Upload
        successful, failed = upload_simple(df_clean, supabase)
        
        # 6. Relatório
        total = successful + failed
        success_rate = (successful / total * 100) if total > 0 else 0
        
        print(f"\n{'='*50}")
        print(f"📊 RELATÓRIO FINAL:")
        print(f"  📥 Total: {total:,} registros")
        print(f"  ✅ Sucesso: {successful:,} ({success_rate:.1f}%)")
        print(f"  ❌ Falhas: {failed:,}")
        print(f"  📋 Colunas: {len(working_columns)}")
        print(f"{'='*50}")
        
        if success_rate >= 80:
            print("🎉 Upload bem-sucedido!")
            return 0
        else:
            print("❌ Muitas falhas no upload")
            return 1
            
    except Exception as e:
        print(f"💥 Erro crítico: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
