import requests
import zipfile
import pandas as pd
import io
from pathlib import Path
import urllib3

class DataLoader:
    def __init__(self, database=None):
        self.zip_url = None 
        self.database = database
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
    def download_and_process(self):
        """
        Download and process data, but ONLY for the years 2024, 2025 and 2026.
        """
        if not self.zip_url:
            print("Erro: A URL do ZIP não foi definida.")
            return False

        try:
            print("Baixando dados do IBAMA...")
            
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            print("Aviso: A verificação do certificado SSL será desabilitada para este download.")

            response = requests.get(self.zip_url, timeout=60, verify=False)
            response.raise_for_status()
                
            z = zipfile.ZipFile(io.BytesIO(response.content))
            
            # --- ALTERAÇÃO AQUI: Definindo os arquivos alvo ---
            target_files = [
                "auto_infracao_ano_2024.csv",
                "auto_infracao_ano_2025.csv",
                "auto_infracao_ano_2026.csv"
            ]
            print(f"Filtro aplicado: Processando apenas os arquivos {target_files}")

            all_data = []
            
            # Itera por todos os arquivos no ZIP, mas só processa os que estão na lista alvo
            for filename in z.namelist():
                # Verifica se o nome do arquivo está na nossa lista de alvos
                if any(target_file in filename for target_file in target_files):
                    print(f"Processando arquivo alvo: {filename}...")
                    try:
                        with z.open(filename) as f:
                            try:
                                df = pd.read_csv(f, encoding='utf-8', sep=';', on_bad_lines='skip', low_memory=False)
                            except UnicodeDecodeError:
                                print(f"  Falha com UTF-8, tentando com latin-1...")
                                with z.open(filename) as f_latin:
                                    df = pd.read_csv(f_latin, encoding='latin1', sep=';', on_bad_lines='skip', low_memory=False)
                            all_data.append(df)
                    except Exception as e:
                        print(f"  Erro ao processar o arquivo {filename}: {e}")
            
            if not all_data:
                print("Nenhum dado dos anos 2024, 2025 ou 2026 foi encontrado ou pôde ser extraído.")
                return False
                
            combined_df = pd.concat(all_data, ignore_index=True)
            print(f"Total de registros combinados (2024-2026): {len(combined_df)}")
            
            if self.database:
                self.database.save_dataframe(combined_df, "ibama_infracao")
                
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"Erro de rede ao baixar os dados: {e}")
            return False
        except Exception as e:
            print(f"Erro no processamento: {e}")
            return False
