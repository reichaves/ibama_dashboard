import streamlit as st
import pandas as pd
import numpy as np
import re
from typing import Dict, Any, Optional
from fuzzywuzzy import process

class ChatbotFixed:
    def __init__(self, llm_integration=None):
        self.llm_integration = llm_integration
        self.cached_data = None
        self.llm_config = {
            "provider": "groq",
            "temperature": 0.0,
            "max_tokens": 500
        }
        
    def set_llm_config(self, provider="groq", temperature=0.0, max_tokens=500):
        """Define configurações do LLM."""
        self.llm_config = {
            "provider": provider,
            "temperature": temperature,
            "max_tokens": max_tokens
        }
        
    def initialize_chat_state(self):
        """Inicializa o estado do chat."""
        if "messages" not in st.session_state:
            st.session_state.messages = []
    
    def _get_cached_data(self) -> pd.DataFrame:
        """Obtém dados em cache para análises rápidas."""
        if self.cached_data is None:
            try:
                if (hasattr(self.llm_integration, 'database') and 
                    self.llm_integration.database.is_cloud and 
                    self.llm_integration.database.supabase):
                    
                    try:
                        from src.utils.supabase_utils import SupabasePaginator
                        paginator = SupabasePaginator(self.llm_integration.database.supabase)
                        self.cached_data = paginator.get_all_records()
                        
                        # CORREÇÃO: Processa os dados carregados
                        self.cached_data = self._process_cached_data(self.cached_data)
                        print(f"✅ Cache carregado e processado: {len(self.cached_data)} registros")
                    except ImportError:
                        result = self.llm_integration.database.supabase.table('ibama_infracao').select('*').limit(50000).execute()
                        self.cached_data = pd.DataFrame(result.data)
                        self.cached_data = self._process_cached_data(self.cached_data)
                else:
                    self.cached_data = pd.DataFrame()
                    
            except Exception as e:
                print(f"Erro ao carregar cache: {e}")
                self.cached_data = pd.DataFrame()
        
        return self.cached_data
    
    def _process_cached_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Processa os dados carregados para análises corretas."""
        if df.empty:
            return df
            
        # Garante que os dados são únicos
        if 'NUM_AUTO_INFRACAO' in df.columns:
            df = df.drop_duplicates(subset=['NUM_AUTO_INFRACAO'], keep='first')
        
        # Converte valores monetários para float
        if 'VAL_AUTO_INFRACAO' in df.columns:
            df['VAL_AUTO_INFRACAO_NUMERIC'] = pd.to_numeric(
                df['VAL_AUTO_INFRACAO'].astype(str).str.replace(',', '.'), 
                errors='coerce'
            )
        
        # Classifica CPF/CNPJ corretamente
        if 'CPF_CNPJ_INFRATOR' in df.columns:
            df['DOC_TYPE'] = df['CPF_CNPJ_INFRATOR'].apply(self._classify_cpf_cnpj)
        
        # Limpa campos de texto
        text_columns = ['NOME_INFRATOR', 'TIPO_INFRACAO', 'UF', 'MUNICIPIO']
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
        
        return df
    
    def _classify_cpf_cnpj(self, doc: str) -> str:
        """Classifica documento como CPF, CNPJ ou Unknown."""
        if pd.isna(doc) or doc == '':
            return 'Unknown'
        
        # Remove caracteres não numéricos
        cleaned_doc = ''.join(filter(str.isdigit, str(doc)))
        
        if len(cleaned_doc) == 11:
            return 'CPF'
        elif len(cleaned_doc) == 14:
            return 'CNPJ'
        else:
            return 'Unknown'
    
    def _format_currency_brazilian(self, value: float) -> str:
        """Formata valor como moeda brasileira."""
        if pd.isna(value) or value == 0:
            return "R$ 0,00"
        
        if value >= 1_000_000_000:
            return f"R$ {value/1_000_000_000:.1f} bilhões"
        elif value >= 1_000_000:
            return f"R$ {value/1_000_000:.1f} milhões"
        elif value >= 1_000:
            return f"R$ {value:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        else:
            return f"R$ {value:.2f}".replace('.', ',')
    
    def _find_similar_names(self, search_name: str, df: pd.DataFrame, min_score: int = 90) -> list:
        """Encontra nomes similares usando fuzzy matching."""
        try:
            # Importa fuzzywuzzy apenas quando necessário
            from fuzzywuzzy import process
            
            unique_names = df['NOME_INFRATOR'].dropna().unique()
            matches = process.extractBests(search_name, unique_names, score_cutoff=min_score, limit=5)
            return [match[0] for match in matches]
        except ImportError:
            # Fallback: busca simples por substring
            search_lower = search_name.lower()
            unique_names = df['NOME_INFRATOR'].dropna().unique()
            return [name for name in unique_names if search_lower in name.lower()][:5]
    
    def _parse_question(self, question: str) -> dict:
        """Extrai intent e filtros da pergunta usando regex e mapeamento semântico."""
        q = question.lower()

        # Anos (ex: 2024, 2025, 2026)
        years = [int(y) for y in re.findall(r'\b(202[0-9])\b', question)]

        # Tipos de infração
        tipo_map = {
            "flora": "Flora", "fauna": "Fauna", "pesca": "Pesca",
            "administrativ": "Administrativo", "uso nocivo": "Uso Nocivo",
            "mineração": "Mineração", "mineracao": "Mineração",
        }
        tipos = [v for k, v in tipo_map.items() if k in q]

        # Estados (nome completo ou sigla como palavra inteira)
        uf_map = {
            "acre": "AC", "alagoas": "AL", "amapá": "AP", "amapa": "AP",
            "amazonas": "AM", "bahia": "BA", "ceará": "CE", "ceara": "CE",
            "distrito federal": "DF", "espírito santo": "ES", "espirito santo": "ES",
            "goiás": "GO", "goias": "GO", "maranhão": "MA", "maranhao": "MA",
            "mato grosso do sul": "MS", "mato grosso": "MT",
            "minas gerais": "MG", "pará": "PA", "para": "PA", "paraíba": "PB",
            "paraiba": "PB", "paraná": "PR", "parana": "PR",
            "pernambuco": "PE", "piauí": "PI", "piaui": "PI",
            "rio de janeiro": "RJ", "rio grande do norte": "RN",
            "rio grande do sul": "RS", "rondônia": "RO", "rondonia": "RO",
            "roraima": "RR", "santa catarina": "SC", "são paulo": "SP",
            "sao paulo": "SP", "sergipe": "SE", "tocantins": "TO",
        }
        ufs = []
        for name, code in uf_map.items():
            if name in q and code not in ufs:
                ufs.append(code)
        # Siglas isoladas (ex: "no PA", "em SP")
        for code in ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MS","MT",
                     "MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"]:
            if re.search(r'\b' + code + r'\b', question) and code not in ufs:
                ufs.append(code)

        # Gravidades
        gravidades = []
        if "baixa" in q: gravidades.append("Baixa")
        if "média" in q or "media" in q: gravidades.append("Média")
        if "alta" in q: gravidades.append("Alta")

        # Tipo de documento/pessoa
        doc_type = None
        if "empresas" in q or "cnpj" in q: doc_type = "CNPJ"
        elif any(k in q for k in ["pessoas", "cpf", "pessoa física", "pessoa fisica", "pessoa natural"]): doc_type = "CPF"

        # Intent (ordem: mais específico primeiro)
        intent = "general"
        if self._is_specific_name_search(question):
            intent = "specific_name"
        elif any(k in q for k in ["por tipo", "tipos de infra", "dividido por tipo", "valor por tipo", "distribuição por tipo"]):
            intent = "type_distribution"
        elif any(k in q for k in ["gravidade", "por gravidade", "distribuição por gravidade"]):
            intent = "gravity_distribution"
        elif any(k in q for k in ["infratores", "maiores infratores", "quais infratores", "mais multas", "soma de valores"]):
            intent = "top_offenders"
        elif any(k in q for k in ["municípios", "cidades", "município", "municipios"]):
            intent = "top_municipalities"
        elif any(k in q for k in ["estados", "por uf", "por estado", "top estados"]):
            intent = "top_states"
        elif any(k in q for k in ["valor total", "soma de valor", "soma dos valor", "quanto em multas", "total em multas", "total de multas"]):
            intent = "sum_value"
        elif any(k in q for k in ["quantas", "quantos", "número de infra", "total de infra", "conte", "count"]):
            intent = "count"
        elif any(k in q for k in ["soma", "valores", "multas totais"]) and "tipo" in q:
            intent = "type_distribution"
        elif any(k in q for k in ["total", "resumo", "visão geral", "visao geral"]):
            intent = "count"

        return {
            "intent": intent,
            "filters": {"years": years, "ufs": ufs, "tipos": tipos,
                        "gravidades": gravidades, "doc_type": doc_type},
        }

    def _apply_filters(self, df: pd.DataFrame, filters: dict) -> pd.DataFrame:
        """Aplica todos os filtros extraídos da pergunta ao DataFrame."""
        df_f = df.copy()

        if filters.get("years") and 'DAT_HORA_AUTO_INFRACAO' in df_f.columns:
            try:
                years_col = pd.to_datetime(df_f['DAT_HORA_AUTO_INFRACAO'], errors='coerce').dt.year
                df_f = df_f[years_col.isin(filters["years"])]
            except Exception:
                pass

        if filters.get("tipos") and 'TIPO_INFRACAO' in df_f.columns:
            tipos_lower = [t.lower() for t in filters["tipos"]]
            df_f = df_f[df_f['TIPO_INFRACAO'].astype(str).str.lower().isin(tipos_lower)]

        if filters.get("ufs") and 'UF' in df_f.columns:
            df_f = df_f[df_f['UF'].isin(filters["ufs"])]

        if filters.get("gravidades") and 'GRAVIDADE_INFRACAO' in df_f.columns:
            df_f = df_f[df_f['GRAVIDADE_INFRACAO'].isin(filters["gravidades"])]

        if filters.get("doc_type") and 'DOC_TYPE' in df_f.columns:
            df_f = df_f[df_f['DOC_TYPE'] == filters["doc_type"]]

        return df_f

    def _build_filter_description(self, filters: dict) -> str:
        """Gera descrição legível dos filtros ativos."""
        parts = []
        if filters.get("years"):
            parts.append(f"Ano(s): {', '.join(map(str, filters['years']))}")
        if filters.get("tipos"):
            parts.append(f"Tipo: {', '.join(filters['tipos'])}")
        if filters.get("ufs"):
            parts.append(f"UF: {', '.join(filters['ufs'])}")
        if filters.get("gravidades"):
            parts.append(f"Gravidade: {', '.join(filters['gravidades'])}")
        if filters.get("doc_type"):
            label = "Empresas (CNPJ)" if filters["doc_type"] == "CNPJ" else "Pessoas Físicas (CPF)"
            parts.append(f"Infrator: {label}")
        return " | ".join(parts) if parts else "Todos os dados"

    def _intent_count(self, df: pd.DataFrame, filter_desc: str) -> Dict[str, Any]:
        """Conta infrações com os filtros aplicados."""
        count = len(df)
        if count == 0:
            return {"answer": f"❌ Nenhuma infração encontrada para: **{filter_desc}**", "source": "data_analysis"}

        answer = f"**📊 Contagem de Infrações**\n\n"
        answer += f"**Filtros:** {filter_desc}\n\n"
        answer += f"**Total:** {count:,} infrações\n"
        if 'UF' in df.columns:
            answer += f"**Estados:** {df['UF'].nunique()} diferentes\n"
        if 'MUNICIPIO' in df.columns:
            answer += f"**Municípios:** {df['MUNICIPIO'].nunique():,} diferentes\n"
        if 'VAL_AUTO_INFRACAO_NUMERIC' in df.columns:
            total_val = df['VAL_AUTO_INFRACAO_NUMERIC'].sum()
            if total_val > 0:
                answer += f"**Valor total de multas:** {self._format_currency_brazilian(total_val)}\n"
        return {"answer": answer, "source": "data_analysis"}

    def _intent_sum_value(self, df: pd.DataFrame, filter_desc: str) -> Dict[str, Any]:
        """Soma valores de multas com os filtros aplicados."""
        if 'VAL_AUTO_INFRACAO_NUMERIC' not in df.columns:
            return {"answer": "❌ Dados de valores não disponíveis.", "source": "error"}
        df_val = df[df['VAL_AUTO_INFRACAO_NUMERIC'].notna() & (df['VAL_AUTO_INFRACAO_NUMERIC'] > 0)]
        if df_val.empty:
            return {"answer": f"❌ Nenhum valor encontrado para: **{filter_desc}**", "source": "data_analysis"}

        total = df_val['VAL_AUTO_INFRACAO_NUMERIC'].sum()
        count = len(df_val)
        answer = f"**💰 Valor Total de Multas**\n\n"
        answer += f"**Filtros:** {filter_desc}\n\n"
        answer += f"**Valor total:** {self._format_currency_brazilian(total)}\n"
        answer += f"**Infrações com valor:** {count:,}\n"
        answer += f"**Valor médio por infração:** {self._format_currency_brazilian(total / count)}\n"
        return {"answer": answer, "source": "data_analysis"}

    def _answer_with_data_analysis(self, question: str) -> Dict[str, Any]:
        """Responde perguntas usando parser de intenção + filtros combinados."""
        df = self._get_cached_data()
        if df.empty:
            return {"answer": "❌ Não foi possível carregar os dados para análise.", "source": "error"}

        try:
            parsed = self._parse_question(question)
            filters = parsed["filters"]
            intent = parsed["intent"]
            has_filters = any([filters["years"], filters["ufs"], filters["tipos"],
                               filters["gravidades"], filters["doc_type"]])

            df_filtered = self._apply_filters(df, filters)
            filter_desc = self._build_filter_description(filters)

            if intent == "count":
                return self._intent_count(df_filtered, filter_desc)

            elif intent == "sum_value":
                if has_filters:
                    return self._intent_sum_value(df_filtered, filter_desc)
                return self._analyze_values_by_type_corrected(df, question)

            elif intent == "type_distribution":
                return self._analyze_values_by_type_corrected(df_filtered, question)

            elif intent == "gravity_distribution":
                return self._analyze_by_gravity_corrected(df_filtered, question)

            elif intent == "top_offenders":
                if filters["doc_type"] == "CPF":
                    return self._analyze_top_individuals_by_value(df_filtered, question)
                elif filters["doc_type"] == "CNPJ":
                    return self._analyze_top_companies_by_value(df_filtered, question)
                return self._analyze_top_offenders_by_value(df_filtered, question)

            elif intent == "top_states":
                return self._analyze_top_states(df_filtered, question)

            elif intent == "top_municipalities":
                return self._analyze_top_municipalities(df_filtered, question)

            elif intent == "specific_name":
                return self._analyze_specific_offender_corrected(df, question)

            else:
                # Fallback: se há filtros ativos, mostra contagem; senão, resumo geral
                if has_filters:
                    return self._intent_count(df_filtered, filter_desc)
                return self._analyze_general(df, question)

        except Exception as e:
            return {"answer": f"❌ Erro na análise: {str(e)}", "source": "error"}
    
    def _analyze_values_by_type_corrected(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """CORREÇÃO: Análise correta de valores por tipo de infração."""
        try:
            if 'TIPO_INFRACAO' not in df.columns or 'VAL_AUTO_INFRACAO_NUMERIC' not in df.columns:
                return {"answer": "❌ Colunas necessárias não encontradas.", "source": "error"}
            
            # Remove valores inválidos
            df_clean = df[
                df['TIPO_INFRACAO'].notna() & 
                df['VAL_AUTO_INFRACAO_NUMERIC'].notna() &
                (df['TIPO_INFRACAO'] != '') &
                (df['VAL_AUTO_INFRACAO_NUMERIC'] > 0)
            ]
            
            if df_clean.empty:
                return {"answer": "❌ Nenhum dado válido encontrado.", "source": "error"}
            
            # CORREÇÃO: Soma valores por tipo (não conta registros)
            values_by_type = df_clean.groupby('TIPO_INFRACAO')['VAL_AUTO_INFRACAO_NUMERIC'].sum().sort_values(ascending=False)
            
            total_value = values_by_type.sum()
            
            answer = "**💰 Valor Total de Multas por Tipo de Infração:**\n\n"
            
            for i, (tipo, valor) in enumerate(values_by_type.head(10).items(), 1):
                percentage = (valor / total_value) * 100
                answer += f"{i}. **{tipo.title()}**: {self._format_currency_brazilian(valor)} ({percentage:.1f}%)\n"
            
            answer += f"\n📊 **Total Geral**: {self._format_currency_brazilian(total_value)}"
            answer += f"\n📋 **Tipos analisados**: {len(values_by_type)} diferentes"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro na análise de valores por tipo: {e}", "source": "error"}
    
    def _analyze_by_gravity_corrected(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """CORREÇÃO: Análise correta por gravidade incluindo sem avaliação."""
        try:
            if 'GRAVIDADE_INFRACAO' not in df.columns:
                return {"answer": "❌ Coluna de gravidade não encontrada.", "source": "error"}
            
            # Substitui valores nulos/vazios por "Sem avaliação"
            df_processed = df.copy()
            df_processed['GRAVIDADE_INFRACAO'] = df_processed['GRAVIDADE_INFRACAO'].fillna('Sem avaliação')
            df_processed['GRAVIDADE_INFRACAO'] = df_processed['GRAVIDADE_INFRACAO'].replace('', 'Sem avaliação')
            
            # Conta infrações por gravidade
            gravity_counts = df_processed['GRAVIDADE_INFRACAO'].value_counts()
            total_infractions = gravity_counts.sum()
            
            answer = "**⚖️ Distribuição de Infrações por Gravidade:**\n\n"
            
            # Ordem específica para apresentação
            gravity_order = ['Baixa', 'Média', 'Sem avaliação']
            
            for gravity in gravity_order:
                if gravity in gravity_counts.index:
                    count = gravity_counts[gravity]
                    percentage = (count / total_infractions) * 100
                    
                    # Emoji por gravidade
                    if gravity == 'Baixa':
                        emoji = "🟢"
                    elif gravity == 'Média':
                        emoji = "🟡"
                    else:
                        emoji = "⚫"
                    
                    answer += f"{emoji} **{gravity}**: {count:,} infrações ({percentage:.1f}%)\n"
            
            # Adiciona outras gravidades não previstas
            for gravity, count in gravity_counts.items():
                if gravity not in gravity_order:
                    percentage = (count / total_infractions) * 100
                    answer += f"🔵 **{gravity}**: {count:,} infrações ({percentage:.1f}%)\n"
            
            answer += f"\n📊 **Total analisado**: {total_infractions:,} infrações"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro na análise por gravidade: {e}", "source": "error"}
    
    def _analyze_top_offenders_by_value(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """CORREÇÃO: Top infratores por VALOR total (não quantidade)."""
        try:
            required_cols = ['NOME_INFRATOR', 'CPF_CNPJ_INFRATOR', 'VAL_AUTO_INFRACAO_NUMERIC']
            if not all(col in df.columns for col in required_cols):
                return {"answer": "❌ Colunas necessárias não encontradas.", "source": "error"}
            
            # Remove valores inválidos
            df_clean = df[
                df['NOME_INFRATOR'].notna() & 
                df['CPF_CNPJ_INFRATOR'].notna() &
                df['VAL_AUTO_INFRACAO_NUMERIC'].notna() &
                (df['NOME_INFRATOR'] != '') & 
                (df['CPF_CNPJ_INFRATOR'] != '') &
                (df['VAL_AUTO_INFRACAO_NUMERIC'] > 0)
            ]
            
            if df_clean.empty:
                return {"answer": "❌ Dados válidos não disponíveis.", "source": "error"}
            
            # CORREÇÃO: Agrupa por infrator e SOMA valores (não conta registros)
            top_offenders = df_clean.groupby(['NOME_INFRATOR', 'CPF_CNPJ_INFRATOR'])['VAL_AUTO_INFRACAO_NUMERIC'].sum().sort_values(ascending=False).head(10)
            
            answer = "**💰 Top 10 Infratores por Valor Total de Multas:**\n\n"
            
            for i, ((name, doc), value) in enumerate(top_offenders.items(), 1):
                # Mascara documentos para privacidade
                if len(str(doc).replace('.', '').replace('-', '').replace('/', '')) == 11:  # CPF
                    doc_masked = f"{str(doc)[:3]}.***.***-{str(doc)[-2:]}"
                else:  # CNPJ
                    doc_masked = str(doc)
                
                display_name = name[:50] + "..." if len(name) > 50 else name
                answer += f"{i}. **{display_name.title()}**\n"
                answer += f"   • Valor total: {self._format_currency_brazilian(value)}\n"
                answer += f"   • Documento: {doc_masked}\n\n"
            
            total_analyzed = top_offenders.sum()
            answer += f"📊 **Total dos top 10**: {self._format_currency_brazilian(total_analyzed)}"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro na análise de top infratores: {e}", "source": "error"}
    
    def _analyze_top_individuals_by_value(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """CORREÇÃO: Top pessoas físicas por valor."""
        try:
            if 'DOC_TYPE' not in df.columns:
                return {"answer": "❌ Classificação de documentos não disponível.", "source": "error"}
            
            # Filtra apenas pessoas físicas (CPF)
            df_cpf = df[df['DOC_TYPE'] == 'CPF']
            
            if df_cpf.empty:
                return {"answer": "❌ Nenhuma pessoa física encontrada.", "source": "error"}
            
            return self._analyze_top_offenders_by_value(df_cpf, question.replace("pessoas", "pessoas físicas"))
            
        except Exception as e:
            return {"answer": f"❌ Erro na análise de pessoas físicas: {e}", "source": "error"}
    
    def _analyze_top_companies_by_value(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """CORREÇÃO: Top empresas por valor."""
        try:
            if 'DOC_TYPE' not in df.columns:
                return {"answer": "❌ Classificação de documentos não disponível.", "source": "error"}
            
            # Filtra apenas empresas (CNPJ)
            df_cnpj = df[df['DOC_TYPE'] == 'CNPJ']
            
            if df_cnpj.empty:
                return {"answer": "❌ Nenhuma empresa encontrada.", "source": "error"}
            
            return self._analyze_top_offenders_by_value(df_cnpj, question.replace("empresas", "empresas"))
            
        except Exception as e:
            return {"answer": f"❌ Erro na análise de empresas: {e}", "source": "error"}
    
    def _is_specific_name_search(self, question: str) -> bool:
        """Detecta se a pergunta busca por um nome específico."""
        indicators = [
            "shell brasil", "petrobras", "vale", "empresa", "ltda", "sa", 
            "tem infracoes", "infrações de", "qual tipo"
        ]
        return any(indicator in question.lower() for indicator in indicators)
    
    def _analyze_specific_offender_corrected(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """CORREÇÃO: Busca por infrator específico com fuzzy matching."""
        try:
            # Extrai nome da pergunta (simples heurística)
            question_words = question.lower().split()
            
            # Tenta identificar nomes de empresas conhecidos
            known_companies = {
                "shell": "Shell Brasil",
                "petrobras": "Petrobras",
                "vale": "Vale",
                "rumo": "Rumo"
            }
            
            search_name = None
            for key, company in known_companies.items():
                if key in question.lower():
                    search_name = company
                    break
            
            if not search_name:
                # Fallback: pega palavras capitalizadas como possível nome
                potential_names = [word for word in question.split() if word[0].isupper() and len(word) > 3]
                if potential_names:
                    search_name = " ".join(potential_names[:3])  # Máximo 3 palavras
            
            if not search_name:
                return {"answer": "❌ Não consegui identificar o nome da empresa na pergunta.", "source": "error"}
            
            # Busca nomes similares
            similar_names = self._find_similar_names(search_name, df)
            
            if not similar_names:
                return {"answer": f"❌ Nenhuma empresa encontrada similar a '{search_name}'.", "source": "error"}
            
            # Filtra dados para os nomes encontrados
            df_filtered = df[df['NOME_INFRATOR'].isin(similar_names)]
            
            if df_filtered.empty:
                return {"answer": "❌ Nenhum dado encontrado para os nomes similares.", "source": "error"}
            
            # Analisa tipos de infrações
            if 'TIPO_INFRACAO' not in df_filtered.columns:
                return {"answer": "❌ Coluna de tipos de infração não encontrada.", "source": "error"}
            
            infraction_types = df_filtered['TIPO_INFRACAO'].value_counts()
            
            answer = f"**🏢 Infrações encontradas para '{search_name}':**\n\n"
            
            if len(similar_names) > 1:
                answer += f"**Nomes similares encontrados:** {', '.join(similar_names)}\n\n"
            
            answer += "**Tipos de infrações:**\n"
            for tipo, count in infraction_types.items():
                answer += f"• **{tipo}**: {count} infrações\n"
            
            # Adiciona valor total se disponível
            if 'VAL_AUTO_INFRACAO_NUMERIC' in df_filtered.columns:
                total_value = df_filtered['VAL_AUTO_INFRACAO_NUMERIC'].sum()
                if total_value > 0:
                    answer += f"\n💰 **Valor total das multas**: {self._format_currency_brazilian(total_value)}"
            
            answer += f"\n📊 **Total de infrações**: {len(df_filtered)}"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro na busca específica: {e}", "source": "error"}
    
    def _analyze_geographic_specific_corrected(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """CORREÇÃO: Análise geográfica específica com filtros corretos."""
        try:
            question_lower = question.lower()
            
            # Filtros baseados na pergunta
            filters = {}
            
            # Estado
            if "pará" in question_lower or "pa" in question_lower:
                filters['UF'] = 'PA'
            
            # Tipo de infração
            if "fauna" in question_lower:
                filters['TIPO_INFRACAO'] = 'Fauna'
            elif "flora" in question_lower:
                filters['TIPO_INFRACAO'] = 'Flora'
            
            # Tipo de documento
            if "empresas" in question_lower or "cnpj" in question_lower:
                filters['DOC_TYPE'] = 'CNPJ'
            elif "pessoas" in question_lower or "cpf" in question_lower:
                filters['DOC_TYPE'] = 'CPF'
            
            # Aplica filtros
            df_filtered = df.copy()
            for column, value in filters.items():
                if column in df_filtered.columns:
                    df_filtered = df_filtered[df_filtered[column] == value]
            
            if df_filtered.empty:
                filter_description = ', '.join([f"{k}={v}" for k, v in filters.items()])
                return {"answer": f"❌ Nenhum dado encontrado para os filtros: {filter_description}", "source": "error"}
            
            # Analisa por valor se solicitado
            if "soma de valores" in question_lower or "valor" in question_lower:
                if 'VAL_AUTO_INFRACAO_NUMERIC' not in df_filtered.columns:
                    return {"answer": "❌ Dados de valores não disponíveis.", "source": "error"}
                
                # Agrupa por infrator e soma valores
                top_by_value = df_filtered.groupby(['NOME_INFRATOR', 'CPF_CNPJ_INFRATOR'])['VAL_AUTO_INFRACAO_NUMERIC'].sum().sort_values(ascending=False).head(10)
                
                filter_description = ', '.join([f"{k}: {v}" for k, v in filters.items()])
                answer = f"**💰 Top 10 por Valor Total - {filter_description}:**\n\n"
                
                for i, ((name, doc), value) in enumerate(top_by_value.items(), 1):
                    display_name = name[:40] + "..." if len(name) > 40 else name
                    answer += f"{i}. **{display_name.title()}**\n"
                    answer += f"   • Valor: {self._format_currency_brazilian(value)}\n"
                    answer += f"   • Doc: {doc}\n\n"
                
                total_analyzed = top_by_value.sum()
                answer += f"📊 **Total dos top 10**: {self._format_currency_brazilian(total_analyzed)}"
                
            else:
                # Análise por quantidade de infrações
                top_by_count = df_filtered.groupby(['NOME_INFRATOR', 'CPF_CNPJ_INFRATOR']).size().sort_values(ascending=False).head(10)
                
                filter_description = ', '.join([f"{k}: {v}" for k, v in filters.items()])
                answer = f"**📊 Top 10 por Quantidade - {filter_description}:**\n\n"
                
                for i, ((name, doc), count) in enumerate(top_by_count.items(), 1):
                    display_name = name[:40] + "..." if len(name) > 40 else name
                    answer += f"{i}. **{display_name.title()}**: {count} infrações\n"
                
                answer += f"\n📊 **Total de registros**: {len(df_filtered)}"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro na análise geográfica: {e}", "source": "error"}
    
    # Métodos originais mantidos para compatibilidade
    def _analyze_top_states(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """Analisa os estados com mais infrações."""
        try:
            if 'UF' not in df.columns:
                return {"answer": "❌ Coluna UF não encontrada.", "source": "error"}
            
            state_counts = df['UF'].value_counts().head(10)
            
            answer = "**🏆 Top Estados com Mais Infrações:**\n\n"
            for i, (uf, count) in enumerate(state_counts.items(), 1):
                percentage = (count / state_counts.sum()) * 100
                answer += f"{i}. **{uf}**: {count:,} infrações ({percentage:.1f}%)\n"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro ao analisar estados: {e}", "source": "error"}
    
    def _analyze_top_municipalities(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """Analisa os municípios com mais infrações."""
        try:
            if 'MUNICIPIO' not in df.columns or 'UF' not in df.columns:
                return {"answer": "❌ Colunas necessárias não encontradas.", "source": "error"}
            
            df_clean = df[df['MUNICIPIO'].notna() & df['UF'].notna()]
            muni_counts = df_clean.groupby(['MUNICIPIO', 'UF']).size().sort_values(ascending=False).head(10)
            
            answer = "**🏙️ Top Municípios com Mais Infrações:**\n\n"
            for i, ((municipio, uf), count) in enumerate(muni_counts.items(), 1):
                answer += f"{i}. **{municipio.title()} ({uf})**: {count:,} infrações\n"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro ao analisar municípios: {e}", "source": "error"}
    
    def _analyze_totals(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """Analisa totais gerais."""
        try:
            total_records = len(df)
            total_states = df['UF'].nunique() if 'UF' in df.columns else 0
            total_municipalities = df['MUNICIPIO'].nunique() if 'MUNICIPIO' in df.columns else 0
            
            answer = "**📊 Resumo Geral dos Dados:**\n\n"
            answer += f"• **Total de infrações**: {total_records:,}\n"
            answer += f"• **Estados envolvidos**: {total_states}\n"
            answer += f"• **Municípios afetados**: {total_municipalities:,}\n"
            
            return {"answer": answer, "source": "data_analysis"}
            
        except Exception as e:
            return {"answer": f"❌ Erro ao calcular totais: {e}", "source": "error"}
    
    def _analyze_general(self, df: pd.DataFrame, question: str) -> Dict[str, Any]:
        """Análise genérica dos dados."""
        if not df.empty:
            total_records = len(df)
            total_states = df['UF'].nunique() if 'UF' in df.columns else 0
            total_municipalities = df['MUNICIPIO'].nunique() if 'MUNICIPIO' in df.columns else 0
            
            answer = f"📊 **Sistema de Análise IBAMA:**\n\n"
            answer += f"Tenho {total_records:,} infrações disponíveis para análise.\n\n"
            answer += f"**Dados incluem:**\n"
            answer += f"• {total_states} estados brasileiros\n"
            answer += f"• {total_municipalities:,} municípios afetados\n"
            answer += f"• Período: 2024-2026\n"
            answer += f"• Valores de multas, tipos de infração, gravidade\n\n"
            
            answer += f"**Posso ajudar com:**\n"
            answer += f"• Análise por valor total de multas por tipo\n"
            answer += f"• Top infratores por valor (pessoas físicas e empresas)\n"
            answer += f"• Distribuição por gravidade\n"
            answer += f"• Busca por empresas específicas\n"
            answer += f"• Análises geográficas (estado + tipo + documento)\n\n"
            
            answer += f"**Exemplo:** 'Qual o valor total por tipo de infração?'"
        else:
            answer = "❌ Não foi possível carregar os dados para análise."
        
        return {"answer": answer, "source": "data_analysis"}
    
    def _add_ai_warning(self, answer: str, source: str) -> str:
        """Adiciona aviso sobre IA a todas as respostas."""
        warning = "\n\n⚠️ **Aviso Importante:** Todas as respostas precisam ser checadas. Os modelos de IA podem ter erros de alucinação, baixa qualidade em certos pontos, vieses ou problemas éticos."
        
        if source == "data_analysis":
            source_info = "\n\n*💡 Resposta baseada em análise direta dos dados*"
        elif source == "knowledge_base":
            source_info = "\n\n*📚 Resposta baseada em conhecimento especializado*"
        elif source == "llm":
            model_name = "Llama 3.1" if self.llm_config["provider"] == "groq" else "Gemini 2.5 Flash"
            source_info = f"\n\n*🤖 Resposta gerada por {model_name}*"
        else:
            source_info = ""
        
        return answer + source_info + warning
    
    def query(self, question: str, provider: str = 'direct') -> Dict[str, Any]:
        """Processa uma pergunta do usuário."""
        
        question_lower = question.lower()
        
        # Palavras-chave que indicam perguntas sobre dados
        data_keywords = [
            "valor total", "soma", "valores", "tipo", "infração", "gravidade",
            "infratores", "multas", "empresas", "pessoas", "cpf", "cnpj",
            "shell", "petrobras", "vale", "pará", "fauna", "flora",
            "estados", "uf", "municípios", "total", "quantos", "top"
        ]
        
        # Para perguntas sobre dados, usa análise local CORRIGIDA
        if any(keyword in question_lower for keyword in data_keywords):
            return self._answer_with_data_analysis(question)
        
        # Para perguntas conceituais, tenta LLM se disponível
        if self.llm_integration:
            try:
                return self.llm_integration.query(question, self.llm_config["provider"])
            except Exception as e:
                return {
                    "answer": f"❌ Análise não disponível: {str(e)}",
                    "source": "error"
                }
        
        # Default: análise local
        return self._answer_with_data_analysis(question)
    
    def display_chat_interface(self):
        """Exibe a interface do chatbot CORRIGIDA."""
        
        # Header com informações do modelo atual
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            st.subheader("💬 Chatbot Inteligente (CORRIGIDO)")
        
        with col2:
            model_emoji = "🦙" if self.llm_config["provider"] == "groq" else "💎"
            model_name = "Llama 3.1" if self.llm_config["provider"] == "groq" else "Gemini 2.5 Flash"
            st.caption(f"{model_emoji} Usando: {model_name}")
        
        with col3:
            if st.button("🔄 Recarregar", help="Limpa cache e recarrega dados"):
                self.cached_data = None
                st.success("Cache limpo!")
        
        # Aviso sobre correções
        st.info("🔧 **Versão Corrigida**: Agora usa agregações corretas do pandas, classificação adequada de CPF/CNPJ e busca fuzzy para nomes.")
        
        # Histórico de mensagens
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
        
        # Input do usuário
        if prompt := st.chat_input("Faça sua pergunta sobre os dados do IBAMA..."):
            # Adiciona mensagem do usuário
            st.session_state.messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)
            
            # Processa resposta
            with st.chat_message("assistant"):
                model_emoji = "🦙" if self.llm_config["provider"] == "groq" else "💎"
                with st.spinner(f"{model_emoji} A IA está analisando os dados..."):
                    try:
                        response = self.query(prompt)
                        answer = response.get("answer", "❌ Não foi possível processar sua pergunta.")
                        source = response.get("source", "unknown")
                        
                        # Adiciona aviso obrigatório sobre IA
                        final_answer = self._add_ai_warning(answer, source)
                        
                        st.markdown(final_answer)
                        
                        # Adiciona ao histórico
                        st.session_state.messages.append({"role": "assistant", "content": final_answer})
                        
                    except Exception as e:
                        error_msg = f"❌ Erro ao processar pergunta: {str(e)}"
                        st.markdown(error_msg)
                        st.session_state.messages.append({"role": "assistant", "content": error_msg})
    
    def display_sample_questions(self):
        """Exibe perguntas de exemplo CORRIGIDAS."""
        with st.expander("💡 Perguntas de Exemplo (CORRIGIDAS)"):
            
            st.write("**📊 Análises por filtros:**")
            corrected_questions = [
                "Quantas infrações contra a flora existiram em 2026?",
                "Quantas infrações de fauna houve no Pará em 2025?",
                "Qual o valor total das multas de flora em 2024?",
                "Qual o valor total de infrações dividido por tipos de infrações?",
                "Qual a distribuição por gravidade de infrações?",
                "Quais os infratores com mais multas em soma de valores?",
                "Quais os infratores do tipo empresas com mais multas no Pará pela fauna?",
                "Top 10 estados com mais infrações em 2026",
            ]
            
            for question in corrected_questions:
                if st.button(question, key=f"corrected_{hash(question)}"):
                    self._handle_sample_question(question)
            
            st.write("**🔍 Busca por Empresas:**")
            search_questions = [
                "A Petrobras tem infrações de que tipo?",
                "Quais infrações da Vale?",
                "Shell Brasil infrações"
            ]
            
            for question in search_questions:
                if st.button(question, key=f"search_{hash(question)}"):
                    self._handle_sample_question(question)
    
    def _handle_sample_question(self, question: str):
        """Manipula clique em pergunta de exemplo."""
        # Adiciona pergunta do usuário
        st.session_state.messages.append({"role": "user", "content": question})
        
        # Processa resposta
        response = self.query(question)
        answer = response.get("answer", "❌ Erro ao processar pergunta.")
        source = response.get("source", "unknown")
        
        # Adiciona aviso obrigatório sobre IA
        final_answer = self._add_ai_warning(answer, source)
        
        st.session_state.messages.append({"role": "assistant", "content": final_answer})
        st.rerun()
