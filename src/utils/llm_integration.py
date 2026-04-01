import os
import json
import re  # Importa o módulo de expressões regulares
from typing import Dict, Any, Optional, Literal
from openai import OpenAI
import google.generativeai as genai
import pandas as pd

import config
try:
    from src.utils.tools import search_internet
except ImportError:
    # Fallback se o módulo não existir
    def search_internet(query: str) -> str:
        return "Busca na internet não disponível nesta instância."

class LLMIntegration:
    def __init__(self, database=None):
        """
        Inicializa a integração com LLMs (Groq/Llama e Google/Gemini).
        
        Args:
            database: Instância do banco de dados para consultas SQL
        """
        self.database = database
        self.groq_client = None
        self.gemini_model = None
        self.groq_model_name = "llama-3.1-70b-versatile"
        self.gemini_model_name = "gemini-2.5-flash"

        # Configuração do Groq (Llama 3.1)
        groq_api_key = config.get_secret('GROQ_API_KEY')
        if groq_api_key:
            try:
                self.groq_client = OpenAI(
                    api_key=groq_api_key, 
                    base_url="https://api.groq.com/openai/v1"
                )
                print("✅ Groq/Llama 3.1 inicializado com sucesso")
            except Exception as e:
                print(f"❌ Erro ao inicializar Groq: {e}")
                self.groq_client = None

        # Configuração do Google Gemini
        google_api_key = config.get_secret('GOOGLE_API_KEY')
        if google_api_key:
            try:
                genai.configure(api_key=google_api_key)
                self.gemini_model = genai.GenerativeModel(model_name=self.gemini_model_name)
                print("✅ Google Gemini inicializado com sucesso")
            except Exception as e:
                print(f"❌ Erro ao inicializar Gemini: {e}")
                self.gemini_model = None

        # Aviso se nenhuma API estiver disponível
        if not self.groq_client and not self.gemini_model:
            print("⚠️ Nenhuma API de LLM configurada. Funcionalidades de IA serão limitadas.")

    def _get_system_prompt(self) -> str:
        """
        Gera o prompt do sistema para geração de SQL baseado no esquema do banco.
        
        Returns:
            str: Prompt formatado para o LLM
        """
        try:
            schema_df = self.database.get_table_info()
            schema_str = "\n".join([
                f'- "{row["name"]}" ({row["type"]})' 
                for _, row in schema_df.iterrows()
            ])
        except Exception:
            schema_str = "Não foi possível carregar o esquema da tabela."
        
        # Instruções específicas por tipo de banco
        if self.database and self.database.is_cloud:
            sql_dialect_instructions = """
            Você gera código SQL para PostgreSQL (Supabase).
            Regras IMPORTANTÍSSIMAS para PostgreSQL:
            1.  Sempre coloque os nomes das colunas entre aspas duplas (ex: "UF").
            2.  Para análises temporais, use: EXTRACT(YEAR FROM TO_TIMESTAMP("DAT_HORA_AUTO_INFRACAO", 'YYYY-MM-DD HH24:MI:SS')).
            3.  Para cálculos em "VAL_AUTO_INFRACAO", use: CAST(REPLACE("VAL_AUTO_INFRACAO", ',', '.') AS NUMERIC).
            4.  Use LIMIT para restringir resultados (máximo 1000 registros).
            5.  Para filtros de texto, use ILIKE para busca case-insensitive.
            """
        else:
            sql_dialect_instructions = """
            Você gera código SQL para DuckDB.
            Regras IMPORTANTÍSSIMAS para DuckDB:
            1.  Para análises temporais, use: EXTRACT(YEAR FROM TRY_CAST("DAT_HORA_AUTO_INFRACAO" AS TIMESTAMP)).
            2.  Para cálculos em "VAL_AUTO_INFRACAO", use: CAST(REPLACE("VAL_AUTO_INFRACAO", ',', '.') AS DOUBLE).
            3.  Use LIMIT para restringir resultados.
            """

        return f"""
        Você é um assistente especialista em dados do IBAMA. Sua função é gerar uma única consulta SQL para responder à pergunta.
        Retorne APENAS o código SQL, nada mais.

        {sql_dialect_instructions}

        Esquema da tabela `ibama_infracao`:
        {schema_str}

        IMPORTANTE: 
        - Sempre use LIMIT para evitar consultas muito grandes
        - Para análises TOP/ranking, use ORDER BY com LIMIT
        - Para buscas de texto, seja flexível com LIKE ou ILIKE
        - Sempre valide que as colunas existem no esquema
        """

    def _extract_sql_from_response(self, response_text: str) -> Optional[str]:
        """
        Extrai a primeira consulta SQL de um texto usando expressões regulares.
        Ignora qualquer texto antes do 'SELECT'.
        
        Args:
            response_text: Texto de resposta do LLM
            
        Returns:
            str: Query SQL extraída ou None se não encontrada
        """
        if not response_text:
            return None
        
        # Remove blocos de código markdown se existirem
        response_text = re.sub(r'```sql\n?', '', response_text)
        response_text = re.sub(r'```\n?', '', response_text)
        
        # Procura por 'SELECT' (case-insensitive) e captura tudo até o final
        # ou até encontrar uma quebra de linha dupla
        patterns = [
            r"SELECT\s+.*?(?=\n\n|\Z)",  # SELECT até quebra dupla ou fim
            r"SELECT\s+.*",              # SELECT até o fim (fallback)
        ]
        
        for pattern in patterns:
            match = re.search(pattern, response_text, re.IGNORECASE | re.DOTALL)
            if match:
                sql = match.group(0).strip()
                # Remove comentários e linhas vazias no final
                sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
                sql = sql.strip()
                if sql:
                    return sql
        
        return None

    def _validate_sql_query(self, sql_query: str) -> bool:
        """
        Valida se a query SQL é segura para execução.
        
        Args:
            sql_query: Query SQL para validar
            
        Returns:
            bool: True se a query é segura
        """
        if not sql_query:
            return False
        
        sql_lower = sql_query.lower().strip()
        
        # Deve começar com SELECT
        if not sql_lower.startswith('select'):
            return False
        
        # Não deve conter comandos perigosos
        dangerous_keywords = [
            'drop', 'delete', 'insert', 'update', 'alter', 'create',
            'truncate', 'grant', 'revoke', 'exec', 'execute'
        ]
        
        for keyword in dangerous_keywords:
            if f' {keyword} ' in f' {sql_lower} ':
                return False
        
        return True

    def query(self, question: str, provider: Literal['groq', 'gemini'] = 'groq') -> Dict[str, Any]:
        """
        Processa uma pergunta usando o LLM especificado.
        
        Args:
            question: Pergunta do usuário
            provider: Provedor de LLM ('groq' ou 'gemini')
            
        Returns:
            Dict com resposta, fonte e informações de debug
        """
        tool_choice = self._decide_tool(question)
        
        try:
            if tool_choice == 'internet':
                # Busca na internet para perguntas conceituais
                try:
                    search_result = search_internet(question)
                    final_answer = search_result + "\n\n⚠️ **Aviso Importante:** Todas as respostas precisam ser checadas. Os modelos de IA podem ter erros de alucinação, baixa qualidade em certos pontos, vieses ou problemas éticos."
                    
                    return {
                        "answer": final_answer,
                        "source": "internet",
                        "provider": provider
                    }
                except Exception as e:
                    return {
                        "answer": f"Busca na internet não disponível: {str(e)}",
                        "source": "error"
                    }

            elif tool_choice == 'database':
                # Geração de SQL e consulta ao banco
                raw_sql_response = self.generate_sql(question, provider)
                
                # Extrai e valida SQL
                sql_query = self._extract_sql_from_response(raw_sql_response)
                
                if not sql_query:
                    return {
                        "answer": "A IA não conseguiu gerar uma consulta SQL válida. Tente reformular sua pergunta.",
                        "source": "error",
                        "debug_info": {"raw_response": raw_sql_response}
                    }
                
                if not self._validate_sql_query(sql_query):
                    return {
                        "answer": "A consulta SQL gerada não é segura ou válida. Tente uma pergunta diferente.",
                        "source": "error",
                        "debug_info": {"sql_query": sql_query}
                    }
                
                # Executa a consulta
                try:
                    db_results = self.database.execute_query(sql_query)
                    
                    if db_results.empty:
                        final_answer = "Não encontrei dados para sua consulta no banco de dados."
                    else:
                        final_answer = self._format_results(question, db_results)
                    
                    # Adiciona aviso sobre IA apenas se a resposta foi gerada por LLM
                    final_answer += "\n\n⚠️ **Aviso Importante:** Todas as respostas precisam ser checadas. Os modelos de IA podem ter erros de alucinação, baixa qualidade em certos pontos, vieses ou problemas éticos."
                    
                    return {
                        "answer": final_answer,
                        "source": "database",
                        "provider": provider,
                        "debug_info": {"sql_query": sql_query}
                    }
                
                except Exception as db_error:
                    return {
                        "answer": f"Erro ao executar consulta no banco de dados: {str(db_error)}",
                        "source": "error",
                        "debug_info": {"sql_query": sql_query, "db_error": str(db_error)}
                    }

        except Exception as e:
            return {
                "answer": f"Ocorreu um erro inesperado: {str(e)}",
                "source": "error",
                "debug_info": {"exception": str(e)}
            }

    def generate_sql(self, question: str, provider: str, temperature: float = 0.0, max_tokens: int = 500) -> str:
        """
        Gera SQL a partir de uma pergunta usando o LLM especificado.
        
        Args:
            question: Pergunta do usuário
            provider: Provedor de LLM ('groq' ou 'gemini')
            temperature: Criatividade do modelo (0.0 = determinístico)
            max_tokens: Máximo de tokens na resposta
            
        Returns:
            str: Resposta bruta da IA
        """
        system_prompt = self._get_system_prompt()
        user_prompt = f"Pergunta: {question}"
        
        if provider == 'gemini' and self.gemini_model:
            try:
                # Configuração de geração para Gemini
                generation_config = {
                    "temperature": temperature,
                    "max_output_tokens": min(max_tokens, 2048)  # Limite do Gemini
                }
                
                response = self.gemini_model.generate_content(
                    f"{system_prompt}\n\n{user_prompt}", 
                    generation_config=generation_config
                )
                return response.text
            except Exception as e:
                print(f"Erro no Gemini: {e}")
                return ""
                
        elif provider == 'groq' and self.groq_client:
            try:
                response = self.groq_client.chat.completions.create(
                    model=self.groq_model_name,
                    messages=[
                        {"role": "system", "content": system_prompt}, 
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=temperature, 
                    max_tokens=min(max_tokens, 1024)  # Limite do Groq
                )
                return response.choices[0].message.content
            except Exception as e:
                print(f"Erro no Groq: {e}")
                return ""
        
        return ""

    def generate_analysis(self, prompt: str, provider: str, temperature: float = 0.3, max_tokens: int = 1000) -> str:
        """
        Gera análise de dados usando o LLM.
        
        Args:
            prompt: Prompt de análise
            provider: Provedor de LLM
            temperature: Criatividade do modelo
            max_tokens: Máximo de tokens
            
        Returns:
            str: Análise gerada
        """
        analysis_prompt = f"""
        Você é um especialista em análise de dados ambientais do IBAMA.
        Analise os dados fornecidos e forneça insights relevantes em português brasileiro.
        Seja claro, objetivo e forneça informações úteis.
        
        {prompt}
        """
        
        if provider == 'gemini' and self.gemini_model:
            try:
                generation_config = {
                    "temperature": temperature,
                    "max_output_tokens": min(max_tokens, 2048)
                }
                response = self.gemini_model.generate_content(
                    analysis_prompt, 
                    generation_config=generation_config
                )
                analysis_result = response.text
                analysis_result += "\n\n⚠️ **Aviso Importante:** Todas as respostas precisam ser checadas. Os modelos de IA podem ter erros de alucinação, baixa qualidade em certos pontos, vieses ou problemas éticos."
                return analysis_result
            except Exception as e:
                return f"Erro na análise com Gemini: {str(e)}"
                
        elif provider == 'groq' and self.groq_client:
            try:
                response = self.groq_client.chat.completions.create(
                    model=self.groq_model_name,
                    messages=[{"role": "user", "content": analysis_prompt}],
                    temperature=temperature, 
                    max_tokens=min(max_tokens, 1024)
                )
                analysis_result = response.choices[0].message.content
                analysis_result += "\n\n⚠️ **Aviso Importante:** Todas as respostas precisam ser checadas. Os modelos de IA podem ter erros de alucinação, baixa qualidade em certos pontos, vieses ou problemas éticos."
                return analysis_result
            except Exception as e:
                return f"Erro na análise com Groq: {str(e)}"
        
        return "Análise não disponível (nenhum modelo configurado)"

    def _format_results(self, question: str, results: pd.DataFrame) -> str:
        """
        Formata os resultados da consulta SQL para exibição.
        
        Args:
            question: Pergunta original
            results: DataFrame com resultados
            
        Returns:
            str: Resultados formatados
        """
        if results.empty:
            return "Nenhum resultado encontrado para sua consulta."
        
        # Para resultados simples (uma célula)
        if len(results) == 1 and len(results.columns) == 1:
            value = results.iloc[0, 0]
            if isinstance(value, (int, float)):
                return f"**Resultado:** {value:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
            else:
                return f"**Resultado:** {value}"
        
        # Para resultados tabulares
        try:
            # Limita número de linhas para exibição
            display_results = results.head(50) if len(results) > 50 else results
            
            # Formata nomes das colunas
            display_results = display_results.copy()
            display_results.columns = [
                col.replace('_', ' ').title() 
                for col in display_results.columns
            ]
            
            # Converte para markdown
            markdown_table = display_results.to_markdown(index=False)
            
            # Adiciona informações extras se necessário
            extra_info = ""
            if len(results) > 50:
                extra_info = f"\n\n*Exibindo primeiras 50 linhas de {len(results)} resultados.*"
            
            return f"{markdown_table}{extra_info}"
            
        except Exception as e:
            return f"Erro ao formatar resultados: {str(e)}"

    def _decide_tool(self, question: str) -> Literal['database', 'internet']:
        """
        Decide qual ferramenta usar baseado na pergunta.
        
        Args:
            question: Pergunta do usuário
            
        Returns:
            str: 'database' ou 'internet'
        """
        question_lower = question.lower()
        
        # Palavras-chave que indicam busca na internet
        web_keywords = [
            "endereço", "o que é", "significado de", "site oficial", 
            "telefone", "contato", "história", "quem é o presidente", 
            "localização", "site", "como funciona", "definição"
        ]
        
        # Palavras-chave que indicam consulta ao banco
        database_keywords = [
            "quantos", "quais", "top", "ranking", "maior", "menor",
            "total", "soma", "média", "count", "estados", "uf",
            "municípios", "infrações", "multas", "valores", "dados"
        ]
        
        # Verifica se tem indicadores claros de busca web
        if any(keyword in question_lower for keyword in web_keywords):
            return 'internet'
        
        # Verifica se tem indicadores claros de consulta banco
        if any(keyword in question_lower for keyword in database_keywords):
            return 'database'
        
        # Default: assume que é consulta ao banco (mais comum)
        return 'database'

    def get_available_providers(self) -> Dict[str, bool]:
        """
        Retorna status dos provedores de LLM disponíveis.
        
        Returns:
            Dict com status de cada provedor
        """
        return {
            "groq": self.groq_client is not None,
            "gemini": self.gemini_model is not None
        }

    def test_connection(self, provider: str) -> Dict[str, Any]:
        """
        Testa a conexão com um provedor específico.
        
        Args:
            provider: Nome do provedor ('groq' ou 'gemini')
            
        Returns:
            Dict com resultado do teste
        """
        test_question = "SELECT COUNT(*) FROM ibama_infracao LIMIT 1"
        
        try:
            if provider == 'groq' and self.groq_client:
                response = self.groq_client.chat.completions.create(
                    model=self.groq_model_name,
                    messages=[{"role": "user", "content": "Teste"}],
                    temperature=0.0,
                    max_tokens=10
                )
                return {"status": "success", "message": "Groq conectado com sucesso"}
                
            elif provider == 'gemini' and self.gemini_model:
                response = self.gemini_model.generate_content(
                    "Teste",
                    generation_config={"temperature": 0.0, "max_output_tokens": 10}
                )
                return {"status": "success", "message": "Gemini conectado com sucesso"}
            
            else:
                return {"status": "error", "message": f"Provedor {provider} não configurado"}
                
        except Exception as e:
            return {"status": "error", "message": f"Erro na conexão: {str(e)}"}
