# app.py

import streamlit as st
import pandas as pd

# Importa os componentes do projeto
from src.utils.database import Database
from src.utils.data_loader import DataLoader
from src.utils.llm_integration import LLMIntegration
from src.components.visualization import DataVisualization
from src.components.chatbot import Chatbot

def main():
    st.set_page_config(page_title="Análise de Infrações IBAMA", page_icon="🌳", layout="wide")
    
    st.title("🌳 Análise de Autos de Infração do IBAMA")
    
    # Inicializa os componentes principais
    db = Database()
    data_loader = DataLoader(database=db)
    llm = LLMIntegration(database=db)
    viz = DataVisualization(database=db)
    chatbot = Chatbot(llm_integration=llm)
    chatbot.initialize_chat_state()
    
    # --- BARRA LATERAL (SIDEBAR) APRIMORADA ---
    with st.sidebar:
        st.header("🔎 Filtros do Dashboard")

        try:
            ufs_list = db.execute_query("SELECT DISTINCT UF FROM ibama_infracao WHERE UF IS NOT NULL ORDER BY UF")['UF'].tolist()
            selected_ufs = st.multiselect("Selecione o Estado (UF)", options=ufs_list, default=[])

            years_df = db.execute_query("SELECT DISTINCT EXTRACT(YEAR FROM TRY_CAST(DAT_HORA_AUTO_INFRACAO AS TIMESTAMP)) as ano FROM ibama_infracao WHERE ano IS NOT NULL ORDER BY ano DESC")
            years_list = [int(y) for y in years_df['ano'].tolist()]
            if years_list:
                min_year, max_year = min(years_list), max(years_list)
                year_range = st.slider("Selecione o Intervalo de Anos", min_year, max_year, (min_year, max_year))
            else:
                year_range = None
        except Exception as e:
            st.error("Erro ao carregar filtros.")
            selected_ufs = []
            year_range = None

        st.divider()
        if st.button("🔄 Atualizar Dados Agora"):
            with st.spinner("Atualizando dados..."):
                data_loader.download_and_process()
                st.success("Dados atualizados!")
                st.rerun()
        
        chatbot.display_sample_questions()

        # --- ALTERAÇÃO AQUI: Adicionando as informações auxiliares ---
        st.divider()
        with st.expander("⚠️ Avisos Importantes"):
            st.warning(
                "**Não use IA para escrever um texto inteiro!** O auxílio é melhor para gerar resumos, "
                "filtrar informações ou auxiliar a entender contextos - que depois devem ser checados. "
                "Inteligência Artificial comete erros (alucinações, viés, baixa qualidade, problemas éticos)!"
            )
            st.info(
                "Este projeto não se responsabiliza pelos conteúdos criados a partir deste site. "
                "Cheque as informações com os dados originais do Ibama e mais fontes."
            )

        with st.expander("ℹ️ Sobre este App"):
            st.markdown(
                """
                **Fonte dos dados:** [Portal de Dados Abertos do IBAMA](https://dadosabertos.ibama.gov.br/dataset/fiscalizacao-auto-de-infracao/resource/b2aba344-95df-43c0-b2ba-f4353cfd9a00)

                **Desenvolvido por:** Reinaldo Chaves.

                Para mais informações, contribuições e feedback, visite o repositório do projeto:
                _(link do repositório a ser adicionado aqui)_
                """
            )

    # ... (o resto do app.py permanece o mesmo) ...
    tab1, tab2, tab3 = st.tabs(["📊 Dashboard Interativo", "💬 Chatbot com IA", "🔍 Explorador SQL"])
    
    with tab1:
        st.header("Dashboard de Análise de Infrações Ambientais")
        st.caption("Use os filtros na barra lateral para explorar os dados.")
        viz.create_overview_metrics(selected_ufs, year_range)
        st.divider()
        viz.create_infraction_map(selected_ufs, year_range)
        st.divider()
        col1, col2 = st.columns(2)
        with col1:
            viz.create_municipality_hotspots_chart(selected_ufs, year_range)
            viz.create_fine_value_by_type_chart(selected_ufs, year_range)
            viz.create_gravity_distribution_chart(selected_ufs, year_range)
        with col2:
            viz.create_state_distribution_chart(selected_ufs, year_range)
            viz.create_infraction_status_chart(selected_ufs, year_range)
            viz.create_main_offenders_chart(selected_ufs, year_range)
    
    with tab2:
        chatbot.display_chat_interface()
    
    with tab3:
        st.header("Explorador de Dados SQL")
        query = st.text_area("Escreva sua consulta SQL (apenas SELECT)", value="SELECT * FROM ibama_infracao LIMIT 10", height=150)
        if st.button("Executar Consulta"):
            if query.strip().lower().startswith("select"):
                try:
                    df = db.execute_query(query)
                    st.dataframe(df)
                except Exception as e:
                    st.error(f"Erro na consulta: {e}")
            else:
                st.error("Apenas consultas SELECT são permitidas por segurança.")

if __name__ == "__main__":
    main()