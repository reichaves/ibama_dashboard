# 🌳 IBAMA Dashboard - Environmental Violations Analysis

> **Status:** 🚧 **Beta Version** - Under active development

Interactive dashboard for analyzing violation data from the Brazilian Institute of Environment and Renewable Natural Resources (IBAMA), featuring Artificial Intelligence capabilities for natural language queries.

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://ibamadashboard.streamlit.app/)
[![GitHub](https://img.shields.io/badge/GitHub-Repository-blue)](https://github.com/reichaves/ibama_dashboard)
[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

## 📊 About the Project

This application processes and analyzes public data from IBAMA environmental violation records, providing a modern and intuitive interface for exploring Brazilian environmental data. Specifically developed for **journalists**, **researchers**, **academics**, and **citizens interested** in environmental transparency.

### 🎯 Objectives
- **Democratize access** to Brazilian environmental data
- **Facilitate journalistic analysis** of environmental violations
- **Support academic research** with modern tools
- **Promote transparency** in environmental enforcement
- **Combine traditional analysis** with Artificial Intelligence

## ✨ Key Features

### 📈 **Interactive Dashboard**
- **Real-time metrics**: Total violations, fine amounts, affected municipalities
- **Geographic visualizations**: Heat maps of violations by region
- **Temporal analysis**: Advanced filters by year and month
- **Specialized rankings**: 
  - Top 10 individual violators (masked CPF)
  - Top 10 company violators (complete CNPJ)
  - States and municipalities with most violations
- **Distribution by severity**: Low, Medium, and violations without assessment

### 🤖 **AI-Powered Chatbot**
- **Natural language queries**: "Which states have the most fishing violations?"
- **Two available models**:
  - 🦙 **Llama 3.1 70B (Groq)**: Fast, ideal for simple queries
  - 💎 **Gemini 1.5 Pro (Google)**: Advanced, for complex analysis
  - (using older versions due to economic limitations, but you can change the model with your API key)
- **Intelligent analysis**: Combines local data with AI processing
- **Transparency**: Clear warnings about AI limitations

### 🔍 **SQL Explorer**
- **Manual Mode**: Interface for direct SQL queries
- **AI Mode**: Automatic SQL generation from natural language
- **Automatic analysis**: Intelligent interpretation of results
- **Ready examples**: Pre-defined queries for quick start

## 📰 Journalistic Applications

### **For Investigative Journalism:**
- **Pattern identification**: Companies or individuals with violation history
- **Regional analysis**: Comparison between states and regions
- **Time series**: Evolution of violations over time
- **Data cross-referencing**: Correlation between violation types and location

### **For News Reports:**
- **Verifiable data**: All information from official sources (IBAMA)
- **Ready visualizations**: Exportable charts for articles
- **Specific queries**: Search for particular cases or regions
- **Historical context**: Comparison with previous periods

### **Story Examples:**
- "The 10 companies that received the most environmental fines in 2024"
- "Amazon municipalities lead ranking of fauna violations"
- "30% increase in biopiracy fines in the last year"
- "Profile of environmental violations in your state"

## 🔬 Research Applications

### **Academic Research:**
- **Quantitative analysis**: Structured data for statistical studies
- **Historical series**: Data from 2024 for temporal analysis
- **Geolocation**: Coordinates for spatial studies
- **Categorization**: Violation types for thematic studies

### **Supported Research Areas:**
- **Environmental Law**: Enforcement effectiveness
- **Geography**: Spatial distribution of environmental crimes
- **Economics**: Economic impact of environmental fines
- **Social Sciences**: Profile of environmental violators
- **Public Policy**: Evaluation of enforcement programs

### **Data Methodology:**
- **Primary source**: IBAMA Open Data Portal
- **Updates**: Data updated daily
- **Quality**: Automatic validation and data cleaning
- **Transparency**: Open source code for auditing

## 🚨 Important Limitations and Warnings

### **⚠️ AI Limitations**
- **Hallucinations**: Models may generate incorrect information
- **Biases**: May reflect prejudices from training data
- **Limited context**: Don't understand political or social nuances
- **Mandatory verification**: **ALWAYS** confirm information with primary sources

### **📊 Data Limitations**
- **Period**: Data available mainly from 2024-2025
- **Completeness**: Not all violations may be classified
- **Processing**: Data undergoes automatic cleaning that may introduce errors
- **Interpretation**: Correlation does not imply causation

### **🔒 Privacy and Ethics**
- **Masked CPF**: Individuals have protected data (XXX.***.***-XX)
- **Complete CNPJ**: Companies have full transparency (public data)
- **Responsibility**: User responsible for ethical use of information

## 🛠️ Technologies Used

### **Frontend and Interface**
- **[Streamlit](https://streamlit.io/)**: Web framework for data applications
- **[Plotly](https://plotly.com/)**: Interactive visualizations
- **[Pandas](https://pandas.pydata.org/)**: Data manipulation and analysis

### **Artificial Intelligence**
- **[Groq](https://groq.com/)**: API for Llama 3.1 70B (fast processing)
- **[Google Gemini](https://ai.google.dev/)**: Advanced model for complex analysis
- **[OpenAI API](https://openai.com/)**: Compatible interface for LLMs

### **Database**
- **[Supabase](https://supabase.com/)**: PostgreSQL in the cloud (production)
- **[DuckDB](https://duckdb.org/)**: Local analytical database (development)

### **Data Processing**
- **[NumPy](https://numpy.org/)**: Numerical computing
- **[APScheduler](https://apscheduler.readthedocs.io/)**: Task scheduling
- **[Requests](https://requests.readthedocs.io/)**: Data downloading

### **Deploy and Infrastructure**
- **[Streamlit Community Cloud](https://streamlit.io/cloud)**: Free hosting
- **[GitHub](https://github.com/)**: Version control and CI/CD
- **[Python 3.8+](https://python.org)**: Base language

## 🚀 How to Use

### **💻 Online Access (Recommended)**
1. Visit: **[ibamadashboard.streamlit.app](https://ibamadashboard.streamlit.app/)**
2. Use the sidebar filters to explore the data
3. Navigate through the 3 main tabs:
   - **📊 Dashboard**: Interactive visualizations
   - **💬 Chatbot**: Natural language questions
   - **🔍 SQL**: Custom queries

### **🏠 Local Installation**

#### **Prerequisites:**
- Python 3.8 or higher
- Git
- API Keys (optional, for AI):
  - [Groq API Key](https://console.groq.com/) (free)
  - [Google AI API Key](https://ai.google.dev/) (paid)

#### **Steps:**
```bash
# 1. Clone the repository
git clone https://github.com/reichaves/ibama_dashboard.git
cd ibama_dashboard

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment variables (optional)
cp .env.example .env
# Edit .env with your API keys

# 4. Run the application
streamlit run app.py
```

#### **Environment Variables (Optional):**
```bash
# For AI functionalities
GROQ_API_KEY=your_groq_key_here
GOOGLE_API_KEY=your_google_key_here

# For database (if not configured, uses local data)
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
```

## 📖 Quick Usage Guide

### **For Journalists:**
1. **Start with the Dashboard** for general overview
2. **Use the Chatbot** for specific questions: "Biggest violators in Pará"
3. **Export visualizations** by clicking the camera icon on charts
4. **Always verify** important data with primary sources

### **For Researchers:**
1. **Use the SQL Explorer** for complex queries
2. **Leverage advanced filters** by period and region
3. **Document** your queries for reproducibility
4. **Properly cite** the data source (IBAMA Portal)

### **For Developers:**
1. **Fork the repository** for customizations
2. **Check the code documentation** (inline comments)
3. **Contribute** improvements via Pull Requests
4. **Report bugs** in the GitHub Issues section

## 📊 Data and Sources

### **Official Source**
- **Origin**: [IBAMA Open Data Portal](https://dadosabertos.ibama.gov.br/dataset/fiscalizacao-auto-de-infracao)
- **Format**: Compressed CSV, updated periodically
- **License**: Public data, Brazilian public domain

### **Data Structure**
- **Period**: Mainly 2024-2025
- **Granularity**: Per individual violation record
- **Geolocation**: Coordinates when available
- **Classification**: Type, severity, violation status

### **Processing**
- **Automatic cleaning**: Removal of duplicates and invalid data
- **Validation**: Format verification (CPF/CNPJ, dates, values)
- **Enrichment**: Addition of geographic and temporal analysis

## 🤝 How to Contribute

### **For Users:**
- **Report bugs** or problems encountered
- **Suggest improvements** in functionality
- **Share** interesting use cases

### **For Developers:**
- **Fork** the repository
- **Create branch** for your feature: `git checkout -b feature/new-functionality`
- **Commit** your changes: `git commit -m 'Add new functionality'`
- **Push** to branch: `git push origin feature/new-functionality`
- **Open Pull Request** explaining the changes

### **Contribution Guidelines:**
- Keep code clean and documented
- Add tests when possible
- Follow Python style conventions (PEP 8)
- Update documentation when necessary

## 🗓️ Roadmap and Future Versions

### **🚧 Current Version (Beta)**
- ✅ Basic dashboard working
- ✅ AI chatbot integrated
- ✅ SQL explorer operational
- ✅ Advanced filters implemented

## ⚖️ Legal and Ethical Aspects

### **Responsible Use**
- **Presumption of innocence**: Fines don't mean confirmed guilt
- **Context necessary**: Isolated data can be misleading
- **Verification**: Always confirm important information
- **Privacy**: Respect masked personal data

### **Transparency**
- **Open source**: Publicly auditable algorithms
- **Clear methodology**: Documented data processing
- **Explicit limitations**: Warnings about restrictions and biases

## 📞 Support and Contact

### **Developer**
- **Name**: Reinaldo Chaves
- **GitHub**: [@reichaves](https://github.com/reichaves)
- **Project**: [github.com/reichaves/ibama_dashboard](https://github.com/reichaves/ibama_dashboard)

### **Support**
- **Issues**: Use GitHub Issues for bugs and suggestions
- **Documentation**: README and code comments
- **Community**: Streamlit Community for technical questions

### **Academic Citation**
```
Chaves, R. (2025). IBAMA Dashboard - Environmental Violations Analysis. 
Available at: https://github.com/reichaves/ibama_dashboard
```

## 📄 License

This project is under the MIT license. See the [LICENSE](LICENSE) file for complete details.

---

**⚠️ Legal Notice**: This project is an independent initiative for democratizing public data. It has no official affiliation with IBAMA or the Brazilian government. Use the information responsibly and always verify important data with official sources.

**🔍 Transparency**: All code is open and auditable. Contributions and improvements are welcome from the community.

---

*Last updated: July 2025 | Version: Beta 0.9*
