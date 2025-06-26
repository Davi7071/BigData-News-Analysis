# BigData News Analysis

Análise de notícias em larga escala usando Python, PySpark e visualizações.

## Descrição

Este projeto coleta automaticamente títulos de artigos de múltiplos sites de notícias, processa-os em um ambiente distribuído com PySpark para limpeza e contagem de palavras, e gera visualizações (nuvem de palavras e gráfico de barras) dos termos mais frequentes. O resultado também é exportado em formato JSON para integração com outras ferramentas ou dashboards.

## Funcionalidades

- **Coleta de dados**: raspagem de até _N_ títulos por site usando `newspaper3k`.
- **Validação de sites**: verifica se cada domínio está acessível antes de raspar.
- **Processamento distribuído**: normalização de texto, remoção de stopwords e contagem de palavras em PySpark.
- **Exportação**: salva as 20 palavras mais frequentes em JSON no Google Drive.
- **Visualizações**:  
  - **WordCloud** com os termos mais recorrentes.  
  - **Gráfico de barras** mostrando as 10 palavras de maior frequência.



## Configuração

- No Google Colab, monte seu Drive:
  ```python
  from google.colab import drive
  drive.mount('/content/drive')
  ```
- Edite a lista de sites em `news_sites` (no notebook `notebook/bigdata.ipynb` ou no script `.py`) para incluir os domínios de sua preferência.
- Ajuste o parâmetro `max_articles` na função de coleta para definir quantos títulos deseja raspar por site.

## Uso

1. Abra o notebook `notebook/bigdata.ipynb` no Colab ou localmente.
2. Execute as células sequencialmente:
   1. Instalação de dependências
   2. Importações e downloads do NLTK
   3. Verificação de acessibilidade dos sites
   4. Coleta de títulos
   5. Inicialização do Spark
   6. Limpeza e pré-processamento
   7. Contagem de palavras
   8. Exportação para JSON
   9. Geração de WordCloud e gráfico de barras

3. Confira o arquivo JSON gerado em:
   ```bash
   /content/drive/MyDrive/resultado.json
   ```

## Pipeline de Processamento

1. **Extração**
   - Raspagem de títulos de artigos via `newspaper.build()`.
2. **Transformação**
   - Conversão para minúsculas, remoção de URLs/caracteres especiais.
   - Tokenização e remoção de stopwords (NLTK).
3. **Contagem**
   - Contagem distribuída de ocorrências de cada palavra (PySpark).
4. **Carregamento**
   - Exportação das 20 palavras mais frequentes para JSON no Drive.
   
   ---

## English Version

# BigData News Analysis

Large-scale news analysis using Python, PySpark, and visualizations.

## Description

This project automatically collects article headlines from multiple news websites, processes them in a distributed PySpark environment for cleaning and word counting, and generates visualizations (word cloud and bar chart) of the most frequent terms. The results are also exported in JSON format for integration with other tools or dashboards.

## Features

- **Data Collection**: Scrapes up to _N_ headlines per site using `newspaper3k`.
- **Site Validation**: Checks if each domain is accessible before scraping.
- **Distributed Processing**: Text normalization, stopword removal, and word counting in PySpark.
- **Export**: Saves the top 20 most frequent words as a JSON file on Google Drive.
- **Visualizations**:  
  - **Word Cloud** of the most common terms.  
  - **Bar Chart** showing the 10 highest-frequency words.

## Configuration

- In Google Colab, mount your Drive:
  ```python
  from google.colab import drive
  drive.mount('/content/drive')
  ```
- Edit the list of sites in `news_sites` (in the `notebook/bigdata.ipynb` notebook or the `.py` script) to include the domains of your choice.
- Adjust the `max_articles` parameter in the scraping function to set how many headlines you want to scrape per site.

## Usage

1. Open the `notebook/bigdata.ipynb` notebook in Colab or locally.
2. Run the cells in sequence:
   1. Install dependencies
   2. Import libraries and download NLTK data
   3. Validate site accessibility
   4. Scrape headlines
   5. Initialize Spark
   6. Clean and preprocess text
   7. Perform word count
   8. Export results to JSON
   9. Generate word cloud and bar chart

3. Check the generated JSON file at:
   ```bash
   /content/drive/MyDrive/resultado.json
   ```

## Processing Pipeline

1. **Extraction**
   - Scrape article headlines via `newspaper.build()`.
2. **Transformation**
   - Convert text to lowercase, remove URLs and special characters.
   - Tokenize and remove stopwords (NLTK).
3. **Counting**
   - Distributed counting of word occurrences (PySpark).
4. **Loading**
   - Export the top 20 most frequent words to JSON on Drive.
