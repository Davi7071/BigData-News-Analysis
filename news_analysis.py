"""Pipeline de análise de títulos de notícias com PySpark.

Etapas:
    1. Verificação de acessibilidade dos sites
    2. Coleta de títulos (newspaper4k), com deduplicação e data de publicação
    3. Limpeza, tokenização e remoção de stopwords no Spark
    4. Contagem de palavras e bigramas no Spark
    5. Exportação (CSV bruto + JSON) e visualizações

Uso via linha de comando:
    python news_analysis.py --max-artigos 30 --saida output
"""

import argparse
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests

log = logging.getLogger("news_analysis")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

NEWS_SITES = [
    "https://www.cnnbrasil.com.br",
    "https://g1.globo.com",
    "https://www.bbc.com/portuguese",
    "https://www.uol.com.br",
    "https://www.terra.com.br",
    "https://www.estadao.com.br",
    "https://www1.folha.uol.com.br",
    "https://noticias.uol.com.br",
    "https://ultimosegundo.ig.com.br",
    "https://exame.com",
    "https://jovempan.com.br",
    "https://oglobo.globo.com",
    "https://valor.globo.com",
    "https://www.correiobraziliense.com.br",
    "https://gauchazh.clicrbs.com.br",
]

TLDS_SUSPEITOS = (".tk", ".ml", ".ga", ".cf", ".gq")

# Palavras sem valor informativo em manchetes (pronomes, preposições, verbos
# auxiliares etc.). Pontuação, números e emojis já são removidos na limpeza.
STOPWORDS_GRAMATICAIS = {
    "do", "de", "que", "como", "onde", "não", "é", "a", "an", "ou", "isso", "diz", "dia", "sobre",
    "porque", "da", "das", "e", "em", "o", "os", "as", "um", "uma", "para", "por",
    "na", "no", "com", "ele", "ficou", "ge", "cara", "vez", "cima", "vai", "faz", "resumo", "pode", "mim", "maior",
    "uns", "umas", "eu", "tu", "ela", "nós", "vós", "eles", "elas", "me", "te", "se", "nos", "vos",
    "comigo", "contigo", "consigo", "meu", "minha", "meus", "minhas", "teu", "tua", "teus", "tuas",
    "seu", "sua", "seus", "suas", "nosso", "nossa", "nossos", "nossas", "este", "esta", "estes", "estas",
    "aquilo", "aquele", "aquela", "aqueles", "aquelas", "quem", "qual", "quais", "cujo", "cuja", "cujos", "cujas",
    "ante", "após", "até", "contra", "desde", "entre", "perante", "sem", "sob", "trás",
    "mas", "porém", "todavia", "contudo", "entretanto", "pois", "portanto", "logo", "embora",
    "enquanto", "caso",
    "aqui", "ali", "lá", "cá", "agora", "já", "hoje", "ontem", "sempre", "nunca", "depois", "antes", "bem", "mal",
    "muito", "pouco", "mais", "menos", "talvez", "sim", "também", "ainda", "só", "tudo", "nada", "todo", "cada",
    "ser", "sou", "era", "foi", "fui", "são", "será", "seriam", "seja", "foram", "fosse",
    "estar", "estou", "está", "estava", "estavam", "estive", "esteve", "estejam",
    "ter", "tenho", "tem", "tinha", "tinham", "teve", "tiveram", "terá", "teriam", "tenha", "tiver",
    "haver", "há", "havia", "houve", "houver", "houveram", "houvesse",
    "ir", "vou", "vamos", "vão", "iria", "iriam",
    "poder", "posso", "podia", "podiam", "poderia", "poderiam",
    "dever", "devo", "deve", "devia", "deveria", "deveriam",
    "querer", "quero", "quer", "queria", "queriam",
}

# Termos removidos por escolha editorial: são palavras com significado, mas
# que aparecem muito em manchetes/chamadas e poluem o ranking. Revise esta
# lista conforme o objetivo da análise.
STOPWORDS_EDITORIAIS = {
    "tópico", "tópicos", "tema", "temas", "assunto", "assuntos", "categoria", "categorias", "etiqueta", "etiquetas",
    "tag", "tags", "palavra-chave", "palavras-chave", "domínio", "domínios", "semântico", "semântica",
    "classificação", "conteúdo", "informação", "notícia", "notícias",
    "pede", "pede-se", "precisa", "precisar", "preciso", "saiba", "saber", "news", "update", "updates",
    "time", "fala", "falas", "ataque", "pi", "mira", "novo", "nova", "novos", "novas", "manda", "abre", "veja",
    "dona", "rede", "três", "latest", "vídeo", "vivo", "assistir", "assista", "confira", "entenda", "mostra",
}


# ---------------------------------------------------------------------------
# Coleta
# ---------------------------------------------------------------------------

def verificar_site(url, timeout=10):
    """Retorna True se o site responde com sucesso e não usa um TLD suspeito."""
    if not url.startswith("http"):
        url = "https://" + url
    dominio = urlparse(url).netloc.lower()
    if dominio.endswith(TLDS_SUSPEITOS):
        return False
    try:
        resposta = requests.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
        return resposta.ok
    except requests.RequestException:
        return False


def filtrar_sites_validos(sites):
    """Verifica os sites em paralelo e devolve apenas os acessíveis."""
    with ThreadPoolExecutor(max_workers=8) as pool:
        resultados = list(pool.map(verificar_site, sites))
    validos = []
    for site, ok in zip(sites, resultados):
        log.info("%s %s", "✅ Válido:  " if ok else "❌ Inválido:", site)
        if ok:
            validos.append(site)
    log.info("Total de sites válidos: %d de %d", len(validos), len(sites))
    return validos


def _normalizar_data(data):
    if data is None:
        return None
    if data.tzinfo is None:
        data = data.replace(tzinfo=timezone.utc)
    return data.astimezone(timezone.utc)


def coletar_titulos(site_url, max_artigos=30, pausa=0.5):
    """Coleta até `max_artigos` títulos de um site.

    Retorna uma lista de dicts com título, URL, site e data de publicação.
    A ordem dos artigos é a descoberta pelo newspaper (não necessariamente
    cronológica); use o filtro por data em `coletar` para restringir o período.
    """
    import newspaper

    config = newspaper.Config()
    config.browser_user_agent = USER_AGENT
    config.memoize_articles = False
    config.fetch_images = False
    config.request_timeout = 10

    registros = []
    try:
        fonte = newspaper.build(site_url, config=config)
    except Exception as erro:  # newspaper pode lançar vários tipos de erro de rede/parse
        log.warning("Erro ao processar o site %s: %s", site_url, erro)
        return registros

    for artigo in fonte.articles[:max_artigos]:
        try:
            artigo.download()
            artigo.parse()
        except Exception as erro:
            log.debug("Falha em %s: %s", artigo.url, erro)
            continue
        titulo = (artigo.title or "").strip()
        if titulo:
            registros.append({
                "titulo": titulo,
                "url": artigo.url,
                "site": site_url,
                "data_publicacao": _normalizar_data(artigo.publish_date),
            })
        time.sleep(pausa)

    log.info("%-40s %d títulos", site_url, len(registros))
    return registros


def _chave_titulo(titulo):
    return re.sub(r"\s+", " ", titulo).strip().casefold()


_SEPARADOR_SUFIXO = re.compile(r"\s+[-–—|]\s+(?=[^-–—|]+$)")


def remover_sufixos(titulos, min_repeticoes=3):
    """Remove sufixos com o nome do site ("... – Pizza Fria", "... | Exame").

    Só remove o trecho após o último separador quando ele se repete em pelo
    menos `min_repeticoes` títulos, para não cortar manchetes que usam hífen.
    """
    partes = titulos.map(lambda t: _SEPARADOR_SUFIXO.split(t, maxsplit=1))
    sufixos = partes.map(lambda p: p[1].strip().casefold() if len(p) == 2 else None)
    repetidos = set(sufixos.value_counts().loc[lambda c: c >= min_repeticoes].index)
    return pd.Series(
        [p[0] if s in repetidos else t for t, p, s in zip(titulos, partes, sufixos)],
        index=titulos.index,
    )


def coletar(sites, max_artigos=30, dias=None, max_sites_paralelos=5):
    """Coleta títulos de vários sites e devolve um DataFrame pandas deduplicado.

    Se `dias` for informado, descarta artigos com data de publicação mais
    antiga que esse limite. Artigos sem data são mantidos (costumam ser
    manchetes de capa) e contabilizados no log.
    """
    # O newspaper registra como CRITICAL cada feed/categoria inexistente
    # (/rss, /feed...), o que não é erro para este pipeline.
    logging.getLogger("newspaper").setLevel(logging.CRITICAL + 1)

    with ThreadPoolExecutor(max_workers=max_sites_paralelos) as pool:
        resultados = pool.map(lambda s: coletar_titulos(s, max_artigos), sites)
        registros = [r for lista in resultados for r in lista]

    df = pd.DataFrame(registros, columns=["titulo", "url", "site", "data_publicacao"])
    df["data_publicacao"] = pd.to_datetime(df["data_publicacao"], utc=True)
    df["coletado_em"] = datetime.now(timezone.utc)
    df["titulo"] = remover_sufixos(df["titulo"])
    total = len(df)

    df = df.drop_duplicates(subset="url")
    df = df[~df["titulo"].map(_chave_titulo).duplicated()]
    log.info("Títulos coletados: %d (%d duplicados removidos)", len(df), total - len(df))

    if dias is not None:
        limite = datetime.now(timezone.utc) - timedelta(days=dias)
        sem_data = df["data_publicacao"].isna()
        antes = len(df)
        df = df[sem_data | (df["data_publicacao"] >= limite)]
        log.info(
            "Filtro de %d dias: %d artigos antigos removidos; %d sem data mantidos",
            dias, antes - len(df), int(sem_data.sum()),
        )

    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Processamento no Spark
# ---------------------------------------------------------------------------

def criar_spark(app_name="BigDataNoticias"):
    from pyspark.sql import SparkSession

    return SparkSession.builder.appName(app_name).getOrCreate()


def carregar_stopwords(extras=()):
    import nltk

    nltk.download("stopwords", quiet=True)
    # Inglês incluído porque alguns portais publicam títulos de jogos, filmes etc. em inglês
    stopwords_nltk = set(nltk.corpus.stopwords.words("portuguese")) | set(nltk.corpus.stopwords.words("english"))
    return stopwords_nltk | STOPWORDS_GRAMATICAIS | STOPWORDS_EDITORIAIS | set(extras)


def processar(spark, df_titulos, stopwords):
    """Limpa e tokeniza os títulos no Spark.

    Retorna um DataFrame Spark com as colunas `tokens` (palavras sem
    stopwords) e `bigramas`.
    """
    from pyspark.ml.feature import NGram, RegexTokenizer, StopWordsRemover
    from pyspark.sql import functions as F

    sdf = spark.createDataFrame(df_titulos[["titulo", "site"]])

    limpo = F.lower(F.col("titulo"))
    limpo = F.regexp_replace(limpo, r"https?://\S+|www\.\S+", " ")
    # Mantém apenas letras (inclusive acentuadas) e hífens internos,
    # descartando pontuação, números, símbolos e emojis.
    limpo = F.regexp_replace(limpo, r"[^\p{L}\s-]", " ")
    limpo = F.regexp_replace(limpo, r"(?<!\p{L})-|-(?!\p{L})", " ")
    sdf = sdf.withColumn("limpo", limpo)

    tokenizer = RegexTokenizer(
        inputCol="limpo", outputCol="palavras", pattern=r"\s+", minTokenLength=2
    )
    remover = StopWordsRemover(
        inputCol="palavras", outputCol="tokens",
        stopWords=sorted(stopwords), caseSensitive=True,  # texto já está em minúsculas
    )
    ngram = NGram(n=2, inputCol="tokens", outputCol="bigramas")

    return ngram.transform(remover.transform(tokenizer.transform(sdf)))


def contar(sdf, coluna):
    """Conta ocorrências dos elementos de uma coluna de arrays, em ordem decrescente."""
    from pyspark.sql import functions as F

    return (
        sdf.select(F.explode(coluna).alias("termo"))
        .groupBy("termo")
        .count()
        .orderBy(F.desc("count"), F.asc("termo"))
    )


# ---------------------------------------------------------------------------
# Exportação e visualização
# ---------------------------------------------------------------------------

def salvar_titulos(df_titulos, pasta):
    pasta = Path(pasta)
    pasta.mkdir(parents=True, exist_ok=True)
    carimbo = datetime.now().strftime("%Y%m%d_%H%M")
    caminho = pasta / f"titulos_{carimbo}.csv"
    df_titulos.to_csv(caminho, index=False, encoding="utf-8")
    return caminho


def salvar_json(contagem_pd, caminho):
    caminho = Path(caminho)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    dados = dict(zip(contagem_pd["termo"], contagem_pd["count"].astype(int)))
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
    return caminho


def grafico_nuvem(contagem_pd, titulo, caminho=None):
    import matplotlib.pyplot as plt
    from wordcloud import WordCloud

    frequencias = dict(zip(contagem_pd["termo"], contagem_pd["count"]))
    nuvem = WordCloud(width=800, height=400, background_color="white").generate_from_frequencies(frequencias)

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.imshow(nuvem, interpolation="bilinear")
    ax.axis("off")
    ax.set_title(titulo, fontsize=16)
    if caminho:
        fig.savefig(caminho, bbox_inches="tight", dpi=120)
    return fig


def grafico_barras(contagem_pd, titulo, caminho=None):
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(contagem_pd["termo"], contagem_pd["count"], color="skyblue")
    ax.set_title(titulo, fontsize=14)
    ax.set_xlabel("Termos", fontsize=12)
    ax.set_ylabel("Frequência", fontsize=12)
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    if caminho:
        fig.savefig(caminho, dpi=120)
    return fig


# ---------------------------------------------------------------------------
# Execução completa
# ---------------------------------------------------------------------------

def executar(sites=NEWS_SITES, max_artigos=30, dias=None, pasta_saida="output", top_n=20):
    """Roda o pipeline inteiro e devolve um dict com os artefatos gerados."""
    pasta = Path(pasta_saida)
    pasta.mkdir(parents=True, exist_ok=True)

    validos = filtrar_sites_validos(sites)
    df_titulos = coletar(validos, max_artigos=max_artigos, dias=dias)
    if df_titulos.empty:
        raise RuntimeError("Nenhum título coletado.")
    caminho_csv = salvar_titulos(df_titulos, pasta)

    spark = criar_spark()
    sdf = processar(spark, df_titulos, carregar_stopwords()).cache()

    palavras = contar(sdf, "tokens").limit(200).toPandas()
    bigramas = contar(sdf, "bigramas").limit(200).toPandas()

    data = datetime.now().strftime("%d/%m/%Y")
    resultado = {
        "titulos": df_titulos,
        "palavras": palavras,
        "bigramas": bigramas,
        "csv": caminho_csv,
        "json_palavras": salvar_json(palavras.head(top_n), pasta / "resultado.json"),
        "json_bigramas": salvar_json(bigramas.head(top_n), pasta / "bigramas.json"),
        "fig_nuvem": grafico_nuvem(
            palavras, f"Termos mais frequentes nos títulos ({data})", pasta / "nuvem.png"
        ),
        "fig_barras": grafico_barras(
            palavras.head(10), "Top 10 palavras mais frequentes", pasta / "top10_palavras.png"
        ),
        "fig_bigramas": grafico_barras(
            bigramas.head(10), "Top 10 bigramas mais frequentes", pasta / "top10_bigramas.png"
        ),
    }
    sdf.unpersist()
    return resultado


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--max-artigos", type=int, default=30, help="títulos por site (padrão: 30)")
    parser.add_argument("--dias", type=int, default=None, help="descarta artigos publicados há mais de N dias")
    parser.add_argument("--saida", default="output", help="pasta de saída (padrão: output)")
    parser.add_argument("--top", type=int, default=20, help="quantidade de termos no JSON (padrão: 20)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(message)s")
    resultado = executar(max_artigos=args.max_artigos, dias=args.dias, pasta_saida=args.saida, top_n=args.top)

    print("\nTop 10 palavras:")
    print(resultado["palavras"].head(10).to_string(index=False))
    print(f"\nArquivos salvos em: {Path(args.saida).resolve()}")


if __name__ == "__main__":
    main()
