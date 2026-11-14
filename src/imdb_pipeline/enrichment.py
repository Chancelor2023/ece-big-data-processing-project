"""Spark selection and Wikipedia text processing for IMDb enrichment."""

import re
from html.parser import HTMLParser

from pyspark.sql import Window, functions as F, types as T


def select_films(basics, ratings, start_year=1980, end_year=2025, top_n=10):
    eligible = basics.filter(
        (F.col("titleType") == "movie")
        & F.col("startYear").between(start_year, end_year)
    ).join(ratings, "tconst", "inner").filter(F.col("numVotes") > 0)
    order = Window.partitionBy("startYear", "genre").orderBy(
        F.col("numVotes").desc(), F.col("averageRating").desc_nulls_last(),
        F.col("tconst").asc(),
    )
    return eligible.withColumn("genre", F.explode("genres")).withColumn(
        "genre_rank", F.row_number().over(order)
    ).filter(F.col("genre_rank") <= top_n)


def select_people(principals, films, per_film=3):
    # Filter the large credit table before grouping or ranking.
    credits = principals.join(F.broadcast(films.select("tconst")), "tconst", "left_semi")
    distinct_people = credits.groupBy("tconst", "nconst").agg(
        F.min("ordering").alias("first_ordering"),
        F.sort_array(F.collect_set("category")).alias("categories"),
    )
    order = Window.partitionBy("tconst").orderBy("first_ordering", "nconst")
    return distinct_people.withColumn("principal_rank", F.row_number().over(order)).filter(
        F.col("principal_rank") <= per_film
    )


class ArticleParagraphs(HTMLParser):
    """Used inside a Spark UDF; extract source paragraphs without generating text."""

    def __init__(self, kind):
        super().__init__(convert_charrefs=True)
        self.kind = kind
        self.heading = ""
        self.heading_parts = None
        self.seen_heading = False
        self.skip = []
        self.paragraph = None
        self.paragraphs = []
        self.has_plot = False

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        classes = attributes.get("class", "").split()
        ignored = tag in {"table", "script", "style", "sup", "figure"} or any(
            name in classes for name in ("mw-editsection", "reference", "noprint")
        )
        if self.skip or ignored:
            if tag not in {"br", "img", "hr", "meta", "link", "input", "wbr"}:
                self.skip.append(tag)
            return
        if tag == "h2":
            self.seen_heading = True
            self.heading_parts = []
        elif tag == "p":
            plot = self.heading in {"plot", "synopsis", "plot synopsis", "plot summary", "story"}
            if (self.kind == "film" and plot) or (self.kind == "person" and not self.seen_heading):
                self.paragraph = []
        elif tag == "br" and self.paragraph is not None:
            self.paragraph.append(" ")

    def handle_endtag(self, tag):
        if self.skip:
            if tag in self.skip:
                self.skip = self.skip[:len(self.skip) - 1 - self.skip[::-1].index(tag)]
            return
        if tag == "h2" and self.heading_parts is not None:
            self.heading = re.sub(r"\s+", " ", "".join(self.heading_parts)).strip().lower()
            self.has_plot |= self.heading in {"plot", "synopsis", "plot synopsis", "plot summary", "story"}
            self.heading_parts = None
        elif tag == "p" and self.paragraph is not None:
            text = re.sub(r"\s+", " ", "".join(self.paragraph)).strip()
            if text:
                self.paragraphs.append(text)
            self.paragraph = None

    def handle_data(self, data):
        if not self.skip:
            if self.heading_parts is not None:
                self.heading_parts.append(data)
            elif self.paragraph is not None:
                self.paragraph.append(data)


def article_text(markup, kind):
    if not markup:
        return None, "missing_article_html"
    parser = ArticleParagraphs(kind)
    parser.feed(markup)
    text = "\n\n".join(parser.paragraphs)
    if text:
        return text, "available"
    return None, "missing_plot_section" if kind == "film" else "missing_biography_intro"


RAW_SCHEMA = T.StructType([
    T.StructField(name, T.StringType()) for name in (
        "imdb_id", "kind", "status", "entity_uri", "article_url", "lookup_url",
        "retrieval_url", "retrieved_at_utc", "raw_response", "error",
    )
])


def clean_collected(raw):
    # All text cleaning, casts and output-schema construction run in Spark.
    extract = F.udf(article_text, "struct<text:string,status:string>")
    frame = raw.withColumn("article_title", F.get_json_object("raw_response", "$.parse.title"))
    frame = frame.withColumn("revision_id", F.get_json_object("raw_response", "$.parse.revid").cast("long"))
    frame = frame.withColumn("page_id", F.get_json_object("raw_response", "$.parse.pageid").cast("long"))
    frame = frame.withColumn("_content", extract(
        F.get_json_object("raw_response", "$.parse.text"), F.col("kind")
    ))
    return frame.select(
        "imdb_id", "kind", "entity_uri", "article_url", "lookup_url", "retrieval_url",
        "article_title", "page_id", "revision_id",
        F.to_timestamp("retrieved_at_utc").alias("retrieved_at_utc"),
        F.when(F.col("status") == "collected", F.col("_content.status"))
        .otherwise(F.col("status")).alias("status"),
        F.when(F.col("status") == "collected", F.col("_content.text")).alias("text"),
        F.when(F.col("revision_id").isNotNull(), F.concat(
            F.lit("https://en.wikipedia.org/w/index.php?oldid="), F.col("revision_id")
        )).alias("source_url"),
        F.when(F.col("status") == "collected", F.lit(
            "https://creativecommons.org/licenses/by-sa/4.0/"
        )).alias("license_url"),
        "error",
    )
