"""Typed Spark transformations for people, crews, episodes, credits and aliases."""

from pyspark.sql import functions as F

from imdb_pipeline.preparation import normalized
from imdb_pipeline.schemas import OTHER_COLUMNS, TABLE_KEYS


def prepare_other_rows(raw, table):
    columns = OTHER_COLUMNS[table]
    source = raw.select(
        *[normalized(F.col(c)).alias(c) for c in columns],
        "_raw_line", "_field_count",
    )
    expressions = {c: F.col(c) for c in columns}
    issues = []
    required = []

    def identifier(name, prefix, mandatory=True):
        value = F.col(name)
        valid = value.isNotNull() & value.rlike(f"^{prefix}[0-9]+$")
        if mandatory:
            required.append((~valid, f"invalid_{name}"))
        return valid

    def integer(name, minimum=0, maximum=None):
        value = F.expr(f"try_cast(`{name}` as int)")
        valid = value.isNotNull() & (value >= minimum)
        if maximum is not None:
            valid = valid & (value <= maximum)
        issues.append(F.when(
            F.col(name).isNotNull() & ~valid, F.lit(f"invalid_{name}")
        ))
        result = F.when(valid, value)
        expressions[name] = result
        return result

    def boolean(name):
        value = F.col(name)
        issues.append(F.when(
            value.isNotNull() & ~value.isin("0", "1"),
            F.lit(f"invalid_{name}"),
        ))
        expressions[name] = F.when(value.isin("0", "1"), value == "1")

    def array(name, separator=",", prefix=None):
        tokens = F.transform(
            F.split(F.col(name), separator), lambda item: F.trim(item)
        )
        usable = F.filter(tokens, lambda item: ~item.isin("", "\\N"))
        if prefix:
            valid = F.filter(usable, lambda item: item.rlike(f"^{prefix}[0-9]+$"))
            issues.append(F.when(
                F.size(valid) != F.size(usable),
                F.lit(f"invalid_token_{name}"),
            ))
            usable = valid
        unique = F.array_sort(F.array_distinct(usable))
        issues.append(F.when(
            F.size(unique) != F.size(usable), F.lit(f"duplicate_token_{name}")
        ))
        expressions[name] = F.when(F.size(unique) > 0, unique).otherwise(
            F.lit(None).cast("array<string>")
        )

    if table == "name.basics":
        identifier("nconst", "nm")
        birth = integer("birthYear", 1, 9999)
        death = integer("deathYear", 1, 9999)
        reverse = birth.isNotNull() & death.isNotNull() & (death < birth)
        issues.append(F.when(reverse, F.lit("deathYear_before_birthYear")))
        expressions["deathYear"] = F.when(~reverse, death)
        array("primaryProfession")
        array("knownForTitles", prefix="tt")
    elif table == "title.crew":
        identifier("tconst", "tt")
        array("directors", prefix="nm")
        array("writers", prefix="nm")
    elif table == "title.episode":
        identifier("tconst", "tt")
        identifier("parentTconst", "tt")
        required.append((F.col("tconst") == F.col("parentTconst"), "self_parent"))
        # Season zero denotes specials and is not an invalid season.
        integer("seasonNumber", 0)
        integer("episodeNumber", 0)
    elif table == "title.principals":
        identifier("tconst", "tt")
        identifier("nconst", "nm")
        ordering = integer("ordering", 1)
        required.append((ordering.isNull(), "invalid_ordering"))
        characters = F.from_json(F.col("characters"), "array<string>")
        issues.append(F.when(
            F.col("characters").isNotNull() & characters.isNull(),
            F.lit("invalid_characters_json"),
        ))
        expressions["characters"] = characters
    elif table == "title.akas":
        identifier("titleId", "tt")
        ordering = integer("ordering", 1)
        required.append((ordering.isNull(), "invalid_ordering"))
        boolean("isOriginalTitle")
        # IMDb uses ASCII STX within these TSV fields, not a comma separator.
        array("types", separator="\x02")
        array("attributes", separator="\x02")
    else:
        raise ValueError(f"Unsupported table: {table}")
    rejection = F.when(F.col("_field_count") != len(columns), "invalid_field_count")
    for invalid, reason in required:
        rejection = rejection.when(invalid, reason)
    return source.select(
        *[expressions[c].alias(c) for c in columns],
        "_raw_line", "_field_count",
        F.array_compact(F.array(*issues)).alias("_issues"),
        rejection.alias("_reject_reason"),
    )


SCALAR_REFERENCES = {
    "name.basics": [],
    "title.crew": [("tconst", "title.basics", "tconst")],
    "title.episode": [
        ("tconst", "title.basics", "tconst"),
        ("parentTconst", "title.basics", "tconst"),
    ],
    "title.principals": [
        ("tconst", "title.basics", "tconst"),
        ("nconst", "name.basics", "nconst"),
    ],
    "title.akas": [("titleId", "title.basics", "tconst")],
}
ARRAY_REFERENCES = {
    "name.basics": (["knownForTitles"], "title.basics", "tconst"),
    "title.crew": (["directors", "writers"], "name.basics", "nconst"),
}
