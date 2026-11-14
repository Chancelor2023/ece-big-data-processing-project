"""Download the remaining five official IMDb source archives."""

from imdb_pipeline.schemas import OTHER_COLUMNS
from jobs.preparation.download_titles import main


if __name__ == "__main__":
    main(default_tables=list(OTHER_COLUMNS), manifest_name="imdb_downloads.json")
