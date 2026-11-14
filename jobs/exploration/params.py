from dataclasses import dataclass

@dataclass(frozen=True)
class Params:
    min_runtime: int = 40
    z_threshold: float = 2.0
    min_genre_size: int = 30
    min_votes: int = 1000
    year_from: int = 1980
    year_to: int = 2025
    top_n: int = 3