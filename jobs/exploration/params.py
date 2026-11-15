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

# Q4 - one-hit directors
Q4_HIGH_RATING = 8.0        # the single "hit" must be >= this
Q4_LOW_AVG_RATING = 6.0     # average of the other movies must be <= this
Q4_MIN_OTHER_MOVIES = 2     # "multiple other movies"
Q4_MIN_VOTES = 100          # ignore ratings based on very few votes

# Q5 - series whose quality declined
Q5_MIN_FULL_SEASONS = 3
Q5_MIN_EPISODES_PER_SEASON = 10
Q5_MIN_EPISODE_VOTES = 10

# Q6 - success across languages
Q6_SUCCESS_RATING = 7.0
Q6_SUCCESS_MIN_VOTES = 1000
Q6_MIN_LANGUAGES = 2
Q6_MIN_LANGUAGES_STRICT = 3

# Q7 - early career peak
Q7_MIN_CAREER_YEARS = 20
Q7_MIN_MOVIES = 15
Q7_MIN_VOTES = 100

# Q8 - multi-talented professionals
Q8_MIN_VOTES = 100