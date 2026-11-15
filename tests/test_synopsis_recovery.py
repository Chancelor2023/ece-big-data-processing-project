"""Conservative source extraction and preservation of existing film text."""
import unittest
from imdb_pipeline.enrichment import (
    article_text, apply_synopsis_recoveries, same_wikipedia_article,
)
from imdb_pipeline.spark import create_spark


class SynopsisParserTests(unittest.TestCase):
    def test_simple_h2_aliases_stop_at_next_section(self):
        for heading in ("Summary", "Premise", "Storyline"):
            with self.subTest(heading=heading):
                markup = f'<p>Introduction.</p><h2>{heading}<span class="mw-editsection">edit</span></h2><p>Source synopsis.<sup>[1]</sup></p><h3>Ending</h3><p>Ending.</p><h2>Reception</h2><p>Review text.</p>'
                self.assertEqual(article_text(markup, "film"), ("Source synopsis.\n\nEnding.", "available"))

    def test_multiple_plot_sections_require_review(self):
        markup = '<h2>Summary</h2><p>First film.</p><h2>Premise</h2><p>Another film.</p>'
        self.assertEqual(article_text(markup, "film"), (None, "ambiguous_plot_sections"))

    def test_empty_duplicate_plot_section_preserves_synopsis(self):
        populated = '<h2>Plot</h2><p>Source synopsis.</p>'
        for empty in ('<h2>Plot</h2>', '<h2>Plot</h2><p> \n </p>'):
            for sections in (empty + populated, populated + empty):
                with self.subTest(sections=sections):
                    markup = sections + '<h2>Reception</h2><p>Review text.</p>'
                    self.assertEqual(article_text(markup, "film"),
                                     ("Source synopsis.", "available"))

    def test_h3_and_mixed_headings_are_not_accepted(self):
        for markup in ('<h2>Films</h2><h3>Plot</h3><p>Unscoped film.</p>',
                       '<h2>Plot and theme</h2><p>Mixed content.</p>'):
            self.assertEqual(article_text(markup, "film"), (None, "missing_plot_section"))

    def test_infobox_and_lead_are_not_substitutes(self):
        markup = '<table><tr><td><h2>Summary</h2><p>Infobox.</p></td></tr></table><p>Lead.</p><h2>Cast</h2><p>People.</p>'
        self.assertEqual(article_text(markup, "film"), (None, "missing_plot_section"))

    def test_biography_remains_intro_only(self):
        markup = '<p>Human biography.</p><h2>Summary</h2><p>Later text.</p>'
        self.assertEqual(article_text(markup, "person"), ("Human biography.", "available"))

    def test_redirect_and_series_pages_require_review(self):
        self.assertFalse(same_wikipedia_article('https://en.wikipedia.org/wiki/Zeitgeist:_Moving_Forward', 'Zeitgeist (film series)'))
        self.assertFalse(same_wikipedia_article(None, 'Example'))
        self.assertFalse(same_wikipedia_article('https://example.com/wiki/Film', 'Film'))
        self.assertTrue(same_wikipedia_article('https://en.wikipedia.org/wiki/Habf%C3%BCrd%C5%91', 'Habfürdő'))
        self.assertTrue(same_wikipedia_article('https://en.wikipedia.org/wiki/A_Film_(2020_film)', 'A Film (2020 film)'))


class SynopsisUpdateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark('synopsis-update-tests')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_only_missing_text_and_status_change(self):
        films = self.spark.createDataFrame([
            ('tt1', 'available', 'Existing text.', 'url1'),
            ('tt2', 'missing_plot_section', None, 'url2'),
            ('tt3', 'not_found', None, None),
            ('tt4', 'missing_plot_section', None, 'url4'),
        ], 'tconst string, status string, synopsis string, source_url string')
        recoveries = self.spark.createDataFrame([
            ('tt1', 'Do not overwrite.'), ('tt2', 'Source text.'),
            ('tt3', 'Not an eligible target.'), ('tt4', ' '),
        ], 'tconst string, synopsis string')
        rows = {r.tconst:r for r in apply_synopsis_recoveries(films,recoveries).collect()}
        self.assertEqual(len(rows),4)
        self.assertEqual(rows['tt1'].synopsis,'Existing text.')
        self.assertEqual((rows['tt2'].synopsis,rows['tt2'].status,rows['tt2'].source_url),('Source text.','available','url2'))
        self.assertEqual(rows['tt3'].status,'not_found')
        self.assertEqual(rows['tt4'].status,'missing_plot_section')


if __name__ == '__main__':
    unittest.main()
