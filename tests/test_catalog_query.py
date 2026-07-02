"""Pure-logic tests for catalog LIKE/ILIKE pattern building (§1.1 fix)."""
import unittest

from src.catalog_query import (
    contains_pattern,
    escape_like_literal,
    prefix_pattern,
    substring_pattern,
)


class EscapeLikeLiteralTests(unittest.TestCase):
    def test_escapes_backslash_percent_and_underscore(self):
        self.assertEqual(escape_like_literal("a_b%c\\d"), r"a\_b\%c\\d")

    def test_plain_text_is_unchanged(self):
        self.assertEqual(escape_like_literal("report2024.txt"), "report2024.txt")

    def test_leaves_user_wildcards_untouched(self):
        self.assertEqual(escape_like_literal("IMG_*?"), r"IMG\_*?")


class ContainsPatternTests(unittest.TestCase):
    def test_plain_term_becomes_substring_match(self):
        self.assertEqual(contains_pattern("report"), "%report%")

    def test_underscore_is_treated_literally_not_as_wildcard(self):
        # Regression: "report_2024" previously became an unanchored single-char
        # wildcard match (and dropped its surrounding %), returning wrong/zero
        # rows. It must now be a literal substring match.
        self.assertEqual(contains_pattern("report_2024"), r"%report\_2024%")

    def test_literal_percent_is_escaped_and_wrapped(self):
        self.assertEqual(contains_pattern("50%"), r"%50\%%")

    def test_star_translates_to_percent_and_anchors(self):
        self.assertEqual(contains_pattern("*.mov"), "%.mov")

    def test_question_mark_translates_to_single_char(self):
        self.assertEqual(contains_pattern("IMG_?"), r"IMG\__")

    def test_prefix_wildcard_keeps_literal_underscore(self):
        self.assertEqual(contains_pattern("IMG_*"), r"IMG\_%")


class PrefixAndSubstringTests(unittest.TestCase):
    def test_prefix_escapes_and_appends_wildcard(self):
        self.assertEqual(prefix_pattern("C:/data_1"), r"C:/data\_1%")

    def test_substring_wraps_escaped_literal(self):
        self.assertEqual(substring_pattern("a_b"), r"%a\_b%")

    def test_substring_of_path_with_percent(self):
        self.assertEqual(substring_pattern("/srv/50%off"), r"%/srv/50\%off%")


if __name__ == "__main__":
    unittest.main()
