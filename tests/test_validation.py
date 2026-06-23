"""
Unit tests for sidecar validation logic.
Run: python tests/test_validation.py
"""

import json
import re
import unittest

LETTERS_RE = re.compile(r"^[A-Z]{3,5}$")


def letter_sum(letters: str) -> int:
    return sum(ord(c) - 64 for c in letters)


def validate_message(msg: object) -> tuple[bool, str]:
    if not isinstance(msg, dict):
        return False, "not a JSON object"
    msg_id = msg.get("id")
    letters = msg.get("letters")
    number = msg.get("number")
    if not isinstance(msg_id, str) or not msg_id:
        return False, f"invalid id: {msg_id!r}"
    if not isinstance(letters, str) or not LETTERS_RE.match(letters):
        return False, f"invalid letters: {letters!r}"
    if not isinstance(number, int) or isinstance(number, bool) or not (1 <= number <= 200):
        return False, f"invalid number: {number!r}"
    return True, ""


class TestLetterSum(unittest.TestCase):
    def test_abc_equals_6(self):
        self.assertEqual(letter_sum("ABC"), 6)

    def test_zz_equals_52(self):
        self.assertEqual(letter_sum("ZZ"), 52)


class TestValidationLogic(unittest.TestCase):
    def test_sum_greater_than_number_passes(self):
        self.assertTrue(letter_sum("ABC") > 5)  # 6 > 5

    def test_sum_equal_to_number_fails(self):
        # strictly greater than, not >=
        self.assertFalse(letter_sum("ABC") > 6)  # 6 == 6

    def test_sum_less_than_number_fails(self):
        self.assertFalse(letter_sum("ABC") > 7)  # 6 < 7


class TestSchemaValidation(unittest.TestCase):
    def _base(self) -> dict:
        return {
            "id": "test-id",
            "vehicle_id": "bus-101",
            "letters": "ABC",
            "number": 100,
            "timestamp": "2024-01-01T00:00:00+00:00",
        }

    def test_lowercase_letters_rejected(self):
        ok, _ = validate_message({**self._base(), "letters": "abc"})
        self.assertFalse(ok)

    def test_letters_too_long_rejected(self):
        ok, _ = validate_message({**self._base(), "letters": "ABCDEF"})
        self.assertFalse(ok)

    def test_letters_too_short_rejected(self):
        ok, _ = validate_message({**self._base(), "letters": "AB"})
        self.assertFalse(ok)

    def test_number_below_range_rejected(self):
        ok, _ = validate_message({**self._base(), "number": 0})
        self.assertFalse(ok)

    def test_number_above_range_rejected(self):
        ok, _ = validate_message({**self._base(), "number": 201})
        self.assertFalse(ok)

    def test_number_as_float_rejected(self):
        ok, _ = validate_message({**self._base(), "number": 5.5})
        self.assertFalse(ok)

    def test_number_as_bool_rejected(self):
        # bool is a subclass of int in Python, so isinstance(True, int) is True.
        # The explicit bool check is load-bearing: without it True (==1) would pass.
        ok, _ = validate_message({**self._base(), "number": True})
        self.assertFalse(ok)

    def test_malformed_json_raises(self):
        with self.assertRaises(json.JSONDecodeError):
            json.loads("{not valid json}")

    def test_not_a_dict_rejected(self):
        ok, _ = validate_message([1, 2, 3])
        self.assertFalse(ok)

    def test_missing_letters_rejected(self):
        msg = {k: v for k, v in self._base().items() if k != "letters"}
        ok, _ = validate_message(msg)
        self.assertFalse(ok)

    def test_missing_number_rejected(self):
        msg = {k: v for k, v in self._base().items() if k != "number"}
        ok, _ = validate_message(msg)
        self.assertFalse(ok)

    def test_missing_id_rejected(self):
        msg = {k: v for k, v in self._base().items() if k != "id"}
        ok, _ = validate_message(msg)
        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main(verbosity=2)
