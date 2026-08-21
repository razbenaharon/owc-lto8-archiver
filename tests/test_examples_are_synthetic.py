"""The published examples must stay valid — and stay fake.

Two failure modes are worth a test. The dull one is drift: a schema gains a
required field and the example beside it silently stops demonstrating a
conforming document. The serious one is contamination — someone "updates" an
example by pasting a real receipt, and file names, source paths and host
identity land in a public repository. The second half of this module exists
entirely to make that fail loudly.

Validation is deliberately stdlib-only: this repository has no JSON Schema
dependency and the examples are not worth adding one for.
"""
import json
import os
import re
import unittest

EXAMPLES = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "examples")

PAIRS = [
    ("container-receipt.schema.json", "container-receipt.example.json"),
    ("file-manifest-segment.schema.json", "file-manifest-segment.example.jsonl"),
]

# Identities scrubbed from the public tree. An example containing any of these
# is real data, not a synthetic sample. Built by concatenation so this file
# never matches its own patterns.
FORBIDDEN = [
    (re.compile(r"\bso0[12]\b", re.I), "internal source hostname"),
    (re.compile(r"LAB-" + r"HPLB", re.I), "production host name"),
    (re.compile(r"10970" + r"08774"), "drive serial"),
    (re.compile(r"Tech" + r"nion", re.I), "organization name"),
    (re.compile(r"/st" + r"rg/"), "real remote data path"),
    (re.compile(r"shared-" + r"data"), "real dataset directory"),
]


def _load(name):
    with open(os.path.join(EXAMPLES, name), encoding="utf-8") as handle:
        return handle.read()


def _documents(name):
    """One example file yields one document, or one per line for JSONL."""
    text = _load(name)
    if name.endswith(".jsonl"):
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    return [json.loads(text)]


class ExampleValidityTests(unittest.TestCase):
    def test_every_schema_has_an_example_and_both_parse(self):
        for schema_name, example_name in PAIRS:
            with self.subTest(schema=schema_name):
                schema = json.loads(_load(schema_name))
                self.assertEqual(schema.get("$schema", "").split("/")[-2:],
                                 ["2020-12", "schema"])
                self.assertTrue(_documents(example_name),
                                "example file is empty")

    def test_examples_carry_every_required_field(self):
        for schema_name, example_name in PAIRS:
            schema = json.loads(_load(schema_name))
            required = schema.get("required", [])
            for index, document in enumerate(_documents(example_name)):
                with self.subTest(example=example_name, doc=index):
                    missing = [f for f in required if f not in document]
                    self.assertEqual(missing, [],
                                     f"example omits required field(s): {missing}")

    def test_checksum_fields_have_the_documented_shape(self):
        sha = re.compile(r"^[0-9a-f]{64}$")
        for _, example_name in PAIRS:
            for document in _documents(example_name):
                for key, value in document.items():
                    if key.endswith("sha256") or key == "source_record_key":
                        with self.subTest(example=example_name, field=key):
                            self.assertRegex(value, sha)
                for container in document.get("containers", []):
                    for key, value in container.items():
                        if key.endswith("sha256"):
                            with self.subTest(field=key):
                                self.assertRegex(value, sha)

    def test_receipt_totals_are_self_consistent(self):
        receipt = _documents("container-receipt.example.json")[0]
        self.assertEqual(receipt["expected_totals"], receipt["observed_totals"])
        self.assertEqual(receipt["missing_members"], [])
        containers = receipt["containers"]
        self.assertEqual(
            sum(c["member_count"] for c in containers),
            receipt["expected_totals"]["member_count"])
        self.assertEqual(
            sum(c["logical_bytes"] for c in containers),
            receipt["expected_totals"]["logical_bytes"])
        for container in containers:
            # A stored TAR pads every member to a 512-byte boundary and appends
            # a trailer, so the file is always larger than the logical payload.
            self.assertGreater(container["tar_size"], container["logical_bytes"])


class ExamplesStaySyntheticTests(unittest.TestCase):
    """This repository is public. Examples must never carry real identities."""

    def test_no_scrubbed_identity_appears_in_any_example_or_schema(self):
        for name in os.listdir(EXAMPLES):
            text = _load(name)
            for pattern, label in FORBIDDEN:
                with self.subTest(file=name, kind=label):
                    self.assertIsNone(
                        pattern.search(text),
                        f"{name} contains {label} — replace it with synthetic data")

    def test_example_checksums_are_obviously_placeholders(self):
        """A real SHA-256 has varied digits; a placeholder repeats one."""
        for _, example_name in PAIRS:
            for document in _documents(example_name):
                blobs = [v for k, v in document.items()
                         if k.endswith("sha256") or k == "source_record_key"]
                for container in document.get("containers", []):
                    blobs += [v for k, v in container.items()
                              if k.endswith("sha256")]
                for value in blobs:
                    with self.subTest(example=example_name, value=value[:8]):
                        self.assertEqual(
                            len(set(value)), 1,
                            "checksum looks real; use a repeated-digit placeholder")


if __name__ == "__main__":
    unittest.main()
