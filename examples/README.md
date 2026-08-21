# Examples

**Everything in this directory is synthetic.** The values were invented for
documentation, not derived from any real archive: the paths, host names, tape
labels, identifiers and checksums do not correspond to anything that exists.

Real manifests and receipts are operational data. They enumerate actual file
names, directory structures and source paths, so they are kept outside this
repository and are never published. The schemas here are what is public; the
data they describe is not.

| File | What it describes |
| --- | --- |
| `container-receipt.schema.json` | `receipt.json`, published beside the stored-TAR containers of one chunk. The integrity anchor: exact size and SHA-256 per container, so an intact container can be distinguished from a truncated one without reading tape. |
| `container-receipt.example.json` | A one-container receipt, fully synthetic. |
| `file-manifest-segment.schema.json` | One line of a per-file manifest segment (`*.jsonl.zst`). After a validated export is pruned these segments — not PostgreSQL — hold the per-file inventory of packed small files. |
| `file-manifest-segment.example.jsonl` | Two synthetic rows, uncompressed for readability; real segments are zstd-compressed. |

Validate an example against its schema with any JSON Schema 2020-12 checker,
for instance:

```bash
python -m pip install check-jsonschema
check-jsonschema --schemafile examples/container-receipt.schema.json examples/container-receipt.example.json
```
