# Sample documents

All documents are fictional. Companies and services in them do not exist.

| File | What it is | Expected result | Scenario |
| --- | --- | --- | --- |
| `policy-1.txt` | Privacy policy, aggressive data sharing | Analyzed, 4 issues | happy path |
| `policy-2.txt` | Privacy policy, minimal data sharing | Analyzed, 1 issue | happy path |
| `terms-1.txt` | Terms of service | Analyzed, 2 issues | happy path |
| `bad.txt` | Almost empty document | Analyzed, 0 issues — edge case: nearly empty input | happy path |
| `transient.txt` | Marker `#transient`: the fake LLM fails ~70% of calls with 529 | Retried, eventually analyzed | transient failure |
| `poison.txt` | Marker `#poison`: the document can never be parsed | Straight to DLQ, no retries | permanent failure |
| `down.txt` | Marker `#always-down`: the fake LLM always returns 503 | All retries exhausted, then DLQ | provider outage |

The fake LLM counts lines that mention third parties ("third part…") as potential issues, so the number of issues in a policy is predictable.

Upload a sample:

```bash
curl -F "file=@samples/policy-1.txt" http://localhost:5xxx/documents
```
