| Check | Expected | Actual | Result |
|---|---:|---:|---|
| rejected as INVALID_AMOUNT | 911 | 911 | PASS  |
| rejected as NEGATIVE_AMOUNT | 839 | 839 | PASS  |
| rejected as UNKNOWN_ACCOUNT | 815 | 815 | PASS  |
| rejected as MISSING_TIMESTAMP | 279 | 279 | PASS  |
| duplicate rows detected | 11,322 | 11,322 | PASS  |
| late arrivals detected | 55,550 | 55,550 | PASS  |
| missing merchant warnings | 4,398 | 4,398 | PASS  |
| lowercase currency left after cleaning | 0 | 0 | PASS  |

| Rule | Injected cases | Detected | Recall | Alerts | Alerts on injected cases | Precision |
|---|---:|---:|---:|---:|---:|---:|
| OUTLIER | 20 | 20 | 100% | 36 | 20 | 56% |
| STRUCTURING | 12 | 12 | 100% | 12 | 12 | 100% |
| VELOCITY | 15 | 15 | 100% | 15 | 15 | 100% |
