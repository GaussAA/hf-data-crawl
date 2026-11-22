"""详情接口的返回键与耗时；决定 files 换端点后的超时预算。"""

import time

import httpx

HF = "https://huggingface.co"
http = httpx.Client(proxy="http://127.0.0.1:7897", timeout=300.0, follow_redirects=True)

for dataset in ("stanfordnlp/imdb", "HuggingFaceFW/fineweb", "mickeykang/Event6D"):
    for label, params in (("默认", {}), ("blobs", {"blobs": "true"})):
        start = time.monotonic()
        resp = http.get(f"{HF}/api/datasets/{dataset}", params=params)
        cost = time.monotonic() - start
        keys = sorted(resp.json()) if resp.status_code == 200 else []
        print(
            f"{dataset[:34]:36s} {label:6s} {len(resp.content) / 1e3:8.0f} KB "
            f"{cost:6.2f}s  键={keys}"
        )
