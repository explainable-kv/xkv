import ssl, sys, threading, time, json
from xkv_client import Client, XkvError
ctx = ssl.create_default_context(cafile="cert.pem")
url = "https://127.0.0.1:7443"
agent = Client(url, token=open("token").read().strip(), actor="pricing-agent", ssl_context=ctx)
reader = Client(url, token=open("read-token").read().strip(), ssl_context=ctx)
seen = []
def watch():
    for rev in reader.watch(prefix="price/"):
        seen.append(f'{rev["rev"]} {rev["key"]}={rev["value"]} by {rev["actor"]}: {rev["reason"]}')
        if len(seen) == 2: return
t = threading.Thread(target=watch); t.start(); time.sleep(0.3)
agent.put("signal:competitor/sku-1042", "18.95", reason="scraped competitor price", actor="price-scraper", ref="crawl-2211")
agent.put("price/sku-1042", "18.90", reason="match the competitor minus 5 cents", causes=["signal:competitor/sku-1042"], ref="run-8812")
agent.put("policy:margin-floor", "19.40", reason="finance set a 19.40 floor", actor="finance@shop", ref="FIN-77")
agent.put("price/sku-1042", "19.40", reason="floor from the margin policy", causes=["policy:margin-floor", "signal:competitor/sku-1042"], ref="run-8813")
t.join(5)
print("watcher (read token) saw:"); [print("  " + s) for s in seen]
try:
    reader.put("price/sku-1042", "1.00", reason="try")
except XkvError as e:
    print("read token write:", e.status, e.message)
try:
    Client(url, ssl_context=ctx).stats()
except XkvError as e:
    print("no token:", e.status, e.message)
prov = agent.why("price/sku-1042", prov=True)
json.dump(prov, open("why.jsonld", "w"), indent=1)
print("PROV nodes:", ", ".join(sorted(set(n["@type"] for n in prov["@graph"]))), len(prov["@graph"]))
