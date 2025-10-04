#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Batch-SPARQL für Wikidata (nur Standardlibs): liest eine Datei mit einer ID pro Zeile (z.B. Q42),
# fragt bestimmte Properties ab und schreibt das Ergebnis als JSON-Datei.

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import gzip

ENDPOINT = "https://query.wikidata.org/sparql"
HEADERS = {
    "User-Agent": "DLA Marbach OPAC Enrichment; mailto:dla@felixlohmeier.de",
    "Accept": "application/json",
    "Accept-Encoding": "gzip",
    "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
}

BATCH_SIZE = 10000
SLEEP_BETWEEN = 1.0  # Sekunden
MAX_RETRIES = 5
BACKOFF_FACTOR = 10.0
TIMEOUT = 60  # Sekunden

# SPARQL-Template: Platzhalter für select_vars, values und optional_lines.
SPARQL_TEMPLATE = """SELECT {select_vars}
WHERE {{
  VALUES ?id {{ {values} }}
{optional_lines}
}}
"""

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def run_query(sparql):
    body = urllib.parse.urlencode({"query": sparql}).encode("utf-8")
    req = urllib.request.Request(ENDPOINT, data=body, headers=HEADERS)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                data = resp.read()
                # Prüfung auf gzip Kompression
                # Python 3.4: resp.info().get() verwenden
                content_encoding = resp.info().get("Content-Encoding") or ""
                if "gzip" in content_encoding.lower():
                    data = gzip.decompress(data)
                return json.loads(data.decode("utf-8"))
        except urllib.error.HTTPError as e:
            # 429, 503 etc. -> retry with backoff
            if attempt == MAX_RETRIES:
                raise RuntimeError("HTTP error after {0} attempts: {1}".format(attempt, e)) from e
            wait = BACKOFF_FACTOR ** (attempt - 1)
            time.sleep(wait)
        except urllib.error.URLError as e:
            if attempt == MAX_RETRIES:
                raise RuntimeError("URL error after {0} attempts: {1}".format(attempt, e)) from e
            wait = BACKOFF_FACTOR ** (attempt - 1)
            time.sleep(wait)
    raise RuntimeError("SPARQL request failed after retries")

def main(id_file, properties):
    # Wikidata-IDs aus der TXT-Datei einlesen
    with open(id_file, 'r', encoding='utf-8') as f:
        raw = [line.strip() for line in f]
    ids = []
    for x in raw:
        if x.startswith("Q"):
            ids.append("wd:" + x)
        else:
            print("Skipping invalid id: {0!r}".format(x), file=sys.stderr)

    total = len(ids)
    processed = 0

    for batch in chunks(ids, BATCH_SIZE):
        # SPARQL-Abfrage für einen Batch vorbeiten
        values = " ".join(batch)
        select_vars = "\n".join(["?id"] + ["?{0}".format(p) for p in properties])
        optional_lines = "\n".join(["OPTIONAL {{ ?id wdt:{0} ?{0}. }}".format(p) for p in properties])
        sparql = SPARQL_TEMPLATE.format(
            select_vars=select_vars,
            values=values,
            optional_lines=optional_lines,
        )
        res = run_query(sparql)
        bindings = res.get("results", {}).get("bindings", [])
        # Ausgabe der Bindings als JSON-Lines, nur wenn >= 2 Felder vorhanden
        for b in bindings:
            rec = {}
            for k, v in b.items():
                if isinstance(v, dict) and "value" in v:
                    rec[k] = v.get("value")
            # Bereinige das id-Feld: entferne den Wikidata-Entity-Präfix, sodass nur z.B. "Q42" übrig bleibt
            prefix = "http://www.wikidata.org/entity/"
            if "id" in rec and rec["id"].startswith(prefix):
                rec["id"] = rec["id"][len(prefix):]
            if len(rec) >= 2:
                print(json.dumps(rec, ensure_ascii=False))
            print(json.dumps(rec, ensure_ascii=False))
        if not bindings:
            processed += len(batch)
            time.sleep(SLEEP_BETWEEN)
            continue
        processed += len(batch)
        print("Processed {0}/{1}".format(processed, total), file=sys.stderr)
        time.sleep(SLEEP_BETWEEN)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch SPARQL Abzug von Wikidata mit einer Liste von Wikidata-IDs")
    parser.add_argument("id_file", help="Pfad zur TXT-Datei mit Wikidata-IDs (eine ID pro Zeile in der Form Q42)")
    parser.add_argument("properties", help="Komma-separierte Liste der gewünschten Wikidata-Properties. Beispiel: P18,P109")
    args = parser.parse_args()
    props = [p.strip().upper() for p in args.properties.split(",") if p.strip()]
    main(args.id_file, props)
