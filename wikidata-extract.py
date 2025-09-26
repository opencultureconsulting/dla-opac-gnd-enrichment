#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Batch-SPARQL für Wikidata (nur Standardlibs): liest eine Datei mit einer ID pro Zeile (z.B. Q42),
# fragt bestimmte Properties ab und schreibt diese jeweils in eine TSV-Datei.

import sys
import time
import os
import json
import urllib.request
import urllib.parse
import urllib.error

ENDPOINT = "https://query.wikidata.org/sparql"
HEADERS = {
    "User-Agent": "DLA Marbach OPAC Enrichment; mailto:dla@felixlohmeier.de",
    "Accept": "application/json",
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
    req = urllib.request.Request(ENDPOINT, data=body, headers=HEADERS, method="POST")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                data = resp.read()
                return json.loads(data.decode("utf-8"))
        except urllib.error.HTTPError as e:
            # 429, 503 etc. -> retry with backoff
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"HTTP error after {attempt} attempts: {e}") from e
            wait = BACKOFF_FACTOR ** (attempt - 1)
            time.sleep(wait)
        except urllib.error.URLError as e:
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"URL error after {attempt} attempts: {e}") from e
            wait = BACKOFF_FACTOR ** (attempt - 1)
            time.sleep(wait)
    raise RuntimeError("SPARQL request failed after retries")

def main(id_file, out_dir, properties=None):
    with open(id_file, "r", encoding="utf-8") as f:
        raw = [line.strip() for line in f]
    # Nur IDs akzeptieren, die mit Q beginnen
    ids = []
    for x in raw:
        if x.startswith("Q"):
            ids.append("wd:" + x)
        else:
            print(f"Skipping invalid id: {x!r}", file=sys.stderr)

    total = len(ids)
    processed = 0

    # Ausgabe-Verzeichnis anlegen
    os.makedirs(out_dir, exist_ok=True)
    # Normiere Properties
    properties = [p.strip().upper() for p in properties if p.strip()]
    if not properties:
        print("No properties specified.", file=sys.stderr)
        return
    # Ausgabe-Dateien aus properties erstellen
    prop_files = {p: os.path.join(out_dir, f"{p}.tsv") for p in properties}

    # Offene Filehandles
    handles = {}
    for key, path in prop_files.items():
        fh = open(path, "w", encoding="utf-8")
        handles[key] = fh

    try:
        for batch in chunks(ids, BATCH_SIZE):
            values = " ".join(batch)
            # Erzeuge SELECT-Variablen und OPTIONAL-Zeilen dynamisch aus properties
            select_vars = "\n".join(["?id"] + [f"?{p}" for p in properties])
            optional_lines = "\n".join([f"OPTIONAL {{ ?id wdt:{p} ?{p}. }}" for p in properties])
            sparql = SPARQL_TEMPLATE.format(
                select_vars=select_vars,
                values=values,
                optional_lines=optional_lines,
            )
            res = run_query(sparql)
            bindings = res.get("results", {}).get("bindings", [])
            if not bindings:
                processed += len(batch)
                time.sleep(SLEEP_BETWEEN)
                continue

            for b in bindings:
                qid = b.get("id", {}).get("value", "").strip()
                qid = qid.replace('http://www.wikidata.org/entity/','')
                if not qid:
                    continue
                for prop in properties:
                    if prop in b:
                        val = b[prop].get("value", "").strip()
                        # spezielle Bereinigung für Datei-URIs bei P18/P109
                        if prop in ("P18", "P109"):
                            val = val.replace('http://commons.wikimedia.org/wiki/Special:FilePath/','')
                        if val:
                            handles[prop].write(f"{qid}\t{val}\n")

            processed += len(batch)
            print(f"Processed {processed}/{total}", file=sys.stderr)
            time.sleep(SLEEP_BETWEEN)
    finally:
        for fh in handles.values():
            fh.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 wikidata-extract.py input_ids.txt out_dir P18,P109")
        sys.exit(1)
    props = None
    if len(sys.argv) >= 4:
        props = [p.strip().upper() for p in sys.argv[3].split(",") if p.strip()]
    main(sys.argv[1], sys.argv[2], props)
