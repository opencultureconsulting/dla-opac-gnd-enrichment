#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Batch Abfrage von Wikimedia Commons API query/imageinfo
# https://www.mediawiki.org/wiki/API:Imageinfo
# Input: Wikimedia Commons Dateinamen
# Output: .query.pages als JSON-Lines
# Beispiele:
# python3 commons-extract.py "An Anna Blume.jpg" "AndalusQuran.JPG"
# python3 commons-extract.py "An%20Anna%20Blume.jpg" "AndalusQuran.JPG"
# python3 commons-extract.py < list.txt

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import gzip

# Wikimedia Commons API imageinfo/extmetadata
ENDPOINT = "https://commons.wikimedia.org/w/api.php"
PARAMS = "?action=query&format=json&prop=imageinfo&iiprop=extmetadata"
HEADERS = {
    "User-Agent": "DLA Marbach OPAC Enrichment; mailto:dla@felixlohmeier.de",
    "Accept-Encoding": "gzip"
}
BATCH_SIZE = 50 # Maximale Anzahl, die Commons API erlaubt
SLEEP_BETWEEN = 1.0  # Sekunden zwischen den Anfragen
MAX_RETRIES = 5
BACKOFF_FACTOR = 10.0
TIMEOUT = 30 # Sekunden Timeout für HTTP-Anfragen

def chunks(lst, n):
    """Teilt eine Liste in n-große Stücke auf."""
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def run_query(encoded_titles):
    """Führt eine Anfrage durch und gibt das Ergebnis als JSON zurück."""
    url = f"{ENDPOINT}{PARAMS}&titles={encoded_titles}"
    req = urllib.request.Request(url, headers=HEADERS, method="GET")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                data = resp.read()
                # Prüfung auf gzip Kompression
                content_encoding = (resp.getheader("Content-Encoding") if hasattr(resp, "getheader")
                                    else resp.info().get("Content-Encoding")) or ""
                if "gzip" in content_encoding.lower():
                    data = gzip.decompress(data)
                return json.loads(data.decode("utf-8"))
        except urllib.error.HTTPError as e:
            # Fehlerbehandlung: Retry bei HTTP-Fehlern (z.B. 429, 503)
            print(f"HTTP error: {e}", file=sys.stderr)
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"HTTP error after {attempt} attempts: {e}") from e
            wait = BACKOFF_FACTOR ** (attempt - 1)
            time.sleep(wait)
        except urllib.error.URLError as e:
            # Fehlerbehandlung: Retry bei URL-Fehlern
            print(f"URL error: {e}", file=sys.stderr)
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"URL error after {attempt} attempts: {e}") from e
            wait = BACKOFF_FACTOR ** (attempt - 1)
            time.sleep(wait)
    raise RuntimeError("request failed after retries")

def main(filenames):
    # Einlesen der Dateinamen (entweder von stdin oder als Argumente)
    inputs = []
    if not filenames or (len(filenames) == 1 and filenames[0] == "-"):
        data = sys.stdin.read()
        inputs = data.splitlines()
    else:
        inputs = list(filenames)

    ids = []
    for x in inputs:
        # URL decoding
        x = urllib.parse.unquote(x)
        # Präfix File: setzen
        if x.lower().startswith("file:"):
            x = x[len("file:"):]
        ids.append("File:" + x)

    total = len(ids)
    processed = 0

    i = 0
    while i < total:
        # Dynamische Batchgröße (Commons-Dateinamen url-kodiert <= 7800 Zeichen)
        batch_size = min(BATCH_SIZE, total - i)
        while batch_size > 0:
            batch = ids[i:i+batch_size]
            titles = "|".join(batch)
            encoded_titles = urllib.parse.quote(titles, safe='')
            if len(encoded_titles) <= 7800:
                break
            batch_size -= 1
        # Anfrage an die API für den aktuellen Batch
        res = run_query(encoded_titles)

        # Ausgabe der Ergebnisse (query/pages) als JSON Lines
        pages = res.get("query", {}).get("pages", {})
        page_items = list(pages.values())
        for page in page_items:
            print(json.dumps(page, ensure_ascii=False))

        processed += len(batch)
        print(f"Processed {processed}/{total}", file=sys.stderr)
        time.sleep(SLEEP_BETWEEN)
        i += batch_size

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch Abfrage von Wikimedia Commons Bildinfos (imageinfo und imageinfo/extmetadata)")
    parser.add_argument("filenames", nargs="*", help="Wikimedia Commons Dateinamen")
    args = parser.parse_args()
    main(args.filenames)
