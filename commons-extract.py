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

ENDPOINT = "https://commons.wikimedia.org/w/api.php"
HEADERS = {
    "User-Agent": "DLA Marbach OPAC Enrichment; mailto:dla@felixlohmeier.de",
    "Accept-Encoding": "gzip"
}
BATCH_SIZE = 50 # Maximale Anzahl, die Commons API erlaubt
SLEEP_BETWEEN = 1.0  # Sekunden
MAX_RETRIES = 5
BACKOFF_FACTOR = 10.0
TIMEOUT = 30 # Sekunden

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def run_query(titles):
    params = {
                "action": "query",
                "format": "json",
                "prop": "imageinfo",
                "iiprop": "extmetadata",
                "titles": titles
            }
    url = ENDPOINT + '?' + urllib.parse.urlencode(params)
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
            # 429, 503 etc. -> retry with backoff
            print(f"HTTP error: {e}", file=sys.stderr)
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"HTTP error after {attempt} attempts: {e}") from e
            wait = BACKOFF_FACTOR ** (attempt - 1)
            time.sleep(wait)
        except urllib.error.URLError as e:
            print(f"URL error: {e}", file=sys.stderr)
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"URL error after {attempt} attempts: {e}") from e
            wait = BACKOFF_FACTOR ** (attempt - 1)
            time.sleep(wait)
    raise RuntimeError("request failed after retries")

def main(filenames):
    # Support für stdin
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
        # Präfix file: or File: entfernen
        if x.lower().startswith("file:"):
            x = x[len("file:"):]
        ids.append("File:" + x)

    total = len(ids)
    processed = 0

    for batch in chunks(ids, BATCH_SIZE):
        # Abfrage für einen Batch vorbeiten
        titles = "|".join(batch)
        res = run_query(titles)

        # Ausgabe als JSON Lines
        pages = res.get("query", {}).get("pages", {})
        page_items = list(pages.values())
        for page in page_items:
            print(json.dumps(page, ensure_ascii=False))

        processed += len(batch)
        print(f"Processed {processed}/{total}", file=sys.stderr)
        time.sleep(SLEEP_BETWEEN)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch Abfrage von Wikimedia Commons Bildinfos (imageinfo und imageinfo/extmetadata)")
    parser.add_argument("filenames", nargs="*", help="Wikimedia Commons Dateinamen")
    args = parser.parse_args()
    main(args.filenames)
