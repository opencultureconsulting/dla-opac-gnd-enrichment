#!/usr/bin/env python3
import argparse
import pandas as pd
import json

parser = argparse.ArgumentParser(description='Transformiere GZIP-komprimierte JSONL-Datei')
parser.add_argument('jsonl_file', help='Pfad zur GZIP-komprimierten JSONL-Datei')
args = parser.parse_args()

chunksize = 50000

def process_chunk(df):

    # Felder umbennen
    df = df.rename(columns={
        'gndIdentifier': 'id',
    })

    # depiction.id
    df['depiction'] = [
        [item['id'] for item in x] if isinstance(x, list) else None
        for x in df['depiction']
    ]
    df['depiction'] = [None if not x else x for x in df['depiction']]

    # sameAs.id
    df['wikidata'] = [
        [item['id'] for item in x
            if isinstance(item, dict) and 'id' in item and item['id'].startswith('http://www.wikidata.org/entity/')
        ] if isinstance(x, list) else None
        for x in df['sameAs']
    ]
    df['wikidata'] = [None if not x else x for x in df['wikidata']]
    del df['sameAs']
    # df['sameAs'] = [None if not x else x for x in df['sameAs']]

    # Unnötige Spalten entfernen (nur falls vorhanden)
    drop_cols = [
        'preferredName',
        'type',
        'variantName'
    ]
    df = df.drop(columns=[col for col in drop_cols if col in df.columns], errors='ignore')

    # Als JSON-Lines ohne null auf Stdout
    for rec in df.to_dict(orient='records'):
        # Entferne NaN-Werte aus dem Dictionary
        clean_rec = {}
        for k, v in rec.items():
            if isinstance(v, list):
                clean_rec[k] = v
            elif pd.notna(v):
                clean_rec[k] = v
        print(json.dumps(clean_rec, ensure_ascii=False))

for chunk in pd.read_json(args.jsonl_file, lines=True, compression='gzip', chunksize=chunksize):
    process_chunk(chunk)