#!/usr/bin/env python3
import argparse
import gzip
import json
import os
import sys

parser = argparse.ArgumentParser(description='Filter GZIP-komprimierte JSONL-Datei mit einer Liste von GND-IDs')
parser.add_argument('jsonl_gz_file', help='Pfad zur GZIP-komprimierten JSONL-Datei')
parser.add_argument('id_file', help='Pfad zur TXT-Datei mit GND-IDs')
args = parser.parse_args()

if not os.path.exists(args.jsonl_gz_file) or not os.path.exists(args.id_file):
    print('Eine der Eingabedateien wurde nicht gefunden.', file=sys.stderr)
    exit(1)

# Nur bestimmte Felder übernehmnen 
fields = [
    'depiction',
    'gndIdentifier',
    'preferredName',
    'sameAs',
    'type',
    'variantName'
]

# GND-IDs aus der TXT-Datei einlesen
with open(args.id_file, 'r', encoding='utf-8') as f:
    mygnd = {line.strip() for line in f}

# JSONL-GZIP-Datei filtern    
with gzip.open(args.jsonl_gz_file, 'rt', encoding='utf-8') as infile:
    for line in infile:
        record = json.loads(line)
        if 'gndIdentifier' in record and record['gndIdentifier'] in mygnd:
            filtered_record = {key: record[key] for key in fields if key in record}
            print(json.dumps(filtered_record, ensure_ascii=False))
