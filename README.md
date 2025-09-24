# dla-opac-gnd-enrichment
Generierung eines Enrichment Cache mit Daten aus lobid-gnd und Wikidata für den Katalog des Deutschen Literaturarchivs Marbach auf Basis von im DLA verwendeten GND-IDs.

## Voraussetzungen

* [go-task](https://taskfile.dev)
* [curl](https://curl.se)
* Python 3

## Nutzung

```sh
task
```

## GND-ID suchen

```sh
zgrep 118540238 data/filtered-person.jsonl.gz | jq
```