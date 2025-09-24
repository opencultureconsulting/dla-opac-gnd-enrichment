# dla-opac-gnd-enrichment

Generierung eines Enrichment Cache mit Daten aus lobid-gnd und Wikidata für den Katalog des Deutschen Literaturarchivs Marbach auf Basis von im DLA verwendeten GND-IDs.

## Voraussetzungen

* [go-task](https://taskfile.dev)
* [curl](https://curl.se)
* [jq](https://jqlang.org/)
* Python 3

## Nutzung

Gesamten Prozess starten:

```sh
task
```

Verfügbare Einzeltasks:

```sh
$ task --list
task: Available tasks for this project:
* datendienst:          Download von GND-IDs aus dem DLA Datendienst
* lobid-download:       Bulk-Download der GND als JSON-Lines über lobid-gnd
* lobid-extract:        Gewünschte Daten in einzelne Tabellen extrahieren
* lobid-filter:         GND-Download reduzieren auf im DLA verwendete GND-IDs
* output:               Plausibilitätsprüfung und ggf. Kopie der Daten in das Verzeichnis output
* wikidata:             (TODO) Mit den aus lobid-extract ermittelten Wikidata IDs den Wikidata Query Service abfragen
```

## Daten

Das Ergebnis wird im Ordner [output](output) bereitgestellt. Für jedes ausgewählte Feld aus der GND oder Wikidata wird eine TSV-Datei pro Teilbestand des DLA (ak: Werktitel, ks: Körperschaften, pe: Personen) bereitgestellt.

Die TSV-Dateien haben immer zwei Spalten:
1. Spalte: GND-ID bzw. Wikidata-ID
2. Spalte: Wert aus dem ausgewählten Feld

Jeder Wert kommt in eine eigene Zeile, ggf. wird die GND-ID bzw. Wikidata-ID wiederholt.

Beispiel aus pe-gnd-wikidata.tsv:
```
116891998	Q95880566
116892927	Q20733597
116892927	Q94822365
```
