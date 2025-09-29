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

Die in [Taskfile.yml](Taskfile.yml) definierten Tasks können auch einzeln ausgeführt werden. In der Regel in dieser Reihenfolge:

* `datendienst`: Download von GND-IDs aus dem DLA Datendienst
* `lobid-download`: Bulk-Download der GND als JSON-Lines über lobid-gnd
* `lobid-filter`: GND-Download reduzieren auf im DLA verwendete GND-IDs
* `lobid-transform`: Gewünschte Daten aus lobid-filtered in einzelne Tabellen schreiben
* `wikidata-extract`: Mit den aus lobid-gnd ermittelten Wikidata IDs den Wikidata Query Service abfragen
* `wikidata-transform`: Gewünschte Daten aus wikidata-extracted in einzelne Tabellen schreiben
* `output`: Plausibilitätsprüfung und ggf. Kopie der Daten in das Verzeichnis output

Der GitHub Actions Workflow [default.yml](.github/workflows/default.yml) führt den Gesamtprozess aus und lädt anschließend die neu generierten Daten mit einem Commit ins GitHub Repository.

## Daten

Zwischenergebnisse werden im Ordner `data` abgelegt. Das Gesamtergebnis wird schließlich im Ordner [output](output) bereitgestellt. Für jedes ausgewählte Feld aus der GND oder Wikidata wird eine TSV-Datei bereitgestellt.

Die TSV-Dateien haben immer zwei Spalten:
1. Spalte: GND-ID oder Wikidata-ID
2. Spalte: Wert aus dem ausgewählten Feld

Jeder Wert kommt in eine eigene Zeile, ggf. wird die GND-ID bzw. Wikidata-ID wiederholt.

Beispiel aus [output/lobid-wikidata.tsv](output/lobid-wikidata.tsv):
```
116891998	Q95880566
116892927	Q20733597
116892927	Q94822365
```

Diese Daten werden im ETL-Workflow des Online-Katalogs geladen und zur Anreicherung verwendet:
* Import Enrichment Cache: [dla-opac-transform: main.sh](https://github.com/dla-marbach/dla-opac-transform/blob/main/scripts/main.sh)
* Beispiel für Anreicherung: [dla-opac-transform: wikidata_pe.yaml](https://github.com/dla-marbach/dla-opac-transform/blob/main/config/main/02/wikidata_pe.yaml)
