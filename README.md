# GTFS EXTRACT TOOL

## Cilji

- [x] --exclude-file stringArray     izloči eno ali več datotek iz originalnega feeda
- [x] --include-file stringArray     v končnem feedu bodo samo te datoteke
- [x] --exclude-field stringArray    izloči podane polja v datoteki; format: file name, field names…
- [x] --include-filed stringArray    v output feedu bodo v podani datoteki samo podana polja; format: file name, field names…
- [x] --exclude-empty-files          izloči prazne datoteke iz feeda
- [ ] --exclude-empty-fields         izloči prazna polja iz feeda
- [x] --exclude-shapes               izloči celoten shapes iz feeda

## Installation

Orodje lahko zgradite z uporabo ukaza `go build -o gtfs-tool .`, kjer je nastavljen trnutni imenik v korenu tega projekta.
Instalacija z `go install` še ni podprta.

## Primer uporabe

Pomoč z orodjem je na voljo z ukazom `gtfs-tool extract --help`

Primer uporabe:
```go
./gtfs-tool extract --exclude-files=stop_times.txt,rider_categories.txt --exclude-file agency.txt --include-fields routes.txt,route_id,route_long_name --verboseverbose --exclude-fields fare_media.txt,fare_media_name,fare_media_type --exclude-shapes feed.zip feed2.zip
```

Ta ukaz bo izločil datoteke `stop_times.txt`, `rider_categories.txt`, `agency.txt` in `shapes.txt` (z foreign key v `trips.txt`).
V datoteki `routes.txt` bo obdržal samo stolpca `route_id` in `route_long_name`.
Iz nove datoteke `fare_media.txt` bo izločil stolpca `fare_media_name` in `fare_media_type`.
Vse te operacije potekajo na vhodnem feedu `feed.zip` in so shranjene v `feed2.zip`.

Z uporabo zastavice `-v` ali `--verbose`, se izpiše katere datoteke se obdelujejo.
Uporaba zastavice `--verboseverbose` izpiše še več podatkov o izvajanju.