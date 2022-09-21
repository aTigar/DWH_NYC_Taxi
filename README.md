# NYC Taxi

## Fehleranalyse

### Anmeldefehler mit `pyodbc`

1. [ODBC-Treiber 18](https://learn.microsoft.com/de-de/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16) installieren
2. HTWK-VPN aktivieren
3. `.env` anlegen, Muster aus `.env_sample` übernehmen und ausfüllen
4. IntelliJ IDEA schließen
5. Terminal öffnen:
    ```shell
    runas /netonly /user:htwk\username "C:\Program Files\JetBrains\IntelliJ IDEA <version>\bin\idea64.exe"
    ```
- Anschließend sollten keine Anmeldefehler entstehen über pyodbc

## Daten

### Struktur

Die Quelldaten als .parquet und .csv Dateien haben folgendes Dateischema:

```
data/
├─ taxi/
│  ├─ fhv_xx_yyyy.parquet
│  ├─ green_xx_yyyy.parquet
│  ├─ yellow_xx_yyyy.parquet
├─ covid/
│  ├─ x.csv
├─ weather/
│  ├─ x.csv
```
### Quellen

Die Daten können von folgenden Quellen bezogen werden:
- https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- https://www.weather.gov/okx/
- https://data.cityofnewyork.us/Health/COVID-19-Daily-Counts-of-Cases-Hospitalizations-an/rc75-m7u3

## Ausführen

Der ETL Prozess kann mittels Ausführung der etl.py gestartet werden.


