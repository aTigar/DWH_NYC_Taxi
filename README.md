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
