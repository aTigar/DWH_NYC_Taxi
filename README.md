# NYC Taxi

## Fehleranalyse

### Anmeldefehler mit `pyodbc`

1. HTWK-VPN aktivieren
2. `.env` anlegen und Muster aus `.env_sample` übernehmen und ausfüllen
3. Intellij Idea schließen
4. Terminal öffnen:
    ```shell
    runas /netonly /user:htwk\username "C:\Program Files\JetBrains\IntelliJ IDEA 2022.1.1\bin\idea64.exe"
    ```
- Anschließend sollten keine Anmeldefehler entstehen über pyodbc
