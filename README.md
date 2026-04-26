# put verknüpft alphanum code mit givenname: put_given_code.py funktioniert
# get all observation: import_requests.py !!funktioniert 

# actimi_to_sensdoc.py funktioniert macht alles

# get all pat ressource: request_Pat.py holt alle Patientenresourcen aus Actimi
#verknüpfe given name mit observation m zeitstempel: 
# Patient_main_obs holt alle Observations: Output lesbar in Terminal
#post_obs funktioniert nicht

#run_pipeline funktioniert nicht
#muss noch eingepflegt werden, so ähnlich
sync:
  schedule: false
  poll_interval_hours: 1
  window_hours: 1
  filter: "vital"
  debug: false
  help_note: "Use --help for CLI usage or --debug to print resolved config values."
  preview: false

 @baseUrl = https://partner-api-ovok.logixsy.com/v2/partner
@token = "O2SUM53XRT24E6XD1LWZDEHT"
@after = 2025-03-18T14:30:00Z

 actime_to_sensdoc verwendet Requests für HTTP-API-Calls, YAML für Konfiguration und Multithreading für Performance.

1. Konfiguration und Setup-Funktionen
Diese laden und verarbeiten die YAML-Konfiguration und Umgebungsvariablen.

a. parse_args(): Parst Kommandozeilenargumente (z.B. --dry-run, --config).

b. Schritte: Erstellt einen ArgumentParser, definiert Optionen, parsed die Eingabe.
load_config(path: Path): Lädt YAML-Konfigurationsdatei.

c. Schritte: Öffnet die Datei, parsed YAML mit yaml.safe_load().
load_env_file(path: Path): Lädt .env-Datei für Umgebungsvariablen.

d. Schritte: Liest Zeilen, parsed KEY=VALUE, setzt os.environ falls nicht vorhanden.
env_required(name: str): Holt erforderliche Umgebungsvariable.

e. Schritte: Ruft os.getenv(), wirft Fehler falls fehlend.
read_server(cfg, block_name): Extrahiert Server-Block aus Config.

f. Schritte: Holt Dict aus cfg, prüft Typ.
build_auth() / build_optional_auth(): Baut HTTP-Basic-Auth aus Config.

Schritte: Holt User/Pass aus Env, erstellt HTTPBasicAuth-Objekt.
request_actimi_access_token(): Holt Access-Token von Actimi-API.

Schritte: POST-Request mit API-Key, parsed Response für Token.
build_settings(args, cfg): Baut Settings-Objekt aus Args und Config.

Schritte: Parsed alle Config-Blöcke, setzt Defaults, baut Auth, lädt Payloads.
2. Hilfsfunktionen für Datenverarbeitung
Diese verarbeiten FHIR-Ressourcen und Namen.

parse_dt(value): Parsed ISO-Datetime-String zu datetime.

Schritte: Strip, handle 'Z', fromisoformat(), setze UTC.
http_get_json(): GET-Request mit Retry-Logik.

Schritte: Retry bei Timeouts/5xx, exponential Backoff, parsed JSON.
iter_resources(payload): Iteriert über FHIR Bundle-Einträge.

Schritte: Handle Listen oder Bundles, yield Ressourcen.
first_given(), second_given(), all_given_names(): Extrahieren Given-Names aus Patient.

Schritte: Durchsuche name[], filter Given-Names.
normalize_name(): Normalisiert Namen (Lowercase, Strip).

Schritte: Casefold, Split/Join.
patient_name_parts(): Holt alle Name-Teile eines Patienten.

Schritte: Sammle Family/Given aus name[].
observation_codes(): Extrahiert LOINC-Codes aus Observation.

Schritte: Durchsuche code.coding[], sammle Codes.
extract_effective_datetime(): Holt effektives Datum aus Observation.

Schritte: Prüfe effectiveDateTime, effective.dateTime, etc.
observation_in_window(): Prüft, ob Observation im Zeitfenster liegt.

Schritte: Parsed Datum, vergleiche mit from_time.
3. API-Fetch-Funktionen
Diese holen Daten von den APIs.

fetch_patients(): Holt alle Patienten von einem Server.

Schritte: GET mit Pagination, sammle alle Patienten.
fetch_patient_map_by_given(): Baut Map von Given-Name zu Patient.

Schritte: Ruft fetch_patients(), mappe Given-Names.
find_sensdoc_patient_by_rule() / find_sensdoc_patient_by_given_alias(): Findet Patienten per Regel/Alias.

Schritte: Filter Patienten nach Namen/Geburtsdatum/ID.
fetch_all_actimi_observations(): Holt alle Observations von Actimi.

Schritte: GET alle Observations, gruppiere per Given-Name.
4. Transformations- und Sync-Funktionen
Diese transformieren und syncen Daten.

build_target_observation(): Transformiert Actimi-Observation für Sensdoc.

Schritte: Clone, setze neue Subject/Effective, füge Identifier.
post_or_put_observation(): POST/PUT Observation mit Conditional Create.

Schritte: Baue Headers, POST, handle 200/201.
ensure_patient_given_alias(): Fügt Alias zu Patient hinzu.

Schritte: Prüfe ob vorhanden, update Name[], PUT Patient.
search_encounter_by_start_datetime(): Sucht Encounter per Datum.

Schritte: GET mit Query, parsed Ergebnis.
create_encounter_for_patient(): Erstellt neuen Encounter.

Schritte: Baue JSON, POST, return ID.
ensure_patient_encounter_reference(): Stellt Encounter sicher.

Schritte: Suche/Create, return Ref und Flag.
create_communication(): Erstellt Communication (Akteneintrag).

Schritte: Baue JSON mit Payloads aus Codes, POST.
5. Haupt-Sync-Logik
_sync_patient(): Sync einen Patienten.

Schritte: Filter Observations, gruppiere per Zeit, erstelle Observations/Communications.
sync(): Haupt-Sync-Funktion.

Schritte: Lade Patienten/Observations, parallel sync per Patient.
main(): Einstiegspunkt.

Schritte: Parse Args, lade Config, baue Settings, rufe sync().
print_debug(): Debug-Ausgabe der Settings.

Zusammenfassung des Ablaufs:
Setup: Lade Config, baue Settings.
Fetch: Hole Patienten/Observations von APIs.
Sync: Für jeden Patienten: Filter/Gruppiere Observations, erstelle in Sensdoc, generiere Communications.
Output: Statistiken und Debug-Info.