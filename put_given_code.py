import json
import yaml
import requests
from dotenv import load_dotenv
import os
import sys

# 1. Lade .env-Datei
load_dotenv()

# 2. Lade Konfiguration
def load_config(config_path: str = "config/config_new.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# 3. Lade Datenbank-Zugangsdaten
def get_auth_from_config(config: dict) -> tuple:
    user_env = config["sensdoc"]["auth"]["user_env"]
    pass_env = config["sensdoc"]["auth"]["pass_env"]
    user = os.getenv(user_env)
    password = os.getenv(pass_env)
    if not user or not password:
        raise ValueError(f"Umgebungsvariablen {user_env} oder {pass_env} fehlen!")
    return user, password

# 4. Prüfe, ob Patient die Filterkriterien erfüllt
def matches_filter(patient: dict, config: dict) -> bool:
    filter_config = config.get("filter", {})
    if not filter_config:
        return True

    if "name" not in patient:
        return False

    # Prüfe birthDate auf Patient-Ebene
    patient_birthdate = patient.get("birthDate")
    filter_birthdate = filter_config.get("birthDate")
    if filter_birthdate and patient_birthdate != filter_birthdate:
        return False

    for name in patient["name"]:
        family = name.get("family")
        filter_family = filter_config.get("family")
        if filter_family and family != filter_family:
            continue  # Prüfe nächsten Namen

        # Wenn family passt (oder kein Filter), und birthDate passt, dann ok
        return True

    return False

# 5. Füge Code zu Patienten hinzu
def add_fixed_code_to_patient(patient: dict, config: dict) -> dict:
    # Lese Code aus Config - versuche beide Strukturen
    code = config.get("fixed_code")
    if not code:
        # Fallback auf sensdoc.code (alte Struktur)
        code = config.get("sensdoc", {}).get("code")
    if not code:
        raise ValueError("Code nicht in Konfiguration gefunden! Bitte 'fixed_code' oder 'sensdoc.code' hinzufügen.")

    for name in patient["name"]:
        if "given" not in name:
            name["given"] = []
        given_list = name["given"]

        if code not in given_list:
            given_list.append(code)
            # Use .get() for optional fields to avoid KeyError
            given_name = name.get('given', [None])[0] if name.get('given') else 'N/A'
            family_name = name.get('family', 'N/A')
            birthdate = name.get('birthDate', 'N/A')
            print(f"✅ Code '{code}' hinzugefügt zu: {given_name} {family_name} ({birthdate})")
        else:
            print(f"ℹ️ Code '{code}' bereits vorhanden.")

    return patient

# 6. Hole Patienten aus Datenbank basierend auf Filter (family und birthDate)
def fetch_patients_from_db(config: dict) -> list:
    filter_config = config.get("filter", {})
    family = filter_config.get("family")
    birthdate = filter_config.get("birthDate")
    
    if not family or not birthdate:
        raise ValueError("Filter muss 'family' und 'birthDate' enthalten!")
    
    url = f"{config['sensdoc']['database']['url']}/Patient?family={family}&birthdate={birthdate}"
    user, password = get_auth_from_config(config)
    
    try:
        response = requests.get(url, auth=(user, password))
        if response.status_code == 200:
            bundle = response.json()
            patients = []
            if "entry" in bundle:
                for entry in bundle["entry"]:
                    if "resource" in entry:
                        patients.append(entry["resource"])
            return patients
        else:
            print(f"❌ Fehler beim Abrufen: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        print("Fehler:", e)
        return []

# 7. Speichere Patienten in Datenbank
def save_patient_to_db(patient: dict, config: dict) -> bool:
    url = f"{config['sensdoc']['database']['url']}/Patient/{patient['id']}"
    user, password = get_auth_from_config(config)

    try:
        response = requests.put(url, json=patient, auth=(user, password))
        if response.status_code in [200, 201]:
            print(f"✅ Patient {patient['id']} erfolgreich aktualisiert")
            return True
        else:
            print(f"❌ Fehler beim Speichern: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print("Fehler:", e)
        return False

# 8. Hauptprogramm
if __name__ == "__main__":
    print("🔄 Lade Konfiguration...")
    
    # Konfigurationsdatei als Argument übergeben oder default nutzen
    config_file = "config/config_new.yaml"
    if len(sys.argv) > 1:
        if sys.argv[1].endswith(".yaml") or sys.argv[1].endswith(".yml"):
            config_file = sys.argv[1]
    
    config = load_config(config_file)
    
    dry_run = config.get("dry_run", False)
    if dry_run:
        print("🔄 Dry-Run Modus aktiviert - keine Änderungen werden gespeichert.")
    
    print("🔄 Suche Patienten in Datenbank basierend auf Filter...")
    patients = fetch_patients_from_db(config)
    
    if not patients:
        print("❌ Keine Patienten gefunden, die den Filterkriterien entsprechen.")
        exit(1)
    
    print(f"🔄 {len(patients)} Patient(en) gefunden. Bearbeite...")
    
    for patient in patients:
        if matches_filter(patient, config):
            updated_patient = add_fixed_code_to_patient(patient, config)
            if not dry_run:
                print("🔄 Speichere Patienten in Datenbank...")
                save_patient_to_db(updated_patient, config)
            else:
                print("🔄 Dry-Run: Patient würde gespeichert werden.")
        else:
            print(f"ℹ️ Patient {patient.get('id', 'unbekannt')} passt nicht zum Filter.")