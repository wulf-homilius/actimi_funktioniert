import os
import requests
import yaml


def load_config(config_path: str = "config/del_given.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_auth_from_config(config: dict) -> requests.auth.HTTPBasicAuth:
    user_env = config["sensdoc"]["auth"]["user_env"]
    pass_env = config["sensdoc"]["auth"]["pass_env"]
    user = os.getenv(user_env)
    password = os.getenv(pass_env)
    if not user or not password:
        raise ValueError(f"Umgebungsvariablen {user_env} oder {pass_env} fehlen!")
    return requests.auth.HTTPBasicAuth(user, password)


def main() -> None:
    config = load_config()
    auth = get_auth_from_config(config)

    base_url = config["sensdoc"]["database"]["url"].rstrip("/")
    patient_id = config["filter"]["patient_id"]
    expected_value = config.get("fixed_code") or config["filter"].get("given")
    target_index = 2  # 3. Eintrag (0-basiert)

    patient_url = f"{base_url}/Patient/{patient_id}"

    response = requests.get(patient_url, auth=auth, headers={"Accept": "application/fhir+json"})
    print(f"GET {patient_url}: Status {response.status_code}")
    if response.status_code != 200:
        raise RuntimeError(f"Patient konnte nicht geladen werden: {response.text}")

    patient = response.json()

    if not patient.get("name") or not isinstance(patient["name"], list):
        raise RuntimeError("Patient hat kein 'name'-Array.")

    given = patient["name"][0].get("given")
    if not isinstance(given, list):
        raise RuntimeError("Patient.name[0].given ist kein Array.")

    if len(given) <= target_index:
        raise RuntimeError(f"given hat nur {len(given)} Eintraege; Index 2 existiert nicht.")

    current_value = given[target_index]
    if expected_value and current_value != expected_value:
        raise RuntimeError(
            f"Sicherheitscheck fehlgeschlagen: given[2] ist '{current_value}', erwartet '{expected_value}'."
        )

    removed = given.pop(target_index)
    patient["name"][0]["given"] = given

    if config.get("dry_run", True):
        print(f"Dry run: Wuerde given[2] ('{removed}') aus Patient/{patient_id} entfernen.")
        print(f"Neues given-Array: {given}")
        return

    put_response = requests.put(
        patient_url,
        auth=auth,
        headers={"Content-Type": "application/fhir+json", "Accept": "application/fhir+json"},
        json=patient,
    )
    print(f"PUT {patient_url}: Status {put_response.status_code}")
    if put_response.status_code not in (200, 201):
        raise RuntimeError(f"Update fehlgeschlagen: {put_response.text}")

    print(f"Erfolgreich entfernt: given[2] = '{removed}'")
    print(f"Aktuelles given-Array: {given}")


if __name__ == "__main__":
    main()
