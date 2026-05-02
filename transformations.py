from __future__ import annotations

import json
from typing import Any, Dict, List, Optional


def observation_codes(observation: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    code = observation.get("code") or {}
    if isinstance(code, dict):
        for coding in code.get("coding", []) or []:
            if isinstance(coding, dict):
                code_value = coding.get("code")
                if isinstance(code_value, str) and code_value.strip():
                    out.append(code_value.strip())
    for component in observation.get("component", []) or []:
        if not isinstance(component, dict):
            continue
        comp_code = component.get("code") or {}
        if not isinstance(comp_code, dict):
            continue
        for coding in comp_code.get("coding", []) or []:
            if not isinstance(coding, dict):
                continue
            code_value = coding.get("code")
            if isinstance(code_value, str) and code_value.strip() and code_value.strip() not in out:
                out.append(code_value.strip())
    return out


def _has_component_code(observation: Dict[str, Any], target_code: str) -> bool:
    for component in observation.get("component", []) or []:
        if not isinstance(component, dict):
            continue
        comp_code = component.get("code") or {}
        if not isinstance(comp_code, dict):
            continue
        for coding in comp_code.get("coding", []) or []:
            if isinstance(coding, dict) and str(coding.get("code", "")).strip() == target_code:
                return True
    return False


def _is_blood_pressure_panel(observation: Dict[str, Any]) -> bool:
    return _has_component_code(observation, "8480-6") and _has_component_code(observation, "8462-4")


def observation_transfer_code(observation: Dict[str, Any]) -> Optional[str]:
    codes = observation_codes(observation)
    return codes[0] if codes else None


def extract_effective_datetime(observation: Dict[str, Any]) -> Optional[str]:
    candidates: List[Any] = [
        observation.get("effectiveDateTime"),
        (observation.get("effective") or {}).get("dateTime"),
    ]
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _shallow_clone_observation(source: Dict[str, Any]) -> Dict[str, Any]:
    return json.loads(json.dumps(source))


def upsert_identifier(observation: Dict[str, Any], system: str, value: str) -> None:
    identifiers = observation.get("identifier")
    if not isinstance(identifiers, list):
        identifiers = []
        observation["identifier"] = identifiers
    for identifier in identifiers:
        if isinstance(identifier, dict) and identifier.get("system") == system and identifier.get("value") == value:
            return
    identifiers.append({"system": system, "value": value})


def expand_observation_for_transfer(observation: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not _is_blood_pressure_panel(observation):
        return [observation]

    base_id = str(observation.get("id", "")).strip()
    effective = extract_effective_datetime(observation)
    expanded: List[Dict[str, Any]] = []
    for component in observation.get("component", []) or []:
        if not isinstance(component, dict):
            continue
        code_node = component.get("code") or {}
        if not isinstance(code_node, dict):
            continue
        code_value: Optional[str] = None
        display_value: Optional[str] = None
        for coding in code_node.get("coding", []) or []:
            if not isinstance(coding, dict):
                continue
            candidate = coding.get("code")
            if isinstance(candidate, str) and candidate.strip() in ("8480-6", "8462-4"):
                code_value = candidate.strip()
                display_value = str(coding.get("display") or "").strip() or None
                break
        if not code_value:
            continue
        value_quantity = component.get("valueQuantity")
        if not isinstance(value_quantity, dict) or value_quantity.get("value") is None:
            continue

        item = _shallow_clone_observation(observation)
        item["code"] = {
            "coding": [{"system": "http://loinc.org", "code": code_value, **({"display": display_value} if display_value else {})}],
            "text": display_value or code_value,
        }
        item["valueQuantity"] = dict(value_quantity)
        item.pop("component", None)
        if effective:
            item["effectiveDateTime"] = effective
        if base_id:
            item["id"] = f"{base_id}-{code_value}"
        expanded.append(item)
    return expanded or [observation]


def build_target_observation(source_observation: Dict[str, Any], target_patient_id: str, source_identifier_system: str, settings: Any) -> Dict[str, Any]:
    transformed = _shallow_clone_observation(source_observation)
    source_id = str(transformed.get("id", "")).strip()
    effective_datetime = extract_effective_datetime(transformed)
    transformed.pop("id", None)

    meta = transformed.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    profiles = [p for p in (meta.get("profile") or []) if isinstance(p, str) and p.strip()]
    if "http://hl7.org/fhir/StructureDefinition/vitalsigns" not in profiles:
        profiles.append("http://hl7.org/fhir/StructureDefinition/vitalsigns")
    meta["profile"] = profiles
    transformed["meta"] = meta

    transformed["subject"] = {"reference": f"Patient/{target_patient_id}"}
    transformed.pop("effective", None)
    if effective_datetime:
        transformed["effectiveDateTime"] = effective_datetime

    transformed["category"] = [{"coding": [dict(category)]} for category in settings.observation_category_codings]
    transformed["status"] = "final"

    # Convert Actimi "value" object into FHIR value[x] when needed.
    if "value" in transformed and not transformed.get("valueQuantity"):
        value_node = transformed.get("value")
        if isinstance(value_node, dict):
            quantity = value_node.get("Quantity") or value_node.get("quantity")
            if isinstance(quantity, dict):
                value_quantity = {
                    "value": quantity.get("value"),
                    "unit": quantity.get("unit") or quantity.get("code"),
                }
                if value_quantity["unit"]:
                    value_quantity["system"] = "http://unitsofmeasure.org"
                    if value_quantity["unit"] == "mmHg":
                        value_quantity["code"] = "mm[Hg]"
                    elif isinstance(value_quantity["unit"], str) and value_quantity["unit"].lower() in ("bpm", "/min", "per min"):
                        value_quantity["code"] = "/min"
                    elif value_quantity["unit"] in ("°C", "Cel", "C"):
                        value_quantity["code"] = "Cel"
                    else:
                        value_quantity["code"] = value_quantity["unit"]
                transformed["valueQuantity"] = value_quantity
        elif isinstance(value_node, (str, int, float)):
            transformed["valueString"] = str(value_node)
        transformed.pop("value", None)

    value_quantity = transformed.get("valueQuantity")
    if isinstance(value_quantity, dict):
        if value_quantity.get("unit") and not value_quantity.get("system"):
            value_quantity["system"] = "http://unitsofmeasure.org"
        if value_quantity.get("unit") and not value_quantity.get("code"):
            unit = value_quantity.get("unit")
            if unit == "mmHg":
                value_quantity["code"] = "mm[Hg]"
            elif isinstance(unit, str) and unit.lower() in ("bpm", "/min", "per min"):
                value_quantity["code"] = "/min"
            else:
                value_quantity["code"] = unit

    if source_id:
        upsert_identifier(transformed, source_identifier_system, source_id)
    return transformed


def extract_observation_value(observation: Dict[str, Any]) -> str:
    if "valueQuantity" in observation:
        qty = observation["valueQuantity"]
        return f"{qty.get('value')} {qty.get('unit', '')}".strip()
    if "valueString" in observation:
        return str(observation["valueString"])
    return ""


def has_observation_value(observation: Dict[str, Any]) -> bool:
    value_quantity = observation.get("valueQuantity")
    if isinstance(value_quantity, dict) and value_quantity.get("value") is not None:
        return True
    value_string = observation.get("valueString")
    if isinstance(value_string, str) and value_string.strip():
        return True
    value_codeable = observation.get("valueCodeableConcept")
    if isinstance(value_codeable, dict) and value_codeable:
        return True
    value_node = observation.get("value")
    if isinstance(value_node, dict):
        quantity = value_node.get("Quantity") or value_node.get("quantity")
        if isinstance(quantity, dict) and quantity.get("value") is not None:
            return True
    if isinstance(value_node, (str, int, float)):
        return True
    return False
