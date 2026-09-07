#!/usr/bin/env python
"""
sync_contract_to_dbt_vars.py — Read agent_contract.yaml and update dbt vars.

This ensures the dbt tests use the same thresholds as the streaming gatekeeper.
"""

import os
import yaml
import json

def sync_contract():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    contract_path = os.path.join(project_root, "configs", "agent_contract.yaml")
    dbt_project_path = os.path.join(project_root, "dbt_delta_guard", "dbt_project.yml")
    
    # Read contract
    with open(contract_path, "r") as f:
        contract = yaml.safe_load(f)
    
    # Extract thresholds from semantic rules
    vars_dict = {}
    for rule in contract.get("semantic_rules", []):
        if "cost_usd" in rule.get("rule", ""):
            # Extract max value from rule like "cost_usd >= 0.0 AND cost_usd <= 50.0"
            import re
            match = re.search(r'cost_usd\s*<=\s*([\d.]+)', rule["rule"])
            if match:
                vars_dict["max_cost_usd"] = float(match.group(1))
        elif "timestamp" in rule.get("rule", ""):
            # Extract freshness window
            if "INTERVAL 24 HOURS" in rule["rule"]:
                vars_dict["max_event_age_hours"] = 24
            if "INTERVAL 5 MINUTES" in rule["rule"]:
                vars_dict["max_future_skew_minutes"] = 5
    
    # Update dbt_project.yml
    with open(dbt_project_path, "r") as f:
        dbt_config = yaml.safe_load(f)
    
    if "vars" not in dbt_config:
        dbt_config["vars"] = {}
    
    dbt_config["vars"].update(vars_dict)
    
    with open(dbt_project_path, "w") as f:
        yaml.dump(dbt_config, f, sort_keys=False)
    
    print(f"[OK] Synced contract vars to dbt: {vars_dict}")
    return vars_dict

if __name__ == "__main__":
    sync_contract()