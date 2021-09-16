#!/usr/bin/env python3

import os
import csv
import json

DATA_DIR = "missed_atts"
BLOCK_DIR = "blocks"
MISSED_ATTS_CSV = "missed_atts.csv"
MISSED_SUBNETS_CSV = "missed_subnets.csv"

FIELD_NAMES = ["validator_index", "missed_attestations"]

def get_slot(filename):
    return int(filename.split(".")[0].split("_")[1])

def main():
    filenames = sorted(file for _, _, files in os.walk(DATA_DIR) for file in files)

    # Map from validator index to number of attestations missed.
    missed_validators = {}

    # Map from subnet to number of missed attestations on that subnet.
    missed_subnets = {}

    # Map from slot mod 32 to number of missed attestations at that slot.
    missed_by_slot = { i: 0 for i in range(0, 32) }

    for filename in filenames:
        with open(os.path.join(DATA_DIR, filename), "r") as f:
            json_data = json.load(f)

            slot_mod = get_slot(filename) % 32

            validators_missed = len(json_data["all"])

            missed_by_slot[slot_mod] += validators_missed

            for validator in json_data["all"]:
                if validator in missed_validators:
                    missed_validators[validator] += 1
                else:
                    missed_validators[validator] = 1

            for att in json_data["per_attestation"]:
                subnet = att["subnet"]
                if subnet in missed_subnets:
                    missed_subnets[subnet] += 1
                else:
                    missed_subnets[subnet] = 1


    with open(MISSED_ATTS_CSV, "w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELD_NAMES)
        writer.writeheader()

        for (validator_index, count) in missed_validators.items():
            writer.writerow({ "validator_index": validator_index, "missed_attestations": count })

    with open(MISSED_SUBNETS_CSV, "w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["subnet", "missed_attestations"])
        writer.writeheader()

        for (subnet, count) in missed_subnets.items():
            writer.writerow({ "subnet": subnet, "missed_attestations": count })

    with open("missed_by_slot.csv", "w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["slot_mod", "missed_attestations"])
        writer.writeheader()

        for (slot_mod, count) in missed_by_slot.items():
            writer.writerow({ "slot_mod": slot_mod, "missed_attestations": count })

if __name__ == "__main__":
    main()
