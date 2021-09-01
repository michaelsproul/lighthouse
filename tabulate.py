#!/usr/bin/env python3

import os
import csv
import json

DATA_DIR = "block_stats"
OUTPUT_FILE = "summary.csv"

FIELD_NAMES = ["slot", "graffiti", "num_attestations", "useless_attestations", "validators_covered", "block_reward"]

def get_slot(filename):
    return int(filename.split(".")[0].split("_")[1])

def main():
    filenames = sorted(file for _, _, files in os.walk(DATA_DIR) for file in files)

    csv_file = open(OUTPUT_FILE, "w", newline="")
    writer = csv.DictWriter(csv_file, fieldnames=FIELD_NAMES)
    writer.writeheader()

    for filename in filenames:
        with open(os.path.join(DATA_DIR, filename), "r") as f:
            json_data = json.load(f)

            useless_attestations = sum(1 for reward_map in json_data["per_attestation_rewards"]
                                       if len(reward_map) == 0)

            validators_covered = len(json_data["prev_epoch_rewards"]) + len(json_data["curr_epoch_rewards"])

            row = {
                "slot": get_slot(filename),
                "graffiti": json_data["graffiti"],
                "num_attestations": len(json_data["per_attestation_rewards"]),
                "useless_attestations": useless_attestations,
                "validators_covered": validators_covered,
                "block_reward": json_data["total"]
            }

            writer.writerow(row)

    csv_file.close()

if __name__ == "__main__":
    main()
