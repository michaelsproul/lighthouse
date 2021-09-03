#!/usr/bin/env python3

import os
import csv
import json

DATA_DIR = "block_stats"
BLOCK_DIR = "blocks"
OUTPUT_FILE = "summary.csv"

FIELD_NAMES = ["slot", "graffiti", "num_attestations", "useless_attestations", "validators_covered",
               "block_reward", "parent_slot", "num_salvaged", "salvaged_votes", "salvaged_rewards"]

def get_slot(filename):
    return int(filename.split(".")[0].split("_")[1])

def main():
    filenames = sorted(file for _, _, files in os.walk(DATA_DIR) for file in files)

    csv_file = open(OUTPUT_FILE, "w", newline="")
    writer = csv.DictWriter(csv_file, fieldnames=FIELD_NAMES)
    writer.writeheader()

    parent_slot = get_slot(filenames[0]) - 1

    for filename in filenames:
        with open(os.path.join(DATA_DIR, filename), "r") as f:
            json_data = json.load(f)
            per_attestation_rewards = json_data["per_attestation_rewards"]

            useless_attestations = sum(1 for reward_map in json_data["per_attestation_rewards"]
                                       if len(reward_map) == 0)

            validators_covered = len(json_data["prev_epoch_rewards"]) + len(json_data["curr_epoch_rewards"])

            # Compute number of useful attestations from past slots, i.e. number of profitable
            # attestations which previous proposers missed.
            with open(os.path.join(BLOCK_DIR, filename), "r") as block_file:
                block = json.load(block_file)["data"]

            attestations = block["message"]["body"]["attestations"]
            salvaged_attestations = []
            salvaged_votes = 0
            salvaged_rewards = 0
            for (i, (att, rewards)) in enumerate(zip(attestations, per_attestation_rewards)):
                att_slot = int(att["data"]["slot"])

                if len(rewards) > 0 and att_slot < parent_slot:
                    salvaged_attestations.append(i)
                    salvaged_votes += len(rewards)
                    salvaged_rewards += sum(rewards.values())

            slot = get_slot(filename)
            row = {
                "slot": slot,
                "graffiti": json_data["graffiti"],
                "num_attestations": len(json_data["per_attestation_rewards"]),
                "useless_attestations": useless_attestations,
                "validators_covered": validators_covered,
                "block_reward": json_data["total"],
                "parent_slot": parent_slot,
                "num_salvaged": len(salvaged_attestations),
                "salvaged_votes": salvaged_votes,
                "salvaged_rewards": salvaged_rewards
                # "salvaged_attestations": "|".join(map(str, salvaged_attestations)),
            }
            parent_slot = slot

            writer.writerow(row)

    csv_file.close()

if __name__ == "__main__":
    main()
