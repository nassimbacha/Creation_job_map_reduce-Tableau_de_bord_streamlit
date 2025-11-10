#!/usr/bin/env python3
import sys

current_key = None
total = 0.0

for line in sys.stdin:
    line = line.strip()
    try:
        key, value = line.split('\t', 1)
        value = float(value)
    except (ValueError, IndexError):
        continue

    if current_key == key:
        total += value
    else:
        if current_key:
            try:
                pays, annee_mois = current_key.split('_', 1)
                print(f"{annee_mois} {pays}\t{total:.2f}")
            except ValueError:
                print(f"{current_key}\t{total:.2f}")
        current_key = key
        total = value

if current_key:
    try:
        pays, annee_mois = current_key.split('_', 1)
        print(f"{annee_mois} {pays}\t{total:.2f}")
    except ValueError:
        print(f"{current_key}\t{total:.2f}")
