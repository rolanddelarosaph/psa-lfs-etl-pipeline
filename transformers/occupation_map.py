# =============================================================================
# transformers/occupation_map.py
# PSA PSOC (Philippine Standard Occupation Classification) mapping.
# Identical across ALL years and quarters — single source of truth.
# =============================================================================

# Raw PSA 2-digit PSOC codes → 1-digit major group code
OCCUPATION_MAP = {
    # 1 — Managers
    "11": "1", "12": "1", "13": "1", "14": "1",

    # 2 — Professionals
    "21": "2", "22": "2", "23": "2", "24": "2", "25": "2", "26": "2",

    # 3 — Technicians and Associate Professionals
    "31": "3", "32": "3", "33": "3", "34": "3", "35": "3",

    # 4 — Clerical Support Workers
    "41": "4", "42": "4", "43": "4", "44": "4",

    # 5 — Service and Sales Workers
    "51": "5", "52": "5", "53": "5", "54": "5",

    # 6 — Skilled Agricultural, Forestry and Fishery Workers
    "61": "6", "62": "6", "63": "6",

    # 7 — Craft and Related Trades Workers
    "71": "7", "72": "7", "73": "7", "74": "7", "75": "7",

    # 8 — Plant and Machine Operators and Assemblers
    "81": "8", "82": "8", "83": "8",

    # 9 — Elementary Occupations
    "91": "9", "92": "9", "93": "9", "94": "9", "95": "9", "96": "9",
}

# Human-readable labels for the 1-digit codes (used in final legend step)
OCCUPATION_LABELS = {
    1: "Managers",
    2: "Professionals",
    3: "Technicians and Associate Professionals",
    4: "Clerical Support Workers",
    5: "Service and Sales Workers",
    6: "Skilled Agricultural, Forestry and Fishery Workers",
    7: "Craft and Related Trades Workers",
    8: "Plant and Machine Operators and Assemblers",
    9: "Elementary Occupations",
}


def apply_occupation_map(series):
    """
    Map raw 2-digit PSOC codes to 1-digit major group codes.
    Codes not found in the map are set to 'Unknown'.

    Args:
        series: pandas Series of raw occupation codes

    Returns:
        pandas Series of 1-digit major group codes (as strings) or 'Unknown'
    """
    return series.astype(str).map(OCCUPATION_MAP).fillna("Unknown")
