# =============================================================================
# transformers/grade_maps.py
# Two grade mapping versions extracted from PSA LFS codebooks.
#
# WHY TWO VERSIONS:
#   v1 — Used only in 2016 Q2. Pre-K12 education coding system.
#         Code "210" = Grade 1, "310" = Grade 7, etc.
#   v2 — Used from 2016 Q3 onwards. K12 system introduced.
#         Code "110" = Grade 1, "210" = Grade 7 (DIFFERENT meaning!),
#         "710" = 1st Year Bachelor, etc.
#
# DANGER: Never apply v1 to a v2 file or vice versa. The code "210" means
#         two completely different things in each version. The pipeline
#         selects the correct version via config/settings.py.
# =============================================================================


# ---------------------------------------------------------------------------
# GRADE MAP V1 — 2016 Q2 ONLY (Pre-K12 PSA coding)
# ---------------------------------------------------------------------------
GRADE_MAP_V1 = {
    "000": "No Grade Completed",
    "0":   "No Grade Completed",
    "1":   "No Grade Completed",
    "001": "No Grade Completed",
    "002": "No Grade Completed",
    "2":   "No Grade Completed",
    "10":  "No Grade Completed",
    "010": "No Grade Completed",

    # Elementary
    "210": "Grade 1",
    "220": "Grade 2",
    "230": "Grade 3",
    "240": "Grade 4",
    "250": "Grade 5",
    "260": "Elementary Graduate / Grade 6 Completer",
    "270": "Elementary Graduate / Grade 6 Completer",
    "280": "Elementary Graduate / Grade 6 Completer",

    # Junior High School
    "310": "Grade 7 / 1st Year Junior High",
    "320": "Grade 8 / 2nd Year Junior High",
    "330": "Grade 9 / 3rd Year Junior High",
    "340": "Junior High Graduate",
    "350": "Junior High Graduate",
}


def _map_grade_v1(code_str: str) -> str:
    """Apply grade map v1 logic (2016 Q2 only)."""
    if code_str in GRADE_MAP_V1:
        return GRADE_MAP_V1[code_str]
    if code_str.isdigit():
        code_int = int(code_str)
        if code_int == 410:
            return "Post-Secondary Non-Tertiary Undergraduate"
        elif code_int == 420 or (501 <= code_int <= 589):
            return "Post-Secondary Non-Tertiary Graduate"
        elif code_int == 810:
            return "1st Year Bachelor Level"
        elif code_int == 820:
            return "2nd Year Bachelor Level"
        elif code_int == 830:
            return "3rd Year Bachelor Level"
        elif code_int == 840:
            return "4th Year Bachelor Level"
        elif code_int == 900 or (601 <= code_int <= 689):
            return "College Graduate"
    return "Unknown"


# ---------------------------------------------------------------------------
# GRADE MAP V2 — 2016 Q3 onwards (K12 PSA coding)
# ---------------------------------------------------------------------------
GRADE_MAP_V2 = {
    "000": "No Grade Completed",
    "0":   "No Grade Completed",
    "1":   "No Grade Completed",
    "001": "No Grade Completed",
    "002": "No Grade Completed",
    "2":   "No Grade Completed",
    "10":  "No Grade Completed",
    "010": "No Grade Completed",

    # Elementary (1xx codes)
    "110": "Grade 1",
    "120": "Grade 2",
    "130": "Grade 3",
    "140": "Grade 4",
    "150": "Grade 5",
    "160": "Elementary Graduate / Grade 6 Completer",
    "170": "Elementary Graduate / Grade 6 Completer",
    "180": "Elementary Graduate / Grade 6 Completer",

    # Junior High School (2xx codes — NOTE: 210 no longer means Grade 1!)
    "210": "Grade 7 / 1st Year Junior High",
    "220": "Grade 8 / 2nd Year Junior High",
    "230": "Grade 9 / 3rd Year Junior High",
    "240": "Junior High Graduate",
    "250": "Junior High Graduate",

    # K-12 batch (alternative codes introduced with K12 rollout)
    "410": "Grade 1",
    "420": "Grade 2",
    "430": "Grade 3",
    "440": "Grade 4",
    "450": "Grade 5",
    "460": "Elementary Graduate / Grade 6 Completer",
    "470": "Grade 7 / 1st Year Junior High",
    "480": "Grade 8 / 2nd Year Junior High",
    "490": "Grade 9 / 3rd Year Junior High",
    "500": "Junior High Graduate",
    "510": "Grade 11 - Academic Track",
    "520": "Grade 12 Graduate - Academic Track",
}


def _map_grade_v2(code_str: str) -> str:
    """Apply grade map v2 logic (2016 Q3 onwards)."""
    if code_str in GRADE_MAP_V2:
        return GRADE_MAP_V2[code_str]
    if code_str.isdigit():
        code_int = int(code_str)
        if code_int == 310:
            return "Post-Secondary Non-Tertiary Undergraduate"
        elif code_int == 320 or (601 <= code_int <= 689):
            return "Post-Secondary Non-Tertiary Graduate"
        elif code_int == 710:
            return "1st Year Bachelor Level"
        elif code_int == 720:
            return "2nd Year Bachelor Level"
        elif code_int == 730:
            return "3rd Year Bachelor Level"
        elif code_int == 740:
            return "4th Year Bachelor Level"
        elif code_int == 750:
            return "5th Year Bachelor Level"
        elif code_int == 760:
            return "6th Year Bachelor Level"
        elif code_int == 800 or (801 <= code_int <= 889) or \
             (910 <= code_int <= 920) or (930 <= code_int <= 940):
            return "College Graduate"
    return "Unknown"


# ---------------------------------------------------------------------------
# PUBLIC API — used by base_cleaner.py
# ---------------------------------------------------------------------------
def apply_grade_map(series, version: int):
    """
    Apply the correct grade mapping to a pandas Series of raw grade codes.

    Args:
        series:  pandas Series of raw grade code values
        version: 1 (2016 Q2 only) or 2 (all other quarters)

    Returns:
        pandas Series of human-readable grade labels
    """
    if version == 1:
        return series.astype(str).apply(_map_grade_v1)
    elif version == 2:
        return series.astype(str).apply(_map_grade_v2)
    else:
        raise ValueError(f"Unknown grade map version: {version}. Must be 1 or 2.")
