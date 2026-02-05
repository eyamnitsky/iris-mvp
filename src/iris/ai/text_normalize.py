import re

def clean_email_text(text: str) -> str:
    if not text:
        return ""
    lines = []
    for line in text.splitlines():
        if line.strip().startswith(">"):
            continue
        if re.match(r"^\s*On .* wrote:\s*$", line):
            break
        lines.append(line)
    cleaned = "\n".join(lines).strip()
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned[:6000]


def normalize_slang(text: str) -> str:
    """
    Lightweight normalization to help smaller models.
    Also covers "around 2" already, but this helps "2ish"/"2-ish"/"2:30ish".
    """
    if not text:
        return ""

    t = text

    t = re.sub(r"\b(\d{1,2})\s*-\s*ish\b", r"around \1", t, flags=re.IGNORECASE)
    t = re.sub(r"\b(\d{1,2})\s*ish\b", r"around \1", t, flags=re.IGNORECASE)

    t = re.sub(r"\b(\d{1,2}\s*(?:am|pm))\s*-\s*ish\b", r"around \1", t, flags=re.IGNORECASE)
    t = re.sub(r"\b(\d{1,2}\s*(?:am|pm))\s*ish\b", r"around \1", t, flags=re.IGNORECASE)

    t = re.sub(r"\b(\d{1,2}:\d{2})\s*ish\b", r"around \1", t, flags=re.IGNORECASE)

    t = re.sub(r"\bnoon\s*ish\b", "around noon", t, flags=re.IGNORECASE)

    return t
