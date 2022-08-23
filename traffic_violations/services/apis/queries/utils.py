import re

STRIP_EXCESS_CHARACTERS_REGEX = r'\n\s+'

def format_query_string(raw_string: str) -> str:
    return re.sub(STRIP_EXCESS_CHARACTERS_REGEX, '', raw_string)