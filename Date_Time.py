from dateutil import parser
import re

def validate_datetime(input_str):
    if not input_str or not isinstance(input_str, str):
        raise ValueError("Input must be a non-empty string.")
    cleaned_str = input_str.strip()
    cleaned_str = re.sub(r'[\-,]', ' ', cleaned_str)

    try:
        dt = parser.parse(cleaned_str, dayfirst=True, fuzzy=True)
        formatted = dt.strftime('%d/%m/%Y %H:%M')
        return formatted

    except (ValueError, OverflowError):
        raise ValueError("Invalid datetime or cannot parse.")
