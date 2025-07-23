import re

def validate_email(email_str):
    
    email_str = email_str.strip()
    email_no_spaces = email_str.replace(" ", "")
    email_clean = email_no_spaces.lower()
    regex = r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'

    if not re.match(regex, email_clean):
        raise ValueError("Invalid email format.")

    return email_clean


