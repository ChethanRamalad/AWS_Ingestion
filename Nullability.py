def validate_nullability(field_name, value, is_nullable):

    null_equivalents = [None, '', 'null', 'none', 'nan']

    if isinstance(value, str):
        check_val = value.strip().lower()
    else:
        check_val = value

    is_null = check_val in null_equivalents

    if not is_nullable and is_null:
        return False, f"Field '{field_name}' is NOT nullable but received null/empty value."

    return True, ""

# # Example tests:
# if __name__ == "__main__":
#     test_cases = [
#         ('email', 'user@example.com', False),
#         ('email', '', False),
#         ('middle_name', '', True),
#         ('price', None, False),
#         ('discount_code', None, True),
#         ('age', 'NaN', False),
#         ('notes', '   ', True)
#     ]

#     for field, val, nullable in test_cases:
#         valid, msg = validate_nullability(field, val, nullable)
#         print(f"Field: {field}, Value: {val!r}, Nullable: {nullable} -> Valid: {valid} {msg}")
