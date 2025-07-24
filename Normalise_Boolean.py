def normalize_boolean(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value == 1:
            return True
        elif value == 0:
            return False
        else:
           raise ValueError(f"Invalid integer for boolean conversion: {value}")
    if isinstance(value, str):
        val = value.strip().lower()
        if val in {'1', 'true', 'yes', 'y', 't'}:
            return True
        elif val in {'0', 'false', 'no', 'n', 'f', ''}:
            return False
        else:
            raise ValueError(f"Invalid string for boolean conversion: '{value}'")
    raise ValueError(f"Unsupported type for boolean conversion: {type(value)}")
# if __name__ == "__main__":
#     test_values = [
#         True, False, 
#         1, 0, 2,
#         'Yes', 'No', 'TRUE', 'False', 'y', 'n', '',
#         'maybe', 't', 'f',
#         None,
#     ]

#     for val in test_values:
#         try:
#             normalized = normalize_boolean(val)
#             print(f"Input: {val!r} => Normalized: {normalized}")
#         except ValueError as e:
#             print(f"Input: {val!r} => Error: {e}")
