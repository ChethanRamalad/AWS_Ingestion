def normalize_price_to_inr(price_input, usd_to_inr_rate=82.5):
    price_str = str(price_input).strip()
    is_usd = False
    if price_str.startswith('$') or 'usd' in price_str.lower():
        is_usd = True
    for symbol in ['₹', 'Rs.', 'Rs', '$', 'usd', ',', ' ']:
        price_str = price_str.lower().replace(symbol, '')
    price_str = price_str.strip()

    if price_str == '':
        raise ValueError("Empty price after cleaning.")

    try:
        price_float = float(price_str)
    except ValueError:
        raise ValueError(f"Cannot convert price to float: '{price_input}'")

    if price_float < 0:
        raise ValueError("Price cannot be negative.")
    if is_usd:
        price_float *= usd_to_inr_rate
    normalized = ('%.10g' % price_float)
    if 'e' in normalized or 'E' in normalized:
        normalized = format(price_float, 'f').rstrip('0').rstrip('.')
        normalized = normalized if normalized else '0'

    return normalized


# if __name__ == "__main__":
#     test_prices = [
#         "₹1,200.00",
#         "$15.50",
#         "USD 100",
#         "1200.50",
#         "001200.00",
#         "0.00",
#         0,
#         "2000",
#         "2000.000000",
#         "12.34000",
#         "Rs. 4500",
#         "usd 0",
#         "-100",
#         "abc"
#     ]

#     for p in test_prices:
#         try:
#             norm_price = normalize_price_to_inr(p)
#             print(f"Input: {p!r} -> Normalized INR Price: {norm_price}")
#         except ValueError as e:
#             print(f"Input: {p!r} -> Error: {e}")
