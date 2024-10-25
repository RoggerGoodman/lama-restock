def clean_and_convert(values):  # Clean the numbers (remove commas and convert to int)
    cleaned_values = []
    for value in values:
        # If the value contains a decimal different from ',00', skip the entire row (outer loop iteration)
        if ',' in value and not value.endswith(',00'):
            return None  # This signals that the article must be skipped

        # Clean and convert
        cleaned_value = int(value.replace(',00', '').replace(
            '.', ''))  # Remove commas, convert to int
        cleaned_values.append(cleaned_value)

    return cleaned_values


def custom_round(value):
    # Get the integer part and the decimal part
    integer_part = int(value)
    decimal_part = value - integer_part

    # Apply the rounding logic
    if decimal_part <= 0.55:  # TODO Could be made user editable
        return integer_part  # Round down
    else:
        return integer_part + 1  # Round up
    
def custom_round2(value, deviation, current_stock):
    # Get the integer part and the decimal part
    integer_part = int(value)
    decimal_part = value - integer_part

    # Apply the rounding logic
    if decimal_part <= 0.3:  # TODO Could be made user editable
        if (deviation > 10):
            return integer_part + 1  # Round up
        elif (current_stock < 0):
            return integer_part + 1  # Round up
        return integer_part  # Round down
    else:
        return integer_part + 1  # Round up



# def custom_round_misha_edition(value):
#     if not isinstance(value, int) or not isinstance(value, float):
#         raise ValueError('Value must be a fucking int or float dumbass')
#     return int(value) if value - int(value) <= 0.3 else int(value) + 1
