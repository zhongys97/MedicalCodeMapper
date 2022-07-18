
def find_common_parent(list_of_codes):

    if len(list_of_codes) == 1:
        return True, list_of_codes[0]

    min_length = 20
    for code in list_of_codes:
        if len(code) < min_length:
            min_length = len(code)
    if min_length == 0:
        return False, ""

    num_commons = -1
    for digit_pos in range(min_length):
        first_digits = []
        for code in list_of_codes:
            first_digits.append(code[:digit_pos + 1])


        if len(set(first_digits)) != 1:
            if digit_pos > 0:
                num_commons = digit_pos
                break
            elif digit_pos == 0:
                return False, ""

    if num_commons != -1:
        unformatted = code[:num_commons]
        return True, unformatted[:-1] if unformatted[-1] == "." else unformatted
    else:
        unformatted = code[:min_length]
        return True, unformatted[:-1] if unformatted[-1] == "." else unformatted
