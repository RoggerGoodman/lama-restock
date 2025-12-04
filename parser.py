def parse_cod_v(raw_text):
    """
    Converts input like:
    '25765 01 31613 01 36839 02'
    into a list of tuples: [(25765, 1), (31613, 1), (36839, 2)]
    """
    # Split into tokens
    tokens = raw_text.split()

    # Must be pairs
    if len(tokens) % 2 != 0:
        raise ValueError("Input must contain an even number of tokens (cod v cod v ...)")

    result = []
    for i in range(0, len(tokens), 2):
        cod = int(tokens[i])
        v_raw = tokens[i+1]

        # Remove leading zeros: "01" → 1
        v = int(v_raw.lstrip("0")) if v_raw.lstrip("0") else 0

        result.append((cod, v))

    return result


def generate_sql_values(pairs):
    """
    Generates SQL VALUES section, e.g.:
    (25765,1),(31613,1),(36839,2)
    """
    return ",".join(f"({cod},{v})" for cod, v in pairs)


raw = """
18210 01 26860 01 2359 01 25437 01 18047 01 18048 01
26361 02 18049 01 18046 01 26361 01 18380 04 18380 06 18380 05
37651 01 26942 02 18380 03 18380 01 18380 07 26942 01 26231 01
26859 01 32528 01 26858 01 27692 01 28021 01
"""

pairs = parse_cod_v(raw)
print(generate_sql_values(pairs))
