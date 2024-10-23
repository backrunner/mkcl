import base36

# Unix timestamp for 2000-01-01 00:00:00 UTC in milliseconds
EPOCH_2000 = 946684800000

def generate_id(timestamp):
    """
    Generate a base36 ID from a given timestamp.

    Args:
    timestamp (int): Unix timestamp in milliseconds.

    Returns:
    str: Base36 encoded ID.
    """
    delta = timestamp - EPOCH_2000
    return base36.dumps(delta)
