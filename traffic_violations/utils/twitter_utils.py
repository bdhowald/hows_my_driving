def padding_spaces_needed(*items_to_pad: str) -> int:
    max_count_length: int = len(
        str(max(
            items_to_pad)))
    return (max_count_length * 2) + 1
