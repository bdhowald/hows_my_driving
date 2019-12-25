def determine_ordinal_indicator(cardinal_number: int) -> str:
    if cardinal_number % 10 == 1 and cardinal_number % 100 != 11:
        return 'st'
    elif cardinal_number % 10 == 2 and cardinal_number % 100 != 12:
        return 'nd'
    elif cardinal_number % 10 == 3 and cardinal_number % 100 != 13:
        return 'rd'
    else:
        return 'th'