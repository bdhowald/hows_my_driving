from string import Template

LAST_QUERIED_STRING = "This vehicle was last queried on {} at {}"

LOOKUP_BOROUGH_STRING = 'Violations by borough for {}:\n\n'
LOOKUP_BOROUGH_STRING_CONTD = "Violations by borough for {}, cont'd:\n\n"
LOOKUP_RESULTS_DETAIL_STRING = '{}| {}\n'
LOOKUP_SUMMARY_STRING = '{}{}has been queried {} time{}.\n\n'
LOOKUP_TICKETS_STRING = "Total parking and camera violation tickets: {}\n\n"
LOOKUP_TICKETS_STRING_CONTD = "Parking and camera violation tickets for {}, cont'd:\n\n"
LOOKUP_YEAR_STRING = "Violations by year for {}:\n\n"
LOOKUP_YEAR_STRING_CONTD = "Violations by year for {}, cont'd:\n\n"


PLATE_TYPES_LOOKUP_STRING = ' (types: {}) '

PREVIOUS_LOOKUP_STATUS_STRING = ' by @{}: https://twitter.com/{}/status/{}.'

REPEAT_LOOKUP_STRING = ' Since then, {} has received {} new ticket{}.\n\n'

VEHICLE_HASHTAG = '#{}_{}'

def get_plate_types_string(plate_types):
    if plate_types:
        return PLATE_TYPES_LOOKUP_STRING.format(plate_types)
    else:
        return ' '

def pluralize(number):
    return '' if number == 1 else 's'