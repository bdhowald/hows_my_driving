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

NO_TICKETS_FOUND_STRING = "I couldn't find any tickets for {}:{}{}."

PLATE_TYPES_LOOKUP_STRING = ' (types: {}) '

PREVIOUS_LOOKUP_STATUS_STRING = ' by @{}: https://twitter.com/{}/status/{}.'

REPEAT_CAMERA_OFFENDER_STRING = (
    'Featured #RepeatCameraOffender:\n\n'
    '{} has received {} camera violations:\n\n'
    '{} | Red Light Camera Violations\n'
    '{} | Speed Safety Camera Violations\n\n'
    'This makes {}{} the {} camera violator in New York City.')

REPEAT_LOOKUP_DATE_FORMAT: str = '%B %-d, %Y'
REPEAT_LOOKUP_STRING = ' Since then, {} has received {} new ticket{}.\n\n'
REPEAT_LOOKUP_TIME_FORMAT: str = '%I:%M%p'

VEHICLE_HASHTAG = '#{}_{}'

def get_plate_types_string(plate_types: str) -> str:
    if plate_types:
        return PLATE_TYPES_LOOKUP_STRING.format(plate_types)
    else:
        return ' '

def pluralize(number: int) -> str:
    return '' if number == 1 else 's'