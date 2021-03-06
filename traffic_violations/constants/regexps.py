import re

HASHTAG_PATTERN = re.compile('[^#\w]+', re.UNICODE)

LEGACY_STRING_PARTS_REGEX = r'(?<!state:|plate:)\s'

medallion_regex = r'^[0-9][A-Z][0-9]{2}$'
MEDALLION_PATTERN = re.compile(medallion_regex)

numbers_regex = r'[0-9]{4}'
NUMBER_PATTERN = re.compile(numbers_regex)

PLATE_FORMAT_REGEX = r'(?=(\b[a-zA-Z9]{2}\s*:\s*[a-zA-Z0-9]+\s*:\s*[a-zA-Z0-9]{3}(?:,[a-zA-Z0-9]{3})*\b|\b[a-zA-Z9]{2}\s*:\s*[a-zA-Z0-9]{3}(?:,[a-zA-Z0-9]{3})*\s*:\s*[a-zA-Z0-9]+\b|\b[a-zA-Z0-9]+\s*:\s*[a-zA-Z9]{2}\s*:\s*[a-zA-Z0-9]{3}(?:,[a-zA-Z0-9]{3})*\b|\b[a-zA-Z0-9]+\s*:\s*[a-zA-Z0-9]{3}(?:,[a-zA-Z0-9]{3})*\s*:\s*[a-zA-Z9]{2}\b|\b[a-zA-Z0-9]{3}(?:,[a-zA-Z0-9]{3})*\s*:\s*[a-zA-Z9]{2}\s*:\s*[a-zA-Z0-9]+\b|\b[a-zA-Z0-9]{3}(?:,[a-zA-Z0-9]{3})*\s*:\s*[a-zA-Z0-9]+\s*:\s*[a-zA-Z9]{2}\b|\b[a-zA-Z9]{2}\s*:\s*[a-zA-Z0-9]+\b|\b[a-zA-Z0-9]+\s*:\s*[a-zA-Z9]{2}\b))'

PLATE_PATTERN = re.compile('[\W_]+', re.UNICODE)

PLATE_TYPES = ['AGC', 'AGR', 'AMB', 'APP', 'ARG',
               'ATD', 'ATV', 'AYG', 'BOB', 'BOT',
               'CBS', 'CCK', 'CHC', 'CLG', 'CMB',
               'CME', 'CMH', 'COM', 'CSP', 'DLR',
               'FAR', 'FPW', 'GAC', 'GSM', 'HAC',
               'HAM', 'HIR', 'HIS', 'HOU', 'HSM',
               'IRP', 'ITP', 'JCA', 'JCL', 'JSC',
               'JWV', 'LMA', 'LMB', 'LMC', 'LOC',
               'LTR', 'LUA', 'MCD', 'MCL', 'MED',
               'MOT', 'NLM', 'NYA', 'NYC', 'NYS',
               'OMF', 'OML', 'OMO', 'OMR', 'OMS',
               'OMT', 'OMV', 'ORC', 'ORG', 'ORM',
               'PAS', 'PHS', 'PPH', 'PSD', 'RGC',
               'RGL', 'SCL', 'SEM', 'SNO', 'SOS',
               'SPC', 'SPO', 'SRF', 'SRN', 'STA',
               'STG', 'SUP', 'THC', 'TOW', 'TRA',
               'TRC', 'TRL', 'USC', 'USS', 'VAS',
               'VPL', 'WUG']
PLATE_TYPES_PATTERN = re.compile(f"^({'|'.join(PLATE_TYPES)})$")

STATE_ABBREVIATIONS = ['99', 'AB', 'AK', 'AL', 'AR',
                       'AZ', 'BC', 'CA', 'CO', 'CT',
                       'DC', 'DE', 'DP', 'FL', 'FM',
                       'FO', 'GA', 'GU', 'GV', 'HI',
                       'IA', 'ID', 'IL', 'IN', 'KS',
                       'KY', 'LA', 'MA', 'MB', 'MD',
                       'ME', 'MI', 'MN', 'MO', 'MP',
                       'MS', 'MT', 'MX', 'NB', 'NC',
                       'ND', 'NE', 'NF', 'NH', 'NJ',
                       'NM', 'NS', 'NT', 'NU', 'NV',
                       'NY', 'OH', 'OK', 'ON', 'OR',
                       'PA', 'PE', 'PR', 'PW', 'QC',
                       'RI', 'SC', 'SD', 'SK', 'TN',
                       'TX', 'UT', 'VA', 'VI', 'VT',
                       'WA', 'WI', 'WV', 'WY', 'YT']
STATE_ABBREVIATIONS_PATTERN = re.compile(f"^({'|'.join(STATE_ABBREVIATIONS)})$")

state_minus_words_regex = r'^(99|AB|AK|AL|AR|AZ|BC|CA|CO|CT|DC|DE|DP|FL|FM|FO|GA|GU|GV|IA|ID|IL|KS|KY|LA|MA|MB|MD|MH|MI|MN|MO|MP|MS|MT|MX|NB|NC|ND|NE|NF|NH|NJ|NM|NS|NT|NU|NV|NY|PA|PE|PR|PW|QC|RI|SC|SD|SK|STATE|TN|TX|UT|VA|VI|VT|WA|WI|WV|WY|YT)$'
STATE_MINUS_WORDS_PATTERN = re.compile(state_minus_words_regex)



# state_full_regex   =
# r'^(ALABAMA|ALASKA|ARKANSAS|ARIZONA|CALIFORNIA|COLORADO|CONNECTICUT|DELAWARE|D\.C\.|DISTRICT
# OF COLUMBIA|FEDERATED STATES OF
# MICRONESIA|FLORIDA|GEORGIA|GUAM|HAWAII|IDAHO|ILLINOIS|INDIANA|IOWA|KANSAS|KENTUCKY|LOUISIANA|MAINE|MARSHALL
# ISLANDS|MARYLAND|MASSACHUSETTS|MICHIGAN|MINNESTOA|MISSISSIPPI|MISSOURI|MONTANA|NEBRASKA|NEVADA|NEW
# HAMPSHIRE|NEW JERSEY|NEW MEXICO|NEW YORK|NORTH CAROLINA|NORTH
# DAKOTA|NORTHERN MARIANA
# ISLANDS|OHIO|OKLAHOMA|OREGON|PALAU|PENNSYLVANIA|PUERTO RICO|RHODE
# ISLAND|SOUTH CAROLINA|SOUTH
# DAKOTA|TENNESSEE|TEXAS|UTAH|VERMONT|U\.S\. VIRGIN ISLANDS|US VIRGIN
# ISLANDS|VIRGIN ISLANDS|VIRGINIA|WASHINGTON|WEST
# VIRGINIA|WISCONSIN|WYOMING)$'
# state_full_pattern = re.compile(state_full_regex)