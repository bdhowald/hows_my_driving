# Imports
import getpass
import logging
import json
import optparse
import pdb
import pytz
import re
import requests
import sys
import tweepy

from collections import Counter
from datetime import datetime, timezone, time, timedelta
from pprint import pprint
from sqlalchemy import create_engine


LOGGING_LEVELS = {'critical': logging.CRITICAL,
                  'error': logging.ERROR,
                  'warning': logging.WARNING,
                  'info': logging.INFO,
                  'debug': logging.DEBUG}



class TrafficViolationsTweeter:

    def __init__(self):
        password_str = 'SafeStreetsNow2018!' if getpass.getuser() == 'safestreets' else ''


        # Create a engine for connecting to MySQL
        self.engine = create_engine('mysql+pymysql://root:' + password_str + '@localhost/traffic_violations?charset=utf8')


        # Create a logger
        self.logger = logging.getLogger('hows_my_driving')


        # Set up Twitter auth
        self.auth = tweepy.OAuthHandler('UzW1c1Gy4FmktSkfXF1dgfupr', '17Uf3vrk44m5fjTUEBPx8sPltX45OfZZtApWhWqt139O0GBgkV')
        self.auth.set_access_token('976593574732222465-YKv9y3mT9Vhu7Ufm7xkfk6Z2T3By3K9', 'B9dNrNT5io8GrB18Roy4eUSqDoR9YvJoErHKfT8aw3sGl')

        self.api = tweepy.API(self.auth, retry_count=3, retry_delay=5, retry_errors=set([403, 500, 503]))


    def run(self):
        print('Setting up logging')
        parser = optparse.OptionParser()
        parser.add_option('-l', '--logging-level', help='Logging level')
        parser.add_option('-f', '--logging-file', help='Logging file name')
        (options, args) = parser.parse_args()
        logging_level = LOGGING_LEVELS.get(options.logging_level, logging.NOTSET)
        logging.basicConfig(level=logging_level, filename=options.logging_file,
                            format='%(asctime)s %(levelname)s: %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')


        twitterStream = tweepy.Stream(self.auth, MyStreamListener(self))
        # twitterStream.filter(track=['howsmydrivingny'])

        userstream = twitterStream.userstream()


    def detect_state(self, state_input):
        state_abbr_regex   = r'^(99|AB|AK|AL|AR|AZ|BC|CA|CO|CT|DC|DE|DP|FL|FM|FO|GA|GU|GV|HI|IA|ID|IL|IN|KS|KY|LA|MA|MB|MD|ME|MI|MN|MO|MP|MS|MT|MX|NB|NC|ND|NE|NF|NH|NJ|NM|NS|NT|NV|NY|OH|OK|ON|OR|PA|PE|PR|PW|QB|RI|SC|SD|SK|TN|TX|UT|VA|VI|VT|WA|WI|WV|WY|YT)$'
        # state_full_regex   = r'^(ALABAMA|ALASKA|ARKANSAS|ARIZONA|CALIFORNIA|COLORADO|CONNECTICUT|DELAWARE|D\.C\.|DISTRICT OF COLUMBIA|FEDERATED STATES OF MICRONESIA|FLORIDA|GEORGIA|GUAM|HAWAII|IDAHO|ILLINOIS|INDIANA|IOWA|KANSAS|KENTUCKY|LOUISIANA|MAINE|MARSHALL ISLANDS|MARYLAND|MASSACHUSETTS|MICHIGAN|MINNESTOA|MISSISSIPPI|MISSOURI|MONTANA|NEBRASKA|NEVADA|NEW HAMPSHIRE|NEW JERSEY|NEW MEXICO|NEW YORK|NORTH CAROLINA|NORTH DAKOTA|NORTHERN MARIANA ISLANDS|OHIO|OKLAHOMA|OREGON|PALAU|PENNSYLVANIA|PUERTO RICO|RHODE ISLAND|SOUTH CAROLINA|SOUTH DAKOTA|TENNESSEE|TEXAS|UTAH|VERMONT|U\.S\. VIRGIN ISLANDS|US VIRGIN ISLANDS|VIRGIN ISLANDS|VIRGINIA|WASHINGTON|WEST VIRGINIA|WISCONSIN|WYOMING)$'

        state_abbr_pattern = re.compile(state_abbr_regex)
        # state_full_pattern = re.compile(state_full_regex)

        return state_abbr_pattern.search(state_input.upper()) != None# or state_full_pattern.search(state_input.upper()) != None


    def find_campaign_hashtags(self, string_parts):
        # Instantiate a connection.
        conn = self.engine.connect()

        # Look for campaign hashtags in the message's text.
        campaigns_present = conn.execute(""" select id, hashtag from campaigns where hashtag in (%s) """ % ','.join(['%s'] * len(string_parts)), string_parts)
        result            = [tuple(i) for i in campaigns_present.cursor]

        # Close the connection.
        conn.close()

        return result


    def form_successful_response_parts(self, query_result, username):

        MAX_TWITTER_STATUS_LENGTH = 280

        # response_chunks holds tweet-length-sized parts of the response
        # to be tweeted out or appended into a single direct message.
        response_chunks   = []
        violations_string = ""


        if query_result.get('included_campaigns'):
            campaign_chunk  = []
            campaign_string = ""

            for campaign in query_result['included_campaigns']:
                num_vehicles = campaign['campaign_vehicles']
                num_tickets  = campaign['campaign_tickets']

                next_string_part = "{} {} {} {} {} been tagged with {}.\n".format(num_vehicles, 'vehicle with' if num_vehicles == 1 else 'vehicles with a total of', num_tickets, 'ticket' if num_tickets == 1 else 'tickets', 'has' if num_vehicles == 1 else 'have', campaign['campaign_hashtag'])

                # how long would it be
                potential_response_length = len(username + ' ' + campaign_string + next_string_part)

                if (potential_response_length <= MAX_TWITTER_STATUS_LENGTH):
                    campaign_string += next_string_part
                else:
                    campaign_chunk.append(username + ' ' + campaign_string)
                    campaign_string = next_string_part

            # Get any part of string left over
            campaign_chunk.append(username + ' ' + campaign_string)

            # Put campaign string in its own tweet
            response_chunks.append(campaign_chunk)


        # Get total violations
        total_violations = sum([s['count'] for s in query_result['violations']])
        self.logger.debug("total_violations: %s", total_violations)


        # Append to initially blank string to build tweet.
        violations_string += "#{}_{} has been queried {} {}.\n\n".format(query_result['state'], query_result['plate'], query_result['frequency'], 'time' if int(query_result['frequency']) == 1 else 'times')

        # If this vehicle has been queried before...
        if query_result.get('previous_result'):

            # Find new violations.
            previous_violations = query_result['previous_result']['num_tickets']
            new_violations      = total_violations - previous_violations

            # If there are new violations...
            if new_violations > 0:

                # Determine when the last lookup was...
                previous_time = query_result['previous_result']['created_at']
                now           = datetime.now()
                utc           = pytz.timezone('UTC')
                eastern       = pytz.timezone('US/Eastern')

                adjusted_time = utc.localize(previous_time).astimezone(eastern)
                adjusted_now  = utc.localize(now).astimezone(eastern)

                # If at least five have passed...
                if adjusted_now - timedelta(minutes=5) > adjusted_time:

                    # Add the new ticket info and previous lookup time to the string.
                    violations_string += 'Since the last time the vehicle was queried ({} at {}), #{}_{} has received {} new {}.\n\n'.format(adjusted_time.strftime('%B %e, %Y'), adjusted_time.strftime('%I:%M%p'), query_result['state'], query_result['plate'], new_violations, 'ticket' if new_violations == 1 else 'tickets')


        violations_string += "Total parking and camera violation tickets: {}\n\n".format(total_violations)

        max_count_length = len(str(max([i['count'] for i in query_result['violations']])))
        spaces_needed    = (max_count_length * 2) + 1

        self.logger.debug('\nspaces_needed: %s\n', spaces_needed)

        # Grab every violation grouped by name
        for violation in query_result['violations']:

            # Titleize for readability.
            violation_name = violation['name'].title()
            # Give no-name tickets a key
            if len(violation_name) == 0:
                violation_name = "No Violation Description Available"

            violation_count        = violation['count']
            violation_count_length = len(str(violation_count))

            # e.g., if spaces_needed is 5, and violation_count_length is 2, we need to pad to 3.
            # e.g., if spaces_needed is 5, and violation_count_length is 1, we need to pad to 4.
            left_justify_amount    = spaces_needed - violation_count_length
            self.logger.debug('\nleft_justify_amount: %s\n', left_justify_amount)

            # formulate next string part
            next_string_part    = "{}| {}\n".format(str(violation_count).ljust(left_justify_amount), violation_name)

            # how long would it be
            potential_response_length = len(username + ' ' + violations_string + next_string_part)

            # If username, space, violation string so far and new part are less or equal than 280 characters, append to existing tweet string.
            if (potential_response_length <= MAX_TWITTER_STATUS_LENGTH):
                violations_string += next_string_part

                self.logger.debug("length: %s", len(violations_string))
                self.logger.debug("string: %s", violations_string)

            else:
                # Append ready string into parts for response.
                response_chunks.append(username + ' ' + violations_string)

                violations_string = "Parking and camera violation tickets for #{}_{}, cont'd:\n\n".format(query_result['state'], query_result['plate'])
                violations_string += next_string_part

                self.logger.debug("violations_string: %s", violations_string)
                self.logger.debug("length: %s", len(violations_string))
                self.logger.debug("string: %s", violations_string)


        # If we finish the list with a non-empty string, append that string to response parts
        if len(violations_string) != 0:
            # Append ready string into parts for response.
            response_chunks.append(username + ' ' + violations_string)

            self.logger.debug("length: %s", len(violations_string))
            self.logger.debug("string: %s", violations_string)

        # Send it back!
        return response_chunks


    def handle_short_response(self, message_id, message_type, response_message, username):
        if message_type == 'direct_message':
            self.is_production() and self.api.send_direct_message(screen_name = username, text = response_message)
        else:
            self.is_production() and self.api.update_status(response_message, in_reply_to_status_id = message_id)

        self.logger.debug("%s %s", username, response_message)


    def infer_plate_and_state_data(self, list_of_vehicle_tuples):
        plate_data = []

        for vehicle_tuple in list_of_vehicle_tuples:
            this_plate = { 'original_string': ':'.join(vehicle_tuple), 'valid_plate': False }

            if len(vehicle_tuple) != 2:
                this_plate['valid_plate'] = False
            else:
                part0 = vehicle_tuple[0]
                part1 = vehicle_tuple[1]

                is_part0_state = self.detect_state(part0)
                is_part1_state = self.detect_state(part1)

                if is_part0_state and len(part1) > 0:
                    this_plate['state'] = part0
                    this_plate['plate'] = part1
                    this_plate['valid_plate'] = True
                elif is_part1_state and len(part0) > 0:
                    this_plate['state'] = part1
                    this_plate['plate'] = part0
                    this_plate['valid_plate'] = True

            plate_data.append(this_plate)

        return plate_data


    def initiate_reply(self, received):
        self.logger.info('\n')
        self.logger.info('Calling initiate_reply')

        # Print args
        self.logger.info('args:')
        self.logger.info('received: %s', received)

        utc = pytz.timezone('UTC')

        args_for_response = {}

        if hasattr(received, 'extended_tweet'):
            self.logger.debug('\n\nWe have an extended tweet\n\n')

            extended_tweet = received.extended_tweet

            # don't perform if there is no text
            if 'full_text' in extended_tweet:
                entities = extended_tweet['entities']

                if 'user_mentions' in entities:
                    array_of_usernames = [v['screen_name'] for v in entities['user_mentions']]

                    if 'HowsMyDrivingNY' in array_of_usernames:
                        full_text       = extended_tweet['full_text']
                        modified_string = ' '.join(full_text.split())

                        args_for_response['created_at']          = utc.localize(received.created_at).astimezone(timezone.utc).strftime('%a %b %d %H:%M:%S %z %Y')
                        args_for_response['id']                  = received.id
                        args_for_response['mentioned_users']     = [s.lower() for s in array_of_usernames]
                        args_for_response['legacy_string_parts'] = re.split(r'(?<!state:|plate:)\s', modified_string.lower())
                        args_for_response['string_parts']        = re.split(' ', modified_string.lower())
                        args_for_response['username']            = received.user.screen_name


                        if received.user.screen_name != 'HowsMyDrivingNY':
                            selfprocess_response_message(received, args_for_response)


        elif hasattr(received, 'entities'):
            self.logger.debug('\n\nWe have entities\n\n')

            entities = received.entities

            if 'user_mentions' in entities:
                array_of_usernames = [v['screen_name'] for v in entities['user_mentions']]

                if 'HowsMyDrivingNY' in array_of_usernames:
                    text            = received.text
                    modified_string = ' '.join(text.split())

                    args_for_response['created_at']          = utc.localize(received.created_at).astimezone(timezone.utc).strftime('%a %b %d %H:%M:%S %z %Y')
                    args_for_response['id']                  = received.id
                    args_for_response['mentioned_users']     = [s.lower() for s in array_of_usernames]
                    args_for_response['legacy_string_parts'] = re.split(r'(?<!state:|plate:)\s', modified_string.lower())
                    args_for_response['string_parts']        = re.split(' ', modified_string.lower())
                    args_for_response['username']            = received.user.screen_name

                    if received.user.screen_name != 'HowsMyDrivingNY':
                        self.process_response_message(received, args_for_response)


        elif hasattr(received, 'direct_message'):
            self.logger.debug('\n\nWe have a direct message\n\n')

            direct_message  = received.direct_message
            recipient       = direct_message['recipient']
            sender          = direct_message['sender']

            if recipient['screen_name']  == 'HowsMyDrivingNY':
                text            = direct_message['text']
                modified_string = ' '.join(text.split())

                args_for_response['created_at']          = direct_message['created_at']
                args_for_response['id']                  = direct_message['id']
                args_for_response['legacy_string_parts'] = re.split(r'(?<!state:|plate:)\s', modified_string.lower())
                args_for_response['string_parts']        = re.split(' ', modified_string.lower())
                args_for_response['username']            = sender['screen_name']

                if sender['screen_name'] != 'HowsMyDrivingNY':
                    self.process_response_message(received, args_for_response)


    def is_production(self):
        return True if getpass.getuser() == 'safestreets' else False


    def perform_queries(self, args):

        self.logger.debug('Performing lookup for plate.')

        # Instantiate connection.
        conn = self.engine.connect()

        # pattern only allows alphanumeric characters.
        plate_pattern = re.compile('[\W_]+', re.UNICODE)

        # Grab plate and plate from args.
        created_at   = datetime.strptime(args['created_at'], '%a %b %d %H:%M:%S %z %Y').strftime('%Y-%m-%d %H:%M:%S') if 'created_at' in args else None
        message_id   = args['message_id'] if 'message_id' in args else None
        message_type = args['message_type']
        plate        = plate_pattern.sub('', args['plate'].strip().upper())
        state        = args['state'].strip().upper()
        username     = re.sub('@', '', args['username'])

        self.logger.debug('Listing args... plate: %s, state: %s, message_id: %s, created_at: %s', plate, state, str(message_id), str(created_at))



        # Find medallion plates
        #
        medallion_regex    = r'^[0-9][A-Z][0-9]{2}$'
        medallion_pattern  = re.compile(medallion_regex)

        if medallion_pattern.search(plate.upper()) != None:
            medallion_response = requests.get('https://data.cityofnewyork.us/resource/7drc-shp9.json?license_number={}'.format(plate))
            medallion_data     = medallion_response.json()

            sorted_list        = sorted(set([res['dmv_license_plate_number'] for res in medallion_data]))
            plate              = sorted_list[-1] if sorted_list else plate


        # # Run search_query on local database.
        # search_query = conn.execute("""select violation as name, count(violation) as count from all_traffic_violations_redo where plate = %s and state = %s group by violation""", (plate, state))
        # # Query the result and get cursor.Dumping that data to a JSON is looked by extension
        # result = {'violations': [dict(zip(tuple (search_query.keys()), i)) for i in search_query.cursor]}

        # set up return data structure
        combined_violations = {}

        # set up remaining query params
        limit = 10000
        token = 'q198HrEaAdCJZD4XCLDl2Uq0G'

        # Grab data from 'Open Parking and Camera Violations'
        #
        # response from city open data portal
        opacv_endpoint = 'https://data.cityofnewyork.us/resource/uvbq-3m68.json'
        opacv_response = requests.get(opacv_endpoint + '?$limit={}&$$app_token={}&plate={}&state={}'.format(limit, token, plate, state))
        opacv_data     = opacv_response.json()

        # log response
        self.logger.debug('violations raw: %s', opacv_response)
        self.logger.debug('Open Parking and Camera Violations data: %s', opacv_data)

        # humanized names for violations
        opacv_humanized_names = {'': 'No Description Given',  'ALTERING INTERCITY BUS PERMIT' : 'Altered Intercity Bus Permit',  'ANGLE PARKING' : 'No Angle Parking',  'ANGLE PARKING-COMM VEHICLE' : 'No Angle Parking',  'BEYOND MARKED SPACE' : 'No Parking Beyond Marked Space',  'BIKE LANE' : 'Blocking Bike Lane',  'BLUE ZONE' : 'No Parking - Blue Zone',  'BUS LANE VIOLATION' : 'Bus Lane Violation',  'BUS PARKING IN LOWER MANHATTAN' : 'Bus Parking in Lower Manhattan',  'COMML PLATES-UNALTERED VEHICLE' : 'Commercial Plates on Unaltered Vehicle',  'CROSSWALK' : 'Blocking Crosswalk',  'DETACHED TRAILER' : 'Detached Trailer',  'DIVIDED HIGHWAY' : 'No Stopping - Divided Highway',  'DOUBLE PARKING' : 'Double Parking',  'DOUBLE PARKING-MIDTOWN COMML' : 'Double Parking - Midtown Commercial Zone',  'ELEVATED/DIVIDED HIGHWAY/TUNNL' : 'No Stopping in Tunnel or on Elevated Highway',  'EXCAVATION-VEHICLE OBSTR TRAFF' : 'No Stopping - Adjacent to Street Construction',  'EXPIRED METER' : 'Expired Meter',  'EXPIRED METER-COMM METER ZONE' : 'Expired Meter - Commercial Meter Zone',  'EXPIRED MUNI METER' : 'Expired Meter',  'EXPIRED MUNI MTR-COMM MTR ZN' : 'Expired Meter - Commercial Meter Zone',  'FAIL TO DISP. MUNI METER RECPT' : 'Failure to Display Meter Receipt',  'FAIL TO DSPLY MUNI METER RECPT' : 'Failure to Display Meter Receipt',  'FAILURE TO DISPLAY BUS PERMIT' : 'Failure to Display Bus Permit',  'FAILURE TO STOP AT RED LIGHT' : 'Failure to Stop at Red Light',  'FEEDING METER' : 'Feeding Meter',  'FIRE HYDRANT' : 'Fire Hydrant',  'FRONT OR BACK PLATE MISSING' : 'Front or Back Plate Missing',  'IDLING' : 'Idling',  'IMPROPER REGISTRATION' : 'Improper Registration',  'INSP STICKER-MUTILATED/C\'FEIT' : 'Inspection Sticker Mutilated or Counterfeit',  'INSP. STICKER-EXPIRED/MISSING' : 'Inspection Sticker Expired or Missing',  'INTERSECTION' : 'No Stopping - Intersection',  'MARGINAL STREET/WATER FRONT' : 'No Parking on Marginal Street or Waterfront',  'MIDTOWN PKG OR STD-3HR LIMIT' : 'Midtown Parking or Standing - 3 Hour Limit',  'MISCELLANEOUS' : 'Miscellaneous',  'MISSING EQUIPMENT' : 'Missing Required Equipment',  'NGHT PKG ON RESID STR-COMM VEH' : 'No Nighttime Parking on Residential Street - Commercial Vehicle',  'NIGHTTIME STD/ PKG IN A PARK' : 'No Nighttime Standing or Parking in a Park',  'NO MATCH-PLATE/STICKER' : 'Plate and Sticker Do Not Match',  'NO OPERATOR NAM/ADD/PH DISPLAY' : 'Failure to Display Operator Information',  'NO PARKING-DAY/TIME LIMITS' : 'No Parking - Day/Time Limits',  'NO PARKING-EXC. AUTH. VEHICLE' : 'No Parking - Except Authorized Vehicles',  'NO PARKING-EXC. HNDICAP PERMIT' : 'No Parking - Except Disability Permit',  'NO PARKING-EXC. HOTEL LOADING' : 'No Parking - Except Hotel Loading',  'NO PARKING-STREET CLEANING' : 'No Parking - Street Cleaning',  'NO PARKING-TAXI STAND' : 'No Parking - Taxi Stand',  'NO STANDING EXCP D/S' : 'No Standing - Except Department of State',  'NO STANDING EXCP DP' : 'No Standing - Except Diplomat',  'NO STANDING-BUS LANE' : 'No Standing - Bus Lane',  'NO STANDING-BUS STOP' : 'No Standing - Bus Stop',  'NO STANDING-COMM METER ZONE' : 'No Standing - Commercial Meter Zone',  'NO STANDING-COMMUTER VAN STOP' : 'No Standing - Commuter Van Stop',  'NO STANDING-DAY/TIME LIMITS' : 'No Standing - Day/Time Limits',  'NO STANDING-EXC. AUTH. VEHICLE' : 'No Standing - Except Authorized Vehicle',  'NO STANDING-EXC. TRUCK LOADING' : 'No Standing - Except Truck Loading',  'NO STANDING-FOR HIRE VEH STOP' : 'No Standing - For Hire Vehicle Stop',  'NO STANDING-HOTEL LOADING' : 'No Standing - Hotel Loading',  'NO STANDING-OFF-STREET LOT' : 'No Standing - Off-Street Lot',  'NO STANDING-SNOW EMERGENCY' : 'No Standing - Snow Emergency',  'NO STANDING-TAXI STAND' : 'No Standing - Taxi Stand',  'NO STD(EXC TRKS/GMTDST NO-TRK)' : 'No Standing - Except Trucks in Garment District',  'NO STOP/STANDNG EXCEPT PAS P/U' : 'No Stopping or Standing Except for Passenger Pick-Up',  'NO STOPPING-DAY/TIME LIMITS' : 'No Stopping - Day/Time Limits',  'NON-COMPLIANCE W/ POSTED SIGN' : 'Non-Compliance with Posted Sign',  'OBSTRUCTING DRIVEWAY' : 'Obstructing Driveway',  'OBSTRUCTING TRAFFIC/INTERSECT' : 'Obstructing Traffic or Intersection',  'OT PARKING-MISSING/BROKEN METR' : 'Overtime Parking at Missing or Broken Meter',  'OTHER' : 'Other',  'OVERNIGHT TRACTOR TRAILER PKG' : 'Overnight Parking of Tractor Trailer',  'OVERTIME PKG-TIME LIMIT POSTED' : 'Overtime Parking - Time Limit Posted',  'OVERTIME STANDING DP' : 'Overtime Standing - Diplomat',  'OVERTIME STDG D/S' : 'Overtime Standing - Department of State',  'PARKED BUS-EXC. DESIG. AREA' : 'Bus Parking Outside of Designated Area',  'PEDESTRIAN RAMP' : 'Blocking Pedestrian Ramp',  'PHTO SCHOOL ZN SPEED VIOLATION' : 'School Zone Speed Camera Violation',  'PKG IN EXC. OF LIM-COMM MTR ZN' : 'Parking in Excess of Limits - Commercial Meter Zone',  'PLTFRM LFTS LWRD POS COMM VEH' : 'Commercial Vehicle Platform Lifts in Lowered Position',  'RAILROAD CROSSING' : 'No Stopping - Railroad Crossing',  'REG STICKER-MUTILATED/C\'FEIT' : 'Registration Sticker Mutilated or Counterfeit',  'REG. STICKER-EXPIRED/MISSING' : 'Registration Sticker Expired or Missing',  'REMOVE/REPLACE FLAT TIRE' : 'Replacing Flat Tire on Major Roadway',  'SAFETY ZONE' : 'No Standing - Safety Zone',  'SELLING/OFFERING MCHNDSE-METER' : 'Selling or Offering Merchandise From Metered Parking',  'SIDEWALK' : 'Parked on Sidewalk',  'STORAGE-3HR COMMERCIAL' : 'Street Storage of Commercial Vehicle Over 3 Hours',  'TRAFFIC LANE' : 'No Stopping - Traffic Lane',  'TUNNEL/ELEVATED/ROADWAY' : 'No Stopping in Tunnel or on Elevated Highway',  'UNALTERED COMM VEH-NME/ADDRESS' : 'Commercial Plates on Unaltered Vehicle',  'UNALTERED COMM VEHICLE' : 'Commercial Plates on Unaltered Vehicle',  'UNAUTHORIZED BUS LAYOVER' : 'Bus Layover in Unauthorized Location',  'UNAUTHORIZED PASSENGER PICK-UP' : 'Unauthorized Passenger Pick-Up',  'VACANT LOT' : 'No Parking - Vacant Lot',  'VEH-SALE/WSHNG/RPRNG/DRIVEWAY' : 'No Parking on Street to Wash or Repair Vehicle',  'VEHICLE FOR SALE(DEALERS ONLY)' : 'No Parking on Street to Display Vehicle for Sale',  'VIN OBSCURED' : 'Vehicle Identification Number Obscured',  'WASH/REPAIR VEHCL-REPAIR ONLY' : 'No Parking on Street to Wash or Repair Vehicle',  'WRONG WAY' : 'No Parking Opposite Street Direction'}

        # only data we're looking for
        opacv_desired_keys = ['amount_due', 'payment_amount', 'violation']

        # add violation if it's missing
        for record in opacv_data:
            if record.get('violation') is None:
                record['violation'] = "No Violation Description Available"
            else:
                record['violation'] = opacv_humanized_names[record['violation']]

            combined_violations[record['summons_number']] = { key: record.get(key) for key in opacv_desired_keys }

        # collect summons numbers to use for excluding duplicates later
        opacv_summons_numbers  = list(combined_violations.keys())



        # Grab data from each of the fiscal year violation datasets
        #

        # collect the data in a list
        fy_endpoints = ['https://data.cityofnewyork.us/resource/j7ig-zgkq.json', 'https://data.cityofnewyork.us/resource/aagd-wyjz.json', 'https://data.cityofnewyork.us/resource/avxe-2nrn.json', 'https://data.cityofnewyork.us/resource/ati4-9cgt.json', 'https://data.cityofnewyork.us/resource/qpyv-8eyi.json']

        # humanized names for violations
        fy_humanized_names = {'01-No Intercity Pmt Displ': 'Failure to Display Bus Permit',  '02-No operator N/A/PH': 'Failure to Display Operator Information',  '03-Unauth passenger pick-up': 'Unauthorized Passenger Pick-Up',  '04-Downtown Bus Area, 3 Hr Lim': 'Bus Parking in Lower Manhattan - Exceeded 3-Hour limit',  '04A-Downtown Bus Area, Non-Bus': 'Bus Parking in Lower Manhattan - Non-Bus',  '04B-Downtown Bus Area, No Prmt': 'Bus Parking in Lower Manhattan - No Permit',  '06-Nighttime PKG (Trailer)': 'Overnight Parking of Tractor Trailer',  '08-Engine Idling': 'Idling',  '09-Blocking the Box': 'Obstructing Traffic or Intersection',  '10-No Stopping': 'No Stopping or Standing Except for Passenger Pick-Up',  '11-No Stand (exc hotel load)': 'No Parking - Except Hotel Loading',  '12-No Stand (snow emergency)': 'No Standing - Snow Emergency',  '13-No Stand (taxi stand)': 'No Standing - Taxi Stand',  '14-No Standing': 'No Standing - Day/Time Limits',  '16-No Std (Com Veh) Com Plate': 'No Standing - Except Truck Loading/Unloading',  '16A-No Std (Com Veh) Non-COM': 'No Standing - Except Truck Loading/Unloading',  '17-No Stand (exc auth veh)': 'No Parking - Except Authorized Vehicles',  '18-No Stand (bus lane)': 'No Standing - Bus Lane',  '19-No Stand (bus stop)': 'No Standing - Bus Stop',  '20-No Parking (Com Plate)': 'No Parking - Day/Time Limits',  '20A-No Parking (Non-COM)': 'No Parking - Day/Time Limits',  '21-No Parking (street clean)': 'No Parking - Street Cleaning',  '22-No Parking (exc hotel load)': 'No Parking - Except Hotel Loading',  '23-No Parking (taxi stand)': 'No Parking - Taxi Stand',  '24-No Parking (exc auth veh)': 'No Parking - Except Authorized Vehicles',  '25-No Stand (commutr van stop)': 'No Standing - Commuter Van Stop',  '26-No Stnd (for-hire veh only)': 'No Standing - For Hire Vehicle Stop',  '27-No Parking (exc handicap)': 'No Parking - Except Disability Permit',  '28-O/T STD,DPL/Con,30 Mn,D Dec': 'Overtime Standing - Diplomat',  '29-Altered Intercity bus pmt': 'Altered Intercity Bus Permit',  '30-No stopping/standing': 'No Stopping/Standing',  '31-No Stand (Com. Mtr. Zone)': 'No Standing - Commercial Meter Zone',  '32-Overtime PKG-Missing Meter': 'Overtime Parking at Missing or Broken Meter',  '32A Overtime PKG-Broken Meter': 'Overtime Parking at Missing or Broken Meter',  '33-Feeding Meter': 'Feeding Meter',  '35-Selling/Offer Merchandise': 'Selling or Offering Merchandise From Metered Parking',  '37-Expired Muni Meter': 'Expired Meter', '37-Expired Parking Meter': 'Expired Meter', '38-Failure to Display Muni Rec': 'Failure to Display Meter Receipt', '38-Failure to Dsplay Meter Rec': 'Failure to Display Meter Receipt', '39-Overtime PKG-Time Limt Post': 'Overtime Parking - Time Limit Posted',  '40-Fire Hydrant': 'Fire Hydrant',  '42-Exp. Muni-Mtr (Com. Mtr. Z)': 'Expired Meter - Commercial Meter Zone', '42-Exp Meter (Com Zone)': 'Expired Meter - Commercial Meter Zone', '43-Exp. Mtr. (Com. Mtr. Zone)': 'Expired Meter - Commercial Meter Zone',  '44-Exc Limit (Com. Mtr. Zone)': 'Overtime Parking - Commercial Meter Zone',  '45-Traffic Lane': 'No Stopping - Traffic Lane',  '46-Double Parking (Com Plate)': 'Double Parking',  '46A-Double Parking (Non-COM)': 'Double Parking',  '46B-Double Parking (Com-100Ft)': 'Double Parking - Within 100 ft. of Loading Zone',  '47-Double PKG-Midtown': 'Double Parking - Midtown Commercial Zone',  '47A-Angle PKG - Midtown': 'Double Parking - Angle Parking',  '48-Bike Lane': 'Blocking Bike Lane',  '49-Excavation (obstruct traff)': 'No Stopping - Adjacent to Street Construction',  '50-Crosswalk': 'Blocking Crosswalk',  '51-Sidewalk': 'Parked on Sidewalk',  '52-Intersection': 'No Stopping - Intersection',  '53-Safety Zone': 'No Standing - Safety Zone',  '55-Tunnel/Elevated Roadway': 'No Stopping in Tunnel or on Elevated Highway',  '56-Divided Highway': 'No Stopping - Divided Highway',  '57-Blue Zone': 'No Parking - Blue Zone',  '58-Marginal Street/Water Front': 'No Parking on Marginal Street or Waterfront',  '59-Angle PKG-Commer. Vehicle': 'No Angle Parking',  '60-Angle Parking': 'No Angle Parking',  '61-Wrong Way': 'No Parking Opposite Street Direction',  '62-Beyond Marked Space': 'No Parking Beyond Marked Space',  '63-Nighttime STD/PKG in a Park': 'No Nighttime Standing or Parking in a Park',  '64-No STD Ex Con/DPL, D/S Dec': 'No Standing - Consul or Diplomat',  '65-O/T STD,Dpl/Con,30 Mn,D/S': 'Overtime Standing - Consul or Diplomat Over 30 Minutes',  '66-Detached Trailer': 'Detached Trailer',  '67-Blocking Ped. Ramp': 'Blocking Pedestrian Ramp',  '68-Not Pkg. Comp. w Psted Sign': 'Non-Compliance with Posted Sign',  '69-Failure to Disp Muni Recpt': 'Failure to Display Meter Receipt',  '69-Fail to Dsp Prking Mtr Rcpt': 'Failure to Display Meter Receipt', '70-Reg. Sticker Missing (NYS)': 'Registration Sticker Expired or Missing',  '70A-Reg. Sticker Expired (NYS)': 'Registration Sticker Expired or Missing',  '70B-Impropr Dsply of Reg (NYS)': 'Improper Display of Registration',  '71-Insp. Sticker Missing (NYS': 'Inspection Sticker Expired or Missing',  '71A-Insp Sticker Expired (NYS)': 'Inspection Sticker Expired or Missing',  '71B-Improp Safety Stkr (NYS)': 'Improper Safety Sticker',  '72-Insp Stkr Mutilated': 'Inspection Sticker Mutilated or Counterfeit',  '72A-Insp Stkr Counterfeit': 'Inspection Sticker Mutilated or Counterfeit',  '73-Reg Stkr Mutilated': 'Registration Sticker Mutilated or Counterfeit',  '73A-Reg Stkr Counterfeit': 'Registration Sticker Mutilated or Counterfeit',  '74-Missing Display Plate': 'Front or Back Plate Missing',  '74A-Improperly Displayed Plate': 'Improperly Displayed Plate',  '74B-Covered Plate': 'Covered Plate',  '75-No Match-Plate/Reg. Sticker': 'Plate and Sticker Do Not Match',  '77-Parked Bus (exc desig area)': 'Bus Parking Outside of Designated Area',  '78-Nighttime PKG on Res Street': 'Nighttime Parking on Residential Street - Commercial Vehicle',  '79-Bus Layover': 'Bus Layover in Unauthorized Location',  '80-Missing Equipment (specify)': 'Missing Required Equipment',  '81-No STD Ex C,A&D Dec, 30 Mn': 'No Standing - Except Diplomat',  '82-Unaltered Commerc Vehicle': 'Commercial Plates on Unaltered Vehicle',  '83-Improper Registration': 'Improper Registration',  '84-Platform lifts in low posit': 'Commercial Vehicle Platform Lifts in Lowered Position',  '85-Storage-3 hour Commercial': 'Street Storage of Commercial Vehicle Over 3 Hours',  '86-Midtown PKG or STD-3 hr lim': 'Midtown Parking or Standing - 3 Hour Limit',  '89-No Stand Exc Com Plate': 'No Standing - Except Trucks in Garment District',  '91-Veh for Sale (Dealer Only)': 'No Parking on Street to Display Vehicle for Sale',  '92-Washing/Repairing Vehicle': 'No Parking on Street to Wash or Repair Vehicle',  '93-Repair Flat Tire (Maj Road)': 'Replacing Flat Tire on Major Roadway',  '96-Railroad Crossing': 'No Stopping - Railroad Crossing',  '98-Obstructing Driveway': 'Obstructing Driveway',  'BUS LANE VIOLATION': 'Bus Lane Violation',  'FAILURE TO STOP AT RED LIGHT': 'Failure to Stop at Red Light',  'Field Release Agreement': 'Field Release Agreement',  'PHTO SCHOOL ZN SPEED VIOLATION': 'School Zone Speed Camera Violation'}

        # only data we're looking for
        fy_desired_keys = ['violation']

        # iterate through the endpoints
        for endpoint in fy_endpoints:
            query_string = '?$limit={}&$$app_token={}&plate_id={}&registration_state={}'.format(limit, token, plate, state)
            response     = requests.get(endpoint + query_string)
            data         = response.json()

            self.logger.debug('endpoint: %s', endpoint)
            self.logger.debug('fy_response: %s', data)

            for record in data:
                if record.get('violation_description') is None:
                    record['violation'] = "No Violation Description Available"
                else:
                    if fy_humanized_names.get(record['violation_description']):
                        record['violation'] = fy_humanized_names.get(record['violation_description'])
                    else:
                        record['violation'] = re.sub('[0-9]*-', '', record['violation_description'])

                # structure response and only use the data we need
                new_data = { key: record.get(key) for key in fy_desired_keys }

                if combined_violations.get(record['summons_number']) is None:
                    combined_violations[record['summons_number']] = new_data
                else:
                    # Merge records together, treating fiscal year data as authoritative.
                    combined_violations[record['summons_number']] = {**combined_violations.get(record['summons_number']), **new_data}



        # Marshal all ticket data into form.
        tickets  = Counter([v['violation'] for k,v in combined_violations.items() if v.get('violation')]).most_common()
        result   = {'plate': plate, 'state': state, 'violations': [{'name':k.title(),'count':v} for k,v in tickets]}

        self.logger.debug('violations sorted: %s', result)



        # See if we've seen this vehicle before.
        previous_lookup = conn.execute(""" select num_tickets, created_at from plate_lookups where plate = %s and state = %s and count_towards_frequency = %s ORDER BY created_at DESC LIMIT 1""", (plate, state, True))
        # Turn data into list of dicts with attribute keys
        previous_data   = [dict(zip(tuple (previous_lookup.keys()), i)) for i in previous_lookup.cursor]

        # if we have a previous lookup, add it to the return data.
        if previous_data:
          result['previous_result'] = previous_data[0]

          self.logger.debug('we have previous data: %s', previous_data[0])


        # Find the number of times we have seen this vehicle before.
        current_frequency = conn.execute(""" select count(*) as lookup_frequency from plate_lookups where plate = %s and state = %s and count_towards_frequency = %s """, (plate, state, True)).fetchone()[0]

        # Default to counting everything.
        count_towards_frequency = 1

        # Calculate the number of violations.
        total_violations = len(combined_violations)

        # If this came from message, add it to the plate_lookups table.
        if message_type and message_id and created_at:
            # Insert plate lookup
            insert_lookup = conn.execute(""" insert into plate_lookups (plate, state, observed, message_id, lookup_source, created_at, twitter_handle, count_towards_frequency, num_tickets) values (%s, %s, NULL, %s, %s, %s, %s, %s, %s) """, (plate, state, message_id, message_type, created_at, username, count_towards_frequency, total_violations))

            # Iterate through included campaigns to tie lookup to each
            if args.get('included_campaigns'):
                result['included_campaigns'] = []

                for campaign in args['included_campaigns']:
                    # insert join record for campaign lookup
                    conn.execute(""" insert into campaigns_plate_lookups (campaign_id, plate_lookup_id) values (%s, %s) """, (campaign[0], insert_lookup.lastrowid))

                    # get new total for tickets
                    campaign_tickets_query_string = """
                      select count(id) as campaign_vehicles, ifnull(sum(num_tickets), 0) as campaign_tickets
                            from plate_lookups t1
                           where id in
                               (select plate_lookup_id
                                  from campaigns_plate_lookups
                                where campaign_id = %s)
                             and t1.created_at =
                               (select MAX(t2.created_at)
                                  from plate_lookups t2
                                  join campaigns_plate_lookups cpl
                                    on t2.id = cpl.plate_lookup_id
                                 where t2.plate = t1.plate
                                   and t2.state = t1.state
                                   and count_towards_frequency = 1
                                   and t2.id = cpl.plate_lookup_id);
                    """

                    campaign_tickets_result = conn.execute(campaign_tickets_query_string.replace('\n', ''), (campaign[0])).fetchone()

                    # return data
                    # result['included_campaigns'].append((campaign[1], int(campaign_tickets), int(num_vehicles)))
                    result['included_campaigns'].append({'campaign_hashtag': campaign[1], 'campaign_tickets': int(campaign_tickets_result[1]), 'campaign_vehicles': int(campaign_tickets_result[0])})



        # how many times have we searched for this plate from a tweet
        result['frequency'] = current_frequency + 1

        self.logger.debug('returned_result: %s', result)

        # Close the connection.
        conn.close()

        return result


    def print_daily_summary(self):
        # Open connection.
        conn = self.engine.connect()

        utc           = pytz.timezone('UTC')
        eastern       = pytz.timezone('US/Eastern')

        today         = datetime.now(eastern).date()

        midnight_yesterday = (eastern.localize(datetime.combine(today, time.min)) - timedelta(days=1)).astimezone(utc)
        end_of_yesterday   = (eastern.localize(datetime.combine(today, time.min)) - timedelta(seconds=1)).astimezone(utc)

        query_string = """
            select count(t1.id) as lookups,
                   ifnull(sum(num_tickets), 0) as total_tickets,
                   count(case when num_tickets = 0 then 1 end) as empty_lookups
              from plate_lookups t1
             where count_towards_frequency = 1
               and t1.created_at =
                 (select MAX(t2.created_at)
                    from plate_lookups t2
                   where t2.plate = t1.plate
                     and t2.state = t1.state
                     and created_at between %s
                     and %s);
        """

        query = conn.execute(query_string.replace('\n', ''), (midnight_yesterday.strftime('%Y-%m-%d %H:%M:%S'), end_of_yesterday.strftime('%Y-%m-%d %H:%M:%S'))).fetchone()

        num_lookups   = query[0]
        num_tickets   = query[1]
        empty_lookups = query[2]

        if num_lookups > 0:
            summary_string = 'On {}, users requested {} {}. {} received {} {}. {} {} returned no tickets.'.format(midnight_yesterday.strftime('%A, %B %-d, %Y'), num_lookups, 'lookup' if num_lookups == 1 else 'lookups', 'That vehicle has' if num_lookups == 1 else 'Collectively, those vehicles have', "{:,}".format(num_tickets), 'ticket' if num_tickets == 1 else 'tickets', empty_lookups, 'lookup' if empty_lookups == 1 else 'lookups')

            self.is_production() and self.api.update_status(summary_string)

        # Close connection.
        conn.close()


    def process_response_message(self, message, response_args):

        self.logger.info('\n')
        self.logger.info("Calling process_response_message")

        # Print args
        self.logger.info('args:')
        self.logger.info('message: %s', message)
        self.logger.info('response_args: %s', response_args)


        # Grab string parts
        string_parts = response_args['string_parts']
        self.logger.debug('string_parts: %s', string_parts)

        plate_tuples = [match.split(':') for match in re.findall(r'(\b[a-zA-Z9]{2}:[a-zA-Z0-9]+\b|\b[a-zA-Z0-9]+:[a-zA-Z9]{2}\b)', ' '.join(string_parts)) if all(substr not in match.lower() for substr in ['://', 'state:', 'plate:'])]
        self.logger.debug('plate_tuples: %s', plate_tuples)

        potential_vehicles = self.infer_plate_and_state_data(plate_tuples)
        self.logger.debug('potential_vehicles: %s', potential_vehicles)


        # Grab legacy string parts
        legacy_string_parts = response_args['legacy_string_parts']
        self.logger.debug('legacy_string_parts: %s', legacy_string_parts)

        legacy_plate_data = dict([[piece.strip() for piece in match.split(':')] for match in [part.lower() for part in legacy_string_parts if ('state:' in part.lower() or 'plate:' in part.lower())]])
        if legacy_plate_data:
            if self.detect_state(legacy_plate_data.get('state')):
                legacy_plate_data['valid_plate'] = True
            else:
                legacy_plate_data['valid_plate'] = False

            potential_vehicles.append(legacy_plate_data)

        self.logger.debug('potential_vehicles: %s', potential_vehicles)


        # Grab user info
        username =  '@' + response_args['username']
        self.logger.debug('username: %s', username)

        mentioned_users = response_args['mentioned_users'] if 'mentioned_users' in response_args else []
        self.logger.debug('mentioned_users: %s', mentioned_users)


        # Grab tweet details for reply.
        message_id = response_args['id']
        self.logger.debug("message id: %s", message_id)

        message_created_at = response_args['created_at']
        self.logger.debug('message created at: %s', message_created_at)

        message_type = 'direct_message' if hasattr(message, 'direct_message') else 'status'
        self.logger.debug('message_type: %s', message_type)


        # Collect response parts here.
        response_parts    = []
        successful_lookup = False


        # Wrap in try/catch block
        try:
            # Split plate and state strings into key/value pairs.
            query_info = {}

            query_info['created_at']         = message_created_at
            query_info['message_id']         = message_id
            query_info['message_type']       = message_type
            query_info['username']           = username
            query_info['included_campaigns'] = self.find_campaign_hashtags(string_parts)

            self.logger.debug("lookup info: %s", query_info)

            # for each vehicle, we need to determine if the supplied information amounts to a valid plate
            # then we need to look up each valid plate
            # then we need to respond in a single thread in order with the responses

            for potential_vehicle in potential_vehicles:

                if potential_vehicle.get('valid_plate'):

                    query_info['plate'] = potential_vehicle.get('plate')
                    query_info['state'] = potential_vehicle.get('state')

                    # Do the real work!
                    result = self.perform_queries(query_info)

                    # Record successful lookup.
                    successful_lookup = True

                    # If query returns, format output.
                    if any(result['violations']):

                        response_parts.append(self.form_successful_response_parts(result, username))
                        # [[campaign_stuff], tickets_0, tickets_1, etc.]

                    else:
                        # Let user know we didn't find anything.
                        # sorry_message = "{} Sorry, I couldn't find any tickets for that plate.".format(username)
                        response_parts.append(["{} Sorry, I couldn't find any tickets for {}:{}.".format(username, potential_vehicle.get('state').upper(), potential_vehicle.get('plate').upper())])

                else:

                    # Legacy data where state is not a valid abbreviation.
                    if potential_vehicle.get('state'):
                        self.logger.debug("We have a state, but it's invalid.")

                        response_parts.append(["{} The state should be two characters, but you supplied '{}'. Please try again.".format(username, potential_vehicle.get('state'))])

                    # '<state>:<plate>' format, but no valid state could be detected.
                    elif potential_vehicle.get('original_string'):
                        self.logger.debug("We don't have a state, but we have an attempted lookup with the new format.")

                        response_parts.append(["{} Sorry, a plate and state could not be inferred from {}.".format(username, potential_vehicle.get('original_string'))])

                    # If we have a plate, but no state.
                    elif potential_vehicle.get('plate'):
                        self.logger.debug("We have a plate, but no state")

                        response_parts.append(["{} Sorry, the state appears to be blank.\n\nJust a reminder, the format is <state|province|territory>:<plate>, e.g. NY:abc1234".format(username, query_info['state'])])


            # If we don't look up a single plate successfully,
            # figure out how we can help the user.
            if not successful_lookup:

                self.logger.debug('The data seems to be in the wrong format.')

                state_regex    = r'^(99|AB|AK|AL|AR|AZ|BC|CA|CO|CT|DC|DE|DP|FL|FM|FO|GA|GU|GV|HI|IA|ID|IL|IN|KS|KY|LA|MA|MB|MD|ME|MI|MN|MO|MP|MS|MT|MX|NB|NC|ND|NE|NF|NH|NJ|NM|NS|NT|NV|NY|OH|OK|ON|OR|PA|PE|PR|PW|QB|RI|SC|SD|SK|TN|TX|UT|VA|VI|VT|WA|WI|WV|WY|YT)$'
                numbers_regex  = r'[0-9]{4}'

                state_pattern  = re.compile(state_regex)
                number_pattern = re.compile(numbers_regex)

                state_matches  = [state_pattern.search(s.upper()) != None for s in string_parts]
                number_matches = [number_pattern.search(s.upper()) != None for s in list(filter(lambda part: re.sub(r'\.|@', '', part.lower()) not in set(mentioned_users), string_parts))]

                # We have what appears to be a plate and a state abbreviation.
                if all([any(state_matches), any(number_matches)]):
                    self.logger.debug('There is both plate and state information in this message.')

                    # Let user know plate format
                    response_parts.append(["{} I’d be happy to look that up for you!\n\nJust a reminder, the format is <state|province|territory>:<plate>, e.g. NY:abc1234".format(username)])

                # Maybe we have plate or state. Let's find out.
                else:
                    self.logger.debug('The tweet is missing either state or plate or both.')

                    state_regex_minus_words   = r'^(99|AB|AK|AL|AR|AZ|BC|CA|CO|CT|DC|DE|DP|FL|FM|FO|GA|GU|GV|IA|ID|IL|KS|KY|LA|MA|MB|MD|MH|MI|MN|MO|MP|MS|MT|MX|NB|NC|ND|NE|NF|NH|NJ|NM|NS|NT|NV|NY|PA|PE|PR|PW|QB|RI|SC|SD|SK|STATE|TN|TX|UT|VA|VI|VT|WA|WI|WV|WY|YT)$'
                    state_minus_words_pattern = re.compile(state_regex_minus_words)

                    state_minus_words_matches = [state_minus_words_pattern.search(s.upper()) != None for s in string_parts]

                    number_matches = [number_pattern.search(s.upper()) != None for s in list(filter(lambda part: re.sub(r'\.|@', '', part.lower()) not in set(mentioned_users), string_parts))]

                    # We have either plate or state.
                    if any(state_minus_words_matches) or any(number_matches):

                        # Let user know plate format
                        response_parts.append(["{} I think you're trying to look up a plate, but can't be sure.\n\nJust a reminder, the format is <state|province|territory>:<plate>, e.g. NY:abc1234".format(username)])

                    # We have neither plate nor state. Do nothing.
                    else:
                        self.logger.debug('ignoring message since no plate or state information to respond to.')


        except Exception as e:
            # Log error

            response_parts.append(["{} Sorry, I encountered an error. Tagging @bdhowald.".format(username)])

            # if message_type == 'direct_message':
            #     is_production and self.api.send_direct_message(screen_name = username, text = response_message)
            # else:
            #     is_production and self.api.update_status(response_message, message_id)

            logger.error('Missing necessary information to continue')
            logger.error(e)
            logger.error(str(e))
            logger.error(e.args)
            logging.exception("stack trace")


        # Respond to user
        if message_type == 'direct_message':

            self.logger.debug('responding as direct message')

            combined_message = self.recursively_process_direct_messages(response_parts)

            self.logger.debug('combined_message: %s', combined_message)

            self.is_production() and self.api.send_direct_message(screen_name = username, text = combined_message)

        else:
            # If we have at least one successful lookup, favorite the status
            if successful_lookup:

                # Favorite every look-up from a status
                try:
                    self.is_production() and self.api.create_favorite(message_id)

                # But don't crash on error
                except tweepy.error.TweepError as te:
                    # There's no easy way to know if this status has already been favorited
                    pass

            self.logger.debug('responding as status update')

            self.recursively_process_status_updates(response_parts, message_id)


    def recursively_process_direct_messages(self, response_parts):

        return_message = []

        # Iterate through all response parts
        for part in response_parts:
            if isinstance(part, list):
                return_message.append(self.recursively_process_direct_messages(part))
            else:
                return_message.append(part)

        return '\n'.join(return_message)


    def recursively_process_status_updates(self, response_parts, message_id):

        # Iterate through all response parts
        for part in response_parts:
            # Some may be lists themselves
            if isinstance(part, list):
                message_id = self.recursively_process_status_updates(part, message_id)
            else:
                new_message = self.is_production() and self.api.update_status(part, in_reply_to_status_id = message_id)
                message_id  = self.is_production() and new_message.id

                self.logger.debug("message_id: %s", str(message_id))

        return message_id


class MyStreamListener (tweepy.StreamListener):

    def __init__(self, tweeter):
        # Create a logger
        self.logger = logging.getLogger('hows_my_driving')

        super(MyStreamListener,self).__init__()


    def on_status(self, status):
        self.logger.debug("\n\n\non_status: %s\n\n\n", status.text)

    def on_data(self, data):
        data_dict = json.loads(data)
        self.logger.debug("\n\ndata: %s\n\n", json.dumps(data_dict, indent=4, sort_keys=True))


        if 'delete' in data_dict:
            self.logger.debug('\n\ndelete\n')
            self.logger.debug("\ndata_dict['delete']: %s\n\n", data_dict['delete'])
            # delete = data['delete']['status']

            # if self.on_delete(delete['id'], delete['user_id']) is False:
            #     return False
        elif 'event' in data_dict:
            self.logger.debug('\n\nevent\n')
            self.logger.debug("\ndata_dict['event']: %s\n\n", data_dict['event'])

            status = tweepy.Status.parse(self.api, data_dict)
            # if self.on_event(status) is False:
            #     return False
        elif 'direct_message' in data_dict:
            self.logger.debug('\n\ndirect_message\n')
            self.logger.debug("\ndata_dict['direct_message']: %s\n\n", data_dict['direct_message'])

            message = tweepy.Status.parse(self.api, data_dict)

            tweeter.initiate_reply(message)
            # if self.on_direct_message(status) is False:
            #     return False
        elif 'friends' in data_dict:
            self.logger.debug('\n\nfriends\n')
            self.logger.debug("\ndata_dict['friends']: %s\n\n", data_dict['friends'])

            # if self.on_friends(data['friends']) is False:
            #     return False
        elif 'limit' in data_dict:
            self.logger.debug('\n\nlimit\n')
            self.logger.debug("\ndata_dict['limit']: %s\n\n", data_dict['limit'])

            # if self.on_limit(data['limit']['track']) is False:
            #     return False
        elif 'disconnect' in data_dict:
            self.logger.debug('\n\ndisconnect\n')
            self.logger.debug("\ndata_dict['disconnect']: %s\n\n", data_dict['disconnect'])

            # if self.on_disconnect(data['disconnect']) is False:
            #     return False
        elif 'warning' in data_dict:
            self.logger.debug('\n\nwarning\n')
            self.logger.debug("\ndata_dict['warning']: %s\n\n", data_dict['warning'])

            # if self.on_warning(data['warning']) is False:
            #     return False
        elif 'retweeted_status' in data_dict:
            self.logger.debug("\n\nis_retweet: %s\n", 'retweeted_status' in data_dict)
            self.logger.debug("\ndata_dict['retweeted_status']: %s\n\n", data_dict['retweeted_status'])

        elif 'in_reply_to_status_id' in data_dict:
            self.logger.debug('\n\nin_reply_to_status_id\n')
            self.logger.debug("\ndata_dict['in_reply_to_status_id']: %s\n\n", data_dict['in_reply_to_status_id'])

            status = tweepy.Status.parse(self.api, data_dict)

            tweeter.initiate_reply(status)
            # if self.on_status(status) is False:
            #     return False
        else:
            self.logger.error("Unknown message type: " + str(data))



    def on_event(self, status):
        self.logger.debug("on_event: %s", status)

    def on_error(self, status):
        self.logger.debug("on_error: %s", status)
        self.logger.debug("self: %s", self)

    def on_direct_message(self, status):
        self.logger.debug("on_direct_message: %s", status)


if __name__ == '__main__':
    if sys.argv[-1] == 'print_daily_summary':
        tweeter = TrafficViolationsTweeter()
        tweeter.print_daily_summary()
    else:
        tweeter = TrafficViolationsTweeter()
        tweeter.run()
        app.run()