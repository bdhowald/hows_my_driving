import logging
import pytz
import re
import pdb
import requests
import requests_futures.sessions


from collections import Counter
from datetime import datetime, timezone, time, timedelta
from pprint import pprint
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from reply_argument_builder import ReplyArgumentBuilder

class TrafficViolationsAggregator:

    MAX_TWITTER_STATUS_LENGTH = 280

    PRECINCTS = {
      'manhattan': {
        'manhattan south': [1,5,6,7,9,10,13,14,17,18],
        'manhattan north': [19,20,22,23,24,25,26,28,30,32,33,34]
      },
      'bronx': {
        'bronx'          : [40,41,42,43,44,45,46,47,48,49,50,52]
      },
      'brooklyn': {
        'brooklyn south' : [60,61,62,63,66,67,68,69,70,71,72,76,78],
        'brooklyn north' : [73,75,77,79,81,83,84,88,90,94]
      },
      'queens': {
        'queens south'   : [100,101,102,103,105,106,107,113],
        'queens north'   : [104,108,109,110,111,112,114,115]
      },
      'staten island': {
        'staten island'  : [120,121,122,123]
      }
    }

    PRECINCTS_BY_BORO = {borough: [precinct for grouping in [precinct_list for bureau, precinct_list in regions.items()] for precinct in grouping] for borough,regions in PRECINCTS.items()}

    COUNTY_CODES = {
      'bronx'        : ['BRONX', 'BX', 'PBX'],
      'brooklyn'     : ['BK', 'BROOK', 'K', 'KINGS', 'PK'],
      'manhattan'    : ['MAH', 'MANHA', 'MN', 'NEUY', 'NY', 'PNY'],
      'queens'       : ['Q', 'QN', 'QNS', 'QUEEN'],
      'staten island': ['R', 'RICH', 'ST'],
    }

    # humanized names for violations
    OPACV_HUMANIZED_NAMES = {'': 'No Description Given',  'ALTERING INTERCITY BUS PERMIT' : 'Altered Intercity Bus Permit',  'ANGLE PARKING' : 'No Angle Parking',  'ANGLE PARKING-COMM VEHICLE' : 'No Angle Parking',  'BEYOND MARKED SPACE' : 'No Parking Beyond Marked Space',  'BIKE LANE' : 'Blocking Bike Lane',  'BLUE ZONE' : 'No Parking - Blue Zone',  'BUS LANE VIOLATION' : 'Bus Lane Violation',  'BUS PARKING IN LOWER MANHATTAN' : 'Bus Parking in Lower Manhattan',  'COMML PLATES-UNALTERED VEHICLE' : 'Commercial Plates on Unaltered Vehicle',  'CROSSWALK' : 'Blocking Crosswalk',  'DETACHED TRAILER' : 'Detached Trailer',  'DIVIDED HIGHWAY' : 'No Stopping - Divided Highway',  'DOUBLE PARKING' : 'Double Parking',  'DOUBLE PARKING-MIDTOWN COMML' : 'Double Parking - Midtown Commercial Zone',  'ELEVATED/DIVIDED HIGHWAY/TUNNL' : 'No Stopping in Tunnel or on Elevated Highway',  'EXCAVATION-VEHICLE OBSTR TRAFF' : 'No Stopping - Adjacent to Street Construction',  'EXPIRED METER' : 'Expired Meter',  'EXPIRED METER-COMM METER ZONE' : 'Expired Meter - Commercial Meter Zone',  'EXPIRED MUNI METER' : 'Expired Meter',  'EXPIRED MUNI MTR-COMM MTR ZN' : 'Expired Meter - Commercial Meter Zone',  'FAIL TO DISP. MUNI METER RECPT' : 'Failure to Display Meter Receipt',  'FAIL TO DSPLY MUNI METER RECPT' : 'Failure to Display Meter Receipt',  'FAILURE TO DISPLAY BUS PERMIT' : 'Failure to Display Bus Permit',  'FAILURE TO STOP AT RED LIGHT' : 'Failure to Stop at Red Light',  'FEEDING METER' : 'Feeding Meter',  'FIRE HYDRANT' : 'Fire Hydrant',  'FRONT OR BACK PLATE MISSING' : 'Front or Back Plate Missing',  'IDLING' : 'Idling',  'IMPROPER REGISTRATION' : 'Improper Registration',  'INSP STICKER-MUTILATED/C\'FEIT' : 'Inspection Sticker Mutilated or Counterfeit',  'INSP. STICKER-EXPIRED/MISSING' : 'Inspection Sticker Expired or Missing',  'INTERSECTION' : 'No Stopping - Intersection',  'MARGINAL STREET/WATER FRONT' : 'No Parking on Marginal Street or Waterfront',  'MIDTOWN PKG OR STD-3HR LIMIT' : 'Midtown Parking or Standing - 3 Hour Limit',  'MISCELLANEOUS' : 'Miscellaneous',  'MISSING EQUIPMENT' : 'Missing Required Equipment',  'NGHT PKG ON RESID STR-COMM VEH' : 'No Nighttime Parking on Residential Street - Commercial Vehicle',  'NIGHTTIME STD/ PKG IN A PARK' : 'No Nighttime Standing or Parking in a Park',  'NO MATCH-PLATE/STICKER' : 'Plate and Sticker Do Not Match',  'NO OPERATOR NAM/ADD/PH DISPLAY' : 'Failure to Display Operator Information',  'NO PARKING-DAY/TIME LIMITS' : 'No Parking - Day/Time Limits',  'NO PARKING-EXC. AUTH. VEHICLE' : 'No Parking - Except Authorized Vehicles',  'NO PARKING-EXC. HNDICAP PERMIT' : 'No Parking - Except Disability Permit',  'NO PARKING-EXC. HOTEL LOADING' : 'No Parking - Except Hotel Loading',  'NO PARKING-STREET CLEANING' : 'No Parking - Street Cleaning',  'NO PARKING-TAXI STAND' : 'No Parking - Taxi Stand',  'NO STANDING EXCP D/S' : 'No Standing - Except Department of State',  'NO STANDING EXCP DP' : 'No Standing - Except Diplomat',  'NO STANDING-BUS LANE' : 'No Standing - Bus Lane',  'NO STANDING-BUS STOP' : 'No Standing - Bus Stop',  'NO STANDING-COMM METER ZONE' : 'No Standing - Commercial Meter Zone',  'NO STANDING-COMMUTER VAN STOP' : 'No Standing - Commuter Van Stop',  'NO STANDING-DAY/TIME LIMITS' : 'No Standing - Day/Time Limits',  'NO STANDING-EXC. AUTH. VEHICLE' : 'No Standing - Except Authorized Vehicle',  'NO STANDING-EXC. TRUCK LOADING' : 'No Standing - Except Truck Loading',  'NO STANDING-FOR HIRE VEH STOP' : 'No Standing - For Hire Vehicle Stop',  'NO STANDING-HOTEL LOADING' : 'No Standing - Hotel Loading',  'NO STANDING-OFF-STREET LOT' : 'No Standing - Off-Street Lot',  'NO STANDING-SNOW EMERGENCY' : 'No Standing - Snow Emergency',  'NO STANDING-TAXI STAND' : 'No Standing - Taxi Stand',  'NO STD(EXC TRKS/GMTDST NO-TRK)' : 'No Standing - Except Trucks in Garment District',  'NO STOP/STANDNG EXCEPT PAS P/U' : 'No Stopping or Standing Except for Passenger Pick-Up',  'NO STOPPING-DAY/TIME LIMITS' : 'No Stopping - Day/Time Limits',  'NON-COMPLIANCE W/ POSTED SIGN' : 'Non-Compliance with Posted Sign',  'OBSTRUCTING DRIVEWAY' : 'Obstructing Driveway',  'OBSTRUCTING TRAFFIC/INTERSECT' : 'Obstructing Traffic or Intersection',  'OT PARKING-MISSING/BROKEN METR' : 'Overtime Parking at Missing or Broken Meter',  'OTHER' : 'Other',  'OVERNIGHT TRACTOR TRAILER PKG' : 'Overnight Parking of Tractor Trailer',  'OVERTIME PKG-TIME LIMIT POSTED' : 'Overtime Parking - Time Limit Posted',  'OVERTIME STANDING DP' : 'Overtime Standing - Diplomat',  'OVERTIME STDG D/S' : 'Overtime Standing - Department of State',  'PARKED BUS-EXC. DESIG. AREA' : 'Bus Parking Outside of Designated Area',  'PEDESTRIAN RAMP' : 'Blocking Pedestrian Ramp',  'PHTO SCHOOL ZN SPEED VIOLATION' : 'School Zone Speed Camera Violation',  'PKG IN EXC. OF LIM-COMM MTR ZN' : 'Parking in Excess of Limits - Commercial Meter Zone',  'PLTFRM LFTS LWRD POS COMM VEH' : 'Commercial Vehicle Platform Lifts in Lowered Position',  'RAILROAD CROSSING' : 'No Stopping - Railroad Crossing',  'REG STICKER-MUTILATED/C\'FEIT' : 'Registration Sticker Mutilated or Counterfeit',  'REG. STICKER-EXPIRED/MISSING' : 'Registration Sticker Expired or Missing',  'REMOVE/REPLACE FLAT TIRE' : 'Replacing Flat Tire on Major Roadway',  'SAFETY ZONE' : 'No Standing - Safety Zone',  'SELLING/OFFERING MCHNDSE-METER' : 'Selling or Offering Merchandise From Metered Parking',  'SIDEWALK' : 'Parked on Sidewalk',  'STORAGE-3HR COMMERCIAL' : 'Street Storage of Commercial Vehicle Over 3 Hours',  'TRAFFIC LANE' : 'No Stopping - Traffic Lane',  'TUNNEL/ELEVATED/ROADWAY' : 'No Stopping in Tunnel or on Elevated Highway',  'UNALTERED COMM VEH-NME/ADDRESS' : 'Commercial Plates on Unaltered Vehicle',  'UNALTERED COMM VEHICLE' : 'Commercial Plates on Unaltered Vehicle',  'UNAUTHORIZED BUS LAYOVER' : 'Bus Layover in Unauthorized Location',  'UNAUTHORIZED PASSENGER PICK-UP' : 'Unauthorized Passenger Pick-Up',  'VACANT LOT' : 'No Parking - Vacant Lot',  'VEH-SALE/WSHNG/RPRNG/DRIVEWAY' : 'No Parking on Street to Wash or Repair Vehicle',  'VEHICLE FOR SALE(DEALERS ONLY)' : 'No Parking on Street to Display Vehicle for Sale',  'VIN OBSCURED' : 'Vehicle Identification Number Obscured',  'WASH/REPAIR VEHCL-REPAIR ONLY' : 'No Parking on Street to Wash or Repair Vehicle',  'WRONG WAY' : 'No Parking Opposite Street Direction'}

    # humanized names for violations
    FY_HUMANIZED_NAMES = {'01': 'Failure to Display Bus Permit',  '02': 'Failure to Display Operator Information',  '03': 'Unauthorized Passenger Pick-Up',  '04': 'Bus Parking in Lower Manhattan - Exceeded 3-Hour limit',  '04A': 'Bus Parking in Lower Manhattan - Non-Bus',  '04B': 'Bus Parking in Lower Manhattan - No Permit',  '06': 'Overnight Parking of Tractor Trailer',  '08': 'Idling',  '09': 'Obstructing Traffic or Intersection',  '10': 'No Stopping or Standing Except for Passenger Pick-Up',  '11': 'No Parking - Except Hotel Loading',  '12': 'No Standing - Snow Emergency',  '13': 'No Standing - Taxi Stand',  '14': 'No Standing - Day/Time Limits',  '16': 'No Standing - Except Truck Loading/Unloading',  '16A': 'No Standing - Except Truck Loading/Unloading',  '17': 'No Parking - Except Authorized Vehicles',  '18': 'No Standing - Bus Lane',  '19': 'No Standing - Bus Stop',  '20': 'No Parking - Day/Time Limits',  '20A': 'No Parking - Day/Time Limits',  '21': 'No Parking - Street Cleaning',  '22': 'No Parking - Except Hotel Loading',  '23': 'No Parking - Taxi Stand',  '24': 'No Parking - Except Authorized Vehicles',  '25': 'No Standing - Commuter Van Stop',  '26': 'No Standing - For Hire Vehicle Stop',  '27': 'No Parking - Except Disability Permit',  '28': 'Overtime Standing - Diplomat',  '29': 'Altered Intercity Bus Permit',  '30': 'No Stopping/Standing',  '31': 'No Standing - Commercial Meter Zone',  '32': 'Overtime Parking at Missing or Broken Meter',  '32A': 'Overtime Parking at Missing or Broken Meter',  '33': 'Feeding Meter',  '35': 'Selling or Offering Merchandise From Metered Parking',  '37': 'Expired Meter',  '37': 'Expired Meter',  '38': 'Failure to Display Meter Receipt',  '38': 'Failure to Display Meter Receipt',  '39': 'Overtime Parking - Time Limit Posted',  '40': 'Fire Hydrant',  '42': 'Expired Meter - Commercial Meter Zone',  '42': 'Expired Meter - Commercial Meter Zone',  '43': 'Expired Meter - Commercial Meter Zone',  '44': 'Overtime Parking - Commercial Meter Zone',  '45': 'No Stopping - Traffic Lane',  '46': 'Double Parking',  '46A': 'Double Parking',  '46B': 'Double Parking - Within 100 ft. of Loading Zone',  '47': 'Double Parking - Midtown Commercial Zone',  '47A': 'Double Parking - Angle Parking',  '48': 'Blocking Bike Lane',  '49': 'No Stopping - Adjacent to Street Construction',  '50': 'Blocking Crosswalk',  '51': 'Parked on Sidewalk',  '52': 'No Stopping - Intersection',  '53': 'No Standing - Safety Zone',  '55': 'No Stopping in Tunnel or on Elevated Highway',  '56': 'No Stopping - Divided Highway',  '57': 'No Parking - Blue Zone',  '58': 'No Parking on Marginal Street or Waterfront',  '59': 'No Angle Parking',  '60': 'No Angle Parking',  '61': 'No Parking Opposite Street Direction',  '62': 'No Parking Beyond Marked Space',  '63': 'No Nighttime Standing or Parking in a Park',  '64': 'No Standing - Consul or Diplomat',  '65': 'Overtime Standing - Consul or Diplomat Over 30 Minutes',  '66': 'Detached Trailer',  '67': 'Blocking Pedestrian Ramp',  '68': 'Non-Compliance with Posted Sign',  '69': 'Failure to Display Meter Receipt',  '69': 'Failure to Display Meter Receipt',  '70': 'Registration Sticker Expired or Missing',  '70A': 'Registration Sticker Expired or Missing',  '70B': 'Improper Display of Registration',  '71': 'Inspection Sticker Expired or Missing',  '71A': 'Inspection Sticker Expired or Missing',  '71B': 'Improper Safety Sticker',  '72': 'Inspection Sticker Mutilated or Counterfeit',  '72A': 'Inspection Sticker Mutilated or Counterfeit',  '73': 'Registration Sticker Mutilated or Counterfeit',  '73A': 'Registration Sticker Mutilated or Counterfeit',  '74': 'Front or Back Plate Missing',  '74A': 'Improperly Displayed Plate',  '74B': 'Covered Plate',  '75': 'Plate and Sticker Do Not Match',  '77': 'Bus Parking Outside of Designated Area',  '78': 'Nighttime Parking on Residential Street - Commercial Vehicle',  '79': 'Bus Layover in Unauthorized Location',  '80': 'Missing Required Equipment',  '81': 'No Standing - Except Diplomat',  '82': 'Commercial Plates on Unaltered Vehicle',  '83': 'Improper Registration',  '84': 'Commercial Vehicle Platform Lifts in Lowered Position',  '85': 'Street Storage of Commercial Vehicle Over 3 Hours',  '86': 'Midtown Parking or Standing - 3 Hour Limit',  '89': 'No Standing - Except Trucks in Garment District',  '91': 'No Parking on Street to Display Vehicle for Sale',  '92': 'No Parking on Street to Wash or Repair Vehicle',  '93': 'Replacing Flat Tire on Major Roadway',  '96': 'No Stopping - Railroad Crossing',  '98': 'Obstructing Driveway',  '01-No Intercity Pmt Displ': 'Failure to Display Bus Permit',  '02-No operator N/A/PH': 'Failure to Display Operator Information',  '03-Unauth passenger pick-up': 'Unauthorized Passenger Pick-Up',  '04-Downtown Bus Area,3 Hr Lim': 'Bus Parking in Lower Manhattan - Exceeded 3-Hour limit',  '04A-Downtown Bus Area,Non-Bus': 'Bus Parking in Lower Manhattan - Non-Bus',  '04A-Downtown Bus Area, Non-Bus': 'Bus Parking in Lower Manhattan - Non-Bus', '04B-Downtown Bus Area,No Prmt': 'Bus Parking in Lower Manhattan - No Permit',  '06-Nighttime PKG (Trailer)': 'Overnight Parking of Tractor Trailer',  '08-Engine Idling': 'Idling',  '09-Blocking the Box': 'Obstructing Traffic or Intersection',  '10-No Stopping': 'No Stopping or Standing Except for Passenger Pick-Up',  '11-No Stand (exc hotel load)': 'No Parking - Except Hotel Loading',  '12-No Stand (snow emergency)': 'No Standing - Snow Emergency',  '13-No Stand (taxi stand)': 'No Standing - Taxi Stand',  '14-No Standing': 'No Standing - Day/Time Limits',  '16-No Std (Com Veh) Com Plate': 'No Standing - Except Truck Loading/Unloading',  '16A-No Std (Com Veh) Non-COM': 'No Standing - Except Truck Loading/Unloading',  '17-No Stand (exc auth veh)': 'No Parking - Except Authorized Vehicles',  '18-No Stand (bus lane)': 'No Standing - Bus Lane',  '19-No Stand (bus stop)': 'No Standing - Bus Stop',  '20-No Parking (Com Plate)': 'No Parking - Day/Time Limits',  '20A-No Parking (Non-COM)': 'No Parking - Day/Time Limits',  '21-No Parking (street clean)': 'No Parking - Street Cleaning',  '22-No Parking (exc hotel load)': 'No Parking - Except Hotel Loading',  '23-No Parking (taxi stand)': 'No Parking - Taxi Stand',  '24-No Parking (exc auth veh)': 'No Parking - Except Authorized Vehicles',  '25-No Stand (commutr van stop)': 'No Standing - Commuter Van Stop',  '26-No Stnd (for-hire veh only)': 'No Standing - For Hire Vehicle Stop',  '27-No Parking (exc handicap)': 'No Parking - Except Disability Permit',  '28-O/T STD,PL/Con,0 Mn, Dec': 'Overtime Standing - Diplomat',  '29-Altered Intercity bus pmt': 'Altered Intercity Bus Permit',  '30-No stopping/standing': 'No Stopping/Standing',  '31-No Stand (Com. Mtr. Zone)': 'No Standing - Commercial Meter Zone',  '32-Overtime PKG-Missing Meter': 'Overtime Parking at Missing or Broken Meter',  '32A Overtime PKG-Broken Meter': 'Overtime Parking at Missing or Broken Meter',  '33-Feeding Meter': 'Feeding Meter',  '35-Selling/Offer Merchandise': 'Selling or Offering Merchandise From Metered Parking',  '37-Expired Muni Meter': 'Expired Meter','37-Expired Parking Meter': 'Expired Meter','38-Failure to Display Muni Rec': 'Failure to Display Meter Receipt','38-Failure to Dsplay Meter Rec': 'Failure to Display Meter Receipt','39-Overtime PKG-Time Limt Post': 'Overtime Parking - Time Limit Posted',  '40-Fire Hydrant': 'Fire Hydrant',  '42-Exp. Muni-Mtr (Com. Mtr. Z)': 'Expired Meter - Commercial Meter Zone','42-Exp Meter (Com Zone)': 'Expired Meter - Commercial Meter Zone','43-Exp. Mtr. (Com. Mtr. Zone)': 'Expired Meter - Commercial Meter Zone',  '44-Exc Limit (Com. Mtr. Zone)': 'Overtime Parking - Commercial Meter Zone',  '45-Traffic Lane': 'No Stopping - Traffic Lane',  '46-Double Parking (Com Plate)': 'Double Parking',  '46A-Double Parking (Non-COM)': 'Double Parking',  '46B-Double Parking (Com-100Ft)': 'Double Parking - Within 100 ft. of Loading Zone',  '47-Double PKG-Midtown': 'Double Parking - Midtown Commercial Zone',  '47A-Angle PKG - Midtown': 'Double Parking - Angle Parking',  '48-Bike Lane': 'Blocking Bike Lane',  '49-Excavation (obstruct traff)': 'No Stopping - Adjacent to Street Construction',  '50-Crosswalk': 'Blocking Crosswalk',  '51-Sidewalk': 'Parked on Sidewalk',  '52-Intersection': 'No Stopping - Intersection',  '53-Safety Zone': 'No Standing - Safety Zone',  '55-Tunnel/Elevated Roadway': 'No Stopping in Tunnel or on Elevated Highway',  '56-Divided Highway': 'No Stopping - Divided Highway',  '57-Blue Zone': 'No Parking - Blue Zone',  '58-Marginal Street/Water Front': 'No Parking on Marginal Street or Waterfront',  '59-Angle PKG-Commer. Vehicle': 'No Angle Parking',  '60-Angle Parking': 'No Angle Parking',  '61-Wrong Way': 'No Parking Opposite Street Direction',  '62-Beyond Marked Space': 'No Parking Beyond Marked Space',  '63-Nighttime STD/PKG in a Park': 'No Nighttime Standing or Parking in a Park',  '64-No STD Ex Con/DPL,D/S Dec': 'No Standing - Consul or Diplomat',  '65-O/T STD,pl/Con,0 Mn,/S': 'Overtime Standing - Consul or Diplomat Over 30 Minutes',  '66-Detached Trailer': 'Detached Trailer',  '67-Blocking Ped. Ramp': 'Blocking Pedestrian Ramp',  '68-Not Pkg. Comp. w Psted Sign': 'Non-Compliance with Posted Sign',  '69-Failure to Disp Muni Recpt': 'Failure to Display Meter Receipt',  '69-Fail to Dsp Prking Mtr Rcpt': 'Failure to Display Meter Receipt','70-Reg. Sticker Missing (NYS)': 'Registration Sticker Expired or Missing',  '70A-Reg. Sticker Expired (NYS)': 'Registration Sticker Expired or Missing',  '70B-Impropr Dsply of Reg (NYS)': 'Improper Display of Registration',  '71-Insp. Sticker Missing (NYS': 'Inspection Sticker Expired or Missing',  '71A-Insp Sticker Expired (NYS)': 'Inspection Sticker Expired or Missing',  '71B-Improp Safety Stkr (NYS)': 'Improper Safety Sticker',  '72-Insp Stkr Mutilated': 'Inspection Sticker Mutilated or Counterfeit',  '72A-Insp Stkr Counterfeit': 'Inspection Sticker Mutilated or Counterfeit',  '73-Reg Stkr Mutilated': 'Registration Sticker Mutilated or Counterfeit',  '73A-Reg Stkr Counterfeit': 'Registration Sticker Mutilated or Counterfeit',  '74-Missing Display Plate': 'Front or Back Plate Missing',  '74A-Improperly Displayed Plate': 'Improperly Displayed Plate',  '74B-Covered Plate': 'Covered Plate',  '75-No Match-Plate/Reg. Sticker': 'Plate and Sticker Do Not Match',  '77-Parked Bus (exc desig area)': 'Bus Parking Outside of Designated Area',  '78-Nighttime PKG on Res Street': 'Nighttime Parking on Residential Street - Commercial Vehicle',  '79-Bus Layover': 'Bus Layover in Unauthorized Location',  '80-Missing Equipment (specify)': 'Missing Required Equipment',  '81-No STD Ex C,&D Dec,30 Mn': 'No Standing - Except Diplomat',  '82-Unaltered Commerc Vehicle': 'Commercial Plates on Unaltered Vehicle',  '83-Improper Registration': 'Improper Registration',  '84-Platform lifts in low posit': 'Commercial Vehicle Platform Lifts in Lowered Position',  '85-Storage-3 hour Commercial': 'Street Storage of Commercial Vehicle Over 3 Hours',  '86-Midtown PKG or STD-3 hr lim': 'Midtown Parking or Standing - 3 Hour Limit',  '89-No Stand Exc Com Plate': 'No Standing - Except Trucks in Garment District',  '91-Veh for Sale (Dealer Only)': 'No Parking on Street to Display Vehicle for Sale',  '92-Washing/Repairing Vehicle': 'No Parking on Street to Wash or Repair Vehicle',  '93-Repair Flat Tire (Maj Road)': 'Replacing Flat Tire on Major Roadway',  '96-Railroad Crossing': 'No Stopping - Railroad Crossing',  '98-Obstructing Driveway': 'Obstructing Driveway',  'BUS LANE VIOLATION': 'Bus Lane Violation',  'FAILURE TO STOP AT RED LIGHT': 'Failure to Stop at Red Light',  'Field Release Agreement': 'Field Release Agreement',  'PHTO SCHOOL ZN SPEED VIOLATION': 'School Zone Speed Camera Violation'}

    HASHTAG_PATTERN    = re.compile('[^#\w]+', re.UNICODE)
    STATE_ABBR_REGEX   = r'^(99|AB|AK|AL|AR|AZ|BC|CA|CO|CT|DC|DE|DP|FL|FM|FO|GA|GU|GV|HI|IA|ID|IL|IN|KS|KY|LA|MA|MB|MD|ME|MI|MN|MO|MP|MS|MT|MX|NB|NC|ND|NE|NF|NH|NJ|NM|NS|NT|NV|NY|OH|OK|ON|OR|PA|PE|PR|PW|QC|RI|SC|SD|SK|TN|TX|UT|VA|VI|VT|WA|WI|WV|WY|YT)$'


    def __init__(self, db_service, logger, google_api_key):
      self.db_service             = db_service
      self.logger                 = logger
      self.google_api_key         = google_api_key
      self.reply_argument_builder = ReplyArgumentBuilder



    def detect_borough(self, location_parts):

        sanitized_parts = [re.sub('\(?[ENSW]/?B\)? *', '', part) for part in location_parts]

        location_strs   = [' '.join(location_parts) + ' New York NY', ''.join(location_parts) + ' New York NY']

        # location_str = re.sub('[ENSW]B *', '', location_str)
        # lookup_string = ' '.join([location_str, 'New York NY'])

        for geo_string in location_strs:

            # try to find it in the geocodes table first.

            # Instantiate a connection.
            conn = self.db_service.get_connection()

            boro_from_geocode = conn.execute(""" select borough from geocodes where lookup_string = %s """, geo_string).fetchone()

            if boro_from_geocode:

                return [boro_from_geocode[0]]

            else:

                url           = 'https://maps.googleapis.com/maps/api/geocode/json'
                api_key       = self.google_api_key
                params        = {'address': geo_string, 'key': api_key}

                req     = requests.get(url, params=params)
                results = req.json()['results']

                if results and results[0]:
                    if results[0].get('address_components'):

                        address_components = results[0].get('address_components')
                        boros = [comp['long_name'] for comp in results[0].get('address_components') if comp.get('types') and 'sublocality_level_1' in comp['types']]

                        if boros:
                            # insert geocode
                            conn.execute(""" insert into geocodes (lookup_string, borough, geocoding_service) values (%s, %s, 'google') """, (geo_string, boros[0]))

                            # return the boro
                            return boros

            # Close the connection
            conn.close()

        return []



    def detect_campaign_hashtags(self, string_parts):
        # hashtag_pattern   = re.compile('[^#\w]+', re.UNICODE)

        # Instantiate connection.
        conn = self.db_service.get_connection()

        # Look for campaign hashtags in the message's text.
        campaigns_present = conn.execute(""" select id, hashtag from campaigns where hashtag in (%s) """ % ','.join(['%s'] * len(string_parts)), [self.HASHTAG_PATTERN.sub('', string) for string in string_parts])
        result            = [tuple(i) for i in campaigns_present.cursor]

        # Close the connection
        conn.close()

        return result



    def detect_state(self, state_input):
        # state_abbr_regex   = r'^(99|AB|AK|AL|AR|AZ|BC|CA|CO|CT|DC|DE|DP|FL|FM|FO|GA|GU|GV|HI|IA|ID|IL|IN|KS|KY|LA|MA|MB|MD|ME|MI|MN|MO|MP|MS|MT|MX|NB|NC|ND|NE|NF|NH|NJ|NM|NS|NT|NV|NY|OH|OK|ON|OR|PA|PE|PR|PW|QC|RI|SC|SD|SK|TN|TX|UT|VA|VI|VT|WA|WI|WV|WY|YT)$'
        # state_full_regex   = r'^(ALABAMA|ALASKA|ARKANSAS|ARIZONA|CALIFORNIA|COLORADO|CONNECTICUT|DELAWARE|D\.C\.|DISTRICT OF COLUMBIA|FEDERATED STATES OF MICRONESIA|FLORIDA|GEORGIA|GUAM|HAWAII|IDAHO|ILLINOIS|INDIANA|IOWA|KANSAS|KENTUCKY|LOUISIANA|MAINE|MARSHALL ISLANDS|MARYLAND|MASSACHUSETTS|MICHIGAN|MINNESTOA|MISSISSIPPI|MISSOURI|MONTANA|NEBRASKA|NEVADA|NEW HAMPSHIRE|NEW JERSEY|NEW MEXICO|NEW YORK|NORTH CAROLINA|NORTH DAKOTA|NORTHERN MARIANA ISLANDS|OHIO|OKLAHOMA|OREGON|PALAU|PENNSYLVANIA|PUERTO RICO|RHODE ISLAND|SOUTH CAROLINA|SOUTH DAKOTA|TENNESSEE|TEXAS|UTAH|VERMONT|U\.S\. VIRGIN ISLANDS|US VIRGIN ISLANDS|VIRGIN ISLANDS|VIRGINIA|WASHINGTON|WEST VIRGINIA|WISCONSIN|WYOMING)$'

        state_abbr_pattern = re.compile(self.STATE_ABBR_REGEX)
        # state_full_pattern = re.compile(state_full_regex)

        return state_abbr_pattern.search(state_input.upper()) != None# or state_full_pattern.search(state_input.upper()) != None



    def find_max_camera_violations_streak(self, list_of_violation_times):
        if list_of_violation_times:
            max_streak      = 0
            min_streak_date = None
            max_streak_date = None

            for date in list_of_violation_times:

                self.logger.debug("date: %s", date)

                year_later = date + (datetime(date.year + 1, 1, 1) - datetime(date.year, 1, 1))
                self.logger.debug("year_later: %s", year_later)

                year_long_tickets = [comp_date for comp_date in list_of_violation_times if date <= comp_date < year_later]
                this_streak       = len(year_long_tickets)

                if this_streak > max_streak:

                    max_streak      = this_streak
                    min_streak_date = year_long_tickets[0]
                    max_streak_date = year_long_tickets[-1]

            return {
              'min_streak_date': min_streak_date.strftime('%B %-d, %Y'),
              'max_streak': max_streak,
              'max_streak_date': max_streak_date.strftime('%B %-d, %Y')
            }

        return dict()



    def find_potential_vehicles(self, list_of_strings):

        # Use new logic of '<state>:<plate>'
        plate_tuples = [match.split(':') for match in re.findall(r'(\b[a-zA-Z9]{2}\s*:\s*[a-zA-Z0-9]+\b|\b[a-zA-Z0-9]+\s*:\s*[a-zA-Z9]{2}\b)', ' '.join(list_of_strings)) if all(substr not in match.lower() for substr in ['://', 'state:', 'plate:'])]

        return self.infer_plate_and_state_data(plate_tuples)



    def find_potential_vehicles_using_legacy_logic(self, list_of_strings):
        # Find potential plates

        # Use old logic of 'state:<state> plate:<plate>'
        potential_vehicles = []
        legacy_plate_data  = dict([[piece.strip() for piece in match.split(':')] for match in [part.lower() for part in list_of_strings if ('state:' in part.lower() or 'plate:' in part.lower() or 'types:' in part.lower())]])

        if legacy_plate_data:
            if self.detect_state(legacy_plate_data.get('state')) and legacy_plate_data.get('plate'):
                legacy_plate_data['valid_plate'] = True
            else:
                legacy_plate_data['valid_plate'] = False

            potential_vehicles.append(legacy_plate_data)

        return potential_vehicles



    def form_campaign_lookup_response_parts(self, query_result, username):
        campaign_chunks = []
        campaign_string = ""

        for campaign in query_result['included_campaigns']:
            num_vehicles = campaign['campaign_vehicles']
            num_tickets  = campaign['campaign_tickets']

            next_string_part = "{} {} {} {} {} been tagged with {}.\n\n".format(num_vehicles, 'vehicle with' if num_vehicles == 1 else 'vehicles with a total of', num_tickets, 'ticket' if num_tickets == 1 else 'tickets', 'has' if num_vehicles == 1 else 'have', campaign['campaign_hashtag'])

            # how long would it be
            potential_response_length = len(username + ' ' + campaign_string + next_string_part)

            if (potential_response_length <= self.MAX_TWITTER_STATUS_LENGTH):
                campaign_string += next_string_part
            else:
                campaign_chunks.append(campaign_string)
                campaign_string = next_string_part

        # Get any part of string left over
        campaign_chunks.append(campaign_string)

        return campaign_chunks



    def form_plate_lookup_response_parts(self, query_result, username):

        # response_chunks holds tweet-length-sized parts of the response
        # to be tweeted out or appended into a single direct message.
        response_chunks   = []
        violations_string = ""


        # Get total violations
        total_violations = sum([s['count'] for s in query_result['violations']])
        self.logger.debug("total_violations: %s", total_violations)


        # Append to initially blank string to build tweet.
        violations_string += "#{}_{}{} has been queried {} {}.\n\n".format(query_result['state'], query_result['plate'], (' (types: ' + query_result['plate_types'] + ')') if query_result['plate_types'] else '', query_result['frequency'], 'time' if int(query_result['frequency']) == 1 else 'times')

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


        violations_keys = {
          'count'                       : 'count',
          'continued_format_string'     : "Parking and camera violation tickets for #{}_{}, cont'd:\n\n",
          'continued_format_string_args': [query_result['state'], query_result['plate']],
          'cur_string'                  : violations_string,
          'description'                 : 'title',
          'default_description'         : 'No Year Available',
          'prefix_format_string'        : "Total parking and camera violation tickets: {}\n\n",
          'prefix_format_string_args'   : [total_violations],
          'result_format_string'        : '{}| {}\n',
          'username'                    : username
        }

        response_chunks += self.handle_response_part_formation(query_result['violations'], violations_keys)


        if query_result.get('years'):
            years_keys = {
              'count'                       : 'count',
              'continued_format_string'     : "Violations by year for #{}_{}, cont'd:\n\n",
              'continued_format_string_args': [query_result['state'], query_result['plate']],
              'description'                 : 'title',
              'default_description'         : 'No Year Available',
              'prefix_format_string'        : "Violations by year for #{}_{}:\n\n",
              'prefix_format_string_args'   : [query_result['state'], query_result['plate']],
              'result_format_string'        : '{}| {}\n',
              'username'                    : username
            }

            response_chunks += self.handle_response_part_formation(query_result['years'], years_keys)


        if query_result.get('boroughs'):
            boroughs_keys = {
              'count'                       : 'count',
              'continued_format_string'     : "Violations by borough for #{}_{}, cont'd:\n\n",
              'continued_format_string_args': [query_result['state'], query_result['plate']],
              'description'                 : 'title',
              'default_description'         : 'No Borough Available',
              'prefix_format_string'        : "Violations by borough for #{}_{}:\n\n",
              'prefix_format_string_args'   : [query_result['state'], query_result['plate']],
              'result_format_string'        : '{}| {}\n',
              'username'                    : username
            }

            response_chunks += self.handle_response_part_formation(query_result['boroughs'], boroughs_keys)


        if query_result.get('fines') and any([k[1] != 0 for k in query_result.get('fines')]):

            fines = query_result['fines']

            cur_string = "Known fines for #{}_{}:\n\n".format(query_result['state'], query_result['plate'])

            max_count_length = len('${:,.2f}'.format(max( t[1] for t in fines)))
            spaces_needed    = (max_count_length * 2) + 1

            for fine_type, amount in fines:

                currency_string = '${:,.2f}'.format(amount)
                count_length    = len(str(currency_string))

                # e.g., if spaces_needed is 5, and count_length is 2, we need to pad to 3.
                left_justify_amount = spaces_needed - count_length

                # formulate next string part
                next_part = '{}| {}\n'.format(currency_string.ljust(left_justify_amount), fine_type.replace('_', ' ').title())

                # determine current string length if necessary
                potential_response_length = len(username + ' ' + cur_string + next_part)

                # If username, space, violation string so far and new part are less or
                # equal than 280 characters, append to existing tweet string.
                if (potential_response_length <= self.MAX_TWITTER_STATUS_LENGTH):
                    cur_string += next_part
                else:
                    response_chunks.append(cur_string)

                    cur_string = "Known fines for #{}_{}, cont'd:\n\n"
                    cur_string += next_part

            # add to container
            response_chunks.append(cur_string)


        if query_result.get('camera_streak_data'):

            streak_data = query_result['camera_streak_data']

            if streak_data.get('max_streak') and streak_data['max_streak'] >= 5:

                # formulate streak string
                streak_string = "Under @bradlander's proposed legislation, this vehicle could have been booted or impounded due to its {} camera violations (>= 5/year) from {} to {}.\n".format(streak_data['max_streak'], streak_data['min_streak_date'], streak_data['max_streak_date'])

                # add to container
                response_chunks.append(streak_string)


        # Send it back!
        return response_chunks



    def form_summary_string(self, summary, username):
        return ["The {} vehicles you queried have collectively received {} {} with at least {} in fines, of which {} has been paid.\n\n".format(summary['vehicles'], summary['tickets'], 'ticket' if summary['tickets'] == 1 else 'tickets', '${:,.2f}'.format(summary['fines']['fined'] - summary['fines']['reduced']), '${:,.2f}'.format(summary['fines']['paid']))]



    def handle_response_part_formation(self, collection, keys):

        # collect the responses
        response_container = []

        cur_string = keys['cur_string'] if keys.get('cur_string') else ''

        if keys['prefix_format_string']:
            cur_string += keys['prefix_format_string'].format(*keys['prefix_format_string_args'])

        max_count_length = len(str(max( item[ keys['count'] ] for item in collection )))
        spaces_needed    = (max_count_length * 2) + 1

        # Grab item
        for item in collection:

            # Titleize for readability.
            description = item[keys['description']].title()

            # Use a default description if need be
            if len(description) == 0:
                description = keys['default_description']

            count        = item[keys['count']]
            count_length = len(str(count))

            # e.g., if spaces_needed is 5, and count_length is 2, we need to pad to 3.
            left_justify_amount = spaces_needed - count_length

            # formulate next string part
            next_part = keys['result_format_string'].format(str(count).ljust(left_justify_amount), description)

            # determine current string length
            potential_response_length = len(keys['username'] + ' ' + cur_string + next_part)

            # If username, space, violation string so far and new part are less or
            # equal than 280 characters, append to existing tweet string.
            if (potential_response_length <= self.MAX_TWITTER_STATUS_LENGTH):
                cur_string += next_part
            else:
                response_container.append(cur_string)
                if keys['continued_format_string']:
                    cur_string = keys['continued_format_string'].format(*keys['continued_format_string_args'])
                else:
                    cur_string = ''

                cur_string += next_part


            self.logger.debug("cur_string: %s", cur_string)
            self.logger.debug("length: %s", len(cur_string))
            self.logger.debug("string: %s", cur_string)

        # If we finish the list with a non-empty string,
        # append that string to response parts
        if len(cur_string) != 0:
            # Append ready string into parts for response.
            response_container.append(cur_string)

            self.logger.debug("length: %s", len(cur_string))
            self.logger.debug("string: %s", cur_string)

        # Return parts
        return response_container



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
                    this_plate['state']       = part0
                    this_plate['plate']       = part1
                    this_plate['valid_plate'] = True
                    # this_plate['types']       = None
                elif is_part1_state and len(part0) > 0:
                    this_plate['state']       = part1
                    this_plate['plate']       = part0
                    this_plate['valid_plate'] = True
                    # this_plate['types']       = None

            plate_data.append(this_plate)

        return plate_data



    def initiate_reply(self, message, message_source, message_type):
        self.logger.info('\n')
        self.logger.info('Calling initiate_reply')

        args_for_response = self.reply_argument_builder.build_reply_data(message, message_source, message_type)

        if args_for_response['needs_reply']:
            return self.create_response(args_for_response)



    def perform_campaign_lookup(self, included_campaigns):

        self.logger.debug('Performing lookup for campaigns.')

        # Instantiate connection.
        conn = self.db_service.get_connection()

        result = {'included_campaigns': []}

        for campaign in included_campaigns:
            # get new total for tickets
            campaign_tickets_query_string = """
              select count(id) as campaign_vehicles,
                     ifnull(sum(num_tickets), 0) as campaign_tickets
                from plate_lookups t1
               where (plate, state)
                  in
                (select plate, state
                  from campaigns_plate_lookups cpl
                  join plate_lookups t2
                    on t2.id = cpl.plate_lookup_id
                 where campaign_id = %s)
                 and t1.created_at =
                  (select MAX(t3.created_at)
                     from plate_lookups t3
                    where t3.plate = t1.plate
                      and t3.state = t1.state
                      and count_towards_frequency = 1);
            """

            campaign_tickets_result = conn.execute(campaign_tickets_query_string.replace('\n', ''), (campaign[0])).fetchone()
            # return data
            # result['included_campaigns'].append((campaign[1], int(campaign_tickets), int(num_vehicles)))
            result['included_campaigns'].append({'campaign_hashtag': campaign[1], 'campaign_tickets': int(campaign_tickets_result[1]), 'campaign_vehicles': int(campaign_tickets_result[0])})

        # Close the connection
        conn.close()

        return result



    def perform_plate_lookup(self, args):

        self.logger.debug('Performing lookup for plate.')

        # Instantiate connection.
        conn = self.db_service.get_connection()

        # pattern only allows alphanumeric characters.
        plate_pattern = re.compile('[\W_]+', re.UNICODE)

        # Grab plate and plate from args.
        created_at   = datetime.strptime(args['created_at'], '%a %b %d %H:%M:%S %z %Y').strftime('%Y-%m-%d %H:%M:%S') if 'created_at' in args else None
        message_id   = args['message_id'] if 'message_id' in args else None
        message_type = args['message_type']
        plate        = plate_pattern.sub('', args['plate'].strip().upper())
        state        = args['state'].strip().upper()
        plate_types  = args['plate_types']
        username     = re.sub('@', '', args['username'])

        self.logger.debug('Listing args... plate: %s, state: %s, message_id: %s, created_at: %s', plate, state, str(message_id), str(created_at))

        # Set up retry ability
        s_req = requests_futures.sessions.FuturesSession(max_workers=6)

        retries = Retry(total=5,
                        backoff_factor=0.1,
                        status_forcelist=[ 500, 502, 503, 504 ],
                        raise_on_status=False)

        s_req.mount('https://', HTTPAdapter(max_retries=retries))


        # Find medallion plates
        #
        medallion_regex    = r'^[0-9][A-Z][0-9]{2}$'
        medallion_pattern  = re.compile(medallion_regex)

        if medallion_pattern.search(plate.upper()) != None:
            medallion_response = s_req.get('https://data.cityofnewyork.us/resource/7drc-shp9.json?license_number={}'.format(plate))
            medallion_data     = medallion_response.result().json()

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
        opacv_query    = opacv_endpoint + '?$limit={}&$$app_token={}&plate={}&state={}'.format(limit, token, plate, state)

        if plate_types is not None:
            opacv_query += "&$where=license_type%20in(" + ','.join(['%27' + type + '%27' for type in plate_types.split(',')]) + ")"

        opacv_response = s_req.get(opacv_query)
        opacv_data     = opacv_response.result().json()

        # log response
        self.logger.debug('violations raw: %s', opacv_response)
        self.logger.debug('Open Parking and Camera Violations data: %s', opacv_data)

        # only data we're looking for
        opacv_desired_keys = ['borough', 'county', 'fined', 'issue_date', 'paid', 'precinct', 'outstanding', 'reduced', 'violation']


        # add violation if it's missing
        for record in opacv_data:
            if record.get('violation'):
                record['violation'] = self.OPACV_HUMANIZED_NAMES[record['violation']]

            if record.get('issue_date') is None:
                record['has_date'] = False
            else:
                try:
                    record['issue_date'] = datetime.strptime(record['issue_date'], '%m/%d/%Y').strftime('%Y-%m-%dT%H:%M:%S.%f')
                    record['has_date']   = True
                except ValueError as ve:
                    record['has_date']   = False

            if record.get('precinct'):
                boros = [boro for boro, precincts in self.PRECINCTS_BY_BORO.items() if int(record['precinct']) in self.PRECINCTS]
                if boros:
                    record['borough'] = boros[0]
                else:
                    if record.get('county'):
                        boros = [name for name,codes in self.COUNTY_CODES.items() if record.get('county') in codes]
                        if boros:
                            record['borough'] = boros[0]


            record['fined']         = None
            record['paid']          = None
            record['reduced']       = None
            record['outstanding']   = None

            for key in ['amount_due', 'fine_amount', 'interest_amount', 'payment_amount', 'penalty_amount', 'reduction_amount']:
                if key in record:
                    try:
                        amount = float(record[key])

                        if key in ['fine_amount', 'interest_amount', 'penalty_amount']:
                            if record['fined'] is None:
                                record['fined'] = 0

                            record['fined'] += amount

                        elif key == 'reduction_amount':
                            if record['reduced'] is None:
                                record['reduced'] = 0

                            record['reduced'] += amount

                        elif key == 'amount_due':
                            if record['outstanding'] is None:
                                record['outstanding'] = 0

                            record['outstanding'] += amount

                        elif key == 'payment_amount':
                            if record['paid'] is None:
                                record['paid'] = 0

                            record['paid'] += amount

                    except ValueError as ve:

                        self.logger.error('Error parsing value into float')
                        self.logger.error(e)
                        self.logger.error(str(e))
                        self.logger.error(e.args)
                        logging.exception("stack trace")

                        pass

            combined_violations[record['summons_number']] = { key: record.get(key) for key in opacv_desired_keys }



        # collect summons numbers to use for excluding duplicates later
        opacv_summons_numbers  = list(combined_violations.keys())


        # Grab data from each of the fiscal year violation datasets
        #

        # collect the data in a list
        fy_endpoints = ['https://data.cityofnewyork.us/resource/j7ig-zgkq.json', 'https://data.cityofnewyork.us/resource/aagd-wyjz.json', 'https://data.cityofnewyork.us/resource/avxe-2nrn.json', 'https://data.cityofnewyork.us/resource/ati4-9cgt.json', 'https://data.cityofnewyork.us/resource/9wgk-ev5c.json', 'https://data.cityofnewyork.us/resource/qpyv-8eyi.json']

        # only data we're looking for
        fy_desired_keys = ['borough', 'issue_date', 'violation', 'violation_precinct', 'violation_county']

        # iterate through the endpoints
        for endpoint in fy_endpoints:
            query_string = '?$limit={}&$$app_token={}&plate_id={}&registration_state={}'.format(limit, token, plate, state)

            if plate_types is not None:
                query_string += "&$where=plate_type%20in(" + ','.join(['%27' + type + '%27' for type in plate_types.split(',')]) + ")"

            response     = s_req.get(endpoint + query_string)
            result       = response.result()

            if result.status_code in range(200,300):
                # Only attempt to read json on a successful response.
                data     = result.json()
            elif result.status_code in range(300,400):
                return {'error': 'redirect', 'plate': plate, 'state': state}
            elif result.status_code in range(400,500):
                return {'error': 'user error', 'plate': plate, 'state': state}
            elif result.status_code in range(500,600):
                return {'error': 'server error', 'plate': plate, 'state': state}
            else:
                return {'error': 'unknown error', 'plate': plate, 'state': state}



            self.logger.debug('endpoint: %s', endpoint)
            self.logger.debug('fy_response: %s', data)


            for record in data:
                if record.get('violation_description') is None:
                    if record.get('violation_code') and self.FY_HUMANIZED_NAMES.get(record['violation_code']):
                        record['violation'] = self.FY_HUMANIZED_NAMES.get(record['violation_code'])
                else:
                    if self.FY_HUMANIZED_NAMES.get(record['violation_description']):
                        record['violation'] = self.FY_HUMANIZED_NAMES.get(record['violation_description'])
                    else:
                        record['violation'] = re.sub('[0-9]*-', '', record['violation_description'])

                if record.get('issue_date') is None:
                    record['has_date']   = False
                else:
                    try:
                        record['issue_date'] = datetime.strptime(record['issue_date'], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%S.%f')
                        record['has_date']   = True
                    except ValueError as ve:
                        record['has_date']   = False


                if record.get('violation_precinct'):
                    boros = [boro for boro, precincts in self.PRECINCTS_BY_BORO.items() if int(record['violation_precinct']) in precincts]
                    if boros:
                        record['borough'] = boros[0]
                    else:
                        if record.get('violation_county'):
                            boros = [name for name,codes in self.COUNTY_CODES.items() if record.get('violation_county') in codes]
                            if boros:
                                record['borough'] = boros[0]
                        else:
                            if record.get('street_name'):
                                street_name         = record.get('street_name')
                                intersecting_street = record.get('intersecting_street') or ''

                                google_boros = self.detect_borough([street_name, intersecting_street])
                                if google_boros:
                                    record['borough'] = google_boros[0].lower()


                # structure response and only use the data we need
                new_data = { key: record.get(key) for key in fy_desired_keys }

                if combined_violations.get(record['summons_number']) is None:
                    combined_violations[record['summons_number']] = new_data
                else:
                    # Merge records together, treating fiscal year data as authoritative.
                    return_record = combined_violations[record['summons_number']] = {**combined_violations.get(record['summons_number']), **new_data}

                    # If we still don't have a violation after merging records, record it as blank
                    if return_record.get('violation') is None:
                        return_record['violation'] = "No Violation Description Available"
                    if return_record.get('borough') is None:
                        record['borough'] = 'No Borough Available'



        for record in combined_violations.values():
            if record.get('violation') is None:
                record['violation'] = "No Violation Description Available"

            if record.get('borough') is None:
                record['borough'] = 'No Borough Available'



        # Marshal all ticket data into form.
        fines    = [
          ('fined',       sum(v['fined'] for v in combined_violations.values() if v.get('fined'))),
          ('reduced',     sum(v['reduced'] for v in combined_violations.values() if v.get('reduced'))),
          ('paid',        sum(v['paid'] for v in combined_violations.values() if v.get('paid'))),
          ('outstanding', sum(v['outstanding'] for v in combined_violations.values() if v.get('outstanding')))
        ]
        tickets  = Counter([v['violation'] for v in combined_violations.values() if v.get('violation')]).most_common()
        years    = Counter([datetime.strptime(v['issue_date'], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y') if v.get('issue_date') else 'No Year Available' for v in combined_violations.values()]).most_common()
        boroughs = Counter([v['borough'] for v in combined_violations.values() if v.get('borough')]).most_common()

        camera_streak_data = self.find_max_camera_violations_streak(sorted([datetime.strptime(v['issue_date'],'%Y-%m-%dT%H:%M:%S.%f') for v in combined_violations.values() if v.get('violation') and v['violation'] in ['Failure to Stop at Red Light', 'School Zone Speed Camera Violation']]))

        result   = {
          'boroughs'    : [{'title':k.title(),'count':v} for k, v in boroughs],
          'fines'       : fines,
          'plate'       : plate,
          'plate_types' : plate_types,
          'state'       : state,
          'violations'  : [{'title':k.title(),'count':v} for k,v in tickets],
          'years'       : sorted([{'title':k.title(),'count':v} for k,v in years], key=lambda k: k['title'])
        }

        # No need to add streak data if it doesn't exist
        if camera_streak_data:
            result['camera_streak_data'] = camera_streak_data


        self.logger.debug('violations sorted: %s', result)

        previous_lookup = None

        # See if we've seen this vehicle before.
        if plate_types:
            previous_lookup = conn.execute(""" select num_tickets, created_at from plate_lookups where plate = %s and state = %s and plate_types = %s and count_towards_frequency = %s ORDER BY created_at DESC LIMIT 1""", (plate, state, plate_types, True))
        else:
            previous_lookup = conn.execute(""" select num_tickets, created_at from plate_lookups where plate = %s and state = %s and plate_types IS NULL and count_towards_frequency = %s ORDER BY created_at DESC LIMIT 1""", (plate, state, True))

        # Turn data into list of dicts with attribute keys
        previous_data   = [dict(zip(tuple (previous_lookup.keys()), i)) for i in previous_lookup.cursor]

        # if we have a previous lookup, add it to the return data.
        if previous_data:
            result['previous_result'] = previous_data[0]

            self.logger.debug('we have previous data: %s', previous_data[0])


        # Find the number of times we have seen this vehicle before.
        if plate_types:
            current_frequency = conn.execute(""" select count(*) as lookup_frequency from plate_lookups where plate = %s and state = %s and plate_types = %s and count_towards_frequency = %s """, (plate, state, plate_types, True)).fetchone()[0]
        else:
            current_frequency = conn.execute(""" select count(*) as lookup_frequency from plate_lookups where plate = %s and state = %s and plate_types IS NULL and count_towards_frequency = %s """, (plate, state, True)).fetchone()[0]

        # Default to counting everything.
        count_towards_frequency = 1

        # Calculate the number of violations.
        total_violations = len(combined_violations)

        # If this came from message, add it to the plate_lookups table.
        if message_type and message_id and created_at:
            # Insert plate lookupresult
            insert_lookup = conn.execute(""" insert into plate_lookups (plate, state, plate_types, observed, message_id, lookup_source, created_at, external_username, count_towards_frequency, num_tickets, boot_eligible, responded_to) values (%s, %s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, 1) """, (plate, state, plate_types, message_id, message_type, created_at, username, count_towards_frequency, total_violations, camera_streak_data.get('max_streak') >= 5 if camera_streak_data else False))

            # Iterate through included campaigns to tie lookup to each
            for campaign in args['included_campaigns']:
                # insert join record for campaign lookup
                conn.execute(""" insert into campaigns_plate_lookups (campaign_id, plate_lookup_id) values (%s, %s) """, (campaign[0], insert_lookup.lastrowid))


        # how many times have we searched for this plate from a tweet
        result['frequency'] = current_frequency + 1

        self.logger.debug('returned_result: %s', result)

        # Close the connection
        conn.close()

        return result



    def create_response(self, response_args):

        self.logger.info('\n')
        self.logger.info("Calling create_response")

        # Print args
        self.logger.info('args:')
        self.logger.info('response_args: %s', response_args)


        # Grab string parts
        string_parts = response_args['string_parts']
        self.logger.debug('string_parts: %s', string_parts)

        # Find potential plates
        potential_vehicles = self.find_potential_vehicles(string_parts)
        self.logger.debug('potential_vehicles: %s', potential_vehicles)

        # Find included campaign hashtags
        included_campaigns = self.detect_campaign_hashtags(string_parts)
        self.logger.debug('included_campaigns: %s', included_campaigns)


        # Grab legacy string parts
        legacy_string_parts = response_args['legacy_string_parts']
        self.logger.debug('legacy_string_parts: %s', legacy_string_parts)

        potential_vehicles += self.find_potential_vehicles_using_legacy_logic(legacy_string_parts)
        self.logger.debug('potential_vehicles: %s', potential_vehicles)


        # Grab user info
        username = response_args['username']
        self.logger.debug('username: %s', username)

        mentioned_users = response_args['mentioned_users'] if 'mentioned_users' in response_args else []
        self.logger.debug('mentioned_users: %s', mentioned_users)


        # Grab tweet details for reply.
        message_id = response_args['id']
        self.logger.debug("message id: %s", message_id)

        message_created_at = response_args['created_at']
        self.logger.debug('message created at: %s', message_created_at)

        message_source = response_args['type']
        self.logger.debug('message_source: %s', message_source)

        message_type = response_args['type']
        self.logger.debug('message_type: %s', message_type)


        # # get necessary formatter
        # formatter = None

        # if response_args['source'] == 'api':
        #     formatter = APIResponseFormatter()

        # elif response_args['source'] == 'twitter':
        #     formatter = TwitterResponseFormatter()

        # else:
        #     formatter = ResponseFormatter()



        # Collect response parts here.
        response_parts    = []
        successful_lookup = False
        error_on_lookup   = False


        # Wrap in try/catch block
        try:
            # Split plate and state strings into key/value pairs.
            query_info = {}

            query_info['created_at']         = message_created_at
            query_info['included_campaigns'] = included_campaigns
            query_info['message_id']         = message_id
            query_info['message_type']       = message_type
            query_info['username']           = username

            self.logger.debug("lookup info: %s", query_info)

            # for each vehicle, we need to determine if the supplied information amounts to a valid plate
            # then we need to look up each valid plate
            # then we need to respond in a single thread in order with the responses

            summary = {
              'fines'   : {
                'fined'       : 0,
                'outstanding' : 0,
                'reduced'     : 0,
                'paid'        : 0
              },
              'tickets' : 0,
              'vehicles': 0
            }

            for potential_vehicle in potential_vehicles:

                if potential_vehicle.get('valid_plate'):

                    query_info['plate']                  = potential_vehicle.get('plate')
                    query_info['state']                  = potential_vehicle.get('state')
                    query_info['plate_types']            = potential_vehicle.get('types').upper() if 'types' in potential_vehicle else None

                    # Do the real work!
                    plate_lookup = self.perform_plate_lookup(query_info)


                    # Increment summary vehicle info
                    summary['vehicles'] += 1


                    if plate_lookup.get('violations'):

                        # Increment summary ticket info
                        summary['tickets'] += sum(s['count'] for s in plate_lookup['violations'])
                        for k,v in {key[0]: key[1] for key in plate_lookup['fines']}.items():
                            summary['fines'][k] += v

                        # Record successful lookup.
                        successful_lookup = True

                        response_parts.append(self.form_plate_lookup_response_parts(plate_lookup, username))
                        # [[campaign_stuff], tickets_0, tickets_1, etc.]

                    elif plate_lookup.get('error'):

                        # Record lookup error.
                        error_on_lookup = True

                        response_parts.append(["Sorry, I received an error when looking up {}:{}{}. Please try again.".format(plate_lookup.get('state').upper(), plate_lookup.get('plate').upper(), (' (types: ' + potential_vehicle.get('types').upper() + ')') if potential_vehicle.get('types') else '')])

                    else:

                        # Record successful lookup.
                        successful_lookup = True

                        # Let user know we didn't find anything.
                        # sorry_message = "{} Sorry, I couldn't find any tickets for that plate."
                        response_parts.append(["Sorry, I couldn't find any tickets for {}:{}{}.".format(potential_vehicle.get('state').upper(), potential_vehicle.get('plate').upper(), (' (types: ' + potential_vehicle.get('types').upper() + ')') if potential_vehicle.get('types') else '',  )])

                else:

                    # Record the failed lookup.

                    # Instantiate a connection.
                    conn = self.db_service.get_connection()

                    # Insert failed lookup
                    conn.execute(""" insert into failed_plate_lookups (external_username, message_id, responded_to) values (%s, %s, 1) """, re.sub('@', '', username), message_id)

                    # Close the connection
                    conn.close()

                    # Legacy data where state is not a valid abbreviation.
                    if potential_vehicle.get('state'):
                        self.logger.debug("We have a state, but it's invalid.")

                        response_parts.append(["The state should be two characters, but you supplied '{}'. Please try again.".format(potential_vehicle.get('state'))])

                    # '<state>:<plate>' format, but no valid state could be detected.
                    elif potential_vehicle.get('original_string'):
                        self.logger.debug("We don't have a state, but we have an attempted lookup with the new format.")

                        response_parts.append(["Sorry, a plate and state could not be inferred from {}.".format(potential_vehicle.get('original_string'))])

                    # If we have a plate, but no state.
                    elif potential_vehicle.get('plate'):
                        self.logger.debug("We have a plate, but no state")

                        response_parts.append(["Sorry, the state appears to be blank.".format(query_info['state'])])


            # If we have multiple vehicles, prepend a summary.
            if summary['vehicles'] > 1:
                response_parts.insert(0, self.form_summary_string(summary, username))


            # Look up campaign hashtags after doing the plate lookups and then prepend to response.
            if included_campaigns:
                campaign_lookup = self.perform_campaign_lookup(included_campaigns)
                response_parts.insert(0, self.form_campaign_lookup_response_parts(campaign_lookup, username))

                successful_lookup = True



            # If we don't look up a single plate successfully,
            # figure out how we can help the user.
            if not successful_lookup and not error_on_lookup:

                # Record the failed lookup

                # Instantiate a connection.
                conn = self.db_service.get_connection()

                # Insert failed lookup
                conn.execute(""" insert into failed_plate_lookups (external_username, message_id, responded_to) values (%s, %s, 1) """, re.sub('@', '', username), message_id)

                # Close the connection
                conn.close()


                self.logger.debug('The data seems to be in the wrong format.')

                state_regex    = r'^(99|AB|AK|AL|AR|AZ|BC|CA|CO|CT|DC|DE|DP|FL|FM|FO|GA|GU|GV|HI|IA|ID|IL|IN|KS|KY|LA|MA|MB|MD|ME|MI|MN|MO|MP|MS|MT|MX|NB|NC|ND|NE|NF|NH|NJ|NM|NS|NT|NV|NY|OH|OK|ON|OR|PA|PE|PR|PW|QC|RI|SC|SD|SK|TN|TX|UT|VA|VI|VT|WA|WI|WV|WY|YT)$'
                numbers_regex  = r'[0-9]{4}'

                state_pattern  = re.compile(state_regex)
                number_pattern = re.compile(numbers_regex)

                state_matches  = [state_pattern.search(s.upper()) != None for s in string_parts]
                number_matches = [number_pattern.search(s.upper()) != None for s in list(filter(lambda part: re.sub(r'\.|@', '', part.lower()) not in set(mentioned_users), string_parts))]

                # We have what appears to be a plate and a state abbreviation.
                if all([any(state_matches), any(number_matches)]):
                    self.logger.debug('There is both plate and state information in this message.')

                    # Let user know plate format
                    response_parts.append(["I’d be happy to look that up for you!\n\nJust a reminder, the format is <state|province|territory>:<plate>, e.g. NY:abc1234"])

                # Maybe we have plate or state. Let's find out.
                else:
                    self.logger.debug('The tweet is missing either state or plate or both.')

                    state_regex_minus_words   = r'^(99|AB|AK|AL|AR|AZ|BC|CA|CO|CT|DC|DE|DP|FL|FM|FO|GA|GU|GV|IA|ID|IL|KS|KY|LA|MA|MB|MD|MH|MI|MN|MO|MP|MS|MT|MX|NB|NC|ND|NE|NF|NH|NJ|NM|NS|NT|NV|NY|PA|PE|PR|PW|QC|RI|SC|SD|SK|STATE|TN|TX|UT|VA|VI|VT|WA|WI|WV|WY|YT)$'
                    state_minus_words_pattern = re.compile(state_regex_minus_words)

                    state_minus_words_matches = [state_minus_words_pattern.search(s.upper()) != None for s in string_parts]

                    number_matches = [number_pattern.search(s.upper()) != None for s in list(filter(lambda part: re.sub(r'\.|@', '', part.lower()) not in set(mentioned_users), string_parts))]

                    # We have either plate or state.
                    if any(state_minus_words_matches) or any(number_matches):

                        # Let user know plate format
                        response_parts.append(["I think you're trying to look up a plate, but can't be sure.\n\nJust a reminder, the format is <state|province|territory>:<plate>, e.g. NY:abc1234"])

                    # We have neither plate nor state. Do nothing.
                    else:
                        self.logger.debug('ignoring message since no plate or state information to respond to.')


        except Exception as e:
            # Log error
            response_parts.append(["Sorry, I encountered an error. Tagging @bdhowald."])

            self.logger.error('Missing necessary information to continue')
            self.logger.error(e)
            self.logger.error(str(e))
            self.logger.error(e.args)
            logging.exception("stack trace")


        # Indicate successful response processing.
        return {
          'error_on_lookup'  : error_on_lookup,
          'response_args'    : response_args,
          'response_parts'   : response_parts,
          'success'          : True,
          'successful_lookup': successful_lookup
        }
