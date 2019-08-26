import logging
import os
import re
import requests
import requests_futures.sessions

from collections import Counter
from datetime import datetime
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from traffic_violations.services.location_service import LocationService
from traffic_violations.models.plate_lookup import PlateLookup
from typing import Any, Dict, List


class OpenDataService:

    COUNTY_CODES = {
        'bronx': ['BRONX', 'BX', 'PBX'],
        'brooklyn': ['BK', 'BROOK', 'K', 'KINGS', 'PK'],
        'manhattan': ['MAH', 'MANHA', 'MN', 'NEUY', 'NY', 'PNY'],
        'queens': ['Q', 'QN', 'QNS', 'QUEEN'],
        'staten island': ['R', 'RICH', 'ST'],
    }

    FISCAL_YEAR_DATABASE_ENDPOINTS = {
      2014: 'https://data.cityofnewyork.us/resource/j7ig-zgkq.json',
      2015: 'https://data.cityofnewyork.us/resource/aagd-wyjz.json',
      2016: 'https://data.cityofnewyork.us/resource/avxe-2nrn.json',
      2017: 'https://data.cityofnewyork.us/resource/ati4-9cgt.json',
      2018: 'https://data.cityofnewyork.us/resource/9wgk-ev5c.json',
      2019: 'https://data.cityofnewyork.us/resource/faiq-9dfq.json',
      2020: 'https://data.cityofnewyork.us/resource/pvqr-7yc4.json'
    }

    FISCAL_YEAR_DATABASE_NEEDED_FIELDS = ['borough', 'has_date', 'issue_date',
                          'violation', 'violation_precinct', 'violation_county']

    HOWS_MY_DRIVING_NY_FINE_KEYS = ['fined', 'paid', 'reduced', 'outstanding']

    # humanized names for violations
    HUMANIZED_NAMES_FOR_OPEN_PARKING_AND_CAMERA_VIOLATIONS = {'': 'No Description Given',  'ALTERING INTERCITY BUS PERMIT': 'Altered Intercity Bus Permit',  'ANGLE PARKING': 'No Angle Parking',  'ANGLE PARKING-COMM VEHICLE': 'No Angle Parking',  'BEYOND MARKED SPACE': 'No Parking Beyond Marked Space',  'BIKE LANE': 'Blocking Bike Lane',  'BLUE ZONE': 'No Parking - Blue Zone',  'BUS LANE VIOLATION': 'Bus Lane Violation',  'BUS PARKING IN LOWER MANHATTAN': 'Bus Parking in Lower Manhattan',  'COMML PLATES-UNALTERED VEHICLE': 'Commercial Plates on Unaltered Vehicle',  'CROSSWALK': 'Blocking Crosswalk',  'DETACHED TRAILER': 'Detached Trailer',  'DIVIDED HIGHWAY': 'No Stopping - Divided Highway',  'DOUBLE PARKING': 'Double Parking',  'DOUBLE PARKING-MIDTOWN COMML': 'Double Parking - Midtown Commercial Zone',  'ELEVATED/DIVIDED HIGHWAY/TUNNL': 'No Stopping in Tunnel or on Elevated Highway',  'EXCAVATION-VEHICLE OBSTR TRAFF': 'No Stopping - Adjacent to Street Construction',  'EXPIRED METER': 'Expired Meter',  'EXPIRED METER-COMM METER ZONE': 'Expired Meter - Commercial Meter Zone',  'EXPIRED MUNI METER': 'Expired Meter',  'EXPIRED MUNI MTR-COMM MTR ZN': 'Expired Meter - Commercial Meter Zone',  'FAIL TO DISP. MUNI METER RECPT': 'Failure to Display Meter Receipt',  'FAIL TO DSPLY MUNI METER RECPT': 'Failure to Display Meter Receipt',  'FAILURE TO DISPLAY BUS PERMIT': 'Failure to Display Bus Permit',  'FAILURE TO STOP AT RED LIGHT': 'Failure to Stop at Red Light',  'FEEDING METER': 'Feeding Meter',  'FIRE HYDRANT': 'Fire Hydrant',  'FRONT OR BACK PLATE MISSING': 'Front or Back Plate Missing',  'IDLING': 'Idling',  'IMPROPER REGISTRATION': 'Improper Registration',  'INSP STICKER-MUTILATED/C\'FEIT': 'Inspection Sticker Mutilated or Counterfeit',  'INSP. STICKER-EXPIRED/MISSING': 'Inspection Sticker Expired or Missing',  'INTERSECTION': 'No Stopping - Intersection',  'MARGINAL STREET/WATER FRONT': 'No Parking on Marginal Street or Waterfront',  'MIDTOWN PKG OR STD-3HR LIMIT': 'Midtown Parking or Standing - 3 Hour Limit',  'MISCELLANEOUS': 'Miscellaneous',  'MISSING EQUIPMENT': 'Missing Required Equipment',  'NGHT PKG ON RESID STR-COMM VEH': 'No Nighttime Parking on Residential Street - Commercial Vehicle',  'NIGHTTIME STD/ PKG IN A PARK': 'No Nighttime Standing or Parking in a Park',  'NO MATCH-PLATE/STICKER': 'Plate and Sticker Do Not Match',  'NO OPERATOR NAM/ADD/PH DISPLAY': 'Failure to Display Operator Information',  'NO PARKING-DAY/TIME LIMITS': 'No Parking - Day/Time Limits',  'NO PARKING-EXC. AUTH. VEHICLE': 'No Parking - Except Authorized Vehicles',  'NO PARKING-EXC. HNDICAP PERMIT': 'No Parking - Except Disability Permit',  'NO PARKING-EXC. HOTEL LOADING': 'No Parking - Except Hotel Loading',  'NO PARKING-STREET CLEANING': 'No Parking - Street Cleaning',  'NO PARKING-TAXI STAND': 'No Parking - Taxi Stand',  'NO STANDING EXCP D/S': 'No Standing - Except Department of State',  'NO STANDING EXCP DP': 'No Standing - Except Diplomat',  'NO STANDING-BUS LANE': 'No Standing - Bus Lane',  'NO STANDING-BUS STOP': 'No Standing - Bus Stop',  'NO STANDING-COMM METER ZONE': 'No Standing - Commercial Meter Zone',
                          'NO STANDING-COMMUTER VAN STOP': 'No Standing - Commuter Van Stop',  'NO STANDING-DAY/TIME LIMITS': 'No Standing - Day/Time Limits',  'NO STANDING-EXC. AUTH. VEHICLE': 'No Standing - Except Authorized Vehicle',  'NO STANDING-EXC. TRUCK LOADING': 'No Standing - Except Truck Loading',  'NO STANDING-FOR HIRE VEH STOP': 'No Standing - For Hire Vehicle Stop',  'NO STANDING-HOTEL LOADING': 'No Standing - Hotel Loading',  'NO STANDING-OFF-STREET LOT': 'No Standing - Off-Street Lot',  'NO STANDING-SNOW EMERGENCY': 'No Standing - Snow Emergency',  'NO STANDING-TAXI STAND': 'No Standing - Taxi Stand',  'NO STD(EXC TRKS/GMTDST NO-TRK)': 'No Standing - Except Trucks in Garment District',  'NO STOP/STANDNG EXCEPT PAS P/U': 'No Stopping or Standing Except for Passenger Pick-Up',  'NO STOPPING-DAY/TIME LIMITS': 'No Stopping - Day/Time Limits',  'NON-COMPLIANCE W/ POSTED SIGN': 'Non-Compliance with Posted Sign',  'OBSTRUCTING DRIVEWAY': 'Obstructing Driveway',  'OBSTRUCTING TRAFFIC/INTERSECT': 'Obstructing Traffic or Intersection',  'OT PARKING-MISSING/BROKEN METR': 'Overtime Parking at Missing or Broken Meter',  'OTHER': 'Other',  'OVERNIGHT TRACTOR TRAILER PKG': 'Overnight Parking of Tractor Trailer',  'OVERTIME PKG-TIME LIMIT POSTED': 'Overtime Parking - Time Limit Posted',  'OVERTIME STANDING DP': 'Overtime Standing - Diplomat',  'OVERTIME STDG D/S': 'Overtime Standing - Department of State',  'PARKED BUS-EXC. DESIG. AREA': 'Bus Parking Outside of Designated Area',  'PEDESTRIAN RAMP': 'Blocking Pedestrian Ramp',  'PHTO SCHOOL ZN SPEED VIOLATION': 'School Zone Speed Camera Violation',  'PKG IN EXC. OF LIM-COMM MTR ZN': 'Parking in Excess of Limits - Commercial Meter Zone',  'PLTFRM LFTS LWRD POS COMM VEH': 'Commercial Vehicle Platform Lifts in Lowered Position',  'RAILROAD CROSSING': 'No Stopping - Railroad Crossing',  'REG STICKER-MUTILATED/C\'FEIT': 'Registration Sticker Mutilated or Counterfeit',  'REG. STICKER-EXPIRED/MISSING': 'Registration Sticker Expired or Missing',  'REMOVE/REPLACE FLAT TIRE': 'Replacing Flat Tire on Major Roadway',  'SAFETY ZONE': 'No Standing - Safety Zone',  'SELLING/OFFERING MCHNDSE-METER': 'Selling or Offering Merchandise From Metered Parking',  'SIDEWALK': 'Parked on Sidewalk',  'STORAGE-3HR COMMERCIAL': 'Street Storage of Commercial Vehicle Over 3 Hours',  'TRAFFIC LANE': 'No Stopping - Traffic Lane',  'TUNNEL/ELEVATED/ROADWAY': 'No Stopping in Tunnel or on Elevated Highway',  'UNALTERED COMM VEH-NME/ADDRESS': 'Commercial Plates on Unaltered Vehicle',  'UNALTERED COMM VEHICLE': 'Commercial Plates on Unaltered Vehicle',  'UNAUTHORIZED BUS LAYOVER': 'Bus Layover in Unauthorized Location',  'UNAUTHORIZED PASSENGER PICK-UP': 'Unauthorized Passenger Pick-Up',  'VACANT LOT': 'No Parking - Vacant Lot',  'VEH-SALE/WSHNG/RPRNG/DRIVEWAY': 'No Parking on Street to Wash or Repair Vehicle',  'VEHICLE FOR SALE(DEALERS ONLY)': 'No Parking on Street to Display Vehicle for Sale',  'VIN OBSCURED': 'Vehicle Identification Number Obscured',  'WASH/REPAIR VEHCL-REPAIR ONLY': 'No Parking on Street to Wash or Repair Vehicle',  'WRONG WAY': 'No Parking Opposite Street Direction'}

    # humanized names for violations
    HUMANIZED_NAMES_FOR_FISCAL_YEAR_DATABASE_VIOLATIONS = {'01': 'Failure to Display Bus Permit',  '1': 'Failure to Display Bus Permit',  '02': 'Failure to Display Operator Information',  '2': 'Failure to Display Operator Information',  '03': 'Unauthorized Passenger Pick-Up',  '3': 'Unauthorized Passenger Pick-Up',  '04': 'Bus Parking in Lower Manhattan - Exceeded 3-Hour limit',  '04A': 'Bus Parking in Lower Manhattan - Non-Bus',  '04B': 'Bus Parking in Lower Manhattan - No Permit',  '4': 'Bus Parking in Lower Manhattan - Exceeded 3-Hour limit',  '06': 'Overnight Parking of Tractor Trailer',  '6': 'Overnight Parking of Tractor Trailer',  '08': 'Idling',  '8': 'Idling',  '09': 'Obstructing Traffic or Intersection',  '9': 'Obstructing Traffic or Intersection',  '10': 'No Stopping or Standing Except for Passenger Pick-Up',  '11': 'No Parking - Except Hotel Loading',  '12': 'No Standing - Snow Emergency',  '13': 'No Standing - Taxi Stand',  '14': 'No Standing - Day/Time Limits',  '16': 'No Standing - Except Truck Loading/Unloading',  '16A': 'No Standing - Except Truck Loading/Unloading',  '17': 'No Parking - Except Authorized Vehicles',  '18': 'No Standing - Bus Lane',  '19': 'No Standing - Bus Stop',  '20': 'No Parking - Day/Time Limits',  '20A': 'No Parking - Day/Time Limits',  '21': 'No Parking - Street Cleaning',  '22': 'No Parking - Except Hotel Loading',  '23': 'No Parking - Taxi Stand',  '24': 'No Parking - Except Authorized Vehicles',  '25': 'No Standing - Commuter Van Stop',  '26': 'No Standing - For Hire Vehicle Stop',  '27': 'No Parking - Except Disability Permit',  '28': 'Overtime Standing - Diplomat',  '29': 'Altered Intercity Bus Permit',  '30': 'No Stopping/Standing',  '31': 'No Standing - Commercial Meter Zone',  '32': 'Overtime Parking at Missing or Broken Meter',  '32A': 'Overtime Parking at Missing or Broken Meter',  '33': 'Feeding Meter',  '35': 'Selling or Offering Merchandise From Metered Parking',  '37': 'Expired Meter',  '37': 'Expired Meter',  '38': 'Failure to Display Meter Receipt',  '38': 'Failure to Display Meter Receipt',  '39': 'Overtime Parking - Time Limit Posted',  '40': 'Fire Hydrant',  '42': 'Expired Meter - Commercial Meter Zone',  '42': 'Expired Meter - Commercial Meter Zone',  '43': 'Expired Meter - Commercial Meter Zone',  '44': 'Overtime Parking - Commercial Meter Zone',  '45': 'No Stopping - Traffic Lane',  '46': 'Double Parking',  '46A': 'Double Parking',  '46B': 'Double Parking - Within 100 ft. of Loading Zone',  '47': 'Double Parking - Midtown Commercial Zone',  '47A': 'Double Parking - Angle Parking',  '48': 'Blocking Bike Lane',  '49': 'No Stopping - Adjacent to Street Construction',  '50': 'Blocking Crosswalk',  '51': 'Parked on Sidewalk',  '52': 'No Stopping - Intersection',  '53': 'No Standing - Safety Zone',  '55': 'No Stopping in Tunnel or on Elevated Highway',  '56': 'No Stopping - Divided Highway',  '57': 'No Parking - Blue Zone',  '58': 'No Parking on Marginal Street or Waterfront',  '59': 'No Angle Parking',  '60': 'No Angle Parking',  '61': 'No Parking Opposite Street Direction',  '62': 'No Parking Beyond Marked Space',  '63': 'No Nighttime Standing or Parking in a Park',  '64': 'No Standing - Consul or Diplomat',  '65': 'Overtime Standing - Consul or Diplomat Over 30 Minutes',  '66': 'Detached Trailer',  '67': 'Blocking Pedestrian Ramp',  '68': 'Non-Compliance with Posted Sign',  '69': 'Failure to Display Meter Receipt',  '69': 'Failure to Display Meter Receipt',  '70': 'Registration Sticker Expired or Missing',  '70A': 'Registration Sticker Expired or Missing',  '70B': 'Improper Display of Registration',  '71': 'Inspection Sticker Expired or Missing',  '71A': 'Inspection Sticker Expired or Missing',  '71B': 'Improper Safety Sticker',  '72': 'Inspection Sticker Mutilated or Counterfeit',  '72A': 'Inspection Sticker Mutilated or Counterfeit',  '73': 'Registration Sticker Mutilated or Counterfeit',  '73A': 'Registration Sticker Mutilated or Counterfeit',  '74': 'Front or Back Plate Missing',  '74A': 'Improperly Displayed Plate',  '74B': 'Covered Plate',  '75': 'Plate and Sticker Do Not Match',  '77': 'Bus Parking Outside of Designated Area',  '78': 'Nighttime Parking on Residential Street - Commercial Vehicle',  '79': 'Bus Layover in Unauthorized Location',  '80': 'Missing Required Equipment',  '81': 'No Standing - Except Diplomat',  '82': 'Commercial Plates on Unaltered Vehicle',  '83': 'Improper Registration',  '84': 'Commercial Vehicle Platform Lifts in Lowered Position',  '85': 'Street Storage of Commercial Vehicle Over 3 Hours',  '86': 'Midtown Parking or Standing - 3 Hour Limit',  '89': 'No Standing - Except Trucks in Garment District',  '91': 'No Parking on Street to Display Vehicle for Sale',  '92': 'No Parking on Street to Wash or Repair Vehicle',  '93': 'Replacing Flat Tire on Major Roadway',  '96': 'No Stopping - Railroad Crossing',  '98': 'Obstructing Driveway',  '01-No Intercity Pmt Displ': 'Failure to Display Bus Permit',  '02-No operator N/A/PH': 'Failure to Display Operator Information',  '03-Unauth passenger pick-up': 'Unauthorized Passenger Pick-Up',  '04-Downtown Bus Area,3 Hr Lim': 'Bus Parking in Lower Manhattan - Exceeded 3-Hour limit',  '04A-Downtown Bus Area,Non-Bus': 'Bus Parking in Lower Manhattan - Non-Bus',  '04A-Downtown Bus Area, Non-Bus': 'Bus Parking in Lower Manhattan - Non-Bus', '04B-Downtown Bus Area,No Prmt': 'Bus Parking in Lower Manhattan - No Permit',
                          '06-Nighttime PKG (Trailer)': 'Overnight Parking of Tractor Trailer',  '08-Engine Idling': 'Idling',  '09-Blocking the Box': 'Obstructing Traffic or Intersection',  '10-No Stopping': 'No Stopping or Standing Except for Passenger Pick-Up',  '11-No Stand (exc hotel load)': 'No Parking - Except Hotel Loading',  '12-No Stand (snow emergency)': 'No Standing - Snow Emergency',  '13-No Stand (taxi stand)': 'No Standing - Taxi Stand',  '14-No Standing': 'No Standing - Day/Time Limits',  '16-No Std (Com Veh) Com Plate': 'No Standing - Except Truck Loading/Unloading',  '16A-No Std (Com Veh) Non-COM': 'No Standing - Except Truck Loading/Unloading',  '17-No Stand (exc auth veh)': 'No Parking - Except Authorized Vehicles',  '18-No Stand (bus lane)': 'No Standing - Bus Lane',  '19-No Stand (bus stop)': 'No Standing - Bus Stop',  '20-No Parking (Com Plate)': 'No Parking - Day/Time Limits',  '20A-No Parking (Non-COM)': 'No Parking - Day/Time Limits',  '21-No Parking (street clean)': 'No Parking - Street Cleaning',  '22-No Parking (exc hotel load)': 'No Parking - Except Hotel Loading',  '23-No Parking (taxi stand)': 'No Parking - Taxi Stand',  '24-No Parking (exc auth veh)': 'No Parking - Except Authorized Vehicles',  '25-No Stand (commutr van stop)': 'No Standing - Commuter Van Stop',  '26-No Stnd (for-hire veh only)': 'No Standing - For Hire Vehicle Stop',  '27-No Parking (exc handicap)': 'No Parking - Except Disability Permit',  '28-O/T STD,PL/Con,0 Mn, Dec': 'Overtime Standing - Diplomat',  '29-Altered Intercity bus pmt': 'Altered Intercity Bus Permit',  '30-No stopping/standing': 'No Stopping/Standing',  '31-No Stand (Com. Mtr. Zone)': 'No Standing - Commercial Meter Zone',  '32-Overtime PKG-Missing Meter': 'Overtime Parking at Missing or Broken Meter',  '32A Overtime PKG-Broken Meter': 'Overtime Parking at Missing or Broken Meter',  '33-Feeding Meter': 'Feeding Meter',  '35-Selling/Offer Merchandise': 'Selling or Offering Merchandise From Metered Parking',  '37-Expired Muni Meter': 'Expired Meter', '37-Expired Parking Meter': 'Expired Meter', '38-Failure to Display Muni Rec': 'Failure to Display Meter Receipt', '38-Failure to Dsplay Meter Rec': 'Failure to Display Meter Receipt', '39-Overtime PKG-Time Limt Post': 'Overtime Parking - Time Limit Posted',  '40-Fire Hydrant': 'Fire Hydrant',  '42-Exp. Muni-Mtr (Com. Mtr. Z)': 'Expired Meter - Commercial Meter Zone', '42-Exp Meter (Com Zone)': 'Expired Meter - Commercial Meter Zone', '43-Exp. Mtr. (Com. Mtr. Zone)': 'Expired Meter - Commercial Meter Zone',  '44-Exc Limit (Com. Mtr. Zone)': 'Overtime Parking - Commercial Meter Zone',  '45-Traffic Lane': 'No Stopping - Traffic Lane',  '46-Double Parking (Com Plate)': 'Double Parking',  '46A-Double Parking (Non-COM)': 'Double Parking',  '46B-Double Parking (Com-100Ft)': 'Double Parking - Within 100 ft. of Loading Zone',  '47-Double PKG-Midtown': 'Double Parking - Midtown Commercial Zone',  '47A-Angle PKG - Midtown': 'Double Parking - Angle Parking',  '48-Bike Lane': 'Blocking Bike Lane',  '49-Excavation (obstruct traff)': 'No Stopping - Adjacent to Street Construction',  '50-Crosswalk': 'Blocking Crosswalk',  '51-Sidewalk': 'Parked on Sidewalk',  '52-Intersection': 'No Stopping - Intersection',  '53-Safety Zone': 'No Standing - Safety Zone',  '55-Tunnel/Elevated Roadway': 'No Stopping in Tunnel or on Elevated Highway',  '56-Divided Highway': 'No Stopping - Divided Highway',  '57-Blue Zone': 'No Parking - Blue Zone',  '58-Marginal Street/Water Front': 'No Parking on Marginal Street or Waterfront',  '59-Angle PKG-Commer. Vehicle': 'No Angle Parking',  '60-Angle Parking': 'No Angle Parking',  '61-Wrong Way': 'No Parking Opposite Street Direction',  '62-Beyond Marked Space': 'No Parking Beyond Marked Space',  '63-Nighttime STD/PKG in a Park': 'No Nighttime Standing or Parking in a Park',  '64-No STD Ex Con/DPL,D/S Dec': 'No Standing - Consul or Diplomat',  '65-O/T STD,pl/Con,0 Mn,/S': 'Overtime Standing - Consul or Diplomat Over 30 Minutes',  '66-Detached Trailer': 'Detached Trailer',  '67-Blocking Ped. Ramp': 'Blocking Pedestrian Ramp',  '68-Not Pkg. Comp. w Psted Sign': 'Non-Compliance with Posted Sign',  '69-Failure to Disp Muni Recpt': 'Failure to Display Meter Receipt',  '69-Fail to Dsp Prking Mtr Rcpt': 'Failure to Display Meter Receipt', '70-Reg. Sticker Missing (NYS)': 'Registration Sticker Expired or Missing',  '70A-Reg. Sticker Expired (NYS)': 'Registration Sticker Expired or Missing',  '70B-Impropr Dsply of Reg (NYS)': 'Improper Display of Registration',  '71-Insp. Sticker Missing (NYS': 'Inspection Sticker Expired or Missing',  '71A-Insp Sticker Expired (NYS)': 'Inspection Sticker Expired or Missing',  '71B-Improp Safety Stkr (NYS)': 'Improper Safety Sticker',  '72-Insp Stkr Mutilated': 'Inspection Sticker Mutilated or Counterfeit',  '72A-Insp Stkr Counterfeit': 'Inspection Sticker Mutilated or Counterfeit',  '73-Reg Stkr Mutilated': 'Registration Sticker Mutilated or Counterfeit',  '73A-Reg Stkr Counterfeit': 'Registration Sticker Mutilated or Counterfeit',  '74-Missing Display Plate': 'Front or Back Plate Missing',  '74A-Improperly Displayed Plate': 'Improperly Displayed Plate',  '74B-Covered Plate': 'Covered Plate',  '75-No Match-Plate/Reg. Sticker': 'Plate and Sticker Do Not Match',  '77-Parked Bus (exc desig area)': 'Bus Parking Outside of Designated Area',  '78-Nighttime PKG on Res Street': 'Nighttime Parking on Residential Street - Commercial Vehicle',  '79-Bus Layover': 'Bus Layover in Unauthorized Location',  '80-Missing Equipment (specify)': 'Missing Required Equipment',  '81-No STD Ex C,&D Dec,30 Mn': 'No Standing - Except Diplomat',  '82-Unaltered Commerc Vehicle': 'Commercial Plates on Unaltered Vehicle',  '83-Improper Registration': 'Improper Registration',  '84-Platform lifts in low posit': 'Commercial Vehicle Platform Lifts in Lowered Position',  '85-Storage-3 hour Commercial': 'Street Storage of Commercial Vehicle Over 3 Hours',  '86-Midtown PKG or STD-3 hr lim': 'Midtown Parking or Standing - 3 Hour Limit',  '89-No Stand Exc Com Plate': 'No Standing - Except Trucks in Garment District',  '91-Veh for Sale (Dealer Only)': 'No Parking on Street to Display Vehicle for Sale',  '92-Washing/Repairing Vehicle': 'No Parking on Street to Wash or Repair Vehicle',  '93-Repair Flat Tire (Maj Road)': 'Replacing Flat Tire on Major Roadway',  '96-Railroad Crossing': 'No Stopping - Railroad Crossing',  '98-Obstructing Driveway': 'Obstructing Driveway',  'BUS LANE VIOLATION': 'Bus Lane Violation',  'FAILURE TO STOP AT RED LIGHT': 'Failure to Stop at Red Light',  'Field Release Agreement': 'Field Release Agreement',  'PHTO SCHOOL ZN SPEED VIOLATION': 'School Zone Speed Camera Violation'}

    MAX_RESULTS = 10_000

    MEDALLION_ENDPOINT = 'https://data.cityofnewyork.us/resource/rhe8-mgbb.json'
    MEDALLION_PATTERN = re.compile(r'^[0-9][A-Z][0-9]{2}$')

    OPEN_DATA_TOKEN = os.environ['NYC_OPEN_DATA_TOKEN']

    OPEN_PARKING_AND_CAMERA_VIOLATIONS_ENDPOINT = 'https://data.cityofnewyork.us/resource/uvbq-3m68.json'
    OPEN_PARKING_AND_CAMERA_VIOLATIONS_NEEDED_FIELDS = ['borough', 'county', 'fined', 'has_date',
      'issue_date', 'paid', 'precinct', 'outstanding', 'reduced', 'violation']
    OPEN_PARKING_AND_CAMERA_VIOLATIONS_FINE_KEYS = ['amount_due', 'fine_amount', 'interest_amount', 'payment_amount', 'penalty_amount', 'reduction_amount']

    PRECINCTS = {
        'manhattan': {
            'manhattan south': [1, 5, 6, 7, 9, 10, 13, 14, 17, 18],
            'manhattan north': [19, 20, 22, 23, 24, 25, 26, 28, 30, 32, 33, 34]
        },
        'bronx': {
            'bronx': [40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52]
        },
        'brooklyn': {
            'brooklyn south': [60, 61, 62, 63, 66, 67, 68, 69, 70, 71, 72, 76, 78],
            'brooklyn north': [73, 75, 77, 79, 81, 83, 84, 88, 90, 94]
        },
        'queens': {
            'queens south': [100, 101, 102, 103, 105, 106, 107, 113],
            'queens north': [104, 108, 109, 110, 111, 112, 114, 115]
        },
        'staten island': {
            'staten island': [120, 121, 122, 123]
        }
    }

    PRECINCTS_BY_BORO = {borough: [precinct for grouping in [precinct_list for bureau, precinct_list in regions.items(
    )] for precinct in grouping] for borough, regions in PRECINCTS.items()}


    def __init__(self, logger):
        # Set up retry ability
        s_req = requests_futures.sessions.FuturesSession(max_workers=9)

        retries = Retry(total=5,
                        backoff_factor=0.1,
                        status_forcelist=[403, 500, 502, 503, 504],
                        raise_on_status=False)

        s_req.mount('https://', HTTPAdapter(max_retries=retries))

        self.api = s_req

        self.logger = logger
        self.location_service = LocationService(logger)


    def lookup_vehicle(self, plate_lookup: PlateLookup) -> Dict[str, Any]:
        return self._perform_all_queries(plate_lookup=plate_lookup)


    def _add_fine_data_for_open_parking_and_camera_violations_summons(self, summons) -> Dict[str, Any]:
        for output_key in self.HOWS_MY_DRIVING_NY_FINE_KEYS:
            summons[output_key] = 0

        for fine_key in self.OPEN_PARKING_AND_CAMERA_VIOLATIONS_FINE_KEYS:
            if fine_key in summons:
                try:
                    amount = float(summons[fine_key])

                    if fine_key in ['fine_amount', 'interest_amount', 'penalty_amount']:
                        summons['fined'] += amount

                    elif fine_key == 'reduction_amount':
                        summons['reduced'] += amount

                    elif fine_key == 'amount_due':
                        summons['outstanding'] += amount

                    elif fine_key == 'payment_amount':
                        summons['paid'] += amount

                except ValueError as ve:

                    self.logger.error('Error parsing value into float')
                    self.logger.error(e)
                    self.logger.error(str(e))
                    self.logger.error(e.args)
                    logging.exception("stack trace")

                    pass

        return summons

    def _add_query_limit_and_token(self, url: str) -> str:
        return f'{url}&$limit={self.MAX_RESULTS}&$$app_token={self.OPEN_DATA_TOKEN}'

    def _calculate_aggregate_data(self, plate_lookup, violations) -> Dict[str, Any]:
        # Marshal all ticket data into form.
        fines = [
            ('fined',       sum(v['fined']
                                for v in violations.values() if v.get('fined'))),
            ('reduced',     sum(v['reduced']
                                for v in violations.values() if v.get('reduced'))),
            ('paid',        sum(v['paid']
                                for v in violations.values() if v.get('paid'))),
            ('outstanding', sum(v['outstanding']
                                for v in violations.values() if v.get('outstanding')))
        ]

        tickets = Counter([v['violation'] for v in violations.values(
        ) if v.get('violation')]).most_common()

        years = Counter([datetime.strptime(v['issue_date'], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y') if v.get(
            'has_date') else 'No Year Available' for v in violations.values()]).most_common()

        boroughs = Counter([v['borough'] for v in violations.values(
        ) if v.get('borough')]).most_common()


        camera_violations = ['Failure to Stop at Red Light', 'School Zone Speed Camera Violation']

        camera_streak_data = self._find_max_camera_violations_streak(sorted([datetime.strptime(v['issue_date'], '%Y-%m-%dT%H:%M:%S.%f') for v in violations.values(
        ) if v.get('violation') and v['violation'] in camera_violations]))

        result = {
            'boroughs': [{'title': k.title(), 'count': v} for k, v in boroughs],
            'fines': fines,
            'num_violations': len(violations),
            'plate': plate_lookup.plate,
            'plate_types': plate_lookup.plate_types,
            'state': plate_lookup.state,
            'violations': [{'title': k.title(), 'count': v} for k, v in tickets],
            'years': sorted([{'title': k.title(), 'count': v} for k, v in years], key=lambda k: k['title'])
        }

        # No need to add streak data if it doesn't exist
        if camera_streak_data:
            result['camera_streak_data'] = camera_streak_data

        return result


    def _find_max_camera_violations_streak(self, list_of_violation_times) -> Dict[str, Any]:
        if list_of_violation_times:
            max_streak = 0
            min_streak_date = None
            max_streak_date = None

            for date in list_of_violation_times:

                self.logger.debug("date: %s", date)

                year_later = date + \
                    (datetime(date.year + 1, 1, 1) - datetime(date.year, 1, 1))
                self.logger.debug("year_later: %s", year_later)

                year_long_tickets = [
                    comp_date for comp_date in list_of_violation_times if date <= comp_date < year_later]
                this_streak = len(year_long_tickets)

                if this_streak > max_streak:

                    max_streak = this_streak
                    min_streak_date = year_long_tickets[0]
                    max_streak_date = year_long_tickets[-1]

            return {
                'min_streak_date': min_streak_date.strftime('%B %-d, %Y'),
                'max_streak': max_streak,
                'max_streak_date': max_streak_date.strftime('%B %-d, %Y')
            }

        return {}


    def _normalize_fiscal_year_database_summons(self, summons) -> Dict[str, Any]:
        # get human readable ticket type name
        if summons.get('violation_description') is None:
            if summons.get('violation_code') and self.HUMANIZED_NAMES_FOR_FISCAL_YEAR_DATABASE_VIOLATIONS.get(summons['violation_code']):
                summons['violation'] = self.HUMANIZED_NAMES_FOR_FISCAL_YEAR_DATABASE_VIOLATIONS.get(
                    summons['violation_code'])
        else:
            if self.HUMANIZED_NAMES_FOR_FISCAL_YEAR_DATABASE_VIOLATIONS.get(summons['violation_description']):
                summons['violation'] = self.HUMANIZED_NAMES_FOR_FISCAL_YEAR_DATABASE_VIOLATIONS.get(
                    summons['violation_description'])
            else:
                summons['violation'] = re.sub(
                    '[0-9]*-', '', summons['violation_description'])

        if summons.get('issue_date') is None:
            summons['has_date'] = False
        else:
            try:
                summons['issue_date'] = datetime.strptime(
                    summons['issue_date'], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%S.%f')
                summons['has_date'] = True
            except ValueError as ve:
                summons['has_date'] = False

        if summons.get('violation_precinct'):
            boros = [boro for boro, precincts in self.PRECINCTS_BY_BORO.items() if int(
                summons['violation_precinct']) in precincts]
            if boros:
                summons['borough'] = boros[0]
            else:
                if summons.get('violation_county'):
                    boros = [name for name, codes in self.COUNTY_CODES.items(
                    ) if summons.get('violation_county') in codes]
                    if boros:
                        summons['borough'] = boros[0]
                else:
                    if summons.get('street_name'):
                        street_name = summons.get('street_name')
                        intersecting_street = summons.get(
                            'intersecting_street') or ''

                        geocoded_borough = self.location_service.get_borough_from_location_strings(
                            [street_name, intersecting_street])
                        if geocoded_borough:
                            summons['borough'] = geocoded_borough.lower()

        return summons


    def _normalize_open_parking_and_camera_violations_summons(self, summons) -> Dict[str, Any]:
        # get human readable ticket type name
        if summons.get('violation'):
            summons['violation'] = self.HUMANIZED_NAMES_FOR_OPEN_PARKING_AND_CAMERA_VIOLATIONS[
                summons['violation']]

        # normalize the date
        if summons.get('issue_date') is None:
            summons['has_date'] = False

        else:
            try:
                summons['issue_date'] = datetime.strptime(
                    summons['issue_date'], '%m/%d/%Y').strftime('%Y-%m-%dT%H:%M:%S.%f')
                summons['has_date'] = True
            except ValueError as ve:
                summons['has_date'] = False

        if summons.get('precinct'):
            boros = [boro for boro, precincts in self.PRECINCTS_BY_BORO.items() if int(
                summons['precinct']) in self.PRECINCTS]
            if boros:
                summons['borough'] = boros[0]
            else:
                if summons.get('county'):
                    boros = [name for name, codes in self.COUNTY_CODES.items(
                    ) if summons.get('county') in codes]
                    if boros:
                        summons['borough'] = boros[0]

        summons = self._add_fine_data_for_open_parking_and_camera_violations_summons(summons=summons)

        return summons


    def _perform_all_queries(self, plate_lookup: PlateLookup) -> Dict[str, Any]:
        # set up return data structure
        violations = {}


        result: Dict[str, bool] = self._perform_medallion_query(plate_lookup=plate_lookup)

        if result.get('error'):
            return result


        result = self._perform_open_parking_and_camera_violations_query(
            plate_lookup=plate_lookup, violations=violations)

        if result.get('error'):
            return result


        result = self._perform_fiscal_year_database_queries(
            plate_lookup=plate_lookup, violations=violations)

        if result.get('error'):
            return result


        for record in violations.values():
            if record.get('violation') is None:
                record['violation'] = "No Violation Description Available"

            if record.get('borough') is None:
                record['borough'] = 'No Borough Available'

        return self._calculate_aggregate_data(plate_lookup=plate_lookup,
            violations=violations)


    def _perform_fiscal_year_database_queries(self, plate_lookup, violations) -> Dict[str, bool]:
        """Grab data from each of the fiscal year violation datasets"""

        # iterate through the endpoints
        for year, endpoint in self.FISCAL_YEAR_DATABASE_ENDPOINTS.items():

            fiscal_year_database_query_string: str = (
                f"{endpoint}?"
                f"plate_id={plate_lookup.plate}&"
                f"registration_state={plate_lookup.state}"
                f"{'&$where=plate_type%20in(' + ','.join(['%27' + type + '%27' for type in plate_lookup.plate_types.split(',')]) + ')' if plate_lookup.plate_types is not None else ''}")

            fiscal_year_database_response: Dict[str, Any] = self._perform_query(
                  query_string=fiscal_year_database_query_string)

            if fiscal_year_database_response.get('error'):
                return fiscal_year_database_response

            if fiscal_year_database_response.get('data'):
                fiscal_year_database_data : List[str, str] = fiscal_year_database_response.get('data')

                self.logger.debug(
                    f'Fiscal year data for {plate_lookup.state}:{plate_lookup.plate}'
                    f'{":" + plate_lookup.plate_types if plate_lookup.plate_types else ""} for {year}: '
                    f'{fiscal_year_database_data}')

                for record in fiscal_year_database_data:
                    record = self._normalize_fiscal_year_database_summons(summons=record)

                    # structure response and only use the data we need
                    new_data: Dict[str, Any] = {needed_field: record.get(needed_field) for needed_field in self.FISCAL_YEAR_DATABASE_NEEDED_FIELDS}

                    if violations.get(record['summons_number']) is None:
                        violations[record['summons_number']] = new_data
                    else:
                        # Merge records together, treating fiscal year data as
                        # authoritative.
                        return_record = violations[record['summons_number']] = {**violations.get(record['summons_number']), **new_data}

                        # If we still don't have a violation (description) after merging records,
                        # record it as blank
                        if return_record.get('violation') is None:
                            return_record[
                                'violation'] = "No Violation Description Available"
                        if return_record.get('borough') is None:
                            record['borough'] = 'No Borough Available'

        return {'success': True}


    def _perform_medallion_query(self, plate_lookup: PlateLookup) -> Dict[str, bool]:
      if self.MEDALLION_PATTERN.search(plate_lookup.plate) != None:

          medallion_query_string: str = (
              f'{self.MEDALLION_ENDPOINT}?'
              f'license_number={plate_lookup.plate}')

          medallion_response: Dict[str, Any] = self._perform_query(query_string=medallion_query_string)

          if medallion_response.get('error'):
              return medallion_response

          if medallion_response.get('data'):
              medallion_data : List[str, Any] = medallion_response.get('data')

              self.logger.debug(
                  f'Medallion data for {plate_lookup.state}:{plate_lookup.plate}'
                  f'{medallion_data}')

              sorted_list: Dict[str, Any] = sorted(
                  set([res['dmv_license_plate_number'] for res in medallion_data]))
              plate_lookup.plate = sorted_list[-1] if sorted_list else plate_lookup.plate

      return {'success': True}


    def _perform_open_parking_and_camera_violations_query(self, plate_lookup, violations) -> Dict[str, bool]:
        """Grab data from 'Open Parking and Camera Violations'"""

        # response from city open data portal
        open_parking_and_camera_violations_query_string: str = (
            f'{self.OPEN_PARKING_AND_CAMERA_VIOLATIONS_ENDPOINT}?'
            f'plate={plate_lookup.plate}&'
            f'state={plate_lookup.state}'
            f"{'&$where=license_type%20in(' + ','.join(['%27' + type + '%27' for type in plate_lookup.plate_types.split(',')]) + ')' if plate_lookup.plate_types is not None else ''}")

        open_parking_and_camera_violations_response: Dict[str, Any] = self._perform_query(
            query_string=open_parking_and_camera_violations_query_string)

        if open_parking_and_camera_violations_response.get('error'):
            return open_parking_and_camera_violations_response

        if open_parking_and_camera_violations_response.get('data'):
            open_parking_and_camera_violations_data : List[str, str] = \
                open_parking_and_camera_violations_response.get('data')

            self.logger.debug(
                f'Open Parking and Camera Violations data for {plate_lookup.state}:{plate_lookup.plate}'
                f'{":" + plate_lookup.plate_types if plate_lookup.plate_types else ""}: '
                f'{open_parking_and_camera_violations_data}')

            # only data we're looking for
            opacv_desired_keys = self.OPEN_PARKING_AND_CAMERA_VIOLATIONS_NEEDED_FIELDS

            # add violation if it's missing
            for record in open_parking_and_camera_violations_data:

                record = self._normalize_open_parking_and_camera_violations_summons(summons=record)

                violations[record['summons_number']] = {
                    needed_field: record.get(needed_field) for needed_field in self.OPEN_PARKING_AND_CAMERA_VIOLATIONS_NEEDED_FIELDS}

        return {'success': True}


    def _perform_query(self, query_string: str) -> Dict[str, Any]:
        full_url: str = f'{self._add_query_limit_and_token(query_string)}'
        response = self.api.get(full_url)

        result = response.result()

        if result.status_code in range(200, 300):
            # Only attempt to read json on a successful response.
            return {'data': result.json()}
        elif result.status_code in range(300, 400):
            return {'error': 'redirect', 'url': full_url}
        elif result.status_code in range(400, 500):
            return {'error': 'user error', 'url': full_url}
        elif result.status_code in range(500, 600):
            return {'error': 'server error', 'url': full_url}
        else:
            return {'error': 'unknown error', 'url': full_url}
