FISCAL_YEAR_DATABASE_NEEDED_FIELDS = ['borough', 'has_date', 'issue_date',
                                      'summons_number', 'violation',
                                      'violation_precinct', 'violation_county']

OPEN_PARKING_AND_CAMERA_VIOLATIONS_NEEDED_FIELDS = ['borough', 'county', 'fined',
                                                    'has_date', 'issue_date', 'paid',
                                                    'precinct', 'outstanding', 'reduced',
                                                    'summons_number', 'violation']

OPEN_PARKING_AND_CAMERA_VIOLATIONS_FINE_KEYS = ['amount_due', 'fine_amount',
                                                'interest_amount', 'payment_amount',
                                                'penalty_amount', 'reduction_amount']
