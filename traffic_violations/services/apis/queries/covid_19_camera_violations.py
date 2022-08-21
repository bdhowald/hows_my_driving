import calendar
import datetime
import re
import urllib

from traffic_violations.constants.open_data import endpoints

DATE_FORMAT_STRING = '%m/%d/%Y'
ISSUE_DATE_TEMPLATE_STRING = "issue_date LIKE"
MONTHS_IN_YEAR = 12
STRIP_EXCESS_CHARACTERS_REGEX = r'\n\s+'


def get_covid_19_camera_violations_query(
    start_date: datetime.date,
    end_date: datetime.date
) -> str:
    if start_date > end_date:
        raise ValueError('start_date cannot come after end_date')

    select_clause = _format_query_string(_build_select_clause())
    where_clause = _format_query_string(_build_where_clause(start_date, end_date))
    group_clause = _format_query_string(_build_group_clause())
    order_clause = _format_query_string(_build_order_clause())

    return(
        f"{endpoints.OPEN_PARKING_AND_CAMERA_VIOLATIONS_ENDPOINT}?"
        f"{_format_query_string('&'.join([select_clause, where_clause, group_clause, order_clause]))}"
    )

def _build_group_clause() -> str:
    return """
        $group=plate,
               state
    """

def _build_issue_date_statement(date: datetime.date) -> str:
    formatted_date = date.strftime(DATE_FORMAT_STRING)
    return f"{ISSUE_DATE_TEMPLATE_STRING} '{formatted_date}'"

def _build_order_clause() -> str:
    return "$order=total_camera_violations desc"

def _build_select_clause() -> str:
    return """
        $select=plate,
                state,
                count(summons_number) AS total_camera_violations,
                sum(
                    CASE WHEN violation='PHTO SCHOOL ZN SPEED VIOLATION' THEN 1 ELSE 0 END
                ) AS speed_camera_count,
                sum(
                    CASE WHEN violation='FAILURE TO STOP AT RED LIGHT' THEN 1 ELSE 0 END
                ) AS red_light_camera_count
    """

def _build_where_clause(
    start_date: datetime.date,
    end_date: datetime.date
) -> str:

    return f"""
        $where=    violation in('PHTO SCHOOL ZN SPEED VIOLATION', 'FAILURE TO STOP AT RED LIGHT')
               and ({_get_issue_date_statements(start_date, end_date)})
    """

def _get_all_issue_date_statements_in_range(
  start_date: datetime.date,
  end_date: datetime.date
) -> str:
    issue_date_statements = []
    for day in range(start_date.day, end_date.day + 1):
        issue_date_statements.append(
            _build_issue_date_statement(datetime.date(start_date.year, start_date.month, day))
        )

    return issue_date_statements

def _get_issue_date_statements(
  start_date: datetime.date,
  end_date: datetime.date
) -> str:
    all_statements = []

    start_month_range = calendar.monthrange(start_date.year, start_date.month)
    end_month_range = calendar.monthrange(end_date.year, end_date.month)

    within_a_month = start_date.year == end_date.year and end_date.month == start_date.month
    within_a_year = start_date.year == end_date.year

    if within_a_month:
        all_statements.extend(_get_all_issue_date_statements_in_range(start_date, end_date))
    else:
        # handle start month
        if start_date.day == 1:
            # contains whole start month
            all_statements.append(
                f"{ISSUE_DATE_TEMPLATE_STRING} '{start_date.month}/__/{start_date.year}'"
            )
        else:
            # contains part of start month
            all_statements_for_start_month = _get_all_issue_date_statements_in_range(
                start_date, datetime.date(start_date.year, start_date.month, start_month_range[1])
            )
            all_statements.extend(all_statements_for_start_month)

        # handle end month
        if end_date.day == end_month_range[1]:
            # contains whole start month
            all_statements.append(
                f"{ISSUE_DATE_TEMPLATE_STRING} '{end_date.month}/__/{end_date.year}'"
            )
        else:
            # contains part of end month
            all_statements_for_end_month = _get_all_issue_date_statements_in_range(
                datetime.date(end_date.year, end_date.month, 1), end_date
            )
            all_statements.extend(all_statements_for_end_month)

        # handle months in the middle
        if within_a_year:
            for month_index in range(start_date.month + 1, end_date.month):
                padded_month = f"0{month_index}" if month_index < 10 else str(month_index)
                all_statements.append(
                    f"{ISSUE_DATE_TEMPLATE_STRING} '{padded_month}/__/{start_date.year}'"
                )
        else:
            # handle years in the middle
            if end_date.year - start_date.year >= 2:
                for year in range(start_date.year + 1, end_date.year):
                    all_statements.append(
                        f"{ISSUE_DATE_TEMPLATE_STRING} '__/__/{year}'"
                    )
            else:
                # handle months at end of start year (add one as months indexed from 1)
                for month_index in range(start_date.month + 1, MONTHS_IN_YEAR + 1):
                    padded_month = f"0{month_index}" if month_index < 10 else str(month_index)
                    all_statements.append(
                        f"{ISSUE_DATE_TEMPLATE_STRING} '{padded_month}/__/{start_date.year}'"
                    )

                # handle months at start of end year (add one as months indexed from 1)
                for month_index in range(1, end_date.month):
                    padded_month = f"0{month_index}" if month_index < 10 else str(month_index)
                    all_statements.append(
                        f"{ISSUE_DATE_TEMPLATE_STRING} '{padded_month}/__/{end_date.year}'"
                    )

    return ' OR '.join(all_statements)

def _format_query_string(raw_string: str) -> str:
    return re.sub(STRIP_EXCESS_CHARACTERS_REGEX, '', raw_string)