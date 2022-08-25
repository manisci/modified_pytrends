from datetime import date, timedelta
from functools import partial
from time import sleep
from calendar import monthrange
import random
import pandas as pd

from pytrends.exceptions import ResponseError
from pytrends.request import TrendReq as UTrendReq

# from pytrends.request import TrendReq

GET_METHOD='get'


class TrendReq(UTrendReq):
    def _get_data(self, url, method=GET_METHOD, trim_chars=0, **kwargs):
        return super()._get_data(url, method=GET_METHOD, trim_chars=trim_chars, headers=headers, **kwargs)

headers = {
    'authority': 'trends.google.com',
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9,fr;q=0.8,fa-IR;q=0.7,fa;q=0.6,fr-CA;q=0.5,fr-FR;q=0.4',
    # Requests sorts cookies= alphabetically
    # 'cookie': '__utmc=10102256; __utmz=10102256.1659457449.2.2.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); __utma=10102256.306050207.1659373112.1660921829.1661178713.19; __utmt=1; __utmb=10102256.2.10.1661178713; SID=MwhOF1_UD51tfGg-neLl1VKD4TUyBbOtYDSmSpV6qrI4tNZmBfpYw0vIJUF49Be_THtThw.; __Secure-1PSID=MwhOF1_UD51tfGg-neLl1VKD4TUyBbOtYDSmSpV6qrI4tNZmm5c1pdopOdpVfasfYOSg2A.; __Secure-3PSID=MwhOF1_UD51tfGg-neLl1VKD4TUyBbOtYDSmSpV6qrI4tNZmRp7upOMbkPwSmt3AZ3RbfA.; HSID=Aq5n7VfbZrg47c1OH; SSID=AdAPt40UvkPXtsj6V; APISID=oeAdWOruFLKdxAG2/A-epBgeL4R0f2-f_3; SAPISID=jJOI67_EfR6VfD4C/A1l5j3kjzqAYohbSW; __Secure-1PAPISID=jJOI67_EfR6VfD4C/A1l5j3kjzqAYohbSW; __Secure-3PAPISID=jJOI67_EfR6VfD4C/A1l5j3kjzqAYohbSW; __Secure-ENID=6.SE=aTNv_e0QWMZviwacy7_7tGB4u37uzewNWyRFO1Srj_IPgXMYGbG50W3oYLWvj8DuBl-00zXWFGGWn67_eWOy-7M4F6p53YEG-4yJvo7jWM5643E6lUGuvtWlNPpQsBHa7Xi55HFwumFKeSU-7YAtOdEyQxNpPhinvJe7E_q_Kro; S=billing-ui-v3=eBHmI9CyIZ7XMmMPSWLG77K5p7BzsVw6:billing-ui-v3-efe=eBHmI9CyIZ7XMmMPSWLG77K5p7BzsVw6; NID=511=BTFyjUcm2zY8uXXW06U5DjzOlYmXPnSCYsI2CRKqRQ3ZPBcxj3chGkcsHLe__eI_1kABZ7NlX1I3SuUWGrwwq0asVH0uWpplVVCVHhJxWOQHK-7DvxFC5De86o688uE1C1PwwaxJ-xWJXeXyKHgDtZO_D3G_whe6S9rto8zgxdgwpxJsUATA3zYaTLBDqileux5BKzNy17uIhfkwI-mwYdlul-nEtGsknzTJRV2zKdtoz1UmVzN4sq3HYnQamA83748CayHAe5CoIoQnh5JaDTTgOJNM_YRlwOCTL4CznJDy1Q3xE4sQkBmnFhVTExTxGjoGj_UO_-DBivDZ720; 1P_JAR=2022-08-22-13; SEARCH_SAMESITE=CgQImpYB; AEC=AakniGNFXSGQsAOV0P3JUBdesmXolm2Z442u8QI0Oz5_kp_aNDOlbwxdmhU; SIDCC=AEf-XMSOp-PCFXeQIr95jiJeMFIMQU9Vfg4m4lcfxFfkvPZMN_IbCo3NabwQ5BM3cQVrgOS4PTMU; __Secure-1PSIDCC=AEf-XMQWpLBdD7VE6HEW6nR0MIP0TK_zcL2VyUiDJLmiEABN1Q2oVBDB_0_Ss2wEskYCqrVMYng; __Secure-3PSIDCC=AEf-XMSZeZ3UqX4EyoAlt6B0bDYXNJ9YZk0lYnRzeagE0-x2v9lezMcvUAJBE1tWcHAJaX5bimP4',
    'referer': 'https://trends.google.com/trends/explore?geo=CA&q=beach',
    'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    'x-client-data': 'CIe2yQEIprbJAQipncoBCNCgygEIyujKAQiUocsBCLy0zAEI9cDMAQiywcwBCMTBzAEI1sHMAQjcxMwBCJ3JzAEI48vMAQ==',
}


def get_last_date_of_month(year: int, month: int) -> date:
    """Given a year and a month returns an instance of the date class
    containing the last day of the corresponding month.

    Source: https://stackoverflow.com/questions/42950/get-last-day-of-the-month-in-python
    """
    return date(year, month, monthrange(year, month)[1])


def convert_dates_to_timeframe(start: date, stop: date) -> str:
    """Given two dates, returns a stringified version of the interval between
    the two dates which is used to retrieve data for a specific time frame
    from Google Trends.
    """
    return f"{start.strftime('%Y-%m-%d')} {stop.strftime('%Y-%m-%d')}"


def _fetch_data(pytrends, build_payload, timeframe: str) -> pd.DataFrame:
    """Attempts to fecth data and retries in case of a ResponseError."""
    attempts, fetched = 0, False
    while not fetched:
        try:
            build_payload(timeframe=timeframe)
        except ResponseError as err:
            print(err)
            print(f'Trying again in {60 + 5 * attempts} seconds.')
            sleep(60 + 5 * attempts)
            attempts += 1
            if attempts > 3:
                print('Failed after 3 attemps, abort fetching.')
                break
        else:
            fetched = True
    return pytrends.interest_over_time()


def get_daily_data(word: str,
                 start_year: int,
                 start_mon: int,
                 stop_year: int,
                 stop_mon: int,
                 geo: str = 'US',
                 verbose: bool = True,
                 wait_time: float = 5.0) -> pd.DataFrame:
    """Given a word, fetches daily search volume data from Google Trends and
    returns results in a pandas DataFrame.

    Details: Due to the way Google Trends scales and returns data, special
    care needs to be taken to make the daily data comparable over different
    months. To do that, we download daily data on a month by month basis,
    and also monthly data. The monthly data is downloaded in one go, so that
    the monthly values are comparable amongst themselves and can be used to
    scale the daily data. The daily data is scaled by multiplying the daily
    value by the monthly search volume divided by 100.
    For a more detailed explanation see http://bit.ly/trendsscaling

    Args:
        word (str): Word to fetch daily data for.
        start_year (int): the start year
        start_mon (int): start 1st day of the month
        stop_year (int): the end year
        stop_mon (int): end at the last day of the month
        geo (str): geolocation
        verbose (bool): If True, then prints the word and current time frame
            we are fecthing the data for.

    Returns:
        complete (pd.DataFrame): Contains 4 columns.
            The column named after the word argument contains the daily search
            volume already scaled and comparable through time.
            The column f'{word}_unscaled' is the original daily data fetched
            month by month, and it is not comparable across different months
            (but is comparable within a month).
            The column f'{word}_monthly' contains the original monthly data
            fetched at once. The values in this column have been backfilled
            so that there are no NaN present.
            The column 'scale' contains the scale used to obtain the scaled
            daily data.
    """

    # Set up start and stop dates
    start_date = date(start_year, start_mon, 1) 
    stop_date = get_last_date_of_month(stop_year, stop_mon)

    # Start pytrends for US region
    # hl='en-US', tz=360
    pytrends = TrendReq()
    # Initialize build_payload with the word we need data for
    build_payload = partial(pytrends.build_payload,
                            kw_list=[word], cat=0, geo=geo, gprop='')

    # Obtain monthly data for all months in years [start_year, stop_year]
    monthly = _fetch_data(pytrends, build_payload,
                         convert_dates_to_timeframe(start_date, stop_date))

    # Get daily data, month by month
    results = {}
    # if a timeout or too many requests error occur we need to adjust wait time
    current = start_date
    while current < stop_date:
        last_date_of_month = get_last_date_of_month(current.year, current.month)
        timeframe = convert_dates_to_timeframe(current, last_date_of_month)
        if verbose:
            print(f'{word}:{timeframe}')
        results[current] = _fetch_data(pytrends, build_payload, timeframe)
        current = last_date_of_month + timedelta(days=1)
        sleep(random.randint(int(1 * wait_time), int(5 * wait_time)))  # don't go too fast or Google will send 429s

    daily = pd.concat(results.values()).drop(columns=['isPartial'])
    complete = daily.join(monthly, lsuffix='_unscaled', rsuffix='_monthly')

    # Scale daily data by monthly weights so the data is comparable
    complete[f'{word}_monthly'].ffill(inplace=True)  # fill NaN values
    complete['scale'] = complete[f'{word}_monthly'] / 100
    complete[word] = complete[f'{word}_unscaled'] * complete.scale

    return complete
