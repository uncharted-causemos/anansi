import requests
from dateutil.parser import parse


def stream_url(url):
    r = requests.get(url, stream=True)
    if r.encoding is None:
        r.encoding = 'utf-8'
    return r


def get_event_time(value):
    if value is None:
        return {}

    t = parse(value)
    return {
        "date": value,
        "day": t.day,
        "month": t.month,
        "year": t.year 
    }

