import time
import json
import requests
from dateutil.parser import parse
from smart_open import open


def stream_url(url):
    r = requests.get(url, stream=True)
    if r.encoding is None:
        r.encoding = 'utf-8'
    return r

def json_file_content(filename):
    content = None
    with open(filename, 'r') as F:
        content = F.read()
        content = json.loads(content)
    return content


def epoch_millis():
    return round(time.time() * 1000)
      
def get_event_time(value):
    if value is None:
        return {}

    t = parse(value)
    return {
        "date": round(t.timestamp() * 1000),
        "day": t.day,
        "month": t.month,
        "year": t.year 
    }

