from datetime import datetime

def convert_unix_time_to_human_time_string(unix_time: int) -> str:
    unix_time_secs = int(unix_time) / 1000
    human_time_str = datetime.utcfromtimestamp(unix_time_secs).strftime('%Y-%m-%d-%H-%M')
    return human_time_str

def convert_unix_time_to_day_of_week(unix_time: int) -> str:
    unix_time_secs = int(unix_time) / 1000
    return datetime.utcfromtimestamp(unix_time_secs).weekday()

def convert_unix_time_to_month(unix_time: int) -> str:
    unix_time_secs = int(unix_time) / 1000
    return datetime.utcfromtimestamp(unix_time_secs).month

def convert_unix_time_to_hour(unix_time: int) -> str:
    unix_time_secs = int(unix_time) / 1000
    return datetime.utcfromtimestamp(unix_time_secs).hour

def convert_unix_time_to_hour_quarter(unix_time: int) -> str:
    unix_time_secs = int(unix_time) / 1000
    minute_frac = datetime.utcfromtimestamp(unix_time_secs).minute / 60
    if minute_frac < 0.25:
        return 1
    elif minute_frac < 0.5:
        return 2
    elif minute_frac < 0.75:
        return 3
    else: 
        return 4