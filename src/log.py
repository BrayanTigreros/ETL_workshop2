import datetime

def log_progress(message, log_file='etl_log.txt'): 
    timestamp_format = '%Y-%m-%d %H:%M:%S' # Year-Month-Day Hour-Minute-Second 
    now = datetime.datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 