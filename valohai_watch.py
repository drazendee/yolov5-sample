import os
from pathlib import Path

import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

import csv
import json

VH_OUTPUTS_DIR = os.getenv('VH_OUTPUTS_DIR')
metric_output_dir = os.path.join(VH_OUTPUTS_DIR)

class ValohaiHandler(PatternMatchingEventHandler):
    def on_modified(self, event):
        print(f'event type: {event.event_type}  path : {event.src_path}')
        if('.csv' in event.src_path): 
            with open(event.src_path, "r") as file:
                data = list(csv.reader(file, delimiter=','))

                keys = data[0]
                latest_values = data[-1]

                metadata = {}

                for i in range(len(keys)) :
                    key = keys[i].strip()
                    value = latest_values[i].strip()
                    
                    metadata[key] = value

                print(json.dumps(metadata))

if __name__ == "__main__":
    event_handler = ValohaiHandler(patterns=['*.csv'])
    observer = Observer()
    observer.schedule(event_handler, path=metric_output_dir, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()