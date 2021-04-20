#!/usr/bin/env python3
import sys, os, signal, pathlib
import datetime
import boto3

"""
Downloads statements.json and metadata.json into a folder as per key-ed in S3
This assumes you have aws secrets and tokens setup under the wmuser:
e.g. ~/.aws/credentials
"""


root_dir = pathlib.Path(os.path.abspath(__file__)).parent.parent 
default_save_dir = "data/indra/" 

# Handling ctrl+c exit
def exit_handle(signum, frame):
    sys.exit(0)

def toMB(v):
    return str(round((v / (1024 * 1024)), 2)) + "MB"

signal.signal(signal.SIGINT, exit_handle)
if __name__ == "__main__":
    session = boto3.Session(
        profile_name='wmuser'
    )

    # Get resources and listing of objects
    s3 = session.resource("s3")
    wm_bucket = s3.Bucket("world-modelers")
    objects = wm_bucket.objects.filter(Prefix="indra_models")

    # Interactive session starts
    while True:
        # Format
        line = " | ".join(["Id".ljust(5), "Key".ljust(60), "Size".rjust(20), "Last Modified".rjust(30)])
        print(line)
        print("-" * 130)

        # Filter out old INDRA assemblies
        filtered_objects = {}
        for idx, obj in enumerate(objects):
            if (obj.last_modified).replace(tzinfo = None) > datetime.datetime(2020, 10, 1, tzinfo = None):

                if not obj.key.endswith("json"):
                    continue
                
                collection_key = obj.key.split("/")[1]

                if collection_key not in filtered_objects:
                    filtered_objects[collection_key] = {}
                    filtered_objects[collection_key]["key"] = collection_key 

                fo = filtered_objects[collection_key] 
                if obj.key.endswith("statements.json"):
                    fo["statements"] = obj
                    fo["size"] = obj.size
                    fo["last_modified"] = obj.last_modified
                elif obj.key.endswith("metadata.json"):
                    fo["metadata"] = obj

        # Display
        keys = []
        for i, (k, v) in enumerate(filtered_objects.items()):
            keys.append(v)
            line = " | ".join([str(i).ljust(5), k.ljust(60), toMB(v["size"]).rjust(20), str(v["last_modified"]).rjust(30)])
            print(line)
        
        # User input
        print("")
        print("[ctrl + c to exit]")
        target_cmd = input("Enter Id to download: ")

        target = keys[int(target_cmd)]

        for k in ["statements", "metadata"]:
            obj = target[k]
            print(obj.key)
            out_path = os.path.join(target["key"], k + ".json")
            dir_path = pathlib.Path(out_path).parent
            dir_path.mkdir(parents=True, exist_ok=True)
            wm_bucket.download_file(obj.key, out_path)

        print("Done")
        print("\n\n")
