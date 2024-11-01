from sys import argv
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from os import listdir, pathsep, walk, path
import re

def has_logfiles(dir : str):
    print(f"Checking for unprocessed logfiles in {dir}")
    logfiles = [file for file in listdir(dir) if file.endswith(".raftlog")]
    csv_files = [file for file in listdir(dir) if file.endswith(".csv")]
    return len(logfiles) > 0 and len(csv_files) == 0

def process_all_subdirs (root_dir : str):
    for root, dirs, files in walk(root_dir):
        for dir in dirs:
            dir_path = path.join(root, dir)
            if (has_logfiles(dir_path)):
                process_dir(dir_path, dir_path)

def process_dir(in_directory : str, out_directory : str, tag : str = None):
    if tag is None:
        tag = path.basename(path.normpath(in_directory))
    print(f"Found logfiles in {in_directory}.\nWriting output to {out_directory}.\nTagging with {tag}.")

    file_contents = []

    for file in listdir(in_directory):
        if file.endswith(".raftlog"):
            # Read the JSON
            file_path = path.join(in_directory, file)
            print(f"Reading JSON file {file_path}")
            try :
                df = pd.read_json(file_path)
                file_contents.append(df)

                # Get the server ID
                server_id, _ = file.split("_", maxsplit=1)

                # Create column of server ID
                id_col = [server_id] * len(df)
                df["serverId"] = id_col
            except ValueError:
                print(f"Could not read {file_path}. Skipping run.")
                return


    joined_df = pd.concat(file_contents)
    joined_df.to_csv(f"{out_directory}/data_{tag}.csv")

if __name__ == "__main__":
    process_all_subdirs(path.abspath(path.normpath(argv[1])))