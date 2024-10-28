from sys import argv
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from os import listdir, pathsep

in_directory = argv[2]
if len(argv) < 4:
    out_directory = in_directory
else:
    out_directory = argv[3]

file_contents = []

for file in listdir(in_directory):
    if file.endswith(".raftlog"):
        # Read the JSON
        df = pd.read_json(file)
        file_contents.append(df)

        # Get the server ID
        server_id, _ = file.split("_", maxsplit=1)

        # Create column of server ID
        id_col = [server_id] * len(df)
        df["serverId"] = id_col


joined_df = pd.concat(file_contents)
joined_df.to_csv(f"data_{argv[1]}.csv")