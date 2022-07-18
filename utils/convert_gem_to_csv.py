import pandas as pd

file_path = "2018_I9gem.txt"

with open(file_path, "r") as file:
    lines = file.readlines()
    sources = []
    destinations = []
    flags = []
    for line in lines:
        sources.append(line[:6].replace(" ", ""))
        destinations.append(line[6:14].replace(" ", ""))
        flags.append(line[14:-1].replace(" ", ""))

    df = pd.DataFrame({"ICD9CM": sources, "ICD10CM": destinations, "Flags": flags})



    df.to_csv("icd9cm_icd10cm_table.csv", index=False)
