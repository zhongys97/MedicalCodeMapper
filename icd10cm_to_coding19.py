import pandas as pd
from utils.find_common_parent import find_common_parent

coding19_path = "resources/old_coding19.tsv"

coding19_df = pd.read_csv(coding19_path, sep='\t')

coding19_codes = coding19_df.iloc[:, 0].to_list()


def icd10cm_coding19_mapper(icd10cm_code):
    icd10cm_code = icd10cm_code.replace(".", "").replace(" ", "")
    if icd10cm_code in coding19_codes:
        return icd10cm_code
    for idx in range(1, len(icd10cm_code)):
        shortened_code = icd10cm_code[:(-1 * idx)]
        if shortened_code in coding19_codes:
            return shortened_code
    return "error"


if __name__ == "__main__":

    # input icd10cm codes can either have the "." removed or in its original format with "." as the forth digit

    test_icd10cm_codes = ["H18.509", "H40.009", "T78.2XX"]
    mapped_coding19_codes = []
    for icd10cm_code in test_icd10cm_codes:
        mapped_coding19_codes.append(icd10cm_coding19_mapper(icd10cm_code))
    print(mapped_coding19_codes)



