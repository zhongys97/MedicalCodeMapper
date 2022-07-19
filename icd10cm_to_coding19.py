import pandas as pd
import pickle
from utils.find_common_parent import find_common_parent

coding19_path = "resources/old_coding19.tsv"
tree_file_path = "resources/coding19_tree.pickle"


def get_node_id_from_coding(coding_df, coding_str):
    return coding_df[coding_df['coding'].str.match(coding_str)]['node_id'].values[0]

def get_all_high_level_codings(coding19_tree, detailed_node_id):
    all_relavant_codings = []
    cur_id = detailed_node_id
    while True:
        cur_node = coding19_tree.parent(cur_id)
        if cur_node.tag == "Root":
            break
        all_relavant_codings.append(cur_node.tag)
        cur_id = cur_node.identifier
    # print(all_relavant_codings)
    return all_relavant_codings



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

    coding19_df = pd.read_csv(coding19_path, sep='\t')
    coding19_codes = coding19_df.iloc[:, 0].to_list()

    with open(tree_file_path, "rb") as file:
        coding19_tree = pickle.load(file)

    # input icd10cm codes can either have the "." removed or in its original format with "." as the forth digit

    test_icd10cm_codes = ["H18.509", "H40.009", "T78.2XX"]
    mapped_coding19_codes = []
    for icd10cm_code in test_icd10cm_codes:
        parent_codes = []
        detailed_level_coding = icd10cm_coding19_mapper(icd10cm_code)
        detailed_level_node_id = get_node_id_from_coding(coding19_df, detailed_level_coding)
        all_codings = [detailed_level_coding, *get_all_high_level_codings(coding19_tree, detailed_level_node_id)]
        # print(all_codings)
        mapped_coding19_codes.append(all_codings)
    print(mapped_coding19_codes)



