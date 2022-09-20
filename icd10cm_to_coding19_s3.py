import pandas as pd
import pickle
import numpy as np
import boto3
import io
from utils.find_common_parent import find_common_parent

CODING19_PATH = "resources/old_coding19.tsv"
TREE_FILE_PATH = "resources/coding19_tree.pickle"


class ICD10CM_Coding19_Mapper():
    def __init__(self, coding19_csv_path, coding19_tree_hierarchy_path=None, coding19_tree_hierarchy_stream=None,
                 is_stream=False):
        self.coding19_df = pd.read_csv(coding19_csv_path, sep='\t')
        self.all_coding19_codes = self.coding19_df.iloc[:, 0].to_list()

        if not is_stream:
            with open(coding19_tree_hierarchy_path, "rb") as file:
                self.coding19_tree = pickle.load(file)
        else:
            self.coding19_tree = pickle.loads(coding19_tree_hierarchy_stream)

    def get_node_id_from_coding(self, coding_str):
        return self.coding19_df[self.coding19_df['coding'].str.match(coding_str)]['node_id'].values[0]

    def get_all_high_level_codings(self, detailed_node_id):
        all_relavant_codings = []
        cur_id = detailed_node_id
        while True:
            cur_node = self.coding19_tree.parent(cur_id)
            if cur_node.tag == "Root":
                break
            all_relavant_codings.append(cur_node.tag)
            cur_id = cur_node.identifier
        # print(all_relavant_codings)
        return all_relavant_codings

    def map_detail_code_icd10cm_coding19(self, icd10cm_code):
        icd10cm_code = icd10cm_code.replace(".", "").replace(" ", "")
        if icd10cm_code in self.all_coding19_codes:
            return True, icd10cm_code
        for idx in range(1, len(icd10cm_code)):
            shortened_code = icd10cm_code[:(-1 * idx)]
            if shortened_code in self.all_coding19_codes:
                return True, shortened_code
        return False, ""

    def map_all_relevant_icd10cm_coding19(self, icd10cm_code):
        mapping_success, detailed_level_coding = self.map_detail_code_icd10cm_coding19(icd10cm_code)
        if mapping_success:
            detailed_level_node_id = self.get_node_id_from_coding(detailed_level_coding)
            all_codings = [detailed_level_coding, *self.get_all_high_level_codings(detailed_level_node_id)]
            return all_codings
        return []

    def indices_for_19k_vec_from_desc(self, list_of_descriptions):
        # index starting from 0
        indices = []
        for des in list_of_descriptions:
            idx = self.coding19_df["coding"][self.coding19_df["coding"] == des].index.tolist()[0]
            indices.append(idx)
        return indices


if __name__ == "__main__":

    test_icd10cm_codes = ["H18.509", "H40.009", "T78.2XX"]

    s3 = boto3.client('s3')
    bucket = 's3demo-yishan'

    coding19_path_response = s3.get_object(Bucket=bucket, Key='mapping_pipeline/' + CODING19_PATH)

    coding19_tree_response = s3.get_object(Bucket=bucket, Key='mapping_pipeline/' + TREE_FILE_PATH)

    mapper = ICD10CM_Coding19_Mapper(coding19_csv_path=coding19_path_response['Body'],
                                     coding19_tree_hierarchy_stream=coding19_tree_response['Body'].read(),
                                     is_stream=True)

    for icd10cm_code in test_icd10cm_codes:
        descriptions = mapper.map_all_relevant_icd10cm_coding19(icd10cm_code)
        indices = mapper.indices_for_19k_vec_from_desc(descriptions)
        print(indices)







