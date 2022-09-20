import pandas as pd
from utils.find_common_parent import find_common_parent


TEST_CODES = [10129005, 10137002]

mapping_tsv_path = "resources/tls_Icd10cmHumanReadableMap_US1000124_20220301.tsv"
snomed_relation_tsv_path = "resources/sct2_Relationship_Snapshot_US1000124_20220301.txt"


class SNOMED_ICD10CM_Mapper():
    def __init__(self, mapping_table_path, snomed_relation_path):
        self.mapping_table_df = pd.read_csv(mapping_table_path, sep='\t')
        self.snomed_relation_df = pd.read_csv(snomed_relation_path, sep='\t')

    def get_term_parents(self, child_concept_id):
        parent_ids = []
        matched_concepts_df = self.snomed_relation_df[self.snomed_relation_df["sourceId"] == child_concept_id]
        for row_idx in range(len(matched_concepts_df)):
            parent_ids.append(matched_concepts_df.iloc[row_idx, 5])
        return parent_ids


    def find_matched_rows(self, parent_snomed_code):
        matched_concepts_df = self.mapping_table_df[self.mapping_table_df["referencedComponentId"] == parent_snomed_code]
        return matched_concepts_df


    def split_into_mapping_groups(self, matched_df):
        mapping_groups_dfs = []
        num_groups = matched_df.iloc[-1, 7]
        for group_num in range(num_groups):
            mapping_groups_dfs.append(matched_df[matched_df["mapGroup"] == group_num + 1])
        return mapping_groups_dfs


    def get_list_icd10cm_codes_helper(self, matched_df, child_snomed_code=None):


        # when we don't have detail info, for each mapping group,
        if child_snomed_code is None:
            icd10cm_codes = []

            # only 1 row, then 2 possibilities
            if len(matched_df) == 1:
                if matched_df.iloc[0, 10].startswith("ALWAYS"):
                    return [matched_df.iloc[0, 11]]
                # second case, no appropriate mapping
                else:
                    return []
            # more than 1 row, find common parent of the n-1 rows
            # go with max(len(common parent), len(default case))
            else:
                find_parent_success, common_parent = find_common_parent(matched_df.iloc[:-1, 11].to_list())
                default_coding = matched_df.iloc[-1, 11]
                # print(type(common_parent), type(default_coding))
                if find_parent_success:
                    # cannot be classified (e.g. 10129005 group 2)
                    if type(default_coding) is float:
                        return [common_parent]
                    return [default_coding] if len(default_coding) > len(common_parent) else [common_parent]
                else:
                    if type(default_coding) is float:
                        return []
                    return [default_coding]

        else:
            icd10cm_codes = []
            for row_idx in range(len(matched_df)):
                row = matched_df.iloc[row_idx, :].to_list()
                if str(child_snomed_code) in row[9]:
                    icd10cm_codes.append(row[11])
            return icd10cm_codes


    def map_snomedct_to_icd10cm(self, snomedct_code):
        #check parent
        parents_concept_id = self.get_term_parents(snomedct_code)
        parent_child_codes = []
        for parent_id in parents_concept_id:

            matched = self.find_matched_rows(parent_id)

            codes = self.get_list_icd10cm_codes_helper(matched, snomedct_code)
            if len(codes):
                parent_child_codes = [*parent_child_codes, *codes]

        # self
        matched = self.find_matched_rows(snomedct_code)
        if len(matched):
            mapping_groups_dfs = self.split_into_mapping_groups(matched)
            for mapping_group in mapping_groups_dfs:
                code = self.get_list_icd10cm_codes_helper(mapping_group)
                if len(code):
                    parent_child_codes = [*parent_child_codes, *code]
            parent_child_codes = list(set(parent_child_codes))
            return parent_child_codes
        return ""


if __name__ == "__main__":



    ids = [33028007]

    mapper = SNOMED_ICD10CM_Mapper(mapping_tsv_path, snomed_relation_tsv_path)
    for test_id in ids:



        print(mapper.map_snomedct_to_icd10cm(test_id))




