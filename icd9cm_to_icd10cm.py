import pandas as pd
from utils.find_common_parent import find_common_parent

MAP_REF_PATH_9CM_10CM = "resources/icd9cm_icd10cm_table.csv"


class ICD9CM_ICD10CM_Mapper():
    def __init__(self, mapping_table_path):
        self.full_mapping_df = pd.read_csv(mapping_table_path, dtype=str)

    def find_matched_rows(self, icd9cm_code):
        matched_df = self.full_mapping_df[self.full_mapping_df["ICD9CM"] == icd9cm_code]
        return matched_df

    def split_into_combinations(self, matched_df):

        third_digits = []
        for i in range(len(matched_df)):
            third_digits.append(matched_df.iloc[i, 2][2])
        third_digits = set(third_digits)

        comb_dfs = []
        for cur_comb in third_digits:
            comb_dfs.append(matched_df[matched_df["Flags"].str[2] == cur_comb])
        return comb_dfs


    def split_combination_into_scenarios(self, comb_df):
        forth_digits = []
        for i in range(len(comb_df)):
            forth_digits.append(comb_df.iloc[i, 2][3])
        forth_digits = set(forth_digits)

        scenarios_dfs = []
        for cur_scenario in forth_digits:
            scenarios_dfs.append(comb_df[comb_df["Flags"].str[3] == cur_scenario])
        return scenarios_dfs

    def split_scenario_into_choices(self, scenario_df):
        fifth_digits = []
        for i in range(len(scenario_df)):
            fifth_digits.append(scenario_df.iloc[i, 2][4])
        fifth_digits = set(fifth_digits)

        choices_dfs = []
        for cur_choice in fifth_digits:
            choices_dfs.append(scenario_df[scenario_df["Flags"].str[4] == cur_choice])
        return choices_dfs

    def get_icd10cm_codes(self, icd9cm_code):
        icd9cm_code = icd9cm_code.replace(".", "")
        matched_df = self.find_matched_rows(icd9cm_code)

        if len(matched_df) == 0:
            modified_matched_df = self.find_matched_rows(icd9cm_code + "0")
            if len(modified_matched_df) == 0:
                return False, []
            else:
                matched_df = modified_matched_df

        mapped_icd10cm_codes = []
        # only 1 row
        if len(matched_df) == 1:

            # case 1: unique complete mapping
            if matched_df.iloc[0, -1] == "00000" or matched_df.iloc[0, -1] == "10000":
                return True, [matched_df.iloc[0, 1]]

            # case 2: no mapping
            if matched_df.iloc[0, -1] == "11000":
                return False, []

        # more than one mapping possibility
        else:
            list_of_mapped_icd10_codes = []
            # break into list of comb dataframe
            comb_dfs = self.split_into_combinations(matched_df)
            for combination in comb_dfs:
                scenario_dfs = self.split_combination_into_scenarios(combination)
                for scenario in scenario_dfs:
                    choices = self.split_scenario_into_choices(scenario)
                    for choice in choices:


                        icd10cm_codes = choice["ICD10CM"].to_list()
                        find_parent_success, mapped_icd10cm_code = find_common_parent(icd10cm_codes)

                        if find_parent_success and len(mapped_icd10cm_code) < 3:
                            list_of_mapped_icd10_codes = [*list_of_mapped_icd10_codes, *icd10cm_codes]

                        elif find_parent_success and mapped_icd10cm_code not in list_of_mapped_icd10_codes:
                            list_of_mapped_icd10_codes.append(mapped_icd10cm_code)
            return True, list_of_mapped_icd10_codes



if __name__ == "__main__":

    ICD9CM_codes = ["533.3"]

    mapper = ICD9CM_ICD10CM_Mapper(MAP_REF_PATH_9CM_10CM)

    for icd9cm_code in ICD9CM_codes:
        map_success, mapped_code = mapper.get_icd10cm_codes(icd9cm_code)

        if map_success:
            print(mapped_code)
