import pandas as pd
from utils.find_common_parent import find_common_parent

mapping_file_path = "resources/icd9cm_icd10cm_table.csv"



def find_matched_rows(full_mapping_df, icd9cm_code):
    matched_df = full_mapping_df[full_mapping_df["ICD9CM"] == icd9cm_code]
    return matched_df


def split_into_combinations(matched_df):

    third_digits = []
    for i in range(len(matched_df)):
        third_digits.append(matched_df.iloc[i, 2][2])
    third_digits = set(third_digits)

    comb_dfs = []
    for cur_comb in third_digits:
        comb_dfs.append(matched_df[matched_df["Flags"].str[2] == cur_comb])
    return comb_dfs


def split_combination_into_scenarios(comb_df):
    forth_digits = []
    for i in range(len(comb_df)):
        forth_digits.append(comb_df.iloc[i, 2][3])
    forth_digits = set(forth_digits)

    scenarios_dfs = []
    for cur_scenario in forth_digits:
        scenarios_dfs.append(comb_df[comb_df["Flags"].str[3] == cur_scenario])
    return scenarios_dfs


def split_scenario_into_choices(scenario_df):
    fifth_digits = []
    for i in range(len(scenario_df)):
        fifth_digits.append(scenario_df.iloc[i, 2][4])
    fifth_digits = set(fifth_digits)

    choices_dfs = []
    for cur_choice in fifth_digits:
        choices_dfs.append(scenario_df[scenario_df["Flags"].str[4] == cur_choice])
    return choices_dfs



def get_icd10cm_codes(matched_df):
    if len(matched_df) == 0:
        return False, []

    mapped_icd10cm_codes = []
    # only 1 row
    if len(matched_df) == 1:

        # case 1: unique complete mapping
        if matched_df.iloc[0, -1] == "00000":
            return True, [matched_df.iloc[0, 1]]

        # case 2: no mapping
        if matched_df.iloc[0, -1] == "11000":
            return False, []

    # more than one mapping possibility

    else:
        list_of_mapped_icd10_codes = []
        # break into list of comb dataframe
        comb_dfs = split_into_combinations(matched_df)
        for combination in comb_dfs:
            scenario_dfs = split_combination_into_scenarios(combination)
            for scenario in scenario_dfs:
                choices = split_scenario_into_choices(scenario)
                for choice in choices:

                    icd10cm_codes = choice["ICD10CM"].to_list()
                    find_parent_success, mapped_icd10cm_code = find_common_parent(icd10cm_codes)
                    if find_parent_success and mapped_icd10cm_code not in list_of_mapped_icd10_codes:
                        list_of_mapped_icd10_codes.append(mapped_icd10cm_code)
        return True, list_of_mapped_icd10_codes



if __name__ == "__main__":

    full_mapping_df = pd.read_csv(mapping_file_path, dtype=str)

    matched = find_matched_rows(full_mapping_df, "80199")
    map_success, mapped_code = get_icd10cm_codes(matched)
    if map_success:
        print(mapped_code)
