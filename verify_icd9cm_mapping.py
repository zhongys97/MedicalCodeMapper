import numpy as np
import pandas as pd
from icd9cm_to_icd10cm import *
from icd10cm_to_coding19 import *

MAP_REF_PATH_9CM_10CM = "resources/icd9cm_icd10cm_table.csv"
CODING19_PATH = "resources/old_coding19.tsv"
TREE_FILE_PATH = "resources/coding19_tree.pickle"

ICD9CM_ICD10CM_LABELS_PATH = "./resources/verification/emory_9cm_to_10cm.csv"

def get_after_exclaimation(code_string):
    try:
        start_index = code_string.index("!")
        return True, code_string[start_index+1:]
    except:
        return False, ""


def get_icd10cm_labels_from_icd9cm(icd9cm_icd10cm_labels_df, icd9cm_code):
    icd9cm_code_long = "ICD9CM!" + icd9cm_code
    matched = icd9cm_icd10cm_labels_df[icd9cm_icd10cm_labels_df["ICD9CM"] == icd9cm_code_long]
    if len(matched) > 0:
        icd10cm_codes_long = set(matched["ICD10CM"].to_list())

        icd10cm_codes = []
        for icd10cm_code_long in icd10cm_codes_long:
            success, icd10cm_code = get_after_exclaimation(icd10cm_code_long)
            if success:
                icd10cm_codes.append(icd10cm_code)
        return True, icd10cm_codes
    else:
        return False, []


def get_all_labeled_icd9cm_inputs(labels_df):
    all_icd9cm_long = set(labels_df["ICD9CM"].to_list())
    icd9cm_codes = []
    for long_code in all_icd9cm_long:
        success, icd9cm_code = get_after_exclaimation(long_code)
        if success:
            icd9cm_codes.append(icd9cm_code)

    with open("icd9cm_labeled_inputs.txt", 'w') as file:
        for input in icd9cm_codes:
            if len(input) <= 8:
                file.write(input)
                file.write("\n")


def get_mapping_quality_analysis(my_mapping, clinician_labels, coding19_mapper):
    """
    exact mapping: return 0

    label more general: return 1
    label more general up 1 level: return 2
    label more general up 2 level: return 3
    label more general up 3 levels: return 4
    questionable mapping, my mapping is not a close ancestor of labeled code: return 5
    wrong mapping, my mapping has no overlap with the label based on coding 19
    """
    if len(my_mapping) == 0:
        # print("Wrong", my_mapping, clinician_labels)
        return 6

    clinician_labels_dot_removed = []
    for clinician_label in clinician_labels:
        clinician_labels_dot_removed.append(clinician_label.replace(".", ""))
    clinician_labels = clinician_labels_dot_removed

    if my_mapping == clinician_labels:
        # print("Exact", my_mapping, clinician_labels)
        return 0

    checked_intersect = False
    for my_mapped_code in my_mapping:
        for clinician_label in clinician_labels:
            if my_mapped_code[:-3] not in clinician_label:
                # print("Wrong", my_mapping, clinician_labels)
                my_all_relevant = set(coding19_mapper.map_all_relevant_icd10cm_coding19(my_mapped_code))
                label_all_relevant = set(coding19_mapper.map_all_relevant_icd10cm_coding19(clinician_label))
                checked_intersect = True
                if len(my_all_relevant.intersection(label_all_relevant)) == 0:
                    print(my_all_relevant, label_all_relevant)
                    return 6

    if checked_intersect:
        return 5

    for my_mapped_code in my_mapping:
        for clinician_label in clinician_labels:
            if my_mapped_code[:-2] not in clinician_label:
                # print("More general up 3 levels", my_mapping, clinician_labels)
                return 4

    for my_mapped_code in my_mapping:
        for clinician_label in clinician_labels:
            if my_mapped_code[:-1] not in clinician_label:
                # print("More general up 2 level", my_mapping, clinician_labels)
                return 3

    for my_mapped_code in my_mapping:
        for clinician_label in clinician_labels:
            if my_mapped_code not in clinician_label:
                # print("More general up 1 level", my_mapping, clinician_labels)
                return 2

    # print("More general up 0 level", my_mapping, clinician_labels)
    return 1




if __name__ == "__main__":
    icd9cm_map_df = pd.read_csv(ICD9CM_ICD10CM_LABELS_PATH)

    icd10_mapper = ICD9CM_ICD10CM_Mapper(MAP_REF_PATH_9CM_10CM)

    coding19_mapper = ICD10CM_Coding19_Mapper(coding19_csv_path=CODING19_PATH, coding19_tree_hierarchy_path=TREE_FILE_PATH)

    exact, general, general_up_1, general_up_2, general_up_3, questionable, wrong = 0, 0, 0, 0, 0, 0, 0

    with open("icd9cm_labeled_inputs.txt", 'r') as file:
        lines = file.readlines()
        for i in range(30):
            line = lines[i]
            test_icd9cm_code = line[:-1]
            _, labels_mapped_codes = get_icd10cm_labels_from_icd9cm(icd9cm_map_df, test_icd9cm_code)

            _, my_mapped_codes = icd10_mapper.get_icd10cm_codes(test_icd9cm_code.replace(".", ""))

            # print(test_icd9cm_code)
            ret = get_mapping_quality_analysis(my_mapped_codes, labels_mapped_codes, coding19_mapper)
            if ret == 0:
                exact += 1
            elif ret == 1:
                general += 1
            elif ret == 2:
                general_up_1 += 1
            elif ret == 3:
                general_up_2 += 1
            elif ret == 4:
                general_up_3 += 1
            elif ret == 5:
                questionable += 1
            elif ret == 6:
                print(test_icd9cm_code, my_mapped_codes, labels_mapped_codes)
                wrong += 1

    counts = np.array([exact, general, general_up_1, general_up_2, general_up_3, questionable, wrong])
    rate = counts / np.sum(counts)
    print(rate)





