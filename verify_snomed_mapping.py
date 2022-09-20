import numpy as np
import pandas as pd
from snomed_to_icd10 import map_snomedct_to_icd10cm
from icd10cm_to_coding19 import *


mapping_tsv_path = "resources/tls_Icd10cmHumanReadableMap_US1000124_20220301.tsv"
snomed_relation_tsv_path = "resources/sct2_Relationship_Snapshot_US1000124_20220301.txt"

CODING19_PATH = "resources/old_coding19.tsv"
TREE_FILE_PATH = "resources/coding19_tree.pickle"


SNOMED_ICD10CM_LABELS_PATH = "./resources/verification/emory_snomed_to_10cm.csv"
ICD9CM_ICD10CM_LABELS_PATH = "./resources/verification/emory_9cm_to_10cm.csv"

def get_after_exclaimation(code_string):
    try:
        start_index = code_string.index("!")
        return True, code_string[start_index+1:]
    except:
        return False, ""


def get_icd10cm_labels_from_snomed(snomed_icd10cm_labels_df, snomed_code):
    snomed_code_long = "SNOMED!" + snomed_code
    matched = snomed_icd10cm_labels_df[snomed_icd10cm_labels_df["SNOMED"] == snomed_code_long]
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

def get_all_labeled_snomed_inputs(labels_df):
    all_icd9cm_long = set(labels_df["SNOMED"].to_list())
    icd9cm_codes = []
    for long_code in all_icd9cm_long:
        success, icd9cm_code = get_after_exclaimation(long_code)
        if success:
            icd9cm_codes.append(icd9cm_code)

    with open("snomed_labeled_inputs.txt", 'w') as file:
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
                    # print(my_all_relevant, label_all_relevant)
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


    coding19_mapper = ICD10CM_Coding19_Mapper(coding19_csv_path=CODING19_PATH,
                                              coding19_tree_hierarchy_path=TREE_FILE_PATH)
    snomed_map_df = pd.read_csv(SNOMED_ICD10CM_LABELS_PATH)

    mapping_df = pd.read_csv(mapping_tsv_path, sep='\t')
    snomed_term_relation_df = pd.read_csv(snomed_relation_tsv_path, sep='\t')


    exact, general, general_up_1, general_up_2, general_up_3, questionable, wrong = 0, 0, 0, 0, 0, 0, 0

    with open("snomed_labeled_inputs.txt", 'r') as file:
        lines = file.readlines()
        for i in range(len(lines)):
            line = lines[i]
            test_snomed_code = line[:-1]
            # print(test_snomed_code)
            _, labels_mapped_codes = get_icd10cm_labels_from_snomed(snomed_map_df, test_snomed_code)


            my_mapped_codes = map_snomedct_to_icd10cm(snomed_term_relation_df, mapping_df, int(test_snomed_code))
            # print(test_icd9cm_code)
            # print(my_mapped_codes, labels_mapped_codes)
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
                # print(test_icd9cm_code, my_mapped_codes, labels_mapped_codes)
                wrong += 1

        counts = np.array([exact, general, general_up_1, general_up_2, general_up_3, questionable, wrong])
        rate = counts / np.sum(counts)
        print(rate)

