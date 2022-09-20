import os

from icd10cm_to_coding19 import *
from icd9cm_to_icd10cm import *

CODING19_PATH = "resources/old_coding19.tsv"
TREE_FILE_PATH = "resources/coding19_tree.pickle"

MAP_REF_PATH_9CM_10CM = "resources/icd9cm_icd10cm_table.csv"

SNOMED_MAPPING_TABLE_PATH = "resources/tls_Icd10cmHumanReadableMap_US1000124_20220301.tsv"
SNOMED_CONCEPT_RELATIONS_PATH = "resources/sct2_Relationship_Snapshot_US1000124_20220301.txt"


def get_batch_patient_csv(pids, input_filtered_csvs_path):
    patient_df_list = []

    all_subcsvs = os.listdir(input_filtered_csvs_path)
    all_rows_count = 0
    snomed_count = 0
    for i in range(len(all_subcsvs)):
        subcsv_path = os.path.join(input_filtered_csvs_path, all_subcsvs[i])
        dtype = {"SNOMEDDESC": str, "DIAGNOSIS_ICD10_CD": str, "DIAGNOSIS_ICD_CD": str}
        df = pd.read_csv(subcsv_path, dtype=dtype)
        all_rows_count += len(df)
        num_snomed = len(df["SNOMEDDESC"].dropna().to_list())
        snomed_count += num_snomed
        patient_df = df[df["PATIENT_ID"].isin(pids)]
        # print(patient_df)
        patient_df_list.append(patient_df)

    all_records_batch_patient = pd.concat(patient_df_list, ignore_index=True)
    all_records_batch_patient['RECORDED_DT'] = pd.to_datetime(all_records_batch_patient['RECORDED_DT']).dt.date
    all_records_batch_patient.sort_values(by=['RECORDED_DT'], inplace=True, ascending=True)
    batch_patients_df_list = [single_patient_df for _, single_patient_df in
                              all_records_batch_patient.groupby(['PATIENT_ID'])]
    return batch_patients_df_list


def get_single_patient_csv(pid, input_filtered_csvs_path):
    patient_df_list = []

    all_subcsvs = os.listdir(input_filtered_csvs_path)
    all_rows_count = 0
    snomed_count = 0
    for i in range(len(all_subcsvs)):
        subcsv_path = os.path.join(input_filtered_csvs_path, all_subcsvs[i])
        dtype = {"SNOMEDDESC": str, "DIAGNOSIS_ICD10_CD": str, "DIAGNOSIS_ICD_CD": str}
        df = pd.read_csv(subcsv_path, dtype=dtype)
        all_rows_count += len(df)
        num_snomed = len(df["SNOMEDDESC"].dropna().to_list())
        snomed_count += num_snomed
        patient_df = df[df["PATIENT_ID"] == pid]
        # print(patient_df)
        patient_df_list.append(patient_df)

    all_records_one_patient = pd.concat(patient_df_list, ignore_index=True)
    all_records_one_patient['RECORDED_DT'] = pd.to_datetime(all_records_one_patient['RECORDED_DT']).dt.date

    all_records_one_patient.sort_values(by=['RECORDED_DT'], inplace=True, ascending=True)
    return all_records_one_patient


def map_one_patient_emory_to_coding19(single_patient_df, icd9cm_to_10cm_mapper, icd10cm_to_coding19_mapper):
    """
    Only map the icd10 and icd9 codes to the coding19 standard.
    Did not take in snomed because they are text descriptions

    :param single_patient_df: dataframe of one single patient with diagnosis codes in snomed, icd9 and icd10
    :return:
        patient_df_coding19L: dataframe of the patient, but only with indices of coding19, and the number of rows are
                            the number of unique dates in record
        this_patient_codes: preprocessing features - diagnosis codes
        this_patient_dates: preprocessing features - index of dates of diagnosis (starts with 1)
        max_freq: preprocessing features - max frequency of occurance. If the same diagnosis code exists in half of the dates, this will be 0.5

    """

    
    # group by dates, which will be represented one row in the output
    df_list = [d for _, d in single_patient_df.groupby(['RECORDED_DT'])]

    # dictionary to keep track of coding19 codes count
    coding19_indices_count_dict = {}
    # lists to append to as preprocessing feature
    this_patient_codes = []
    this_patient_dates = []

    rows_list = []
    for date_idx in range(len(df_list)):
        cur_df = df_list[date_idx]

        icd10cm_codes = cur_df["DIAGNOSIS_ICD10_CD"].dropna().to_list()
        icd9cm_codes = cur_df["DIAGNOSIS_ICD_CD"].dropna().to_list()

        for icd9cm_code in icd9cm_codes:
            map_success, mapped_icd10cm_codes = icd9cm_to_10cm_mapper.get_icd10cm_codes(icd9cm_code)
            if map_success:
                icd10cm_codes = [*icd10cm_codes, *mapped_icd10cm_codes]

        coding19_one_date = []
        for icd10cm_code in icd10cm_codes:
            coding19_one_date = [*coding19_one_date,
                                 *icd10cm_to_coding19_mapper.map_all_relevant_icd10cm_coding19(icd10cm_code)]

        indices = icd10cm_to_coding19_mapper.indices_for_19k_vec_from_desc(coding19_one_date)

        # remove duplicate in the coding19 diagnosis codes. These are the diagnosis within the same day
        indices = list(set(indices))

        for i in range(len(indices)):
            # build the preprocess feature list
            this_patient_codes.append(indices[i])
            this_patient_dates.append(date_idx + 1)

            # accumulate the dictionary
            if indices[i] in coding19_indices_count_dict:
                coding19_indices_count_dict[indices[i]] += 1
            else:
                coding19_indices_count_dict[indices[i]] = 1

        row_dict = {"PATIENT_ID": cur_df.iloc[0, 0], "RECORDED_DT": cur_df.iloc[0, 5], "CODING19_INDICES": str(indices)}
        rows_list.append(row_dict)
    patient_df_coding19 = pd.DataFrame(rows_list)

    max_freq = max(coding19_indices_count_dict.values()) / len(rows_list)

    return patient_df_coding19, this_patient_codes, this_patient_dates, max_freq


if __name__ == "__main__":
    # pids = [163822, 197091]
    # # pids = [68950413, 100001739]
    # # single_patient_record_df = get_single_patient_csv(38392663, "D:/filtered_data")

    # patient_df_list = get_batch_patient_csv(pids, "D:/filtered_data")

    s3 = boto3.client('s3')
    bucket='s3demo-yishan'

    icd9_10cm_table_response = s3.get_object(Bucket = bucket, Key='mapping_pipeline/' + MAP_REF_PATH_9CM_10CM)
    icd9cm_to_10cm_mapper = ICD9CM_ICD10CM_Mapper(icd9_10cm_table_response['Body'])
    
    # snomed_to_10cm_mapper = SNOMED_ICD10CM_Mapper(SNOMED_MAPPING_TABLE_PATH, SNOMED_CONCEPT_RELATIONS_PATH)

    coding19_path_response = s3.get_object(Bucket = bucket, Key='mapping_pipeline/' + CODING19_PATH)
    coding19_tree_response = s3.get_object(Bucket = bucket, Key='mapping_pipeline/' + TREE_FILE_PATH)
    icd10cm_to_coding19_mapper = ICD10CM_Coding19_Mapper(coding19_csv_path=coding19_path_response['Body'], coding19_tree_hierarchy_stream=coding19_tree_response['Body'].read(), is_stream=True)


    pid = 1809860

    unmapped_csv_response = s3.get_object(Bucket = bucket, Key=f'unmapped_patients/{pid}.csv')
    single_patient_record_df = pd.read_csv(unmapped_csv_response["Body"])
    # print(df.head())

    single_patient_record_coding19_df, this_patient_codes, this_patient_dates, max_freq = map_one_patient_emory_to_coding19(single_patient_record_df, icd9cm_to_10cm_mapper=icd9cm_to_10cm_mapper, icd10cm_to_coding19_mapper=icd10cm_to_coding19_mapper)

    csv_buffer=io.StringIO()
    single_patient_record_coding19_df.to_csv(csv_buffer, index=False)
    response = s3.put_object(Bucket=bucket, Key=f"mapped_patients_csv/{pid}.csv", Body=csv_buffer.getvalue())
    patient_list_to_pickle = [this_patient_codes, this_patient_dates, max_freq]
    
    pickle_byte_obj = pickle.dumps(patient_list_to_pickle)
    response = s3.put_object(Bucket=bucket, Key=f"mapped_patients_pickle/{pid}.pkl", Body=pickle_byte_obj)


