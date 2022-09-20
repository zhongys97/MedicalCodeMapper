import time
import pandas as pd
import boto3
import io 
import concurrent.futures
from extract_and_map import *
from time import perf_counter


# def do_add(time_delay, num1, num2):
#     time.sleep(time_delay)
#     print(f"{num1} + {num2} = {num1 + num2}")
#     return f"finished waiting {time_delay}"

def map_a_patient_file(pid):
    try:
        s3 = boto3.client('s3')
        bucket='s3demo-yishan'

        path = 'unmapped_patients/' + pid + ".csv"
        pid_csv_response = s3.get_object(Bucket = bucket, Key='unmapped_patients/' + pid + ".csv")
        single_patient_unmapped_df = pd.read_csv(pid_csv_response["Body"])
    except:
        print("error opening a specific csv")
        return f"{pid} did not mapped due to input error"

    try:
    
        single_patient_record_coding19_df, this_patient_codes, this_patient_dates, max_freq = map_one_patient_emory_to_coding19(single_patient_unmapped_df, icd9cm_to_10cm_mapper, icd10cm_to_coding19_mapper)

    except:
        print("error mapping a specific df")
        return f"{pid} did not mapped due to io error"

    try:
        csv_buffer=io.StringIO()
        single_patient_record_coding19_df.to_csv(csv_buffer, index=False)
        response = s3.put_object(Bucket=bucket, Key=f"mapped_patients_csv/{pid}.csv", Body=csv_buffer.getvalue())
        patient_list_to_pickle = [this_patient_codes, this_patient_dates, max_freq]
        
        pickle_byte_obj = pickle.dumps(patient_list_to_pickle)
        response = s3.put_object(Bucket=bucket, Key=f"mapped_patients_pickle/{pid}.pkl", Body=pickle_byte_obj)
    except:
        print("error saving mapped results")
        return f"{pid} did not mapped due to output error"

    return f"mapped {pid}"



if __name__ == "__main__":

    try:
        s3 = boto3.client('s3')
        bucket='s3demo-yishan'
        unmapped_result = s3.list_objects(Bucket = bucket, Prefix='unmapped_patients/')

        icd9_10cm_table_response = s3.get_object(Bucket = bucket, Key='mapping_pipeline/' + MAP_REF_PATH_9CM_10CM)
        icd9cm_to_10cm_mapper = ICD9CM_ICD10CM_Mapper(icd9_10cm_table_response['Body'])


        coding19_path_response = s3.get_object(Bucket = bucket, Key='mapping_pipeline/' + CODING19_PATH)
        coding19_tree_response = s3.get_object(Bucket = bucket, Key='mapping_pipeline/' + TREE_FILE_PATH)
        icd10cm_to_coding19_mapper = ICD10CM_Coding19_Mapper(coding19_csv_path=coding19_path_response['Body'], coding19_tree_hierarchy_stream=coding19_tree_response['Body'].read(), is_stream=True)
    except:
        print("mapper load failed")

    unmapped_csv_data_bodies = []
    for o in unmapped_result.get('Contents'):
        data = s3.get_object(Bucket=bucket, Key=o.get('Key'))
        
        if (data["ContentType"] == "text/csv" or data["ContentType"] == "binary/octet-stream"):
            pid = o["Key"].replace('unmapped_patients/', "").replace(".csv", "")
            unmapped_csv_data_bodies.append(pid)


    result =[]
    with concurrent.futures.ProcessPoolExecutor() as executor:
        
        result = executor.map(map_a_patient_file, unmapped_csv_data_bodies)

        # for r in result:
        #     continue
    print("finished")
    

    
  