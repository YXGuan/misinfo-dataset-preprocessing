# misinfo-dataset-preprocessing

## Why doing this?
we want to achieve a few things during the data set preprocessing:
1. The veracity mapping:  0 --> True 1--> False etc
2. The "Veracity" column will be re-ordered to the 2nd
3. Converting all the "null" to "na" and all data to string type
4. Making the "split" column evenly distributed
5. Converting from csv to parquet
6. Spliting the parquet file to 3 categories (train, test validation)

## Procedures:

### Step 1 download the data
https://osf.io/5azde/?view_only=cf103519a4454286becf5699f85bd77b

### Step 2 run the script from 1 to 6 locally
- veracityMapping_1.py (make sure to update the file name)
- moveVeracity_2.py
- typeconversion_3.py
- preSplit_4.py
- conversionParquet_5.py
- parquetSplitting_6.py

### Step 3 upload/update the dataset to HF
https://huggingface.co/datasets/ComplexDataLab/Misinfo_Dataset/tree/main

# post-processing
One issue found with the method above is that due to unknown reason, the veracity column has "True, False, true, false". Therefore, an addition script, misinfo_veracitymapping.py, has been written to map True to true and False to false


processinghuggingface.py was also created to split the entire dataset into subsets based on each individual dataset. Credits to [Taylor](https://github.com/taylorlynn)

![image](https://github.com/user-attachments/assets/19da7189-892d-469c-be88-05246f07fa79)

