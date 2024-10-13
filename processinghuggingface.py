# -*- coding: utf-8 -*-
"""ProcessingHuggingFace.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1VYD1-Ro7RcHb0KYcY6v5Pw0zT1K5i49m
"""

!pip install pyarrow
!pip install fastparquet

!pip install datasets


from datasets import load_dataset

import os

# Load a dataset from Hugging Face
dataset = load_dataset("ComplexDataLab/Misinfo_Datasets", "default")

# Convert the 'train' split of the dataset to a pandas DataFrame
df_train = dataset['train'].to_pandas()
df_test = dataset['test'].to_pandas()
df_validation = dataset['validation'].to_pandas()

unique_values = df_train['dataset'].unique()

print(unique_values)
print(unique_values.shape)

for value in unique_values:
  os.makedirs(value, exist_ok=True)

for value in unique_values:
  filtered_df = df_train[df_train['dataset'] == value]  # Filter by the 'Category' column
  path = value + '/' + value + '_train.parquet'
  filtered_df.to_parquet(path, engine='pyarrow')  # Just for spacing between the outputs

for value in unique_values:
  filtered_df = df_test[df_test['dataset'] == value]  # Filter by the 'Category' column
  path = value + '/' + value + '_test.parquet'
  filtered_df.to_parquet(path, engine='pyarrow')  # Just for spacing between the outputs

for value in unique_values:
  filtered_df = df_validation[df_validation['dataset'] == value]  # Filter by the 'Category' column
  path = value + '/' + value + '_validation.parquet'
  filtered_df.to_parquet(path, engine='pyarrow')  # Just for spacing between the outputs

import shutil
from google.colab import files

# Define the folder name you want to download
for folder_name in unique_values:
# Zip the folder
  shutil.make_archive(folder_name, 'zip', folder_name)

# Download the zipped folder
  files.download(f'{folder_name}.zip')