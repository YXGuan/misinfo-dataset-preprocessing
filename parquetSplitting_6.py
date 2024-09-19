import pandas as pd

# Define the input and output file paths
input_file = 'dat_claims_veracity_mapped_moved_converted_updatedSplit.parquet'  # Replace with your file path
output_train = 'train.parquet'      # Output file for 'train' split
output_test = 'test.parquet'        # Output file for 'test' split
output_validation = 'validation.parquet'  # Output file for 'validation' split

# Read the entire Parquet file into a DataFrame
df = pd.read_parquet(input_file)

# Split the DataFrame based on the 'split' column
train_df = df[df['split'] == 'train']
train_df.to_parquet(output_train, index=False)
del train_df

test_df = df[df['split'] == 'test']
test_df.to_parquet(output_test, index=False)
del test_df

validation_df = df[df['split'] == 'validation']
validation_df.to_parquet(output_validation, index=False)
del validation_df

# Write the filtered DataFrames to Parquet files

print("Splitting complete. Data saved to train, test, and validation files.")