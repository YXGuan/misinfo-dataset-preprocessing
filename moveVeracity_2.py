import pandas as pd
import gc

# Define the path for input and output CSV files
input_csv = 'dat_claims_veracity_mapped.csv'
output_csv = 'dat_claims_veracity_mapped_moved.csv'


# Function to rearrange columns
def rearrange_columns(chunk):
    # Move 'veracity' to the second column
    columns = list(chunk.columns)
    
    # Rearranged column order
    new_order = ['claim']  # Start with 'claim'
    
    # Find and append 'veracity'
    if 'veracity' in columns:
        new_order.append('veracity')
        columns.remove('veracity')
        
    # Append the rest of the columns except 'claim' and 'veracity'
    new_order.extend(columns)
    
    # Reorganize the chunk with the new order
    return chunk[new_order]

# Set parameters
# input_csv = 'input_file.csv'  # Input CSV file
# output_csv = 'output_file.csv'  # Output CSV file
chunk_size = 100000  # Number of rows to read in each chunk

# Process the CSV file in chunks
with pd.read_csv(input_csv, chunksize=chunk_size) as reader:
    for i, chunk in enumerate(reader):
        # Rearrange the columns
        rearranged_chunk = rearrange_columns(chunk)
        
        # Write the chunk to the output file (append mode)
        if i == 0:
            rearranged_chunk.to_csv(output_csv, index=False)
        else:
            rearranged_chunk.to_csv(output_csv, index=False, header=False, mode='a')
        
        # Force garbage collection after processing each chunk
        del chunk
        del rearranged_chunk
        gc.collect()