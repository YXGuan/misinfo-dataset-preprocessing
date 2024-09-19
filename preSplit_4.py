import pandas as pd
import numpy as np
import gc

# Function to split data into train, validation, and test sets
def split_data(group):
    n = len(group)
    shuffled_indices = np.random.permutation(n)
    train_end = int(0.7 * n)
    validation_end = int(0.8 * n)
    
    train_indices = shuffled_indices[:train_end]
    validation_indices = shuffled_indices[train_end:validation_end]
    test_indices = shuffled_indices[validation_end:]
    
    split_labels = np.empty(n, dtype=object)
    split_labels[train_indices] = 'train'
    split_labels[validation_indices] = 'validation'
    split_labels[test_indices] = 'test'
    
    return split_labels

# Initialize an empty list to collect processed chunks
processed_chunks = []

# Define the file name and the chunk size
file_name = 'dat_claims_veracity_mapped_moved_converted.csv'
chunk_size = 100000  # Adjust this based on your system's memory capacity

# Process the CSV file in chunks
for chunk in pd.read_csv(file_name, chunksize=chunk_size):
    chunks_with_split = []
    grouped = chunk.groupby('dataset')
    
    for name, group in grouped:
        group['split'] = split_data(group)
        chunks_with_split.append(group)
    
    # Concatenate processed groups back into a single DataFrame
    processed_chunk = pd.concat(chunks_with_split)
    
    # Append the processed chunk to the list
    processed_chunks.append(processed_chunk)
    
    # Explicitly call garbage collection
    gc.collect()

# Concatenate all processed chunks
updated_df = pd.concat(processed_chunks, ignore_index=True)

# Save the updated DataFrame back to a CSV file
updated_df.to_csv('dat_claims_veracity_mapped_moved_converted_updatedSplit.csv', index=False)