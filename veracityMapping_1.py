import pandas as pd
import gc

# Define the mapping rules
veracity_mapping = {
    '1': 'true',
    '1.0': 'true',
    '2': 'false',
    '2.0': 'false',
    '3': 'unknown',
    '3.0': 'unknown',
    '': 'Na',
    'null': 'Na',
    'na': 'Na',
    'Na': 'Na'
}

input_file_path = 'dat_claims.csv'
output_file_path = 'dat_claims_veracity_mapped.csv'

chunksize = 100000  # Define the size of chunks to read

# Use a context manager to write to the output file
with pd.read_csv(input_file_path, chunksize=chunksize, dtype=str) as reader:
    for i, chunk in enumerate(reader):
        # Map the veracity values for each chunk
        chunk['veracity'] = chunk['veracity'].map(veracity_mapping).astype(str)
        
        # If it's the first chunk, write with headers, otherwise append without headers
        mode = 'w' if i == 0 else 'a'
        header = i == 0
        
        chunk.to_csv(output_file_path, mode=mode, header=header, index=False)
        
        # Explicitly call garbage collection after processing each chunk
        gc.collect()

print(f'Mapped CSV file generated: {output_file_path}')