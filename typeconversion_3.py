import pandas as pd
import gc  # Garbage collection module

# Define the input and output file paths
input_file = 'dat_claims_veracity_mapped_moved.csv'  # Replace with your file path
output_file = 'dat_claims_veracity_mapped_moved_converted.csv'  # Replace with your desired output path

# Define the chunk size (number of rows per chunk)
chunk_size = 100000  # Adjust based on your RAM capacity
processed_rows = 0  # To keep track of processed rows

# Read the CSV file in chunks and process each chunk
with pd.read_csv(input_file, chunksize=chunk_size, low_memory=False) as reader:
    # Initialize a variable to write the header only once
    write_header = True

    for chunk in reader:
        print("Reading a new chunk...")
        
        # Display data types before conversion for debugging
        print("Data types before conversion:")
        print(chunk.dtypes)

        # Ensure NaN values are handled properly before conversion
        new_chunk = chunk.fillna('Na')  # Replace NaN with empty string
        
        # Convert all columns to string type
        new_chunk = new_chunk.astype(str)

        # Check conversion success by printing new data types
        print("Data types after conversion:")
        print(new_chunk.dtypes)

        # Check some data after conversion for debugging
        print("Head of the chunk after conversion:")
        print(new_chunk.head())

        # Write the chunk to the output file
        new_chunk.to_csv(output_file, mode='a', index=False, header=write_header)
        
        # After writing the first chunk, set write_header to False
        write_header = False

        # Update processed rows and print progress
        processed_rows += len(new_chunk)
        print(f"Processed {processed_rows} rows so far...")

        # Manual garbage collection to free memory
        gc.collect()

print("Data type conversion complete and saved to the output file.")