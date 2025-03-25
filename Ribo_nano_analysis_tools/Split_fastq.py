import os
from itertools import takewhile
from itertools import repeat
from concurrent.futures import ProcessPoolExecutor

def Split_fastq_help():
    _help_content = \
    """
    
    split_func used for extract specific length of reads.
    
    split_func used for extract equal and longer than length of reads.
    
    """
    print(_help_content)
    
def fastq_reader(file_path):
    with open(file_path, 'r') as f:
        while True:
            lines = []
            for _ in range(4):
                line = f.readline()
                if not line:
                    break
                lines.append(line.strip())
            if len(lines) == 4:
                yield lines
            else:
                break

def get_lines(fi):
    """
    get lines very fast
    """
    buffer = 1024 * 1024 *8
    with open(fi, "r") as f:
        buf_gen = takewhile(lambda x: x, (f.read(buffer) for _ in repeat(None)))
        return sum(buf.count('\n') for buf in buf_gen)

def count_records(fi):
    _count = get_lines(fi)
    count = _count//4
    return count
    
def split_fastq(file_path, num_parts):
    """split Fastq"""
    total_records = count_records(file_path)
    part_size = total_records // num_parts
    remainder = total_records % num_parts

    record_iter = fastq_reader(file_path)
    parts = []
    for i in range(num_parts):
        part_name = f'temp_part_{i}.fastq'
        parts.append(part_name)
        num_records = part_size + (1 if i < remainder else 0)
        with open(part_name, 'w') as f:
            for _ in range(num_records):
                try:
                    record = next(record_iter)
                    f.write('\n'.join(record) + '\n')
                except StopIteration:
                    break
        print(f"\t\t{part_name} generated, finsied {i + 1}/{num_parts}")
    print(f"\t\t---Split finised!---")
    return parts

def process_part(input_part, seq_len, output_part):
    """
    Deal single fasta
    """
    with open(input_part, 'r') as infile, open(output_part, 'w') as outfile:
        while True:
            record = []
            for _ in range(4):
                line = infile.readline()
                if not line:
                    break
                record.append(line.strip())
            if len(record) != 4:
                break
            if len(record[1]) == seq_len:
                outfile.write('\n'.join(record) + '\n')

def merge_outputs(output_parts, final_output):
    with open(final_output, 'w') as f:
        for part in output_parts:
            with open(part, 'r') as pf:
                f.write(pf.read())
            os.remove(part)

def split_func(input_file:str, 
               num_threads:int, 
               output_file:str, 
               seq_len:int):
    print(f">>>Starting spliting the fastq to {num_threads} files...<<<")
    parts = split_fastq(input_file, 
                        num_threads)
    output_parts = [f'temp_output_{i}.fastq' for i in range(num_threads)]
    print(f">>>Starting filtering length {seq_len}nt in fastq")
    with ProcessPoolExecutor(max_workers=num_threads) as executor:
        executor.map(process_part, parts, [seq_len]*num_threads, output_parts)
    
    # merge output
    print(f">>>Merging all fastq...")
    merge_outputs(output_parts, output_file)
    
    print(f">>>Deleting all mediate fastq...")
    for part in parts:
        os.remove(part)
        
    print(f"---{output_file} had generated!---")

def process_part_bigger(input_part, seq_len, output_part):
    """
    Deal single fasta
    """
    with open(input_part, 'r') as infile, open(output_part, 'w') as outfile:
        while True:
            record = []
            for _ in range(4):
                line = infile.readline()
                if not line:
                    break
                record.append(line.strip())
            if len(record) != 4:
                break
            if len(record[1]) >= seq_len:
                outfile.write('\n'.join(record) + '\n')

def split_func_bigger(input_file:str, 
               num_threads:int, 
               output_file:str, 
               seq_len:int):
    print(f">>>Starting spliting the fastq to {num_threads} files...<<<")
    parts = split_fastq(input_file, 
                        num_threads)
    output_parts = [f'temp_output_{i}.fastq' for i in range(num_threads)]
    print(f">>>Starting filtering length {seq_len}nt in fastq")
    with ProcessPoolExecutor(max_workers=num_threads) as executor:
        executor.map(process_part_bigger, parts, [seq_len]*num_threads, output_parts)
    
    # merge output
    print(f">>>Merging all fastq...")
    merge_outputs(output_parts, output_file)
    
    print(f">>>Deleting all mediate fastq...")
    for part in parts:
        os.remove(part)
        
    print(f"---{output_file} had generated!---")

def process_part_region(input_part, seq_len1, seq_len2, output_part):
    """
    Deal single fasta
    """
    with open(input_part, 'r') as infile, open(output_part, 'w') as outfile:
        while True:
            record = []
            for _ in range(4):
                line = infile.readline()
                if not line:
                    break
                record.append(line.strip())
            if len(record) != 4:
                break
            if (len(record[1]) >= seq_len1) and (len(record[1]) <= seq_len2):
                outfile.write('\n'.join(record) + '\n')

def split_func_region(input_file:str, 
               num_threads:int, 
               output_file:str, 
               seq_len1:int,
               seq_len2:int):
    print(f">>>Starting spliting the fastq to {num_threads} files...<<<")
    parts = split_fastq(input_file, 
                        num_threads)
    output_parts = [f'temp_output_{i}.fastq' for i in range(num_threads)]
    print(f">>>Starting filtering length {seq_len1}nt to {seq_len2}nt in fastq")
    with ProcessPoolExecutor(max_workers=num_threads) as executor:
        executor.map(process_part_region, 
                     parts, 
                     [seq_len1]*num_threads, 
                     [seq_len2]*num_threads, 
                     output_parts)
    
    # merge output
    print(f">>>Merging all fastq...")
    merge_outputs(output_parts, output_file)
    
    print(f">>>Deleting all mediate fastq...")
    for part in parts:
        os.remove(part)
        
    print(f"---{output_file} had generated!---")
