import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--arxiv_path', type=str, default='arxiv-metadata-oai-snapshot.json',
                    help='Path to the original arXiv dataset in JSON format')
parser.add_argument('--output_dir', type=str, default='data', help='Directory for the output data')
parser.add_argument('--split_size', type=int, default=50000, help='Records per split')
args = parser.parse_args()

arxiv_path = Path(args.arxiv_path)
output_dir = Path(args.output_dir)
size_of_the_split = args.split_size

split_count = 1
with arxiv_path.open() as arxiv:
    for i, line in enumerate(arxiv.readlines()):
        if i % size_of_the_split == 0:
            if i != 0:
                file.close()
            file = open(output_dir / f"original_data_split{split_count}.json", 'w')
            split_count += 1
        file.write(line)

file.close()
