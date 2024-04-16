INPUT=hdfs:///e11701226/dic24_shared/amazon-reviews/full/reviewscombined.json

# Run the preprocess_runner.py to preprocess the input file
python preprocess_runner.py -r hadoop $INPUT > temp.txt

# Run chi_square.py to calculate the chi-square value
python chi_square.py -r hadoop temp.txt > output.txt