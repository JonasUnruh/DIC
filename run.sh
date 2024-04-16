INPUT=./reviews_devset.json

# Run the preprocess_runner.py to preprocess the input file
python preprocess_runner.py $INPUT > temp.txt

# Run chi_square.py to calculate the chi-square value
python chi_square.py temp.txt > output.txt