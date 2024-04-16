INPUT=./reviews_devset.json

# Run the preprocess_runner.py to preprocess the input file
# Server version
# python preprocess_runner.py --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -r hadoop $INPUT > temp.txt
# Local version
python preprocess_runner.py $INPUT > temp.txt


# Run chi_square.py to calculate the chi-square value
# Server version
# python --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -r hadoop chi_square.py temp.txt > output.txt
# Local version
python chi_square.py temp.txt > output.txt