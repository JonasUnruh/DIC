import pandas as pd
from nltk.tokenize import RegexpTokenizer
from tqdm import tqdm
import json
from multiprocessing import Pool, cpu_count

data = pd.read_json("./reviews_devset.json", lines = True)
stopwords = pd.read_csv("./stopwords.txt", sep = "\s+", header = None)
stopwords = list(stopwords[0])
tokenizer = RegexpTokenizer(r'\s+|\d+|[(){}[\].!?,;:+=\-_"\'`~#@&*%€$§\\/]', gaps = True)
categories = data["category"].unique()
DataFrameDict = {elem : [] for elem in categories}

for key in DataFrameDict.keys():
    DataFrameDict[key] = data["reviewText"][data.category == key]

for cat in tqdm(categories):
    tokens = []

    for i in tqdm(DataFrameDict[cat].index):
            test = tokenizer.tokenize(DataFrameDict[cat][i].lower())
            test = [word for word in test if word not in stopwords and len(word) > 1]
            tokens.append(test)

    DataFrameDict[cat] = tokens          

with open("./data.json", "w") as json_file:
     json.dump(DataFrameDict, json_file)