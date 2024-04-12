import json
import re

from mrjob.job import MRJob
from mrjob.step import MRStep


class PreprocessJob(MRJob):
    """MapReduce job executing preprocessing steps on Amazon reviews dataset."""

    FILES = ["stopwords.txt"]

    # 1. Step Mapper
    def preprocess_mapper(self, _, line):
        """Mapping key/value pairs depending on review text and categories;
        not using given stopwords and tokens.

        Key: words
        Value: category
        """
        data = json.loads(line)
        stopwords = set(i.strip() for i in open("stopwords.txt"))
        categories = data["category"]
        reviewText = data["reviewText"].lower()
        unigrams = re.split(
            r'\s+|\d+|[(){}[\].!?,;:+=\-_"\'`~#@&*%€$§\\/]', "", reviewText
        )
        print(unigrams)

        # for key in DataFrameDict.keys():
        #     DataFrameDict[key] = data["reviewText"][data.category == key]

        # for cat in categories:
        #     tokens = []

        #     for i in DataFrameDict[cat].index:
        #         test = DataFrameDict[cat][i].lower()
        #         test = [
        #             word for word in test if word not in stopwords and len(word) > 1
        #         ]
        #         tokens.append(test)

        #     DataFrameDict[cat] = tokens

    # 2. Step Combiner

    # 3. Step Reducer

    def steps(self):
        # defining steps of MapReduce job

        return [
            MRStep(
                mapper=self.preprocess_mapper,
            )
        ]


if __name__ == "__main__":
    PreprocessJob.run()
