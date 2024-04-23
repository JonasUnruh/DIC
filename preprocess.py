import json
import re
from collections import defaultdict

from mrjob.job import MRJob
from mrjob.step import MRStep


class PreprocessJob(MRJob):
    '''MapReduce job executing preprocessing steps on Amazon reviews dataset.'''

    FILES = ["stopwords.txt"]

    counters = {
        "total": 0,
        "categories": {}
    }

    # 1. Step Mapper init
    def preprocess_mapper_init(self):
        # Load stopwords file for every mapper
        self.stopwords = set(i.strip() for i in open("stopwords.txt"))
    

    # 2. Step Mapper
    def preprocess_mapper(self, _, line):
        '''Mapping key/value pairs depending on review text and categories;
        not using given stopwords and tokens.

        Key: words
        Value: category
        '''
        
        data = json.loads(line)

        category = str(data["category"])
        reviewText = str(data["reviewText"]).lower()

        #count total number of reviews and total number of reviews per category
        self.increment_counter("counters", "total")
        self.increment_counter("categories", category)

        #Tokenize the text to unigrams and make values unique
        unigrams = re.split(r'\s+|\d+|[(){}[\].!?,;:+=_"\'`~#@&*%€$§\\/\-]', reviewText)
        unigrams = set(unigrams)

        #check for stop and single length words and word category pairs
        for word in unigrams:
            if word not in self.stopwords and len(word) > 1:
                yield word, {category: 1}


    # 3. Step Combiner
    def preprocess_combiner(self, word, category_dict):
        '''
        Combine data to lower amount of data transfered in between steps.
        We take the pairs as input and create a dict for every word that counts the occurences per category.
        This function runs after the mapper function

        Key: word
        Value: dict with category as key and occurences as value

        note: same word still appears multiple times
        '''

        word_count_dict = defaultdict(int)

        for category in category_dict:
            for cat in category:
                word_count_dict[cat] += 1
      
        yield word, word_count_dict


    # 4. Step Reducer
    def preprocess_reducer(self, word, word_count):
        '''
        Basically repeat combine step to return a word connected to a dict that holds the occurences per category

        Key: word
        Value: dict with category as key and occurences as value

        note: words are unique
        '''

        word_count_dict = defaultdict(int)

        for word_dict in word_count:
            for cat, count in word_dict.items():
                word_count_dict[cat] += count

        yield word, word_count_dict


    def steps(self):
        '''
        Define all needed steps for MRJob
        '''

        return [
            MRStep(
                mapper_init = self.preprocess_mapper_init,
                mapper=self.preprocess_mapper,
                combiner=self.preprocess_combiner,
                reducer=self.preprocess_reducer
            )
        ]

if __name__ == "__main__":
    PreprocessJob.run()
