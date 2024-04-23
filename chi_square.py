import json

from mrjob.job import MRJob
from mrjob.step import MRStep


class ChiSquare(MRJob):
    '''
    Calculate chi square values for words in categories and return top 75 words per category and merged dictionary
    '''

    FILES = ['counters.txt']

    # 1. Step Mapper Init
    def read_counters(self):
        '''
        Read in counters.txt from previous job
        '''

        with open("counters.txt", "r") as file:
            self.total, self.counters = file.readline().split(" ", 1)
            self.total = int(self.total)
            self.counters = json.loads(self.counters.replace("'", '"'))  


    # 2. Step Mapper
    def chi_square_mapper(self, _, line):
        '''
        Calculate chi square value for every word and return its value with its category

        Key: category
        Value: dictionary with word as key and chi square value as value
        '''
        
        try:
            word, cat_dict = str(line).split(maxsplit = 1)

        except:
            return 
        
        cat_dict = json.loads(cat_dict.replace("'", '"'))

        for cat in cat_dict:
            A = cat_dict[cat]
            B = sum(cat_dict.values()) - A
            C = self.counters[cat] - A
            D = self.total - A - B - C

            chi_square = (self.total * pow((A * D - B * C), 2)) / ((A + B) * (A + C) * (B + D) * (C + D))

            yield cat, (word, chi_square)


    # 3. Step Reducer
    def chi_square_reducer(self, category, values):
        '''
        Sort chi square values and reduce output to top 75 values

        Key: None
        Value: dictionary with category as key and top 75 words as dictionary with word as key and chi square value as value
        '''

        chi_square_dict = dict()

        for word, chi_square in values:
            chi_square_dict[word] = chi_square

        chi_square_dict = dict(sorted(chi_square_dict.items(), key=lambda x: x[1], reverse = True)[:75])

        yield None, (category, chi_square_dict)


    # 4. Step Reducer
    def category_sort_reducer(self, _, dict):
        '''
        Sort categories alphabetically, return top 75 words per category and merged dictionary

        Key: category
        Value: top 75 words as dictionary with word as key and chi square value as value
        + one line with top 75 words in all categories
        '''

        dict_pairs = list(dict)

        words = list(set(word for dict_pair in dict_pairs for word in dict_pair[1].keys()))
        

        dict_pairs = sorted(dict_pairs)
        
        for category, top_75 in dict_pairs:
            yield category, top_75

        yield "Merged Dictionary:", ' '.join(sorted(words))


    def steps(self):
        '''
        Define all needed steps for MRJob
        '''

        return [
            MRStep(
                mapper_init = self.read_counters,
                mapper = self.chi_square_mapper,
                reducer = self.chi_square_reducer
            ),
            MRStep(
                reducer = self.category_sort_reducer
            )
        ]
    
if __name__ == "__main__":
    ChiSquare.run()