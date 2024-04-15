from mrjob.job import MRJob
from mrjob.step import MRStep

import json
import re

class ChiSquare(MRJob):

    total = 0
    counters = {}

    def read_counters(self):
        with open("C:/Users/Jonas/Desktop/DIC/DIC/counters.txt", "r") as file:
            self.total, self.counters = file.readline().split(" ", 1)
            self.total = int(self.total)
            self.counters = json.loads(self.counters.replace("'", '"'))
        

    def chi_square_mapper(self, _, line):
        '''
        Calculate chi square value for every word and return its value with its category
        '''        

        try:
            word, cat_dict = str(line).split(maxsplit = 1)

        except:
            return 
        
        cat_dict = re.sub(r'\x00', '', cat_dict)
        cat_dict = json.loads(cat_dict.replace("'", '"'))
        #chi_square_dict = dict()

        for cat in cat_dict.keys():
            A = cat_dict[cat]
            B = sum(cat_dict.values()) - A
            C = self.counters[cat] - A
            D = self.total - A - B - C

            chi_square = (self.total * pow((A * D - B * C), 2)) / ((A + B) * (A + C) * (B + D) * (C + D))

            #chi_square_dict[word] = chi_square

            yield cat, (word, chi_square)


    def chi_square_reducer(self, category, values):
        '''
        Sort chi square values and reduce output to top 75 values
        '''

        chi_square_dict = dict()

        for word, chi_square in values:
            chi_square_dict[word] = chi_square

        chi_square_dict = dict(sorted(chi_square_dict.items(), key=lambda x: x[1], reverse = True)[:75])

        yield category, chi_square_dict


    def steps(self):
        # defining steps of MapReduce job

        return [
            MRStep(
                mapper_init = self.read_counters,
                mapper = self.chi_square_mapper,
                reducer = self.chi_square_reducer
            )
        ]
    
if __name__ == "__main__":
    ChiSquare.run()