class Synonym:
    def __init__(self):
        self.synonyms = {
            'ethnicity': 'race',
        }
        self.reversed_synonyms = {self.synonyms[key]: key for key in self.synonyms}

    def get_synonym(self, query):
        if query in self.synonyms:
            return self.synonyms[query]
        elif query in self.reversed_synonyms:
            return self.reversed_synonyms[query]
        else:
            return None
