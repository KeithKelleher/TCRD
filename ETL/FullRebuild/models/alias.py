
class alias:
    def __init__(self, type, term):
        self.type = type
        self.term = term
    def __str__(self):
        return f"{self.type} -> {self.term}"

    def getInsertTuple(self):
        return (self.protein_id, self.type, self.term)

    @staticmethod
    def getFields():
        return ('protein_id', 'type', 'value')

    @staticmethod
    def appendToList(array, newAlias):
        found = list(filter(lambda each: newAlias.type == each.type and newAlias.term == each.term, array))
        if(len(found) == 0):
            array.append(newAlias)