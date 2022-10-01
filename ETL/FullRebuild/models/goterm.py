from common import common

class goterm:
    def __init__(self, association):
        self.id = association.id
        self.type = association.go_type
        self.term = association.go_term

    def __str__(self):
        return f"{self.id}: {self.term} ({self.type})"

    def getInsertTuple(self):
        return (self.id, self.term, self.type)

    @staticmethod
    def getFields():
        return ('id', 'term', 'type')


class go_association:
    def _getType(self, abbreviation):
        switcher = {
            "C": "Component",
            "F": "Function",
            "P": "Process",
        }
        return switcher.get(abbreviation)

    def __init__(self, gotermObj):
        self.id = gotermObj['id']
        term = next(common.findMatches(gotermObj, 'properties', 'key', 'GoTerm'))
        evidence = next(common.findMatches(gotermObj, 'properties', 'key', 'GoEvidenceType'))
        pieces = term['value'].split(':')
        self.go_type = self._getType(pieces[0])
        self.go_term = pieces[1]
        pieces = evidence['value'].split(':')
        self.eco_term = pieces[0]
        self.eco_assigned_by = pieces[1]

    def __str__(self):
        return f"{self.id}: {self.go_term} ({self.go_type}): {self.eco_term} ({self.eco_assigned_by})"

    def getInsertTuple(self):
        return (self.protein_id, self.id, self.eco_term, self.eco_assigned_by)

    @staticmethod
    def getFields():
        return ('protein_id', 'go_id', 'goeco', 'assigned_by')