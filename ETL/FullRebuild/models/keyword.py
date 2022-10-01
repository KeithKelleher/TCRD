class keyword:
    def __init__(self, keywordObj):
        self.id = keywordObj['id']
        self.category = keywordObj['category']
        self.name = keywordObj['name']

    def getInsertTuple(self):
        return ('UniProt Keyword', self.category, self.protein_id, self.id, self.name)

    @staticmethod
    def getFields():
        return ('xtype', 'subtype', 'protein_id', 'value', 'xtra')