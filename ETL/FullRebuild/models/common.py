class common:
    @staticmethod
    def findMatches(proteinObj, listField, dataField, matchString):
        if listField in proteinObj:
            return filter(lambda match: match[dataField] == matchString, proteinObj[listField])
        return iter([])