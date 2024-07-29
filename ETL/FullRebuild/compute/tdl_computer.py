_hasMOADrug = 'hasMOADrug'
_hasLigand = 'hasLigand'
_hasGoLeaf = 'hasGoLeaf'
_pubmedScore = 'pubmedScore'
_abCount = 'abCount'
_geneRifCount = 'geneRifCount'
_oldTDL = 'oldTDL'
_symbol = 'symbol'
_tdl = 'tdl'

class tdl_computer:
    def __init__(self, conn, database):
        self.conn = conn
        self.database = database

    def calculateAllTDLs(self):
        tdlDataDictionary = self.fetchDataForTDLs()
        count = 0
        oldFacet = {}
        newFacet = {}
        for key, value in tdlDataDictionary.items():
            tdl = calculateOneTDL(value)
            value[_tdl] = tdl
            oldTDL = value[_oldTDL]
            self.scoreFacet(oldFacet, oldTDL)
            self.scoreFacet(newFacet, tdl)
            symbol = value[_symbol]
            if (tdl != oldTDL):
                count += 1
                print(f"""{key} ({symbol}) : {oldTDL} => {tdl}""")
        print(f"""{count} changed TDLs""")
        print (oldFacet)
        print (newFacet)
        return tdlDataDictionary

    def scoreFacet(self, facet, tdl):
        if tdl in facet:
            facet[tdl] += 1
        else:
            facet[tdl] = 1


    def fetchDataForTDLs(self):
        oldTDLs = self.getOldTDLs()
        targetsWithMOADrugs = self.getTargetsWithMOADrugs()
        targetsWithLigands = self.getTargetsWithActiveLigands()
        targetsWithGOLeaves = self.getTargetsWithExpLeafTerms()
        pubmedScores = self.getPubmedScores()
        abCounts = self.getAbCount()
        geneRifCounts = self.getGeneRifCount()

        outputDictionary = {}

        for row in oldTDLs:
            [uniprot, tdl, sym] = row
            if uniprot in outputDictionary:
                outputDictionary[uniprot][_oldTDL] = tdl
                outputDictionary[uniprot][_symbol] = sym
            else:
                outputDictionary[uniprot] = {_oldTDL: tdl,
                                             _symbol: sym}


        for row in targetsWithMOADrugs:
            uniprot = row[0]
            if uniprot in outputDictionary:
                outputDictionary[uniprot][_hasMOADrug] = True
            else:
                outputDictionary[uniprot] = {_hasMOADrug: True}

        for row in targetsWithLigands:
            uniprot = row[0]
            if uniprot in outputDictionary:
                outputDictionary[uniprot][_hasLigand] = True
            else:
                outputDictionary[uniprot] = {_hasLigand: True}

        for row in targetsWithGOLeaves:
            uniprot = row[0]
            if uniprot in outputDictionary:
                outputDictionary[uniprot][_hasGoLeaf] = True
            else:
                outputDictionary[uniprot] = {_hasGoLeaf: True}

        for row in pubmedScores:
            [uniprot, score] = row
            if uniprot in outputDictionary:
                outputDictionary[uniprot][_pubmedScore] = score
            else:
                outputDictionary[uniprot] = {_pubmedScore: score}

        for row in abCounts:
            [uniprot, score] = row
            if uniprot in outputDictionary:
                outputDictionary[uniprot][_abCount] = score
            else:
                outputDictionary[uniprot] = {_abCount: score}

        for row in geneRifCounts:
            [uniprot, score] = row
            if uniprot in outputDictionary:
                outputDictionary[uniprot][_geneRifCount] = score
            else:
                outputDictionary[uniprot] = {_geneRifCount: score}

        return outputDictionary


    def getOldTDLs(self):
        data = self.conn.get_records(f"""
        SELECT protein.uniprot, target.tdl, protein.sym from {self.database}.protein, {self.database}.t2tc, {self.database}.target
        where t2tc.target_id = target.id
        and t2tc.protein_id = protein.id""")
        return data

    def getTargetsWithMOADrugs(self):
        data = self.conn.get_records(f"""
        SELECT distinct protein.uniprot FROM {self.database}.drug_activity, {self.database}.target, {self.database}.t2tc, {self.database}.protein
        where drug_activity.target_id = target.id
        and t2tc.target_id = target.id
        and t2tc.protein_id = protein.id
        and has_moa = 1""")
        return data

    # TODO this has to be updated when the data is loaded from Uniprot, and not just copied from TCRD
    def getTargetsWithExpLeafTerms(self):
        data = self.conn.get_records(f"""
        SELECT distinct uniprot FROM {self.database}.tdl_info, {self.database}.protein
        where protein_id = protein.id
        and itype = 'Experimental MF/BP Leaf Term GOA'""")
        return data

    def getTargetsWithActiveLigands(self):
        data = self.conn.get_records(f"""
        SELECT
            distinct uniprot
        FROM
            {self.database}.ncats_ligand_activity,
            {self.database}.t2tc,
            {self.database}.protein,
            {self.database}.target
        WHERE
            ncats_ligand_activity.target_id = target.id
            AND ncats_ligand_activity.target_id = t2tc.target_id
            AND t2tc.protein_id = protein.id
            AND 
            (
				 (target.fam = 'GPCR' AND act_value >= 7) OR
				 (target.fam = 'Kinase' AND act_value >= 7.52288) OR
				 (target.fam = 'IC' AND act_value >= 5) OR
				 ((target.fam is null or target.fam not in ('IC', 'Kinase', 'GPCR')) and act_value >= 6)
			 )
    """)
        return data

    def getPubmedScores(self):
        data = self.conn.get_records(f"""SELECT 
            uniprot, number_value
        FROM
            {self.database}.tdl_info,
            {self.database}.protein
        WHERE
            itype = 'JensenLab PubMed Score'
            AND protein_id = protein.id""")
        return data

    def getAbCount(self):
        data = self.conn.get_records(f"""
        SELECT 
            uniprot, integer_value
        FROM
            {self.database}.tdl_info,
            {self.database}.protein
        WHERE
            itype = 'Ab Count'
            AND protein_id = protein.id""")
        return data

    def getGeneRifCount(self):
        data = self.conn.get_records(f"""
        SELECT uniprot, count(distinct generif.id)
            FROM {self.database}.generif, {self.database}.protein
            where protein_id = protein.id
            group by uniprot""")
        return data

def calculateOneTDL(targetObj):
    if _hasMOADrug in targetObj:
        return 'Tclin'
    if _hasLigand in targetObj:
        return 'Tchem'
    if _hasGoLeaf in targetObj:
        return 'Tbio'
    darkPoints = 0
    pmScore = float(targetObj[_pubmedScore]) if _pubmedScore in targetObj else 0
    rifCount = int(targetObj[_geneRifCount]) if _geneRifCount in targetObj else 0
    abCount = int(targetObj[_abCount]) if _abCount in targetObj else 0
    if pmScore < 5:
        darkPoints += 1
    if rifCount <= 3:
        darkPoints += 1
    if abCount <= 50:
        darkPoints += 1
    targetObj['darkPoints'] = darkPoints
    if darkPoints >= 2:
        return 'Tdark'
    return 'Tbio'