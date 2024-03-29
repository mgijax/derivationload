
##########################################################################
#
# Purpose:
# 	This script parses the input file, resolves terms to keys and
#       creates a bcp file for ALL_CellLine_Derivation
#
# Usage: derivationload.py
#
# Env Vars:
#	 1. INFILE_NAME
#	 2. OUTPUTDIR
#	 3. OUTFILE_NAME
#        4. JOBSTREAM
#
# Inputs:
#        INFILE_NAME is an input file in tab-delimitted file format:
#        1. Derivation Name (may be null)
#        2. Description (may be null)
#        3. Vector Name (Cell Line Vector Name Vocab Term)
#        4. Vector Type (Cell Line Vector Type Vocab Term)
#        5. Parent Cell Line Name (ALL_CellLine.cellLine)
#        6. Parent Cell Line Strain (ALL_CellLine._Strain_key)
#        7. Cell Line Creator (Cell Line Creator Vocab Term)
#        8. Reference (JNumber, may be null)
#	 9. Derivation Type (Allele Type Vocab Term)
#
# Outputs:
#	 1. bcp file 
#	 2. log files
# 
# Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred - this load is all or nothing. If
#	   any errors, load will exit
#	   1. IO Errors
#	   2. Record Format Errors
#	   3. Derivation already in the database	
#	   4. Derivation name or description too long
#	   5. A term won't resolve to a key
#
#  Assumes:  Nothing
#
#  Notes:  None
#
###########################################################################

import sys
import os
import db
import mgi_utils
import loadlib
import string

db.useOneConnection(1)
print('%s' % mgi_utils.date())

inFilePath = os.environ['INFILE_NAME']
bcpFilePath = '%s/%s' % (os.environ['OUTPUTDIR'], os.environ['OUTFILE_NAME'])

user = os.environ['JOBSTREAM']

# column delimiter
colDelim = "\t"
# record delimiter
lineDelim = "\n"

TAB= "\t"
userKey = 0
date = loadlib.loaddate

# derivations currently in the database, by name
dbDerivNameList = []

# derivations in the input, by name
inputDerivNameList = []

# vector names mapped to database term key
dbVectorNameMap = {}

# vector types mapped to database term key
dbVectorTypeMap = {}

# {parentName|parentStrain:parentKey, ...}
dbParentMap = {}

# derivation type mapped to database term key
dbDerivTypeMap = {}

# creator mapped to database term key
dbCreatorMap = {}

# JNumbers mapped to database reference key:w
dbJNumMap = {}

#
# Initialize
#

# get next available derivation key
results = db.sql(''' select nextval('all_cellline_derivation_seq') as derivKey ''', 'auto')
nextAvailableDerivKey = results[0]['derivKey']
if nextAvailableDerivKey == None:
        nextAvailableDerivKey = 1

# get load user key
results = db.sql("select _User_key from MGI_User where login = '%s' " % user, 'auto')
userKey = results[0]['_User_key']

# get list of named derivations in the database, so we don't add dups
results = db.sql('select name from ALL_CellLine_Derivation where name != null', 'auto')
for r in results:
     dbDerivNameList.append(r['name'])

# map database vector name terms to their term keys
results = db.sql('select term, _Term_key from VOC_Term where _Vocab_key = 72', 'auto')
for r in results:
    dbVectorNameMap[r['term']] = r['_Term_key']


# map database vector type terms to their term keys
results = db.sql('select term, _Term_key from VOC_Term where _Vocab_key = 64', 'auto')
for r in results:
    dbVectorTypeMap[r['term']] = r['_Term_key']

# map parent cell line names to their  cell line keys
results = db.sql('''select a.cellLine, s.strain,  a._CellLine_key
        from ALL_CellLine a, PRB_Strain s
        where a.isMutant = 0
        and a._Strain_key = s._Strain_key
        ''', 'auto')
for r in results:
    key = '%s|%s' % (r['cellLine'], r['strain'])
    dbParentMap[key] = r['_CellLine_key']

# map derivation type (aka allelel type) terms to their term keys
results = db.sql('select term, _Term_key from VOC_Term where _Vocab_key = 38', 'auto')
for r in results:
    dbDerivTypeMap[r['term']] = r['_Term_key']

# map database creator name terms to their term keys
results = db.sql('select term, _Term_key from VOC_Term where _Vocab_key = 62', 'auto')
for r in results:
    dbCreatorMap[r['term']] = r['_Term_key']

# map database Jnumbers to their reference keys
results = db.sql('''select accID, _Object_key 
        from ACC_Accession 
        where _MGIType_key = 1 and _LogicalDB_key = 1 and prefixPart = 'J:'
        ''', 'auto')
for r in results:
    dbJNumMap[r['accID']] = r['_Object_key']

try:
    inFile = open(inFilePath, 'r')
except:
    exit('Could not open file for reading %s\n' % inFilePath)
                
try:
    bcpFile = open(bcpFilePath, 'w')
except:
    exit('Could not open file for writing %s\n' % bcpFilePath)

#
# Process
#

moveOn = 0
currentLine = 0
errorCt = 0
for line in inFile.readlines():
    currentLine = currentLine + 1
    lineList = str.split(line, TAB)
    if len(lineList) != 9:
        sys.exit('Line has only %s of 8 columns: %s ' %(len(lineList), line))
    
    inName = str.strip(lineList[0])		# may be null

    # skip any duplicates in the input file
    if inName in inputDerivNameList:
        print("Duplicate input derivation, skipping: %s See line %s" % (inName, currentLine))
        continue

    inputDerivNameList.append(inName)
    inDescript = str.strip(lineList[1]) 	# may be null
    inVector = str.strip(lineList[2])
    inVectorType = str.strip(lineList[3])
    inParent = str.strip(lineList[4])
    inStrain = str.strip(lineList[5])
    inCreator = str.strip(lineList[6]) 	# may be null
    inJNum = str.strip(lineList[7]) 		# may be null
    inDerivType = str.strip(lineList[8])

    # check to see if there is a derivation by this name already in the db
    if inName != 'null' and inName != 'Null':
        if inName in dbDerivNameList:
            print('Derivation name already in database, skipping: %s See line %s' % (inName, currentLine))
            moveOn = 1
    else:
        inName = ''

    # resolve vector to key
    if inVector in dbVectorNameMap:
        vectorKey = dbVectorNameMap[inVector]
    else:
        print('Cannot resolve Vector Name: %s See line %s' % (inVector, currentLine))
        moveOn = 1

    # resolve vector type to key
    if inVectorType in dbVectorTypeMap:
        vectorTypeKey = dbVectorTypeMap[inVectorType]
    else:
        print('Cannot resolve Vector Type: %s See line %s' % (inVectorType, currentLine))
        moveOn = 1

    # resolve parent to key
    parentAndStrain = '%s|%s' % (inParent, inStrain)
    if parentAndStrain in dbParentMap:
        parentKey = dbParentMap[parentAndStrain]
    else:
        print('Cannot resolve Parent: %s See line %s' % (parentAndStrain, currentLine))
        moveOn = 1

    # resolve derivation type to key
    if inDerivType in dbDerivTypeMap:
        derivTypeKey = dbDerivTypeMap[inDerivType]
    else:
        print('Cannot resolve Derivation Type: %s See line %s' % (inDerivType, currentLine))
        moveOn = 1

    # resolve creator to key if not null
    if inCreator != 'null' and inCreator != 'Null': 
        if inCreator in dbCreatorMap:
            creatorKey = dbCreatorMap[inCreator]
        else:
            print('Cannot resolve Creator name: %s See line %s' % (inCreator, currentLine))
            moveOn = 1
    else: 
        creatorKey = ''

    # resolve JNumber to key if not null
    if inJNum == 'null' or inJNum == 'Null' or inJNum == '':
        referenceKey = ''
    elif inJNum in dbJNumMap:
        referenceKey = dbJNumMap[inJNum]
    else:
        print('Cannot resolve JNumber: %s See line %s' % (inJNum, currentLine))
        moveOn = 1
    if moveOn == 1:
        errorCt = errorCt + 1
        moveOn = 0
        continue
    bcpFile.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' %
        (nextAvailableDerivKey, colDelim, inName, colDelim, inDescript, \
         colDelim, vectorKey, colDelim, vectorTypeKey, colDelim, \
         parentKey, colDelim, derivTypeKey, colDelim, creatorKey, \
         colDelim, referenceKey, colDelim, userKey, colDelim, userKey, \
         colDelim, date, colDelim, date, lineDelim))

    # increment the primary key
    nextAvailableDerivKey = nextAvailableDerivKey + 1

print('Total derivations not loaded because already in the database or resolving errors: %s' % errorCt)
#
# Post Process
#

inFile.close()
bcpFile.close()

print('%s' % mgi_utils.date())
db.useOneConnection(0)
