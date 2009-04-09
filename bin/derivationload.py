#!/usr/local/bin/python

##########################################################################
#
# Purpose:
# 	This script parses the input file, resolves terms to keys and
#       creates a bcp file for ALL_CellLine_Derivation
#
# Usage: derivationload.py
# Env Vars:
#	 1. INFILE_NAME
#	 2. OUTPUTDIR
#	 3. OUTFILE_NAME
#        4. JOBSTREAM
#
# Inputs:
#        INFILE_NAME is an input file in tab-delimitted file format:
#        1. Derivation Name (varchar 255, may be null)
#        2. Description (varchar 255, may be null)
#        3. Vector Name (Cell Line Vector Name Vocab Term)
#        4. Vector Type (Cell Line Vector Type Vocab Term)
#        5. Parent Cell Line Name (ALL_CellLine.cellLine)
#        6. Parent Cell Line Strain (ALL_CellLine._Strain_key)
#        7. Cell Line Creator (Cell Line Creator Vocab Term, may be null)
#        8. Reference (JNumber, may be null)
#	 9. Derivation Type (Allele Type Vocab Term)
# Outputs:
#	 1. bcp file 
#	 2. log files
# 
# Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
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
#db.set_sqlLogFunction(db.sqlLogAll)
print '%s' % mgi_utils.date()

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
results = db.sql('select derivKey = max(_Derivation_key) + 1 ' + \
                'from ALL_CellLine_Derivation', 'auto')
nextAvailableDerivKey = results[0]['derivKey']

# get load user key
results = db.sql('select _User_key ' + \
		'from MGI_User ' + \
		'where login = "%s"' % user, 'auto')
userKey = results[0]['_User_key']

# get list of named derivations in the database, so we don't add dups
results = db.sql('select name ' + \
	'from ALL_CellLine_Derivation ' + \
	'where name != null', 'auto')
for r in results:
     dbDerivNameList.append(r['name'])

# map database vector name terms to their term keys
results = db.sql('select term, _Term_key ' + \
	'from VOC_Term ' + \
	' where _Vocab_key = 72', 'auto')
for r in results:
    dbVectorNameMap[r['term']] = r['_Term_key']


# map database vector type terms to their term keys
results = db.sql('select term, _Term_key ' + \
	'from VOC_Term ' + \
	'where _Vocab_key = 64', 'auto')
for r in results:
    dbVectorTypeMap[r['term']] = r['_Term_key']

# map parent cell line names to their  cell line keys
results = db.sql('select a.cellLine, s.strain,  a._CellLine_key ' + \
	'from ALL_CellLine a, PRB_Strain s ' + \
	'where a.isMutant = 0 ' + \
	'and a._Strain_key = s._Strain_key', 'auto')
for r in results:
    key = '%s|%s' % (r['cellLine'], r['strain'])
    dbParentMap[key] = r['_CellLine_key']

# map derivation type (aka allelel type) terms to their term keys
results = db.sql('select term, _Term_key ' + \
        'from VOC_Term ' + \
        'where _Vocab_key = 38', 'auto')
for r in results:
    dbDerivTypeMap[r['term']] = r['_Term_key']

# map database creator name terms to their term keys
results = db.sql('select term, _Term_key ' + \
	'from VOC_Term ' + \
	'where _Vocab_key = 62', 'auto')
for r in results:
    dbCreatorMap[r['term']] = r['_Term_key']

# map database Jnumbers to their reference keys
results = db.sql('select accID, _Object_key ' + \
	'from ACC_Accession ' + \
	'where _MGIType_key = 1 ' + \
	'and _LogicalDB_key = 1 ' + \
	'and prefixPart = "J:"', 'auto')
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

for line in inFile.readlines():
    lineList = string.split(line, TAB)
    if len(lineList) != 9:
	sys.exit('Line has only %s of 8 columns: %s ' %(len(lineList), line))
    
    inName = string.strip(lineList[0])		# may be null
    if len(inName) > 255:
	sys.exit('Derivation name > 255: %s' % inName)
    inDescript = string.strip(lineList[1]) 	# may be null
    if len(inDescript) > 255:
	sys.exit('Derivation description > 255: %s' % inName)
    inVector = string.strip(lineList[2])
    inVectorType = string.strip(lineList[3])
    inParent = string.strip(lineList[4])
    inStrain = string.strip(lineList[5])
    inCreator = string.strip(lineList[6]) 	# may be null
    inJNum = string.strip(lineList[7]) 		# may be null
    inDerivType = string.strip(lineList[8])

    # check to see if there is a derivation by this name already in the db
    if inName != 'null' and inName != 'Null':
	if inName in dbDerivNameList:
	    sys.exit('Derivation name already in database: %s' % inName)
    else:
	inName = ''

    # resolve vector to key
    if dbVectorNameMap.has_key(inVector):
        vectorKey = dbVectorNameMap[inVector]
    else:
	sys.exit('Cannot resolve Vector Name: %s' % inVector)

    # resolve vector type to key
    if dbVectorTypeMap.has_key(inVectorType):
        vectorTypeKey = dbVectorTypeMap[inVectorType]
    else:
        sys.exit('Cannot resolve Vector Type: %s' % inVectorType)

    # resolve parent to key
    parentAndStrain = '%s|%s' % (inParent, inStrain)
    if dbParentMap.has_key(parentAndStrain):
        parentKey = dbParentMap[parentAndStrain]
    else:
        sys.exit('Cannot resolve Parent: %s' % parentAndStrain)

    # resolve derivation type to key
    if dbDerivTypeMap.has_key(inDerivType):
        derivTypeKey = dbDerivTypeMap[inDerivType]
    else:
        sys.exit('Cannot resolve Derivation Type: %s' % inDerivType)

    # resolve creator to key if not null
    if inCreator != 'null' and inCreator != 'Null': 
	if dbCreatorMap.has_key(inCreator):
	    creatorKey = dbCreatorMap[inCreator]
	else:
	    sys.exit('Cannot resolve Creator name: %s' % inCreator)
    else: 
	creatorKey = ''

    # resolve JNumber to key if not null
    if inJNum == 'null' or inJNum == 'Null':
	referenceKey = ''
    elif dbJNumMap.has_key(inJNum):
	referenceKey = dbReferenceMap[inJNum]
    else:
	sys.exit('Cannot resolve JNumber: %s' % inJNum)

    bcpFile.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' %
	(nextAvailableDerivKey, colDelim, inName, colDelim, inDescript, \
	colDelim, vectorKey, colDelim, vectorTypeKey, colDelim, \
	parentKey, colDelim, derivTypeKey, colDelim, creatorKey, \
	colDelim, referenceKey, colDelim, userKey, colDelim, userKey, \
	colDelim, date, colDelim, date, lineDelim))

    # increment the primary key
    nextAvailableDerivKey = nextAvailableDerivKey + 1

#
# Post Process
#

inFile.close()
bcpFile.close()

print '%s' % mgi_utils.date()
db.useOneConnection(0)
