#!/usr/local/bin/python

##########################################################################
#
# Purpose:
#       To automatically generate an input file for the Derivation load
#	for parent cell line/strain pairs currently in the database
#       for use by the TR7493 migration
#
# Usage: createDerivationInputFile.py
# Env Vars:
#	 1. OUTFILE_NAME
#
# Inputs:
#	1. queries that database
# Outputs:
#	 1. tab delimited file in derivation load format:
#            1. Derivation Name i.e.
#	 	Creator_DerivationType_"Library"_ParentCellLine_\
#		        ParentCellLineStrain_Vector_DerivationReference
#            2. Description - null
#            3. Vector Name - "Not Specified"
#            4. Vector Type - "Not Specified"
#            5. Parent Cell Line Name - From database
#            6. Parent Cell Line Strain - From database
#            7. Cell Line Creator - "Not Specified"
#            8. DerivationReference - null
#            9. Derivation Type - Allele Type Vocab Term from database
#	 2. log file
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

outFilePath = os.environ['OUTFILE_NAME']

TAB= '\t'
NL = '\n'
SPACE = ' '

# set of derivation types (aka allele types) we'll use to create derivations
derivationTypes = []

# parent cell lines and associated information
# key = parent
# value = [strain1, ..., strainN]
parentCellLines = {}

# name is calculated
derivationName = ''
# reference and description are null
reference = ''
description = ''

vectorName = "Not Specified"
vectorType = "Not Specified"
creator = "Not Specified"

#
# Initialize
#

# get the list of derivationTypes we'll use from the 
# allele Type vocabulary
results = db.sql('select term ' + \
	'from VOC_Term ' + \
	'where _Vocab_key = 38', 'auto')
print "%s allele types" % len(results)

for r in results:
    derivationTypes.append(r['term'])

# get the parentCellLines names with their strains
results = db.sql('select cl.cellLine, s.strain ' + \
	    'from ALL_CellLine cl, PRB_Strain s ' + \
	    'where cl.isMutant = 0 ' + \
	    'and cl._Strain_key = s._Strain_key ' + \
	    'order by cl.cellLine', 'auto')

print "%s parent cell lines" % len(results)
for r in results:
    cellLine = r['cellLine']
    strain = r['strain']
    if parentCellLines.has_key(cellLine):
	strainList = parentCellLines[cellLine]
	strainList.append(strain)
	parentCellLines[cellLine] = strainList
    else:
	strainList = [strain]
	parentCellLines[cellLine] = strainList

#for p in parentCellLines.keys():
#    print "%s %s" % (p, parentCellLines[p])

try:
    outFile = open(outFilePath, 'w')
except:
    exit('Could not open file for writing %s\n' % outFilePath)


#
# Process
#
for p in parentCellLines.keys():
    #print p
    strainList = parentCellLines[p]
    for s in strainList:
	for d in derivationTypes:
	    name = creator + SPACE + d + SPACE + "Library" + \
	    SPACE + p + SPACE + s + SPACE + vectorName + \
	    SPACE + "Null"
	    outFile.write(name + TAB + description + TAB + \
		vectorName + TAB + vectorType + TAB + \
		p + TAB + s + TAB + creator + TAB + \
		reference + TAB + d + NL)

#
# Post Process
#

outFile.close()

print '%s' % mgi_utils.date()
db.useOneConnection(0)
