#!/bin/sh
#
#  derivationload.sh
###########################################################################
#
#  Purpose: 
#	This script creates a bcp file for ALL_CellLineDerivation and
#       executes bcp in
#
  Usage="derivationload.sh fullInputFilePath"
#
#  Env Vars:
#
#      See the configuration file
#
#  Inputs:
#
#      - Common configuration file -
#		/usr/local/mgi/live/mgiconfig/master.config.sh
#      - Derivation load configuration file - derivationload.config
#      - MGD database to resolve objects to keys
#      - input file - see python script header
#	 
#
#  Outputs:
#
#      - An archive file
#      - Log files defined by the environment variables ${LOG_PROC},
#        ${LOG_DIAG}, ${LOG_CUR} and ${LOG_VAL}
#      - BCP files for for inserts to ALL_CellLine_Derivation
#      - Records written to the database tables
#      - Exceptions written to standard error
#      - Configuration and initialization errors are written to a log file
#        for the shell script
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  Fatal error occurred
#      2:  Non-fatal error occurred
#
#  Assumes:  Nothing
#
#  Implementation:  
#
#  Notes:  None
#
###########################################################################

#
#  Set up a log file for the shell script in case there is an error
#  during configuration and initialization.
#
cd `dirname $0`/..
LOG=`pwd`/derivationload.log
rm -f ${LOG}

#
#  Verify the argument(s) to the shell script.
#
if [ $# -ne 1 ]
then
    echo "Usage: $0" | tee -a ${LOG}
    exit 1
fi

INFILE_NAME=$1
export INFILE_NAME


#
#  Make sure the input file is readable.
#

if [ ! -r ${INFILE_NAME} ]
then
    echo "Cannot read input file: ${INFILE_NAME}" | tee -a ${LOG}
    exit 1
fi

#
#  Establish the configuration file name.
#
CONFIG_LOAD=`pwd`/derivationload.config
#
#  Make sure the configuration file is readable.
#
if [ ! -r ${CONFIG_LOAD} ]
then
    echo "Cannot read configuration file: ${CONFIG_LOAD}" | tee -a ${LOG}
    exit 1
fi

#
# source config file
#
echo "Sourcing config file"

. ${CONFIG_LOAD}

#
#  Source the DLA library functions.
#
if [ "${DLAJOBSTREAMFUNC}" != "" ]
then
    if [ -r ${DLAJOBSTREAMFUNC} ]
    then
	. ${DLAJOBSTREAMFUNC}
    else
	echo "Cannot source DLA functions script: ${DLAJOBSTREAMFUNC}"
	exit 1
    fi
else
    echo "Environment variable DLAJOBSTREAMFUNC has not been defined."
    exit 1
fi

##################################################################
##################################################################
#
# main
#
##################################################################
##################################################################

#
# createArchive including OUTPUTDIR, startLog, getConfigEnv, get job key
#
preload ${OUTPUTDIR}

#
# rm all files/dirs from OUTPUTDIR RPTDIR
#
cleanDir ${OUTPUTDIR} ${RPTDIR}

#
# run the derivation load
#
echo "\n`date`" >> ${LOG_DIAG} ${LOG_PROC}
echo "Running derivation load using ${INFILE_NAME}" | tee -a ${LOG_DIAG} ${LOG_PROC}

${DERIVATIONLOAD}/bin/derivationload.py ${INFILE_NAME} >> ${LOG_DIAG} 2>&1
STAT=$?
checkStatus ${STAT} "derivationload.py"

#
# bcp in ALL_CellLine_Derivation, dropping/recreating indexes
#

# drop indexes
echo "" | tee -a ${LOG_DIAG}
date | tee -a ${LOG_DIAG}
echo "Drop indexes on ${TABLE} table" | tee -a ${LOG_DIAG}
${MGD_DBSCHEMADIR}/index/${TABLE}_drop.object >> ${LOG_DIAG} 2>&1

#  bcp in
date | tee -a ${LOG_DIAG}
echo "bcp in  ${TABLE} " | tee -a ${LOG_DIAG}
echo "" | tee -a ${LOG_DIAG}
${MGI_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} ${TABLE} ${OUTPUTDIR} ${OUTFILE_NAME} ${COLDELIM} ${LINEDELIM} >> ${LOG_DIAG} 2>&1
STAT=$?
if [ ${STAT} -ne 0 ]
then
    echo "${MGI_DBUTILS}/bin/bcpin.csh failed" | tee -a ${LOG_DIAG}
    exit 1
fi

# create indexes
echo "" | tee -a ${LOG_DIAG}
date | tee -a ${LOG_DIAG}
echo "Create indexes on ${TABLE} table" >> ${LOG_DIAG} 2>&1
${MGD_DBSCHEMADIR}/index/${TABLE}_create.object >> ${LOG_DIAG} 2>&1

#
# run postload cleanup and email logs
#
shutDown

exit 0

