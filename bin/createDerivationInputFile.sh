#!/bin/sh
#
#  derivationload.sh
###########################################################################
#
#  Purpose: 
#	This script creates a derivationload input file
#       for existing parent cell lines in the database
#
  Usage="Usage: derivationload.sh outputFilePath"
#
###########################################################################

#
#  Set up a log file for the shell script in case there is an error
#  during configuration and initialization.
#
cd `dirname $0`/..
LOG=`pwd`/createDerivationInputFile.log
rm -f ${LOG}

#
#  Verify the argument(s) to the shell script.
#
if [ $# -ne 1 ]
then
    echo $Usage | tee -a ${LOG}
    exit 1
fi

OUTFILE_NAME=$1
export OUTFILE_NAME

#
#  If the MGICONFIG environment variable does not have a local override,
#  use the default "live" settings.
#

if [ "${MGICONFIG}" = "" ]
then
    MGICONFIG=/home/sc/genetraps/mgiconfig
    export MGICONFIG
fi

#  establish name of master config and source it
CONFIG_MASTER=${MGICONFIG}/master.config.sh

export CONFIG_MASTER

. ${CONFIG_MASTER}

##################################################################
##################################################################
#
# main
#
##################################################################
##################################################################


${DERIVATIONLOAD}/bin/createDerivationInputFile.py >> ${LOG_DIAG} 2>&1

exit 0

