#!/usr/bin/env bash
######################################################################################################
#Descibe: Start a topology 
#Author : liucheng
#Date:2014-11-20
######################################################################################################

#set -x
ECHO=echo
case $SHELL in
    */bin/[b,B]ash)
        ECHO="echo -e"
        ;;
esac

SCRIPT_NAME=$( basename $0 )
DIR_NAME=$( dirname "${BASH_SOURCE-$0}" )
LIB_PATH=$( dirname `readlink -f $DIR_NAME ` )"/lib"
BIN_PATH=$( dirname `readlink -f $DIR_NAME ` )"/bin"
#### need to check  topic exist
MAIN_CLASS="com.travelsky.roc.topology.example.ObjectTransportTopology"
JAR_NAME="roc-0.0.1-SNAPSHOT.jar"
/usr/local/share/storm/bin/storm jar $LIB_PATH"/topology/"$JAR_NAME $MAIN_CLASS test1


