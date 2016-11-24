#!/usr/bin/env bash
######################################################################################################
#Descibe: Start all the topologies if its' properties in audit/conf and  name like AUDIT_*_V*_V*_*.properties
#Author : liucheng
#Date:2014-11-21
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
CONF_PATH=$( dirname `readlink -f $DIR_NAME ` )"/conf"

function usage
{
    $ECHO "\n#######################USAGE#####################################"
    $ECHO "\n\tUsage:$SCROPT_NAME \n"
    $ECHO "\n\t Notice: \n"
    $ECHO "###################################################################\n"
}  

echo "Now begin to start all the topologies that has a standard properties in the directory /conf "

CONFIG_FILE_LIST=`ls $CONF_PATH/AUDIT_*_*_*_*.properties`
echo $CONFIG_FILE_LIST
if [[ "$CONFIG_FILE_LIST" = "" ]];then
    echo "No standard properties in conf !"
    exit -1
fi

for conf in $CONFIG_FILE_LIST
do
    CONFIG_FILE_NAME=`echo $conf | sed 's/.*\///'`
    ##--------------------------  normal job --------------------------## 
    echo "[INFO] Start normal topology for config file $CONFIG_FILE_NAME"
    storm jar $LIB_PATH/"audit-tracker-0.0.1-SNAPSHOT-jar-with-dependencies.jar" com.travelsky.bdp.audit.topology.NormalTopology  $CONFIG_FILE_NAME
    if [ $? != 0  ] ; then
        echo "[ERROR] start normal topology for $TOPIC failed!"
    fi 
    ##--------------------------abNormal job --------------------------##
    echo "[INFO] Start abNormal topology for $TOPIC"
    # todo
    if [ $? != 0  ] ; then
        echo "[ERROR] start abNormal topology for $TOPIC failed!"
    fi

    ##--------------------------reload job --------------------------##
    echo "[INFO] Start reload topology for $TOPIC"
    # todo
    if [ $? != 0  ] ; then
        echo "[ERROR] start reload topology for $TOPIC failed!"
    fi
done    

