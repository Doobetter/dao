#!/usr/bin/env bash
######################################################################################################
#Descibe: Stop all the topologies if its' properties in audit/conf and  name like AUDIT_*_V*_V*_*.properties
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

function stopByTopic
{
    TOPIC_NAME=$1
    if [[ "$TOPIC_NAME" = "" ]] ;then
        echo "[ERROR] Topic Name should not be null !"
        return -1
    fi
    echo "Now stop a topology for topic $TOPIC_NAME "
    ####storm list
    TOPIC_WITHOUT_DOT=`echo $TOPIC_NAME |sed 's/\./-/g'`
    TP_LIST=`storm list |grep $TOPIC_WITHOUT_DOT | grep 'ACTIVE'|awk '{print $1}'|sed ':a;N;s/\n/ /;ta'`
    
    
    if [ "$TP_LIST" = "" ] ;then
        echo "[INFO] $TOPIC_NAME has no topology in storm cluster!"
        return 0
    fi
    for tn in $TP_LIST 
    do
        if [[ $tn != "" ]] ;then
            storm kill $tn
        fi
    done

}

echo "Now begin to stop all the topologies that has a standard properties in the directory /conf "

TOPIC_CONF_LIST=`ls $CONF_PATH/AUDIT_*_V*_V*_*.properties`
if [ "$TOPIC_CONF_LIST" = "" ];then
    echo "No standard properties in conf !"
    exit -1
fi

for conf in $TOPIC_CONF_LIST
do
    ## GET FILE NAME 
    TOPIC=`echo $conf | sed 's/.*\///'|sed 's/\.pro.*//'`
    ##--------------------------  stop  --------------------------## 
    stopByTopic $TOPIC   
    if [ $? != 0  ] ; then
        echo "[ERROR] stop  topology for $TOPIC failed!"
        exit -1
    fi 
done   
echo "Stop all the topology Successfully !" 

