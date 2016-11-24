#!/usr/bin/env bash
######################################################################################################
#Descibe: Stop a topology 
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
function usage
{
    $ECHO "\n#######################USAGE#####################################"
    $ECHO "\n\tUsage:$SCRIPT_NAME  -topicName topic -jobType (normal|abNormal|reload) \n"
    $ECHO "\n\t\t Eg. $SCRIPT_NAME  -topicName AUDIT_MCSS_V4.2.0_V2.6.8_D -jobType normal \n"
    $ECHO "\n\t\t Notice: topicName must be the first parameter and jobType is the second .\n"
    $ECHO "###################################################################\n"
}  


if [ $# -ne 4 ]
then
    echo "argument error"
    usage
    exit 1
fi
TOPIC_NAME=$2
JOB_TYPE=$4
if [[ $TOPIC_NAME"x" = "x" ]] ;then
    echo "[ERROR] Topic Name should not be null !"
fi
echo "Now stop a topology for topic $TOPIC_NAME \n"
echo "Job type is $JOB_TYPE"

####storm list
TOPIC_WITHOUT_DOT=`echo $TOPIC_NAME |sed 's/\./-/g'`
TP_LIST=`storm list |grep $TOPIC_WITHOUT_DOT |grep ACTIVE`
if [ "$TP_LIST" = "" ] ;then
    echo "[INFO] $TOPIC_NAME has no active topology in storm cluster!"
    exit -1
fi

#### need to check  topic exist
TOPOLOGY_NAME=""
if [[ $JOB_TYPE = "normal" ]] ; then
    TOPOLOGY_NAME=`echo $TP_LIST|grep AUDIT_NORMAL_`
elif [[ $JOB_TYPE = "abNormal" ]] ; then
    TOPOLOGY_NAME=`echo $TP_LIST|grep AUDIT_ABNORMAL_`
elif [[ $JOB_TYPE = "reload" ]] ;then
    TOPOLOGY_NAME=`echo $TP_LIST|grep AUDIT_RELOAD_`
else 
   echo "\n[ERROR] jobType is wrong "
   usage  
   exit 1
fi
if [[ $TOPOLOGY_NAME != "" ]] ;then
    TOPOLOGY_NAME=`echo $TOPOLOGY_NAME |awk '{print $1}'`
    storm kill $TOPOLOGY_NAME > /dev/null
    if [ $? != 0 ];then
        echo "Stop the topology failed !"
    fi
fi
echo "Stop the topology Successfully !"

