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
CONF_PATH=$( dirname `readlink -f $DIR_NAME ` )"/conf"
DATA_PATH=$( dirname `readlink -f $DIR_NAME ` )"/data"
JAR_NAME=""
CONF_NAME=""
function usage
{
    $ECHO "\n#######################USAGE#####################################"
    $ECHO "\n\tUsage:$SCRIPT_NAME --jar jarName [--main mainClass] --conf fileName \n"
    $ECHO "\n\tEg. $SCRIPT_NAME --jar roc-client-0.0.1-SNAPSHOT.jar  --conf topology-example01.xml \n"
    $ECHO "###################################################################\n"
}  


if [ $# -ne 6 ]
then
    echo "argument error"
    usage
    exit 1
fi

while [ $# -gt 1 ]
do
    if [ "--jar" = "$1" ]
    then
        shift
        JAR_NAME=$1
        shift
    elif [ "--main" = "$1" ]
    then
        shift
        MAIN_CLASS=$1
        shift
    elif [ "--conf" = "$1" ]
    then
        shift
        CONF_NAME=$1
        shift
    else
        # Presume we are at end of options and break
        break
    fi
done


if [[ $JAR_NAME"x" = "x" ]] ;then
    echo "[ERROR] JAR Name should not be null !"
    exit 1
fi

if [[ $CONF_NAME"x" = "x" ]] ;then
    echo "[ERROR] Conf Name should not be null !"
    exit 1
fi

if [[ $MAIN_CLASS"x" = "x" ]] ; then 
    echo "[Error] No main class"
    exit 1
fi
echo "Now begin to start a topology from  $CONFIG_FILE_NAME "
COMMON_CONF_PATH=$CONF_PATH"/topology-common.xml"
SPECIAL_CONF_PATH=$CONF_PATH"/"$CONF_NAME

if [ $MAIN_CLASS != "" ] ;then
   echo '33' 
   /usr/local/share/storm/bin/storm jar $LIB_PATH"/topology/"$JAR_NAME  $MAIN_CLASS  --commonFilePath=$COMMON_CONF_PATH --specialFilePath=$SPECIAL_CONF_PATH --noteFileDir=$DATA_PATH
fi
echo "111"

