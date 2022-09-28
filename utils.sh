#!/bin/bash
set -e
trap ctrl_c INT

#GET SCRIPT PATH
#SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

#Formatting
RED='\033[0;31m'
LBLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
ORANGE='\033[0;33m'
NC='\033[0m'

get_config() {
   key=".$1"
   result=$(jq -r "$key" config.json)
   if [[ $result == "null" ]] || [[ -z $result ]];then
        return 1
   else
	echo "$result"
   fi
}
ctrl_c(){
	echo -en "\033[2K"
	echo -en "\n\r${RED}[*] Keyboard Interrupt${NC}"
	echo -e "\033[K"
	tput cnorm
	kill $! &> /dev/null
	exit 0
}

headerstyle(){
   echo -en "${ORANGE}>>> $@ ${NC}\n\r";
}
linestyle(){
  echo -en "${ORANGE}---------------------------------------------- $@ ${NC}\n\r";
}
logdebug(){
  echo -en "$(now_date log)[~] $@ ${NC}\n\r";
}
loginfo(){
   echo -en "${LBLUE}$(now_date log)[i] $@ ${NC}\n\r";
}
logwarn(){
  echo -en "${YELLOW}$(now_date log)[!] $@ ${NC}\n\r";
}
logerror(){
  echo -en "${RED}$(now_date log)[X] $@ ${NC}\n\r";
}
logsuccess(){
  echo -en "${GREEN}$(now_date log)[>] $@ ${NC}\n\r";
}
logfile(){
   echo "$(now_date log) $@" >> log.out
}

wait_with_spinner(){
    local pid=$!
    local delay=1
    local spinstr='|/-\'
    while [ "$(ps a | awk '{print $1}' | grep $pid)" ]; do
        local temp=${spinstr#?}
        printf " [%c]  " "$spinstr"
        local spinstr=$temp${spinstr%"$temp"}
        sleep $delay
        printf "\b\b\b\b\b\b"
    done
    printf "    \b\b\b\b"
}

now_date(){
DATE_OUTPUT=$1
case $DATE_OUTPUT in

  human|normal)
    date "+%d-%m-%y|%H:%M:%S"
    ;;
  log)
    date "+[%d-%m-%y|%H:%M:%S]"
    ;;
  file)
    date "+%y%m%d%H%M%S"
    ;;
  epoch)
    date +%s
    ;;

  *)
    date +%s
    ;;
esac
}

folder_not_found(){
	logerror "Could not find folder " "${@}"
}
print_time_spent(){
   time_start=$1
   time_end=$2
   total_time=$(echo "scale=2; ($time_end - $time_start)" | bc)
   loginfo "Installer took ${total_time} seconds"
   if (( "$total_time" <= 5 ));then
     logdebug "That was fast :D"
     logdebug "Did you forgot to add something to install?"
   fi
}
set_phase_timeout(){
  unset phase_failed
  phase_timeout=$1
  now_date_epoch=$(date +%s)
  phase_timeout_expiration=$(expr "$now_date_epoch" + "$phase_timeout")
  loginfo "Next phase will timeout in ${phase_timeout} seconds"
}

check_phase_timeout(){
if [[ -z $phase_timeout ]];then
	logwarn "Phase timeout not set.. Add set_phase_timeout <secs> to function"
else
phase_time_left=$(expr "$phase_timeout_expiration" - $(now_date epoch))
if (( $(now_date epoch) < "$phase_timeout_expiration" ));then
	logdebug "Still waiting (${phase_time_left})..."
  	  else
	    logerror "Phase timeout reached!"
	    phase_failed=1
	    unset phase_timeout_expiration
	    unset phase_timeout
	    break;
	  fi
fi
}
#To use the dependency checker, pass an array to the function, like this:

#   dependencies=( kubectl helm jq gimme-aws-creds aws git kustomize )
#   check_dependencies "${dependencies[@]}"
check_dependencies(){
apps=("$@")
for app_name in "${apps[@]}";do
	if [ -z $(which $app_name) ];then
		logerror "$app_name Not found. Plase install and try again. (try using 'brew install ${app_name}', or search in github)"
		exit
	fi
done
}

validate_credentials(){
	loginfo "Validating AWS Credentials"
	if [[ -n $(aws sts get-caller-identity 2> /dev/null| grep -i $1) ]];then
		logsuccess "AWS token is still valid"
	else
		logwarn "AWS Token expired"
		aws_role="arn:aws:iam::${1}:role/bu-torq-poweruser"
		loginfo "Connecting to AWS account $1 with role $aws_role"
		okta_aws_profile=$(GIMME_AWS_CREDS_CRED_PROFILE=acc-role gimme-aws-creds --roles $aws_role | grep -i profile | awk '{print $3}')
		export AWS_PROFILE=$okta_aws_profile
		if aws sts get-caller-identity 1> /dev/null;then
		  logsuccess "Logged in to AWS successfully!"
		else
		  logerror "Unable to login to AWS... Please run 'aws sts get-caller-identity' manually and validate your AWS_PROFILE env var"
		  exit 1
		fi
		loginfo "Using profile: ${okta_aws_profile}"
	fi
}


display_help() {
   linestyle
   headerstyle ""
   headerstyle "Cassandra-Migrate | 1-Click migration for Cassandra clusters"
   headerstyle ""
   linestyle
   headerstyle "maintainer: mpw <x@mpw.sh>"
   linestyle
   headerstyle "Actions:"
   echo "snapshot       - Create snapshot (only source cluster will be used)"
   echo "restore        - Restore from snapshot (only target cluster will be used)"
   echo "migrate        - Create snapshot in source cluster and restore in target cluster"
   echo "validate       - Validate keyspace and table data in source or target cluster"
   echo "                 (Usage: validate source | validate target)"
   headerstyle "Options:"
   echo "-c|--config    - Specify a config file to use (Optional, default: config.toml)"
   echo "-n|--node-id   - Specify a node to run the desired action instead of running in all nodes (Optional)"
   echo "--wait         - Wait for each node to finish before continuing with the next."
   echo "                 Disables working with all nodes at the same time. (Optional. default: False)"
   echo "--no-upload    - Skip uploading the snapshot to S3. (Optional. Default: false)"
   echo " 		 This will just take a snapshot and save it to <work-path>/snapshot/"
   echo "--no-download  - Skip downloading the snapshot from S3. (Optional. Default: false)"
   echo "                 This asumes the snapshot is already present in <work-path>/snapshot/<snapshot-name> and will restore from there"
   echo "-h|--help      - Show this help message."

   headerstyle "Usage (Single task):"
   echo "./start.sh snapshot --config railcon-migration-config.toml"
   echo "./start.sh restore --no-download"
   headerstyle "Usage (Full migration):"
   echo "./start.sh migrate"
   headerstyle "Usage (Run data validation in target cluster):"
   echo "./start.sh validate target --config yourconfig.toml --node-id 0"
}
