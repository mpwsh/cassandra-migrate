#!/bin/bash
source utils.sh
set -e
#set -x

create_config(){
if [ -z "$toml_path" ];then
  toml_path=config.toml
  logdebug "Argument --config <config-file> not specified. Using config.toml"
else
  logdebug "Reading config from $toml_path"
fi
#json_path=${toml_path//\.toml/\.json}
json_path="config.json"
#convert config to json for jq
./tools/toml2json "$toml_path" > "$json_path"
}

prepare_env(){
#setup variables
rm -rf log.out && touch log.out
aws_profile=$(get_config s3.aws_profile)
loginfo "Retrieving AWS Credentials for account $aws_profile"
mkdir -p .aws
grep -A4 -iw "$aws_profile" ~/.aws/credentials > .aws/credentials
}


validate_config(){
target_node_count=$(jq '.target.nodes | length' $json_path || logerror "Unable to get number of target nodes")
loginfo "Found $source_node_count Source nodes"
loginfo "Found $target_node_count Target nodes"
}

prepare_node(){
#usage
#prepare_node source
#prepare_node target
side=$1
case $side in
	source)
	node_count=$(jq '.source.nodes | length' $json_path || logerror "Unable to get number of source nodes" && exit 1)
	task_script="create-snapshot.sh"
	node_args="${source_args[*]}"
	;;

	target)
	node_count=$(jq '.target.nodes | length' $json_path || logerror "Unable to get number of target nodes" && exit 1)
	task_script="restore-snapshot.sh"
	node_args="${target_args[*]}"
	;;
	*)
	logerror "Operation not supported" && exit
	;;
esac
for ((node_index = 0; node_index < $node_count; ++node_index)); do
	[ "$node_id" != "undefined" ] && node_index=$node_id
	node_ip=$(get_config $side.nodes["$node_index"].host_ip)
	node_name=$(get_config $side.nodes["$node_index"].name)
	ssh_user=$(get_config $side.ssh_user)
	ssh_key=$(get_config $side.ssh_key_path)
	work_path=$(get_config $side.work_path)
 	loginfo "Working with $side node: $node_name - IP: $node_ip"
	loginfo "Creating work path and snapshot folder"
	ssh -o StrictHostKeyChecking=no -i "$ssh_key" "$ssh_user@$node_ip" "mkdir -p $work_path" &>/dev/null
	loginfo "Sending required files to work on $side node: $node_name"
	scp -o StrictHostKeyChecking=no -i "$ssh_key" -r .aws utils.sh $task_script $json_path "$ssh_user@$node_ip:$work_path"  &>/dev/null
	loginfo "Executing script in $side node: $node_name"
	ssh -o StrictHostKeyChecking=no -tt -i "$ssh_key" "$ssh_user@$node_ip" "cd $work_path && sudo bash $task_script --node-id $node_index $node_args" && logfile "$node_name - All steps completed in node from $side cluster" &
	if [[ $wait_nodes -eq 1 ]] || [ $node_index -eq $node_id ];then
	wait_with_spinner
	fi
	[ "$node_id" != "undefined" ] && break;
done
}

wait_for_completion(){
side=$1
case side in
	source)
	node_count=$(jq '.source.nodes | length' $json_path || logerror "Unable to get number of source nodes" && exit 1)
	;;
	target)
	node_count=$(jq '.target.nodes | length' $json_path || logerror "Unable to get number of target nodes" && exit 1)
	;;
esac
	waiting=true
	while $waiting;do
	nodes_completed=0
	for ((node_index = 0; node_index < "$node_count"; ++node_index)); do
	  node_name=$(get_config $side.nodes["$node_index"].name)
	  if [ -n "$(grep -e "$node_name" log.out | grep -i completed)" ];then
		  #logsuccess "Node $node_name snapshot completed."
		  ((nodes_completed=nodes_completed+1))
	  fi
 	done
	if [[ "$node_id" != "undefined" ]] && [ $nodes_completed -eq 1 ];then
		grep -e "$node_name" log.out | grep -i completed
		waiting=false
		break;
	else
	if [ $nodes_completed -eq $node_count ] ;then
		grep -e "$node_name" log.out | grep -i completed
		waiting=false
		break;
	else
	    nodes_completed=0
	fi
	fi
	sleep 5
	done
}

validate_args(){
      # --node-id is required when running [validate]
      if [ $validate_target -eq 1 ] || [ $validate_source -eq 1 ] && [ "$node_id" == "undefined" ];then
	    node_id=0
	    logdebug "node-id not specified. Using node 0 for validation."
	    logdebug "Usage: ./start.sh validate --config <your-config>.toml --node-id 1"
      fi
}

#Main
headerstyle "Remember to re-login to AWS using gimme-aws-creds or awslogin to renew auth tokens first"
headerstyle "Starting..."

declare -a source_args=()
declare -a target_args=()
create_snapshot=0
restore_snapshot=0
validate_source=0
validate_target=0
migrate=0
wait_nodes=0
node_id="undefined"
unset PARAMS
while (( "$#" )); do
  case "$1" in
     snapshot)
      create_snapshot=1
      loginfo "Preparing source nodes to create a new snapshot"
      shift
      ;;
    restore)
      restore_snapshot=1
      loginfo "Preparing target nodes to restore from snapshot"
      shift
      ;;
    migrate)
      migrate=1
      loginfo "Starting migration process. Preparing source nodes"
      shift
      ;;
    validate)
      if [ -n "$2" ] && [ "${2:0:1}" != "-" ]; then
	if [ "${2}" == "source" ];then
      	  source_args+=("--validate")
	  loginfo "Executing row count validation in all tables from Source cluster"
	  validate_source=1
	fi
        if [ "${2}" == "target" ];then
      	  target_args+=("--validate")
	  validate_target=1
	  loginfo "Executing row count validation in all tables from Target cluster"
	fi
        shift 2
      else
	  loginfo "Executing row count validation in all tables from Source and Target cluster after completion"
	  #Output of this test should be saved in json and copied locally
      	  target_args+=("--validate")
      	  source_args+=("--validate")
	  validate_source=1
	  validate_target=1
	  shift
      fi
      ;;
    --no-upload)
      source_args+=("--no-upload")
      loginfo "Skipping snapshot upload to S3 bucket."
      shift
      ;;
    --no-download)
      target_args+=("--no-download")
      loginfo "Skipping snapshot download from S3 bucket."
      shift
      ;;
    -w|--wait)
      wait_nodes=1
      loginfo "--wait detected. Working with 1 node and waiting for completion before continuing with the next"
      shift
      ;;
    -c|--config)
      if [ -n "$2" ] && [ "${2:0:1}" != "-" ]; then
        toml_path="${2}"
        shift 2
      else
        logerror "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    -n|--node-id)
      if [ -n "$2" ] && [ "${2:0:1}" != "-" ]; then
        node_id="${2}"
        shift 2
      else
        logerror "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    -h|--help)
      display_help
      exit 0
      shift
      ;;
    --*=|-*) # unsupported flags
      display_help
      logerror "Error: Unsupported flag $1" >&2
      exit 1
      ;;
    *) # preserve positional arguments
      PARAMS="$PARAMS $1"
      shift
      ;;


esac
done

create_config
validate_args
linestyle
prepare_env
validate_config
#if backup or migration

#[ $create_snapshot -eq 1 ] || [ $migrate -eq 1 ] || [ $validate_source -eq 1 ] && prepare_source  && wait_for_snapshot
#[ $restore_snapshot -eq 1 ] || [ $migrate -eq 1 ] || [ $validate_target -eq 1 ] && prepare_target && wait_for_restore
[ $create_snapshot -eq 1 ] || [ $migrate -eq 1 ] || [ $validate_source -eq 1 ] && prepare_node "source" && wait_for_completion "source"
[ $restore_snapshot -eq 1 ] || [ $migrate -eq 1 ] || [ $validate_target -eq 1 ] && prepare_node "target" && wait_for_completion "target"

# Cleanup
loginfo "Removing AWS creds from $(pwd)"
rm -f aws-creds
loginfo "All done!"
