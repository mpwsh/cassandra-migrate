#!/bin/bash
source utils.sh
set -e
#set -x
#Usage ./prepare-source.sh --node-id <node-id>
#Example: ./prepare-source.sh --node-id 0

get_tables(){
got_keyspace=$1
#try config first if not, get all from keyspace with cqlsh
readarray -t tables < <(jq --arg keyspace_name "$got_keyspace" -r '.snapshot.keyspace[] | select(.name==$keyspace_name) .tables[]?' "$json_path")
#trying with cqlsh
if [ ${#tables[@]} -le 0 ]; then
  loginfo "[$node_name] No tables to validate from keyspace $got_keyspace found in config. Validating all tables from keyspace." && \
  readarray -t tables < <(cassutils cqlsh "${cqlsh_args[@]}" -e "SELECT json * FROM system_schema.tables WHERE keyspace_name = '"$got_keyspace"';" | grep -v 'json\| rows\|\-\-\-\-' | jq -r .table_name)
fi
if [ ${#tables[@]} -le 0 ]; then
    logwarn "No tables found in keyspace $got_keyspace. Skipping"
fi
}

get_keyspaces(){
readarray -t keyspaces < <(jq -r '.snapshot.keyspace[] .name?' "$json_path")
if [ "$keyspaces" == "" ];then
  loginfo "Keyspaces not defined in config. Using all keyspaces."
  exclude_system_keyspaces=$(get_config source.exclude_system_keyspaces)
  readarray -t raw_keyspaces < <(cassutils cqlsh ${cqlsh_args[@]} -e "SELECT json * FROM system_schema.keyspaces" | grep -v 'json\| rows\|\-\-\-\-' | jq -r .keyspace_name)
 [ $exclude_system_keyspaces == true ] && keyspaces=($(echo "${raw_keyspaces[@]}" | grep -vw "system\|system_auth\|db_reaper\|system_schema\|system_traces\|system_distributed"))
 fi
}

get_table_stats(){
	nodetool tablestats
}
get_keyspace_stats(){
	nodetool  "${nodetool_args[@]}" cfstats -F json -- eta.routes_v1
}
clear_old_snapshot(){
#Remove snapshot after copying backup
for keyspace in "${keyspaces[@]}";do
loginfo "[$node_name] Clearing snapshot from keyspace $keyspace with name $snapshot_name from Cassandra data path"
cassutils nodetool "${nodetool_args[@]}" clearsnapshot -t "$snapshot_name" "$keyspace"
done
}

take_snapshot() {
clear_old_snapshot
for keyspace in "${keyspaces[@]}";do
  loginfo "[$node_name] Starting nodetool snapshot on keyspace $keyspace - snapshot name: $snapshot_name"
  readarray -t save_tables < <(jq --arg keyspace_name "$keyspace" -r '.snapshot.keyspace[] | select(.name==$keyspace_name) .tables[]?' "$json_path")
  if [ -n "$save_tables" ];then
  for table in "${save_tables[@]}";do
    loginfo "[$node_name] Taking snapshot of table $table"
	cassutils nodetool "${nodetool_args[@]}" flush -- "$keyspace" "$table"
	cassutils nodetool "${nodetool_args[@]}" cleanup -- "$keyspace" "$table"
  	cassutils nodetool "${nodetool_args[@]}" snapshot -t "$snapshot_name" -cf "$table" "$keyspace"
  done
  else
    	cassutils nodetool "${nodetool_args[@]}" flush "$keyspace"
  	cassutils nodetool "${nodetool_args[@]}" cleanup "$keyspace"
  	cassutils nodetool "${nodetool_args[@]}" snapshot -t "$snapshot_name" "$keyspace"
   fi
done
#Copy files to snapshot_path and compress (if enabled)
#uncomment after debug
loginfo "[$node_name] Finding data folder with snapshots and moving to backup path: $snapshot_path"
find "${data_path:?}" -type f -path "*snapshots/$snapshot_name*" -printf %P\\0 | rsync -qavP --files-from=- --from0 "$data_path" "$snapshot_path"
old_data_path=$(find "$snapshot_path" -name 'data')
if [[ -z "$old_data_path" ]];then
  logerror "Old data path is empty! This might cause issues when moving files."
  logerror "This error might appear because the 'source.data_path' is set to an incorrect value."
  logerror "For Kubernetes PV use: /var/lib/kubelet/plugins/kubernetes.io/aws-ebs/mounts/aws"
  logerror "For DCOS use: /var/lib/mesos"
  logerror "For Cassandra on VM/EC2 use: /var/lib/cassandra"
  exit 1
else
#loginfo "[$node_name] Moving data $old_data_path to prettier path: $snapshot_path"
mv ${old_data_path:?}/* $snapshot_path
fi
#rm -rf $(cut -d/ -f5 <<< $old_data_path)
empty_path=$(cut -d/ -f5 <<< "$old_data_path")
if [[ -z "$empty_path" ]];then
  logerror "Old data path is empty! This might cause issues when removing files. "
  logerror "Please verify your source.data_path in config.toml and try again"
  exit 1
else
#loginfo "[$node_name] Removing old path $snapshot_path/$empty_path"
rm -rf "${snapshot_path:?}/$empty_path"
rm -rf "$snapshot_path/data"
rm -rf "$snapshot_path/pods"
fi
if [[ "$(get_config snapshot.compressed)" == true ]];then
#compress
loginfo "[$node_name] Compressing snapshot to $snapshot_name/$node_name.tar.gz"
tar cvfz "$work_path"/snapshot/"$snapshot_name"/"$node_name".tar.gz -C "$snapshot_path" . &>/dev/null
#remove uncompressed folder
loginfo "[$node_name] Removing uncompressed folder $snapshot_path"
rm -rf "$snapshot_path"
fi
clear_old_snapshot
}

upload_snapshot(){

#s3 details
s3_bucket=$(get_config s3.bucket_name || echo "null")
s3_prefix=$(get_config s3.prefix || echo "null")
s3_region=$(get_config s3.region || echo "null")
aws_profile=$(get_config s3.aws_profile || echo "null")
#awscli
export AWS_DEFAULT_REGION="$s3_region"
export AWS_REGION="$s3_region"
export AWS_PROFILE="$aws_profile"
export AWS_DEFAULT_OUTPUT=json
#aws creds debug
#cassutils aws sts get-caller-identity

  if [[ $s3_prefix != "null" ]];then
    s3_path="$s3_bucket/$s3_prefix/$snapshot_name/$node_name"
  else
    s3_path="$s3_bucket/$snapshot_name/$node_name"
  fi
  loginfo "[$node_name] Uploading snapshot to S3 bucket: s3://$s3_path"
  if [[ $(get_config snapshot.compressed) == true ]];then
      cassutils aws s3 cp "$snapshot_path".tar.gz s3://"$s3_path".tar.gz
  else
      cassutils aws s3 cp --recursive "$snapshot_path" s3://"$s3_path"/ &>/dev/null
  fi
}

#prepare variables
init_env() {
#identity
ssh_user=$(get_config source.ssh_user)
ssh_key=$(get_config source.ssh_key_path)
host_ip=$(get_config source.nodes["$node_id"].host_ip)
node_name=$(get_config source.nodes["$node_id"].name)
listen_ip=$(get_config source.nodes["$node_id"].listen_ip || echo "$host_ip")
#workspace
export TERM=dumb
json_path=config.json
work_path=$(get_config source.work_path)
data_path=$(get_config source.data_path)

#Snapshot
snapshot_name=$(get_config snapshot.name)
snapshot_path="./snapshot/$snapshot_name/$node_name"
[ $validate -ne 1 ] && rm -rf "$work_path/snapshot" && mkdir -p "$work_path/snapshot/$snapshot_name/$node_name"
#cassandra credentials
cassandra_user=$(get_config source.cassandra.user || echo "null")
cassandra_pw=$(get_config source.cassandra.password || echo "null")
#prepare cqlsh args
#initalize cqlsh args
declare -ag cqlsh_args=()
[ "$cassandra_pw" != "null" ] && cqlsh_args+=("-u" "$cassandra_user") && cqlsh_args+=("-p" "$cassandra_pw")
cqlsh_args+=("$listen_ip")
cqlsh_args+=("--connect-timeout=$(get_config validation.connection_timeout || echo "200")")

#initalize nodetool args
declare -ag nodetool_args=()
[ "$cassandra_pw" != "null" ] && nodetool_args+=("-u" "$cassandra_user") && nodetool_args+=("-pw" "$cassandra_pw")
#nodetool_args+=("-h $listen_ip")

#retrieve keyspaces
get_keyspaces

#Docker
cassutils_image="$(get_config cassutils_image)"
docker image inspect "$cassutils_image" > /dev/null || loginfo "[$node_name] Pulling cassutils image from $cassutils_image" && docker pull "$cassutils_image" &>/dev/null && logsuccess "[$node_name] Found cassutils docker image in host"
yum install -y rsync &>/dev/null
}

keyspace_exists(){
 keyspace=$1
 cassutils cqlsh -u "$cassandra_user" -p "$cassandra_pw" -e "SELECT * FROM system_schema.keyspaces WHERE keyspace_name='""$keyspace""'" "$listen_ip" 9042 &> /dev/null
 echo $?
 if [ $? -ne 0 ];then
  logerror "[$node_name] Error while validating keyspace $keyspace. Check your configuration"
  exit
 fi
}

validate_data(){
#Create CSV structure
file_date=$(now_date file)
report_filename="source-row-validation-$file_date.csv"
echo "keyspace,table_name,row_count" > $report_filename
loginfo "[$node_name] Creating file $report_filename with validation results"
#Retrieving keyspaces
for keyspace in "${keyspaces[@]}";do
  [ -z "$keyspace" ] && logerror "[$node_name] Keyspace name not found in config" && exit
  #retrieve tables from config
  get_tables "$keyspace"
  keyspace_exists $keyspace
  loginfo "[$node_name] Starting validation of keyspace $keyspace"
  for table in "${tables[@]}";do
    [ -z "$table" ] && logerror "[$node_name] Keyspace name not found in config" && exit
    loginfo "[$node_name] Validating rows from keyspace $keyspace, table: $table"
    row_count=$(cassutils cqlsh "${cqlsh_args[@]}" -e "COPY $keyspace.$table to '/dev/null' with numprocesses=$(get_config validation.numprocesses || echo 1) AND PAGESIZE=5000 AND PAGETIMEOUT=$(get_config validation.connection_timeout || echo 200) AND MAXATTEMPTS=50;" | sed -n 5p | sed 's/ .*//' 2>/dev/null)
    # Show an error if row count is empty
    if [[ "$row_count" == *":1:Error"* ]] || [[ -z "$row_count" ]];then
	logerror "[$node_name] Error while validating data from table $table in keyspace $keyspace"
    else
    logsuccess "$keyspace.$table row count: $row_count"
    echo "$keyspace,$table,$row_count" >> $report_filename
    fi
  done
done
logdebug "Printing results in csv format"
cat "$report_filename"
loginfo "Download the report locally using:"
loginfo "scp -o StrictHostKeyChecking=no -i $ssh_key $ssh_user@$host_ip:$work_path/$report_filename ."
}
validate_config(){
	if [ ! -d "$data_path" ];then
		logerror "Provided data_path does NOT exist. Please verify your config.toml file and try again"
		exit 1
	fi
}

cassutils() {
  docker run --rm -ti -e TERM -e AWS_DEFAULT_OUTPUT -e AWS_REGION -e AWS_PROFILE -e AWS_DEFAULT_OUTPUT \
  --network=host -v "$work_path":/root "$cassutils_image" "$@"
}

skip_upload=0
validate=0
unset PARAMS
while (( "$#" )); do
  case "$1" in
    --no-upload)
      skip_upload=1
      loginfo "Skipping snapshot upload to S3 bucket."
      shift
      ;;
    --validate)
      validate=1
      loginfo "Executing validation process"
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

init_env
validate_config
[ $validate -ne 1 ] && take_snapshot
[ $validate -ne 1 ] && [ $skip_upload -ne 1 ] && upload_snapshot
[ $validate -eq 1 ] && validate_data
logsuccess "[$node_name] All steps completed! Closing session"
