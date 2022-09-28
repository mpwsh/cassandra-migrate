#!/bin/bash
source utils.sh
set -e
#set -x
#Disable filename expansion
#set -f
#You should trigger this script from `./start.sh --restore --config config.toml` or `./start.sh --migrate --config config.toml`
#Usage ./prepare-target.sh --node-id <node-id>
#Example: ./prepare-target.sh --node-id 0

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
    logwarn "[$node_name] No tables found in keyspace $got_keyspace. Skipping"
fi
}

restore_snapshot() {
if [[ $(get_config snapshot.compressed) == true ]];then
    #decompress
    for keyspace in "${keyspaces[@]}";do
      tar -C "$snapshot_path" xvfz "./snapshot/$snapshot_name/$source_node_name.tar.gz" "$source_node_name/$keyspace" &>/dev/null
done
fi
for keyspace in "${keyspaces[@]}";do
#Save replication factor specified
replication_factor=$(jq --arg keyspace_name "$keyspace" -r '.snapshot.keyspace[] | select(.name==$keyspace_name) .replication_factor?' "$json_path")
#Get tables to restore from config
readarray -t tables_from_config < <(jq --arg keyspace_name "$keyspace" -r '.snapshot.keyspace[] | select(.name==$keyspace_name) .tables[]?' "$json_path")
if [[ $replication_factor == "null" ]];then
	replication_factor=$target_node_count
fi
#Check if keyspace name is also modified on restore and change it.
new_keyspace_name=$(jq --arg keyspace_name "$keyspace" -r '.snapshot.keyspace[] | select(.name==$keyspace_name) .restore_as?' "$json_path")
if [[ -n $new_keyspace_name ]] && [[ "$new_keyspace_name" != "null" ]];then
  if [ -d "$snapshot_path/$keyspace" ];then
    loginfo "[$node_name] Moving keyspace $snapshot_path/$keyspace folder to $snapshot_path/$new_keyspace_name"
    mv "$snapshot_path/$keyspace" "$snapshot_path/$new_keyspace_name" 2>/dev/null
  else
    [ "$skip_download" -eq 0 ] && logerror "[$node_name] Unable to find keyspace folder $snapshot_path/$keyspace" && logerror "[$node_name] Validate that the snapshot folder for keyspace $keyspace in S3 bucket s3://$s3_path" && exit
  fi
#If keyspace is being migrated, replace the keyspace name in schema.cql from all tables in the keyspace.
  for file in $(find "$snapshot_path/$new_keyspace_name" -name 'schema.cql');do
	#for file in $(find "$snapshot_path"/$new_keyspace_name -name 'schema.cql');do
	#Test: Removing old table ID before migrating.
	#TODO!: This should be a variable in config.toml
	#clear_table_ids=true|false
	grep -v "WITH ID" "$file" | sed -e '1,/AND/ s/AND/WITH/' | sed "s/ $keyspace\./ $new_keyspace_name./g" >> "$snapshot_path/$new_keyspace_name/skeleton-tmp.cql"
  done

	#Fix create type. Adding IF
	sed 's/CREATE TYPE IF/CREATE TYPE/g' "$snapshot_path/$new_keyspace_name/skeleton-tmp.cql" | sed 's/CREATE TYPE/CREATE TYPE IF/g' > "$snapshot_path/$new_keyspace_name/skeleton.cql"
	rm -rf "$snapshot_path/$new_keyspace_name"/skeleton-tmp.cql
	keyspace=$new_keyspace_name
else
#Find all tables and create a keyspace-skeleton
for file in $(find "$snapshot_path/$keyspace" -name 'schema.cql');do
	cat "$file" >> "$snapshot_path/$keyspace/skeleton.cql"
done
fi

#create keyspace
loginfo "[$node_name] Creating Keyspace: $keyspace"
cassutils cqlsh "${cqlsh_args[@]}" -e "CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class': 'NetworkTopologyStrategy', '""$cassandra_dc""': '""$replication_factor""'}  AND durable_writes = true;" || logerror "[$node_name] Error while creating keyspace $keyspace"

#Validate replication factor for keyspace
loginfo "[$node_name] Validating replication factor for keyspace: $keyspace"
cassutils cqlsh "${cqlsh_args[@]}" -e "SELECT * FROM system_schema.keyspaces WHERE keyspace_name='""$keyspace""'" || logerror "[$node_name] Error while validating keyspace $keyspace"



#Create tables using skeleton
loginfo "[$node_name] Creating tables using skeleton.cql in $snapshot_path/$keyspace/skeleton.cql"
cassutils cqlsh "${cqlsh_args[@]}" -f "$snapshot_path/$keyspace/skeleton.cql" &> /dev/null || logerror "[$node_name] Error while creating tables on keyspace $keyspace"


#Insert the data into tables with sstableloader
loginfo "[$node_name] Inserting data in keyspace $keyspace"
tables_left=$(find "$snapshot_path/$keyspace" -maxdepth 1 -type d | wc -l)


declare -a restore_tables=()
if [ -n "$tables_from_config" ];then
	for table in "${tables_from_config[@]}";do
	  for table_path in $(find $snapshot_path/$keyspace -iname "$table*");do
		  restore_tables+=($table_path)
	  done
  	done
else
loginfo "[$node_name] No tables to restore in config. Restoring all tables from keyspace snapshot"
tables_from_snapshot=($(dirname $(find "$snapshot_path/$keyspace" -name 'schema.cql')))
for table in ${tables_from_snapshot[@]};do
   table_name=$(cut -d/ -f6 <<< "$table")
   for table_path in $(find $snapshot_path/$keyspace -iname "$table_name*");do
   restore_tables+=($table_path)
done
done
fi
if [ -z "$restore_tables" ];then
	logerror "Table list is empty. Unable to proceed."
	exit 1
fi
for table_dir in "${restore_tables[@]}";do
	table_name=$(cut -d/ -f6 <<< "$table_dir")
	loader_path="$snapshot_path/$keyspace/$table_name"
	if ! test -f "$snapshot_path/$keyspace/$table_name/schema.cql";then
	     loginfo "[$node_name] Moving snapshot to SSTableloader compatible path: $loader_path"
	     mv $table_dir/snapshots/$snapshot_name/* "$loader_path"
	     rm -rf "$loader_path"/snapshots
	fi

	if [[ $(get_config snapshot.restore_to_all_nodes) == true ]];then
	  node_list_ss=$(jq -r .target.nodes[].listen_ip config.json | awk 'NR > 1 { printf(",") } {printf "%s",$0}')
	else
	  node_list_ss="$listen_ip"
	fi

  	loginfo "[$node_name] Working with table $table_name - Tables left: $tables_left"
	cassutils sstableloader -d "$node_list_ss" --conf-path /root/cassandra.yaml --no-progress -u "$cassandra_user" -pw "$cassandra_pw" "$loader_path" &> /dev/null || logerror "[$node_name] SStableloader failed while restoring snapshot of keyspace: "$keyspace" - table: "$table_name"" &> /dev/null
  (( tables_left -= 1 ))
done

loginfo "[$node_name] Validating tables from migrated keyspace $keyspace"
readarray -t tables < <(cassutils cqlsh "${cqlsh_args[@]}" -e "SELECT json * FROM system_schema.tables WHERE keyspace_name = '"$keyspace"';" | grep -v 'json\| rows\|\-\-\-\-' | jq -r .table_name)
if [ ${#tables[@]} -le 0 ]; then
    logerror "[$node_name] No tables found in keyspace $keyspace."
else
    loginfo "[$node_name] tables: ${tables[@]}"
fi
done

#loginfo "[$node_name] Validating all keyspaces"
#cassutils cqlsh $listen_ip -u $cassandra_user -p "$cassandra_pw" -e "desc keyspaces"
}

download_snapshot(){
#s3 details
s3_bucket=$(get_config s3.bucket_name)
s3_prefix=$(get_config s3.prefix)
s3_region=$(get_config s3.region)
aws_profile=$(get_config s3.aws_profile)
#awscli
export AWS_DEFAULT_REGION="$s3_region"
export AWS_REGION="$s3_region"
export AWS_PROFILE="$aws_profile"
export AWS_DEFAULT_OUTPUT=json
#aws creds debug
#cassutils aws sts get-caller-identity

  if [[ $s3_prefix != "null" ]];then
    s3_path="$s3_bucket/$s3_prefix/$snapshot_name/$source_node_name"
  else
    s3_path="$s3_bucket/$snapshot_name/$source_node_name"
  fi
  loginfo "[$node_name] Downloading snapshot $snapshot_name from S3 bucket: $s3_bucket"
  if [[ $(get_config snapshot.compressed) == true ]];then
      cassutils aws s3 cp  "s3://$s3_path.tar.gz" "./snapshot/$snapshot_name/"
  else
    #config_keyspace_list=$(get_config snapshot.keyspace[].name)
    #keyspaces=("$config_keyspace_list")
    for keyspace in "${keyspaces[@]}";do
    if [[ $s3_prefix != "null" ]];then
      mkdir -p "$snapshot_path"/"$keyspace"
      cassutils aws s3 cp --recursive "s3://$s3_path/$keyspace" "$snapshot_path/$keyspace" &>/dev/null
    else
      mkdir -p "$snapshot_path"/"$keyspace"
      cassutils aws s3 cp --recursive "s3://$s3_path/$keyspace" "$snapshot_path/$keyspace" &>/dev/null
    fi
    done
  fi
  logsuccess "[$node_name] Finished downloading $snapshot_name from $s3_bucket"
}

get_keyspaces(){
readarray -t keyspaces < <(jq -r '.snapshot.keyspace[] .name?' "$json_path")
 if [ "$keyspaces" == "" ];then
  loginfo "[$node_name] Keyspaces not defined in config. Using all keyspaces."
  exclude_system_keyspaces=$(get_config source.exclude_system_keyspaces)
  readarray -t raw_keyspaces < <(cassutils cqlsh "${cqlsh_args[@]}" -e "SELECT json * FROM system_schema.keyspaces" | grep -v 'json\| rows\|\-\-\-\-' | jq -r .keyspace_name)
 [ $exclude_system_keyspaces == true ] && keyspaces=($(echo "${raw_keyspaces[@]}" | grep -vw "system\|system_auth\|db_reaper\|system_schema\|system_traces\|system_distributed"))
 fi
}

keyspace_exists(){
 keyspace=$1
 cassutils cqlsh -u "$cassandra_user" -p "$cassandra_pw" -e "SELECT * FROM system_schema.keyspaces WHERE keyspace_name='""$keyspace""'" "$listen_ip" 9042 &> /dev/null
 if [ $? -ne 0 ];then
  logerror "[$node_name] Error while validating keyspace $keyspace. Check your configuration"
  exit
 fi
}

validate_data(){
#Create CSV structure
file_date=$(now_date file)
report_filename="target-row-validation-$file_date.csv"
echo "keyspace,table_name,row_count" > "$report_filename"
loginfo "[$node_name] Creating file $report_filename with validation results"
#Retrieving keyspaces
for keyspace in "${keyspaces[@]}";do
  #check if restore_as field contains a new name for the keyspace
  new_keyspace_name=$(jq --arg keyspace_name "$keyspace" -r '.snapshot.keyspace[] | select(.name==$keyspace_name) .restore_as?' "$json_path")
  [ -n "$new_keyspace_name" ] && [ "$new_keyspace_name" != "null" ] && keyspace=$new_keyspace_name
  get_tables "$keyspace"
  keyspace_exists $keyspace
  loginfo "[$node_name] Starting validation of keyspace $keyspace"
  for table in "${tables[@]}";do
    loginfo "[$node_name] Validating rows from keyspace $keyspace, table: $table"
    row_count=$(cassutils cqlsh "${cqlsh_args[@]}" -e "COPY $keyspace.$table to '/dev/null' with numprocesses=$(get_config validation.numprocesses || echo 1) AND PAGESIZE=5000 AND PAGETIMEOUT=$(get_config validation.connection_timeout || echo 200) AND MAXATTEMPTS=50;" | sed -n 5p | sed 's/ .*//' 2>/dev/null)
    # Show an error if row count is empty
    if [[ "$row_count" == *":1:Error"* ]] || [[ -z "$row_count" ]];then
	logerror "[$node_name] Error while validating data from table $table in keyspace $keyspace. Aborting validation."
	logerror "Check the tables in the config file"
	return
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

#prepare variables
init_env() {
#Identity
json_path=config.json
ssh_user=$(get_config source.ssh_user)
ssh_key=$(get_config source.ssh_key_path)
host_ip=$(get_config source.nodes["$node_id"].host_ip)
source_node_name=$(get_config source.nodes["$node_id"].name)
node_name=$(get_config target.nodes["$node_id"].name)
listen_ip=$(get_config target.nodes["$node_id"].listen_ip || echo "$host_ip")
loginfo "[$node_name] Preparing Environment"
#workspace
#dump term for cqlsh
export TERM=dumb
work_path=$(get_config target.work_path)
#Find Cassandra cluster config (cassandra.yaml)
cp "$(find / -name 'cassandra.yaml' | head -n1)" cassandra-tmp.yaml
#Remove deprecated values from config
grep -v "enable_sasi_indexes\|memtable_cleanup_threshold\|cached_rows_fail_threshold\|cached_rows_warn_threshold\|replica_filtering_protection" cassandra-tmp.yaml > cassandra.yaml
#snapshot
snapshot_name=$(get_config snapshot.name)
snapshot_path="./snapshot/$snapshot_name/$source_node_name"
[ $skip_download -ne 1 ] && [ $validate -ne 1 ] && rm -rf $work_path/snapshot && mkdir -p "$snapshot_path"

#cassandra credentials
cassandra_user=$(get_config target.cassandra.user || echo "null")
cassandra_pw=$(get_config target.cassandra.password || echo "null")
cassandra_dc=$(get_config target.cassandra.dc_name || echo "null")

#initalize cqlsh args
declare -ag cqlsh_args=()
[ "$cassandra_pw" != "null" ] && cqlsh_args+=("-u" "$cassandra_user") && cqlsh_args+=("-p" "$cassandra_pw")
cqlsh_args+=("$listen_ip")
cqlsh_args+=("--connect-timeout=$(get_config validation.connection_timeout || echo "200")")

#retrieve keyspaces
get_keyspaces
#Cleanup
rm -rf cassandra-tmp.yaml

#Pull cassutils docker image

#Retrieving keyspaces and tables
target_node_count=$(jq '.target.nodes | length' $json_path)
readarray -t keyspaces < <(jq -r '.snapshot.keyspace[] .name?' "$json_path")

#Docker
cassutils_image="$(get_config cassutils_image)"
#check if image is in host, if not pull first
image_pulled=$(docker images | grep -i cassutils)
if [ -z "$image_pulled" ];then
  loginfo "[$node_name] Pulling cassutils image from $cassutils_image"
  docker pull "$cassutils_image" &>/dev/null
else
  logsuccess "[$node_name] Found cassutils docker image in host"
fi
logsuccess "[$node_name] Done preparing Environment"
}

cassutils() {
  docker run --rm -ti -e TERM -e AWS_DEFAULT_OUTPUT -e AWS_REGION -e AWS_PROFILE -e AWS_DEFAULT_OUTPUT \
  --network=host -v "$work_path":/root "$cassutils_image" "$@"
}


skip_download=0
validate=0
unset PARAMS
while (( "$#" )); do
  case "$1" in
    --no-download)
      skip_download=1
      loginfo "Skipping snapshot download from S3 bucket."
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
[ $validate -ne 1 ] && [ $skip_download -ne 1 ] && download_snapshot
[ $validate -ne 1 ] && restore_snapshot
[ $validate -eq 1 ] && validate_data
logsuccess "[$node_name] All steps completed! Closing session"
