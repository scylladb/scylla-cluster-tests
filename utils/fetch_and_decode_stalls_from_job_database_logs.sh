#!/usr/bin/bash
# This script searches in the log all stalles, find unique stalles and decode them.
# The script analyzes the database.logs that are located under avocado/job-results/<job-folder>/<test-folder>/<cluster-folder>. The script is going through nodes folders and analyze database.log for every node.
# The script expects to receive 3 parameters:
#   $1 - cluster logs path under job folder (for example: ~/avocado/job-results/latest/test-results/1-longevity_test.py\:LongevityTest.test_custom_time/julia-sst2-2mv-100-backpressure-db-cluster-1e0e59be)
#   $2 - node IP: it's needed for decoding. Will use credential file (see below)
#   $3 - OS user on the host, where job logs are located and where the script will be run
# *** The credential file has to be on the host, where script is running: ~/.ssh/scylla-qa-ec2. It's hard-coded in the script
# Information about the decoded backtraces location will be printed out by script on the console.

# Usage:    ./fetch_and_decode_stalls_from_job_database_logs.sh <logs path> <node IP> <local OS user>
# Example:  ./fetch_and_decode_stalls_from_job_database_logs.sh ~/avocado/job-results/latest/test-results/1-longevity_test.py\:LongevityTest.test_custom_time/julia-sst2-2mv-100-backpressure-db-cluster-1e0e59be 5.32.175.60 centos

LOGS_PATH=$1
LOG_FILE_NAME="database.log"
NODE_IP=$2
OS_USER=$3

stalls_dir="$LOGS_PATH/stalls"
echo "Create backtraces folder $stalls_dir"
sudo install -d -o $OS_USER -g $OS_USER $stalls_dir

prev=$LOGS_PATH

#
# Cut backtraces from nodes' log
#
for dir in $(find $prev -type d); do
	echo "DIR: $dir"
	if [[ ! $prev =~ $dir ]]; then
		analized_log=$dir/$LOG_FILE_NAME
		echo "LOG FILE: $analized_log"
		if [[ ! -f $analized_log ]]; then
  	 		echo "$LOG_FILE_NAME file not found in $dir !"
		else
			echo ""
			echo "============================= Analize log: $dir/$LOG_FILE_NAME ======================================="
			echo ""
			analize_dir="$dir/backtrace_logs"
			sudo install -d -o $OS_USER -g $OS_USER $analize_dir
			grepped_file="$analize_dir/analize.log"
			egrep -v " exceptions::unavailable_exception|exceptions::mutation_write_timeout_exception|0,      0,       0,|0,       0,       0,       0|compaction|repair -|LeveledManifest|stream_session -|Connection refused|NoSuchElem|UnavailableException|stderr|fail to connect" $analized_log  > $grepped_file
			echo "Name is $grepped_file"
			stall=0
			while IFS='' read -r line || [[ -n "$line" ]]; do
				if [ $stall == 1 ]; then
					if [[ $line != *"Backtrace:"* ]]; then
						trace="$(echo $line | awk '{split($0, a, "|"); print a[2]}' | sed -e 's/^[[:space:]]*//')"
						if [[ $trace == "0x"* ]] || [[ $trace == "/lib"* ]]; then
							echo $trace >> $stall_name
						else
							echo "Created backtrace file $stall_name"
							echo "--------------------------------------------------------------------------------------------------"
							stall=0
						fi
					fi
				fi

				if [[ $line == *"Reactor stall"* ]]; then
                                        stall_name="$(echo $line | awk '{split($0, a); print a[1]"_"a[3]":"a[4]":"a[5]"_"a[11]"ms_shard"a[15]}')"
                                        stall_name="$stalls_dir/$stall_name"
                                        echo "Found backtrace will be saved into $stall_name file"
                                        stall=1
                                fi

			done < $grepped_file
			sudo rm -r $analize_dir
		fi
		prev="$dir"
	fi
done

yes '' | sed 3q
echo "Search for unique backtraces"
yes '' | sed 2q

#
# Find unique backtraces
#
unique_stalls_dir="$LOGS_PATH/unique_stalls"
echo "Create unique backtraces folder $unique_stalls_dir"
sudo install -d -o $OS_USER -g $OS_USER $unique_stalls_dir
echo ""
echo "Start comparing"
while true; do
	if [ -z "$(ls -A $stalls_dir)" ]; then
		break
	fi
	base_file=""
	for file in $(find $stalls_dir -type f); do
		if [ -z $base_file ]; then
			base_file=$file
			base_file_name=$(basename -- "$file")
		fi
		file_name=$(basename -- "$file")
		if [[ ! "$base_file_name" =~ "$file_name" ]]; then
			dif="$(diff $file $base_file)"
			#echo "Diff : $dif"
			if [ -z "$dif" ]; then
				echo $file_name >> $unique_stalls_dir/$base_file_name
				sudo rm $file
				echo "Files $base_file_name and $file_name are same"
			fi
		fi
	done
	echo "Save into unique file $unique_stalls_dir/$base_file_name"
	echo "$(basename -- $base_file)" >> "$unique_stalls_dir/$base_file_name"
	echo "" >> "$unique_stalls_dir/$base_file_name"
	echo "Backtrace:" >> "$unique_stalls_dir/$base_file_name"
	cat $base_file >> "$unique_stalls_dir/$base_file_name"
	sudo rm $base_file
done
sudo rm -r $stalls_dir

echo "*********************** Finished. Found backtraces are places in $unique_stalls_dir folder *************************"

#
# Decode backtraces
#
decoded_file_name="$unique_stalls_dir/decoded_backtraces.log"
ssh  -a -x   -o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=30 -o ServerAliveInterval=300 -l centos -p 22 -i ~/.ssh/scylla-qa-ec2 $NODE_IP "true"

for file in $(find $unique_stalls_dir -type f); do
	echo "Decoding of $(basename -- $file) file ...."
	echo "============================ $(basename -- $file)  ================================" >> $decoded_file_name
	cat $file >> $decoded_file_name
	backtrace=$(less $file | egrep "0x|/lib" |awk 'ORS=" "')
	ssh  -a -x   -o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=30 -o ServerAliveInterval=300 -l centos -p 22 -i ~/.ssh/scylla-qa-ec2 $NODE_IP "addr2line -Cpife /usr/lib/debug/bin/scylla.debug $backtrace" >> $decoded_file_name
	echo "$(yes '' | sed 2q)" >> $decoded_file_name
done
# Print 3 empty lines
yes '' | sed 3q
echo "******** Found $(ls unique_stalls_dir file | wc -l) unique backtraces ********"

yes '' | sed 2q
echo "******** Decoded backtraces are saved into $decoded_file_name file ********"
