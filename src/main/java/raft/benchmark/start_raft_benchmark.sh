#!/bin/bash
own_rank=${PRUN_CPU_RANK}
total_nodes=$2
time_limit_minutes=$3
type=$4
total_workers=$(($total_nodes-1))

echo "Node $(hostname) has rank ${own_rank}"

JAR_FILE="./MSc_Thesis_Raft-0.1-jar-with-dependencies.jar"
WORKER_PORT=55000
CTRLR_PORT=55001
config_filename="./config_run_${SLURM_JOB_ID}.cfg"

# if it is this node, become controller, else worker
if [[ "$own_rank" == "0" ]]; then
    echo "$(hostname) is the controller"
    echo "$(hostname)" > ./controller_host.txt    
    
    # Run the controller
    echo "Controller node executing: java -jar $JAR_FILE controller $(hostname) $CTRLR_PORT $total_workers $time_limit_minutes $type"
    java -jar $JAR_FILE controller $(hostname) $CTRLR_PORT $total_workers $time_limit_minutes $type

    # Squirrel away the data
    # Step 1: Make dir
    impl_dir=$type
    worker_count_dir="${total_workers}_nodes"
    time_dir="${time_limit_minutes}_minutes"
    run_dir="run_${SLURM_JOB_ID}"
    dst_path="./bench_data/$impl_dir/$worker_count_dir/$time_dir/$run_dir/"
    mkdir -p $dst_path

    # Step 2: Move files
    mv ./*.raftlog $dst_path
    mv $config_filename $dst_path

    # Remove stale files
    rm ./controller_host.txt
    rm ./*cfg_tmp*

else
    echo "$own_rank $(hostname) $WORKER_PORT" >> "${config_filename}_tmpout_$own_rank" # Print own details to a file
    sleep 10     # Wait for all nodes to start (fingers crossed it's enough?)

    tmp_cfg=${config_filename}_tmpin_$own_rank
    cat ${config_filename}_tmpout_* > $tmp_cfg  # Concatenate all into a config file

    controller_hostname=$(cat ./controller_host.txt)

    # Run a worker
    java -jar $JAR_FILE worker $controller_hostname $CTRLR_PORT $own_rank $tmp_cfg
fi



