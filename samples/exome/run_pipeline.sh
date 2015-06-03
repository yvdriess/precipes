#!/bin/bash
#set -uex

function print_usage_string {
	echo "$usage_string" >&2
}

function main {
	# set default (optional) parameters
	export NUM_NODES=1
	export NUM_WORKERS=1
	export HOSTFILE=${PBS_NODEFILE:-""}
	export EXEC_PIPELINE=./precipes

	# handle optional arguments
	usage_string="
Usage: run_pipeline [options] <pipeline_cfg> [sample_name]+  
	Options: 	(-n num_nodes)                                             
				(-w num_workers)                                            
				(-f hostfile)                                              
				(-p pipeline_binary)                                       
				(-t)					: enable ITAC trace when using MPI 
				(-v)					: verbose output                   
				(-h)					: print this usage information     
				(-m)					: use MPI (default: sockets)
"

	local opt_found=0
	local verbose=0
	local with_itac=0
	local with_mpi=0
	local with_gdb=0
	
	while getopts "n:w:f:l:p:hvtgm" opt; do
		opt_found=1
		case $opt in
			n) export NUM_NODES=$OPTARG ;;
			w) export NUM_WORKERS=$OPTARG ;;
			f) export HOSTFILE=$OPTARG ;;
			p) export EXEC_PIPELINE=$OPTARG ;;			
			v) verbose=1 ;;
			g) with_gdb=1 ;;
			t) with_itac=1 ;;
			m) with_mpi=1 ;;
			h) print_usage_string ; exit 1 ;;		
			/?) echo "Invalid option: -$OPTARG" >&2
				echo "$usage_string" >&2
				exit 1
				;;
		esac
	done
	# check if no ops specified
	# if (( ! opt_found )); then
	# 	print_usage
	# 	exit $E_OPTERROR;
	# fi
	# shift out the optional arguments
	shift $((OPTIND-1))
	# handle mandatory arguments
	export PIPELINE_CFG=${1:? ERROR no pipeline config file specified. $(print_usage_string)}
	export SAMPLES=${@:2}

	# remove all arguments (avoids polluting scripts down the line)
	shift $(($#))

	if [[ -z $SAMPLES ]]; then
		echo "ERROR specify at least one run label (e.g. sample name). $(print_usage_string)" >&2
		exit 1
	fi

	if (( $verbose )); then
		echo "VERBOSE is on"
		echo "executing $0.sh script with environment:"
		env
	else
		echo "VERBOSE is off"
	fi
	local debug=""
	if (( $NUM_NODES > 1 ))
	then
		if (( $with_mpi ))
		then
			export DIST_CNC=MPI
			export CNC_SCHEDULER=FIFO_STEAL #TBB_TASK
			export CNC_NUM_THREADS=$NUM_WORKERS
		# export I_MPI_FABRICS=shm:tcp
		# export I_MPI_TCP_NETMASK=ib
		# export I_MPI_DYNAMIC_CONNECTION=0
		# export I_MPI_DAPL_TRANSLATION_CACHE=off
			if (( $with_itac )); then
				## intel trace analyser
				export VT_LOGFILE_FORMAT=SINGLESTF
				export VT_LOGFILE_NAME=${VT_LOGFILE_NAME:-pipeline_trace.stf}
                # this makes idle threads not show up in the trace
				export VT_ENTER_USERCODE=off
				export LD_PRELOAD="libcnc_debug.so:libcnc_mpi_itac_debug.so:libVT.so"; fi
			if (( $verbose )); then 
				# 	Intel MPI options:	I_MPI_FABRICS=$I_MPI_FABRICS; I_MPI_TCP_NETMASK=$I_MPI_TCP_NETMASK; I_MPI_DYNAMIC_CONNECTION=$I_MPI_DYNAMIC_CONNECTION 
				echo "executing $EXEC_PIPELINE $PIPELINE_CFG using MPI on $NUM_NODES #nodes, with 
	CNC options: 		DIST_CNC=$DIST_CNC; CNC_SCHEDULER=$CNC_SCHEDULER; CNC_NUM_THREADS=$NUM_WORKERS
	Preload options:    LD_PRELOAD=$LD_PRELOAD"
				echo "executing:	mpirun $debug -prepend-rank -f $HOSTFILE -rr -n $NUM_NODES $EXEC_PIPELINE $PIPELINE_CFG $SAMPLES"
			fi
			if (( $with_gdb )); then
				$debug=-gdb
			fi
			mpirun $debug -prepend-rank -f $HOSTFILE -rr -n $NUM_NODES $EXEC_PIPELINE $PIPELINE_CFG $SAMPLES
		else
			# You can use the following environment variables to configure the behavior of this script:
#   CNC_CLIENT_EXE  sets the client executable to start
#   CNC_CLIENT_ARGS sets the arguments passed to the client processes/executables
#   CNC_HOST_FILE   sets the file to read hostnames from on which client proceses will be started
#   CNC_NUM_CLIENTS sets the number of client processes to start
			export CNC_HOST_FILE=$HOSTFILE
			export CNC_NUM_CLIENTS=$(( NUM_NODES - 1 ))
			export DIST_CNC=SOCKETS
			export CNC_SCHEDULER=FIFO_STEAL #TBB_TASK
			export CNC_NUM_THREADS=$NUM_WORKERS
			export CNC_SOCKET_HOST=$PWD/cnc_socket_start.sh
			# the cnc-using binary will call the CNC_SOCKET_HOST script to launch the multiple clients
			if (( $with_gdb )); then
				$debug=gdb -args 
			fi
			$debug $EXEC_PIPELINE $PIPELINE_CFG $SAMPLES
		fi
	else
		export DIST_CNC=SHMEM
		export CNC_SCHEDULER=FIFO_STEAL #TBB_TASK
		export CNC_NUM_THREADS=$NUM_WORKERS
		if (( $verbose )); then
			echo "running $PIPELINE_CFG locally using $NUM_WORKERS workers, whith
	CNC options:		CNC_SCHEDULER=TBB_TASK; CNC_NUM_THREADS=$NUM_WORKERS"
			echo "executing $EXEC_PIPELINE $PIPELINE_CFG $SAMPLES"
		fi
		exec $EXEC_PIPELINE $PIPELINE_CFG $SAMPLES
	fi
}

main "$@"