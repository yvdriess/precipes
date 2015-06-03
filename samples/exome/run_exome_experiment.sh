#!/bin/bash
#set -uex

## file containing all the names of the phase 3, 1000 genome project, NA12878 exome samples
# 
export EXOME_SAMPLE_LIST=exome_sample_list.txt

export REQUIRED_REFERENCE_FILES="ucsc.hg19.fasta dbsnp_137.hg19.vcf Mills_and_1000G_gold_standard.indels.hg19.vcf"

function print_usage_string {
	echo "$usage_string" >&2
}

function init_env {
	if [ -f setenv.sh ]; then
		source setenv.sh
	else
		echo "Please provide a setenv.sh script with the pipeline-specific environment variables." >&2
		exit 1
	fi
}

function init_local_dir {
	if [[ -n "$LOCAL_DIR" ]]; then
		local hostfile=${HOSTFILE:-${PBS_NODEFILE:-""}}
		if [[ -n $hostfile ]]; then
			for remote in $(cat $hostfile); do
				ssh -o NoHostAuthenticationForLocalhost=yes -o StrictHostKeyChecking=no $remote "mkdir -p $LOCAL_DIR"
			done
		else
			mkdir -p "$LOCAL_DIR"
		fi
	else
		echo "Please provide a LOCAL_DIR variable in setenv.sh." >&2
		exit 1
	fi
}

function exome_pipeline_sanity_check {
	for file in $REQUIRED_REFERENCE_FILES; do
		if [ ! $(find "$REF_DIR" -name "$file") ]; then
			echo "Cannot find $file in $REF_DIR, please put all reference files ($REQUIRED_REFERENCE_FILES) in their respective directory :" >&2
			echo "  ucsc.hg19.fast :  $REF" >&2
			echo "  dbsnp_137.hg19.vcf and Mills_and_1000G_gold_standard.indels.hg19.vcf  :  $KNOWN_SITES" >&2
			exit 1
		fi
	done
}

function main {
	init_env
	init_local_dir
	# handle optional arguments
	usage_string="Usage: run_exome_experiment [options] <pipeline_cfg>  
Drives the run_pipeline.sh script to run the exome experiment. Execution logs and running times are generated in .log and .times files.
	Options: 	(-n num_nodes)
				(-w num_workers) 
				(-s num_samples)
				(-h)					: print this usage information
"
	# defaults
	local num_samples=1
	local num_nodes=1
	local num_workers=1
	local verbose=0
	while getopts "s:n:w:hv" opt; do
		opt_found=1
		case $opt in
			s) num_samples=$OPTARG ;;
			n) num_nodes=$OPTARG ;;
			w) num_workers=$OPTARG ;;
			v) verbose=1 ;;
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
	shift $((OPTIND-1))
	export PIPELINE_CFG=${1:? ERROR no pipeline config file specified. $(print_usage_string)}
	# remove all arguments (avoids polluting scripts down the line)
	shift $(($#))

	exome_pipeline_sanity_check

# taking the first num_samples #samples
	local samples=$(head -n $num_samples $EXOME_SAMPLE_LIST)
	local RANDOM=$$
	local POSTFIX=${RANDOM}_`date +"%F"`
	local experiment=exome_${num_samples}s_${num_nodes}n_${num_workers}w_$POSTFIX
	mkdir -p trace
	mkdir -p log
	export VT_LOGFILE_NAME=trace/$experiment.stf
	echo "running experiment called $experiment for samples: " 
	echo $samples
	TIMEFORMAT="%R"
	local verbose_flag=""
	if (( $verbose )); then
		verbose_flag="-v"
		echo "executing: ./run_pipeline.sh $verbose_flag -n $num_nodes -w $num_workers $PIPELINE_CFG $samples"
	fi
	local start=$(date +%s)
	./run_pipeline.sh $verbose_flag -n $num_nodes -w $num_workers $PIPELINE_CFG $samples |& tee log/$experiment.log
	local wallclock_time=$(( $(date +%s) - $start ))
	echo $wallclock_time > log/$experiment.time
	echo "done, finished in $wallclock_time seconds"
	echo "wrote log output in :"
	echo "log/$experiment.log"
    #cleanup
}

main "$@"
