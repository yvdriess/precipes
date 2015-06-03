#!/bin/bash

LOCAL_DIR=${1:?ERROR usage: cleanup <LOCAL_DIR> [HOSTFILE (=PBS_NODEFILE)]}
HOSTFILE=${2:-$PBS_NODEFILE}
# function cleanup_action {
# find $LOCAL_DIR -maxdepth 1 \
# 	-type f \
# 	-regex '.*/sample_[0-9]+.*\.\(bam\|sam\|idx\|fastq\|sai\|bai\|intervals\|recal\|picard-duplicates\|vcf\)' \
# 	-o -regex '.*/SRR[0-9]+.*\.\(bam\|sam\|idx\|fastq\|sai\|bai\|intervals\|recal\|picard-duplicates\|vcf\)' \
# 	-o -regex '.*/ERR[0-9]+.*\.\(bam\|sam\|idx\|fastq\|sai\|bai\|intervals\|recal\|picard-duplicates\|vcf\)' \
# 	-delete
# }

function main {
	local cleanup_action="rm -vr $LOCAL_DIR/*"
	if [[ -n $HOSTFILE ]]; then
		echo "doing a remote cleanup of $LOCAL_DIR"
		for remote in `cat $HOSTFILE`; do
			echo -n "cleanup $remote :  "
			ssh -o NoHostAuthenticationForLocalhost=yes -o StrictHostKeyChecking=no $remote "$cleanup_action"
		done
	else
		echo "doing a cleanup of $LOCAL_DIR"
		exec $cleanup_action
	fi
}

main "$@"