###
# These are domain/application specific environment variables used in
# the exome sequencing pipeline script. A script will take care to replicate these to
# other hosts of the network. In other words, make sure these
# dirs/files are visible on every node.
###

export PIPELINE_CFG=exome_aws_pipeline.json

## misc. tools/binaries used in pipeline

export NUM_THREADS=6
export BWA=bwa/bwa
export SAMTOOLS=samtools/samtools
export PICARDTOOLS=picard
export GATK=gatk


## reference files, assumed by pipeline to just be there

# REF_DIR and REMOTE_REF are used by another script to pull in the ref files.
# I put those here because this functionality could conceivably be put into the pipeline script
#export REMOTE_REF=s3://jnj.exasci/reference
export REF_DIR=data

export REF=$REF_DIR/ucsc_hg19
export GATKREF=$REF_DIR/ucsc_hg19
export DBSNP=$REF_DIR/dbsnp

# comment out for non-verbose
export VERBOSE=true

# writing our own scripts to fetch/copy files and to check file existence
#  this should work for either curl or wget, but at least one needs to be installed
# curl supports file:// to copy local FS files, wget does not
function fetch { # we need to fetch files from the web, select between curl (also handles local moves) and wget
	if [ ! $VERBOSE ]; then		
		wget_silent=-q
		curl_silent=--silent
	fi
	if command -v curl &> /dev/null; then
		curl $curl_silent $1 -o $2
	elif command -v wget &> /dev/null; then
		wget $wget_silent -O $2 $1
	else
		echo "ERROR: needs curl or wget to proceed"
		exit 1;
	fi
}
function check { 
	if [ ! $VERBOSE ]; then		
		wget_silent=-q
		curl_silent=--silent
	fi
	if   command -v curl &> /dev/null; then
		curl $curl_silent --output /dev/null --head --fail $1
	elif command -v wget &> /dev/null; then
		wget $wget_silent --spider $1
	else
		echo "ERROR: needs curl or wget to proceed"
		exit 1;
	fi
}

export CHECK_EXISTS="check"
export FETCH="fetch "

## these are used in the pipeline to locate/place input, output and temporary data

# exome_aws_pipeline.json uses wget to fetch the input files from s3 storage
export READS=http://s3.amazonaws.com/1000genomes/data/NA12878/sequence_read
export EXOME_SAMPLE_LIST=exome_sample_list.txt
export LOCAL_DIR=workspace


#export HOSTFILE=../workspace/hostfile
