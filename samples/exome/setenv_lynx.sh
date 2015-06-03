###
# These are domain/application specific environment variables used in
# the exome sequencing pipeline script. A script will take care to replicate these to
# other hosts of the network. In other words, make sure these
# dirs/files are visible on every node.
###

export PIPELINE_CFG=exome_best_practices_pipeline.json

## misc. tools/binaries used in pipeline

export NUM_THREADS=6 # for multithreaded tools inside the pipeline, e.g. bwa
export BWA=bwa/bwa
export PICARDTOOLS=picard
export GATK=gatk
#export READGROUPS='@RG\tID:Group1\tLB:lib1\tPL:illumina\tSM:sample1'

## reference files, assumed by pipeline to just be there

# REF_DIR and REMOTE_REF are used by another script to pull in the ref files.
# I put those here because this functionality could conceivably be put into the pipeline script
#export REMOTE_REF=s3://jnj.exasci/reference
export REF_DIR=/data/biodata/sequencing/referenceFiles

export REF=$REF_DIR/ucsc_hg19
export KNOWN_SITES=$REF_DIR/known_sites

# checking and fetching files differs depending on file system, network, cloud, etc.
export CHECK_EXISTS="test -f"
export CHECK_NOT_EXISTS="test ! -f"
export FETCH="cp -v "


## these are used in the pipeline to locate/place input, output and temporary data

# exome_aws_pipeline.json uses wget to fetch the input files from s3 storage
#export READS=http://s3.amazonaws.com/1000genomes/data/NA12878/sequence_read
export READS=/data/biodata/sequencing/exome
export EXOME_SAMPLE_LIST=exome_sample_list.txt
export LOCAL_DIR=/scratch/yvdriess/workspace

#export HOSTFILE=../workspace/hostfile
