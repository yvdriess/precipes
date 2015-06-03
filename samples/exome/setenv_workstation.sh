###
# These are domain/application specific environment variables used in
# the exome sequencing pipeline script. A script should take care to replicate these to
# other hosts of the network. In other words, make sure these
# dirs/files are visible on every node.
###

## Analysis tools
# variables pointing to various analysis tool binaries
# exome_best_practices_pipeline.json expects $BWA to be the binary
#                                            $PICARDTOOOLS and $GATK a directory with the needed .jar files
export BWA=bwa/bwa
export PICARDTOOLS=picard
export GATK=gatk
export NUM_THREADS=6 # for multithreaded tools inside the pipeline, e.g. bwa

## 
# we hardcoded the readgroups in the recipe to sidestep issues with bash/echo/json string expansion
# exercise for the reader, how any backslashes do you need here in order to pass the string '\t' (not a tab) to bwa (hint: more than two)
#export READGROUPS='@RG\tID:Group1\tLB:lib1\tPL:illumina\tSM:sample1'

## reference files, assumed by pipeline to just be there

# REF_DIR and REMOTE_REF can be used by another script to pull in the ref files.
# I put those here because this functionality could conceivably be put into the pipeline script
# e.g.
#export REMOTE_REF=s3://**************/reference
export REF_DIR=data/reference

## point to the directory containing ucsc.hg19.fasta
export REF=$REF_DIR/ucsc_hg19
## point to the directory containing dbsnp_137.hg19.vcf and Mills_and_1000G_gold_standard.indels.hg19.vcf
export KNOWN_SITES=$REF_DIR/known_sites

## These are used by the exome_best_practices_pipeline.json recipe to pull read (fastq) files.
# checking and fetching files differs depending on file system, network, cloud, etc.
# grid or amazon cloud will want to use curl or wget (cfr setenv_aws.sh)
# for local (or networked) FS files, just use basic linux commands as below
export FETCH="cp -v "
export CHECK_EXISTS="test -f"
export CHECK_NOT_EXISTS="test ! -f"

## Location of the fastq (read) files
# expecting the files from ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/data/NA12878/sequence_read/
export READS=data/exome

# When not relying on local read files, you can change FETCH, CHECK_* and READS to take remote read files on demand
# e.g. using wget or curl to fetch the read files from s3 storage (cfr. setenv_amazon.sh)
# in that case change the location of READS as follows:
#export READS=http://s3.amazonaws.com/1000genomes/data/NA12878/sequence_read

## Point to a working directory in which to place all input/output/temporary files
# when running on a remote (i.e. cluster node), you might want this to point to a /scratch directory
export LOCAL_DIR=workspace
