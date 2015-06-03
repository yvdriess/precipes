# Exome Best Practices Pipeline execution example

We have provided a set of execution scripts here supporting the execution of the `exome_best_practices_pipeline.json` recipe. 

Note: Providing a non-trivial sequencing pipeline example is tricky due to the tools, reference files and input files required. Not to mention of the context-sensitive and portability issues related to making analysis tools run in the first place. The scripts here should run in a generic linux environment and the binaries of the required analysis tools have been provided in this directory (+- 120 MB). 

### Supporting Scripts
  - `./run_exome_experiment.sh` is the start point, takes care of launching and logging the given recipe.
  - `./run_pipeline.sh` generic precipes launch script, mainly used to deal with cluster launch issues.
  - `exome_best_practices_pipeline.json` the actual recipe. Uses environment variables for portability reasons, the `setenv.sh` script takes care of these.
  - `setenv.sh` is by default linked to `setenv_workstation.sh` and sets up the recipe for local (workstation) parallel execution. Look in `setenv_lynx.sh` or `setenv_aws.sh` for cluster or cloud examples.

### Prerequisites

*Know your way around NGS*
We cannot simply the large reference files required here. You will need to provide the reference files and input files yourself. These are common enough such that every bioinformatician should have them laying around. All these reference files are online and can be found on http://hgdownload.cse.ucsc.edu/downloads.html and ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/
  
*Reference files*
Put put the following reference files in their respective directories, or use symlinks to point to a directory with these files:
  - UCSC HG19 fasta 
    - `ucsc.hg19.fast` in: `data/reference/ucsc_hg19`)
  - Mills and 1000G gold standard indels
    - `Mills_and_1000G_gold_standard.indels.hg19.vcf` in: `data/reference/known_sites`
  - hg19 dbsnp
    - `Mills_and_1000G_gold_standard.indels.hg19.vcf` in: `data/reference/known_sites`

*Input Files*
  - 1000Genome NA12878 (phase 3) fastq.gz files in: `data/exome`
  - 
`exome_sample_list.txt` contains a list of all sample names considered in the `run_exome_experiment.sh` script. If you have the space (557GB) you can fetch the entire list from `wget ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/data/NA12878/sequence_read/`. Typically you will want to run only run a handful of exome samples, passing `-s 32` to `run_exome_experiment.sh` will run the first 32 samples in `exome_sample_list.txt` in parallel. You can thus make due with only fetching the necessary read files.
Be warned that 32 samples run with only one worker on one compute node runs for nearly a week!
