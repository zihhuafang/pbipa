PATH:=${PATH}:${CURDIR}/../../scripts:${CURDIR}/../../bash
export PATH

SNAKE_DIR=${IPA2_WORKSPACE}/workflow/
WF_SNAKEMAKE=${SNAKE_DIR}/ipa.snakefile
CROMWELL?=/pbi/dept/consensus/isovic/tools/cromwell-38.jar
WF_CROMWELL=${IPA2_WORKSPACE}/modules/pbpipeline-resources/wdl/pb_assembly_hifi.wdl
LAST_CROMWELL_RUNDIR:=$(shell find cromwell-executions/ -mindepth 2 -maxdepth 2 | xargs ls -td | head -n 1)
TIMESTAMP:=$(shell date +%Y_%m_%d-%H_%M_%S)

# Input config file.
CONFIG_FN?="config.yaml"

# No need to modify these.
cluster_shell=/bin/bash
cluster_pwd=-cwd
cluster_stderr=qsub_log/
cluster_stdout=qsub_log/

# Change this for either the local or distributed mode.
# Controlls the number of jobs that will be run concurrently.
NJOBS?=2

# Change these to your preferences.
cluster_queue=sequel-farm
cluster_cpu=-pe smp {params.num_threads}

CLUSTER_ARGS=qsub -S ${cluster_shell} ${cluster_pwd} -q ${cluster_queue} ${cluster_cpu} -e ${cluster_stderr} -o ${cluster_stdout} -V

.PHONY: snake qsub wdl link check logenv clean

snake:
	${MAKE} logenv
	/usr/bin/time snakemake -p -j ${NJOBS} -d RUN -s ${WF_SNAKEMAKE} --configfile ${CONFIG_FN} 2>&1 | tee log.${TIMESTAMP}.snakemake.local.tee

local:
	#${MAKE} logenv
	which ipa
	/usr/bin/time ipa local --njobs ${NJOBS} --nthreads 4 --run-dir RUN --verbose --unlock --resume -i input.fofn

dist:
	mkdir -p RUN/qsub_log
	${MAKE} logenv
	/usr/bin/time ipa dist --njobs ${NJOBS} --nthreads 4 --run-dir RUN --verbose --unlock --cluster "${CLUSTER_ARGS}" -i input.fofn 2>&1 | tee RUN.log.${TIMESTAMP}.snakemake.qsub.tee

qsub:
	mkdir -p RUN/qsub_log
	${MAKE} logenv
	CWD=${PWD}/RUN /usr/bin/time snakemake --rerun-incomplete -p -d RUN -s ${WF_SNAKEMAKE} -j ${NJOBS} --cluster "qsub -S ${cluster_shell} ${cluster_pwd} -q ${cluster_queue} ${cluster_cpu} -e ${cluster_stderr} -o ${cluster_stdout} -V" --verbose -p --latency-wait 60 --configfile ${CONFIG_FN} -- finish 2>&1 | tee log.${TIMESTAMP}.snakemake.qsub.tee

unlock:
	mkdir -p RUN/qsub_log
	${MAKE} logenv
	CWD=${PWD}/RUN /usr/bin/time snakemake --unlock -p -d RUN -s ${WF_SNAKEMAKE} -j ${NJOBS} --cluster "qsub -S ${cluster_shell} ${cluster_pwd} -q ${cluster_queue} ${cluster_cpu} -e ${cluster_stderr} -o ${cluster_stdout} -V" --verbose -p --latency-wait 60 --configfile ${CONFIG_FN} -- finish 2>&1 | tee log.${TIMESTAMP}.snakemake.qsub.tee

wdl:
	${MAKE} logenv
	/usr/bin/time java -jar ${CROMWELL} run --inputs config.wdl.json ${WF_CROMWELL} 2>&1 | tee log.${TIMESTAMP}.wdl.local.tee
	${MAKE} link

link:
	rm -f crumbs
	ln -sf ${LAST_CROMWELL_RUNDIR} crumbs

check:
	assembly-stats -t RUN/final/*_ctg.fasta | column -t

logenv:
	env > log.${TIMESTAMP}.env.txt
	rm -f log.${TIMESTAMP}.path_to_tools.txt
	which pancake >> log.${TIMESTAMP}.path_to_tools.txt
	which pblayout >> log.${TIMESTAMP}.path_to_tools.txt
	which nighthawk >> log.${TIMESTAMP}.path_to_tools.txt
	which falconc >> log.${TIMESTAMP}.path_to_tools.txt
	which ipa2-task >> log.${TIMESTAMP}.path_to_tools.txt
	pancake --version > log.${TIMESTAMP}.versions.txt
	pblayout --version >> log.${TIMESTAMP}.versions.txt
	nighthawk --version >> log.${TIMESTAMP}.versions.txt
	ipa2-task version 2>/dev/null | tail -n 1 >> log.${TIMESTAMP}.versions.txt
	falconc --version 2>> log.${TIMESTAMP}.versions.txt

clean:
	rm -rf RUN
