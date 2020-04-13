SNAKE_DIR=${IPA2_WORKSPACE}/workflow/
WF_SNAKEMAKE=${SNAKE_DIR}/ipa.snakemake
CROMWELL?=/pbi/dept/consensus/isovic/tools/cromwell-38.jar
WF_CROMWELL=${IPA2_WORKSPACE}/modules/pbpipeline-resources/wdl/pb_assembly_hifi.wdl
LAST_CROMWELL_RUNDIR=$(shell find cromwell-executions/ -mindepth 2 -maxdepth 2 | xargs ls -td | head -n 1)

cluster_S=/bin/bash
cluster_P=-cwd
cluster_E=qsub_log/
cluster_O=qsub_log/
cluster_Q=default
cluster_CPU=-pe smp 8
cluster_JOBS=10

snake:
	CWD=${PWD}/RUN snakemake -p -j 1 -d RUN -s ${WF_SNAKEMAKE} --configfile config.json --config MAKEDIR=.. -- finish

qsub:
	mkdir -p RUN/qsub_log
	CWD=${PWD}/RUN snakemake -p -d RUN -s ${WF_SNAKEMAKE} -j ${cluster_JOBS} --cluster "qsub -S ${cluster_S} ${cluster_P} -q ${cluster_Q} ${cluster_CPU} -e ${cluster_E} -o ${cluster_O} -V" --verbose -p --latency-wait 60 --configfile config.json --config MAKEDIR=.. -- finish

wdl:
	/usr/bin/time java -jar ${CROMWELL} run --inputs config.wdl.json ${WF_CROMWELL} 2>&1 | tee log.wdl.tee
	${MAKE} link
	# -/usr/bin/time bash run.sh 2>&1 | tee log.wdl.tee

link:
	rm -f crumbs
	ln -sf ${LAST_CROMWELL_RUNDIR} crumbs

check:
	grep ">" RUN/assemble/p_ctg.fasta | head

clean:
	rm -rf RUN
