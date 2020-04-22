SNAKE_DIR=${IPA2_WORKSPACE}/workflow/
WF_SNAKEMAKE=${SNAKE_DIR}/ipa.snakemake
CROMWELL?=/pbi/dept/consensus/isovic/tools/cromwell-38.jar
WF_CROMWELL=${IPA2_WORKSPACE}/modules/pbpipeline-resources/wdl/pb_assembly_hifi.wdl
LAST_CROMWELL_RUNDIR=$(shell find cromwell-executions/ -mindepth 2 -maxdepth 2 | xargs ls -td | head -n 1)

snake:
	CWD=${PWD}/RUN snakemake -p -j 1 -d RUN -s ${WF_SNAKEMAKE} --configfile config.json --config MAKEDIR=.. -- finish

wdl:
	/usr/bin/time java -jar ${CROMWELL} run --inputs config.wdl.json ${WF_CROMWELL} 2>&1 | tee log.wdl.tee
	${MAKE} link
	# -/usr/bin/time bash run.sh 2>&1 | tee log.wdl.tee

link:
	rm -f crumbs
	ln -sf ${LAST_CROMWELL_RUNDIR} crumbs

check:
	grep ">" RUN/assemble/p_ctg.fasta | head

verify: snake
	echo ">ctg.000000F ctg_linear 203334 2689845" > expected.csv
	echo ">ctg.000000F-001-00 51286 618940" >> expected.csv
	echo ">ctg.000000F-001-01 51279 518897" >> expected.csv
	grep ">" RUN/assemble/p_ctg.fasta | cut -d' ' -f1,3-5 > result.csv
	grep ">" RUN/assemble/a_ctg_all.fasta | cut -d' ' -f1,4-5 >> result.csv
	diff expected.csv result.csv | wc -l | awk '{ if ($$1 == 0) { print "OK! -> results == expected" } else { print "FAIL -> results != expected." } }'

clean:
	rm -rf RUN
