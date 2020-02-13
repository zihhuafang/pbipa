SNAKE_DIR=${PLATINUM_WORKSPACE}/workflow/
SNAKE_SCRIPT=${SNAKE_DIR}/pt.snakemake

snake:
	CWD=${PWD}/RUN snakemake -p -j 1 -d RUN -s ${SNAKE_SCRIPT} --configfile config.json --config MAKEDIR=.. -- finish

check:
	grep ">" RUN/assemble/p_ctg.fasta | head

clean:
	rm -rf RUN
