SNAKE_DIR=${IPA2_WORKSPACE}/workflow/
SNAKE_SCRIPT=${SNAKE_DIR}/ipa.phased.snakemake

snake:
	CWD=${PWD}/RUN snakemake -p -j 8 -d RUN -s ${SNAKE_SCRIPT} --configfile config.json --config MAKEDIR=.. -- finish

check:
	grep ">" RUN/assemble/p_ctg.fasta | head

verify: snake
	echo "ctg_linear 203322 2100115" > expected.csv
	echo "ctg_linear 163302 1649589" >> expected.csv
	grep ">" RUN/assemble/p_ctg.fasta | cut -d' ' -f3-5 > result.csv
	diff expected.csv result.csv | wc -l | awk '{ if ($$1 == 0) { print "OK! -> results == expected" } else { print "FAIL -> results != expected." } }'

clean:
	rm -rf RUN
