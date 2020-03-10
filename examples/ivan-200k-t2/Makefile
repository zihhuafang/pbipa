SNAKE_DIR=${IPA2_WORKSPACE}/workflow/
SNAKE_SCRIPT=${SNAKE_DIR}/ipa.phased.snakemake

snake:
	CWD=${PWD}/RUN snakemake -p -j 8 -d RUN -s ${SNAKE_SCRIPT} --configfile config.json --config MAKEDIR=.. -- finish

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
