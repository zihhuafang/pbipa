PATH:=${PATH}:${CURDIR}/../../scripts:${CURDIR}/../../bash
export PATH

all:
	${MAKE} fresh-dry
	${MAKE} fresh
	${MAKE} resume-dry
	${MAKE} resume
print:
	which ipa
	rm -rf RUN/
	ipa local --njobs 2 --only-print --verbose -i input.fofn
fresh-dry:
	rm -rf RUN/
	ipa local --njobs 2 --dry-run --no-polish --no-phase -i input.fofn
fresh:
	rm -rf RUN/
	ipa local --njobs 2 -i input.fofn
resume-dry:
	ipa local --njobs 2 --dry-run --resume -i input.fofn
resume:
	ipa local --njobs 2 --verbose --resume -i input.fofn
validate:
	ipa validate
