PATH:=${PATH}:${CURDIR}/../../scripts:${CURDIR}/../../bash
export PATH

all:
	${MAKE} fresh-dry
	${MAKE} fresh
	${MAKE} resume-dry
	${MAKE} resume
fresh-dry:
	which ipa
	rm -rf RUN/
	ipa local --dry-run --no-polish --no-phase input.fofn
fresh:
	rm -rf RUN/
	ipa local input.fofn
resume-dry:
	ipa local --dry-run --resume input.fofn
resume:
	ipa local --verbose --resume input.fofn
validate:
	ipa validate
