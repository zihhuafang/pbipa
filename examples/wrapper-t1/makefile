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
	ipa local --only-print --verbose -i input.fofn
fresh-dry:
	rm -rf RUN/
	ipa local --dry-run --no-polish --no-phase -i input.fofn
fresh:
	rm -rf RUN/
	ipa local -i input.fofn
resume-dry:
	ipa local --dry-run --resume -i input.fofn
resume:
	ipa local --verbose --resume -i input.fofn
validate:
	ipa validate
