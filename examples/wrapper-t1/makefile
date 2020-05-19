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
	ipa -i input.fofn --dry-run --no-polish --no-phase  local
fresh:
	rm -rf RUN/
	ipa -i input.fofn            local
resume-dry:
	ipa -i input.fofn --dry-run  local --resume
resume:
	ipa -i input.fofn --verbose  local --resume
validate:
	ipa validate
