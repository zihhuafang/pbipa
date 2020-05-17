all:
	${MAKE} fresh-dry
	${MAKE} fresh
	${MAKE} resume-dry
	${MAKE} resume
fresh-dry:
	rm -rf RUN/
	ipa -i input.fofn --dry-run --no-polish --no-phase  local
fresh:
	rm -rf RUN/
	ipa -i input.fofn            local
resume-dry:
	ipa -i input.fofn --dry-run  local --resume
resume:
	ipa -i input.fofn --verbose  local --resume
