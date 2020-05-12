all:
	${MAKE} fresh-dry
	${MAKE} fresh
	${MAKE} resume-dry
	${MAKE} resume
fresh-dry:
	rm -rf RUN/
	ipa local --dry-run --no-polish --no-phase input.fofn
fresh:
	rm -rf RUN/
	ipa local input.fofn
resume-dry:
	ipa local --dry-run --resume input.fofn
resume:
	ipa local --resume input.fofn
