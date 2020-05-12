all:
	${MAKE} fresh-dry
	${MAKE} fresh
	${MAKE} resume-dry
	${MAKE} resume
fresh-dry:
	rm -rf RUN/
	ipa --dry-run --no-polish --no-phase input.fofn
fresh:
	rm -rf RUN/
	ipa input.fofn
resume-dry:
	ipa --dry-run --resume input.fofn
resume:
	ipa --resume input.fofn
