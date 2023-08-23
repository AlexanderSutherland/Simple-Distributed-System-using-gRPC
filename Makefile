PREPCMD = sed -i"" "/\/\*\*\[BEGIN-REMOVE-FOR-STUDENTS\]\*\*\//,/\/\*\*\[END-REMOVE-FOR-STUDENTS\]\*\*\//d"

default: usage

part2:
	$(MAKE) -C part2


clean_part2:
	$(MAKE) clean -C part2

protos:
	$(MAKE) protos -C part2



protos_part2:
	$(MAKE) protos -C part2

clean_all:
	$(MAKE) clean_all -C part2

clean_protos:
	$(MAKE) clean_protos -C part2

the_works: clean_all protos part1 part2

SREPO_DIR = ./student-repo/


.PHONY: part2
.PHONY: part1_clean
.PHONY: part2_clean
.PHONY: clean_all
.PHONY: clean_protos
.PHONY: protos
.PHONY: the_works



