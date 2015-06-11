RTDMN_demo_RSW2014.pdf: RTDMN_demo_RSW2014.tex Makefile beamerthemeCMINKI.sty
	echo "$< $@"
	latex $< 
	dvipdf $(basename $<) $@
	rm $(basename $<).dvi
	rm  $(basename $<).log

all: RTDMN_demo_RSW2014.pdf
