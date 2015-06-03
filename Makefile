ifeq ($(CNCROOT),)
$(error CNCROOT not defined in environment, please install Intel CnC and source its cncvars.sh script)
endif
ifeq ($(TBBROOT),)
$(error CNCROOT not defined in environment, please install Intel TBB and source its cncvars.sh script)
endif

LIBJSONHOME = libjson
DRYRUN = -DDRYRUN

all: precipes

precipes: precipes.cpp precipes.hpp libjson.a
	$(CXX) -O2 -std=c++11 -o $@ $< -I$(CNCROOT)/include -I$(LIBJSONHOME) -L$(CNCROOT)/lib/intel64 -lcnc -ltbb -ltbbmalloc -lrt -L$(LIBJSONHOME) -ljson

libjson.a:
	make -C	$(LIBJSONHOME)

clean :
	make -C $(LIBJSONHOME) clean
	rm -f precipes *.o
