ifeq ($(CNCROOT),)
$(error CNCROOT not defined in environment, please install Intel CnC and source its cncvars.sh script)
endif
ifeq ($(TBBROOT),)
$(error CNCROOT not defined in environment, please install Intel TBB and source its cncvars.sh script)
endif

LIBJSONHOME = libjson
ITACDIR = /opt/intel/itac/8.1.3
DRYRUN = -DDRYRUN
MY_BOOST_ROOT = boost_1_57_0
BOOST_LIBS = -lboost_serialization -lboost_regex -lboost_filesystem -lboost_system

OPT = -O2 -g -std=c++11 -DDEBUG -DJSON_ISO_STRICT #-pedantic  
LIBS = -L$(CNCROOT)/lib/intel64 -L$(LIBJSONHOME) -lcnc_debug -ltbb -ltbbmalloc -lrt -ljson  -lcurl -L$(MY_BOOST_ROOT)/stage/lib $(BOOST_LIBS)
INCLUDES = -I$(CNCROOT)/include -I$(LIBJSONHOME) -I$(MY_BOOST_ROOT)

all: precipes #traced_precipes

precipes: precipes.cpp pipeline_graph.hpp.gch parse_json.hpp.gch command_string.hpp.gch precipes.hpp.gch libjson.h.gch libjson.a
	$(CXX) $(OPT) -o $@ $< $(INCLUDES) $(LIBS) 

# precompile the quite sizable headers
%.hpp.gch: %.hpp libjson.h.gch
	$(CXX) $(OPT) -x c++-header $(INCLUDES) $<

libjson.h.gch: $(LIBJSONHOME)/libjson.h
	$(CXX) $(OPT) -w -I$(LIBJSONHOME) -x c++-header -o $@ $<

libjson.a:
	make -C $(LIBJSONHOME)

# traced_precipes: precipes.cpp pipeline_graph.hpp.gch parse_json.hpp.gch command_string.hpp.gch precipes.hpp.gch libjson.h.gch
# 	$(CXX) $(OPT) -o $@ $< -DCNC_WITH_ITAC $(INCLUDES) -I$(ITACDIR)/include $(LIBS) -L$(ITACDIR)/slib -lVTnull 

.PHONY: clean

clean :
	rm -f precipes *.o *.gch
	make -C $(LIBJSONHOME) clean