.PHONY: all distributed clean 

#REDIS_TOOLKIT_INSTALL = $(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.jdbc
REDIS_TOOLKIT_INSTALL = ../../com.ibm.streamsx.redis

SPLC_FLAGS ?= -a
SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?= --data-directory=data -t $(REDIS_TOOLKIT_INSTALL)
SPL_MAIN_COMPOSITE = application::RedisTest


all: distributed

distributed:
	cd ../..; ant
	JAVA_HOME=$(STREAMS_INSTALL)/java $(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS)

clean: 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm -rf output

