CC=g++
CXXFLAGS=-g -Wall -pedantic -std=c++0x -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 -Wno-long-long -Wno-invalid-offsetof -I.

LDFLAGS=
LIBS=

MAKEDEPEND=${CC} -MM
PROGRAM=testindex

OBJS = index/leaf_node.o index/inner_node.o index/index.o testindex.o

DEPS:= ${OBJS:%.o=%.d}

all: $(PROGRAM)

${PROGRAM}: ${OBJS}
	${CC} ${CXXFLAGS} ${LDFLAGS} ${OBJS} ${LIBS} -o $@

clean:
	rm -f ${PROGRAM} ${OBJS} ${DEPS}

${OBJS} ${DEPS} ${PROGRAM} : Makefile

.PHONY : all clean

%.d : %.cpp
	${MAKEDEPEND} ${CXXFLAGS} $< -MT ${@:%.d=%.o} > $@

%.o : %.cpp
	${CC} ${CXXFLAGS} -c -o $@ $<

-include ${DEPS}
