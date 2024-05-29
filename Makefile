
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++20 -Wall

all: correctness persistence

correctness: bloomfilter.o memtable.o skiplist.o sstable.o kvstore.o correctness.o

persistence: bloomfilter.o memtable.o skiplist.o sstable.o kvstore.o persistence.o

performance: bloomfilter.o memtable.o skiplist.o sstable.o kvstore.o performance.o

clean:
	-rm -f correctness persistence performance *.o
