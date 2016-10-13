# Indicates that clean is a command, not a file in the filesystem
.PHONY: clean

MAIN = minibase

SRCS = DBFile.java Page.java Pair.java BufferManager.java BMTester.java

OBJS = $(SRCS:.java=.class)

# Declares .java and .class as relevant suffixes
.SUFFIXES: .java .class

# Indicates that the final product depends on the class files
$(MAIN):  $(OBJS)
	

# Instructions on how to transform each .java to a .class.
# $< is an automatic variable that is equal to the first dependency,
# i.e. the file to be compiled
.java.class:
	javac  $<

clean:
	rm -f *.class *~
