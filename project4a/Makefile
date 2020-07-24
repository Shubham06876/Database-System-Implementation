CC = g++ -O2 -Wno-deprecated 

LD_FLAGS = -l pthread -lgtest

tag = -i
test_out_tag = -ll

ifdef linux
tag = -n
test_out_tag = -lfl
endif

gtest: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Statistics.o GTest.o y.tab.o lex.yy.o
	$(CC) -o gtest Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Statistics.o GTest.o y.tab.o lex.yy.o $(test_out_tag) $(LD_FLAGS)

a4-1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Statistics.o y.tab.o lex.yy.o test.o
	$(CC) -o a4-1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Statistics.o y.tab.o lex.yy.o test.o -lfl

a2-2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Statistics.o Pipe.o y.tab.o lex.yy.o a2test.o
	$(CC) -o a2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Statistics.o Pipe.o y.tab.o lex.yy.o a2test.o $(test_out_tag) -lpthread

GTest.o:
	$(CC) -g -c GTest.cc

a2test.o: a2test.cc
	$(CC) -g -c a2test.cc
	
test.o: test.cc
	$(CC) -g -c test.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
