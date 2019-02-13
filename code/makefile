CC = g++ -g -O0 -Wno-deprecated

tag = -i

ifdef linux
	tag = -n
endif

main.out:	Schema.o Catalog.o EfficientMap.o main.o
	$(CC) -o main.out main.o EfficientMap.o Catalog.o Schema.o -lsqlite3

main.o:	main.cc
	$(CC) -c main.cc

Catalog.o: Catalog.h Catalog.cc
	$(CC) -c Catalog.cc

Schema.o: Config.h Schema.h Schema.cc
	$(CC) -c Schema.cc

EfficientMap.o: EfficientMap.h EfficientMap.cc
	$(CC) -c EfficientMap.cc

clean:
	rm -f *.o
	rm -f *.out
