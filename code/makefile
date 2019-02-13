CC = g++ -g -O0 -Wno-deprecated

tag = -i

ifdef linux
	tag = -n
endif

main.out:	Schema.o EfficientMap.o Catalog.o main.o
	$(CC) -o main.out main.o Catalog.o EfficientMap.o Schema.o -lsqlite3 -lstdc++

main.o:	main.cc
	$(CC) -c main.cc

Catalog.o: Catalog.h Catalog.cc
	$(CC) -c Catalog.cc

EfficientMap.o: EfficientMap.h EfficientMap.cc
	$(CC) -c EfficientMap.cc

Schema.o: Config.h Schema.h Schema.cc
	$(CC) -c Schema.cc

clean:
	rm -f *.o
	rm -f *.out
