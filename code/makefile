CC = g++ -g -O0 -Wno-deprecated

tag = -i

ifdef linux
	tag = -n
endif

main.out:	InefficientMap.o EfficientMap.o DataTypeClass.o Keyify.o Schema.o Catalog.o main.o 
	$(CC) -o main.out main.o Catalog.o Schema.o Keyify.o DataTypeClass.o EfficientMap.o InefficientMap.o -lsqlite3
	
main.o:	main.cc
	$(CC) -c main.cc
	
Catalog.o: Catalog.h Catalog.cc	
	$(CC) -c Catalog.cc
	
Schema.o: Config.h Schema.h Schema.cc	
	$(CC) -c Schema.cc
	
EfficientMap.o: EfficientMap.h EfficientMap.cc
	$(CC) -c EfficientMap.cc

InefficientMap.o: InefficientMap.h InefficientMap.cc
	$(CC) -c InefficientMap.cc
	
Keyify.o: Keyify.h Keyify.cc
	$(CC) -c Keyify.cc	
	
DataTypeClass.o: DataTypeClass.h DataTypeClass.cc
	$(CC) -c DataTypeClass.cc

clean: 
	rm -f *.o
	rm -f *.out
