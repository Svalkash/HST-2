CC = g++ -std=c++11
CUC = nvcc -ccbin g++ -std=c++11
CUFLAGS = -m64 -gencode arch=compute_35,code=sm_35 -gencode arch=compute_37,code=sm_37 -gencode arch=compute_50,code=sm_50 -gencode arch=compute_52,code=sm_52 -gencode arch=compute_60,code=sm_60 -gencode arch=compute_61,code=sm_61 -gencode arch=compute_70,code=sm_70 -gencode arch=compute_75,code=sm_75 -gencode arch=compute_80,code=sm_80 -gencode arch=compute_86,code=sm_86 -gencode arch=compute_86,code=compute_86
LDFLAGS = -L /usr/local/cuda/lib64 -lcudart

.PHONY: all cli host clean

all: build/main.exe

build/main.o: src/main.cpp src/linearprobing.h
	$(CC) -o $@ -c $<
build/test.o: src/test.cpp src/linearprobing.h
	$(CC) -o $@ -c $<
build/linearprobing.o: src/linearprobing.cu src/linearprobing.h
	$(CUC) $(CUFLAGS) -o $@ -c $<
build/main.exe: build/main.o build/test.o build/linearprobing.o
	$(CC) $(LDFLAGS) $^ -o $@

#---------------------------------------------------------------

clean:
	rm -f build/*

#---------------------------------------------------------------