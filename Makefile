CFLAGS=-Wall -g
CXXFLAGS=$(CFLAGS) -std=c++11
CC=c++

all: main verify

main: main.o
verify: verify.o
