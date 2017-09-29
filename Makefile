ifndef NEIGHBORS
NEIGHBORS=2
endif

build: clean
	mkdir _build
	mkdir target
	javac -classpath `yarn classpath`:./src -d _build ./src/KNeighborhood.java
	jar -cvf ./target/KNeighborhood.jar -C _build/ .
	rm -rf ./_build

run:
	hadoop jar ./target/KNeighborhood.jar org.myorg.KNeighborhood $(INPUT) $(OUTPUT) $(NEIGHBORS)

clean:
	rm -rf ./target
	rm -rf ./build

gzip:
	-gzip -q ./input/books/*

gunzip:
	-gunzip -q ./input/books/*

