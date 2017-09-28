build: clean
	mkdir _build
	mkdir target
	javac -classpath `yarn classpath`:./src -d _build ./src/KNeighborhood.java
	jar -cvf ./target/KNeighborhood.jar -C _build/ .
	rm -rf ./_build

run:
	hadoop jar ./target/KNeighborhood.jar org.myorg.KNeighborhood $(INPUT) $(OUTPUT)

clean:
	rm -rf ./target
	rm -rf ./_build

gunzip:
	gunzip -q ./input/books/*

gzip:
	gzip -q ./input/books/*

