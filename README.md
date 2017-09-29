## Assignment A2 for CS6240
### Fall 2017
### Shreysa Sharma 

###Directory Structure

`src` - Contains Kneighborhood.java associated with the implementation.

`README.md` - Description of the implementation and how to run

`Makefile` - Makefile to assist in building, running and managing the project.

`report.Rmd` - Report file in R-Markdown format.

`report.html` - HTML rendering of the above report.

`input/books` - Contains the input files that was provided for assignment A0 (taken from assignment task page).

`input/big-corpus` - contains an empty folder where the reviewer can place their big corpus files

`output` - Contains csv files with the timings of Map reduce runs, comparison information of map reduce vs thraeded file and results of program  with threads (1-16) with k value of 2. These files are used in the generation of report.


###Instructions for building and running the program

I used the cloudera set up CDH 5(default) provided at https://www.cloudera.com/downloads/quickstart_vms/5-12.html.
1. Download and set up the above mentioned system if not already done.

2. Clone the code on the above set up environment
3. Unzip the input files by running "make gunzip" (this will unzip the file for assignment A0).
4. Build the project by running "make build".
5. Make a directory in hadoop file system and copy the input files there.
6. Map Reduce run: shoot the command "make run INPUT=<input_path_on_hadoop> OUTPUT=<non_existent_output_path>
KNEIGHBORS=<K_VALUE> eg: "make run INPUT=input_books OUTPUT=output_01 KNEIGHBORS=2"
7. The analysis of time taken for run of the map reduce job that I executed on the big dataset provided for assignment A2 is placed in output/Map_reduce_run.csv.
8.The analysis of time taken for run of the threaded program on the dataset provided for A0A1 is stored in output/results_kval_2.csv.
9. Comparison of runs on the 2 datasets is in the file output/dataset_time_comparison.csv.
10. make clean removes all the *.class files and the target folder, useful for a clean build eg. "make run".


### System requirements

1. CDH 5 Cloudera environment
2. make
3. pandoc
4. RMarkdown
5. ggplot


