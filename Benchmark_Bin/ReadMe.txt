This directory contains the Windows executable file to create our Reporting L-Long Match benchmark result.
This executable is made for benchmarking and has all the modules of our HP-PBWT, but it does not output any matches.
Instead, it outputs how long it takes for the sequential and parallel versions to complete.

A simple demonstration: HP-PBWT-BM.exe 1.cfg

The input file  *.cfg (I.E 1.cfg) is a configuration file. It requires three lines of configurations:

First Line(space dilimited):
  1. Number of sites to test.
  2. Number of threads to use, this depends on how many logic cores the machine has.
  3. Number of blocks to partition for L-Long match report. At least 2 to 3 times of number of threads. Feel free to try different numbers.
  4. Maximal number of helper threads for reporting matches in each block. I recommend setting it to 80% of the number of threads, this may cause chaos if set it wrong.
  5. Target length, in terms of number of sites.
  6. Run Sequential? 1 for yes; 0 for not.
  7. Run Parallel?  1 for yes; 0 for not.
  8. Read from VCF file? 1 for yes; 0 for not. The program will generate random inputs if 0 is chosen.
  9. VCF path, if yes for 8, a VCF path needs to be provided. 
          For using VCF file: 
            If you do not know how many sites are in the VCF, put 0 at parameter 1., this will trigger a scan to find out how many sites there are, this also takes time.
            Meanwhile, if the number of sites is given at parameter 1., the program will run the site scan.
            If the provided number of sites is less than the extra site in the VCF, the program will only read the provided number of sites.
  10. Output directory. Leaving a space will make the output to the current directory.


The second line is a list of the number of rounds you would like to repeat an experiment.
The third line is how many haplotypes are in each experiment.


E.G.
Second Line:  2 10
Third Line:  1000 500

This means you like to run the 1000 haplotype experiment 2 times, and the 50 haplotype experiment 10 times.




The results will be in a *.exLog.txt file, which has 13 columns:
round number of this experiment
total number of rounds of this experiment
number of haplotypes
number of sites
number of threads to use
Number of blocks to partition for L-Long match report
Maximum number of helper threads for reporting matches in each block
Target length
Run Sequential? 1 for yes; 0 for not.
Run Parallel?  1 for yes; 0 for not.
Read from VCF file? 1 for yes; 0 for not.
Sequential result in ms
Parallel result in ms
