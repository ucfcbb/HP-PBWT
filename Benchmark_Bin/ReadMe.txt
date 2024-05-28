This directory contains the windows executable file to create our Reporting L-Long Match benchmark result.
This executable is made for benchmark, has all the modules of our HP-PBWT, but it does not output any matches.
Instead, it outputs the how long time for the sequential and parallel version to complete.

A simple demostration: HP-PBWT-BM.exe 1.cfg

The input file  *.cfg (I.E 1.cfg) is a configureation file. It requires three lines of configuations:

First Line(space dilimited):
  1. Number of sites to test.
  2. Number of theads to use, this depends on how many logic core the machines has.
  3. Number of block to partition for L-Long match report. At least 2 to 3 times of number of thread. Feel free to try different numbers.
  4. Maximal number of helper thread for reporting matches in each block. Remommending set it to 80% of number of thread, this may cause chaos if set it run.
  5. Target length, interm of number of sites.
  6. Run Sequential? 1 for yes; 0 for not.
  7. Run Parallel?  1 for yes; 0 for not.
  8. Read from VCF file? 1 for yes; 0 for not. The program will generate random inputs if 0 is chosen.
  9. VCF path, if yes for 8, a vcf path needs to be provided. 
          For using VCF file: 
            If you do not know how many sites is the VCF, put 0 at parameter 1., this will triger a scan to find out how many sites there is, this also takes time.
            Meanwhile, if a number of site is given at parameter 1., the program will run the site scan.
            If a provided number of site is less than the exta site in the VCF, the program will only read the provided number of sites.
  10. Output directory. Leaving a space will make the output to current directory.


Second Line is a list of numbers of how many rounds of you like to repeat an experiment.
Third Line is how many haplotypes in each experiment.


E.G.
Second Line:  2 10
Third Line:  1000 500

This means you like to run the 1000 haplotype experiment 2 times, and the 50 haplotype experiment 10 times.




The results will be in a *.exLog.txt file, which has 13 columns:
round number of this experiment
total number of rounds of this experiment
number of halotypes
number of sites
number of thead to use
Number of block to partition for L-Long match report
Maximal number of helper thread for reporting matches in each block
Target length
Run Sequential? 1 for yes; 0 for not.
Run Parallel?  1 for yes; 0 for not.
Read from VCF file? 1 for yes; 0 for not.
Sequential result in ms
Parallel result in ms
