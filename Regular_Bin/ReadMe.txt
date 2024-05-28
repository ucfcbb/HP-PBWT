This directory contains the Windows executable file to output L-Long matches and Set-Maximal Matches

A simple demonstration: HP-PBWT-Reg.exe input.VCF 10 20 8 0 out.txt

Inputs:
1. a VCF file.
2. Number of threads to use, this depends on how many logic cores the machine has.
3. Number of blocks to partition for L-Long match report. At least 2 to 3 times of number of threads. Feel free to try different numbers.
4. Maximal number of helper threads for reporting matches in each block. Recommend setting to 80% of the number of threads, this may cause chaos if set it wrong.
5. Target length for L-Long Matches, in terms of the number of sites.
   Set to 0 to run Set-Maximal Matches




Output format:

4 columns for L-Long Matches:
haplotypeID_1
haplotypeID_2
match length
match ends location(inclusively)



4 columns for Set Maximal Matches:
haplotypeID_1
haplotypeID_2
match ends location(inclusively)
match length


