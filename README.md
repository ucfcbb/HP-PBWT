# HP-PBWT
Haplotype-based parallel PBWT.

Contact Author: Kecong Tang (Kecong.Tang@ucf.edu or Benjamin.KT.vln@gmail.com)

HP-PBWT is a tool to parallel Durbin's PBWT's all vs all L-Long Matches and Set-Maximal Matches by dividing the haplotype dimension. More detailed information will be provided later.

Durbin's PBWT can be found at https://github.com/richarddurbin/pbwt.

Source Code files for general purposes can be found in source_IO_Included/ 

Source Code files for IO-Excluded bencharmk can be found in source_IO_Excluded/




# Dependencies

Compile and run C#:

1. Install .NET SDK:

   For Winodows:

   Follow instructions from https://learn.microsoft.com/en-us/dotnet/core/install/windows

   For Linux:
   
   sudo apt-get update
   
   sudo apt-get install -y dotnet-sdk-8.0
   
   Addational details could be found at https://learn.microsoft.com/en-us/dotnet/core/install/linux-ubuntu-install?tabs=dotnet9&pivots=os-linux-ubuntu-2410
   
3. Build:
 
   a. Create a directory (E.g. HP-PBWT).
   
   b. Download all the files from the source directory (source_IO_Included/ or source_IO_Excluded/) to the directory created in a.
   
   c. Run "dotnet build --configuration Release" in the directory
   
4. Run:
   
   The compiled executable file should be /bin/Release/net8.0/HP-PBWT
   
   The directory name depends on the dotnet version, "net8.0" for dotnet 8.0.
   
## Citation

The HP-PBWT method was presented at **ICCABS 2025**. The official proceedings are currently in production.  

A preprint version is available on BioRxiv:  Tang, Kecong, et al. "Haplotype-based Parallel PBWT for Biobank Scale Data." bioRxiv (2025): 2025-02.
https://www.biorxiv.org/content/10.1101/2025.02.04.636317v1
