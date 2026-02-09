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
The HP-PBWT method was formally published in the proceedings of ICCABS 2025.

Official publication:

Kecong Tang, Ahsan Sanaullah, Degui Zhi, and Shaojie Zhang.

Haplotype-based Parallel PBWT for Biobank Scale Data.

In Computational Advances in Bio and Medical Sciences, 13th International Conference (ICCABS 2025), Revised Selected Papers. Springer Nature, 2025.

DOI: https://doi.org/10.1007/978-3-032-02489-3_10

A preprint version of this work was previously available on bioRxiv (2025).


```
@inproceedings{Tang2025HPPBWT,
  author    = {Tang, Kecong and Sanaullah, Ahsan and Zhi, Degui and Zhang, Shaojie},
  title     = {Haplotype-Based Parallel PBWT for Biobank Scale Data},
  booktitle = {Computational Advances in Bio and Medical Sciences},
  series    = {ICCABS 2025, Revised Selected Papers},
  year      = {2025},
  publisher = {Springer Nature},
  doi       = {10.1007/978-3-032-02489-3_10}
}
```
