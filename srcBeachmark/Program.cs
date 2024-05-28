/*
Author: Kecong Tang (Benny) at University of Central Florida. 

Program entrance of our HP-PBWT


!!!Benchmark Mode!!!
Note, this program has all the modules of our HP-PBWT, but it does not output any matches.
Instead, it outputs the how long time for the sequential and parallel version to complete.

*/




using HP_PBWT_BM;
using System.Collections;
using System.Text;

internal class Program
{
    public static string outDir = "";
    public static string VCF_Path = "";
    public static List<int> LM_Collection = new List<int>();
    public static List<int> SMM_Collection = new List<int>();
    public static int LMC_Size = 1000000;
    public static int nTotalHap = 0;
    public static int minBlockSize_Recur = 100;

    private static void Main(string[] args)
    {
        Console.WriteLine("CFG: " + args[0]);

        string[] lines = File.ReadAllLines(args[0]);
        Console.WriteLine(lines[0]);
        Console.WriteLine(lines[1]);
        Console.WriteLine(lines[2]);


        string[] parts = lines[0].Split(' ');

        int nSite, nRound, nThread, longMatchLen, runSMM, runSeq, runPal, useVCF;

        nSite = Convert.ToInt32(parts[0]);
        nThread = Convert.ToInt32(parts[1]);
        hpPBWT.LM_Blk_nThread = Convert.ToInt32(parts[2]);
        hpPBWT.LM_Report_nThread = Convert.ToInt32(parts[3]);
        longMatchLen = Convert.ToInt32(parts[4]);
        //runSMM = Convert.ToInt32(parts[5]);
        runSeq = Convert.ToInt32(parts[5]);
        runPal = Convert.ToInt32(parts[6]);
        useVCF = Convert.ToInt32(parts[7]);
        VCF_Path = parts[8];
        outDir = parts[9];

        List<int> nHaps = new List<int>();
        List<int> nRounds = new List<int>();

        nRounds = Array.ConvertAll(lines[1].Split(" "), s => int.Parse(s)).ToList();
        nHaps = Array.ConvertAll(lines[2].Split(" "), s => int.Parse(s)).ToList();



        Console.WriteLine("nSite " + nSite);
        Console.WriteLine("nThread " + nThread);
        Console.WriteLine("PBWT.LM_Blk_nThread " + hpPBWT.LM_Blk_nThread);
        Console.WriteLine("PBWT.LM_Report_nThread " + hpPBWT.LM_Report_nThread);
        Console.WriteLine("longMatchLen " + longMatchLen);
        //Console.WriteLine("runSMM " + runSMM);
        Console.WriteLine("runSeq " + runSeq);
        Console.WriteLine("runPal " + runPal);
        Console.WriteLine("useVCF " + useVCF);
        Console.WriteLine("VCF_Path " + VCF_Path);


        Execute.Benchmark_LM(nHaps, nRounds, nSite, nThread, longMatchLen, runSeq, runPal, useVCF);







    }
}