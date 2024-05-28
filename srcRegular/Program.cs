/*
Author: Kecong Tang (Benny) at University of Central Florida. 

Program entrance of our HP-PBWT

*/




using HP_PBWT_Reg;
using System.Collections;
using System.Text;

internal class Program
{
    public static string outPath = "";
    public static string VCF_Path = "";
    public static List<int> LM_Collection = new List<int>();
    public static List<int> SMM_Collection = new List<int>();
    public static int LMC_Size = 1000000;
    public static int nTotalHap = 0;
    public static int minBlockSize_Recur = 100;
    public static int nThread = 10;
    public static utl.AsyncWriter aw;

    private static void Main(string[] args)
    {
        int longMatchLen;
        VCF_Path = args[0];

        nThread = Convert.ToInt32(args[1]);
        hpPBWT.LM_Blk_nThread = Convert.ToInt32(args[2]);
        hpPBWT.LM_Report_nThread = Convert.ToInt32(args[3]);
        longMatchLen = Convert.ToInt32(args[4]);
        outPath = args[5];

        Console.WriteLine("VCF_Path: " + VCF_Path);
        Console.WriteLine("nThread: " + nThread);
        Console.WriteLine("nBlk: " + hpPBWT.LM_Blk_nThread);
        Console.WriteLine("nReport_Thread: " + hpPBWT.LM_Report_nThread);
        Console.WriteLine("longMatchLen: " + longMatchLen);
        Console.WriteLine("outPath: " + outPath);

        Execute.LM_n_SMM(nThread, longMatchLen);





    }
}