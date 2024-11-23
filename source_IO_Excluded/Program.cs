 
using System.Collections;
using System.Diagnostics;
using HP_PBWT;

public class Program
{
    #region user inputs


    public static int nSite = 0;
    public static int nHap = 0;
    public static int nThread = 0;
    public static int LLM_Len = 0;
    public static int nRun = 0;

    #endregion

    public static int MS_Cooldown = 60000;
    public static int LC_THD;
    public static List<BitArray> panel;
    public static int[] Sums;

    private static void Main(string[] args)
    {
        if (args.Count() != 6)
        {
            Console.WriteLine("HP-PBWT In Memory Version For Benchmark Purpose.");
            Console.WriteLine("It uses my C# implementation of the original sequential PBWT, Set # of Thread to 1 to trigger.");
            Console.WriteLine("Random Panel is generated in memory for L-Long Match Benchmark.");
            Console.WriteLine("\nInputs:");
            Console.WriteLine("<# of Sites, int.> <# of Haplotypes, int.> <# of Thread, int.> <L-Long Match Length, int.> <# of Runs, int.> <MS for Cooldown, int.>");
            return;
        }



        nSite = Convert.ToInt32(args[0]);
        nHap = Convert.ToInt32(args[1]);
        nThread = Convert.ToInt32(args[2]);
        LLM_Len = Convert.ToInt32(args[3]);
        nRun = Convert.ToInt32(args[4]);
        MS_Cooldown = Convert.ToInt32(args[5]);

        LC_THD = LLM_Len - 1;


        Console.WriteLine("HP-PBWT L-Long Match Benchmark Version.");
        Console.WriteLine("# of Sites: " + nSite);
        Console.WriteLine("# of Haplotypes: " + nHap);
        Console.WriteLine("nThread: " + nThread);
        Console.WriteLine("Length: " + LLM_Len);

        Console.WriteLine("nRun: " + nRun);
        Console.WriteLine();

        StreamWriter swLog = new StreamWriter(DateTime.Now.Ticks + ".log");
        swLog.NewLine = "\n";


        panel = utl.RandomPanelGenerator(nSite, nHap);


        Console.WriteLine(DateTime.Now + " Cooldown for " + MS_Cooldown + " ms...");
        Thread.Sleep(MS_Cooldown);


        for (int r = 0; r < nRun; r++)
        {
            Sums = new int[nHap];

            Stopwatch spw;

            Console.WriteLine(DateTime.Now + " Cooldown for " + MS_Cooldown + " ms...");
            Thread.Sleep(MS_Cooldown);


            if (nThread != 1)
            {
                Console.WriteLine("HP-PBWT");
                PBWT.Pal pal = new PBWT.Pal();
                spw = Stopwatch.StartNew();
                spw.Start();

                pal.Run();

                spw.Stop();
            }
            else
            {
                Console.WriteLine("Sequential PBWT");
                PBWT.Seq seq = new PBWT.Seq();

                spw = Stopwatch.StartNew();
                spw.Start();

                seq.Run();

                spw.Stop();
            }

            //checkSum();

            swLog.WriteLine(nSite + "\t" + nHap + "\t" + nThread + "\t" + LLM_Len + "\t" + MS_Cooldown + "\t" + spw.ElapsedMilliseconds);

            Random rnd = new Random((int)DateTime.Now.Ticks);
            Console.WriteLine("Random Access: " + Sums[rnd.Next(nHap)]);
            Console.WriteLine();
            Console.WriteLine("MS: " + spw.ElapsedMilliseconds);
        }


        swLog.Close();
    }




    public static void checkSum()
    {
        bool good = false;
        Parallel.For(0, nHap, (i, stat) =>
        {
            if (Program.Sums[i] > 0)
            {
                good = true;
                Console.WriteLine("Good Number. " + i + "  " + Program.Sums[i]);
                stat.Break();
            }


        });

        if (!good)
        {
            Console.WriteLine("XXXXX   ----All 0s----   XXXXX");
        }
    }

}