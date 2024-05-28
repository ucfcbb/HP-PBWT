/*
Author: Kecong Tang (Benny) at University of Central Florida. 

Execution modules. It contains:
report L-Long Match entrance point
random data generation
P D array initialization


!!!Benchmark Mode!!!
Note, this program has all the modules of our HP-PBWT, but it does not output any matches.
Instead, it outputs the how long time for the sequential and parallel version to complete.

*/

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using HP_PBWT_BM;
using static HP_PBWT_BM.hpPBWT;

namespace HP_PBWT_BM
{
    internal class Execute
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        /// <param name="nRound"></param>
        /// <param name="nThread"></param>
        /// <param name="longMatchLen">will run long match if >0 </param>
        /// <param name="runSMM">will run SMM if !=0</param>
        /// <param name="runSeq"></param>
        /// <param name="runPal"></param>
        /// <param name="useVCF"></param>
        public static void Benchmark_LM(List<int> nHaps, List<int> nRounds, int nSite, int nThread, 
            int longMatchLen = -1, int runSeq = 0, int runPal = 0, int useVCF = 0)
        {
            if (longMatchLen > 0)
            {
                Console.Write("LM ");
            }

            Console.WriteLine();
            //hpPBWT.LM_Blk_nThread.ToString() + "\t" + hpPBWT.LM_Report_nThread.ToString()
            string logPath = Program.outDir + "S" + DateTime.Now.Ticks.ToString() + "s" + nSite.ToString() + ".exLog.txt";
            StreamWriter sw = new StreamWriter(logPath);
            sw.WriteLine("round\tnRound\tnHap\tnSite\tnThread\tnBlk\tnThreadRPT\tlongMatchLen\trunSeq\trunPal\tuseVCF\tSeqMs\tPalMs");
            bool vcfRead = false;
            List<BitArray> sites = null;
            List<BitArray> wholeSites = null;
            utl.VCF_MemV9 vcf = null;

            for (int t = 0; t < nRounds.Count; t++)
            {
                int nRound = nRounds[t];
                int nHap = nHaps[t];

                Program.nTotalHap = nHap;

                for (int r = 0; r < nRound; r++)
                {

                    if (useVCF == 1)
                    {
                        if (vcfRead == false)
                        {

                            vcf = new utl.VCF_MemV9(Program.VCF_Path, nSite, 200, nThread, 5000, true);
                            vcf.Read();
                            wholeSites = vcf.panel_BitArr;
                            vcfRead = true;


                        }

                        if (nHap == wholeSites[0].Count)
                        {
                            sites = wholeSites;

                        }
                        else
                        {
                            List<int> haps = utl.randomSelect(nHap, vcf.nIndv * 2, r);
                            sites = new List<BitArray>();

                            for (int i = 0; i < nSite; i++)
                            {
                                sites.Add(new BitArray(nHap));
                            }


                            Parallel.For(0, sites.Count, (i) =>
                            {
                                makeSite_SelectHaps(wholeSites[i], sites[i], haps);
                            });
                        }

                    }
                    else
                    {
                        Console.WriteLine(DateTime.Now + " Making inputs & Holders " + nHap + " x " + nSite + "...");

                        sites = new List<BitArray>();
                        for (int i = 0; i < nSite; i++)
                        {
                            sites.Add(new BitArray(nHap));
                        }

                        Parallel.For(0, nSite, (i) =>
                        {
                            Random rnd = new Random((int)DateTime.Now.Ticks + Thread.CurrentThread.ManagedThreadId);
                            makeSites(nHap, sites[i], rnd);

                        });

                    }


                    if (Program.LM_Collection.Count() == 0)
                    {
                        for (int i = 0; i < Program.LMC_Size; i++)
                        {
                            Program.LM_Collection.Add(0);
                        }
                    }


                    PDA initPDA;
                    Stopwatch stw = new Stopwatch();

                    string seqResStr = "";
                    string palResStr = "";
                    double seqMS = -1;
                    double palMS = -1;
                    if (runSeq == 1)
                    {
                        initPDA = makeInitPDA(sites[0], nHap);
                        Console.WriteLine(DateTime.Now + " Running Sequential...");
                        hpPBWT.Seq seq = new hpPBWT.Seq(nHap);
                        stw = new Stopwatch();
                        stw.Start();
                        PDA seqRes = seq.Run(initPDA, sites, longMatchLen);
                        stw.Stop();
                        Console.WriteLine(DateTime.Now + " Sequential Completed.");
                        seqResStr = "Seq " + stw.Elapsed.TotalMilliseconds.ToString() + "ms";
                        seqMS = stw.Elapsed.TotalMilliseconds;
                        Console.WriteLine(stw.Elapsed.TotalMilliseconds + "ms");

                        Random rnd = new Random((int)DateTime.Now.Ticks);
                        Console.WriteLine("Random Access: " + Program.LM_Collection[rnd.Next(Program.LMC_Size)]);

                    }

                    if (runPal == 1)
                    {
                        stw = new Stopwatch();
                        initPDA = makeInitPDA(sites[0], nHap);
                        Console.WriteLine(DateTime.Now + " Running Parallel...");
                        hpPBWT.Pal pal = new Pal(nHap, nThread);
                        stw.Restart();
                        PDA palRes = pal.Run(initPDA, sites, longMatchLen);
                        stw.Stop();
                        Console.WriteLine(DateTime.Now + " Parallel Completed.");
                        palMS = stw.Elapsed.TotalMilliseconds;
                        palResStr = "Pal " + stw.Elapsed.TotalMilliseconds.ToString() + "ms";
                        Console.WriteLine(stw.Elapsed.TotalMilliseconds + "ms");

                        Random rnd = new Random((int)DateTime.Now.Ticks);
                        Console.WriteLine("Random Access: " + Program.LM_Collection[rnd.Next(Program.LMC_Size)]);

                    }

                    Console.WriteLine(r.ToString() + "\t" + nRound.ToString() + "\t" + nHap.ToString() + "\t" + nSite.ToString() + "\t" + nThread.ToString()
                        + "\t" + hpPBWT.LM_Blk_nThread.ToString() + "\t" + hpPBWT.LM_Report_nThread.ToString()
                       + "\t" + longMatchLen.ToString()  + "\t" + runSeq.ToString() + "\t" + runPal.ToString()
                        + "\t" + useVCF.ToString() + "\t" + seqMS.ToString() + "\t" + palMS.ToString());


                    sw.WriteLine(r.ToString() + "\t" + nRound.ToString() + "\t" + nHap.ToString() + "\t" + nSite.ToString() + "\t" + nThread.ToString()
                        + "\t" + hpPBWT.LM_Blk_nThread.ToString() + "\t" + hpPBWT.LM_Report_nThread.ToString()
                       + "\t" + longMatchLen.ToString() + "\t" + runSeq.ToString() + "\t" + runPal.ToString()
                        + "\t" + useVCF.ToString() + "\t" + seqMS.ToString() + "\t" + palMS.ToString());


                }
            }

            sw.Close();

        }
        public static void makeSite_SelectHaps(BitArray wholeSite, BitArray newSite, List<int> haps)
        {

            for (int i = 0; i < haps.Count(); i++)
            {
                newSite[i] = wholeSite[haps[i]];
            }

        }

        public static PDA makeInitPDA(BitArray site, int size)
        {
            PDA oldPDA = new PDA(size);
            List<int> ones = new List<int>();
            List<int> zeros = new List<int>();
            for (int i = 0; i < size; i++)
            {
                if (site[i] == true)
                {
                    if (ones.Count() == 0)
                    {
                        oldPDA.mLens[i] = 0;
                    }
                    else
                    {
                        oldPDA.mLens[i] = 1;
                    }
                    ones.Add(i);
                }
                else
                {
                    if (zeros.Count() == 0)
                    {
                        oldPDA.mLens[i] = 0;

                    }
                    else
                    {
                        oldPDA.mLens[i] = 1;

                    }

                    zeros.Add(i);

                }
            }

            for (int i = 0; i < zeros.Count(); i++)
            {
                oldPDA.pArr[i] = zeros[i];
            }

            int k = 0;
            for (int i = zeros.Count(); i < size; i++)
            {
                oldPDA.pArr[i] = ones[k];
                k++;
            }

            oldPDA.zeroCnt = zeros.Count();

            return oldPDA;

        }

        public static void makeSites(int size, BitArray site, Random rnd)
        {

            for (int i = 0; i < size; i++)
            {
                if (rnd.Next(2) == 1)
                {
                    site[i] = true;
                }
                else
                {
                    site[i] = false;

                }
            }
 
        }


    }
}
