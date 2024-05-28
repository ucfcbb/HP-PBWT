/*
Author: Kecong Tang (Benny) at University of Central Florida. 

Execution modules. It contains:
report L-Long Match and Set Maximal entrance point
random data generation
P D array initialization


*/

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using HP_PBWT_Reg;
using static HP_PBWT_Reg.hpPBWT;

namespace HP_PBWT_Reg
{
    internal class Execute
    {

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

        public static void LM_n_SMM(int nThread, int longMatchLen = -1)
        {
            if (longMatchLen > 0)
            {
                Console.Write("LM ");
            }
            else
            {
                Console.Write("SMM ");
            }
            Console.WriteLine();


            utl.VCF_ReaderLight reader = new utl.VCF_ReaderLight(Program.VCF_Path);
            BitArray firstSite = reader.ReadLine();
            Program.nTotalHap = reader.nHap;
            PDA initPDA = makeInitPDA(firstSite, reader.nHap);



            Console.WriteLine(DateTime.Now + " Running HP-PBWT...");
            hpPBWT.Pal pal = new Pal(reader.nHap, Program.nThread);

            pal.Run(initPDA, reader, longMatchLen);

            Console.WriteLine(DateTime.Now + " HP-PBWT Completed.");


            

        }

    }
}
