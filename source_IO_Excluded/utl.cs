using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HP_PBWT
{
    public class utl
    {
        public static List<BitArray> RandomPanelGenerator(int nSite, int nHap)
        {
            Console.Write(DateTime.Now + " Generating Random Panel " + nSite + " by " + nHap + " ...");

            List<BitArray> panel = new List<BitArray>();
            for (int i = 0; i < nSite; i++)
            {
                panel.Add(new BitArray(nHap));

            }
            //Console.WriteLine(DateTime.Now + " Holder Created.");

            Parallel.For(0, nSite, (s) =>
            {
                Random rnd = new Random((int)DateTime.Now.Ticks + Thread.CurrentThread.ManagedThreadId);

                for (int h = 0; h < nHap; h++)
                {
                    if (rnd.Next(2) == 1)
                    {
                        panel[s][h] = true;
                    }
                    else
                    {
                        panel[s][h] = false;
                    }

                }

            });
            Console.WriteLine(" Done.");

            return panel;
        }
    }
}
