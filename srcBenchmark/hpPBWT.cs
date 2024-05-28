/*
Author: Kecong Tang (Benny) at University of Central Florida. 

Core HP-PBWT modules, has my personally implemented sequential PBWT, and parallel PBWT.

!!!Benchmark Mode!!!
Note, this program has all the modules of our HP-PBWT, but it does not output any matches.
Instead, it outputs the how long time for the sequential and parallel version to complete.

*/


using System;
using System.Collections.Concurrent;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

namespace HP_PBWT_BM
{
    public class hpPBWT
    {

        static ConcurrentDictionary<int, bool> LM_Worker_Pool = new ConcurrentDictionary<int, bool>();

        static int LM_BlockSize = -1;
        static int LM_SearchRange = -1;
        static PDA LM_CurrPDA;
        static int LM_Len;
        static BitArray LM_Site;
        static int LM_WorkingSite = 0;
        static int LM_nHap = 0;
        static int nHapCovered = 0;
        public static int LM_Report_nThread = 6;
        public static int LM_Blk_nThread = 10;
        static ParallelOptions LM_Report_pop = new ParallelOptions();

        public class PDA
        {
            public int[] pArr;

            public int zeroCnt = 0;
            public int[] mLens;

            public int nHap;
            public PDA(int nHapTotal)
            {
                nHap = nHapTotal;
                pArr = new int[nHap];

                mLens = new int[nHap];

            }

        }


        public class StatArr
        {
            public int cnt = 0;
            public int[] vals;
            public StatArr(int maxSize)
            {
                vals = new int[maxSize];
            }

            public void Add(int val)
            {
                vals[cnt] = val;
                cnt++;
            }

            public void Set(int val, int index)
            {
                vals[index] = val;
            }


        }

        public class BiArr
        {
            public StatArr zeros;
            public StatArr ones;

            public int nHap;
            public BiArr(int nHapTotal)
            {
                nHap = nHapTotal;
                zeros = new StatArr(nHap);
                ones = new StatArr(nHap);

            }



        }

        public class Seq
        {
            PDA newPDA;
            PDA oldPDA;
            PDA temPDA;

            public Seq(int numHap)
            {
                newPDA = new PDA(numHap);
                oldPDA = new PDA(numHap);
            }

            public void OneSort(PDA oldPDA, PDA newPDA, BitArray site)
            {
                BiArr biPDA = new BiArr(oldPDA.nHap);


                int prvLowM_One = 0;
                int prvLowM_Zero = 0;
                int hIndex = 0;

                prvLowM_One = -1;
                prvLowM_Zero = -1;

                for (int i = 0; i < oldPDA.nHap; i++)
                {

                    if (i == oldPDA.zeroCnt)
                    {
                        prvLowM_One = -1;
                        if (biPDA.zeros.cnt > 0)
                        {
                            prvLowM_One = 0;
                        }
                        prvLowM_Zero = -1;
                        if (biPDA.ones.cnt > 0)
                        {
                            prvLowM_Zero = 0;
                        }
                    }


                    hIndex = oldPDA.pArr[i];
                    //incoming is 0
                    if (site[hIndex] == false)
                    {
                        biPDA.zeros.Add(hIndex);

                        newPDA.mLens[hIndex] = Math.Min(oldPDA.mLens[hIndex], prvLowM_One) + 1;
                        prvLowM_One = int.MaxValue;
                        prvLowM_Zero = Math.Min(prvLowM_Zero, oldPDA.mLens[hIndex]);


                    }
                    else//incoming is 1
                    {
                        biPDA.ones.Add(hIndex);

                        newPDA.mLens[hIndex] = Math.Min(oldPDA.mLens[hIndex], prvLowM_Zero) + 1;
                        prvLowM_Zero = int.MaxValue;
                        prvLowM_One = Math.Min(prvLowM_One, oldPDA.mLens[hIndex]);
                    }

                }

                for (int i = 0; i < biPDA.zeros.cnt; i++)
                {
                    newPDA.pArr[i] = biPDA.zeros.vals[i];
                }

                int oneIndex = 0;
                for (int i = biPDA.zeros.cnt; i < oldPDA.nHap; i++)
                {
                    newPDA.pArr[i] = biPDA.ones.vals[oneIndex];
                    oneIndex++;
                }
                newPDA.zeroCnt = biPDA.zeros.cnt;


            }



            public PDA Run(PDA initPDA, List<BitArray> sites, int matchLength)
            {

                oldPDA = initPDA;

                ConcurrentBag<string> SMM_Collection = new ConcurrentBag<string>();

                Random rnd = new Random((int)DateTime.Now.Ticks);
                int rIndex;

                for (int i = 1; i < sites.Count() - 1; i++)
                {

                    //Console.WriteLine(DateTime.Now.ToString() + " Site " + i);

                    OneSort(oldPDA, newPDA, sites[i]);
                    if (matchLength > 0 && i >= matchLength)
                    {

                        ReportLongMatches(matchLength, newPDA, i, sites);

                    }

                    temPDA = oldPDA;
                    oldPDA = newPDA;
                    newPDA = temPDA;

                    SMM_Collection.Clear();
                }

                return newPDA;



            }


            /// <summary>
            /// 
            /// </summary>
            /// <param name="len"></param>
            /// <param name="pda">new pda</param>
            /// <param name="siteIndex">sorted to this index(included)</param>
            /// <param name="sites"></param>
            /// <returns></returns>
            public void ReportLongMatches(int len, PDA pda, int siteIndex, List<BitArray> sites)
            {


                BitArray site;
                if (siteIndex < sites.Count() - 1)
                {
                    site = sites[siteIndex + 1];
                }
                else
                {
                    site = null;
                }


                int zeroCnt = 0, oneCnt = 0;
                int topIndex = 0;
                int minVal = int.MaxValue;


                //need to consider special case: last site

                for (int h = 0; h < pda.pArr.Count(); h++)
                {
                    if (pda.mLens[pda.pArr[h]] < len)
                    {
                        if (zeroCnt != 0 && oneCnt != 0)
                        {
                            for (int i = topIndex; i < h - 1; i++)
                            {
                                minVal = int.MaxValue;
                                for (int k = i + 1; k < h; k++)
                                {
                                    minVal = Math.Min(minVal, pda.mLens[pda.pArr[k]]);

                                    if (site == null)
                                    {
                                        Program.LM_Collection[pda.pArr[i] % Program.LMC_Size] += pda.pArr[k] + minVal;
                                    }
                                    else
                                    {
                                        if (site[pda.pArr[i]] != site[pda.pArr[k]])
                                        {
                                            Program.LM_Collection[pda.pArr[i] % Program.LMC_Size] += pda.pArr[k] + minVal;
                                        }
                                    }

                                }
                            }

                        }

                        zeroCnt = 0;
                        oneCnt = 0;
                        topIndex = h;
                    }

                    if (site != null)
                    {
                        if (site[pda.pArr[h]] == true)
                        {
                            oneCnt++;
                        }
                        else
                        {
                            zeroCnt++;
                        }
                    }
                    else
                    {
                        oneCnt = 1;
                        zeroCnt = 1;
                    }



                }
                //tail case
                if (zeroCnt != 0 && oneCnt != 0)
                {
                    for (int i = topIndex; i < pda.pArr.Count(); i++)
                    {
                        minVal = int.MaxValue;
                        for (int k = i + 1; k < pda.pArr.Count(); k++)
                        {
                            minVal = Math.Min(minVal, pda.mLens[pda.pArr[k]]);

                            if (site == null)
                            {
                                Program.LM_Collection[pda.pArr[i] % Program.LMC_Size] += pda.pArr[k] + minVal;
                            }
                            else
                            {
                                if (site[pda.pArr[i]] != site[pda.pArr[k]])
                                {
                                    Program.LM_Collection[pda.pArr[i] % Program.LMC_Size] += pda.pArr[k] + minVal;
                                }
                            }

                        }
                    }

                }





            }



        }


        public class Pal
        {
            public class UpLowRange
            {
                public int UpRange;
                public int LowerRange;
                public UpLowRange(int up, int low)
                {
                    UpRange = up;
                    LowerRange = low;
                }
            }

            PDA newPDA;
            PDA oldPDA;
            PDA temPDA;
            int[] ppsHolder;
            int[] offsetsHolder;

            int nThread;
            ParallelOptions pOp = new ParallelOptions();


            public Pal(int numHap, int numThread)
            {
                nThread = numThread;
                newPDA = new PDA(numHap);
                oldPDA = new PDA(numHap);

                ppsHolder = new int[numHap];
                offsetsHolder = new int[nThread];
                pOp.MaxDegreeOfParallelism = nThread;
                //Run_LM_Workers();
            }

            public void coreP_Arr(PDA oldPDA, PDA newPDA, int[] psHolder, int[] offsets, BitArray site, int nThread)
            {

                int blockSize = Convert.ToInt32(Math.Ceiling(Convert.ToDouble(oldPDA.nHap) / Convert.ToDouble(nThread)));

                #region step 1 local sum
                Parallel.For(0, nThread, (i) =>
                {
                    int s = i * blockSize;
                    if (s >= oldPDA.nHap)
                    { return; }
                    int e = i * blockSize + blockSize;
                    if (e > oldPDA.nHap)
                    {
                        e = oldPDA.nHap;
                    }

                    //psHolder[s] = site[oldPDA.pArr[s]];

                    if (site[oldPDA.pArr[s]] == true)
                    {
                        psHolder[s] = 1;
                    }
                    else
                    {
                        psHolder[s] = 0;
                    }

                    for (int k = s + 1; k < e; k++)
                    {
                        //psHolder[k] = psHolder[k - 1] + site[oldPDA.pArr[k]];
                        if (site[oldPDA.pArr[k]] == true)
                        {
                            psHolder[k] = psHolder[k - 1] + 1;
                        }
                        else
                        {
                            psHolder[k] = psHolder[k - 1];
                        }
                    }

                    offsets[i] = psHolder[e - 1];
                });
                #endregion

                #region step 2 seq offset handling
                for (int i = 1; i < nThread; i++)
                {
                    offsets[i] = offsets[i - 1] + offsets[i];
                }

                #endregion

                #region step 3 apply offsets
                Parallel.For(1, nThread, (i) =>
                {
                    int s = i * blockSize;
                    if (s >= oldPDA.nHap)
                    { return; }
                    int e = i * blockSize + blockSize;
                    if (e > oldPDA.nHap)
                    {
                        e = oldPDA.nHap;
                    }

                    for (int k = s; k < e; k++)
                    {
                        psHolder[k] = psHolder[k] + offsets[i - 1];
                    }
                });
                #endregion

                #region step 4 pps -> index settle to new p arr 
                newPDA.zeroCnt = oldPDA.nHap - psHolder[oldPDA.nHap - 1];
                int oneOff = newPDA.zeroCnt - 1;
                Parallel.For(0, oldPDA.nHap, (i) =>
                //for (int i = 0; i < oldPDA.nHap; i++)
                {
                    if (site[oldPDA.pArr[i]] == true)
                    {
                        newPDA.pArr[psHolder[i] + oneOff] = oldPDA.pArr[i];
                    }
                    else
                    {
                        newPDA.pArr[i - psHolder[i]] = oldPDA.pArr[i];
                    }
                });

                #endregion

            }

            public void coreD_Arr(PDA oldPDA, PDA newPDA, int[] psHolder, BitArray site, int nThread, int sortedTo)
            {


                int blockSize = Convert.ToInt32(Math.Ceiling(Convert.ToDouble(oldPDA.nHap) / Convert.ToDouble(nThread)));

                //for (int i = 0; i < nThread; i++)
                Parallel.For(0, nThread, (i) =>
                {
                    int s = i * blockSize;
                    if (s >= oldPDA.nHap)
                    { return; }

                    int e = i * blockSize + blockSize;

                    if (e >= oldPDA.nHap)
                    {
                        e = oldPDA.nHap;
                    }

                    int prvLowM_Zero = -1;
                    int prvLowM_One = -1;
                    int hID = oldPDA.pArr[s];
                    int seakHID;


                    #region first block
                    if (s == 0)
                    {
                        prvLowM_Zero = -1;
                        prvLowM_One = -1;
                        hID = oldPDA.pArr[s];

                        for (int k = s; k < e; k++)
                        {

                            hID = oldPDA.pArr[k];

                            //incoming is 0
                            if (site[hID] == false)
                            {
                                newPDA.mLens[hID] = Math.Min(oldPDA.mLens[hID], prvLowM_One) + 1;
                                prvLowM_One = int.MaxValue;
                                prvLowM_Zero = Math.Min(prvLowM_Zero, oldPDA.mLens[hID]);
                            }
                            else//incoming is 1
                            {
                                newPDA.mLens[hID] = Math.Min(oldPDA.mLens[hID], prvLowM_Zero) + 1;
                                prvLowM_Zero = int.MaxValue;
                                prvLowM_One = Math.Min(prvLowM_One, oldPDA.mLens[hID]);
                            }
                        }


                    }
                    #endregion

                    #region other blocks
                    else
                    {

                        //look up to set prvLowM_One and prvLowM_Zero
                        prvLowM_Zero = -1;
                        prvLowM_One = -1;

                        bool minZeroSearch = true;
                        bool minOneSearch = true;

                        //special cases:
                        //A there is no 0 in upper blocks
                        //B there is no 1 in uppper blocks
                        if (psHolder[s - 1] == s)//A there is no 0 in upper blocks
                        {
                            prvLowM_Zero = -1;
                            minZeroSearch = false;
                        }

                        if (psHolder[s - 1] == 0) //B there is no 1 in uppper blocks
                        {
                            prvLowM_One = -1;
                            minOneSearch = false;
                        }

                        //regular search

                        if (minZeroSearch)
                        {

                            int seakIndex = s - 1;
                            //locate first upper zero
                            //seakHID = oldPDA.pArr[seakIndex];
                            while (seakIndex >= 0 && site[oldPDA.pArr[seakIndex]] != false)
                            {
                                seakIndex--;
                            }
                            if (seakIndex >= 0)
                            {
                                prvLowM_Zero = oldPDA.mLens[oldPDA.pArr[seakIndex]];
                                while (seakIndex >= 0 && site[oldPDA.pArr[seakIndex]] == false)
                                {
                                    prvLowM_Zero = Math.Min(oldPDA.mLens[oldPDA.pArr[seakIndex]], prvLowM_Zero);
                                    seakIndex--;
                                }

                                if (seakIndex == -1)
                                {
                                    prvLowM_Zero = -1;
                                }
                            }

                        }


                        if (minOneSearch)
                        {
                            int seakIndex = s - 1;
                            //locate first upper one
                            while (seakIndex >= 0 && site[oldPDA.pArr[seakIndex]] != true)
                            {
                                seakIndex--;
                            }
                            if (seakIndex >= 0)
                            {
                                prvLowM_One = oldPDA.mLens[oldPDA.pArr[seakIndex]];
                                while (seakIndex >= 0 && site[oldPDA.pArr[seakIndex]] == true)
                                {
                                    prvLowM_One = Math.Min(oldPDA.mLens[oldPDA.pArr[seakIndex]], prvLowM_One);
                                    seakIndex--;
                                }

                                if (seakIndex == -1)
                                {
                                    prvLowM_One = -1;
                                }
                            }
                        }

                        if (site[oldPDA.pArr[s - 1]] == false)
                        {
                            prvLowM_One = int.MaxValue;
                        }
                        else
                        {
                            prvLowM_Zero = int.MaxValue;
                        }
                        //first row 
                        if (site[oldPDA.pArr[s - 1]] == site[oldPDA.pArr[s]])
                        {
                            //match continue
                            //incoming is 0
                            if (site[hID] == false)
                            {
                                newPDA.mLens[hID] = oldPDA.mLens[hID] + 1;
                                prvLowM_One = int.MaxValue;
                                prvLowM_Zero = Math.Min(prvLowM_Zero, oldPDA.mLens[hID]);
                            }
                            else//incoming is 1
                            {
                                newPDA.mLens[hID] = oldPDA.mLens[hID] + 1;
                                prvLowM_Zero = int.MaxValue;
                                prvLowM_One = Math.Min(prvLowM_One, oldPDA.mLens[hID]);
                            }

                        }
                        else
                        {
                            //No match
                            //incoming is 0
                            if (site[hID] == false)
                            {
                                newPDA.mLens[hID] = Math.Min(prvLowM_One, oldPDA.mLens[hID]) + 1;
                                prvLowM_One = int.MaxValue;
                                prvLowM_Zero = Math.Min(prvLowM_Zero, oldPDA.mLens[hID]);
                            }
                            else//incoming is 1
                            {
                                newPDA.mLens[hID] = Math.Min(prvLowM_Zero, oldPDA.mLens[hID]) + 1;
                                prvLowM_Zero = int.MaxValue;
                                prvLowM_One = Math.Min(prvLowM_One, oldPDA.mLens[hID]);
                            }
                        }


                        for (int k = s + 1; k < e; k++)
                        {

                            hID = oldPDA.pArr[k];

                            //incoming is 0
                            if (site[hID] == false)
                            {
                                newPDA.mLens[hID] = Math.Min(oldPDA.mLens[hID], prvLowM_One) + 1;
                                prvLowM_One = int.MaxValue;
                                prvLowM_Zero = Math.Min(prvLowM_Zero, oldPDA.mLens[hID]);


                            }
                            else//incoming is 1
                            {
                                newPDA.mLens[hID] = Math.Min(oldPDA.mLens[hID], prvLowM_Zero) + 1;
                                prvLowM_Zero = int.MaxValue;
                                prvLowM_One = Math.Min(prvLowM_One, oldPDA.mLens[hID]);
                            }
                        }
                    }
                    #endregion

                    //}
                });


            }



            public void BiCoreLM(int upIndex, int downIndex)
            {

                if (!(upIndex < downIndex))
                { return; }
                //Console.WriteLine("-> " + upIndex + " " + downIndex);
                int up, down;
                bool hasOne = false;
                bool hasZero = false;
                PDA pda = LM_CurrPDA;
                int len = LM_Len;
                BitArray site = LM_Site;

                int topIndex = upIndex;
                if (downIndex - upIndex <= Program.minBlockSize_Recur)
                {//small block no recursive

                    for (int h = upIndex; h <= downIndex; h++)
                    {
                        if (pda.mLens[pda.pArr[h]] < len)
                        {
                            if (hasOne && hasZero)
                            {
                                BiReportLM(topIndex, h, pda, site);
                            }
                            hasZero = false;
                            hasOne = false;
                            topIndex = h;
                        }

                        if (site == null)
                        {
                            hasOne = true;
                            hasZero = true;
                        }
                        else
                        {
                            if (site[pda.pArr[h]] == true)
                            {
                                hasOne = true;
                            }
                            else
                            {
                                hasZero = true;
                            }
                        }

                    }
                    //Interlocked.Add(ref nHapCovered, downIndex - upIndex);
                    return;
                }


                int mid = (upIndex + downIndex) / 2;

                int upRange, downRange;
                if (pda.mLens[pda.pArr[mid]] >= len)
                {//in the block
                    up = mid - 1;
                    while (up >= 0 && pda.mLens[pda.pArr[up]] >= len)
                    { up--; }

                    down = mid;
                    while (down + 1 < pda.nHap && pda.mLens[pda.pArr[down + 1]] >= len)
                    { down++; }

                    //recursive calls

                    //Thread t1 = new Thread(() => { BiCoreLM(upIndex, up - 1); });
                    //Thread t2 = new Thread(() => { BiCoreLM(down + 1, downIndex); });
                    //t1.Start();
                    //t2.Start();
                    //t1.Join();
                    //t2.Join();

                    //LM_Queue.Add(new LMQ_Item(upIndex, up - 1));
                    //LM_Queue.Add(new LMQ_Item(down + 1, downIndex));

                    //report matches
                    BiReportLM(up, down, pda, site);
                    BiCoreLM(upIndex, up - 1);
                    BiCoreLM(down + 1, downIndex);


                    //Interlocked.Add(ref nHapCovered, down - up);
                }
                else
                {//out of the block
                    up = mid - 1;
                    upRange = mid - LM_SearchRange;
                    downRange = mid + LM_SearchRange;
                    while (up >= 0 && pda.mLens[pda.pArr[up]] < len && up >= upRange)
                    { up--; }

                    down = mid;
                    while (down + 1 < pda.nHap && pda.mLens[pda.pArr[down + 1]] < len && down + 1 <= downRange)
                    { down++; }


                    //Interlocked.Add(ref nHapCovered, down - up);
                    //recursive calls

                    //Thread t1 = new Thread(() => { BiCoreLM(upIndex, up); });
                    //Thread t2 = new Thread(() => { BiCoreLM(down, downIndex); });
                    //t1.Start();
                    //t2.Start();
                    //t1.Join();
                    //t2.Join();

                    //LM_Queue.Add(new LMQ_Item(upIndex, up));
                    //LM_Queue.Add(new LMQ_Item(down, downIndex));

                    BiCoreLM(upIndex, up);
                    BiCoreLM(down, downIndex);

                }

            }


            public void BiReportLM(int up, int down, PDA pda, BitArray site)
            {
                Parallel.For(up, down - 1, (i) =>
                //for (int i = up; i < down - 1; i++)
                {
                    int minDV = int.MaxValue;
                    for (int k = i + 1; k <= down; k++)
                    {
                        minDV = Math.Min(minDV, pda.mLens[pda.pArr[k]]);

                        if (site == null || (site[i] != site[k]))
                        {
                            //report match
                            Program.LM_Collection[pda.pArr[i] % Program.LMC_Size] += pda.pArr[k] + minDV;
                        }
                    }
                }
                );


            }

            public void Blk_ReportLM(int up, int down, int len, PDA pda, BitArray site, bool lastBlock = false)
            {

                bool hasZero = false;
                bool hasOne = false;
                int topIndex = up;
                //int minVal = int.MaxValue;

                //need to consider special case: last site

                for (int h = up; h <= down; h++)
                {
                    if (pda.mLens[pda.pArr[h]] < len)
                    {
                        if (hasZero && hasOne)
                        {
                            Parallel.For(topIndex, h - 1, LM_Report_pop, (i) =>
                            //for (int i = topIndex; i < h - 1; i++)
                            {
                                int minVal = int.MaxValue;
                                for (int k = i + 1; k < h; k++)
                                {
                                    minVal = Math.Min(minVal, pda.mLens[pda.pArr[k]]);

                                    if (site == null || (site[pda.pArr[i]] != site[pda.pArr[k]]))
                                    {
                                        Program.LM_Collection[pda.pArr[i] % Program.LMC_Size] += pda.pArr[k] + minVal;
                                    }

                                }
                            }
                            );

                        }

                        hasOne = false;
                        hasZero = false;
                        //zeroCnt = 0;
                        //oneCnt = 0;
                        topIndex = h;
                    }

                    if (site != null)
                    {
                        if (site[pda.pArr[h]] == true)
                        {
                            hasOne = true;
                        }
                        else
                        {
                            hasZero = true;
                        }
                    }
                    else
                    {
                        hasOne = true;
                        hasZero = true;
                    }



                }

                if (lastBlock == false)
                { return; }
                //this needs to be moved
                //tail case
                if (hasOne && hasZero)
                {
                    Parallel.For(topIndex, pda.pArr.Count(), LM_Report_pop, (i) =>
                    //for (int i = topIndex; i < pda.pArr.Count(); i++)
                    {
                        int minVal = int.MaxValue;
                        for (int k = i + 1; k < pda.pArr.Count(); k++)
                        {
                            minVal = Math.Min(minVal, pda.mLens[pda.pArr[k]]);

                            if (site == null || (site[pda.pArr[i]] != site[pda.pArr[k]]))
                            {
                                Program.LM_Collection[pda.pArr[i] % Program.LMC_Size] += pda.pArr[k] + minVal;
                            }

                        }
                    }
                    );

                }







            }

            public void Blk_Case_1xx1(int up, int down, int len, PDA pda, BitArray site)
            {

                if (pda.mLens[pda.pArr[up - 1]] < len)
                {
                    Blk_Case_0xx1(up - 1, down, len, pda, site);
                }
            }

            public void Blk_Case_1xx0(int up, int down, int len, PDA pda, BitArray site)
            {
                if (pda.mLens[pda.pArr[up - 1]] < len)
                {
                    Blk_Case_0xx0(up - 1, down, len, pda, site);
                }

            }

            public void Blk_Case_0xx0(int up, int down, int len, PDA pda, BitArray site)
            {
                Blk_ReportLM(up, down, len, pda, site);
            }

            public void Blk_Case_0xx1(int up, int down, int len, PDA pda, BitArray site)
            {
                if (down == pda.nHap - 1)
                {
                    Blk_ReportLM(up, down, len, pda, site, true);
                }
                else if (pda.mLens[pda.pArr[down + 1]] < len)
                {
                    Blk_ReportLM(up, down, len, pda, site);
                }
                else
                {//extend down
                    int newDown = down + LM_BlockSize;
                    while (newDown < pda.nHap - 1 && pda.mLens[pda.pArr[newDown]] >= len)
                    {
                        newDown += LM_BlockSize;
                    }

                    if (newDown > pda.nHap - 1)
                    {
                        newDown = pda.nHap - 1;
                    }

                    if (newDown == pda.nHap - 1)
                    {
                        Blk_ReportLM(up, newDown, len, pda, site, true);
                    }
                    else
                    {
                        Blk_ReportLM(up, newDown, len, pda, site);
                    }
                }
            }

            public void ReportLongMatches(int len, PDA pda, int siteIndex, List<BitArray> sites)
            {
                BitArray site;
                if (siteIndex < sites.Count() - 1)
                {
                    site = sites[siteIndex + 1];
                }
                else
                {
                    site = null;
                }



                LM_BlockSize = Convert.ToInt32(Math.Ceiling(Convert.ToDouble(pda.nHap) / Convert.ToDouble(LM_Blk_nThread)));

                //for (int b = 0; b < nThread; b++)
                Parallel.For(0, LM_Blk_nThread, (b) =>
                {
                    int s = b * LM_BlockSize;
                    if (s >= pda.nHap)
                    { return; }

                    int e = s + LM_BlockSize - 1;

                    if (e >= pda.nHap)
                    {
                        e = pda.nHap - 1;
                    }


                    if (pda.mLens[pda.pArr[s]] < len && pda.mLens[pda.pArr[e]] < len)
                    {
                        Blk_Case_0xx0(s, e, len, pda, site);
                    }
                    else if (pda.mLens[pda.pArr[s]] >= len && pda.mLens[pda.pArr[e]] >= len)
                    {
                        Blk_Case_1xx1(s, e, len, pda, site);
                    }
                    else if (pda.mLens[pda.pArr[s]] >= len && pda.mLens[pda.pArr[e]] < len)
                    {
                        Blk_Case_1xx0(s, e, len, pda, site);
                    }
                    else
                    {
                        Blk_Case_0xx1(s, e, len, pda, site);
                    }



                }
                );


            }


            public void OneSort(PDA oldPDA, PDA newPDA, BitArray site, int[] ppsHolder, int[] offsetsHolder, int nThread, int sortedTo = 0)
            {
                coreP_Arr(oldPDA, newPDA, ppsHolder, offsetsHolder, site, nThread);

                coreD_Arr(oldPDA, newPDA, ppsHolder, site, nThread, sortedTo);

            }




            public PDA Run(PDA initPDA, List<BitArray> sites, int matchLength)
            {
                LM_Report_pop.MaxDegreeOfParallelism = LM_Report_nThread;
                oldPDA = initPDA;

                Random rnd = new Random((int)DateTime.Now.Ticks);
                int rIndex;


                LM_nHap = initPDA.nHap;


                for (int i = 1; i < sites.Count() - 1; i++)
                {

                    //Console.WriteLine(DateTime.Now.ToString() + " Site " + i);

                    OneSort(oldPDA, newPDA, sites[i], ppsHolder, offsetsHolder, nThread, i);


                    if (matchLength > 0 && i >= matchLength)
                    {
                        ReportLongMatches(matchLength, newPDA, i, sites);
                    }

                    temPDA = oldPDA;
                    oldPDA = newPDA;
                    newPDA = temPDA;



                }
                return newPDA;




            }

        }
    }
}
