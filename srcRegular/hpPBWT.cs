/*
Author: Kecong Tang (Benny) at University of Central Florida. 

Core HP-PBWT modules, has my personally implemented sequential PBWT.


*/


using System;
using System.Collections.Concurrent;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

namespace HP_PBWT_Reg
{
    public class hpPBWT
    {

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



            public void Blk_ReportLM(int up, int down, int len, PDA pda, BitArray site, int siteIndex, bool lastBlock = false)
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
                            //Parallel.For(topIndex,h-1, LM_Report_pop ,(i)=>
                            for (int i = topIndex; i < h - 1; i++)
                            {
                                int minVal = int.MaxValue;
                                for (int k = i + 1; k < h; k++)
                                {
                                    minVal = Math.Min(minVal, pda.mLens[pda.pArr[k]]);

                                    if (site == null || (site[pda.pArr[i]] != site[pda.pArr[k]]))
                                    {
                                        //Program.LM_Collection[pda.pArr[i]%Program.LMC_Size] += pda.pArr[k] + minVal;
                                        Program.aw.Add(pda.pArr[i] + "\t" + pda.pArr[k] + "\t" + minVal + "\t" + siteIndex);
                                        //Program.LM_SW.WriteLine(pda.pArr[i] + "\t" + pda.pArr[k] + "\t"+minVal+"\t"+siteIndex);
                                    }

                                }
                            }
                            //);

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

                                Program.aw.Add(pda.pArr[i] + "\t" + pda.pArr[k] + "\t" + minVal + "\t" + siteIndex);
                            }

                        }
                    }
                    );

                }


            }

            public void Blk_Case_1xx1(int up, int down, int len, PDA pda, BitArray site, int siteIndex)
            {

                if (pda.mLens[pda.pArr[up - 1]] < len)
                {
                    Blk_Case_0xx1(up - 1, down, len, pda, site, siteIndex);
                }
            }

            public void Blk_Case_1xx0(int up, int down, int len, PDA pda, BitArray site, int siteIndex)
            {
                if (pda.mLens[pda.pArr[up - 1]] < len)
                {
                    Blk_Case_0xx0(up - 1, down, len, pda, site, siteIndex);
                }

            }

            public void Blk_Case_0xx0(int up, int down, int len, PDA pda, BitArray site, int siteIndex)
            {
                Blk_ReportLM(up, down, len, pda, site, siteIndex);
            }

            public void Blk_Case_0xx1(int up, int down, int len, PDA pda, BitArray site, int siteIndex)
            {
                if (down == pda.nHap - 1)
                {
                    Blk_ReportLM(up, down, len, pda, site, siteIndex, true);
                }
                else if (pda.mLens[pda.pArr[down + 1]] < len)
                {
                    Blk_ReportLM(up, down, len, pda, site, siteIndex);
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
                        Blk_ReportLM(up, newDown, len, pda, site, siteIndex, true);
                    }
                    else
                    {
                        Blk_ReportLM(up, newDown, len, pda, site, siteIndex);
                    }
                }
            }
            public void ReportLongMatches(int len, PDA pda, BitArray site, int siteIndex)
            {


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
                        Blk_Case_0xx0(s, e, len, pda, site, siteIndex);
                    }
                    else if (pda.mLens[pda.pArr[s]] >= len && pda.mLens[pda.pArr[e]] >= len)
                    {
                        Blk_Case_1xx1(s, e, len, pda, site, siteIndex);
                    }
                    else if (pda.mLens[pda.pArr[s]] >= len && pda.mLens[pda.pArr[e]] < len)
                    {
                        Blk_Case_1xx0(s, e, len, pda, site, siteIndex);
                    }
                    else
                    {
                        Blk_Case_0xx1(s, e, len, pda, site, siteIndex);
                    }



                }
                );


            }

            public void ReportSetMaxMatches(PDA pda, int siteIndex, BitArray site)
            {

                ConcurrentBag<string> results = new ConcurrentBag<string>();

  
                int nHap = Program.nTotalHap;

       
                //edge case
                if (site == null)
                {
                    Parallel.For(0, nHap, pOp, (oneChosenIndex) =>
                    //foreach (int oneChosenIndex in chosens)
                    {
                        int rangeUp = oneChosenIndex, rangeDown = oneChosenIndex;
                        int currHID = pda.pArr[oneChosenIndex];
                        int len;
                        int scanIndex;


                        if (oneChosenIndex == 0)
                        {
                            len = pda.mLens[pda.pArr[oneChosenIndex + 1]];
                        }
                        else if (oneChosenIndex == nHap - 1)
                        {
                            len = pda.mLens[pda.pArr[oneChosenIndex]];
                        }
                        else
                        {
                            len = Math.Max(pda.mLens[pda.pArr[oneChosenIndex]], pda.mLens[pda.pArr[oneChosenIndex + 1]]);
                        }

                        //check down
                        if (oneChosenIndex != nHap - 1)
                        {
                            scanIndex = oneChosenIndex + 1;

                            //scanIndex++;
                            while (scanIndex < nHap && len <= pda.mLens[pda.pArr[scanIndex]])
                            {
                                scanIndex++;
                            }
                            rangeDown = scanIndex - 1;
                        }

                        if (oneChosenIndex != 0)
                        {//check up 
                            scanIndex = oneChosenIndex - 1;

                            //scanIndex--;
                            while (scanIndex >= 0 && len <= pda.mLens[pda.pArr[scanIndex + 1]])
                            {

                                scanIndex--;
                            }

                            rangeUp = scanIndex + 1;
                        }

                        //report 

                        for (int i = rangeUp; i <= rangeDown; i++)
                        {
                            if (i == oneChosenIndex)
                            { continue; }
                            Program.aw.Add(currHID.ToString() + " " + pda.pArr[i].ToString() + " " + siteIndex.ToString() + " " + len.ToString());
                            //results.SMM_Add_ByLen(currHID, pda.pArr[i], siteIndex, len);
                            
                        }
                    });

                    return;
                }

                //todo parallel
                Parallel.For(0, nHap, pOp, (oneChosenIndex) =>
                //foreach (int oneChosenIndex in chosens)
                {
                    int rangeUp = oneChosenIndex, rangeDown = oneChosenIndex;
                    int currHID = pda.pArr[oneChosenIndex];
                    int len;
                    bool val = site[currHID];
                    int scanIndex;

                    bool runThough = false;


                    if (oneChosenIndex == 0)
                    {
                        len = pda.mLens[pda.pArr[oneChosenIndex + 1]];
                    }
                    else if (oneChosenIndex == site.Count - 1)
                    {
                        len = pda.mLens[pda.pArr[oneChosenIndex]];
                    }
                    else
                    {
                        len = Math.Max(pda.mLens[pda.pArr[oneChosenIndex]], pda.mLens[pda.pArr[oneChosenIndex + 1]]);
                    }

                    //check down
                    if (oneChosenIndex != site.Count - 1)
                    {
                        scanIndex = oneChosenIndex + 1;

                        //scanIndex++;
                        while (scanIndex < site.Count && len <= pda.mLens[pda.pArr[scanIndex]])
                        {
                            if (val == site[pda.pArr[scanIndex]])
                            {
                                runThough = true;
                                break;
                            }
                            scanIndex++;
                        }
                        rangeDown = scanIndex - 1;
                    }

                    if (runThough == true)
                    { return; }


                    if (oneChosenIndex != 0)
                    {//check up 
                        scanIndex = oneChosenIndex - 1;

                        //scanIndex--;
                        while (scanIndex >= 0
                            //&& val != site[pda.pArr[scanIndex]]
                            && len <= pda.mLens[pda.pArr[scanIndex + 1]])
                        {
                            if (val == site[pda.pArr[scanIndex]])
                            {
                                runThough = true;
                                break;
                            }
                            scanIndex--;
                        }

                        rangeUp = scanIndex + 1;
                    }

                    if (runThough == true)
                    { return; }

                    //report 

                    for (int i = rangeUp; i <= rangeDown; i++)
                    {
                        if (i == oneChosenIndex)
                        { continue; }
                        //results.SMM_Add_ByLen(currHID, pda.pArr[i], siteIndex, len);
                        //results.Add(currHID.ToString() + " " + pda.pArr[i].ToString() + " " + siteIndex.ToString() + " " + len.ToString());
                        Program.aw.Add(currHID.ToString() + " " + pda.pArr[i].ToString() + " " + siteIndex.ToString() + " " + len.ToString());
                    }
                });

         
                return;
            }

            public void OneSort(PDA oldPDA, PDA newPDA, BitArray site, int[] ppsHolder, int[] offsetsHolder, int nThread, int sortedTo = 0)
            {
                coreP_Arr(oldPDA, newPDA, ppsHolder, offsetsHolder, site, nThread);

                coreD_Arr(oldPDA, newPDA, ppsHolder, site, nThread, sortedTo);

            }




            public PDA Run(PDA initPDA, utl.VCF_ReaderLight reader, int matchLength)
            {
                LM_Report_pop.MaxDegreeOfParallelism = LM_Report_nThread;
                oldPDA = initPDA;

                LM_nHap = initPDA.nHap;



                BitArray site = reader.ReadLine();
                int siteCnt = 1;

                Program.aw = new utl.AsyncWriter(Program.outPath);

                while (site != null)
                {
                    //Console.WriteLine(DateTime.Now.ToString() + " Site " + siteCnt);

                    OneSort(oldPDA, newPDA, site, ppsHolder, offsetsHolder, nThread, siteCnt);

                    site = reader.ReadLine();

                    siteCnt++;

                    if (matchLength > 0)
                    {
                        if (siteCnt >= matchLength)
                        {
                            ReportLongMatches(matchLength, newPDA, site,siteCnt);
                        }
                    }
                    else
                    {
                        ReportSetMaxMatches(newPDA, siteCnt, site);
                    }

                    temPDA = oldPDA;
                    oldPDA = newPDA;
                    newPDA = temPDA;



                }
                LM_WorkingSite = -1;

                Program.aw.DoneAdding_WaitWriter();



                return newPDA;




            }

        }
    }
}
