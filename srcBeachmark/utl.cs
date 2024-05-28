/*
Author: Kecong Tang (Benny) at University of Central Florida. 

My supporting utility module, contains commonly used and reusable functions in some other projects.

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

namespace HP_PBWT_BM
{
    internal class utl
    {
        public class VCF_MemV9
        {
            class queueItem
            {
                public int seqIndex = 0;
                public string line = "";
                public queueItem(int RowOrder, string content)
                {
                    seqIndex = RowOrder;
                    line = content;
                }
                public queueItem()
                { }
            }
            ConcurrentDictionary<int, bool> parserPool = new ConcurrentDictionary<int, bool>();
            BlockingCollection<queueItem> procQueue;

            //each BitArray is a site
            public List<BitArray> panel_BitArr = new List<BitArray>();
            public Dictionary<string, int> indvID_To_IndexID = new Dictionary<string, int>();

            List<string> IndexID_To_Indv = new List<string>();
            public ConcurrentDictionary<int, int> phy_To_SiteID = new ConcurrentDictionary<int, int>();
            public List<int> siteIndex_To_Phy = new List<int>();
            //string inFileName = "";
            public List<string> topHeaders = new List<string>();
            public string indvTagRow = "";
            public List<string> rowHeaders = new List<string>();
            int msParserWait = 100;
            int msReaderWait = 1000;
            public int nIndv = 0;
            string vcf_Path = "";
            int scale = 0;
            public int nSite = 0;
            int nParser = 10;
            List<double> alleleSum = new List<double>();
            List<bool> rareAllele = new List<bool>();
            public List<double> MAF = new List<double>();
            bool is01StrFile = false;

            public VCF_MemV9(string path, int siteCount = 0,
                int displayScale = 10000, int nParser_Thread = 10, int BC_SizeLimit = 50000, bool use01strFile = false)
            {
                is01StrFile = use01strFile;
                vcf_Path = path;
                scale = displayScale;
                nSite = siteCount;
                nParser = nParser_Thread;
                procQueue = new BlockingCollection<queueItem>(BC_SizeLimit);
                Console.WriteLine("Initializing Container...");
                if (nSite <= 0)
                {
                    Console.WriteLine("# Site is Not Given, Counting...");
                    ulong tem = lineCount(vcf_Path, "#");
                    if (tem > int.MaxValue)
                    {
                        Console.WriteLine("File has too many sites!");
                        Console.ReadKey();
                        Environment.Exit(1);
                    }
                    nSite = Convert.ToInt32(tem);

                }
                string line;
                string[] parts;

                if (String.IsNullOrWhiteSpace(vcf_Path) == false)
                {
                    StreamReader sr = new StreamReader(vcf_Path);
                    //top headers
                    while ((line = sr.ReadLine()) != null && line.StartsWith("##"))
                    {
                        topHeaders.Add(line);
                    }
                    string arrType = "Bit Array";

                    if (is01StrFile == false)
                    {
                        //process indv header
                        indvTagRow = line;
                        parts = line.Split('\t');

                        nIndv = parts.Count() - 9;
                        int indexID = 0;
                        for (int i = 9; i < parts.Count(); i++)
                        {
                            indvID_To_IndexID.Add(parts[i], indexID);
                            IndexID_To_Indv.Add(parts[i]);
                            indexID += 2;
                        }
                    }
                    else
                    {
                        nIndv = line.Length / 2;
                    }
                    Console.WriteLine(vcf_Path + " Allocating Space, " + arrType + " " + (nIndv * 2) + " x " + nSite + "...");




                    for (int i = 0; i < nSite; i++)
                    {
                        panel_BitArr.Add(new BitArray(nIndv * 2));
                        alleleSum.Add(0);
                        siteIndex_To_Phy.Add(0);
                        rowHeaders.Add("");
                        rareAllele.Add(false);
                    }



                    sr.Close();
                }


            }

            public bool RareAllele(int siteIndex)
            {
                return rareAllele[siteIndex];

            }




            public List<double> Get_Maf()
            {
                if (MAF.Count() != 0)
                {
                    return MAF;
                }


                List<double> res = new List<double>();


                foreach (double one in alleleSum)
                {
                    res.Add(0);

                }

                Parallel.For(0, alleleSum.Count(), (i) =>
                {
                    double one = alleleSum[i];
                    if (one > nIndv)
                    {
                        res[i] = (1 - one / nIndv / 2);
                    }
                    else
                    {
                        res[i] = (one / nIndv / 2);
                    }
                });


                MAF = res;
                return res;
            }

            public void SetSite_BySIndex(int sIndex, BitArray siteContent)
            {
                for (int h = 0; h < siteContent.Length; h++)
                {
                    panel_BitArr[sIndex][h] = siteContent[h];
                }
            }
            public void SetSite_ByPos(int Pos, BitArray siteContent)
            {
                int sIndex = GetSiteIndex_ByPhy(Pos);
                SetSite_BySIndex(sIndex, siteContent);
            }

            public void SetSite_ByPos(string Pos, BitArray siteContent)
            {
                int sIndex = GetSiteIndex_ByPhy(Convert.ToInt32(Pos));
                SetSite_BySIndex(sIndex, siteContent);
            }


            /// <summary>
            /// allele sum are updated during regular reading
            /// if the panel is created by join 2 VCF_Mem(the special way)
            /// 
            /// calling this method will update the AlleleSum
            /// If the panel is created by the second way, this method should be called before Get_Maf
            /// </summary>
            public void Update_AlleleSum()
            {
                for (int i = 0; i < nSite; i++)
                {
                    alleleSum.Add(0);
                }
                Parallel.For(0, nSite, (s) =>
                {
                    for (int h = 0; h < nIndv * 2; h++)
                    {
                        if (panel_BitArr[s][h] == true)
                        {
                            alleleSum[s]++;
                        }
                    }
                });

            }

            public string Get_SiteStr_BySiteIndex(int siteIndex)
            {
                StringBuilder sb = new StringBuilder();
                sb.Append(rowHeaders[siteIndex]);
                int hID = 0;

                for (int i = 0; i < nIndv; i++)
                {
                    hID = i * 2;
                    sb.Append(bool_To_01char(panel_BitArr[siteIndex][hID]));
                    sb.Append("|");
                    hID++;
                    sb.Append(bool_To_01char(panel_BitArr[siteIndex][hID]));
                    sb.Append("\t");
                }
                sb.Length--;

                return sb.ToString();
            }


            public void WriteArrayFile(string outPath)
            {
                StreamWriter sw = new StreamWriter(outPath);

                sw.NewLine = "\n";

                StringBuilder sb = new StringBuilder();
                for (int s = 0; s < panel_BitArr.Count(); s++)
                {
                    for (int h = 0; h < panel_BitArr[s].Count; h++)
                    {

                        sb.Append(bool_To_01char(panel_BitArr[s][h]));
                        sb.Append('\t');

                    }
                    sb.Length--;
                    sw.WriteLine(sb.ToString());
                    sb.Clear();
                }

                sw.Close();
            }

            public bool Get_Cell(int siteIndex, int hapIndex)
            {
                return panel_BitArr[siteIndex][hapIndex];

            }



            public void WriteAlleleSum(string outPath)
            {
                StreamWriter sw = new StreamWriter(outPath);
                for (int i = 0; i < nSite; i++)
                {
                    sw.WriteLine(siteIndex_To_Phy[i] + "\t" + alleleSum[i]);
                }

                sw.Close();
            }

            public List<int> Get_RAC()
            {
                int nHap = nIndv * 2;
                List<int> res = new List<int>();
                for (int i = 0; i < alleleSum.Count(); i++)
                {
                    if (alleleSum[i] > nIndv)
                    {
                        res.Add(Convert.ToInt32(nHap - alleleSum[i]));
                    }
                    else
                    {
                        res.Add(Convert.ToInt32(alleleSum[i]));
                    }
                }

                return res;
            }


            //indexers 
            public BitArray GetSite_BySiteIndex(int siteIndex)
            {
                return panel_BitArr[siteIndex];
            }

            public BitArray GetSite_ByPhy(int phyLocation)
            {
                return panel_BitArr[phy_To_SiteID[phyLocation]];
            }

            public int Try_GetSiteIndex_ByPhy(int phyLocation)
            {
                if (phy_To_SiteID.ContainsKey(phyLocation))
                {
                    return phy_To_SiteID[phyLocation];
                }
                else
                {
                    return -1;
                }
            }

            public int GetSiteIndex_ByPhy(int phyLocation)
            {
                return phy_To_SiteID[phyLocation];

            }




            public BitArray GetHaplotype(string indvTag, bool haplotypeID)
            {
                BitArray hap = new BitArray(phy_To_SiteID.Keys.Count());
                int col_Index = indvID_To_IndexID[indvTag];
                if (haplotypeID == true)
                {
                    col_Index++;
                }
                for (int s = 0; s < panel_BitArr.Count(); s++)
                {
                    hap[s] = panel_BitArr[s][col_Index];
                }

                return hap;
            }

            public List<string> GetAll_Indv()
            {
                return indvID_To_IndexID.Keys.ToList();
            }

            /// <summary>
            /// one count for each site
            /// </summary>
            /// <returns></returns>
            public List<double> Get_AlleleSum()
            {
                return alleleSum;
            }

            public List<string> Get_TopHeaders()
            {
                return topHeaders;
            }

            public string Get_IndvTagRow()
            {
                return indvTagRow;
            }

            public List<string> Get_RowHeaders()
            {
                return rowHeaders;
            }

            public int Get_nSite()
            {
                return phy_To_SiteID.Keys.Count();
            }

            public List<int> Get_All_Phy()
            {
                return siteIndex_To_Phy;
            }

            void Parser()
            {
                //Console.WriteLine(DateTime.Now + " Parser " + System.Threading.Thread.CurrentThread.ManagedThreadId + " Start.");
                parserPool.TryAdd(System.Threading.Thread.CurrentThread.ManagedThreadId, false);

                queueItem data = new queueItem();
                while (procQueue.Count() != 0 || procQueue.IsAddingCompleted == false)
                {
                    if (procQueue.TryTake(out data) == false)
                    {
                        System.Threading.Thread.Sleep(msParserWait);
                        continue;
                    }
                    string line = data.line;
                    int rowIndex = data.seqIndex;


                    string[] parts = line.Split('\t');
                    //row header
                    rowHeaders[rowIndex] = line.Substring(0, indexOf_nTH_Char(line, 9, '\t') + 1);
                    int h = 0;
                    char c = '.';
                    for (int i = 9; i < parts.Count(); i++)
                    {
                        c = parts[i][0];
                        panel_BitArr[rowIndex][h] = charToBool(c);
                        h++;
                        if (c == '1')
                        {
                            alleleSum[rowIndex]++;
                        }

                        c = parts[i][2];
                        panel_BitArr[rowIndex][h] = charToBool(c);
                        h++;
                        if (c == '1')
                        {
                            alleleSum[rowIndex]++;
                        }
                    }

                    int phy = Convert.ToInt32(parts[1]);
                    phy_To_SiteID.TryAdd(phy, rowIndex);
                    siteIndex_To_Phy[rowIndex] = phy;

                    if (alleleSum[rowIndex] > nIndv)
                    {//0 is the rare
                        rareAllele[rowIndex] = false;
                    }
                    else
                    {//1 is the rare
                        rareAllele[rowIndex] = true;
                    }
                }
                parserPool[System.Threading.Thread.CurrentThread.ManagedThreadId] = true;
                //Console.WriteLine(DateTime.Now + " Parser " + System.Threading.Thread.CurrentThread.ManagedThreadId + " End.");

            }

            void Parser_01str()
            {
                //Console.WriteLine(DateTime.Now + " Parser " + System.Threading.Thread.CurrentThread.ManagedThreadId + " Start.");
                parserPool.TryAdd(System.Threading.Thread.CurrentThread.ManagedThreadId, false);

                queueItem data = new queueItem();
                while (procQueue.Count() != 0 || procQueue.IsAddingCompleted == false)
                {
                    if (procQueue.TryTake(out data) == false)
                    {
                        System.Threading.Thread.Sleep(msParserWait);
                        continue;
                    }
                    string line = data.line;
                    int rowIndex = data.seqIndex;



                    int h = 0;

                    for (int i = 0; i < line.Length; i++)
                    {

                        panel_BitArr[rowIndex][i] = charToBool(line[i]);

                    }

                }
                parserPool[System.Threading.Thread.CurrentThread.ManagedThreadId] = true;
                //Console.WriteLine(DateTime.Now + " Parser " + System.Threading.Thread.CurrentThread.ManagedThreadId + " End.");

            }


            void Reader()
            {

                string line;

                //data
                int nSite_Read = 0;
                StreamReader sr = new StreamReader(vcf_Path);
                Console.WriteLine(DateTime.Now + " VCF_MEM: " + vcf_Path + " Reading Started.");
                //List<Task> allPaser = new List<Task>();
                if (is01StrFile == false)
                {
                    while ((line = sr.ReadLine()) != null && line.StartsWith("##"))
                    { continue; }
                }


                while ((line = sr.ReadLine()) != null && nSite_Read < nSite)
                {
                    string innerLine = line;
                    int innerSiteIndex = nSite_Read;

                    while (!procQueue.TryAdd(new queueItem(innerSiteIndex, innerLine)))
                    {
                        //Console.WriteLine(DateTime.Now+" Collection Adding Failed " + innerSiteIndex+" Waiting...");
                        System.Threading.Thread.Sleep(msReaderWait);
                    }

                    nSite_Read++;
                    if (nSite_Read % scale == 0)
                    {
                        Console.WriteLine(DateTime.Now + " " + vcf_Path + " Reader: " + nSite_Read + " / " + nSite);
                    }
                }
                sr.Close();
                procQueue.CompleteAdding();

                Console.WriteLine(DateTime.Now + " " + vcf_Path + " Reader: Read Completed.");

            }


            void updateRareAlleleDict()
            {
                int nhap = nIndv * 2;
                for (int i = 0; i < alleleSum.Count(); i++)
                {
                    if (alleleSum[i] > nIndv)
                    {//0 is the rare
                        rareAllele[i] = false;
                    }
                    else
                    {//1 is the rare
                        rareAllele[i] = true;
                    }
                }



            }


            public void Read()
            {

                //call parser

                runParsers(is01StrFile);
                //call reader
                Reader();

                waitParsers();


            }


            void waitParsers()
            {
                Console.WriteLine(DateTime.Now + " Wait for Pasers...");
                //Task.WaitAll(all_PaserTSK.ToArray());

                bool waitComplete = false;
                while (waitComplete == false)
                {
                    System.Threading.Thread.Sleep(msParserWait);
                    waitComplete = true;
                    foreach (bool done in parserPool.Values)
                    {
                        if (done == false)
                        {
                            waitComplete = false;
                            break;
                        }
                    }
                }

                Console.WriteLine(DateTime.Now + " Pasers All Finish.");
            }

            void runParsers(bool read_01strFile = false)
            {
                if (read_01strFile == false)
                {
                    for (int i = 0; i < nParser; i++)
                    {
                        Task one = Task.Factory.StartNew(() => Parser());
                    }
                }
                else
                {
                    for (int i = 0; i < nParser; i++)
                    {
                        Task one = Task.Factory.StartNew(() => Parser_01str());
                    }

                }

            }

            public int GetPhy_BySiteIndex(int siteIndex)
            {
                return siteIndex_To_Phy[siteIndex];
            }

            static char bool_To_01char(bool val)
            {
                if (val == true)
                {
                    return '1';
                }
                else
                {
                    return '0';
                }
            }

            static bool charToBool(char c)
            {
                if (c == '1')
                {
                    return true;
                }
                else if (c == '0')
                {
                    return false;
                }
                else
                {
                    Console.WriteLine("Bug! CharToBool, Not 1 or 0!");
                    return false;
                }
            }

            public ulong lineCount(string path, string skipHeader = "")
            {
                Console.WriteLine("Counting Lines...\t" + path);
                StreamReader sr = new StreamReader(path);
                string line;
                ulong cnt = 0;
                if (String.IsNullOrEmpty(skipHeader))
                {
                    while ((line = sr.ReadLine()) != null)
                    {
                        cnt++;
                    }
                }
                else
                {
                    while ((line = sr.ReadLine()) != null && line.StartsWith(skipHeader))
                    {
                        continue;
                    }
                    cnt++;
                    while ((line = sr.ReadLine()) != null)
                    {
                        cnt++;
                    }

                }
                sr.Close();
                Console.WriteLine(cnt + "\tlines");
                return cnt;
            }


            public void WriteVCF(string outPath)
            {
                StreamWriter sw = new StreamWriter(outPath);
                sw.NewLine = "\n";


                foreach (string line in topHeaders)
                {
                    sw.WriteLine(line);
                }

                sw.WriteLine(indvTagRow);

                StringBuilder sb;
                int hID = 0;
                for (int s = 0; s < nSite; s++)
                {

                    sb = new StringBuilder();
                    sb.Append(rowHeaders[s]);
                    for (int i = 0; i < nIndv; i++)
                    {
                        hID = i * 2;
                        if (panel_BitArr[s][hID] == false)
                        {
                            sb.Append('0');
                        }
                        else
                        {
                            sb.Append('1');
                        }

                        sb.Append("|");

                        hID++;
                        if (panel_BitArr[s][hID] == false)
                        {
                            sb.Append('0');
                        }
                        else
                        {
                            sb.Append('1');
                        }
                        sb.Append("\t");

                    }
                    sb.Length--;
                    sw.WriteLine(sb.ToString());

                }


                sw.Close();
            }

            public void Write_01str_File(string outPath)
            {

                StreamWriter sw = new StreamWriter(outPath);
                sw.NewLine = "\n";
                StringBuilder sb;
                int hID = 0;
                for (int s = 0; s < nSite; s++)
                {

                    sb = new StringBuilder();
                    //sb.Append(rowHeaders[s]);
                    for (int i = 0; i < nIndv * 2; i++)
                    {

                        if (panel_BitArr[s][i] == false)
                        {
                            sb.Append('0');
                        }
                        else
                        {
                            sb.Append('1');
                        }
                    }
                    //sb.Length--;
                    sw.WriteLine(sb.ToString());

                }


                sw.Close();


            }
        }


        public static List<int> randomSelect(int nSelect, int nTotal, int extraSeed = 1)
        {
            List<int> result = new List<int>();
            if (nSelect == nTotal)
            {
                for (int i = 0; i < nSelect; i++)
                {
                    result.Add(i);
                }
                return result;
            }


            Random rnd = new Random((int)(DateTime.Now.Ticks + extraSeed));
            List<double> nums = new List<double>();
            Dictionary<double, int> val_To_Index = new Dictionary<double, int>();
            double oneNum = 0;
            for (int i = 0; i < nTotal; i++)
            {
                oneNum = rnd.NextDouble();
                while (val_To_Index.ContainsKey(oneNum))
                {
                    oneNum = rnd.NextDouble();
                }
                val_To_Index.Add(oneNum, i);
                nums.Add(oneNum);
            }

            nums.Sort();

            for (int i = 0; i < nSelect; i++)
            {
                result.Add(val_To_Index[nums[i]]);
            }

            return result;
        }


        public static char bool_To_01char(bool val)
        {
            if (val == true)
            {
                return '1';
            }
            else
            {
                return '0';
            }
        }

        public static int indexOf_nTH_Char(string orgString, int n, char c)
        {
            int result = -1;
            int cnt = 0;
            for (int i = 0; i < orgString.Length; i++)
            {
                if (orgString[i] == c)
                {
                    cnt++;
                    if (cnt == n)
                    {
                        result = i;
                        break;
                    }
                }
            }
            return result;
        }
    }
}
