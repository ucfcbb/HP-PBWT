/*
Author: Kecong Tang (Benny) at University of Central Florida. 

My supporting utility module, contains commonly used and reusable functions in some other projects.


*/




using System;
using System.Collections.Concurrent;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HP_PBWT_Reg
{
    public class utl
    {
        public class VCF_ReaderLight
        {
            public int lineRead_BLK_Size = 1024;//64 has 32 hap
            public int nHap_BLK;
            public int nHap = 0;
            public BitArray site;
            StreamReader sr;
            public VCF_ReaderLight(string VCF_Path)
            {
                nHap_BLK = lineRead_BLK_Size / 2;
                sr = new StreamReader(VCF_Path);
                //top headers
                string line;
                string[] parts;
                while ((line = sr.ReadLine()) != null && line.StartsWith("##"))
                {
                    continue;
                }

                parts = line.Split('\t');

                nHap = (parts.Count() - 9) * 2;

                site = new BitArray(nHap);

            }

            public BitArray ReadLine()
            {
                string line;
                if ((line = sr.ReadLine()) == null)
                {
                    return null;
                }

                int offSet = indexOf_nTH_Char(line, 9, '\t');

                int nBlk = Convert.ToInt32(Math.Ceiling(Convert.ToDouble(line.Length - offSet) / Convert.ToDouble(lineRead_BLK_Size)));


                int off = 0;
                for (int i = offSet; i < line.Length; i++)
                {
                    if (line[i] == '1')
                    {
                        site[off] = true;
                        off++;
                    }
                    else if (line[i] == '0')
                    {
                        site[off] = false;
                        off++;
                    }
                    else
                    {
                        continue;
                    }
                }




                return site;
            }

            public void Close()
            {

                sr.Close();
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

        public class AsyncWriter
        {
            Task writerTask;
            ConcurrentQueue<string> buffer = new ConcurrentQueue<string>();
            StreamWriter sw;
            int msWriterWait = 10;
            bool doneAdding = false;
            public AsyncWriter(string outPath)
            {
                sw = new StreamWriter(outPath);
                writerTask = Task.Factory.StartNew(() => runWriter());
            }

            public void Add(string line)
            {
                buffer.Enqueue(line);

            }

            void runWriter()
            {
                string outStr;
                while (doneAdding == false || buffer.IsEmpty == false)
                {

                    if (buffer.TryDequeue(out outStr))
                    {
                        sw.WriteLine(outStr);
                    }
                    else
                    {
                        System.Threading.Thread.Sleep(msWriterWait);
                    }
                }

                sw.Close();
                buffer.Clear();
            }

            public void DoneAdding_WaitWriter()
            {
                doneAdding = true;
                writerTask.Wait();
            }
        }
    }
}
