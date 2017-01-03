using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.IO;
using System.Threading;
using System.Drawing;

using FastDFS.Client;
namespace FastDFS
{
    class Program
    {
        static void Main(string[] args)
        {

            //===========================Initial========================================
            List<IPEndPoint> trackerIPs = new List<IPEndPoint>();
            IPEndPoint endPoint = new IPEndPoint(IPAddress.Parse("192.168.81.233"), 22122);
            trackerIPs.Add(endPoint);
            ConnectionManager.Initialize(trackerIPs);
            StorageNode node = FastDFSClient.GetStorageNode("group1");
            //===========================UploadFile=====================================
            byte[] content = null;
            if (File.Exists(@"D:\材料科学与工程基础.doc"))
            {
                FileStream streamUpload = new FileStream(@"D:\材料科学与工程基础.doc", FileMode.Open);
                using (BinaryReader reader = new BinaryReader(streamUpload))
                {
                    content = reader.ReadBytes((int)streamUpload.Length);
                }
            }
            //string fileName = FastDFSClient.UploadAppenderFile(node, content, "mdb");
            //主文件
            string fileName = FastDFSClient.UploadFile(node, content, "doc");

            //UploadFileByName
            //string fileName = FastDFSClient.UploadFileByName(node, @"D:\材料科学与工程基础.doc");

            //从文件
            string slavefileName = FastDFSClient.UploadSlaveFile("group1", content, fileName, "-part1", "doc");

            //===========================BatchUploadFile=====================================
            string[] _FileEntries = Directory.GetFiles(@"E:\fastimage\三维", "*.jpg");
            DateTime start = DateTime.Now;
            foreach (string file in _FileEntries)
            {
                string name = Path.GetFileName(file);
                content = null;
                FileStream streamUpload = new FileStream(file, FileMode.Open);
                using (BinaryReader reader = new BinaryReader(streamUpload))
                {
                    content = reader.ReadBytes((int)streamUpload.Length);
                }
                //string fileName = FastDFSClient.UploadAppenderFile(node, content, "mdb");
                fileName = FastDFSClient.UploadFile(node, content, "jpg");
            }
            DateTime end = DateTime.Now;
            TimeSpan consume = ((TimeSpan)(end - start));
            double consumeSeconds = Math.Ceiling(consume.TotalSeconds);
            //===========================QueryFile=======================================
            fileName = "M00/03/80/wKhR6VAhwA72jCDyAABYAMjfFsM288.doc";
            FDFSFileInfo fileInfo = FastDFSClient.GetFileInfo(node, fileName);
            Console.WriteLine(string.Format("FileName:{0}", fileName));
            Console.WriteLine(string.Format("FileSize:{0}", fileInfo.FileSize));
            Console.WriteLine(string.Format("CreateTime:{0}", fileInfo.CreateTime));
            Console.WriteLine(string.Format("Crc32:{0}", fileInfo.Crc32));
            //==========================AppendFile=======================================
            FastDFSClient.AppendFile("group1", fileName, content);
            FastDFSClient.AppendFile("group1", fileName, content);

            //===========================DownloadFile====================================
            fileName = "M00/00/00/wKhR6VAAAN7J2FLQAABYAMjfFsM849.doc";
            string localName = @"D:\SZdownload.doc";
            if (File.Exists(@"D:\SZdownload.doc"))
                File.Delete(@"D:\SZdownload.doc");
            if (fileInfo.FileSize >= 1024)//如果文件大小大于1KB  分次写入
            {
                FileStream fs = new FileStream(localName, FileMode.OpenOrCreate, FileAccess.Write);
                //string name_ = LocalName.Substring(LocalName.LastIndexOf("\\") + 1, LocalName.Length - LocalName.LastIndexOf("\\") - 1);
                long offset = 0;
                long len = 1024;
                while (len > 0)
                {
                    byte[] buffer = new byte[len];
                    buffer = FastDFSClient.DownloadFile(node, fileName, offset, len);
                    fs.Write(buffer, 0, int.Parse(len.ToString()));
                    fs.Flush();
                    // setrichtext(name_ + "已经下载：" + (offset / fileInfo.FileSize) + "%");
                    offset = offset + len;
                    len = (fileInfo.FileSize - offset) >= 1024 ? 1024 : (fileInfo.FileSize - offset);
                }
                fs.Close();

            }
            else//如果文件大小小小于1KB  直接写入文件
            {
                byte[] buffer = new byte[fileInfo.FileSize];
                buffer = FastDFSClient.DownloadFile(node, fileName);
                FileStream fs = new FileStream(localName, FileMode.OpenOrCreate, FileAccess.Write);
                fs.Write(buffer, 0, buffer.Length);
                fs.Flush();
                fs.Close();
            }
            //byte[] buffer = FastDFSClient.DownloadFile(node, fileName, 0L, 0L);
            //if (File.Exists(@"D:\SZdownload.mdb"))
            //    File.Delete(@"D:\SZdownload.mdb");
            //FileStream stream = new FileStream(@"D:\SZdownload.mdb", FileMode.CreateNew);
            //using (BinaryWriter write = new BinaryWriter(stream, Encoding.BigEndianUnicode))
            //{
            //    write.Write(buffer);
            //    write.Close();
            //}
            //stream.Close();
            //===========================RemoveFile=======================================
            //FastDFSClient.RemoveFile("group1", fileName);

            //===========================Http测试，流读取=======================================
            string url = "http://img13.360buyimg.com/da/g5/M02/0D/16/rBEDik_nOJ0IAAAAAAA_cbJCY-UAACrRgMhVLEAAD-J352.jpg";
            System.Net.HttpWebRequest req = (System.Net.HttpWebRequest)System.Net.HttpWebRequest.Create(url);
            System.Net.HttpWebResponse res = (System.Net.HttpWebResponse)req.GetResponse();
            Image myImage = Image.FromStream(res.GetResponseStream());
            myImage.Save("c:\\fast.jpg");//保存
            //===========================Http测试，直接下载=======================================
            using (WebClient web = new WebClient())
            {
                web.DownloadFile("http://img13.360buyimg.com/da/g5/M02/0D/16/rBEDik_nOJ0IAAAAAAA_cbJCY-UAACrRgMhVLEAAD-J352.jpg", "C:\\abc.jpg");
                web.DownloadFile("http://192.168.81.233/M00/00/00/wKhR6VADbNr5s7ODAAIOGO1_YmA574.jpg", "C:\\abc.jpg");
            }
            //===========================防盗链请求=======================================
            start = new DateTime(1970, 1, 1); 
            end = DateTime.Now;
            consume = (TimeSpan)(end - start);
            int ts = (int)(consume.TotalSeconds);
            string pwd = FastDFS.Client.Util.GetToken("M00/03/81/wKhR6VAh0sfyH0AxAABYAMjfFsM301-part1.doc", ts, "FastDFS1qaz2wsxsipsd");
            string anti_steel_url = "http://192.168.81.233/M00/03/81/wKhR6VAh0sfyH0AxAABYAMjfFsM301-part1.doc?token=" + pwd + "&ts=" + ts;
            string url1 = "http://192.168.81.233/M00/01/E0/wKhR6VANJBiInHb5AAClVeZnxGg341.pdf";
            using (WebClient web = new WebClient())
            {
                web.DownloadFile(anti_steel_url, "C:\\salve.doc");
            }
            Console.WriteLine("Complete");
            Console.Read();
        }
    }
}
