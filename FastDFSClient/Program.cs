using System.Collections.Generic;
using System.Net;
using FastDFS.Client;
using System.Configuration;
using System.Text.RegularExpressions;
using System;
using System.IO;

namespace FastDFS.ConsoleApp
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Usage();
                return;
            }

            try
            {
                List<IPEndPoint> trackers = TrackersFromSettings();
                ConnectionManager.Initialize(trackers);
                StorageNode node = FastDFSClient.GetStorageNode("group1");

                string op = args[0];
                if (string.Compare(op, "-u") == 0 ||
                    string.Compare(op, "--upload") == 0)
                {
                    DoUpload(node, args[1]);
                }
                else if (string.Compare(op, "-d") == 0 ||
                         string.Compare(op, "--download") == 0)
                {
                    if (args.Length >= 3)
                        DoDownload(node, args[1], args[2]);
                    else
                        DoDownload(node, args[1], null);

                }
                else
                {
                    Console.WriteLine("Invalid options");
                    Usage();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR:{0}", ex.Message);
            }
            return;
        }

        private static void DoDownload(StorageNode node, string fileId, string destFileName)
        {
            //get metadata
            IDictionary<string, string> metaData = FastDFSClient.GetMetaData(node, fileId);

            //download file
            string destDir = Environment.CurrentDirectory;
            string destName = metaData["Name"] + Path.GetExtension(fileId);

            if (!string.IsNullOrEmpty(destFileName))
            {
                string dir = Path.GetDirectoryName(destFileName);
                if (!string.IsNullOrEmpty(dir))
                    destDir = dir;

                destName = Path.GetFileName(destFileName);
            }

            string fullName = FastDFSClient.DownloadFileEx(node, fileId, destDir, destName);
            Console.WriteLine("{0}", fullName);
        }

        private static void DoUpload(StorageNode node, string fileName)
        {
            FileInfo fi = new FileInfo(fileName);
            if (!fi.Exists)
            {
                Console.WriteLine("File not found");
                return;
            }

            // upload file
            string id = FastDFSClient.UploadFileByName(node, fileName);
            Console.WriteLine("{0}", id);

            // set name as metadata
            Dictionary<string, string> metaData = new Dictionary<string, string>();
            metaData["Name"] = Path.GetFileNameWithoutExtension(fileName);
            FastDFSClient.SetMetaData(node, id, metaData);
        }

        private static void Usage()
        {
            Console.WriteLine("fdfsclient <-u|--upload> <file_to_upload>");
            Console.WriteLine("fdfsclient <-d|--download> <file_id> [filename]");
        }

        private static List<IPEndPoint> TrackersFromSettings()
        {
            List<IPEndPoint> trackers = new List<IPEndPoint>();

            foreach(string key in ConfigurationManager.AppSettings)
            {
                if (Regex.IsMatch(key, "Tracker[0-9]"))
                {
                    trackers.Add(CreateEndPoint(ConfigurationManager.AppSettings[key]));
                }
            }
            return trackers;
        }

        private static IPEndPoint CreateEndPoint(string ipAndPort)
        {
            int pos = ipAndPort.IndexOf(':');

            if (pos < 0)
            {
                // assume the default port if port isn't specified
                return new IPEndPoint(IPAddress.Parse(ipAndPort), 22122);
            }

            int port = int.Parse(ipAndPort.Substring(pos + 1));
            return new IPEndPoint(IPAddress.Parse(ipAndPort.Substring(0, pos)), port);
        }
    }
}
