using System.Collections.Generic;
using System.Net;
using FastDFS.Client;
using System.Configuration;
using System.Text.RegularExpressions;
using System;
using System.IO;
using System.Threading;

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

                string op = args[0];
                if (string.Compare(op, "-u") == 0 ||
                    string.Compare(op, "--upload") == 0)
                {
                    StorageNode node = FastDFSClient.GetStorageNode("group1");
                    DoUpload(node, args[1]);
                }
                else if (string.Compare(op, "-d") == 0 ||
                         string.Compare(op, "--download") == 0)
                {
                    int timeoutSecs = 300;
                    string fildid = null;
                    string destFileName = null;

                    int argIndex = 1;
                    if (string.Compare(args[argIndex], "--timeout") == 0)
                    {
                        argIndex++;
                        timeoutSecs = int.Parse(args[argIndex]);
                        argIndex++;
                    }

                    fildid = args[argIndex++];

                    if (args.Length > argIndex)
                    {
                        destFileName = args[argIndex];
                    }

                    string groupName;
                    string fileName;
                    SplitGroupNameAndFileName(fildid, out groupName, out fileName);

                    StorageNode[] nodes = FastDFSClient.QueryStorageNodesForFile(groupName, fileName);
                    if (nodes == null)
                    {
                        throw new FDFSException(string.Format("Group {0} not found.", groupName));
                    }

                    StorageNode node = SelectStorageNode(nodes, fileName, timeoutSecs);
                    if (node == null)
                    {
                        throw new FDFSException("node not available");
                    }
                    DoDownload(node, fileName, destFileName);
                }
                else if (string.Compare(op, "-q") == 0 ||
                         string.Compare(op, "--query") == 0)
                {
                    string groupName;
                    string fileName;

                    SplitGroupNameAndFileName(args[1], out groupName, out fileName);

                    StorageNode[] nodes = FastDFSClient.QueryStorageNodesForFile(groupName, fileName);
                    if (nodes == null)
                    {
                        throw new FDFSException(string.Format("Group {0} not found.", groupName));
                    }

                    foreach (StorageNode node in nodes)
                    {
                        Console.WriteLine("Group:{0}\nServer:{1}\n", node.GroupName, node.EndPoint.ToString());
                        if (FileExistsOnStorageNode(node, fileName))
                        {
                            Console.WriteLine("file exists.");
                        }
                        else
                        {
                            Console.WriteLine("file doesn't exist.");
                        }
                    }
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

        private static StorageNode SelectStorageNode(StorageNode[] nodes, string fileName, int timeoutSecs)
        {
            foreach (StorageNode node in nodes)
            {
                if (WaitForFileExists(node, fileName, TimeSpan.FromSeconds(timeoutSecs)))
                    return node;
            }
            return null;
        }

        private static bool WaitForFileExists(StorageNode node, string fileName, TimeSpan timeSpan)
        {
            DateTime begin = DateTime.Now;
            bool fileExists = false;
            TimeSpan sleepTime = TimeSpan.FromSeconds(5);
            do
            {
                fileExists = FileExistsOnStorageNode(node, fileName);
                if (fileExists)
                    return true;

                Thread.Sleep(sleepTime);

            } while (!fileExists && DateTime.Now - begin < timeSpan);
            return false;
        }

        private static bool FileExistsOnStorageNode(StorageNode node, string fileName)
        {
            try
            {
                FastDFSClient.GetFileInfo(node, fileName);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private static void SplitGroupNameAndFileName(string fileId, out string groupName, out string fileName)
        {
            int index = fileId.IndexOf('/');
            groupName = fileId.Substring(0, index);
            fileName = fileId.Substring(index + 1);
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
            // upload file
            string id = FastDFSClient.UploadFileByName(node, fileName);
            Console.WriteLine("{0}/{1}", node.GroupName, id);

            // set name as metadata
            Dictionary<string, string> metaData = new Dictionary<string, string>();
            metaData["Name"] = Path.GetFileNameWithoutExtension(fileName);
            FastDFSClient.SetMetaData(node, id, metaData);
        }

        private static void Usage()
        {
            Console.WriteLine("fdfsclient <-u|--upload> <file_to_upload>");
            Console.WriteLine("fdfsclient <-d|--download> [--timeout secs] <file_id> [filename]");
            Console.WriteLine("fdfsclient <-q|--query> <file_id>");
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
