using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using FastDFS.Client;
using ManyConsole;

namespace FastDFS.ConsoleApp
{
	abstract class FastDFSCommand : ConsoleCommand
	{
		protected int _timeout = 300;
		protected List<IPEndPoint> _trackers = new List<IPEndPoint>();
		protected bool _dryrun = false;

		protected FastDFSCommand()
		{
			HasOption("t|timeout=",
			          "timeout in seconds.",
			          (int v) => _timeout = v);
			HasOption ("h|host=", "tracker server in address:port format. Can add multiple times. ",
			           v => _trackers.Add(CreateEndPoint(v)) );
			HasOption ("dryrun", "dryrun only",
			           v => _dryrun = true);
		}

		private List<IPEndPoint> TrackersFromSettings()
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

		private IPEndPoint CreateEndPoint(string ipAndPort)
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

		protected static bool WaitForFileExists(StorageNode node, string fileName, TimeSpan timeSpan)
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

		protected static bool FileExistsOnStorageNode(StorageNode node, string fileName)
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

		protected static void SplitGroupNameAndFileName(string fileId, out string groupName, out string fileName)
		{
			int index = fileId.IndexOf('/');
			groupName = fileId.Substring(0, index);
			fileName = fileId.Substring(index + 1);
		}

		protected virtual string OptionsToString()
		{
			var sb = new StringBuilder ();
			sb.AppendFormat ("--timeout {0} ", _timeout);
			_trackers.ForEach( t => sb.AppendFormat("--host {0} ", t) );
			return sb.ToString ();
		}

		public override int? OverrideAfterHandlingArgumentsBeforeRun (string[] remainingArguments)
		{
			// use configuration if not specified in command line.
			if (_trackers.Count == 0)
				_trackers = TrackersFromSettings ();
			return base.OverrideAfterHandlingArgumentsBeforeRun (remainingArguments);
		}
	}


	class DownloadCommand : FastDFSCommand
	{
		string _destFileName;

		public DownloadCommand()
		{
			IsCommand("download", "download file from FastDFS");
			HasOption ("o|output=", "output filename",
			           v => _destFileName = v);
			HasAdditionalArguments (1, "<file_id>");
		}

		public override int Run(string[] remainingArguments)
		{
			string file_id = remainingArguments [0];
			if (_dryrun) {
				Console.WriteLine ("fdfsclient download {0} --output {1} {2}", OptionsToString(), _destFileName, file_id);
				return 0;
			}
	
			ConnectionManager.Initialize(_trackers);
			string groupName = null;
			string fileName  = null;
			SplitGroupNameAndFileName(file_id, out groupName, out fileName);

			StorageNode[] nodes = FastDFSClient.QueryStorageNodesForFile(groupName, fileName);
			if (nodes == null)
				throw new FDFSException(string.Format("Group {0} not found.", groupName));

			StorageNode node = SelectStorageNode(nodes, fileName, _timeout);
			if (node == null)
				throw new FDFSException("node not available");
			DoDownload(node, fileName, _destFileName);
			return 0;
		}

		StorageNode SelectStorageNode(StorageNode[] nodes, string fileName, int timeoutSecs)
		{
			foreach (StorageNode node in nodes)
			{
				if (WaitForFileExists(node, fileName, TimeSpan.FromSeconds(timeoutSecs)))
					return node;
			}
			return null;
		}

		void DoDownload(StorageNode node, string fileId, string destFileName)
		{
			//get metadata
			IDictionary<string, string> metaData = FastDFSClient.GetMetaData(node, fileId);

			//download file
			string destDir = Environment.CurrentDirectory;
			string destName = metaData["filename"] + Path.GetExtension(fileId);

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
	}


	class UploadCommand : FastDFSCommand
	{
		public UploadCommand()
		{
			IsCommand("upload", "upload file to FastDFS");
			HasAdditionalArguments (1, "<filename>");
		}

		public override int Run(string[] remainingArguments)
		{
			string filename = remainingArguments.FirstOrDefault();
			if (_dryrun) {
				Console.WriteLine ("fdfsclient upload {0} {1}", OptionsToString(), filename);
				return 0;
			}
			ConnectionManager.Initialize(_trackers);
			StorageNode node = FastDFSClient.GetStorageNode("group1");
			string fileid = DoUpload(node, filename);
			Console.WriteLine("{0}", fileid);
			Console.WriteLine("http://{0}/{1}", _trackers[0].Address.ToString(), fileid);
			return 0;
		}

		string DoUpload(StorageNode node, string fileName)
		{
			// upload file
			string id = FastDFSClient.UploadFileByName(node, fileName);

			// set name as metadata
			Dictionary<string, string> metaData = new Dictionary<string, string>();
			metaData["filename"] = Path.GetFileNameWithoutExtension(fileName);
			FastDFSClient.SetMetaData(node, id, metaData);
			return string.Format("{0}/{1}", node.GroupName, id);
		}
	}


	class QueryCommand : FastDFSCommand
	{
		public QueryCommand()
		{
			IsCommand("query", "query file on FastDFS");
			HasAdditionalArguments (1, "<fild_id>");
		}

		public override int Run(string[] remainingArguments)
		{
			string file_id = remainingArguments [0];
			if (_dryrun) {
				Console.WriteLine ("fdfsclient query {0} {1}", OptionsToString(), file_id);
				return 0;
			}
			ConnectionManager.Initialize(_trackers);

			string groupName = null;
			string fileName  = null;
			SplitGroupNameAndFileName(file_id, out groupName, out fileName);

			StorageNode[] nodes = FastDFSClient.QueryStorageNodesForFile(groupName, fileName);
			if (nodes == null) {
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
			return 0;
		}
	}


	class Program
	{
		public static int Main (string[] args)
		{
            try
            {
                // locate any commands in the assembly (or use an IoC container, or whatever source)
                var commands = GetCommands();

                // then run them.
                return ConsoleCommandDispatcher.DispatchCommand(commands, args, Console.Out);
            }
            catch(Exception ex)
            {
                Console.Error.WriteLine("Unhandled Exception:{0}\n{1}", ex.Message, ex.StackTrace);
                return 1;
            }
		}

		public static IEnumerable<ConsoleCommand> GetCommands()
		{
			return ConsoleCommandDispatcher.FindCommandsInSameAssemblyAs(typeof(Program));
		}
	}
}
