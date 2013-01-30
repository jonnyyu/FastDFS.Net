using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
namespace FastDFS.Client
{
    public class StorageNode
    {
        public string GroupName;
        public IPEndPoint EndPoint;
        public byte StorePathIndex;
    }
}
