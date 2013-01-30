using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FastDFS.Client
{
    public class FDFSConfig
    {
        public static int Storage_MaxConnection = 20;
        public static int Tracker_MaxConnection = 10;
        public static int ConnectionTimeout = 5;    //Second
        public static int Connection_LifeTime = 3600;
        public static Encoding Charset = Encoding.UTF8;
    }
}
