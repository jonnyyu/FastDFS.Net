using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FastDFS.Client
{
    public class FDFSException : Exception
    {
        public FDFSException(string msg) : base(msg)
        {
            
        }
    }
}
