using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Collections;
using System.Net;
using System.Net.Sockets;

namespace FastDFS.Client
{
    public class FDFSRequest
    {
        private FDFSHeader _header;
        public FDFSHeader Header
        {
            set { _header = value; }
            get { return _header; }
        }
        private byte[] _body;
        public byte[] Body
        {
            set { _body = value; }
            get { return _body; }
        }
        private Connection _connection;
        public Connection Connection
        {
            get { return _connection; }
            set { this._connection = value; }
        }

        public FDFSRequest()
        { 
            
        }
        
        public byte[] ToByteArray()
        {
            throw new NotImplementedException();
        }
        
        public virtual FDFSRequest GetRequest(params object[] paramList)
        {
            throw new NotImplementedException();
        }
        public virtual byte[] GetResponse()
        {
            if(this._connection == null)
                this._connection = ConnectionManager.GetTrackerConnection();
            _connection.Open();
            try
            {
                NetworkStream stream = this._connection.GetStream();
                byte[] headerBuffer = this._header.ToByte();
                stream.Write(headerBuffer, 0, headerBuffer.Length);
                stream.Write(this._body, 0, this._body.Length);
                
                FDFSHeader header = new FDFSHeader(stream);
                if (header.Status != 0)
                    throw new FDFSException(string.Format("Get Response Error,Error Code:{0}", header.Status));
                byte[] body = new byte[header.Length];
                if (header.Length != 0)                
                    stream.Read(body, 0, (int)header.Length);
                
                _connection.Close();
                return body;
            }
            catch(Exception ex)
            {
                _connection.Release();
                throw ex;//可以看Storage节点的log看
                //22    -〉下载字节数超过文件长度 invalid download file bytes: 10 > file remain bytes: 4
                //      -> 或者 pkg length is not correct
                //2     -〉没有此文件 error info: No such file or directory.
            }            
        }
    }
}