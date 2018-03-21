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


        protected virtual void SendRequest(Stream outputStream)
        {
            byte[] headerBuffer = this._header.ToByte();
            outputStream.Write(headerBuffer, 0, headerBuffer.Length);
            outputStream.Write(this._body, 0, this._body.Length);
        }


        public void GetResponse()
        {
            GetResponse(null);
        }
        
        public virtual void GetResponse(FDFSResponse response)
        {
            if(this._connection == null)
                this._connection = ConnectionManager.GetTrackerConnection();
            _connection.Open();
            try
            {
                NetworkStream stream = this._connection.GetStream();
                this.SendRequest(stream);
                
                
                FDFSHeader header = new FDFSHeader(stream);
                if (header.Status != 0)
                    throw new FDFSException(string.Format("Get Response Error,Error Code:{0}", header.Status));

                if (response != null)
                    response.ReceiveResponse(stream, header.Length);
                _connection.Close();
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

    public class FDFSResponse
    {
        public virtual void ReceiveResponse(Stream stream, long length) {
            byte[] content = new byte[length];
            stream.Read(content, 0, (int)length);
            LoadContent(content);
        }
        protected virtual void LoadContent(byte[] content)
        {
        }
    }
}