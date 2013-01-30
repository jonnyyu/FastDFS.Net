using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.IO;

namespace FastDFS.Client
{
    /// <summary>
    /// download/fetch file from storage server
    /// 
    /// Reqeust 
    ///     Cmd: STORAGE_PROTO_CMD_DOWNLOAD_FILE 14
    ///     Body:
    ///     @ FDFS_PROTO_PKG_LEN_SIZE bytes: file offset
    ///     @ FDFS_PROTO_PKG_LEN_SIZE bytes: download file bytes      
    ///     @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
    ///     @ filename bytes: filename
    /// Response
    ///     Cmd: STORAGE_PROTO_CMD_RESP
    ///     Status: 0 right other wrong
    ///     Body: 
    ///     @ file content
    /// </summary>
    public class DOWNLOAD_FILE : FDFSRequest
    {
        private static DOWNLOAD_FILE _instance = null;
        public static DOWNLOAD_FILE Instance
        {
            get
            {
                if (_instance == null)
                    _instance = new DOWNLOAD_FILE();
                return _instance;
            }
        }
        private DOWNLOAD_FILE()
        {            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="paramList">
        /// 1,IPEndPoint    IPEndPoint-->the storage IPEndPoint
        /// 2,long offset-->file offset
        /// 3,long byteSize -->download file bytes
        /// 4,string groupName
        /// 5,string fileName
        /// </param>
        /// <returns></returns>
        public override FDFSRequest GetRequest(params object[] paramList)
        {
            if (paramList.Length != 5)
                throw new FDFSException("param count is wrong");
            IPEndPoint endPoint = (IPEndPoint)paramList[0];
            long offset = (long)paramList[1];
            long byteSize = (long)paramList[2];
            string groupName = (string)paramList[3];
            string fileName = (string)paramList[4];

            DOWNLOAD_FILE result = new DOWNLOAD_FILE();
            result.Connection = ConnectionManager.GetStorageConnection(endPoint);

            if (groupName.Length > Consts.FDFS_GROUP_NAME_MAX_LEN)
                throw new FDFSException("groupName is too long");

            long length = Consts.FDFS_PROTO_PKG_LEN_SIZE +
                Consts.FDFS_PROTO_PKG_LEN_SIZE +
                Consts.FDFS_GROUP_NAME_MAX_LEN +
                fileName.Length;
            byte[] bodyBuffer = new byte[length];
            byte[] offsetBuffer = Util.LongToBuffer(offset);
            byte[] byteSizeBuffer = Util.LongToBuffer(byteSize);
            byte[] groupNameBuffer = Util.StringToByte(groupName);
            byte[] fileNameBuffer = Util.StringToByte(fileName);
            Array.Copy(offsetBuffer, 0, bodyBuffer, 0, offsetBuffer.Length);
            Array.Copy(byteSizeBuffer, 0, bodyBuffer, Consts.FDFS_PROTO_PKG_LEN_SIZE, byteSizeBuffer.Length);
            Array.Copy(groupNameBuffer, 0, bodyBuffer, Consts.FDFS_PROTO_PKG_LEN_SIZE +
                Consts.FDFS_PROTO_PKG_LEN_SIZE, groupNameBuffer.Length);
            Array.Copy(fileNameBuffer, 0, bodyBuffer, Consts.FDFS_PROTO_PKG_LEN_SIZE +
                Consts.FDFS_PROTO_PKG_LEN_SIZE + Consts.FDFS_GROUP_NAME_MAX_LEN, fileNameBuffer.Length);

            result.Body = bodyBuffer;
            result.Header = new FDFSHeader(length, Consts.STORAGE_PROTO_CMD_DOWNLOAD_FILE, 0);
            return result;
        }

        public class Response : FDFSResponse
        {
            public byte[] Content;
            protected override void LoadContent(byte[] responseByte)
            {
                Content = responseByte;
            }
        }

        public class ResponseEx : FDFSResponse
        {
            public string FullPath;
            public ResponseEx(string fullPath)
            {
                FullPath = fullPath;
            }

            public override void ReceiveResponse(System.IO.Stream inStream, long length)
            {
                using (FileStream fs = new FileStream(FullPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
                {
                    byte[] buf = new byte[512 * 1024];
                    long total = 0;
                    int bytesRead = 0;
                    while ((bytesRead = inStream.Read(buf, 0, buf.Length)) > 0)
                    {
                        fs.Write(buf, 0, bytesRead);
                        total += bytesRead;
                        if (total == length)
                            break;
                    }
                }
            }
        }
    }
}