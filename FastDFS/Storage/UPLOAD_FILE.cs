using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.IO;

namespace FastDFS.Client
{
    /// <summary>
    /// upload file to storage server
    /// 
    /// Reqeust 
    ///     Cmd: STORAGE_PROTO_CMD_UPLOAD_FILE 11
    ///     Body:
    ///     @ FDFS_PROTO_PKG_LEN_SIZE bytes: filename size
    ///     @ FDFS_PROTO_PKG_LEN_SIZE bytes: file bytes size
    ///     @ filename
    ///     @ file bytes: file content 
    /// Response
    ///     Cmd: STORAGE_PROTO_CMD_RESP
    ///     Status: 0 right other wrong
    ///     Body: 
    ///     @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
    ///     @ filename bytes: filename   
    /// </summary>
    public class UPLOAD_FILE : FDFSRequest
    {
        private Stream Stream = null;

        private static UPLOAD_FILE _instance = null;
        public static UPLOAD_FILE Instance
        {
            get
            {
                if (_instance == null)
                    _instance = new UPLOAD_FILE();
                return _instance;
            }
        }
        private UPLOAD_FILE()
        {            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="paramList">
        /// 1,IPEndPoint     IPEndPoint-->the storage IPEndPoint
        /// 2,Byte           StorePathIndex
        /// 3,long           FileSize
        /// 4,string         File Ext
        /// 5,byte[FileSize] File Content or FileStream
        /// </param>
        /// <returns></returns>
        public override FDFSRequest GetRequest(params object[] paramList)
        {
            if (paramList.Length != 5)
                throw new FDFSException("param count is wrong");
            IPEndPoint endPoint = (IPEndPoint)paramList[0];
            
            byte storePathIndex = (byte)paramList[1];
            long fileSize = (long)paramList[2];
            string ext = (string)paramList[3];
            
            Stream stream = paramList[4] as Stream;
            if (paramList[4] is byte[])
            {
                stream = new MemoryStream((byte[])paramList[4]);
            }

            #region 拷贝后缀扩展名值
            byte[] extBuffer = new byte[Consts.FDFS_FILE_EXT_NAME_MAX_LEN];
            byte[] bse = Util.StringToByte(ext);
            int ext_name_len = bse.Length;
            if (ext_name_len > Consts.FDFS_FILE_EXT_NAME_MAX_LEN)
            {
                ext_name_len = Consts.FDFS_FILE_EXT_NAME_MAX_LEN;
            }
            Array.Copy(bse, 0, extBuffer, 0, ext_name_len);
            #endregion
            
            UPLOAD_FILE request = new UPLOAD_FILE();
            request.Connection = ConnectionManager.GetStorageConnection(endPoint);
            if(ext.Length>Consts.FDFS_FILE_EXT_NAME_MAX_LEN)
                throw new FDFSException("file ext is too long");

            long headerLength = 1 + Consts.FDFS_PROTO_PKG_LEN_SIZE + Consts.FDFS_FILE_EXT_NAME_MAX_LEN;

            byte[] bodyBuffer = new byte[headerLength];
            bodyBuffer[0] = storePathIndex;

            byte[] fileSizeBuffer = Util.LongToBuffer(fileSize);
            Array.Copy(fileSizeBuffer, 0, bodyBuffer, 1, fileSizeBuffer.Length);
            Array.Copy(extBuffer, 0, bodyBuffer, 1 + Consts.FDFS_PROTO_PKG_LEN_SIZE, extBuffer.Length);
            
            request.Stream = stream;
            request.Body = bodyBuffer;
            request.Header = new FDFSHeader(headerLength + stream.Length, Consts.STORAGE_PROTO_CMD_UPLOAD_FILE, 0);
            return request;
        }


        protected override void SendRequest(Stream outputStream)
        {
            base.SendRequest(outputStream);

            long total = 0;
            int bytesRead = 0;
            byte[] buffer = new byte[512 * 1024];
            while ((bytesRead = Stream.Read(buffer, 0, buffer.Length)) > 0)
            {
                outputStream.Write(buffer, 0, bytesRead);
                total += bytesRead;
            }
        }

        public class Response : FDFSResponse
        {
            public string GroupName;
            public string FileName;
            protected override void LoadContent(byte[] responseBody)
            {
                byte[] groupNameBuffer = new byte[Consts.FDFS_GROUP_NAME_MAX_LEN];
                Array.Copy(responseBody, groupNameBuffer, Consts.FDFS_GROUP_NAME_MAX_LEN);
                GroupName = Util.ByteToString(groupNameBuffer).TrimEnd('\0');

                byte[] fileNameBuffer = new byte[responseBody.Length - Consts.FDFS_GROUP_NAME_MAX_LEN];
                Array.Copy(responseBody, Consts.FDFS_GROUP_NAME_MAX_LEN, fileNameBuffer, 0, fileNameBuffer.Length);
                FileName = Util.ByteToString(fileNameBuffer).TrimEnd('\0');
            }
        }

    }
}
