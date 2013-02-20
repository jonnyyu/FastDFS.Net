using System;
using System.Collections.Generic;
using System.Text;
using FastDFS.Client;
namespace FastDFS.Client
{
    /// <summary>
    /// query which storage server to download the file
    /// 
    /// Reqeust 
    ///     Cmd: TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE 102
    ///     Body: 
    ///     @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
    ///     @ filename bytes: filename
    /// Response
    ///     Cmd: TRACKER_PROTO_CMD_RESP
    ///     Status: 0 right other wrong
    ///     Body: 
    ///     @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
    ///     @ IP_ADDRESS_SIZE - 1 bytes:  storage server ip address
    ///     @ FDFS_PROTO_PKG_LEN_SIZE bytes: storage server port    
    /// </summary>
    public class QUERY_FETCH_ONE : FDFSRequest
    {
        private static QUERY_FETCH_ONE _instance = null;
        public static QUERY_FETCH_ONE Instance
        {
            get
            {
                if (_instance == null)
                    _instance = new QUERY_FETCH_ONE();
                return _instance;
            }
        }

        private QUERY_FETCH_ONE()
        {
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="paramList">
        /// 1,string groupName
        /// 2,string fileName
        /// </param>
        /// <returns></returns>
        public override FDFSRequest GetRequest(params object[] paramList)
        {
            if (paramList.Length != 2 && paramList.Length != 3)
                throw new FDFSException("param count is wrong");

            QUERY_FETCH_ONE result = new QUERY_FETCH_ONE();
            string groupName = (string)paramList[0];
            string fileName = (string)paramList[1];


            byte cmd = Consts.TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE;
            if (paramList.Length == 3)
                cmd = (byte)paramList[2];

            if(groupName.Length > Consts.FDFS_GROUP_NAME_MAX_LEN)
                throw new FDFSException("GroupName is too long");

            byte[] groupNameBuffer = Util.StringToByte(groupName);
            byte[] fileNameBuffer = Util.StringToByte(fileName);
            int length = Consts.FDFS_GROUP_NAME_MAX_LEN + fileNameBuffer.Length;
            byte[] body = new byte[length];

            Array.Copy(groupNameBuffer, 0, body, 0, groupNameBuffer.Length);
            Array.Copy(groupNameBuffer, 0, body, 0, groupNameBuffer.Length);

            result.Body = body;
            result.Header = new FDFSHeader(length, cmd, 0);
            return result;
        }

        public class Response : FDFSResponse
        {
            public string GroupName;
            public string IPStr;
            public int Port;

            protected override void LoadContent(byte[] metaDataBuffer)
            {
                byte[] groupNameBuffer = new byte[Consts.FDFS_GROUP_NAME_MAX_LEN];
                Array.Copy(metaDataBuffer, groupNameBuffer, Consts.FDFS_GROUP_NAME_MAX_LEN);
                GroupName = Util.ByteToString(groupNameBuffer).TrimEnd('\0');
                byte[] ipAddressBuffer = new byte[Consts.IP_ADDRESS_SIZE - 1];
                Array.Copy(metaDataBuffer, Consts.FDFS_GROUP_NAME_MAX_LEN, ipAddressBuffer, 0, Consts.IP_ADDRESS_SIZE - 1);
                IPStr = new string(FDFSConfig.Charset.GetChars(ipAddressBuffer)).TrimEnd('\0');
                byte[] portBuffer = new byte[Consts.FDFS_PROTO_PKG_LEN_SIZE];
                Array.Copy(metaDataBuffer, Consts.FDFS_GROUP_NAME_MAX_LEN + Consts.IP_ADDRESS_SIZE - 1,
                    portBuffer, 0, Consts.FDFS_PROTO_PKG_LEN_SIZE);
                Port = (int)Util.BufferToLong(portBuffer, 0);
            }
        }
    }
}
