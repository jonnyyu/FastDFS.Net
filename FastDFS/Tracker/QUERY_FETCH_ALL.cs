using System;
using System.Collections.Generic;
using System.Text;

namespace FastDFS.Client
{
    /// <summary>
    /// query all storage servers to download the file
    /// 
    /// Reqeust 
    ///     Cmd: TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ALL 105
    ///     Body: 
    ///     @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
    ///     @ filename bytes: filename
    /// Response
    ///     Cmd: TRACKER_PROTO_CMD_RESP
    ///     Status: 0 right other wrong
    ///     Body: 
    ///     @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
    ///     @ IP_ADDRESS_SIZE - 1 bytes:  storage server ip address (multi)
    ///     @ FDFS_PROTO_PKG_LEN_SIZE bytes: storage server port (multi)
    /// </summary>
    public class QUERY_FETCH_ALL : FDFSRequest
    {
        private static QUERY_FETCH_ALL _instance = null;
        public static QUERY_FETCH_ALL Instance
        {
            get
            {
                if (_instance == null)
                    _instance = new QUERY_FETCH_ALL();
                return _instance;
            }
        }

        private QUERY_FETCH_ALL()
        {
            
        }

        public override FDFSRequest GetRequest(params object[] paramList)
        {
            string groupName = (string)paramList[0];
            string fileName = (string)paramList[1];
            return QUERY_FETCH_ONE.Instance.GetRequest(groupName, fileName, Consts.TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ALL);
        }

        public class Response : FDFSResponse
        {
            public string GroupName;
            public List<string> IPStrs;
            public int Port;

            public Response()
            {
                IPStrs = new List<string>();

            }

            protected override void LoadContent(byte[] metaDataBuffer)
            {
                int bytesRead = 0;

                byte[] groupNameBuffer = new byte[Consts.FDFS_GROUP_NAME_MAX_LEN];
                Array.Copy(metaDataBuffer, bytesRead, groupNameBuffer, 0, Consts.FDFS_GROUP_NAME_MAX_LEN);
                GroupName = Util.ByteToString(groupNameBuffer).TrimEnd('\0');
                bytesRead += Consts.FDFS_GROUP_NAME_MAX_LEN;

                byte[] ipAddressBuffer = new byte[Consts.IP_ADDRESS_SIZE - 1];
                Array.Copy(metaDataBuffer, bytesRead, ipAddressBuffer, 0, Consts.IP_ADDRESS_SIZE - 1);
                IPStrs.Add(new string(FDFSConfig.Charset.GetChars(ipAddressBuffer)).TrimEnd('\0'));
                bytesRead += Consts.IP_ADDRESS_SIZE - 1;

                byte[] portBuffer = new byte[Consts.FDFS_PROTO_PKG_LEN_SIZE];
                Array.Copy(metaDataBuffer, bytesRead, portBuffer, 0, Consts.FDFS_PROTO_PKG_LEN_SIZE);
                Port = (int)Util.BufferToLong(portBuffer, 0);
                bytesRead += Consts.FDFS_PROTO_PKG_LEN_SIZE;

                while (metaDataBuffer.Length - bytesRead >= Consts.IP_ADDRESS_SIZE - 1)
                {
                    ipAddressBuffer = new byte[Consts.IP_ADDRESS_SIZE - 1];
                    Array.Copy(metaDataBuffer, bytesRead, ipAddressBuffer, 0, Consts.IP_ADDRESS_SIZE - 1);
                    IPStrs.Add(new string(FDFSConfig.Charset.GetChars(ipAddressBuffer)).TrimEnd('\0'));
                    bytesRead += Consts.IP_ADDRESS_SIZE - 1;
                }
            }

        }
    }
}