using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;

namespace FastDFS.Client
{
    /// <summary>
    /// get metat data from storage server
    /// 
    /// Reqeust 
    ///     Cmd: STORAGE_PROTO_CMD_GET_METADATA 15
    ///     Body:   
    ///     @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
    ///     @ filename bytes: filename
    /// Response
    ///     Cmd: STORAGE_PROTO_CMD_RESP
    ///     Status: 0 right other wrong
    ///     Body: 
    ///     @ meta data buff, each meta data seperated by \x01, name and value seperated by \x02
    /// </summary>
    public class GET_METADATA : FDFSRequest
    {
        private static GET_METADATA _instance = null;
        public static GET_METADATA Instance
        {
            get
            {
                if (_instance == null)
                    _instance = new GET_METADATA();
                return _instance;
            }
        }

        private GET_METADATA()
        {
        }

        public override FDFSRequest GetRequest(params object[] paramList)
        {
            IPEndPoint endPoint = (IPEndPoint)paramList[0];
            string groupName    = (string)paramList[1];
            string fileName     = (string)paramList[2];


            byte[] groupNameBuffer = Util.CreateGroupNameBuffer(groupName);
            byte[] fileNameBuffer = Util.StringToByte(fileName);

            int length = Consts.FDFS_GROUP_NAME_MAX_LEN + // group name
                         fileNameBuffer.Length;           // filename

            List<byte> bodyBuffer = new List<byte>(length);
            bodyBuffer.AddRange(groupNameBuffer);
            bodyBuffer.AddRange(fileNameBuffer);

            GET_METADATA request = new GET_METADATA();
            request.Connection = ConnectionManager.GetStorageConnection(endPoint);
            request.Body = bodyBuffer.ToArray();
            request.Header = new FDFSHeader(bodyBuffer.Count, Consts.STORAGE_PROTO_CMD_GET_METADATA, 0);
            return request;
        }


        public class Response : FDFSResponse
        {
            public Dictionary<string, string> MetaData;
            

            protected override void LoadContent(byte[] metaDataBuffer)
            {
                MetaData = CreateMetaDataFromBuffer(metaDataBuffer);
            }

            private Dictionary<string, string> CreateMetaDataFromBuffer(byte[] metaDataBuffer)
            {
                Dictionary<string, string> metaData = new Dictionary<string,string>();
                int itemSeparaterIndex = -1;
                int keyValueSeparaterIndex = -1;
                int startIndex = 0;

                do
                {
                    string key = null, value = null;

                    keyValueSeparaterIndex = Array.IndexOf<byte>(metaDataBuffer, Consts.METADATA_KEY_VALUE_SEPARATOR, startIndex);
                    if (keyValueSeparaterIndex < 0)
                        throw new FDFSException("invalid metadata buffer format");

                    key = Util.ByteToString(metaDataBuffer, startIndex, keyValueSeparaterIndex - startIndex);
                    startIndex = keyValueSeparaterIndex + 1;

                    itemSeparaterIndex = Array.IndexOf<byte>(metaDataBuffer, Consts.METADATA_PAIR_SEPARATER, startIndex);

                    if (itemSeparaterIndex < 0)
                        value = Util.ByteToString(metaDataBuffer, startIndex, (metaDataBuffer.Length - 1) - startIndex);
                    else
                        value = Util.ByteToString(metaDataBuffer, startIndex, itemSeparaterIndex - startIndex);

                    metaData.Add(key, value);
                } while (itemSeparaterIndex >= 0);

                return metaData;
            }


        }
    }
}
