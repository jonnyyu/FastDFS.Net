using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Net.Sockets;
namespace FastDFS.Client
{
    public class FastDFSClient
    {
        /// <summary>
        /// 获取存储节点
        /// </summary>
        /// <param name="groupName">组名</param>
        /// <returns>存储节点实体类</returns>
        public static StorageNode GetStorageNode(string groupName)
        {
            FDFSRequest trackerRequest = QUERY_STORE_WITH_GROUP_ONE.Instance.GetRequest(groupName);
            QUERY_STORE_WITH_GROUP_ONE.Response trackerResponse = new QUERY_STORE_WITH_GROUP_ONE.Response();
            trackerRequest.GetResponse(trackerResponse);
            IPEndPoint storeEndPoint = new IPEndPoint(IPAddress.Parse(trackerResponse.IPStr), trackerResponse.Port);
            StorageNode result = new StorageNode();
            result.GroupName = trackerResponse.GroupName;
            result.EndPoint = storeEndPoint;
            result.StorePathIndex = trackerResponse.StorePathIndex;
            return result;
        }
        /// <summary>
        /// 上传文件
        /// </summary>
        /// <param name="storageNode">GetStorageNode方法返回的存储节点</param>
        /// <param name="contentByte">文件内容</param>
        /// <param name="fileExt">文件扩展名(注意:不包含".")</param>
        /// <returns>文件名</returns>
        public static string UploadFile(StorageNode storageNode,byte[] contentByte,string fileExt)
        {
            FDFSRequest storageRequest = UPLOAD_FILE.Instance.GetRequest(storageNode.EndPoint, storageNode.StorePathIndex, contentByte.Length, fileExt, contentByte);
            UPLOAD_FILE.Response storageResponse = new UPLOAD_FILE.Response();
            storageRequest.GetResponse(storageResponse);
            return storageResponse.FileName;
        }

        public static string UploadFileByName(StorageNode storageNode, string filename)
        {
            using (FileStream fs = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                string extension = Path.GetExtension(filename).Substring(1);
                FDFSRequest storageRequest = UPLOAD_FILE.Instance.GetRequest(storageNode.EndPoint, storageNode.StorePathIndex, fs.Length, extension, fs);
                UPLOAD_FILE.Response storageResponse = new UPLOAD_FILE.Response();
                storageRequest.GetResponse(storageResponse);
                return storageResponse.FileName;
            }
        }

        /// <summary>
        /// 上传从文件
        /// </summary>
        /// <param name="storageNode">GetStorageNode方法返回的存储节点</param>
        /// <param name="contentByte">文件内容</param>
        /// <param name="master_filename">主文件名</param>
        /// <param name="prefix_name">从文件后缀</param>
        /// <param name="fileExt">文件扩展名(注意:不包含".")</param>
        /// <returns>文件名</returns>
        public static string UploadSlaveFile(string groupName, byte[] contentByte, string master_filename, string prefix_name, string fileExt)
        {
            FDFSRequest trackerRequest = QUERY_UPDATE.Instance.GetRequest(groupName, master_filename);
            QUERY_UPDATE.Response trackerResponse = new QUERY_UPDATE.Response();
            trackerRequest.GetResponse(trackerResponse);
            IPEndPoint storeEndPoint = new IPEndPoint(IPAddress.Parse(trackerResponse.IPStr), trackerResponse.Port);

            FDFSRequest storageRequest = UPLOAD_SLAVE_FILE.Instance.GetRequest(storeEndPoint, contentByte.Length, master_filename, prefix_name, fileExt, contentByte);
            UPLOAD_FILE.Response storageResponse = new UPLOAD_FILE.Response();
            storageRequest.GetResponse(storageResponse);
            return storageResponse.FileName;
        }


        /// <summary>
        /// 上传可以Append的文件
        /// </summary>
        /// <param name="storageNode">GetStorageNode方法返回的存储节点</param>
        /// <param name="contentByte">文件内容</param>
        /// <param name="fileExt">文件扩展名(注意:不包含".")</param>
        /// <returns>文件名</returns>
        public static string UploadAppenderFile(StorageNode storageNode, byte[] contentByte, string fileExt)
        {
            FDFSRequest storageRequest = UPLOAD_APPEND_FILE.Instance.GetRequest(storageNode.EndPoint, storageNode.StorePathIndex, contentByte.Length, fileExt, contentByte);
            UPLOAD_APPEND_FILE.Response storageResponse = new UPLOAD_APPEND_FILE.Response();
            storageRequest.GetResponse(storageResponse);
            return storageResponse.FileName;
        }
        /// <summary>
        /// 附加文件
        /// </summary>
        /// <param name="groupName">组名</param>
        /// <param name="fileName">文件名</param>
        /// <param name="contentByte">文件内容</param>
        public static void AppendFile(string groupName,string fileName, byte[] contentByte)
        {
            FDFSRequest trackerRequest = QUERY_UPDATE.Instance.GetRequest(groupName, fileName);
            QUERY_UPDATE.Response trackerResponse = new QUERY_UPDATE.Response();
            trackerRequest.GetResponse(trackerResponse);
            IPEndPoint storeEndPoint = new IPEndPoint(IPAddress.Parse(trackerResponse.IPStr), trackerResponse.Port);

            FDFSRequest storageRequest = APPEND_FILE.Instance.GetRequest(storeEndPoint, fileName, contentByte);
            storageRequest.GetResponse();
        }
        /// <summary>
        /// 删除文件
        /// </summary>
        /// <param name="groupName">组名</param>
        /// <param name="fileName">文件名</param>
        public static void RemoveFile(string groupName,string fileName)
        {
            FDFSRequest trackerRequest = QUERY_UPDATE.Instance.GetRequest(groupName, fileName);
            QUERY_UPDATE.Response trackerResponse = new QUERY_UPDATE.Response();
            trackerRequest.GetResponse(trackerResponse);
            IPEndPoint storeEndPoint = new IPEndPoint(IPAddress.Parse(trackerResponse.IPStr), trackerResponse.Port);
            FDFSRequest storageRequest = DELETE_FILE.Instance.GetRequest(storeEndPoint, groupName, fileName);
            storageRequest.GetResponse();
        }
        /// <summary>
        /// 下载文件
        /// </summary>
        /// <param name="storageNode">GetStorageNode方法返回的存储节点</param>
        /// <param name="fileName">文件名</param>
        /// <returns>文件内容</returns>
        public static byte[] DownloadFile(StorageNode storageNode,string fileName)
        {
            FDFSRequest storageRequest = DOWNLOAD_FILE.Instance.GetRequest(storageNode.EndPoint, 0L, 0L, storageNode.GroupName, fileName);
            DOWNLOAD_FILE.Response storageResponse = new DOWNLOAD_FILE.Response();
            storageRequest.GetResponse(storageResponse);
            return storageResponse.Content;
        }
        /// <summary>
        /// 增量下载文件
        /// </summary>
        /// <param name="storageNode">GetStorageNode方法返回的存储节点</param>
        /// <param name="fileName">文件名</param>
        /// <param name="offset">从文件起始点的偏移量</param>
        /// <param name="length">要读取的字节数</param>
        /// <returns>文件内容</returns>
        public static byte[] DownloadFile(StorageNode storageNode, string fileName, long offset, long length)
        {
            FDFSRequest storageRequest = DOWNLOAD_FILE.Instance.GetRequest(storageNode.EndPoint, offset, length, storageNode.GroupName, fileName);
            DOWNLOAD_FILE.Response storageResponse = new DOWNLOAD_FILE.Response();
            storageRequest.GetResponse(storageResponse);
            return storageResponse.Content;
        }

        public static string DownloadFileEx(StorageNode storageNode, string filename, string destDir, string destFileName = null)
        {
            string fullPath = null;
            if (destFileName == null)
            {
                IDictionary<string, string> metaData = GetMetaData(storageNode, filename);
                destFileName = metaData["Name"] + Path.GetExtension(filename);
            }
            fullPath = Path.Combine(destDir, destFileName);
            FDFSRequest storageRequest = DOWNLOAD_FILE.Instance.GetRequest(storageNode.EndPoint, 0L, 0L, storageNode.GroupName, filename);
            DOWNLOAD_FILE.ResponseEx storageResponse = new DOWNLOAD_FILE.ResponseEx(fullPath);
            storageRequest.GetResponse(storageResponse);
            return storageResponse.FullPath;
        }

        /// <summary>
        /// 获取文件信息
        /// </summary>
        /// <param name="storageNode">GetStorageNode方法返回的存储节点</param>
        /// <param name="fileName">文件名</param>
        /// <returns></returns>
        public static FDFSFileInfo GetFileInfo(StorageNode storageNode, string fileName)
        {
            FDFSRequest storageRequest = QUERY_FILE_INFO.Instance.GetRequest(storageNode.EndPoint, storageNode.GroupName, fileName);
            FDFSFileInfo result = new FDFSFileInfo();
            storageRequest.GetResponse(result);
            return result;
        }


        public static void SetMetaData(StorageNode storageNode, string fileName, IDictionary<string, string> metadata, MetaDataOption option = MetaDataOption.Overwrite)
        {
            FDFSRequest storageRequest = SET_METADATA.Instance.GetRequest(storageNode.EndPoint, storageNode.GroupName, fileName, metadata, option);
            storageRequest.GetResponse(); // no response body
        }

        public static IDictionary<string, string> GetMetaData(StorageNode storageNode, string fileName)
        {
            FDFSRequest storageRequest = GET_METADATA.Instance.GetRequest(storageNode.EndPoint, storageNode.GroupName, fileName);
            GET_METADATA.Response storageResponse = new GET_METADATA.Response();
            storageRequest.GetResponse(storageResponse);
            return storageResponse.MetaData;
        }


    }
}
