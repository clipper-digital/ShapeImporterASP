using Microsoft.Extensions.Configuration;
using Renci.SshNet;
using Serilog.Events;
using Serilog;
using SharpMap.Data.Providers;
using SharpMap.Geometries;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace ShapeImporterASP
{
    public partial class _Default : Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            string sTimeout = System.Configuration.ConfigurationManager.AppSettings["ASPTimeout"];
            int iTimeout = 43200;
            if (!string.IsNullOrEmpty(sTimeout))
                if (!int.TryParse(sTimeout, out iTimeout))
                    iTimeout = 43200;

            Page.Server.ScriptTimeout = iTimeout;

            if (!CheckConfig())
                return;

            CleanFileStorage();
            bool bStatus = DownloadShapeZipFile();

            if(bStatus)
                bStatus = UnzipShapeFiles();

            if(bStatus)
                bStatus = ExtractATZs();

            if (bStatus)
            {
                bool bDoNotPostDelete = false;
                string sDoNotPostDelete = System.Configuration.ConfigurationManager.AppSettings["DoNotPostDelete"];
                if(! string.IsNullOrEmpty(sDoNotPostDelete))
                    bool.TryParse(sDoNotPostDelete, out bDoNotPostDelete);
                if(! bDoNotPostDelete)
                    CleanFileStorage();
            }
        }

        // ========================================================================================

        public void CleanFileStorage()
        {
            string sStorageUNC = System.Configuration.ConfigurationManager.AppSettings["FileStorageUNC"];
            if (string.IsNullOrEmpty(sStorageUNC))
            {
                Response.StatusCode = 420;
                return;
            }

            string[] sFiles = Directory.GetFiles(sStorageUNC);
            if(sFiles.Length != 0)
            {
                foreach(string sFile in sFiles)
                {
                    File.Delete(sFile);
                }
            }
        }

        // ========================================================================================

        public bool DownloadShapeZipFile()
        {
            string sURL = System.Configuration.ConfigurationManager.AppSettings["FTPServer"];
            int iPort = int.Parse(System.Configuration.ConfigurationManager.AppSettings["FTPPort"]);
            string sUsername = System.Configuration.ConfigurationManager.AppSettings["FTPUsername"];
            string sPassword = System.Configuration.ConfigurationManager.AppSettings["FTPPassword"];
            string sRemoteFile = System.Configuration.ConfigurationManager.AppSettings["FTPFile"];
            string sStorageUNC = System.Configuration.ConfigurationManager.AppSettings["FileStorageUNC"];
            string sZipFilename = System.Configuration.ConfigurationManager.AppSettings["LocalZipFilename"];

            if(! System.IO.Directory.Exists(sStorageUNC))
            {
                SLog("File share " + sStorageUNC + " doesn't exist...quitting", LogEventLevel.Error);
                Response.StatusCode = 421;
                return false;
            }
            try
            {
                if (System.IO.File.Exists(sStorageUNC + sZipFilename))
                {
                    SLog("Attempting to delete " + sStorageUNC + sZipFilename, LogEventLevel.Information);
                    System.IO.File.Delete(sStorageUNC + sZipFilename);
                }
            }
            catch (Exception e)
            {
                SLog("Exeception deleting " + sStorageUNC + sZipFilename + ": " + e.ToString(), LogEventLevel.Error);
                Response.StatusCode = 406;
                return false;
            }

            FileStream fs;
            try
            {
                SLog("Attempting to create " + sStorageUNC + sZipFilename, LogEventLevel.Information);
                fs = System.IO.File.Create(sStorageUNC + sZipFilename);
            }
            catch (Exception e)
            {
                SLog("Exception creating " + sStorageUNC + sZipFilename + ": " + e.ToString(), LogEventLevel.Error);
                Response.StatusCode = 407;
                return false;
            }

            SftpClient sftp = new SftpClient(sURL, iPort, sUsername, sPassword);
            try
            {
                SLog("Attempting to connect to " + sURL + ":" + iPort.ToString(), LogEventLevel.Information);
                sftp.Connect();
            }
            catch (Exception e)
            {
                fs.Close();
                fs.Dispose();
                SLog("Exception connecting to " + sURL + ":" + iPort.ToString() + ": " + e.ToString(), LogEventLevel.Error);
                Response.StatusCode = 408;
                return false;
            }
            try
            {
                SLog("Starting zip file download", LogEventLevel.Information);
                sftp.DownloadFile(sRemoteFile, fs);
            }
            catch (Exception e)
            {
                sftp.Disconnect();
                sftp.Dispose();
                fs.Close();
                fs.Dispose();
                SLog("Exception downloading file: " + e.ToString(), LogEventLevel.Error);
                Response.StatusCode = 409;
                return false;
            }
            fs.Close();
            fs.Dispose();
            sftp.Disconnect();
            sftp.Dispose();
            Response.StatusCode = 200;
            SLog("Download complete...setting StatusCode to 200", LogEventLevel.Information);
            return true;
        }

        // ========================================================================================

        public bool UnzipShapeFiles()
        {
            string sStorageUNC = System.Configuration.ConfigurationManager.AppSettings["FileStorageUNC"];
            string sZipFilename = System.Configuration.ConfigurationManager.AppSettings["LocalZipFilename"];

            FileStream fs;
            try
            {
                SLog("Attempting to open " + sStorageUNC + sZipFilename, LogEventLevel.Information);
                fs = System.IO.File.OpenRead(sStorageUNC + sZipFilename);
            }
            catch (Exception e)
            {
                SLog("Exception opening " + sStorageUNC + sZipFilename + ": " + e.ToString(), LogEventLevel.Error);
                Response.StatusCode = 411;
                return false;
            }

            ZipArchive za;
            try
            {
                SLog("Creating ZipArchive from zip file", LogEventLevel.Information);
                za = new ZipArchive(fs, ZipArchiveMode.Read);
            }
            catch (Exception e)
            {
                SLog("Exception creating ZipArchive from " + sZipFilename + ": " + e.ToString(), LogEventLevel.Error);
                Response.StatusCode = 412;
                fs.Close();
                fs.Dispose();
                return false;
            }

            foreach (ZipArchiveEntry ze in za.Entries)
            {
                Stream zipStream;
                try
                {
                    SLog("Opening " + ze.FullName + " from archive", LogEventLevel.Information);
                    zipStream = ze.Open();
                }
                catch (Exception e)
                {
                    SLog("Exception reading " + ze.FullName + " from " + sZipFilename + ": " + e.ToString(), LogEventLevel.Error);
                    Response.StatusCode = 413;
                    return false;
                }

                FileStream outStream;
                try
                {
                    SLog("Attempting to create " + sStorageUNC + "\\" + ze.FullName, LogEventLevel.Information);
                    outStream = System.IO.File.Create(sStorageUNC + "\\" + ze.FullName);
                }
                catch (Exception e)
                {
                    SLog("Exception creating " + ze.FullName + ": " + e.ToString(), LogEventLevel.Error);
                    Response.StatusCode = 414;
                    zipStream.Close();
                    zipStream.Dispose();
                    return false;
                }

                try
                {
                    SLog("Copying data from archive to " + ze.FullName, LogEventLevel.Information);
                    zipStream.CopyTo(outStream);
                }
                catch (Exception e)
                {
                    SLog("Exception writing to " + ze.FullName + ": " + e.ToString(), LogEventLevel.Error);
                    Response.StatusCode = 415;
                    zipStream.Close();
                    zipStream.Dispose();
                    return false;
                }

                outStream.Close();
            }
            SLog("Unzipping process complete, setting StatusCode to 200", LogEventLevel.Information);
            Response.StatusCode = 200;
            fs.Close();
            fs.Dispose();
            return true;
        }

        // ========================================================================================

        public bool ExtractATZs()
        {
            string sStorageUNC = System.Configuration.ConfigurationManager.AppSettings["FileStorageUNC"];
            string sConn = System.Configuration.ConfigurationManager.ConnectionStrings["SQLConn"].ToString();

            string[] sSHPFiles;
            try
            {
                SLog("Looking for shp files", LogEventLevel.Information);
                sSHPFiles = System.IO.Directory.GetFiles(sStorageUNC, "*.shp");
            }
            catch (Exception e)
            {
                SLog("Exception finding SHP files: " + e.ToString(), LogEventLevel.Error);
                Response.StatusCode = 418;
                return false;
            }
            if (sSHPFiles.Length == 0)
            {
                SLog("There are no shp files in " + sStorageUNC + ", quitting", LogEventLevel.Warning);
                Response.StatusCode = 200;
                return true;
            }

            SqlConnection conn = new SqlConnection(sConn);
            try
            {
                conn.Open();
            }
            catch (Exception e)
            {
                SLog("Exception opening data connection: " + e.ToString(), LogEventLevel.Error);
                Response.StatusCode = 419;
                return false;
            }

            SqlCommand cmd = new SqlCommand("delete from ATZPolygonDataLoaderT", conn);
            cmd.CommandTimeout = 3600;
            try
            {
                SLog("Running 'delete from ATZPolygonDataLoaderT", LogEventLevel.Information);
                cmd.ExecuteNonQuery();
            }
            catch(Exception e)
            {
                SLog("Exception deleting from ATZPolygonDataLoaderT: " + e.ToString(), LogEventLevel.Error);
                conn.Close();
                Response.StatusCode = 422;
                return false;
            }
            foreach (string sSHPFile in sSHPFiles)
            {
                SLog("Loading shapes from " + sSHPFile, LogEventLevel.Information);
                using (ShapeFile sf = new ShapeFile(sSHPFile, true))
                {
                    try
                    {
                        SLog("Attempting to open shape file " + sSHPFile, LogEventLevel.Information);
                        sf.Open();
                    }
                    catch(Exception e)
                    {
                        SLog("Exception opening shape file " + sSHPFile + ": " + e.ToString(), LogEventLevel.Error);
                        continue;
                    }
                    int iFeatureCount = sf.GetFeatureCount();
                    for (uint i = 0; i < iFeatureCount; ++i)
                    {
                        Geometry sharpGeometry = sf.GetGeometryByID(i);
                        if (!(sharpGeometry is Point))
                        {
                            string sSQL = "insert into ATZPolygonDataLoaderT (ATZ, PolygonData) values " +
                                          "('" + sf.GetFeature(i).ItemArray[1].ToString() + "','" + sharpGeometry.ToString() + "')";
                            cmd = new SqlCommand(sSQL, conn);
                            cmd.ExecuteNonQuery();
                            cmd.Dispose();
                        }
                    }
                    sf.Close();
                }
            }

            string sRunSproc = System.Configuration.ConfigurationManager.AppSettings["RunSproc"];
            bool bRunSproc = false;
            if (!string.IsNullOrEmpty(sRunSproc))
                if (!bool.TryParse(sRunSproc, out bRunSproc))
                    bRunSproc = false;
            if (bRunSproc)
            {
                SLog("Executing SPUpdateDEsotoCarrierRoutes_Step1 stored procedure", LogEventLevel.Information);
                cmd = new SqlCommand("SPUpdateDesotoCarrierRoutes_Step1", conn);
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.CommandTimeout = 3600;
                cmd.ExecuteNonQuery();
            }

            conn.Close();
            Response.StatusCode = 200;
            return true;
        }

        // ========================================================================================

        public bool CheckConfig()
        {
            string sURL = System.Configuration.ConfigurationManager.AppSettings["FTPServer"];
            if (string.IsNullOrEmpty(sURL))
            {
                Response.StatusCode = 400;
                return false;
            }
            int iPort = 0;
            string s = System.Configuration.ConfigurationManager.AppSettings["FTPPort"];
            if (!int.TryParse(s, out iPort))
            {
                Response.StatusCode = 401;
                return false;
            }
            string sUsername = System.Configuration.ConfigurationManager.AppSettings["FTPUsername"];
            if (string.IsNullOrEmpty(sUsername))
            {
                Response.StatusCode = 402;
                return false;
            }
            string sPassword = System.Configuration.ConfigurationManager.AppSettings["FTPPassword"];
            if (string.IsNullOrEmpty(sPassword))
            {
                Response.StatusCode = 403;
                return false;
            }
            string sRemoteFile = System.Configuration.ConfigurationManager.AppSettings["FTPFile"];
            if (string.IsNullOrEmpty(sRemoteFile))
            {
                Response.StatusCode = 404;
                return false;
            }
            string sStorageUNC = System.Configuration.ConfigurationManager.AppSettings["FileStorageUNC"];
            if (string.IsNullOrEmpty(sStorageUNC))
            {
                Response.StatusCode = 405;
                return false;
            }
            string sZipFilename = System.Configuration.ConfigurationManager.AppSettings["LocalZipFilename"];
            if (string.IsNullOrEmpty(sZipFilename))
            {
                Response.StatusCode = 421;
                return false;
            }
            string sConn = System.Configuration.ConfigurationManager.ConnectionStrings["SQLConn"].ToString();
            if (string.IsNullOrEmpty(sConn))
            {
                Response.StatusCode = 417;
                return false;
            }
            return true;
        }

        // ========================================================================================

        public static void SLog(string sMessage, LogEventLevel logLevel, string sTags = "", string sHost = "", string sService = "")
        {
            if (string.IsNullOrEmpty(sHost)) sHost = System.Configuration.ConfigurationManager.AppSettings["LogHost"];
            if (string.IsNullOrEmpty(sService)) sService = System.Configuration.ConfigurationManager.AppSettings["LogService"];
            string sAPIKey = System.Configuration.ConfigurationManager.AppSettings["DD_API_KEY"];
            string[] sTagArray = new string[] { };
            if (!string.IsNullOrEmpty(sTags))
                sTagArray = sTags.Split(',');
            string sMinimum = System.Configuration.ConfigurationManager.AppSettings["MinLevel"];
            if (!string.IsNullOrEmpty(sMinimum)) sMinimum = sMinimum.ToLower();
            else sMinimum = "";
            ServicePointManager.SecurityProtocol = (SecurityProtocolType)0xc00;
            var log = new LoggerConfiguration();
            log.WriteTo.DatadogLogs(sAPIKey, source: "csharp", host: sHost, service: sService, tags: sTagArray);
            if (sMinimum.Equals("verbose")) log.MinimumLevel.Verbose();
            else if (sMinimum.Equals("debug")) log.MinimumLevel.Debug();
            else if (sMinimum.Equals("error")) log.MinimumLevel.Error();
            else if (sMinimum.Equals("fatal")) log.MinimumLevel.Fatal();
            else if (sMinimum.Equals("info")) log.MinimumLevel.Information();
            else if (sMinimum.Equals("warning")) log.MinimumLevel.Warning();
            else log.MinimumLevel.Verbose();
            var logger = log.CreateLogger();
            logger.Write(logLevel, DateTime.Now.ToString() + " " + logLevel.ToString() + ": " + sMessage);
            logger.Dispose();
        }
    }
}