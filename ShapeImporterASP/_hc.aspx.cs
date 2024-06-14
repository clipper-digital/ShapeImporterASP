using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace ShapeImporterASP
{
    public partial class _hc : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            string sConn = System.Configuration.ConfigurationManager.ConnectionStrings["SQLConn"].ToString();
            if (! string.IsNullOrEmpty(sConn))
            {
                try
                {
                    SqlConnection conn = new SqlConnection(sConn);
                    conn.Close();
                    Response.StatusCode = 200;
                }
                catch (Exception)
                {
                    Response.StatusCode = 401;
                }
                Response.Write("");
            }
        }
    }
}