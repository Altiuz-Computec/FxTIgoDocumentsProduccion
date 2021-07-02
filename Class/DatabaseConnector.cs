using FxDocumentsTigo.Class.Serialization;
using FxTigoDocuments.Class;
using FxTigoDocuments.Class.Enumeration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Net;
using System.Text;

namespace FxDocumentsTigo.Class
{
    class DatabaseConnector
    {

        private const string DefaultDatabaseName = "dbfacturaelectronica";

        private const int CodigoDian = 0;
        private const string IdTipoDocumentoNotaCredito = "91";
        private const string IdTipoDocumentoNotaDebito = "92";

        private string DefaultDatabaseStringConnection { get; set; }

        public DatabaseConnector()
        {
            try
            {
                DefaultDatabaseStringConnection = Settings.GetVariable(DefaultDatabaseName);
            }
            catch (NullReferenceException nre)
            {
                throw nre;
            } catch (Exception e)
            {
                throw e;
            }
        }

        public void InsertarTrazabilidad(string TIPO_CLIENTE,string TIPO_DOCUMENTO,string CONTRATO,string NUMERO_FACTURA,
            string IDENTIFICACION,string NUMEROCUENTA,string FECHAFACTURA,string REFERENCIA_PAGO,long? NUMERO_MOVIL)
        {

            IPHostEntry host;
            string localIP = "";
            host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (IPAddress ip in host.AddressList)
            {
                if (ip.AddressFamily.ToString() == "InterNetwork")
                {
                    localIP = ip.ToString();
                }
        }


          

            using (var connection = new SqlConnection("Server=tcp:serverfacturaelectronica.database.windows.net,1433;Initial Catalog=Dbfacturaelectronica;Persist Security Info=False;User ID=user_fe;Password=*Factura2019*;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=60;"))
            {


           connection.Open();
              
               using (SqlCommand command = connection.CreateCommand())
              {
                   try
                   {
             command.CommandText = "INSERT INTO SERVICIOS_SELECT.TRAZABILIDAD_WS_TIGO(ID_CONSULTA,TIPOCLIENTE,TIPODOCUMENTO,CONTRATO,NUMEROFACTURA,IP,RESPUESTAWS,FECHACONSULTA,IDENTIFICACION,NUMEROCUENTA,FECHAFACTURA,REFERENCIA_PAGO,NUMEROMOVIL,TIPO) VALUES(@guidValue,@param1,@param2,@param3,@param4,@param5,@param6,@param7,@param8,@param9,@param10,@param11,@param12,@param13)";
              command.Parameters.AddWithValue("@guidValue",Guid.NewGuid());
              command.Parameters.AddWithValue("@param1", TIPO_CLIENTE);
               command.Parameters.AddWithValue("@param2", TIPO_DOCUMENTO);
               command.Parameters.AddWithValue("@param3", CONTRATO);
               command.Parameters.AddWithValue("@param4", NUMERO_FACTURA);
               command.Parameters.AddWithValue("@param5", localIP);
               command.Parameters.AddWithValue("@param6", "OK");
              command.Parameters.AddWithValue("@param7", DateTime.Now);
              command.Parameters.AddWithValue("@param8", IDENTIFICACION);
              command.Parameters.AddWithValue("@param9", NUMEROCUENTA);
               command.Parameters.AddWithValue("@param10", FECHAFACTURA);
              command.Parameters.AddWithValue("@param11", REFERENCIA_PAGO);
               command.Parameters.AddWithValue("@param12", NUMERO_MOVIL);
               command.Parameters.AddWithValue("@param13", "CONSULTA"
                   );
                command.ExecuteNonQuery();
                   }
                   catch (Exception ex)
                   {
                      // Captura y propaga la excepción
                       throw ex;
                   }
                    finally
                   {
                       //Cierra la conexion
                       connection.Close();
                   }
               }
                    
               }
                
           }

    







    public List<RespuestaMovil> GetFacturasMovil (List<long> obligados, Parametros parametros)
        {
            List<RespuestaMovil> facturas = new List<RespuestaMovil>();
            Dictionary<string, string> filters = new Dictionary<string, string>();

            //Crea la conexion
            using (var connection = new SqlConnection(DefaultDatabaseStringConnection))
            {
                try
                {
                    //Abre la conexion
                    connection.Open();

                    if ((parametros.NumeroCuenta != null && parametros.Identification != null ) || (parametros.Identification == ""))
                    {
                        //Crea la consulta
                        StringBuilder query = new StringBuilder("SELECT DF.IDENTIFICACION_OBLIGADO, DF.NUM_RESOLU_DIAN, DF.NO_DOCUMENTO, DF.IDENTIFICACION_ADQUIRIENTE, DF.NOMBRE_ADQUIRIENTE, ")
                                                        .Append("DF.CAMPO_ALMACENAMIENTO_1 AS MOVIL, DF.CAMPO_ALMACENAMIENTO_2 AS PLAN_MOVIL, DF.NUMERO_CUENTA, ")
                                                        .Append("FORMAT(DF.FECHA_FACTURA, 'yyyy-MM-dd') AS FECHA_FACTURA, ")
                                                        .Append("FORMAT(DF.FECHA_VENCIMIENTO, 'yyyy-MM-dd') AS FECHA_VENCIMIENTO, ")
                                                        .Append("UF.URL, UF.ID_TIPO_ARCHIVO, UF.IS_MIGRADO ")
                                                        .Append("FROM FELECV4.DATOS_FACTURA DF ")
                                                        .Append("FULL OUTER JOIN FELECV4.AUDITORIA_FACTURA AF ")
                                                        .Append("ON DF.IDENTIFICACION_OBLIGADO = AF.IDENTIFICACION_OBLIGADO AND DF.NO_DOCUMENTO = AF.NO_DOCUMENTO AND DF.NUM_RESOLU_DIAN = AF.NUM_RESOLU_DIAN ")
                                                        .Append("FULL OUTER JOIN FELECV4.URL_FACTURA UF ")
                                                        .Append("ON DF.IDENTIFICACION_OBLIGADO = UF.IDENTIFICACION_OBLIGADO AND DF.NO_DOCUMENTO = UF.NO_DOCUMENTO AND DF.NUM_RESOLU_DIAN = UF.NUM_RESOLU_DIAN AND DF.IDENTIFICACION_ADQUIRIENTE = UF.IDENTIFICACION_ADQUIRIENTE ")
                                                        .Append("WHERE DF.IDENTIFICACION_OBLIGADO IN (@obligado)  AND DF.FECHA_FACTURA >= @fechaInicio AND DF.FECHA_FACTURA <= @fechaFin ")
                                                        .Append("AND AF.CODIGO_RTA_WS_DIAN = @codigoDian ");

                        if (obligados?.Count > 0)
                        {
                            string inObligados = string.Join(", ", obligados);
                            query.Replace("@obligado", inObligados);
                        }
                        else
                        {
                            throw new Exception("La consulta no puede realizarse sin la identificacion de al menos un obligado");
                        }

                        //Se adicionan los filtros de búsqueda opcionales si contienen valor
                        /*if (!string.IsNullOrWhiteSpace(parametros.Identification))
                        {
                            query.Append("AND DF.IDENTIFICACION_ADQUIRIENTE = @identificacionAdquiriente ");
                            filters.Add("@identificacionAdquiriente", parametros.Identification);
                        }*/

                        if (!string.IsNullOrWhiteSpace(parametros.NumeroFactura))
                        {
                            query.Append("AND DF.NO_DOCUMENTO = @noDocumento ");
                            filters.Add("@noDocumento", parametros.NumeroFactura);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.NumeroCuenta))
                        {
                            if (parametros.NumeroCuenta.Trim().Length > 10)
                            {
                                query.Append("AND DF.NUMERO_CUENTA IN (@numeroCuenta1, @numeroCuenta2) ");
                                filters.Add("@numeroCuenta1", parametros.NumeroCuenta.Trim()[0..^2]);
                                filters.Add("@numeroCuenta2", parametros.NumeroCuenta.Trim());
                            }
                            else
                            {
                                query.Append("AND DF.NUMERO_CUENTA = @numeroCuenta ");
                                filters.Add("@numeroCuenta", parametros.NumeroCuenta.Trim());
                            }
                        }

                        if (parametros.Movil != null && parametros.Movil != 0)
                        {
                            query.Append("AND DF.CAMPO_ALMACENAMIENTO_1 = @movil ");
                            filters.Add("@movil", parametros.Movil.ToString());
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.Plan))
                        {
                            query.Append("AND DF.CAMPO_ALMACENAMIENTO_2 = @plan ");
                            filters.Add("@plan", parametros.Plan);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.Ciclo))
                        {
                            query.Append("AND AF.CICLO = @ciclo ");
                            filters.Add("@ciclo", parametros.Ciclo);
                        }

                        //Se ordenan los resultados de la consulta por fecha de factura
                        query.Append("ORDER BY DF.FECHA_FACTURA DESC, DF.HORA_FACTURA DESC ");

                        //Realiza la consulta en la BD 
                        using (SqlCommand cmd = new SqlCommand(query.ToString(), connection))
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.CommandText = query.ToString();

                            cmd.Parameters.Add("@fechaInicio", SqlDbType.Date).Value = parametros.FechaInicio;
                            cmd.Parameters.Add("@fechaFin", SqlDbType.Date).Value = parametros.FechaFin;
                            cmd.Parameters.Add("@identificacionAdquiriente", SqlDbType.VarChar).Value = parametros.Identification;
                            cmd.Parameters.Add("@codigoDian", SqlDbType.SmallInt).Value = CodigoDian;

                            foreach (var filter in filters)
                            {
                                cmd.Parameters.Add(filter.Key, SqlDbType.VarChar).Value = filter.Value;
                            }

                            using (var reader = cmd.ExecuteReader())
                            {
                                RespuestaMovil factura;
                                long idObligado;
                                while (reader.Read())
                                {
                                    factura = new RespuestaMovil();
                                    factura.TipoDocumento = parametros.TipoDocumento;
                                    idObligado = reader.GetInt64("IDENTIFICACION_OBLIGADO");
                                    factura.NumeroResolucion = reader.GetInt64("NUM_RESOLU_DIAN");
                                    factura.NumeroFactura = reader.GetString("NO_DOCUMENTO");
                                    factura.Identification = reader.GetString("IDENTIFICACION_ADQUIRIENTE");
                                    factura.Nombre = reader["NOMBRE_ADQUIRIENTE"].ToString();
                                    factura.Movil = (long.TryParse(reader["MOVIL"].ToString(), out long number)) ? number : 0;
                                    factura.Plan = reader["PLAN_MOVIL"].ToString();
                                    factura.NumeroCuenta = reader["NUMERO_CUENTA"].ToString();
                                    factura.FechaFacturacion = reader["FECHA_FACTURA"].ToString();
                                    factura.FechaVencimiento = reader["FECHA_VENCIMIENTO"].ToString();
                                    factura.UrlPdf = reader["URL"].ToString();
                                    factura.TipoArchivo = (int.TryParse(reader["ID_TIPO_ARCHIVO"].ToString(), out int code)) ? code : -1;
                                    factura.EsMigrado = (int.TryParse(reader["IS_MIGRADO"].ToString(), out int flag)) ? flag : 0;

                                    factura.Documentos = new Dictionary<string, string>();
                                    factura.Documentos.Add(TipoArchivo.xml.ToString(), Utilities.GetUrlParametros(idObligado, factura, TipoArchivo.xml));
                                    IPHostEntry host;
                                    string localIP = "";
                                    host = Dns.GetHostEntry(Dns.GetHostName());
                                    foreach (IPAddress ip in host.AddressList)
                                    {
                                        if (ip.AddressFamily.ToString() == "InterNetwork")
                                        {
                                            localIP = ip.ToString();
                                        }
                                    }
                                    //InsertarTrazabilidad(parametros.TipoCliente, parametros.TipoDocumento, "", factura.NumeroFactura, factura.Identification, factura.NumeroCuenta,
                                    //     factura.FechaFacturacion, "", factura.Movil);

                                    if (!string.IsNullOrWhiteSpace(factura.UrlPdf))
                                    {
                                        factura.Documentos.Add(TipoArchivo.pdf.ToString(), Utilities.GetUrlParametros(idObligado, factura, TipoArchivo.pdf));
                                    }
                                    else
                                    {
                                        factura.Documentos.Add(TipoArchivo.pdf.ToString(), "");
                                    }

                                   
                                    if (factura.Nombre.Contains("bict"))
                                    {
                                        
                                    }
                                    else
                                    {
                                        facturas.Add(factura);
                                    }
                                    }
                            }
                        }
                    }


                else if (parametros.NumeroFactura != null && parametros.Identification != null && parametros.Identification == "")
                    {
                        //Crea la consulta
                        StringBuilder query = new StringBuilder("SELECT DF.IDENTIFICACION_OBLIGADO, DF.NUM_RESOLU_DIAN, DF.NO_DOCUMENTO, DF.IDENTIFICACION_ADQUIRIENTE, DF.NOMBRE_ADQUIRIENTE, ")
                                                        .Append("DF.CAMPO_ALMACENAMIENTO_1 AS MOVIL, DF.CAMPO_ALMACENAMIENTO_2 AS PLAN_MOVIL, DF.NUMERO_CUENTA, ")
                                                        .Append("FORMAT(DF.FECHA_FACTURA, 'yyyy-MM-dd') AS FECHA_FACTURA, ")
                                                        .Append("FORMAT(DF.FECHA_VENCIMIENTO, 'yyyy-MM-dd') AS FECHA_VENCIMIENTO, ")
                                                        .Append("UF.URL, UF.ID_TIPO_ARCHIVO, UF.IS_MIGRADO ")
                                                        .Append("FROM FELECV4.DATOS_FACTURA DF ")
                                                        .Append("FULL OUTER JOIN FELECV4.AUDITORIA_FACTURA AF ")
                                                        .Append("ON DF.IDENTIFICACION_OBLIGADO = AF.IDENTIFICACION_OBLIGADO AND DF.NO_DOCUMENTO = AF.NO_DOCUMENTO AND DF.NUM_RESOLU_DIAN = AF.NUM_RESOLU_DIAN ")
                                                        .Append("FULL OUTER JOIN FELECV4.URL_FACTURA UF ")
                                                        .Append("ON DF.IDENTIFICACION_OBLIGADO = UF.IDENTIFICACION_OBLIGADO AND DF.NO_DOCUMENTO = UF.NO_DOCUMENTO AND DF.NUM_RESOLU_DIAN = UF.NUM_RESOLU_DIAN AND DF.IDENTIFICACION_ADQUIRIENTE = UF.IDENTIFICACION_ADQUIRIENTE ")
                                                        .Append("WHERE DF.IDENTIFICACION_OBLIGADO IN (@obligado) AND DF.FECHA_FACTURA >= @fechaInicio AND DF.FECHA_FACTURA <= @fechaFin ")
                                                        .Append("AND AF.CODIGO_RTA_WS_DIAN = @codigoDian ");

                        if (obligados?.Count > 0)
                        {
                            string inObligados = string.Join(", ", obligados);
                            query.Replace("@obligado", inObligados);
                        }
                        else
                        {
                            throw new Exception("La consulta no puede realizarse sin la identificacion de al menos un obligado");
                        }

                        //Se adicionan los filtros de búsqueda opcionales si contienen valor
                        /*if (!string.IsNullOrWhiteSpace(parametros.Identification))
                        {
                            query.Append("AND DF.IDENTIFICACION_ADQUIRIENTE = @identificacionAdquiriente ");
                            filters.Add("@identificacionAdquiriente", parametros.Identification);
                        }*/

                        if (!string.IsNullOrWhiteSpace(parametros.NumeroFactura))
                        {
                            query.Append("AND DF.NO_DOCUMENTO = @noDocumento ");
                            filters.Add("@noDocumento", parametros.NumeroFactura);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.NumeroCuenta))
                        {
                            if (parametros.NumeroCuenta.Trim().Length > 10)
                            {
                                query.Append("AND DF.NUMERO_CUENTA IN (@numeroCuenta1, @numeroCuenta2) ");
                                filters.Add("@numeroCuenta1", parametros.NumeroCuenta.Trim()[0..^2]);
                                filters.Add("@numeroCuenta2", parametros.NumeroCuenta.Trim());
                            }
                            else
                            {
                                query.Append("AND DF.NUMERO_CUENTA = @numeroCuenta ");
                                filters.Add("@numeroCuenta", parametros.NumeroCuenta.Trim());
                            }
                        }

                        if (parametros.Movil != null && parametros.Movil != 0)
                        {
                            query.Append("AND DF.CAMPO_ALMACENAMIENTO_1 = @movil ");
                            filters.Add("@movil", parametros.Movil.ToString());
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.Plan))
                        {
                            query.Append("AND DF.CAMPO_ALMACENAMIENTO_2 = @plan ");
                            filters.Add("@plan", parametros.Plan);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.Ciclo))
                        {
                            query.Append("AND AF.CICLO = @ciclo ");
                            filters.Add("@ciclo", parametros.Ciclo);
                        }

                        //Se ordenan los resultados de la consulta por fecha de factura
                        query.Append("ORDER BY DF.FECHA_FACTURA DESC, DF.HORA_FACTURA DESC ");

                        //Realiza la consulta en la BD 
                        using (SqlCommand cmd = new SqlCommand(query.ToString(), connection))
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.CommandText = query.ToString();

                            cmd.Parameters.Add("@fechaInicio", SqlDbType.Date).Value = parametros.FechaInicio;
                            cmd.Parameters.Add("@fechaFin", SqlDbType.Date).Value = parametros.FechaFin;
                            cmd.Parameters.Add("@identificacionAdquiriente", SqlDbType.VarChar).Value = parametros.Identification;
                            cmd.Parameters.Add("@codigoDian", SqlDbType.SmallInt).Value = CodigoDian;

                            foreach (var filter in filters)
                            {
                                cmd.Parameters.Add(filter.Key, SqlDbType.VarChar).Value = filter.Value;
                            }

                            using (var reader = cmd.ExecuteReader())
                            {
                                RespuestaMovil factura;
                                long idObligado;
                                while (reader.Read())
                                {
                                    factura = new RespuestaMovil();
                                    factura.TipoDocumento = parametros.TipoDocumento;
                                    idObligado = reader.GetInt64("IDENTIFICACION_OBLIGADO");
                                    factura.NumeroResolucion = reader.GetInt64("NUM_RESOLU_DIAN");
                                    factura.NumeroFactura = reader.GetString("NO_DOCUMENTO");
                                    factura.Identification = reader.GetString("IDENTIFICACION_ADQUIRIENTE");
                                    factura.Nombre = reader["NOMBRE_ADQUIRIENTE"].ToString();
                                    factura.Movil = (long.TryParse(reader["MOVIL"].ToString(), out long number)) ? number : 0;
                                    factura.Plan = reader["PLAN_MOVIL"].ToString();
                                    factura.NumeroCuenta = reader["NUMERO_CUENTA"].ToString();
                                    factura.FechaFacturacion = reader["FECHA_FACTURA"].ToString();
                                    factura.FechaVencimiento = reader["FECHA_VENCIMIENTO"].ToString();
                                    factura.UrlPdf = reader["URL"].ToString();
                                    factura.TipoArchivo = (int.TryParse(reader["ID_TIPO_ARCHIVO"].ToString(), out int code)) ? code : -1;
                                    factura.EsMigrado = (int.TryParse(reader["IS_MIGRADO"].ToString(), out int flag)) ? flag : 0;

                                    factura.Documentos = new Dictionary<string, string>();
                                    factura.Documentos.Add(TipoArchivo.xml.ToString(), Utilities.GetUrlParametros(idObligado, factura, TipoArchivo.xml));
                                    IPHostEntry host;
                                    string localIP = "";
                                    host = Dns.GetHostEntry(Dns.GetHostName());
                                    foreach (IPAddress ip in host.AddressList)
                                    {
                                        if (ip.AddressFamily.ToString() == "InterNetwork")
                                        {
                                            localIP = ip.ToString();
                                        }
                                    }
                                    //InsertarTrazabilidad(parametros.TipoCliente, parametros.TipoDocumento, "", factura.NumeroFactura, factura.Identification, factura.NumeroCuenta,
                                    //    factura.FechaFacturacion, "", factura.Movil);

                                    if (!string.IsNullOrWhiteSpace(factura.UrlPdf))
                                    {
                                        factura.Documentos.Add(TipoArchivo.pdf.ToString(), Utilities.GetUrlParametros(idObligado, factura, TipoArchivo.pdf));
                                    }
                                    else
                                    {
                                        factura.Documentos.Add(TipoArchivo.pdf.ToString(), "");
                                    }

                                    facturas.Add(factura);
                                }
                            }
                        }
                    }

                    else 
                    {
                        //Crea la consulta
                        StringBuilder query = new StringBuilder("SELECT DF.IDENTIFICACION_OBLIGADO, DF.NUM_RESOLU_DIAN, DF.NO_DOCUMENTO, DF.IDENTIFICACION_ADQUIRIENTE, DF.NOMBRE_ADQUIRIENTE, ")
                                                        .Append("DF.CAMPO_ALMACENAMIENTO_1 AS MOVIL, DF.CAMPO_ALMACENAMIENTO_2 AS PLAN_MOVIL, DF.NUMERO_CUENTA, ")
                                                        .Append("FORMAT(DF.FECHA_FACTURA, 'yyyy-MM-dd') AS FECHA_FACTURA, ")
                                                        .Append("FORMAT(DF.FECHA_VENCIMIENTO, 'yyyy-MM-dd') AS FECHA_VENCIMIENTO, ")
                                                        .Append("UF.URL, UF.ID_TIPO_ARCHIVO, UF.IS_MIGRADO ")
                                                        .Append("FROM FELECV4.DATOS_FACTURA DF ")
                                                        .Append("FULL OUTER JOIN FELECV4.AUDITORIA_FACTURA AF ")
                                                        .Append("ON DF.IDENTIFICACION_OBLIGADO = AF.IDENTIFICACION_OBLIGADO AND DF.NO_DOCUMENTO = AF.NO_DOCUMENTO AND DF.NUM_RESOLU_DIAN = AF.NUM_RESOLU_DIAN ")
                                                        .Append("FULL OUTER JOIN FELECV4.URL_FACTURA UF ")
                                                        .Append("ON DF.IDENTIFICACION_OBLIGADO = UF.IDENTIFICACION_OBLIGADO AND DF.NO_DOCUMENTO = UF.NO_DOCUMENTO AND DF.NUM_RESOLU_DIAN = UF.NUM_RESOLU_DIAN AND DF.IDENTIFICACION_ADQUIRIENTE = UF.IDENTIFICACION_ADQUIRIENTE ")
                                                        .Append("WHERE DF.IDENTIFICACION_OBLIGADO IN (@obligado) AND DF.IDENTIFICACION_ADQUIRIENTE = @identificacionAdquiriente AND DF.FECHA_FACTURA >= @fechaInicio AND DF.FECHA_FACTURA <= @fechaFin ")
                                                        .Append("AND AF.CODIGO_RTA_WS_DIAN = @codigoDian ");

                        if (obligados?.Count > 0)
                        {
                            string inObligados = string.Join(", ", obligados);
                            query.Replace("@obligado", inObligados);
                        }
                        else
                        {
                            throw new Exception("La consulta no puede realizarse sin la identificacion de al menos un obligado");
                        }

                        //Se adicionan los filtros de búsqueda opcionales si contienen valor
                        /*if (!string.IsNullOrWhiteSpace(parametros.Identification))
                        {
                            query.Append("AND DF.IDENTIFICACION_ADQUIRIENTE = @identificacionAdquiriente ");
                            filters.Add("@identificacionAdquiriente", parametros.Identification);
                        }*/

                        if (!string.IsNullOrWhiteSpace(parametros.NumeroFactura))
                        {
                            query.Append("AND DF.NO_DOCUMENTO = @noDocumento ");
                            filters.Add("@noDocumento", parametros.NumeroFactura);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.NumeroCuenta))
                        {
                            if (parametros.NumeroCuenta.Trim().Length > 10)
                            {
                                query.Append("AND DF.NUMERO_CUENTA IN (@numeroCuenta1, @numeroCuenta2) ");
                                filters.Add("@numeroCuenta1", parametros.NumeroCuenta.Trim()[0..^2]);
                                filters.Add("@numeroCuenta2", parametros.NumeroCuenta.Trim());
                            }
                            else
                            {
                                query.Append("AND DF.NUMERO_CUENTA = @numeroCuenta ");
                                filters.Add("@numeroCuenta", parametros.NumeroCuenta.Trim());
                            }
                        }

                        if (parametros.Movil != null && parametros.Movil != 0)
                        {
                            query.Append("AND DF.CAMPO_ALMACENAMIENTO_1 = @movil ");
                            filters.Add("@movil", parametros.Movil.ToString());
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.Plan))
                        {
                            query.Append("AND DF.CAMPO_ALMACENAMIENTO_2 = @plan ");
                            filters.Add("@plan", parametros.Plan);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.Ciclo))
                        {
                            query.Append("AND AF.CICLO = @ciclo ");
                            filters.Add("@ciclo", parametros.Ciclo);
                        }

                        //Se ordenan los resultados de la consulta por fecha de factura
                        query.Append("ORDER BY DF.FECHA_FACTURA DESC, DF.HORA_FACTURA DESC ");

                        //Realiza la consulta en la BD 
                        using (SqlCommand cmd = new SqlCommand(query.ToString(), connection))
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.CommandText = query.ToString();

                            cmd.Parameters.Add("@fechaInicio", SqlDbType.Date).Value = parametros.FechaInicio;
                            cmd.Parameters.Add("@fechaFin", SqlDbType.Date).Value = parametros.FechaFin;
                            cmd.Parameters.Add("@identificacionAdquiriente", SqlDbType.VarChar).Value = parametros.Identification;
                            cmd.Parameters.Add("@codigoDian", SqlDbType.SmallInt).Value = CodigoDian;

                            foreach (var filter in filters)
                            {
                                cmd.Parameters.Add(filter.Key, SqlDbType.VarChar).Value = filter.Value;
                            }

                            using (var reader = cmd.ExecuteReader())
                            {
                                RespuestaMovil factura;
                                long idObligado;
                                while (reader.Read())
                                {
                                    factura = new RespuestaMovil();
                                    factura.TipoDocumento = parametros.TipoDocumento;
                                    idObligado = reader.GetInt64("IDENTIFICACION_OBLIGADO");
                                    factura.NumeroResolucion = reader.GetInt64("NUM_RESOLU_DIAN");
                                    factura.NumeroFactura = reader.GetString("NO_DOCUMENTO");
                                    factura.Identification = reader.GetString("IDENTIFICACION_ADQUIRIENTE");
                                    factura.Nombre = reader["NOMBRE_ADQUIRIENTE"].ToString();
                                    factura.Movil = (long.TryParse(reader["MOVIL"].ToString(), out long number)) ? number : 0;
                                    factura.Plan = reader["PLAN_MOVIL"].ToString();
                                    factura.NumeroCuenta = reader["NUMERO_CUENTA"].ToString();
                                    factura.FechaFacturacion = reader["FECHA_FACTURA"].ToString();
                                    factura.FechaVencimiento = reader["FECHA_VENCIMIENTO"].ToString();
                                    factura.UrlPdf = reader["URL"].ToString();
                                    factura.TipoArchivo = (int.TryParse(reader["ID_TIPO_ARCHIVO"].ToString(), out int code)) ? code : -1;
                                    factura.EsMigrado = (int.TryParse(reader["IS_MIGRADO"].ToString(), out int flag)) ? flag : 0;

                                    factura.Documentos = new Dictionary<string, string>();
                                    factura.Documentos.Add(TipoArchivo.xml.ToString(), Utilities.GetUrlParametros(idObligado, factura, TipoArchivo.xml));
                                    IPHostEntry host;
                                    string localIP = "";
                                    host = Dns.GetHostEntry(Dns.GetHostName());
                                    foreach (IPAddress ip in host.AddressList)
                                    {
                                        if (ip.AddressFamily.ToString() == "InterNetwork")
                                        {
                                            localIP = ip.ToString();
                                        }
                                    }
                                    //InsertarTrazabilidad(parametros.TipoCliente, parametros.TipoDocumento, parametros.Contrato, factura.NumeroFactura, factura.Identification, factura.NumeroCuenta,
                                    //    factura.FechaFacturacion, "", factura.Movil);

                                    if (!string.IsNullOrWhiteSpace(factura.UrlPdf))
                                    {
                                        factura.Documentos.Add(TipoArchivo.pdf.ToString(), Utilities.GetUrlParametros(idObligado, factura, TipoArchivo.pdf));
                                    }
                                    else
                                    {
                                        factura.Documentos.Add(TipoArchivo.pdf.ToString(), "");
                                    }
                                    if (factura.NumeroFactura.Contains("BICT")){

                                    }
                                    else {
                                        facturas.Add(factura);
                                    }
                                }
                            }
                        }
                    }

                }
                catch (Exception ex)
                {
                    // Captura y propaga la excepción
                    throw ex;
                }
                finally
                {
                    //Cierra la conexion
                    connection.Close();
                }
            }

            return facturas;
        }


        public List<RespuestaFijo> GetFacturasFijo(List<long> obligados, Parametros parametros)
        {
            List<RespuestaFijo> facturas = new List<RespuestaFijo>();
            Dictionary<string, string> filters = new Dictionary<string, string>();

            //Crea la conexion
            using (var connection = new SqlConnection(DefaultDatabaseStringConnection))
            {
                try
                {
                    //Abre la conexion
                    connection.Open();


                    if ((parametros.Identification != null && parametros.Contrato != null) || (parametros.Identification == ""))

                    {

                        //Crea la consulta
                        StringBuilder query = new StringBuilder("SELECT DF.IDENTIFICACION_OBLIGADO, DF.NUM_RESOLU_DIAN, DF.NO_DOCUMENTO, DF.IDENTIFICACION_ADQUIRIENTE, DF.NOMBRE_ADQUIRIENTE, ")
                                                        .Append("DF.CAMPO_ALMACENAMIENTO_1 AS CONTRATO, DF.CAMPO_ALMACENAMIENTO_2  AS REFERENCIA_PAGO, ")
                                                        .Append("FORMAT(DF.FECHA_FACTURA, 'yyyy-MM-dd') AS FECHA_FACTURA, ")
                                                        .Append("FORMAT(DF.FECHA_VENCIMIENTO, 'yyyy-MM-dd') AS FECHA_VENCIMIENTO, ")
                                                        .Append("UF.URL, UF.ID_TIPO_ARCHIVO, UF.IS_MIGRADO ")
                                                        .Append("FROM FELECV4.DATOS_FACTURA DF ")
                                                        .Append("FULL OUTER JOIN FELECV4.AUDITORIA_FACTURA AF ")
                                                        .Append("ON DF.IDENTIFICACION_OBLIGADO = AF.IDENTIFICACION_OBLIGADO AND DF.NO_DOCUMENTO = AF.NO_DOCUMENTO AND DF.NUM_RESOLU_DIAN = AF.NUM_RESOLU_DIAN ")
                                                        .Append("FULL OUTER JOIN FELECV4.URL_FACTURA UF ")
                                                        .Append("ON DF.IDENTIFICACION_OBLIGADO = UF.IDENTIFICACION_OBLIGADO AND DF.NO_DOCUMENTO = UF.NO_DOCUMENTO AND DF.NUM_RESOLU_DIAN = UF.NUM_RESOLU_DIAN AND DF.IDENTIFICACION_ADQUIRIENTE = UF.IDENTIFICACION_ADQUIRIENTE ")
                                                        .Append("WHERE DF.IDENTIFICACION_OBLIGADO IN (@obligado)  AND DF.FECHA_FACTURA >= @fechaInicio AND DF.FECHA_FACTURA <= @fechaFin ")
                                                        .Append("AND AF.CODIGO_RTA_WS_DIAN = @codigoDian ");


                        if (obligados?.Count > 0)
                        {
                            string inObligados = string.Join(", ", obligados);
                            query.Replace("@obligado", inObligados);
                        }
                        else
                        {
                            throw new Exception("La consulta no puede realizarse sin la identificacion de al menos un obligado");
                        }

                        //Se adicionan los filtros de búsqueda opcionales si contienen valor
                        /*if (!string.IsNullOrWhiteSpace(parametros.Identification))
                        {
                            query.Append("AND DF.IDENTIFICACION_ADQUIRIENTE = @identificacionAdquiriente ");
                            filters.Add("@identificacionAdquiriente", parametros.Identification);
                        }*/

                        if (!string.IsNullOrWhiteSpace(parametros.NumeroFactura))
                        {
                            query.Append("AND DF.NO_DOCUMENTO = @noDocumento ");
                            filters.Add("@noDocumento", parametros.NumeroFactura);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.Contrato))
                        {
                            query.Append("AND DF.CAMPO_ALMACENAMIENTO_1 = @contrato ");
                            filters.Add("@contrato", parametros.Contrato);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.ReferenciaPago))
                        {
                            query.Append("AND DF.CAMPO_ALMACENAMIENTO_2 = @referenciaPago ");
                            filters.Add("@referenciaPago", parametros.ReferenciaPago);
                        }

                        //Se ordenan los resultados de la consulta por fecha de factura
                        query.Append("ORDER BY DF.FECHA_FACTURA DESC, DF.HORA_FACTURA DESC ");


                        //Realiza la consulta en la BD 
                        using (SqlCommand cmd = new SqlCommand(query.ToString(), connection))
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.CommandText = query.ToString();

                            cmd.Parameters.Add("@fechaInicio", SqlDbType.Date).Value = parametros.FechaInicio;
                            cmd.Parameters.Add("@fechaFin", SqlDbType.Date).Value = parametros.FechaFin;
                            cmd.Parameters.Add("@identificacionAdquiriente", SqlDbType.VarChar).Value = parametros.Identification;
                            cmd.Parameters.Add("@codigoDian", SqlDbType.SmallInt).Value = CodigoDian;

                            foreach (var filter in filters)
                            {
                                cmd.Parameters.Add(filter.Key, SqlDbType.VarChar).Value = filter.Value;
                            }

                            using (var reader = cmd.ExecuteReader())
                            {
                                RespuestaFijo factura;
                                long idObligado;
                                while (reader.Read())
                                {
                                    factura = new RespuestaFijo();
                                    factura.TipoDocumento = parametros.TipoDocumento;
                                    idObligado = reader.GetInt64("IDENTIFICACION_OBLIGADO");
                                    factura.NumeroResolucion = reader.GetInt64("NUM_RESOLU_DIAN");
                                    factura.NumeroFactura = reader.GetString("NO_DOCUMENTO");
                                    factura.Identification = reader.GetString("IDENTIFICACION_ADQUIRIENTE");
                                    factura.Nombre = reader["NOMBRE_ADQUIRIENTE"].ToString();
                                    factura.Contrato = reader["CONTRATO"].ToString();
                                    factura.ReferenciaPago = reader["REFERENCIA_PAGO"].ToString();
                                    factura.FechaFacturacion = reader["FECHA_FACTURA"].ToString();
                                    factura.FechaVencimiento = reader["FECHA_VENCIMIENTO"].ToString();
                                    factura.UrlPdf = reader["URL"].ToString();
                                    factura.TipoArchivo = (int.TryParse(reader["ID_TIPO_ARCHIVO"].ToString(), out int code)) ? code : -1;
                                    factura.EsMigrado = (int.TryParse(reader["IS_MIGRADO"].ToString(), out int flag)) ? flag : 0;

                                    factura.Documentos = new Dictionary<string, string>();

                                    factura.Documentos.Add(TipoArchivo.xml.ToString(), Utilities.GetUrlParametros(idObligado, factura, TipoArchivo.xml));


                                    //InsertarTrazabilidad(parametros.TipoCliente, parametros.TipoDocumento, factura.Contrato, factura.NumeroFactura, factura.Identification, "", factura.FechaFacturacion,
                                    //    factura.ReferenciaPago, 0);

                                    if (!string.IsNullOrWhiteSpace(factura.UrlPdf))
                                    {
                                        factura.Documentos.Add(TipoArchivo.pdf.ToString(), Utilities.GetUrlParametros(idObligado, factura, TipoArchivo.pdf));
                                    }
                                    else
                                    {
                                        factura.Documentos.Add(TipoArchivo.pdf.ToString(), "");
                                    }

                                    facturas.Add(factura);
                                }
                            }
                        }
                    }
                   
                    else if ((parametros.Identification != null && parametros.NumeroFactura != null )|| (parametros.Identification == ""))

                    {

                        //Crea la consulta
                        StringBuilder query = new StringBuilder("SELECT DF.IDENTIFICACION_OBLIGADO, DF.NUM_RESOLU_DIAN, DF.NO_DOCUMENTO, DF.IDENTIFICACION_ADQUIRIENTE, DF.NOMBRE_ADQUIRIENTE, ")
                                                        .Append("DF.CAMPO_ALMACENAMIENTO_1 AS CONTRATO, DF.CAMPO_ALMACENAMIENTO_2  AS REFERENCIA_PAGO, ")
                                                        .Append("FORMAT(DF.FECHA_FACTURA, 'yyyy-MM-dd') AS FECHA_FACTURA, ")
                                                        .Append("FORMAT(DF.FECHA_VENCIMIENTO, 'yyyy-MM-dd') AS FECHA_VENCIMIENTO, ")
                                                        .Append("UF.URL, UF.ID_TIPO_ARCHIVO, UF.IS_MIGRADO ")
                                                        .Append("FROM FELECV4.DATOS_FACTURA DF ")
                                                        .Append("FULL OUTER JOIN FELECV4.AUDITORIA_FACTURA AF ")
                                                        .Append("ON DF.IDENTIFICACION_OBLIGADO = AF.IDENTIFICACION_OBLIGADO AND DF.NO_DOCUMENTO = AF.NO_DOCUMENTO AND DF.NUM_RESOLU_DIAN = AF.NUM_RESOLU_DIAN ")
                                                        .Append("FULL OUTER JOIN FELECV4.URL_FACTURA UF ")
                                                        .Append("ON DF.IDENTIFICACION_OBLIGADO = UF.IDENTIFICACION_OBLIGADO AND DF.NO_DOCUMENTO = UF.NO_DOCUMENTO AND DF.NUM_RESOLU_DIAN = UF.NUM_RESOLU_DIAN AND DF.IDENTIFICACION_ADQUIRIENTE = UF.IDENTIFICACION_ADQUIRIENTE ")
                                                        .Append("WHERE DF.IDENTIFICACION_OBLIGADO IN (@obligado)  AND DF.FECHA_FACTURA >= @fechaInicio AND DF.FECHA_FACTURA <= @fechaFin ")
                                                        .Append("AND AF.CODIGO_RTA_WS_DIAN = @codigoDian ");


                        if (obligados?.Count > 0)
                        {
                            string inObligados = string.Join(", ", obligados);
                            query.Replace("@obligado", inObligados);
                        }
                        else
                        {
                            throw new Exception("La consulta no puede realizarse sin la identificacion de al menos un obligado");
                        }

                        //Se adicionan los filtros de búsqueda opcionales si contienen valor
                        /*if (!string.IsNullOrWhiteSpace(parametros.Identification))
                        {
                            query.Append("AND DF.IDENTIFICACION_ADQUIRIENTE = @identificacionAdquiriente ");
                            filters.Add("@identificacionAdquiriente", parametros.Identification);
                        }*/

                        if (!string.IsNullOrWhiteSpace(parametros.NumeroFactura))
                        {
                            query.Append("AND DF.NO_DOCUMENTO = @noDocumento ");
                            filters.Add("@noDocumento", parametros.NumeroFactura);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.Contrato))
                        {
                            query.Append("AND DF.CAMPO_ALMACENAMIENTO_1 = @contrato ");
                            filters.Add("@contrato", parametros.Contrato);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.ReferenciaPago))
                        {
                            query.Append("AND DF.CAMPO_ALMACENAMIENTO_2 = @referenciaPago ");
                            filters.Add("@referenciaPago", parametros.ReferenciaPago);
                        }

                        //Se ordenan los resultados de la consulta por fecha de factura
                        query.Append("ORDER BY DF.FECHA_FACTURA DESC, DF.HORA_FACTURA DESC ");


                        //Realiza la consulta en la BD 
                        using (SqlCommand cmd = new SqlCommand(query.ToString(), connection))
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.CommandText = query.ToString();

                            cmd.Parameters.Add("@fechaInicio", SqlDbType.Date).Value = parametros.FechaInicio;
                            cmd.Parameters.Add("@fechaFin", SqlDbType.Date).Value = parametros.FechaFin;
                            cmd.Parameters.Add("@identificacionAdquiriente", SqlDbType.VarChar).Value = parametros.Identification;
                            cmd.Parameters.Add("@codigoDian", SqlDbType.SmallInt).Value = CodigoDian;

                            foreach (var filter in filters)
                            {
                                cmd.Parameters.Add(filter.Key, SqlDbType.VarChar).Value = filter.Value;
                            }

                            using (var reader = cmd.ExecuteReader())
                            {
                                RespuestaFijo factura;
                                long idObligado;
                                while (reader.Read())
                                {
                                    factura = new RespuestaFijo();
                                    factura.TipoDocumento = parametros.TipoDocumento;
                                    idObligado = reader.GetInt64("IDENTIFICACION_OBLIGADO");
                                    factura.NumeroResolucion = reader.GetInt64("NUM_RESOLU_DIAN");
                                    factura.NumeroFactura = reader.GetString("NO_DOCUMENTO");
                                    factura.Identification = reader.GetString("IDENTIFICACION_ADQUIRIENTE");
                                    factura.Nombre = reader["NOMBRE_ADQUIRIENTE"].ToString();
                                    factura.Contrato = reader["CONTRATO"].ToString();
                                    factura.ReferenciaPago = reader["REFERENCIA_PAGO"].ToString();
                                    factura.FechaFacturacion = reader["FECHA_FACTURA"].ToString();
                                    factura.FechaVencimiento = reader["FECHA_VENCIMIENTO"].ToString();
                                    factura.UrlPdf = reader["URL"].ToString();
                                    factura.TipoArchivo = (int.TryParse(reader["ID_TIPO_ARCHIVO"].ToString(), out int code)) ? code : -1;
                                    factura.EsMigrado = (int.TryParse(reader["IS_MIGRADO"].ToString(), out int flag)) ? flag : 0;

                                    factura.Documentos = new Dictionary<string, string>();

                                    factura.Documentos.Add(TipoArchivo.xml.ToString(), Utilities.GetUrlParametros(idObligado, factura, TipoArchivo.xml));


                                    //InsertarTrazabilidad(parametros.TipoCliente, parametros.TipoDocumento, factura.Contrato, factura.NumeroFactura, factura.Identification, "", factura.FechaFacturacion,
                                    //    factura.ReferenciaPago, 0);

                                    if (!string.IsNullOrWhiteSpace(factura.UrlPdf))
                                    {
                                        factura.Documentos.Add(TipoArchivo.pdf.ToString(), Utilities.GetUrlParametros(idObligado, factura, TipoArchivo.pdf));
                                    }
                                    else
                                    {
                                        factura.Documentos.Add(TipoArchivo.pdf.ToString(), "");
                                    }

                                    facturas.Add(factura);
                                }
                            }
                        }
                    }
                    else
                    {
                        
                            

                            //Crea la consulta
                            StringBuilder query = new StringBuilder("SELECT DF.IDENTIFICACION_OBLIGADO, DF.NUM_RESOLU_DIAN, DF.NO_DOCUMENTO, DF.IDENTIFICACION_ADQUIRIENTE, DF.NOMBRE_ADQUIRIENTE, ")
                                                            .Append("DF.CAMPO_ALMACENAMIENTO_1 AS CONTRATO, DF.CAMPO_ALMACENAMIENTO_2  AS REFERENCIA_PAGO, ")
                                                            .Append("FORMAT(DF.FECHA_FACTURA, 'yyyy-MM-dd') AS FECHA_FACTURA, ")
                                                            .Append("FORMAT(DF.FECHA_VENCIMIENTO, 'yyyy-MM-dd') AS FECHA_VENCIMIENTO, ")
                                                            .Append("UF.URL, UF.ID_TIPO_ARCHIVO, UF.IS_MIGRADO ")
                                                            .Append("FROM FELECV4.DATOS_FACTURA DF ")
                                                            .Append("FULL OUTER JOIN FELECV4.AUDITORIA_FACTURA AF ")
                                                            .Append("ON DF.IDENTIFICACION_OBLIGADO = AF.IDENTIFICACION_OBLIGADO AND DF.NO_DOCUMENTO = AF.NO_DOCUMENTO AND DF.NUM_RESOLU_DIAN = AF.NUM_RESOLU_DIAN ")
                                                            .Append("FULL OUTER JOIN FELECV4.URL_FACTURA UF ")
                                                            .Append("ON DF.IDENTIFICACION_OBLIGADO = UF.IDENTIFICACION_OBLIGADO AND DF.NO_DOCUMENTO = UF.NO_DOCUMENTO AND DF.NUM_RESOLU_DIAN = UF.NUM_RESOLU_DIAN AND DF.IDENTIFICACION_ADQUIRIENTE = UF.IDENTIFICACION_ADQUIRIENTE ")
                                                            .Append("WHERE DF.IDENTIFICACION_OBLIGADO IN (@obligado) AND DF.IDENTIFICACION_ADQUIRIENTE = @identificacionAdquiriente AND DF.FECHA_FACTURA >= @fechaInicio AND DF.FECHA_FACTURA <= @fechaFin ")
                                                            .Append("AND AF.CODIGO_RTA_WS_DIAN = @codigoDian ");

                            if (obligados?.Count > 0)
                            {
                                string inObligados = string.Join(", ", obligados);
                                query.Replace("@obligado", inObligados);
                            }
                            else
                            {
                                throw new Exception("La consulta no puede realizarse sin la identificacion de al menos un obligado");
                            }

                            //Se adicionan los filtros de búsqueda opcionales si contienen valor
                            /*if (!string.IsNullOrWhiteSpace(parametros.Identification))
                            {
                                query.Append("AND DF.IDENTIFICACION_ADQUIRIENTE = @identificacionAdquiriente ");
                                filters.Add("@identificacionAdquiriente", parametros.Identification);
                            }*/

                            if (!string.IsNullOrWhiteSpace(parametros.NumeroFactura))
                            {
                                query.Append("AND DF.NO_DOCUMENTO = @noDocumento ");
                                filters.Add("@noDocumento", parametros.NumeroFactura);
                            }

                            if (!string.IsNullOrWhiteSpace(parametros.Contrato))
                            {
                                query.Append("AND DF.CAMPO_ALMACENAMIENTO_1 = @contrato ");
                                filters.Add("@contrato", parametros.Contrato);
                            }

                            if (!string.IsNullOrWhiteSpace(parametros.ReferenciaPago))
                            {
                                query.Append("AND DF.CAMPO_ALMACENAMIENTO_2 = @referenciaPago ");
                                filters.Add("@referenciaPago", parametros.ReferenciaPago);
                            }

                            //Se ordenan los resultados de la consulta por fecha de factura
                            query.Append("ORDER BY DF.FECHA_FACTURA DESC, DF.HORA_FACTURA DESC ");

                            //Realiza la consulta en la BD 
                            using (SqlCommand cmd = new SqlCommand(query.ToString(), connection))
                            {
                                cmd.CommandType = CommandType.Text;
                                cmd.CommandText = query.ToString();

                                cmd.Parameters.Add("@fechaInicio", SqlDbType.Date).Value = parametros.FechaInicio;
                                cmd.Parameters.Add("@fechaFin", SqlDbType.Date).Value = parametros.FechaFin;
                                cmd.Parameters.Add("@identificacionAdquiriente", SqlDbType.VarChar).Value = parametros.Identification;
                                cmd.Parameters.Add("@codigoDian", SqlDbType.SmallInt).Value = CodigoDian;

                                foreach (var filter in filters)
                                {
                                    cmd.Parameters.Add(filter.Key, SqlDbType.VarChar).Value = filter.Value;
                                }

                                using (var reader = cmd.ExecuteReader())
                                {
                                    RespuestaFijo factura;
                                    long idObligado;
                                    while (reader.Read())
                                    {
                                        factura = new RespuestaFijo();
                                        factura.TipoDocumento = parametros.TipoDocumento;
                                        idObligado = reader.GetInt64("IDENTIFICACION_OBLIGADO");
                                        factura.NumeroResolucion = reader.GetInt64("NUM_RESOLU_DIAN");
                                        factura.NumeroFactura = reader.GetString("NO_DOCUMENTO");
                                        factura.Identification = reader.GetString("IDENTIFICACION_ADQUIRIENTE");
                                        factura.Nombre = reader["NOMBRE_ADQUIRIENTE"].ToString();
                                        factura.Contrato = reader["CONTRATO"].ToString();
                                        factura.ReferenciaPago = reader["REFERENCIA_PAGO"].ToString();
                                        factura.FechaFacturacion = reader["FECHA_FACTURA"].ToString();
                                        factura.FechaVencimiento = reader["FECHA_VENCIMIENTO"].ToString();
                                        factura.UrlPdf = reader["URL"].ToString();
                                        factura.TipoArchivo = (int.TryParse(reader["ID_TIPO_ARCHIVO"].ToString(), out int code)) ? code : -1;
                                        factura.EsMigrado = (int.TryParse(reader["IS_MIGRADO"].ToString(), out int flag)) ? flag : 0;

                                        factura.Documentos = new Dictionary<string, string>();

                                        factura.Documentos.Add(TipoArchivo.xml.ToString(), Utilities.GetUrlParametros(idObligado, factura, TipoArchivo.xml));

                                        if (!string.IsNullOrWhiteSpace(factura.UrlPdf))
                                        {
                                            factura.Documentos.Add(TipoArchivo.pdf.ToString(), Utilities.GetUrlParametros(idObligado, factura, TipoArchivo.pdf));
                                        }
                                        else
                                        {
                                            factura.Documentos.Add(TipoArchivo.pdf.ToString(), "");
                                        }

                                        facturas.Add(factura);
                                    }
                                }
                            }
                        }
                       

                    


                }
                catch (Exception ex)
                {
                    // Captura y propaga la excepción
                    throw ex;
                }
                finally
                {
                    //Cierra la conexion
                    connection.Close();
                }
            }

            return facturas;
        }


   public List<RespuestaMovil> GetNotasMovil(List<long> obligados, Parametros parametros)
        {
            List<RespuestaMovil> notas = new List<RespuestaMovil>();
            Dictionary<string, string> filters = new Dictionary<string, string>();

            //Crea la conexion
            using (var connection = new SqlConnection(DefaultDatabaseStringConnection))
            {
                try
                {
                    //Abre la conexion
                    connection.Open();
                    if (parametros.NumeroCuenta != null && parametros.Identification != null)
                    {
                        //Crea la consulta
                        StringBuilder query = new StringBuilder("SELECT DD.IDENTIFICACION_OBLIGADO, DD.NO_DOCUMENTO, DD.IDENTIFICACION_ADQUIRIENTE, DD.NOMBRE_ADQUIRIENTE, ")
                                                   .Append("DD.CAMPO_ALMACENAMIENTO_1 AS MOVIL, DD.CAMPO_ALMACENAMIENTO_2 AS PLAN_MOVIL, DD.NUMERO_CUENTA, ")
                                                   .Append("FORMAT(DD.FECHA_DOCUMENTO, 'yyyy-MM-dd') AS FECHA_DOCUMENTO, ")
                                                   .Append("FORMAT(DD.FECHA_VENCIMIENTO, 'yyyy-MM-dd') AS FECHA_VENCIMIENTO, ")
                                                   .Append("UD.URL, UD.ID_TIPO_ARCHIVO, UD.IS_MIGRADO ")
                                                   .Append("FROM FELECV4.DATOS_DOC_ELECTRONICO DD ")
                                                   .Append("FULL OUTER JOIN FELECV4.AUDITORIA_DOC_ELECTRONICO AD ")
                                                   .Append("ON DD.IDENTIFICACION_OBLIGADO = AD.IDENTIFICACION_OBLIGADO AND DD.NO_DOCUMENTO = AD.NO_DOCUMENTO AND DD.ID_TIPO_DOCUMENTO = AD.ID_TIPO_DOCUMENTO ")
                                                   .Append("FULL OUTER JOIN FELECV4.URL_DOCUMENTO_ELECTRONICO UD ")
                                                   .Append("ON DD.IDENTIFICACION_OBLIGADO = UD.IDENTIFICACION_OBLIGADO AND DD.NO_DOCUMENTO = UD.NO_DOCUMENTO AND DD.ID_TIPO_DOCUMENTO = DD.ID_TIPO_DOCUMENTO AND DD.IDENTIFICACION_ADQUIRIENTE = UD.IDENTIFICACION_ADQUIRIENTE ")
                                                   .Append("WHERE DD.IDENTIFICACION_OBLIGADO IN (@obligado) AND DD.ID_TIPO_DOCUMENTO = @tipoDocumento AND DD.FECHA_DOCUMENTO >= @fechaInicio AND DD.FECHA_DOCUMENTO <= @fechaFin ")
                                                   .Append("AND AD.CODIGO_RTA_WS_DIAN = @codigoDian ");

                        if (obligados?.Count > 0)
                        {
                            string inObligados = string.Join(", ", obligados);
                            query.Replace("@obligado", inObligados);
                        }
                        else
                        {
                            throw new Exception("La consulta no puede realizarse sin la identificacion de al menos un obligado");
                        }

                        //Se adicionan los filtros de búsqueda opcionales si contienen valor
                        /*if (!string.IsNullOrWhiteSpace(parametros.Identification))
                        {
                            query.Append("AND DD.IDENTIFICACION_ADQUIRIENTE = @identificacionAdquiriente ");
                            filters.Add("@identificacionAdquiriente", parametros.Identification);
                        }*/

                        if (!string.IsNullOrWhiteSpace(parametros.NumeroFactura))
                        {
                            query.Append("AND DD.NO_DOCUMENTO = @noDocumento ");
                            filters.Add("@noDocumento", parametros.NumeroFactura);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.NumeroCuenta))
                        {
                            if (parametros.NumeroCuenta.Trim().Length > 10)
                            {
                                query.Append("AND DD.NUMERO_CUENTA IN (@numeroCuenta1, @numeroCuenta2) ");
                                filters.Add("@numeroCuenta1", parametros.NumeroCuenta.Trim()[0..^2]);
                                filters.Add("@numeroCuenta2", parametros.NumeroCuenta.Trim());
                            }
                            else
                            {
                                query.Append("AND DD.NUMERO_CUENTA = @numeroCuenta ");
                                filters.Add("@numeroCuenta", parametros.NumeroCuenta.Trim());
                            }
                        }

                        if (parametros.Movil != null && parametros.Movil != 0)
                        {
                            query.Append("AND DD.CAMPO_ALMACENAMIENTO_1 = @movil ");
                            filters.Add("@movil", parametros.Movil.ToString());
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.Plan))
                        {
                            query.Append("AND DD.CAMPO_ALMACENAMIENTO_2 = @plan ");
                            filters.Add("@plan", parametros.Plan);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.Ciclo))
                        {
                            query.Append("AND AD.CICLO = @ciclo ");
                            filters.Add("@ciclo", parametros.Ciclo);
                        }

                        //Se ordenan los resultados de la consulta por fecha de factura
                        query.Append("ORDER BY DD.FECHA_DOCUMENTO DESC, DD.HORA_DOCUMENTO DESC ");

                        //Realiza la consulta en la BD 
                        using (SqlCommand cmd = new SqlCommand(query.ToString(), connection))
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.CommandText = query.ToString();

                            cmd.Parameters.Add("@tipoDocumento", SqlDbType.VarChar).Value = (parametros.TipoDocumento == TipoDocumento.NOTA_CREDITO.ToString()) ? IdTipoDocumentoNotaCredito : IdTipoDocumentoNotaDebito;
                            cmd.Parameters.Add("@identificacionAdquiriente", SqlDbType.VarChar).Value = parametros.Identification;
                            cmd.Parameters.Add("@fechaInicio", SqlDbType.Date).Value = parametros.FechaInicio;
                            cmd.Parameters.Add("@fechaFin", SqlDbType.Date).Value = parametros.FechaFin;
                            cmd.Parameters.Add("@codigoDian", SqlDbType.SmallInt).Value = CodigoDian;

                            foreach (var filter in filters)
                            {
                                cmd.Parameters.Add(filter.Key, SqlDbType.VarChar).Value = filter.Value;
                            }

                            using (var reader = cmd.ExecuteReader())
                            {
                                RespuestaMovil nota;
                                long idObligado;
                                while (reader.Read())
                                {
                                    nota = new RespuestaMovil();
                                    nota.TipoDocumento = parametros.TipoDocumento;
                                    idObligado = reader.GetInt64("IDENTIFICACION_OBLIGADO");
                                    nota.NumeroResolucion = 0;
                                    nota.NumeroFactura = reader.GetString("NO_DOCUMENTO");
                                    nota.Identification = reader.GetString("IDENTIFICACION_ADQUIRIENTE");
                                    nota.Nombre = reader["NOMBRE_ADQUIRIENTE"].ToString();
                                    nota.Movil = (long.TryParse(reader["MOVIL"].ToString(), out long number)) ? number : 0;
                                    nota.Plan = reader["PLAN_MOVIL"].ToString();
                                    nota.NumeroCuenta = reader["NUMERO_CUENTA"].ToString();
                                    nota.FechaFacturacion = reader["FECHA_DOCUMENTO"].ToString();
                                    nota.FechaVencimiento = reader["FECHA_VENCIMIENTO"].ToString();
                                    nota.UrlPdf = reader["URL"].ToString();
                                    nota.TipoArchivo = (int.TryParse(reader["ID_TIPO_ARCHIVO"].ToString(), out int code)) ? code : -1;
                                    nota.EsMigrado = (int.TryParse(reader["IS_MIGRADO"].ToString(), out int flag)) ? flag : 0;

                                    nota.Documentos = new Dictionary<string, string>();
                                    nota.Documentos.Add(TipoArchivo.xml.ToString(), Utilities.GetUrlParametros(idObligado, nota, TipoArchivo.xml));

                                    InsertarTrazabilidad(parametros.TipoCliente, parametros.TipoDocumento, "", nota.NumeroFactura, nota.Identification, nota.NumeroCuenta, nota.FechaFacturacion,
                                       "", nota.Movil);

                                    if (!string.IsNullOrWhiteSpace(nota.UrlPdf))
                                    {
                                        nota.Documentos.Add(TipoArchivo.pdf.ToString(), Utilities.GetUrlParametros(idObligado, nota, TipoArchivo.pdf));
                                    }
                                    else
                                    {
                                        nota.Documentos.Add(TipoArchivo.pdf.ToString(), "");
                                    }

                                    notas.Add(nota);
                                }
                            }
                        }
                    }

                    else if (parametros.NumeroFactura != null && parametros.Identification != null)
                    {
                        //Crea la consulta
                        StringBuilder query = new StringBuilder("SELECT DD.IDENTIFICACION_OBLIGADO, DD.NO_DOCUMENTO, DD.IDENTIFICACION_ADQUIRIENTE, DD.NOMBRE_ADQUIRIENTE, ")
                                                    .Append("DD.CAMPO_ALMACENAMIENTO_1 AS MOVIL, DD.CAMPO_ALMACENAMIENTO_2 AS PLAN_MOVIL, DD.NUMERO_CUENTA, ")
                                                    .Append("FORMAT(DD.FECHA_DOCUMENTO, 'yyyy-MM-dd') AS FECHA_DOCUMENTO, ")
                                                    .Append("FORMAT(DD.FECHA_VENCIMIENTO, 'yyyy-MM-dd') AS FECHA_VENCIMIENTO, ")
                                                    .Append("UD.URL, UD.ID_TIPO_ARCHIVO, UD.IS_MIGRADO ")
                                                    .Append("FROM FELECV4.DATOS_DOC_ELECTRONICO DD ")
                                                    .Append("FULL OUTER JOIN FELECV4.AUDITORIA_DOC_ELECTRONICO AD ")
                                                    .Append("ON DD.IDENTIFICACION_OBLIGADO = AD.IDENTIFICACION_OBLIGADO AND DD.NO_DOCUMENTO = AD.NO_DOCUMENTO AND DD.ID_TIPO_DOCUMENTO = AD.ID_TIPO_DOCUMENTO ")
                                                    .Append("FULL OUTER JOIN FELECV4.URL_DOCUMENTO_ELECTRONICO UD ")
                                                    .Append("ON DD.IDENTIFICACION_OBLIGADO = UD.IDENTIFICACION_OBLIGADO AND DD.NO_DOCUMENTO = UD.NO_DOCUMENTO AND DD.ID_TIPO_DOCUMENTO = DD.ID_TIPO_DOCUMENTO AND DD.IDENTIFICACION_ADQUIRIENTE = UD.IDENTIFICACION_ADQUIRIENTE ")
                                                    .Append("WHERE DD.IDENTIFICACION_OBLIGADO IN (@obligado) AND DD.ID_TIPO_DOCUMENTO = @tipoDocumento AND DD.FECHA_DOCUMENTO >= @fechaInicio AND DD.FECHA_DOCUMENTO <= @fechaFin ")
                                                    .Append("AND AD.CODIGO_RTA_WS_DIAN = @codigoDian ");

                        if (obligados?.Count > 0)
                        {
                            string inObligados = string.Join(", ", obligados);
                            query.Replace("@obligado", inObligados);
                        }
                        else
                        {
                            throw new Exception("La consulta no puede realizarse sin la identificacion de al menos un obligado");
                        }

                        //Se adicionan los filtros de búsqueda opcionales si contienen valor
                        /*if (!string.IsNullOrWhiteSpace(parametros.Identification))
                        {
                            query.Append("AND DD.IDENTIFICACION_ADQUIRIENTE = @identificacionAdquiriente ");
                            filters.Add("@identificacionAdquiriente", parametros.Identification);
                        }*/

                        if (!string.IsNullOrWhiteSpace(parametros.NumeroFactura))
                        {
                            query.Append("AND DD.NO_DOCUMENTO = @noDocumento ");
                            filters.Add("@noDocumento", parametros.NumeroFactura);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.NumeroCuenta))
                        {
                            if (parametros.NumeroCuenta.Trim().Length > 10)
                            {
                                query.Append("AND DD.NUMERO_CUENTA IN (@numeroCuenta1, @numeroCuenta2) ");
                                filters.Add("@numeroCuenta1", parametros.NumeroCuenta.Trim()[0..^2]);
                                filters.Add("@numeroCuenta2", parametros.NumeroCuenta.Trim());
                            }
                            else
                            {
                                query.Append("AND DD.NUMERO_CUENTA = @numeroCuenta ");
                                filters.Add("@numeroCuenta", parametros.NumeroCuenta.Trim());
                            }
                        }

                        if (parametros.Movil != null && parametros.Movil != 0)
                        {
                            query.Append("AND DD.CAMPO_ALMACENAMIENTO_1 = @movil ");
                            filters.Add("@movil", parametros.Movil.ToString());
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.Plan))
                        {
                            query.Append("AND DD.CAMPO_ALMACENAMIENTO_2 = @plan ");
                            filters.Add("@plan", parametros.Plan);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.Ciclo))
                        {
                            query.Append("AND AD.CICLO = @ciclo ");
                            filters.Add("@ciclo", parametros.Ciclo);
                        }

                        //Se ordenan los resultados de la consulta por fecha de factura
                        query.Append("ORDER BY DD.FECHA_DOCUMENTO DESC, DD.HORA_DOCUMENTO DESC ");

                        //Realiza la consulta en la BD 
                        using (SqlCommand cmd = new SqlCommand(query.ToString(), connection))
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.CommandText = query.ToString();

                            cmd.Parameters.Add("@tipoDocumento", SqlDbType.VarChar).Value = (parametros.TipoDocumento == TipoDocumento.NOTA_CREDITO.ToString()) ? IdTipoDocumentoNotaCredito : IdTipoDocumentoNotaDebito;
                            cmd.Parameters.Add("@identificacionAdquiriente", SqlDbType.VarChar).Value = parametros.Identification;
                            cmd.Parameters.Add("@fechaInicio", SqlDbType.Date).Value = parametros.FechaInicio;
                            cmd.Parameters.Add("@fechaFin", SqlDbType.Date).Value = parametros.FechaFin;
                            cmd.Parameters.Add("@codigoDian", SqlDbType.SmallInt).Value = CodigoDian;

                            foreach (var filter in filters)
                            {
                                cmd.Parameters.Add(filter.Key, SqlDbType.VarChar).Value = filter.Value;
                            }

                            using (var reader = cmd.ExecuteReader())
                            {
                                RespuestaMovil nota;
                                long idObligado;
                                while (reader.Read())
                                {
                                    nota = new RespuestaMovil();
                                    nota.TipoDocumento = parametros.TipoDocumento;
                                    idObligado = reader.GetInt64("IDENTIFICACION_OBLIGADO");
                                    nota.NumeroResolucion = 0;
                                    nota.NumeroFactura = reader.GetString("NO_DOCUMENTO");
                                    nota.Identification = reader.GetString("IDENTIFICACION_ADQUIRIENTE");
                                    nota.Nombre = reader["NOMBRE_ADQUIRIENTE"].ToString();
                                    nota.Movil = (long.TryParse(reader["MOVIL"].ToString(), out long number)) ? number : 0;
                                    nota.Plan = reader["PLAN_MOVIL"].ToString();
                                    nota.NumeroCuenta = reader["NUMERO_CUENTA"].ToString();
                                    nota.FechaFacturacion = reader["FECHA_DOCUMENTO"].ToString();
                                    nota.FechaVencimiento = reader["FECHA_VENCIMIENTO"].ToString();
                                    nota.UrlPdf = reader["URL"].ToString();
                                    nota.TipoArchivo = (int.TryParse(reader["ID_TIPO_ARCHIVO"].ToString(), out int code)) ? code : -1;
                                    nota.EsMigrado = (int.TryParse(reader["IS_MIGRADO"].ToString(), out int flag)) ? flag : 0;

                                    nota.Documentos = new Dictionary<string, string>();
                                    nota.Documentos.Add(TipoArchivo.xml.ToString(), Utilities.GetUrlParametros(idObligado, nota, TipoArchivo.xml));

                                    InsertarTrazabilidad(parametros.TipoCliente, parametros.TipoDocumento, "", nota.NumeroFactura, nota.Identification, nota.NumeroCuenta, nota.FechaFacturacion,
                                       "", nota.Movil);

                                    if (!string.IsNullOrWhiteSpace(nota.UrlPdf))
                                    {
                                        nota.Documentos.Add(TipoArchivo.pdf.ToString(), Utilities.GetUrlParametros(idObligado, nota, TipoArchivo.pdf));
                                    }
                                    else
                                    {
                                        nota.Documentos.Add(TipoArchivo.pdf.ToString(), "");
                                    }

                                    notas.Add(nota);
                                }
                            }
                        }
                    }

                    else 
                    {
                        //Crea la consulta
                        StringBuilder query = new StringBuilder("SELECT DD.IDENTIFICACION_OBLIGADO, DD.NO_DOCUMENTO, DD.IDENTIFICACION_ADQUIRIENTE, DD.NOMBRE_ADQUIRIENTE, ")
                                                    .Append("DD.CAMPO_ALMACENAMIENTO_1 AS MOVIL, DD.CAMPO_ALMACENAMIENTO_2 AS PLAN_MOVIL, DD.NUMERO_CUENTA, ")
                                                    .Append("FORMAT(DD.FECHA_DOCUMENTO, 'yyyy-MM-dd') AS FECHA_DOCUMENTO, ")
                                                    .Append("FORMAT(DD.FECHA_VENCIMIENTO, 'yyyy-MM-dd') AS FECHA_VENCIMIENTO, ")
                                                    .Append("UD.URL, UD.ID_TIPO_ARCHIVO, UD.IS_MIGRADO ")
                                                    .Append("FROM FELECV4.DATOS_DOC_ELECTRONICO DD ")
                                                    .Append("FULL OUTER JOIN FELECV4.AUDITORIA_DOC_ELECTRONICO AD ")
                                                    .Append("ON DD.IDENTIFICACION_OBLIGADO = AD.IDENTIFICACION_OBLIGADO AND DD.NO_DOCUMENTO = AD.NO_DOCUMENTO AND DD.ID_TIPO_DOCUMENTO = AD.ID_TIPO_DOCUMENTO ")
                                                    .Append("FULL OUTER JOIN FELECV4.URL_DOCUMENTO_ELECTRONICO UD ")
                                                    .Append("ON DD.IDENTIFICACION_OBLIGADO = UD.IDENTIFICACION_OBLIGADO AND DD.NO_DOCUMENTO = UD.NO_DOCUMENTO AND DD.ID_TIPO_DOCUMENTO = DD.ID_TIPO_DOCUMENTO AND DD.IDENTIFICACION_ADQUIRIENTE = UD.IDENTIFICACION_ADQUIRIENTE ")
                                                    .Append("WHERE DD.IDENTIFICACION_OBLIGADO IN (@obligado) AND DD.ID_TIPO_DOCUMENTO = @tipoDocumento AND DD.IDENTIFICACION_ADQUIRIENTE = @identificacionAdquiriente ")
                                                    .Append("AND DD.FECHA_DOCUMENTO >= @fechaInicio AND DD.FECHA_DOCUMENTO <= @fechaFin AND AD.CODIGO_RTA_WS_DIAN = @codigoDian ");

                        if (obligados?.Count > 0)
                        {
                            string inObligados = string.Join(", ", obligados);
                            query.Replace("@obligado", inObligados);
                        }
                        else
                        {
                            throw new Exception("La consulta no puede realizarse sin la identificacion de al menos un obligado");
                        }

                        //Se adicionan los filtros de búsqueda opcionales si contienen valor
                        /*if (!string.IsNullOrWhiteSpace(parametros.Identification))
                        {
                            query.Append("AND DD.IDENTIFICACION_ADQUIRIENTE = @identificacionAdquiriente ");
                            filters.Add("@identificacionAdquiriente", parametros.Identification);
                        }*/

                        if (!string.IsNullOrWhiteSpace(parametros.NumeroFactura))
                        {
                            query.Append("AND DD.NO_DOCUMENTO = @noDocumento ");
                            filters.Add("@noDocumento", parametros.NumeroFactura);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.NumeroCuenta))
                        {
                            if (parametros.NumeroCuenta.Trim().Length > 10)
                            {
                                query.Append("AND DD.NUMERO_CUENTA IN (@numeroCuenta1, @numeroCuenta2) ");
                                filters.Add("@numeroCuenta1", parametros.NumeroCuenta.Trim()[0..^2]);
                                filters.Add("@numeroCuenta2", parametros.NumeroCuenta.Trim());
                            }
                            else
                            {
                                query.Append("AND DD.NUMERO_CUENTA = @numeroCuenta ");
                                filters.Add("@numeroCuenta", parametros.NumeroCuenta.Trim());
                            }
                        }

                        if (parametros.Movil != null && parametros.Movil != 0)
                        {
                            query.Append("AND DD.CAMPO_ALMACENAMIENTO_1 = @movil ");
                            filters.Add("@movil", parametros.Movil.ToString());
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.Plan))
                        {
                            query.Append("AND DD.CAMPO_ALMACENAMIENTO_2 = @plan ");
                            filters.Add("@plan", parametros.Plan);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.Ciclo))
                        {
                            query.Append("AND AD.CICLO = @ciclo ");
                            filters.Add("@ciclo", parametros.Ciclo);
                        }

                        //Se ordenan los resultados de la consulta por fecha de factura
                        query.Append("ORDER BY DD.FECHA_DOCUMENTO DESC, DD.HORA_DOCUMENTO DESC ");

                        //Realiza la consulta en la BD 
                        using (SqlCommand cmd = new SqlCommand(query.ToString(), connection))
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.CommandText = query.ToString();

                            cmd.Parameters.Add("@tipoDocumento", SqlDbType.VarChar).Value = (parametros.TipoDocumento == TipoDocumento.NOTA_CREDITO.ToString()) ? IdTipoDocumentoNotaCredito : IdTipoDocumentoNotaDebito;
                            cmd.Parameters.Add("@identificacionAdquiriente", SqlDbType.VarChar).Value = parametros.Identification;
                            cmd.Parameters.Add("@fechaInicio", SqlDbType.Date).Value = parametros.FechaInicio;
                            cmd.Parameters.Add("@fechaFin", SqlDbType.Date).Value = parametros.FechaFin;
                            cmd.Parameters.Add("@codigoDian", SqlDbType.SmallInt).Value = CodigoDian;

                            foreach (var filter in filters)
                            {
                                cmd.Parameters.Add(filter.Key, SqlDbType.VarChar).Value = filter.Value;
                            }

                            using (var reader = cmd.ExecuteReader())
                            {
                                RespuestaMovil nota;
                                long idObligado;
                                while (reader.Read())
                                {
                                    nota = new RespuestaMovil();
                                    nota.TipoDocumento = parametros.TipoDocumento;
                                    idObligado = reader.GetInt64("IDENTIFICACION_OBLIGADO");
                                    nota.NumeroResolucion = 0;
                                    nota.NumeroFactura = reader.GetString("NO_DOCUMENTO");
                                    nota.Identification = reader.GetString("IDENTIFICACION_ADQUIRIENTE");
                                    nota.Nombre = reader["NOMBRE_ADQUIRIENTE"].ToString();
                                    nota.Movil = (long.TryParse(reader["MOVIL"].ToString(), out long number)) ? number : 0;
                                    nota.Plan = reader["PLAN_MOVIL"].ToString();
                                    nota.NumeroCuenta = reader["NUMERO_CUENTA"].ToString();
                                    nota.FechaFacturacion = reader["FECHA_DOCUMENTO"].ToString();
                                    nota.FechaVencimiento = reader["FECHA_VENCIMIENTO"].ToString();
                                    nota.UrlPdf = reader["URL"].ToString();
                                    nota.TipoArchivo = (int.TryParse(reader["ID_TIPO_ARCHIVO"].ToString(), out int code)) ? code : -1;
                                    nota.EsMigrado = (int.TryParse(reader["IS_MIGRADO"].ToString(), out int flag)) ? flag : 0;

                                    nota.Documentos = new Dictionary<string, string>();
                                    nota.Documentos.Add(TipoArchivo.xml.ToString(), Utilities.GetUrlParametros(idObligado, nota, TipoArchivo.xml));

                                    InsertarTrazabilidad(parametros.TipoCliente, parametros.TipoDocumento, "", nota.NumeroFactura, nota.Identification, nota.NumeroCuenta, nota.FechaFacturacion,
                                    "", nota.Movil);

                                    if (!string.IsNullOrWhiteSpace(nota.UrlPdf))
                                    {
                                        nota.Documentos.Add(TipoArchivo.pdf.ToString(), Utilities.GetUrlParametros(idObligado, nota, TipoArchivo.pdf));
                                    }
                                    else
                                    {
                                        nota.Documentos.Add(TipoArchivo.pdf.ToString(), "");
                                    }

                                    notas.Add(nota);
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Captura y propaga la excepción
                    throw ex;
                }
                finally
                {
                    //Cierra la conexion
                    connection.Close();
                }
            }

            return notas;
        }


        public List<RespuestaFijo> GetNotasFijo(List<long> obligados, Parametros parametros)
        {
            List<RespuestaFijo> notas = new List<RespuestaFijo>();
            Dictionary<string, string> filters = new Dictionary<string, string>();

            //Crea la conexion
            using (var connection = new SqlConnection(DefaultDatabaseStringConnection))
            {
                try
                {
                    //Abre la conexion
                    connection.Open();
                    if (parametros.Identification != null && parametros.Contrato != null)
                    {
                        //Crea la consulta
                        StringBuilder query = new StringBuilder("SELECT DD.IDENTIFICACION_OBLIGADO, DD.NO_DOCUMENTO, DD.IDENTIFICACION_ADQUIRIENTE, DD.NOMBRE_ADQUIRIENTE, ")
                                                    .Append("DD.CAMPO_ALMACENAMIENTO_1 AS CONTRATO, DD.CAMPO_ALMACENAMIENTO_2  AS REFERENCIA_PAGO, ")
                                                    .Append("FORMAT(DD.FECHA_DOCUMENTO , 'yyyy-MM-dd') AS FECHA_DOCUMENTO, ")
                                                    .Append("FORMAT(DD.FECHA_VENCIMIENTO , 'yyyy-MM-dd') AS FECHA_VENCIMIENTO, ")
                                                    .Append("UD.URL, UD.ID_TIPO_ARCHIVO, UD.IS_MIGRADO ")
                                                    .Append("FROM FELECV4.DATOS_DOC_ELECTRONICO DD ")
                                                    .Append("FULL OUTER JOIN FELECV4.AUDITORIA_DOC_ELECTRONICO AD ")
                                                    .Append("ON DD.IDENTIFICACION_OBLIGADO = AD.IDENTIFICACION_OBLIGADO AND DD.NO_DOCUMENTO = AD.NO_DOCUMENTO AND DD.ID_TIPO_DOCUMENTO = AD.ID_TIPO_DOCUMENTO ")
                                                    .Append("FULL OUTER JOIN FELECV4.URL_DOCUMENTO_ELECTRONICO UD ")
                                                    .Append("ON DD.IDENTIFICACION_OBLIGADO = UD.IDENTIFICACION_OBLIGADO AND DD.NO_DOCUMENTO = UD.NO_DOCUMENTO AND DD.ID_TIPO_DOCUMENTO = UD.ID_TIPO_DOCUMENTO AND DD.IDENTIFICACION_ADQUIRIENTE = UD.IDENTIFICACION_ADQUIRIENTE ")
                                                    .Append("WHERE DD.IDENTIFICACION_OBLIGADO IN (@obligado) AND DD.ID_TIPO_DOCUMENTO = @tipoDocumento AND DD.FECHA_DOCUMENTO >= @fechaInicio AND DD.FECHA_DOCUMENTO <= @fechaFin")
                                                    .Append(" AND AD.CODIGO_RTA_WS_DIAN = @codigoDian ");

                        if (obligados?.Count > 0)
                        {
                            string inObligados = string.Join(", ", obligados);
                            query.Replace("@obligado", inObligados);
                        }
                        else
                        {
                            throw new Exception("La consulta no puede realizarse sin la identificacion de al menos un obligado");
                        }

                        //Se adicionan los filtros de búsqueda opcionales si contienen valor
                        /*if (!string.IsNullOrWhiteSpace(parametros.Identification))
                        {
                            query.Append("AND DD.IDENTIFICACION_ADQUIRIENTE = @identificacionAdquiriente ");
                            filters.Add("@identificacionAdquiriente", parametros.Identification);
                        }*/

                        if (!string.IsNullOrWhiteSpace(parametros.NumeroFactura))
                        {
                            query.Append("AND DD.NO_DOCUMENTO = @noDocumento ");
                            filters.Add("@noDocumento", parametros.NumeroFactura);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.Contrato))
                        {
                            query.Append("AND DD.CAMPO_ALMACENAMIENTO_1 = @contrato ");
                            filters.Add("@contrato", parametros.Contrato);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.ReferenciaPago))
                        {
                            query.Append("AND DD.CAMPO_ALMACENAMIENTO_2 = @referenciaPago ");
                            filters.Add("@referenciaPago", parametros.ReferenciaPago);
                        }

                        //Se ordenan los resultados de la consulta por fecha de factura
                        query.Append("ORDER BY DD.FECHA_DOCUMENTO DESC, DD.HORA_DOCUMENTO DESC ");

                        //Realiza la consulta en la BD 
                        using (SqlCommand cmd = new SqlCommand(query.ToString(), connection))
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.CommandText = query.ToString();

                            cmd.Parameters.Add("@tipoDocumento", SqlDbType.VarChar).Value = (parametros.TipoDocumento == TipoDocumento.NOTA_CREDITO.ToString()) ? IdTipoDocumentoNotaCredito : IdTipoDocumentoNotaDebito;
                            cmd.Parameters.Add("@identificacionAdquiriente", SqlDbType.VarChar).Value = parametros.Identification;
                            cmd.Parameters.Add("@fechaInicio", SqlDbType.Date).Value = parametros.FechaInicio;
                            cmd.Parameters.Add("@fechaFin", SqlDbType.Date).Value = parametros.FechaFin;
                            cmd.Parameters.Add("@codigoDian", SqlDbType.SmallInt).Value = CodigoDian;

                            foreach (var filter in filters)
                            {
                                cmd.Parameters.Add(filter.Key, SqlDbType.VarChar).Value = filter.Value;
                            }

                            using (var reader = cmd.ExecuteReader())
                            {
                                RespuestaFijo nota;
                                long idObligado;
                                while (reader.Read())
                                {
                                    nota = new RespuestaFijo();
                                    nota.TipoDocumento = parametros.TipoDocumento;
                                    idObligado = reader.GetInt64("IDENTIFICACION_OBLIGADO");
                                    nota.NumeroResolucion = 0;
                                    nota.NumeroFactura = reader.GetString("NO_DOCUMENTO");
                                    nota.Identification = reader.GetString("IDENTIFICACION_ADQUIRIENTE");
                                    nota.Nombre = reader["NOMBRE_ADQUIRIENTE"].ToString();
                                    nota.Contrato = reader["CONTRATO"].ToString();
                                    nota.ReferenciaPago = reader["REFERENCIA_PAGO"].ToString();
                                    nota.FechaFacturacion = reader["FECHA_DOCUMENTO"].ToString();
                                    nota.FechaVencimiento = reader["FECHA_VENCIMIENTO"].ToString();
                                    nota.UrlPdf = reader["URL"].ToString();
                                    nota.TipoArchivo = (int.TryParse(reader["ID_TIPO_ARCHIVO"].ToString(), out int code)) ? code : -1;
                                    nota.EsMigrado = (int.TryParse(reader["IS_MIGRADO"].ToString(), out int flag)) ? flag : 0;

                                    InsertarTrazabilidad(parametros.TipoCliente, parametros.TipoDocumento, nota.Contrato, nota.NumeroFactura, nota.Identification,
                                       "", nota.FechaFacturacion, nota.ReferenciaPago, 0);

                                    nota.Documentos = new Dictionary<string, string>();

                                    nota.Documentos.Add(TipoArchivo.xml.ToString(), Utilities.GetUrlParametros(idObligado, nota, TipoArchivo.xml));

                                    if (!string.IsNullOrWhiteSpace(nota.UrlPdf))
                                    {
                                        nota.Documentos.Add(TipoArchivo.pdf.ToString(), Utilities.GetUrlParametros(idObligado, nota, TipoArchivo.pdf));
                                    }
                                    else
                                    {
                                        nota.Documentos.Add(TipoArchivo.pdf.ToString(), "");
                                    }

                                    notas.Add(nota);
                                }
                            }
                        }
                    }

                    else
                    {
                        //Crea la consulta
                        StringBuilder query = new StringBuilder("SELECT DD.IDENTIFICACION_OBLIGADO, DD.NO_DOCUMENTO, DD.IDENTIFICACION_ADQUIRIENTE, DD.NOMBRE_ADQUIRIENTE, ")
                                                    .Append("DD.CAMPO_ALMACENAMIENTO_1 AS CONTRATO, DD.CAMPO_ALMACENAMIENTO_2  AS REFERENCIA_PAGO, ")
                                                    .Append("FORMAT(DD.FECHA_DOCUMENTO , 'yyyy-MM-dd') AS FECHA_DOCUMENTO, ")
                                                    .Append("FORMAT(DD.FECHA_VENCIMIENTO , 'yyyy-MM-dd') AS FECHA_VENCIMIENTO, ")
                                                    .Append("UD.URL, UD.ID_TIPO_ARCHIVO, UD.IS_MIGRADO ")
                                                    .Append("FROM FELECV4.DATOS_DOC_ELECTRONICO DD ")
                                                    .Append("FULL OUTER JOIN FELECV4.AUDITORIA_DOC_ELECTRONICO AD ")
                                                    .Append("ON DD.IDENTIFICACION_OBLIGADO = AD.IDENTIFICACION_OBLIGADO AND DD.NO_DOCUMENTO = AD.NO_DOCUMENTO AND DD.ID_TIPO_DOCUMENTO = AD.ID_TIPO_DOCUMENTO ")
                                                    .Append("FULL OUTER JOIN FELECV4.URL_DOCUMENTO_ELECTRONICO UD ")
                                                    .Append("ON DD.IDENTIFICACION_OBLIGADO = UD.IDENTIFICACION_OBLIGADO AND DD.NO_DOCUMENTO = UD.NO_DOCUMENTO AND DD.ID_TIPO_DOCUMENTO = UD.ID_TIPO_DOCUMENTO AND DD.IDENTIFICACION_ADQUIRIENTE = UD.IDENTIFICACION_ADQUIRIENTE ")
                                                    .Append("WHERE DD.IDENTIFICACION_OBLIGADO IN (@obligado) AND DD.ID_TIPO_DOCUMENTO = @tipoDocumento AND DD.IDENTIFICACION_ADQUIRIENTE = @identificacionAdquiriente ")
                                                    .Append("AND DD.FECHA_DOCUMENTO >= @fechaInicio AND DD.FECHA_DOCUMENTO <= @fechaFin AND AD.CODIGO_RTA_WS_DIAN = @codigoDian ");

                        if (obligados?.Count > 0)
                        {
                            string inObligados = string.Join(", ", obligados);
                            query.Replace("@obligado", inObligados);
                        }
                        else
                        {
                            throw new Exception("La consulta no puede realizarse sin la identificacion de al menos un obligado");
                        }

                        //Se adicionan los filtros de búsqueda opcionales si contienen valor
                        /*if (!string.IsNullOrWhiteSpace(parametros.Identification))
                        {
                            query.Append("AND DD.IDENTIFICACION_ADQUIRIENTE = @identificacionAdquiriente ");
                            filters.Add("@identificacionAdquiriente", parametros.Identification);
                        }*/

                        if (!string.IsNullOrWhiteSpace(parametros.NumeroFactura))
                        {
                            query.Append("AND DD.NO_DOCUMENTO = @noDocumento ");
                            filters.Add("@noDocumento", parametros.NumeroFactura);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.Contrato))
                        {
                            query.Append("AND DD.CAMPO_ALMACENAMIENTO_1 = @contrato ");
                            filters.Add("@contrato", parametros.Contrato);
                        }

                        if (!string.IsNullOrWhiteSpace(parametros.ReferenciaPago))
                        {
                            query.Append("AND DD.CAMPO_ALMACENAMIENTO_2 = @referenciaPago ");
                            filters.Add("@referenciaPago", parametros.ReferenciaPago);
                        }

                        //Se ordenan los resultados de la consulta por fecha de factura
                        query.Append("ORDER BY DD.FECHA_DOCUMENTO DESC, DD.HORA_DOCUMENTO DESC ");

                        //Realiza la consulta en la BD 
                        using (SqlCommand cmd = new SqlCommand(query.ToString(), connection))
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.CommandText = query.ToString();

                            cmd.Parameters.Add("@tipoDocumento", SqlDbType.VarChar).Value = (parametros.TipoDocumento == TipoDocumento.NOTA_CREDITO.ToString()) ? IdTipoDocumentoNotaCredito : IdTipoDocumentoNotaDebito;
                            cmd.Parameters.Add("@identificacionAdquiriente", SqlDbType.VarChar).Value = parametros.Identification;
                            cmd.Parameters.Add("@fechaInicio", SqlDbType.Date).Value = parametros.FechaInicio;
                            cmd.Parameters.Add("@fechaFin", SqlDbType.Date).Value = parametros.FechaFin;
                            cmd.Parameters.Add("@codigoDian", SqlDbType.SmallInt).Value = CodigoDian;

                            foreach (var filter in filters)
                            {
                                cmd.Parameters.Add(filter.Key, SqlDbType.VarChar).Value = filter.Value;
                            }

                            using (var reader = cmd.ExecuteReader())
                            {
                                RespuestaFijo nota;
                                long idObligado;
                                while (reader.Read())
                                {
                                    nota = new RespuestaFijo();
                                    nota.TipoDocumento = parametros.TipoDocumento;
                                    idObligado = reader.GetInt64("IDENTIFICACION_OBLIGADO");
                                    nota.NumeroResolucion = 0;
                                    nota.NumeroFactura = reader.GetString("NO_DOCUMENTO");
                                    nota.Identification = reader.GetString("IDENTIFICACION_ADQUIRIENTE");
                                    nota.Nombre = reader["NOMBRE_ADQUIRIENTE"].ToString();
                                    nota.Contrato = reader["CONTRATO"].ToString();
                                    nota.ReferenciaPago = reader["REFERENCIA_PAGO"].ToString();
                                    nota.FechaFacturacion = reader["FECHA_DOCUMENTO"].ToString();
                                    nota.FechaVencimiento = reader["FECHA_VENCIMIENTO"].ToString();
                                    nota.UrlPdf = reader["URL"].ToString();
                                    nota.TipoArchivo = (int.TryParse(reader["ID_TIPO_ARCHIVO"].ToString(), out int code)) ? code : -1;
                                    nota.EsMigrado = (int.TryParse(reader["IS_MIGRADO"].ToString(), out int flag)) ? flag : 0;

                                    InsertarTrazabilidad(parametros.TipoCliente, parametros.TipoDocumento, nota.Contrato, nota.NumeroFactura, nota.Identification,
                                       "", nota.FechaFacturacion, nota.ReferenciaPago, 0);

                                    nota.Documentos = new Dictionary<string, string>();

                                    nota.Documentos.Add(TipoArchivo.xml.ToString(), Utilities.GetUrlParametros(idObligado, nota, TipoArchivo.xml));

                                    if (!string.IsNullOrWhiteSpace(nota.UrlPdf))
                                    {
                                        nota.Documentos.Add(TipoArchivo.pdf.ToString(), Utilities.GetUrlParametros(idObligado, nota, TipoArchivo.pdf));
                                    }
                                    else
                                    {
                                        nota.Documentos.Add(TipoArchivo.pdf.ToString(), "");
                                    }

                                    notas.Add(nota);
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Captura y propaga la excepción
                    throw ex;
                }
                finally
                {
                    //Cierra la conexion
                    connection.Close();
                }
            }

            return notas;
        }


    }

}
