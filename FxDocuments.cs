using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Net.Http;
using System.Net;
using System.Text;
using FxDocumentsTigo.Class.Serialization;
using FxDocumentsTigo.Class;
using System.Collections.Generic;
using FxDocumentsTigo.Class.Authorization;
using FxTigoDocuments.Class.Encryption;
using FxTigoDocuments.Class.Serialization;
using FxTigoDocuments.Class.Services;

namespace FxDocumentsTigo
{
    public static class FxDocuments
    {

        [FunctionName("DocumentsTigo")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "documents")] HttpRequest req, ILogger log)
        {
            log.LogInformation("Iniciando procesamiento de solicitud de consulta de documento...");

            //Recibe el flujo de entrada
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

            Cliente cliente;
            //Validacion del token
            try
            {
                AuthorizationService authorizationService = new AuthorizationService(req);
                cliente = new Cliente(authorizationService.GetIdAuthorized());
            }
            catch (Exception e)
            {
                log.LogError(e.ToString());

                var message = new { message = $"Por favor incluya un token de autorización válido en el encabezado de la petición" };

                return GetResponse(HttpStatusCode.Unauthorized, message);
            }

            //Deserealiza el request y obtiene los parametros de entrada
            Parametros parametros = JsonConvert.DeserializeObject<Parametros>(requestBody);

            //Se valida que el objeto de los parametros de entrada no sea nulo
            if (parametros != null)
            {
                //Se valida que los parametros contengan los compos requeridos y campos minimos
                try
                {
                    Validador validador = new Validador(parametros);
                    validador.ValidarParametros();
                }
                catch (Exception e)
                {
                    log.LogError(e.ToString());
                    var message = new { message = e.Message };
                    return GetResponse(HttpStatusCode.BadRequest, message);
                }

                //Se realiza la consulta de los documentos
                try
                {
                    List<RespuestaFijo> documentosFijo = new List<RespuestaFijo>();
                    List<RespuestaMovil> documentosMovil = new List<RespuestaMovil>();

                    //Primero se realiza la consulta de los documentos de Validación Previa
                    if (parametros.HasValidacionPrevia)
                    {
                        DatabaseConnector databaseConnector = new DatabaseConnector();

                        if (parametros.TipoCliente == TipoCliente.FIJO.ToString())
                        {
                            if (parametros.TipoDocumento == TipoDocumento.FACTURA.ToString())
                            {
                                documentosFijo.AddRange(databaseConnector.GetFacturasFijo(cliente.Fijo, parametros));

                               


                            }
                            else
                            {
                                documentosFijo.AddRange(databaseConnector.GetNotasFijo(cliente.Fijo, parametros));
                            }                                                      
                        }
                        else
                        {
                            if (parametros.TipoDocumento == TipoDocumento.FACTURA.ToString())
                            {
                                documentosMovil.AddRange(databaseConnector.GetFacturasMovil(cliente.Movil, parametros));
                            }
                            else
                            {
                                documentosMovil.AddRange(databaseConnector.GetNotasMovil(cliente.Movil, parametros));
                            }
                        }
                       
                    }

                    


                    //A continuacion, se realiza la consulta de los documentos de OnDemand
                    if (parametros.HasOndemand)
                    {
                        //Integración con servicio de consulta de Ondemand
                        OnDemandService onDemandService = new OnDemandService(requestBody);
                        if (parametros.TipoCliente == TipoCliente.FIJO.ToString())
                        {
                            if (parametros.TipoDocumento == TipoDocumento.FACTURA.ToString())
                            {
                                documentosFijo.AddRange(onDemandService.GetDocumentosFijo());
                            }
                            else
                            {
                                documentosFijo.AddRange(onDemandService.GetDocumentosFijo());
                            }
                        }
                        else
                        {
                            if (parametros.TipoDocumento == TipoDocumento.FACTURA.ToString())
                            {
                                documentosMovil.AddRange(onDemandService.GetDocumentosMovil());
                            }
                            else
                            {
                                documentosMovil.AddRange(onDemandService.GetDocumentosMovil());
                            }
                        }
                    }

                    //Finalmente, se cifran las urls de descarga de los documentos y se retorna el resultado de la consulta
                    if (parametros.TipoCliente == TipoCliente.FIJO.ToString())
                    {
                        if (documentosFijo?.Count > 0)
                        {
                            EncryptionService<RespuestaFijo> encryptionService = new EncryptionService<RespuestaFijo>(documentosFijo);
                            documentosFijo = encryptionService.EncryptUrls();

                            return GetResponse(HttpStatusCode.OK, documentosFijo);
                            
                        }
                        else
                        {
                            //No se encontraron documentos
                            log.LogWarning("La solicitud no genera resultados");
                            var message = new { message = "La solicitud no genera resultados" };
                            return GetResponse(HttpStatusCode.NotFound, message);
                        }
                    }
                    else
                    {
                        if (documentosMovil?.Count > 0)
                        {
                            EncryptionService<RespuestaMovil> encryptionService = new EncryptionService<RespuestaMovil>(documentosMovil);
                            documentosMovil = encryptionService.EncryptUrls();

                            return GetResponse(HttpStatusCode.OK, documentosMovil);
                        }
                        else
                        {
                            //No se encontraron documentos
                            log.LogWarning("La solicitud no genera resultados");
                            var message = new { message = "La solicitud no genera resultados" };
                            return GetResponse(HttpStatusCode.NotFound, message);
                        }
                    }
                }
                catch (Exception e)
                {
                    log.LogError(e.ToString());
                    var message = new { message = $"Se ha presentado una excepción, por favor intente nuevamente o contacte al administrador" };
                    return GetResponse(HttpStatusCode.InternalServerError, message);
                }
            } else
            {
                log.LogError("Unable to get input data object");
                var message = new { message = $"Please check the parameters in the body request" };
                return GetResponse(HttpStatusCode.BadRequest, message);
            }          
        }

        private static HttpResponseMessage GetResponse(HttpStatusCode httpStatusCode, object response)
        {
            return new HttpResponseMessage(httpStatusCode)
            {
                Content = new StringContent(JsonConvert.SerializeObject(response), Encoding.UTF8, "application/json")
            };
        }
    }
}
