using FxDocumentsTigo.Class.Serialization;
using System;
using System.Collections.Generic;

namespace FxTigoDocuments.Class.Services
{
    class OnDemandService
    {

        public string Request { get; set; }

        public OnDemandService(string request)
        {
            Request = request;
        }

        public List<RespuestaMovil> GetDocumentosMovil()
        {
            try
            {
                OnDemandClient<RespuestaMovil> onDemandClient = new OnDemandClient<RespuestaMovil>();
                List<RespuestaMovil> documentosMovil = onDemandClient.GetDocuments(Request);
                if (documentosMovil?.Count > 0)
                {
                    foreach (RespuestaMovil documento in documentosMovil)
                    {
                        documento.Movil = (documento.Movil == null) ? 0 : documento.Movil;
                        documento.Plan = (documento.Plan == null) ? "" : documento.Plan;
                    }
                }

                return documentosMovil;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public List<RespuestaFijo> GetDocumentosFijo()
        {
            try
            {
                OnDemandClient<RespuestaFijo> onDemandClient = new OnDemandClient<RespuestaFijo>();
                List<RespuestaFijo> documentosFijo = onDemandClient.GetDocuments(Request);
                return documentosFijo;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

    }
}
