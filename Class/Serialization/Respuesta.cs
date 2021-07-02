using Newtonsoft.Json;
using System.Collections.Generic;

namespace FxDocumentsTigo.Class.Serialization
{
    class Respuesta
    {

        [JsonProperty("identificacion")]
        public string Identification { get; set; }       
        [JsonProperty("nombre")]
        public string Nombre { get; set; }
        [JsonProperty("fecha_facturacion")]
        public string FechaFacturacion { get; set; }        
        [JsonProperty("numero_factura")]
        public string NumeroFactura { get; set; }        
        [JsonProperty("fecha_vencimiento")]
        public string FechaVencimiento { get; set; }
        [JsonProperty("tipo_documento")]
        public string TipoDocumento { get; set; }
        [JsonProperty("documentos")]
        public Dictionary<string, string> Documentos { get; set; }

        [JsonIgnore]
        public long NumeroResolucion { get; set; }
        [JsonIgnore]
        public string UrlPdf { get; set; }
        [JsonIgnore]
        public int TipoArchivo { get; set; }
        [JsonIgnore]
        public int EsMigrado { get; set; }

    }
}
