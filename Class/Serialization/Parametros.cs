using Newtonsoft.Json;

namespace FxDocumentsTigo.Class.Serialization
{
    class Parametros
    {

        [JsonProperty("tipo_cliente")]
        public string TipoCliente { get; set; }
        [JsonProperty("identificacion")]
        public string Identification { get; set; }
        [JsonProperty("numero_cuenta")]
        public string NumeroCuenta { get; set; }
        [JsonProperty("movil")]
        public long? Movil { get; set; }
        [JsonProperty("fecha_inicio")]
        public string FechaInicio { get; set; }
        [JsonProperty("fecha_fin")]
        public string FechaFin { get; set; }
        [JsonProperty("plan")]
        public string Plan { get; set; }
        [JsonProperty("ciclo")]
        public string Ciclo { get; set; }
        [JsonProperty("numero_factura")]
        public string NumeroFactura { get; set; }
        [JsonProperty("contrato")]
        public string Contrato { get; set; }
        [JsonProperty("referencia_pago")]
        public string ReferenciaPago { get; set; }
        [JsonProperty("tipo_documento")]
        public string TipoDocumento { get; set; }

        [JsonIgnore]
        public bool HasOndemand { get; set; }
        [JsonIgnore]
        public bool HasValidacionPrevia { get; set; }

    }
}
