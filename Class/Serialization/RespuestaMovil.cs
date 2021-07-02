using Newtonsoft.Json;

namespace FxDocumentsTigo.Class.Serialization
{
    class RespuestaMovil : Respuesta
    {

        [JsonProperty("numero_cuenta")]
        public string NumeroCuenta { get; set; }
        [JsonProperty("movil")]
        public long? Movil { get; set; }
        [JsonProperty("plan")]
        public string Plan { get; set; }

    }
}
