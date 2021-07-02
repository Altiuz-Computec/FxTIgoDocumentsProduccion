using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace FxDocumentsTigo.Class.Serialization
{
    class RespuestaFijo : Respuesta
    {

        [JsonProperty("contrato")]
        public string Contrato { get; set; }
        [JsonProperty("referencia_pago")]
        public string ReferenciaPago { get; set; }

    }
}
