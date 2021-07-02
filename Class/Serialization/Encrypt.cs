using Newtonsoft.Json;
using System.Collections.Generic;

namespace FxTigoDocuments.Class.Serialization
{
    class Encrypt
    {

        [JsonProperty("encrypted")]
        public Dictionary<string, string> Encrypted { get; set; }

    }
}
