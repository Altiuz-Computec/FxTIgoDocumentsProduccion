using FxDocumentsTigo.Class;
using FxTigoDocuments.Class.Serialization;
using Newtonsoft.Json;
using RestSharp;
using System;
using System.Collections.Generic;

namespace FxTigoDocuments.Class.Encryption
{
    class EncryptionClient
    {

        private const string EncryptEndpointVariable = "ENCRYPT_ENDPOINT";

        private string EncryptEndpoint { get; set; }

        public EncryptionClient()
        {
            try
            {
                EncryptEndpoint = Settings.GetVariable(EncryptEndpointVariable);
            }
            catch (NullReferenceException nre)
            {
                throw nre;
            }
        }

        public Dictionary<string, string> Encrypt(List<string> parameters)
        {
            var input = new { parameters };

            var client = new RestClient(EncryptEndpoint);
            var request = new RestRequest(EncryptEndpoint, Method.POST);
            request.AddHeader("Content-Type", "application/json");
            request.AddParameter("application/json", JsonConvert.SerializeObject(input), ParameterType.RequestBody);

            IRestResponse response = client.Execute(request);

            if (response != null && (int)response.StatusCode == 200)
            {
                return JsonConvert.DeserializeObject<Encrypt>(response.Content).Encrypted;
            }
            else
            {
                throw new Exception($"Code '{(int)response.StatusCode}' when trying to get response from encryption service");
            }
        }

    }
}
