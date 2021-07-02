using FxDocumentsTigo.Class;
using FxDocumentsTigo.Class.Serialization;
using Newtonsoft.Json;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Net;

namespace FxTigoDocuments.Class.Services
{
    class OnDemandClient<T> where T : Respuesta
    {

        private const string OnDemandEndpointVariable = "ONDEMAND_ENDPOINT";

        private string OnDemandEndpoint { get; set; }

        public OnDemandClient()
        {
            try
            {
                OnDemandEndpoint = Settings.GetVariable(OnDemandEndpointVariable);
            }
            catch (NullReferenceException nre)
            {
                throw nre;
            }
        }

        public List<T> GetDocuments(string input)
        {
            var client = new RestClient(OnDemandEndpoint);
            var request = new RestRequest(OnDemandEndpoint, Method.POST);
            request.AddHeader("Content-Type", "application/json");
            request.AddParameter("application/json", input, ParameterType.RequestBody);

            IRestResponse response = client.Execute(request);

            if (response != null)
            {
                switch ((int)response.StatusCode)
                {
                    case ((int)HttpStatusCode.OK):
                        return JsonConvert.DeserializeObject<List<T>>(response.Content);
                    
                    case ((int)HttpStatusCode.NotFound):
                        return new List<T>();
                    
                    default:
                        throw new Exception($"Code '{(int)response.StatusCode}' when trying to get response from OnDemand documents service");
                }
            }
            else
            {
                throw new Exception($"Unable to get response from OnDemand documents service");
            }
        }

    }
}
