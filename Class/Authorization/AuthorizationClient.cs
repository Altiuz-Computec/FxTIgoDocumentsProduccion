using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RestSharp;
using System;

namespace FxDocumentsTigo.Class.Authorization
{
    class AuthorizationClient
    {

        private const string JWTValidatorEndpointVariable = "JWT_VALIDATOR_ENDPOINT";
        private const string JWTClaimsEndpointVariable = "JWT_CLAIMS_ENDPOINT";

        private string JWTValidatorEndpoint { get; set; }
        private string JWTCLaimsEndpoint { get; set; }

        public AuthorizationClient()
        {
            try
            {
                JWTValidatorEndpoint = Settings.GetVariable(JWTValidatorEndpointVariable);
                JWTCLaimsEndpoint = Settings.GetVariable(JWTClaimsEndpointVariable);
            }
            catch (NullReferenceException nre)
            {
                throw nre;
            }
        }

        public bool ValidateToken(string token)
        {
            var input = new { jwt = token };

            var client = new RestClient(JWTValidatorEndpoint);
            var request = new RestRequest(JWTValidatorEndpoint, Method.POST);
            request.AddHeader("Content-Type", "application/json");
            request.AddParameter("application/json", JsonConvert.SerializeObject(input), ParameterType.RequestBody);

            IRestResponse response = client.Execute(request);

            if (response != null && (int)response.StatusCode == 200)
            {
                return response.Content == "true";
            }
            else
            {
                throw new UnauthorizedAccessException($"Code '{(int)response.StatusCode}' when trying to get response from JWT validator service");
            }
        }

        public string GetId(string token)
        {
            var input = new { jwt = token };

            var client = new RestClient(JWTCLaimsEndpoint);
            var request = new RestRequest(JWTCLaimsEndpoint, Method.POST);
            request.AddHeader("Content-Type", "application/json");
            request.AddParameter("application/json", JsonConvert.SerializeObject(input), ParameterType.RequestBody);

            IRestResponse response = client.Execute(request);

            JObject jsonResponse = JsonConvert.DeserializeObject<JObject>(response.Content);

            if (response != null && (int)response.StatusCode == 201)
            {
                if (jsonResponse.ContainsKey("id"))
                {
                    return (string)jsonResponse.SelectToken("id");
                }
                else
                {
                    throw new Exception("Cannot find the propery 'issuer' in the JWT claims response");
                }
            }
            else
            {
                throw new UnauthorizedAccessException($"Code '{(int)response.StatusCode}' when trying to get response from JWT claims service");
            }
        }

    }
}
