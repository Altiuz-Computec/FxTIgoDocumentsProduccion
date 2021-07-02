using Microsoft.AspNetCore.Http;
using System;
using System.Linq;

namespace FxDocumentsTigo.Class.Authorization
{
    class AuthorizationService
    {

        public HttpRequest Request { get; set; }

        public AuthorizationService(HttpRequest request)
        {
            Request = request;
        }

        public string GetIdAuthorized()
        {
            if (Request.Headers?.Count > 0)
            {
                if (Request.Headers.TryGetValue("Authorization", out Microsoft.Extensions.Primitives.StringValues authorizationHeaders))
                {
                    var tokens = authorizationHeaders.ToArray().Where(h => h.StartsWith("Bearer")).ToArray();
                    if (tokens.Length != 1)
                    {
                        //La peticion contiene multiples headers de autorizacion
                        throw new UnauthorizedAccessException("Request has multiple authorization headers");
                    }

                    string token = tokens[0].Substring(7);
                    if (string.IsNullOrWhiteSpace(token))
                    {
                        //El token esta vacio          
                        throw new UnauthorizedAccessException("Authorization header is empty");
                    }

                    AuthorizationClient authorizationClient = new AuthorizationClient();
                    if (authorizationClient.ValidateToken(token))
                    {
                        return authorizationClient.GetId(token);
                    }
                    else
                    {
                        throw new UnauthorizedAccessException("Token is not valid");
                    }
                }
                else
                {
                    //La peticion no contiene ningun header de autorizacion
                    throw new UnauthorizedAccessException("Request has not authorization headers");
                }
            }
            else
            {
                //La peticion no contiene headers
                throw new UnauthorizedAccessException("Request has not headers");
            }
        }
    }
}
