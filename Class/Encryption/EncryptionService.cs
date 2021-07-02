using FxDocumentsTigo.Class;
using FxDocumentsTigo.Class.Serialization;
using System;
using System.Collections.Generic;

namespace FxTigoDocuments.Class.Encryption
{
    class EncryptionService<T> where T : Respuesta
    {

        private const string DownloadDomainVariable = "DOWNLOAD_DOMAIN";

        private string DownloadDomain { get; set; }

        public List<T> Documentos { get; set; }

        public EncryptionService(List<T> documentos)
        {
            try
            {
                Documentos = documentos;
                DownloadDomain = Settings.GetVariable(DownloadDomainVariable);
            }
            catch (NullReferenceException nre)
            {
                throw nre;
            }
        }

        public List<T> EncryptUrls()
        {            
            try
            {
                List<string> parameters = new List<string>();
                foreach (var documento in Documentos)
                {
                    foreach (var file in documento.Documentos)
                    {
                        if (!string.IsNullOrWhiteSpace(file.Value))
                        {
                            parameters.Add(file.Value);
                        }
                    }
                }

                if (parameters.Count > 0)
                {
                    EncryptionClient encryptionClient = new EncryptionClient();
                    Dictionary<string, string> encrypted = encryptionClient.Encrypt(parameters);
                    List<string> keys;
                    byte[] encryptedUrl;
                    foreach (var documento in Documentos)
                    {
                        keys = new List<string>(documento.Documentos.Keys);
                        foreach (var key in keys)
                        {
                            if (encrypted.ContainsKey(documento.Documentos[key]))
                            {
                                encryptedUrl = System.Text.Encoding.UTF8.GetBytes(encrypted[documento.Documentos[key]]);
                                documento.Documentos[key] = $"{DownloadDomain}/{System.Convert.ToBase64String(encryptedUrl)}";
                            }
                        }
                    }
                }

                return Documentos;
            }
            catch (Exception e)
            {
                throw e;
            }
        }

    }
}
