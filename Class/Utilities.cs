using FxDocumentsTigo.Class.Serialization;
using FxTigoDocuments.Class.Enumeration;

namespace FxTigoDocuments.Class
{
    class Utilities
    {

        private const string IdOrigen = "VP";
        
        public static string GetUrlParametros(long obligado, RespuestaFijo respuesta, TipoArchivo tipoArchivo)
        {
            return $"{obligado};{respuesta.NumeroResolucion};{respuesta.NumeroFactura};{respuesta.TipoDocumento};{respuesta.Identification};{tipoArchivo};{respuesta.TipoArchivo};{IdOrigen};{respuesta.EsMigrado}";
        }

        public static string GetUrlParametros(long obligado, RespuestaMovil respuesta, TipoArchivo tipoArchivo)
        {
            return $"{obligado};{respuesta.NumeroResolucion};{respuesta.NumeroFactura};{respuesta.TipoDocumento};{respuesta.Identification};{tipoArchivo};{respuesta.TipoArchivo};{IdOrigen};{respuesta.EsMigrado}";
        }

    }
}
