using FxDocumentsTigo.Class.Serialization;
using System;
using System.Globalization;
using System.Text.RegularExpressions;

namespace FxDocumentsTigo.Class
{
    class Validador
    {

        private const string FechaOndemandVariable = "DATE_ONDEMAND";

        private const string DatePattern = @"^(\d{4})(\-)(((0)[1-9])|((1)[0-2]))(\-)((0)[1-9]|[1-2][0-9]|(3)[0-1])$";

        private const string DateRepresentation = "yyyy-MM-dd";

        public Parametros Parametros { get; set; }

        public Validador(Parametros parametros)
        {
            Parametros = parametros;
        }

        public void ValidarParametros()
        {
            try
            {
                ValidarTipoCliente();
                ValidarFechaInicio();
                ValidarFechaFin();
                ValidarPeriodoConsulta();
                ValidarTipoDocumento();
                ValidarIdentificacion();                              

                

                ValidarConsultaOndemand();
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        private void ValidarTipoCliente()
        {
            if (Parametros.TipoCliente == null)
            {
                throw new NullReferenceException($"El parámetro 'tipo_cliente' es requerido");
            }
            else
            {
                if (!Enum.IsDefined(typeof(TipoCliente), Parametros.TipoCliente))
                {
                    throw new FormatException($"El valor '{Parametros.TipoCliente}' del parámetro 'tipo_cliente' no es permitido");
                }
            }            
        }

        private void ValidarFechaInicio()
        {
            if (Parametros.FechaInicio == null)
            {
                throw new NullReferenceException($"El parámetro 'fecha_inicio' es requerido");
            }
            else
            {
                Regex rg = new Regex(DatePattern);
                if (!rg.IsMatch(Parametros.FechaInicio))
                {
                    throw new FormatException($"El valor '{Parametros.FechaInicio}' del parámetro 'fecha_inicio' no es permitido");
                }
            }
        }

        private void ValidarFechaFin()
        {
            if (Parametros.FechaFin == null)
            {
                throw new NullReferenceException($"El parámetro 'fecha_fin' es requerido");
            }
            else
            {
                Regex rg = new Regex(DatePattern);
                if (!rg.IsMatch(Parametros.FechaFin))
                {
                    throw new FormatException($"El valor '{Parametros.FechaFin}' del parámetro 'fecha_fin' no es permitido");
                }
            }
        }

        private void ValidarTipoDocumento()
        {
            if (Parametros.TipoDocumento == null)
            {
                throw new NullReferenceException($"El parámetro 'tipo_documento' es requerido");
            }
            else
            {
                if (!Enum.IsDefined(typeof(TipoDocumento), Parametros.TipoDocumento))
                {
                    throw new FormatException($"El valor '{Parametros.TipoDocumento}' del parámetro 'tipo_documento' no es permitido");
                }
            }
        }

        private void ValidarIdentificacion()
        {
            if (string.IsNullOrWhiteSpace(Parametros.Identification))
            {
                throw new NullReferenceException($"El parámetro 'identificacion' es requerido");
            }
        }

        private void ValidarPeriodoConsulta()
        {
            var provider = CultureInfo.InvariantCulture;

            var fechaInicio = DateTime.ParseExact(Parametros.FechaInicio, DateRepresentation, provider);
            var fechaFin = DateTime.ParseExact(Parametros.FechaFin, DateRepresentation, provider);

            if (fechaInicio > fechaFin)
            {
                throw new FormatException($"El parámetro 'fecha_inicio' no puede ser mayor que el parámetro 'fecha_fin'");
            }
        }

        /*private void ValidarContrato()
        {
            if (string.IsNullOrWhiteSpace(Parametros.Contrato))
            {
                throw new NullReferenceException($"El parámetro 'contrato' es requerido");
            }
        }*/

        /*private void ValidarParametrosMovil()
        {
            bool condicion = string.IsNullOrWhiteSpace(Parametros.Identification) && string.IsNullOrWhiteSpace(Parametros.NumeroCuenta) && Parametros.Movil == null &&
                             string.IsNullOrWhiteSpace(Parametros.Plan) && string.IsNullOrWhiteSpace(Parametros.Ciclo) && string.IsNullOrWhiteSpace(Parametros.NumeroFactura);
            if (condicion)
            {
                throw new NullReferenceException($"Debe utilizar al menos un parámetro más para realizar la consulta");
            }
        }*/

        /*private void ValidarParametrosFijo()
        {
            if (string.IsNullOrWhiteSpace(Parametros.Contrato))
            {
                throw new NullReferenceException($"El parámetro 'contrato' es requerido");
            }
            
            bool condicion = string.IsNullOrWhiteSpace(Parametros.Identification) && string.IsNullOrWhiteSpace(Parametros.NumeroFactura) &&
                             string.IsNullOrWhiteSpace(Parametros.Contrato) && string.IsNullOrWhiteSpace(Parametros.ReferenciaPago);
            if (condicion)
            {
                throw new NullReferenceException($"Debe utilizar al menos un parámetro más para realizar la consulta");
            }
        }*/

        private void ValidarConsultaOndemand()
        {
            var provider = CultureInfo.InvariantCulture;

            DateTime fechaOndemand = DateTime.ParseExact(Settings.GetVariable(FechaOndemandVariable), DateRepresentation, provider);
            Parametros.HasOndemand = DateTime.ParseExact(Parametros.FechaInicio, DateRepresentation, provider) < fechaOndemand;
            Parametros.HasValidacionPrevia = DateTime.ParseExact(Parametros.FechaFin, DateRepresentation, provider) > fechaOndemand;
        }

    }
}
