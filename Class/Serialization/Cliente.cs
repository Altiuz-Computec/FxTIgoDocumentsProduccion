using System;
using System.Collections.Generic;
using System.Linq;
using FxDocumentsTigo.Class;


namespace FxTigoDocuments.Class.Serialization
{

    class Cliente
    {

        private const string MovilVariable = "MOVIL";
        private const string FijoVariable = "FIJO";
        private const string Separador = ";";

        public List<long> Movil { get; set; }
        public List<long> Fijo { get; set; }
        public long Identificacion { get; set; }
        public bool isMovil { get; set; }
        public bool isFijo { get; set; }

        public Cliente(string id)
        {
            try
            {
                Identificacion = long.Parse(id);
                Movil = Settings.GetVariable(MovilVariable)?.Split(Separador).ToList().Select(long.Parse).ToList();
                Fijo = Settings.GetVariable(FijoVariable)?.Split(Separador).ToList().Select(long.Parse).ToList();
                isMovil = Movil.Contains(Identificacion);
                isFijo = Fijo.Contains(Identificacion);

                if (!isMovil && !isFijo)
                {
                    throw new ArgumentException($"El cliente '{id}' no se encuentra autorizado para acceder al servicio");
                }
            }
            catch (Exception e)
            {
                throw e;
            }
        }

    }
}
