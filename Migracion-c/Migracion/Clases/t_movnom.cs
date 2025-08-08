using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migracion.Clases
{
    internal class t_movnom
    {
        public string Empleado { get; set; }
        public string Cedula { get; set; }
        public string Apellido1 { get; set; }
        public string Apellido2 { get; set; }
        public string Nombre1 { get; set; }
        public string Nombre2 { get; set; }
        public string Concepto { get; set; }
        public string Periodo { get; set; }
        public DateTime fecha { get; set; }
        public int Devengado { get; set; }
        public int Descuento { get; set; }

    }
}
