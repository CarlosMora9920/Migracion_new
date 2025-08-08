using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migracion.Clases
{
    internal class t_planDet
    {
        public DateOnly Fecha { get; set; }
        public string Empleado { get; set; }
        public DateTime FechaCreacion { get; set; }
        public string Usa_crea { get; set; }
        public int NumeroPlanilla { get; set; } 

    }
}
