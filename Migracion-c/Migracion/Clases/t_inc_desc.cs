using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migracion.Clases
{
    class t_inc_desc
    {
        public string periodo { get; set; }//periodo
        public int entidad { get; set; }//tipo
        public int solo_arp { get; set; }//descuentodiasarp
        public string cod_ent { get; set; }//entidadid
        public string empleado { get; set; }//empleadoid
        public DateTime fecha_ini { get; set; }//fechainicio
        public DateTime fecha_fin { get; set; }//fechafinal
        public int vr_inc_sal { get; set; }//valorincapacidadsalud
        public int vr_inc_arp { get; set; }//valorincapacidadriesgos
        public int vr_inc_mat { get; set; }//valorlicenciamaternidad
        public string au_inc_sal { get; set; }//valorincapacidadsaludautorizacion
        public string au_inc_arp { get; set; }//valorincapacidadriesgosautorizacion
        public string au_inc_mat { get; set; }//valorlicenciamaternidadautorizacion
        public int apor_pag { get; set; }//aportespagadosaotrossubsistemas
    }
}
