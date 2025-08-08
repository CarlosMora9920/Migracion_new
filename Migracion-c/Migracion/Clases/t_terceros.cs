using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace Migracion.Clases
{
    internal class t_terceros
    {
        
        public string TipoPersona { get; set; }
        public int IdTipoIdentificacion { get; set; }
        public long Identificacion { get; set; } 
        public string Nombre1 { get; set; }
        public string Nombre2 { get; set; }
        public string Apellido1 { get; set; }
        public string Apellido2 { get; set; }
        public string Genero { get; set; }
        public DateTime FechaNacimiento { get; set; }
        public string RazonSocial { get; set; }
        public string NombreComercial { get; set; }
        public string Direccion { get; set; }
        public string Email { get; set; }
        public string Email2 { get; set; }
        public int IdDepartamento { get; set; }
        public int IdMunicipio { get; set; }
        public string Telefono1 { get; set; }
        public string Telefono2 { get; set; }
        public int Estado { get; set; }
        public int EsCliente { get; set; }
        public int EsEmpleado { get; set; }
        public int EsPasante { get; set; }
        public int EsProveedor { get; set; }
        public DateTime? FechaCreacion { get; set; }
        public DateTime? FechaActualizacion { get; set; }
        public int DiasCredito { get; set; }
        public int EsProveedorFruver { get; set; }
        public int EsProveedorBolsaAgropecuaria { get; set; }
        public int EsProveedorCampesinoDirecto { get; set; }
        public int EsProveedorRestaurante { get; set; }
        public int EsProveedorPanaderia { get; set; }
        public int EsOtroTipo { get; set; }
        public int EsGasto { get; set; }
        public int CotizaEPS { get; set; }
        public int CotizaFondoPension { get; set; }
        public int CotizaARP { get; set; }
        public int TarifaARP { get; set; }
        public int RegimenSimplificado { get; set; }
        public int NoPracticarRetFuente { get; set; }
        public int NoPracticarRetIVA { get; set; }
        public int Autorretenedor { get; set; }
        public int EsRetenedorFuente { get; set; }
        public int DescontarAsohofrucol { get; set; }
        public int AsumirImpuestos { get; set; }
        public int RetenerFenalce { get; set; }
        public int AsumirFenalce { get; set; }
        public int BolsaAgropecuaria { get; set; }
        public int RegimenComun { get; set; }
        public int RetenerSiempre { get; set; }
        public int GranContribuyente { get; set; }
        public int AutorretenedorIVA { get; set; }
        public int IdCxPagar { get; set; }
        public int DeclaranteRenta { get; set; }
        public int DescuentoNIIF { get; set; }
        public int DescontarFNFP { get; set; }
        public int ManejaIVAProductoBonificado { get; set; }
        public int ReteIVALey560_2020 { get; set; }
        public int RegimenSimpleTributacion { get; set; }
        public int RetencionZOMAC { get; set; }
        public int TipoDescuentoFinanciero { get; set; }
        public int Porcentaje1 { get; set; }
        public int Porcentaje2 { get; set; }
        public int Porcentaje3 { get; set; }
        public int Porcentaje4 { get; set; }
        public int Porcentaje5 { get; set; }
        public int ClienteExcentoIVA { get; set; }
        public int IdRUT { get; set; }
        public int IdICATerceroCiudad { get; set; }
        public int EstadoRUT { get; set; }
        public DateTime? FechaRut { get; set; }
        public int IdCxCobrar { get; set; }
        public string DigitoVerificacion { get; set; }
        public int IdEmpleado { get; set; }
        public int BaseDecreciente { get; set; }
        public int IdRegimenContribuyente { get; set; }
        public string IdResponsabilidadesFiscales { get; set; }
        public string IdResponsabilidadesTributarias { get; set; }
        public string IdUbicacionDANE { get; set; }
        public string CodigoPostal { get; set; }
        public int BloqueoPago { get; set; }
        public string ObservacionBloqueo { get; set; }
        public int FrecuenciaServicio { get; set; }
        public int PromesaServicio { get; set; }
        public int DiasInvSeguridad { get; set; }
    }
}
