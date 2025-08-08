using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Transactions;
using Migracion.Clases;
using Npgsql;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Migracion
{
     class Procesos
    {

        public static void InsertarVendedores(List<t_vended> vendedList)
        {
            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                // Iniciar transacción
                using (var tx = conn.BeginTransaction())
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.Transaction = tx;

                    // Insertar cada registro de vended
                    foreach (var vended in vendedList)
                    {
                        // Comando SQL con parámetros
                        cmd.CommandText = @"
                        INSERT INTO vendedores (codigo, nombre, telefonos, direccion, ciudad, numerodedocumento, fechahoraingreso, estado, sucursalid)
                        VALUES (@codigo, @nombre, @telefonos, @direccion, @ciudad, @numerodedocumento, @fechahoraingreso, @estado, @sucursalid)";

                        // Parametrización de la consulta
                        cmd.Parameters.Clear();  // Limpiar los parámetros antes de agregar nuevos
                        cmd.Parameters.AddWithValue("codigo", vended.Vended.Trim());
                        cmd.Parameters.AddWithValue("nombre", vended.Nombre.Trim());
                        cmd.Parameters.AddWithValue("telefonos", vended.Tel.Trim());
                        cmd.Parameters.AddWithValue("direccion", vended.Direcc.Trim());
                        cmd.Parameters.AddWithValue("ciudad", vended.Ciudad.Trim());
                        cmd.Parameters.AddWithValue("numerodedocumento", vended.Cedula);
                        cmd.Parameters.AddWithValue("fechahoraingreso", vended.FechIng); // Asegúrate de que la fecha sea del tipo correcto
                        cmd.Parameters.AddWithValue("estado", true); // Por ejemplo, siempre `true` para estado
                        cmd.Parameters.AddWithValue("sucursalid", 10); // Un valor constante para sucursalid (puedes ajustarlo)

                        // Ejecutar el comando de inserción
                        cmd.ExecuteNonQuery();
                    }

                    // Confirmar la transacción
                    tx.Commit();
                }
            }

            Console.WriteLine("Datos insertados correctamente.");
        }

        public static void InsertarClientes(List<t_cliente> Clien_list, List<t_ciudad> ciudade_lis)
        {

            int xTipoPersona = 0;
            string xDepartamento = "";
            string xMunicipio = "";

            int contador = 0; // Contador

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                // Iniciar transacción
                using (var tx = conn.BeginTransaction())
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.Transaction = tx;

                    foreach (var clientes in Clien_list)
                    {
                        try
                        {
                            cmd.Parameters.Clear(); // ← CRUCIA

                            // Comando SQL con parámetros
                            cmd.CommandText = @"
                       INSERT INTO clientes (
                        idtiposdepersona, idtiposdedocumento, numerodedocumento, digitodeverificacion, 
                        razonsocial, direccion, emails, telefonos, idmunicipios, papellido, 
                        sapellido, pnombre, snombre, estado, iddepartamento
                        )
                        VALUES (
                        @tipoPersona,
                        (SELECT id FROM tiposdedocumento WHERE codigo = @codigoDoc LIMIT 1),
                        @numeroDoc,
                        @dv,
                        @razonSocial,
                        @direccion,
                        @email,
                        @telefono,
                        (SELECT id FROM municipios WHERE nombre = @municipio LIMIT 1),
                        @apl1,
                        @apl2,
                        @nom1,
                        @nom2,
                        @estado,
                        (SELECT id FROM departamentos WHERE nombre = @departamento LIMIT 1)
                        );";

                            if (clientes.tipo_per == "Natural")
                            {
                                xTipoPersona = 1;
                            }
                            else
                            {
                                xTipoPersona = 2;
                            }


                            var ubicacion = ciudade_lis.FirstOrDefault(c => c.Dane == clientes.Dane);


                            if (ubicacion != null)
                            {
                                xDepartamento = ubicacion.Departamento;
                                xMunicipio = ubicacion.Municipio;
                            }

                            cmd.Parameters.AddWithValue("tipoPersona", xTipoPersona);
                            cmd.Parameters.AddWithValue("codigoDoc", string.IsNullOrWhiteSpace(clientes.tdoc) ? DBNull.Value : clientes.tdoc);
                            cmd.Parameters.AddWithValue("numeroDoc", BigInteger.Parse(clientes.anexo));
                            cmd.Parameters.AddWithValue("dv", string.IsNullOrWhiteSpace(clientes.dv) ? DBNull.Value : BigInteger.Parse(clientes.dv));
                            cmd.Parameters.AddWithValue("razonSocial", string.IsNullOrWhiteSpace(clientes.nombre) ? DBNull.Value : clientes.nombre);
                            cmd.Parameters.AddWithValue("direccion", string.IsNullOrWhiteSpace(clientes.direcc) ? DBNull.Value : clientes.direcc);
                            cmd.Parameters.AddWithValue("email", string.IsNullOrWhiteSpace(clientes.emailfe1) ? DBNull.Value : clientes.emailfe1);
                            cmd.Parameters.AddWithValue("telefono", string.IsNullOrWhiteSpace(clientes.tel) ? DBNull.Value : clientes.tel);
                            cmd.Parameters.AddWithValue("municipio", string.IsNullOrWhiteSpace(xMunicipio) ? DBNull.Value : xMunicipio);
                            cmd.Parameters.AddWithValue("apl1", string.IsNullOrWhiteSpace(clientes.apl1) ? DBNull.Value : clientes.apl1);
                            cmd.Parameters.AddWithValue("apl2", string.IsNullOrWhiteSpace(clientes.apl2) ? DBNull.Value : clientes.apl2);
                            cmd.Parameters.AddWithValue("nom1", string.IsNullOrWhiteSpace(clientes.nom1) ? DBNull.Value : clientes.nom1);
                            cmd.Parameters.AddWithValue("nom2", string.IsNullOrWhiteSpace(clientes.nom2) ? DBNull.Value : clientes.nom2);
                            cmd.Parameters.AddWithValue("estado", clientes.bloqueado == 1 ? false : true);
                            cmd.Parameters.AddWithValue("departamento", string.IsNullOrWhiteSpace(xDepartamento) ? DBNull.Value : xDepartamento);

                            // Ejecutar el comando de inserción
                            cmd.ExecuteNonQuery();



                            contador++; // Incrementar contador
                            Console.WriteLine($"Cliente insertado #{contador} - Documento: {clientes.anexo}"); // Mostrar en consola
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error al insertar cliente con documento {clientes.anexo}: {ex.Message}");
                        }
                    }

                    // Confirmar la transacción
                    tx.Commit();
                }
            }

            Console.WriteLine("Datos insertados correctamente.");

        }

        public static void InsertarHistoNove(List<t_histonove> nove_list)
        {

            int contador = 0; // Contador

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                // Iniciar transacción
                using (var tx = conn.BeginTransaction())
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.Transaction = tx;

                    foreach (var histonove in nove_list)
                    {
                        try
                        {
                            cmd.Parameters.Clear(); // ← CRUCIA

                            // Comando SQL con parámetros
                            cmd.CommandText = @"
                        INSERT INTO cambioeps (
                            entidadesnominaanteriorid,
                            entidadesnominanuevaid,
                            usuarioid,
                            fechahora,
                            empleadoid,
                            reportartraslado,
                            fechacambio
                        )
                        VALUES (
                            (SELECT DISTINCT id FROM public.""EntidadesNomina"" WHERE codigo = @codAnt),
                            (SELECT DISTINCT id FROM public.""EntidadesNomina""WHERE codigo = @codAct),
                            (SELECT DISTINCT  ""Id"" FROM public.""AspNetUsers"" WHERE ""UserName"" = @userReg),
                            @fechaHora,
                            (SELECT DISTINCT ""Id"" FROM public.""Empleados"" WHERE ""CodigoEmpleado"" = @empleado),
                            @reportarTraslado,
                            @fechaCambio
                        );";


                            cmd.Parameters.AddWithValue("codAnt", histonove.cod_ant.Trim());
                            cmd.Parameters.AddWithValue("codAct", histonove.cod_act.Trim());
                            cmd.Parameters.AddWithValue("userReg", histonove.usua_reg.Trim());
                            cmd.Parameters.AddWithValue("fechaHora", new DateTime(
                                histonove.fech_reg.Year,
                                histonove.fech_reg.Month,
                                histonove.fech_reg.Day,
                                histonove.hora_reg.Hour,
                                histonove.hora_reg.Minute,
                                histonove.hora_reg.Second
                            ));
                            cmd.Parameters.AddWithValue("empleado", histonove.empleado.Trim());
                            cmd.Parameters.AddWithValue("reportarTraslado", histonove.repo_soi == 1);
                            cmd.Parameters.AddWithValue("fechaCambio", histonove.fech_camb);

                            // Ejecutar el comando de inserción
                            cmd.ExecuteNonQuery();



                            contador++; // Incrementar contador
                            Console.WriteLine($"Ingresando registro.. #{contador}"); // Mostrar en consola
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error al insertar cliente con documento: {ex.Message}");
                        }
                    }

                    // Confirmar la transacción
                    tx.Commit();
                }
            }

            Console.WriteLine("Datos insertados correctamente.");

        }

        public static void InsertarHistonoveP(List<t_histonove> nove_pen)
        {

            int contador = 0; // Contador

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                // Iniciar transacción
                using (var tx = conn.BeginTransaction())
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.Transaction = tx;

                    foreach (var histonove in nove_pen)
                    {
                        try
                        {
                            cmd.Parameters.Clear(); // ← CRUCIA

                            // Comando SQL con parámetros
                            cmd.CommandText = @"
                        INSERT INTO cambiopension (
                            entidadesnominaanteriorid,
                            entidadesnominanuevaid,
                            usuarioid,
                            fechahora,
                            empleadoid,
                            reportartraslado,
                            fechacambio
                        )
                        VALUES (
                            (SELECT DISTINCT id FROM public.""EntidadesNomina"" WHERE codigo = @codAnt),
                            (SELECT DISTINCT id FROM public.""EntidadesNomina""WHERE codigo = @codAct),
                            (SELECT DISTINCT  ""Id"" FROM public.""AspNetUsers"" WHERE ""UserName"" = @userReg),
                            @fechaHora,
                            (SELECT DISTINCT ""Id"" FROM public.""Empleados"" WHERE ""CodigoEmpleado"" = @empleado),
                            @reportarTraslado,
                            @fechaCambio
                        );";


                            cmd.Parameters.AddWithValue("codAnt", histonove.cod_ant.Trim());
                            cmd.Parameters.AddWithValue("codAct", histonove.cod_act.Trim());
                            cmd.Parameters.AddWithValue("userReg", histonove.usua_reg.Trim());
                            cmd.Parameters.AddWithValue("fechaHora", new DateTime(
                                histonove.fech_reg.Year,
                                histonove.fech_reg.Month,
                                histonove.fech_reg.Day,
                                histonove.hora_reg.Hour,
                                histonove.hora_reg.Minute,
                                histonove.hora_reg.Second
                            ));
                            cmd.Parameters.AddWithValue("empleado", histonove.empleado.Trim());
                            cmd.Parameters.AddWithValue("reportarTraslado", histonove.repo_soi == 1);
                            cmd.Parameters.AddWithValue("fechaCambio", histonove.fech_camb);

                            // Ejecutar el comando de inserción
                            cmd.ExecuteNonQuery();



                            contador++; // Incrementar contador
                            Console.WriteLine($"Cliente insertado #{contador}"); // Mostrar en consola
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error al insertar cliente con documento: {ex.Message}");
                        }
                    }

                    // Confirmar la transacción
                    tx.Commit();
                }
            }

            Console.WriteLine("Datos insertados correctamente.");

        }

        public static void InsertarHistonoveS(List<t_histonove> nove_pen)
        {
            int contador = 0; // Contador

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                // Iniciar transacción
                using (var tx = conn.BeginTransaction())
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.Transaction = tx;

                    foreach (var histonove in nove_pen)
                    {
                        try
                        {
                            cmd.Parameters.Clear(); // ← CRUCIA

                            // Comando SQL con parámetros
                            cmd.CommandText = @"
                        INSERT INTO cambiosalario (
                            salarioanterior,
                            salarionuevo,
                            usuarioid,
                            fechahora,
                            empleadoid,
                            reportavariacion,
                            fechahoravariacion
                        )
                        VALUES (
                            @val_ant,
                            @val_act,
                            (SELECT DISTINCT  ""Id"" FROM public.""AspNetUsers"" WHERE ""UserName"" = @userReg),
                            @fechaHora,
                            (SELECT DISTINCT ""Id"" FROM public.""Empleados"" WHERE ""CodigoEmpleado"" = @empleado),
                            @reportarTraslado,
                            @fechaCambio
                        );";


                            cmd.Parameters.AddWithValue("val_ant", histonove.val_ant);
                            cmd.Parameters.AddWithValue("val_act", histonove.val_act);
                            cmd.Parameters.AddWithValue("userReg", histonove.usua_reg.Trim());
                            cmd.Parameters.AddWithValue("fechaHora", new DateTime(
                                histonove.fech_reg.Year,
                                histonove.fech_reg.Month,
                                histonove.fech_reg.Day,
                                histonove.hora_reg.Hour,
                                histonove.hora_reg.Minute,
                                histonove.hora_reg.Second
                            ));
                            cmd.Parameters.AddWithValue("empleado", histonove.empleado.Trim());
                            cmd.Parameters.AddWithValue("reportarTraslado", histonove.repo_soi == 1);
                            cmd.Parameters.AddWithValue("fechaCambio", histonove.fech_camb);

                            // Ejecutar el comando de inserción
                            cmd.ExecuteNonQuery();



                            contador++; // Incrementar contador
                            Console.WriteLine($"Cliente insertado #{contador}"); // Mostrar en consola
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error al insertar cliente con documento: {ex.Message}");
                            //ex.InnerException
                        }
                    }

                    // Confirmar la transacción
                    tx.Commit();
                }
            }

            Console.WriteLine("Datos insertados correctamente.");

        }

        //public static void InsertarProductos(List<t_items> items_list)
        //{
        //    int contador = 0; // Contador
        //    var cronometer = Stopwatch.StartNew();

        //    Stopwatch sw = new Stopwatch();
        //    sw.Start();


        //    using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
        //    {
        //        conn.Open();

        //        // Iniciar transacción
        //        using (var tx = conn.BeginTransaction())
        //        using (var cmd = new NpgsqlCommand())
        //        {
        //            cmd.Connection = conn;
        //            cmd.Transaction = tx;

        //            foreach (var items in items_list)
        //            {
        //                try
        //                {
        //                    cmd.Parameters.Clear(); // ← CRUCIAL

        //                    // Comando SQL con parámetros
        //cmd.CommandText = $@"
        //INSERT INTO ""Productos"" (
        //                    ""Codigo"", ""IdTipoProducto"", ""FechaCreacion"", ""Nombre"", ""NombreCorto"", ""ReferenciaFabrica"",
        //                    ""PesoUnitario"", ""UnidadesPorCaja"", ""IdCategoria"", ""IdSubgruposProductos"", ""IdMarcasProductos"",
        //                    ""PesoDrenado"", ""Ean8"", ""Ean13"", ""Estado"", ""Unidad"", ""Talla"", ""IdLineasProductos"", ""IdSublineasProductos"",
        //                    ""NoAutorizar"", ""Inactivo"", ""EsKIT"", ""Iva"", ""Precio"", ""PrecioAnterior"", ""PrecioCambio"", ""Rentabilidad"",
        //                    ""BaseImpuestoConsumo"", ""IvaExento"", ""ImpuestoConsumo"", ""IdGrupoContable"", ""FactorImpuestoConsumo"",
        //                    ""IdRetFuente"", ""CostoAjuste"", ""Fruver"", ""BolsaAgro"", ""ModificaPrecio"", ""Fenaice"", ""AcumulaTira"",
        //                    ""SubsidioDesempleo"", ""ModificaCantidad"", ""BotFut"", ""ContabilizarGrabado"", ""NoAutorizaCompra"",
        //                    ""BalanzaToledo"", ""PrefijoEAN"", ""DescuentoAsohofrucol"", ""Motocicleta"", ""Pedido"", ""Codensa"", ""Ingreso"",
        //                    ""CheckeoPrecio"", ""IdEvento"", ""Tipo"", ""AdmiteDescuento"", ""DescuentoMaximo"", ""Decimales"", ""FechaCambio"",
        //                    ""CostoReposicion"", ""FenaicePorcentaje"", ""BotFutPorcentaje"", ""PrefijoEANPorcentaje"", ""CodensaPorcentaje"",
        //                    ""CodigoAlterno"",""CCosto"",""ExcluirDeListado"",""PermitirSaldoEnRojo"",""ExigirTarjetMercapesos"",""Corrosivo"",
        //                    ""CAS"",""CategoriaCAS"",""PermitirVentaPorDebajoCosto"",""DetalleAlElaborarDocumento"",""UnicamenteManejarCantidad"",
        //                    ""SolicitarSerieAlFacturar"",""TerceroAutomaticoPOS"",""ProductoGenerico"",""PesoBruto"",""PesoNeto"",""PesoTara"",
        //                    ""UnidadMinimaDeVenta"",""Factor"",""InicioMercapesos"",""FinalMercapesos"",""DescuentoEspecial"",
        //                    ""FactorDescuento"",""ValorDescuentoFijo"",""InicioDescuento"",""FinalDescuento"",""DecodificarProducto"",
        //                    ""DiaDecodificacion"",""DiaUsuarioDecodificacion"",""ValidarMaxVentasPorTercero"",""UnidadMaximaVentas"",""NoIncluirDespacho"",""FechaBloqueoPedido"",
        //                    ""TipoControl"",""DiasSiguientesCompra"",""PrecioVenta3"",""NoIncluirEnInventarioParaPedido"",""AutorizarTrasladoProducto"",
        //                    ""PrecioControlVingilancia"",""NoIncluirreporteChequeo"",""NoCalcularCostoPromedio"",""ProductoSobreStock"",""EnviarAAsopanela"",""UnidadArticulo"",
        //                    ""GrupoDescuento"",""CodigoEan1"",""CodigoEan2"",""ProductoInAndOut"",""DomiciliosCom"",""PesoPOS"",
        //                    ""OrdenadoInicioFactura"",""ModificarConTolerancia"",""PorcentajeTolerancia"",""Stock"",""ExcluidoCOVID"",""IdUnidadDIAN"",
        //                    ""IdTipoBienDIAN"",""DiasSinIVA"",""DescuentoFNFP"",""IdVariedadFNFP"",""DescuentoNIIF"",""NoUtilizar"",
        //                    ""FactorImpuestoConsumoRest"",""IdBodega"",""IdUnidadProducto"",""PrecioIva"",""ValorIva"",""CostoAjusteNIIF"",
        //                    ""DescuentoPorcentaje"",""RentabilidadSugerida"",""ConfirmarCambioPrecio"",""ProductoOfertado"",""FechaPrimerMovimiento"",""TipoImpuestoAlimentos"",
        //                    ""ValorTipoImpuestoAlimentos"",""GeneraImpuestoSaludable"",""CodigoReferencia"",""Referencia""
        //                    ) VALUES(
        //                     @Codigo, @IdTipoProducto, @FechaCreacion, @Nombre, @NombreCorto, @ReferenciaFabrica,
        //                     @PesoUnitario, @UnidadesPorCaja, @IdCategoria,
        //                     (SELECT DISTINCT ""id"" FROM ""SubgruposProductos"" WHERE Codigo = @subgrupo LIMIT 1),
        //                     (SELECT DISTINCT ""id"" FROM ""MarcasProductos"" WHERE Codigo = @marca LIMIT 1) ,
        //                     @PesoDrenado, @Ean8, @Ean13, @Estado,@Unidad,@Talla,
        //                     (SELECT DISTINCT ""id"" FROM ""LineasProductos"" WHERE Codigo = @linea LIMIT 1),
        //                     (SELECT DISTINCT ""id"" FROM ""SublineasProductos"" WHERE Codigo = @sublinea LIMIT 1),
        //                     @NoAutorizar, @Inactivo,@EsKIT, @Iva, @Precio,@PrecioAnterior,@PrecioCambio,@Rentabilidad,
        //                     @BaseImpuestoConsumo,@IvaExento,@ImpuestoConsumo,@IdGrupoContable,@FactorImpuestoConsumo,
        //                     @IdRetFuente,@CostoAjuste, @Fruver,@BolsaAgro,@ModificaPrecio,@Fenaice,@AcumulaTira,
        //                     @SubsidioDesempleo, @ModificaCantidad, @BotFut, @ContabilizarGrabado, @NoAutorizaCompra,
        //                     @BalanzaToledo, @PrefijoEAN, @DescuentoAsohofrucol, @Motocicleta, @Pedido, @Codensa, @Ingreso,
        //                     @CheckeoPrecio, @IdEvento, @Tipo, @AdmiteDescuento, @DescuentoMaximo, @Decimales, @FechaCambio,
        //                     @CostoReposicion, @FenaicePorcentaje, @BotFutPorcentaje, @PrefijoEANPorcentaje, @CodensaPorcentaje,
        //                     @CodigoAlterno,@CCosto,@ExcluirDeListado,@PermitirSaldoEnRojo,@ExigirTarjetMercapesos,@Corrosivo,
        //                     @CAS,@CategoriaCAS,@PermitirVentaPorDebajoCosto,@DetalleAlElaborarDocumento,@UnicamenteManejarCantidad,
        //                     @SolicitarSerieAlFacturar,@TerceroAutomaticoPOS,@ProductoGenerico,@PesoBruto,@PesoNeto,@PesoTara,
        //                     @UnidadMinimaDeVenta,@Factor,@InicioMercapesos,@FinalMercapesos,@DescuentoEspecial,
        //                     @FactorDescuento,@ValorDescuentoFijo,@InicioDescuento,@FinalDescuento,@DecodificarProducto,
        //                     @DiaDecodificacion,@DiaUsuarioDecodificacion,@ValidarMaxVentasPorTercero,@UnidadMaximaVentas,@NoIncluirDespacho,@FechaBloqueoPedido,
        //                     @TipoControl,@DiasSiguientesCompra,@PrecioVenta3,@NoIncluirEnInventarioParaPedido,@AutorizarTrasladoProducto,
        //                     @PrecioControlVingilancia,@NoIncluirreporteChequeo,@NoCalcularCostoPromedio,@ProductoSobreStock,@EnviarAAsopanela,@UnidadArticulo,
        //                     @GrupoDescuento,@CodigoEan1,@CodigoEan2,@ProductoInAndOut,@DomiciliosCom,@PesoPOS,
        //                     @OrdenadoInicioFactura,@ModificarConTolerancia,@PorcentajeTolerancia,@Stock,@ExcluidoCOVID,
        //                     (SELECT DISTINCT ""Id"" FROM ""UnidadesDIAN"" WHERE ""Codigo"" = @unidcantfe LIMIT 1),
        //                     (SELECT DISTINCT ""Id"" FROM ""TipoBienDIAN"" WHERE ""Codigo"" = @tipobiend LIMIT 1),
        //                     @DiasSinIVA, @DescuentoFNFP, @IdVariedadFNFP, @DescuentoNIIF, @NoUtilizar,
        //                     @FactorImpuestoConsumoRest,
        //                     (SELECT DISTINCT id FROM bodegas WHERE Codigo = @bod_asoc LIMIT 1),
        //                     (SELECT DISTINCT id FROM unidaddemedida WHERE descripcion = @unidad LIMIT 1),
        //                     @PrecioIva, @ValorIva, @CostoAjusteNIIF,
        //                     @DescuentoPorcentaje,
        //                     @RentabilidadSugerida, @ConfirmarCambioPrecio, @ProductoOfertado, @FechaPrimerMovimiento, @TipoImpuestoAlimentos,
        //                     @ValorTipoImpuestoAlimentos, @GeneraImpuestoSaludable, @CodigoReferencia, @Referencia
        //                    );";

        //                    string shortnm = items.shortname?.Length > 50 ? items.shortname.Substring(0, 50) : items.shortname;

        //                    cmd.Parameters.AddWithValue("@Codigo", items.Codigo);
        //                    cmd.Parameters.AddWithValue("@IdTipoProducto", int.Parse(items.tipo));
        //                    cmd.Parameters.AddWithValue("@FechaCreacion", items.fecha_cre);
        //                    cmd.Parameters.AddWithValue("@Nombre", items.nombre);
        //                    cmd.Parameters.AddWithValue("@NombreCorto", shortnm);
        //                    cmd.Parameters.AddWithValue("@ReferenciaFabrica", items.refabrica);
        //                    cmd.Parameters.AddWithValue("@PesoUnitario", items.peso_uni);
        //                    cmd.Parameters.AddWithValue("@UnidadesPorCaja", items.undxcaja);
        //                    cmd.Parameters.AddWithValue("@IdCategoria", 0);
        //                    cmd.Parameters.AddWithValue("@subgrupo", items.subgrupo);
        //                    cmd.Parameters.AddWithValue("@marca", items.marca);
        //                    cmd.Parameters.AddWithValue("@PesoDrenado", items.pdrenado);
        //                    cmd.Parameters.AddWithValue("@Ean8", items.cod_ean8);
        //                    cmd.Parameters.AddWithValue("@Ean13", items.cod_bar);
        //                    cmd.Parameters.AddWithValue("@Estado", items.bloqueado == 1 ? false : true);
        //                    cmd.Parameters.AddWithValue("@Unidad", items.unidad);
        //                    cmd.Parameters.AddWithValue("@Talla", items.talla);
        //                    cmd.Parameters.AddWithValue("@linea", items.linea);
        //                    cmd.Parameters.AddWithValue("@sublinea", items.sublinea);
        //                    cmd.Parameters.AddWithValue("@NoAutorizar", items.no_compra);
        //                    cmd.Parameters.AddWithValue("@Inactivo", items.bloqueado);
        //                    cmd.Parameters.AddWithValue("@EsKIT", items.es_kitpro ==1 ? false:true);
        //                    cmd.Parameters.AddWithValue("@Iva", items.iva);
        //                    cmd.Parameters.AddWithValue("@Precio", items.pvtali);
        //                    cmd.Parameters.AddWithValue("@PrecioAnterior", items.pvta_a1);
        //                    cmd.Parameters.AddWithValue("@PrecioCambio", items.cambiopv_1);
        //                    cmd.Parameters.AddWithValue("@Rentabilidad", CalcularRentabilidad(items.pvta1i, items.costo_rep, 3, items.iva, items.iconsumo, items.listap, items.imp_salu, items.vr_imps, DateTime.Now , items.gen_impu));
        //                    cmd.Parameters.AddWithValue("@BaseImpuestoConsumo", items.iconsumo > 0 ? true : false);
        //                    cmd.Parameters.AddWithValue("@IvaExento", items.excluido == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@ImpuestoConsumo", items.listap);
        //                    cmd.Parameters.AddWithValue("@IdGrupoContable", 0);
        //                    cmd.Parameters.AddWithValue("@FactorImpuestoConsumo", items.F_ICONSUMO);
        //                    cmd.Parameters.AddWithValue("@IdRetFuente", 0);
        //                    cmd.Parameters.AddWithValue("@CostoAjuste", items.costoajus);
        //                    cmd.Parameters.AddWithValue("@Fruver", items.es_fruver == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@BolsaAgro", items.bolsa == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@ModificaPrecio", items.mod_ppos == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@Fenaice", string.IsNullOrWhiteSpace(items.fenalce) ? true : false);
        //                    cmd.Parameters.AddWithValue("@AcumulaTira", items.acu_tpos == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@SubsidioDesempleo", items.subsidio == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@ModificaCantidad", items.mod_qpos == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@BotFut", items.es_bol == "1" ? true : false);
        //                    cmd.Parameters.AddWithValue("@ContabilizarGrabado", items.contabgrav == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@NoAutorizaCompra", items.no_compra == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@BalanzaToledo", items.sitoledo == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@PrefijoEAN", string.IsNullOrWhiteSpace(items.pref_ean) ? true : false);
        //                    cmd.Parameters.AddWithValue("@DescuentoAsohofrucol", items.es_bordado ==1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@Motocicleta", items.es_moto == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@Pedido", items.sipedido == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@Codensa", items.escodensa == "S" ? true : false);
        //                    cmd.Parameters.AddWithValue("@Ingreso", items.es_ingreso ==1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@CheckeoPrecio", items.cheqpr == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@IdEvento", 0);
        //                    cmd.Parameters.AddWithValue("@Tipo", short.Parse(items.tipo));
        //                    cmd.Parameters.AddWithValue("@AdmiteDescuento", items.si_descto == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@DescuentoMaximo", items.descmax);
        //                    cmd.Parameters.AddWithValue("@Decimales", items.deci_cant);
        //                    cmd.Parameters.AddWithValue("@FechaCambio", items.fech_cp1);
        //                    cmd.Parameters.AddWithValue("@CostoReposicion", items.costo_rep);
        //                    cmd.Parameters.AddWithValue("@FenaicePorcentaje", 0);
        //                    cmd.Parameters.AddWithValue("@BotFutPorcentaje", 0);
        //                    cmd.Parameters.AddWithValue("@PrefijoEANPorcentaje", string.IsNullOrWhiteSpace(items.pref_ean)?0: int.Parse(items.pref_ean));
        //                    cmd.Parameters.AddWithValue("@CodensaPorcentaje", 0);
        //                    cmd.Parameters.AddWithValue("@CodigoAlterno", items.cod_alt);
        //                    cmd.Parameters.AddWithValue("@CCosto",string.IsNullOrWhiteSpace(items.CCosto)?DBNull.Value:int.Parse(items.CCosto));
        //                    cmd.Parameters.AddWithValue("@ExcluirDeListado", items.elegido == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@PermitirSaldoEnRojo", items.sdo_rojo == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@ExigirTarjetMercapesos", items.pidempeso == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@Corrosivo", items.corrosivo == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@CAS", string.IsNullOrWhiteSpace(items.n_cas)?DBNull.Value: items.n_cas);
        //                    cmd.Parameters.AddWithValue("@CategoriaCAS", string.IsNullOrWhiteSpace(items.cat_cas) ? DBNull.Value : items.cat_cas);
        //                    cmd.Parameters.AddWithValue("@PermitirVentaPorDebajoCosto", items.vta_costo == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@DetalleAlElaborarDocumento", items.si_detdoc == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@UnicamenteManejarCantidad", items.solocant == 1 ? true :false);
        //                    cmd.Parameters.AddWithValue("@SolicitarSerieAlFacturar", items.si_serie == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@TerceroAutomaticoPOS", items.terceAutom == 1 ? true :false);
        //                    cmd.Parameters.AddWithValue("@ProductoGenerico", items.generico == 1 ?true : false);
        //                    cmd.Parameters.AddWithValue("@PesoBruto", items.pesocajb);
        //                    cmd.Parameters.AddWithValue("@PesoNeto", items.pesocajn);
        //                    cmd.Parameters.AddWithValue("@PesoTara", items.peso_car);
        //                    cmd.Parameters.AddWithValue("@UnidadMinimaDeVenta", items.unidmin);
        //                    cmd.Parameters.AddWithValue("@Factor", items.puntaje);
        //                    cmd.Parameters.AddWithValue("@InicioMercapesos", items.fecha1_mp == null ? DBNull.Value:items.fecha1_mp);
        //                    cmd.Parameters.AddWithValue("@FinalMercapesos", items.fecha2_mp == null ? DBNull.Value : items.fecha2_mp);
        //                    cmd.Parameters.AddWithValue("@DescuentoEspecial", items.desc_esp == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@FactorDescuento", items.fact_esp);
        //                    cmd.Parameters.AddWithValue("@ValorDescuentoFijo", items.valor_esp);
        //                    cmd.Parameters.AddWithValue("@InicioDescuento", items.fechdesci == null ? DBNull.Value : items.fechdesci);
        //                    cmd.Parameters.AddWithValue("@FinalDescuento", items.fechdescf == null ? DBNull.Value : items.fechdescf);
        //                    cmd.Parameters.AddWithValue("@DecodificarProducto", items.descod == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@DiaDecodificacion", items.descod_f == null ? DBNull.Value : items.descod_f);
        //                    cmd.Parameters.AddWithValue("@DiaUsuarioDecodificacion", items.fchdescusr == null ? DBNull.Value : items.fchdescusr);
        //                    cmd.Parameters.AddWithValue("@ValidarMaxVentasPorTercero", items.validmax == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@UnidadMaximaVentas", items.maxventa);
        //                    cmd.Parameters.AddWithValue("@NoIncluirDespacho", items.val_desp == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@FechaBloqueoPedido", items.f_bloqp == null ? DBNull.Value : items.f_bloqp);
        //                    cmd.Parameters.AddWithValue("@TipoControl", items.cont_devol);
        //                    cmd.Parameters.AddWithValue("@DiasSiguientesCompra", 0);
        //                    cmd.Parameters.AddWithValue("@PrecioVenta3", items.pvta3i);
        //                    cmd.Parameters.AddWithValue("@NoIncluirEnInventarioParaPedido", items.no_invped == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@AutorizarTrasladoProducto", items.aut_trasl == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@PrecioControlVingilancia", items.lp_cyvig == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@NoIncluirreporteChequeo", items.chequeo == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@NoCalcularCostoPromedio", items.costeo2 == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@ProductoSobreStock", items.sobrestock == 1 ? true :false);
        //                    cmd.Parameters.AddWithValue("@EnviarAAsopanela", items.Asopanela ==1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@UnidadArticulo", 0);
        //                    cmd.Parameters.AddWithValue("@GrupoDescuento", string.IsNullOrWhiteSpace(items.grupodes)? DBNull.Value:items.grupodes);
        //                    cmd.Parameters.AddWithValue("@CodigoEan1", string.IsNullOrWhiteSpace(items.ean_1) ? DBNull.Value : items.ean_1);
        //                    cmd.Parameters.AddWithValue("@CodigoEan2", string.IsNullOrWhiteSpace(items.ean_2) ? DBNull.Value : items.ean_2);
        //                    cmd.Parameters.AddWithValue("@ProductoInAndOut", items.inandout == 1 ? true :false);
        //                    cmd.Parameters.AddWithValue("@DomiciliosCom", items.domi_com == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@PesoPOS", items.si_descto == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@OrdenadoInicioFactura", items.ord_prio == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@ModificarConTolerancia", items.modq_reg ==1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@PorcentajeTolerancia", items.mod_toler);
        //                    cmd.Parameters.AddWithValue("@Stock", items.stockdomi);
        //                    cmd.Parameters.AddWithValue("@ExcluidoCOVID", items.ext_covid ==1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@unidcantfe", string.IsNullOrWhiteSpace(items.unidcantfe)?DBNull.Value:items.unidcantfe);
        //                    cmd.Parameters.AddWithValue("@tipobiend", string.IsNullOrWhiteSpace(items.tipobiend) ? DBNull.Value : items.tipobiend);
        //                    cmd.Parameters.AddWithValue("@DiasSinIVA", items.pdiasiva == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@DescuentoFNFP", items.es_dfnfp ==1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@IdVariedadFNFP", items.varifnfp);
        //                    cmd.Parameters.AddWithValue("@DescuentoNIIF", items.DESFINNIIF);
        //                    cmd.Parameters.AddWithValue("@NoUtilizar", false);
        //                    cmd.Parameters.AddWithValue("@FactorImpuestoConsumoRest", items.F_ICONSUMO == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@bod_asoc", items.bod_asoc);
        //                    cmd.Parameters.AddWithValue("@PrecioIva", items.pvtali);
        //                    cmd.Parameters.AddWithValue("@ValorIva", FCalcImp(items.pvta1i, items.iva, items.iconsumo, 1, 2, items.imp_salu, items.vr_imps, DateTime.Now, items.gen_impu, "I"));
        //                    cmd.Parameters.AddWithValue("@CostoAjusteNIIF", items.costoajus);
        //                    cmd.Parameters.AddWithValue("@DescuentoPorcentaje", items.descuento);
        //                    cmd.Parameters.AddWithValue("@RentabilidadSugerida", items.por_rentab);
        //                    cmd.Parameters.AddWithValue("@ConfirmarCambioPrecio", items.confirpre == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@ProductoOfertado", items.ofertado ==1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@FechaPrimerMovimiento", items.fech1comp == null ? DBNull.Value : items.fech1comp);
        //                    cmd.Parameters.AddWithValue("@TipoImpuestoAlimentos", items.imp_salu);
        //                    cmd.Parameters.AddWithValue("@ValorTipoImpuestoAlimentos", items.vr_imps);
        //                    cmd.Parameters.AddWithValue("@GeneraImpuestoSaludable", items.gen_impu == 1 ? true : false);
        //                    cmd.Parameters.AddWithValue("@CodigoReferencia", items.cod_ref);
        //                    cmd.Parameters.AddWithValue("@Referencia", items.refer);

        //                    // Ejecutar el comando de inserción
        //                    cmd.ExecuteNonQuery();



        //                    contador++; // Incrementar contador
        //                    Console.WriteLine($"Producto insertado #{contador}"); // Mostrar en consola
        //                }
        //                catch (Exception ex)
        //                {
        //                    Console.WriteLine($"⚠️ Error al insertar producto: {ex.Message}");


        //                    foreach (NpgsqlParameter p in cmd.Parameters)
        //                    {
        //                        if (p.Value is string s && s.Length > 50)
        //                        {
        //                            Console.WriteLine($"Posible error en parámetro: {p.ParameterName} (valor='{s}', longitud={s.Length})");
        //                        }
        //                    }

        //                    throw;


        //                    sw.Stop();

        //                    TimeSpan tiempoTran = sw.Elapsed;

        //                    cronometer.Stop();
        //                    Console.WriteLine("tiempo ejecucion"  + tiempoTran.ToString());
        //                    Console.WriteLine($"Guardado completado en: {cronometer.Elapsed.TotalMilliseconds} ms");


        //                    conn.Close();
        //                    //ex.InnerException
        //                }
        //            }

        //            // Confirmar la transacción
        //            tx.Commit();

        //        }
        //    }

        //    Console.WriteLine("Datos insertados correctamente.");

        //}


        public static void InsertarProductos(List<t_items> items_list, int batchSize = 100)
        {
            int contador = 0;
            var cronometer = Stopwatch.StartNew();
            Stopwatch sw = new Stopwatch();
            sw.Start();

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                while (contador < items_list.Count)
                {
                    using (var tx = conn.BeginTransaction())
                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.Transaction = tx;

                        var commandText = new StringBuilder();
                        commandText.AppendLine(@"
                    INSERT INTO ""Productos"" (
                        ""Codigo"", ""IdTipoProducto"", ""FechaCreacion"", ""Nombre"", ""NombreCorto"", ""ReferenciaFabrica"",
                        ""PesoUnitario"", ""UnidadesPorCaja"", ""IdCategoria"", ""IdSubgruposProductos"", ""IdMarcasProductos"",
                        ""PesoDrenado"", ""Ean8"", ""Ean13"", ""Estado"", ""Unidad"", ""Talla"", ""IdLineasProductos"", ""IdSublineasProductos"",
                        ""NoAutorizar"", ""Inactivo"", ""EsKIT"", ""Iva"", ""Precio"", ""PrecioAnterior"", ""PrecioCambio"", ""Rentabilidad"",
                        ""BaseImpuestoConsumo"", ""IvaExento"", ""ImpuestoConsumo"", ""IdGrupoContable"", ""FactorImpuestoConsumo"",
                        ""IdRetFuente"", ""CostoAjuste"", ""Fruver"", ""BolsaAgro"", ""ModificaPrecio"", ""Fenaice"", ""AcumulaTira"",
                        ""SubsidioDesempleo"", ""ModificaCantidad"", ""BotFut"", ""ContabilizarGrabado"", ""NoAutorizaCompra"",
                        ""BalanzaToledo"", ""PrefijoEAN"", ""DescuentoAsohofrucol"", ""Motocicleta"", ""Pedido"", ""Codensa"", ""Ingreso"",
                        ""CheckeoPrecio"", ""IdEvento"", ""Tipo"", ""AdmiteDescuento"", ""DescuentoMaximo"", ""Decimales"", ""FechaCambio"",
                        ""CostoReposicion"", ""FenaicePorcentaje"", ""BotFutPorcentaje"", ""PrefijoEANPorcentaje"", ""CodensaPorcentaje"",
                        ""CodigoAlterno"", ""CCosto"", ""ExcluirDeListado"", ""PermitirSaldoEnRojo"", ""ExigirTarjetMercapesos"", ""Corrosivo"",
                        ""CAS"", ""CategoriaCAS"", ""PermitirVentaPorDebajoCosto"", ""DetalleAlElaborarDocumento"", ""UnicamenteManejarCantidad"",
                        ""SolicitarSerieAlFacturar"", ""TerceroAutomaticoPOS"", ""ProductoGenerico"", ""PesoBruto"", ""PesoNeto"", ""PesoTara"",
                        ""UnidadMinimaDeVenta"", ""Factor"", ""InicioMercapesos"", ""FinalMercapesos"", ""DescuentoEspecial"",
                        ""FactorDescuento"", ""ValorDescuentoFijo"", ""InicioDescuento"", ""FinalDescuento"", ""DecodificarProducto"",
                        ""DiaDecodificacion"", ""DiaUsuarioDecodificacion"", ""ValidarMaxVentasPorTercero"", ""UnidadMaximaVentas"", ""NoIncluirDespacho"", ""FechaBloqueoPedido"",
                        ""TipoControl"", ""DiasSiguientesCompra"", ""PrecioVenta3"", ""NoIncluirEnInventarioParaPedido"", ""AutorizarTrasladoProducto"",
                        ""PrecioControlVingilancia"", ""NoIncluirreporteChequeo"", ""NoCalcularCostoPromedio"", ""ProductoSobreStock"", ""EnviarAAsopanela"", ""UnidadArticulo"",
                        ""GrupoDescuento"", ""CodigoEan1"", ""CodigoEan2"", ""ProductoInAndOut"", ""DomiciliosCom"", ""PesoPOS"",
                        ""OrdenadoInicioFactura"", ""ModificarConTolerancia"", ""PorcentajeTolerancia"", ""Stock"", ""ExcluidoCOVID"", ""IdUnidadDIAN"",
                        ""IdTipoBienDIAN"", ""DiasSinIVA"", ""DescuentoFNFP"", ""IdVariedadFNFP"", ""DescuentoNIIF"", ""NoUtilizar"",
                        ""FactorImpuestoConsumoRest"", ""IdBodega"", ""IdUnidadProducto"", ""PrecioIva"", ""ValorIva"", ""CostoAjusteNIIF"",
                        ""DescuentoPorcentaje"", ""RentabilidadSugerida"", ""ConfirmarCambioPrecio"", ""ProductoOfertado"", ""FechaPrimerMovimiento"", ""TipoImpuestoAlimentos"",
                        ""ValorTipoImpuestoAlimentos"", ""GeneraImpuestoSaludable"", ""CodigoReferencia"", ""Referencia""
                    ) VALUES ");

                        var valuesList = new List<string>();
                        int currentBatchSize = 0;

                        while (contador < items_list.Count && currentBatchSize < batchSize)
                        {
                            var items = items_list[contador];
                            string shortnm = items.shortname?.Length > 50 ? items.shortname.Substring(0, 50) : items.shortname;

                            var values = $@"
                    (
                        @Codigo{contador}, @IdTipoProducto{contador}, @FechaCreacion{contador}, @Nombre{contador}, @NombreCorto{contador}, 
                        @ReferenciaFabrica{contador}, @PesoUnitario{contador}, @UnidadesPorCaja{contador}, @IdCategoria{contador},
                        (SELECT DISTINCT ""id"" FROM ""SubgruposProductos"" WHERE Codigo = @subgrupo{contador} LIMIT 1),
                        (SELECT DISTINCT ""id"" FROM ""MarcasProductos"" WHERE Codigo = @marca{contador} LIMIT 1),
                        @PesoDrenado{contador}, @Ean8{contador}, @Ean13{contador}, @Estado{contador}, @Unidad{contador}, @Talla{contador},
                        (SELECT DISTINCT ""id"" FROM ""LineasProductos"" WHERE Codigo = @linea{contador} LIMIT 1),
                        (SELECT DISTINCT ""id"" FROM ""SublineasProductos"" WHERE Codigo = @sublinea{contador} LIMIT 1),
                        @NoAutorizar{contador}, @Inactivo{contador}, @EsKIT{contador}, @Iva{contador}, @Precio{contador}, 
                        @PrecioAnterior{contador}, @PrecioCambio{contador}, @Rentabilidad{contador},
                        @BaseImpuestoConsumo{contador}, @IvaExento{contador}, @ImpuestoConsumo{contador}, @IdGrupoContable{contador}, 
                        @FactorImpuestoConsumo{contador}, @IdRetFuente{contador}, @CostoAjuste{contador}, @Fruver{contador}, 
                        @BolsaAgro{contador}, @ModificaPrecio{contador}, @Fenaice{contador}, @AcumulaTira{contador},
                        @SubsidioDesempleo{contador}, @ModificaCantidad{contador}, @BotFut{contador}, @ContabilizarGrabado{contador}, 
                        @NoAutorizaCompra{contador}, @BalanzaToledo{contador}, @PrefijoEAN{contador}, @DescuentoAsohofrucol{contador}, 
                        @Motocicleta{contador}, @Pedido{contador}, @Codensa{contador}, @Ingreso{contador},
                        @CheckeoPrecio{contador}, @IdEvento{contador}, @Tipo{contador}, @AdmiteDescuento{contador}, 
                        @DescuentoMaximo{contador}, @Decimales{contador}, @FechaCambio{contador},
                        @CostoReposicion{contador}, @FenaicePorcentaje{contador}, @BotFutPorcentaje{contador}, 
                        @PrefijoEANPorcentaje{contador}, @CodensaPorcentaje{contador},
                        @CodigoAlterno{contador}, @CCosto{contador}, @ExcluirDeListado{contador}, @PermitirSaldoEnRojo{contador}, 
                        @ExigirTarjetMercapesos{contador}, @Corrosivo{contador},
                        @CAS{contador}, @CategoriaCAS{contador}, @PermitirVentaPorDebajoCosto{contador}, 
                        @DetalleAlElaborarDocumento{contador}, @UnicamenteManejarCantidad{contador},
                        @SolicitarSerieAlFacturar{contador}, @TerceroAutomaticoPOS{contador}, @ProductoGenerico{contador}, 
                        @PesoBruto{contador}, @PesoNeto{contador}, @PesoTara{contador},
                        @UnidadMinimaDeVenta{contador}, @Factor{contador}, @InicioMercapesos{contador}, @FinalMercapesos{contador}, 
                        @DescuentoEspecial{contador},
                        @FactorDescuento{contador}, @ValorDescuentoFijo{contador}, @InicioDescuento{contador}, 
                        @FinalDescuento{contador}, @DecodificarProducto{contador},
                        @DiaDecodificacion{contador}, @DiaUsuarioDecodificacion{contador}, @ValidarMaxVentasPorTercero{contador}, 
                        @UnidadMaximaVentas{contador}, @NoIncluirDespacho{contador}, @FechaBloqueoPedido{contador},
                        @TipoControl{contador}, @DiasSiguientesCompra{contador}, @PrecioVenta3{contador}, 
                        @NoIncluirEnInventarioParaPedido{contador}, @AutorizarTrasladoProducto{contador},
                        @PrecioControlVingilancia{contador}, @NoIncluirreporteChequeo{contador}, @NoCalcularCostoPromedio{contador}, 
                        @ProductoSobreStock{contador}, @EnviarAAsopanela{contador}, @UnidadArticulo{contador},
                        @GrupoDescuento{contador}, @CodigoEan1{contador}, @CodigoEan2{contador}, @ProductoInAndOut{contador}, 
                        @DomiciliosCom{contador}, @PesoPOS{contador},
                        @OrdenadoInicioFactura{contador}, @ModificarConTolerancia{contador}, @PorcentajeTolerancia{contador}, 
                        @Stock{contador}, @ExcluidoCOVID{contador},
                        (SELECT DISTINCT ""Id"" FROM ""UnidadesDIAN"" WHERE ""Codigo"" = @unidcantfe{contador} LIMIT 1),
                        (SELECT DISTINCT ""Id"" FROM ""TipoBienDIAN"" WHERE ""Codigo"" = @tipobiend{contador} LIMIT 1),
                        @DiasSinIVA{contador}, @DescuentoFNFP{contador}, @IdVariedadFNFP{contador}, @DescuentoNIIF{contador}, 
                        @NoUtilizar{contador},
                        @FactorImpuestoConsumoRest{contador},
                        (SELECT DISTINCT id FROM bodegas WHERE Codigo = @bod_asoc{contador} LIMIT 1),
                        (SELECT DISTINCT id FROM unidaddemedida WHERE descripcion = @unidad{contador} LIMIT 1),
                        @PrecioIva{contador}, @ValorIva{contador}, @CostoAjusteNIIF{contador},
                        @DescuentoPorcentaje{contador}, @RentabilidadSugerida{contador}, @ConfirmarCambioPrecio{contador}, 
                        @ProductoOfertado{contador}, @FechaPrimerMovimiento{contador}, @TipoImpuestoAlimentos{contador},
                        @ValorTipoImpuestoAlimentos{contador}, @GeneraImpuestoSaludable{contador}, @CodigoReferencia{contador}, 
                        @Referencia{contador}
                    )";

                            valuesList.Add(values);
                            contador++;
                            currentBatchSize++;
                        }

                        // Combina todos los VALUES en una sola consulta
                        commandText.Append(string.Join(",", valuesList));

                        // Asigna el comando completo
                        cmd.CommandText = commandText.ToString();

                        // Agregar parámetros para el lote actual
                        for (int i = 0; i < currentBatchSize; i++)
                        {
                            var items = items_list[contador - currentBatchSize + i];
                            string shortnm = items.shortname?.Length > 50 ? items.shortname.Substring(0, 50) : items.shortname;
                            cmd.Parameters.AddWithValue($"@Codigo{contador - currentBatchSize + i}", items.Codigo);
                            cmd.Parameters.AddWithValue($"@IdTipoProducto{contador - currentBatchSize + i}", int.Parse(items.tipo));
                            cmd.Parameters.AddWithValue($"@FechaCreacion{contador - currentBatchSize + i}", items.fecha_cre);
                            cmd.Parameters.AddWithValue($"@Nombre{contador - currentBatchSize + i}", items.nombre);
                            cmd.Parameters.AddWithValue($"@NombreCorto{contador - currentBatchSize + i}", shortnm);
                            cmd.Parameters.AddWithValue($"@ReferenciaFabrica{contador - currentBatchSize + i}", items.refabrica);
                            cmd.Parameters.AddWithValue($"@PesoUnitario{contador - currentBatchSize + i}", items.peso_uni);
                            cmd.Parameters.AddWithValue($"@UnidadesPorCaja{contador - currentBatchSize + i}", items.undxcaja);
                            cmd.Parameters.AddWithValue($"@IdCategoria{contador - currentBatchSize + i}", 0); // Asignar 0 o el valor correspondiente
                            cmd.Parameters.AddWithValue($"@subgrupo{contador - currentBatchSize + i}", items.subgrupo);
                            cmd.Parameters.AddWithValue($"@marca{contador - currentBatchSize + i}", items.marca);
                            cmd.Parameters.AddWithValue($"@PesoDrenado{contador - currentBatchSize + i}", items.pdrenado);
                            cmd.Parameters.AddWithValue($"@Ean8{contador - currentBatchSize + i}", items.cod_ean8);
                            cmd.Parameters.AddWithValue($"@Ean13{contador - currentBatchSize + i}", items.cod_bar);
                            cmd.Parameters.AddWithValue($"@Estado{contador - currentBatchSize + i}", items.bloqueado == 1 ? false : true);
                            cmd.Parameters.AddWithValue($"@Unidad{contador - currentBatchSize + i}", items.unidad);
                            cmd.Parameters.AddWithValue($"@Talla{contador - currentBatchSize + i}", items.talla);
                            cmd.Parameters.AddWithValue($"@linea{contador - currentBatchSize + i}", items.linea);
                            cmd.Parameters.AddWithValue($"@sublinea{contador - currentBatchSize + i}", items.sublinea);
                            cmd.Parameters.AddWithValue($"@NoAutorizar{contador - currentBatchSize + i}", items.no_compra);
                            cmd.Parameters.AddWithValue($"@Inactivo{contador - currentBatchSize + i}", items.bloqueado);
                            cmd.Parameters.AddWithValue($"@EsKIT{contador - currentBatchSize + i}", items.es_kitpro == 1 ? false : true);
                            cmd.Parameters.AddWithValue($"@Iva{contador - currentBatchSize + i}", items.iva);
                            cmd.Parameters.AddWithValue($"@Precio{contador - currentBatchSize + i}", items.pvtali);
                            cmd.Parameters.AddWithValue($"@PrecioAnterior{contador - currentBatchSize + i}", items.pvta_a1);
                            cmd.Parameters.AddWithValue($"@PrecioCambio{contador - currentBatchSize + i}", items.cambiopv_1);
                            cmd.Parameters.AddWithValue($"@Rentabilidad{contador - currentBatchSize + i}", CalcularRentabilidad(items.pvta1i, items.costo_rep, 3, items.iva, items.iconsumo, items.listap, items.imp_salu, items.vr_imps, DateTime.Now, items.gen_impu));
                            cmd.Parameters.AddWithValue($"@BaseImpuestoConsumo{contador - currentBatchSize + i}", items.iconsumo > 0 ? true : false);
                            cmd.Parameters.AddWithValue($"@IvaExento{contador - currentBatchSize + i}", items.excluido == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ImpuestoConsumo{contador - currentBatchSize + i}", items.listap);
                            cmd.Parameters.AddWithValue($"@IdGrupoContable{contador - currentBatchSize + i}", 0); // Asignar 0 o el valor correspondiente
                            cmd.Parameters.AddWithValue($"@FactorImpuestoConsumo{contador - currentBatchSize + i}", items.F_ICONSUMO);
                            cmd.Parameters.AddWithValue($"@IdRetFuente{contador - currentBatchSize + i}", 0); // Asignar 0 o el valor correspondiente
                            cmd.Parameters.AddWithValue($"@CostoAjuste{contador - currentBatchSize + i}", items.costoajus);
                            cmd.Parameters.AddWithValue($"@Fruver{contador - currentBatchSize + i}", items.es_fruver == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@BolsaAgro{contador - currentBatchSize + i}", items.bolsa == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ModificaPrecio{contador - currentBatchSize + i}", items.mod_ppos == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@Fenaice{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.fenalce) ? true : false);
                            cmd.Parameters.AddWithValue($"@AcumulaTira{contador - currentBatchSize + i}", items.acu_tpos == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@SubsidioDesempleo{contador - currentBatchSize + i}", items.subsidio == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ModificaCantidad{contador - currentBatchSize + i}", items.mod_qpos == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@BotFut{contador - currentBatchSize + i}", items.es_bol == "1" ? true : false);
                            cmd.Parameters.AddWithValue($"@ContabilizarGrabado{contador - currentBatchSize + i}", items.contabgrav == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@NoAutorizaCompra{contador - currentBatchSize + i}", items.no_compra == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@BalanzaToledo{contador - currentBatchSize + i}", items.sitoledo == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@PrefijoEAN{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.pref_ean) ? true : false);
                            cmd.Parameters.AddWithValue($"@DescuentoAsohofrucol{contador - currentBatchSize + i}", items.es_bordado == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@Motocicleta{contador - currentBatchSize + i}", items.es_moto == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@Pedido{contador - currentBatchSize + i}", items.sipedido == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@Codensa{contador - currentBatchSize + i}", items.escodensa == "S" ? true : false);
                            cmd.Parameters.AddWithValue($"@Ingreso{contador - currentBatchSize + i}", items.es_ingreso == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@CheckeoPrecio{contador - currentBatchSize + i}", items.cheqpr == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@IdEvento{contador - currentBatchSize + i}", 0); // Asignar 0 o el valor correspondiente
                            cmd.Parameters.AddWithValue($"@Tipo{contador - currentBatchSize + i}", short.Parse(items.tipo));
                            cmd.Parameters.AddWithValue($"@AdmiteDescuento{contador - currentBatchSize + i}", items.si_descto == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@DescuentoMaximo{contador - currentBatchSize + i}", items.descmax);
                            cmd.Parameters.AddWithValue($"@Decimales{contador - currentBatchSize + i}", items.deci_cant);
                            cmd.Parameters.AddWithValue($"@FechaCambio{contador - currentBatchSize + i}", items.fech_cp1);
                            cmd.Parameters.AddWithValue($"@CostoReposicion{contador - currentBatchSize + i}", items.costo_rep);
                            cmd.Parameters.AddWithValue($"@FenaicePorcentaje{contador - currentBatchSize + i}", 0); // Asignar 0 o el valor correspondiente
                            cmd.Parameters.AddWithValue($"@BotFutPorcentaje{contador - currentBatchSize + i}", 0); // Asignar 0 o el valor correspondiente
                            cmd.Parameters.AddWithValue($"@PrefijoEANPorcentaje{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.pref_ean) ? 0 : int.Parse(items.pref_ean));
                            cmd.Parameters.AddWithValue($"@CodensaPorcentaje{contador - currentBatchSize + i}", 0); // Asignar 0 o el valor correspondiente
                            cmd.Parameters.AddWithValue($"@CodigoAlterno{contador - currentBatchSize + i}", items.cod_alt);
                            cmd.Parameters.AddWithValue($"@CCosto{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.CCosto) ? DBNull.Value : int.Parse(items.CCosto));
                            cmd.Parameters.AddWithValue($"@ExcluirDeListado{contador - currentBatchSize + i}", items.elegido == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@PermitirSaldoEnRojo{contador - currentBatchSize + i}", items.sdo_rojo == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ExigirTarjetMercapesos{contador - currentBatchSize + i}", items.pidempeso == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@Corrosivo{contador - currentBatchSize + i}", items.corrosivo == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@CAS{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.n_cas) ? DBNull.Value : items.n_cas);
                            cmd.Parameters.AddWithValue($"@CategoriaCAS{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.cat_cas) ? DBNull.Value : items.cat_cas);
                            cmd.Parameters.AddWithValue($"@PermitirVentaPorDebajoCosto{contador - currentBatchSize + i}", items.vta_costo == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@DetalleAlElaborarDocumento{contador - currentBatchSize + i}", items.si_detdoc == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@UnicamenteManejarCantidad{contador - currentBatchSize + i}", items.solocant == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@SolicitarSerieAlFacturar{contador - currentBatchSize + i}", items.si_serie == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@TerceroAutomaticoPOS{contador - currentBatchSize + i}", items.terceAutom == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ProductoGenerico{contador - currentBatchSize + i}", items.generico == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@PesoBruto{contador - currentBatchSize + i}", items.pesocajb);
                            cmd.Parameters.AddWithValue($"@PesoNeto{contador - currentBatchSize + i}", items.pesocajn);
                            cmd.Parameters.AddWithValue($"@PesoTara{contador - currentBatchSize + i}", items.peso_car);
                            cmd.Parameters.AddWithValue($"@UnidadMinimaDeVenta{contador - currentBatchSize + i}", items.unidmin);
                            cmd.Parameters.AddWithValue($"@Factor{contador - currentBatchSize + i}", items.puntaje);
                            cmd.Parameters.AddWithValue($"@InicioMercapesos{contador - currentBatchSize + i}", items.fecha1_mp == null ? DBNull.Value : items.fecha1_mp);
                            cmd.Parameters.AddWithValue($"@FinalMercapesos{contador - currentBatchSize + i}", items.fecha2_mp == null ? DBNull.Value : items.fecha2_mp);
                            cmd.Parameters.AddWithValue($"@DescuentoEspecial{contador - currentBatchSize + i}", items.desc_esp == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@FactorDescuento{contador - currentBatchSize + i}", items.fact_esp);
                            cmd.Parameters.AddWithValue($"@ValorDescuentoFijo{contador - currentBatchSize + i}", items.valor_esp);
                            cmd.Parameters.AddWithValue($"@InicioDescuento{contador - currentBatchSize + i}", items.fechdesci == null ? DBNull.Value : items.fechdesci);
                            cmd.Parameters.AddWithValue($"@FinalDescuento{contador - currentBatchSize + i}", items.fechdescf == null ? DBNull.Value : items.fechdescf);
                            cmd.Parameters.AddWithValue($"@DecodificarProducto{contador - currentBatchSize + i}", items.descod == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@DiaDecodificacion{contador - currentBatchSize + i}", items.descod_f == null ? DBNull.Value : items.descod_f);
                            cmd.Parameters.AddWithValue($"@DiaUsuarioDecodificacion{contador - currentBatchSize + i}", items.fchdescusr == null ? DBNull.Value : items.fchdescusr);
                            cmd.Parameters.AddWithValue($"@ValidarMaxVentasPorTercero{contador - currentBatchSize + i}", items.validmax == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@UnidadMaximaVentas{contador - currentBatchSize + i}", items.maxventa);
                            cmd.Parameters.AddWithValue($"@NoIncluirDespacho{contador - currentBatchSize + i}", items.val_desp == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@FechaBloqueoPedido{contador - currentBatchSize + i}", items.f_bloqp == null ? DBNull.Value : items.f_bloqp);
                            cmd.Parameters.AddWithValue($"@TipoControl{contador - currentBatchSize + i}", items.cont_devol);
                            cmd.Parameters.AddWithValue($"@DiasSiguientesCompra{contador - currentBatchSize + i}", 0); // Asignar 0 o el valor correspondiente
                            cmd.Parameters.AddWithValue($"@PrecioVenta3{contador - currentBatchSize + i}", items.pvta3i);
                            cmd.Parameters.AddWithValue($"@NoIncluirEnInventarioParaPedido{contador - currentBatchSize + i}", items.no_invped == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@AutorizarTrasladoProducto{contador - currentBatchSize + i}", items.aut_trasl == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@PrecioControlVingilancia{contador - currentBatchSize + i}", items.lp_cyvig == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@NoIncluirreporteChequeo{contador - currentBatchSize + i}", items.chequeo == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@NoCalcularCostoPromedio{contador - currentBatchSize + i}", items.costeo2 == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ProductoSobreStock{contador - currentBatchSize + i}", items.sobrestock == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EnviarAAsopanela{contador - currentBatchSize + i}", items.Asopanela == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@UnidadArticulo{contador - currentBatchSize + i}", 0); // Asignar 0 o el valor correspondiente
                            cmd.Parameters.AddWithValue($"@GrupoDescuento{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.grupodes) ? DBNull.Value : items.grupodes);
                            cmd.Parameters.AddWithValue($"@CodigoEan1{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.ean_1) ? DBNull.Value : items.ean_1);
                            cmd.Parameters.AddWithValue($"@CodigoEan2{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.ean_2) ? DBNull.Value : items.ean_2);
                            cmd.Parameters.AddWithValue($"@ProductoInAndOut{contador - currentBatchSize + i}", items.inandout == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@DomiciliosCom{contador - currentBatchSize + i}", items.domi_com == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@PesoPOS{contador - currentBatchSize + i}", items.si_descto == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@OrdenadoInicioFactura{contador - currentBatchSize + i}", items.ord_prio == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ModificarConTolerancia{contador - currentBatchSize + i}", items.modq_reg == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@PorcentajeTolerancia{contador - currentBatchSize + i}", items.mod_toler);
                            cmd.Parameters.AddWithValue($"@Stock{contador - currentBatchSize + i}", items.stockdomi);
                            cmd.Parameters.AddWithValue($"@ExcluidoCOVID{contador - currentBatchSize + i}", items.ext_covid == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@unidcantfe{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.unidcantfe) ? DBNull.Value : items.unidcantfe);
                            cmd.Parameters.AddWithValue($"@tipobiend{contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(items.tipobiend) ? DBNull.Value : items.tipobiend);
                            cmd.Parameters.AddWithValue($"@DiasSinIVA{contador - currentBatchSize + i}", items.pdiasiva == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@DescuentoFNFP{contador - currentBatchSize + i}", items.es_dfnfp == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@IdVariedadFNFP{contador - currentBatchSize + i}", items.varifnfp);
                            cmd.Parameters.AddWithValue($"@DescuentoNIIF{contador - currentBatchSize + i}", items.DESFINNIIF);
                            cmd.Parameters.AddWithValue($"@NoUtilizar{contador - currentBatchSize + i}", false);
                            cmd.Parameters.AddWithValue($"@FactorImpuestoConsumoRest{contador - currentBatchSize + i}", items.F_ICONSUMO == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@bod_asoc{contador - currentBatchSize + i}", items.bod_asoc);
                            cmd.Parameters.AddWithValue($"@PrecioIva{contador - currentBatchSize + i}", items.pvtali);
                            cmd.Parameters.AddWithValue($"@ValorIva{contador - currentBatchSize + i}", FCalcImp(items.pvta1i, items.iva, items.iconsumo, 1, 2, items.imp_salu, items.vr_imps, DateTime.Now, items.gen_impu, "I"));
                            cmd.Parameters.AddWithValue($"@CostoAjusteNIIF{contador - currentBatchSize + i}", items.costoajus);
                            cmd.Parameters.AddWithValue($"@DescuentoPorcentaje{contador - currentBatchSize + i}", items.descuento);
                            cmd.Parameters.AddWithValue($"@RentabilidadSugerida{contador - currentBatchSize + i}", items.por_rentab);
                            cmd.Parameters.AddWithValue($"@ConfirmarCambioPrecio{contador - currentBatchSize + i}", items.confirpre == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ProductoOfertado{contador - currentBatchSize + i}", items.ofertado == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@FechaPrimerMovimiento{contador - currentBatchSize + i}", items.fech1comp == null ? DBNull.Value : items.fech1comp);
                            cmd.Parameters.AddWithValue($"@TipoImpuestoAlimentos{contador - currentBatchSize + i}", items.imp_salu);
                            cmd.Parameters.AddWithValue($"@ValorTipoImpuestoAlimentos{contador - currentBatchSize + i}", items.vr_imps);
                            cmd.Parameters.AddWithValue($"@GeneraImpuestoSaludable{contador - currentBatchSize + i}", items.gen_impu == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@CodigoReferencia{contador - currentBatchSize + i}", items.cod_ref);
                            cmd.Parameters.AddWithValue($"@Referencia{contador - currentBatchSize + i}", items.refer);
                        }

                        cmd.ExecuteNonQuery();
                        tx.Commit();
                    }
                }
            }
            cronometer.Stop();
        }



        public static void insertarTerceros(List<t_terceros> terceros_list,List<t_ciudad> ciudad_list ,int batchSize = 100)
        {
            int contador = 0;
            var cronometer = Stopwatch.StartNew();
            Stopwatch sw = new Stopwatch();
            sw.Start();

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                while (contador < terceros_list.Count)
                {
                    using (var tx = conn.BeginTransaction())
                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.Transaction = tx;

                        var commandText = new StringBuilder();

                        commandText.AppendLine(@"
                        INSERT INTO ""Terceros"" (
                        ""TipoPersona"", ""IdTipoIdentificacion"", ""Identificacion"", ""Nombre1"", ""Nombre2"", 
                        ""Apellido1"", ""Apellido2"", ""Genero"", ""FechaNacimiento"", ""RazonSocial"", 
                        ""NombreComercial"", ""Direccion"", ""Email"", ""Email2"", ""IdDepartamento"", 
                        ""IdMunicipio"", ""Telefono1"", ""Telefono2"", ""Estado"", ""EsCliente"", 
                        ""EsEmpleado"", ""EsPasante"", ""EsProveedor"", ""FechaCreacion"", ""FechaActualizacion"", 
                        ""DiasCredito"", ""EsProveedorFruver"", ""EsProveedorBolsaAgropecuaria"", ""EsProveedorCampesinoDirecto"", 
                        ""EsProveedorRestaurante"", ""EsProveedorPanaderia"", ""EsOtroTipo"", ""EsGasto"", 
                        ""CotizaEPS"", ""CotizaFondoPension"", ""CotizaARP"", ""TarifaARP"", ""RegimenSimplificado"", 
                        ""NoPracticarRetFuente"", ""NoPracticarRetIVA"", ""Autorretenedor"", ""EsRetenedorFuente"", 
                        ""DescontarAsohofrucol"", ""AsumirImpuestos"", ""RetenerFenalce"", ""AsumirFenalce"", 
                        ""BolsaAgropecuaria"", ""RegimenComun"", ""RetenerSiempre"", ""GranContribuyente"", 
                        ""AutorretenedorIVA"", ""IdCxPagar"", ""DeclaranteRenta"", ""DescuentoNIIF"", 
                        ""DescontarFNFP"", ""ManejaIVAProductoBonificado"", ""ReteIVALey560_2020"", ""RegimenSimpleTributacion"", 
                        ""RetencionZOMAC"", ""TipoDescuentoFinanciero"", ""Porcentaje1"", ""Porcentaje2"", 
                        ""Porcentaje3"", ""Porcentaje4"", ""Porcentaje5"", ""IdRUT"", ""IdICATerceroCiudad"", 
                        ""ClienteExcentoIVA"", ""EstadoRUT"", ""FechaRut"", ""IdCxCobrar"", ""DigitoVerificacion"", 
                        ""IdEmpleado"", ""BaseDecreciente"", ""IdRegimenContribuyente"", ""IdResponsabilidadesFiscales"", 
                        ""IdResponsabilidadesTributarias"", ""IdUbicacionDANE"", ""CodigoPostal"", ""BloqueoPago"", 
                        ""ObservacionBloqueo"", ""FrecuenciaServicio"", ""PromesaServicio"", ""DiasInvSeguridad""
                        )VALUES");

                        var valuesList = new List<string>();
                        int currentBatchSize = 0;

                        while (contador < terceros_list.Count && currentBatchSize < batchSize)
                        {
                            var items = terceros_list[contador];

                            
                            var values = $@"
                    (
                         @TipoPersona{contador}, @IdTipoIdentificacion{contador}, @Identificacion{contador}, @Nombre1{contador}, @Nombre2{contador}, @Apellido1{contador}, 
                         @Apellido2{contador}, @Genero{contador}, @FechaNacimiento{contador}, @RazonSocial{contador}, @NombreComercial{contador}, @Direccion{contador}, 
                         @Email{contador}, @Email2{contador}, 
                         (SELECT DISTINCT id FROM departamentos WHERE nombre = @IdDepartamento{contador} LIMIT 1), 
                         (SELECT DISTINCT id FROM municipios WHERE nombre = @IdMunicipio{contador} LIMIT 1), 
                         @Telefono1{contador}, @Telefono2{contador}, @Estado{contador}, 
                         @EsCliente{contador}, @EsEmpleado{contador}, @EsPasante{contador}, @EsProveedor{contador}, 
                         @FechaCreacion{contador}, @FechaActualizacion{contador}, @DiasCredito{contador}, 
                         @EsProveedorFruver{contador}, @EsProveedorBolsaAgropecuaria{contador}, @EsProveedorCampesinoDirecto{contador}, 
                         @EsProveedorRestaurante{contador}, @EsProveedorPanaderia{contador}, @EsOtroTipo{contador}, @EsGasto{contador}, 
                         @CotizaEPS{contador}, @CotizaFondoPension{contador}, @CotizaARP{contador}, @TarifaARP{contador}, 
                         @RegimenSimplificado{contador}, @NoPracticarRetFuente{contador}, @NoPracticarRetIVA{contador}, 
                         @Autorretenedor{contador}, @EsRetenedorFuente{contador}, @DescontarAsohofrucol{contador}, 
                         @AsumirImpuestos{contador}, @RetenerFenalce{contador}, @AsumirFenalce{contador}, 
                         @BolsaAgropecuaria{contador}, @RegimenComun{contador}, @RetenerSiempre{contador}, 
                         @GranContribuyente{contador}, @AutorretenedorIVA{contador}, @IdCxPagar{contador}, 
                         @DeclaranteRenta{contador}, @DescuentoNIIF{contador}, @DescontarFNFP{contador}, 
                         @ManejaIVAProductoBonificado{contador}, @ReteIVALey560_2020{contador}, 
                         @RegimenSimpleTributacion{contador}, @RetencionZOMAC{contador}, @TipoDescuentoFinanciero{contador}, 
                         @Porcentaje1{contador}, @Porcentaje2{contador}, @Porcentaje3{contador}, @Porcentaje4{contador}, @Porcentaje5{contador}, 
                         @IdRUT{contador}, @IdICATerceroCiudad{contador}, @ClienteExcentoIVA{contador}, 
                         @EstadoRUT{contador}, @FechaRut{contador}, @IdCxCobrar{contador}, @DigitoVerificacion{contador}, 
                         @IdEmpleado{contador}, @BaseDecreciente{contador}, @IdRegimenContribuyente{contador}, 
                         @IdResponsabilidadesFiscales{contador}, @IdResponsabilidadesTributarias{contador}, 
                         @IdUbicacionDANE{contador}, @CodigoPostal{contador}, @BloqueoPago{contador}, 
                         @ObservacionBloqueo{contador}, @FrecuenciaServicio{contador}, @PromesaServicio{contador}, 
                         @DiasInvSeguridad{contador}
                         )";

                            valuesList.Add(values);
                            contador++;
                            currentBatchSize++;
                        }

                        // Combina todos los VALUES en una sola consulta
                        commandText.Append(string.Join(",", valuesList));

                        // Asigna el comando completo
                        cmd.CommandText = commandText.ToString();

                        // Agregar parámetros para el lote actual
                        for (int i = 0; i < currentBatchSize; i++)
                        {

                            string xDepartamento = "";
                            string xMunicipio    = "";
                            int    xPersona = 0;

                            var anexos = terceros_list[contador - currentBatchSize + i];

                            var ubicacion = ciudad_list.FirstOrDefault(c => c.Dane == anexos.IdUbicacionDANE);


                            if (ubicacion != null)
                            {
                                xDepartamento = ubicacion.Departamento;
                                xMunicipio    = ubicacion.Municipio;
                            }

                            if (anexos.TipoPersona == "Natural")
                            {
                                xPersona = 1;
                            }else
                            {
                                xPersona = 2;
                            }

                            cmd.Parameters.AddWithValue($"@TipoPersona{contador - currentBatchSize + i}", xPersona);
                            cmd.Parameters.AddWithValue($"@IdTipoIdentificacion{contador - currentBatchSize + i}", anexos.IdTipoIdentificacion);
                            cmd.Parameters.AddWithValue($"@Identificacion{contador - currentBatchSize + i}", anexos.Identificacion);
                            cmd.Parameters.AddWithValue($"@Nombre1{contador - currentBatchSize + i}", !string.IsNullOrEmpty(anexos.Nombre1) ? anexos.Nombre1 : (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@Nombre2{contador - currentBatchSize + i}", !string.IsNullOrEmpty(anexos.Nombre2) ? anexos.Nombre2 : (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@Apellido1{contador - currentBatchSize + i}", !string.IsNullOrEmpty(anexos.Apellido1) ? anexos.Apellido1 : (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@Apellido2{contador - currentBatchSize + i}", !string.IsNullOrEmpty(anexos.Apellido2) ? anexos.Apellido2 : (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@Genero{contador - currentBatchSize + i}", anexos.Genero == "M" ? 1 : 2);
                            cmd.Parameters.AddWithValue($"@FechaNacimiento{contador - currentBatchSize + i}",  anexos.FechaNacimiento);
                            cmd.Parameters.AddWithValue($"@RazonSocial{contador - currentBatchSize + i}", anexos.RazonSocial);
                            cmd.Parameters.AddWithValue($"@NombreComercial{contador - currentBatchSize + i}", !string.IsNullOrEmpty(anexos.NombreComercial) ? anexos.NombreComercial : (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@Direccion{contador - currentBatchSize + i}", !string.IsNullOrEmpty(anexos.Direccion) ? anexos.Direccion : (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@Email{contador - currentBatchSize + i}", !string.IsNullOrEmpty(anexos.Email) ? anexos.Email : (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@Email2{contador - currentBatchSize + i}", !string.IsNullOrEmpty(anexos.Email2) ? anexos.Email2 : (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@IdDepartamento{contador - currentBatchSize + i}", string.IsNullOrEmpty(xDepartamento) ? (object)DBNull.Value : $"(SELECT DISTINCT id FROM departamentos WHERE departamentos.nombre = '{xDepartamento}' LIMIT 1)");
                            cmd.Parameters.AddWithValue($"@IdMunicipio{contador - currentBatchSize + i}", string.IsNullOrEmpty(xMunicipio) ? (object)DBNull.Value : $"(SELECT DISTINCT id FROM municipios WHERE municipios.nombre = '{xMunicipio}' LIMIT 1)");
                            cmd.Parameters.AddWithValue($"@Telefono1{contador - currentBatchSize + i}", anexos.Telefono1);
                            cmd.Parameters.AddWithValue($"@Telefono2{contador - currentBatchSize + i}", anexos.Telefono2);
                            cmd.Parameters.AddWithValue($"@Estado{contador - currentBatchSize + i}", anexos.Estado == 1 ? false : true);
                            cmd.Parameters.AddWithValue($"@EsCliente{contador - currentBatchSize + i}", anexos.EsCliente == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsEmpleado{contador - currentBatchSize + i}", anexos.EsEmpleado == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsPasante{contador - currentBatchSize + i}", anexos.EsPasante == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsProveedor{contador - currentBatchSize + i}", anexos.EsProveedor == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@FechaCreacion{contador - currentBatchSize + i}", anexos.FechaCreacion.HasValue ? anexos.FechaCreacion: DateTime.Now);
                            cmd.Parameters.AddWithValue($"@FechaActualizacion{contador - currentBatchSize + i}", anexos.FechaActualizacion.HasValue ? anexos.FechaActualizacion : DateTime.Now);
                            cmd.Parameters.AddWithValue($"@DiasCredito{contador - currentBatchSize + i}", anexos.DiasCredito);
                            cmd.Parameters.AddWithValue($"@EsProveedorFruver{contador - currentBatchSize + i}", anexos.EsProveedorFruver == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsProveedorBolsaAgropecuaria{contador - currentBatchSize + i}", anexos.EsProveedorBolsaAgropecuaria == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsProveedorCampesinoDirecto{contador - currentBatchSize + i}", anexos.EsProveedorCampesinoDirecto == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsProveedorRestaurante{contador - currentBatchSize + i}", anexos.EsProveedorRestaurante == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsProveedorPanaderia{contador - currentBatchSize + i}", anexos.EsProveedorPanaderia == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsOtroTipo{contador - currentBatchSize + i}", anexos.EsOtroTipo == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsGasto{contador - currentBatchSize + i}", anexos.EsGasto == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@CotizaEPS{contador - currentBatchSize + i}", anexos.CotizaEPS == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@CotizaFondoPension{contador - currentBatchSize + i}", anexos.CotizaFondoPension == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@CotizaARP{contador - currentBatchSize + i}", anexos.CotizaARP == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@TarifaARP{contador - currentBatchSize + i}", anexos.TarifaARP);
                            cmd.Parameters.AddWithValue($"@RegimenSimplificado{contador - currentBatchSize + i}", anexos.RegimenSimplificado == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@NoPracticarRetFuente{contador - currentBatchSize + i}", anexos.NoPracticarRetFuente == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@NoPracticarRetIVA{contador - currentBatchSize + i}", anexos.NoPracticarRetIVA == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@Autorretenedor{contador - currentBatchSize + i}", anexos.Autorretenedor == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@EsRetenedorFuente{contador - currentBatchSize + i}", anexos.EsRetenedorFuente == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@DescontarAsohofrucol{contador - currentBatchSize + i}", anexos.DescontarAsohofrucol == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@AsumirImpuestos{contador - currentBatchSize + i}", anexos.AsumirImpuestos == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@RetenerFenalce{contador - currentBatchSize + i}", anexos.RetenerFenalce == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@AsumirFenalce{contador - currentBatchSize + i}", anexos.AsumirFenalce == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@BolsaAgropecuaria{contador - currentBatchSize + i}", anexos.BolsaAgropecuaria == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@RegimenComun{contador - currentBatchSize + i}", anexos.RegimenComun == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@RetenerSiempre{contador - currentBatchSize + i}", anexos.RetenerSiempre == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@GranContribuyente{contador - currentBatchSize + i}", anexos.GranContribuyente == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@AutorretenedorIVA{contador - currentBatchSize + i}", anexos.AutorretenedorIVA == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@IdCxPagar{contador - currentBatchSize + i}", anexos.IdCxPagar == 0 ? 0 : anexos.IdCxPagar);
                            cmd.Parameters.AddWithValue($"@DeclaranteRenta{contador - currentBatchSize + i}", anexos.DeclaranteRenta == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@DescuentoNIIF{contador - currentBatchSize + i}", anexos.DescuentoNIIF == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@DescontarFNFP{contador - currentBatchSize + i}", anexos.DescontarFNFP == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ManejaIVAProductoBonificado{contador - currentBatchSize + i}", anexos.ManejaIVAProductoBonificado == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@ReteIVALey560_2020{contador - currentBatchSize + i}", anexos.ReteIVALey560_2020 == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@RegimenSimpleTributacion{contador - currentBatchSize + i}", anexos.RegimenSimpleTributacion == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@RetencionZOMAC{contador - currentBatchSize + i}", false);
                            cmd.Parameters.AddWithValue($"@TipoDescuentoFinanciero{contador - currentBatchSize + i}", anexos.TipoDescuentoFinanciero);
                            cmd.Parameters.AddWithValue($"@Porcentaje1{contador - currentBatchSize + i}", anexos.Porcentaje1);
                            cmd.Parameters.AddWithValue($"@Porcentaje2{contador - currentBatchSize + i}", anexos.Porcentaje2);
                            cmd.Parameters.AddWithValue($"@Porcentaje3{contador - currentBatchSize + i}", anexos.Porcentaje3);
                            cmd.Parameters.AddWithValue($"@Porcentaje4{contador - currentBatchSize + i}", anexos.Porcentaje4);
                            cmd.Parameters.AddWithValue($"@Porcentaje5{contador - currentBatchSize + i}", anexos.Porcentaje5);
                            cmd.Parameters.AddWithValue($"@IdRUT{contador - currentBatchSize + i}", anexos.IdRUT);
                            cmd.Parameters.AddWithValue($"@IdICATerceroCiudad{contador - currentBatchSize + i}", anexos.IdICATerceroCiudad);
                            cmd.Parameters.AddWithValue($"@ClienteExcentoIVA{contador - currentBatchSize + i}", false);
                            cmd.Parameters.AddWithValue($"@EstadoRUT{contador - currentBatchSize + i}", (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@FechaRut{contador - currentBatchSize + i}", anexos.FechaRut.HasValue ? anexos.FechaRut : DateTime.Now);
                            cmd.Parameters.AddWithValue($"@IdCxCobrar{contador - currentBatchSize + i}", anexos.IdCxCobrar == 0 ? 0 : anexos.IdCxCobrar);
                            cmd.Parameters.AddWithValue($"@DigitoVerificacion{contador - currentBatchSize + i}", string.IsNullOrEmpty(anexos.DigitoVerificacion) ? (object)DBNull.Value : anexos.DigitoVerificacion);
                            cmd.Parameters.AddWithValue($"@IdEmpleado{contador - currentBatchSize + i}", anexos.IdEmpleado == 0 ? 0 : $"(SELECT DISTINCT \"Id\" FROM \"Empleados\" WHERE \"NumerodeDocumento\" = '{anexos.IdTipoIdentificacion}' LIMIT 1)");
                            cmd.Parameters.AddWithValue($"@BaseDecreciente{contador - currentBatchSize + i}", anexos.BaseDecreciente == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@RentabilidadSugerida{contador - currentBatchSize + i}", "0");
                            cmd.Parameters.AddWithValue($"@IdRegimenContribuyente{contador - currentBatchSize + i}", 0);

                            // Manejo de subconsultas
                            int? idResponsabilidadesFiscales = null;
                            if (!string.IsNullOrEmpty(anexos.IdResponsabilidadesFiscales))
                            {
                                using (var cmdSubquery = new NpgsqlCommand("SELECT DISTINCT id FROM responsabilidadesfiscales WHERE codigo = @codigo LIMIT 1", conn))
                                {
                                    cmdSubquery.Parameters.AddWithValue("@codigo", anexos.IdResponsabilidadesFiscales);
                                    var result = cmdSubquery.ExecuteScalar();
                                    if (result != null)
                                    {
                                        idResponsabilidadesFiscales = Convert.ToInt32(result);
                                    }
                                }
                            }

                            cmd.Parameters.AddWithValue($"@IdResponsabilidadesFiscales{contador - currentBatchSize + i}", idResponsabilidadesFiscales.HasValue ? (object)idResponsabilidadesFiscales.Value : DBNull.Value);

                            int? idResponsabilidadesTributarias = null;
                            if (!string.IsNullOrEmpty(anexos.IdResponsabilidadesTributarias))
                            {
                                using (var cmdSubquery = new NpgsqlCommand("SELECT DISTINCT id FROM responsabilidadestributarias WHERE codigo = @codigo LIMIT 1", conn))
                                {
                                    cmdSubquery.Parameters.AddWithValue("@codigo", anexos.IdResponsabilidadesTributarias);
                                    var result = cmdSubquery.ExecuteScalar();
                                    if (result != null)
                                    {
                                        idResponsabilidadesTributarias = Convert.ToInt32(result);
                                    }
                                }
                            }

                            cmd.Parameters.AddWithValue($"@IdResponsabilidadesTributarias{contador - currentBatchSize + i}", idResponsabilidadesTributarias.HasValue ? (object)idResponsabilidadesTributarias.Value : DBNull.Value);

                            cmd.Parameters.AddWithValue($"@idubicaciondane{contador - currentBatchSize + i}", string.IsNullOrEmpty(anexos.IdUbicacionDANE) ? 0 : Convert.ToInt32(anexos.IdUbicacionDANE));
                            cmd.Parameters.AddWithValue($"@CodigoPostal{contador - currentBatchSize + i}", anexos.CodigoPostal);
                            cmd.Parameters.AddWithValue($"@BloqueoPago{contador - currentBatchSize + i}", false);
                            cmd.Parameters.AddWithValue($"@ObservacionBloqueo{contador - currentBatchSize + i}", (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@FrecuenciaServicio{contador - currentBatchSize + i}", (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@PromesaServicio{contador - currentBatchSize + i}", (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@DiasInvSeguridad{contador - currentBatchSize + i}", (object)DBNull.Value);


                        }

                        cmd.ExecuteNonQuery();
                        tx.Commit();
                    }
                }
            }
            cronometer.Stop();
            TimeSpan tiempoTranscurrido = cronometer.Elapsed;
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours} hora");
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes} minutos");
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds} segundos");
        }




        public static void InsertarFenalpag(List<t_fenalpag> fenal_list)
        {
            int contador = 0; // Contador

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                // Iniciar transacción
                using (var tx = conn.BeginTransaction())
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.Transaction = tx;

                    foreach (var fenalpag in fenal_list)
                    {
                        try
                        {
                            cmd.Parameters.Clear(); // ← CRUCIA

                            // Comando SQL con parámetros
                            cmd.CommandText = @"
                       INSERT INTO fenalpag (
                           tipodedocumentoid, tipopersonaid, nombre1, nombre2, apellido1, apellido2, generoid,
                           fechanacimiento, direccion, ciudad, telefonos, fax, email, terceroid,
                           fechahoracreacion, usuariocreaid, numerodocumento, estado, observacion, nombrecompleto,
                           empleadoid, fenalpagconfiguracionid, codigoaprobacion
                       )
                       VALUES (
                           (SELECT DISTINCT id FROM tiposdedocumento WHERE Codigo = @tdoc LIMIT 1),
                           (SELECT DISTINCT id FROM tiposdepersona WHERE descripcion = @tipo_per LIMIT 1),
                           @nom1, @nom2, @apl1, @apl2,
                           @generoid, @fech_nac, @direccion, @ciudad, @telefono, @fax, @email,
                           (SELECT DISTINCT ""Id"" FROM ""Terceros"" WHERE ""Identificacion"" = @anexo LIMIT 1),
                           @fechahora, 
                           (SELECT DISTINCT ""Id"" FROM ""AspNetUsers"" WHERE ""UserName"" = @usuario),
                           @numerodocumento, @estado, @observacion, @nombrecompleto,
                           (SELECT DISTINCT ""Id"" FROM ""Empleados"" WHERE ""CodigoEmpleado"" = @cod_empleado LIMIT 1),
                           (SELECT DISTINCT id FROM fenalpagconfiguracion WHERE id = @cod_sis),
                           @codigoaprobacion
                       );";

                            cmd.Parameters.AddWithValue("@tdoc", fenalpag.Tdoc);
                            cmd.Parameters.AddWithValue("@tipo_per", fenalpag.Tipo_per.Trim().ToUpper());
                            cmd.Parameters.AddWithValue("@nom1", fenalpag.Nom1.Trim());
                            cmd.Parameters.AddWithValue("@nom2", fenalpag.Nom2.Trim());
                            cmd.Parameters.AddWithValue("@apl1", fenalpag.Apl1.Trim());
                            cmd.Parameters.AddWithValue("@apl2", fenalpag.Apl2.Trim());
                            cmd.Parameters.AddWithValue("@generoid", fenalpag.Sexo == "M" ? 1 : 2);
                            cmd.Parameters.AddWithValue("@fech_nac", fenalpag.fech_nac); // DateTime
                            cmd.Parameters.AddWithValue("@direccion", fenalpag.Direcc.Trim());
                            cmd.Parameters.AddWithValue("@ciudad", fenalpag.Ciudad.ToString());
                            cmd.Parameters.AddWithValue("@telefono", fenalpag.Tel.Trim());
                            cmd.Parameters.AddWithValue("@fax", fenalpag.Tel.Trim());
                            cmd.Parameters.AddWithValue("@email", fenalpag.Emailfe1.Trim());
                            cmd.Parameters.AddWithValue("@anexo", Convert.ToInt64(fenalpag.Anexo));
                            cmd.Parameters.AddWithValue("@fechahora", DateTime.Now);
                            cmd.Parameters.AddWithValue("@usuario", fenalpag.Usuario.Trim());
                            cmd.Parameters.AddWithValue("@numerodocumento", fenalpag.Anexo);
                            cmd.Parameters.AddWithValue("@estado", fenalpag.Usado == 1 ? false : true);
                            cmd.Parameters.AddWithValue("@observacion", ""); // vacío
                            cmd.Parameters.AddWithValue("@nombrecompleto", fenalpag.Nombre.Trim());
                            cmd.Parameters.AddWithValue("@cod_empleado", fenalpag.Anexo.ToString());
                            cmd.Parameters.AddWithValue("@cod_sis", Convert.ToInt32(fenalpag.Cod_sis));
                            cmd.Parameters.AddWithValue("@codigoaprobacion", fenalpag.Autoriz.Trim());

                            // Ejecutar el comando de inserción
                            cmd.ExecuteNonQuery();



                            contador++; // Incrementar contador
                            Console.WriteLine($"Cliente insertado #{contador}"); // Mostrar en consola
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error al insertar cliente con documento: {ex.Message}");
                            //ex.InnerException
                        }
                    }

                    // Confirmar la transacción
                    tx.Commit();
                }
            }

            Console.WriteLine("Datos insertados correctamente.");

        }



        public static void InsertarMovnom(List<t_movnom> movnomList)
        {
            int contador = 0; // Contador

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                // Iniciar transacción
                using (var tx = conn.BeginTransaction())
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.Transaction = tx;

                    foreach (var movnom in movnomList)
                    {
                        try
                        {
                            
                            cmd.Parameters.Clear(); // ← CRUCIA
                            

                            cmd.CommandText = @"
                            INSERT INTO nominaprocesoliquidacion(
                                empleadoid, numerodedocumento, primerapellido, segundoapelliedo,
                                primernombre, segundonombre, conceptosnominaid, periodoliquidacionnominaid,
                                periodoliquidacionnominadescripcion, estado,conceptosnominadescripcion,fechahoraliquidacion,
                                devengado, descuento
                            )
                            VALUES (
                                (SELECT DISTINCT ""Id"" FROM ""Empleados"" WHERE ""CodigoEmpleado"" = @cod_empleado LIMIT 1),
                                @documento, @apellido1, @apellido2, @nombre1, @nombre2,
                                (SELECT DISTINCT id FROM conceptosnomina WHERE codigo = @concepto LIMIT 1),
                                (SELECT DISTINCT ""Id"" FROM ""PeriodoLiquidacionNomina"" WHERE ""Periodo"" = @periodo),
                                (SELECT DISTINCT ""Descripcion"" FROM ""PeriodoLiquidacionNomina"" WHERE ""Periodo"" = @periodo LIMIT 1),
                                @estado, 
                                (SELECT DISTINCT descripcion FROM conceptosnomina WHERE codigo = @concepto LIMIT 1),
                                @fechahora, @devengado, @descuento
                            );";

                            // Asignación de parámetros
                            cmd.Parameters.AddWithValue("@cod_empleado", movnom.Empleado ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("@documento", string.IsNullOrWhiteSpace(movnom.Cedula) ? DBNull.Value : movnom.Cedula.Trim());
                            cmd.Parameters.AddWithValue("@apellido1", string.IsNullOrWhiteSpace(movnom.Apellido1) ? DBNull.Value : movnom.Apellido1.Trim());
                            cmd.Parameters.AddWithValue("@apellido2", string.IsNullOrWhiteSpace(movnom.Apellido2) ? DBNull.Value : movnom.Apellido2.Trim());
                            cmd.Parameters.AddWithValue("@nombre1", string.IsNullOrWhiteSpace(movnom.Nombre1) ? DBNull.Value : movnom.Nombre1.Trim());
                            cmd.Parameters.AddWithValue("@nombre2", string.IsNullOrWhiteSpace(movnom.Nombre2) ? DBNull.Value : movnom.Nombre2.Trim());
                            cmd.Parameters.AddWithValue("@concepto", movnom.Concepto.ToString().Trim());
                            cmd.Parameters.AddWithValue("@periodo", string.IsNullOrWhiteSpace(movnom.Periodo)? (object)DBNull.Value :Convert.ToInt32(movnom.Periodo));
                            cmd.Parameters.AddWithValue("@estado", true);
                            cmd.Parameters.AddWithValue("@fechahora", movnom.fecha); // DateTime
                            cmd.Parameters.AddWithValue("@devengado", movnom.Devengado); // A ajustar si tienes este dato
                            cmd.Parameters.AddWithValue("@descuento", movnom.Descuento); // A ajustar si tienes este dato

                            string finalQuery = GetInterpolatedQuery(cmd);
                            Console.WriteLine("SQL con valores:");
                            Console.WriteLine(finalQuery);



                            // Ejecutar el comando de inserción 
                            cmd.ExecuteNonQuery();


                            contador++; // Incrementar contador
                            Console.WriteLine($"Registro insertado #{contador}"); // Mostrar en consola
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error al insertar registro: {ex.Message}");
                            //ex.InnerException
                        }
                    }

                    // Confirmar la transacción
                    tx.Commit();
                }
            }

            Console.WriteLine("Datos insertados correctamente.");
        }

        public static void InsertarPlanDet(List<t_planDet> PlanDetList)
        {
            int contador = 0; // Contador
            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                // Iniciar transacción
                using (var tx = conn.BeginTransaction())
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.Transaction = tx;

                    // Insertar cada registro de vended
                    foreach (var planDet in PlanDetList)
                    {


                        try
                        {

                            cmd.Parameters.Clear();
                            cmd.CommandText = @"
                            INSERT INTO planillabiometricadomingosyfestivos (dia, estado, empleadoid, fechahoracreacion, usuarioidcreacion, numeroplanilla, diasfestivosid)
                            VALUES (@dia, @estado,
                            (SELECT DISTINCT ""Id"" FROM ""Empleados"" WHERE ""CodigoEmpleado"" = @empleadoid LIMIT 1),
                            @fechahoracreacion, 
                            (SELECT DISTINCT ""Id"" FROM ""AspNetUsers"" WHERE ""UserName"" = @usuarioidcreacion),
                            @numeroplanilla , 
                            (SELECT DISTINCT id FROM ""DiasFestivos"" WHERE dia = @dia)
                            )";

                            // Parametrización de la consulta
                            //cmd.Parameters.Clear();  // Limpiar los parámetros antes de agregar nuevos
                            cmd.Parameters.AddWithValue("dia", planDet.Fecha);
                            cmd.Parameters.AddWithValue("estado", 1);
                            cmd.Parameters.AddWithValue("empleadoid", planDet.Empleado.Trim());
                            cmd.Parameters.AddWithValue("fechahoracreacion", planDet.FechaCreacion);
                            cmd.Parameters.AddWithValue("usuarioidcreacion", planDet.Usa_crea);
                            cmd.Parameters.AddWithValue("numeroplanilla", planDet.NumeroPlanilla);

                            string finalQuery = GetInterpolatedQuery(cmd);
                            Console.WriteLine("SQL con valores:");
                            Console.WriteLine(finalQuery);

                            // Ejecutar el comando de inserción 
                            cmd.ExecuteNonQuery();


                            contador++; // Incrementar contador
                            Console.WriteLine($"Registro insertado #{contador}"); // Mostrar en consola
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error al insertar registro: {ex.Message}");
                            conn.Close();
                            //ex.InnerException
                        }

                    }

                    // Confirmar la transacción
                    tx.Commit();
                }
            }

            Console.WriteLine("Datos insertados correctamente.");
        }


        public static void InsertarDcom(List<t_dcom> DcomList)
        {
            int contador = 0;

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                foreach (var dcomS in DcomList)
                {
                    using (var tx = conn.BeginTransaction())
                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.Transaction = tx;

                        try
                        {
                            cmd.CommandText = @"
                        INSERT INTO detalleordenesdecompra(
                            idordenesdecompra, idproducto, cantidad, valor, estado, fechahoramovimiento,
                            porcentaje, descuento, sugerido, requerido, bonificacion, observacion,
                            sucursalid, diasinventario, iva, impuestoconsumo, talla, nombreproducto,
                            subgruposproducto, marcasproducto, ""tipoProducto"", valoriva, valorimpoconsumo,
                            valordscto, ean8, ean13, codigoreferencia, valorbonificacion
                        )
                        VALUES (
                            (SELECT DISTINCT Id FROM ordenesdecompra WHERE numeroordencompra = @idordenesdecompra LIMIT 1),
                            (SELECT DISTINCT ""Id"" FROM ""Productos"" WHERE ""Codigo"" = @idproducto LIMIT 1),
                            @cantidad, @valor, @estado, @fechahoramovimiento, @porcentaje,
                            @descuento, @sugerido, @requerido, @bonificacion, @observacion,
                            (SELECT DISTINCT id FROM ""Sucursales"" WHERE codigo = @sucursalid),
                            @diasinventario, @iva, @impuestoconsumo, @talla, @nombreproducto,
                            @subgruposproducto, @marcasproducto, @tipoProducto, @valoriva,
                            @valorimpoconsumo, @valordscto, @ean8, @ean13, @codigoreferencia, @valorbonificacion
                        );";

                            // Limpiar y agregar parámetros
                            cmd.Parameters.Clear();
                            cmd.Parameters.AddWithValue("@idordenesdecompra", dcomS.ordenedecompra);
                            cmd.Parameters.AddWithValue("@idproducto", dcomS.producto);
                            cmd.Parameters.AddWithValue("@cantidad", dcomS.cantidad);
                            cmd.Parameters.AddWithValue("@valor", dcomS.valor);
                            cmd.Parameters.AddWithValue("@estado", true);
                            cmd.Parameters.AddWithValue("@fechahoramovimiento", dcomS.fechahoramovimiento);
                            cmd.Parameters.AddWithValue("@porcentaje", dcomS.porcentaje);
                            cmd.Parameters.AddWithValue("@descuento", dcomS.descuento);
                            cmd.Parameters.AddWithValue("@sugerido", dcomS.sugerido);
                            cmd.Parameters.AddWithValue("@requerido", dcomS.requerido);
                            cmd.Parameters.AddWithValue("@bonificacion", dcomS.bonificacion);
                            cmd.Parameters.AddWithValue("@observacion", dcomS.observacion ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("@sucursalid", dcomS.sucursal);
                            cmd.Parameters.AddWithValue("@diasinventario", dcomS.diasinventario);
                            cmd.Parameters.AddWithValue("@iva", dcomS.iva);
                            cmd.Parameters.AddWithValue("@impuestoconsumo", dcomS.impuestoconsumo);
                            cmd.Parameters.AddWithValue("@talla", dcomS.talla);
                            cmd.Parameters.AddWithValue("@nombreproducto", dcomS.nombreproducto);
                            cmd.Parameters.AddWithValue("@subgruposproducto", dcomS.subgruposproducto);
                            cmd.Parameters.AddWithValue("@marcasproducto", dcomS.marcasproducto);
                            cmd.Parameters.AddWithValue("@tipoProducto", dcomS.tipoProducto);
                            cmd.Parameters.AddWithValue("@valoriva", dcomS.valoriva);
                            cmd.Parameters.AddWithValue("@valorimpoconsumo", dcomS.valorimpoconsumo);
                            cmd.Parameters.AddWithValue("@valordscto", dcomS.valordscto);
                            cmd.Parameters.AddWithValue("@ean8", dcomS.ean8 ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("@ean13", dcomS.ean13 ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("@codigoreferencia", dcomS.codigoreferencia);
                            cmd.Parameters.AddWithValue("@valorbonificacion", dcomS.valorbonificacion);

                            // Ejecutar e insertar
                            cmd.ExecuteNonQuery();
                            tx.Commit();

                            contador++;
                            Console.WriteLine($"✔ Registro insertado #{contador}");
                        }
                        catch (Exception ex)
                        {
                            tx.Rollback();
                            Console.WriteLine($"⚠️ Error en registro #{contador + 1}: {ex.Message}");
                        }
                    }
                }

                Console.WriteLine("✅ Inserción finalizada.");
            }
        }


        public static void InsertarCreditos(List<t_fenalCredtos> creditosList)
        {
            int contador = 0; // Contador
            string xEstado = "";
            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                // Iniciar transacción
                using (var tx = conn.BeginTransaction())
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.Transaction = tx;

                    // Insertar cada registro de vended
                    foreach (var creditos in creditosList)
                    {


                        try
                        {

                            cmd.Parameters.Clear();
                            cmd.CommandText = @"
                            INSERT INTO public.fenalpagcreditos(
	                            valormercanciacliente, cantidadcuotascliente, fenalpagid, tasainteres, iva, porcentajecuotainicial,
	                            numerocuotas, valorlimitecredito, idformapago, porcentajeaval, numerocuotacobrainteres,
	                            valorminimocredito, estado, valorcuotainicialcliente, porcentajecuotainicialcliente, periodicidad)
                            VALUES (
	                            @valormercanciacliente, @cantidadcuotascliente,
                                (SELECT id FROM fenalpag WHERE numerodocumento = @fenalpagid LIMIT 1), 
                                @tasainteres, @iva, @porcentajecuotainicial,
	                            @numerocuotas, @valorlimitecredito,
                                (SELECT ""Id"" FROM ""FormasPago"" where ""Codigo"" = @idformapago LIMIT 1),
                                @porcentajeaval, @numerocuotacobrainteres,
	                            @valorminimocredito, @estado, @valorcuotainicialcliente, @porcentajecuotainicialcliente, @periodicidad
                            )";

                            switch(creditos.periodicidad)
                            {
                                case 1:

                                    xEstado = "Quincenal";

                                    break;
                                case 2:

                                    xEstado = "Mensual";

                                    break;
                                case 3:

                                    xEstado = "Semanal";

                                    break;
                            }




                            // Parametrización de la consulta
                            //cmd.Parameters.Clear();  // Limpiar los parámetros antes de agregar nuevos
                            cmd.Parameters.AddWithValue("valormercanciacliente", creditos.valormercanciacliente);
                            cmd.Parameters.AddWithValue("cantidadcuotascliente", creditos.cantidadcuotascliente);
                            cmd.Parameters.AddWithValue("fenalpagid", creditos.fenalpagid.ToString());
                            cmd.Parameters.AddWithValue("tasainteres", creditos.tasainteres);
                            cmd.Parameters.AddWithValue("iva", creditos.iva);
                            cmd.Parameters.AddWithValue("porcentajecuotainicial", creditos.porcentajecuotainicial);
                            cmd.Parameters.AddWithValue("numerocuotas", creditos.numerocuotas);
                            cmd.Parameters.AddWithValue("valorlimitecredito", 0);
                            cmd.Parameters.AddWithValue("idformapago", creditos.idformapago.ToString());
                            cmd.Parameters.AddWithValue("porcentajeaval", creditos.porcentajeaval);
                            cmd.Parameters.AddWithValue("numerocuotacobrainteres",0);
                            cmd.Parameters.AddWithValue("valorminimocredito", 0);
                            cmd.Parameters.AddWithValue("estado", true);
                            cmd.Parameters.AddWithValue("valorcuotainicialcliente", creditos.valorcuotainicialcliente);
                            cmd.Parameters.AddWithValue("porcentajecuotainicialcliente", creditos.porcentajecuotainicialcliente);
                            cmd.Parameters.AddWithValue("periodicidad", xEstado);

                            string finalQuery = GetInterpolatedQuery(cmd);
                            Console.WriteLine("SQL con valores:");
                            Console.WriteLine(finalQuery);

                            // Ejecutar el comando de inserción 
                            cmd.ExecuteNonQuery();


                            contador++; // Incrementar contador
                            Console.WriteLine($"Registro insertado #{contador}"); // Mostrar en consola
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error al insertar registro: {ex.Message}");
                            conn.Close();
                            //ex.InnerException
                        }

                    }

                    // Confirmar la transacción
                    tx.Commit();
                }
            }

            Console.WriteLine("Datos insertados correctamente.");
        }


        //public static void InsertarCajasMovimientos(List<t_cabFact> movimientosList)
        //{
        //    int contador = 0;

        //    using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
        //    {
        //        conn.Open();

        //        foreach (var mov in movimientosList)
        //        {
        //            using (var tx = conn.BeginTransaction())
        //            using (var cmd = new NpgsqlCommand())
        //            {
        //                cmd.Connection = conn;
        //                cmd.Transaction = tx;

        //                try
        //                {
        //                    cmd.CommandText = @"
        //                INSERT INTO public.cajasmovimientos(
        //                    fechahoramovimiento, sucursalid, sucursalnombre, cajaid, cajacodigo, usuarioid, usuarionombre,
        //                    terceroid, nombretercero, tarjetamercapesosid, tarjetamercapesoscodigo, documentofactura,
        //                    totalventa, valorcambio, tipopagoid,numerofactura, documentoid,
        //                    documentonombre, identificacion)
        //                VALUES (
        //                    @fechahoramovimiento, 
        //                    (SELECT id FROM ""Sucursales"" WHERE codigo= @sucursalid LIMIT 1),
        //                    (SELECT nombre FROM ""Sucursales"" WHERE codigo= @sucursalid LIMIT 1), 
        //                    (
        //                    SELECT ""Id"" 
        //                    FROM ""CajaSucursal"" 
        //                    WHERE ""CodigoCaja"" = @cajacodigo 
        //                        AND ""IdSucursal"" = (SELECT id FROM ""Sucursales"" WHERE codigo = @sucursalid LIMIT 1)
        //                    LIMIT 1
        //                    ),
        //                    @cajacodigo,
        //                    (SELECT DISTINCT ""Id"" FROM ""AspNetUsers"" WHERE ""UserName"" = @usuarioid LIMIT 1),
        //                    @usuarionombre,
        //                    (SELECT DISTINCT ""Id"" FROM ""Terceros"" WHERE ""Identificacion"" = @terceroid LIMIT 1),
        //                    @nombretercero, 
        //                    (SELECT ""Id""	FROM ""MercaPesos"" where ""Tarjeta"" =@tarjetamercapesosid LIMIT 1),
        //                    (SELECT ""Id""	FROM ""MercaPesos"" where ""Codigo"" =@tarjetamercapesoscodigo LIMIT 1), @documentofactura,
        //                    @totalventa, @valorcambio, 
        //                    (SELECT id FROM tiposdepago where id = @tipopagoid LIMIT 1),
        //                    @numerofactura, 
        //                    (SELECT ""Id"" FROM ""Documentos"" WHERE ""Codigo"" = @documentoid LIMIT 1),
        //                    @documentonombre, @identificacion);";

        //                    cmd.Parameters.Clear();
        //                    cmd.Parameters.AddWithValue("@fechahoramovimiento", mov.fechahoramovimiento);
        //                    cmd.Parameters.AddWithValue("@sucursalid", mov.sucursalid);
        //                    //cmd.Parameters.AddWithValue("@sucursalnombre", mov.sucursalnombre);
        //                    //cmd.Parameters.AddWithValue("@cajaid", mov.cajaid);
        //                    cmd.Parameters.AddWithValue("@cajacodigo", mov.cajacodigo);
        //                    cmd.Parameters.AddWithValue("@usuarioid", mov.usuarioid);
        //                    cmd.Parameters.AddWithValue("@usuarionombre", mov.usuarionombre);
        //                    cmd.Parameters.AddWithValue("@terceroid", mov.terceroid);
        //                    cmd.Parameters.AddWithValue("@nombretercero", mov.nombretercero);
        //                    cmd.Parameters.AddWithValue("@tarjetamercapesosid", mov.tarjetamercapesosid == 0 ? DBNull.Value : mov.tarjetamercapesosid);
        //                    cmd.Parameters.AddWithValue("@tarjetamercapesoscodigo", string.IsNullOrWhiteSpace(mov.tarjetamercapesoscodigo) ? DBNull.Value : mov.tarjetamercapesoscodigo);
        //                    cmd.Parameters.AddWithValue("@documentofactura", mov.documentofactura ?? (object)DBNull.Value);
        //                    cmd.Parameters.AddWithValue("@totalventa", mov.totalventa);
        //                    cmd.Parameters.AddWithValue("@valorcambio", mov.valorcambio);
        //                    cmd.Parameters.AddWithValue("@tipopagoid", mov.tipopagoid);
        //                    cmd.Parameters.AddWithValue("@numerofactura", mov.numerofactura);
        //                    cmd.Parameters.AddWithValue("@documentoid", mov.documentoid);
        //                    cmd.Parameters.AddWithValue("@documentonombre", mov.documentonombre);
        //                    cmd.Parameters.AddWithValue("@identificacion", mov.identificacion);

        //                    string finalQuery = GetInterpolatedQuery(cmd);
        //                    Console.WriteLine("SQL con valores:");
        //                    Console.WriteLine(finalQuery);

        //                    cmd.ExecuteNonQuery();
        //                    tx.Commit();

        //                    contador++;
        //                    Console.WriteLine($"✔ Registro insertado #{contador}");
        //                }
        //                catch (Exception ex)
        //                {
        //                    tx.Rollback();
        //                    Console.WriteLine($"⚠️ Error en registro #{contador + 1}: {ex.Message}");
        //                }
        //            }
        //        }

        //        Console.WriteLine("✅ Inserción finalizada.");
        //    }
        //}


        public static void InsertarCajasMovimientos(List<t_cabFact> movimientosList, int batchSize = 100)
        {
            int contador = 0;
            var cronometer = Stopwatch.StartNew();
            Stopwatch sw = new Stopwatch();
            sw.Start();

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                while (contador < movimientosList.Count)
                {
                    using (var tx = conn.BeginTransaction())
                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.Transaction = tx;

                        var commandText = new StringBuilder();

                        commandText.AppendLine(@"
                        INSERT INTO public.cajasmovimientosgenerales(
                            fechahoramovimiento, sucursalid, sucursalnombre, cajaid, cajacodigo, usuarioid, usuarionombre,
                            terceroid, nombretercero, tarjetamercapesosid, tarjetamercapesoscodigo, documentofactura,
                            totalventa, valorcambio, tipopagoid,numerofactura, documentoid,
                            documentonombre, identificacion)VALUES");

                        var valuesList = new List<string>();
                        int currentBatchSize = 0;

                        while (contador < movimientosList.Count && currentBatchSize < batchSize)
                        {
                            var mov = movimientosList[contador];

                            
                            var values = $@"
                    (
                            @fechahoramovimiento{contador}, 
                            (SELECT id FROM ""Sucursales"" WHERE codigo= @sucursalid{contador} LIMIT 1),
                            (SELECT nombre FROM ""Sucursales"" WHERE codigo= @sucursalid{contador} LIMIT 1), 
                            (
                            SELECT ""Id"" 
                            FROM ""CajaSucursal"" 
                            WHERE ""CodigoCaja"" = @cajacodigo{contador} 
                                AND ""IdSucursal"" = (SELECT id FROM ""Sucursales"" WHERE codigo = @sucursalid{contador} LIMIT 1)
                            LIMIT 1
                            ),
                            @cajacodigo,
                            (SELECT DISTINCT ""Id"" FROM ""AspNetUsers"" WHERE ""UserName"" = @usuarioid{contador} LIMIT 1),
                            @usuarionombre{contador},
                            (SELECT DISTINCT ""Id"" FROM ""Terceros"" WHERE ""Identificacion"" = @terceroid{contador} LIMIT 1),
                            @nombretercero{contador}, 
                            (SELECT ""Id""	FROM ""MercaPesos"" where ""Tarjeta"" =@tarjetamercapesosid{contador} LIMIT 1),
                            (SELECT ""Id""	FROM ""MercaPesos"" where ""Codigo"" =@tarjetamercapesoscodigo{contador} LIMIT 1), @documentofactura{contador},
                            @totalventa{contador}, @valorcambio{contador}, 
                            (SELECT id FROM tiposdepago where id = @tipopagoid{contador} LIMIT 1),
                            @numerofactura{contador}, 
                            (SELECT ""Id"" FROM ""Documentos"" WHERE ""Codigo"" = @documentoid{contador} LIMIT 1),
                            @documentonombre{contador}, @identificacion{contador}
                         )";

                            valuesList.Add(values);
                            contador++;
                            currentBatchSize++;
                        }

                        // Combina todos los VALUES en una sola consulta
                        commandText.Append(string.Join(",", valuesList));

                        // Asigna el comando completo
                        cmd.CommandText = commandText.ToString();

                        // Agregar parámetros para el lote actual
                        for (int i = 0; i < currentBatchSize; i++)
                        {

                            var mov = movimientosList[contador - currentBatchSize + i];

                            cmd.Parameters.AddWithValue($"@fechahoramovimiento{contador - currentBatchSize + i}", mov.fechahoramovimiento);
                            cmd.Parameters.AddWithValue($"@sucursalid{contador - currentBatchSize + i}", mov.sucursalid);
                            //cmd.Parameters.AddWithValue("@sucursalnombre", mov.sucursalnombre);
                            //cmd.Parameters.AddWithValue("@cajaid", mov.cajaid);
                            cmd.Parameters.AddWithValue($"@cajacodigo {contador - currentBatchSize + i}", mov.cajacodigo);
                            cmd.Parameters.AddWithValue($"@usuarioid {contador - currentBatchSize + i}", mov.usuarioid);
                            cmd.Parameters.AddWithValue($"@usuarionombre {contador - currentBatchSize + i}", mov.usuarionombre);
                            cmd.Parameters.AddWithValue($"@terceroid {contador - currentBatchSize + i}", mov.terceroid);
                            cmd.Parameters.AddWithValue($"@nombretercero {contador - currentBatchSize + i}", mov.nombretercero);
                            cmd.Parameters.AddWithValue($"@tarjetamercapesosid {contador - currentBatchSize + i}", mov.tarjetamercapesosid == 0 ? DBNull.Value : mov.tarjetamercapesosid);
                            cmd.Parameters.AddWithValue($"@tarjetamercapesoscodigo {contador - currentBatchSize + i}", string.IsNullOrWhiteSpace(mov.tarjetamercapesoscodigo) ? DBNull.Value : mov.tarjetamercapesoscodigo);
                            cmd.Parameters.AddWithValue($"@documentofactura {contador - currentBatchSize + i}", mov.documentofactura ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@totalventa {contador - currentBatchSize + i}", mov.totalventa);
                            cmd.Parameters.AddWithValue($"@valorcambio {contador - currentBatchSize + i}", mov.valorcambio);
                            cmd.Parameters.AddWithValue($"@tipopagoid {contador - currentBatchSize + i}", mov.tipopagoid);
                            cmd.Parameters.AddWithValue($"@numerofactura {contador - currentBatchSize + i}", mov.numerofactura);
                            cmd.Parameters.AddWithValue($"@documentoid {contador - currentBatchSize + i}", mov.documentoid);
                            cmd.Parameters.AddWithValue($"@documentonombre {contador - currentBatchSize + i}", mov.documentonombre);
                            cmd.Parameters.AddWithValue($"@identificacion{contador - currentBatchSize + i}", mov.identificacion);


                        }

                        cmd.ExecuteNonQuery();
                        tx.Commit();
                    }
                }
            }
            cronometer.Stop();
            TimeSpan tiempoTranscurrido = cronometer.Elapsed;
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours} hora");
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes} minutos");
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds} segundos");
        }




        public static void InsertarCajasDetalles(List<t_detFact> detfactList)
        {
            int contador = 0;

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                foreach (var detalle in detfactList)
                {
                    using (var tx = conn.BeginTransaction())
                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.Transaction = tx;

                        try
                        {
                            cmd.CommandText = @"
                            INSERT INTO public.cajasmovimientosgenerales(
                                cajasmovimientosid, productoid, productonombre, productovalorsiniva,
                                productoporcentajeiva, productovaloriva, productoexento,
                                promocionid, promocionnombre, promocionvalordscto, productovalorimptoconsumo,
                                vendedorid, valordescuentogeneral, totalitem, productoidmarca, productoidlinea,
                                productoidsublinea, valoracumulartarejetamercapesos, cantidaddelproducto,
                                productovalorantesdscto, productoporcentajedscto, productovalordescuento,
                                factorimptoconsumo, productonombremarca, productonombrelinea,
                                productonombresublinea)
                            VALUES (
                                
                                (SELECT id	FROM public.cajasmovimientos where numerofactura = @cajasmovimientosid LIMIT 1),
                                (SELECT ""Id"" FROM public.""Productos"" where ""Codigo"" = @productoid LIMIT 1),
                                @productonombre, @productovalorsiniva,
                                @productoporcentajeiva, @productovaloriva, @productoexento,
                                (SELECT id FROM public.""PromocionesDescuentos"" where codigo =@promocionid LIMIT 1),
                                @promocionnombre, @promocionvalordscto, @productovalorimptoconsumo,
                                (SELECT id FROM public.vendedores where codigo =@vendedorid LIMIT 1),
                                @valordescuentogeneral, @totalitem,
                                (SELECT id FROM public.""MarcasProductos"" where codigo =  @productoidmarca LIMIT 1),
                                (SELECT id FROM public.""LineasProductos"" where codigo = @productoidlinea LIMIT 1),
                                (SELECT id FROM public.""SublineasProductos"" where codigo =@productoidsublinea LIMIT 1),
                                @valoracumulartarejetamercapesos, @cantidaddelproducto,
                                @productovalorantesdscto, @productoporcentajedscto, @productovalordescuento,
                                @factorimptoconsumo, 
                                (SELECT nombre FROM public.""MarcasProductos"" where codigo =  @productoidmarca LIMIT 1),
                                (SELECT nombre FROM public.""LineasProductos"" where codigo = @productoidlinea LIMIT 1),
                                (SELECT nombre FROM public.""SublineasProductos"" where codigo =@productoidsublinea LIMIT 1)
                                );";

                            cmd.Parameters.Clear();
                            cmd.Parameters.AddWithValue("@cajasmovimientosid", Convert.ToString(detalle.cajasmovimientosid));
                            cmd.Parameters.AddWithValue("@productoid", detalle.productoid);
                            cmd.Parameters.AddWithValue("@productonombre", detalle.productonombre);
                            cmd.Parameters.AddWithValue("@productovalorsiniva", detalle.productovalorsiniva);
                            cmd.Parameters.AddWithValue("@productoporcentajeiva", detalle.productoporcentajeiva);
                            cmd.Parameters.AddWithValue("@productovaloriva", detalle.productovaloriva);
                            cmd.Parameters.AddWithValue("@productoexento", detalle.productoexento == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@promocionid", detalle.promocionid);
                            cmd.Parameters.AddWithValue("@promocionnombre", detalle.promocionnombre ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("@promocionvalordscto", detalle.promocionvalordscto);
                            cmd.Parameters.AddWithValue("@productovalorimptoconsumo", detalle.productovalorimptoconsumo);
                            cmd.Parameters.AddWithValue("@vendedorid", detalle.vendedorid ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("@valordescuentogeneral", detalle.valordescuentogeneral);
                            cmd.Parameters.AddWithValue("@totalitem", detalle.totalitem);
                            cmd.Parameters.AddWithValue("@productoidmarca", detalle.productoidmarca);
                            cmd.Parameters.AddWithValue("@productoidlinea", detalle.productoidlinea);
                            cmd.Parameters.AddWithValue("@productoidsublinea", detalle.productoidsublinea);
                            cmd.Parameters.AddWithValue("@valoracumulartarejetamercapesos", detalle.valoracumulartarejetamercapesos);
                            cmd.Parameters.AddWithValue("@cantidaddelproducto", detalle.cantidaddelproducto);
                            cmd.Parameters.AddWithValue("@productovalorantesdscto", detalle.productovalorantesdscto);
                            cmd.Parameters.AddWithValue("@productoporcentajedscto", detalle.productoporcentajedscto);
                            cmd.Parameters.AddWithValue("@productovalordescuento", detalle.productovalordescuento);
                            cmd.Parameters.AddWithValue("@factorimptoconsumo", detalle.factorimptoconsumo);
                            cmd.Parameters.AddWithValue("@productonombremarca", detalle.productonombremarca ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("@productonombrelinea", detalle.productonombrelinea ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue("@productonombresublinea", detalle.productonombresublinea ?? (object)DBNull.Value);
                            

                            string finalQuery = GetInterpolatedQuery(cmd);
                            Console.WriteLine("SQL con valores:");
                            Console.WriteLine(finalQuery);

                            cmd.ExecuteNonQuery();
                            tx.Commit();

                            contador++;
                            Console.WriteLine($"✔ Registro insertado #{contador}");
                        }
                        catch (Exception ex)
                        {
                            //tx.Rollback();
                            Console.WriteLine($"⚠️ Error en registro #{contador + 1}: {ex.Message}");
                        }
                    }
                }

                Console.WriteLine("✅ Inserción finalizada.");
            }
        }


        public static void InsertarCajasDetalles(List<t_detFact> detfactList, int batchSize = 100)
        {
            int contador = 0;
            var cronometer = Stopwatch.StartNew();
            Stopwatch sw = new Stopwatch();
            sw.Start();

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();

                while (contador < detfactList.Count)
                {
                    using (var tx = conn.BeginTransaction())
                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.Transaction = tx;

                        var commandText = new StringBuilder();

                        commandText.AppendLine(@"
                        INSERT INTO public.cajasmovimientosgenerales(
                                cajasmovimientosid, productoid, productonombre, productovalorsiniva,
                                productoporcentajeiva, productovaloriva, productoexento,
                                promocionid, promocionnombre, promocionvalordscto, productovalorimptoconsumo,
                                vendedorid, valordescuentogeneral, totalitem, productoidmarca, productoidlinea,
                                productoidsublinea, valoracumulartarejetamercapesos, cantidaddelproducto,
                                productovalorantesdscto, productoporcentajedscto, productovalordescuento,
                                factorimptoconsumo, productonombremarca, productonombrelinea,
                                productonombresublinea)VALUES");

                        var valuesList = new List<string>();
                        int currentBatchSize = 0;

                        while (contador < detfactList.Count && currentBatchSize < batchSize)
                        {
                            var items = detfactList[contador];

                            
                            var values = $@"
                    (
                                (SELECT id	FROM public.cajasmovimientos where numerofactura = @cajasmovimientosid{contador} LIMIT 1),
                                (SELECT ""Id"" FROM public.""Productos"" where ""Codigo"" = @productoid{contador} LIMIT 1),
                                @productonombre{contador}, @productovalorsiniva{contador},
                                @productoporcentajeiva{contador}, @productovaloriva{contador}, @productoexento{contador},
                                (SELECT id FROM public.""PromocionesDescuentos"" where codigo =@promocionid{contador} LIMIT 1),
                                @promocionnombre{contador}, @promocionvalordscto{contador}, @productovalorimptoconsumo{contador},
                                (SELECT id FROM public.vendedores where codigo =@vendedorid{contador} LIMIT 1),
                                @valordescuentogeneral{contador}, @totalitem{contador},
                                (SELECT id FROM public.""MarcasProductos"" where codigo =  @productoidmarca{contador} LIMIT 1),
                                (SELECT id FROM public.""LineasProductos"" where codigo = @productoidlinea{contador} LIMIT 1),
                                (SELECT id FROM public.""SublineasProductos"" where codigo =@productoidsublinea{contador} LIMIT 1),
                                @valoracumulartarejetamercapesos{contador}, @cantidaddelproducto{contador},
                                @productovalorantesdscto{contador}, @productoporcentajedscto{contador}, @productovalordescuento{contador},
                                @factorimptoconsumo{contador}, 
                                (SELECT nombre FROM public.""MarcasProductos"" where codigo =  @productoidmarca{contador} LIMIT 1),
                                (SELECT nombre FROM public.""LineasProductos"" where codigo = @productoidlinea{contador} LIMIT 1),
                                (SELECT nombre FROM public.""SublineasProductos"" where codigo =@productoidsublinea{contador} LIMIT 1)
                         )";

                            valuesList.Add(values);
                            contador++;
                            currentBatchSize++;
                        }

                        // Combina todos los VALUES en una sola consulta
                        commandText.Append(string.Join(",", valuesList));

                        // Asigna el comando completo
                        cmd.CommandText = commandText.ToString();

                        // Agregar parámetros para el lote actual
                        for (int i = 0; i < currentBatchSize; i++)
                        {

                            var detalle = detfactList[contador - currentBatchSize + i];


                            cmd.Parameters.AddWithValue($"@cajasmovimientosid{ contador - currentBatchSize + i}", Convert.ToString(detalle.cajasmovimientosid));
                            cmd.Parameters.AddWithValue($"@productoid{contador - currentBatchSize + i}", detalle.productoid);
                            cmd.Parameters.AddWithValue($"@productonombre{contador - currentBatchSize + i}", detalle.productonombre);
                            cmd.Parameters.AddWithValue($"@productovalorsiniva{contador - currentBatchSize + i}", detalle.productovalorsiniva);
                            cmd.Parameters.AddWithValue($"@productoporcentajeiva{contador - currentBatchSize + i}", detalle.productoporcentajeiva);
                            cmd.Parameters.AddWithValue($"@productovaloriva{contador - currentBatchSize + i}", detalle.productovaloriva);
                            cmd.Parameters.AddWithValue($"@productoexento {contador - currentBatchSize + i}", detalle.productoexento == 1 ? true : false);
                            cmd.Parameters.AddWithValue($"@promocionid {contador - currentBatchSize + i}", detalle.promocionid);
                            cmd.Parameters.AddWithValue($"@promocionnombre {contador - currentBatchSize + i}", detalle.promocionnombre ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@promocionvalordscto {contador - currentBatchSize + i}", detalle.promocionvalordscto);
                            cmd.Parameters.AddWithValue($"@productovalorimptoconsumo {contador - currentBatchSize + i}", detalle.productovalorimptoconsumo);
                            cmd.Parameters.AddWithValue($"@vendedorid {contador - currentBatchSize + i}", detalle.vendedorid ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@valordescuentogeneral {contador - currentBatchSize + i}", detalle.valordescuentogeneral);
                            cmd.Parameters.AddWithValue($"@totalitem {contador - currentBatchSize + i}", detalle.totalitem);
                            cmd.Parameters.AddWithValue($"@productoidmarca {contador - currentBatchSize + i}", detalle.productoidmarca);
                            cmd.Parameters.AddWithValue($"@productoidlinea {contador - currentBatchSize + i}", detalle.productoidlinea);
                            cmd.Parameters.AddWithValue($"@productoidsublinea {contador - currentBatchSize + i}", detalle.productoidsublinea);
                            cmd.Parameters.AddWithValue($"@valoracumulartarejetamercapesos {contador - currentBatchSize + i}", detalle.valoracumulartarejetamercapesos);
                            cmd.Parameters.AddWithValue($"@cantidaddelproducto {contador - currentBatchSize + i}", detalle.cantidaddelproducto);
                            cmd.Parameters.AddWithValue($"@productovalorantesdscto {contador - currentBatchSize + i}", detalle.productovalorantesdscto);
                            cmd.Parameters.AddWithValue($"@productoporcentajedscto{contador - currentBatchSize + i}", detalle.productoporcentajedscto);
                            cmd.Parameters.AddWithValue($"@productovalordescuento{contador - currentBatchSize + i}", detalle.productovalordescuento);
                            cmd.Parameters.AddWithValue($"@factorimptoconsumo {contador - currentBatchSize + i}", detalle.factorimptoconsumo);
                            cmd.Parameters.AddWithValue($"@productonombremarca {contador - currentBatchSize + i}", detalle.productonombremarca ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@productonombrelinea {contador - currentBatchSize + i}", detalle.productonombrelinea ?? (object)DBNull.Value);
                            cmd.Parameters.AddWithValue($"@productonombresublinea {contador - currentBatchSize + i}", detalle.productonombresublinea ?? (object)DBNull.Value);
                            // Manejo de subconsultas



                        }

                        cmd.ExecuteNonQuery();
                        tx.Commit();
                    }
                }
            }
            cronometer.Stop();
            TimeSpan tiempoTranscurrido = cronometer.Elapsed;
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalHours} hora");
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalMinutes} minutos");
            Console.WriteLine($"Tiempo transcurrido: {tiempoTranscurrido.TotalSeconds} segundos");
        }










        public static double CalcularRentabilidad(
            double pvtaxx, double pcosto, int pmodo, double piva,
            double pconsumo, int picon_1111,
            int pr_tipoiup, double pr_vr_ibu, DateTime pr_fech_iu, int pr_gen_iup)
            {
                if (double.IsNaN(picon_1111)) // FoxPro hace TYPE, aquí usamos NaN como equivalente inválido
                    picon_1111 = 0;

                double x_porc = 0.00;

                if (pvtaxx == 0 || pcosto == 0)
                    return 0;

                double xFact_iup = 0.00;
                double x_vr_ibu = 0.00;

                // Determinar impuesto saludable
                switch (pr_tipoiup)
                {
                    case 1:
                        xFact_iup = FPorcIcup(pr_fech_iu);
                        break;

                    case 2:
                        // Bebidas: no se hace nada según lógica actual
                        break;
                }

                double x_iva = 0;

                switch (pmodo)
                {
                    case 1:
                        if (pcosto > 0)
                        {
                            x_iva = 1 + (piva / 100.0);
                            x_porc = Math.Round(((pvtaxx / ((pcosto * x_iva) + pconsumo)) - 1) * 100, 2);
                        }
                        break;

                    case 2:
                        x_iva = 1 + (piva / 100.0);
                        x_porc = Math.Round((1 - ((pcosto + pconsumo) / (pvtaxx / x_iva))) * 100, 2);
                        break;

                    case 3:
                        x_iva = 1 + ((piva + xFact_iup) / 100.0);

                        if (picon_1111 == 1)
                        {
                            x_porc = pvtaxx / (((pcosto + pr_vr_ibu) * x_iva) + pconsumo);
                            x_porc = (x_porc - 1) * 100;
                            x_porc = Math.Round(x_porc, 2);
                        }
                        else
                        {
                            x_porc = Math.Round(((pvtaxx / ((pcosto + pr_vr_ibu) * x_iva)) - 1) * 100.0, 2);
                        }
                        break;

                    case 4:
                        x_iva = 1 + ((piva + xFact_iup) / 100.0);

                        if (picon_1111 == 1)
                        {
                            x_porc = pvtaxx / ((pcosto * x_iva) + pconsumo + pr_vr_ibu);
                            x_porc = (x_porc - 1) * 100;
                            x_porc = Math.Round(x_porc, 2);
                        }
                        else
                        {
                            x_porc = (pvtaxx - pconsumo) / ((pcosto * x_iva) + pr_vr_ibu);
                            x_porc = (x_porc - 1) * 100;
                            x_porc = Math.Round(x_porc, 2);
                        }
                        break;
                }

                if (x_porc > 999.99)
                    x_porc = 999.99;

                if (x_porc < 0)
                    x_porc = 0; // FoxPro no hace nada, puedes decidir si lo dejas en cero

                return x_porc;

            }


        private static double FPorcIcup(DateTime fecha)
        {
            // Simulación. Sustituir con la lógica real de cálculo del impuesto por fecha.
            int año = fecha.Year;
            if (año >= 2024) return 10.0;
            if (año == 2023) return 8.0;
            return 5.0;
        }


        public static decimal FCalcImp(
        decimal pVrBase,
        decimal pFiva,
        decimal pVrIconsu,
        int pModo,
        int pRound,
        int pmTipoIup,
        decimal pmVrIbu,
        DateTime pmFechIu,
        int pmGenIup,
        string pReturnUp)
        {
            int xNumRound = pRound;
            decimal xFactIup = 0.00m;
            decimal xVrIbu = 0.00m;

            if (pmGenIup == 1)
            {
                switch (pmTipoIup)
                {
                    case 1: // Comestibles
                        xFactIup = FporcIcup(pmFechIu);
                        break;
                    case 2: // Bebidas
                        xVrIbu = pmVrIbu;
                        break;
                }
            }

            decimal xVrBase = 0.00m;
            decimal xRetorno = 0.00m;
            decimal xBaseGrav = 0.00m;
            decimal xCalcIva = 0.00m;
            decimal xCalcUp = 0.00m;

            switch (pModo)
            {
                case 1: // Valor base con impuestos incluidos
                    xVrBase = pVrBase - pVrIconsu - xVrIbu;

                    decimal divisor = Math.Round(1 + Math.Round((pFiva + xFactIup) / 100.00m, 4), 4);
                    xBaseGrav = Math.Round(xVrBase / divisor, xNumRound);

                    if (pFiva > 0 && xFactIup == 0)
                    {
                        xCalcIva = xVrBase - xBaseGrav;
                        xCalcUp = pmTipoIup == 2 ? xVrIbu : 0;
                    }
                    else if (pFiva == 0 && xFactIup > 0)
                    {
                        xCalcIva = 0;
                        xCalcUp = xVrBase - xBaseGrav;
                    }
                    else if (pFiva > 0 && xFactIup > 0)
                    {
                        xCalcUp = Math.Round(xBaseGrav * Math.Round(xFactIup / 100.00m, 4), 2);
                        xCalcIva = xVrBase - xBaseGrav - xCalcUp;
                    }
                    else
                    {
                        xCalcIva = 0;
                        xCalcUp = pmTipoIup == 2 ? xVrIbu : 0;
                    }
                    break;

                case 2: // Valor base sin IVA ni ultraprocesados, pero con impoconsumo
                    xVrBase = pVrBase - pVrIconsu;

                    if (pFiva > 0 && xFactIup == 0)
                    {
                        xCalcIva = Math.Round(xVrBase * Math.Round(pFiva / 100.00m, 4), xNumRound);
                        xCalcUp = pmTipoIup == 2 ? xVrIbu : 0;
                    }
                    else if (pFiva == 0 && xFactIup > 0)
                    {
                        xCalcIva = 0;
                        xCalcUp = Math.Round(xVrBase * Math.Round(xFactIup / 100.00m, 4), xNumRound);
                    }
                    else if (pFiva > 0 && xFactIup > 0)
                    {
                        xCalcUp = Math.Round(xVrBase * Math.Round(xFactIup / 100.00m, 4), 2);
                        xCalcIva = Math.Round(xVrBase * Math.Round(pFiva / 100.00m, 4), 2);
                    }
                    else
                    {
                        xCalcIva = 0;
                        xCalcUp = pmTipoIup == 2 ? xVrIbu : 0;
                    }
                    break;
            }

            switch (pReturnUp)
            {
                case "U":
                    xRetorno = xCalcUp;
                    break;
                case "I":
                    xRetorno = xCalcIva;
                    break;
                case "A":
                    xRetorno = xCalcIva + xCalcUp;
                    break;
            }

            return xRetorno;
        }


        public static decimal FporcIcup(DateTime pFechCaus)
        {
            decimal xIcup = 0.00m;

            if (pFechCaus.Year == 2023)
            {
                DateTime xFeIcup = new DateTime(2023, 11, 1);

                if (pFechCaus >= xFeIcup)
                {
                    xIcup = 10.00m;
                }
            }
            else if (pFechCaus.Year == 2024)
            {
                xIcup = 15.00m;
            }
            else if (pFechCaus.Year == 2025)
            {
                xIcup = 20.00m;
            }

            return xIcup;
        }

        public static void InsertarDocumentos(List<t_docum> docum_list)
        {
            int contador = 0; // Contador

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();
                //Iniciar transaccion
                using (var tx = conn.BeginTransaction())
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.Transaction = tx;

                    foreach (var docum in docum_list)
                    {
                        try
                        {
                            cmd.Parameters.Clear();

                            // Comando SQL con parámetros
                            cmd.CommandText = $@"INSERT INTO public.""Documentos""(""Codigo"", ""Nombre"", ""Asimilar"", ""CD"", ""CT"",
                                ""IN"", ""VD"", ""UV"", ""CR"", ""PC"", ""DO"", ""TR"", ""CA"", ""DD"", ""LT"", ""NL"", ""RD"", ""RO"",
                                ""ControlFechas"", ""Vendedor"", ""Zona"", ""CCosto"", ""Resolucion"", ""ActivarColumna"", ""ControlaPagos"",
                                ""Cuentas"", ""FechaCreacion"", ""IdDocumentoContrapartida"", ""Naturaleza"", ""Detalles"", ""Mensaje1"",
                                ""Mensaje2"", ""Mensaje3"", ""ValoresCartera"", ""Anexo1"", ""Anexo2"", ""Anexo3"", ""Anexo4"", ""Anexo5"",
                                ""Anexo6"", ""MovimientoCartera"", ""FusionarDocumento"")VALUES (@Codigo,@Nombre,@Asimilar,@CD,@CT,
                                @IN,@VD,@UV,@CR,@PC,@DO,@TR,@CA,@DD,@LT,@NL,@RD,@RO,@ControlFechas,@Vendedor,@Zona,@CCosto,@Resolucion,
                                @ActivarColumna,@ControlaPagos,@Cuentas,@FechaCreacion,@IdDocumentoContrapartida,@Naturaleza,@Detalles,@Mensaje1,
                                @Mensaje2,@Mensaje3,@ValoresCartera,@Anexo1,@Anexo2,@Anexo3,@Anexo4,@Anexo5,@Anexo6,@MovimientoCartera,@FusionarDocumento                                            
                                )";

                            long bTipodoc = docum.tipo_doc; //se convierte a bigint. long es lo mismo que bigint


                            cmd.Parameters.AddWithValue("@Codigo", docum.docum);
                            cmd.Parameters.AddWithValue("@Nombre", docum.nombre);
                            cmd.Parameters.AddWithValue("@Asimilar", bTipodoc);
                            cmd.Parameters.AddWithValue("@CD", docum.contabil == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@CT", docum.si_cnomb == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@IN", docum.bloqueado == 1 ? false : true);
                            cmd.Parameters.AddWithValue("@VD", docum.vali_doc == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@UV", docum.si_consec == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@CR", docum.controlrut == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@PC", docum.camb_ter == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@DO", docum.desc_ord == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@TR", docum.es_trans == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@CA", docum.cons_proc == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@DD", docum.desc_doci == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@LT", docum.silibtes == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@NL", docum.n_lineas > 1 ? true : false);
                            cmd.Parameters.AddWithValue("@RD", docum.n_recup == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@RO", docum.obser_doc == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@ControlFechas", docum.cont_fec);
                            cmd.Parameters.AddWithValue("@Vendedor", docum.vend_det == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@Zona", docum.zon_det == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@CCosto", docum.cco_det == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@Resolucion", docum.es_resolu == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@ActivarColumna", docum.sniif_on == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@ControlaPagos", docum.si_contpag == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@Cuentas", false);
                            cmd.Parameters.AddWithValue("@FechaCreacion", docum.fecha_cre);
                            cmd.Parameters.AddWithValue("@IdDocumentoContrapartida", DBNull.Value);
                            cmd.Parameters.AddWithValue("@Naturaleza", 1);
                            cmd.Parameters.AddWithValue("@Detalles", "");
                            cmd.Parameters.AddWithValue("@Mensaje1", docum.Mensaje1);
                            cmd.Parameters.AddWithValue("@Mensaje2", docum.Mensaje2);
                            cmd.Parameters.AddWithValue("@Mensaje3", docum.Mensaje3);
                            cmd.Parameters.AddWithValue("@ValoresCartera", docum.afin_cxc);
                            cmd.Parameters.AddWithValue("@Anexo1", docum.Anexo1);
                            cmd.Parameters.AddWithValue("@Anexo2", docum.Anexo2);
                            cmd.Parameters.AddWithValue("@Anexo3", docum.Anexo3);
                            cmd.Parameters.AddWithValue("@Anexo4", docum.Anexo4);
                            cmd.Parameters.AddWithValue("@Anexo5", docum.Anexo5);
                            cmd.Parameters.AddWithValue("@Anexo6", docum.Anexo6);
                            cmd.Parameters.AddWithValue("@MovimientoCartera", docum.afin_tipo);
                            cmd.Parameters.AddWithValue("@FusionarDocumento", docum.afin_doc);

                            // Ejecutar el comando de inserción
                            cmd.ExecuteNonQuery();

                            contador++; // Incrementar contador
                            Console.WriteLine($"Documento insertado #{contador}"); // Mostrar en consola
                        }

                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error al insertar cliente con documento: {ex.InnerException}");
                        }
                    }
                    // Confirmar la transacción
                    tx.Commit();
                }
                conn.Close();
            }
            Console.WriteLine("Datos insertados correctamente.");
        }


        public static void InsertarIncapacidadesNomina(List<t_inc_desc> inc_desc_list)
        {
            int contador = 0; // Contador

            using (var conn = new NpgsqlConnection("Host=10.141.10.10:9088;Username=postgres;Password=#756913%;Database=mercacentro"))
            {
                conn.Open();
                //Iniciar transaccion
                using (var tx = conn.BeginTransaction())
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.Transaction = tx;

                    foreach (var inc_desc in inc_desc_list)
                    {
                        try
                        {
                            cmd.Parameters.Clear(); // ← CRUCIA

                            // Comando SQL con parámetros
                            cmd.CommandText = $@"INSERT INTO public.""IncapacidadesNomina""(periodo, tipo, descuentodiasarp, entidadid, empleadoid,
                                    fechainicio, fechafinal, valorincapacidadsalud, valorincapacidadriesgos, valorlicenciamaternidad,
                                    valorincapacidadsaludautorizacion, valorincapacidadriesgosautorizacion, valorlicenciamaternidadautorizacion,
                                    aportespagadosaotrossubsistemas) VALUES (@periodo,@tipo,@descuentodiasarp,
                                    (SELECT DISTINCT id FROM ""EntidadesNomina"" WHERE codigo = @entidadid LIMIT 1),
                                    (SELECT DISTINCT ""Id"" FROM ""Empleados"" WHERE ""CodigoEmpleado"" = @empleadoid LIMIT 1),
                                    @fechainicio,@fechafinal,@valorincapacidadsalud,@valorincapacidadriesgos,@valorlicenciamaternidad,
                                    @valorincapacidadsaludautorizacion,@valorincapacidadriesgosautorizacion,@valorlicenciamaternidadautorizacion,
                                    @aportespagadosaotrossubsistemas)";


                            cmd.Parameters.AddWithValue("@periodo", inc_desc.periodo);
                            cmd.Parameters.AddWithValue("@tipo", inc_desc.entidad);
                            cmd.Parameters.AddWithValue("@descuentodiasarp", inc_desc.solo_arp == 1 ? true : false);
                            cmd.Parameters.AddWithValue("@entidadid", inc_desc.cod_ent);
                            cmd.Parameters.AddWithValue("@empleadoid", inc_desc.empleado);
                            cmd.Parameters.AddWithValue("@fechainicio", inc_desc.fecha_ini);
                            cmd.Parameters.AddWithValue("@fechafinal", inc_desc.fecha_fin);
                            cmd.Parameters.AddWithValue("@valorincapacidadsalud", inc_desc.vr_inc_sal);
                            cmd.Parameters.AddWithValue("@valorincapacidadriesgos", inc_desc.vr_inc_arp);
                            cmd.Parameters.AddWithValue("@valorlicenciamaternidad", inc_desc.vr_inc_mat);
                            cmd.Parameters.AddWithValue("@valorincapacidadsaludautorizacion", inc_desc.au_inc_sal != "" ? int.Parse(inc_desc.au_inc_sal) : DBNull.Value);
                            cmd.Parameters.AddWithValue("@valorincapacidadriesgosautorizacion", inc_desc.au_inc_arp != "" ? int.Parse(inc_desc.au_inc_arp) : DBNull.Value);
                            cmd.Parameters.AddWithValue("@valorlicenciamaternidadautorizacion", inc_desc.au_inc_mat != "" ? int.Parse(inc_desc.au_inc_mat) : DBNull.Value);
                            cmd.Parameters.AddWithValue("@aportespagadosaotrossubsistemas", inc_desc.apor_pag);


                            // Ejecutar el comando de inserción
                            cmd.ExecuteNonQuery();

                            contador++; // Incrementar contador
                            Console.WriteLine($"Documento insertado #{contador}"); // Mostrar en consola
                        }

                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error al insertar cliente con documento: {ex.InnerException}");
                            //conn.Close();
                        }
                    }
                    // Confirmar la transacción
                    tx.Commit();
                }
                conn.Close();
            }
            Console.WriteLine("Datos insertados correctamente.");
        }



        public static string GetInterpolatedQuery(NpgsqlCommand cmd)
        {
            string query = cmd.CommandText;

            foreach (NpgsqlParameter param in cmd.Parameters)
            {
                string paramName = param.ParameterName;
                object value = param.Value;

                string formattedValue;

                if (value == null || value == DBNull.Value)
                {
                    formattedValue = "NULL";
                }
                else if (value is string strVal)
                {
                    formattedValue = $"'{strVal.Replace("'", "''")}'";
                }
                else if (value is DateTime dtVal)
                {
                    formattedValue = $"'{dtVal:yyyy-MM-dd HH:mm:ss}'";
                }
                else if (value is bool boolVal)
                {
                    formattedValue = boolVal ? "TRUE" : "FALSE";
                }
                else
                {
                    formattedValue = value.ToString();
                }

                // Reemplazar solo el nombre del parámetro exacto (evitar que @a reemplace dentro de @anexo, por ejemplo)
                query = Regex.Replace(query, $@"(?<!\w){Regex.Escape(paramName)}(?!\w)", formattedValue);
            }

            return query;
        }


    }       
}
