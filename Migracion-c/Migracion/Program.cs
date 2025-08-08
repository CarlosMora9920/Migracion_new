using System;
using System.Data.OleDb;
using System.Diagnostics;
using System.IO;
using System.Numerics;
using System.Text;
using DotNetDBF;
using Migracion;
using Migracion.Clases;
using Npgsql;
using static System.Runtime.InteropServices.JavaScript.JSType;

class Program
{
    // 12-05-2025 Generado por : CarlosM
    static void Main(string[] args)
    {

        
        //Variables que contiene las rutas de las tablas de FoxPro
        string dbfFilePath        = @"C:\tmp_archivos\vended.dbf";
        string dbfFileCliente     = @"C:\tmp_archivos\anexosAa.dbf";
        string dbfFileCiudad      = @"C:\tmp_archivos\ciudad.DBF";
        string dbfFileHistoNove   = @"C:\tmp_archivos\histonove.DBF";
        string dbfFileHistonve_P  = @"C:\tmp_archivos\histonove_p.dbf";
        string dbfFileHistonove_S = @"c:\tmp_archivos\histonove_s.dbf";
        string dbfFileDocum       = @"j:\businsas\mersas\datos\docum.dbf";
        string dbfFileitems       = @"c:\tmp_archivos\items.dbf";
        string dbfFenalpag        = @"c:\tmp_archivos\fenalpag.dbf";
        string dbfMovnom          = @"c:\tmp_archivos\movnom.dbf";
        string dbfPlanDet         = @"C:\tmp_archivos\plan_det.dbf";
        string dbfDcom            = @"C:\tmp_archivos\dcom.DBF";
        string dbfFCreditos       = @"C:\tmp_archivos\creditos.DBF";
        string dbfCabFact         = @"C:\tmp_archivos\movd.DBF";
        string dbfDetFact         = @"C:\tmp_archivos\movp.DBF";
        string dbfFileinc_desc    = @"c:\tmp_archivos\migracion\inc_desc.dbf";//copia de J:
        string dbfTercero         = @"C:\tmp_archivos\anexosbb.dbf";

        // Son listas que se generar para almacenar todos los datos de las
        // tablas del FoxPro
        var vendedList    = new List<t_vended>();
        var Clien_list    = new List<t_cliente>();
        var ciudade_lis   = new List<t_ciudad>();
        var nove_list     = new List<t_histonove>();
        var nove_pen      = new List<t_histonove>();
        var nove_sal      = new List<t_histonove>();
        var items_list    = new List<t_items>();
        var docum_list    = new List<t_docum>();
        var fenal_list    = new List<t_fenalpag>();
        var movnomList    = new List<t_movnom>();
        var PlanDetList   = new List<t_planDet>();
        var DcomList      = new List<t_dcom>();
        var creditosList  = new List<t_fenalCredtos>();
        var cabfactList   = new List<t_cabFact>();
        var detfactList   = new List<t_detFact>();
        var inc_desc_list = new List<t_inc_desc>();
        var tercero_list  = new List<t_terceros>();

        //Instanciando la clase proceso
        Procesos pro = new Procesos();

        //Provider
        System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

        // Solicitando la clase a procesar
        Console.WriteLine("Por favor seleccionar tabla:");
        int xTabla = int.Parse(Console.ReadLine());

        
        switch(xTabla)
        {
            case 1:  // using de vended

                using (FileStream fs = File.OpenRead(dbfFilePath))
                {
                    var reader = new DBFReader(fs);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var vendedor = new t_vended
                        {
                            Vended = record[0]?.ToString().Trim(),
                            Nombre = record[1]?.ToString().Trim(),
                            Cedula = Convert.ToInt32(record[2]),
                            Tel = record[3]?.ToString().Trim(),
                            Direcc = record[4]?.ToString().Trim(),
                            Ciudad = record[5]?.ToString().Trim(),
                            FechIng = Convert.ToDateTime(record[6]),
                            Ccosto = record[7]?.ToString().Trim()
                        };

                        vendedList.Add(vendedor);
                    }
                }

                Procesos.InsertarVendedores(vendedList);

                break;
            case 2:  //using clientes y el de ciudad

                using (FileStream fs = File.OpenRead(dbfFileCliente))
                {
                    var reader = new DBFReader(fs);
                    reader.CharEncoding = System.Text.Encoding.GetEncoding(1252); // ANSI típico de VFP

                    object[] record;
                    int fila = 0;
                    while (true)
                    {
                        try
                        {
                            record = reader.NextRecord();
                            if (record == null) break;

                            fila++;

                            var cliente = new t_cliente
                            {
                                tdoc = record[0]?.ToString()?.Trim(),
                                anexo = record[1]?.ToString()?.Trim(),
                                nombre = record[2]?.ToString()?.Trim(),
                                dv = record[4]?.ToString()?.Trim(),
                                direcc = record[5]?.ToString()?.Trim(),
                                emailfe1 = record[144]?.ToString()?.Trim(),
                                tel = record[9]?.ToString()?.Trim(),
                                apl1 = record[83]?.ToString()?.Trim(),
                                apl2 = record[84]?.ToString()?.Trim(),
                                nom1 = record[85]?.ToString()?.Trim(),
                                nom2 = record[86]?.ToString()?.Trim(),
                                Dane = record[74]?.ToString()?.Trim(),
                                tipo_per = record[109]?.ToString()?.Trim(),
                                bloqueado = record[12] != null && record[12].ToString() == "1" ? 1 : 0
                            };

                            Clien_list.Add(cliente);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"⚠️ Error en fila {fila}: {ex.Message}");
                            continue; // o break si no quieres continuar
                        }
                    }
                }

                using (FileStream fc = File.OpenRead(dbfFileCiudad))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var ciudad = new t_ciudad
                        {
                            Municipio = record[1]?.ToString()?.Trim(),
                            Departamento = record[2]?.ToString()?.Trim(),
                            Dane = record[3]?.ToString()?.Trim()
                        };

                        ciudade_lis.Add(ciudad);

                    }
                }

                Procesos.InsertarClientes(Clien_list, ciudade_lis);

                break;
            case 3: // cambioEps

                using (FileStream fc = File.OpenRead(dbfFileHistoNove))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var histonove = new t_histonove
                        {
                            cod_ant = record[4]?.ToString()?.Trim(),
                            cod_act = record[5]?.ToString()?.Trim(),
                            usua_reg = record[9]?.ToString()?.Trim(),
                            fech_reg = Convert.ToDateTime(record[7]),
                            hora_reg = TimeOnly.Parse(record[8].ToString()),
                            empleado = record[0]?.ToString()?.Trim(),
                            fech_camb = Convert.ToDateTime(record[6]),
                        };

                        nove_list.Add(histonove);

                    }
                }

                Procesos.InsertarHistoNove(nove_list);

                break;
            case 4:  // cambioPension

                using (FileStream fc = File.OpenRead(dbfFileHistonve_P))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var histonoveP = new t_histonove
                        {
                            cod_ant = record[4]?.ToString()?.Trim(),
                            cod_act = record[5]?.ToString()?.Trim(),
                            usua_reg = record[9]?.ToString()?.Trim(),
                            fech_reg = Convert.ToDateTime(record[7]),
                            hora_reg = TimeOnly.Parse(record[8].ToString()),
                            empleado = record[0]?.ToString()?.Trim(),
                            fech_camb = Convert.ToDateTime(record[6]),
                        };

                        nove_list.Add(histonoveP);

                    }
                }

                Procesos.InsertarHistonoveP(nove_list);


                break;
            case 5:

                using (FileStream fc = File.OpenRead(dbfFileHistonove_S))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var histonoveP = new t_histonove
                        {
                            val_ant = Convert.ToInt32(record[2]),
                            val_act = Convert.ToInt32(record[3]),
                            usua_reg = record[9]?.ToString()?.Trim(),
                            fech_reg = Convert.ToDateTime(record[7]),
                            hora_reg = TimeOnly.Parse(record[8].ToString()),
                            empleado = record[0]?.ToString()?.Trim(),
                            fech_camb = Convert.ToDateTime(record[6]),
                        };

                        nove_sal.Add(histonoveP);

                    }
                }

                Procesos.InsertarHistonoveS(nove_sal);

                break;
            case 6://documentos

                using (FileStream fc = File.OpenRead(dbfFileDocum))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var documentos = new t_docum
                        {
                            docum = record[0]?.ToString()?.Trim(),   //codigo
                            nombre = record[1]?.ToString()?.Trim(),  //nombre
                            tipo_doc = Convert.ToInt32(record[4]),  //asimilar
                            contabil = Convert.ToInt32(record[18]),   //CD
                            si_cnomb = Convert.ToInt32(record[40]),   //CT
                            bloqueado = Convert.ToInt32(record[36]),  //IN
                            vali_doc = Convert.ToInt32(record[83]),   //VD
                            si_consec = Convert.ToInt32(record[105]),  //UV
                            controlrut = Convert.ToInt32(record[120]), //CR
                            camb_ter = Convert.ToInt32(record[137]),   //PC
                            desc_ord = Convert.ToInt32(record[138]),   //DO
                            es_trans = Convert.ToInt32(record[143]),   //TR
                            cons_proc = Convert.ToInt32(record[144]),  //CA
                            desc_doci = Convert.ToInt32(record[145]),  //DD
                            silibtes = Convert.ToInt32(record[147]),   //LT
                            n_lineas = Convert.ToInt32(record[2]),   //NL, esta campo para nosotros es numerico, ellos lo tienen boleano
                            n_recup = Convert.ToInt32(record[156]),    //RD
                            obser_doc = Convert.ToInt32(record[178]),  //RO
                            cont_fec = Convert.ToInt32(record[48]),   //ControlFechas
                            vend_det = Convert.ToInt32(record[28]),   //Vendedor
                            zon_det = Convert.ToInt32(record[29]),    //Zona
                            cco_det = Convert.ToInt32(record[30]),    //CCosto
                            es_resolu = Convert.ToInt32(record[149]),  //Resolucion
                            sniif_on = Convert.ToInt32(record[166]),   //ActivarColumna
                            si_contpag = Convert.ToInt32(record[167]), //ControlaPagos
                            fecha_cre = Convert.ToDateTime(record[19]),//FechaCreacion
                            Mensaje1 = record[31]?.ToString()?.Trim(),//Mensaje1
                            Mensaje2 = record[32]?.ToString()?.Trim(),//Mensaje2
                            Mensaje3 = record[33]?.ToString()?.Trim(),//Mensaje3
                            afin_cxc = Convert.ToInt32(record[129]),   //ValoresCartera
                            Anexo1 = record[22]?.ToString()?.Trim(),  //Anexo1
                            Anexo2 = record[23]?.ToString()?.Trim(),  //Anexo2
                            Anexo3 = record[24]?.ToString()?.Trim(),  //Anexo3
                            Anexo4 = record[25]?.ToString()?.Trim(),  //Anexo4
                            Anexo5 = record[26]?.ToString()?.Trim(),  //Anexo5
                            Anexo6 = record[27]?.ToString()?.Trim(),  //Anexo6
                            afin_tipo = Convert.ToInt32(record[132]),  //MovimientoCartera
                            afin_doc = record[131]?.ToString()?.Trim(),//FusionarDocumento
                        };
                        docum_list.Add(documentos);
                    }
                }
                Procesos.InsertarDocumentos(docum_list);


                break;


            case 7: // items

                using (FileStream fc = File.OpenRead(dbfFileitems))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var itemsPick = new t_items
                        {
                            Codigo = record[0]?.ToString()?.Trim(),
                            tipo   = record[12]?.ToString()?.Trim(),
                            fecha_cre = Convert.ToDateTime(record[11]),
                            nombre = record[1]?.ToString()?.Trim(),
                            shortname = record[181]?.ToString()?.Trim(),
                            refabrica = record[86]?.ToString()?.Trim(),
                            peso_uni = Convert.ToInt32(record[61]),
                            undxcaja = Convert.ToInt32(record[57]),
                            subgrupo = record[58]?.ToString()?.Trim(),
                            marca = record[42]?.ToString()?.Trim(),
                            pdrenado = Convert.ToInt32(record[160]),
                            cod_ean8 = record[42]?.ToString()?.Trim(),
                            cod_bar = record[43]?.ToString()?.Trim(),
                            bloqueado = Convert.ToInt32(record[18]),
                            unidad = record[19]?.ToString()?.Trim(),
                            talla = record[87]?.ToString()?.Trim(),
                            linea = record[4]?.ToString()?.Trim(),
                            sublinea = record[5]?.ToString()?.Trim(),
                            no_compra = Convert.ToInt32(record[121]),
                            es_kitpro = Convert.ToInt32(record[138]),
                            iva = Convert.ToInt32(record[6]),
                            pvta1i = Convert.ToInt32(record[7]),
                            pvta_a1 = Convert.ToInt32(record[20]),
                            cambiopv_1= Convert.ToInt32(record[92]),
                            iconsumo = Convert.ToInt32(record[49]),
                            excluido = Convert.ToInt32(record[44]),
                            listap = Convert.ToInt32(record[79]),
                            F_ICONSUMO = Convert.ToInt32(record[135]),
                            costoajus = Convert.ToInt32(record[127]),
                            es_fruver = Convert.ToInt32(record[83]),
                            bolsa = Convert.ToInt32(record[104]),
                            mod_ppos = Convert.ToInt32(record[47]),
                            fenalce = record[103]?.ToString()?.Trim(),
                            acu_tpos = Convert.ToInt32(record[48]),
                            subsidio = Convert.ToInt32(record[105]),
                            mod_qpos = Convert.ToInt32(record[84]),
                            es_bol = record[132]?.ToString()?.Trim(),
                            contabgrav = Convert.ToInt32(record[96]),
                            sitoledo = Convert.ToInt32(record[88]),
                            pref_ean = record[89]?.ToString()?.Trim(),
                            es_bordado = Convert.ToInt32(record[100]),
                            es_moto = Convert.ToInt32(record[122]),
                            sipedido = Convert.ToInt32(record[136]),
                            escodensa = record[133]?.ToString()?.Trim(),
                            es_ingreso = Convert.ToInt32(record[129]),
                            cheqpr = Convert.ToInt32(record[131]),
                            si_descto = Convert.ToInt32(record[36]),
                            descmax = Convert.ToInt32(record[50]),
                            deci_cant = Convert.ToInt32(record[85]),
                            fech_cp1 = Convert.ToDateTime(record[9]),
                            costo_rep = Convert.ToInt32(record[14]),
                            cod_alt = record[3]?.ToString()?.Trim(),
                            CCosto = record[33]?.ToString()?.Trim(),
                            elegido = Convert.ToInt32(record[55]),
                            sdo_rojo = Convert.ToInt32(record[60]),
                            pidempeso = Convert.ToInt32(record[140]),
                            corrosivo = Convert.ToInt32(record[143]),
                            n_cas = record[141]?.ToString()?.Trim(),
                            cat_cas = record[142]?.ToString()?.Trim(),
                            vta_costo = Convert.ToInt32(record[62]),
                            si_detdoc = Convert.ToInt32(record[69]),
                            solocant = Convert.ToInt32(record[91]),
                            si_serie = Convert.ToInt32(record[125]),
                            terceAutom = Convert.ToInt32(record[126]),
                            generico = Convert.ToInt32(record[161]),
                            pesocajb = Convert.ToInt32(record[63]),
                            pesocajn = Convert.ToInt32(record[64]),
                            peso_car = Convert.ToInt32(record[159]),
                            unidmin = Convert.ToInt32(record[65]),
                            puntaje= Convert.ToInt32(record[112]),
                            fecha1_mp = Convert.ToDateTime(record[113]),
                            fecha2_mp = Convert.ToDateTime(record[114]),
                            desc_esp = Convert.ToInt32(record[107]),
                            fact_esp = Convert.ToInt32(record[108]),
                            valor_esp = Convert.ToInt32(record[109]),
                            fechdesci = Convert.ToDateTime(record[162]),
                            fechdescf = Convert.ToDateTime(record[163]),
                            descod = Convert.ToInt32(record[110]),
                            descod_f = Convert.ToDateTime(record[111]),
                            fchdescusr = Convert.ToDateTime(record[130]),
                            validmax = Convert.ToInt32(record[145]),
                            maxventa = Convert.ToInt32(record[146]),
                            val_desp = Convert.ToInt32(record[148]),
                            f_bloqp = Convert.ToDateTime(record[156]),
                            cont_devol = Convert.ToInt32(record[116]),
                            pvta3i = Convert.ToInt32(record[52]),
                            no_invped = Convert.ToInt32(record[107]),
                            aut_trasl = Convert.ToInt32(record[115]),
                            lp_cyvig = Convert.ToInt32(record[118]),
                            chequeo = Convert.ToInt32(record[106]),
                            costeo2 = Convert.ToInt32(record[119]),
                            sobrestock = Convert.ToInt32(record[123]),
                            Asopanela = Convert.ToInt32(record[124]),
                            grupodes = record[82]?.ToString()?.Trim(),
                            ean_1 = record[80]?.ToString()?.Trim(),
                            ean_2 = record[81]?.ToString()?.Trim(),
                            inandout = Convert.ToInt32(record[155]),
                            domi_com = Convert.ToInt32(record[107]),
                            ord_prio = Convert.ToInt32(record[164]),
                            modq_reg = Convert.ToInt32(record[178]),
                            mod_toler = Convert.ToInt32(record[179]),
                            stockdomi = Convert.ToInt32(record[169]),
                            ext_covid = Convert.ToInt32(record[173]),
                            unidcantfe = record[170]?.ToString()?.Trim(),
                            tipobiend = record[176]?.ToString()?.Trim(),
                            pdiasiva = Convert.ToInt32(record[177]),
                            es_dfnfp = Convert.ToInt32(record[149]),
                            varifnfp = Convert.ToInt32(record[150]),
                            DESFINNIIF = Convert.ToInt32(record[147]),
                            bod_asoc = record[39]?.ToString()?.Trim(),
                            pvtali = Convert.ToInt32(record[7]),
                            descuento = Convert.ToInt32(record[139]),
                            por_rentab = Convert.ToInt32(record[193]),
                            confirpre = Convert.ToInt32(record[137]),
                            ofertado = Convert.ToInt32(record[171]),
                            fech1comp = Convert.ToDateTime(record[144]),
                            vr_imps = Convert.ToInt32(record[196]),
                            cod_ref = record[90]?.ToString()?.Trim(),
                            refer = record[2]?.ToString()?.Trim(),


                        };

                            items_list.Add(itemsPick);

                    }
                }

                Procesos.InsertarProductos(items_list);
                break;
                //en el aprendizaje se aprende. cpd
            case 8: //bono devolu catrelina
                 
                break;

            case 9:   //fenalpag

                using (FileStream fc = File.OpenRead(dbfFenalpag))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var fenalpag = new t_fenalpag
                        {
                            Tdoc = record[1]?.ToString()?.Trim(),
                            Tipo_per = record[2]?.ToString()?.Trim(),
                            Nom1 = record[3]?.ToString()?.Trim(),
                            Nom2 = record[4]?.ToString()?.Trim(),
                            Apl1 = record[5]?.ToString()?.Trim(),
                            Apl2 = record[6]?.ToString()?.Trim(),
                            Sexo = record[7]?.ToString()?.Trim(),
                            fech_nac = Convert.ToDateTime(record[8]),
                            Direcc = record[9]?.ToString()?.Trim(),
                            Ciudad = record[10]?.ToString()?.Trim(),
                            Tel = record[12]?.ToString()?.Trim(),
                            Emailfe1 = record[13]?.ToString()?.Trim(),
                            Anexo = Convert.ToInt32(record[0]),
                            Usuario = record[34]?.ToString()?.Trim(),
                            Usado = Convert.ToInt32(record[39]),
                            Nombre = record[11]?.ToString()?.Trim(),
                            Cod_sis = record[60]?.ToString()?.Trim(),
                            Autoriz = record[37]?.ToString()?.Trim(),

                        };

                        fenal_list.Add(fenalpag);

                    }
                }

                Procesos.InsertarFenalpag(fenal_list);

                break;
            case 10:

                using (FileStream fc = File.OpenRead(dbfMovnom))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var movnom = new t_movnom
                        {
                            
                            Empleado  = record[4]?.ToString()?.Trim(),
                            Cedula    = record[5]?.ToString()?.Trim(),
                            Apellido1 = record[9]?.ToString()?.Trim(),
                            Apellido2 = record[10]?.ToString()?.Trim(),
                            Nombre1   = record[7]?.ToString()?.Trim(),
                            Nombre2   = record[8]?.ToString()?.Trim(),
                            Concepto  = record[18]?.ToString()?.Trim(),
                            Periodo   = record[31]?.ToString()?.Trim(),
                            fecha     = Convert.ToDateTime(record[28]),
                            Devengado = Convert.ToInt32(record[23]),
                            Descuento = Convert.ToInt32(record[24]),

                        };

                        movnomList.Add(movnom);

                    }
                }

                Procesos.InsertarMovnom(movnomList);


                break;

            case 11:

                using (FileStream fc = File.OpenRead(dbfPlanDet))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var planDet = new t_planDet
                        {
                           
                            Fecha          = DateOnly.FromDateTime(Convert.ToDateTime(record[3].ToString())),
                            Empleado       = record[2]?.ToString()?.Trim(),
                            FechaCreacion  = Convert.ToDateTime(record[4]),
                            Usa_crea       = record[5]?.ToString()?.Trim(),
                            NumeroPlanilla = Convert.ToInt32(record[0]),
                        };

                        PlanDetList.Add(planDet);

                    }
                }

                Procesos.InsertarPlanDet(PlanDetList);


                break;

            case 12:

                using (FileStream fc = File.OpenRead(dbfDcom))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var Sdcom = new t_dcom
                        {

                            ordenedecompra = record[0]?.ToString()?.Trim(),
                            producto = record[5]?.ToString()?.Trim(),
                            cantidad= Convert.ToInt32(record[19]),  
                            valor = Convert.ToInt32(record[18]),                            
                            fechahoramovimiento = Convert.ToDateTime(record[26]),
                            descuento = Convert.ToInt32(record[35]),
                            sugerido = Convert.ToInt32(record[37]),
                            requerido = Convert.ToInt32(record[36]),
                            bonificacion = Convert.ToInt32(record[31]),
                            observacion = record[24]?.ToString()?.Trim(),
                            sucursal = record[1]?.ToString()?.Trim(),
                            diasinventario = Convert.ToInt32(record[17]),
                            iva = Convert.ToInt32(record[21]),
                            impuestoconsumo = Convert.ToInt32(record[55]),
                            talla = record[57]?.ToString()?.Trim(),
                            nombreproducto = record[6]?.ToString()?.Trim(),
                            subgruposproducto = record[7]?.ToString()?.Trim(),
                            marcasproducto = record[8]?.ToString()?.Trim(),
                            tipoProducto = record[9]?.ToString()?.Trim(),
                            valoriva = Convert.ToInt32(record[41]),
                            valorimpoconsumo = Convert.ToInt32(record[56]),
                            ean8 = record[10]?.ToString()?.Trim(),
                            ean13 = record[11]?.ToString()?.Trim(),
                            codigoreferencia = record[12]?.ToString()?.Trim(),
                            
                        };

                        DcomList.Add(Sdcom);

                    }
                }

                Procesos.InsertarDcom(DcomList);

                break;
            case 13:

                using (FileStream fc = File.OpenRead(dbfFCreditos))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var sCreditos = new t_fenalCredtos
                        {

                            valormercanciacliente = Convert.ToInt32(record[30]),
                            cantidadcuotascliente = Convert.ToInt32(record[17]),
                            fenalpagid = Convert.ToInt32(record[0]),
                            tasainteres = Convert.ToInt32(record[16]),
                            iva = Convert.ToInt32(record[24]),
                            porcentajecuotainicial = Convert.ToInt32(record[19]),
                            numerocuotas = Convert.ToInt32(record[17]),
                            idformapago = record[48]?.ToString()?.Trim(),
                            porcentajeaval = Convert.ToInt32(record[55]),
                            valorcuotainicialcliente = Convert.ToInt32(record[18]),
                            porcentajecuotainicialcliente = Convert.ToInt32(record[19]),
                            periodicidad = Convert.ToInt32(record[38]),


                        };

                        creditosList.Add(sCreditos);

                    }
                }

                Procesos.InsertarCreditos(creditosList);

                break;

            case 14:

                int recordIndex = 0;
                using (FileStream fc = File.OpenRead(dbfCabFact))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {


                        try
                        {
                            var sCabFact = new t_cabFact
                            {

                                fechahoramovimiento = Convert.ToDateTime(record[1]),
                                sucursalid = record[0]?.ToString()?.Trim(),
                                cajacodigo = record[11]?.ToString()?.Trim(),
                                usuarioid = record[17]?.ToString()?.Trim(),
                                usuarionombre = record[18]?.ToString()?.Trim(),
                                terceroid = long.TryParse(record[7]?.ToString(), out var temp) ? temp : 0,
                                nombretercero = record[8]?.ToString()?.Trim(),
                                tarjetamercapesosid = Convert.ToInt32(record[48]),
                                tarjetamercapesoscodigo = record[55]?.ToString()?.Trim(),
                                documentofactura = record[2]?.ToString()?.Trim(),
                                totalventa = Convert.ToInt32(record[102]),
                                valorcambio = Convert.ToInt32(record[13]),
                                numerofactura = Convert.ToInt32(record[4]),
                                documentoid = record[2]?.ToString()?.Trim(),
                                documentonombre = record[3]?.ToString()?.Trim(),
                                identificacion = long.TryParse(record[7]?.ToString(), out var temp2) ? temp2 : 0,
                                tipopagoid    = Convert.ToInt32(record[104]),


                            };

                            cabfactList.Add(sCabFact);
                        }
                        catch (Exception ex)
                        {


                        }
                        
                        
                        

                        //creditosList.Add(sCabFact);

                    }
                }

                Procesos.InsertarCajasMovimientos(cabfactList);
                break;
            case 15:

                using (FileStream fc = File.OpenRead(dbfDetFact))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var sDetFact = new t_detFact
                        {
                         
                            cajasmovimientosid = Convert.ToInt32(record[3]),
                            productoid = record[4]?.ToString()?.Trim(),
                            productonombre = record[5]?.ToString()?.Trim(),
                            productovalorsiniva = Convert.ToInt32(record[14]),
                            productoporcentajeiva = Convert.ToInt32(record[16]),
                            productovaloriva = Convert.ToInt32(record[15]),
                            productoexento = Convert.ToInt32(record[34]),
                            promocionid = record[35]?.ToString()?.Trim(),
                            promocionnombre = record[65]?.ToString()?.Trim(),
                            promocionvalordscto = Convert.ToInt32(record[39]),
                            productovalorimptoconsumo = Convert.ToInt32(record[36]),
                            vendedorid = record[8]?.ToString()?.Trim(),
                            valordescuentogeneral = Convert.ToInt32(record[39]),
                            totalitem = Convert.ToInt32(record[33]),
                            productoidmarca = record[23]?.ToString()?.Trim(),
                            productoidlinea = record[20]?.ToString()?.Trim(),
                            productoidsublinea = record[21]?.ToString()?.Trim(),
                            //public int valoracumulartarejetamercapesos { get; set; }
                            cantidaddelproducto = Convert.ToInt32(record[9]),
                            productovalorantesdscto = Convert.ToInt32(record[38]),
                            productoporcentajedscto = Convert.ToInt32(record[66]),
                            productovalordescuento = Convert.ToInt32(record[39]),
                            factorimptoconsumo = Convert.ToInt32(record[41]),
                            productonombremarca = record[67]?.ToString()?.Trim(),

                        };



                        detfactList.Add(sDetFact);

                    }
                }

                Procesos.InsertarCajasDetalles(detfactList);


                break;

            case 16:

                using (FileStream fc = File.OpenRead(dbfFileinc_desc))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;
                    var codigosIgnorados = new List<string> { "R1", ".02", ".R0", "S0", "R0", "R06", "R23", "R36", "R43", "R53", "S01", ".S0" };

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        // Validar que cod_ent (record[2]) y empleado (record[9]) no estén vacíos
                        string codent = record[2]?.ToString()?.Trim();
                        string empleado = record[9]?.ToString()?.Trim();

                        if (string.IsNullOrWhiteSpace(codent) || string.IsNullOrWhiteSpace(empleado))
                        {
                            continue; // Saltar si están vacíos
                        }

                        if (codigosIgnorados.Contains(codent))
                        {
                            continue; // Salta esta iteración si el código está en la lista
                        }

                        // Validar fechas
                        if (!DateTime.TryParse(record[11]?.ToString(), out DateTime fechaIni))
                        {
                            continue; // Saltar si la fecha inicial no es válida
                        }

                        if (!DateTime.TryParse(record[12]?.ToString(), out DateTime fechaFin))
                        {
                            continue; // Saltar si la fecha final no es válida
                        }

                        var IncapaNomina = new t_inc_desc
                        {
                            periodo = record[0]?.ToString()?.Trim(), //periodo
                            entidad = Convert.ToInt32(record[1]),//tipo
                            solo_arp = Convert.ToInt32(record[13]),//descuentodiasarp
                            cod_ent = codent, //entidadid
                            empleado = empleado, //empleadoid
                            fecha_ini = fechaIni,//fechainicio
                            fecha_fin = fechaFin,//fechafinal
                            vr_inc_sal = Convert.ToInt32(record[3]), //valorincapacidadsalud
                            vr_inc_arp = Convert.ToInt32(record[4]),//valorincapacidadriesgos
                            vr_inc_mat = Convert.ToInt32(record[5]), //valorlicenciamaternidad
                            au_inc_sal = record[6]?.ToString()?.Trim(), //valorincapacidadsaludautorizacion
                            au_inc_arp = record[7]?.ToString()?.Trim(),//valorincapacidadriesgosautorizacion
                            au_inc_mat = record[8]?.ToString()?.Trim(), //valorlicenciamaternidadautorizacion
                            apor_pag = Convert.ToInt32(record[10]), //aportespagadosaotrossubsistemas
                        };
                        inc_desc_list.Add(IncapaNomina);
                    }
                }
                Procesos.InsertarIncapacidadesNomina(inc_desc_list);


                break;

            case 17:

                //using (FileStream fc = File.OpenRead(dbfTercero))
                //{
                //    var reader = new DBFReader(fc);
                //    reader.CharEncoding = System.Text.Encoding.UTF8;

                //    object[] record;
                //    while ((record = reader.NextRecord()) != null)
                //    {
                //        var sTerceros = new t_terceros
                //        {

                //            TipoPersona                  = record[109]?.ToString()?.Trim(),
                //            IdTipoIdentificacion         = Convert.ToInt32(record[82]),
                //            Identificacion               = long.TryParse(record[1]?.ToString(), out var temp) ? temp : 0,
                //            Nombre1                      = record[85]?.ToString()?.Trim(),
                //            Nombre2                      = record[86]?.ToString()?.Trim(),
                //            Apellido1                    = record[83]?.ToString()?.Trim(),
                //            Apellido2                    = record[841]?.ToString()?.Trim(),
                //            Genero                       = record[110]?.ToString()?.Trim(),
                //            FechaNacimiento              = Convert.ToDateTime(record[111]),
                //            RazonSocial                  = record[2]?.ToString()?.Trim(),
                //            NombreComercial              = record[58]?.ToString()?.Trim(),
                //            Direccion                    = record[5]?.ToString()?.Trim(),
                //            Email                        = record[87]?.ToString()?.Trim(),
                //            Email2                       = record[144]?.ToString()?.Trim(),
                //            IdDepartamento               = Convert.ToInt32(record[3]),
                //            IdMunicipio                  = Convert.ToInt32(record[3]),
                //            Telefono1                    = record[7]?.ToString()?.Trim(),
                //            Telefono2                    = record[7]?.ToString()?.Trim(),
                //            Estado                       = Convert.ToInt32(record[12]),
                //            EsCliente                    = Convert.ToInt32(record[13]),
                //            EsEmpleado                   = Convert.ToInt32(record[91]),
                //            EsPasante                    = Convert.ToInt32(record[93]),
                //            EsProveedor                  = Convert.ToInt32(record[22]),
                //            FechaCreacion               = Convert.ToDateTime(record[11]),
                //            FechaActualizacion           = Convert.ToDateTime(record[112]),
                //            DiasCredito                  = Convert.ToInt32(record[21]),
                //            EsProveedorFruver            = Convert.ToInt32(record[88]),
                //            EsProveedorBolsaAgropecuaria = Convert.ToInt32(record[89]),
                //            EsProveedorCampesinoDirecto  = Convert.ToInt32(record[90]),
                //            EsProveedorRestaurante       = Convert.ToInt32(record[97]),
                //            EsProveedorPanaderia         = Convert.ToInt32(record[98]),
                //            EsOtroTipo                   = Convert.ToInt32(record[92]),
                //            EsGasto                      = Convert.ToInt32(record[94]),
                //            CotizaEPS                    = Convert.ToInt32(record[127]),
                //            CotizaFondoPension           = Convert.ToInt32(record[128]),
                //            CotizaARP                    = Convert.ToInt32(record[129]),
                //            TarifaARP                    = Convert.ToInt32(record[130]),
                //            RegimenSimplificado          = Convert.ToInt32(record[95]),
                //            NoPracticarRetFuente         = Convert.ToInt32(record[63]),
                //            NoPracticarRetIVA            = Convert.ToInt32(record[64]),
                //            Autorretenedor               = Convert.ToInt32(record[24]),
                //            EsRetenedorFuente            = Convert.ToInt32(record[62]),
                //            DescontarAsohofrucol         = Convert.ToInt32(record[60]),
                //            AsumirImpuestos              = Convert.ToInt32(record[70]),
                //            RetenerFenalce               = Convert.ToInt32(record[71]),
                //            AsumirFenalce                = Convert.ToInt32(record[72]),
                //            BolsaAgropecuaria            = Convert.ToInt32(record[73]),
                //            RegimenComun                 = Convert.ToInt32(record[28]),
                //            RetenerSiempre               = Convert.ToInt32(record[25]),
                //            GranContribuyente            = Convert.ToInt32(record[26]),
                //            AutorretenedorIVA            = Convert.ToInt32(record[27]),
                //            IdCxPagar                    = Convert.ToInt32(record[50]),
                //            DeclaranteRenta              = Convert.ToInt32(record[116]),
                //            DescuentoNIIF                = Convert.ToInt32(record[132]),
                //            DescontarFNFP                = Convert.ToInt32(record[133]),
                //            ManejaIVAProductoBonificado  = Convert.ToInt32(record[143]),
                //            ReteIVALey560_2020           = Convert.ToInt32(record[51]),
                //            RegimenSimpleTributacion     = Convert.ToInt32(record[151]),
                //            TipoDescuentoFinanciero      = Convert.ToInt32(record[136]),
                //            Porcentaje1                  = Convert.ToInt32(record[138]),
                //            Porcentaje2                  = Convert.ToInt32(record[139]),
                //            Porcentaje3                  = Convert.ToInt32(record[140]),
                //            Porcentaje4                  = Convert.ToInt32(record[141]),
                //            Porcentaje5                  = Convert.ToInt32(record[142]),
                //            IdICATerceroCiudad           = Convert.ToInt32(record[135]),
                //            EstadoRUT                    = Convert.ToInt32(record[77]),
                //            FechaRut                     = Convert.ToDateTime(record[78]),
                //            IdCxCobrar                   = Convert.ToInt32(record[49]),
                //            DigitoVerificacion           = record[4]?.ToString()?.Trim(),
                //            IdEmpleado                   = Convert.ToInt32(record[1]),
                //            BaseDecreciente              = Convert.ToInt32(record[137]),
                //            IdResponsabilidadesFiscales  = record[165]?.ToString()?.Trim(),   
                //            IdResponsabilidadesTributarias = record[167]?.ToString()?.Trim(), 
                //            IdUbicacionDANE              =  record[74]?.ToString()?.Trim(),
                //            CodigoPostal                 = record[164]?.ToString()?.Trim(),


                //        };

                //        tercero_list.Add(sTerceros);

                //    }
                //}


                using (FileStream fc = File.OpenRead(dbfTercero))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var value = GetValueOrDefault<string>(record, 1, "0"); // Obtener como string

                        var sTerceros = new t_terceros
                        {

                            
                            
                            TipoPersona = GetValueOrDefault<string>(record, 109, null),
                            IdTipoIdentificacion = Convert.ToInt32(record[82]),
                            Identificacion = long.TryParse(record[1]?.ToString(), out var temp) ? temp : 0,
                            Nombre1 = GetValueOrDefault<string>(record, 85, null),
                            Nombre2 = GetValueOrDefault<string>(record, 86, null),
                            Apellido1 = GetValueOrDefault<string>(record, 83, null),
                            Apellido2 = GetValueOrDefault<string>(record, 841, null),
                            Genero = GetValueOrDefault<string>(record, 110, null),
                            FechaNacimiento = GetValueOrDefault<DateTime>(record, 111, DateTime.MinValue),
                            RazonSocial = GetValueOrDefault<string>(record, 2, null),
                            NombreComercial = GetValueOrDefault<string>(record, 58, null),
                            Direccion = GetValueOrDefault<string>(record, 5, null),
                            Email = GetValueOrDefault<string>(record, 87, null),
                            Email2 = GetValueOrDefault<string>(record, 144, null),
                            IdDepartamento = GetValueOrDefault<int>(record, 3, 0),
                            IdMunicipio = GetValueOrDefault<int>(record, 3, 0),
                            Telefono1 = GetValueOrDefault<string>(record, 7, null),
                            Telefono2 = GetValueOrDefault<string>(record, 7, null),
                            Estado = GetValueOrDefault<int>(record, 12, 0),
                            EsCliente = GetValueOrDefault<int>(record, 13, 0),
                            EsEmpleado = GetValueOrDefault<int>(record, 91, 0),
                            EsPasante = GetValueOrDefault<int>(record, 93, 0),
                            EsProveedor = GetValueOrDefault<int>(record, 22, 0),
                            FechaCreacion = GetValueOrDefault<DateTime>(record, 11, DateTime.MinValue),
                            FechaActualizacion = GetValueOrDefault<DateTime>(record, 112, DateTime.MinValue),
                            DiasCredito = GetValueOrDefault<int>(record, 21, 0),
                            EsProveedorFruver = GetValueOrDefault<int>(record, 88, 0),
                            EsProveedorBolsaAgropecuaria = GetValueOrDefault<int>(record, 89, 0),
                            EsProveedorCampesinoDirecto = GetValueOrDefault<int>(record, 90, 0),
                            EsProveedorRestaurante = GetValueOrDefault<int>(record, 97, 0),
                            EsProveedorPanaderia = GetValueOrDefault<int>(record, 98, 0),
                            EsOtroTipo = GetValueOrDefault<int>(record, 92, 0),
                            EsGasto = GetValueOrDefault<int>(record, 94, 0),
                            CotizaEPS = GetValueOrDefault<int>(record, 127, 0),
                            CotizaFondoPension = GetValueOrDefault<int>(record, 128, 0),
                            CotizaARP = GetValueOrDefault<int>(record, 129, 0),
                            TarifaARP = GetValueOrDefault<int>(record, 130, 0),
                            RegimenSimplificado = GetValueOrDefault<int>(record, 95, 0),
                            NoPracticarRetFuente = GetValueOrDefault<int>(record, 63, 0),
                            NoPracticarRetIVA = GetValueOrDefault<int>(record, 64, 0),
                            Autorretenedor = GetValueOrDefault<int>(record, 24, 0),
                            EsRetenedorFuente = GetValueOrDefault<int>(record, 62, 0),
                            DescontarAsohofrucol = GetValueOrDefault<int>(record, 60, 0),
                            AsumirImpuestos = GetValueOrDefault<int>(record, 70, 0),
                            RetenerFenalce = GetValueOrDefault<int>(record, 71, 0),
                            AsumirFenalce = GetValueOrDefault<int>(record, 72, 0),
                            BolsaAgropecuaria = GetValueOrDefault<int>(record, 73, 0),
                            RegimenComun = GetValueOrDefault<int>(record, 28, 0),
                            RetenerSiempre = GetValueOrDefault<int>(record, 25, 0),
                            GranContribuyente = GetValueOrDefault<int>(record, 26, 0),
                            AutorretenedorIVA = GetValueOrDefault<int>(record, 27, 0),
                            IdCxPagar = GetValueOrDefault<int>(record, 50, 0),
                            DeclaranteRenta = GetValueOrDefault<int>(record, 116, 0),
                            DescuentoNIIF = GetValueOrDefault<int>(record, 132, 0),
                            DescontarFNFP = GetValueOrDefault<int>(record, 133, 0),
                            ManejaIVAProductoBonificado = GetValueOrDefault<int>(record, 143, 0),
                            ReteIVALey560_2020 = GetValueOrDefault<int>(record, 51, 0),
                            RegimenSimpleTributacion = GetValueOrDefault<int>(record, 151, 0),
                            TipoDescuentoFinanciero = GetValueOrDefault<int>(record, 136, 0),
                            Porcentaje1 = GetValueOrDefault<int>(record, 138, 0),
                            Porcentaje2 = GetValueOrDefault<int>(record, 139, 0),
                            Porcentaje3 = GetValueOrDefault<int>(record, 140, 0),
                            Porcentaje4 = GetValueOrDefault<int>(record, 141, 0),
                            Porcentaje5 = GetValueOrDefault<int>(record, 142, 0),
                            IdICATerceroCiudad = GetValueOrDefault<int>(record, 135, 0),
                            EstadoRUT = GetValueOrDefault<int>(record, 77, 0),
                            FechaRut = GetValueOrDefault<DateTime>(record, 78, DateTime.MinValue),
                            IdCxCobrar = GetValueOrDefault<int>(record, 49, 0),
                            DigitoVerificacion = GetValueOrDefault<string>(record, 4, null),
                            IdEmpleado = GetValueOrDefault<int>(record, 1, 0),
                            BaseDecreciente = GetValueOrDefault<int>(record, 137, 0),
                            IdResponsabilidadesFiscales = GetValueOrDefault<string>(record, 165, null),
                            IdResponsabilidadesTributarias = GetValueOrDefault<string>(record, 167, null),
                            IdUbicacionDANE = GetValueOrDefault<string>(record, 74, null),
                            CodigoPostal = GetValueOrDefault<string>(record, 164, null),

                        };

                        tercero_list.Add(sTerceros);
                    }
                }


                using (FileStream fc = File.OpenRead(dbfFileCiudad))
                {
                    var reader = new DBFReader(fc);
                    reader.CharEncoding = System.Text.Encoding.UTF8;

                    object[] record;
                    while ((record = reader.NextRecord()) != null)
                    {
                        var ciudad = new t_ciudad
                        {
                            Municipio = record[1]?.ToString()?.Trim(),
                            Departamento = record[2]?.ToString()?.Trim(),
                            
                        };

                        ciudade_lis.Add(ciudad);

                    }
                }



                Procesos.insertarTerceros(tercero_list, ciudade_lis);

                break;
        }
        
        
    }


    private static T GetValueOrDefault<T>(object[] record, int index, T defaultValue)
    {
        if (index < record.Length)
        {
            return record[index] is T value ? value : defaultValue;
        }
        return defaultValue;
    }


}


