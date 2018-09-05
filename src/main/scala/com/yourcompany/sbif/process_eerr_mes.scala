package com.yourcompany.sbif


import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import java.util.Calendar;
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import com.yourcompany.tables.master._
import com.yourcompany.sbif.datalake._
import org.apache.spark.sql.functions._
import com.yourcompany.settings._


//import com.huemulsolutions.bigdata.tables._
//import com.huemulsolutions.bigdata.dataquality._


object process_eerr_mes {
  
  /**
   * Este codigo se ejecuta cuando se llama el JAR desde spark2-submit. el codigo esta preparado para hacer reprocesamiento masivo.
  */
  def main(args : Array[String]) {
    //Creacion API
    val huemulBigDataGov  = new huemul_BigDataGovernance(s"Masterizacion tabla tbl_sbif_eerr_mes - ${this.getClass.getSimpleName}", args, globalSettings.Global)
    
    /*************** PARAMETROS **********************/
    var param_ano = huemulBigDataGov.arguments.GetValue("ano", null, "Debe especificar el parametro año, ej: ano=2017").toInt
    var param_mes = huemulBigDataGov.arguments.GetValue("mes", null, "Debe especificar el parametro mes, ej: mes=12").toInt
     
    var param_dia = 1
    val param_numMeses = huemulBigDataGov.arguments.GetValue("num_meses", "1").toInt

    /*************** CICLO REPROCESO MASIVO **********************/
    var i: Int = 1
    var FinOK: Boolean = true
    var Fecha = huemulBigDataGov.setDateTime(param_ano, param_mes, param_dia, 0, 0, 0)
    
    while (i <= param_numMeses) {
      param_ano = huemulBigDataGov.getYear(Fecha)
      param_mes = huemulBigDataGov.getMonth(Fecha)
      println(s"Procesando Año $param_ano, Mes $param_mes ($i de $param_numMeses)")
      
      //Ejecuta codigo
      var FinOK = procesa_master(huemulBigDataGov, null, param_ano, param_mes)
      
      if (FinOK)
        i+=1
      else {
        println(s"ERROR Procesando Año $param_ano, Mes $param_mes ($i de $param_numMeses)")
        i = param_numMeses + 1
      }
        
      Fecha.add(Calendar.MONTH, 1)      
    }
    
    
    huemulBigDataGov.close
  }
  
  /**
    masterizacion de archivo [[CAMBIAR]] <br>
    param_ano: año de los datos  <br>
    param_mes: mes de los datos  <br>
   */
  def procesa_master(huemulBigDataGov: huemul_BigDataGovernance, ControlParent: huemul_Control, param_ano: Integer, param_mes: Integer): Boolean = {
    val Control = new huemul_Control(huemulBigDataGov, ControlParent)    
    
    try {             
      /*************** AGREGAR PARAMETROS A CONTROL **********************/
      Control.AddParamInfo("param_ano", param_ano.toString())
      Control.AddParamInfo("param_mes", param_mes.toString())
      
      //Obtiene listado de instituciones del mes
      val periodo_mes = huemulBigDataGov.ReplaceWithParams("{{YYYY}}-{{MM}}-{{DD}}", param_ano, param_mes, 1, 0, 0, 0, null)
      val itbl_institucion_mes = new tbl_comun_institucion_mes(huemulBigDataGov, Control)
      val itbl_institucion_mes_data = huemulBigDataGov.DF_ExecuteQuery("df_ins", s"""select ins_id from ${itbl_institucion_mes.GetTable()} where periodo_mes = '${periodo_mes}'  """).collect()
      
      if (itbl_institucion_mes_data.length == 0)
        Control.RaiseError(s"Error: No se encontraron instituciones cargadas para el periodo ${periodo_mes}")
        
      var DF_RAW_B1_FINAL: DataFrame = null
      var DF_RAW_R1_FINAL: DataFrame = null
      var DF_RAW_C1_FINAL: DataFrame = null
      var DF_RAW_C2_FINAL: DataFrame = null
      
      itbl_institucion_mes_data.foreach { x => 
        val CodIns = x.getString(0)
        /*************** ABRE RAW DESDE DATALAKE **********************/
        Control.NewStep(s"Abre DataLake institucion B1: ${CodIns}")  
        var DF_RAW_B1 =  new raw_B1_mes(huemulBigDataGov, Control)
        if (!DF_RAW_B1.open("DF_RAW", Control, param_ano, param_mes, 1, 0, 0, 0, CodIns))       
          Control.RaiseError(s"error encontrado, abortar: ${DF_RAW_B1.Error.ControlError_Message}")
        
        if (DF_RAW_B1_FINAL == null)
          DF_RAW_B1_FINAL= DF_RAW_B1.DataFramehuemul.DataFrame.withColumn("ins_Id", lit(CodIns))
        else 
          DF_RAW_B1_FINAL = DF_RAW_B1_FINAL.union(DF_RAW_B1.DataFramehuemul.DataFrame.withColumn("ins_Id", lit(CodIns)))
          
          
        Control.NewStep(s"Abre DataLake institucion R1: ${CodIns}")  
        var DF_RAW_R1 =  new raw_R1_mes(huemulBigDataGov, Control)
        if (!DF_RAW_R1.open("DF_RAW", Control, param_ano, param_mes, 1, 0, 0, 0, CodIns))       
          Control.RaiseError(s"error encontrado, abortar: ${DF_RAW_R1.Error.ControlError_Message}")
        
        if (DF_RAW_R1_FINAL == null)
          DF_RAW_R1_FINAL= DF_RAW_R1.DataFramehuemul.DataFrame.withColumn("ins_Id", lit(CodIns))
        else 
          DF_RAW_R1_FINAL = DF_RAW_R1_FINAL.union(DF_RAW_R1.DataFramehuemul.DataFrame.withColumn("ins_Id", lit(CodIns)))
          
          
        Control.NewStep(s"Abre DataLake institucion C1: ${CodIns}")  
        var DF_RAW_C1 =  new raw_C1_mes(huemulBigDataGov, Control)
        if (!DF_RAW_C1.open("DF_RAW", Control, param_ano, param_mes, 1, 0, 0, 0, CodIns))       
          Control.RaiseError(s"error encontrado, abortar: ${DF_RAW_C1.Error.ControlError_Message}")
        
        if (DF_RAW_C1_FINAL == null)
          DF_RAW_C1_FINAL= DF_RAW_C1.DataFramehuemul.DataFrame.withColumn("ins_Id", lit(CodIns))
        else 
          DF_RAW_C1_FINAL = DF_RAW_C1_FINAL.union(DF_RAW_C1.DataFramehuemul.DataFrame.withColumn("ins_Id", lit(CodIns)))
          
        Control.NewStep(s"Abre DataLake institucion C2: ${CodIns}")  
        var DF_RAW_C2 =  new raw_C2_mes(huemulBigDataGov, Control)
        if (!DF_RAW_C2.open("DF_RAW", Control, param_ano, param_mes, 1, 0, 0, 0, CodIns))       
          Control.RaiseError(s"error encontrado, abortar: ${DF_RAW_C2.Error.ControlError_Message}")
        
        if (DF_RAW_C2_FINAL == null)
          DF_RAW_C2_FINAL= DF_RAW_C2.DataFramehuemul.DataFrame.withColumn("ins_Id", lit(CodIns))
        else 
          DF_RAW_C2_FINAL = DF_RAW_C2_FINAL.union(DF_RAW_C2.DataFramehuemul.DataFrame.withColumn("ins_Id", lit(CodIns)))
          
      }
      
      DF_RAW_B1_FINAL.createOrReplaceTempView("DF_RAW_B1")
      DF_RAW_R1_FINAL.createOrReplaceTempView("DF_RAW_R1")
      DF_RAW_C1_FINAL.createOrReplaceTempView("DF_RAW_C1")
      DF_RAW_C2_FINAL.createOrReplaceTempView("DF_RAW_C2")
      
      
      
      /*********************************************************/
      /*************** LOGICAS DE NEGOCIO **********************/
      /*********************************************************/
      //instancia de clase tbl_sbif_eerr_mes 
      val huemulTable = new tbl_sbif_eerr_mes(huemulBigDataGov, Control)
      
      Control.NewStep("Generar Logica de Negocio")
      huemulTable.DF_from_SQL("FinalRAW"
                          , s"""SELECT TO_DATE("${param_ano}-${param_mes}-1") as periodo_mes
                                     ,planCuenta_id
                                     ,ins_Id
                                     ,cast('B1' as String) AS eerr_origen
                                     ,cast(translate(b1_Monto_clp,',','.') as Decimal(23,2)) * 1000000 as eerr_Monto_clp
                                     ,cast(translate(b1_Monto_ipc,',','.') as Decimal(23,2)) * 1000000 as eerr_Monto_ipc
                                     ,cast(translate(b1_Monto_tdc,',','.') as Decimal(23,2)) * 1000000 as eerr_Monto_tdc
                                     ,cast(translate(b1_Monto_tdcb,',','.') as Decimal(23,2)) * 1000000 as eerr_Monto_tdcb
                                     
                                     ,cast(translate(b1_Monto_clp,',','.') as Decimal(23,2)) * 1000000 +
                                      cast(translate(b1_Monto_ipc,',','.') as Decimal(23,2)) * 1000000 +
                                      cast(translate(b1_Monto_tdc,',','.') as Decimal(23,2)) * 1000000 +
                                      cast(translate(b1_Monto_tdcb,',','.') as Decimal(23,2)) * 1000000 as eerr_Monto
                               FROM DF_RAW_B1

                               UNION ALL
                               SELECT TO_DATE("${param_ano}-${param_mes}-1") as periodo_mes
                                     ,planCuenta_id
                                     ,ins_Id
                                     ,cast('R1' as String) AS eerr_origen
                                     ,cast(translate(r1_Monto_clp,',','.') as Decimal(23,2)) * 1000000 as eerr_Monto_clp
                                     ,cast(translate(r1_Monto_ipc,',','.') as Decimal(23,2)) * 1000000 as eerr_Monto_ipc
                                     ,cast(translate(r1_Monto_tdc,',','.') as Decimal(23,2)) * 1000000 as eerr_Monto_tdc
                                     ,cast(translate(r1_Monto_tdcb,',','.') as Decimal(23,2)) * 1000000 as eerr_Monto_tdcb
                                     
                                     ,cast(translate(r1_Monto_clp,',','.') as Decimal(23,2)) * 1000000 +
                                      cast(translate(r1_Monto_ipc,',','.') as Decimal(23,2)) * 1000000 +
                                      cast(translate(r1_Monto_tdc,',','.') as Decimal(23,2)) * 1000000 +
                                      cast(translate(r1_Monto_tdcb,',','.') as Decimal(23,2)) * 1000000 as eerr_Monto
                               FROM DF_RAW_R1

                               UNION ALL
                               SELECT TO_DATE("${param_ano}-${param_mes}-1") as periodo_mes
                                     ,planCuenta_id
                                     ,ins_Id
                                     ,cast('C1' as String) AS eerr_origen
                                     ,cast(0 as Decimal(23,2))  as eerr_Monto_clp
                                     ,cast(0 as Decimal(23,2))  as eerr_Monto_ipc
                                     ,cast(0 as Decimal(23,2))  as eerr_Monto_tdc
                                     ,cast(0 as Decimal(23,2))  as eerr_Monto_tdcb
                                     
                                     ,cast(translate(c1_Monto,',','.') as Decimal(23,2)) * 1000000 as eerr_Monto
                               FROM DF_RAW_C1

                               UNION ALL
                               SELECT TO_DATE("${param_ano}-${param_mes}-1") as periodo_mes
                                     ,planCuenta_id
                                     ,ins_Id
                                     ,cast('C2' as String) AS eerr_origen
                                     ,cast(0 as Decimal(23,2))  as eerr_Monto_clp
                                     ,cast(0 as Decimal(23,2))  as eerr_Monto_ipc
                                     ,cast(0 as Decimal(23,2))  as eerr_Monto_tdc
                                     ,cast(0 as Decimal(23,2))  as eerr_Monto_tdcb
                                     
                                     ,cast(translate(c2_Monto,',','.') as Decimal(23,2)) * 1000000 as eerr_Monto
                               FROM DF_RAW_C2
""")
      
      DF_RAW_B1_FINAL.unpersist()
      DF_RAW_R1_FINAL.unpersist()
      DF_RAW_C1_FINAL.unpersist()
      DF_RAW_C1_FINAL.unpersist()
      
     
      Control.NewStep("Asocia columnas de la tabla con nombres de campos de SQL")
      
      huemulTable.periodo_mes.SetMapping("periodo_mes")
      huemulTable.planCuenta_id.SetMapping("planCuenta_id")
      huemulTable.ins_id.SetMapping("ins_id")
      huemulTable.eerr_origen.SetMapping("eerr_origen")
      huemulTable.eerr_Monto_clp.SetMapping("eerr_Monto_clp")
      huemulTable.eerr_Monto_ipc.SetMapping("eerr_Monto_ipc")
      huemulTable.eerr_Monto_tdc.SetMapping("eerr_Monto_tdc")
      huemulTable.eerr_Monto_tdcb.SetMapping("eerr_Monto_tdcb")
      huemulTable.eerr_Monto.SetMapping("eerr_Monto")

      
      
      Control.NewStep("Ejecuta Proceso")    
      if (!huemulTable.executeFull("FinalSaved"))
        Control.RaiseError(s"User: Error al intentar masterizar instituciones (${huemulTable.Error_Code}): ${huemulTable.Error_Text}")
      
      Control.FinishProcessOK
    } catch {
      case e: Exception => {
        Control.Control_Error.GetError(e, this.getClass.getName, null)
        Control.FinishProcessError()
      }
    }
    
    return Control.Control_Error.IsOK()   
  }
  
}

/**
* Objeto permite mover archivos HDFS desde ambiente origen (ejemplo producción) a ambientes destino (ejemplo ambiente experimental)
*/
object process_eerr_mes_Migrar {
 
 def main(args : Array[String]) {
   //Creacion API
    val huemulBigDataGov  = new huemul_BigDataGovernance(s"Migración de datos tabla tbl_sbif_eerr_mes  - ${this.getClass.getSimpleName}", args, globalSettings.Global)
    
    /*************** PARAMETROS **********************/
    var param_ano = huemulBigDataGov.arguments.GetValue("ano", null, "Debe especificar el parametro año, ej: ano=2017").toInt
    var param_mes = huemulBigDataGov.arguments.GetValue("mes", null, "Debe especificar el parametro mes, ej: mes=12").toInt
    var param_dia = 1
   
    var param = huemulBigDataGov.ReplaceWithParams("{{YYYY}}-{{MM}}-{{DD}}", param_ano, param_mes, param_dia, 0, 0, 0)
    
   val clase = new tbl_sbif_eerr_mes(huemulBigDataGov, null)
   clase.CopyToDest(param, "[[environment]]")
   huemulBigDataGov.close
 }
 
}

