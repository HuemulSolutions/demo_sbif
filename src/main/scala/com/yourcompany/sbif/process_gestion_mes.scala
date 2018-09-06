package com.yourcompany.sbif

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.yourcompany.tables.master._
import com.yourcompany.settings._

object process_gestion_mes {
  def main(args: Array[String]): Unit = {
    val huemulBigDataGov  = new huemul_BigDataGovernance(s"Masterizacion tabla tbl_comun_institucion - ${this.getClass.getSimpleName}", args, globalSettings.Global)
    
    /*************** PARAMETROS **********************/
    var param_ano = huemulBigDataGov.arguments.GetValue("ano", null, "Debe especificar el parametro año, ej: ano=2017").toInt
    var param_mes = huemulBigDataGov.arguments.GetValue("mes", null, "Debe especificar el parametro mes, ej: mes=12").toInt
    
    val Control = new huemul_Control(huemulBigDataGov, null)    
    
    try {             
      /*************** AGREGAR PARAMETROS A CONTROL **********************/
      Control.AddParamInfo("param_ano", param_ano.toString())
      Control.AddParamInfo("param_mes", param_mes.toString())
     
      
      /*********************************************************/
      /*************** LOGICAS DE NEGOCIO **********************/
      /*********************************************************/
      //instancia de clase tbl_comun_institucion 
      val itbl_sbif_gestion_mes = new tbl_sbif_gestion_mes(huemulBigDataGov, Control)
      val itbl_sbif_eerr_mes = new tbl_sbif_eerr_mes(huemulBigDataGov, Control)
      val itbl_sbif_planCuenta = new tbl_sbif_planCuenta(huemulBigDataGov, Control)
      
      val periodo_mes = huemulBigDataGov.ReplaceWithParams("{{YYYY}}-{{MM}}-{{DD}}", param_ano, param_mes, 1, 0, 0, 0, null)
      
      
      //Tablas a ser utilizadas en el proceso
      Control.NewStep("Obtiene datos de EERR consolidado del mes")
      itbl_sbif_eerr_mes.DF_from_SQL("eerr", s"""select ins_id
                                                       ,planCuenta_id
                                                       ,eerr_Monto
                                                FROM ${itbl_sbif_eerr_mes.GetTable()}
                                                WHERE periodo_mes =  '${periodo_mes}'
                                              """)
                                              
      //Valida N° de registros obtenidos
      val DQ_NumReg = itbl_sbif_eerr_mes.DataFramehuemul.DQ_NumRowsInterval(itbl_sbif_eerr_mes, 100, 100000)
      if (DQ_NumReg.isError)
        Control.RaiseError(s"User: N° de filas fuera de lo esperado, CodError (${DQ_NumReg.Error_Code}), descripcion(${DQ_NumReg.Description}) ")
      
      Control.NewStep("Generar Logica de Negocio: Obtiene cruce de plan de cuentas con eerr mensual")
      itbl_sbif_gestion_mes.DF_from_SQL("TablaFinal", s""" SELECT '${periodo_mes}' as periodo_mes
                                                          ,ins_id
                                                          ,planCuenta.producto_id
                                                          ,planCuenta.Negocio_Id
                                                          ,sum(case when planCuenta.planCuenta_Concepto = 'COLOCACION' then eerr_Monto else 0 end) as gestion_colocacion_mes
                                                          ,sum(case when planCuenta.planCuenta_Concepto = 'COLOCACION_M90' then eerr_Monto else 0 end) as gestion_colocacionMora90_mes
                                                          ,sum(case when planCuenta.planCuenta_Concepto = 'PROVISION' then eerr_Monto else 0 end) as gestion_provision_mes
                                                          ,sum(case when planCuenta.planCuenta_Concepto = 'PROVISION' then eerr_Monto else 0 end) as gestion_provision_Ano
                                                          ,sum(case when planCuenta.planCuenta_Concepto = 'PROVISION' then eerr_Monto else 0 end) as gestion_provisionStock
                                                          ,sum(case when planCuenta.planCuenta_Concepto = 'CASTIGO' then eerr_Monto else 0 end) as gestion_castigo_mes
                                                          ,sum(case when planCuenta.planCuenta_Concepto = 'CASTIGO' then eerr_Monto else 0 end) as gestion_castigo_Ano
                                                          ,sum(case when planCuenta.planCuenta_Concepto = 'RECUPERO' then eerr_Monto else 0 end) as gestion_recupero_mes
                                                          ,sum(case when planCuenta.planCuenta_Concepto = 'RECUPERO' then eerr_Monto else 0 end) as gestion_recupero_Ano
                                                          ,CAST(0 AS Decimal(20,2)) as gestion_gastoRiesgo_mes
                                                          ,CAST(0 AS Decimal(20,2)) as gestion_gastoRiesgo_Ano
                                                          ,sum(case when planCuenta.planCuenta_Concepto = 'INGRESO_INTERES' then eerr_Monto else 0 end) as gestion_ingresoInteres_mes
                                                          ,sum(case when planCuenta.planCuenta_Concepto = 'GASTO_INTERES' then eerr_Monto else 0 end) as gestion_gastoInteres_mes
                                                          ,sum(case when planCuenta.planCuenta_Concepto = 'INGRESO_COMISION' then eerr_Monto else 0 end) as gestion_ingresoComision_mes
                                                          ,sum(case when planCuenta.planCuenta_Concepto = 'GASTO_INTERES' then eerr_Monto else 0 end) as gestion_gastoComision_mes
                                                          ,sum(case when planCuenta.planCuenta_Concepto = 'UTILIDAD_FINANCIERA' then eerr_Monto else 0 end) as gestion_utilidadFinanciera_mes
                                                         FROM eerr
                                                           INNER JOIN ${itbl_sbif_planCuenta.GetTable()} PlanCuenta
                                                              on eerr.planCuenta_id = planCuenta.planCuenta_id
                                                         WHERE planCuenta.planCuenta_Concepto is not null
                                                         GROUP BY ins_id
                                                                 ,planCuenta.producto_id
                                                                 ,planCuenta.Negocio_Id
                                             """)
      
      Control.NewStep("Asocia columnas de la tabla con nombres de campos de SQL")
      
      itbl_sbif_gestion_mes.periodo_mes.SetMapping("periodo_mes")
      itbl_sbif_gestion_mes.ins_id.SetMapping("ins_id")
      itbl_sbif_gestion_mes.producto_id.SetMapping("producto_id")
      itbl_sbif_gestion_mes.negocio_id.SetMapping("negocio_id")
      itbl_sbif_gestion_mes.gestion_colocacion_mes.SetMapping("gestion_colocacion_mes")
      itbl_sbif_gestion_mes.gestion_colocacionMora90_mes.SetMapping("gestion_colocacionMora90_mes")
      itbl_sbif_gestion_mes.gestion_provision_mes.SetMapping("gestion_provision_mes")
      itbl_sbif_gestion_mes.gestion_provision_Ano.SetMapping("gestion_provision_Ano")
      itbl_sbif_gestion_mes.gestion_provisionStock.SetMapping("gestion_provisionStock")
      itbl_sbif_gestion_mes.gestion_castigo_mes.SetMapping("gestion_castigo_mes")
      itbl_sbif_gestion_mes.gestion_castigo_Ano.SetMapping("gestion_castigo_Ano")
      itbl_sbif_gestion_mes.gestion_recupero_mes.SetMapping("gestion_recupero_mes")
      itbl_sbif_gestion_mes.gestion_recupero_Ano.SetMapping("gestion_recupero_Ano")
      itbl_sbif_gestion_mes.gestion_gastoRiesgo_mes.SetMapping("gestion_gastoRiesgo_mes")
      itbl_sbif_gestion_mes.gestion_gastoRiesgo_Ano.SetMapping("gestion_gastoRiesgo_Ano")
      itbl_sbif_gestion_mes.gestion_ingresoInteres_mes.SetMapping("gestion_ingresoInteres_mes")
      itbl_sbif_gestion_mes.gestion_gastoInteres_mes.SetMapping("gestion_gastoInteres_mes")
      itbl_sbif_gestion_mes.gestion_ingresoComision_mes.SetMapping("gestion_ingresoComision_mes")
      itbl_sbif_gestion_mes.gestion_gastoComision_mes.SetMapping("gestion_gastoComision_mes")
      itbl_sbif_gestion_mes.gestion_utilidadFinanciera_mes.SetMapping("gestion_utilidadFinanciera_mes")
     
      Control.NewStep("Ejecuta Proceso")    
      if (!itbl_sbif_gestion_mes.executeFull("FinalSaved"))
        Control.RaiseError(s"User: Error al intentar masterizar instituciones (${itbl_sbif_gestion_mes.Error_Code}): ${itbl_sbif_gestion_mes.Error_Text}")
      
     
      Control.FinishProcessOK
    } catch {
      case e: Exception => {
        Control.Control_Error.GetError(e, this.getClass.getName, null)
        Control.FinishProcessError()
      }
    }
    
    huemulBigDataGov.close()
  }
}