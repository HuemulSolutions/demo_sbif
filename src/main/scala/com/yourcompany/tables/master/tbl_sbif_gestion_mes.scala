package com.yourcompany.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.dataquality._
import org.apache.spark.sql.types._


class tbl_sbif_gestion_mes(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_Table(huemulBigDataGov, Control) with Serializable {
 /**********   C O N F I G U R A C I O N   D E   L A   T A B L A   ****************************************/
  //Tipo de tabla, Master y Reference son catalogos sin particiones de periodo
  this.setTableType(huemulType_Tables.Transaction)
  //Base de Datos en HIVE donde sera creada la tabla
  this.setDataBase(huemulBigDataGov.GlobalSettings.MASTER_DataBase)
  //Tipo de archivo que sera almacenado en HDFS
  this.setStorageType(huemulType_StorageType.PARQUET)
  //Ruta en HDFS donde se guardara el archivo PARQUET
  this.setGlobalPaths(huemulBigDataGov.GlobalSettings.MASTER_SmallFiles_Path)
  //Ruta en HDFS especifica para esta tabla (Globalpaths / localPath)
  this.setLocalPath("sbif/")
    //columna de particion
  this.setPartitionField("periodo_mes")
  //Frecuencia de actualización
  this.setFrequency(huemulType_Frequency.MONTHLY)
  
  /**********   S E T E O   I N F O R M A T I V O   ****************************************/
  //Nombre del contacto de TI
  this.setDescription("uCalcula indicadores a partir del plan de cuentas")
  //Nombre del contacto de negocio
  this.setBusiness_ResponsibleName("Responsable de negocio")
  //Nombre del contacto de TI
  this.setIT_ResponsibleName("SBIF")
   
  /**********   D A T A   Q U A L I T Y   ****************************************/
  //DataQuality: maximo numero de filas o porcentaje permitido, dejar comentado o null en caso de no aplicar
  //this.setDQ_MaxNewRecords_Num(null)  //ej: 1000 para permitir maximo 1.000 registros nuevos cada vez que se intenta insertar
  //this.setDQ_MaxNewRecords_Perc(null) //ej: 0.2 para limitar al 20% de filas nuevas
    
  /**********   S E G U R I D A D   ****************************************/
  //Solo estos package y clases pueden ejecutar en modo full, si no se especifica todos pueden invocar
  this.WhoCanRun_executeFull_addAccess("process_gestion_mes", "com.yourcompany.sbif")
  //Solo estos package y clases pueden ejecutar en modo solo Insert, si no se especifica todos pueden invocar
  //this.WhoCanRun_executeOnlyInsert_addAccess("[[MyclassName]]", "[[my.package.path]]")
  //Solo estos package y clases pueden ejecutar en modo solo Update, si no se especifica todos pueden invocar
  //this.WhoCanRun_executeOnlyUpdate_addAccess("[[MyclassName]]", "[[my.package.path]]")
  

  /**********   C O L U M N A S   ****************************************/

    //Columna de periodo
  val periodo_mes = new huemul_Columns (StringType, true,"periodo de los datos")
  periodo_mes.setIsPK(true)
    
  val ins_id = new huemul_Columns (StringType, true, "Código institución.") 
  ins_id.setIsPK(true)
  ins_id.setARCO_Data(false)  
  ins_id.setSecurityLevel(huemulType_SecurityLevel.Public)  
  ins_id.setDQ_MinLen(3) 
  ins_id.setDQ_MaxLen(3)
  
  val producto_id = new huemul_Columns (StringType, true, "Código de productos (tc, prestamo cuota, linea de crédito).") 
  producto_id.setIsPK(true)
  producto_id.setARCO_Data(false)  
  producto_id.setSecurityLevel(huemulType_SecurityLevel.Public)  
  
  val negocio_id = new huemul_Columns (StringType, true, "Código de negocio (consumo, hipotecario, comercial).") 
  negocio_id.setIsPK(true)
  negocio_id.setARCO_Data(false)  
  negocio_id.setSecurityLevel(huemulType_SecurityLevel.Public)  
  
  /**Deuda**/
  val gestion_colocacion_mes = new huemul_Columns (DecimalType(26,2), true, "Monto de las colocaciones") 
  gestion_colocacion_mes.setARCO_Data(false)  
  gestion_colocacion_mes.setSecurityLevel(huemulType_SecurityLevel.Public)  
  
  val gestion_colocacionMora90_mes = new huemul_Columns (DecimalType(26,2), true, "Monto de las colocaciones con morosidad mayor a 90 dias") 
  gestion_colocacionMora90_mes.setARCO_Data(false)  
  gestion_colocacionMora90_mes.setSecurityLevel(huemulType_SecurityLevel.Public)  

  /**provisiones**/
  val gestion_provision_mes = new huemul_Columns (DecimalType(26,2), true, "Monto de provisiones del mes") 
  gestion_provision_mes.setARCO_Data(false)  
  gestion_provision_mes.setSecurityLevel(huemulType_SecurityLevel.Public)  
  
  val gestion_provision_Ano = new huemul_Columns (DecimalType(26,2), true, "Monto de provisiones acumuladas del año") 
  gestion_provision_Ano.setARCO_Data(false)  
  gestion_provision_Ano.setSecurityLevel(huemulType_SecurityLevel.Public)  
  
  val gestion_provisionStock = new huemul_Columns (DecimalType(26,2), true, "Monto de provisiones constituidas") 
  gestion_provisionStock.setARCO_Data(false)  
  gestion_provisionStock.setSecurityLevel(huemulType_SecurityLevel.Public) 
  
  /**castigos**/
  val gestion_castigo_mes = new huemul_Columns (DecimalType(26,2), true, "Monto de castigos del mes") 
  gestion_castigo_mes.setARCO_Data(false)  
  gestion_castigo_mes.setSecurityLevel(huemulType_SecurityLevel.Public)  
  
  val gestion_castigo_Ano = new huemul_Columns (DecimalType(26,2), true, "Monto de castigos acumuladas del año") 
  gestion_castigo_Ano.setARCO_Data(false)  
  gestion_castigo_Ano.setSecurityLevel(huemulType_SecurityLevel.Public)  
  
  /**recuperaciones**/
  val gestion_recupero_mes = new huemul_Columns (DecimalType(26,2), true, "Monto de recuperaciones del mes") 
  gestion_recupero_mes.setARCO_Data(false)  
  gestion_recupero_mes.setSecurityLevel(huemulType_SecurityLevel.Public)  
  
  val gestion_recupero_Ano = new huemul_Columns (DecimalType(26,2), true, "Monto de recuperaciones acumuladas del año") 
  gestion_recupero_Ano.setARCO_Data(false)  
  gestion_recupero_Ano.setSecurityLevel(huemulType_SecurityLevel.Public)  
  
  
  /**Gasto en Riesgo**/
   val gestion_gastoRiesgo_mes = new huemul_Columns (DecimalType(26,2), true, "Monto de gasto en Riesgo es del mes (provision_mes + castigo_mes - recupero_mes)") 
  gestion_gastoRiesgo_mes.setARCO_Data(false)  
  gestion_gastoRiesgo_mes.setSecurityLevel(huemulType_SecurityLevel.Public)  
  
  val gestion_gastoRiesgo_Ano = new huemul_Columns (DecimalType(26,2), true, "Monto de gasto en Riesgo acumuladas del año (privision_mes + castigo_mes - recupero_mes") 
  gestion_gastoRiesgo_Ano.setARCO_Data(false)  
  gestion_gastoRiesgo_Ano.setSecurityLevel(huemulType_SecurityLevel.Public)  
  
  
   /**Ingresos por Interes**/
  val gestion_ingresoInteres_mes = new huemul_Columns (DecimalType(26,2), true, "Monto de Ingresos por intereses y reajustes  del mes") 
  gestion_ingresoInteres_mes.setARCO_Data(false)  
  gestion_ingresoInteres_mes.setSecurityLevel(huemulType_SecurityLevel.Public)  
  
   /**gastos por Interes**/
  val gestion_gastoInteres_mes = new huemul_Columns (DecimalType(26,2), true, "Monto de gastos por intereses y reajustes  del mes") 
  gestion_gastoInteres_mes.setARCO_Data(false)  
  gestion_gastoInteres_mes.setSecurityLevel(huemulType_SecurityLevel.Public)  
  
   /**Ingresos por Comision**/
  val gestion_ingresoComision_mes = new huemul_Columns (DecimalType(26,2), true, "Monto de Ingresos por Comisiones del mes") 
  gestion_ingresoComision_mes.setARCO_Data(false)  
  gestion_ingresoComision_mes.setSecurityLevel(huemulType_SecurityLevel.Public)  
  
  /**gastos por Comision**/
  val gestion_gastoComision_mes = new huemul_Columns (DecimalType(26,2), true, "Monto de gastos por Comisiones del mes") 
  gestion_gastoComision_mes.setARCO_Data(false)  
  gestion_gastoComision_mes.setSecurityLevel(huemulType_SecurityLevel.Public)  
  
 
  /**Utilidad**/
  val gestion_utilidadFinanciera_mes = new huemul_Columns (DecimalType(26,2), true, "Monto de gastos por Comisiones del mes") 
  gestion_utilidadFinanciera_mes.setARCO_Data(false)  
  gestion_utilidadFinanciera_mes.setSecurityLevel(huemulType_SecurityLevel.Public)  
  
   
  //**********Atributos adicionales de DataQuality
  //yourColumn.setIsPK(true) //valor por default en cada campo es false
  //yourColumn.setIsUnique(true) //valor por default en cada campo es false
  //yourColumn.setNullable(true) //valor por default en cada campo es false
  //yourColumn.setIsUnique(true) //valor por default en cada campo es false
  //yourColumn.setDQ_MinDecimalValue(Decimal.apply(0))
  //yourColumn.setDQ_MaxDecimalValue(Decimal.apply(200.0))
  //yourColumn.setDQ_MinDateTimeValue("2018-01-01")
  //yourColumn.setDQ_MaxDateTimeValue("2018-12-31")
  //yourColumn.setDQ_MinLen(5)
  //yourColumn.setDQ_MaxLen(100)
  //**********Otros atributos
  //yourColumn.setDefaultValue("'string'") // "10" // "'2018-01-01'"
  //yourColumn.setEncryptedType("tipo")
    
  //**********Ejemplo para aplicar DataQuality de Integridad Referencial
  val itbl_comun_institucion = new tbl_comun_institucion(huemulBigDataGov,Control)
  val fk_tbl_comun_institucion = new huemul_Table_Relationship(itbl_comun_institucion, false)
  fk_tbl_comun_institucion.AddRelationship(itbl_comun_institucion.ins_id, ins_id)
  
  
  val itbl_comun_negocio = new tbl_comun_negocio(huemulBigDataGov,Control)
  val fk_tbl_comun_negocio = new huemul_Table_Relationship(itbl_comun_negocio, false)
  fk_tbl_comun_negocio.AddRelationship(itbl_comun_negocio.negocio_id, negocio_id)


  val itbl_comun_producto = new tbl_comun_producto(huemulBigDataGov,Control)
  val fk_tbl_comun_producto = new huemul_Table_Relationship(itbl_comun_producto, false)
  fk_tbl_comun_producto.AddRelationship(itbl_comun_producto.producto_id, producto_id)

   
  //**********Ejemplo para agregar reglas de DataQuality Avanzadas  -->ColumnXX puede ser null si la validacion es a nivel de tabla
  //**************Parametros
  //********************  ColumnXXColumna a la cual se aplica la validacion, si es a nivel de tabla poner null
  //********************  Descripcion de la validacion, ejemplo: "Consistencia: Campo1 debe ser mayor que campo 2"
  //********************  Formula SQL En Positivo, ejemplo1: campo1 > campo2  ;ejemplo2: sum(campo1) > sum(campo2)  
  //********************  CodigoError: Puedes especificar un codigo para la captura posterior de errores, es un numero entre 1 y 999
  //********************  QueryLevel es opcional, por default es "row" y se aplica al ejemplo1 de la formula, para el ejmplo2 se debe indicar "Aggregate"
  //********************  Notification es opcional, por default es "error", y ante la aparicion del error el programa falla, si lo cambias a "warning" y la validacion falla, el programa sigue y solo sera notificado
  //val DQ_NombreRegla: huemul_DataQuality = new huemul_DataQuality(ColumnXX,"Descripcion de la validacion", "Campo_1 > Campo_2",1)
  //**************Adicionalmeente, puedes agregar "tolerancia" a la validacion, es decir, puedes especiicar 
  //************** numFilas = 10 para permitir 10 errores (al 11 se cae)
  //************** porcentaje = 0.2 para permitir una tolerancia del 20% de errores
  //************** ambos parametros son independientes (condicion o), cualquiera de las dos tolerancias que no se cumpla se gatilla el error o warning
  //DQ_NombreRegla.setTolerance(numfilas, porcentaje)
 
  this.ApplyTableDefinition()
}
