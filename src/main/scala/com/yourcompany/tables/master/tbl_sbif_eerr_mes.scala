package com.yourcompany.tables.master


import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.dataquality._
import org.apache.spark.sql.types._


class tbl_sbif_eerr_mes(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_Table(huemulBigDataGov, Control) with Serializable {
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
  /**********   S E T E O   I N F O R M A T I V O   ****************************************/
  //Nombre del contacto de TI
  this.setDescription("un estado de resultados consolidado + estado de situación financiera consolidado + información complementaria consolidada + información complementaria individual")
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
  this.WhoCanRun_executeFull_addAccess("process_eerr_mes", "com.yourcompany.sbif")
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
  
  val planCuenta_id = new huemul_Columns (StringType, true, "Código contable. Es un campo de 7 digitos que identifica el concepto contable que se describe en el archivo Modelo-MB1.txt.") 
  planCuenta_id.setIsPK(true)
  planCuenta_id.setARCO_Data(false)  
  planCuenta_id.setSecurityLevel(huemulType_SecurityLevel.Public)  
  planCuenta_id.setDQ_MinLen(7) 
  planCuenta_id.setDQ_MaxLen(7)  
  
  val eerr_origen = new huemul_Columns (StringType, true, "Codigo del archivo de origen (b1,r1,c1,c2") 
  eerr_origen.setARCO_Data(false)  
  eerr_origen.setSecurityLevel(huemulType_SecurityLevel.Public)  

  val eerr_Monto_clp = new huemul_Columns (DecimalType(26,2), true, "Monto Moneda Chilena No Reajustable ") 
  eerr_Monto_clp.setARCO_Data(false)  
  eerr_Monto_clp.setSecurityLevel(huemulType_SecurityLevel.Public)  

  val eerr_Monto_ipc = new huemul_Columns (DecimalType(26,2), true, "Monto Moneda reajustable por factores de IPC ") 
  eerr_Monto_ipc.setARCO_Data(false)  
  eerr_Monto_ipc.setSecurityLevel(huemulType_SecurityLevel.Public)  

  val eerr_Monto_tdc = new huemul_Columns (DecimalType(26,2), true, "Monto Moneda reajustable por Tipo de Cambio ") 
  eerr_Monto_tdc.setARCO_Data(false)  
  eerr_Monto_tdc.setSecurityLevel(huemulType_SecurityLevel.Public)  

  val eerr_Monto_tdcb = new huemul_Columns (DecimalType(26,2), true, "Monto en Moneda Extranjera de acuerdo al tipo de cambio de representación contable usado por el banco ") 
  eerr_Monto_tdcb.setARCO_Data(false)  
  eerr_Monto_tdcb.setSecurityLevel(huemulType_SecurityLevel.Public)  
  
  val eerr_Monto = new huemul_Columns (DecimalType(26,2), true, "suma de los campos eerr_Monto_clp+eerr_Monto_ipc+eerr_Monto_tdc+eerr_Monto_tdcb ") 
  eerr_Monto.setARCO_Data(false)  
  eerr_Monto.setSecurityLevel(huemulType_SecurityLevel.Public)  



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
  val itbl_comun_institucion_mes = new tbl_comun_institucion_mes(huemulBigDataGov,Control)
  val fk_tbl_comun_institucion_mes = new huemul_Table_Relationship(itbl_comun_institucion_mes, false)
  fk_tbl_comun_institucion_mes.AddRelationship(itbl_comun_institucion_mes.ins_id, ins_id)
  fk_tbl_comun_institucion_mes.AddRelationship(itbl_comun_institucion_mes.periodo_mes, periodo_mes)
  
  val itbl_sbif_planCuenta_mes = new tbl_sbif_planCuenta_mes(huemulBigDataGov,Control)
  val fk_tbl_sbif_planCuenta_mes = new huemul_Table_Relationship(itbl_sbif_planCuenta_mes, false)
  fk_tbl_sbif_planCuenta_mes.AddRelationship(itbl_sbif_planCuenta_mes.planCuenta_id, planCuenta_id)
  fk_tbl_sbif_planCuenta_mes.AddRelationship(itbl_sbif_planCuenta_mes.periodo_mes, periodo_mes)
    
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

