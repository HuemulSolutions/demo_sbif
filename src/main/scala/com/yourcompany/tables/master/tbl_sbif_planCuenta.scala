package com.yourcompany.tables.master

/*
instrucciones (parte a):
   1. Crear una clase en el packete que contiene las tablas (com.yourcompany.tables.master) con el nombre "tbl_comun_planCuenta"
   2. copiar el codigo desde estas instrucciones hasta ***    M A S T E R   P R O C E S S     *** y pegarlo en "tbl_comun_planCuenta"
   3. Revisar detalladamente la configuracion de la tabla
      3.1 busque el texto "[[LLENAR ESTE CAMPO]]" y reemplace la descripcion segun corresponda 
   4. seguir las instrucciones "parte b"
*/

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.dataquality._
import org.apache.spark.sql.types._


class tbl_sbif_planCuenta(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_Table(huemulBigDataGov, Control) with Serializable {
  /**********   C O N F I G U R A C I O N   D E   L A   T A B L A   ****************************************/
  //Tipo de tabla, Master y Reference son catalogos sin particiones de periodo
  this.setTableType(huemulType_Tables.Reference)
  //Base de Datos en HIVE donde sera creada la tabla
  this.setDataBase(huemulBigDataGov.GlobalSettings.MASTER_DataBase)
  //Tipo de archivo que sera almacenado en HDFS
  this.setStorageType(huemulType_StorageType.PARQUET)
  //Ruta en HDFS donde se guardara el archivo PARQUET
  this.setGlobalPaths(huemulBigDataGov.GlobalSettings.MASTER_SmallFiles_Path)
  //Ruta en HDFS especifica para esta tabla (Globalpaths / localPath)
  this.setLocalPath("sbif/")
  //Frecuencia de actualización
  this.setFrequency(huemulType_Frequency.ANY_MOMENT)
  
  /**********   S E T E O   I N F O R M A T I V O   ****************************************/
  //Nombre del contacto de TI
  this.setDescription("[[LLENAR ESTE CAMPO]]")
  //Nombre del contacto de negocio
  this.setBusiness_ResponsibleName("[[LLENAR ESTE CAMPO]]")
  //Nombre del contacto de TI
  this.setIT_ResponsibleName("[[LLENAR ESTE CAMPO]]")
   
  /**********   D A T A   Q U A L I T Y   ****************************************/
  //DataQuality: maximo numero de filas o porcentaje permitido, dejar comentado o null en caso de no aplicar
  //this.setDQ_MaxNewRecords_Num(null)  //ej: 1000 para permitir maximo 1.000 registros nuevos cada vez que se intenta insertar
  //this.setDQ_MaxNewRecords_Perc(null) //ej: 0.2 para limitar al 20% de filas nuevas
    
  /**********   S E G U R I D A D   ****************************************/
  //Solo estos package y clases pueden ejecutar en modo full, si no se especifica todos pueden invocar
  this.WhoCanRun_executeFull_addAccess("process_planCuenta", "com.yourcompany.sbif")
  //Solo estos package y clases pueden ejecutar en modo solo Insert, si no se especifica todos pueden invocar
  //this.WhoCanRun_executeOnlyInsert_addAccess("[[MyclassName]]", "[[my.package.path]]")
  //Solo estos package y clases pueden ejecutar en modo solo Update, si no se especifica todos pueden invocar
  //this.WhoCanRun_executeOnlyUpdate_addAccess("[[MyclassName]]", "[[my.package.path]]")
  

  /**********   C O L U M N A S   ****************************************/

    
   
  val planCuenta_id = new huemul_Columns (StringType, true, "código del plan de cuentas") 
  planCuenta_id.setIsPK(true) 
  planCuenta_id.setARCO_Data(false)  
  planCuenta_id.setSecurityLevel(huemulType_SecurityLevel.Public)  
  planCuenta_id.setDQ_MinLen(7) 
  planCuenta_id.setDQ_MaxLen(7)  

  val planCuenta_Nombre = new huemul_Columns (StringType, true, "Nombre de la cuenta") 
  planCuenta_Nombre.setARCO_Data(false)  
  planCuenta_Nombre.setSecurityLevel(huemulType_SecurityLevel.Public)  
  planCuenta_Nombre.setMDM_EnableOldValue(true)  
  planCuenta_Nombre.setMDM_EnableDTLog(true)  
  planCuenta_Nombre.setMDM_EnableProcessLog(true)  
  planCuenta_Nombre.setDQ_MinLen(5) 
  planCuenta_Nombre.setDQ_MaxLen(100)  
  
  val negocio_id = new huemul_Columns (StringType, false, "Asignación del negocio de la cuenta cuentable") 
  negocio_id.setARCO_Data(false)  
  negocio_id.setSecurityLevel(huemulType_SecurityLevel.Public)  
  negocio_id.setMDM_EnableOldValue(true)  
  negocio_id.setMDM_EnableDTLog(true)  
  negocio_id.setMDM_EnableProcessLog(true)  
  negocio_id.setDefaultValue(null)
  negocio_id.setNullable(true) 
  
  val producto_id = new huemul_Columns (StringType, false, "Asignación del Producdto de la cuenta cuentable") 
  producto_id.setARCO_Data(false)  
  producto_id.setSecurityLevel(huemulType_SecurityLevel.Public)  
  producto_id.setMDM_EnableOldValue(true)  
  producto_id.setMDM_EnableDTLog(true)  
  producto_id.setMDM_EnableProcessLog(true)  
  producto_id.setDefaultValue(null)
  producto_id.setNullable(true) 
  
  val planCuenta_Concepto = new huemul_Columns (StringType, false, "Indica el tipo de concepto que calcula (colocacion, provision, etc)") 
  planCuenta_Concepto.setARCO_Data(false)  
  planCuenta_Concepto.setSecurityLevel(huemulType_SecurityLevel.Public)  
  planCuenta_Concepto.setMDM_EnableOldValue(true)  
  planCuenta_Concepto.setMDM_EnableDTLog(true)  
  planCuenta_Concepto.setMDM_EnableProcessLog(true)  
  planCuenta_Concepto.setDefaultValue(null)
  planCuenta_Concepto.setNullable(true) 



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
  
  val itbl_comun_negocio = new tbl_comun_negocio(huemulBigDataGov,Control)
  val fktbl_comun_negocio = new huemul_Table_Relationship(itbl_comun_negocio, true)
  fktbl_comun_negocio.AddRelationship(itbl_comun_negocio.negocio_id, negocio_id)
  
  val itbl_comun_producto = new tbl_comun_producto(huemulBigDataGov,Control)
  val fktbl_comun_producto = new huemul_Table_Relationship(itbl_comun_producto, true)
  fktbl_comun_producto.AddRelationship(itbl_comun_producto.producto_id, producto_id)
  
  
    
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

