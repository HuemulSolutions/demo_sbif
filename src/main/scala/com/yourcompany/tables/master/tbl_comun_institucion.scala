package com.yourcompany.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.control._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.StringType

/**
 * Nombre de la clase es el nombre de la tabla y nombre del archivo en HDFS
 */
class tbl_comun_institucion(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_Table(huemulBigDataGov, Control) with Serializable{
  /**********   C O N F I G U R A C I O N   D E   L A   T A B L A   ****************************************/
  //Tipo de tabla, Master y Reference son catálogos sin particiones de periodo
  this.setTableType(huemulType_Tables.Reference)
  //Base de Datos en HIVE donde será creada la tabla
  this.setDataBase(huemulBigDataGov.GlobalSettings.MASTER_DataBase)
  //Tipo de archivo que será almacenado en HDFS
  this.setStorageType(huemulType_StorageType.PARQUET)
  //Ruta en HDFS donde se guardará el archivo PARQUET
  this.setGlobalPaths(huemulBigDataGov.GlobalSettings.MASTER_SmallFiles_Path)
  //Ruta en HDFS específica para esta tabla (Globalpaths / localPath)
  this.setLocalPath("sbif/")
  
  /**********   S E T E O   I N F O R M A T I V O   ****************************************/
  //Nombre del contacto de negocio
  this.setBusiness_ResponsibleName("Nombre del usuario de negocio: Ej. Juan Perez")
  //Nombre del contacto de TI
  this.setIT_ResponsibleName("Nombre del responsable de TI: Ej. Pedro Segura")
  //Descripción de la tabla
  this.setDescription("Contiene información de instituciones de la información de EERR")
  
  
  /**********   C O L U M N A S   ****************************************/
  val ins_id = new huemul_Columns(StringType,true,"Código de institución entregado por SBIF")
  //Campo institución es Primary Key de la tabla
  ins_id.setIsPK ( true)
  //Mínimo y Máximo largo del campo es 3 caracteres
  ins_id.setDQ_MinLen ( 3)
  ins_id.setDQ_MaxLen ( 3)
  
  val ins_nombre = new huemul_Columns(StringType,true,"Nombre de la institución entregada por la SBIF")
  //Atributo no puede ser nulo
  ins_nombre.setNullable ( false)
  //Mínimo del nombre no puede ser inferior a 5
  ins_nombre.setDQ_MinLen ( 5)
  //máximo largo del nombre no puede ser mayor a 100
  ins_nombre.setDQ_MaxLen ( 100)
  
  
  //Aplica las definiciones
  this.ApplyTableDefinition()
}