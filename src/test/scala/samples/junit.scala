package samples

import org.junit._
import Assert._
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.yourcompany.tables.master._


@Test
class AppTest {
    val args: Array[String] = new Array[String](1)
    args(0) = "Environment=production,RegisterInControl=false,TestPlanMode=true"
      
    val huemulBigDataGov = new huemul_BigDataGovernance("Pruebas Inicialización de Clases",args,generador.globalSettings.Global)
    val Control = new huemul_Control(huemulBigDataGov,null)

    /****************************************************************************************/
    /**************   tbl_comun_institucion_mes  *********************/
    /****************************************************************************************/
    
    @Test
    def test_tbl_comun_institucion_mes() = assertTrue(TEST_tbl_comun_institucion_mes)
    def TEST_tbl_comun_institucion_mes: Boolean = {
      var SinError = true
      
      try {
        val Master = new tbl_comun_institucion_mes(huemulBigDataGov,Control)
        if (Master.Error_isError) {
          println(s"Codigo: ${Master.Error_Code}, Descripción: ${Master.Error_Text}")
          SinError = false
        }
      } catch {
        case e: Exception => 
          SinError = false
          println(e)
      }
      return SinError
    }



    /****************************************************************************************/
    /**************   tbl_comun_institucion  *********************/
    /****************************************************************************************/
    
    @Test
    def test_tbl_comun_institucion() = assertTrue(TEST_tbl_comun_institucion)
    def TEST_tbl_comun_institucion: Boolean = {
      var SinError = true
      
      try {
        val Master = new tbl_comun_institucion(huemulBigDataGov,Control)
        if (Master.Error_isError) {
          println(s"Codigo: ${Master.Error_Code}, Descripción: ${Master.Error_Text}")
          SinError = false
        }
      } catch {
        case e: Exception => 
          SinError = false
          println(e)
      }
      return SinError
    }

    
    /****************************************************************************************/
    /**************   tbl_sbif_eerr_mes  *********************/
    /****************************************************************************************/
    @Test
    def test_tbl_sbif_eerr_mes() = assertTrue(TEST_tbl_sbif_eerr_mes)
    def TEST_tbl_sbif_eerr_mes: Boolean = {
      var SinError = true
      try {
        val Master = new tbl_sbif_eerr_mes(huemulBigDataGov,Control)
        if (Master.Error_isError) {
          println(s"Codigo: ${Master.Error_Code}, Descripción: ${Master.Error_Text}")
          SinError = false
        }
      } catch {
        case e: Exception => 
          SinError = false
          println(e)
      }
      return SinError
    }

    
    /****************************************************************************************/
    /**************   tbl_sbif_gestion_mes  *********************/
    /****************************************************************************************/
    @Test
    def test_tbl_sbif_gestion_mes() = assertTrue(TEST_tbl_sbif_gestion_mes)
    
    def TEST_tbl_sbif_gestion_mes: Boolean = {
      var SinError = true
      try {
        val Master = new tbl_sbif_gestion_mes(huemulBigDataGov,Control)
        if (Master.Error_isError) {
          println(s"Codigo: ${Master.Error_Code}, Descripción: ${Master.Error_Text}")
          SinError = false
        }
      } catch {
        case e: Exception => 
          SinError = false
          println(e)
      }
      return SinError
    }

    
    /****************************************************************************************/
    /**************   tbl_sbif_planCuenta_mes  *********************/
    /****************************************************************************************/
    @Test
    def test_tbl_sbif_planCuenta_mes() = assertTrue(TEST_tbl_sbif_planCuenta_mes)
    def TEST_tbl_sbif_planCuenta_mes: Boolean = {
      var SinError = true
      try {
        val Master = new tbl_sbif_planCuenta_mes(huemulBigDataGov,Control)
        if (Master.Error_isError) {
          println(s"Codigo: ${Master.Error_Code}, Descripción: ${Master.Error_Text}")
          SinError = false
        }
      } catch {
        case e: Exception => 
          SinError = false
          println(e)
      }
      return SinError
    }

    
    /****************************************************************************************/
    /**************   tbl_sbif_planCuenta  *********************/
    /****************************************************************************************/
    @Test
    def test_tbl_sbif_planCuenta() = assertTrue(TEST_tbl_sbif_planCuenta)
    def TEST_tbl_sbif_planCuenta: Boolean = {
      var SinError = true
      try {
        val Master = new tbl_sbif_planCuenta(huemulBigDataGov,Control)
        if (Master.Error_isError) {
          println(s"Codigo: ${Master.Error_Code}, Descripción: ${Master.Error_Text}")
          SinError = false
        }
      } catch {
        case e: Exception => 
          SinError = false
          println(e)
      }
      return SinError
    }
    
   

}


