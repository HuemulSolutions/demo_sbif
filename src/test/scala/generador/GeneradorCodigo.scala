package generador

import org.junit._
import Assert._
import com.yourcompany.sbif.datalake._
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables.huemulType_Tables

@Test
class AppTest {
    val args: Array[String] = new Array[String](1)
    args(0) = "Environment=production,RegisterInControl=false,TestPlanMode=true"
      
    val huemulBigDataGov = new huemul_BigDataGovernance("Generador de Codigo",args,globalSettings.Global)
    val Control = new huemul_Control(huemulBigDataGov,null)

    @Test
    def testOK() = assertTrue(GeneraCod)

    def GeneraCod(): Boolean = {            
      var row_class = new raw_B1_mes(huemulBigDataGov,Control)
      row_class.GenerateInitialCode("com.yourcompany" //PackageBase
                                  , "sbif"//PackageProject
                                  , "eerr_procesa"//NewObjectName
                                  , "tbl_sbif_eerr_mes"//NewTableName
                                  
                                  , huemulType_Tables.Transaction //TableType
                                  , true //EsMes
                                  , false //AutoMapping)
                                  )                    
       return true
    }
    
}


