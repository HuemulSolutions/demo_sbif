# demo_1.0_sbif
Proyecto demostraciÃ³n de Huemul BigDataGovernance versiÃ³n 1.0


## Forma de ejecución
El siguiente ejemplo muestra la forma de ejecutar el código:

```shell
spark-submit --master yarn --jars huemul-bigdatagovernance-2.0.1.jar,huemul-sql-decode-1.0.jar,demo_settings-2.0.jar,postgresql-9.4.1212.jar,mssql-jdbc-7.2.1.jre8.jar --class com.yourcompany.catalogo.process_negocio  demo_catalogo-2.0.jar environment=production,debugmode=false,ano=2018,mes=6
```

spark-submit --master yarn --jars huemul-bigdatagovernance-2.0.1.jar,huemul-sql-decode-1.0.jar,demo_settings-2.0.jar,postgresql-9.4.1212.jar,mssql-jdbc-7.2.1.jre8.jar --class com.yourcompany.catalogo.process_producto  demo_catalogo-2.0.jar environment=production,debugmode=false,ano=2018,mes=6


spark-submit --master yarn --jars huemul-bigdatagovernance-2.0.1.jar,huemul-sql-decode-1.0.jar,demo_catalogo-2.0.jar,demo_settings-2.0.jar,postgresql-9.4.1212.jar,mssql-jdbc-7.2.1.jre8.jar --class com.yourcompany.sbif.process_mensual  demo_sbif-2.0.jar environment=production,debugmode=false,ano=2018,mes=6


spark-submit --master yarn --jars huemul-bigdatagovernance-2.0.1.jar,huemul-sql-decode-1.0.jar,demo_settings-2.0.jar,demo_catalogo-2.0.jar,postgresql-9.4.1212.jar,mssql-jdbc-7.2.1.jre8.jar --class com.yourcompany.sbif.process_planCuenta  demo_sbif-2.0.jar environment=production,debugmode=false,ano=2018,mes=6

spark-submit --master yarn --jars huemul-bigdatagovernance-2.0.1.jar,huemul-sql-decode-1.0.jar,demo_settings-2.0.jar,demo_catalogo-2.0.jar,postgresql-9.4.1212.jar,mssql-jdbc-7.2.1.jre8.jar --class com.yourcompany.sbif.process_planCuenta_mes  demo_sbif-2.0.jar environment=production,debugmode=false,ano=2018,mes=6

spark-submit --master yarn --jars huemul-bigdatagovernance-2.0.1.jar,huemul-sql-decode-1.0.jar,demo_settings-2.0.jar,demo_catalogo-2.0.jar,postgresql-9.4.1212.jar,mssql-jdbc-7.2.1.jre8.jar --class com.yourcompany.sbif.process_institucion  demo_sbif-2.0.jar environment=production,debugmode=false,ano=2018,mes=6

spark-submit --master yarn --jars huemul-bigdatagovernance-2.0.1.jar,huemul-sql-decode-1.0.jar,demo_settings-2.0.jar,demo_catalogo-2.0.jar,postgresql-9.4.1212.jar,mssql-jdbc-7.2.1.jre8.jar --class com.yourcompany.sbif.process_institucion_mes  demo_sbif-2.0.jar environment=production,debugmode=false,ano=2018,mes=6