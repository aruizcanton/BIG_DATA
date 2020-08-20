declare

cursor MTDT_TABLA
  is
SELECT
      DISTINCT TRIM(MTDT_TC_SCENARIO.TABLE_NAME) "TABLE_NAME", /*(20150907) Angel Ruiz NF. Nuevas tablas.*/
      --TRIM(MTDT_TC_SCENARIO.TABLE_NAME) "TABLE_NAME",
      --TRIM(TABLE_BASE_NAME) "TABLE_BASE_NAME",
      --TRIM(mtdt_modelo_logico.TABLESPACE) "TABLESPACE" (20150907) Angel Ruiz NF. Nuevas tablas.
      TRIM(mtdt_modelo_summary.TABLESPACE) "TABLESPACE",
      TRIM(mtdt_modelo_summary.PARTICIONADO) "PARTICIONADO"
    FROM
      --MTDT_TC_SCENARIO, mtdt_modelo_logico (20150907) Angel Ruiz NF. Nuevas tablas.
      MTDT_TC_SCENARIO, mtdt_modelo_summary
    WHERE 
    --MTDT_TC_SCENARIO.TABLE_TYPE = 'H' and
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) = trim(mtdt_modelo_logico.TABLE_NAME) and (20150907) Angel Ruiz NF. Nuevas tablas.
    trim(MTDT_TC_SCENARIO.TABLE_NAME) = trim(mtdt_modelo_summary.TABLE_NAME)
    --trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('NGA_PARQUE_ABO_MES', 'NGA_PARQUE_SVA_MES', 'NGA_PARQUE_BENEF_MES', 'NGG_TRANSACCIONES_DETAIL', 'NGA_COMIS_POS_ABO_MES', 'NGA_AJUSTE_ABO_MES', 'NGA_NOSTNDR_CONTRATOS_MES', 'NGA_ALTAS_CANAL_MES', 'NGF_PERIMETRO', 'NGA_NOSTNDR_PLANTA_MES');
    --and trim(MTDT_TC_SCENARIO.TABLE_NAME) in ('NGD_PRIMARY_OFFER')
    ;
    
  cursor MTDT_SCENARIO (table_name_in IN VARCHAR2)
  is
    SELECT 
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLE_TYPE) "TABLE_TYPE",
      TRIM(TABLE_COLUMNS) "TABLE_COLUMNS",
      TRIM(TABLE_BASE_NAME) "TABLE_BASE_NAME",
      TRIM("SELECT") "SELECT",
      TRIM ("GROUP") "GROUP",
      TRIM(FILTER) "FILTER",
      TRIM(INTERFACE_COLUMNS) "INTERFACE_COLUMNS",
      TRIM(SCENARIO) "SCENARIO",
      TRIM(VALIDA_TABLE_BASE_NAME) "VALIDA_TABLE_BASE_NAME",
      TRIM(VALIDA_TABLE_NAME) "VALIDA_TABLE_NAME",
      DATE_CREATE,
      DATE_MODIFY
    FROM 
      MTDT_TC_SCENARIO
    WHERE
      TRIM(TABLE_NAME) = table_name_in;
  
  CURSOR MTDT_TC_DETAIL (table_name_in IN VARCHAR2, scenario_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(MTDT_TC_DETAIL.TABLE_NAME) "TABLE_NAME",
      TRIM(MTDT_TC_DETAIL.TABLE_COLUMN) "TABLE_COLUMN",
      TRIM(MTDT_TC_DETAIL.TABLE_BASE_NAME) "TABLE_BASE_NAME",
      TRIM(MTDT_TC_DETAIL.SCENARIO) "SCENARIO",
      TRIM(MTDT_TC_DETAIL.OUTER) "OUTER",
      MTDT_TC_DETAIL.SEVERIDAD,
      TRIM(MTDT_TC_DETAIL.TABLE_LKUP) "TABLE_LKUP",
      TRIM(MTDT_TC_DETAIL.TABLE_COLUMN_LKUP) "TABLE_COLUMN_LKUP",
      TRIM(MTDT_TC_DETAIL.TABLE_LKUP_COND) "TABLE_LKUP_COND",
      TRIM(MTDT_TC_DETAIL.IE_COLUMN_LKUP) "IE_COLUMN_LKUP",
      TRIM(MTDT_TC_DETAIL.LKUP_COM_RULE) "LKUP_COM_RULE",
      TRIM(MTDT_TC_DETAIL.VALUE) "VALUE",
      TRIM(MTDT_TC_DETAIL.RUL) "RUL",
      TRIM(MTDT_TC_DETAIL.VALIDA_TABLE_LKUP) "VALIDA_TABLE_LKUP",
      MTDT_TC_DETAIL.DATE_CREATE,
      MTDT_TC_DETAIL.DATE_MODIFY
  FROM
      MTDT_TC_DETAIL, MTDT_MODELO_DETAIL
  WHERE
      TRIM(MTDT_TC_DETAIL.TABLE_NAME) = table_name_in and
      TRIM(MTDT_TC_DETAIL.SCENARIO) = scenario_in and
      UPPER(trim(MTDT_TC_DETAIL.TABLE_NAME)) = UPPER(trim(MTDT_MODELO_DETAIL.TABLE_NAME)) and
      UPPER(trim(MTDT_TC_DETAIL.TABLE_COLUMN)) = UPPER(trim(MTDT_MODELO_DETAIL.COLUMN_NAME))
  ORDER BY MTDT_MODELO_DETAIL.POSITION ASC;

  /* (20161228) Angel Ruiz. */
  CURSOR c_mtdt_modelo_logico_COLUMNA (table_name_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(MTDT_MODELO_DETAIL.TABLE_NAME) "TABLE_NAME",
      TRIM(MTDT_MODELO_DETAIL.COLUMN_NAME) "COLUMN_NAME",
      MTDT_MODELO_DETAIL.DATA_TYPE,
      MTDT_MODELO_DETAIL.PK,
      TRIM(MTDT_MODELO_DETAIL.NULABLE) "NULABLE",
      TRIM(MTDT_MODELO_DETAIL.VDEFAULT) "VDEFAULT",
      TRIM(MTDT_MODELO_DETAIL.INDICE) "INDICE"
    FROM MTDT_MODELO_DETAIL
    WHERE
      MTDT_MODELO_DETAIL.TABLE_NAME = table_name_in
    ORDER BY POSITION ASC;
      
  CURSOR MTDT_TC_LOOKUP (table_name_in IN VARCHAR2)
  IS
    SELECT
      DISTINCT
      TRIM(TABLE_LKUP) "TABLE_LKUP",
      TABLE_COLUMN_LKUP "TABLE_COLUMN_LKUP",
      TABLE_LKUP_COND "TABLE_LKUP_COND",
      --IE_COLUMN_LKUP "IE_COLUMN_LKUP",
      TRIM("VALUE") "VALUE"
    FROM
      MTDT_TC_DETAIL
  WHERE
      (trim(RUL) = 'LKUP' or trim(RUL) = 'LKUPC') and
      TRIM(TABLE_NAME) = table_name_in;

  CURSOR MTDT_TC_FUNCTION (table_name_in IN VARCHAR2)
  IS
    SELECT
      DISTINCT
      TRIM(TABLE_LKUP) "TABLE_LKUP",
      TABLE_COLUMN_LKUP "TABLE_COLUMN_LKUP",
      TABLE_LKUP_COND "TABLE_LKUP_COND",
      IE_COLUMN_LKUP "IE_COLUMN_LKUP",
      TRIM("VALUE") "VALUE"
    FROM
      MTDT_TC_DETAIL
  WHERE
      RUL = 'FUNCTION' and
      TRIM(TABLE_NAME) = table_name_in;
      

  reg_tabla MTDT_TABLA%rowtype;     
  reg_scenario MTDT_SCENARIO%rowtype;
  reg_detail MTDT_TC_DETAIL%rowtype;
  reg_lookup MTDT_TC_LOOKUP%rowtype;
  reg_function MTDT_TC_FUNCTION%rowtype;
  reg_modelo_logico_col c_mtdt_modelo_logico_COLUMNA%rowtype;

  
  type list_columns_primary  is table of varchar(30);
  type list_strings  IS TABLE OF VARCHAR(400);
  type lista_tablas_from is table of varchar(1500);
  type lista_condi_where is table of varchar(500);
  type list_columns_par  IS TABLE OF VARCHAR(30);

  
  lista_pk                                      list_columns_primary := list_columns_primary ();
  lista_par                                      list_columns_par := list_columns_par ();
  tipo_col                                     varchar2(50);
  primera_col                               PLS_INTEGER;
  columna                                    VARCHAR2(25000);
  prototipo_fun                             VARCHAR2(2000);
  fich_salida_load                        UTL_FILE.file_type;
  fich_salida_exchange              UTL_FILE.file_type;
  fich_salida_pkg                         UTL_FILE.file_type;
  nombre_fich_carga                   VARCHAR2(500);
  nombre_fich_exchange            VARCHAR2(500);
  nombre_fich_pkg                      VARCHAR2(500);
  lista_scenarios_presentes                                    list_strings := list_strings();
  campo_filter                                VARCHAR2(2000);
  nombre_proceso                        VARCHAR2(30);
  nombre_tabla_reducido           VARCHAR2(30);
  nombre_tabla_T                        VARCHAR2(30);
  v_nombre_particion                  VARCHAR2(30);
  --nombre_tabla_base_reducido           VARCHAR2(30);
  OWNER_SA                             VARCHAR2(60);
  OWNER_T                                VARCHAR2(60);
  OWNER_DM                            VARCHAR2(60);
  OWNER_MTDT                       VARCHAR2(60);
  NAME_DM                                VARCHAR2(60);
  OWNER_TC                              VARCHAR2(60);
  PREFIJO_DM                            VARCHAR2(60);
  
  l_FROM                                      lista_tablas_from := lista_tablas_from();
  l_FROM_solo_tablas                               lista_tablas_from := lista_tablas_from();  
  l_WHERE                                   lista_condi_where := lista_condi_where();
  l_WHERE_ON_clause                         lista_condi_where := lista_condi_where();
  v_hay_look_up                           VARCHAR2(1):='N';
  v_nombre_seqg                          VARCHAR(120):='N';
  v_bandera                                   VARCHAR2(1):='S';
  v_nombre_tabla_agr                VARCHAR2(30):='No Existe';
  v_nombre_tabla_agr_redu           VARCHAR2(30):='No Existe';
  v_nombre_proceso_agr              VARCHAR2(30);
  nombre_tabla_T_agr                VARCHAR2(30);
  v_existen_retrasados              VARCHAR2(1) := 'N';
  v_numero_indices                  PLS_INTEGER:=0;
  v_MULTIPLICADOR_PROC                   VARCHAR2(60);
  v_tipo_particionado               VARCHAR2(10);
  v_alias                           VARCHAR2(40);
  v_hay_regla_seq                   BOOLEAN:=false; /*(20170107) Angel Ruiz. NF: reglas SEQ */
  v_nombre_seq                      VARCHAR2(50); /*(20170107) Angel Ruiz. NF: reglas SEQ */
  v_nombre_campo_seq                VARCHAR2(50); /*(20170107) Angel Ruiz. NF: reglas SEQ */
  v_query_validadora                VARCHAR2(6000); /* (20170324) Angel Ruiz */
  v_alias_table_base_name           VARCHAR2(100);  /* (20170328) Angel Ruiz. */
  v_alias_table_lkup                VARCHAR2(100);  /* (20170328) Angel Ruiz. */
  v_num_secuencial                  PLS_INTEGER:=0;






begin
  /* (20141223) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  SELECT VALOR INTO NAME_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM';
  SELECT VALOR INTO OWNER_TC FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_TC';  
  SELECT VALOR INTO PREFIJO_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'PREFIJO_DM';
  SELECT VALOR INTO v_MULTIPLICADOR_PROC FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'MULTIPLICADOR_PROC';
  
  /* (20141223) FIN*/

  fich_salida_load := UTL_FILE.FOPEN ('SALIDA','Lanza_val_TBN.sh','W');
  
  UTL_FILE.put_line(fich_salida_load, '#!/bin/bash');
  UTL_FILE.put_line(fich_salida_load, '#############################################################################');
  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_load, '# Telefonica Moviles Mexico SA DE CV                                        #');
  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_load, '# Archivo    :       ' ||  'Lanza_val_TBN' || '.sh                            #');
  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_load, '# Autor      : <SYNAPSYS>.                               #');
  UTL_FILE.put_line(fich_salida_load, '# Proposito  : Shell que ejecuta los procesos del módulo QUALITY ASSURANCE.   #');
  UTL_FILE.put_line(fich_salida_load, '# Parametros :                                                              #');
  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_load, '# Ejecucion  :                                                              #');
  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_load, '# Historia : 07-Dic-2017 -> Creacion                                    #');
  UTL_FILE.put_line(fich_salida_load, '# Caja de Control - M :                                                     #');
  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_load, '# Observaciones: En caso de reproceso colocar la fecha deseada              #');
  UTL_FILE.put_line(fich_salida_load, '#                en formato YYYYMMDD la fecha minima es a dia vencido       #');
  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_load, '# Caducidad del Requerimiento :                                             #');
  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_load, '# Dependencias :                                                            #');
  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_load, '# Usuario:                                                                  #');   
  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_load, '# Telefono:                                                                 #');   
  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_load, '#############################################################################');
  UTL_FILE.put_line(fich_salida_load, '');
  UTL_FILE.put_line(fich_salida_load, '');
  UTL_FILE.put_line(fich_salida_load, '################################################################################');
  UTL_FILE.put_line(fich_salida_load, '# EJECUCION DEL PROGRAMA EN PRO C O QUERYS                                     #');
  UTL_FILE.put_line(fich_salida_load, '################################################################################');
  UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_ENTORNO}/entorno' || NAME_DM || '_MEX.sh');
  UTL_FILE.put_line(fich_salida_load, '# Comprobamos si el numero de parametros es el correcto');
  UTL_FILE.put_line(fich_salida_load, 'if [ $# -ne 2 ] ; then');
  UTL_FILE.put_line(fich_salida_load, '  SUBJECT="Numero de paramatros de entrada incorrecto. Uso: ${0} <fch_datos> <forzado>"');
  UTL_FILE.put_line(fich_salida_load, '  echo ${SUBJECT}');        
  UTL_FILE.put_line(fich_salida_load, '  exit 1');
  UTL_FILE.put_line(fich_salida_load, 'fi');
  UTL_FILE.put_line(fich_salida_load, '# Recogida de parametros');
  UTL_FILE.put_line(fich_salida_load, 'FCH_CARGA=${1}');
  UTL_FILE.put_line(fich_salida_load, 'FCH_DATOS=${1}');
  UTL_FILE.put_line(fich_salida_load, 'BAN_FORZADO=${2}');
  UTL_FILE.put_line(fich_salida_load, 'FECHA_HORA=${FCH_CARGA}_${FCH_DATOS}_`date +%Y%m%d_%H%M%S`');

  --UTL_FILE.put_line(fich_salida_load, 'echo "load_he_' || reg_tabla.TABLE_NAME || '" > ${' || NAME_DM || '_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_load, '# Comprobamos si existe el directorio de Trazas para fecha de carga');
  UTL_FILE.put_line(fich_salida_load, 'if ! [ -d ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA} ] ; then');
  UTL_FILE.put_line(fich_salida_load, '  mkdir ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
  UTL_FILE.put_line(fich_salida_load, 'fi');
  UTL_FILE.put_line(fich_salida_load, '' || NAME_DM || '_TRAZAS=${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
  UTL_FILE.put_line(fich_salida_load, 'echo "${0}" > ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_QA' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_load, 'echo "Inicia Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_TBN' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Carga: ${FCH_CARGA}"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_TBN' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Datos: ${FCH_DATOS}"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_TBN' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_load, 'echo "Forzado: ${BAN_FORZADO}"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_TBN' || '_${FECHA_HORA}' || '.log ');
  --UTL_FILE.put_line(fich_salida_sh, 'set -x');
  UTL_FILE.put_line(fich_salida_load, '#Permite los acentos y U');
  UTL_FILE.put_line(fich_salida_load, 'NLS_LANG=AMERICAN_AMERICA.WE8ISO8859P1');
  UTL_FILE.put_line(fich_salida_load, 'export NLS_LANG');
  UTL_FILE.put_line(fich_salida_load, '');
  UTL_FILE.put_line(fich_salida_load, '');
  UTL_FILE.put_line(fich_salida_load, '');
  
  fich_salida_exchange := UTL_FILE.FOPEN ('SALIDA','Lanza_val_TNM.sh','W');

  UTL_FILE.put_line(fich_salida_exchange, '#!/bin/bash');
  UTL_FILE.put_line(fich_salida_exchange, '#############################################################################');
  UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_exchange, '# Telefonica Moviles Mexico SA DE CV                                        #');
  UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_exchange, '# Archivo    :       ' ||  'Lanza_val_TNM' || '.sh                            #');
  UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_exchange, '# Autor      : <SYNAPSYS>.                               #');
  UTL_FILE.put_line(fich_salida_exchange, '# Proposito  : Shell que ejecuta los procesos del módulo QUALITY ASSURANCE.   #');
  UTL_FILE.put_line(fich_salida_exchange, '# Parametros :                                                              #');
  UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_exchange, '# Ejecucion  :                                                              #');
  UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_exchange, '# Historia : 07-Dic-2017 -> Creacion                                    #');
  UTL_FILE.put_line(fich_salida_exchange, '# Caja de Control - M :                                                     #');
  UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_exchange, '# Observaciones: En caso de reproceso colocar la fecha deseada              #');
  UTL_FILE.put_line(fich_salida_exchange, '#                en formato YYYYMMDD la fecha minima es a dia vencido       #');
  UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_exchange, '# Caducidad del Requerimiento :                                             #');
  UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_exchange, '# Dependencias :                                                            #');
  UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_exchange, '# Usuario:                                                                  #');   
  UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_exchange, '# Telefono:                                                                 #');   
  UTL_FILE.put_line(fich_salida_exchange, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_exchange, '#############################################################################');
  UTL_FILE.put_line(fich_salida_exchange, '');
  UTL_FILE.put_line(fich_salida_exchange, '');
  UTL_FILE.put_line(fich_salida_exchange, '################################################################################');
  UTL_FILE.put_line(fich_salida_exchange, '# EJECUCION DEL PROGRAMA EN PRO C O QUERYS                                     #');
  UTL_FILE.put_line(fich_salida_exchange, '################################################################################');
  UTL_FILE.put_line(fich_salida_exchange, '. ${' || NAME_DM || '_ENTORNO}/entorno' || NAME_DM || '_MEX.sh');
  UTL_FILE.put_line(fich_salida_exchange, '# Comprobamos si el numero de parametros es el correcto');
  UTL_FILE.put_line(fich_salida_exchange, 'if [ $# -ne 2 ] ; then');
  UTL_FILE.put_line(fich_salida_exchange, '  SUBJECT="Numero de paramatros de entrada incorrecto. Uso: ${0} <fch_datos> <forzado>"');
  UTL_FILE.put_line(fich_salida_exchange, '  echo ${SUBJECT}');        
  UTL_FILE.put_line(fich_salida_exchange, '  exit 1');
  UTL_FILE.put_line(fich_salida_exchange, 'fi');
  UTL_FILE.put_line(fich_salida_exchange, '# Recogida de parametros');
  UTL_FILE.put_line(fich_salida_exchange, 'FCH_CARGA=${1}');
  UTL_FILE.put_line(fich_salida_exchange, 'FCH_DATOS=${1}');
  UTL_FILE.put_line(fich_salida_exchange, 'BAN_FORZADO=${2}');
  UTL_FILE.put_line(fich_salida_exchange, 'FECHA_HORA=${FCH_CARGA}_${FCH_DATOS}_`date +%Y%m%d_%H%M%S`');

  --UTL_FILE.put_line(fich_salida_load, 'echo "load_he_' || reg_tabla.TABLE_NAME || '" > ${' || NAME_DM || '_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_exchange, '# Comprobamos si existe el directorio de Trazas para fecha de carga');
  UTL_FILE.put_line(fich_salida_exchange, 'if ! [ -d ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA} ] ; then');
  UTL_FILE.put_line(fich_salida_exchange, '  mkdir ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
  UTL_FILE.put_line(fich_salida_exchange, 'fi');
  UTL_FILE.put_line(fich_salida_exchange, '' || NAME_DM || '_TRAZAS=${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
  UTL_FILE.put_line(fich_salida_exchange, 'echo "${0}" > ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_QA' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_exchange, 'echo "Inicia Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_TNM' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_exchange, 'echo "Fecha de Carga: ${FCH_CARGA}"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_TNM' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_exchange, 'echo "Fecha de Datos: ${FCH_DATOS}"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_TNM' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_exchange, 'echo "Forzado: ${BAN_FORZADO}"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_TNM' || '_${FECHA_HORA}' || '.log ');
  --UTL_FILE.put_line(fich_salida_sh, 'set -x');
  UTL_FILE.put_line(fich_salida_exchange, '#Permite los acentos y U');
  UTL_FILE.put_line(fich_salida_exchange, 'NLS_LANG=AMERICAN_AMERICA.WE8ISO8859P1');
  UTL_FILE.put_line(fich_salida_exchange, 'export NLS_LANG');
  UTL_FILE.put_line(fich_salida_exchange, '');
  UTL_FILE.put_line(fich_salida_exchange, '');
  UTL_FILE.put_line(fich_salida_exchange, '');
  
  fich_salida_pkg := UTL_FILE.FOPEN ('SALIDA','Lanza_val_LKP.sh','W');

  UTL_FILE.put_line(fich_salida_pkg, '#!/bin/bash');
  UTL_FILE.put_line(fich_salida_pkg, '#############################################################################');
  UTL_FILE.put_line(fich_salida_pkg, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_pkg, '# Telefonica Moviles Mexico SA DE CV                                        #');
  UTL_FILE.put_line(fich_salida_pkg, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_pkg, '# Archivo    :       ' ||  'Lanza_val_LKP' || '.sh                            #');
  UTL_FILE.put_line(fich_salida_pkg, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_pkg, '# Autor      : <SYNAPSYS>.                               #');
  UTL_FILE.put_line(fich_salida_pkg, '# Proposito  : Shell que ejecuta los procesos del módulo QUALITY ASSURANCE.   #');
  UTL_FILE.put_line(fich_salida_pkg, '# Parametros :                                                              #');
  UTL_FILE.put_line(fich_salida_pkg, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_pkg, '# Ejecucion  :                                                              #');
  UTL_FILE.put_line(fich_salida_pkg, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_pkg, '# Historia : 07-Dic-2017 -> Creacion                                    #');
  UTL_FILE.put_line(fich_salida_pkg, '# Caja de Control - M :                                                     #');
  UTL_FILE.put_line(fich_salida_pkg, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_pkg, '# Observaciones: En caso de reproceso colocar la fecha deseada              #');
  UTL_FILE.put_line(fich_salida_pkg, '#                en formato YYYYMMDD la fecha minima es a dia vencido       #');
  UTL_FILE.put_line(fich_salida_pkg, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_pkg, '# Caducidad del Requerimiento :                                             #');
  UTL_FILE.put_line(fich_salida_pkg, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_pkg, '# Dependencias :                                                            #');
  UTL_FILE.put_line(fich_salida_pkg, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_pkg, '# Usuario:                                                                  #');   
  UTL_FILE.put_line(fich_salida_pkg, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_pkg, '# Telefono:                                                                 #');   
  UTL_FILE.put_line(fich_salida_pkg, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_pkg, '#############################################################################');
  UTL_FILE.put_line(fich_salida_pkg, '');
  UTL_FILE.put_line(fich_salida_pkg, '');
  UTL_FILE.put_line(fich_salida_pkg, '################################################################################');
  UTL_FILE.put_line(fich_salida_pkg, '# EJECUCION DEL PROGRAMA EN PRO C O QUERYS                                     #');
  UTL_FILE.put_line(fich_salida_pkg, '################################################################################');
  UTL_FILE.put_line(fich_salida_pkg, '. ${' || NAME_DM || '_ENTORNO}/entorno' || NAME_DM || '_MEX.sh');
  UTL_FILE.put_line(fich_salida_pkg, '# Comprobamos si el numero de parametros es el correcto');
  UTL_FILE.put_line(fich_salida_pkg, 'if [ $# -ne 2 ] ; then');
  UTL_FILE.put_line(fich_salida_pkg, '  SUBJECT="Numero de paramatros de entrada incorrecto. Uso: ${0} <fch_datos> <forzado>"');
  UTL_FILE.put_line(fich_salida_pkg, '  echo ${SUBJECT}');        
  UTL_FILE.put_line(fich_salida_pkg, '  exit 1');
  UTL_FILE.put_line(fich_salida_pkg, 'fi');
  UTL_FILE.put_line(fich_salida_pkg, '# Recogida de parametros');
  UTL_FILE.put_line(fich_salida_pkg, 'FCH_CARGA=${1}');
  UTL_FILE.put_line(fich_salida_pkg, 'FCH_DATOS=${1}');
  UTL_FILE.put_line(fich_salida_pkg, 'BAN_FORZADO=${2}');
  UTL_FILE.put_line(fich_salida_pkg, 'FECHA_HORA=${FCH_CARGA}_${FCH_DATOS}_`date +%Y%m%d_%H%M%S`');

  --UTL_FILE.put_line(fich_salida_load, 'echo "load_he_' || reg_tabla.TABLE_NAME || '" > ${' || NAME_DM || '_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_pkg, '# Comprobamos si existe el directorio de Trazas para fecha de carga');
  UTL_FILE.put_line(fich_salida_pkg, 'if ! [ -d ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA} ] ; then');
  UTL_FILE.put_line(fich_salida_pkg, '  mkdir ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
  UTL_FILE.put_line(fich_salida_pkg, 'fi');
  UTL_FILE.put_line(fich_salida_pkg, '' || NAME_DM || '_TRAZAS=${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
  UTL_FILE.put_line(fich_salida_pkg, 'echo "${0}" > ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_QA' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_pkg, 'echo "Inicia Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_LKP' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_pkg, 'echo "Fecha de Carga: ${FCH_CARGA}"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_LKP' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_pkg, 'echo "Fecha de Datos: ${FCH_DATOS}"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_LKP' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_pkg, 'echo "Forzado: ${BAN_FORZADO}"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_LKP' || '_${FECHA_HORA}' || '.log ');
  --UTL_FILE.put_line(fich_salida_sh, 'set -x');
  UTL_FILE.put_line(fich_salida_pkg, '#Permite los acentos y U');
  UTL_FILE.put_line(fich_salida_pkg, 'NLS_LANG=AMERICAN_AMERICA.WE8ISO8859P1');
  UTL_FILE.put_line(fich_salida_pkg, 'export NLS_LANG');
  UTL_FILE.put_line(fich_salida_pkg, '');
  UTL_FILE.put_line(fich_salida_pkg, '');
  UTL_FILE.put_line(fich_salida_pkg, '');


  open MTDT_TABLA;
  loop
    fetch MTDT_TABLA
    into reg_tabla;
    exit when MTDT_TABLA%NOTFOUND;
    dbms_output.put_line ('Estoy en el primero LOOP. La tabla que tengo es: ' || reg_tabla.TABLE_NAME);
    nombre_tabla_reducido := substr(reg_tabla.TABLE_NAME, 5); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
    --nombre_tabla_base_reducido := substr(reg_tabla.TABLE_BASE_NAME, 4); /* Le quito al nombre de la tabla los caracteres SA_ */
    /* Angel Ruiz (20150311) Hecho porque hay paquetes que no compilan porque el nombre es demasiado largo*/
    if (length(reg_tabla.TABLE_NAME) < 25) then
      nombre_proceso := reg_tabla.TABLE_NAME;
    else
      nombre_proceso := nombre_tabla_reducido;
    end if;
    /* (20150414) Angel Ruiz. Incidencia. El nombre de la partición es demasiado largo */
    if (length(nombre_tabla_reducido) <= 18) then
      v_nombre_particion := 'PA_' || nombre_tabla_reducido;
    else
      v_nombre_particion := nombre_tabla_reducido;
    end if;
    /* (20151112) Angel Ruiz. BUG. Si el nombre de la tabla es superior a los 19 caracteres*/
    /* El nombre d ela tabla que se crea T_*_YYYYMMDD supera los 30 caracteres y da error*/
    if (length(nombre_tabla_reducido) > 19) then
      nombre_tabla_T := substr(nombre_tabla_reducido,1, length(nombre_tabla_reducido) - (length(nombre_tabla_reducido) - 19));
    else
      nombre_tabla_T := nombre_tabla_reducido;
    end if;
    
    --UTL_FILE.put_line (fich_salida_pkg,'CREATE OR REPLACE PACKAGE ' || OWNER_DM || '.pkg_' || nombre_proceso || ' AS');
    lista_scenarios_presentes.delete;
    /******/
    /* COMIEZO LA GENERACION DEL PACKAGE DEFINITION */
    /******/
    dbms_output.put_line ('Comienzo la generacion del PACKAGE DEFINITION');

    dbms_output.put_line ('Estoy en PACKAGE IMPLEMENTATION. :-)');
    v_num_secuencial := 0;    


    
    
    /* Tercero genero los cuerpos de los metodos que implementan los escenarios */
    open MTDT_SCENARIO (reg_tabla.TABLE_NAME);
    loop
      fetch MTDT_SCENARIO
      into reg_scenario;
      exit when MTDT_SCENARIO%NOTFOUND;
      dbms_output.put_line ('Estoy en el segundo LOOP. La tabla que tengo es: ' || reg_tabla.TABLE_NAME || '. El escenario es: ' || reg_scenario.SCENARIO);
      v_hay_regla_seq:=false; /*(20170107) Angl Ruiz. NF: Reglas SEQ */
      /***************************************************************/
      /***************************************************************/
      /* (20170330) Angel Ruiz. NF: Validacion de la unicidad de la columna TABLE_BASE_NAME */
      /***************************************************************/
      /***************************************************************/
      if (reg_scenario.VALIDA_TABLE_BASE_NAME is not null)  /* Tenemos query para llevar a cabo la validacion */
      then
        v_num_secuencial := v_num_secuencial+1;
        if (regexp_instr (reg_scenario.TABLE_BASE_NAME,'[Ss][Ee][Ll][Ee][Cc][Tt]') > 0) then
          /* (20170328) Angel Ruiz. Hay que mirar si se trata de una query */
          /* Si se trata de una query entonces hay que coger su alias para componer el nombre */
          if (REGEXP_LIKE(reg_scenario.TABLE_BASE_NAME, '\) *[a-zA-Z_0-9]+$')) then
            /* (20170328) Angel Ruiz. Tenemos un alias */
            v_alias_table_base_name := trim(substr(REGEXP_SUBSTR (reg_scenario.TABLE_BASE_NAME, '\) *[a-zA-Z_0-9]+$'), 2));
          else
            v_alias_table_base_name := 'TABLE_BASE_NAME_' || v_num_secuencial;
          end if;
        else
          if (REGEXP_LIKE(trim(reg_scenario.TABLE_BASE_NAME), '^[a-zA-Z_0-9#\.&]+ +[a-zA-Z_0-9]+$') = true) then
            /* La tabla de LKUP posee Alias */
            v_alias_table_base_name := trim(substr(REGEXP_SUBSTR (reg_scenario.TABLE_BASE_NAME, ' [a-zA-Z_0-9]+$'), 2));
          else
            if (REGEXP_LIKE(reg_scenario.TABLE_BASE_NAME, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* La tabla de LKUP esta calificada */
              v_alias_table_base_name := substr(regexp_substr(reg_scenario.TABLE_BASE_NAME, '\.[a-zA-Z_0-9&]+'), 2);/*(20170109) Angel Ruiz. BUG.Depues se usa para buscar en el metadato*/
            else
              v_alias_table_base_name := reg_scenario.TABLE_BASE_NAME;
            end if;
          end if;
        end if;
        
        nombre_fich_carga := 'val_TBN_' || reg_scenario.TABLE_NAME || '_' || reg_scenario.SCENARIO || '_' || v_alias_table_base_name || '.sh';
        --nombre_fich_exchange := 'load_ex_' || reg_tabla.TABLE_NAME || '.sh';
        --nombre_fich_pkg := 'val_TBN_' || reg_scenario.TABLE_NAME || '_' || reg_scenario.SCENARIO || '_' || v_alias_table_base_name || '.sql';
        --fich_salida_load := UTL_FILE.FOPEN ('SALIDA',nombre_fich_carga,'W');
        --fich_salida_exchange := UTL_FILE.FOPEN ('SALIDA',nombre_fich_exchange,'W');
        --fich_salida_pkg := UTL_FILE.FOPEN ('SALIDA',nombre_fich_pkg,'W');
    
        UTL_FILE.put_line(fich_salida_load, 'echo "Comienzo proceso ' || nombre_fich_carga || ': `date +%Y%m%d_%H%M%S`" >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_TBN' || '_${FECHA_HORA}' || '.log');
        UTL_FILE.put_line(fich_salida_load, 'sh -x ${NGRD_CARGA}/' || nombre_fich_carga || ' ${FCH_DATOS} ${FCH_DATOS} ${BAN_FORZADO} > ${' || NAME_DM || '_TRAZAS}/' || nombre_fich_carga || '_${FCH_DATOS}_${BAN_FORZADO}.log 2>&' || '1');
        UTL_FILE.put_line(fich_salida_load, 'echo "Fin proceso ' || nombre_fich_carga || ': `date +%Y%m%d_%H%M%S`" >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_TBN' || '_${FECHA_HORA}' || '.log');
        UTL_FILE.put_line(fich_salida_load, 'FIN=`tail -1 ${' || NAME_DM || '_TRAZAS}/' || nombre_fich_carga || '_${FCH_DATOS}_${BAN_FORZADO}.log' || '`');
        UTL_FILE.put_line(fich_salida_load, 'echo "Ha terminado con ${FIN}" >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_TBN' || '_${FECHA_HORA}' || '.log');
        UTL_FILE.put_line(fich_salida_load, '');

        
      end if;   /* FIN de la generacion de las FUNCIONES DE VALIDACION DE TABLE_BASE_NAME */

      /***************************************************************/
      /***************************************************************/
      
      /***************************************************************/
      /***************************************************************/

      /***************************************************************/
      /***************************************************************/
      /* (20170411) Angel Ruiz. NF: Validacion de la unicidad de la columna TABLE_NAME */
      /***************************************************************/
      /***************************************************************/

      if (reg_scenario.VALIDA_TABLE_NAME is not null)  /* Tenemos query para llevar a cabo la validacion */
      then
        v_num_secuencial := v_num_secuencial+1;
        if (regexp_instr (reg_scenario.TABLE_BASE_NAME,'[Ss][Ee][Ll][Ee][Cc][Tt]') > 0) then
          /* (20170328) Angel Ruiz. Hay que mirar si se trata de una query */
          /* Si se trata de una query entonces hay que coger su alias para componer el nombre */
          if (REGEXP_LIKE(reg_scenario.TABLE_BASE_NAME, '\) *[a-zA-Z_0-9]+$')) then
            /* (20170328) Angel Ruiz. Tenemos un alias */
            v_alias_table_base_name := trim(substr(REGEXP_SUBSTR (reg_scenario.TABLE_BASE_NAME, '\) *[a-zA-Z_0-9]+$'), 2));
          else
            v_alias_table_base_name := 'TABLE_BASE_NAME_' || v_num_secuencial;
          end if;
        else
          if (REGEXP_LIKE(trim(reg_scenario.TABLE_BASE_NAME), '^[a-zA-Z_0-9#\.&]+ +[a-zA-Z_0-9]+$') = true) then
            /* La tabla de LKUP posee Alias */
            v_alias_table_base_name := trim(substr(REGEXP_SUBSTR (reg_scenario.TABLE_BASE_NAME, ' [a-zA-Z_0-9]+$'), 2));
          else
            if (REGEXP_LIKE(reg_scenario.TABLE_BASE_NAME, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* La tabla de LKUP esta calificada */
              v_alias_table_base_name := substr(regexp_substr(reg_scenario.TABLE_BASE_NAME, '\.[a-zA-Z_0-9&]+'), 2);/*(20170109) Angel Ruiz. BUG.Depues se usa para buscar en el metadato*/
            else
              v_alias_table_base_name := reg_scenario.TABLE_BASE_NAME;
            end if;
          end if;
        end if;
        
        nombre_fich_carga := 'val_TNM_' || reg_scenario.TABLE_NAME || '_' || reg_scenario.SCENARIO || '_' || v_alias_table_base_name || '.sh';
        --nombre_fich_exchange := 'load_ex_' || reg_tabla.TABLE_NAME || '.sh';
        --nombre_fich_pkg := 'val_TNM_' || reg_scenario.TABLE_NAME || '_' || reg_scenario.SCENARIO || '_' || v_alias_table_base_name || '.sql';
    
        


        UTL_FILE.put_line(fich_salida_exchange, 'echo "Comienzo proceso ' || nombre_fich_carga || ': `date +%Y%m%d_%H%M%S`" >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_TNM' || '_${FECHA_HORA}' || '.log');
        UTL_FILE.put_line(fich_salida_exchange, 'sh -x ${NGRD_CARGA}/' || nombre_fich_carga || ' ${FCH_DATOS} ${FCH_DATOS} ${BAN_FORZADO} > ${' || NAME_DM || '_TRAZAS}/' || nombre_fich_carga || '_${FCH_DATOS}_${BAN_FORZADO}.log 2>&' || '1');
        UTL_FILE.put_line(fich_salida_exchange, 'echo "Fin proceso ' || nombre_fich_carga || ': `date +%Y%m%d_%H%M%S`" >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_TNM' || '_${FECHA_HORA}' || '.log');
        UTL_FILE.put_line(fich_salida_exchange, 'FIN=`tail -1 ${' || NAME_DM || '_TRAZAS}/' || nombre_fich_carga || '_${FCH_DATOS}_${BAN_FORZADO}.log' || '`');
        UTL_FILE.put_line(fich_salida_exchange, 'echo "Ha terminado con ${FIN}" >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_TNM' || '_${FECHA_HORA}' || '.log');
        UTL_FILE.put_line(fich_salida_exchange, '');
        
        
      end if;   /* FIN de la generacion de las FUNCIONES DE VALIDACION DE TABLE_NAME */

      /***************************************************************/
      /***************************************************************/
      /* (20170411) Angel Ruiz. NF: FIN */
      /***************************************************************/
      /***************************************************************/

      /* (20170324) Angel Ruiz. NF. Ocurre que ahora vamos a ir registro por registro */
      /*  de cada escenario para ir generando los procesos de validación para las */
      /* tablas de LKUP_TABLE */
      open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
      primera_col := 1;
      loop
        fetch MTDT_TC_DETAIL
        into reg_detail;
        exit when MTDT_TC_DETAIL%NOTFOUND;
        if (reg_detail.VALIDA_TABLE_LKUP is not null)  /* Tenemos query para llevar a cabo la validacion */
        then
          /* (20170328) Angel Ruiz. Comprobamos si la tabla de LKUP es un query */
          dbms_output.put_line('Estoy en el campo: ' || reg_detail.TABLE_COLUMN);
          dbms_output.put_line('La tabla de LKUP es: ' || reg_detail.TABLE_LKUP);
          v_num_secuencial := v_num_secuencial+1;
          if (regexp_instr (reg_detail.TABLE_LKUP,'[Ss][Ee][Ll][Ee][Cc][Tt]') > 0) then
            /* (20170328) Angel Ruiz. Hay que mirar si se trata de una query */
            /* Si se trata de una query entonces hay que coger su alias para componer el nombre */
            if (REGEXP_LIKE(reg_detail.TABLE_LKUP, '\) *[a-zA-Z_0-9]+$')) then
              /* (20170328) Angel Ruiz. Tenemos un alias */
              v_alias_table_lkup := trim(substr(REGEXP_SUBSTR (reg_detail.TABLE_LKUP, '\) *[a-zA-Z_0-9]+$'), 2));
            else
              v_alias_table_lkup := 'TABLE_LKUP_' || v_num_secuencial;
            end if;
          else
            if (REGEXP_LIKE(trim(reg_detail.TABLE_LKUP), '^[a-zA-Z_0-9#\.&]+ +[a-zA-Z_0-9]+$') = true) then
              /* La tabla de LKUP posee Alias */
              v_alias_table_lkup := trim(REGEXP_SUBSTR(TRIM(reg_detail.TABLE_LKUP), ' +[a-zA-Z_0-9]+$'));
              
            else
              if (REGEXP_LIKE(reg_detail.TABLE_LKUP, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
                /* La tabla de LKUP esta calificada */
                v_alias_table_lkup := substr(regexp_substr(reg_detail.TABLE_LKUP, '\.[a-zA-Z_0-9&]+'), 2);/*(20170109) Angel Ruiz. BUG.Depues se usa para buscar en el metadato*/
              else
                v_alias_table_lkup := reg_detail.TABLE_LKUP;
              end if;
            end if;
          end if;
          dbms_output.put_line ('El ALIAS es: ' || v_alias_table_lkup);
          
          nombre_fich_carga := 'val_LKP_' || reg_detail.TABLE_NAME || '_' || reg_detail.SCENARIO || '_' || v_alias_table_lkup || '.sh';
          --nombre_fich_exchange := 'load_ex_' || reg_tabla.TABLE_NAME || '.sh';
          --nombre_fich_pkg := 'val_LKP_' || reg_detail.TABLE_NAME || '_' || reg_detail.SCENARIO || '_' || v_alias_table_lkup || '.sql';
      
          UTL_FILE.put_line(fich_salida_pkg, 'echo "Comienzo proceso ' || nombre_fich_carga || ': `date +%Y%m%d_%H%M%S`" >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_LKP' || '_${FECHA_HORA}' || '.log');
          UTL_FILE.put_line(fich_salida_pkg, 'sh -x ${NGRD_CARGA}/' || nombre_fich_carga || ' ${FCH_DATOS} ${FCH_DATOS} ${BAN_FORZADO} > ${' || NAME_DM || '_TRAZAS}/' || nombre_fich_carga || '_${FCH_DATOS}_${BAN_FORZADO}.log 2>&' || '1');
          UTL_FILE.put_line(fich_salida_pkg, 'echo "Fin proceso ' || nombre_fich_carga || ': `date +%Y%m%d_%H%M%S`" >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_LKP' || '_${FECHA_HORA}' || '.log');
          UTL_FILE.put_line(fich_salida_pkg, 'FIN=`tail -1 ${' || NAME_DM || '_TRAZAS}/' || nombre_fich_carga || '_${FCH_DATOS}_${BAN_FORZADO}.log' || '`');
          UTL_FILE.put_line(fich_salida_pkg, 'echo "Ha terminado con ${FIN}" >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_LKP' || '_${FECHA_HORA}' || '.log');
          UTL_FILE.put_line(fich_salida_pkg, '');


          /******/
          /* FIN DE LA GENERACION DEL sh de CARGA */
          /******/
          
          /*************************/
          /******/
          /* INICIO DE LA GENERACION DEL sh de EXCHANGE */
          /******/
          /******/
          /* FIN DE LA GENERACION DEL sh de EXCHANGE */
          /******/
          
          /*************************/
        
        end if;
      end loop;
      close MTDT_TC_DETAIL;
      
      /**************/
      /**************/
    
    end loop;
    close MTDT_SCENARIO;
        
  
    
  end loop;
  close MTDT_TABLA;
  UTL_FILE.put_line(fich_salida_load, 'echo "Fin Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_TBN' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_load, 'exit 0');
  UTL_FILE.put_line(fich_salida_exchange, 'echo "Fin Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_TNM' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_exchange, 'exit 0');
  UTL_FILE.put_line(fich_salida_pkg, 'echo "Fin Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_val_LKP' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_pkg, 'exit 0');
  
  UTL_FILE.FCLOSE (fich_salida_load);
  UTL_FILE.FCLOSE (fich_salida_exchange);
  UTL_FILE.FCLOSE (fich_salida_pkg);
  
end;

