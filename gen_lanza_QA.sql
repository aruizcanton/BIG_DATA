declare


cursor MTDT_QUALITY_ASSURANCE
  is
    SELECT 
      TRIM(QUALITY_NAME) "QUALITY_NAME",
      TRIM(QUALITY_TYPE) "QUALITY_TYPE",
      TRIM("SELECT") "PARTE_SELECT",
      TRIM("GROUP") "PARTE_GROUP",
      TRIM(FILTER) "FILTER",
      TRIM(FILE_SPOOL) "FILE_SPOOL",
      TRIM(SCENARIO) "SCENARIO",
      TABLE_BASE_NAME
    FROM
      MTDT_QUALITY_ASSURANCE
    WHERE
      (TRIM(STATUS) = 'P' or TRIM(STATUS) = 'D')
      --and TRIM(QUALITY_NAME) in ('NGQ_CHTFTR_DURACION_VINCULACION'
      --, 'NGQ_CHTFDC_PERIODO_VINCULACION'
      --, 'NGQ_CHTFTR_SIN_ITEM'
      --)
      ;
      

      
      
      
  reg_quality MTDT_QUALITY_ASSURANCE%rowtype;      


  
  
  
  tipo_col                                     varchar2(50);
  primera_col                               PLS_INTEGER;
  columna                                    VARCHAR2(25000);
  prototipo_fun                             VARCHAR2(2000);
  fich_salida_load                        UTL_FILE.file_type;
  fich_salida_exchange              UTL_FILE.file_type;
  fich_salida_pkg                         UTL_FILE.file_type;
  nombre_fich_carga                   VARCHAR2(60);
  nombre_fich_exchange            VARCHAR2(60);
  nombre_fich_pkg                      VARCHAR2(60);
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
  v_poner_paren_cierre boolean:=false;



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

  fich_salida_load := UTL_FILE.FOPEN ('SALIDA','Lanza_QA.sh','W');
  UTL_FILE.put_line(fich_salida_load, '#!/bin/bash');
  UTL_FILE.put_line(fich_salida_load, '#############################################################################');
  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_load, '# Telefonica Moviles Mexico SA DE CV                                        #');
  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_load, '# Archivo    :       ' ||  'lanza_QA' || '.sh                            #');
  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_load, '# Autor      : <SYNAPSYS>.                               #');
  UTL_FILE.put_line(fich_salida_load, '# Proposito  : Shell que ejecuta los procesos del módulo QUALITY ASSURANCE.   #');
  UTL_FILE.put_line(fich_salida_load, '# Parametros :                                                              #');
  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_load, '# Ejecucion  :                                                              #');
  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
  UTL_FILE.put_line(fich_salida_load, '# Historia : 05-Dic-2017 -> Creacion                                    #');
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
  UTL_FILE.put_line(fich_salida_load, 'ValidaEjecucion()');
  UTL_FILE.put_line(fich_salida_load, '{');
  UTL_FILE.put_line(fich_salida_load, 'var_ejecutar_prev=`beeline --silent=true --showHeader=false --outputformat=dsv << EOF');
  UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
  UTL_FILE.put_line(fich_salida_load, 'SELECT case when EJECUTAR=''NO'' then 1 else 0 end \');
  UTL_FILE.put_line(fich_salida_load, 'FROM \');
  UTL_FILE.put_line(fich_salida_load, '(SELECT P.NOMBRE_PROCESO, ''NO'' EJECUTAR \');
  UTL_FILE.put_line(fich_salida_load, ', ROW_NUMBER() OVER (PARTITION BY TIPO_PROCESO ORDER BY M.FCH_INICIO DESC ) AS RN \');
  UTL_FILE.put_line(fich_salida_load, 'FROM ' || OWNER_MTDT ||'.MTDT_MONITOREO M, \');
  UTL_FILE.put_line(fich_salida_load, OWNER_MTDT || '.MTDT_PROCESO P \');
  UTL_FILE.put_line(fich_salida_load, 'WHERE date_format(FCH_CARGA,''yyyyMMdd'')=''${FECHA}'' \');
  UTL_FILE.put_line(fich_salida_load, 'AND  M.CVE_PROCESO=P.CVE_PROCESO \');
  UTL_FILE.put_line(fich_salida_load, 'AND NOMBRE_PROCESO=''${NOMBRE_PROCESO}'' \');
  UTL_FILE.put_line(fich_salida_load, 'AND M.CVE_RESULTADO=0 \');
  UTL_FILE.put_line(fich_salida_load, ') MONITOREO \');
  UTL_FILE.put_line(fich_salida_load, 'WHERE MONITOREO.RN=1;');
  UTL_FILE.put_line(fich_salida_load, '!quit');
  UTL_FILE.put_line(fich_salida_load, 'EOF`');
  UTL_FILE.put_line(fich_salida_load, 'BAN_EJECUTA=`echo ${var_ejecutar_prev} | sed -e ''s/\n//g'' -e ''s/\r//g'' -e ''s/^[ ]*//g'' -e ''s/[ ]*$//g''`');
  UTL_FILE.put_line(fich_salida_load, '');
  UTL_FILE.put_line(fich_salida_load, 'if [ "${BAN_EJECUTA:-SIN_VALOR}" = "SIN_VALOR" ] ; then');
  UTL_FILE.put_line(fich_salida_load, 'BAN_EJECUTA=0');
  UTL_FILE.put_line(fich_salida_load, 'fi');
  UTL_FILE.put_line(fich_salida_load, '');
  UTL_FILE.put_line(fich_salida_load, 'return 0');
  UTL_FILE.put_line(fich_salida_load, '}');
  UTL_FILE.put_line(fich_salida_load, '');
  UTL_FILE.put_line(fich_salida_load, '################################################################################');
  UTL_FILE.put_line(fich_salida_load, '# EJECUCION DEL PROGRAMA EN PRO C O QUERYS                                     #');
  UTL_FILE.put_line(fich_salida_load, '################################################################################');
  UTL_FILE.put_line(fich_salida_load, '. ${NGRD_ENTORNO}/entornoNGRD_MEX.sh');
  UTL_FILE.put_line(fich_salida_load, '################################################################################');
  UTL_FILE.put_line(fich_salida_load, '# LIBRERIAS                                                                    #');
  UTL_FILE.put_line(fich_salida_load, '################################################################################');
  UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_UTILIDADES}/UtilBD.sh');
  UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_UTILIDADES}/UtilNGRD.sh');
  UTL_FILE.put_line(fich_salida_load, '');
  UTL_FILE.put_line(fich_salida_load, 'ObtenContrasena ${BD_SID} ${BD_USER_HIVE}');
  UTL_FILE.put_line(fich_salida_load, 'BD_CLAVE_HIVE=${PASSWORD}');
  UTL_FILE.put_line(fich_salida_load, '');
  UTL_FILE.put_line(fich_salida_load, '# Comprobamos si el numero de parametros es el correcto');
  UTL_FILE.put_line(fich_salida_load, 'if [ $# -ne 2 ] ; then');
  UTL_FILE.put_line(fich_salida_load, '  DIA=`date ''+%A''`');
  UTL_FILE.put_line(fich_salida_load, '  NUM_DIA=`date ''+%d''`');
  UTL_FILE.put_line(fich_salida_load, '  DIA_EJEC=`date ''+%Y%m%d''`');
  UTL_FILE.put_line(fich_salida_load, '  Y=`grep -i VAR_DIA_CONTABILIDAD ${' || NAME_DM || '_ENTORNO}/entorno' || NAME_DM || '_MEX.sh | awk -F''='' ''{print $2}''`');
  UTL_FILE.put_line(fich_salida_load, '  DIA_EJECUCION=`grep -i VAR_DIA_EJECUCION ${' || NAME_DM || '_ENTORNO}/entornoNGRD_MEX.sh | awk -F''='' ''{print $2}''`');
  UTL_FILE.put_line(fich_salida_load, '  ULT_FECHA_VAL_EJE=`date -d "$(date -d "+1 month" "+%Y%m01") - 1 day" "+%Y%m%d"`');
  UTL_FILE.put_line(fich_salida_load, '  FCH_MARGEN_VAL=`expr ${ULT_FECHA_VAL_EJE} - 3`');
  UTL_FILE.put_line(fich_salida_load, '');
  UTL_FILE.put_line(fich_salida_load, '  if [ $NUM_DIA -eq ${DIA_EJECUCION} ]; then');
  UTL_FILE.put_line(fich_salida_load, '    echo "Proceso continua su ejecucion de cierre"');
  UTL_FILE.put_line(fich_salida_load, '    FECHA=`date ''+%Y%m%d'' --date ''-2 day''`');
  UTL_FILE.put_line(fich_salida_load, '    FCH_CARGA=${FECHA}');
  UTL_FILE.put_line(fich_salida_load, '    FCH_DATOS=${FECHA}');
  UTL_FILE.put_line(fich_salida_load, '  else');
  UTL_FILE.put_line(fich_salida_load, '    if [ $DIA = ${Y} ] && ( [ $NUM_DIA -gt "01" ] && [ ${DIA_EJEC} -lt ${ULT_FECHA_VAL_EJE} ] ); then');
  UTL_FILE.put_line(fich_salida_load, '      echo "Proceso continua su ejecucion de ensayo"');
  UTL_FILE.put_line(fich_salida_load, '      FECHA_ENSAYO=`date ''+%Y%m%d''`');
  UTL_FILE.put_line(fich_salida_load, '      FECHA_FMT_HIVE=`echo ${FECHA_ENSAYO} | awk ''{ printf "%s-%s-%s", substr($1,0,4), substr($1,5,2), substr($1,7,2) ; }''`');
  UTL_FILE.put_line(fich_salida_load, 'FECHA_PREV=`beeline --silent=true --showHeader=false --outputformat=dsv << EOF');
  UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
  UTL_FILE.put_line(fich_salida_load, 'select last_day(date_format(cast(''${FECHA_FMT_HIVE}''  as date), ''yyyy-MM-dd'')) from ${ESQUEMA_MT}.dual;');
  UTL_FILE.put_line(fich_salida_load, '!quit');
  UTL_FILE.put_line(fich_salida_load, 'EOF`');
  UTL_FILE.put_line(fich_salida_load, '      if [ $? -ne 0 ]; then');
  UTL_FILE.put_line(fich_salida_load, '        SUBJECT="${REQ_NUM}: ERROR: Al obtener la fecha."');
  UTL_FILE.put_line(fich_salida_load, '        echo "Surgio un error al obtener la fecha del sistema o el parametro no es un formato de fecha YYYYMMDD." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
  UTL_FILE.put_line(fich_salida_load, '        echo `date`');
  UTL_FILE.put_line(fich_salida_load, '        exit 1');
  UTL_FILE.put_line(fich_salida_load, '      fi');
  UTL_FILE.put_line(fich_salida_load, '      FECHAprev=`echo ${FECHA_PREV} | sed -e ''s/\n//g'' -e ''s/\r//g'' -e ''s/^[ ]*//g'' -e ''s/[ ]*$//g''`');
  UTL_FILE.put_line(fich_salida_load, '      FECHA_SIN_FMT=`echo ${FECHAprev} | awk ''{ printf "%s%s%s", substr($1,0,4), substr($1,6,2), substr($1,9,2) ; }''`');
  UTL_FILE.put_line(fich_salida_load, '      FECHA=${FECHA_SIN_FMT}');
  UTL_FILE.put_line(fich_salida_load, '      FCH_CARGA=${FECHA}');
  UTL_FILE.put_line(fich_salida_load, '      FCH_DATOS=${FECHA}');
  UTL_FILE.put_line(fich_salida_load, '      echo "Fecha a considerar ${FECHA}"');
  UTL_FILE.put_line(fich_salida_load, '    else');
  UTL_FILE.put_line(fich_salida_load, '      echo "No es un dia  para ejecutar"');
  UTL_FILE.put_line(fich_salida_load, '      exit 0');
  UTL_FILE.put_line(fich_salida_load, '    fi');
  UTL_FILE.put_line(fich_salida_load, '  fi');
  UTL_FILE.put_line(fich_salida_load, '  BAN_FORZADO="F"');
  UTL_FILE.put_line(fich_salida_load, 'else');
  UTL_FILE.put_line(fich_salida_load, '  # Recogida de parametros');
  UTL_FILE.put_line(fich_salida_load, '  FCH_CARGA=${1}');
  UTL_FILE.put_line(fich_salida_load, '  FCH_DATOS=${1}');
  UTL_FILE.put_line(fich_salida_load, '  BAN_FORZADO=${2}');
  UTL_FILE.put_line(fich_salida_load, 'fi');
  UTL_FILE.put_line(fich_salida_load, '');
  UTL_FILE.put_line(fich_salida_load, '');
  UTL_FILE.put_line(fich_salida_load, '');
  UTL_FILE.put_line(fich_salida_load, 'FECHA_HORA=${FCH_CARGA}_${FCH_DATOS}_`date +%Y%m%d_%H%M%S`');
  --UTL_FILE.put_line(fich_salida_load, 'echo "load_he_' || reg_tabla.TABLE_NAME || '" > ${' || NAME_DM || '_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_load, '# Comprobamos si existe el directorio de Trazas para fecha de carga');
  UTL_FILE.put_line(fich_salida_load, 'if ! [ -d ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA} ] ; then');
  UTL_FILE.put_line(fich_salida_load, '  mkdir ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
  UTL_FILE.put_line(fich_salida_load, 'fi');
  UTL_FILE.put_line(fich_salida_load, '' || NAME_DM || '_TRAZAS=${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
  UTL_FILE.put_line(fich_salida_load, 'echo "${0}" > ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_QA' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_load, 'echo "Inicia Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_QA' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Carga: ${FCH_CARGA}"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_QA' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Datos: ${FCH_DATOS}"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_QA' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_load, 'echo "Forzado: ${BAN_FORZADO}"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_QA' || '_${FECHA_HORA}' || '.log ');
  --UTL_FILE.put_line(fich_salida_sh, 'set -x');
  UTL_FILE.put_line(fich_salida_load, '#Permite los acentos y U');
  UTL_FILE.put_line(fich_salida_load, 'NLS_LANG=AMERICAN_AMERICA.WE8ISO8859P1');
  UTL_FILE.put_line(fich_salida_load, 'export NLS_LANG');
  UTL_FILE.put_line(fich_salida_load, '');
  UTL_FILE.put_line(fich_salida_load, '');
  
  open MTDT_QUALITY_ASSURANCE;
  loop
    fetch MTDT_QUALITY_ASSURANCE
    into reg_quality;
    exit when MTDT_QUALITY_ASSURANCE%NOTFOUND;
    dbms_output.put_line ('Estoy en el primer LOOP. La VALIDACION que tengo es: ' || reg_quality.QUALITY_NAME);
    nombre_fich_carga := reg_quality.QUALITY_NAME || '.sh';
    --nombre_fich_exchange := 'load_ex_' || reg_tabla.TABLE_NAME || '.sh';
    nombre_fich_pkg := reg_quality.QUALITY_NAME || '.sql';
    --fich_salida_load := UTL_FILE.FOPEN ('SALIDA',nombre_fich_carga,'W');
    --fich_salida_exchange := UTL_FILE.FOPEN ('SALIDA',nombre_fich_exchange,'W');
    --fich_salida_pkg := UTL_FILE.FOPEN ('SALIDA',nombre_fich_pkg,'W');
    --nombre_tabla_reducido := substr(reg_tabla.TABLE_NAME, 10); /* Le quito al nombre de la validacion los caracteres iniciales NGQ_XXXX_ */
    /* Angel Ruiz (20150311) Hecho porque hay paquetes que no compilan porque el nombre es demasiado largo*/
    if (length(reg_quality.QUALITY_NAME) < 25) then
      nombre_proceso := reg_quality.QUALITY_NAME;
    else
      nombre_proceso := nombre_tabla_reducido;
    end if;
    
    

    /******/
    /* INICIO DE LA GENERACION DEL sh de CARGA */
    /******/
    UTL_FILE.put_line(fich_salida_load, 'NOMBRE_PROCESO="' || nombre_fich_carga || '"');    
    UTL_FILE.put_line(fich_salida_load, 'ValidaEjecucion ${NOMBRE_PROCESO}');    
    UTL_FILE.put_line(fich_salida_load, 'if [ ${BAN_EJECUTA} -eq 0 ] ; then');    
    UTL_FILE.put_line(fich_salida_load, '  echo "Comienzo proceso ${NOMBRE_PROCESO}: `date +%Y%m%d_%H%M%S`" >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_QA' || '_${FECHA_HORA}' || '.log');
    UTL_FILE.put_line(fich_salida_load, '  sh -x ${NGRD_CARGA}/${NOMBRE_PROCESO}' || ' ${FCH_DATOS} ${FCH_DATOS} ${BAN_FORZADO} > ${' || NAME_DM || '_TRAZAS}/${NOMBRE_PROCESO}' || '_${FCH_DATOS}_${BAN_FORZADO}.log 2>&' || '1');
    UTL_FILE.put_line(fich_salida_load, '  echo "Fin proceso ${NOMBRE_PROCESO}' || ': `date +%Y%m%d_%H%M%S`" >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_QA' || '_${FECHA_HORA}' || '.log');
    UTL_FILE.put_line(fich_salida_load, '  FIN=`tail -1 ${' || NAME_DM || '_TRAZAS}/${NOMBRE_PROCESO}' || '_${FCH_DATOS}_${BAN_FORZADO}.log' || '`');
    UTL_FILE.put_line(fich_salida_load, '  echo "Ha terminado con ${FIN}" >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_QA' || '_${FECHA_HORA}' || '.log');
    UTL_FILE.put_line(fich_salida_load, 'else');
    UTL_FILE.put_line(fich_salida_load, '  echo "Este proceso ya se ha ejecutado ${NOMBRE_PROCESO}" >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_QA' || '_${FECHA_HORA}' || '.log');
    UTL_FILE.put_line(fich_salida_load, 'fi');    
    UTL_FILE.put_line(fich_salida_load, '');    
    
    /******/
    /* FIN DE LA GENERACION DEL sh de CARGA */
    /******/
  end loop;
  close MTDT_QUALITY_ASSURANCE;
  UTL_FILE.put_line(fich_salida_load, 'echo "Fin Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${' || NAME_DM || '_TRAZAS}/' || 'Lanza_QA' || '_${FECHA_HORA}' || '.log ');
  UTL_FILE.put_line(fich_salida_load, 'exit 0');
  UTL_FILE.FCLOSE (fich_salida_load);
  --UTL_FILE.FCLOSE (fich_salida_exchange);
  UTL_FILE.FCLOSE (fich_salida_pkg);
end;

