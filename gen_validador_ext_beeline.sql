declare

cursor MTDT_TABLA
  is
SELECT
      DISTINCT TRIM(MTDT_EXT_SCENARIO.TABLE_NAME) "TABLE_NAME" /*(20150907) Angel Ruiz NF. Nuevas tablas.*/
    FROM
      --MTDT_TC_SCENARIO, mtdt_modelo_logico (20150907) Angel Ruiz NF. Nuevas tablas.
      MTDT_EXT_SCENARIO
    --WHERE MTDT_EXT_SCFORMA_PAGOENARIO.TABLE_TYPE = 'F' and
    WHERE
      (trim(MTDT_EXT_SCENARIO.STATUS) = 'P' or trim(MTDT_EXT_SCENARIO.STATUS) = 'D')
      and trim(MTDT_EXT_SCENARIO.TABLE_NAME) in (
      --'EQUIPO', 'REGION_COMERCIAL_NIVEL3', 'REGION_COMERCIAL_NIVEL2', 'REGION_COMERCIAL_NIVEL1', 'PRIMARY_OFFER'
      --, 'PARQUE_ABO_MES', 'SUPPLEMENTARY_OFFER', 'BONUS', 'HANDSET_PRICE', 'PARQUE_SVA_MES', 'PARQUE_BENEF_MES', 'PSD_RESIDENCIAL'
      --, 'OFFER_ITEM', 'MOVIMIENTOS_ABO_MES', 'MOVIMIENTO_ABO', 'COMIS_POS_ABO_MES', 'AJUSTE_ABO_MES'
      --'OFERTA', 'TRANSACCIONES' , 'PRECIOS', 'MOVIMIENTOS_PLANTA', 'CONFIGURACION_CONTRATO');
      'TRANSACCIONES', 'CONFIGURACION_CONTRATO', 'OFERTA', 'PRECIOS', 'MOVIMIENTOS_PLANTA');
    
    --and trim(MTDT_EXT_SCENARIO.TABLE_NAME) in ('PARQUE_PROMO_CAMPANA', 'MOV_PROMO_CAMPANA'
    --  );
      --and trim(MTDT_EXT_SCENARIO.TABLE_NAME) in ('PARQUE_ABO_PRE', 'PARQUE_ABO_POST', 'CLIENTE', 'CUENTA', 
    --'MOVIMIENTO_ABO', 'PLAN_TARIFARIO',
    --'CATEGORIA_CLIENTE', 'CICLO', 'ESTATUS_OPERACION', 'FORMA_PAGO', 'PROMOCION', 'SEGMENTO_CLIENTE', 
    --'GRUPO_ABONADO', 'REL_GRUPO_ABONADO');
    --and trim(MTDT_EXT_SCENARIO.TABLE_NAME) in ('TRAF_TARIFICADO_VOZ_POST');
  cursor MTDT_SCENARIO (table_name_in IN VARCHAR2)
  is
    SELECT 
      TRIM(MTDT_EXT_SCENARIO.TABLE_NAME) "TABLE_NAME",
      TRIM(MTDT_EXT_SCENARIO.TABLE_TYPE) "TABLE_TYPE",
      TRIM(MTDT_EXT_SCENARIO.TABLE_BASE_NAME) "TABLE_BASE_NAME",
      TRIM(MTDT_EXT_SCENARIO.HINT) "HINT",
      TRIM(MTDT_EXT_SCENARIO.OVER_PARTION) "OVER_PARTION",
      TRIM(MTDT_EXT_SCENARIO.DISTINCT_COL) "DISTINCT_COL",
      TRIM(MTDT_EXT_SCENARIO."SELECT") "SELECT",
      TRIM (MTDT_EXT_SCENARIO."GROUP") "COLUMNA_GROUP",
      TRIM(MTDT_EXT_SCENARIO.FILTER) "FILTER",
      TRIM(MTDT_EXT_SCENARIO.INTERFACE_COLUMNS) "INTERFACE_COLUMNS",
      TRIM(MTDT_EXT_SCENARIO.SCENARIO) "SCENARIO",
      TRIM(MTDT_INTERFACE_SUMMARY.INTERFACE_NAME) "INTERFACE_NAME",
      TRIM(MTDT_INTERFACE_SUMMARY.TYPE) "TYPE",
      TRIM(MTDT_INTERFACE_SUMMARY.SEPARATOR) "SEPARATOR",
      TRIM(MTDT_INTERFACE_SUMMARY.SOURCE) "SOURCE"
    FROM 
      MTDT_EXT_SCENARIO, MTDT_INTERFACE_SUMMARY
    WHERE
      trim(MTDT_EXT_SCENARIO.TABLE_NAME) = table_name_in and
      trim(MTDT_EXT_SCENARIO.TABLE_NAME) = trim(MTDT_INTERFACE_SUMMARY.CONCEPT_NAME) and
      (trim(MTDT_EXT_SCENARIO.STATUS) = 'P' or trim(MTDT_EXT_SCENARIO.STATUS) = 'D') and
      (MTDT_INTERFACE_SUMMARY.STATUS = 'P' or MTDT_INTERFACE_SUMMARY.STATUS = 'D')
    ORDER BY MTDT_EXT_SCENARIO.TABLE_TYPE;
  
  CURSOR MTDT_TC_DETAIL (table_name_in IN VARCHAR2, scenario_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(MTDT_EXT_DETAIL.TABLE_NAME) "TABLE_NAME",
      TRIM(MTDT_EXT_DETAIL.TABLE_COLUMN) "TABLE_COLUMN",
      TRIM(MTDT_EXT_DETAIL.TABLE_BASE_NAME) "TABLE_BASE_NAME",
      TRIM(MTDT_EXT_DETAIL.SCENARIO) "SCENARIO",
      NVL(UPPER(TRIM(MTDT_EXT_DETAIL.OUTER)), 'N') "OUTER",
      MTDT_EXT_DETAIL.SEVERIDAD,
      TRIM(MTDT_EXT_DETAIL.TABLE_LKUP) "TABLE_LKUP",
      TRIM(MTDT_EXT_DETAIL.TABLE_COLUMN_LKUP) "TABLE_COLUMN_LKUP",
      TRIM(MTDT_EXT_DETAIL.TABLE_LKUP_COND) "TABLE_LKUP_COND",
      TRIM(MTDT_EXT_DETAIL.IE_COLUMN_LKUP) "IE_COLUMN_LKUP",
      TRIM(MTDT_EXT_DETAIL.LKUP_COM_RULE) "LKUP_COM_RULE",
      TRIM(MTDT_EXT_DETAIL.VALUE) "VALUE",
      TRIM(MTDT_EXT_DETAIL.RUL) "RUL",
      TRIM(MTDT_EXT_DETAIL.VALIDA_TABLE_LKUP) "VALIDA_TABLE_LKUP",
      TRIM(MTDT_INTERFACE_DETAIL.COLUMNA) "COLUMNA",
      TRIM(MTDT_INTERFACE_DETAIL.TYPE) "TYPE",
      TRIM(MTDT_INTERFACE_DETAIL.LENGTH) "LONGITUD",
      TRIM(MTDT_INTERFACE_DETAIL.POSITION) "POSITION"
  FROM
      MTDT_EXT_DETAIL, MTDT_INTERFACE_DETAIL
  WHERE
      TRIM(MTDT_EXT_DETAIL.TABLE_NAME) = table_name_in and
      TRIM(MTDT_EXT_DETAIL.SCENARIO) = scenario_in and
      (trim(MTDT_EXT_DETAIL.STATUS) = 'P' or trim(MTDT_EXT_DETAIL.STATUS) = 'D') and
      trim(MTDT_EXT_DETAIL.TABLE_NAME) = trim(MTDT_INTERFACE_DETAIL.CONCEPT_NAME) and
      trim(MTDT_EXT_DETAIL.TABLE_COLUMN) = trim (MTDT_INTERFACE_DETAIL.COLUMNA) and
      (trim(MTDT_INTERFACE_DETAIL.STATUS) = 'P' or trim(MTDT_INTERFACE_DETAIL.STATUS) = 'D')
  ORDER BY
      MTDT_INTERFACE_DETAIL.POSITION;
  /* (20160606) Angel Ruiz. NF: Validacion de tipo I. De la extraccion se inserta */
  /* directamente a las tablas de Staging Area */
  CURSOR MTDT_INTERFAZ_DETAIL (concep_name_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(CONCEPT_NAME) "CONCEPT_NAME",
      TRIM(SOURCE) "SOURCE",
      TRIM(COLUMNA) "COLUMNA",
      TRIM(KEY) "KEY",
      TRIM(TYPE) "TYPE",
      TRIM(LENGTH) "LONGITUD",
      TRIM(NULABLE) "NULABLE",
      POSITION,
      TRIM(FORMAT) "FORMAT"
    FROM
      MTDT_INTERFACE_DETAIL
    WHERE
      TRIM(CONCEPT_NAME) = concep_name_in and
      (trim(STATUS) = 'P' or trim(STATUS) = 'D')
    ORDER BY POSITION;
    
  /* (20160606) Angel Ruiz. Fin NF */  
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
      MTDT_EXT_DETAIL
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
      MTDT_EXT_DETAIL
  WHERE
      RUL = 'FUNCTION' and
      TRIM(TABLE_NAME) = table_name_in;
      

  reg_tabla MTDT_TABLA%rowtype;     
  reg_scenario MTDT_SCENARIO%rowtype;
  reg_detail MTDT_TC_DETAIL%rowtype;
  reg_lookup MTDT_TC_LOOKUP%rowtype;
  reg_function MTDT_TC_FUNCTION%rowtype;
  reg_interface_detail MTDT_INTERFAZ_DETAIL%rowtype;
  
  type list_columns_primary  is table of varchar(30);
  type list_strings  IS TABLE OF VARCHAR(400);
  type lista_tablas_from is table of varchar(20000); /* [URC] se cambia longitud de 2000 a 4000 */
  type lista_condi_where is table of varchar(4000); /* [URC] se cambia longitud de 1000 a 4000 */

  
  lista_pk                                      list_columns_primary := list_columns_primary (); 
  tipo_col                                     varchar2(50);
  primera_col                               PLS_INTEGER;
  columna                                    VARCHAR2(4000); -- URC [ Incremento Longitud de 2000 a 4000 por ORA-06502: PL/SQL: error : character string buffer too small numérico o de valor]
  prototipo_fun                             VARCHAR2(2000);
  fich_salida_load                        UTL_FILE.file_type;
  fich_salida_exchange              UTL_FILE.file_type;
  fich_salida_pkg                         UTL_FILE.file_type;
  fich_salida_load_desde_stage             UTL_FILE.file_type;
  fich_salida_pkg_desde_stage              UTL_FILE.file_type;
  nombre_fich_carga                   VARCHAR2(60);
  nombre_fich_exchange            VARCHAR2(60);
  nombre_fich_pkg                      VARCHAR2(60);
  nombre_fich_carga_desde_stage   VARCHAR2(60);
  nombre_fich_pkg_desde_stage     VARCHAR2(60);
  lista_scenarios_presentes                                    list_strings := list_strings();
  v_lista_elementos_scenario        list_strings := list_strings();     

  campo_filter                                VARCHAR2(10000);
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
  REQ_NUMBER                            VARCHAR2(60);
  OWNER_1                               VARCHAR2(60);
  OWNER_2                               VARCHAR2(60);
  OWNER_3                               VARCHAR2(60);
  OWNER_4                               VARCHAR2(60);
  OWNER_5                               VARCHAR2(60);
  BD_SID                                VARCHAR2(60);
  BD_USR                                VARCHAR2(60);
  OWNER_EX                              VARCHAR2(60);
  
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
  v_type                            VARCHAR2(1);
  v_line_size                       INTEGER;
  v_tabla_dinamica                  BOOLEAN;
  v_num_scenarios                   PLS_integer:=0;
  v_hay_sce_COMPUESTO               BOOLEAN := false;
  v_fecha_ini_param                 BOOLEAN:=false;
  v_fecha_fin_param                 BOOLEAN:=false;
  v_long_total                      PLS_INTEGER;
  v_long_parte_decimal              PLS_INTEGER;
  v_mascara                         VARCHAR2(100);
  v_fuente                          varchar2(20);
  v_interface_name                  varchar2(80);
  nombre_interface_a_cargar   VARCHAR2(150);
  nom_inter_a_cargar_sin_fecha       VARCHAR2(150);
  pos_ini_pais                             PLS_integer;
  pos_fin_pais                             PLS_integer;
  pos_ini_fecha                           PLS_integer;
  pos_fin_fecha                           PLS_integer;
  pos_ini_hora                              PLS_integer;
  pos_fin_hora                              PLS_integer;
  pos_ini_mes                             PLS_integer;
  pos_fin_mes                             PLS_integer;
  v_country                            varchar2(20);
  v_type_validation                   varchar2(1);
  v_separador_campos                VARCHAR2(1);
  v_contador                        PLS_integer;
  v_num_campos_interfaz PLS_integer;
  v_usuario_owner                   VARCHAR2(50);
  v_hay_usu_owner                   boolean:=false;
  v_multiplicador_proc              varchar2(60);
  v_separator                       varchar2(3);
  v_ip_productivo                   varchar2(20);
  v_frequency     varchar2(1);
  v_alias_table_lkup                VARCHAR2(100); /* (20170905) Angel Ruiz.*/
  v_query_validadora                VARCHAR2(5000); /* (20170905) Angel Ruiz */
  v_num_secuencial                  PLS_INTEGER:=0; /* (20170905) Angel Ruiz */
  
  


/************/
/*************/
/* (20150918) Angel Ruiz. NUEVA FUNCION */
  function sustituye_comillas_dinam (cadena_in in varchar2) return varchar2
  is
    lon_cadena integer;
    cabeza                varchar2 (1000);
    sustituto              varchar2(100);
    cola                      varchar2(1000);    
    pos                   PLS_integer;
    pos_ant           PLS_integer;
    posicion_ant           PLS_integer;
    cadena_resul varchar(1000);
    begin
      lon_cadena := length (cadena_in);
      pos := 0;
      posicion_ant := 0;
      cadena_resul:= cadena_in;
      if lon_cadena > 0 then
        /* Busco LA COMILLA */
        pos := 0;
        posicion_ant := 0;
        sustituto := '''''';
        loop
          dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '''', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length (''''));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
          pos_ant := pos + length ('''''');
          pos := pos_ant;
        end loop;
      end if;
      return cadena_resul;
    end;

/************/
  /* (20160425) Angel Ruiz. Funcion que en caso de que la cadena que se le pase */
  /* posee la forma BETWEEN CAMPO1 AND CAMPO2 */
  /* retorna BETWEEN ALIAS_IN.CAMPO1 AND ALIAS_IN.CAMPO2 */
  function transformo_between (alias_in varchar2, cadena_in in varchar2, outer_in boolean) return varchar2
  is
    v_campo1 varchar2(50);
    v_campo2 varchar2(50);
    v_cadena varchar2(500);
    v_cadena_out varchar2(600);
  begin
    v_cadena := trim(cadena_in);
    if (REGEXP_LIKE(trim(cadena_in), '^BETWEEN +[A-Z_0-9]+ +AND +[A-Z_0-9]+$') = true) then
      /* El METADATO se ha escrito correctamente por lo que que podemos seguir */
      v_campo1 := trim(REGEXP_SUBSTR(v_cadena, ' +[A-Z_0-9]+',1,1));
      v_campo2 := trim(REGEXP_SUBSTR(v_cadena, ' +[A-Z_0-9]+',1,3));
      if (outer_in = false) then
        v_cadena_out := 'BETWEEN ' || alias_in || '.' || v_campo1 || ' AND ' || alias_in || '.' || v_campo2;
      else
        v_cadena_out := 'BETWEEN ' || alias_in || '.' || v_campo1 || ' (+) AND ' || alias_in || '.' || v_campo2 || ' (+)';
      end if;
    else
      v_cadena_out := cadena_in;
    end if;
    return v_cadena_out;
  end transformo_between;
  function cambio_puntoYcoma_por_coma (cadena_in in varchar2) return varchar2
  is
    lon_cadena integer;
    cabeza                varchar2 (1000);
    sustituto              varchar2(1000);
    cola                      varchar2(1000);    
    pos                   PLS_integer;
    pos_ant           PLS_integer;
    posicion_ant           PLS_integer;
    cadena_resul varchar(1000);
  begin
    lon_cadena := length (cadena_in);
    pos := 0;
    posicion_ant := 0;
    cadena_resul:= cadena_in;
    if (lon_cadena > 0) then
      /* Busco VAR_FUN_NAME_LOOKUP */
      sustituto := ',';
      loop
        dbms_output.put_line ('Entro en el LOOP de cambio_puntoYcoma_por_coma. La cadena es: ' || cadena_resul);
        pos := instr(cadena_resul, ';', pos+1);
        exit when pos = 0;
        dbms_output.put_line ('Pos es mayor que 0');
        dbms_output.put_line ('Primer valor de Pos: ' || pos);
        cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
        dbms_output.put_line ('La cabeza es: ' || cabeza);
        dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
        cola := substr(cadena_resul, pos + length (';'));
        dbms_output.put_line ('La cola es: ' || cola);
        cadena_resul := cabeza || sustituto || cola;
        --pos_ant := pos + length (' to_date ( fch_datos_in, ''yyyymmdd'') ');
        --pos := pos_ant;
      end loop;
    end if;  
    return cadena_resul;
  end cambio_puntoYcoma_por_coma;

  function split_string_punto_coma ( cadena_in in varchar2) return list_strings
  is
  lon_cadena integer;
  elemento varchar2 (400);
  pos integer;
  pos_ant integer;
  lista_elementos                                      list_strings := list_strings (); 
  begin
    lon_cadena := length (cadena_in);
    pos := 0;
    pos_ant := 0;
    if lon_cadena > 0 then
      loop
              dbms_output.put_line ('Entro en el LOOP. La cedena es: ' || cadena_in);
              if pos < lon_cadena then
                pos := instr(cadena_in, ';', pos+1);
              else
                pos := 0;
              end if;
              dbms_output.put_line ('Primer valor de Pos: ' || pos);
              if pos > 0 then
                dbms_output.put_line ('Pos es mayor que 0');
                dbms_output.put_line ('Pos es:' || pos);
                dbms_output.put_line ('Pos_ant es:' || pos_ant);
                elemento := substr(cadena_in, pos_ant+1, (pos - pos_ant) -1);
                dbms_output.put_line ('El elemento es: ' || elemento);
                lista_elementos.EXTEND;
                lista_elementos(lista_elementos.LAST) := UPPER(LTRIM(RTRIM (elemento)));
                pos_ant := pos;
              end if;
       exit when pos = 0;
      end loop;
      lista_elementos.EXTEND;
      lista_elementos(lista_elementos.LAST) := UPPER(LTRIM(RTRIM (substr(cadena_in, pos_ant+1, lon_cadena))));
      dbms_output.put_line ('El ultimo elemento es: ' || UPPER(LTRIM(RTRIM (substr(cadena_in, pos_ant+1, lon_cadena)))));
    end if;
    return lista_elementos;
  end split_string_punto_coma;  
  
  function split_string_coma ( cadena_in in varchar2) return list_strings
  is
  lon_cadena integer;
  elemento varchar2 (50);
  pos integer;
  pos_ant integer;
  lista_elementos                                      list_strings := list_strings (); 
  begin
    lon_cadena := length (cadena_in);
    pos := 0;
    pos_ant := 0;
    if lon_cadena > 0 then
      loop
              dbms_output.put_line ('Entro en el LOOP. La cedena es: ' || cadena_in);
              if pos < lon_cadena then
                pos := instr(cadena_in, ',', pos+1);
              else
                pos := 0;
              end if;
              dbms_output.put_line ('Primer valor de Pos: ' || pos);
              if pos > 0 then
                dbms_output.put_line ('Pos es mayor que 0');
                elemento := substr(cadena_in, pos_ant+1, (pos - pos_ant)-1);
                dbms_output.put_line ('El elemento es: ' || elemento);
                lista_elementos.EXTEND;
                lista_elementos(lista_elementos.LAST) := UPPER(LTRIM(RTRIM (elemento)));
                pos_ant := pos;
              end if;
       exit when pos = 0;
      end loop;
      lista_elementos.EXTEND;
      lista_elementos(lista_elementos.LAST) := UPPER(LTRIM(RTRIM (substr(cadena_in, pos_ant+1, lon_cadena))));
      dbms_output.put_line ('El ultimo elemento es: ' || UPPER(LTRIM(RTRIM (substr(cadena_in, pos_ant+1, lon_cadena)))));
    end if;
    return lista_elementos;
  end split_string_coma;

  function split_string_blanco ( cadena_in in varchar2) return list_strings
  is
  lon_cadena integer;
  elemento varchar2 (50);
  pos integer;
  pos_ant integer;
  lista_elementos                                      list_strings := list_strings (); 
  begin
    lon_cadena := length (cadena_in);
    pos := 0;
    pos_ant := 0;
    if lon_cadena > 0 then
      loop
              dbms_output.put_line ('Entro en el LOOP. La cedena es: ' || cadena_in);
              if pos < lon_cadena then
                pos := instr(cadena_in, ' ', pos+1);
              else
                pos := 0;
              end if;
              dbms_output.put_line ('Primer valor de Pos: ' || pos);
              if pos > 0 then
                dbms_output.put_line ('Pos es mayor que 0');
                elemento := substr(cadena_in, pos_ant+1, (pos - pos_ant)-1);
                dbms_output.put_line ('El elemento es: ' || elemento);
                lista_elementos.EXTEND;
                lista_elementos(lista_elementos.LAST) := UPPER(LTRIM(RTRIM (elemento)));
                pos_ant := pos;
              end if;
       exit when pos = 0;
      end loop;
      lista_elementos.EXTEND;
      lista_elementos(lista_elementos.LAST) := UPPER(LTRIM(RTRIM (substr(cadena_in, pos_ant+1, lon_cadena))));
      dbms_output.put_line ('El ultimo elemento es: ' || UPPER(LTRIM(RTRIM (substr(cadena_in, pos_ant+1, lon_cadena)))));
    end if;
    return lista_elementos;
  end split_string_blanco;

  function extrae_campo_decode (cadena_in in varchar2) return varchar2
  is
    lista_elementos list_strings := list_strings (); 

  begin
    lista_elementos := split_string_coma(cadena_in);
    return lista_elementos(lista_elementos.count - 1);
  
  end extrae_campo_decode;

  function extrae_campo_decode_sin_tabla (cadena_in in varchar2) return varchar2
  is
    lista_elementos list_strings := list_strings (); 

  begin
    lista_elementos := split_string_coma(cadena_in);
    if instr(lista_elementos((lista_elementos.count) - 1), '.') > 0 then
      return substr(lista_elementos(lista_elementos.count - 1), instr(lista_elementos((lista_elementos.count) - 1), '.') + 1);
    else
      return lista_elementos(lista_elementos.count - 1);
    end if;
  end extrae_campo_decode_sin_tabla;

  function transformo_decode(cadena_in in varchar2, alias_in in varchar2, outer_in in integer) return varchar2
  is
    parte_1 varchar2(100);
    parte_2 varchar2(100);
    parte_3 varchar2(100);
    parte_4 varchar2(100);
    decode_out varchar2(1000);
    v_decode varchar2(500);
    v_nuevo_decode varchar2(800);
    v_transfor_out varchar(1000);
    lista_elementos list_strings := list_strings ();
  
  begin
    /* Ejemplo de Decode que analizo DECODE (ID_FUENTE,'SER', ID_CANAL,'1') */
    /* (20160720) Angel Ruiz.(BUG). Rehago completamente el parseo del DECODE */
    /* Me quedo con lo que es la instruccion DECODE, por si el campo */
    /* pudiera estar formado por otro tipo de sentencias como NVL(DECODE(..),...) */
    v_decode := REGEXP_SUBSTR(cadena_in, 'DECODE *\( *[a-zA-Z0-9_]+( *, *[a-zA-Z0-9_'']+)+ *\)');
    --lista_elementos := split_string_coma(cadena_in);
    lista_elementos := split_string_coma(v_decode);
    if (lista_elementos.COUNT > 0 ) then
      dbms_output.put_line ('El nuemro de elementos es: ' || lista_elementos.COUNT);
      FOR indice_decode IN lista_elementos.FIRST .. lista_elementos.LAST
      LOOP
        if (indice_decode = 1) then
        /* Se trata del primer elemento "DECODE (IDE_FUENTE" de nuestro ejemplo */
          parte_1 := trim(substr(lista_elementos(1), instr(lista_elementos(indice_decode), '(') + 1)); /* Me quedo con ID_FUENTE*/
          if (instr(lista_elementos(indice_decode), '''') = 0) then
          /* Esta parte del DECODE no es un literal */
          /* Lo que quiere decir que podemos calificarlo con el nombre de la tabla */
            if (outer_in = 1) then
              parte_1 := alias_in || '.' || parte_1 || '(+)';
            else
              parte_1 := alias_in || '.' || parte_1;
            end if;
            v_nuevo_decode := 'DECODE(' || parte_1;
          end if;
        elsif (indice_decode = lista_elementos.LAST) then
        /* Se trata del ultimo elemento del DECODE. En nuestro ejemplo "'1'" */
          parte_4 := trim(substr(lista_elementos(indice_decode), 1, instr(lista_elementos(indice_decode), ')') - 1));  /* Me quedo con '1' */
          if (instr(parte_4, '''') = 0) then
            /* Esta parte del DECODE no es un literal */
            /* Lo que quiere decir que podemos calificarlo con el nombre de la tabla */
            if (outer_in = 1) then
              parte_4 := alias_in || '.' || parte_4 || '(+)';
            else
              parte_4 := alias_in || '.' || parte_4;
            end if;
          end if;
          v_nuevo_decode := v_nuevo_decode || ',' || parte_4 || ')';
        else
        /* Se trata de todos los elemntos intermedios del DECODE */
          parte_2 := lista_elementos(indice_decode);  /* Me quedo con 'SER' */
          if (instr(parte_2, '''') = 0) then
            /* Esta parte del DECODE no es un literal */
            /* Lo que quiere decir que podemos calificarlo con el nombre de la tabla */
            if (outer_in = 1) then
              parte_2 := alias_in || '.' || parte_2 || '(+)';
            else
              parte_2 := alias_in || '.' || parte_2;
            end if;
          end if;
          v_nuevo_decode := v_nuevo_decode || ', ' || parte_2;
        end if;
      END LOOP;
      dbms_output.put_line ('El valor del DECODE TRANSFORMADO ES: ' || v_nuevo_decode);
      v_transfor_out := REGEXP_REPLACE(cadena_in, 'DECODE *\( *[a-zA-Z0-9_]+( *, *[a-zA-Z0-9_'']+)+ *\)', v_nuevo_decode);
      dbms_output.put_line ('El valor de toda la cadena TRANSFORMADA es: ' || v_transfor_out);
    else
      v_transfor_out := cadena_in;
    end if;
    return v_transfor_out;
  end transformo_decode;
  
  function proceso_campo_value (cadena_in in varchar2, alias_in in varchar) return varchar2
  is
  lon_cadena integer;
  cabeza                varchar2 (1000);
  sustituto              varchar2(100);
  cola                      varchar2(1000);    
  pos                   PLS_integer;
  pos_ant           PLS_integer;
  posicion_ant           PLS_integer;
  v_pos_ini_corchete_ab PLS_integer;
  v_pos_fin_corchete_ce PLS_integer;
  v_cadena_a_buscar varchar2(100);
  cadena_resul varchar(1000);
  begin
    lon_cadena := length (cadena_in);
    pos := 0;
    pos_ant := 0;
    cadena_resul:= cadena_in;
    if (lon_cadena > 0) then
      v_pos_ini_corchete_ab := instr(cadena_in, '[');
      v_pos_fin_corchete_ce := instr(cadena_in, ']');
      v_cadena_a_buscar := substr(cadena_in, v_pos_ini_corchete_ab, (v_pos_fin_corchete_ce - v_pos_ini_corchete_ab) + 1);
      sustituto := alias_in || '.' || substr (cadena_in, v_pos_ini_corchete_ab + 1, (v_pos_fin_corchete_ce - v_pos_ini_corchete_ab) - 1);
      loop
        pos := instr(cadena_resul, v_cadena_a_buscar, pos+1);
        exit when pos = 0;
        dbms_output.put_line ('Pos es mayor que 0');
        dbms_output.put_line ('Primer valor de Pos: ' || pos);
        cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
        dbms_output.put_line ('La cabeza es: ' || cabeza);
        dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
        cola := substr(cadena_resul, pos + length (v_cadena_a_buscar));
        dbms_output.put_line ('La cola es: ' || cola);
        cadena_resul := cabeza || sustituto || cola;
      end loop;
    end if;
    return cadena_resul;
  end;


  function proc_campo_value_condicion (cadena_in in varchar2, nombre_funcion_lookup in varchar2) return varchar2
  is
    lon_cadena integer;
    cabeza                varchar2 (1000);
    sustituto              varchar2(100);
    cola                      varchar2(1000);    
    pos                   PLS_integer;
    pos_ant           PLS_integer;
    posicion_ant           PLS_integer;
    cadena_resul varchar(1000);
  begin
    lon_cadena := length (cadena_in);
    pos := 0;
    posicion_ant := 0;
    cadena_resul:= cadena_in;
    if (lon_cadena > 0) then
      /* Busco VAR_FUN_NAME_LOOKUP */
      sustituto := nombre_funcion_lookup;
      loop
        dbms_output.put_line ('Entro en el LOOP de proc_campo_value_condicion. La cadena es: ' || cadena_resul);
        pos := instr(cadena_resul, 'VAR_FUN_NAME_LOOKUP', pos+1);
        exit when pos = 0;
        dbms_output.put_line ('Pos es mayor que 0');
        dbms_output.put_line ('Primer valor de Pos: ' || pos);
        cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
        dbms_output.put_line ('La cabeza es: ' || cabeza);
        dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
        cola := substr(cadena_resul, pos + length ('VAR_FUN_NAME_LOOKUP'));
        dbms_output.put_line ('La cola es: ' || cola);
        cadena_resul := cabeza || sustituto || cola;
        --pos_ant := pos + length (' to_date ( fch_datos_in, ''yyyymmdd'') ');
        --pos := pos_ant;
      end loop;
    end if;  
    return cadena_resul;
  end;

  function procesa_COM_RULE_lookup (cadena_in in varchar2, v_alias_in varchar2 := NULL) return varchar2
  is
  lon_cadena integer;
  cabeza                varchar2 (1000);
  sustituto              varchar2(100);
  cola                      varchar2(1000);    
  pos                   PLS_integer;
  pos_ant           PLS_integer;
  posicion_ant           PLS_integer;
  cadena_resul varchar(1000);
  begin
    dbms_output.put_line ('Entro en procesa_COM_RULE_lookup');
    lon_cadena := length (cadena_in);
    pos := 0;
    posicion_ant := 0;
    cadena_resul:= cadena_in;
    if lon_cadena > 0 then
      /* Busco LA COMILLA */
      pos := 0;
      posicion_ant := 0;
      sustituto := '''''';
      loop
        dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup. La cadena es: ' || cadena_resul);
        pos := instr(cadena_resul, '''', pos+1);
        exit when pos = 0;
        dbms_output.put_line ('Pos es mayor que 0');
        dbms_output.put_line ('Primer valor de Pos: ' || pos);
        cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
        dbms_output.put_line ('La cabeza es: ' || cabeza);
        dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
        cola := substr(cadena_resul, pos + length (''''));
        dbms_output.put_line ('La cola es: ' || cola);
        cadena_resul := cabeza || sustituto || cola;
        pos_ant := pos + length ('''''');
        pos := pos_ant;
      end loop;
      /* Sustituyo el nombre de Tabla generico por el nombre que le paso como parametro */
      if (v_alias_in is not null) then
        /* Existe un alias que sustituir */
        pos := 0;
        posicion_ant := 0;
        sustituto := v_alias_in;
        loop
          dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup para sustituir el ALIAS. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#TABLE_OWNER#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#TABLE_OWNER#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
          --pos_ant := pos + length ('''''');
          --pos := pos_ant;
        end loop;

      end if;
    end if;
    
    return cadena_resul;
  end;


  function procesa_condicion_lookup (cadena_in in varchar2, v_alias_in varchar2 := NULL, v_indicador BOOLEAN) return varchar2
  is
  lon_cadena integer;
  cabeza                varchar2 (1000);
  sustituto              varchar2(100);
  cola                      varchar2(1000);    
  pos                   PLS_integer;
  pos_ant           PLS_integer;
  posicion_ant           PLS_integer;
  cadena_resul varchar(1000);
  begin
    dbms_output.put_line ('Entro en procesa_condicion_lookup');
    lon_cadena := length (cadena_in);
    pos := 0;
    posicion_ant := 0;
    cadena_resul:= cadena_in;
    if lon_cadena > 0 then
      if (v_indicador = TRUE) then
        /* Busco el signo = */
        sustituto := ' (+)= ';
        loop
          dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '=', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('='));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
          pos_ant := pos + (length (' (+)= '));
          dbms_output.put_line ('La posicion anterior es: ' || pos_ant);
          pos := pos_ant;
        end loop;
      end if;
      /* Sustituyo el nombre de Tabla generico por el nombre que le paso como parametro */
      if (v_alias_in is not null) then
        /* Existe un alias que sustituir */
        pos := 0;
        posicion_ant := 0;
        sustituto := v_alias_in;
        loop
          dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup para sustituir el ALIAS. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#TABLE_OWNER#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#TABLE_OWNER#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
          --pos_ant := pos + length ('''''');
          --pos := pos_ant;
        end loop;
        pos := 0;
        posicion_ant := 0;
        sustituto := v_alias_in;
        loop
          dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup para sustituir el ALIAS. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_1#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_1#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
          --pos_ant := pos + length ('''''');
          --pos := pos_ant;
        end loop;

      end if;
    end if;
    
    return cadena_resul;
  end;

  function procesa_campo_filter (cadena_in in varchar2) return varchar2
  is
    lon_cadena integer;
    cabeza                varchar2 (2000);
    sustituto              varchar2(100);
    cola                      varchar2(2000);    
    pos                   PLS_integer;
    pos_ant           PLS_integer;
    posicion_ant           PLS_integer;
    cadena_resul varchar(20000);
    begin
      lon_cadena := length (cadena_in);
      pos := 0;
      posicion_ant := 0;
      cadena_resul:= cadena_in;
      if lon_cadena > 0 then
        /* Busco VAR_FCH_CARGA */
        --sustituto := ' to_date ( fch_datos_in, ''yyyymmdd'') ';
        sustituto := ' date_format ( fch_datos_in, ''yyyymmdd'') ';
        loop
          dbms_output.put_line ('Entro en el LOOP. La cedena es: ' || cadena_resul);
          pos := instr(cadena_resul, 'VAR_FCH_CARGA', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('VAR_FCH_CARGA'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
          --pos_ant := pos + length (' to_date ( fch_datos_in, ''yyyymmdd'') ');
          --pos := pos_ant;
        end loop;
        /* Busco VAR_PROFUNDIDAD_BAJAS */
        sustituto := ' 90 ';  /* Temporalmente pongo 90 dias */
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de VAR_PROFUNDIDAD_BAJAS. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, 'VAR_PROFUNDIDAD_BAJAS', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('VAR_PROFUNDIDAD_BAJAS'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        /* Busco OWNER_DM */
        sustituto := OWNER_DM;
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_DM#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_DM#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        
        /* Busco #OWNER_IFRS15# */
        --cadena_resul := regexp_replace(cadena_resul, '#OWNER_IFRS15#', OWNER_DM);
        
        /* Busco OWNER_SA */
        sustituto := OWNER_SA; 
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_SA#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_SA#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        /* Busco OWNER_T */
        sustituto := OWNER_T; 
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_T#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_T#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        /* Busco OWNER_MTDT */
        sustituto := OWNER_MTDT; 
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_MTDT#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_MTDT#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        /* Busco OWNER_1 */
        sustituto := OWNER_1; 
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_1#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_1#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;        
        /* Busco OWNER_2 */
        sustituto := OWNER_2; 
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_2#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_2#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;        
        /* Busco OWNER_3 */
        sustituto := OWNER_3; 
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_3#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_3#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        /* Busco OWNER_4 */
        sustituto := OWNER_4; 
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_4#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_4#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        /*************************/
        
        /* (20161108) Angel Ruiz. NF. Diferentes #OWNER para la extraccion */
        pos := regexp_instr(cadena_resul, '#OWNER_[A-Za-z_0-9]+#');
        if (pos > 0 ) then
          v_hay_usu_owner:=true;
          v_usuario_owner:= regexp_substr(cadena_resul, '#OWNER_[A-Za-z_0-9]+#');
        end if;
        /*************************/
        
        
        /* Busco [YYYYMM] */
        if (v_type_validation = 'I') then
        /* (20160606) Angel Ruiz. NF: Se trata de que el tipo de validacion es I lo que significa */
        /* que se extrae desde el origen y va directamente a las tablas de Staging sin pasar por un fichero plano */
        /* por lo que no se pasara como parametro el nombre del fichero plano */
          sustituto := '&' || '1'; 
        else
          sustituto := '&' || '2';
        end if;
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '[YYYYMM]', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('[YYYYMM]'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;                
        /* Busco #FCH_INI# */
        pos := 0;
        if (instr(cadena_resul, '#FCH_INI#', pos+1) > 0) then
          /* Ocurre que el fichero sql que estamos generando tiene parametros de fecha */
          /* inicial y final de extraccion */
          v_fecha_ini_param := true;
          if (v_tabla_dinamica = true) then
            /* Si tb existen tablas dinamicas entonces el parametro sera el 3 */
            if (v_type_validation = 'I') then
            /* (20160607) Angel Ruiz. Si se trata de tipo de validacion I significa */
            /* que no se le pasa el nombre del fichero plano como parametro, por lo que */
            /* por lo que el numero de parametros sera uno menos */
              sustituto := '&' || '2';
            else
              sustituto := '&' || '3';
            end if;
          else
            /* si no el 2 */
            if (v_type_validation = 'I') then
              sustituto := '&' || '1';
            else
              sustituto := '&' || '2';
            end if;
          end if;
          loop
            dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
            pos := instr(cadena_resul, '#FCH_INI#', pos+1);
            exit when pos = 0;
            dbms_output.put_line ('Pos es mayor que 0');
            dbms_output.put_line ('Primer valor de Pos: ' || pos);
            cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
            dbms_output.put_line ('La cabeza es: ' || cabeza);
            dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
            cola := substr(cadena_resul, pos + length ('#FCH_INI#'));
            dbms_output.put_line ('La cola es: ' || cola);
            cadena_resul := cabeza || sustituto || cola;
          end loop;
        end if;
        /* Busco #FCH_FIN# */
        pos := 0;
        if (instr(cadena_resul, '#FCH_FIN#', pos+1) > 0) then
          /* Ocurre que el fichero sql que estamos generando tiene parametros de fecha */
          /* inicial y final de extraccion */
          v_fecha_fin_param := true;
          if (v_tabla_dinamica = true) then
            /* Si tb existen tablas dinamicas entonces el parametro sera el 3 */
            if (v_type_validation = 'I') then
            /* (20160607) Angel Ruiz. Si se trata de tipo de validacion I significa */
            /* que no se le pasa el nombre del fichero plano como parametro, por lo que */
            /* por lo que el numero de parametros sera uno menos */
              sustituto := '&' || '3';
            else
              sustituto := '&' || '4';
            end if;
          else
            /* si no el 2 */
            if (v_type_validation = 'I') then
            /* (20160607) Angel Ruiz. Si se trata de tipo de validacion I significa */
            /* que no se le pasa el nombre del fichero plano como parametro, por lo que */
            /* por lo que el numero de parametros sera uno menos */
              sustituto := '&' || '2';
            else            
              sustituto := '&' || '3';
            end if;
          end if;
          loop
            dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
            pos := instr(cadena_resul, '#FCH_FIN#', pos+1);
            exit when pos = 0;
            dbms_output.put_line ('Pos es mayor que 0');
            dbms_output.put_line ('Primer valor de Pos: ' || pos);
            cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
            dbms_output.put_line ('La cabeza es: ' || cabeza);
            dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
            cola := substr(cadena_resul, pos + length ('#FCH_FIN#'));
            dbms_output.put_line ('La cola es: ' || cola);
            cadena_resul := cabeza || sustituto || cola;
          end loop;
        end if;
        
      end if;
      return cadena_resul;
    end;
/************/
    /* (20161102) Angel Ruiz. Funcion para sustituir en una cadena los */
    /* finales de linea por el caracter \ mas el final de linea */
  function cambia_fin_linea (cadena_in in varchar2) return varchar2
  is
    v_cadena_result varchar2(20000);
  begin
    v_cadena_result := REGEXP_REPLACE(cadena_in,chr(10) || ' *$', ''); /* Suprimo el posible retorno de carro final */
    v_cadena_result := REGEXP_REPLACE(v_cadena_result, chr(10), ' \\' || chr(10));
    return v_cadena_result;
  end;  
/*************/
  function procesa_campo_filter_dinam (cadena_in in varchar2) return varchar2
  is
    lon_cadena integer;
    cabeza                varchar2 (1000);
    sustituto              varchar2(100);
    cola                      varchar2(1000);    
    pos                   PLS_integer;
    pos_ant           PLS_integer;
    posicion_ant           PLS_integer;
    cadena_resul varchar(1000);
    begin
      lon_cadena := length (cadena_in);
      pos := 0;
      posicion_ant := 0;
      cadena_resul:= cadena_in;
      if lon_cadena > 0 then
        /* Busco VAR_FCH_CARGA */
        sustituto := ' to_date ('''' ||  fch_datos_in || '''', ''yyyymmdd'') ';
        loop
          dbms_output.put_line ('Entro en el LOOP. La cedena es: ' || cadena_resul);
          pos := instr(cadena_resul, 'VAR_FCH_CARGA', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('VAR_FCH_CARGA'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
          --pos_ant := pos + length (' to_date ( fch_datos_in, ''yyyymmdd'') ');
          --pos := pos_ant;
        end loop;
        /* Busco VAR_PROFUNDIDAD_BAJAS */
        sustituto := ' 90 ';  /* Temporalmente pongo 90 dias */
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de VAR_PROFUNDIDAD_BAJAS. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, 'VAR_PROFUNDIDAD_BAJAS', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('VAR_PROFUNDIDAD_BAJAS'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        /* Busco OWNER_DM */
        sustituto := OWNER_DM;
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_DM#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_DM#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        /* Busco OWNER_SA */
        sustituto := OWNER_SA; 
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_SA#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_SA#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        /* Busco OWNER_T */
        sustituto := OWNER_T; 
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_T#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_T#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        /* Busco OWNER_MTDT */
        sustituto := OWNER_MTDT; 
        pos := 0;
        loop
          dbms_output.put_line ('Entro en el LOOP de OWNER_DM. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '#OWNER_MTDT#', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length ('#OWNER_MTDT#'));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
        end loop;
        /* (20150914) Angel Ruiz. BUG. Cuando se incluye un FILTER en la tabla con una condicion */
        /* que tenia comillas, las comillas aparecian como simple y no funcionaba */
        /* Busco LA COMILLA */
        pos := 0;
        posicion_ant := 0;
        sustituto := '''''';
        loop
          dbms_output.put_line ('Entro en el LOOP de procesa_condicion_lookup. La cadena es: ' || cadena_resul);
          pos := instr(cadena_resul, '''', pos+1);
          exit when pos = 0;
          dbms_output.put_line ('Pos es mayor que 0');
          dbms_output.put_line ('Primer valor de Pos: ' || pos);
          cabeza := substr(cadena_resul, (posicion_ant + 1), (pos - posicion_ant - 1));
          dbms_output.put_line ('La cabeza es: ' || cabeza);
          dbms_output.put_line ('La  sustitutoria es: ' || sustituto);
          cola := substr(cadena_resul, pos + length (''''));
          dbms_output.put_line ('La cola es: ' || cola);
          cadena_resul := cabeza || sustituto || cola;
          pos_ant := pos + length ('''''');
          pos := pos_ant;
        end loop;
        /* (20150914) Angel Ruiz. FIN BUG. Cuando se incluye un FILTER en la tabla con una condicion */
        /* que tenia comillas, las comillas aparecian como simple y no funcionaba */
      end if;
      return cadena_resul;
    end;

/************/

  function genera_campo_select ( reg_detalle_in in MTDT_TC_DETAIL%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (1500);
    posicion          PLS_INTEGER;
    cad_pri           VARCHAR(500);
    cad_seg         VARCHAR(500);
    cadena            VARCHAR(200);
    pos_del_si      NUMBER(3);
    pos_del_then  NUMBER(3);
    pos_del_else  NUMBER(3);
    pos_del_end   NUMBER(3);
    condicion         VARCHAR2(200);
    condicion_pro         VARCHAR2(200);
    constante         VARCHAR2(100);
    posicion_ant    PLS_integer;
    pos                    PLS_integer;
    cadena_resul  VARCHAR(500);
    sustituto           VARCHAR(30);
    lon_cadena     PLS_integer;
    cabeza             VARCHAR2(500);
    cola                   VARCHAR2(500);
    pos_ant            PLS_integer;
    v_encontrado  VARCHAR2(1);
    v_alias             VARCHAR2(20000);  /*[URC] Cambia longitud de 1000 a 20000 por ORA-06502: PL/SQL: error : character string buffer too small numérico o de valor */
    table_columns_lkup  list_strings := list_strings();
    ie_column_lkup    list_strings := list_strings();
    tipo_columna  VARCHAR2(30);
    mitabla_look_up VARCHAR2(20000);
    l_registro          ALL_TAB_COLUMNS%rowtype;
    v_value VARCHAR(200);
    nombre_campo  VARCHAR2(30);
    v_table_base_name varchar2(100);
    v_alias_table_base_name varchar2(100);
    v_table_look_up varchar2(10000); /*[URC] Cambia longitud de 1000 a 10000 por ORA-06502: PL/SQL: error : character string buffer too small numérico o de valor */
    v_alias_table_look_up varchar2(10000);  /*[URC] Cambia longitud de 1000 a 10000 por ORA-06502: PL/SQL: error : character string buffer too small numérico o de valor */
    v_temporal varchar2(500);
    v_reg_table_lkup varchar2(10000); /*[URC] Cambia longitud de 1000 a 10000 por ORA-06502: PL/SQL: error : character string buffer too small numérico o de valor */
  begin
    /* Seleccionamos el escenario primero */
      dbms_output.put_line('ESTOY EN EL genera_campo_select. Columna: ' || reg_detalle_in.TABLE_NAME || '.' || reg_detalle_in.TABLE_COLUMN);
      dbms_output.put_line('ESTOY EN EL genera_campo_select. Regla: ' || reg_detalle_in.RUL);
      dbms_output.put_line('ESTOY EN EL genera_campo_select. Tabla LookUp: ' || reg_detalle_in.TABLE_LKUP);
      case reg_detalle_in.RUL
      when 'KEEP' then
        /* Se mantienen el valor del campo de la tabla que estamos cargando */
        valor_retorno :=  reg_detalle_in.TABLE_NAME || '.' || reg_detalle_in.TABLE_COLUMN;
      when 'LKUPC' then
        /* (20160329) Angel Ruiz. Detectamos si TBLA_BASE_NAME posee ALIAS */
        if (REGEXP_LIKE(trim(reg_detalle_in.TABLE_BASE_NAME), '^[a-zA-Z_0-9]+ +[a-zA-Z_0-9]+$') = true) then
          v_alias_table_base_name := trim(REGEXP_SUBSTR(TRIM(reg_detalle_in.TABLE_BASE_NAME), ' +[a-zA-Z_0-9]+$'));
          v_table_base_name := trim(REGEXP_SUBSTR(TRIM(reg_detalle_in.TABLE_BASE_NAME), '^+[a-zA-Z_0-9]+ '));
        else
          v_alias_table_base_name := reg_detalle_in.TABLE_BASE_NAME;
          v_table_base_name := reg_detalle_in.TABLE_BASE_NAME;
        end if;
        /* (20150626) Angel Ruiz.  Se trata de hacer el LOOK UP con la tabla dimension de manera condicional */
        l_FROM.extend;
        if (instr (reg_detalle_in.TABLE_LKUP,'SELECT ') > 0 or instr (reg_detalle_in.TABLE_LKUP,'select ') > 0) then
          /* Aparecen queries en lugar de tablas en la columna de nombre de tabla para LookUp */
          /* Me quedo con el Alias que aparece en la SELECT*/
          --if (REGEXP_LIKE(reg_detalle_in.TABLE_LKUP, '^ *\( *SELECT[a-z A-Z\*=.,_\'']*\) *[a-zA-Z_]+$') = true) then
            /* La subquery efectivamente encaja con el patron esperado para una subquery */
            v_alias_table_look_up := trim(substr(REGEXP_SUBSTR (reg_detalle_in.TABLE_LKUP, '\) *[a-zA-Z_0-9]+$'), 2));
            v_alias := v_alias_table_look_up;
            mitabla_look_up := reg_detalle_in.TABLE_LKUP;
          --else
            --v_alias := 'LKUP_' || l_FROM.count;
            --mitabla_look_up := '(' || reg_detalle_in.TABLE_LKUP || ') "LKUP_' || l_FROM.count || '"';
          --end if;
          l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
        else
          if (REGEXP_LIKE(trim(reg_detalle_in.TABLE_LKUP), '^[a-zA-Z_0-9\.]+ +[a-zA-Z_0-9]+$') = true) then
            v_alias_table_look_up := trim(REGEXP_SUBSTR(TRIM(reg_detalle_in.TABLE_LKUP), ' +[a-zA-Z_0-9]+$'));
            v_table_look_up := trim(REGEXP_SUBSTR(TRIM(reg_detalle_in.TABLE_LKUP), '^+[a-zA-Z_0-9\.]+ '));
          else
            v_alias_table_look_up := reg_detalle_in.TABLE_LKUP;
            v_table_look_up := reg_detalle_in.TABLE_LKUP;
          end if;
          mitabla_look_up := reg_detalle_in.TABLE_LKUP;
          /* (20150112) Angel Ruiz */
          /* Puede ocurrir que se se tenga varias veces la misma LookUp pero para campo diferentes */
          /* lo que se traduce en que hay que crear ALIAS */
          /* BUSCAMOS SI YA ESTABA LA TABLA INCLUIDA EN EL FROM*/
          v_encontrado:='N';
          FOR indx IN l_FROM.FIRST .. l_FROM.LAST
          LOOP
            --if (instr(l_FROM(indx),  reg_detalle_in.TABLE_LKUP, 0)) then
            --regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0
            if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
            --if (l_FROM(indx) = ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP) then
              /* La misma tabla ya estaba en otro lookup */
              v_encontrado:='Y';
            end if;
          END LOOP;
          if (v_encontrado='Y') then
            v_alias := reg_detalle_in.TABLE_LKUP || '_' || l_FROM.count;
            l_FROM (l_FROM.last) := ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP || ' "' || v_alias || '"' ;
          else
            --v_alias := reg_detalle_in.TABLE_LKUP;
            v_alias := v_alias_table_look_up;
            l_FROM (l_FROM.last) := ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP;
          end if;
        end if;
        /* Miramos la parte de las condiciones */
        /* Puede haber varios campos por los que hacer LookUp y por lo tanto JOIN */
        table_columns_lkup := split_string_coma (reg_detalle_in.TABLE_COLUMN_LKUP);
        ie_column_lkup := split_string_coma (reg_detalle_in.IE_COLUMN_LKUP);

        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL SELECT */
        /****************************************************************************/
        
        valor_retorno :=  proc_campo_value_condicion (reg_detalle_in.LKUP_COM_RULE, 'NVL(' || reg_detalle_in.VALUE || ', '' '')');
        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL WHERE */
        /****************************************************************************/
        
        if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
          FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
          LOOP
            l_WHERE.extend;
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Columna es: ' || ie_column_lkup(indx));
            
            if (l_WHERE.count = 1) then
              --l_WHERE(l_WHERE.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||', -3)' ||' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
              if (reg_detalle_in."OUTER" = 'Y') then
                l_WHERE(l_WHERE.last) :=  v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
              else
                l_WHERE(l_WHERE.last) :=  v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx);
              end if;
            else
              --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', -3)' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
              if (reg_detalle_in."OUTER" = 'Y') then
                l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
              else
                l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
              end if;
            end if;
          END LOOP;
        else    /* Solo hay un campo condicion */
          
          /* Miramos si la tabla con la que hay que hacer LookUp es una tabla de rangos */
          l_WHERE.extend;
          if (instr (reg_detalle_in.TABLE_LKUP,'RANGO') > 0) then
            if (l_WHERE.count = 1) then
              l_WHERE(l_WHERE.last) := v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              l_WHERE.extend;
              l_WHERE(l_WHERE.last) := ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' <= ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4) || ' (+)';
            else
              l_WHERE(l_WHERE.last) := ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.'  || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              l_WHERE.extend;
              l_WHERE(l_WHERE.last) := ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' <= ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4) || ' (+)';
            end if;
          else
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. La Columna es: ' || reg_detalle_in.IE_COLUMN_LKUP);
            if (l_WHERE.count = 1) then /* si es el primer campo del WHERE */
              /* (20160630) Angel Ruiz. BUG. Ocurre que si los campos IE_COLUMN_LKUP o TABLE_COLUMN_LKUP ya estan calificados no hay que hacerlo */
              if (instr(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name || '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias || '.') = 0) then
                l_WHERE(l_WHERE.last) := v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name || '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias || '.') > 0) then
                l_WHERE(l_WHERE.last) := v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              else
                l_WHERE(l_WHERE.last) := reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              end if;
            else  /* sino es el primer campo del Where  */
              if (reg_detalle_in."OUTER" = 'Y') then
                /* (20160630) Angel Ruiz. BUG. Ocurre que si los campos IE_COLUMN_LKUP o TABLE_COLUMN_LKUP ya estan calificados no hay que hacerlo */
                if (instr(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name || '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias || '.') = 0) then
                  l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name || '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias || '.') > 0) then
                  l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                end if;
              else
                /* (20160630) Angel Ruiz. BUG. Ocurre que si los campos IE_COLUMN_LKUP o TABLE_COLUMN_LKUP ya estan calificados no hay que hacerlo */
                if (instr(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name || '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias || '.') = 0) then
                  l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name || '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias || '.') > 0) then
                  l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP;
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                end if;
              end if;
            end if;
          end if;
        end if;
        if (reg_detalle_in.TABLE_LKUP_COND is not null) then
          /* Existen condiciones en la tabla de Look Up que hay que introducir*/
          l_WHERE.extend;
          /* (20160412) Angel Ruiz. BUG: Si la tabla de LookUP es con OUTER entonces */
          /* debemos procesar la condicion para ponerle el signo outer por dentro */
          if (reg_detalle_in.OUTER IS NOT NULL and reg_detalle_in.OUTER='Y') then
            l_WHERE(l_WHERE.last) :=  ' AND ' || procesa_condicion_lookup(reg_detalle_in.TABLE_LKUP_COND, v_alias, TRUE);
          else
            l_WHERE(l_WHERE.last) :=  ' AND ' || procesa_condicion_lookup(reg_detalle_in.TABLE_LKUP_COND, v_alias, FALSE);
          end if;
        end if;
      when 'LKUP' then
        dbms_output.put_line ('Entro en la regla LKUP.');
        /* Se trata de hacer el LOOK UP con la tabla dimension */
        if (REGEXP_LIKE(trim(reg_detalle_in.TABLE_BASE_NAME), '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9]+') = true) then
          dbms_output.put_line ('ENTRO EN EL IF QUE COMPRUEBA SI LA TABLA ESTA CALIFICADA');
          /* (20160413) Primero detectamos si esta calificada la tabla*/
          /* Si esta calificada hay que sustituir el propietario por un usuario, en caso de ser necesario */
          v_temporal := procesa_campo_filter(trim(reg_detalle_in.TABLE_BASE_NAME));
          if (REGEXP_LIKE(trim(v_temporal), '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9]+ +[a-zA-Z0-9_]+$') = true) then
            /* (20160329) Angel Ruiz. Detectamos si TABLE_BASE_NAME posee ALIAS */
            v_alias_table_base_name := trim(REGEXP_SUBSTR(TRIM(v_temporal), ' +[a-zA-Z_0-9]+$'));
            v_table_base_name := substr(trim(REGEXP_SUBSTR(TRIM(v_temporal), '\.[a-zA-Z_0-9]+ ')),2);
          else
            v_alias_table_base_name := substr(trim(REGEXP_SUBSTR(TRIM(v_temporal), '\.[a-zA-Z_0-9]+')),2);
            v_table_base_name := substr(trim(REGEXP_SUBSTR(TRIM(v_temporal), '\.[a-zA-Z_0-9]+')),2);
          end if;
        else
          dbms_output.put_line ('ENTRO EN EL ELSE DE QUE LA TABLA NO ESTA CALIFICADA');
          /* La tabla no esta calificada */
          --if (REGEXP_LIKE(trim(reg_detalle_in.TABLE_BASE_NAME), '^[a-zA-Z_0-9]+ +[a-zA-Z_0-9]+$') = true) then
          if (REGEXP_LIKE(trim(reg_detalle_in.TABLE_BASE_NAME), '^[a-zA-Z_0-9]+\[*[a-zA-Z_0-9]+\]* +[a-zA-Z_0-9]+$') = true) then
            /* (20160329) Angel Ruiz. Detectamos si TABLE_BASE_NAME posee ALIAS */
            v_alias_table_base_name := trim(REGEXP_SUBSTR(TRIM(reg_detalle_in.TABLE_BASE_NAME), ' +[a-zA-Z_0-9]+$'));
            v_table_base_name := trim(REGEXP_SUBSTR(TRIM(reg_detalle_in.TABLE_BASE_NAME), '^+[a-zA-Z_0-9]+ '));
          else
            v_alias_table_base_name := reg_detalle_in.TABLE_BASE_NAME;
            v_table_base_name := reg_detalle_in.TABLE_BASE_NAME;
          end if;
        end if;
        /* (20150126) Angel Ruiz. Primero recojo la tabla del modelo con la que se hace LookUp. NO puede ser tablas T_* sino su equivalesnte del modelo */
        dbms_output.put_line('ESTOY EN EL LOOKUP. Al principio');
        l_FROM.extend;
        l_FROM_solo_tablas.extend;  /*(20170306) Angel Ruiz */
        /* (20150130) Angel Ruiz */
        /* Nueva incidencia. */
        if (regexp_instr (reg_detalle_in.TABLE_LKUP, '[Ss][Ee][Ll][Ee][Cc][Tt]') > 0) then  /* (20160802) Angel Ruiz. BUG: No detectaba correctamente la palabra SELECT */
          /* Aparecen queries en lugar de tablas en la columna de nombre de tabla para LookUp */
          /* Me quedo con el Alias que aparece en la SELECT*/
          --if (REGEXP_LIKE(reg_detalle_in.TABLE_LKUP, '^ *\( *SELECT[a-z A-Z\*=.,_\'']*\) *[a-zA-Z_]+$') = true) then
            /* La subquery efectivamente encaja con el patron esperado para una subquery */
            dbms_output.put_line('Dentro del IF del SELECT');
            v_alias_table_look_up := trim(substr(REGEXP_SUBSTR (reg_detalle_in.TABLE_LKUP, '\) *[a-zA-Z_0-9]+$'), 2));
            v_alias := v_alias_table_look_up;
            mitabla_look_up := procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
          --else
            --v_alias := 'LKUP_' || l_FROM.count;
            --mitabla_look_up := '(' || reg_detalle_in.TABLE_LKUP || ') "LKUP_' || l_FROM.count || '"';
          --end if;
          --l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
          l_FROM_solo_tablas (l_FROM_solo_tablas.last) := ', ' || mitabla_look_up;
          l_FROM (l_FROM.last) := 'LEFT OUTER JOIN ' || mitabla_look_up || ' ';
          
        else
          dbms_output.put_line('Dentro del ELSE del SELECT');
          /* (20160401) Detectamos si la tabla de LookUp posee Alias */
          v_reg_table_lkup := procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
          if (REGEXP_LIKE(trim(v_reg_table_lkup), '^[a-zA-Z_0-9#\.&]+ +[a-zA-Z_0-9]+$') = true) then
            /* La tabla de LKUP posee Alias */
            dbms_output.put_line('La tabla de LKUP posee alias');
            v_alias_table_look_up := trim(REGEXP_SUBSTR(TRIM(v_reg_table_lkup), ' +[a-zA-Z_0-9]+$'));
            v_table_look_up := trim(REGEXP_SUBSTR(TRIM(v_reg_table_lkup), '^+[a-zA-Z_0-9\.#&]+ '));
            dbms_output.put_line('El alias es: ' || v_alias_table_look_up);
            dbms_output.put_line('La tabla de LKUP es: ' || v_table_look_up);
            if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* La tabla de LKUP esta calificada */
              v_table_look_up := v_table_look_up;
            else
              /* La tabla de LKUP no esta calificada, entonces la califico */
              /*(20160713) Angel Ruiz. BUG. Le anyado el procesa_campo_filter */
              /*(20170119) Angel Ruiz. BUG. Si la taba no esta calificada */
              /* la califico con el usuario extractor para este escenario */
              --v_table_look_up := OWNER_EX || '.' || v_table_look_up;
              --v_table_look_up := '#OWNER_' || reg_scenario.SOURCE || '#' || '.' || v_table_look_up;
              /* (20170829) Angel Ruiz. BUG Corregido. Si la TABLA_LKUP no esta calificada */
              v_table_look_up := '#OWNER_DM' || '#' || '.' || v_table_look_up;
              v_table_look_up := procesa_campo_filter(v_table_look_up);
            end if;
            mitabla_look_up := v_table_look_up || ' ' || v_alias_table_look_up;
            /* Busco si estaba ya en el FROM. Como es una tabla con ALIAS */
            /* si ya estaba en el FROM entonces no la vuelo a meter ya que tiene un ALIAS */
            v_encontrado:='N';
            FOR indx IN l_FROM_solo_tablas.FIRST .. l_FROM_solo_tablas.LAST
            LOOP
              --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
              if (regexp_count(l_FROM_solo_tablas(indx), mitabla_look_up) >0) then
              --if (l_FROM(indx) = ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP) then
                /* La misma tabla ya estaba en otro lookup */
                v_encontrado:='Y';
              end if;
            END LOOP;
            if (v_encontrado='N') then
              /* Solo la introduzco si la tabla no estaba ya */
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              l_FROM_solo_tablas (l_FROM_solo_tablas.last) := ', ' || mitabla_look_up;
              /* (20170306) Angel Ruiz. NF: Sintasix Beeline */
              if (reg_detalle_in.OUTER = 'Y') then
                l_FROM (l_FROM.last) := 'LEFT OUTER JOIN ' || mitabla_look_up || ' ';
              else
                l_FROM (l_FROM.last) := 'JOIN ' || mitabla_look_up || ' ';
              end if;
              /* (20170306) Angel Ruiz. FIN NF: Sintasix Beeline */
            end if;
            v_alias := v_alias_table_look_up;
          else    /* La tabla de LKUP no posee Alias */
            dbms_output.put_line('La tabla de LKUP no posee alias');
            --v_table_look_up := reg_detalle_in.TABLE_LKUP;
            v_table_look_up := v_reg_table_lkup;            
            --if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9]+') = true) then
            if (REGEXP_LIKE(v_table_look_up, '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9&]+') = true) then
              /* La tabla de LKUP esta calificada */
              dbms_output.put_line('La tabla de LKUP esta calificado');
              --v_alias_table_look_up := SUBSTR(REGEXP_SUBSTR(v_table_look_up, '\.[a-zA-Z_0-9]+'), 2);
              v_alias_table_look_up := SUBSTR(REGEXP_SUBSTR(v_table_look_up, '\.[a-zA-Z_0-9&]+'), 2);
              --v_table_look_up := procesa_campo_filter(v_table_look_up);
              v_table_look_up := v_table_look_up;
            else
              dbms_output.put_line('La tabla de LKUP no esta calificado');
              /* La tabla de LKUP no esta calificada, entonces la califico */
              v_alias_table_look_up := v_table_look_up;
              /*(20160713) Angel Ruiz. BUG. Anyado procesa_campo_filter */
              --v_table_look_up := OWNER_EX || '.' || procesa_campo_filter(v_table_look_up);
              /*(20170119) Angel Ruiz. BUG. Si la taba no esta calificada */
              /* la califico con el usuario extractor para este escenario */
              --v_table_look_up := OWNER_EX || '.' || v_table_look_up;
              /* (20170829) Angel Ruiz. BUG cuando la tabla de LookUp no esta calificada */
              --v_table_look_up := '#OWNER_' || reg_scenario.SOURCE || '#' || '.' || v_table_look_up;
              v_table_look_up := '#OWNER_DM' || '#' || '.' || v_table_look_up;
              v_table_look_up:= procesa_campo_filter(v_table_look_up);
            end if;
            dbms_output.put_line('El alias es: ' || v_alias_table_look_up);
            dbms_output.put_line('La tabla de LKUP es: ' || v_table_look_up);
            mitabla_look_up := v_table_look_up;
            v_encontrado:='N';
            FOR indx IN l_FROM_solo_tablas.FIRST .. l_FROM_solo_tablas.LAST
            LOOP
              --if (instr(l_FROM(indx),  reg_detalle_in.TABLE_LKUP, 0)) then
              --regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0
              --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
              if (regexp_count(l_FROM_solo_tablas(indx), mitabla_look_up) >0) then
              --if (l_FROM(indx) = ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP) then
                /* La misma tabla ya estaba en otro lookup */
                v_encontrado:='Y';
              end if;
            END LOOP;
            if (v_encontrado='Y') then
              v_alias := reg_detalle_in.TABLE_LKUP || '_' || l_FROM.count;
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP) || ' "' || v_alias || '"' ;
              --l_FROM (l_FROM.last) := ', ' || mitabla_look_up || ' "' || v_alias || '"' ;
              /* (20170306) Angel Ruiz. NF: Sintasix Beeline */
              if (reg_detalle_in.OUTER = 'Y') then
                l_FROM (l_FROM.last) := 'LEFT OUTER JOIN ' || mitabla_look_up || ' ' || v_alias || ' ' ;
              else
                l_FROM (l_FROM.last) := 'JOIN ' || mitabla_look_up || ' ' || v_alias || ' ' ;
              end if;
              /* (20170306) Angel Ruiz. FIN NF: Sintasix Beeline */
              l_FROM_solo_tablas (l_FROM_solo_tablas.last) := ', ' || mitabla_look_up || ' "' || v_alias || '"' ;
              
            else
              --v_alias := reg_detalle_in.TABLE_LKUP;
              v_alias := v_alias_table_look_up;
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              --l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
              /* (20170306) Angel Ruiz. NF: Sintasix Beeline */
              if (reg_detalle_in.OUTER = 'Y') then
                l_FROM (l_FROM.last) := 'LEFT OUTER JOIN ' || mitabla_look_up || ' ';
              else
                l_FROM (l_FROM.last) := 'JOIN ' || mitabla_look_up || ' ';
              end if;
              /* (20170306) Angel Ruiz. FIN NF */
              l_FROM_solo_tablas (l_FROM_solo_tablas.last) := ', ' || mitabla_look_up;
            end if;
          end if;
          
        end if;

        /* Miramos la parte de las condiciones */
        /* Puede haber varios campos por los que hacer LookUp y por lo tanto JOIN */
        /* (20160302) Angel Ruiz. NF: Campos separados por ; */
        --table_columns_lkup := split_string_coma (reg_detalle_in.TABLE_COLUMN_LKUP);
        --ie_column_lkup := split_string_coma (reg_detalle_in.IE_COLUMN_LKUP);
        table_columns_lkup := split_string_punto_coma (reg_detalle_in.TABLE_COLUMN_LKUP);
        ie_column_lkup := split_string_punto_coma (reg_detalle_in.IE_COLUMN_LKUP);
        
        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL SELECT */
        /****************************************************************************/

        if (reg_detalle_in.LKUP_COM_RULE is not null) then
          /* Ocurre que tenemos una regla compuesta, un LKUP con una condicion */
          cadena := trim(reg_detalle_in.LKUP_COM_RULE);
          pos_del_si := instr(cadena, 'SI');
          pos_del_then := instr(cadena, 'THEN');
          pos_del_else := instr(cadena, 'ELSE');
          pos_del_end := instr(cadena, 'END');  
          condicion := substr(cadena,pos_del_si+length('SI'), pos_del_then-(pos_del_si+length('SI')));
          condicion_pro := procesa_COM_RULE_lookup(condicion);
          constante := substr(cadena, pos_del_else+length('ELSE'),pos_del_end-(pos_del_else+length('ELSE')));
          valor_retorno := 'CASE WHEN ' || trim(condicion_pro) || ' THEN NVL(' || reg_detalle_in.VALUE || ', '' '') ELSE ' || trim(constante) || ' END';
        else
          /* Construyo el campo de SELECT */
          --valor_retorno :=  'NVL(' || reg_detalle_in.VALUE || ', '' '')';
          valor_retorno :=  reg_detalle_in.VALUE;
        end if;
        
        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL WHERE */
        /****************************************************************************/
        l_WHERE_ON_clause.delete;   /* (20170306) Angel Ruiz */
        
        if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
          FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
          LOOP
            --l_WHERE.extend;
            l_WHERE_ON_clause.extend;   /* (20170306) Angel Ruiz. NF: Sintasix Beeline */
            
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Columna es: ' || ie_column_lkup(indx));
            
            if (l_WHERE_ON_clause.count = 1) then
              /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
              if (instr(upper(ie_column_lkup(indx)), 'DECODE') > 0 and instr(upper(table_columns_lkup(indx)), 'DECODE') > 0) then
                l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_decode(ie_column_lkup(indx), v_alias_table_base_name, 0) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
              elsif (instr(upper(ie_column_lkup(indx)), 'DECODE') > 0 and instr(upper(table_columns_lkup(indx)), 'DECODE') = 0) then
                l_WHERE_ON_clause(l_WHERE_ON_clause.last) := transformo_decode(ie_column_lkup(indx), v_alias_table_base_name, 0) || ' = ' || v_alias || '.' || table_columns_lkup(indx);
              elsif (instr(upper(ie_column_lkup(indx)), 'DECODE') = 0 and instr(upper(table_columns_lkup(indx)), 'DECODE') > 0) then
                l_WHERE_ON_clause(l_WHERE_ON_clause.last) := v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
              elsif (instr(upper(table_columns_lkup(indx)), 'BETWEEN') > 0) then
                l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' ' || transformo_between(v_alias, table_columns_lkup(indx), false);
              elsif (regexp_count(upper(table_columns_lkup(indx)), 'TRIM *\(') > 0) then
                l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || table_columns_lkup(indx);
              else
                l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx);
              end if;
            else  /* siguientes elementos del where */
              if (instr(upper(ie_column_lkup(indx)), 'DECODE') > 0 and instr(upper(table_columns_lkup(indx)), 'DECODE') > 0) then
                l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_decode(ie_column_lkup(indx), v_alias_table_base_name, 0) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
              elsif (instr(upper(ie_column_lkup(indx)), 'DECODE') > 0 and instr(upper(table_columns_lkup(indx)), 'DECODE') = 0) then
                l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_decode(ie_column_lkup(indx), v_alias_table_base_name, 0) || ' = ' || v_alias || '.' || table_columns_lkup(indx);
              elsif (instr(upper(ie_column_lkup(indx)), 'DECODE') = 0 and instr(upper(table_columns_lkup(indx)), 'DECODE') > 0) then
                l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
              elsif (instr(upper(table_columns_lkup(indx)), 'BETWEEN') > 0) then
                l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' ' || transformo_between(v_alias, table_columns_lkup(indx), false);
              elsif (regexp_count(upper(table_columns_lkup(indx)), 'TRIM *\(') > 0) then
                l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || table_columns_lkup(indx);
              else
                l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx);
              end if;
            end if;
          END LOOP;
        else    /* Solo hay un campo condicion */
          /* Miramos si la tabla con la que hay que hacer LookUp es una tabla de rangos */
          l_WHERE_ON_clause.extend;
          if (instr (reg_detalle_in.TABLE_LKUP,'RANGO') > 0) then
            if (l_WHERE_ON_clause.count = 1) then
              l_WHERE_ON_clause(l_WHERE_ON_clause.last) := v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
              l_WHERE_ON_clause.extend;
              l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' <= ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4);
            else
              l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.'  || reg_detalle_in.TABLE_COLUMN_LKUP;
              l_WHERE_ON_clause.extend;
              l_WHERE_ON_clause(l_WHERE_ON_clause.last) := ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' <= ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4);
            end if;
          else
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. La Columna es: ' || reg_detalle_in.IE_COLUMN_LKUP);
            if (l_WHERE_ON_clause.count = 1) then /* si es el primer campo del WHERE */
              --l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              if (instr(reg_detalle_in.IE_COLUMN_LKUP, 'DECODE') > 0 or instr(reg_detalle_in.IE_COLUMN_LKUP, 'decode') > 0) then
                l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name, 0) ||  ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
              elsif (instr(upper(reg_detalle_in.TABLE_COLUMN_LKUP), 'BETWEEN') > 0) then
                l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' ' || transformo_between(v_alias, reg_detalle_in.TABLE_COLUMN_LKUP, false);
              elsif (regexp_count(upper(reg_detalle_in.TABLE_COLUMN_LKUP), 'TRIM *\(') > 0) then
                l_WHERE_ON_clause(l_WHERE_ON_clause.last) := v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP;
              else
                /* (20160630) Angel Ruiz. BUG. Ocurre que si los campos IE_COLUMN_LKUP o TABLE_COLUMN_LKUP ya estan calificados no hay que hacerlo */
                if (instr(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name || '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias || '.') = 0) then
                  /* (20161004) Angel Ruiz. BUG.Ocurre que puede ponersele al campo IE_COLUMN_LKUP un ALIAS. */
                  /* pero este ALIAS no corresponde con el ALIAS de la tabla TABLE_BASE_NAME, sino de otra tabla LOOKUP */
                  /* por lo que dejamos el campo IE_COLUMN_LKUP con el alias que trae */
                  if instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0 then
                    /* (20161004) Angel Ruiz. Dejamos el campo IE_COLUMN_LKUP con el alias que trae */
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  else                  
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) := v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  end if;
                elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name || '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias || '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP;
                else
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) := reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                end if;
              end if;
            else  /* sino es el primer campo del Where  */
              --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              if (instr(reg_detalle_in.IE_COLUMN_LKUP, 'DECODE') > 0 or instr(reg_detalle_in.IE_COLUMN_LKUP, 'decode') > 0) then
                l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name, 0) || ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
              elsif (instr(upper(reg_detalle_in.TABLE_COLUMN_LKUP), 'BETWEEN') > 0 ) then
                l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' ' || transformo_between(v_alias, reg_detalle_in.TABLE_COLUMN_LKUP, false);
              elsif (regexp_count(upper(reg_detalle_in.TABLE_COLUMN_LKUP), 'TRIM *\(') > 0) then
                l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP;
              else
                /* (20160630) Angel Ruiz. BUG. Ocurre que si los campos IE_COLUMN_LKUP o TABLE_COLUMN_LKUP ya estan calificados no hay que hacerlo */
                if (instr(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name || '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias || '.') = 0) then
                  /* (20161004) Angel Ruiz. Ocurre que puede ponersele al campo IE_COLUMN_LKUP un ALIAS. */
                  /* pero este ALIAS no corresponde con el ALIAS de la tabla TABLE_BASE_NAME, sino de otra tabla LOOKUP */
                  /* por lo que dejamos el campo IE_COLUMN_LKUP con el alias que trae */
                  if instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0 then
                    /* (20161004) Angel Ruiz. Dejamos el campo IE_COLUMN_LKUP con el alias que trae */
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  else
                    l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  end if;
                elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name || '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias || '.') > 0) then
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP;
                else
                  l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                end if;
              end if;
            end if;
          end if;
        end if;
        if (reg_detalle_in.TABLE_LKUP_COND is not null) then
          /* Existen condiciones en la tabla de Look Up que hay que introducir*/
          --l_WHERE.extend;
          l_WHERE_ON_clause.extend;   /* (20170306) Angel Ruiz. NF: Sintasix Beeline */
          
          --l_WHERE(l_WHERE.last) :=  ' AND ' || procesa_condicion_lookup(reg_detalle_in.TABLE_LKUP_COND, v_alias);
          /* (20160412) Angel Ruiz. BUG: Si la tabla de LookUP es con OUTER entonces */
          /* debemos procesar la condicion para ponerle el signo outer por dentro */
          l_WHERE_ON_clause(l_WHERE_ON_clause.last) :=  ' AND ' || procesa_condicion_lookup (reg_detalle_in.TABLE_LKUP_COND, v_alias, FALSE);
        end if;
        /* (20170306) Angel Ruiz */
        /* Modifico esta parte para HIVE */
        l_FROM (l_FROM.last) := l_FROM (l_FROM.last) || chr(10) || ' ON (';
        FOR indx IN l_WHERE_ON_clause.FIRST .. l_WHERE_ON_clause.LAST
        LOOP
          l_FROM (l_FROM.last) := l_FROM (l_FROM.last) || l_WHERE_ON_clause(indx);
        END LOOP;
        l_FROM (l_FROM.last) := l_FROM (l_FROM.last) || ')';
        
      when 'LKUPD' then
        if (reg_detalle_in.LKUP_COM_RULE is not null) then
          /* Ocurre que tenemos una regla compuesta, un LKUP con una condicion */
          cadena := trim(reg_detalle_in.LKUP_COM_RULE);
          pos_del_si := instr(cadena, 'SI');
          pos_del_then := instr(cadena, 'THEN');
          pos_del_else := instr(cadena, 'ELSE');
          pos_del_end := instr(cadena, 'END');  
          condicion := substr(cadena,pos_del_si+length('SI'), pos_del_then-(pos_del_si+length('SI')));
          condicion_pro := procesa_COM_RULE_lookup(condicion);
          constante := substr(cadena, pos_del_else+length('ELSE'),pos_del_end-(pos_del_else+length('ELSE')));
          valor_retorno := 'CASE WHEN ' || trim(condicion_pro) || ' THEN NVL(' || reg_detalle_in.VALUE || ', '' '') ELSE ' || trim(constante) || ' END';
        else
          valor_retorno := reg_detalle_in.VALUE;
        end if;
        
      when 'FUNCTION' then
        /* se trata de la regla FUNCTION */
        if (reg_detalle_in.LKUP_COM_RULE is not null) then
          /* Ocurre que tenemos una regla compuesta, un LKUP con una condicion */
          cadena := trim(reg_detalle_in.LKUP_COM_RULE);
          pos_del_si := instr(cadena, 'SI');
          pos_del_then := instr(cadena, 'THEN');
          pos_del_else := instr(cadena, 'ELSE');
          pos_del_end := instr(cadena, 'END');  
          condicion := substr(cadena,pos_del_si+length('SI'), pos_del_then-(pos_del_si+length('SI')));
          condicion_pro := procesa_COM_RULE_lookup(condicion);
          constante := substr(cadena, pos_del_else+length('ELSE'),pos_del_end-(pos_del_else+length('ELSE')));
          valor_retorno := 'CASE WHEN ' || trim(condicion_pro) || ' THEN NVL(' || v_alias || '.' || reg_detalle_in.VALUE || ', -2) ELSE ' || trim(constante) || ' END';
        else
          valor_retorno := 'PKG_' || reg_detalle_in.TABLE_NAME || '.' || 'LK_' || reg_detalle_in.TABLE_LKUP || ' (' || reg_detalle_in.IE_COLUMN_LKUP || ')';
        end if;
      when 'DLOAD' then
        --valor_retorno :=  'TO_DATE (''&' || '2'', ''YYYYMMDD'')';
        valor_retorno :=  'date_format (''&' || '2'', ''yyyyMMdd'')';
      when 'DSYS' then
        --valor_retorno :=  'SYSDATE';
        valor_retorno :=  'current_date';
      when 'CODE' then
        posicion := instr(reg_detalle_in.VALUE, 'VAR_IVA');
        if (posicion >0) then
          cad_pri := substr(reg_detalle_in.VALUE, 1, posicion-1);
          cad_seg := substr(reg_detalle_in.VALUE, posicion + length('VAR_IVA'));
          valor_retorno :=  cad_pri || '21' || cad_seg;
        else
          valor_retorno :=  reg_detalle_in.VALUE;
        end if;
        posicion := instr(valor_retorno, 'VAR_FCH_CARGA');
        if (posicion >0) then
          if (instr(valor_retorno, 'date_format') = 0) then
            /*(20170228) Angel Ruiz. Miro si viene un date_format */
            cad_pri := substr(valor_retorno, 1, posicion-1);
            cad_seg := substr(valor_retorno, posicion + length('VAR_FCH_CARGA'));
            --valor_retorno := cad_pri || ' to_date(''&' || '2'', ''yyyymmdd'') ' || cad_seg;
            valor_retorno := cad_pri || ' date_format(''&' || '2'', ''yyyyMMdd'') ' || cad_seg;
          else
            cad_pri := substr(valor_retorno, 1, posicion-1);
            cad_seg := substr(valor_retorno, posicion + length('VAR_FCH_CARGA'));
            valor_retorno := cad_pri || ' &' || '2 ' || cad_seg;
          end if;
        end if;
      when 'HARDC' then
        /* (20160406) Angel Ruiz. Los campos HARDC no traeran comillas */
        /* Heos de detactar si son caracter para ponerlas */
        if (reg_detalle_in.TYPE <> 'NU') then
          if instr(reg_detalle_in.VALUE, '''') > 0 then
            /* realmente trae comillas con lo que me quedo con la parte que no tiene comillas y se las pongo despues */
            valor_retorno := '''' || regexp_substr(reg_detalle_in.VALUE, '[^'']*') || '''';
          else
            valor_retorno := '''' || reg_detalle_in.VALUE || '''';
          end if;
        else
          valor_retorno := reg_detalle_in.VALUE;
        end if;
      when 'SEQ' then
        valor_retorno := OWNER_EX || '.SEQ_' || nombre_tabla_reducido || '.NEXTVAL';
        --if (instr(reg_detalle_in.VALUE, '.NEXTVAL') > 0) then
        --  valor_retorno := '    ' || reg_detalle_in.VALUE;
        --else
        --  valor_retorno := '    ' || reg_detalle_in.VALUE || '.NEXTVAL';
        --end if;
      when 'SEQG' then
        valor_retorno := ''' || var_seqg || ''';
      when 'BASE' then
        /* Se toma el valor del campo de la tabla de staging */
        /* (20170228) Angel Ruiz. Detecto si el campo tiene propietario. Si no es así lo anyado */
        if (instr(reg_detalle_in.VALUE, '.') > 0) then
          valor_retorno := reg_detalle_in.VALUE;
        else
          valor_retorno := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.VALUE;
        end if;
      when 'VAR_FCH_INICIO' then
        --valor_retorno :=  '    ' || ''' || var_fch_inicio || ''';
        --valor_retorno :=  'TO_CHAR(SYSDATE, ''YYYY-MM-DD'')';
        valor_retorno :=  'date_format(current_date, ''yyyyMMdd'')';
        --valor_retorno :=  '    TO_DATE('''''' || fch_registro_in || '''''', ''''YYYYMMDDHH24MISS'''')'; /*(20151221) Angel Ruiz BUG. Debe insertarse la fecha de inicio del proceso de insercion */
      when 'VAR_FILE_NAME' then
        /* (20161104) Angel Ruiz. Nombre del fichero desde el que vamos a cargar */
        pos_ini_fecha := instr(reg_scenario.INTERFACE_NAME, '_YYYYMMDD');
        if (pos_ini_fecha > 0) then
          pos_fin_fecha := pos_ini_fecha + length ('_YYYYMMDD');
          if (v_type_validation = 'I') then
            if v_tabla_dinamica = true then
              valor_retorno :=  '     ''' || substr(reg_scenario.INTERFACE_NAME, 1, pos_ini_fecha -1) || '_&' || '2' || substr(reg_scenario.INTERFACE_NAME, pos_fin_fecha) || '''';
            else
              valor_retorno :=  '     ''' || substr(reg_scenario.INTERFACE_NAME, 1, pos_ini_fecha -1) || '_&' || '1' || substr(reg_scenario.INTERFACE_NAME, pos_fin_fecha) || '''';
            end if;
          else
            if v_tabla_dinamica = true then
              valor_retorno :=  '     ''' || substr(reg_scenario.INTERFACE_NAME, 1, pos_ini_fecha -1) || '_&' || '3' || substr(reg_scenario.INTERFACE_NAME, pos_fin_fecha) || '''';
            else
              valor_retorno :=  '     ''' || substr(reg_scenario.INTERFACE_NAME, 1, pos_ini_fecha -1) || '_&' || '2' || substr(reg_scenario.INTERFACE_NAME, pos_fin_fecha) || '''';
            end if;
          end if;
          v_fecha_ini_param:=true; /*(20161104) Angel Ruiz.  BUG*/
        else
            valor_retorno := '     ''' || reg_scenario.INTERFACE_NAME || '''';
        end if;
        v_fecha_ini_param := true;  /* Ponemos a true este switch ya que vamos a pasar la fecha como parametro al fichero sql */
      when 'VAR' then
        /* Se toma el valor de una variable de entorno */
        if reg_detalle_in.VALUE =  'VAR_FCH_CARGA' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
          if (v_type_validation = 'I') then
            if v_tabla_dinamica = true then
              --valor_retorno :=  '     ' ||  'TO_DATE (''&' || '2'', ''YYYY-MM-DD'')';
              valor_retorno :=  '     ' ||  'date_format (''&' || '2'', ''yyyyMMdd'')';
            else
              --valor_retorno :=  '     ' ||  'TO_DATE (''&' || '1'', ''YYYY-MM-DD'')';
              valor_retorno :=  '     ' ||  'date_format (''&' || '1'', ''yyyyMMdd'')';
            end if;
          else
            if v_tabla_dinamica = true then
              --valor_retorno :=  '     ' ||  'TO_DATE (''&' || '3'', ''YYYY-MM-DD'')';
              valor_retorno :=  '     ' ||  'date_format (''&' || '3'', ''yyyyMMdd'')';
            else
              --valor_retorno :=  '     ' ||  'TO_DATE (''&' || '2'', ''YYYY-MM-DD'')';
              valor_retorno :=  '     ' ||  'date_format (''&' || '2'', ''yyyyMMdd'')';
            end if;
          end if;
          v_fecha_ini_param:=true; /*(20161104) Angel Ruiz.  BUG*/
          --valor_retorno := '    ' || ''' || fch_datos_in || ''';        
        end if;
        if reg_detalle_in.VALUE =  'VAR_PAIS_TM' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
          valor_retorno := '1';
        end if;
      when 'VAR_USER' then
          valor_retorno := '    ' || '''#VAR_USER#''';
      when 'LKUPN' then
        /* (20150824) ANGEL RUIZ. Nueva Regla. Permite rescatar un campo numerico de la tabla de look up y hacer operaciones con el */
        l_FROM.extend;
        if (instr (reg_detalle_in.TABLE_LKUP,'SELECT ') > 0) then
          /* Aparecen queries en lugar de tablas en la columna de nombre de tabla para LookUp */
          v_alias := 'LKUP_' || l_FROM.count;
          mitabla_look_up := '(' || reg_detalle_in.TABLE_LKUP || ') "LKUP_' || l_FROM.count || '"';
          l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
        else
          /* (20150112) Angel Ruiz */
          /* Puede ocurrir que se se tenga varias veces la misma LookUp pero para campo diferentes */
          /* lo que se traduce en que hay que crear ALIAS */
          /* BUSCAMOS SI YA ESTABA LA TABLA INCLUIDA EN EL FROM*/
          v_encontrado:='N';
          FOR indx IN l_FROM.FIRST .. l_FROM.LAST
          LOOP
            --if (instr(l_FROM(indx),  reg_detalle_in.TABLE_LKUP, 0)) then
            --regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0
            if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
            --if (l_FROM(indx) = ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP) then
              /* La misma tabla ya estaba en otro lookup */
              v_encontrado:='Y';
            end if;
          END LOOP;
          if (v_encontrado='Y') then
            v_alias := reg_detalle_in.TABLE_LKUP || '_' || l_FROM.count;
            l_FROM (l_FROM.last) := ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP || ' "' || v_alias || '"' ;
          else
            v_alias := reg_detalle_in.TABLE_LKUP;
            l_FROM (l_FROM.last) := ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP;
          end if;
        end if;
        /* Miramos la parte de las condiciones */
        /* Puede haber varios campos por los que hacer LookUp y por lo tanto JOIN */
        table_columns_lkup := split_string_coma (reg_detalle_in.TABLE_COLUMN_LKUP);
        ie_column_lkup := split_string_coma (reg_detalle_in.IE_COLUMN_LKUP);
        /* Le añadimos al nombre del campo de la tabla de LookUp su Alias */
        v_value := proceso_campo_value (reg_detalle_in.VALUE, v_alias);
        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL SELECT */
        /****************************************************************************/
        if (reg_detalle_in.LKUP_COM_RULE is not null) then
          /* Ocurre que tenemos una regla compuesta, un LKUP con una condicion */
          cadena := trim(reg_detalle_in.LKUP_COM_RULE);
          pos_del_si := instr(cadena, 'SI');
          pos_del_then := instr(cadena, 'THEN');
          pos_del_else := instr(cadena, 'ELSE');
          pos_del_end := instr(cadena, 'END');  
          condicion := substr(cadena,pos_del_si+length('SI'), pos_del_then-(pos_del_si+length('SI')));
          condicion_pro := procesa_COM_RULE_lookup(condicion);
          constante := substr(cadena, pos_del_else+length('ELSE'),pos_del_end-(pos_del_else+length('ELSE')));
          valor_retorno := 'CASE WHEN ' || trim(condicion_pro) || ' THEN NVL(' || v_value || ', -2) ELSE ' || trim(constante) || ' END';
        else
          /* Construyo el campo de SELECT */
          if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
            valor_retorno := 'CASE WHEN (';
            FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
            LOOP
              SELECT * INTO l_registro
              FROM ALL_TAB_COLUMNS
              WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
              COLUMN_NAME = TRIM(ie_column_lkup(indx));
            
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then  /* se trata de un campo VARCHAR */
                if (indx = 1) then
                  valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IN (''''NI#'''', ''''NO INFORMADO'''') ';
                else
                  valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IN (''''NI#'''', ''''NO INFORMADO'''') ';
                end if;
              else 
                if (indx = 1) then
                  valor_retorno := valor_retorno || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' = -3 ';
                else
                  valor_retorno := valor_retorno || 'OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' IS NULL OR ' || reg_detalle_in.TABLE_BASE_NAME || '.' || l_registro.COLUMN_NAME || ' = -3 ';
                end if;
              end if;
            END LOOP;
            valor_retorno := valor_retorno || ') THEN -3 ELSE ' || 'NVL(' || v_value || ', -2) END';
          else
            valor_retorno :=  'NVL(' || v_value || ', -2)';
          end if;

        end if;

        /****************************************************************************/
        /* CONTRUIMOS EL CAMPO PARA LA PARTE DEL WHERE */
        /****************************************************************************/

        if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
          FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
          LOOP
            l_WHERE.extend;
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Columna es: ' || ie_column_lkup(indx));
            
            /* Recojo de que tipo son los campos con los que vamos a hacer LookUp */
            SELECT * INTO l_registro
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
            COLUMN_NAME = TRIM(ie_column_lkup(indx));
            if (l_WHERE.count = 1) then
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  l_WHERE(l_WHERE.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''''NI#'''')' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||  ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                end if;
              else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) :=  'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) ||', -3)' ||' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                l_WHERE(l_WHERE.last) :=  reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
              end if;
            else
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', ''''NI#'''')' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                end if;
              else /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ', -3)' || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
              end if;
            end if;
          END LOOP;
        else    /* Solo hay un campo condicion */
          
          /* Miramos si la tabla con la que hay que hacer LookUp es una tabla de rangos */
          l_WHERE.extend;
          if (instr (reg_detalle_in.TABLE_LKUP,'RANGO') > 0) then
            if (l_WHERE.count = 1) then
              l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              l_WHERE.extend;
              l_WHERE(l_WHERE.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' <= ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4) || ' (+)';
            else
              l_WHERE(l_WHERE.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' >= ' || v_alias || '.'  || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              l_WHERE.extend;
              l_WHERE(l_WHERE.last) := ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' <= ' || v_alias || '.' || 'MAX' || substr(reg_detalle_in.TABLE_COLUMN_LKUP, 4) || ' (+)';
            end if;
          else
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. La Columna es: ' || reg_detalle_in.IE_COLUMN_LKUP);
            SELECT * INTO l_registro
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME =  reg_detalle_in.TABLE_BASE_NAME and
            COLUMN_NAME = reg_detalle_in.IE_COLUMN_LKUP;
            if (l_WHERE.count = 1) then /* si es el primer campo del WHERE */
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''''NI#'''')' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                else
                  l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                end if;
              else    /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                l_WHERE(l_WHERE.last) := reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              end if;
            else  /* sino es el primer campo del Where  */
              if (instr(l_registro.DATA_TYPE, 'VARCHAR') > 0) then     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo CARACTER */
                if (l_registro.DATA_LENGTH <3 and l_registro.NULLABLE = 'Y') then
                  l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', ''''NI#'''')' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                end if;
              else     /* Estamos haciendo JOIN con la tabla de LookUp COD_* por un campo NUMBER */
                --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              end if;
            end if;
          end if;
        end if;
        if (reg_detalle_in.TABLE_LKUP_COND is not null) then
          /* Existen condiciones en la tabla de Look Up que hay que introducir*/
          l_WHERE.extend;
          --l_WHERE(l_WHERE.last) :=  ' AND ' || procesa_condicion_lookup(reg_detalle_in.TABLE_LKUP_COND, v_alias);
          /* (20160412) Angel Ruiz. BUG: Si la tabla de LookUP es con OUTER entonces */
          /* debemos procesar la condicion para ponerle el signo outer por dentro */
          if (reg_detalle_in."OUTER" IS NOT NULL and reg_detalle_in."OUTER"='Y') then
            l_WHERE(l_WHERE.last) :=  ' AND ' || procesa_condicion_lookup(reg_detalle_in.TABLE_LKUP_COND, v_alias, TRUE);
          else
            l_WHERE(l_WHERE.last) :=  ' AND ' || procesa_condicion_lookup(reg_detalle_in.TABLE_LKUP_COND, v_alias, FALSE);
          end if;
          
        end if;
      end case;
    return valor_retorno;
  end;

/*************/
  function genera_encabezado_funcion_pkg (reg_lookup_in in MTDT_TC_LOOKUP%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (500);
    lkup_columns                list_strings := list_strings();
  begin
    /* Se trata de hacer el LOOK UP con la tabla dimension */
    /* Miramos si hay varios campos por los que hay que hay que hacer JOIN */
    dbms_output.put_line('Estoy en genera_encabezado_funcion_pkg. Antes de llamar a string coma');
    lkup_columns := split_string_coma (reg_lookup_in.TABLE_COLUMN_LKUP);
    dbms_output.put_line('Estoy en genera_encabezado_funcion_pkg. Despues de llamar a string coma');
    if (lkup_columns.COUNT > 1)
    then
      valor_retorno := '  FUNCTION ' || 'LK_' || reg_lookup_in.TABLE_LKUP || ' (';
      FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
      LOOP
        if indx = 1 then
          valor_retorno := valor_retorno || lkup_columns(indx) || '_IN ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || '%TYPE';
        else
          valor_retorno := valor_retorno || ', ' || lkup_columns(indx) || '_IN ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || '%TYPE';
        end if;
      END LOOP;
      valor_retorno := valor_retorno || ') return ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.VALUE || '%TYPE RESULT_CACHE;';
    else        
      valor_retorno := '  FUNCTION ' || 'LK_' || reg_lookup_in.TABLE_LKUP || ' (cod_in IN ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || '%TYPE) return ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.value || '%TYPE RESULT_CACHE;';
    end if;
    dbms_output.put_line('Justo antes de retornar');
    dbms_output.put_line('Retorno es: ' || valor_retorno);
    return valor_retorno;
  end;

/************/

  function gen_encabe_regla_function (reg_function_in in MTDT_TC_FUNCTION%rowtype) return VARCHAR2 is
    valor_retorno VARCHAR (250);
    lkup_columns                list_strings := list_strings();
  begin
    valor_retorno := '  FUNCTION ' || 'LK_' || reg_function_in.VALUE || ';';
    return valor_retorno;
  end gen_encabe_regla_function;

  procedure genera_cuerpo_regla_function (reg_function_in in MTDT_TC_FUNCTION%rowtype) is
  begin
    UTL_FILE.put_line (fich_salida_pkg, '  FUNCTION ' || 'LK_' || reg_function_in.VALUE);
    --UTL_FILE.put_line (fich_salida_pkg, '  RESULT_CACHE');
    UTL_FILE.put_line (fich_salida_pkg, '  IS');
    UTL_FILE.put_line (fich_salida_pkg, '  BEGIN');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '    dbms_output.put_line (''Aqui iria el cuerpo de la funcion'');');
    UTL_FILE.put_line (fich_salida_pkg, '    /* AQUI IRIA EL CUERPO DE LA FUNCION */');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '  END ' || 'LK_' || reg_function_in.TABLE_LKUP || ';');
  end genera_cuerpo_regla_function;

/************/
  procedure genera_cuerpo_funcion_pkg (reg_lookup_in in MTDT_TC_LOOKUP%rowtype) is
    valor_retorno VARCHAR (500);
    lkup_columns                list_strings := list_strings();

  begin
    /* Se trata de hacer el LOOK UP con la tabla dimension */
    
    /* Miramos si hay varios campos por los que hay que hay que hacer JOIN */
    lkup_columns := split_string_coma (reg_lookup_in.TABLE_COLUMN_LKUP);
    if (lkup_columns.COUNT > 1)
    then
      valor_retorno := '  FUNCTION ' || 'LK_' || reg_lookup_in.TABLE_LKUP || ' (';
      FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
      LOOP
        if indx = 1 then
          valor_retorno := valor_retorno || lkup_columns(indx) || '_IN ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || '%TYPE';
        else
          valor_retorno := valor_retorno || ', ' || lkup_columns(indx) || '_IN ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || '%TYPE';
        end if;
      END LOOP;
      valor_retorno := valor_retorno || ') ';
      UTL_FILE.put_line (fich_salida_pkg, valor_retorno);
    else        
      UTL_FILE.put_line (fich_salida_pkg, '  FUNCTION ' || 'LK_' || reg_lookup_in.TABLE_LKUP || ' (cod_in IN ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || '%TYPE)'); 
    end if;
    UTL_FILE.put_line (fich_salida_pkg, '    return ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.value || '%TYPE');
    UTL_FILE.put_line (fich_salida_pkg, '    RESULT_CACHE RELIES_ON (' || reg_lookup_in.TABLE_LKUP || ')');
    UTL_FILE.put_line (fich_salida_pkg, '  IS');
    UTL_FILE.put_line (fich_salida_pkg, '    l_row     ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.VALUE || '%TYPE;');
    UTL_FILE.put_line (fich_salida_pkg, '  BEGIN');
    UTL_FILE.put_line (fich_salida_pkg, '    SELECT nvl(' || reg_lookup_in.VALUE || ', -2) INTO l_row'); 
    UTL_FILE.put_line (fich_salida_pkg, '    FROM ' || reg_lookup_in.TABLE_LKUP);
    if (lkup_columns.COUNT > 1) then
      valor_retorno := '    WHERE ' ;
      FOR indx IN lkup_columns.FIRST .. lkup_columns.LAST
      LOOP
        if indx = 1 then
          valor_retorno := valor_retorno || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || ' = ' || lkup_columns(indx) || '_in';
        else
          valor_retorno := valor_retorno || ' and ' || reg_lookup_in.TABLE_LKUP || '.' || lkup_columns(indx) || ' = ' || lkup_columns(indx) || '_in';
        end if;
      END LOOP;
      if (reg_lookup_in.TABLE_LKUP_COND IS NULL) THEN
        valor_retorno := valor_retorno || ';';
      else
        valor_retorno := valor_retorno || reg_lookup_in.TABLE_LKUP_COND || ';';
      end if;
      UTL_FILE.put_line (fich_salida_pkg, valor_retorno);
    else
      /* 20141204 Angel Ruiz - Anyadido para las tablas de LOOK UP que son un rango */
      if (instr (reg_lookup_in.TABLE_LKUP,'RANGO') > 0) then
        /* Se trata de una tabla de Rango y la trato diferente */
        if (reg_lookup_in.TABLE_LKUP_COND IS NULL) THEN
          UTL_FILE.put_line (fich_salida_pkg, '    WHERE cod_in >= ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' and  cod_in <= ' || reg_lookup_in.TABLE_LKUP || '.' || 'MAX' || substr(reg_lookup_in.TABLE_COLUMN_LKUP,4) || ';' );
        else
          UTL_FILE.put_line (fich_salida_pkg, '    WHERE cod_in >= ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' and  cod_in <= ' || reg_lookup_in.TABLE_LKUP || '.' || 'MAX' || substr(reg_lookup_in.TABLE_COLUMN_LKUP,4) || ' and ' || reg_lookup_in.TABLE_LKUP_COND || ';');
        end if;
      else 
        if (reg_lookup_in.TABLE_LKUP_COND IS NULL) THEN
        UTL_FILE.put_line (fich_salida_pkg, '    WHERE ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' = ' || 'cod_in;' );
        else
        UTL_FILE.put_line (fich_salida_pkg, '    WHERE ' || reg_lookup_in.TABLE_LKUP || '.' || reg_lookup_in.TABLE_COLUMN_LKUP || ' = ' || 'cod_in and ' || reg_lookup_in.TABLE_LKUP_COND || ';' );
        end if;
      end if;
    end if;
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '    RETURN l_row;');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '  exception');
    UTL_FILE.put_line (fich_salida_pkg, '  when NO_DATA_FOUND then');
    UTL_FILE.put_line (fich_salida_pkg, '    RETURN -2;');
    UTL_FILE.put_line (fich_salida_pkg, '  when others then');
    UTL_FILE.put_line (fich_salida_pkg, '    RETURN -2;');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '  END ' || 'LK_' || reg_lookup_in.TABLE_LKUP || ';');
    UTL_FILE.put_line (fich_salida_pkg, '');
    UTL_FILE.put_line (fich_salida_pkg, '');
 
  end genera_cuerpo_funcion_pkg;

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
  SELECT VALOR INTO REQ_NUMBER FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'REQ_NUMBER';  
  SELECT VALOR INTO OWNER_1 FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_1';
  SELECT VALOR INTO OWNER_2 FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_2';
  SELECT VALOR INTO OWNER_3 FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_3';
  SELECT VALOR INTO OWNER_4 FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_4';
  SELECT VALOR INTO OWNER_5 FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_5';
  SELECT VALOR INTO BD_SID FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'BD_SID';
  SELECT VALOR INTO BD_USR FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'BD_USR';
  SELECT VALOR INTO OWNER_EX FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_EX';
  SELECT VALOR INTO v_multiplicador_proc FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'MULTIPLICADOR_PROC';
  SELECT VALOR INTO v_ip_productivo FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'IP_PRODUCTIVO';  
  /* (20141223) FIN*/

  open MTDT_TABLA;
  loop
    fetch MTDT_TABLA
    into reg_tabla;
    exit when MTDT_TABLA%NOTFOUND;
    v_tabla_dinamica := false;  /* Por defecto cada interfaz no tiene tabla dinamica */
    v_fecha_ini_param := false; /* Por defecto cada interfaz no tiene fecha inicial */
    v_fecha_fin_param := false; /* Por defecto cada interfaz no tiene fecha final */
    v_hay_usu_owner := false; /* Por defecto cada interfaz no tiene usuario owner */
    dbms_output.put_line ('Estoy en el primero LOOP. La tabla que tengo es: $' || reg_tabla.TABLE_NAME || '$');
    
    /* (20160817) Angel Ruiz. Cambio temporal para adecuarse a la entrega de produccion*/
    --nombre_fich_carga := REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '.sh';
    --nombre_fich_carga := 'ONIX' || '_' || reg_tabla.TABLE_NAME || '.sh';
    /* (20160817) Angel Ruiz FIN Cambio temporal para adecuarse a la entrega de produccion*/
    /* (20160817) Angel Ruiz. Cambio temporal para adecuarse a la entrega de produccion*/
    --nombre_fich_pkg := REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '.sql';
    --nombre_fich_pkg := 'ONIX' || '_' || reg_tabla.TABLE_NAME || '.sql';
    /* (20160817) Angel Ruiz FIN Cambio temporal para adecuarse a la entrega de produccion*/
    --fich_salida_load := UTL_FILE.FOPEN ('SALIDA',nombre_fich_carga,'W');
    --fich_salida_exchange := UTL_FILE.FOPEN ('SALIDA',nombre_fich_exchange,'W');
    --fich_salida_pkg := UTL_FILE.FOPEN ('SALIDA',nombre_fich_pkg,'W');
    nombre_tabla_reducido := substr(reg_tabla.TABLE_NAME, 5); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
    --nombre_tabla_base_reducido := substr(reg_tabla.TABLE_BASE_NAME, 4); /* Le quito al nombre de la tabla los caracteres SA_ */
    /* Angel Ruiz (20150311) Hecho porque hay paquetes que no compilan porque el nombre es demasiado largo*/
    if (length(reg_tabla.TABLE_NAME) < 25) then
      nombre_proceso := reg_tabla.TABLE_NAME;
    else
      nombre_proceso := nombre_tabla_reducido;
    end if;
    /* (20150414) Angel Ruiz. Incidencia. El nombre de la particion es demasiado largo */
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
    /* (20160402) Angel Ruiz. NF:Si el fichero es de ancho fijo */
    /* hay que calcular la longitud de la linea antes de nada */
    v_type := 'N';
    select TYPE into v_type from MTDT_INTERFACE_SUMMARY where trim(CONCEPT_NAME) = trim(reg_tabla.TABLE_NAME);
    /* (20161104) Angel Ruiz. Comento el if siguiente para que calcule la longitud tanto en caso de extraccion */
    /* por posicion como por separador */
    --if (v_type = 'P') then
      /* Se trata de un fichero que se ha de extraer por posicion */
      --select sum(length) into v_line_size from MTDT_INTERFACE_DETAIL where trim(CONCEPT_NAME) = reg_tabla.TABLE_NAME;
      --select 
        --sum(to_number(case 
        --when instr(mtdt_interface_detail.length, ',') > 0 then 
          --(trim(substr(mtdt_interface_detail.length, 1, instr(mtdt_interface_detail.length, ',') - 1)))
        --when instr(mtdt_interface_detail.length, '.') > 0 then
         --(trim(substr(mtdt_interface_detail.length, 1, instr(mtdt_interface_detail.length, '.') - 1)))
        --else
          --trim(mtdt_interface_detail.length)
        --end)) into v_line_size
      --from mtdt_interface_detail where trim(CONCEPT_NAME) = reg_tabla.TABLE_NAME;      
    --end if;
    /* (20160606) Angel Ruiz. NF: Se trata de la validacion en la que en lugar de ir a un fichero plano */
    /* va directamente a las tablas de Stagin */
    select nvl(TYPE_VALIDATION, 'T') into v_type_validation from MTDT_INTERFACE_SUMMARY where trim(CONCEPT_NAME) = trim(reg_tabla.TABLE_NAME);
    
    --UTL_FILE.put_line (fich_salida_pkg,'WHENEVER SQLERROR EXIT 1;');
    --UTL_FILE.put_line (fich_salida_pkg,'WHENEVER OSERROR EXIT 2;');
    --UTL_FILE.put_line (fich_salida_pkg,'#');
    --UTL_FILE.put_line (fich_salida_pkg,'# Options file for Sqoop import');
    --UTL_FILE.put_line (fich_salida_pkg,'#');
    --UTL_FILE.put_line (fich_salida_pkg,'');
    --UTL_FILE.put_line (fich_salida_pkg,'# QUERY');
    --UTL_FILE.put_line (fich_salida_pkg,'--query');
    lista_scenarios_presentes.delete;
    
    /******/
    /* COMIEZO LA GENERACION DEL SQL */
    /******/
    
    /* GENERO los SQL para los escenarios */
    dbms_output.put_line ('Comienzo a generar los metodos para los escenarios');
    
    /* (20160714) Angel Ruiz. BUG. no realiza bien */
    /* la sustitucion de [YYYYMM]. Tengo que buscar primero de todo si  */
    /* hay tablas dinamicas, es decir, si la cadena [YYYYMM] aparece en */
    /* la especificacion del todo interfaz */

    v_contador:=0;
    select count(*) into v_contador from MTDT_EXT_DETAIL where 
    trim(MTDT_EXT_DETAIL.TABLE_NAME) = reg_tabla.TABLE_NAME and 
    instr(MTDT_EXT_DETAIL.TABLE_LKUP, '[YYYYMM]') > 0;
    if (v_contador > 0) then
      v_tabla_dinamica := true;
    end if;
    v_contador:=0;
    select count(*) into v_contador from MTDT_EXT_DETAIL where 
    trim(MTDT_EXT_DETAIL.TABLE_NAME) = reg_tabla.TABLE_NAME and 
    instr(MTDT_EXT_DETAIL.TABLE_BASE_NAME, '[YYYYMM]') > 0;
    if (v_contador > 0) then
      v_tabla_dinamica := true;
    end if;
    /* Tambien puede aparecer [YYYYMM] en TABLE_BASE_NAME */
    v_contador:=0;
    select count(*) into v_contador from MTDT_EXT_SCENARIO where 
    trim(MTDT_EXT_SCENARIO.TABLE_NAME) = reg_tabla.TABLE_NAME and 
    instr(MTDT_EXT_SCENARIO.TABLE_BASE_NAME, '[YYYYMM]') > 0;
    if (v_contador > 0) then
      v_tabla_dinamica := true;
    end if;
    /* Tambien puede aparecer [YYYYMM] en FILTER */
    v_contador:=0;
    select count(*) into v_contador from MTDT_EXT_SCENARIO where 
    trim(MTDT_EXT_SCENARIO.TABLE_NAME) = reg_tabla.TABLE_NAME and 
    instr(MTDT_EXT_SCENARIO.FILTER, '[YYYYMM]') > 0;
    if (v_contador > 0) then
      v_tabla_dinamica := true;
    end if;

    v_contador:=0;
    select count(*) into v_contador from MTDT_EXT_SCENARIO where
    TRIM(MTDT_EXT_SCENARIO.TABLE_NAME) = reg_tabla.TABLE_NAME and
    instr(MTDT_EXT_SCENARIO.FILTER, '#FCH_INI#') > 0;
    if (v_contador > 0) then
      v_fecha_ini_param:=true;
    end if;
    v_contador:=0;
    select count(*) into v_contador from MTDT_EXT_DETAIL where
    TRIM(MTDT_EXT_DETAIL.TABLE_NAME) = reg_tabla.TABLE_NAME and
    instr(MTDT_EXT_DETAIL.TABLE_LKUP, '#FCH_INI#') > 0;
    if (v_contador > 0) then
      v_fecha_ini_param:=true;
    end if;
    v_contador:=0;
    select count(*) into v_contador from MTDT_EXT_SCENARIO where
    TRIM(MTDT_EXT_SCENARIO.TABLE_NAME) = reg_tabla.TABLE_NAME and
    instr(MTDT_EXT_SCENARIO.FILTER, '#FCH_FIN#') > 0;
    if (v_contador > 0) then
      v_fecha_fin_param:=true;
    end if;
    v_contador:=0;
    select count(*) into v_contador from MTDT_EXT_DETAIL where
    TRIM(MTDT_EXT_DETAIL.TABLE_NAME) = reg_tabla.TABLE_NAME and
    instr(MTDT_EXT_DETAIL.TABLE_LKUP, '#FCH_FIN#') > 0;
    if (v_contador > 0) then
      v_fecha_ini_param:=true;
    end if;
    /* (20160714) Fin BUG.*/
    
    
    v_hay_sce_COMPUESTO := false;
    open MTDT_SCENARIO (reg_tabla.TABLE_NAME);
    loop
      fetch MTDT_SCENARIO
      into reg_scenario;
      exit when MTDT_SCENARIO%NOTFOUND;
      dbms_output.put_line ('Estoy en el segundo LOOP. La tabla que tengo es: ' || reg_tabla.TABLE_NAME || '. El escenario es: ' || reg_scenario.SCENARIO);
      /* Elaboramos la implementacion de las funciones de LOOK UP antes de nada */
      
      /* Guardamos una lista con los escenarios que posee la tabla que vamos a cargar */
      lista_scenarios_presentes.EXTEND;
      --lista_scenarios_presentes(lista_scenarios_presentes.LAST) := 'N';
      lista_scenarios_presentes(lista_scenarios_presentes.LAST) := reg_scenario.SCENARIO;
      if (instr(reg_scenario.SCENARIO, 'COMP:') > 0)then
        /* se trata del scenario COMPUESTO de varios otros escenarios */
        /* por lo que tenemos que analizar las operaciones conjunto que tenemos */
        v_lista_elementos_scenario := split_string_blanco (trim(substr(reg_scenario.SCENARIO, instr(reg_scenario.SCENARIO, ':') + 1)));
        v_hay_sce_COMPUESTO := true;
      end if;
    end loop; /* fin del LOOP MTDT_SCENARIO  */
    close MTDT_SCENARIO;


    /* GENERACION DEL PACKAGE BODY */

    dbms_output.put_line ('Estoy en PACKAGE IMPLEMENTATION. :-)');
    
    /* Tercero genero los cuerpos de los metodos que implementan los escenarios */
    
    /* (20170519) Angel Ruiz. MOdificacion para permitir UNION ALL*/
    /* Si solo hay un escenario, naturalmente este no tendra escenarios compuestos */
    /* por lo que introduzco el unico escenario que hay en la coleccion */
    if (lista_scenarios_presentes.COUNT = 1) then
      v_lista_elementos_scenario.delete; /* primero limpio la lista por si tenia valores */
      v_lista_elementos_scenario.extend;
      v_lista_elementos_scenario(v_lista_elementos_scenario.LAST) := lista_scenarios_presentes(lista_scenarios_presentes.COUNT);
    end if;
    /* (20170519) Angel Ruiz. fin */    


    
    v_num_scenarios := 0;
    v_num_secuencial := 0;
    FOR ind_scenario IN v_lista_elementos_scenario.FIRST .. v_lista_elementos_scenario.LAST
    LOOP
      dbms_output.put_line ('El escenario tomado de la lista de escenarios es: #' || v_lista_elementos_scenario(ind_scenario) || '#');
      if (
      UPPER(TRIM(v_lista_elementos_scenario(ind_scenario))) <> 'UNION' and
      UPPER(TRIM(v_lista_elementos_scenario(ind_scenario))) <> 'INTERSECT' and
      UPPER(TRIM(v_lista_elementos_scenario(ind_scenario))) <> 'ALL'
      ) then
        /* Se trata de uno de los operadores que unen los scenarios compuestos */
    
        open MTDT_SCENARIO (reg_tabla.TABLE_NAME);
        loop
          fetch MTDT_SCENARIO
          into reg_scenario;
          exit when MTDT_SCENARIO%NOTFOUND;
          if (UPPER(TRIM(reg_scenario.SCENARIO)) = UPPER(TRIM(v_lista_elementos_scenario(ind_scenario)))) then
            v_num_scenarios := v_num_scenarios + 1;
            dbms_output.put_line ('Estoy en el segundo LOOP. La tabla que tengo es: ' || reg_tabla.TABLE_NAME || '. El escenario es: ' || reg_scenario.SCENARIO);
            /* PROCESO EL ESCENARIO */
            dbms_output.put_line ('Estoy dentro del scenario ' || reg_scenario.SCENARIO);
            --UTL_FILE.put_line(fich_salida_pkg, '');
            if ((reg_scenario.TABLE_TYPE = 'F' and v_hay_sce_COMPUESTO = false) or reg_scenario.TABLE_TYPE = 'C') then
              /* (20160418) Angel Ruiz. Modificacion para los scenarios compuestos. */
              /* No se genera codigo SQL para los escenarios con TABLE_TYPE "F" y que tengan Scenarios Compuestos previos */  
              /****/
              /* Inicio generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
              /****/
              /* Inicializamos las listas que van a contener las tablas del FROM y las clausulas WHERE*/
              --UTL_FILE.put_line (fich_salida_pkg,'/* ESCENARIO: ' || reg_scenario.SCENARIO || ' */ \');
              l_FROM.delete;
              l_WHERE.delete;
              l_FROM_solo_tablas.delete;
              /* Fin de la inicializacion */
              /* (20170720). Implemento el Work Around de hacer un INSERT OVERWRITE INTO */
              --UTL_FILE.put_line(fich_salida_pkg,'INSERT OVERWRITE DIRECTORY ''#DIRECTORIO#/' || reg_scenario.SCENARIO || '''');
              --UTL_FILE.put_line(fich_salida_pkg,'ROW FORMAT DELIMITED FIELDS TERMINATED BY ''|'' NULL DEFINED AS '''' STORED AS TEXTFILE');
              /* (20170720). FIN.*/
              --if (reg_scenario.HINT is not null) then
                /* (20160421) Angel Ruiz. Miro si se ha incluido un HINT */
                --UTL_FILE.put_line(fich_salida_pkg,'SELECT ' || reg_scenario.HINT || '     --ESCENARIO: ' || reg_scenario.SCENARIO);
              --elsif (reg_scenario.DISTINCT_COL is not null) then
                --UTL_FILE.put_line(fich_salida_pkg,'SELECT DISTINCT ' || '     --ESCENARIO: ' || reg_scenario.SCENARIO);
              --else
                --UTL_FILE.put_line(fich_salida_pkg,'SELECT ' || '     --ESCENARIO: ' || reg_scenario.SCENARIO);
              --end if;
              /* (20160614) Angel Ruiz. NF: Tambien pueden aparecer las tablas tipo _[YYYYMM] en el campo TABLE_BASE_NAME */
              if (instr(reg_scenario.TABLE_BASE_NAME, '[YYYYMM]') > 0) then
                  /* Hay una tabla dinamica. Ponemos el switch a true */
                  /* Para posteriormente cuando generamos el Shell script, hacerlo */
                  /* de manera adecuada */
                  v_tabla_dinamica := true;
              end if;
              open MTDT_TC_DETAIL (reg_scenario.TABLE_NAME, reg_scenario.SCENARIO);
              primera_col := 1;
              loop
                fetch MTDT_TC_DETAIL
                into reg_detail;
                exit when MTDT_TC_DETAIL%NOTFOUND;
                if (reg_detail.VALIDA_TABLE_LKUP is not null) then
                  /* Tenemos query para llevar a cabo la validacion */
                  /* (20160414) Angel Ruiz. Miramos si hay alguna tabla dinamica que acabe con */
                  /* [YYYYMM] para generar el procedure de manera adecuada */
                  if (instr(reg_detail.TABLE_LKUP, '[YYYYMM]') > 0) then
                    /* Hay una tabla dinamica. Ponemos el switch a true */
                    /* Para posteriormente cuando generamos el Shell script, hacerlo */
                    /* de manera adecuada */
                    v_tabla_dinamica := true;
                  end if;
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
                  nombre_fich_carga := 'val_SA_HIVE_LKP_' || reg_detail.TABLE_NAME || '_' || reg_detail.SCENARIO || '_' || v_alias_table_lkup || '.sh';
                  nombre_fich_pkg := 'val_SA_HIVE_LKP_' || reg_detail.TABLE_NAME || '_' || reg_detail.SCENARIO || '_' || v_alias_table_lkup || '.sql';
                  fich_salida_load := UTL_FILE.FOPEN ('SALIDA',nombre_fich_carga,'W');
                  fich_salida_pkg := UTL_FILE.FOPEN ('SALIDA',nombre_fich_pkg,'W');
                  UTL_FILE.put_line (fich_salida_pkg, '-- ### ESCENARIO: ' || reg_detail.SCENARIO || ' ###');
                  UTL_FILE.put_line(fich_salida_pkg, '');
                  UTL_FILE.put_line(fich_salida_pkg, '');
                  v_query_validadora := procesa_campo_filter (reg_detail.VALIDA_TABLE_LKUP);
                  if (regexp_instr(v_query_validadora, '^"') > 0) then
                    /* Vienen comillas doble al principio */
                    v_query_validadora := regexp_replace(v_query_validadora, '^ *" *', '');
                  end if;
                  if (regexp_instr(v_query_validadora, ' *" *$') > 0) then
                    /* Vienen comillas doble al final */
                    v_query_validadora := regexp_replace(v_query_validadora, ' *" *$', '');
                  end if;
                  UTL_FILE.put_line(fich_salida_pkg, v_query_validadora);
                  UTL_FILE.put_line(fich_salida_pkg, '');
                  UTL_FILE.put_line(fich_salida_pkg, '');
                  --UTL_FILE.put_line(fich_salida_pkg, '!quit');
                  UTL_FILE.put_line(fich_salida_pkg, '-- ### FIN');

                  /******/
                  /* FIN DE LA GENERACION DEL PACKAGE */
                  /******/
                  /******/    
                  
              
                  /******/
                  /* INICIO DE LA GENERACION DEL sh de CARGA */
                  /******/
                  UTL_FILE.put_line(fich_salida_load, '#!/bin/bash');
                  UTL_FILE.put_line(fich_salida_load, '#############################################################################');
                  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
                  UTL_FILE.put_line(fich_salida_load, '# Telefonica Moviles Mexico SA DE CV                                        #');
                  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
                  UTL_FILE.put_line(fich_salida_load, '# Archivo    : ' || nombre_fich_carga || '  #');
                  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
                  UTL_FILE.put_line(fich_salida_load, '# Autor      : <SYNAPSYS>.                                                  #');
                  UTL_FILE.put_line(fich_salida_load, '# Proposito  : Shell que ejecuta procesos de Validacion                     #');
                  UTL_FILE.put_line(fich_salida_load, '# Parametros :                                                              #');
                  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
                  UTL_FILE.put_line(fich_salida_load, '# Ejecucion  :                                                              #');
                  UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
                  UTL_FILE.put_line(fich_salida_load, '# Historia : 24-Marzo-2017 -> Creacion                                      #');
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
                  UTL_FILE.put_line(fich_salida_load, '################################################################################');
                  UTL_FILE.put_line(fich_salida_load, '#Obtiene los password de base de datos                                         #');
                  UTL_FILE.put_line(fich_salida_load, '################################################################################');
                  UTL_FILE.put_line(fich_salida_load, 'InsertaFinFallido()');
                  UTL_FILE.put_line(fich_salida_load, '{');
                  --UTL_FILE.put_line(fich_salida_load, 'beeline -u ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} -n ${BD_USER_HIVE} -p ${BD_CLAVE_HIVE} -e "\');
                  UTL_FILE.put_line(fich_salida_load, 'beeline << EOF');
                  UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
                  UTL_FILE.put_line(fich_salida_load, 'INSERT INTO ${ESQUEMA_MT}.MTDT_MONITOREO');
                  UTL_FILE.put_line(fich_salida_load, 'SELECT');
                  --UTL_FILE.put_line(fich_salida_load, '  mtdt_proceso.cve_proceso*' || v_MULTIPLICADOR_PROC || '+${ULT_PASO_EJECUTADO}+unix_timestamp(),');  /* mtdt_proceso.cve_proceso*v_MULTIPLICADOR_PROC+mtdt_paso.cve_paso+unix_timestamp() */
                  UTL_FILE.put_line(fich_salida_load, '  mtdt_proceso.cve_proceso,');
                  UTL_FILE.put_line(fich_salida_load, '  1,');
                  UTL_FILE.put_line(fich_salida_load, '  1,');
                  UTL_FILE.put_line(fich_salida_load, '  ''${INICIO_PASO_TMR}'',');
                  UTL_FILE.put_line(fich_salida_load, '  current_timestamp(),');
                  UTL_FILE.put_line(fich_salida_load, '  ''${FCH_DATOS_FMT_HIVE}'',');
                  UTL_FILE.put_line(fich_salida_load, '  ''${FCH_CARGA_FMT_HIVE}'',');
                  UTL_FILE.put_line(fich_salida_load, '  ${TOT_INSERTADOS},');  /* numero de inserts */
                  UTL_FILE.put_line(fich_salida_load, '  ${TOT_MODIFICADOS},'); /* numero de updates */
                  UTL_FILE.put_line(fich_salida_load, '  0,'); /* numero de deletes */
                  UTL_FILE.put_line(fich_salida_load, '  ${TOT_HISTO},'); /* numero de reads */
                  UTL_FILE.put_line(fich_salida_load, '  0,'); /* numero de discards */
                  UTL_FILE.put_line(fich_salida_load, '  ''${BAN_FORZADO}'','); /* BAN_FORZADO */
                  UTL_FILE.put_line(fich_salida_load, '  ''${INICIO_PASO_TMR}''');
                  UTL_FILE.put_line(fich_salida_load, 'FROM');
                  UTL_FILE.put_line(fich_salida_load, '${ESQUEMA_MT}.MTDT_PROCESO');
                  UTL_FILE.put_line(fich_salida_load, 'WHERE');
                  /* (20160817) Angel Ruiz. Cambio temporal para adecuarse a la entrega de produccion*/
                  UTL_FILE.put_line(fich_salida_load, 'MTDT_PROCESO.NOMBRE_PROCESO = ''' || nombre_fich_carga || ''';');
                  UTL_FILE.put_line(fich_salida_load, '!quit');
                  UTL_FILE.put_line(fich_salida_load, 'EOF');
                  UTL_FILE.put_line(fich_salida_load, 'if [ $? -ne 0 ]');
                  UTL_FILE.put_line(fich_salida_load, 'then');
                  UTL_FILE.put_line(fich_salida_load, '  SUBJECT="${INTERFAZ}: ERROR: Al insertar en el metadato Fin Fallido."');
                  UTL_FILE.put_line(fich_salida_load, '  echo "Surgio un error al insertar en el metadato que le proceso no ha terminado OK." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
                  UTL_FILE.put_line(fich_salida_load, '  echo `date`');
                  UTL_FILE.put_line(fich_salida_load, '  exit 1');
                  UTL_FILE.put_line(fich_salida_load, 'fi');
                  UTL_FILE.put_line(fich_salida_load, 'return 0');
                  UTL_FILE.put_line(fich_salida_load, '}');
                  UTL_FILE.put_line(fich_salida_load, '');
                  UTL_FILE.put_line(fich_salida_load, 'InsertaFinOK()');
                  UTL_FILE.put_line(fich_salida_load, '{');
                  --UTL_FILE.put_line(fich_salida_load, 'beeline -u ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} -n ${BD_USER_HIVE} -p ${BD_CLAVE_HIVE} -e "\');
                  UTL_FILE.put_line(fich_salida_load, 'beeline << EOF');
                  UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
                  UTL_FILE.put_line(fich_salida_load, 'INSERT INTO ${ESQUEMA_MT}.MTDT_MONITOREO');
                  UTL_FILE.put_line(fich_salida_load, 'SELECT');
                  --UTL_FILE.put_line(fich_salida_load, '  mtdt_proceso.cve_proceso*' || v_MULTIPLICADOR_PROC || '+${ULT_PASO_EJECUTADO}+unix_timestamp(),');  /* mtdt_proceso.cve_proceso*v_MULTIPLICADOR_PROC+mtdt_paso.cve_paso+unix_timestamp() */ 
                  UTL_FILE.put_line(fich_salida_load, '  mtdt_proceso.cve_proceso,');
                  UTL_FILE.put_line(fich_salida_load, '  1,');
                  UTL_FILE.put_line(fich_salida_load, '  0,');
                  UTL_FILE.put_line(fich_salida_load, '  ''${INICIO_PASO_TMR}'',');
                  UTL_FILE.put_line(fich_salida_load, '  current_timestamp(),');
                  UTL_FILE.put_line(fich_salida_load, '  ''${FCH_DATOS_FMT_HIVE}'',');
                  UTL_FILE.put_line(fich_salida_load, '  ''${FCH_CARGA_FMT_HIVE}'',');
                  UTL_FILE.put_line(fich_salida_load, '  ${TOT_INSERTADOS},');  /* numero de inserts */
                  UTL_FILE.put_line(fich_salida_load, '  ${TOT_MODIFICADOS},'); /* numero de updates */
                  UTL_FILE.put_line(fich_salida_load, '  0,'); /* numero de deletes */
                  UTL_FILE.put_line(fich_salida_load, '  ${TOT_HISTO},'); /* numero de reads */
                  UTL_FILE.put_line(fich_salida_load, '  0,'); /* numero de discards */
                  UTL_FILE.put_line(fich_salida_load, '  ''${BAN_FORZADO}'','); /* BAN_FORZADO */
                  UTL_FILE.put_line(fich_salida_load, '  ''${INICIO_PASO_TMR}''');
                  UTL_FILE.put_line(fich_salida_load, 'FROM');
                  UTL_FILE.put_line(fich_salida_load, '${ESQUEMA_MT}.MTDT_PROCESO');
                  UTL_FILE.put_line(fich_salida_load, 'WHERE');
                  /* (20160817) Angel Ruiz. Cambio temporal para adecuarse a la entrega de produccion*/    
                  UTL_FILE.put_line(fich_salida_load, 'MTDT_PROCESO.NOMBRE_PROCESO = ''' || nombre_fich_carga || ''';');
                  UTL_FILE.put_line(fich_salida_load, '!quit');
                  UTL_FILE.put_line(fich_salida_load, 'EOF');
                  --UTL_FILE.put_line(fich_salida_load, '  MTDT_PROCESO.NOMBRE_PROCESO = ''' || 'ONIX' || '_' || reg_tabla.TABLE_NAME || '.sh'';');
                  /* (20160817) Angel Ruiz FIN Cambio temporal para adecuarse a la entrega de produccion*/
                  UTL_FILE.put_line(fich_salida_load, '  if [ $? -ne 0 ]');
                  UTL_FILE.put_line(fich_salida_load, '  then');
                  UTL_FILE.put_line(fich_salida_load, '    SUBJECT="${INTERFAZ}: ERROR: Al insertar en el metadato Fin OK."');
                  UTL_FILE.put_line(fich_salida_load, '    echo "Surgio un error al insertar en el metadato que le proceso ha terminado OK." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
                  UTL_FILE.put_line(fich_salida_load, '    echo `date`');
                  UTL_FILE.put_line(fich_salida_load, '    exit 1');
                  UTL_FILE.put_line(fich_salida_load, '  fi');
                  UTL_FILE.put_line(fich_salida_load, '  return 0');
                  UTL_FILE.put_line(fich_salida_load, '}');
                  UTL_FILE.put_line(fich_salida_load, '');

                  UTL_FILE.put_line(fich_salida_load, '################################################################################');
                  UTL_FILE.put_line(fich_salida_load, '# SE OBTIENE LA FECHA                                                          #');
                  UTL_FILE.put_line(fich_salida_load, '################################################################################');
                  UTL_FILE.put_line(fich_salida_load, 'ObtieneFecha()');
                  UTL_FILE.put_line(fich_salida_load, '{');
                  UTL_FILE.put_line(fich_salida_load, '  if [ $# = 0 ] ; then');
                  /* (20170808) Angel Ruiz. Compruebo la frecuencia con que va a ser vargado */
                  /* El interfaz para generar el rango de fechas en funcion de la frecuencia */
                  /* Si es D (diaria) el intervalo sera de un dia */
                  /* Si es M (mensual) el intervalo sera de un mes */
                  v_frequency := 'D'; /* Por defecto la frecuencia es diaria */
                  for v_cursor_frecuencia in (
                    select frequency into v_frequency from mtdt_interface_summary
                    where trim(concept_name) = reg_tabla.TABLE_NAME)
                  loop
                    v_frequency := v_cursor_frecuencia.frequency;
                  end loop;
                  if (v_frequency <> 'M') then
                    /* Si se trata de valores de fecuenciua D (diaria) o E (eventual) */
                  
                    UTL_FILE.put_line(fich_salida_load, '    # Se obtiene la fecha inicial y final del periodo a calcular a partir de la fecha del sistema.');
                    --UTL_FILE.put_line(fich_salida_load, '    FECHA=`beeline -u ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} -n ${BD_USER_HIVE} -p ${BD_CLAVE_HIVE} --silent=true --showHeader=false --outputformat=dsv -e "select date_format(current_date, ''yyyyMMdd'') from ${ESQUEMA_MT}.dual;"`');
                    UTL_FILE.put_line(fich_salida_load, '    FECHA_PREV=`beeline --silent=true --showHeader=false --outputformat=dsv << EOF');
                    UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
                    UTL_FILE.put_line(fich_salida_load, 'select date_format(current_date, ''yyyy-MM-dd'') from ${ESQUEMA_MT}.dual;');
                    UTL_FILE.put_line(fich_salida_load, '!quit');
                    UTL_FILE.put_line(fich_salida_load, 'EOF`');
                    UTL_FILE.put_line(fich_salida_load, '    if [ $? -ne 0 ]; then');
                    UTL_FILE.put_line(fich_salida_load, '      SUBJECT="${REQ_NUM}: ERROR: Al obtener la fecha."');
                    UTL_FILE.put_line(fich_salida_load, '      echo "Surgio un error al obtener la fecha del sistema o el parametro no es un formato de fecha YYYYMMDD." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
                    UTL_FILE.put_line(fich_salida_load, '      echo `date`');
                    UTL_FILE.put_line(fich_salida_load, '      InsertaFinFallido');
                    UTL_FILE.put_line(fich_salida_load, '      exit 1');
                    UTL_FILE.put_line(fich_salida_load, '    fi');
                    UTL_FILE.put_line(fich_salida_load, '    FECHA=`echo ${FECHA_PREV} | sed -e ''s/\n//g'' -e ''s/\r//g'' -e ''s/^[ ]*//g'' -e ''s/[ ]*$//g''`');
                    --UTL_FILE.put_line(fich_salida_load, '    FECHA_FMT_HIVE=`echo ${FECHA} | awk ''{ printf "%s-%s-%s", substr($1,0,4), substr($1,5,2), substr($1,7,2) ; }''`');    
                    UTL_FILE.put_line(fich_salida_load, '    FECHA_FMT_HIVE=${FECHA}');
                    /* (20170619) Angel Ruiz. BUG. Me creo una variable con formato YYYYMMDD para usarla posteriormente*/
                    UTL_FILE.put_line(fich_salida_load, '    FECHA_SIN_FMT=`echo ${FECHA_FMT_HIVE} | awk ''{ printf "%s%s%s", substr($1,0,4), substr($1,6,2), substr($1,9,2) ; }''`');
                    if (v_fecha_ini_param = true and v_fecha_fin_param = true) then
                      /* (20160419) Angel Ruiz. Si tenemos parametros de FCH_INI y FCH_FIN tenmos que generar codigo para recuperar la fecha Inicial */
                      --UTL_FILE.put_line(fich_salida_load, '    FECHA_FIN=${FECHA}');
                      UTL_FILE.put_line(fich_salida_load, '    FECHA_FIN_PREV=`beeline --silent=true --showHeader=false --outputformat=dsv << EOF');
                      UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
                      UTL_FILE.put_line(fich_salida_load, 'select date_format(current_date, ''yyyy-MM-dd'') from ${ESQUEMA_MT}.dual;');
                      UTL_FILE.put_line(fich_salida_load, '!quit');
                      UTL_FILE.put_line(fich_salida_load, 'EOF`');
                      UTL_FILE.put_line(fich_salida_load, '    if [ $? -ne 0 ]; then');
                      UTL_FILE.put_line(fich_salida_load, '      SUBJECT="${REQ_NUM}: ERROR: Al obtener la fecha."');
                      UTL_FILE.put_line(fich_salida_load, '      echo "Surgio un error al obtener la fecha del sistema o el parametro no es un formato de fecha YYYYMMDD." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
                      UTL_FILE.put_line(fich_salida_load, '      echo `date`');
                      UTL_FILE.put_line(fich_salida_load, '      InsertaFinFallido');
                      UTL_FILE.put_line(fich_salida_load, '      exit 1');
                      UTL_FILE.put_line(fich_salida_load, '    fi');
                      UTL_FILE.put_line(fich_salida_load, '    FECHA_FIN=`echo ${FECHA_FIN_PREV} | sed -e ''s/\n//g'' -e ''s/\r//g'' -e ''s/^[ ]*//g'' -e ''s/[ ]*$//g''`');
                      
                      --UTL_FILE.put_line(fich_salida_load, '    FECHA_FIN=`sqlplus -s ${BD_USR}/${BD_PWD}@${BD_SID} <<!eof');
                      --UTL_FILE.put_line(fich_salida_load, '      whenever sqlerror exit 1');
                      --UTL_FILE.put_line(fich_salida_load, '      set pagesize 0');
                      --UTL_FILE.put_line(fich_salida_load, '      set heading off');
                      --UTL_FILE.put_line(fich_salida_load, '      select to_char(SYSDATE,''YYYYMMDD'')');
                      --UTL_FILE.put_line(fich_salida_load, '      from dual;');
                      --UTL_FILE.put_line(fich_salida_load, '    quit');
                      --UTL_FILE.put_line(fich_salida_load, '    !eof`');
                      --UTL_FILE.put_line(fich_salida_load, '    if [ $? -ne 0 ]; then');
                      --UTL_FILE.put_line(fich_salida_load, '      SUBJECT="${REQ_NUM}: ERROR: Al obtener la fecha."');
                      --UTL_FILE.put_line(fich_salida_load, '      echo "Surgio un error al obtener la fecha fin o el parametro no es un formato de fecha YYYYMMDD." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
                      --UTL_FILE.put_line(fich_salida_load, '      echo `date`');
                      --UTL_FILE.put_line(fich_salida_load, '      InsertaFinFallido');
                      --UTL_FILE.put_line(fich_salida_load, '      exit 1');
                      --UTL_FILE.put_line(fich_salida_load, '    fi');
                    end if;
                  else  /* if (v_frecuency = "M") then */
                    UTL_FILE.put_line(fich_salida_load, '    FECHA_PREV=`beeline --silent=true --showHeader=false --outputformat=dsv << EOF');
                    UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
                    UTL_FILE.put_line(fich_salida_load, 'select date_format(current_date, ''yyyy-MM-01'') from ${ESQUEMA_MT}.dual;');
                    UTL_FILE.put_line(fich_salida_load, '!quit');
                    UTL_FILE.put_line(fich_salida_load, 'EOF`');
                    UTL_FILE.put_line(fich_salida_load, '    if [ $? -ne 0 ]; then');
                    UTL_FILE.put_line(fich_salida_load, '      SUBJECT="${REQ_NUM}: ERROR: Al obtener la fecha."');
                    UTL_FILE.put_line(fich_salida_load, '      echo "Surgio un error al obtener la fecha del sistema o el parametro no es un formato de fecha YYYYMMDD." >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
                    UTL_FILE.put_line(fich_salida_load, '      echo `date` >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
                    UTL_FILE.put_line(fich_salida_load, '      InsertaFinFallido');
                    UTL_FILE.put_line(fich_salida_load, '      exit 1');
                    UTL_FILE.put_line(fich_salida_load, '    fi');
                    UTL_FILE.put_line(fich_salida_load, '    FECHA=`echo ${FECHA_PREV} | sed -e ''s/\n//g'' -e ''s/\r//g'' -e ''s/^[ ]*//g'' -e ''s/[ ]*$//g''`');
                    UTL_FILE.put_line(fich_salida_load, '    FECHA_FMT_HIVE=${FECHA}');
                    UTL_FILE.put_line(fich_salida_load, '    FECHA_SIN_FMT=`echo ${FECHA_FMT_HIVE} | awk ''{ printf "%s%s%s", substr($1,0,4), substr($1,6,2), substr($1,9,2) ; }''`');
                    if (v_fecha_ini_param = true and v_fecha_fin_param = true) then
                      /* (20160419) Angel Ruiz. Si tenemos parametros de FCH_INI y FCH_FIN tenmos que generar codigo para recuperar la fecha Inicial */
                      --UTL_FILE.put_line(fich_salida_load, '    FECHA_FIN=${FECHA}');
                      UTL_FILE.put_line(fich_salida_load, '    FECHA_FIN_PREV=`beeline --silent=true --showHeader=false --outputformat=dsv << EOF');
                      UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
                      UTL_FILE.put_line(fich_salida_load, 'select date_format(LAST_DAY(current_date),''yyyy-MM-dd'') from ${ESQUEMA_MT}.dual;');
                      UTL_FILE.put_line(fich_salida_load, '!quit');
                      UTL_FILE.put_line(fich_salida_load, 'EOF`');
                      UTL_FILE.put_line(fich_salida_load, '    if [ $? -ne 0 ]; then');
                      UTL_FILE.put_line(fich_salida_load, '      SUBJECT="${REQ_NUM}: ERROR: Al obtener la fecha fin."');
                      UTL_FILE.put_line(fich_salida_load, '      echo "Surgio un error al obtener la fecha del sistema o el parametro no es un formato de fecha YYYYMMDD." >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
                      UTL_FILE.put_line(fich_salida_load, '      echo `date` >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
                      UTL_FILE.put_line(fich_salida_load, '      InsertaFinFallido');
                      UTL_FILE.put_line(fich_salida_load, '      exit 1');
                      UTL_FILE.put_line(fich_salida_load, '    fi');
                      UTL_FILE.put_line(fich_salida_load, '    FECHA_FIN=`echo ${FECHA_FIN_PREV} | sed -e ''s/\n//g'' -e ''s/\r//g'' -e ''s/^[ ]*//g'' -e ''s/[ ]*$//g''`');
                    end if;
                  end if;  /* if (v_frecuency <> "M") then */
                  UTL_FILE.put_line(fich_salida_load, '  else');
                  if (v_frequency <> 'M') then
                    UTL_FILE.put_line(fich_salida_load, '    # Se obtiene la fecha inicial y final del periodo a calcular a partir de la fecha proporcionada como parametro.');
                    UTL_FILE.put_line(fich_salida_load, '    FECHA_FMT_HIVE=`echo $1 | awk ''{ printf "%s-%s-%s", substr($1,0,4), substr($1,5,2), substr($1,7,2) ; }''`');
                    --UTL_FILE.put_line(fich_salida_load, '    FECHA=`beeline -u ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} -n ${BD_USER_HIVE} -p ${BD_CLAVE_HIVE} --silent=true --showHeader=false --outputformat=dsv -e "select date_format(''${FECHA_FMT_HIVE}'', ''yyyyMMdd'') from ${ESQUEMA_MT}.dual;"`');
                    UTL_FILE.put_line(fich_salida_load, '    FECHA_PREV=`beeline --silent=true --showHeader=false --outputformat=dsv << EOF');
                    UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
                    UTL_FILE.put_line(fich_salida_load, 'select date_format(cast(''${FECHA_FMT_HIVE}'' as date), ''yyyy-MM-dd'') from ${ESQUEMA_MT}.dual;');
                    UTL_FILE.put_line(fich_salida_load, '!quit');
                    UTL_FILE.put_line(fich_salida_load, 'EOF`');
                    UTL_FILE.put_line(fich_salida_load, '    if [ $? -ne 0 ]; then');
                    UTL_FILE.put_line(fich_salida_load, '      SUBJECT="${REQ_NUM}: ERROR: Al obtener la fecha."');
                    UTL_FILE.put_line(fich_salida_load, '      echo "Surgio un error al obtener la fecha del sistema o el parametro no es un formato de fecha YYYYMMDD." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
                    UTL_FILE.put_line(fich_salida_load, '      echo `date`');
                    UTL_FILE.put_line(fich_salida_load, '      InsertaFinFallido');
                    UTL_FILE.put_line(fich_salida_load, '      exit 1');
                    UTL_FILE.put_line(fich_salida_load, '    fi');
                    /* (20170619) Angel Ruiz. BUG. Corrijo el formato de FECHA para que siempre tenga formato YYYYMMDD*/
                    UTL_FILE.put_line(fich_salida_load, '    FECHA=`echo ${FECHA_PREV} | sed -e ''s/\n//g'' -e ''s/\r//g'' -e ''s/^[ ]*//g'' -e ''s/[ ]*$//g''`');
                    /* (20170619) Angel Ruiz. BUG. Me creo una variable para usarla despues con formato YYYYMMDD*/
                    UTL_FILE.put_line(fich_salida_load, '    FECHA_SIN_FMT=${1}');
                    
                    
                    if (v_fecha_ini_param = true and v_fecha_fin_param = true) then
                      /* (20160419) Angel Ruiz. Si tenemos parametros de FCH_INI y FCH_FIN tenmos que generar codigo para recuperar la fecha Inicial */
                      UTL_FILE.put_line(fich_salida_load, '    FECHA_FIN=${FECHA}');
                      --UTL_FILE.put_line(fich_salida_load, '    FECHA_FIN=`sqlplus -s ${BD_USR}/${BD_PWD}@${BD_SID} <<!eof');
                      --UTL_FILE.put_line(fich_salida_load, '      whenever sqlerror exit 1');
                      --UTL_FILE.put_line(fich_salida_load, '      set pagesize 0');
                      --UTL_FILE.put_line(fich_salida_load, '      set heading off');
                      --UTL_FILE.put_line(fich_salida_load, '      select to_char(to_date( ''$1'',''YYYYMMDD''), ''YYYYMMDD'')');
                      --UTL_FILE.put_line(fich_salida_load, '      from dual;');
                      --UTL_FILE.put_line(fich_salida_load, '    quit');
                      --UTL_FILE.put_line(fich_salida_load, '    !eof`');
                      --UTL_FILE.put_line(fich_salida_load, '    if [ $? -ne 0 ]; then');
                      --UTL_FILE.put_line(fich_salida_load, '      SUBJECT="${REQ_NUM}: ERROR: Al obtener la fecha."');
                      --UTL_FILE.put_line(fich_salida_load, '      echo "Surgio un error al obtener la fecha final o el parametro no es un formato de fecha YYYYMMDD." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
                      --UTL_FILE.put_line(fich_salida_load, '      echo `date`');
                      --UTL_FILE.put_line(fich_salida_load, '      InsertaFinFallido');
                      --UTL_FILE.put_line(fich_salida_load, '      exit 1');
                      --UTL_FILE.put_line(fich_salida_load, '    fi');
                    end if;
                  else /* v_frequency = 'M'*/
                    /* Se trata de una frecuencia MENSUAL */
                    UTL_FILE.put_line(fich_salida_load, '    # Se obtiene la fecha inicial y final del periodo a calcular a partir de la fecha proporcionada como parametro.');
                    UTL_FILE.put_line(fich_salida_load, '    FECHA_FMT_HIVE=`echo $1 | awk ''{ printf "%s-%s-%s", substr($1,0,4), substr($1,5,2), substr($1,7,2) ; }''`');
                    --UTL_FILE.put_line(fich_salida_load, '    FECHA=`beeline -u ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} -n ${BD_USER_HIVE} -p ${BD_CLAVE_HIVE} --silent=true --showHeader=false --outputformat=dsv -e "select date_format(''${FECHA_FMT_HIVE}'', ''yyyyMMdd'') from ${ESQUEMA_MT}.dual;"`');
                    UTL_FILE.put_line(fich_salida_load, '    FECHA_PREV=`beeline --silent=true --showHeader=false --outputformat=dsv << EOF');
                    UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
                    UTL_FILE.put_line(fich_salida_load, 'select date_format(cast(''${FECHA_FMT_HIVE}'' as date), ''yyyy-MM-01'') from ${ESQUEMA_MT}.dual;');
                    UTL_FILE.put_line(fich_salida_load, '!quit');
                    UTL_FILE.put_line(fich_salida_load, 'EOF`');
                    UTL_FILE.put_line(fich_salida_load, '    if [ $? -ne 0 ]; then');
                    UTL_FILE.put_line(fich_salida_load, '      SUBJECT="${REQ_NUM}: ERROR: Al obtener la fecha."');
                    UTL_FILE.put_line(fich_salida_load, '      echo "Surgio un error al obtener la fecha del sistema o el parametro no es un formato de fecha YYYYMMDD." >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
                    UTL_FILE.put_line(fich_salida_load, '      echo `date` >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
                    UTL_FILE.put_line(fich_salida_load, '      InsertaFinFallido');
                    UTL_FILE.put_line(fich_salida_load, '      exit 1');
                    UTL_FILE.put_line(fich_salida_load, '    fi');
                    UTL_FILE.put_line(fich_salida_load, '    FECHA=`echo ${FECHA_PREV} | sed -e ''s/\n//g'' -e ''s/\r//g'' -e ''s/^[ ]*//g'' -e ''s/[ ]*$//g''`');
                    UTL_FILE.put_line(fich_salida_load, '    FECHA_SIN_FMT=${1}');
                    if (v_fecha_ini_param = true and v_fecha_fin_param = true) then
                      /* (20160419) Angel Ruiz. Si tenemos parametros de FCH_INI y FCH_FIN tenmos que generar codigo para recuperar la fecha Inicial */
                      --UTL_FILE.put_line(fich_salida_load, '    FECHA_FIN=${FECHA}');
                      UTL_FILE.put_line(fich_salida_load, '    FECHA_FIN_PREV=`beeline --silent=true --showHeader=false --outputformat=dsv << EOF');
                      UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
                      UTL_FILE.put_line(fich_salida_load, 'select date_format(LAST_DAY(cast(''${FECHA_FMT_HIVE}'' as date)), ''yyyy-MM-dd'') from ${ESQUEMA_MT}.dual;');
                      UTL_FILE.put_line(fich_salida_load, '!quit');
                      UTL_FILE.put_line(fich_salida_load, 'EOF`');
                      UTL_FILE.put_line(fich_salida_load, '    if [ $? -ne 0 ]; then');
                      UTL_FILE.put_line(fich_salida_load, '      SUBJECT="${REQ_NUM}: ERROR: Al obtener la fecha fin."');
                      UTL_FILE.put_line(fich_salida_load, '      echo "Surgio un error al obtener la fecha del sistema o el parametro no es un formato de fecha YYYYMMDD." >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
                      UTL_FILE.put_line(fich_salida_load, '      echo `date` >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
                      UTL_FILE.put_line(fich_salida_load, '      InsertaFinFallido');
                      UTL_FILE.put_line(fich_salida_load, '      exit 1');
                      UTL_FILE.put_line(fich_salida_load, '    fi');
                      UTL_FILE.put_line(fich_salida_load, '    FECHA_FIN=`echo ${FECHA_FIN_PREV} | sed -e ''s/\n//g'' -e ''s/\r//g'' -e ''s/^[ ]*//g'' -e ''s/[ ]*$//g''`');
                    end if;
                  end if; /* if (v_frecuency <> "M") then */
                  UTL_FILE.put_line(fich_salida_load, '  fi');
                  UTL_FILE.put_line(fich_salida_load, '  echo "Fecha a considerar ${FECHA}"');
                  if (v_tabla_dinamica = true or pos_ini_mes > 0) then
                    /* El interfaz tiene una tabla dinamica o bien en el nombre del fichero tiene un sufijo que es la fecha del mes por lo que hay que obtener la fecha YYYYMM */
                    UTL_FILE.put_line (fich_salida_load,'  FECHA_MES=`echo ${FECHA} | awk ''{ printf "%s%s", substr($1,0,4), substr($1,6,2) ; }''`');
                  end if;
                  UTL_FILE.put_line(fich_salida_load, '  return 0');
                  UTL_FILE.put_line(fich_salida_load, '}');



                  
          
                  UTL_FILE.put_line(fich_salida_load, '################################################################################');
                  UTL_FILE.put_line(fich_salida_load, '# EJECUCION DEL PROGRAMA EN PRO C O QUERYS                                     #');
                  UTL_FILE.put_line(fich_salida_load, '################################################################################');
                  UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_ENTORNO}/entorno' || NAME_DM || '_MEX.sh');
                  UTL_FILE.put_line(fich_salida_load, '# Comprobamos si el numero de parametros es el correcto');
                  UTL_FILE.put_line(fich_salida_load, 'if [ $# -ne 3 ] ; then');
                  UTL_FILE.put_line(fich_salida_load, '  SUBJECT="Numero de paramatros de entrada incorrecto. Uso: ${0} <fch_carga> <fch_datos> <forzado>"');
                  UTL_FILE.put_line(fich_salida_load, '  echo ${SUBJECT}');        
                  UTL_FILE.put_line(fich_salida_load, '  exit 1');
                  UTL_FILE.put_line(fich_salida_load, 'fi');
                  UTL_FILE.put_line(fich_salida_load, '# Recogida de parametros');
                  UTL_FILE.put_line(fich_salida_load, 'FCH_CARGA=${1}');
                  UTL_FILE.put_line(fich_salida_load, 'FCH_DATOS=${2}');
                  UTL_FILE.put_line(fich_salida_load, 'BAN_FORZADO=${3}');
                  UTL_FILE.put_line(fich_salida_load, 'FECHA_HORA=${FCH_CARGA}_${FCH_DATOS}_`date +%Y%m%d_%H%M%S`');
                  UTL_FILE.put_line(fich_salida_load, '# Trasformacion de las fechas de entrada a formato HIVE');
                  UTL_FILE.put_line(fich_salida_load, 'FCH_CARGA_FMT_HIVE=`echo ${FCH_CARGA} | awk ''{ printf "%s-%s-%s", substr($1,0,4), substr($1,5,2), substr($1,7,2) ; }''`');
                  UTL_FILE.put_line(fich_salida_load, 'FCH_DATOS_FMT_HIVE=`echo ${FCH_DATOS} | awk ''{ printf "%s-%s-%s", substr($1,0,4), substr($1,5,2), substr($1,7,2) ; }''`');
                  UTL_FILE.put_line(fich_salida_load, 'FCH_CARGA_MES=`echo ${FCH_CARGA} | awk ''{ printf "%s%s", substr($1,0,4), substr($1,5,2) ; }''`');
                  UTL_FILE.put_line(fich_salida_load, 'TOT_HISTO=0');
                  UTL_FILE.put_line(fich_salida_load, 'TOT_INSERTADOS=0');
                  UTL_FILE.put_line(fich_salida_load, 'TOT_RECHAZADOS=0');
                  UTL_FILE.put_line(fich_salida_load, 'TOT_MODIFICADOS=0');
                  UTL_FILE.put_line(fich_salida_load, 'ULT_PASO_EJECUTADO=1');
                  
                  --UTL_FILE.put_line(fich_salida_load, 'echo "load_he_' || reg_tabla.TABLE_NAME || '" > ${' || NAME_DM || '_TRAZAS}/load_he_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}' || '.log ');
                  UTL_FILE.put_line(fich_salida_load, '# Comprobamos si existe el directorio de Trazas para fecha de carga');
                  UTL_FILE.put_line(fich_salida_load, 'if ! [ -d ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA} ] ; then');
                  UTL_FILE.put_line(fich_salida_load, '  mkdir ${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
                  UTL_FILE.put_line(fich_salida_load, 'fi');
                  UTL_FILE.put_line(fich_salida_load, '' || NAME_DM || '_TRAZAS=${' || NAME_DM || '_TRAZAS}/${FCH_CARGA}');
                  UTL_FILE.put_line(fich_salida_load, 'echo "${0}" > ${' || NAME_DM || '_TRAZAS}/' || substr(nombre_fich_carga, 1, length(nombre_fich_carga) - 3) || '_${FECHA_HORA}' || '.log ');
                  UTL_FILE.put_line(fich_salida_load, 'echo "Inicia Proceso: `date +%d/%m/%Y\ %H:%M:%S`"  >> ${' || NAME_DM || '_TRAZAS}/' || substr(nombre_fich_carga, 1, length(nombre_fich_carga) - 3) || '_${FECHA_HORA}' || '.log ');
                  UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Carga: ${FCH_CARGA}"  >> ${' || NAME_DM || '_TRAZAS}/' || substr(nombre_fich_carga, 1, length(nombre_fich_carga) - 3) || '_${FECHA_HORA}' || '.log ');
                  UTL_FILE.put_line(fich_salida_load, 'echo "Fecha de Datos: ${FCH_DATOS}"  >> ${' || NAME_DM || '_TRAZAS}/' || substr(nombre_fich_carga, 1, length(nombre_fich_carga) - 3) || '_${FECHA_HORA}' || '.log ');
                  UTL_FILE.put_line(fich_salida_load, 'echo "Forzado: ${BAN_FORZADO}"  >> ${' || NAME_DM || '_TRAZAS}/' || substr(nombre_fich_carga, 1, length(nombre_fich_carga) - 3) || '_${FECHA_HORA}' || '.log ');
                  --UTL_FILE.put_line(fich_salida_sh, 'set -x');
                  UTL_FILE.put_line(fich_salida_load, '#Permite los acentos y U');
                  UTL_FILE.put_line(fich_salida_load, 'NLS_LANG=AMERICAN_AMERICA.WE8ISO8859P1');
                  UTL_FILE.put_line(fich_salida_load, 'export NLS_LANG');
                  UTL_FILE.put_line(fich_salida_load, '################################################################################');
                  UTL_FILE.put_line(fich_salida_load, '# VARIABLES ESPECIFICAS PARA EL PROCESO                                        #');
                  UTL_FILE.put_line(fich_salida_load, '################################################################################');
                  --UTL_FILE.put_line(fich_salida_load, 'REQ_NUM="' || v_REQ_NUMER || '"');
                  --UTL_FILE.put_line(fich_salida_load, 'INTERFAZ=' || v_REQ_NUMER || '_load_' || reg_tabla.TABLE_NAME);
                  UTL_FILE.put_line(fich_salida_load, '');
                  UTL_FILE.put_line(fich_salida_load, '################################################################################');
                  UTL_FILE.put_line(fich_salida_load, '# LIBRERIAS                                                                    #');
                  UTL_FILE.put_line(fich_salida_load, '################################################################################');
                  UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_UTILIDADES}/UtilBD.sh');
                  UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_UTILIDADES}/UtilArchivo.sh');
                  UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_UTILIDADES}/UtilUnix.sh');
                  UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_UTILIDADES}/Util' || NAME_DM || '.sh');
                  UTL_FILE.put_line(fich_salida_load, '');
                  UTL_FILE.put_line(fich_salida_load, '################################################################################');
                  UTL_FILE.put_line(fich_salida_load, '# Cuentas  Produccion / Desarrollo                                             #');
                  UTL_FILE.put_line(fich_salida_load, '################################################################################');
                  UTL_FILE.put_line(fich_salida_load, 'if [ "`/sbin/ifconfig -a | grep ''10.225.232.'' | awk ''{print $2}''`" = "10.225.232.153" ]; then');
                  UTL_FILE.put_line(fich_salida_load, '  ### Cuentas para produccion');
                  UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`');
                  UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_ReportesBI.txt`');
                  UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_DWH=`cat ${' || NAME_DM || '_CONFIGURACION}/TelefonosMantto.txt`');
                  UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/TELEFONOS_USUARIOS.txt`');
                  UTL_FILE.put_line(fich_salida_load, 'else');
                  UTL_FILE.put_line(fich_salida_load, '  ### Cuentas para mantenimiento');
                  UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_Usuario_ReportesBI.txt`');
                  UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_ReportesBI.txt`');
                  UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_DWH=`cat ${' || NAME_DM || '_CONFIGURACION}/TelefonosMantto.txt`');
                  UTL_FILE.put_line(fich_salida_load, '  TELEFONOS_USUARIOS=`cat ${' || NAME_DM || '_CONFIGURACION}/TELEFONOS_USUARIOS.txt`');
                  UTL_FILE.put_line(fich_salida_load, 'fi');
                  UTL_FILE.put_line(fich_salida_load, '');
                  UTL_FILE.put_line(fich_salida_load, 'ObtenContrasena ${BD_SID} ${BD_USER_HIVE}');
                  UTL_FILE.put_line(fich_salida_load, 'BD_CLAVE_HIVE=${PASSWORD}');
                  UTL_FILE.put_line(fich_salida_load, 'ObtieneFecha $1');
                  
                  /**************************/
                  /*(20161205) Angel Ruiz. ***************************/
                  --UTL_FILE.put_line(fich_salida_load, 'ULT_PASO_EJECUTADO=`beeline -u ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} -n ${BD_USER_HIVE} -p ${BD_CLAVE_HIVE} --silent=true --showHeader=false --outputformat=dsv -e "\');
                  UTL_FILE.put_line(fich_salida_load, 'ULT_PASO_EJECUTADO_PREV=`beeline --silent=true --showHeader=false --outputformat=dsv << EOF');
                  UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
                  UTL_FILE.put_line(fich_salida_load, 'SELECT nvl(MAX(MTDT_MONITOREO.CVE_PASO),0) \');
                  UTL_FILE.put_line(fich_salida_load, 'FROM \');
                  UTL_FILE.put_line(fich_salida_load, '${ESQUEMA_MT}.MTDT_MONITOREO \');
                  UTL_FILE.put_line(fich_salida_load, 'JOIN ${ESQUEMA_MT}.MTDT_PROCESO \');
                  UTL_FILE.put_line(fich_salida_load, 'ON (MTDT_PROCESO.CVE_PROCESO = MTDT_MONITOREO.CVE_PROCESO) \');
                  UTL_FILE.put_line(fich_salida_load, 'JOIN ${ESQUEMA_MT}.MTDT_PASO \');
                  UTL_FILE.put_line(fich_salida_load, 'ON (MTDT_PROCESO.CVE_PROCESO = MTDT_PASO.CVE_PROCESO \');
                  UTL_FILE.put_line(fich_salida_load, 'AND MTDT_PASO.CVE_PASO = MTDT_MONITOREO.CVE_PASO) \');
                  UTL_FILE.put_line(fich_salida_load, 'WHERE \');
                  UTL_FILE.put_line(fich_salida_load, 'MTDT_MONITOREO.FCH_CARGA = ''${FCH_CARGA_FMT_HIVE}'' AND \');
                  UTL_FILE.put_line(fich_salida_load, 'MTDT_MONITOREO.FCH_DATOS = ''${FCH_DATOS_FMT_HIVE}'' AND \');
                  UTL_FILE.put_line(fich_salida_load, 'MTDT_PROCESO.NOMBRE_PROCESO = ''' || nombre_fich_carga || ''' AND \');
                  UTL_FILE.put_line(fich_salida_load, 'MTDT_MONITOREO.CVE_RESULTADO = 0;');
                  UTL_FILE.put_line(fich_salida_load, '!quit');
                  UTL_FILE.put_line(fich_salida_load, 'EOF`');
                  UTL_FILE.put_line(fich_salida_load, 'ULT_PASO_EJECUTADO=`echo ${ULT_PASO_EJECUTADO_PREV} | sed -e ''s/\n//g'' -e ''s/\r//g'' -e ''s/^[ ]*//g'' -e ''s/[ ]*$//g''`');    
                  UTL_FILE.put_line(fich_salida_load, 'if [ ${ULT_PASO_EJECUTADO} -eq 1 ] && [ "${BAN_FORZADO}" = "N" ]');
                  UTL_FILE.put_line(fich_salida_load, 'then');
                  UTL_FILE.put_line(fich_salida_load, '  SUBJECT="${INTERFAZ}: Ya se ejecutaron Ok todos los pasos de este proceso."');
                  UTL_FILE.put_line(fich_salida_load, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
                  UTL_FILE.put_line(fich_salida_load, '  echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || substr(nombre_fich_carga, 1, length(nombre_fich_carga) - 3) || '_${FECHA_HORA}.log');        
                  UTL_FILE.put_line(fich_salida_load, '  echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || substr(nombre_fich_carga, 1, length(nombre_fich_carga) - 3) || '_${FECHA_HORA}.log');
                  UTL_FILE.put_line(fich_salida_load, '  exit 0');
                  UTL_FILE.put_line(fich_salida_load, 'fi');
                  
                  /*************************/
                  --UTL_FILE.put_line(fich_salida_load, 'INICIO_PASO_TMR=`beeline -u ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} -n ${BD_USER_HIVE} -p ${BD_CLAVE_HIVE} --silent=true --showHeader=false --outputformat=dsv -e "select current_timestamp from ${ESQUEMA_MT}.dual;"` >> ' || '${' || 'NGRD' || '_TRAZAS}/' || 'load_he' || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log ' || '2>&' || '1');
                  UTL_FILE.put_line(fich_salida_load, 'INICIO_PASO_TMR_PREV=`beeline --silent=true --showHeader=false --outputformat=dsv << EOF');
                  UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
                  UTL_FILE.put_line(fich_salida_load, 'select current_timestamp from ${ESQUEMA_MT}.dual;');
                  UTL_FILE.put_line(fich_salida_load, '!quit');
                  UTL_FILE.put_line(fich_salida_load, 'EOF`');
                  --UTL_FILE.put_line(fich_salida_load, 'INICIO_PASO_TMR=`echo ${INICIO_PASO_TMR_PREV} | sed -e ''s/ //g'' -e ''s/\n//g'' -e ''s/\r//g''`');    
                  UTL_FILE.put_line(fich_salida_load, 'INICIO_PASO_TMR=`echo ${INICIO_PASO_TMR_PREV} | sed -e ''s/\n//g'' -e ''s/\r//g'' -e ''s/^[ ]*//g'' -e ''s/[ ]*$//g''`');    
                  UTL_FILE.put_line(fich_salida_load, 'echo "Inicio del proceso de Validacion ' || nombre_fich_carga || '"' || ' >> ' || '${' || 'NGRD' || '_TRAZAS}/' || substr(nombre_fich_carga, 1, length(nombre_fich_carga) - 3) || '_${FECHA_HORA}.log');
                  UTL_FILE.put_line(fich_salida_load, '');
                  if (v_tabla_dinamica = true and v_fecha_ini_param = false and v_fecha_fin_param = false) then
                    --UTL_FILE.put_line(fich_salida_load, '  sed "s/\&' || '2/${FECHA_MES}/g" -e "s/#VAR_USER#/${BD_USER_HIVE}/g"' || ' -e "s/#DIRECTORIO#/${CAD_TMP}/g"' || ' ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL} > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1');
                    if (v_hay_usu_owner = true) then
                      UTL_FILE.put_line(fich_salida_load, 'sed -e "s/\&' || '2/${FECHA_MES}/g" -e "s/' || v_usuario_owner || '/${OWNER_EXT}/g" -e "s/#VAR_USER#/${BD_USER_HIVE}/g" ${NGRD_SQL}/' || nombre_fich_pkg || ' > ${NGRD_SQL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '_tmp.sql');
                    else
                      UTL_FILE.put_line(fich_salida_load, 'sed -e "s/\&' || '2/${FECHA_MES}/g" -e "s/#VAR_USER#/${BD_USER_HIVE}/g" ${NGRD_SQL}/' || nombre_fich_pkg || ' > ${NGRD_SQL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '_tmp.sql');
                    end if;
                  elsif (v_tabla_dinamica = true and v_fecha_ini_param = true and v_fecha_fin_param = true) then
                    if (v_hay_usu_owner = true) then
                      UTL_FILE.put_line(fich_salida_load, '  sed -e "s/\&' || '2/${FECHA_MES}/g" -e "s/\&' || '3/${FECHA}/g" -e "s/\&' || '4/${FECHA_FIN}/g -e "s/' || v_usuario_owner || '/${OWNER_EXT}/g" -e "s/#VAR_USER#/${BD_USER_HIVE}/g" ${NGRD_SQL}/' || nombre_fich_pkg || ' > ${NGRD_SQL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '_tmp.sql');
                    else
                      UTL_FILE.put_line(fich_salida_load, '  sed -e "s/\&' || '2/${FECHA_MES}/g" -e "s/\&' || '3/${FECHA}/g" -e "s/\&' || '4/${FECHA_FIN}/g" -e "s/#VAR_USER#/${BD_USER_HIVE}/g" ${NGRD_SQL}/' || nombre_fich_pkg || ' > ${NGRD_SQL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '_tmp.sql');
                    end if;
                  elsif (v_tabla_dinamica = true and v_fecha_ini_param = true and v_fecha_fin_param = false) then
                    if (v_hay_usu_owner = true) then
                      UTL_FILE.put_line(fich_salida_load, '  sed -e "s/\&' || '2/${FECHA_MES}/g" -e "s/\&' || '3/${FECHA}/g" -e "s/' || v_usuario_owner || '/${OWNER_EXT}/g" -e "s/#VAR_USER#/${BD_USER_HIVE}/g" ${NGRD_SQL}/' || nombre_fich_pkg || ' > ${NGRD_SQL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '_tmp.sql');
                    else
                      UTL_FILE.put_line(fich_salida_load, '  sed -e "s/\&' || '2/${FECHA_MES}/g" -e "s/\&' || '3/${FECHA}/g" -e "s/#VAR_USER#/${BD_USER_HIVE}/g" ${NGRD_SQL}/' || nombre_fich_pkg || ' > ${NGRD_SQL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '_tmp.sql');
                    end if;
                  elsif (v_tabla_dinamica = false and v_fecha_ini_param = true and v_fecha_fin_param = true) then
                    if (v_hay_usu_owner = true) then
                      UTL_FILE.put_line(fich_salida_load, '  sed -e "s/\&' || '2/${FECHA}/g" -e "s/\&' || '3/${FECHA_FIN}/g" -e "s/' || v_usuario_owner || '/${OWNER_EXT}/g" -e "s/#VAR_USER#/${BD_USER_HIVE}/g" ${NGRD_SQL}/' || nombre_fich_pkg || ' > ${NGRD_SQL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '_tmp.sql');
                    else
                      UTL_FILE.put_line(fich_salida_load, '  sed -e "s/\&' || '2/${FECHA}/g" -e "s/\&' || '3/${FECHA_FIN}/g" -e "s/#VAR_USER#/${BD_USER_HIVE}/g" ${NGRD_SQL}/' || nombre_fich_pkg || ' > ${NGRD_SQL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '_tmp.sql');
                    end if;
                  elsif (v_tabla_dinamica = false and v_fecha_ini_param = true and v_fecha_fin_param = false) then
                    if (v_hay_usu_owner = true) then
                      UTL_FILE.put_line(fich_salida_load, 'sed -e "s/\&' || '2/${FECHA}/g" -e "s/' || v_usuario_owner || '/${OWNER_EXT}/g" -e "s/#VAR_USER#/${BD_USER_HIVE}/g" ${NGRD_SQL}/' || nombre_fich_pkg || ' > ${NGRD_SQL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '_tmp.sql');
                    else
                      UTL_FILE.put_line(fich_salida_load, 'sed -e "s/\&' || '2/${FECHA}/g" -e "s/#VAR_USER#/${BD_USER_HIVE}/g" ${NGRD_SQL}/' || nombre_fich_pkg || ' > ${NGRD_SQL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '_tmp.sql');
                    end if;
                  else
                    UTL_FILE.put_line(fich_salida_load, '  sed -e "s/' || v_usuario_owner || '/${OWNER_EXT}/g" -e "s/#VAR_USER#/${BD_USER_HIVE}/g" ${NGRD_SQL}/' || nombre_fich_pkg || ' > ${NGRD_SQL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '_tmp.sql');
                  end if;
                  UTL_FILE.put_line(fich_salida_load, 'beeline --silent=true --showHeader=false --outputformat=dsv --nullemptystring=true << EOF > ' || '${' || NAME_DM || '_TMP_LOCAL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '.dat ' || '2>> ' || '${' || NAME_DM || '_TRAZAS}/' || substr(nombre_fich_carga, 1, length(nombre_fich_carga) - 3) || '_${FECHA_HORA}.log');
                  UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_ML}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
                  UTL_FILE.put_line(fich_salida_load, '!run ${' || NAME_DM || '_SQL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '_tmp.sql');
                  UTL_FILE.put_line(fich_salida_load, '!quit');
                  UTL_FILE.put_line(fich_salida_load, 'EOF');
                  UTL_FILE.put_line(fich_salida_load, 'ERROR=`grep -ic -e ''Error: Error while compiling statement: FAILED:'' -e ''java.lang.RuntimeException'' ${' || NAME_DM || '_TRAZAS}/' || substr(nombre_fich_carga, 1, length(nombre_fich_carga) - 3) || '_${FECHA_HORA}.log`');
                  --UTL_FILE.put_line(fich_salida_load, 'err_salida=$?');
                  UTL_FILE.put_line(fich_salida_load, '');
                  --UTL_FILE.put_line(fich_salida_load, 'if [ ${err_salida} -ne 0 ]; then');
                  UTL_FILE.put_line(fich_salida_load, 'if [ ${ERROR} != 0 ] ; then');
                  UTL_FILE.put_line(fich_salida_load, '  SUBJECT="${INTERFAZ}: Surgio un error en el proceso de validacion ' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '. Error:  ${err_salida}."');
                  UTL_FILE.put_line(fich_salida_load, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
                  UTL_FILE.put_line(fich_salida_load, '  echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || substr(nombre_fich_carga, 1, length(nombre_fich_carga) - 3) || '_${FECHA_HORA}.log');    
                  UTL_FILE.put_line(fich_salida_load, '  echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || substr(nombre_fich_carga, 1, length(nombre_fich_carga) - 3) || '_${FECHA_HORA}.log');
                  UTL_FILE.put_line(fich_salida_load, '  InsertaFinFallido');
                  UTL_FILE.put_line(fich_salida_load, '  exit 1');    
                  UTL_FILE.put_line(fich_salida_load, 'fi');
                  UTL_FILE.put_line(fich_salida_load, '');
                  UTL_FILE.put_line(fich_salida_load, '# Borro el fichero temporal .sql generado en vuelo');
                  UTL_FILE.put_line(fich_salida_load, 'rm -f ${NGRD_SQL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '_tmp.sql');
                  UTL_FILE.put_line(fich_salida_load, '# Surpimimos las lineas en blanco del fichero obtenido');
                  /* (20170718). Angel Ruiz. BUG: Aparecen nuevas lineas con basura al cambiar la forma de coenxion */
                  --UTL_FILE.put_line(fich_salida_load, 'grep -G -v -e ''^.$'' -e ''^[ ]*.$'' -e ''^$'' ' || '${' || NAME_DM || '_TMP_LOCAL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '.dat > ' || '${' || NAME_DM || '_TMP_LOCAL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '_tmp.dat');
                  UTL_FILE.put_line(fich_salida_load, 'grep -G -v -e ''^.$'' -e ''^[ ]*.$'' -e ''^$'' -e ''^ *...$'' -e ''^ *[^A-Za-z0-9]M *'' ' || '${' || NAME_DM || '_TMP_LOCAL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '.dat > ' || '${' || NAME_DM || '_TMP_LOCAL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '_tmp.dat');
                  UTL_FILE.put_line(fich_salida_load, 'mv ${' || NAME_DM || '_TMP_LOCAL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '_tmp.dat ${' || NAME_DM || '_TMP_LOCAL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '.dat');
                  UTL_FILE.put_line(fich_salida_load, 'if [ $? -ne 0 ]');
                  UTL_FILE.put_line(fich_salida_load, 'then');
                  UTL_FILE.put_line(fich_salida_load, '  SUBJECT="${INTERFAZ}: Surgio un error en el proceso de validacion ' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '. Al mover el fichero temporal .${' || NAME_DM || '_TMP_LOCAL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '_tmp.dat' || '"');
                  UTL_FILE.put_line(fich_salida_load, '  ${SHELL_SMS} "${TELEFONOS_DWH}" "${SUBJECT}"');
                  UTL_FILE.put_line(fich_salida_load, '  echo ${SUBJECT} >> ' || '${' || NAME_DM || '_TRAZAS}/' || substr(nombre_fich_carga, 1, length(nombre_fich_carga) - 3) || '_${FECHA_HORA}.log');    
                  UTL_FILE.put_line(fich_salida_load, '  echo `date` >> ' || '${' || NAME_DM || '_TRAZAS}/' || substr(nombre_fich_carga, 1, length(nombre_fich_carga) - 3) || '_${FECHA_HORA}.log');
                  UTL_FILE.put_line(fich_salida_load, '  InsertaFinFallido');
                  UTL_FILE.put_line(fich_salida_load, '  exit 1');    
                  UTL_FILE.put_line(fich_salida_load, 'fi');
                  UTL_FILE.put_line(fich_salida_load, '# Obtenemos el numero de registros obtenidos por la query de validacion');
                  UTL_FILE.put_line(fich_salida_load, 'TOT_INSERTADOS=`wc -l ${' || NAME_DM || '_TMP_LOCAL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '.dat | cut -d'' '' -f1`');
                  UTL_FILE.put_line(fich_salida_load, '# Borramos el fichero de datos obtenido');
                  UTL_FILE.put_line(fich_salida_load, 'rm -f ${' || NAME_DM || '_TMP_LOCAL}/' || substr(nombre_fich_pkg, 1, length(nombre_fich_pkg) - 4) || '.dat');
                  UTL_FILE.put_line(fich_salida_load, '# Insertamos que el proceso se ha Ejecutado Correctamente');
                  UTL_FILE.put_line(fich_salida_load, 'InsertaFinOK');
                  UTL_FILE.put_line(fich_salida_load, '');
                  UTL_FILE.put_line(fich_salida_load, 'echo "El proceso ' || nombre_fich_carga || ' se ha realizado correctamente." >> ' || '${' || NAME_DM || '_TRAZAS}/' || substr(nombre_fich_carga, 1, length(nombre_fich_carga) - 3) || '_${FECHA_HORA}.log');
                  UTL_FILE.put_line(fich_salida_load, '');
                  UTL_FILE.put_line(fich_salida_load, 'exit 0');

                  UTL_FILE.FCLOSE (fich_salida_load);
                  --UTL_FILE.FCLOSE (fich_salida_exchange);
                  UTL_FILE.FCLOSE (fich_salida_pkg);
                end if;  /* if (reg_detail.VALIDA_TABLE_LKUP is not null) then */
              end loop;
              close MTDT_TC_DETAIL;
            end if; /* if ((reg_scenario.TABLE_TYPE = 'F' and v_hay_sce_COMPUESTO = false) or reg_scenario.TABLE_TYPE = 'C') then */
            /**************/
            /**************/
          end if;    /* Final del IF que comprueba que el escenario es el mismo que tenemos */
        end loop;
        close MTDT_SCENARIO;
      end if;  /* IF DEL UNION, INTERSEC, ALL */
    END LOOP; /*     FOR ind_scenario IN v_lista_elementos_scenario.FIRST .. v_lista_elementos_scenario.LAST */
    --UTL_FILE.put_line (fich_salida_pkg, ';');
  
    /**************/
    
  end loop;
  close MTDT_TABLA;
end;

