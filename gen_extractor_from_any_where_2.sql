declare

cursor MTDT_TABLA
  is
SELECT
      DISTINCT TRIM(MTDT_EXT_SCENARIO_1.TABLE_NAME) "TABLE_NAME" /*(20150907) Angel Ruiz NF. Nuevas tablas.*/
    FROM
      --MTDT_TC_SCENARIO, mtdt_modelo_logico (20150907) Angel Ruiz NF. Nuevas tablas.
      MTDT_EXT_SCENARIO_1
    --WHERE MTDT_EXT_SCFORMA_PAGOENARIO.TABLE_TYPE = 'F' and
    WHERE
      (trim(MTDT_EXT_SCENARIO_1.STATUS) = 'P' or trim(MTDT_EXT_SCENARIO_1.STATUS) = 'D')
      --and trim(MTDT_EXT_SCENARIO_1.TABLE_NAME) in (
      --'EQUIPO', 'REGION_COMERCIAL_NIVEL3', 'REGION_COMERCIAL_NIVEL2', 'REGION_COMERCIAL_NIVEL1', 'PRIMARY_OFFER'
      --, 'PARQUE_ABO_MES', 'SUPPLEMENTARY_OFFER', 'BONUS', 'HANDSET_PRICE', 'PARQUE_SVA_MES', 'PARQUE_BENEF_MES', 'PSD_RESIDENCIAL'
      --, 'OFFER_ITEM', 'MOVIMIENTOS_ABO_MES', 'MOVIMIENTO_ABO', 'COMIS_POS_ABO_MES', 'AJUSTE_ABO_MES', 'MODALIDAD_VENTA'
      --, 'DESCUENTO', 'DESC_ADQR_ABO_MES', 'DESC_EJEC_ABO_MES');
    
    --and trim(MTDT_EXT_SCENARIO_1.TABLE_NAME) in ('PARQUE_PROMO_CAMPANA', 'MOV_PROMO_CAMPANA'
    --  );
      --and trim(MTDT_EXT_SCENARIO_1.TABLE_NAME) in ('PARQUE_ABO_PRE', 'PARQUE_ABO_POST', 'CLIENTE', 'CUENTA', 
    --'MOVIMIENTO_ABO', 'PLAN_TARIFARIO',
    --'CATEGORIA_CLIENTE', 'CICLO', 'ESTATUS_OPERACION', 'FORMA_PAGO', 'PROMOCION', 'SEGMENTO_CLIENTE', 
    --'GRUPO_ABONADO', 'REL_GRUPO_ABONADO');
    --and trim(MTDT_EXT_SCENARIO_1.TABLE_NAME) in ('OFFER_ITEM', 'EQUIPO');
    and trim(MTDT_EXT_SCENARIO_1.TABLE_NAME) in ('OFFER_ITEM');

  /********************************/
  /* (20170413) Angel Ruiz. NF: Extraccion de interfaces desde diferentes */
  /* fuentes */
  cursor MTDT_FUENTES (table_name_in IN VARCHAR2)
  is
    SELECT DISTINCT TRIM(SOURCE) "SOURCE"
    FROM MTDT_EXT_SCENARIO_1
    where MTDT_EXT_SCENARIO_1.TABLE_NAME = table_name_in 
    and MTDT_EXT_SCENARIO_1.SOURCE IS NOT NULL;
  /* (20170413) Angel Ruiz. NF: FIN */
  
  cursor MTDT_SCENARIO (table_name_in IN VARCHAR2, fuente_in IN VARCHAR2)
  is
    SELECT 
      TRIM(MTDT_EXT_SCENARIO_1.TABLE_NAME) "TABLE_NAME",
      TRIM(MTDT_EXT_SCENARIO_1.TABLE_TYPE) "TABLE_TYPE",
      TRIM(MTDT_EXT_SCENARIO_1.SOURCE) "SOURCE",
      TRIM(MTDT_EXT_SCENARIO_1.TABLE_BASE_NAME) "TABLE_BASE_NAME",
      TRIM(MTDT_EXT_SCENARIO_1.HINT) "HINT",
      TRIM(MTDT_EXT_SCENARIO_1.OVER_PARTION) "OVER_PARTION",
      TRIM(MTDT_EXT_SCENARIO_1.DISTINCT_COL) "DISTINCT_COL",
      TRIM(MTDT_EXT_SCENARIO_1."SELECT") "SELECT",
      TRIM (MTDT_EXT_SCENARIO_1."GROUP") "COLUMNA_GROUP",
      TRIM(MTDT_EXT_SCENARIO_1.FILTER) "FILTER",
      TRIM(MTDT_EXT_SCENARIO_1.INTERFACE_COLUMNS) "INTERFACE_COLUMNS",
      TRIM(MTDT_EXT_SCENARIO_1.SCENARIO) "SCENARIO",
      TRIM(MTDT_INTERFACE_SUMMARY.INTERFACE_NAME) "INTERFACE_NAME",
      TRIM(MTDT_INTERFACE_SUMMARY.TYPE) "TYPE",
      TRIM(MTDT_INTERFACE_SUMMARY.SEPARATOR) "SEPARATOR",
      --TRIM(MTDT_INTERFACE_SUMMARY.SOURCE) "SOURCE",
      TRIM(MTDT_INTERFACE_SUMMARY.FREQUENCY) "FREQUENCY"
    FROM 
      MTDT_EXT_SCENARIO_1, MTDT_INTERFACE_SUMMARY
    WHERE
      trim(MTDT_EXT_SCENARIO_1.TABLE_NAME) = table_name_in and
      trim(MTDT_EXT_SCENARIO_1.SOURCE) = fuente_in and
      trim(MTDT_EXT_SCENARIO_1.TABLE_NAME) = trim(MTDT_INTERFACE_SUMMARY.CONCEPT_NAME) and
      (trim(MTDT_EXT_SCENARIO_1.STATUS) = 'P' or trim(MTDT_EXT_SCENARIO_1.STATUS) = 'D') and
      (MTDT_INTERFACE_SUMMARY.STATUS = 'P' or MTDT_INTERFACE_SUMMARY.STATUS = 'D')
    ORDER BY MTDT_EXT_SCENARIO_1.TABLE_TYPE;
  
  CURSOR MTDT_TC_DETAIL (table_name_in IN VARCHAR2, scenario_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(MTDT_EXT_DETAIL.TABLE_NAME) "TABLE_NAME",
      TRIM(MTDT_EXT_DETAIL.TABLE_COLUMN) "TABLE_COLUMN",
      TRIM(MTDT_EXT_DETAIL.TABLE_BASE_NAME) "TABLE_BASE_NAME",
      TRIM(MTDT_EXT_DETAIL.SCENARIO) "SCENARIO",
      UPPER(TRIM(MTDT_EXT_DETAIL.OUTER)) "OUTER",
      MTDT_EXT_DETAIL.SEVERIDAD,
      TRIM(MTDT_EXT_DETAIL.TABLE_LKUP) "TABLE_LKUP",
      TRIM(MTDT_EXT_DETAIL.TABLE_COLUMN_LKUP) "TABLE_COLUMN_LKUP",
      TRIM(MTDT_EXT_DETAIL.TABLE_LKUP_COND) "TABLE_LKUP_COND",
      TRIM(MTDT_EXT_DETAIL.IE_COLUMN_LKUP) "IE_COLUMN_LKUP",
      TRIM(MTDT_EXT_DETAIL.LKUP_COM_RULE) "LKUP_COM_RULE",
      TRIM(MTDT_EXT_DETAIL.VALUE) "VALUE",
      TRIM(MTDT_EXT_DETAIL.RUL) "RUL",
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
  reg_fuente MTDT_FUENTES%rowtype;
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
  l_WHERE                                   lista_condi_where := lista_condi_where();
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
  v_country                            varchar2(20);
  v_type_validation                   varchar2(1);
  v_separador_campos                VARCHAR2(1);
  v_contador                        PLS_integer;
  v_num_campos_interfaz PLS_integer;
  v_usuario_owner                   VARCHAR2(50);
  v_hay_usu_owner                   boolean:=false;
  v_multiplicador_proc              varchar2(60);
  v_separator                       varchar2(3);
  v_frequency     varchar2(1);
  v_numero_fuentes                  PLS_integer;


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
    --parte_1 := trim(substr(lista_elementos(1), instr(lista_elementos(1), '(') + 1)); /* Me quedo con ID_FUENTE*/
    --parte_2 := lista_elementos(2);  /* Me quedo con 'SER' */
    --parte_3 := trim(lista_elementos(3));  /* Me quedo con ID_CANAL */
    --parte_4 := trim(substr(lista_elementos(4), 1, instr(lista_elementos(4), ')') - 1));  /* Me quedo con '1' */
    --if (instr(parte_1, '''') = 0) then
      /* Esta parte del DECODE no es un literal */
      /* Lo que quiere decir que podemos calificarlo con el nombre de la tabla */
      --if (outer_in = 1) then
        --parte_1 := alias_in || '.' || parte_1 || '(+)';
      --else
        --parte_1 := alias_in || '.' || parte_1;
      --end if;
    --end if;
    --if (instr(parte_2, '''') = 0) then
      /* Esta parte del DECODE no es un literal */
      /* Lo que quiere decir que podemos calificarlo con el nombre de la tabla */
      --if (outer_in = 1) then
        --parte_2 := alias_in || '.' || parte_2 || '(+)';
      --else
        --parte_2 := alias_in || '.' || parte_2;
      --end if;
    --end if;
    --if (instr(parte_3, '''') = 0) then
      /* Esta parte del DECODE no es un literal */
      /* Lo que quiere decir que podemos calificarlo con el nombre de la tabla */
      --if (outer_in = 1) then
        --parte_3 := alias_in || '.' || parte_3 || '(+)';
      --else
        --parte_3 := alias_in || '.' || parte_3;
      --end if;
    --end if;
    --if (instr(parte_4, '''') = 0) then
      /* Esta parte del DECODE no es un literal */
      /* Lo que quiere decir que podemos calificarlo con el nombre de la tabla */
      --if (outer_in = 1) then
        --parte_4 := alias_in || '.' || parte_4 || '(+)';
      --else
        --parte_4 := alias_in || '.' || parte_4;
      --end if;
    --end if;
    /* Puede ocurrir que alguna parte del decode tanga el signo ' como seria el caso de los campos literales */
    /* como estamos generando querys dinamicas, tenemos que escapar las comillas */
    --if (instr(parte_1, '''') > 0) then
      --parte_1 := sustituye_comillas_dinam(parte_1);
    --end if;
    --if (instr(parte_2, '''') > 0) then
      --parte_2 := sustituye_comillas_dinam(parte_2);
    --end if;
    --if (instr(parte_3, '''') > 0) then
      --parte_3 := sustituye_comillas_dinam(parte_3);
    --end if;
    --if (instr(parte_4, '''') > 0) then
      --parte_4 := sustituye_comillas_dinam(parte_4);
    --end if;
    --decode_out := 'DECODE(' || parte_1 || ', ' || parte_2 || ', ' || parte_3 || ', ' || parte_4 || ')';
    --return decode_out;
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
        sustituto := ' to_date ( fch_datos_in, ''yyyymmdd'') ';
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
          l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
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
              v_table_look_up := '#OWNER_' || reg_scenario.SOURCE || '#' || '.' || v_table_look_up;
            end if;
            mitabla_look_up := v_table_look_up || ' ' || v_alias_table_look_up;
            /* Busco si estaba ya en el FROM. Como es una tabla con ALIAS */
            /* si ya estaba en el FROM entonces no la vuelo a meter ya que tiene un ALIAS */
            v_encontrado:='N';
            FOR indx IN l_FROM.FIRST .. l_FROM.LAST
            LOOP
              --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
              if (regexp_count(l_FROM(indx), mitabla_look_up) >0) then
              --if (l_FROM(indx) = ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP) then
                /* La misma tabla ya estaba en otro lookup */
                v_encontrado:='Y';
              end if;
            END LOOP;
            if (v_encontrado='N') then
              /* Solo la introduzco si la tabla no estaba ya */
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
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
              v_table_look_up := '#OWNER_' || reg_scenario.SOURCE || '#' || '.' || v_table_look_up;
            end if;
            dbms_output.put_line('El alias es: ' || v_alias_table_look_up);
            dbms_output.put_line('La tabla de LKUP es: ' || v_table_look_up);
            mitabla_look_up := v_table_look_up;
            v_encontrado:='N';
            FOR indx IN l_FROM.FIRST .. l_FROM.LAST
            LOOP
              --if (instr(l_FROM(indx),  reg_detalle_in.TABLE_LKUP, 0)) then
              --regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0
              --if (regexp_count(l_FROM(indx), reg_detalle_in.TABLE_LKUP) >0) then
              if (regexp_count(l_FROM(indx), mitabla_look_up) >0) then
              --if (l_FROM(indx) = ', ' || OWNER_EX || '.' || reg_detalle_in.TABLE_LKUP) then
                /* La misma tabla ya estaba en otro lookup */
                v_encontrado:='Y';
              end if;
            END LOOP;
            if (v_encontrado='Y') then
              v_alias := reg_detalle_in.TABLE_LKUP || '_' || l_FROM.count;
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP) || ' "' || v_alias || '"' ;
              l_FROM (l_FROM.last) := ', ' || mitabla_look_up || ' "' || v_alias || '"' ;
            else
              --v_alias := reg_detalle_in.TABLE_LKUP;
              v_alias := v_alias_table_look_up;
              --l_FROM (l_FROM.last) := ', ' || procesa_campo_filter(reg_detalle_in.TABLE_LKUP);
              l_FROM (l_FROM.last) := ', ' || mitabla_look_up;
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
        
        if (table_columns_lkup.COUNT > 1) then      /* Hay varios campos de condicion */
          FOR indx IN table_columns_lkup.FIRST .. table_columns_lkup.LAST
          LOOP
            l_WHERE.extend;
            /* (20150126) Angel Ruiz. Incidencia referente a que siempre se coloca el valor -2 */
            /* Recojo el tipo de dato del campo con el que se va a hacer LookUp */
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Tabla es: ' || reg_detalle_in.TABLE_BASE_NAME);
            dbms_output.put_line('ESTOY EN EL LOOKUP. Este LoopUp es de varias columnas. La Columna es: ' || ie_column_lkup(indx));
            
            if (l_WHERE.count = 1) then
              /* (20160302) Angel Ruiz. NF: DECODE en las columnas de LookUp */
              if (instr(upper(ie_column_lkup(indx)), 'DECODE') > 0 and instr(upper(table_columns_lkup(indx)), 'DECODE') > 0) then
                if (reg_detalle_in."OUTER" = 'Y') then
                  l_WHERE(l_WHERE.last) := transformo_decode(ie_column_lkup(indx), v_alias_table_base_name, 0) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 1);
                else
                  l_WHERE(l_WHERE.last) := transformo_decode(ie_column_lkup(indx), v_alias_table_base_name, 0) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                end if;
              elsif (instr(upper(ie_column_lkup(indx)), 'DECODE') > 0 and instr(upper(table_columns_lkup(indx)), 'DECODE') = 0) then
                if (reg_detalle_in."OUTER" = 'Y') then
                  l_WHERE(l_WHERE.last) := transformo_decode(ie_column_lkup(indx), v_alias_table_base_name, 0) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                else
                  l_WHERE(l_WHERE.last) := transformo_decode(ie_column_lkup(indx), v_alias_table_base_name, 0) || ' = ' || v_alias || '.' || table_columns_lkup(indx);
                end if;
              elsif (instr(upper(ie_column_lkup(indx)), 'DECODE') = 0 and instr(upper(table_columns_lkup(indx)), 'DECODE') > 0) then
                if (reg_detalle_in."OUTER" = 'Y') then
                  l_WHERE(l_WHERE.last) := v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 1);
                else
                  l_WHERE(l_WHERE.last) := v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                end if;
              elsif (instr(upper(table_columns_lkup(indx)), 'BETWEEN') > 0) then
                if (reg_detalle_in."OUTER" = 'Y') then
                  l_WHERE(l_WHERE.last) :=  v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' ' || transformo_between(v_alias, table_columns_lkup(indx), true);
                else
                  l_WHERE(l_WHERE.last) :=  v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' ' || transformo_between(v_alias, table_columns_lkup(indx), false);
                end if;
              elsif (regexp_count(upper(table_columns_lkup(indx)), 'TRIM *\(') > 0) then
                if (reg_detalle_in."OUTER" = 'Y') then
                  l_WHERE(l_WHERE.last) :=  v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || table_columns_lkup(indx) || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || table_columns_lkup(indx);
                end if;                
              else
                if (reg_detalle_in."OUTER" = 'Y') then
                  l_WHERE(l_WHERE.last) :=  v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx);
                end if;
              end if;
            else  /* siguientes elementos del where */
              if (instr(upper(ie_column_lkup(indx)), 'DECODE') > 0 and instr(upper(table_columns_lkup(indx)), 'DECODE') > 0) then
                if (reg_detalle_in."OUTER" = 'Y') then
                  l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_decode(ie_column_lkup(indx), v_alias_table_base_name, 0) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 1);
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_decode(ie_column_lkup(indx), v_alias_table_base_name, 0) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                end if;
              elsif (instr(upper(ie_column_lkup(indx)), 'DECODE') > 0 and instr(upper(table_columns_lkup(indx)), 'DECODE') = 0) then
                if (reg_detalle_in."OUTER" = 'Y') then
                  l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_decode(ie_column_lkup(indx), v_alias_table_base_name, 0) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_decode(ie_column_lkup(indx), v_alias_table_base_name, 0) || ' = ' || v_alias || '.' || table_columns_lkup(indx);
                end if;
              elsif (instr(upper(ie_column_lkup(indx)), 'DECODE') = 0 and instr(upper(table_columns_lkup(indx)), 'DECODE') > 0) then
                if (reg_detalle_in."OUTER" = 'Y') then
                  l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 1);
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || transformo_decode(table_columns_lkup(indx), v_alias, 0);
                end if;              
              elsif (instr(upper(table_columns_lkup(indx)), 'BETWEEN') > 0) then
                if (reg_detalle_in."OUTER" = 'Y') then
                  l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' ' || transformo_between(v_alias, table_columns_lkup(indx), true);
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' ' || transformo_between(v_alias, table_columns_lkup(indx), false);
                end if;
              elsif (regexp_count(upper(table_columns_lkup(indx)), 'TRIM *\(') > 0) then
                if (reg_detalle_in."OUTER" = 'Y') then
                  l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || table_columns_lkup(indx) || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || table_columns_lkup(indx);
                end if;
              else
                if (reg_detalle_in."OUTER" = 'Y') then
                  l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx) || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || ie_column_lkup(indx) || ' = ' || v_alias || '.' || table_columns_lkup(indx);
                end if;
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
              --l_WHERE(l_WHERE.last) := 'NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              if (instr(reg_detalle_in.IE_COLUMN_LKUP, 'DECODE') > 0 or instr(reg_detalle_in.IE_COLUMN_LKUP, 'decode') > 0) then
                if (reg_detalle_in."OUTER" = 'Y') then
                  l_WHERE(l_WHERE.last) :=  transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name, 0) ||  ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 1);
                else
                  l_WHERE(l_WHERE.last) :=  transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name, 0) ||  ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                end if;
              elsif (instr(upper(reg_detalle_in.TABLE_COLUMN_LKUP), 'BETWEEN') > 0) then
                if (reg_detalle_in."OUTER" = 'Y') then
                  l_WHERE(l_WHERE.last) :=  v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' ' || transformo_between(v_alias, reg_detalle_in.TABLE_COLUMN_LKUP, true);
                else
                  l_WHERE(l_WHERE.last) :=  v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' ' || transformo_between(v_alias, reg_detalle_in.TABLE_COLUMN_LKUP, false);
                end if;
              elsif (regexp_count(upper(reg_detalle_in.TABLE_COLUMN_LKUP), 'TRIM *\(') > 0) then
                if (reg_detalle_in."OUTER" = 'Y') then
                  l_WHERE(l_WHERE.last) := v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                else
                  l_WHERE(l_WHERE.last) := v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP;
                end if;
              else
                if (reg_detalle_in."OUTER" = 'Y') then
                  /* (20160630) Angel Ruiz. BUG. Ocurre que si los campos IE_COLUMN_LKUP o TABLE_COLUMN_LKUP ya estan calificados no hay que hacerlo */
                  if (instr(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name || '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias || '.') = 0) then
                    /* (20161004) Angel Ruiz. BUG.Ocurre que puede ponersele al campo IE_COLUMN_LKUP un ALIAS. */
                    /* pero este ALIAS no corresponde con el ALIAS de la tabla TABLE_BASE_NAME, sino de otra tabla LOOKUP */
                    /* por lo que dejamos el campo IE_COLUMN_LKUP con el alias que trae */
                    if instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0 then
                      /* (20161004) Angel Ruiz. Dejamos el campo IE_COLUMN_LKUP con el alias que trae */
                      l_WHERE(l_WHERE.last) := reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                    else                  
                      l_WHERE(l_WHERE.last) := v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                    end if;
                  elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name || '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias || '.') > 0) then
                    l_WHERE(l_WHERE.last) := v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                  else
                    l_WHERE(l_WHERE.last) := reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                  end if;
                else
                  /* (20160630) Angel Ruiz. BUG. Ocurre que si los campos IE_COLUMN_LKUP o TABLE_COLUMN_LKUP ya estan calificados no hay que hacerlo */
                  if (instr(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name || '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias || '.') = 0) then
                    /* (20161004) Angel Ruiz. BUG.Ocurre que puede ponersele al campo IE_COLUMN_LKUP un ALIAS. */
                    /* pero este ALIAS no corresponde con el ALIAS de la tabla TABLE_BASE_NAME, sino de otra tabla LOOKUP */
                    /* por lo que dejamos el campo IE_COLUMN_LKUP con el alias que trae */
                    if instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0 then
                      /* (20161004) Angel Ruiz. Dejamos el campo IE_COLUMN_LKUP con el alias que trae */
                      l_WHERE(l_WHERE.last) := reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                    else                  
                      l_WHERE(l_WHERE.last) := v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                    end if;
                  elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name || '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias || '.') > 0) then
                    l_WHERE(l_WHERE.last) := v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  else
                    l_WHERE(l_WHERE.last) := reg_detalle_in.IE_COLUMN_LKUP ||  ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  end if;
                end if;
              end if;
            else  /* sino es el primer campo del Where  */
              --l_WHERE(l_WHERE.last) :=  ' AND NVL(' || reg_detalle_in.TABLE_BASE_NAME || '.' || reg_detalle_in.IE_COLUMN_LKUP || ', -3)' || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
              if (instr(reg_detalle_in.IE_COLUMN_LKUP, 'DECODE') > 0 or instr(reg_detalle_in.IE_COLUMN_LKUP, 'decode') > 0) then
                if (reg_detalle_in."OUTER" = 'Y') then
                  l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name, 0) || ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 1);
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || transformo_decode(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name, 0) || ' = ' || transformo_decode(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias, 0);
                end if;
              elsif (instr(upper(reg_detalle_in.TABLE_COLUMN_LKUP), 'BETWEEN') > 0 ) then
                if (reg_detalle_in."OUTER" = 'Y') then
                  l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' ' || transformo_between(v_alias, reg_detalle_in.TABLE_COLUMN_LKUP, true);
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' ' || transformo_between(v_alias, reg_detalle_in.TABLE_COLUMN_LKUP, false);
                end if;
              elsif (regexp_count(upper(reg_detalle_in.TABLE_COLUMN_LKUP), 'TRIM *\(') > 0) then
                dbms_output.put_line('ESTOY . La Columna es: ' || reg_detalle_in.IE_COLUMN_LKUP);
                if (reg_detalle_in."OUTER" = 'Y') then
                  l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                else
                  l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP;
                end if;
              else
                if (reg_detalle_in."OUTER" = 'Y') then
                  /* (20160630) Angel Ruiz. BUG. Ocurre que si los campos IE_COLUMN_LKUP o TABLE_COLUMN_LKUP ya estan calificados no hay que hacerlo */
                  if (instr(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name || '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias || '.') = 0) then
                    /* (20161004) Angel Ruiz. BUG.Ocurre que puede ponersele al campo IE_COLUMN_LKUP un ALIAS. */
                    /* pero este ALIAS no corresponde con el ALIAS de la tabla TABLE_BASE_NAME, sino de otra tabla LOOKUP */
                    /* por lo que dejamos el campo IE_COLUMN_LKUP con el alias que trae */
                    if instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0 then
                      /* (20161004) Angel Ruiz. Dejamos el campo IE_COLUMN_LKUP con el alias que trae */
                      l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                    else
                      l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                    end if;
                  elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name || '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias || '.') > 0) then
                    l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                  else
                    l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP || ' (+)';
                  end if;
                else
                  /* (20160630) Angel Ruiz. BUG. Ocurre que si los campos IE_COLUMN_LKUP o TABLE_COLUMN_LKUP ya estan calificados no hay que hacerlo */
                  if (instr(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name || '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias || '.') = 0) then
                    /* (20161004) Angel Ruiz. Ocurre que puede ponersele al campo IE_COLUMN_LKUP un ALIAS. */
                    /* pero este ALIAS no corresponde con el ALIAS de la tabla TABLE_BASE_NAME, sino de otra tabla LOOKUP */
                    /* por lo que dejamos el campo IE_COLUMN_LKUP con el alias que trae */
                    if instr(reg_detalle_in.IE_COLUMN_LKUP, '.') > 0 then
                      /* (20161004) Angel Ruiz. Dejamos el campo IE_COLUMN_LKUP con el alias que trae */
                      l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                    else
                      l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                    end if;
                  elsif (instr(reg_detalle_in.IE_COLUMN_LKUP, v_alias_table_base_name || '.') = 0 and instr(reg_detalle_in.TABLE_COLUMN_LKUP, v_alias || '.') > 0) then
                    l_WHERE(l_WHERE.last) :=  ' AND ' || v_alias_table_base_name || '.' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  else
                    l_WHERE(l_WHERE.last) :=  ' AND ' || reg_detalle_in.IE_COLUMN_LKUP || ' = ' || v_alias || '.' || reg_detalle_in.TABLE_COLUMN_LKUP;
                  end if;
                end if;
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
          if (reg_detalle_in."OUTER" IS NOT NULL and reg_detalle_in."OUTER" = 'Y') then
            l_WHERE(l_WHERE.last) :=  ' AND ' || procesa_condicion_lookup (procesa_campo_filter(reg_detalle_in.TABLE_LKUP_COND), v_alias, TRUE);
          else
            l_WHERE(l_WHERE.last) :=  ' AND ' || procesa_condicion_lookup (procesa_campo_filter(reg_detalle_in.TABLE_LKUP_COND), v_alias, FALSE);
          end if;
          
        end if;
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
        valor_retorno :=  'TO_DATE (''&' || '2'', ''YYYYMMDD'')';
      when 'DSYS' then
        valor_retorno :=  'SYSDATE';
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
          cad_pri := substr(valor_retorno, 1, posicion-1);
          cad_seg := substr(valor_retorno, posicion + length('VAR_FCH_CARGA'));
          valor_retorno := cad_pri || ' to_date(''&' || '2'', ''yyyymmdd'') ' || cad_seg;
        end if;
        valor_retorno := cambia_fin_linea (valor_retorno);
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
        /* (20170502) Angel Ruiz. BUG. Calificamos los campos BASE con la table base name*/
        if (instr(reg_detalle_in.VALUE, '.') = 0) then
          /* Solo si el campo ya no esta calificado lo calificamos */
          if (REGEXP_LIKE(trim(reg_detalle_in.TABLE_BASE_NAME), '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9]+') = true) then
            /* La tabla esta calificada */
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
          --valor_retorno := reg_detalle_in.VALUE;
          valor_retorno := v_alias_table_base_name || '.' || reg_detalle_in.VALUE;
        else
          valor_retorno := reg_detalle_in.VALUE;
        end if;
        /* (20170502) Angel Ruiz. BUG. Calificamos los campos BASE con la table base name*/
      when 'VAR_FCH_INICIO' then
        --valor_retorno :=  '    ' || ''' || var_fch_inicio || ''';
        valor_retorno :=  'TO_CHAR(SYSDATE, ''YYYY-MM-DD'')';
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
              valor_retorno :=  '     ' ||  'TO_DATE (''&' || '2'', ''YYYY-MM-DD'')';
            else
              valor_retorno :=  '     ' ||  'TO_DATE (''&' || '1'', ''YYYY-MM-DD'')';
            end if;
          else
            if v_tabla_dinamica = true then
              valor_retorno :=  '     ' ||  'TO_DATE (''&' || '3'', ''YYYY-MM-DD'')';
            else
              valor_retorno :=  '     ' ||  'TO_DATE (''&' || '2'', ''YYYY-MM-DD'')';
            end if;
          end if;
          v_fecha_ini_param:=true; /*(20161104) Angel Ruiz.  BUG*/
          --valor_retorno := '    ' || ''' || fch_datos_in || ''';        
        end if;
        if reg_detalle_in.VALUE =  'VAR_PAIS_TM' then /* Si se trata de la fecha de carga, la podemos coger del parametro de la funcion */
          valor_retorno := '1';
        end if;
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
    nombre_fich_carga := REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '.sh';
    --nombre_fich_carga := 'ONIX' || '_' || reg_tabla.TABLE_NAME || '.sh';
    /* (20160817) Angel Ruiz FIN Cambio temporal para adecuarse a la entrega de produccion*/
    /* (20160817) Angel Ruiz. Cambio temporal para adecuarse a la entrega de produccion*/
    --nombre_fich_pkg := 'ONIX' || '_' || reg_tabla.TABLE_NAME || '.sql';
    /* (20160817) Angel Ruiz FIN Cambio temporal para adecuarse a la entrega de produccion*/
    fich_salida_load := UTL_FILE.FOPEN ('SALIDA',nombre_fich_carga,'W');
    /* (20170413) Angel Ruiz. NF: Extraccion desde varias FUENTES */
    /* Calculo el numero de fuentes que tiene la interfaz */
    select count(*) into v_numero_fuentes from (select DISTINCT SOURCE from MTDT_EXT_SCENARIO_1 where TABLE_NAME = reg_tabla.TABLE_NAME);
    open MTDT_FUENTES(reg_tabla.TABLE_NAME);
    loop
      fetch MTDT_FUENTES
      into reg_fuente;
      exit when MTDT_FUENTES%NOTFOUND;
      if (v_numero_fuentes > 1) then
        nombre_fich_pkg := REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_' || reg_fuente.SOURCE || '.sql';
      else
        nombre_fich_pkg := REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '.sql';
      end if;
      fich_salida_pkg := UTL_FILE.FOPEN ('SALIDA',nombre_fich_pkg,'W');
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
        select 
          sum(to_number(case 
          when instr(mtdt_interface_detail.length, ',') > 0 then 
            (trim(substr(mtdt_interface_detail.length, 1, instr(mtdt_interface_detail.length, ',') - 1)))
          when instr(mtdt_interface_detail.length, '.') > 0 then
          (trim(substr(mtdt_interface_detail.length, 1, instr(mtdt_interface_detail.length, '.') - 1)))
          else
            trim(mtdt_interface_detail.length)
          end)) into v_line_size
        from mtdt_interface_detail where trim(CONCEPT_NAME) = reg_tabla.TABLE_NAME;      
      --end if;
      /* (20160606) Angel Ruiz. NF: Se trata de la validacion en la que en lugar de ir a un fichero plano */
      /* va directamente a las tablas de Stagin */
      select nvl(TYPE_VALIDATION, 'T') into v_type_validation from MTDT_INTERFACE_SUMMARY where trim(CONCEPT_NAME) = trim(reg_tabla.TABLE_NAME);
    
      --UTL_FILE.put_line (fich_salida_pkg,'WHENEVER SQLERROR EXIT 1;');
      --UTL_FILE.put_line (fich_salida_pkg,'WHENEVER OSERROR EXIT 2;');
      UTL_FILE.put_line (fich_salida_pkg,'#');
      UTL_FILE.put_line (fich_salida_pkg,'# Options file for Sqoop import');
      UTL_FILE.put_line (fich_salida_pkg,'#');
      UTL_FILE.put_line (fich_salida_pkg,'');
      UTL_FILE.put_line (fich_salida_pkg,'# QUERY');
      UTL_FILE.put_line (fich_salida_pkg,'--query');
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
      select count(*) into v_contador from MTDT_EXT_SCENARIO_1 where 
      trim(MTDT_EXT_SCENARIO_1.TABLE_NAME) = reg_tabla.TABLE_NAME and 
      instr(MTDT_EXT_SCENARIO_1.TABLE_BASE_NAME, '[YYYYMM]') > 0;
      if (v_contador > 0) then
        v_tabla_dinamica := true;
      end if;
      /* Tambien puede aparecer [YYYYMM] en FILTER */
      v_contador:=0;
      select count(*) into v_contador from MTDT_EXT_SCENARIO_1 where 
      trim(MTDT_EXT_SCENARIO_1.TABLE_NAME) = reg_tabla.TABLE_NAME and 
      instr(MTDT_EXT_SCENARIO_1.FILTER, '[YYYYMM]') > 0;
      if (v_contador > 0) then
        v_tabla_dinamica := true;
      end if;

      v_contador:=0;
      select count(*) into v_contador from MTDT_EXT_SCENARIO_1 where
      TRIM(MTDT_EXT_SCENARIO_1.TABLE_NAME) = reg_tabla.TABLE_NAME and
      instr(MTDT_EXT_SCENARIO_1.FILTER, '#FCH_INI#') > 0;
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
      select count(*) into v_contador from MTDT_EXT_SCENARIO_1 where
      TRIM(MTDT_EXT_SCENARIO_1.TABLE_NAME) = reg_tabla.TABLE_NAME and
      instr(MTDT_EXT_SCENARIO_1.FILTER, '#FCH_FIN#') > 0;
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
      open MTDT_SCENARIO (reg_tabla.TABLE_NAME, reg_fuente.SOURCE);
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
      v_num_scenarios := 0;
      open MTDT_SCENARIO (reg_tabla.TABLE_NAME, reg_fuente.SOURCE);
      loop
        fetch MTDT_SCENARIO
        into reg_scenario;
        exit when MTDT_SCENARIO%NOTFOUND;
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
          /* Fin de la inicializacion */
          if (reg_scenario.OVER_PARTION is not null) then
            /* (20160510) Angel Ruiz. Hay clausula OVER PARTITION */
            UTL_FILE.put_line(fich_salida_pkg,'SELECT REGISTRY FROM ( ' || '/* ESCENARIO: ' || reg_scenario.SCENARIO || ' */ \');
          end if;
          if (reg_scenario.HINT is not null) then
            /* (20160421) Angel Ruiz. Miro si se ha incluido un HINT */
            UTL_FILE.put_line(fich_salida_pkg,'SELECT ' || reg_scenario.HINT || '/* ESCENARIO: ' || reg_scenario.SCENARIO || ' */ \');
          elsif (reg_scenario.DISTINCT_COL is not null) then
            UTL_FILE.put_line(fich_salida_pkg,'SELECT DISTINCT ' || '/* ESCENARIO: ' || reg_scenario.SCENARIO || ' */ \');
          else
            UTL_FILE.put_line(fich_salida_pkg,'SELECT ' || '/* ESCENARIO: ' || reg_scenario.SCENARIO || ' */ \');
          end if;
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
            /* (20160414) Angel Ruiz. Miramos si hay alguna tabla dinamica que acabe con */
            /* [YYYYMM] para generar el procedure de manera adecuada */
            if (instr(reg_detail.TABLE_LKUP, '[YYYYMM]') > 0) then
              /* Hay una tabla dinamica. Ponemos el switch a true */
              /* Para posteriormente cuando generamos el Shell script, hacerlo */
              /* de manera adecuada */
              v_tabla_dinamica := true;
            end if;
            columna := genera_campo_select (reg_detail);
            if (primera_col = 1) then
              if (reg_scenario.TYPE = 'S') then
                /* Se trata de un fichero plano con separador */
                /* (20160803) Angel Ruiz. BUG. Si la linea supera los 4000 caracteres da error */
                /* por lo que voy a convertir el primer campos a CLOB */
                if (v_line_size > 4000) then
                  UTL_FILE.put_line(fich_salida_pkg, 'TO_CLOB(');
                end if;
                /* (20160803) Angel Ruiz. FIN BUG */
                case 
                  when reg_detail.TYPE = 'NU' then
                    if (reg_detail.RUL = 'HARDC') then
                      /* Se trata de un valor literal */
                      /* Comprobamos si es un NA# */
                      if (reg_detail.VALUE = 'NA') then
                        UTL_FILE.put_line(fich_salida_pkg, '-1' || ' "' || reg_detail.TABLE_COLUMN || '" \');
                      else
                        UTL_FILE.put_line(fich_salida_pkg, columna || ' "' || reg_detail.TABLE_COLUMN || '" \');
                      end if;
                    else
                      UTL_FILE.put_line(fich_salida_pkg, columna || ' "' || reg_detail.TABLE_COLUMN || '" \');
                    end if;
                  when reg_detail.TYPE = 'FE' then
                    /* Se trata de un valor de tipo fecha */
                    /* (20160907) Angel Ruiz. Cambio TEMPORAL para HUSO HORARIO */
                    if (reg_detail.LONGITUD = 8) then
                      --UTL_FILE.put_line(fich_salida_pkg, '|| CASE WHEN ' || columna || ' IS NULL THEN RPAD('' '',' || reg_detail.LONGITUD ||', '' '') ELSE TO_CHAR(' || columna || ', ''YYYYMMDD'') END' || '          --' || reg_detail.TABLE_COLUMN);
                      UTL_FILE.put_line(fich_salida_pkg, 'TO_CHAR(' || columna || ', ''YYYY-MM-DD'')' || ' "' || reg_detail.TABLE_COLUMN || '" \');
                    else
                      --UTL_FILE.put_line(fich_salida_pkg, '|| CASE WHEN ' || columna || ' IS NULL THEN RPAD('' '',' || reg_detail.LONGITUD ||', '' '') ELSE TO_CHAR(' || columna || ', ''YYYYMMDDHH24MISS'') END' || '          --' || reg_detail.TABLE_COLUMN);
                      UTL_FILE.put_line(fich_salida_pkg, 'TO_CHAR(' || columna || ', ''YYYY-MM-DD HH24:MI:SS'')' || ' "' || reg_detail.TABLE_COLUMN || '" \');
                    end if;
                  else
                    UTL_FILE.put_line(fich_salida_pkg, columna || ' "' || reg_detail.TABLE_COLUMN || '" \');
                end case;
                /* (20160803) Angel Ruiz. BUG. Si la linea supera los 4000 caracteres da error */
                /* por lo que voy a convertir el primer campos a CLOB */
                if (v_line_size > 4000) then
                  UTL_FILE.put_line(fich_salida_pkg, ')');
                end if;
                /* (20160803) Angel Ruiz. FIN BUG */
              else
                /* Se trata de un fichero plano por posicion */
                /* (20160803) Angel Ruiz. BUG. Si la linea supera los 4000 caracteres da error */
                /* por lo que voy a convertir el primer campos a CLOB */
                if (v_line_size > 4000) then
                  UTL_FILE.put_line(fich_salida_pkg, 'TO_CLOB(');
                end if;
                /* (20160803) Angel Ruiz. FIN BUG */
                case 
                  when reg_detail.TYPE = 'AN' then
                    /* Se tarta de un valor de tipo alfanumerico */
                    UTL_FILE.put_line(fich_salida_pkg, 'RPAD(NVL(' || columna || ','' ''), ' || reg_detail.LONGITUD || ', '' '')' || '          --' || reg_detail.TABLE_COLUMN);
                  when reg_detail.TYPE = 'NU' then
                    /* Se trata de un valor de tipo numerico */
                    if (reg_detail.RUL = 'HARDC') then
                      /* Se trata de un valor literal */
                      /* Comprobamos si es un NA# */
                      if (reg_detail.VALUE = 'NA') then
                        UTL_FILE.put_line(fich_salida_pkg, '''-' || lpad('1', reg_detail.LONGITUD -1, '0') || '''' || '          --' || reg_detail.TABLE_COLUMN);
                      else
                        /* (20160803) Angel Ruiz. BUG. Me doy cuenta de que si el literal lleva un signo no funciona */
                        /* He de usar el mismo algoritmo que para los importes */
                        /* en caso de que el numero que se hardcodea sea negativo */
                        if (instr(reg_detail.VALUE, '-') = 0) then
                        /* Si el numero que se hardcodea es positivo */
                          UTL_FILE.put_line(fich_salida_pkg, 'NVL(LPAD(' || columna || ', ' || reg_detail.LONGITUD || ', ''0''), RPAD('' '', ' || reg_detail.LONGITUD || ', '' ''))' || '          --' || reg_detail.TABLE_COLUMN);
                        else
                          /* Si el numero que se hardcodea es negativo */
                          if (instr(reg_detail.LONGITUD, ',') > 0 ) then
                            /* Quiere decir que en la longitud aparecen zona de decimales */
                            /* Preparo la mascara */
                            v_long_total := to_number(substr(reg_detail.LONGITUD, 1, instr(reg_detail.LONGITUD, ',') -1));
                            v_long_parte_decimal := to_number(trim(substr(reg_detail.LONGITUD, instr(reg_detail.LONGITUD, ',') +1)));
                            v_mascara := 'S';
                            for indice in  1..(v_long_total-v_long_parte_decimal-2)
                            loop
                              v_mascara := v_mascara || '0';
                            end loop;
                            v_mascara := v_mascara || '.';
                            for indice in  1..v_long_parte_decimal
                            loop
                              v_mascara := v_mascara || '0';
                            end loop;
                            UTL_FILE.put_line(fich_salida_pkg, 'NVL(TO_CHAR(' || columna || ', ''' || v_mascara || '''), RPAD('' '', ' || to_char(to_number(substr(reg_detail.LONGITUD, 1, instr(reg_detail.LONGITUD, ',') -1))) || ', '' ''))' || '          --' || reg_detail.TABLE_COLUMN);
                          else
                            /* Quiere decir que en la longitud no aparece zona de decimales */
                            v_long_total := to_number (trim(reg_detail.LONGITUD));
                            v_long_parte_decimal := 0;
                            v_mascara := 'S';
                            for indice in  1..(v_long_total-v_long_parte_decimal-1)
                            loop
                              v_mascara := v_mascara || '0';
                            end loop;
                            UTL_FILE.put_line(fich_salida_pkg, 'NVL(TO_CHAR(' || columna || ', ''' || v_mascara || '''), RPAD('' '', ' || reg_detail.LONGITUD || ', '' ''))' || '          --' || reg_detail.TABLE_COLUMN);
                          end if;
                        end if;
                      end if;
                    else
                      UTL_FILE.put_line(fich_salida_pkg, 'NVL(LPAD(' || columna || ', ' || reg_detail.LONGITUD || ', ''0''), RPAD('' '', ' || reg_detail.LONGITUD || ', '' ''))' || '          --' || reg_detail.TABLE_COLUMN);
                    end if;
                  when reg_detail.TYPE = 'IM' then
                    /*(20160503) Angel Ruiz */
                    /* Se trata de un valor de tipo importe */
                    if (instr(reg_detail.LONGITUD, ',') > 0 ) then
                      /* Quiere decir que en la longitud aparecen zona de decimales */
                      /* Preparo la mascara */
                      v_long_total := to_number(substr(reg_detail.LONGITUD, 1, instr(reg_detail.LONGITUD, ',') -1));
                      v_long_parte_decimal := to_number(trim(substr(reg_detail.LONGITUD, instr(reg_detail.LONGITUD, ',') +1)));
                      v_mascara := 'S';
                      for indice in  1..(v_long_total-v_long_parte_decimal-2)
                      loop
                        v_mascara := v_mascara || '0';
                      end loop;
                      v_mascara := v_mascara || '.';
                      for indice in  1..v_long_parte_decimal
                      loop
                        v_mascara := v_mascara || '0';
                      end loop;
                      UTL_FILE.put_line(fich_salida_pkg, 'NVL(TO_CHAR(' || columna || ', ''' || v_mascara || '''), RPAD('' '', ' || to_char(to_number(substr(reg_detail.LONGITUD, 1, instr(reg_detail.LONGITUD, ',') -1))) || ', '' ''))' || '          --' || reg_detail.TABLE_COLUMN);
                    else
                      /* Quiere decir que en la longitud no aparece zona de decimales */
                      v_long_total := to_number (trim(reg_detail.LONGITUD));
                      v_long_parte_decimal := 0;
                      v_mascara := 'S';
                      for indice in  1..(v_long_total-v_long_parte_decimal-1)
                      loop
                        v_mascara := v_mascara || '0';
                      end loop;
                      UTL_FILE.put_line(fich_salida_pkg, 'NVL(TO_CHAR(' || columna || ', ''' || v_mascara || '''), RPAD('' '', ' || reg_detail.LONGITUD || ', '' ''))' || '          --' || reg_detail.TABLE_COLUMN);
                    end if;
                  when reg_detail.TYPE = 'FE' then
                    /* Se trata de un valor de tipo fecha */
                    /* (20160907) Angel Ruiz. Cambio TEMPORAL para HUSO HORARIO */
                    if (reg_detail.RUL <> 'VAR_FCH_INICIO' and (instr(upper(reg_detail.VALUE), 'SYSDATE') = 0)) then
                      /* (20161006) Angel Ruiz. BUG. A los sysdate no se les puede hacer el CAST */
                      columna := 'CAST(FROM_TZ( TO_TIMESTAMP(TO_CHAR(' || columna || ',''YYYYMMDDHH24MISS''),''YYYYMMDDHH24MISS''), ''GMT'') AT TIME ZONE ''America/Mexico_City'' AS DATE)';
                    end if;
                    if (reg_detail.LONGITUD = 8) then
                      --UTL_FILE.put_line(fich_salida_pkg, '|| CASE WHEN ' || columna || ' IS NULL THEN RPAD('' '',' || reg_detail.LONGITUD ||', '' '') ELSE TO_CHAR(' || columna || ', ''YYYYMMDD'') END' || '          --' || reg_detail.TABLE_COLUMN);
                      UTL_FILE.put_line(fich_salida_pkg, 'NVL(TO_CHAR(' || columna || ', ''YYYYMMDD''), RPAD('' '',' || reg_detail.LONGITUD ||', '' ''))' || '          --' || reg_detail.TABLE_COLUMN);
                    else
                      --UTL_FILE.put_line(fich_salida_pkg, '|| CASE WHEN ' || columna || ' IS NULL THEN RPAD('' '',' || reg_detail.LONGITUD ||', '' '') ELSE TO_CHAR(' || columna || ', ''YYYYMMDDHH24MISS'') END' || '          --' || reg_detail.TABLE_COLUMN);
                      UTL_FILE.put_line(fich_salida_pkg, 'NVL(TO_CHAR(' || columna || ', ''YYYYMMDDHH24MISS''), RPAD('' '',' || reg_detail.LONGITUD ||', '' ''))' || '          --' || reg_detail.TABLE_COLUMN);
                    end if;
                  when reg_detail.TYPE = 'TI' then
                    /* Se trata de un valor de tipo TIME HHMISS */
                    UTL_FILE.put_line(fich_salida_pkg, 'RPAD(NVL(' || columna || ', '' ''), ' || reg_detail.LONGITUD || ', '' '')' || '          --' || reg_detail.TABLE_COLUMN);
                end case;
                /* Se trata de un fichero plano por posicion */
                /* (20160803) Angel Ruiz. BUG. Si la linea supera los 4000 caracteres da error */
                /* por lo que voy a convertir el primer campos a CLOB */
                if (v_line_size > 4000) then
                  UTL_FILE.put_line(fich_salida_pkg, ')');
                end if;
                /* (20160803) Angel Ruiz. FIN BUG */
              end if;
              primera_col := 0;
            else /* NO SE TRATA DE LA PRIMERA COLUMNA */
              if (reg_scenario.TYPE = 'S') then
                /* Se trata de un fichero plano con separador */
                case 
                  when reg_detail.TYPE = 'NU' then
                    if (reg_detail.RUL = 'HARDC') then
                      /* Se trata de un valor literal */
                      /* Comprobamos si es un NA# */
                      if (reg_detail.VALUE = 'NA') then
                        UTL_FILE.put_line(fich_salida_pkg, ', ' || '-1' || ' "' || reg_detail.TABLE_COLUMN || '" \');
                      else
                        UTL_FILE.put_line(fich_salida_pkg, ', ' || columna || ' "' || reg_detail.TABLE_COLUMN || '" \');
                      end if;
                    else
                      UTL_FILE.put_line(fich_salida_pkg, ', ' || columna || ' "' || reg_detail.TABLE_COLUMN || '" \');
                    end if;
                  when reg_detail.TYPE = 'FE' then
                    /* Se trata de un valor de tipo fecha */
                    /* (20160907) Angel Ruiz. Cambio TEMPORAL para HUSO HORARIO */
                    if (reg_detail.LONGITUD = 8) then
                      --UTL_FILE.put_line(fich_salida_pkg, '|| CASE WHEN ' || columna || ' IS NULL THEN RPAD('' '',' || reg_detail.LONGITUD ||', '' '') ELSE TO_CHAR(' || columna || ', ''YYYYMMDD'') END' || '          --' || reg_detail.TABLE_COLUMN);
                      UTL_FILE.put_line(fich_salida_pkg, ', ' || 'TO_CHAR(' || columna || ', ''YYYY-MM-DD'')' || ' "' || reg_detail.TABLE_COLUMN || '" \');
                    else
                      --UTL_FILE.put_line(fich_salida_pkg, '|| CASE WHEN ' || columna || ' IS NULL THEN RPAD('' '',' || reg_detail.LONGITUD ||', '' '') ELSE TO_CHAR(' || columna || ', ''YYYYMMDDHH24MISS'') END' || '          --' || reg_detail.TABLE_COLUMN);
                      UTL_FILE.put_line(fich_salida_pkg, ', ' || 'TO_CHAR(' || columna || ', ''YYYY-MM-DD HH24:MI:SS'')' || ' "' || reg_detail.TABLE_COLUMN || '" \');
                    end if;
                  else
                    UTL_FILE.put_line(fich_salida_pkg, ', ' || columna || ' "' || reg_detail.TABLE_COLUMN || '" \');
                end case;
              else    /* Se trata de un fichero plano por posicion */
                case
                  when reg_detail.TYPE = 'AN' then
                    /* Se tarta de un valor de tipo alfanumerico */
                    UTL_FILE.put_line(fich_salida_pkg, '|| RPAD(NVL(' || columna || ', '' ''), ' || reg_detail.LONGITUD || ', '' '')' || '          --' || reg_detail.TABLE_COLUMN);
                  when reg_detail.TYPE = 'NU' then
                    /* Se trata de un valor de tipo numerico */
                    if (reg_detail.RUL = 'HARDC') then
                      /* Se trata de un valor literal */
                      /* Comprobamos si es un NA# */
                      if (reg_detail.VALUE = 'NA') then
                        UTL_FILE.put_line(fich_salida_pkg, '|| ''-' || lpad('1', reg_detail.LONGITUD -1, '0') || '''' || '          --' || reg_detail.TABLE_COLUMN);
                      else
                        /* (20160803) Angel Ruiz. BUG. Si el campo viene con signo negativo no funciona y he de tratarlo */
                        /* con el mismo algoritmo que los importes */
                        if (instr(reg_detail.VALUE, '-') = 0) then
                        /* si el valor que vamos a hardcodear no es negativo */
                          UTL_FILE.put_line(fich_salida_pkg, '|| NVL(LPAD(' || columna || ', ' || reg_detail.LONGITUD || ', ''0''), RPAD('' '', ' || reg_detail.LONGITUD || ', '' ''))' || '          --' || reg_detail.TABLE_COLUMN);
                        else
                        /* si el valor que vamos a hardcodear es negativo */
                          if (instr(reg_detail.LONGITUD, ',') > 0 ) then
                            /* Quiere decir que en la longitud aparecen zona de decimales */
                            /* Preparo la mascara */
                            v_long_total := to_number(substr(reg_detail.LONGITUD, 1, instr(reg_detail.LONGITUD, ',') -1));
                            v_long_parte_decimal := to_number(trim(substr(reg_detail.LONGITUD, instr(reg_detail.LONGITUD, ',') +1)));
                            v_mascara := 'S';
                            for indice in  1..(v_long_total-v_long_parte_decimal-2)
                            loop
                              v_mascara := v_mascara || '0';
                            end loop;
                            v_mascara := v_mascara || '.';
                            for indice in  1..v_long_parte_decimal
                            loop
                              v_mascara := v_mascara || '0';
                            end loop;
                            UTL_FILE.put_line(fich_salida_pkg, '|| NVL(TO_CHAR(' || columna || ', ''' || v_mascara || '''), RPAD('' '', ' || to_char(to_number(substr(reg_detail.LONGITUD, 1, instr(reg_detail.LONGITUD, ',') -1))) || ', '' ''))' || '          --' || reg_detail.TABLE_COLUMN);
                          else
                            /* Quiere decir que en la longitud no aparece zona de decimales */
                            v_long_total := to_number (trim(reg_detail.LONGITUD));
                            v_long_parte_decimal := 0;
                            v_mascara := 'S';
                            for indice in  1..(v_long_total-v_long_parte_decimal-1)
                            loop
                              v_mascara := v_mascara || '0';
                            end loop;
                            UTL_FILE.put_line(fich_salida_pkg, '|| NVL(TO_CHAR(' || columna || ', ''' || v_mascara || '''), RPAD('' '', ' || reg_detail.LONGITUD || ', '' ''))' || '          --' || reg_detail.TABLE_COLUMN);
                          end if;
                        end if;
                        /* (20160803) Angel Ruiz. Fin BUG */                      
                      end if;
                    else
                      UTL_FILE.put_line(fich_salida_pkg, '|| NVL(LPAD(' || columna || ', ' || reg_detail.LONGITUD || ', ''0''), RPAD('' '', ' || reg_detail.LONGITUD || ', '' ''))' || '          --' || reg_detail.TABLE_COLUMN);
                    end if;
                  when reg_detail.TYPE = 'IM' then
                    /* Se trata de un valor de tipo importe */
                    --UTL_FILE.put_line(fich_salida_pkg, '|| CASE WHEN ' || columna || ' IS NULL THEN RPAD('' '',' || reg_detail.LONGITUD || ', '' '') ELSE LPAD(' || columna || ', ' || reg_detail.LONGITUD || ', ''0'') END' || '          --' || reg_detail.TABLE_COLUMN);
                    --if (instr(reg_detail.LONGITUD, ',') > 0 ) then
                    --  /* Quiere decir que en la longitud aparecen zona de decimales */
                    --  UTL_FILE.put_line(fich_salida_pkg, '|| NVL(LPAD(' || columna || ', ' || to_char((to_number(substr(reg_detail.LONGITUD, 1, instr(reg_detail.LONGITUD, ',') -1))+1)) || ', ''0''), RPAD('' '', ' || to_char((to_number(substr(reg_detail.LONGITUD, 1, instr(reg_detail.LONGITUD, ',') -1))+1)) || ', '' ''))' || '          --' || reg_detail.TABLE_COLUMN);
                    --else
                    --  UTL_FILE.put_line(fich_salida_pkg, '|| NVL(LPAD(' || columna || ', ' || reg_detail.LONGITUD || ', ''0''), RPAD('' '', ' || reg_detail.LONGITUD || ', '' ''))' || '          --' || reg_detail.TABLE_COLUMN);
                    --end if;
                    /***************************************/
                    /*(20160503) Angel Ruiz */
                    /* Se trata de un valor de tipo importe */
                    if (instr(reg_detail.LONGITUD, ',') > 0 ) then
                      /* Quiere decir que en la longitud aparecen zona de decimales */
                      /* Preparo la mascara */
                      v_long_total := to_number(substr(reg_detail.LONGITUD, 1, instr(reg_detail.LONGITUD, ',') -1));
                      v_long_parte_decimal := to_number(trim(substr(reg_detail.LONGITUD, instr(reg_detail.LONGITUD, ',') +1)));
                      v_mascara := 'S';
                      for indice in  1..(v_long_total-v_long_parte_decimal-2)
                      loop
                        v_mascara := v_mascara || '0';
                      end loop;
                      v_mascara := v_mascara || '.';
                      for indice in  1..v_long_parte_decimal
                      loop
                        v_mascara := v_mascara || '0';
                      end loop;
                      UTL_FILE.put_line(fich_salida_pkg, '|| NVL(TO_CHAR(' || columna || ', ''' || v_mascara || '''), RPAD('' '', ' || to_char(to_number(substr(reg_detail.LONGITUD, 1, instr(reg_detail.LONGITUD, ',') -1))) || ', '' ''))' || '          --' || reg_detail.TABLE_COLUMN);
                    else
                      /* Quiere decir que en la longitud no aparece zona de decimales */
                      v_long_total := to_number (trim(reg_detail.LONGITUD));
                      v_long_parte_decimal := 0;
                      v_mascara := 'S';
                      for indice in  1..(v_long_total-v_long_parte_decimal-1)
                      loop
                        v_mascara := v_mascara || '0';
                      end loop;
                      UTL_FILE.put_line(fich_salida_pkg, '|| NVL(TO_CHAR(' || columna || ', ''' || v_mascara || '''), RPAD('' '', ' || reg_detail.LONGITUD || ', '' ''))' || '          --' || reg_detail.TABLE_COLUMN);
                    end if;
                  when reg_detail.TYPE = 'FE' then
                    /* Se trata de un valor de tipo fecha */
                    /* (20160907) Angel Ruiz. Cambio TEMPORAL para HUSO HORARIO */
                    if (reg_detail.RUL <> 'VAR_FCH_INICIO' and (instr(upper(reg_detail.VALUE), 'SYSDATE') = 0)) then
                      /* (20161006) Angel Ruiz. BUG. A los sysdate no se les puede hacer el CAST */
                      columna := 'CAST(FROM_TZ( TO_TIMESTAMP(TO_CHAR(' || columna || ',''YYYYMMDDHH24MISS''),''YYYYMMDDHH24MISS''), ''GMT'') AT TIME ZONE ''America/Mexico_City'' AS DATE)';
                    end if;
                    if (reg_detail.LONGITUD = 8) then
                      --UTL_FILE.put_line(fich_salida_pkg, '|| CASE WHEN ' || columna || ' IS NULL THEN RPAD('' '',' || reg_detail.LONGITUD ||', '' '') ELSE TO_CHAR(' || columna || ', ''YYYYMMDD'') END' || '          --' || reg_detail.TABLE_COLUMN);
                      UTL_FILE.put_line(fich_salida_pkg, '|| NVL(TO_CHAR(' || columna || ', ''YYYYMMDD''), RPAD('' '',' || reg_detail.LONGITUD ||', '' ''))' || '          --' || reg_detail.TABLE_COLUMN);
                    else
                      --UTL_FILE.put_line(fich_salida_pkg, '|| CASE WHEN ' || columna || ' IS NULL THEN RPAD('' '',' || reg_detail.LONGITUD ||', '' '') ELSE TO_CHAR(' || columna || ', ''YYYYMMDDHH24MISS'') END' || '          --' || reg_detail.TABLE_COLUMN);
                      UTL_FILE.put_line(fich_salida_pkg, '|| NVL(TO_CHAR(' || columna || ', ''YYYYMMDDHH24MISS''), RPAD('' '',' || reg_detail.LONGITUD ||', '' ''))' || '          --' || reg_detail.TABLE_COLUMN);
                    end if;
                  when reg_detail.TYPE = 'TI' then
                    /* Se trata de un valor de tipo TIME HHMISS */
                    UTL_FILE.put_line(fich_salida_pkg, '|| RPAD(NVL(' || columna || ', '' ''), ' || reg_detail.LONGITUD || ', '' '')' || '          --' || reg_detail.TABLE_COLUMN);
                end case;
              end if;
            end if;
          end loop;
          close MTDT_TC_DETAIL;
          /****/
          /* Fin generacion parte  SELECT (CAMPO1, CAMPO2, CAMPO3, ...) */
          /****/
          /*(20160421) Angel Ruiz. Antes de comenzar a generar el FROM comprobamos si existe */
          /* informacion en el campo OVER_PARTITION. Si existe hay que escribirla como ultimo campo */
          if (reg_scenario.OVER_PARTION is not null) then
            UTL_FILE.put_line(fich_salida_pkg, 'REGISTRY \');
            UTL_FILE.put_line(fich_salida_pkg, ', ' || reg_scenario.OVER_PARTION);
          end if;
          /****/
          /* INICIO generacion parte  FROM (TABLA1, TABLA2, TABLA3, ...) */
          /****/
          dbms_output.put_line ('Despues del SELECT');
          UTL_FILE.put_line(fich_salida_pkg,'    FROM \');
          if (regexp_instr (reg_scenario.TABLE_BASE_NAME,'[Ss][Ee][Ll][Ee][Cc][Tt]') > 0 ) then
          /* (20160719) Angel Ruiz. BUG. Pueden venir QUERIES en TABLE_BASE_NAME */
            UTL_FILE.put_line (fich_salida_pkg, '    '  || cambia_fin_linea(procesa_campo_filter(reg_scenario.TABLE_BASE_NAME)) || ' \');
          else
            if (REGEXP_LIKE(trim(reg_scenario.TABLE_BASE_NAME), '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9]+ +[a-zA-Z0-9_]+$') = true) or
            (REGEXP_LIKE(trim(reg_scenario.TABLE_BASE_NAME), '^[a-zA-Z_0-9#]+\.[a-zA-Z_0-9]+ *') = true) then
              /* Comprobamos si la tabla esta calificada */
              UTL_FILE.put_line (fich_salida_pkg, '    '  || procesa_campo_filter(reg_scenario.TABLE_BASE_NAME) || ' \');
            else
              /* L atabla base no esta calificada, por defecto la calificamos con OWNER_EX */
              /*(20170120) Angel Ruiz. BUG. Calificamos la tabla usando el SOURCE del escenario */
              UTL_FILE.put_line (fich_salida_pkg, '    '  || '#OWNER_' || reg_scenario.SOURCE || '#.' || procesa_campo_filter(reg_scenario.TABLE_BASE_NAME) || ' \');
            end if;
          end if;
          /* (20150109) Angel Ruiz. Anyadimos las tablas necesarias para hacer los LOOK_UP */
          v_hay_look_up:='N';
          /* (20150311) ANGEL RUIZ. se produce un error al generar ya que la tabla de hechos no tiene tablas de LookUp */
          if l_FROM.count > 0 then
            FOR indx IN l_FROM.FIRST .. l_FROM.LAST
            LOOP
              if (regexp_instr(l_FROM(indx), '[Ss][Ee][Ll][Ee][Cc][Tt]') > 0) then
                /* Se trata de una subconsulta */
                UTL_FILE.put_line(fich_salida_pkg, '   ' || cambia_fin_linea(procesa_campo_filter(l_FROM(indx))) || ' \');
              else
                UTL_FILE.put_line(fich_salida_pkg, '   ' || l_FROM(indx) || ' \');
              end if;
              v_hay_look_up := 'Y';
            END LOOP;
          end if;
          /* Siempre va a haber clausula WHERE ya que es necesario siempre poner la clausula WHERE $CONDITIONS*/
          --UTL_FILE.put_line(fich_salida_pkg,'    ' || v_FROM);
          dbms_output.put_line ('Despues del FROM');
          if (reg_scenario.FILTER is not null) then
            /* Procesamos el campo FILTER */
            UTL_FILE.put_line(fich_salida_pkg,'    WHERE $CONDITIONS and \');
            dbms_output.put_line ('Antes de procesar el campo FILTER');
            campo_filter := cambia_fin_linea(procesa_campo_filter (reg_scenario.FILTER));
            if (v_hay_look_up = 'Y') then
              /* (20161102) Angel Ruiz. Si hay lookup habra mas lineas por lo que hay que poner el caracter \ final */
              UTL_FILE.put_line(fich_salida_pkg, campo_filter || ' \');
            else
              /* Si no hay lookup no habra mas lineas por lo que no hay que poner el caracter \ final */
              /* (20170123) Angel Ruiz. BUG. */
              if ((reg_scenario.OVER_PARTION is not null) or (reg_scenario.COLUMNA_GROUP is not null) or (v_hay_sce_COMPUESTO = true and v_num_scenarios < lista_scenarios_presentes.count-1)) then
                UTL_FILE.put_line(fich_salida_pkg, campo_filter || ' \');
              else
                UTL_FILE.put_line(fich_salida_pkg, campo_filter);
              end if;
            end if;
            dbms_output.put_line ('Despues de procesar el campo FILTER');
            if (v_hay_look_up = 'Y') then
            /* (20161102) Angel Ruiz. Hay tablas de LookUp. Hay que poner las condiciones de los Where*/
              dbms_output.put_line ('Entro en el que hay Tablas de LookUp');          
              /* (20150109) Angel Ruiz. Anyadimos las tablas necesarias para hacer los LOOK_UP */
              UTL_FILE.put_line(fich_salida_pkg, '   ' || 'AND \');
              FOR indx IN l_WHERE.FIRST .. l_WHERE.LAST
              LOOP
                if (indx <> l_WHERE.LAST) then
                  UTL_FILE.put_line(fich_salida_pkg, '   ' || l_WHERE(indx) || ' \');
                else
                  if ((reg_scenario.OVER_PARTION is not null) or (reg_scenario.COLUMNA_GROUP is not null) or (v_hay_sce_COMPUESTO = true and  v_num_scenarios < lista_scenarios_presentes.count-1)) then
                    UTL_FILE.put_line(fich_salida_pkg, '   ' || l_WHERE(indx) || ' \');
                  else
                    UTL_FILE.put_line(fich_salida_pkg, '   ' || l_WHERE(indx));
                  end if;  
                end if;
              END LOOP;
              /* FIN */
            end if;
          else
            if (v_hay_look_up = 'Y') then
              UTL_FILE.put_line(fich_salida_pkg,'    WHERE $CONDITIONS and \');
              /* Hay tablas de LookUp. Hay que poner las condiciones de los Where*/
              dbms_output.put_line ('Entro en el que hay Tablas de LookUp');          
              /* (20150109) Angel Ruiz. Anyadimos las tablas necesarias para hacer los LOOK_UP */
              FOR indx IN l_WHERE.FIRST .. l_WHERE.LAST
              LOOP
                if (indx <> l_WHERE.LAST) then
                  UTL_FILE.put_line(fich_salida_pkg, '   ' || l_WHERE(indx) || ' \');
                else
                  /* (20150123) Angel Ruiz. Se trata del ultimo elemento. Hay que comprobar si hay mas clausulas en la query */
                  /* para poner la barra \ al final o no hacerlo */
                  if ((reg_scenario.OVER_PARTION is not null) or (reg_scenario.COLUMNA_GROUP is not null) or (v_hay_sce_COMPUESTO = true and  v_num_scenarios < lista_scenarios_presentes.count-1)) then
                    UTL_FILE.put_line(fich_salida_pkg, '   ' || l_WHERE(indx) || ' \');
                  else
                    UTL_FILE.put_line(fich_salida_pkg, '   ' || l_WHERE(indx));
                  end if;
                end if;
              END LOOP;
              /* FIN */
            else
              /* No tenemos contenido en el el campo FILTER y tampoco tenemos tablas de LookUp */
              if ((reg_scenario.OVER_PARTION is not null) or (reg_scenario.COLUMNA_GROUP is not null) or (v_hay_sce_COMPUESTO = true and v_num_scenarios < lista_scenarios_presentes.count-1)) then
                UTL_FILE.put_line(fich_salida_pkg,'    WHERE $CONDITIONS \');
              else
                UTL_FILE.put_line(fich_salida_pkg,'    WHERE $CONDITIONS');
              end if;
            end if;
          end if;
          /*(20160510) Angel Ruiz. Antes de comenzar a generar el FROM comprobamos si existe */
          /* informacion en el campo OVER_PARTITION. Si existe hay que escribirla como ultimo campo */
          if (reg_scenario.OVER_PARTION is not null) then
            UTL_FILE.put_line(fich_salida_pkg, ') \');
            if (reg_scenario.COLUMNA_GROUP is not null or (v_hay_sce_COMPUESTO = true and  v_num_scenarios < lista_scenarios_presentes.count-1)) then
              /* Si aun queda una clausula mas, la del GROUP BY entonces ponemos la barra \ final */
              UTL_FILE.put_line(fich_salida_pkg, 'WHERE RN = 1 \');
            else
              /* Sino entonces no ponemos la barra \ final */
              UTL_FILE.put_line(fich_salida_pkg, 'WHERE RN = 1');
            end if;
          end if;
          if (reg_scenario.COLUMNA_GROUP is not null) then
          /* (20160907) Angel Ruiz. Implementacion del GROUP BY */
            if (v_hay_sce_COMPUESTO = true and v_num_scenarios < lista_scenarios_presentes.count-1) then
              UTL_FILE.put_line(fich_salida_pkg, 'GROUP BY \');
              UTL_FILE.put_line(fich_salida_pkg, cambia_fin_linea(reg_scenario.COLUMNA_GROUP) || ' \');
            else
              UTL_FILE.put_line(fich_salida_pkg, 'GROUP BY \');
              UTL_FILE.put_line(fich_salida_pkg, cambia_fin_linea(reg_scenario.COLUMNA_GROUP));
            end if;
          end if;
        end if;
        /**************/
        /**************/
        if (reg_scenario.TABLE_TYPE = 'F') then
          /* Se trata de un scenario de tipo F, lo que quiere decir que es el unico */
          /* que existe o es el scenario COMP de un conjunto de scenarios */
          /* por lo que escribimos el punto y coma final de la query */
          --UTL_FILE.put_line (fich_salida_pkg, ';');
          UTL_FILE.put_line (fich_salida_pkg, '');
        else
          /* Se trata de un scenario de tipo C, es decir, es un operando en un conjunto */
          /* de escenarios, asi tenemos que escribir la operacion que une este operando */
          /* al resto */
          if (v_num_scenarios < lista_scenarios_presentes.count -1) then
            /* Ocurre que si no calculamos el numero de escenarios totales menos uno, ya que el */
            /* ultimo escenario no tendra operador */
            UTL_FILE.put_line (fich_salida_pkg, v_lista_elementos_scenario (v_num_scenarios * 2) || ' \');
          end if;
          --UTL_FILE.put_line(fich_salida_pkg, '');
        end if;
        
      end loop;
      close MTDT_SCENARIO;
      UTL_FILE.FCLOSE (fich_salida_pkg);
    end loop;
    close MTDT_FUENTES;
    /*******/
  
    /**************/

    /******/
    /* INICIO DE LA GENERACION DEL sh de CARGA */
    /******/

    /* (20160602) Angel Ruiz. NF: Hay que poner la fuente en el nombre del fichero que se genera */
    v_fuente := '';
    v_interface_name := '';
    for v_fuente_cursor in (
      select source, interface_name, country, type_validation, separator from mtdt_interface_summary
      where trim(MTDT_INTERFACE_SUMMARY.CONCEPT_NAME) = reg_tabla.TABLE_NAME
      and (MTDT_INTERFACE_SUMMARY.STATUS = 'P' or MTDT_INTERFACE_SUMMARY.STATUS = 'D'))
    loop
      v_fuente := trim(v_fuente_cursor.source);
      v_interface_name := trim(v_fuente_cursor.interface_name);
      v_country := trim(v_fuente_cursor.country);
      v_type_validation := trim(v_fuente_cursor.type_validation);
      v_separator := trim(v_fuente_cursor.separator);
    end loop;
    
    
    nombre_interface_a_cargar := v_interface_name;
    pos_ini_pais := instr(v_interface_name, '_XXX_');
    if (pos_ini_pais > 0) then
      pos_fin_pais := pos_ini_pais + length ('_XXX_');
      nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_pais -1) || '_' || v_country || '_' || substr(nombre_interface_a_cargar, pos_fin_pais);
    end if;
    pos_ini_fecha := instr(v_interface_name, '_YYYYMMDD');
    if (pos_ini_fecha > 0) then
      pos_fin_fecha := pos_ini_fecha + length ('_YYYYMMDD');
      nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_fecha -1) || '_${FECHA}' || substr(nombre_interface_a_cargar, pos_fin_fecha);
      nom_inter_a_cargar_sin_fecha := substr(nombre_interface_a_cargar, 1, pos_ini_fecha -1);
    end if;
    /* (20160225) Angel Ruiz */
    pos_ini_hora := instr(nombre_interface_a_cargar, 'HH24MISS');
    if (pos_ini_hora > 0) then
      pos_fin_hora := pos_ini_hora + length ('HH24MISS');
      nombre_interface_a_cargar := substr(nombre_interface_a_cargar, 1, pos_ini_hora -1) || '*' || substr(nombre_interface_a_cargar, pos_fin_hora);
    end if;
    
    UTL_FILE.put_line(fich_salida_load, '#!/bin/bash');
    UTL_FILE.put_line(fich_salida_load, '#############################################################################');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Telefonica Moviles Mexico SA DE CV                                        #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    /* (20160817) Angel Ruiz. Cambio temporal para adecuarse a la entrega de produccion*/
    UTL_FILE.put_line(fich_salida_load, '# Archivo    :       ' || REQ_NUMBER || '_' ||  reg_tabla.TABLE_NAME || '.sh           #');
    --UTL_FILE.put_line(fich_salida_load, '# Archivo    :       ' || 'ONIX' || '_' ||  reg_tabla.TABLE_NAME || '.sh           #');
    /* (20160817) Angel Ruiz FIN Cambio temporal para adecuarse a la entrega de produccion*/
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Autor      : <SYNAPSYS>                                                   #');
    UTL_FILE.put_line(fich_salida_load, '# Proposito  : Realizar la extraccion de ' ||  reg_tabla.TABLE_NAME || ' de '|| NAME_DM || '  #');
    UTL_FILE.put_line(fich_salida_load, '# Parametros :                                                              #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Ejecucion  : DIARIO                                                       #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Historia : 02-Noviembre-2016 -> Creacion                                    #');
    UTL_FILE.put_line(fich_salida_load, '# Caja de Control - M : APLICACION:                                         #');
    UTL_FILE.put_line(fich_salida_load, '#                                   GRUPO:                                  #');
    UTL_FILE.put_line(fich_salida_load, '#                                   JOB:                                    #');
    UTL_FILE.put_line(fich_salida_load, '#                                   SCRIPT:                                 #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Caducidad del Requerimiento : Sin caducidad                               #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# Dependencias : NA                                                         #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '# HOST INSTALACION:                                                         #');   
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '#                                                                           #');
    UTL_FILE.put_line(fich_salida_load, '#############################################################################');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '# SE INSERTA EN EL METADATO FIN NO OK                                          #');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');    
    UTL_FILE.put_line(fich_salida_load, 'InsertaFinFallido()');
    UTL_FILE.put_line(fich_salida_load, '{');
    --UTL_FILE.put_line(fich_salida_load, 'beeline -u ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} -n ${BD_USER_HIVE} -p ${BD_CLAVE_HIVE} -e "\');
    UTL_FILE.put_line(fich_salida_load, 'beeline << EOF >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log 2>&' || '1');
    UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
    UTL_FILE.put_line(fich_salida_load, 'INSERT INTO ${ESQUEMA_MT}.MTDT_MONITOREO');
    UTL_FILE.put_line(fich_salida_load, 'SELECT');
    --UTL_FILE.put_line(fich_salida_load, '  mtdt_proceso.cve_proceso*' || v_MULTIPLICADOR_PROC || '+${ULT_PASO_EJECUTADO}+unix_timestamp(),');  /* mtdt_proceso.cve_proceso*v_MULTIPLICADOR_PROC+mtdt_paso.cve_paso+unix_timestamp() */
    UTL_FILE.put_line(fich_salida_load, '  mtdt_proceso.cve_proceso,');
    UTL_FILE.put_line(fich_salida_load, '  1,');
    UTL_FILE.put_line(fich_salida_load, '  1,');
    UTL_FILE.put_line(fich_salida_load, '  ''${INICIO_PASO_TMR}'',');
    UTL_FILE.put_line(fich_salida_load, '  current_timestamp(),');
    UTL_FILE.put_line(fich_salida_load, '  ''${FECHA_FMT_HIVE}'',');
    UTL_FILE.put_line(fich_salida_load, '  ''${FECHA_FMT_HIVE}'',');
    UTL_FILE.put_line(fich_salida_load, '  ${B_CONTEO_BD},');  /* numero de inserts */
    UTL_FILE.put_line(fich_salida_load, '  0,'); /* numero de updates */
    UTL_FILE.put_line(fich_salida_load, '  0,'); /* numero de deletes */
    UTL_FILE.put_line(fich_salida_load, '  ${CONTEO_ARCHIVO},'); /* numero de reads */
    UTL_FILE.put_line(fich_salida_load, '  0,'); /* numero de discards */
    UTL_FILE.put_line(fich_salida_load, '  ''${BAN_FORZADO}'','); /* BAN_FORZADO */
    UTL_FILE.put_line(fich_salida_load, '  ''${INICIO_PASO_TMR}''');
    UTL_FILE.put_line(fich_salida_load, 'FROM');
    UTL_FILE.put_line(fich_salida_load, '${ESQUEMA_MT}.MTDT_PROCESO');
    UTL_FILE.put_line(fich_salida_load, 'WHERE');
    --UTL_FILE.put_line(fich_salida_sh, '  MTDT_PROCESO.NOMBRE_PROCESO = ''load_SA_' || reg_summary.CONCEPT_NAME || '.sh'';"');
    UTL_FILE.put_line(fich_salida_load, 'MTDT_PROCESO.NOMBRE_PROCESO = ''' || REQ_NUMBER || '_'|| reg_tabla.TABLE_NAME || '.sh'';');
    UTL_FILE.put_line(fich_salida_load, '!quit');
    UTL_FILE.put_line(fich_salida_load, 'EOF');
    UTL_FILE.put_line(fich_salida_load, 'ERROR=`grep -ic -e ''Error: Could not open client transport'' -e ''Error: Error while'' -e ''java.lang.RuntimeException'' ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log`');    
    UTL_FILE.put_line(fich_salida_load, 'if [ ${ERROR} -ne 0 ] ; then');
    UTL_FILE.put_line(fich_salida_load, '  SUBJECT="${REQ_NUM}: ERROR: Al insertar en el metadato Fin Fallido."');
    UTL_FILE.put_line(fich_salida_load, '  echo "Surgio un error al insertar en el metadato que le proceso no ha terminado OK." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
    UTL_FILE.put_line(fich_salida_load, '  echo `date`');
    UTL_FILE.put_line(fich_salida_load, '  exit 1');
    UTL_FILE.put_line(fich_salida_load, 'fi');
    UTL_FILE.put_line(fich_salida_load, 'return 0');
    UTL_FILE.put_line(fich_salida_load, '}');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '# SE INSERTA EN EL METADATO FIN OK                                             #');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');    
    UTL_FILE.put_line(fich_salida_load, 'InsertaFinOK()');
    UTL_FILE.put_line(fich_salida_load, '{');
    --UTL_FILE.put_line(fich_salida_load, 'beeline -u ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} -n ${BD_USER_HIVE} -p ${BD_CLAVE_HIVE} -e "\');
    UTL_FILE.put_line(fich_salida_load, 'beeline << EOF >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log 2>&' || '1');
    UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
    UTL_FILE.put_line(fich_salida_load, 'INSERT INTO ${ESQUEMA_MT}.MTDT_MONITOREO');
    UTL_FILE.put_line(fich_salida_load, 'SELECT');
    --UTL_FILE.put_line(fich_salida_load, '  mtdt_proceso.cve_proceso*' || v_MULTIPLICADOR_PROC || '+${ULT_PASO_EJECUTADO}+unix_timestamp(),');  /* mtdt_proceso.cve_proceso*v_MULTIPLICADOR_PROC+mtdt_paso.cve_paso+unix_timestamp() */ 
    UTL_FILE.put_line(fich_salida_load, '  mtdt_proceso.cve_proceso,');
    UTL_FILE.put_line(fich_salida_load, '  1,');
    UTL_FILE.put_line(fich_salida_load, '  0,');
    UTL_FILE.put_line(fich_salida_load, '  ''${INICIO_PASO_TMR}'',');
    UTL_FILE.put_line(fich_salida_load, '  current_timestamp(),');
    UTL_FILE.put_line(fich_salida_load, '  ''${FECHA_FMT_HIVE}'',');
    UTL_FILE.put_line(fich_salida_load, '  ''${FECHA_FMT_HIVE}'',');
    UTL_FILE.put_line(fich_salida_load, '  ${B_CONTEO_BD},');  /* numero de inserts */
    UTL_FILE.put_line(fich_salida_load, '  0,'); /* numero de updates */
    UTL_FILE.put_line(fich_salida_load, '  0,'); /* numero de deletes */
    UTL_FILE.put_line(fich_salida_load, '  ${CONTEO_ARCHIVO},'); /* numero de reads */
    UTL_FILE.put_line(fich_salida_load, '  0,'); /* numero de discards */
    UTL_FILE.put_line(fich_salida_load, '  ''${BAN_FORZADO}'','); /* BAN_FORZADO */
    UTL_FILE.put_line(fich_salida_load, '  ''${INICIO_PASO_TMR}''');
    UTL_FILE.put_line(fich_salida_load, 'FROM');
    UTL_FILE.put_line(fich_salida_load, '${ESQUEMA_MT}.MTDT_PROCESO');
    UTL_FILE.put_line(fich_salida_load, 'WHERE');
    UTL_FILE.put_line(fich_salida_load, 'MTDT_PROCESO.NOMBRE_PROCESO = ''' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '.sh'';');
    UTL_FILE.put_line(fich_salida_load, '!quit');
    UTL_FILE.put_line(fich_salida_load, 'EOF');
    UTL_FILE.put_line(fich_salida_load, 'ERROR=`grep -ic -e ''Error: Could not open client transport'' -e ''Error: Error while'' -e ''java.lang.RuntimeException'' ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log`');    
    UTL_FILE.put_line(fich_salida_load, 'if [ ${ERROR} -ne 0 ] ; then');
    UTL_FILE.put_line(fich_salida_load, '  SUBJECT="${REQ_NUM}: ERROR: Al insertar en el metadato Fin OK."');
    UTL_FILE.put_line(fich_salida_load, '  echo "Surgio un error al insertar en el metadato que le proceso ha terminado OK." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
    UTL_FILE.put_line(fich_salida_load, '  echo `date`');
    UTL_FILE.put_line(fich_salida_load, '  exit 1');
    UTL_FILE.put_line(fich_salida_load, 'fi');
    UTL_FILE.put_line(fich_salida_load, 'return 0');
    UTL_FILE.put_line(fich_salida_load, '}');
    UTL_FILE.put_line(fich_salida_load, '');
    --if (v_type_validation <> 'I') then
      /* (20161103) Angel Ruiz. Si se trata de una validacion que no es de integracion, por lo que se generan ficheros planos */
      /* que hay que validar */
      --UTL_FILE.put_line(fich_salida_load, '################################################################################');
      --UTL_FILE.put_line(fich_salida_load, '# SE REALIZA LA VALIDACION DE LOS ARCHIVOS                                     #');
      --UTL_FILE.put_line(fich_salida_load, '################################################################################');
      --UTL_FILE.put_line(fich_salida_load, 'ValidaInformacionArchivo ()');
      --UTL_FILE.put_line(fich_salida_load, '{');
      --UTL_FILE.put_line(fich_salida_load, '  REPORTE=$1');
      --UTL_FILE.put_line(fich_salida_load, '  ChkReporteHadoop ${REPORTE}  0');
      --UTL_FILE.put_line(fich_salida_load, '  CodErr=$?');
      --UTL_FILE.put_line(fich_salida_load, '  echo "$1 ${REPORTE} $?"');
      --UTL_FILE.put_line(fich_salida_load, '  echo "Codigo error: $CodErr"');
      --UTL_FILE.put_line(fich_salida_load, '  if [ $CodErr -ne 0 ]');
      --UTL_FILE.put_line(fich_salida_load, '  then');
      --UTL_FILE.put_line(fich_salida_load, '    if [ $CodErr -eq 2 ]');
      --UTL_FILE.put_line(fich_salida_load, '    then');
      --UTL_FILE.put_line(fich_salida_load, '      SUBJECT="${REQ_NUM}: Error al generar el reporte ${REPORTE}, [No se genero el archivo .txt]"');
      --UTL_FILE.put_line(fich_salida_load, '      echo "El reporte: ${REPORTE}, no se gener en el servidor." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
      --UTL_FILE.put_line(fich_salida_load, '      InsertaFinFallido');
      --UTL_FILE.put_line(fich_salida_load, '      exit 2');
      --UTL_FILE.put_line(fich_salida_load, '    elif [ $CodErr -eq 3 ]');
      --UTL_FILE.put_line(fich_salida_load, '    then');
      --UTL_FILE.put_line(fich_salida_load, '      SUBJECT="${REQ_NUM}: Error al generar el reporte ${REPORTE}, [archivo .txt Vacio]"');
      --UTL_FILE.put_line(fich_salida_load, '      echo "El reporte: ${REPORTE}, no tiene datos." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
      --UTL_FILE.put_line(fich_salida_load, '      InsertaFinFallido');
      --UTL_FILE.put_line(fich_salida_load, '      exit 3');
      --UTL_FILE.put_line(fich_salida_load, '    elif [ $CodErr -eq 4 ]');
      --UTL_FILE.put_line(fich_salida_load, '    then');
      --UTL_FILE.put_line(fich_salida_load, '      SUBJECT="${REQ_NUM}: Error al generar el reporte ${REPORTE}, [Error de Hive]"');
      --UTL_FILE.put_line(fich_salida_load, '      echo "El reporte: ${REPORTE}, contiene errores de hive." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
      --UTL_FILE.put_line(fich_salida_load, '      InsertaFinFallido');
      --UTL_FILE.put_line(fich_salida_load, '      exit 4');
      --UTL_FILE.put_line(fich_salida_load, '    else');
      --UTL_FILE.put_line(fich_salida_load, '      SUBJECT="${REQ_NUM}: Error al generar el reporte ${REPORTE}, [Error en archivo .txt]"');
      --UTL_FILE.put_line(fich_salida_load, '      echo "El reporte: ${REPORTE}, contiene errores." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
      --UTL_FILE.put_line(fich_salida_load, '      InsertaFinFallido');
      --UTL_FILE.put_line(fich_salida_load, '      exit 1');
      --UTL_FILE.put_line(fich_salida_load, '    fi');
      --UTL_FILE.put_line(fich_salida_load, '  fi');
      --UTL_FILE.put_line(fich_salida_load, '  return 0');
      --UTL_FILE.put_line(fich_salida_load, '}');
    --end if;
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '#Obtiene los password de base de datos                                         #');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, 'ObtenContrasena()');
    UTL_FILE.put_line(fich_salida_load, '{');
    UTL_FILE.put_line(fich_salida_load, '  # Obtenemos el password de la BD');
    UTL_FILE.put_line(fich_salida_load, '  TraeUserExt $1');
    UTL_FILE.put_line(fich_salida_load, '  TraePassExt $1 ${BD_USR_EXT}');
    UTL_FILE.put_line(fich_salida_load, '  TraeCadConexExt $1 ${BD_USR_EXT}');
    UTL_FILE.put_line(fich_salida_load, '  TraeOwnerExt $1 ${BD_USR_EXT}');
    UTL_FILE.put_line(fich_salida_load, '  if [ "${BD_USR_EXT}" = "" ] ; then');
    UTL_FILE.put_line(fich_salida_load, '    SUBJECT="Error BD ${REQ_NUM} (`date +%d/%m/%Y`)"');
    UTL_FILE.put_line(fich_salida_load, '    echo "Error no se pudo obtener el usuario para el sistema fuente $1" >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_load, '    echo `date` >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    --UTL_FILE.put_line(fich_salida_load, '    InsertaFinFallido');
    UTL_FILE.put_line(fich_salida_load, '    exit 1;');
    UTL_FILE.put_line(fich_salida_load, '  fi');
    UTL_FILE.put_line(fich_salida_load, '  if [ "${PASSWORD_EXT}" = "" ] ; then');
    UTL_FILE.put_line(fich_salida_load, '    SUBJECT="Error BD ${REQ_NUM} (`date +%d/%m/%Y`)"');
    UTL_FILE.put_line(fich_salida_load, '    echo "Error no se pudo obtener la password para el sistema fuente $1 y el usuario ${BD_USR}" >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_load, '    echo `date` >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    --UTL_FILE.put_line(fich_salida_load, '    InsertaFinFallido');
    UTL_FILE.put_line(fich_salida_load, '    exit 1;');
    UTL_FILE.put_line(fich_salida_load, '  fi');
    UTL_FILE.put_line(fich_salida_load, '  if [ "${CAD_CONEX_EXT}" = "" ] ; then');
    UTL_FILE.put_line(fich_salida_load, '    SUBJECT="Error BD ${REQ_NUM} (`date +%d/%m/%Y`)"');
    UTL_FILE.put_line(fich_salida_load, '    echo "Error no se pudo obtener la cadena de conexion para el sistema fuente $1" >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_load, '    echo `date` >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    --UTL_FILE.put_line(fich_salida_load, '    InsertaFinFallido');
    UTL_FILE.put_line(fich_salida_load, '    exit 1;');
    UTL_FILE.put_line(fich_salida_load, '  fi');
    UTL_FILE.put_line(fich_salida_load, '  if [ "${OWNER_EXT}" = "" ] ; then');
    UTL_FILE.put_line(fich_salida_load, '    SUBJECT="Error BD ${REQ_NUM} (`date +%d/%m/%Y`)"');
    UTL_FILE.put_line(fich_salida_load, '    echo "Error no se pudo obtener el propietario para el sistema fuente $1" >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_load, '    echo `date` >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    --UTL_FILE.put_line(fich_salida_load, '    InsertaFinFallido');
    UTL_FILE.put_line(fich_salida_load, '    exit 1;');
    UTL_FILE.put_line(fich_salida_load, '  fi');
    
    --UTL_FILE.put_line(fich_salida_load, '  # Validamos la conexion a la base de datos');
    --UTL_FILE.put_line(fich_salida_load, '  ChkConexion $1 $2 ${PASSWORD}');
    --UTL_FILE.put_line(fich_salida_load, '  if [ $? -ne 0 ]; then');
    --UTL_FILE.put_line(fich_salida_load, '    SUBJECT="ERROR: No se pudo establecer la conexion, ${REQ_NUM} (`date +%d/%m/%Y`)"');
    --UTL_FILE.put_line(fich_salida_load, '    echo "No se pudo conectar  a la BD: $1, USER=$2, PASSWORD=${PASSWORD}." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
    --UTL_FILE.put_line(fich_salida_load, '    echo `date`');
    --UTL_FILE.put_line(fich_salida_load, '    InsertaFinFallido');
    --UTL_FILE.put_line(fich_salida_load, '    exit 1');
    --UTL_FILE.put_line(fich_salida_load, '  fi');
    UTL_FILE.put_line(fich_salida_load, '  BD_PWD="${PASSWORD_EXT}"');
    UTL_FILE.put_line(fich_salida_load, '  TraePass HIVE ${BD_USER_HIVE}');
    UTL_FILE.put_line(fich_salida_load, '  if [ "${PASSWORD}" = "" ] ; then');
    UTL_FILE.put_line(fich_salida_load, '    SUBJECT="Error BD ${REQ_NUM} (`date +%d/%m/%Y`)"');
    UTL_FILE.put_line(fich_salida_load, '    echo "Error no se pudo obtener la password para HIVE y el usuario ${BD_USR}" >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_load, '    echo `date` >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    --UTL_FILE.put_line(fich_salida_load, '    InsertaFinFallido');
    UTL_FILE.put_line(fich_salida_load, '    exit 1;');
    UTL_FILE.put_line(fich_salida_load, '  fi');
    UTL_FILE.put_line(fich_salida_load, '  BD_CLAVE_HIVE="${PASSWORD}"');
    UTL_FILE.put_line(fich_salida_load, '  return 0');
    UTL_FILE.put_line(fich_salida_load, '}');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '# SE OBTIENE LA FECHA                                                          #');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, 'ObtieneFecha()');
    UTL_FILE.put_line(fich_salida_load, '{');
    UTL_FILE.put_line(fich_salida_load, '  if [ $# = 0 ] ; then');
    /* (20170331) Angel Ruiz. Compruebo la frecuencia con que va a ser vargado */
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
      UTL_FILE.put_line(fich_salida_load, 'select date_format(current_date, ''yyyyMMdd'') from ${ESQUEMA_MT}.dual;');
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
      UTL_FILE.put_line(fich_salida_load, '    FECHA_FMT_HIVE=`echo ${FECHA} | awk ''{ printf "%s-%s-%s", substr($1,0,4), substr($1,5,2), substr($1,7,2) ; }''`');    
      if (v_fecha_ini_param = true and v_fecha_fin_param = true) then
        /* (20160419) Angel Ruiz. Si tenemos parametros de FCH_INI y FCH_FIN tenmos que generar codigo para recuperar la fecha Inicial */
        --UTL_FILE.put_line(fich_salida_load, '    FECHA_FIN=${FECHA}');
        UTL_FILE.put_line(fich_salida_load, '    FECHA_FIN_PREV=`beeline --silent=true --showHeader=false --outputformat=dsv << EOF');
        UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
        UTL_FILE.put_line(fich_salida_load, 'select date_format(current_date, ''yyyyMMdd'') from ${ESQUEMA_MT}.dual;');
        UTL_FILE.put_line(fich_salida_load, '!quit');
        UTL_FILE.put_line(fich_salida_load, 'EOF`');
        UTL_FILE.put_line(fich_salida_load, '    if [ $? -ne 0 ]; then');
        UTL_FILE.put_line(fich_salida_load, '      SUBJECT="${REQ_NUM}: ERROR: Al obtener la fecha."');
        UTL_FILE.put_line(fich_salida_load, '      echo "Surgio un error al obtener la fecha del sistema o el parametro no es un formato de fecha YYYYMMDD." >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
        UTL_FILE.put_line(fich_salida_load, '      echo `date` >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
        UTL_FILE.put_line(fich_salida_load, '      InsertaFinFallido');
        UTL_FILE.put_line(fich_salida_load, '      exit 1');
        UTL_FILE.put_line(fich_salida_load, '    fi');
        UTL_FILE.put_line(fich_salida_load, '    FECHA_FIN=`echo ${FECHA_FIN_PREV} | sed -e ''s/\n//g'' -e ''s/\r//g'' -e ''s/^[ ]*//g'' -e ''s/[ ]*$//g''`');
      end if;
    else  /* if (v_frecuency = "D") then */
      /* Se trata de una extraccion mensual */
      UTL_FILE.put_line(fich_salida_load, '    # Se obtiene la fecha inicial y final del periodo a calcular a partir de la fecha del sistema.');
      --UTL_FILE.put_line(fich_salida_load, '    FECHA=`beeline -u ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} -n ${BD_USER_HIVE} -p ${BD_CLAVE_HIVE} --silent=true --showHeader=false --outputformat=dsv -e "select date_format(current_date, ''yyyyMMdd'') from ${ESQUEMA_MT}.dual;"`');
      UTL_FILE.put_line(fich_salida_load, '    FECHA_PREV=`beeline --silent=true --showHeader=false --outputformat=dsv << EOF');
      UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
      UTL_FILE.put_line(fich_salida_load, 'select date_format(current_date, ''yyyyMM01'') from ${ESQUEMA_MT}.dual;');
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
      UTL_FILE.put_line(fich_salida_load, '    FECHA_FMT_HIVE=`echo ${FECHA} | awk ''{ printf "%s-%s-%s", substr($1,0,4), substr($1,5,2), substr($1,7,2) ; }''`');    
      if (v_fecha_ini_param = true and v_fecha_fin_param = true) then
        /* (20160419) Angel Ruiz. Si tenemos parametros de FCH_INI y FCH_FIN tenmos que generar codigo para recuperar la fecha Inicial */
        --UTL_FILE.put_line(fich_salida_load, '    FECHA_FIN=${FECHA}');
        UTL_FILE.put_line(fich_salida_load, '    FECHA_FIN_PREV=`beeline --silent=true --showHeader=false --outputformat=dsv << EOF');
        UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
        UTL_FILE.put_line(fich_salida_load, 'select date_format(LAST_DAY(current_date),''yyyyMMdd'') from ${ESQUEMA_MT}.dual;');
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
    end if; /* if (v_frecuency = "D") then */

    UTL_FILE.put_line(fich_salida_load, '  else');
    if (v_frequency <> 'M') then
      /* Si se trata de valores de fecuenciua D (diaria) o E (eventual) */
      UTL_FILE.put_line(fich_salida_load, '    # Se obtiene la fecha inicial y final del periodo a calcular a partir de la fecha proporcionada como parametro.');
      UTL_FILE.put_line(fich_salida_load, '    FECHA_FMT_HIVE=`echo $1 | awk ''{ printf "%s-%s-%s", substr($1,0,4), substr($1,5,2), substr($1,7,2) ; }''`');
      --UTL_FILE.put_line(fich_salida_load, '    FECHA=`beeline -u ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} -n ${BD_USER_HIVE} -p ${BD_CLAVE_HIVE} --silent=true --showHeader=false --outputformat=dsv -e "select date_format(''${FECHA_FMT_HIVE}'', ''yyyyMMdd'') from ${ESQUEMA_MT}.dual;"`');
      UTL_FILE.put_line(fich_salida_load, '    FECHA_PREV=`beeline --silent=true --showHeader=false --outputformat=dsv << EOF');
      UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
      UTL_FILE.put_line(fich_salida_load, 'select date_format(cast(''${FECHA_FMT_HIVE}'' as date), ''yyyyMMdd'') from ${ESQUEMA_MT}.dual;');
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
    else  /* if (v_frecuency <> "M") then */
      /* Se trata de una frecuencia MENSUAL */
      UTL_FILE.put_line(fich_salida_load, '    # Se obtiene la fecha inicial y final del periodo a calcular a partir de la fecha proporcionada como parametro.');
      UTL_FILE.put_line(fich_salida_load, '    FECHA_FMT_HIVE=`echo $1 | awk ''{ printf "%s-%s-%s", substr($1,0,4), substr($1,5,2), substr($1,7,2) ; }''`');
      --UTL_FILE.put_line(fich_salida_load, '    FECHA=`beeline -u ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} -n ${BD_USER_HIVE} -p ${BD_CLAVE_HIVE} --silent=true --showHeader=false --outputformat=dsv -e "select date_format(''${FECHA_FMT_HIVE}'', ''yyyyMMdd'') from ${ESQUEMA_MT}.dual;"`');
      UTL_FILE.put_line(fich_salida_load, '    FECHA_PREV=`beeline --silent=true --showHeader=false --outputformat=dsv << EOF');
      UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
      UTL_FILE.put_line(fich_salida_load, 'select date_format(cast(''${FECHA_FMT_HIVE}'' as date), ''yyyyMM01'') from ${ESQUEMA_MT}.dual;');
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
      if (v_fecha_ini_param = true and v_fecha_fin_param = true) then
        /* (20160419) Angel Ruiz. Si tenemos parametros de FCH_INI y FCH_FIN tenmos que generar codigo para recuperar la fecha Inicial */
        --UTL_FILE.put_line(fich_salida_load, '    FECHA_FIN=${FECHA}');
        UTL_FILE.put_line(fich_salida_load, '    FECHA_FIN_PREV=`beeline --silent=true --showHeader=false --outputformat=dsv << EOF');
        UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
        UTL_FILE.put_line(fich_salida_load, 'select date_format(LAST_DAY(cast(''${FECHA_FMT_HIVE}'' as date)), ''yyyyMMdd'') from ${ESQUEMA_MT}.dual;');
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
    if (v_tabla_dinamica = true) then
      /* El interfaz tiene una tabla dinamica, por lo que hay que obtener la fecha YYYYMM */
      UTL_FILE.put_line(fich_salida_load,'  FECHA_MES=`echo ${FECHA} | awk ''{ printf "%s", substr($1,0,6) ; }''`');
    end if;
    UTL_FILE.put_line(fich_salida_load, '  return 0');
    UTL_FILE.put_line(fich_salida_load, '}');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '# SE OBTIENE LA FECHA Y HORA EN LA QUE SE INICIA EL PROCESO                    #');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, 'ObtieneFechaHora()');
    UTL_FILE.put_line(fich_salida_load, '{');
    --UTL_FILE.put_line(fich_salida_load, '  INICIO_PASO_TMR=`beeline -u ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} -n ${BD_USER_HIVE} -p ${BD_CLAVE_HIVE} --silent=true --showHeader=false --outputformat=dsv -e "select current_timestamp from ${ESQUEMA_MT}.dual;"`');
    UTL_FILE.put_line(fich_salida_load, '  INICIO_PASO_TMR_PREV=`beeline --silent=true --showHeader=false --outputformat=dsv << EOF');
    UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
    UTL_FILE.put_line(fich_salida_load, 'select current_timestamp from ${ESQUEMA_MT}.dual;');
    UTL_FILE.put_line(fich_salida_load, '!quit');
    UTL_FILE.put_line(fich_salida_load, 'EOF`');
    UTL_FILE.put_line(fich_salida_load, 'if [ $? -ne 0 ]; then');
    UTL_FILE.put_line(fich_salida_load, '  SUBJECT="${REQ_NUM}: ERROR: Al obtener la fecha y hora del sistema."');
    UTL_FILE.put_line(fich_salida_load, '  echo "Surgio un error al obtener la fecha y hora del sistema." >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_load, '  echo `date` >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_load, '  InsertaFinFallido');    
    UTL_FILE.put_line(fich_salida_load, '  exit 1');
    UTL_FILE.put_line(fich_salida_load, 'fi');
    --UTL_FILE.put_line(fich_salida_load, 'INICIO_PASO_TMR=`echo ${INICIO_PASO_TMR_PREV} | sed -e ''s/ //g'' -e ''s/\n//g'' -e ''s/\r//g''`');    
    UTL_FILE.put_line(fich_salida_load, 'INICIO_PASO_TMR=`echo ${INICIO_PASO_TMR_PREV} | sed -e ''s/\n//g'' -e ''s/\r//g'' -e ''s/^[ ]*//g'' -e ''s/[ ]*$//g''`');    
    UTL_FILE.put_line(fich_salida_load, 'return 0');
    UTL_FILE.put_line(fich_salida_load, '}');
    if (v_type_validation = 'I') then
      /* (20160620) Angel Ruiz. NF: Implemento la Validacion tipo I donde los datos extraidos van directamente */
      /* a las tablas de STAGING. Por claridad cuando esto sucede le cambio el nombre al procedure que lo hace */
      UTL_FILE.put_line(fich_salida_load, '################################################################################');
      UTL_FILE.put_line(fich_salida_load, '# SE CARGA LA TABLA DE STAGING                                                 #');
      UTL_FILE.put_line(fich_salida_load, '################################################################################');
    else
      UTL_FILE.put_line(fich_salida_load, '################################################################################');
      UTL_FILE.put_line(fich_salida_load, '# SE OBTIENE LA INTERFAZ                                                       #');
      UTL_FILE.put_line(fich_salida_load, '################################################################################');
    end if;
    if (v_type_validation = 'I') then
      /* (20160620) Angel Ruiz. NF: Implemento la Validacion tipo I donde los datos extraidos van directamente */
      /* a las tablas de STAGING. Por claridad cuando esto sucede le cambio el nombre al procedure que lo hace */
      UTL_FILE.put_line(fich_salida_load, 'CargaDirectaTablaStaging()');
    else
      UTL_FILE.put_line(fich_salida_load, 'ObtenInterfaz()');
    end if;
    UTL_FILE.put_line(fich_salida_load, '{');
    --UTL_FILE.put_line(fich_salida_load, '  ARCHIVO_SALIDA="${NOM_INTERFAZ}_${FECHA}"');
    if (v_type_validation <> 'I') then
      /* (20160620) Angel Ruiz. NF: Implemento la Validacion tipo I donde los datos extraidos van directamente */
      /* a las tablas de STAGING. No es necesario declarar el fichero de salida cuando estamos llevando los datos directamente a las tablas de Staging */
      UTL_FILE.put_line(fich_salida_load, '  ARCHIVO_SALIDA="' || nombre_interface_a_cargar || '"');
    end if;
    /********************/
    /* (20170418) Angel Ruiz. NF: DISTINTOS ORIGENES PARA UN MISMO INTERFAZ */
    /********************/
    open MTDT_FUENTES(reg_tabla.TABLE_NAME);
    loop
      fetch MTDT_FUENTES
      into reg_fuente;
      exit when MTDT_FUENTES%NOTFOUND;
      UTL_FILE.put_line(fich_salida_load, '  ObtenContrasena ${SIST_ORIGEN' || '_' || reg_fuente.SOURCE || '}');
      /* (20160817) Angel Ruiz. Cambio temporal para adecuarse a la entrega de produccion*/
      /* (20170428) Angel Ruiz. NF: Interfaz de fuentes diferentes */
      if (v_numero_fuentes > 1) then
        UTL_FILE.put_line(fich_salida_load, '  ARCHIVO_SQL="${REQ_NUM}_' || reg_tabla.TABLE_NAME || '_' || reg_fuente.SOURCE || '.sql"');
      else
        UTL_FILE.put_line(fich_salida_load, '  ARCHIVO_SQL="${REQ_NUM}_' || reg_tabla.TABLE_NAME || '.sql"');
      end if;
      --UTL_FILE.put_line(fich_salida_load, '  ARCHIVO_SQL="ONIX_' || reg_tabla.TABLE_NAME || '.sql"');
      /* (20160817) Angel Ruiz FIN Cambio temporal para adecuarse a la entrega de produccion*/
      if (v_tabla_dinamica = true and v_fecha_ini_param = false and v_fecha_fin_param = false) then
        /* (20160414) Angel Ruiz. Si existe tabla dinamica, entonces hay que hacer una llamada al sqlplus con un parametro mas  */
        if (v_type_validation = 'I') then
          /* (20160607) Angel Ruiz. Si se trata de validacion I desde la extraccion */
          /* va a las tablas de Stagin sin pasar por ficehro plano */
          /* Previamente hay que sustituir los $1 $2 $3 ... por los correspondientes valores en el fichero .sql que se va a usar para cargar */
          UTL_FILE.put_line(fich_salida_load, '  sed "s/\&' || '1/${FECHA_MES}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL} > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1');
          if (v_hay_usu_owner = true) then
            /* (20161109) Angel Ruiz. Puede ocurrir que el fichero .sql generado posea cadenas del tipo #OWNER_*# */
            --UTL_FILE.put_line(fich_salida_load, '  sed "s/' || v_usuario_owner || '/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1.tmp');
            UTL_FILE.put_line(fich_salida_load, '  sed "s/#OWNER_' || reg_fuente.SOURCE || '#/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1.tmp');
            UTL_FILE.put_line(fich_salida_load, '  mv ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1.tmp  ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1');
          end if;
          --UTL_FILE.put_line(fich_salida_load, '  sqlplus ${BD_USR}/${BD_PWD}@${BD_SID} @${PATH_SQL}/${ARCHIVO_SQL} ${FECHA_MES}');
          UTL_FILE.put_line(fich_salida_load, '  sqoop import \');
          UTL_FILE.put_line(fich_salida_load, '  --connect ${CAD_CONEX_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --username ${BD_USR_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --password ${BD_PWD} \');
          UTL_FILE.put_line(fich_salida_load, '  --options-file ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-import \');
          UTL_FILE.put_line(fich_salida_load, '  --delete-target-dir \');
          UTL_FILE.put_line(fich_salida_load, '  --outdir ${' || NAME_DM || '_TMP_LOCAL} \');
          /* (20170112 ANGEL RUIZ. NF: Nueva estructura tablas de staging */
          --UTL_FILE.put_line(fich_salida_load, '  --hive-table ' || '${ESQUEMA_ST}.SA_' || reg_tabla.TABLE_NAME || ' \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-table ' || '${ESQUEMA_ST}.SAH_' || reg_tabla.TABLE_NAME || ' \');
          /* (20170112 ANGEL RUIZ. FIN NF: Nueva estructura tablas de staging */
          UTL_FILE.put_line(fich_salida_load, '  --hive-partition-key fch_carga \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-partition-value "${FECHA_FMT_HIVE}" \');
          /* (20170418) Angel Ruiz. NF: varios origenes para un mismo interfaz */
          --UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          if (v_numero_fuentes > 1) then
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ}_${SIST_ORIGEN_' || reg_fuente.SOURCE || '} \');
          else
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          end if;
          UTL_FILE.put_line(fich_salida_load, '  --null-non-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  --null-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  -m 1');
        else
          /* Previamente hay que sustituir los $1 $2 $3 ... por los correspondientes valores en el fichero .sql que se va a usar para cargar */
          UTL_FILE.put_line(fich_salida_load, '  sed "s/\&' || '2/${FECHA_MES}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL} > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1');
          if (v_hay_usu_owner = true) then
            /* (20161109) Angel Ruiz. Puede ocurrir que el fichero .sql generado posea cadenas del tipo #OWNER_*# */
            --UTL_FILE.put_line(fich_salida_load, '  sed "s/' || v_usuario_owner || '/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1.tmp');
            UTL_FILE.put_line(fich_salida_load, '  sed "s/#OWNER_' || reg_fuente.SOURCE || '#/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1.tmp');
            UTL_FILE.put_line(fich_salida_load, '  mv ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1.tmp  ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1');
          end if;
          --UTL_FILE.put_line(fich_salida_load, '  sqlplus ${BD_USR}/${BD_PWD}@${BD_SID} @${PATH_SQL}/${ARCHIVO_SQL} ${PATH_SALIDA}/${ARCHIVO_SALIDA} ${FECHA_MES}');
          UTL_FILE.put_line(fich_salida_load, '  sqoop import \');
          UTL_FILE.put_line(fich_salida_load, '  --connect ${CAD_CONEX_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --username ${BD_USR} \');
          UTL_FILE.put_line(fich_salida_load, '  --password ${BD_PWD} \');
          UTL_FILE.put_line(fich_salida_load, '  --delete-target-dir \');
          UTL_FILE.put_line(fich_salida_load, '  --outdir ${' || NAME_DM || '_TMP_LOCAL} \');
          UTL_FILE.put_line(fich_salida_load, '  --options-file ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 \');
          /* (20170418) Angel Ruiz. NF: varios origenes para un mismo interfaz */
          if (v_numero_fuentes > 1) then
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ}_${SIST_ORIGEN_' || reg_fuente.SOURCE || '} \');
          else
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          end if;
          UTL_FILE.put_line(fich_salida_load, '  --fields-terminated-by ''' || v_separator || ''' \');
          UTL_FILE.put_line(fich_salida_load, '  --null-non-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  --null-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  -m 1');
        end if;
      elsif (v_tabla_dinamica = true and v_fecha_ini_param = true and v_fecha_fin_param = true) then
        /* (20160414) Angel Ruiz. Si NO existe tabla dinamica, entonces hacemos la llamada normal  */
        if (v_type_validation = 'I') then
          /* (20160607) Angel Ruiz. Si se trata de validacion I desde la extraccion */
          /* va a las tablas de Stagin sin pasar por ficehro plano */
          /* Previamente hay que sustituir los $1 $2 $3 ... por los correspondientes valores en el fichero .sql que se va a usar para cargar */
          UTL_FILE.put_line(fich_salida_load, '  sed "s/\&' || '1/${FECHA_MES}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL} > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1');
          UTL_FILE.put_line(fich_salida_load, '  sed "s/\&' || '2/${FECHA}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2');
          UTL_FILE.put_line(fich_salida_load, '  sed "s/\&' || '3/${FECHA_FIN}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.3');
          if (v_hay_usu_owner = true) then
            /* (20161109) Angel Ruiz. Puede ocurrir que el fichero .sql generado posea cadenas del tipo #OWNER_*# */
            --UTL_FILE.put_line(fich_salida_load, '  sed "s/' || v_usuario_owner || '/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.3 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.3.tmp');
            UTL_FILE.put_line(fich_salida_load, '  sed "s/#OWNER_' || reg_fuente.SOURCE || '#/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.3 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.3.tmp');
            UTL_FILE.put_line(fich_salida_load, '  mv ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.3.tmp  ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.3');
          end if;
          
          --UTL_FILE.put_line(fich_salida_load, '  sqlplus ${BD_USR}/${BD_PWD}@${BD_SID} @${PATH_SQL}/${ARCHIVO_SQL} ${FECHA_MES} ${FECHA} ${FECHA_FIN}');
          UTL_FILE.put_line(fich_salida_load, '  sqoop import \');
          UTL_FILE.put_line(fich_salida_load, '  --connect ${CAD_CONEX_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --username ${BD_USR} \');
          UTL_FILE.put_line(fich_salida_load, '  --password ${BD_PWD} \');
          UTL_FILE.put_line(fich_salida_load, '  --options-file ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.3 \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-import \');
          UTL_FILE.put_line(fich_salida_load, '  --delete-target-dir \');
          UTL_FILE.put_line(fich_salida_load, '  --outdir ${' || NAME_DM || '_TMP_LOCAL} \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-table ' || '${ESQUEMA_ST}.SAH_' || reg_tabla.TABLE_NAME || ' \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-partition-key fch_carga \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-partition-value "${FECHA_FMT_HIVE}" \');
          /* (20170418) Angel Ruiz. NF: varios origenes para un mismo interfaz */
          --UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          if (v_numero_fuentes > 1) then
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ}_${SIST_ORIGEN_' || reg_fuente.SOURCE || '} \');
          else
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          end if;
          UTL_FILE.put_line(fich_salida_load, '  --null-non-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  --null-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  -m 1');
        else
          UTL_FILE.put_line(fich_salida_load, '  sed "s/\&' || '2/${FECHA_MES}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL} > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1');        
          UTL_FILE.put_line(fich_salida_load, '  sed "s/\&' || '3/${FECHA}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2');        
          UTL_FILE.put_line(fich_salida_load, '  sed "s/\&' || '4/${FECHA_FIN}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.3');        
          if (v_hay_usu_owner = true) then
            /* (20161109) Angel Ruiz. Puede ocurrir que el fichero .sql generado posea cadenas del tipo #OWNER_*# */
            --UTL_FILE.put_line(fich_salida_load, '  sed "s/' || v_usuario_owner || '/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.3 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.3.tmp');
            UTL_FILE.put_line(fich_salida_load, '  sed "s/#OWNER_' || reg_fuente.SOURCE || '#/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.3 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.3.tmp');
            UTL_FILE.put_line(fich_salida_load, '  mv ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.3.tmp  ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.3');
          end if;
        
          --UTL_FILE.put_line(fich_salida_load, '  sqlplus ${BD_USR}/${BD_PWD}@${BD_SID} @${PATH_SQL}/${ARCHIVO_SQL} ${PATH_SALIDA}/${ARCHIVO_SALIDA} ${FECHA_MES} ${FECHA} ${FECHA_FIN}');
          UTL_FILE.put_line(fich_salida_load, '  sqoop import \');
          UTL_FILE.put_line(fich_salida_load, '  --connect ${CAD_CONEX_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --username ${BD_USR_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --password ${BD_PWD} \');
          UTL_FILE.put_line(fich_salida_load, '  --delete-target-dir \');        
          UTL_FILE.put_line(fich_salida_load, '  --outdir ${' || NAME_DM || '_TMP_LOCAL} \');
          UTL_FILE.put_line(fich_salida_load, '  --options-file ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.3 \');
          /* (20170418) Angel Ruiz. NF: varios origenes para un mismo interfaz */
          --UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          /* (20170418) Angel Ruiz. NF: varios origenes para un mismo interfaz */
          if (v_numero_fuentes > 1) then
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ}_${SIST_ORIGEN_' || reg_fuente.SOURCE || '} \');
          else
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          end if;
          UTL_FILE.put_line(fich_salida_load, '  --fields-terminated-by ''' || v_separator || ''' \');
          UTL_FILE.put_line(fich_salida_load, '  --null-non-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  --null-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  -m 1');
        end if;
      elsif (v_tabla_dinamica = true and v_fecha_ini_param = true and v_fecha_fin_param = false) then
        if (v_type_validation = 'I') then
          /* (20160607) Angel Ruiz. Si se trata de validacion I desde la extraccion */
          /* va a las tablas de Stagin sin pasar por ficehro plano */
          /* Previamente hay que sustituir los $1 $2 $3 ... por los correspondientes valores en el fichero .sql que se va a usar para cargar */
          UTL_FILE.put_line(fich_salida_load, '  sed "s/\&' || '1/${FECHA_MES}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL} > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1');
          UTL_FILE.put_line(fich_salida_load, '  sed "s/\&' || '2/${FECHA}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2');
          if (v_hay_usu_owner = true) then
            /* (20161109) Angel Ruiz. Puede ocurrir que el fichero .sql generado posea cadenas del tipo #OWNER_*# */
            --UTL_FILE.put_line(fich_salida_load, '  sed "s/' || v_usuario_owner || '/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2.tmp');
            UTL_FILE.put_line(fich_salida_load, '  sed "s/#OWNER_' || reg_fuente.SOURCE || '#/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2.tmp');
            UTL_FILE.put_line(fich_salida_load, '  mv ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2.tmp  ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2');
          end if;
          
          --UTL_FILE.put_line(fich_salida_load, '  sqlplus ${BD_USR}/${BD_PWD}@${BD_SID} @${PATH_SQL}/${ARCHIVO_SQL} ${FECHA_MES} ${FECHA}');
          UTL_FILE.put_line(fich_salida_load, '  sqoop import \');
          UTL_FILE.put_line(fich_salida_load, '  --connect ${CAD_CONEX_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --username ${BD_USR_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --password ${BD_PWD} \');
          UTL_FILE.put_line(fich_salida_load, '  --options-file ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2 \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-import \');
          UTL_FILE.put_line(fich_salida_load, '  --delete-target-dir \');
          UTL_FILE.put_line(fich_salida_load, '  --outdir ${' || NAME_DM || '_TMP_LOCAL} \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-table ' || '${ESQUEMA_ST}.SAH_' || reg_tabla.TABLE_NAME || ' \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-partition-key fch_carga \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-partition-value "${FECHA_FMT_HIVE}" \');
          /* (20170418) Angel Ruiz. NF: varios origenes para un mismo interfaz */
          --UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          if (v_numero_fuentes > 1) then
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ}_${SIST_ORIGEN_' || reg_fuente.SOURCE || '} \');
          else
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          end if;
          UTL_FILE.put_line(fich_salida_load, '  --null-non-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  --null-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  -m 1');
        else
          /* Previamente hay que sustituir los $1 $2 $3 ... por los correspondientes valores en el fichero .sql que se va a usar para cargar */
          UTL_FILE.put_line(fich_salida_load, '  sed "s/\&' || '2/${FECHA_MES}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL} > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1');
          UTL_FILE.put_line(fich_salida_load, '  sed "s/\&' || '3/${FECHA}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2');
          if (v_hay_usu_owner = true) then
            /* (20161109) Angel Ruiz. Puede ocurrir que el fichero .sql generado posea cadenas del tipo #OWNER_*# */
            --UTL_FILE.put_line(fich_salida_load, '  sed "s/' || v_usuario_owner || '/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2.tmp');
            UTL_FILE.put_line(fich_salida_load, '  sed "s/#OWNER_' || reg_fuente.SOURCE || '#/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2.tmp');
            UTL_FILE.put_line(fich_salida_load, '  mv ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2.tmp  ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2');
          end if;
          
          --UTL_FILE.put_line(fich_salida_load, '  sqlplus ${BD_USR}/${BD_PWD}@${BD_SID} @${PATH_SQL}/${ARCHIVO_SQL} ${PATH_SALIDA}/${ARCHIVO_SALIDA} ${FECHA_MES} ${FECHA}');
          UTL_FILE.put_line(fich_salida_load, '  sqoop import \');
          UTL_FILE.put_line(fich_salida_load, '  --connect ${CAD_CONEX_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --username ${BD_USR_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --password ${BD_PWD} \');
          UTL_FILE.put_line(fich_salida_load, '  --delete-target-dir \');        
          UTL_FILE.put_line(fich_salida_load, '  --outdir ${' || NAME_DM || '_TMP_LOCAL} \');
          UTL_FILE.put_line(fich_salida_load, '  --options-file ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2 \');
          /* (20170418) Angel Ruiz. NF: varios origenes para un mismo interfaz */
          --UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          if (v_numero_fuentes > 1) then
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ}_${SIST_ORIGEN_' || reg_fuente.SOURCE || '} \');
          else
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          end if;
          UTL_FILE.put_line(fich_salida_load, '  --fields-terminated-by ''' || v_separator || ''' \');
          UTL_FILE.put_line(fich_salida_load, '  --null-non-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  --null-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  -m 1');
        end if;
      elsif (v_tabla_dinamica = false and v_fecha_ini_param = true and v_fecha_fin_param = true) then
        if (v_type_validation = 'I') then
          /* (20160607) Angel Ruiz. Si se trata de validacion I desde la extraccion */
          /* va a las tablas de Stagin sin pasar por ficehro plano */
          /* Previamente hay que sustituir los $1 $2 $3 ... por los correspondientes valores en el fichero .sql que se va a usar para cargar */
          UTL_FILE.put_line(fich_salida_load, '  sed "s/\&' || '1/${FECHA}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL} > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1');
          UTL_FILE.put_line(fich_salida_load, '  sed "s/\&' || '2/${FECHA_FIN}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2');
          if (v_hay_usu_owner = true) then
            /* (20161109) Angel Ruiz. Puede ocurrir que el fichero .sql generado posea cadenas del tipo #OWNER_*# */
            --UTL_FILE.put_line(fich_salida_load, '  sed "s/' || v_usuario_owner || '/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2.tmp');
            UTL_FILE.put_line(fich_salida_load, '  sed "s/#OWNER_' || reg_fuente.SOURCE || '#/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2.tmp');
            UTL_FILE.put_line(fich_salida_load, '  mv ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2.tmp  ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2');
          end if;
          
          --UTL_FILE.put_line(fich_salida_load, '  sqlplus ${BD_USR}/${BD_PWD}@${BD_SID} @${PATH_SQL}/${ARCHIVO_SQL} ${FECHA} ${FECHA_FIN}');
          UTL_FILE.put_line(fich_salida_load, '  sqoop import \');
          UTL_FILE.put_line(fich_salida_load, '  --connect ${CAD_CONEX_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --username ${BD_USR_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --password ${BD_PWD} \');
          UTL_FILE.put_line(fich_salida_load, '  --options-file ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2 \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-import \');
          UTL_FILE.put_line(fich_salida_load, '  --delete-target-dir \');        
          UTL_FILE.put_line(fich_salida_load, '  --outdir ${' || NAME_DM || '_TMP_LOCAL} \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-table ' || '${ESQUEMA_ST}.SAH_' || reg_tabla.TABLE_NAME || ' \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-partition-key fch_carga \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-partition-value "${FECHA_FMT_HIVE}" \');
          /* (20170418) Angel Ruiz. NF: varios origenes para un mismo interfaz */
          --UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          if (v_numero_fuentes > 1) then
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ}_${SIST_ORIGEN_' || reg_fuente.SOURCE || '} \');
          else
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          end if;          
          UTL_FILE.put_line(fich_salida_load, '  --null-non-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  --null-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  -m 1');
          
        else
          /* Previamente hay que sustituir los $1 $2 $3 ... por los correspondientes valores en el fichero .sql que se va a usar para cargar */
          UTL_FILE.put_line(fich_salida_load, '  sed "s/\&' || '2/${FECHA}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL} > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1');
          UTL_FILE.put_line(fich_salida_load, '  sed "s/\&' || '3/${FECHA_FIN}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2');
          if (v_hay_usu_owner = true) then
            /* (20161109) Angel Ruiz. Puede ocurrir que el fichero .sql generado posea cadenas del tipo #OWNER_*# */
            --UTL_FILE.put_line(fich_salida_load, '  sed "s/' || v_usuario_owner || '/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2.tmp');
            UTL_FILE.put_line(fich_salida_load, '  sed "s/#OWNER_' || reg_fuente.SOURCE || '#/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2.tmp');
            UTL_FILE.put_line(fich_salida_load, '  mv ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2.tmp  ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2');
          end if;
          --UTL_FILE.put_line(fich_salida_load, '  sqlplus ${BD_USR}/${BD_PWD}@${BD_SID} @${PATH_SQL}/${ARCHIVO_SQL} ${PATH_SALIDA}/${ARCHIVO_SALIDA} ${FECHA} ${FECHA_FIN}');
          UTL_FILE.put_line(fich_salida_load, '  sqoop import \');
          UTL_FILE.put_line(fich_salida_load, '  --connect ${CAD_CONEX_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --username ${BD_USR_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --password ${BD_PWD} \');
          UTL_FILE.put_line(fich_salida_load, '  --delete-target-dir \');        
          UTL_FILE.put_line(fich_salida_load, '  --outdir ${' || NAME_DM || '_TMP_LOCAL} \');
          UTL_FILE.put_line(fich_salida_load, '  --options-file ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.2 \');
          /* (20170418) Angel Ruiz. NF: varios origenes para un mismo interfaz */
          --UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          if (v_numero_fuentes > 1) then
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ}_${SIST_ORIGEN_' || reg_fuente.SOURCE || '} \');
          else
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          end if;
          UTL_FILE.put_line(fich_salida_load, '  --fields-terminated-by ''' || v_separator || ''' \');
          UTL_FILE.put_line(fich_salida_load, '  --null-non-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  --null-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  -m 1');
          
        end if;
      elsif (v_tabla_dinamica = false and v_fecha_ini_param = true and v_fecha_fin_param = false) then
        if (v_type_validation = 'I') then
          /* (20160607) Angel Ruiz. Si se trata de validacion I desde la extraccion */
          /* va a las tablas de Stagin sin pasar por ficehro plano */
          /* Previamente hay que sustituir los $1 $2 $3 ... por los correspondientes valores en el fichero .sql que se va a usar para cargar */
          UTL_FILE.put_line(fich_salida_load, '  sed "s/\&' || '1/${FECHA}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL} > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1');
          if (v_hay_usu_owner = true) then
            /* (20161109) Angel Ruiz. Puede ocurrir que el fichero .sql generado posea cadenas del tipo #OWNER_*# */
            --UTL_FILE.put_line(fich_salida_load, '  sed "s/' || v_usuario_owner || '/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1.tmp');
            UTL_FILE.put_line(fich_salida_load, '  sed "s/#OWNER_' || reg_fuente.SOURCE || '#/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1.tmp');
            UTL_FILE.put_line(fich_salida_load, '  mv ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1.tmp  ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1');
          end if;        
          --UTL_FILE.put_line(fich_salida_load, '  sqlplus ${BD_USR}/${BD_PWD}@${BD_SID} @${PATH_SQL}/${ARCHIVO_SQL} ${FECHA}');
          UTL_FILE.put_line(fich_salida_load, '  sqoop import \');
          UTL_FILE.put_line(fich_salida_load, '  --connect ${CAD_CONEX_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --username ${BD_USR_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --password ${BD_PWD} \');
          UTL_FILE.put_line(fich_salida_load, '  --options-file ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-import \');
          UTL_FILE.put_line(fich_salida_load, '  --delete-target-dir \');        
          UTL_FILE.put_line(fich_salida_load, '  --outdir ${' || NAME_DM || '_TMP_LOCAL} \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-table ' || '${ESQUEMA_ST}.SAH_' || reg_tabla.TABLE_NAME || ' \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-partition-key fch_carga \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-partition-value "${FECHA_FMT_HIVE}" \');
          /* (20170418) Angel Ruiz. NF: varios origenes para un mismo interfaz */
          --UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          if (v_numero_fuentes > 1) then
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ}_${SIST_ORIGEN_' || reg_fuente.SOURCE || '} \');
          else
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          end if;
          UTL_FILE.put_line(fich_salida_load, '  --null-non-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  --null-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  -m 1');
        else
          /* Previamente hay que sustituir los $1 $2 $3 ... por los correspondientes valores en el fichero .sql que se va a usar para cargar */
          UTL_FILE.put_line(fich_salida_load, '  sed "s/\&' || '2/${FECHA}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL} > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1');
          if (v_hay_usu_owner = true) then
            /* (20161109) Angel Ruiz. Puede ocurrir que el fichero .sql generado posea cadenas del tipo #OWNER_*# */
            --UTL_FILE.put_line(fich_salida_load, '  sed "s/' || v_usuario_owner || '/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1.tmp');
            UTL_FILE.put_line(fich_salida_load, '  sed "s/#OWNER_' || reg_fuente.SOURCE || '#/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1.tmp');
            UTL_FILE.put_line(fich_salida_load, '  mv ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1.tmp  ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1');
          end if;                
          --UTL_FILE.put_line(fich_salida_load, '  sqlplus ${BD_USR}/${BD_PWD}@${BD_SID} @${PATH_SQL}/${ARCHIVO_SQL} ${PATH_SALIDA}/${ARCHIVO_SALIDA} ${FECHA}');
          UTL_FILE.put_line(fich_salida_load, '  sqoop import \');
          UTL_FILE.put_line(fich_salida_load, '  --connect ${CAD_CONEX_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --username ${BD_USR_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --password ${BD_PWD} \');
          UTL_FILE.put_line(fich_salida_load, '  --delete-target-dir \');        
          UTL_FILE.put_line(fich_salida_load, '  --outdir ${' || NAME_DM || '_TMP_LOCAL} \');
          UTL_FILE.put_line(fich_salida_load, '  --options-file ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 \');
          /* (20170418) Angel Ruiz. NF: varios origenes para un mismo interfaz */
          --UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          if (v_numero_fuentes > 1) then
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ}_${SIST_ORIGEN_' || reg_fuente.SOURCE || '} \');
          else
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          end if;
          UTL_FILE.put_line(fich_salida_load, '  --fields-terminated-by ''' || v_separator || ''' \');
          UTL_FILE.put_line(fich_salida_load, '  --null-non-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  --null-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  -m 1');
        end if;
      else  
        if (v_type_validation = 'I') then
          /* (20160607) Angel Ruiz. Si se trata de validacion I desde la extraccion */
          /* va a las tablas de Stagin sin pasar por ficehro plano */
          if (v_hay_usu_owner = true) then
            /* (20161109) Angel Ruiz. Puede ocurrir que el fichero .sql generado posea cadenas del tipo #OWNER_*# */
            --UTL_FILE.put_line(fich_salida_load, '  sed "s/' || v_usuario_owner || '/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL} > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1');
            UTL_FILE.put_line(fich_salida_load, '  sed "s/#OWNER_' || reg_fuente.SOURCE || '#/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL} > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1');
          end if;                
          
          --UTL_FILE.put_line(fich_salida_load, '  sqlplus ${BD_USR}/${BD_PWD}@${BD_SID} @${PATH_SQL}/${ARCHIVO_SQL}');
          UTL_FILE.put_line(fich_salida_load, '  sqoop import \');
          UTL_FILE.put_line(fich_salida_load, '  --connect ${CAD_CONEX_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --username ${BD_USR_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --password ${BD_PWD} \');
          if (v_hay_usu_owner = true) then
            UTL_FILE.put_line(fich_salida_load, '  --options-file ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 \');
          else
            UTL_FILE.put_line(fich_salida_load, '  --options-file ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL} \');
          end if;
          UTL_FILE.put_line(fich_salida_load, '  --hive-import \');
          UTL_FILE.put_line(fich_salida_load, '  --delete-target-dir \');        
          UTL_FILE.put_line(fich_salida_load, '  --outdir ${' || NAME_DM || '_TMP_LOCAL} \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-table ' || '${ESQUEMA_ST}.SAH_' || reg_tabla.TABLE_NAME || ' \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-partition-key fch_carga \');
          UTL_FILE.put_line(fich_salida_load, '  --hive-partition-value "${FECHA_FMT_HIVE}" \');
          /* (20170418) Angel Ruiz. NF: varios origenes para un mismo interfaz */
          --UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          if (v_numero_fuentes > 1) then
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ}_${SIST_ORIGEN_' || reg_fuente.SOURCE || '} \');
          else
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          end if;
          UTL_FILE.put_line(fich_salida_load, '  --null-non-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  --null-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  -m 1');
          
        else
          if (v_hay_usu_owner = true) then
            /* (20161109) Angel Ruiz. Puede ocurrir que el fichero .sql generado posea cadenas del tipo #OWNER_*# */
            --UTL_FILE.put_line(fich_salida_load, '  sed "s/' || v_usuario_owner || '/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL} > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1');
            UTL_FILE.put_line(fich_salida_load, '  sed "s/#OWNER_' || reg_fuente.SOURCE || '#/${OWNER_EXT}/g" ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL} > ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1');
          end if;                
        
          --UTL_FILE.put_line(fich_salida_load, '  sqlplus ${BD_USR}/${BD_PWD}@${BD_SID} @${PATH_SQL}/${ARCHIVO_SQL} ${PATH_SALIDA}/${ARCHIVO_SALIDA}');
          UTL_FILE.put_line(fich_salida_load, '  sqoop import \');
          UTL_FILE.put_line(fich_salida_load, '  --connect ${CAD_CONEX_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --username ${BD_USR_EXT} \');
          UTL_FILE.put_line(fich_salida_load, '  --password ${BD_PWD} \');
          UTL_FILE.put_line(fich_salida_load, '  --delete-target-dir \');
          UTL_FILE.put_line(fich_salida_load, '  --outdir ${' || NAME_DM || '_TMP_LOCAL} \');
          if (v_hay_usu_owner = true) then
            UTL_FILE.put_line(fich_salida_load, '  --options-file ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.1 \');
          else
            UTL_FILE.put_line(fich_salida_load, '  --options-file ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL} \');
          end if;
          /* (20170418) Angel Ruiz. NF: varios origenes para un mismo interfaz */
          --UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          if (v_numero_fuentes > 1) then
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ}_${SIST_ORIGEN_' || reg_fuente.SOURCE || '} \');
          else
            UTL_FILE.put_line(fich_salida_load, '  --target-dir ${' || NAME_DM || '_TMP}/${INTERFAZ} \');
          end if;
          UTL_FILE.put_line(fich_salida_load, '  --fields-terminated-by ''' || v_separator || ''' \');
          UTL_FILE.put_line(fich_salida_load, '  --null-non-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  --null-string ''\\N'' \');
          UTL_FILE.put_line(fich_salida_load, '  -m 1');
          
        end if;
      end if;
      UTL_FILE.put_line(fich_salida_load, '  if [ $? -ne 0 ]; then');
      UTL_FILE.put_line(fich_salida_load, '    SUBJECT="${REQ_NUM}:  ERROR: Al generar la interfaz ${ARCHIVO_SQL} (ERROR al ejecutar sqoop)."');
      UTL_FILE.put_line(fich_salida_load, '    echo "Surgio un error al generar la interfaz ${ARCHIVO_SALIDA} (El error surgio al ejecutar sqoop)." >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_load, '    echo `date` >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_load, '    InsertaFinFallido');
      UTL_FILE.put_line(fich_salida_load, '    exit 1');
      UTL_FILE.put_line(fich_salida_load, '  fi');
      /* (20170418) Angel Ruiz. NF: varios origenes para un mismo interfaz */
      if (v_numero_fuentes > 1) then
        UTL_FILE.put_line(fich_salida_load, '  # Borramos el fichero temporal');
        UTL_FILE.put_line(fich_salida_load, '  rm ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.*');
      end if;
    end loop;
    close MTDT_FUENTES;
    /************************/
    /* (20170418) Angel Ruiz. NF FIN */
    /************************/
    if (v_type_validation <> 'I') then
      /* (20160607) Angel Ruiz. Si se trata de validacion I desde la extraccion */
      /* va a las tablas de Stagin sin pasar por ficehro plano */
      /* (20161114) Angel Ruiz. NF. Como sqoop deja el fichero en la ruta especificada ${PATH_SALIDA}/ */
      UTL_FILE.put_line(fich_salida_load, '  # Procesamos el fichero obtenido');
      UTL_FILE.put_line(fich_salida_load, '  hadoop fs -test -d ' || '${' || NAME_DM || '_SALIDA}/${INTERFAZ}');      
      UTL_FILE.put_line(fich_salida_load, '  if [ $? -ne 0 ]; then');
      UTL_FILE.put_line(fich_salida_load, '    # El directorio al que se van a copiar los ficheros no existe. Se crea.');
      UTL_FILE.put_line(fich_salida_load, '    hadoop fs -mkdir ${' || NAME_DM || '_SALIDA}/${INTERFAZ}');
      UTL_FILE.put_line(fich_salida_load, '  fi');
      /* (20170112) Angel Ruiz. NF: Nueva estructura de la parte de STAGING */
      UTL_FILE.put_line(fich_salida_load, '  hadoop fs -test -d ' || '${' || NAME_DM || '_SALIDA}/${INTERFAZ}/${FECHA}');      
      UTL_FILE.put_line(fich_salida_load, '  if [ $? -ne 0 ]; then');
      UTL_FILE.put_line(fich_salida_load, '    # El directorio al que se van a copiar los ficheros no existe. Se crea.');
      UTL_FILE.put_line(fich_salida_load, '    hadoop fs -mkdir ${' || NAME_DM || '_SALIDA}/${INTERFAZ}/${FECHA}');
      UTL_FILE.put_line(fich_salida_load, '  fi');
      /* (20170112) Angel Ruiz. FIN NF: Nueva estructura de la parte de STAGING */
      UTL_FILE.put_line(fich_salida_load, '  # Borramos el fichero en el destino si existe. Opcion -f');
      UTL_FILE.put_line(fich_salida_load, '  hadoop fs -rm -f ' || '${' || NAME_DM || '_SALIDA}/${INTERFAZ}/${FECHA}/${ARCHIVO_SALIDA}');
      UTL_FILE.put_line(fich_salida_load, '  # Generamos el fichero extraido en el destino');
      /* (20170419) Angel Ruiz. NF: Interfaz desde varios origenes*/
      if (v_numero_fuentes > 1) then
        UTL_FILE.put_line(fich_salida_load, '  hadoop fs -cat \');
        open MTDT_FUENTES(reg_tabla.TABLE_NAME);
        loop
          fetch MTDT_FUENTES
          into reg_fuente;
          exit when MTDT_FUENTES%NOTFOUND;
          UTL_FILE.put_line(fich_salida_load, '  ${' || NAME_DM || '_TMP}/${INTERFAZ}_${SIST_ORIGEN_' || reg_fuente.SOURCE || '}/part-m-* \');
        end loop;
        close MTDT_FUENTES;
        UTL_FILE.put_line(fich_salida_load,'  > ${' || NAME_DM || '_TMP_LOCAL}/${ARCHIVO_SALIDA}');
        /* (20170419) Angel Ruiz. NF FIN */
        /* (20170112) Angel Ruiz. FIN NF: Nueva estructura de la parte de STAGING */
        UTL_FILE.put_line(fich_salida_load, '  if [ $? -eq 0 ]; then');
        UTL_FILE.put_line(fich_salida_load, '    # Movemos el fichero generado al HDFS con moveFromLocal para que sea borrado del directorio local');
        UTL_FILE.put_line(fich_salida_load, '    hadoop fs -moveFromLocal ' || '${' || NAME_DM || '_TMP_LOCAL}/${ARCHIVO_SALIDA} ${' || NAME_DM || '_SALIDA}/${INTERFAZ}/${FECHA}/${ARCHIVO_SALIDA}');
        UTL_FILE.put_line(fich_salida_load, '    if [ $? -eq 0 ]; then');
        UTL_FILE.put_line(fich_salida_load, '      hadoop fs -rm -r ' || '${' || NAME_DM || '_TMP}/${INTERFAZ}');
        UTL_FILE.put_line(fich_salida_load, '    else');
        UTL_FILE.put_line(fich_salida_load, '      SUBJECT="${REQ_NUM}:  ERROR: Al generar la interfaz ${ARCHIVO_SQL}. Al llevar a cabo el move del fichero generado a HDFS."');
        UTL_FILE.put_line(fich_salida_load, '      echo "Surgio un error al generar la interfaz ${ARCHIVO_SALIDA} (El error surgio al llevar el ficehro a HDFS)." >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
        UTL_FILE.put_line(fich_salida_load, '      echo `date` >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
        UTL_FILE.put_line(fich_salida_load, '      InsertaFinFallido');
        UTL_FILE.put_line(fich_salida_load, '      exit 1');
        UTL_FILE.put_line(fich_salida_load, '    fi');
        UTL_FILE.put_line(fich_salida_load, '  else');
        UTL_FILE.put_line(fich_salida_load, '    SUBJECT="${REQ_NUM}:  ERROR: Al generar la interfaz ${ARCHIVO_SQL} (ERROR al llevar a cabo el hadoop fs -cat)."');
        UTL_FILE.put_line(fich_salida_load, '    echo "Surgio un error al generar la interfaz ${ARCHIVO_SALIDA} (El error surgio al ejecutar sqoop)." >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
        UTL_FILE.put_line(fich_salida_load, '    echo `date` >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
        UTL_FILE.put_line(fich_salida_load, '    InsertaFinFallido');
        UTL_FILE.put_line(fich_salida_load, '    exit 1');
        UTL_FILE.put_line(fich_salida_load, '  fi');
      else  /* if (v_numero_fuentes > 1) then */
        UTL_FILE.put_line(fich_salida_load, '  NUM_FILES=`hadoop fs -ls ${' || NAME_DM || '_TMP}/${INTERFAZ}/part-m-* | wc -l`');
        UTL_FILE.put_line(fich_salida_load, '  if [ ${NUM_FILES} -eq 1 ]; then');
        /* (20170112) Angel Ruiz. NF: Nueva estructura de la parte de STAGING */
        --UTL_FILE.put_line(fich_salida_load, '    hadoop fs -mv ' || '${' || NAME_DM || '_TMP}/${INTERFAZ}/part-m-00000 ${' || NAME_DM || '_SALIDA}/${INTERFAZ}/${ARCHIVO_SALIDA}');
        UTL_FILE.put_line(fich_salida_load, '    hadoop fs -mv ' || '${' || NAME_DM || '_TMP}/${INTERFAZ}/part-m-00000 ${' || NAME_DM || '_SALIDA}/${INTERFAZ}/${FECHA}/${ARCHIVO_SALIDA}');
        /* (20170112) Angel Ruiz. FIN NF: Nueva estructura de la parte de STAGING */
        UTL_FILE.put_line(fich_salida_load, '    if [ $? -eq 0 ]; then');
        UTL_FILE.put_line(fich_salida_load, '      hadoop fs -rm -r ' || '${' || NAME_DM || '_TMP}/${INTERFAZ}');
        UTL_FILE.put_line(fich_salida_load, '    fi');
        UTL_FILE.put_line(fich_salida_load, '  else');
        /* (20170112) Angel Ruiz. FIN NF: Nueva estructura de la parte de STAGING */
        --UTL_FILE.put_line(fich_salida_load, '    hadoop fs -cat ' || '${' || NAME_DM || '_TMP}/${INTERFAZ}/part-m-* > ${' || NAME_DM || '_SALIDA}/${INTERFAZ}/${ARCHIVO_SALIDA}');
        UTL_FILE.put_line(fich_salida_load, '    hadoop fs -cat ' || '${' || NAME_DM || '_TMP}/${INTERFAZ}/part-m-* > ${' || NAME_DM || '_TMP_LOCAL}/${ARCHIVO_SALIDA}');
        /* (20170112) Angel Ruiz. FIN NF: Nueva estructura de la parte de STAGING */
        UTL_FILE.put_line(fich_salida_load, '    if [ $? -eq 0 ]; then');
        UTL_FILE.put_line(fich_salida_load, '      # Movemos el fichero generado al HDFS con moveFromLocal para que sea borrado del directorio local');
        UTL_FILE.put_line(fich_salida_load, '      hadoop fs -moveFromLocal ' || '${' || NAME_DM || '_TMP_LOCAL}/${ARCHIVO_SALIDA} ${' || NAME_DM || '_SALIDA}/${INTERFAZ}/${FECHA}/${ARCHIVO_SALIDA}');
        UTL_FILE.put_line(fich_salida_load, '      if [ $? -eq 0 ]; then');
        UTL_FILE.put_line(fich_salida_load, '        hadoop fs -rm -r ' || '${' || NAME_DM || '_TMP}/${INTERFAZ}');
        UTL_FILE.put_line(fich_salida_load, '      else');
        UTL_FILE.put_line(fich_salida_load, '        SUBJECT="${REQ_NUM}:  ERROR: Al generar la interfaz ${ARCHIVO_SQL}. Al llevar a cabo el move del fichero generado a HDFS."');
        UTL_FILE.put_line(fich_salida_load, '        echo "Surgio un error al generar la interfaz ${ARCHIVO_SALIDA} (El error surgio al llevar el ficehro a HDFS)." >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
        UTL_FILE.put_line(fich_salida_load, '        echo `date` >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
        UTL_FILE.put_line(fich_salida_load, '        InsertaFinFallido');
        UTL_FILE.put_line(fich_salida_load, '        exit 1');
        UTL_FILE.put_line(fich_salida_load, '      fi');
        UTL_FILE.put_line(fich_salida_load, '    else');
        UTL_FILE.put_line(fich_salida_load, '      SUBJECT="${REQ_NUM}:  ERROR: Al generar la interfaz ${ARCHIVO_SQL} (ERROR al llevar a cabo el hadoop fs -cat)."');
        UTL_FILE.put_line(fich_salida_load, '      echo "Surgio un error al generar la interfaz ${ARCHIVO_SALIDA} (El error surgio al ejecutar sqoop)." >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
        UTL_FILE.put_line(fich_salida_load, '      echo `date` >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
        UTL_FILE.put_line(fich_salida_load, '      InsertaFinFallido');
        UTL_FILE.put_line(fich_salida_load, '      exit 1');
        UTL_FILE.put_line(fich_salida_load, '    fi');
        UTL_FILE.put_line(fich_salida_load, '  fi');
        UTL_FILE.put_line(fich_salida_load, '  # Borramos el fichero temporal');
        UTL_FILE.put_line(fich_salida_load, '  rm ${' || NAME_DM || '_SQL}/${ARCHIVO_SQL}.*');
      end if; /* if (v_numero_fuentes > 1) then */
      UTL_FILE.put_line(fich_salida_load, '');
      /* (20170112) Angel Ruiz. FIN NF: Nueva estructura de la parte de STAGING */
      --UTL_FILE.put_line(fich_salida_load, '  ValidaInformacionArchivo ${' || NAME_DM || '_SALIDA}/${INTERFAZ}/${ARCHIVO_SALIDA}');
      --UTL_FILE.put_line(fich_salida_load, '  ValidaInformacionArchivo ${' || NAME_DM || '_SALIDA}/${INTERFAZ}/${FECHA}/${ARCHIVO_SALIDA}');
      /* (20170112) Angel Ruiz. FIN NF: Nueva estructura de la parte de STAGING */
    end if;
    UTL_FILE.put_line(fich_salida_load, '  return 0');
    UTL_FILE.put_line(fich_salida_load, '}');

    if (v_type_validation <> 'I') then
      UTL_FILE.put_line(fich_salida_load, '################################################################################');
      UTL_FILE.put_line(fich_salida_load, '# SE VALIDA EL NUMERO DE REGISTROS DE LA INTERFAZ                              #');
      UTL_FILE.put_line(fich_salida_load, '################################################################################');
      UTL_FILE.put_line(fich_salida_load, 'ValidaConteo()');
      UTL_FILE.put_line(fich_salida_load, '{');
      if (v_type_validation = 'I') then
        /* (20160607) Angel Ruiz. Si se trata de validacion I desde la extraccion */
        /* va a las tablas de Stagin sin pasar por ficehro plano */
        --UTL_FILE.put_line(fich_salida_load, '  CONTEO_ARCHIVO=`beeline -u ${CAD_CONEX}/${ESQUEMA_ST}${PARAM_CONEX} -n ${BD_USUARIO} -p ${BD_CLAVE} --silent=true --showHeader=false --outputformat=dsv -e "select count(*) from ${ESQUEMA_ST}.SA_' || reg_tabla.TABLE_NAME || ';"`');
        UTL_FILE.put_line(fich_salida_load, '  CONTEO_ARCHIVO_PREV=`beeline --silent=true --showHeader=false --outputformat=dsv << EOF');
        UTL_FILE.put_line(fich_salida_load, '!connect ${CAD_CONEX_HIVE}/${ESQUEMA_MT}${PARAM_CONEX} ${BD_USER_HIVE} ${BD_CLAVE_HIVE}');
        UTL_FILE.put_line(fich_salida_load, 'select count(*) from ${ESQUEMA_ST}.SA_' || reg_tabla.TABLE_NAME || ';');
        UTL_FILE.put_line(fich_salida_load, '!quit');
        UTL_FILE.put_line(fich_salida_load, 'EOF`');
        UTL_FILE.put_line(fich_salida_load, '  CONTEO_ARCHIVO=`echo ${CONTEO_ARCHIVO_PREV} | sed -e ''s/\n//g'' -e ''s/\r//g'' -e ''s/^[ ]*//g'' -e ''s/[ ]*$//g''`');
        
        --UTL_FILE.put_line(fich_salida_load, '  CONTEO_ARCHIVO=`sqlplus -s ${BD_USR}/${BD_PWD}@${BD_SID} <<!eof');
        --UTL_FILE.put_line(fich_salida_load, 'whenever sqlerror exit 1');
        --UTL_FILE.put_line(fich_salida_load, 'set pagesize 0');
        --UTL_FILE.put_line(fich_salida_load, 'set heading off');
        --UTL_FILE.put_line(fich_salida_load, 'select count(*)');
        --UTL_FILE.put_line(fich_salida_load, 'from ' || OWNER_SA || '.SA_' || reg_tabla.TABLE_NAME || ';');
        --UTL_FILE.put_line(fich_salida_load, 'quit');
        --UTL_FILE.put_line(fich_salida_load, '!eof`');
      else
        UTL_FILE.put_line(fich_salida_load, '  CONTEO_ARCHIVO=`hadoop fs -cat ${' || NAME_DM || '_SALIDA}/${INTERFAZ}/${FECHA}/${ARCHIVO_SALIDA} | wc -l`');
      end if;
      UTL_FILE.put_line(fich_salida_load, '  if [ $? -ne 0 ]; then');
      UTL_FILE.put_line(fich_salida_load, '    SUBJECT="${REQ_NUM}:  ERROR: Al generar el conteo del fichero (ERROR al ejecutar wc)."');
      UTL_FILE.put_line(fich_salida_load, '    echo "Surgio un error al generar el conteo de la interfaz ' || reg_tabla.TABLE_NAME || ' (El error surgio al ejecutar wc)." >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_load, '    echo `date` >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_load, '    InsertaFinFallido');
      UTL_FILE.put_line(fich_salida_load, '    exit 1');
      UTL_FILE.put_line(fich_salida_load, '  fi');
      UTL_FILE.put_line(fich_salida_load, '  B_CONTEO_BD=${CONTEO_ARCHIVO}');
      --UTL_FILE.put_line(fich_salida_load, '  B_CONTEO_BD=`sqlplus -s ${BD_USR}/${BD_PWD}@${BD_SID} <<!eof');
      --UTL_FILE.put_line(fich_salida_load, 'whenever sqlerror exit 1');
      --UTL_FILE.put_line(fich_salida_load, 'set pagesize 0');
      --UTL_FILE.put_line(fich_salida_load, 'set heading off');
      --UTL_FILE.put_line(fich_salida_load, 'select');
      --if (v_fecha_ini_param = false and v_fecha_fin_param = false) then
        --UTL_FILE.put_line(fich_salida_load, 'GET_CUENTAINTERFAZ(''${INTERFAZ}'', NULL, NULL)');
      --elsif (v_fecha_ini_param = true and v_fecha_fin_param = true) then
        --UTL_FILE.put_line(fich_salida_load, 'GET_CUENTAINTERFAZ(''${INTERFAZ}'', ''${FECHA}'', ''${FECHA_FIN}'')');      
      --elsif (v_fecha_ini_param = true and v_fecha_fin_param = false) then    
        --UTL_FILE.put_line(fich_salida_load, 'GET_CUENTAINTERFAZ(''${INTERFAZ}'', ''${FECHA}'', NULL)');
      --end if;
      --UTL_FILE.put_line(fich_salida_load, 'from dual;');
      --UTL_FILE.put_line(fich_salida_load, 'quit');
      --UTL_FILE.put_line(fich_salida_load, '!eof`');
      --UTL_FILE.put_line(fich_salida_load, '  if [ $? -ne 0 ]; then');
      --UTL_FILE.put_line(fich_salida_load, '    SUBJECT="${REQ_NUM}: ERROR: Al obtener la Bandera de Conteo de Base de Datos."');
      --UTL_FILE.put_line(fich_salida_load, '    echo "Surgio un error al obtener el conteo de Base de datos para esta interfaz mediante la funcion GET_CUENTAINTERFAZ" | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
      --UTL_FILE.put_line(fich_salida_load, '    echo `date`');
      --UTL_FILE.put_line(fich_salida_load, '    InsertaFinFallido');
      --UTL_FILE.put_line(fich_salida_load, '    exit 1');
      --UTL_FILE.put_line(fich_salida_load, '  fi');
      UTL_FILE.put_line(fich_salida_load, '  if [ ${CONTEO_ARCHIVO} -ne ${B_CONTEO_BD} ]; then');
      UTL_FILE.put_line(fich_salida_load, '    SUBJECT="${REQ_NUM}:  ERROR: Los conteos no coinciden (ERROR al ejecutar comparacion de conteos )."');
      UTL_FILE.put_line(fich_salida_load, '    echo "La validacion de conteo ha fallado, favor de validar la extraccion antes de continuar" >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_load, '    echo `date` >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_load, '  fi');
      UTL_FILE.put_line(fich_salida_load, '}');
    end if;
    if (v_type_validation <> 'I') then 
      UTL_FILE.put_line(fich_salida_load, '################################################################################');
      UTL_FILE.put_line(fich_salida_load, '# SE GENERA EL GENERA EL FICHERO DE FLAG                                       #');
      UTL_FILE.put_line(fich_salida_load, '################################################################################');
      UTL_FILE.put_line(fich_salida_load, 'GeneraFlag()');
      UTL_FILE.put_line(fich_salida_load, '{');
      UTL_FILE.put_line(fich_salida_load, '  NAME_FLAG=`echo ${ARCHIVO_SALIDA} | sed -e ''s/\.[Dd][Aa][Tt]/\.flag/''`');
      UTL_FILE.put_line(fich_salida_load, '  echo "INICIA LA CREACION DEL ARCHIVO ${' || NAME_DM || '_SALIDA}/${INTERFAZ}/${FECHA}/${NAME_FLAG} [`date +%d/%m/%Y\ %H:%M:%S`]" >> ${'|| NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_load, '  echo ${CONTEO_ARCHIVO} >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_load, '  echo ${B_CONTEO_BD} >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_load, '  printf "%-50s%015d%015d\n" ${ARCHIVO_SALIDA} ${CONTEO_ARCHIVO} ${B_CONTEO_BD} > ${' || NAME_DM || '_TMP_LOCAL}/${NAME_FLAG}');
      --UTL_FILE.put_line(fich_salida_load, '  hadoop fs -test -e ${' || NAME_DM || '_SALIDA}/${INTERFAZ}/${FECHA}/${NAME_FLAG}');
      --UTL_FILE.put_line(fich_salida_load, '  if [ $? -eq 0 ]; then');
      UTL_FILE.put_line(fich_salida_load, '  # En caso de existir el fichero de flag lo borramos. Opcion -f');
      UTL_FILE.put_line(fich_salida_load, '  hadoop fs -rm -f ' || '${' || NAME_DM || '_SALIDA}/${INTERFAZ}/${FECHA}/${NAME_FLAG}');
      UTL_FILE.put_line(fich_salida_load, '  # Movemos el ficehro de flag al destino');
      UTL_FILE.put_line(fich_salida_load, '  hadoop fs -put ${' || NAME_DM || '_TMP_LOCAL}/${NAME_FLAG} ${' || NAME_DM || '_SALIDA}/${INTERFAZ}/${FECHA}/${NAME_FLAG}');
      UTL_FILE.put_line(fich_salida_load, '  if [ $? -ne 0 ]; then');
      UTL_FILE.put_line(fich_salida_load, '    echo "Error al generar el archivo flag ${' || NAME_DM || '_SALIDA}/${INTERFAZ}/${FECHA}/${NAME_FLAG}" >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_load, '    InsertaFinFallido');
      UTL_FILE.put_line(fich_salida_load, '    exit 3');
      UTL_FILE.put_line(fich_salida_load, '  fi');
      UTL_FILE.put_line(fich_salida_load, '  rm ${' || NAME_DM || '_TMP_LOCAL}/${NAME_FLAG}');
      UTL_FILE.put_line(fich_salida_load, '  echo "TERMINA LA CREACION DEL ARCHIVO ${' || NAME_DM || '_SALIDA}/${INTERFAZ}/${FECHA}/${NAME_FLAG} [`date +%d/%m/%Y\ %H:%M:%S`]" >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
      UTL_FILE.put_line(fich_salida_load, '}');
      --UTL_FILE.put_line(fich_salida_load, '################################################################################');
      --UTL_FILE.put_line(fich_salida_load, '# REALIZA EL ENVIO DE LOS ARCHIVOS POR SCP                                     #');
      --UTL_FILE.put_line(fich_salida_load, '################################################################################');
      --UTL_FILE.put_line(fich_salida_load, 'EnviaArchivos()');
      --UTL_FILE.put_line(fich_salida_load, '{');
      /* (20161004) Angel Ruiz. Modificacion Temporal. Se trata de comentar la linea que envia los ficheros */
      /* dado que seran concatenados con otra fuente */
      --if ( reg_tabla.TABLE_NAME in ('CALIDAD_PERCIBIDA', 'DICCIONARIO_TT', 'ESPECIFICACION_TT', 'ESTADO_TAREA', 'FORMA_CONTACTO'
        --, 'MOTIVO_OPERACION_TT', 'MOVIMIENTOS_TT', 'NODO', 'PROVEEDOR_TELCO', 'TIPIFICACION_TT'
        --, 'TIPO_NODO', 'TIPO_OPERACION_TT', 'UNIDAD_FUNCIONAL1', 'UNIDAD_FUNCIONAL2')) then
        --UTL_FILE.put_line(fich_salida_load, '  PATH_DESTINO="/DWH/dwhprod/DWH/MEX/Fuente"');
        --UTL_FILE.put_line(fich_salida_load, '  scp ${PATH_SALIDA}/${ARCHIVO_SALIDA} ${USER_DESTINO_SCP}@${DESTINO_IP}:${PATH_DESTINO}/${FECHA}');
      --else
        --UTL_FILE.put_line(fich_salida_load, '  scp ${PATH_SALIDA}/${ARCHIVO_SALIDA} ${USER_DESTINO_SCP}@${DESTINO_IP}:${PATH_DESTINO}');
      --end if;
      /* (20161004) Angel Ruiz. Fin cambio Temporal*/
      --UTL_FILE.put_line(fich_salida_load, '  if [ $? -ne 0 ]; then');
      --UTL_FILE.put_line(fich_salida_load, '    SUBJECT="${REQ_NUM}:  Surgio un error en el envio del archivo."');
      --UTL_FILE.put_line(fich_salida_load, '    echo "Surgio un error al enviar el archivo ${ARCHIVO_SALIDA} al servidor ${DESTINO_IP}." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
      --UTL_FILE.put_line(fich_salida_load, '    echo `date`');
      --UTL_FILE.put_line(fich_salida_load, '    InsertaFinFallido');
      --UTL_FILE.put_line(fich_salida_load, '    exit 1');
      --UTL_FILE.put_line(fich_salida_load, '  fi');
      /* (20161004) Angel Ruiz. Modificacion Temporal. Se trata de comentar la linea que envia los ficheros */
      /* dado que seran concatenados con otra fuente */
      --if ( reg_tabla.TABLE_NAME in ('CALIDAD_PERCIBIDA', 'DICCIONARIO_TT', 'ESPECIFICACION_TT', 'ESTADO_TAREA', 'FORMA_CONTACTO'
      --  , 'MOTIVO_OPERACION_TT', 'MOVIMIENTOS_TT', 'NODO', 'PROVEEDOR_TELCO', 'TIPIFICACION_TT'
      --  , 'TIPO_NODO', 'TIPO_OPERACION_TT', 'UNIDAD_FUNCIONAL1', 'UNIDAD_FUNCIONAL2')) then
      --  UTL_FILE.put_line(fich_salida_load, '  scp ${PATH_SALIDA}/${NAME_FLAG} ${USER_DESTINO_SCP}@${DESTINO_IP}:${PATH_DESTINO}/${FECHA}');
      --else
        --UTL_FILE.put_line(fich_salida_load, '  scp ${PATH_SALIDA}/${NAME_FLAG} ${USER_DESTINO_SCP}@${DESTINO_IP}:${PATH_DESTINO}');
      --end if;
      --UTL_FILE.put_line(fich_salida_load, '  if [ $? -ne 0 ]; then');
      --UTL_FILE.put_line(fich_salida_load, '    SUBJECT="${REQ_NUM}:  Surgio un error en el envio del archivo."');
      --UTL_FILE.put_line(fich_salida_load, '    echo "Surgio un error al enviar el archivo ${NAME_FLAG} al servidor ${DESTINO_IP}." | mailx -s "${SUBJECT}" "${CTA_MAIL}"');
      --UTL_FILE.put_line(fich_salida_load, '    echo `date`');
      --UTL_FILE.put_line(fich_salida_load, '    InsertaFinFallido');
      --UTL_FILE.put_line(fich_salida_load, '    exit 1');
      --UTL_FILE.put_line(fich_salida_load, '  fi');
      --UTL_FILE.put_line(fich_salida_load, '  return 0');
      --UTL_FILE.put_line(fich_salida_load, '}');
    end if;
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '# EJECUCION DEL PROGRAMA                                                       #');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '');
    UTL_FILE.put_line(fich_salida_load, '. ${' || NAME_DM || '_ENTORNO}/entornoNGRD_MEX.sh');
    UTL_FILE.put_line(fich_salida_load, 'set -x');
    UTL_FILE.put_line(fich_salida_load, '#Permite los acentos');
    UTL_FILE.put_line(fich_salida_load, 'export NLS_LANG=AMERICAN_AMERICA.WE8ISO8859P1');
    UTL_FILE.put_line(fich_salida_load, '# Comprobamos si hay parametro fecha de extraccion');
    UTL_FILE.put_line(fich_salida_load, 'if [ $# -eq 0 ] ; then');
    UTL_FILE.put_line(fich_salida_load, '  FECHA_LOG=`date +%Y%m%d`');
    UTL_FILE.put_line(fich_salida_load, 'else');
    UTL_FILE.put_line(fich_salida_load, '  FECHA_LOG=${1}');
    UTL_FILE.put_line(fich_salida_load, 'fi');
    UTL_FILE.put_line(fich_salida_load, '# Comprobamos si existe el directorio de Trazas para fecha de carga');
    UTL_FILE.put_line(fich_salida_load, 'if ! [ -d ${' || NAME_DM || '_TRAZAS}/${FECHA_LOG} ] ; then');
    UTL_FILE.put_line(fich_salida_load, '  mkdir ${' || NAME_DM || '_TRAZAS}/${FECHA_LOG}');
    UTL_FILE.put_line(fich_salida_load, 'fi');
    UTL_FILE.put_line(fich_salida_load, NAME_DM || '_TRAZAS=${' || NAME_DM || '_TRAZAS}/${FECHA_LOG}');
    UTL_FILE.put_line(fich_salida_load, 'FECHA_HORA=${FECHA_LOG}_`date +%Y%m%d_%H%M%S`');
    UTL_FILE.put_line(fich_salida_load, 'echo "Inicia Proceso: `date +%d/%m/%Y\ %H:%M:%S`" > ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '# VARIABLES ESPECIFICAS PARA EL PROCESO                                        #');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, 'REQ_NUM="' || REQ_NUMBER || '"');
    /* (20170418) Angel Ruiz. NF: Extraccion desde varios origenes*/
    open MTDT_FUENTES(reg_tabla.TABLE_NAME);
    loop
      fetch MTDT_FUENTES
      into reg_fuente;
      exit when MTDT_FUENTES%NOTFOUND;
      UTL_FILE.put_line(fich_salida_load, 'SIST_ORIGEN' || '_' || reg_fuente.SOURCE || '="' || reg_fuente.SOURCE || '"');
    end loop;
    close MTDT_FUENTES;
    UTL_FILE.put_line(fich_salida_load, '#NOMBRE INTERFAZ');
    UTL_FILE.put_line(fich_salida_load, 'INTERFAZ="' || reg_tabla.TABLE_NAME || '"');
    --SELECT trim(mtdt_interface_summary.SOURCE) into v_fuente_interface from mtdt_interface_summary where trim(MTDT_INTERFACE_DETAIL.CONCEPT_NAME) = reg_tabla.TABLE_NAME;
    --UTL_FILE.put_line(fich_salida_load, 'SIST_ORIGEN="' || v_fuente || '"');
    UTL_FILE.put_line(fich_salida_load, 'BD_USR_EXT=""');
    UTL_FILE.put_line(fich_salida_load, 'BD_CLAVE=""');
    UTL_FILE.put_line(fich_salida_load, 'CAD_CONEX_EXT=""');
    UTL_FILE.put_line(fich_salida_load, '#NOMBRE DE LA INTERFAZ');
    UTL_FILE.put_line(fich_salida_load, 'NOM_INTERFAZ="' || nom_inter_a_cargar_sin_fecha || '"');
    /* (20161004) Angel Ruiz. Suprimo las tres siguientes variables de entorno porque las paso al fichero de entorno ya que es mas practico */
    --UTL_FILE.put_line(fich_salida_load, 'PATH_REQ="/DWH/requerimientos"');
    --UTL_FILE.put_line(fich_salida_load, 'PATH_SQL="${PATH_REQ}/shells/${REQ_NUM}/ONIX_${INTERFAZ}/sql/"');
    --UTL_FILE.put_line(fich_salida_load, 'PATH_SALIDA="${PATH_REQ}/salidasmanual/${REQ_NUM}/ONIX_${INTERFAZ}/datos/"');
    --UTL_FILE.put_line(fich_salida_load, 'PATH_SHELL="${PATH_REQ}/shells/${REQ_NUM}/SAP_INFO/shell/"');
    --UTL_FILE.put_line(fich_salida_load, 'PATH_SHELL="${PATH_REQ}/shells/${REQ_NUM}/ONIX_${INTERFAZ}/shell/"');
    UTL_FILE.put_line(fich_salida_load, 'B_CONTEO_BD=0');
    UTL_FILE.put_line(fich_salida_load, 'CONTEO_ARCHIVO=0');
    UTL_FILE.put_line(fich_salida_load, 'ULT_PASO_EJECUTADO=1');
    UTL_FILE.put_line(fich_salida_load, 'BAN_FORZADO=''N''');
    --UTL_FILE.put_line(fich_salida_load, 'SHELL_SCP="${PATH_SHELL}' || REQ_NUMBER || '_EnviaArchivos.sh"');
    UTL_FILE.put_line(fich_salida_load, 'if [ "`/sbin/ifconfig -a | grep ''10.225.244.'' | awk -F'':'' ''{print substr($2,1,13) }''`" = "10.225.244.21" ]; then');
    --UTL_FILE.put_line(fich_salida_load, '  OWNER="ONIX"');
    UTL_FILE.put_line(fich_salida_load, '  #CUENTAS PARA MANTENIMIENTO');
    UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL="ulises.rosales.ext@telefonica.com"');
    UTL_FILE.put_line(fich_salida_load, '  #BASE DONDE SE CARGARA LA INFORMACION');
    --UTL_FILE.put_line(fich_salida_load, '  BD_SID="QSIEMDESA"');
    --UTL_FILE.put_line(fich_salida_load, '  BD_USR="ONIX"');
    --UTL_FILE.put_line(fich_salida_load, '  BD_PWD=""');
    --if (v_type_validation <> 'I') then
      --UTL_FILE.put_line(fich_salida_load, '  #INFORMACION PARA ENVIO POR SCP');
      --UTL_FILE.put_line(fich_salida_load, '  DESTINO_IP="10.225.173.101"');
      --UTL_FILE.put_line(fich_salida_load, '  USER_DESTINO_SCP="app_bi"');
      --UTL_FILE.put_line(fich_salida_load, '  PATH_DESTINO="/reportes/requerimientos/salidasmanual/' || REQ_NUMBER || '/datos"');
    --end if;
    UTL_FILE.put_line(fich_salida_load, 'else');
    --UTL_FILE.put_line(fich_salida_load, '  OWNER="ONIX"');
    UTL_FILE.put_line(fich_salida_load, '  #CUENTAS PARA MANTENIMIENTO');
    --UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL=`cat ${' || NAME_DM || '_REQ}/shells/Utilerias/Correos_Mtto_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_load, '  CTA_MAIL=`cat ${' || NAME_DM || '_CONFIGURACION}/Correos_Mtto_ReportesBI.txt`');
    UTL_FILE.put_line(fich_salida_load, '  #BASE DONDE SE CARGARA LA INFORMACION');
    --UTL_FILE.put_line(fich_salida_load, '  BD_SID="' || BD_SID || '"');
    --UTL_FILE.put_line(fich_salida_load, '  BD_USR="' || BD_USR || '"');
    --UTL_FILE.put_line(fich_salida_load, '  BD_PWD=""');
    --if (v_type_validation <> 'I') then
      --UTL_FILE.put_line(fich_salida_load, '  #INFORMACION PARA ENVIO POR SCP');
      --UTL_FILE.put_line(fich_salida_load, '  DESTINO_IP="10.225.210.136"');
      --UTL_FILE.put_line(fich_salida_load, '  USER_DESTINO_SCP="dwhprod"');
      --UTL_FILE.put_line(fich_salida_load, '  PATH_DESTINO="/DWH/dwhprod/DWH/MEX/Fuente/ValidaIE/XVALIDAR"');
    --end if;
    UTL_FILE.put_line(fich_salida_load, 'fi');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '# LIBRERIAS                                                                    #');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, 'if [ -z "${LD_UTILBD}" ] ; then');
    --UTL_FILE.put_line(fich_salida_load, '  . ${PATH_REQ}/shells/Utilerias/UtilBD.sh');
    UTL_FILE.put_line(fich_salida_load, '  . ${' || NAME_DM || '_UTILIDADES}/UtilBD.sh');
    UTL_FILE.put_line(fich_salida_load, 'fi');
    UTL_FILE.put_line(fich_salida_load, 'if [ -z "${LD_UTILArchivo}" ] ; then');
    --UTL_FILE.put_line(fich_salida_load, '  . ${PATH_REQ}/shells/Utilerias/UtilBD.sh');
    UTL_FILE.put_line(fich_salida_load, '  . ${' || NAME_DM || '_UTILIDADES}/UtilArchivo.sh');
    UTL_FILE.put_line(fich_salida_load, 'fi');
    --UTL_FILE.put_line(fich_salida_load, 'LdVarOra');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '# LLAMADO DE LAS FUNCIONES ESPECIALES                                          #');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    --if (v_type_validation <> 'I') then
      --UTL_FILE.put_line(fich_salida_load, 'DepuracionDeProducto ${REQ_NUM} ${' || NAME_DM || '_SALIDA}/${NOM_INTERFAZ}* 2');
    --end if;
    /* (20170418) Angel Ruiz. NF: Diferentes origenes para un mismo interfaz */
    --UTL_FILE.put_line(fich_salida_load, '#OBTIENE LA CONTRASENA DE LA B.D. PARA LA EXTRACCION');
    --UTL_FILE.put_line(fich_salida_load, 'ObtenContrasena ${SIST_ORIGEN}');
    /* (20170418) Angel Ruiz. NF Fin */
    UTL_FILE.put_line(fich_salida_load, '#OBTIENE LA CONTRASENA DE HIVE');
    UTL_FILE.put_line(fich_salida_load, 'TraePass HIVE ${BD_USER_HIVE}');
    UTL_FILE.put_line(fich_salida_load, 'BD_CLAVE_HIVE=PASSWORD');
    UTL_FILE.put_line(fich_salida_load, '#OBTIENE LA FECHA');
    UTL_FILE.put_line(fich_salida_load, 'ObtieneFecha $1');
    UTL_FILE.put_line(fich_salida_load, '#OBTIENE LA FECHA Y HORA DEL SISTEMA');
    UTL_FILE.put_line(fich_salida_load, 'ObtieneFechaHora');
    if (v_type_validation = 'I') then
      /* (20160620) Angel Ruiz. NF: Implemento la Validacion tipo I donde los datos extraidos van directamente */
      /* a las tablas de STAGING. Por claridad cuando esto sucede le cambio el nombre al procedure que lo hace */
      UTL_FILE.put_line(fich_salida_load, '#OBTENEMOS Interfaz y la cargamos directamente sobre las tablas de STAGING');
      UTL_FILE.put_line(fich_salida_load, 'CargaDirectaTablaStaging');
      --UTL_FILE.put_line(fich_salida_load, 'ValidaConteo');
      --UTL_FILE.put_line(fich_salida_load, 'ObtenInterfaz');
    else
      UTL_FILE.put_line(fich_salida_load, '#OBTENEMOS Interfaz');
      UTL_FILE.put_line(fich_salida_load, 'ObtenInterfaz');
      UTL_FILE.put_line(fich_salida_load, 'ValidaConteo');
    end if;
    if (v_type_validation <> 'I') then
      UTL_FILE.put_line(fich_salida_load, 'GeneraFlag');
      /* (20160712) Angel Ruiz. Modificacion Temporal. Se trata de comentar la linea que envia los ficheros */
      /* dado que seran concatenados con otra fuente */
      --if ( reg_tabla.TABLE_NAME in ('BANCO', 'CANAL', 'CANAL_OFERTA', 'CART_VENCIDA', 'CAUSA_PAGO'
            --, 'CICLO_FACTURACION', 'CLIENTE', 'CONCEPTO_FACTURA', 'CONCEPTO_PAGO', 'CUENTA'
            --, 'DISTRIBUIDOR', 'ESTATUS_OFERTA', 'ESTATUS_OPERACION', 'FACTURACION_IMEI', 'FORMA_PAGO', 'ICC', 'MOVIMIENTO_ABO', 'OFICINA'
            --, 'ORIGEN_PAGO', 'ORIGEN_VENTA_COMERCIAL', 'PARQUE_ABO_POST', 'PARQUE_ABO_PRE', 'PARQUE_SVA', 'PLAN_TARIFARIO', 'PROMOCION'
            --, 'PUNTO_VENTA', 'TARJETA_PAGO', 'TIPO_CONCEPTO_FACTURA', 'TIPO_CUENTA', 'TRAF_TARIF_DATOS_POST', 'TRAF_TARIF_VOZ_POST'
            --, 'VENDEDOR', 'VENTAS_REGISTRADAS')) then
        --UTL_FILE.put_line(fich_salida_load, '#EnviaArchivos');
      --else
        --UTL_FILE.put_line(fich_salida_load, 'EnviaArchivos');
      --end if;
    end if;
    UTL_FILE.put_line(fich_salida_load, 'InsertaFinOK');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    UTL_FILE.put_line(fich_salida_load, '# FIN DEL SHELL                                                                #');
    UTL_FILE.put_line(fich_salida_load, '################################################################################');
    --UTL_FILE.put_line(fich_salida_load, 'echo "Termina Proceso: `date`"');
    UTL_FILE.put_line(fich_salida_load, 'echo "Termina Proceso: `date +%d/%m/%Y\ %H:%M:%S`" >> ${' || NAME_DM || '_TRAZAS}/' || REQ_NUMBER || '_' || reg_tabla.TABLE_NAME || '_${FECHA_HORA}.log');
    --UTL_FILE.put_line(fich_salida_load, 'InsertaFinOK');
    UTL_FILE.put_line(fich_salida_load, 'exit 0');
    UTL_FILE.put_line(fich_salida_load, '');
    /*************************/
    /*************************/
    /*************************/
    /*************************/
    

    
    UTL_FILE.FCLOSE (fich_salida_load);
    --UTL_FILE.FCLOSE (fich_salida_exchange);
    --UTL_FILE.FCLOSE (fich_salida_pkg);
  end loop;
  close MTDT_TABLA;
end;

