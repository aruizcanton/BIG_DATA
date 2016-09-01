  DECLARE
  /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR EN EL STAGING AREA */
  CURSOR dtd_interfaz_summary
  IS
    SELECT 
      TRIM(CONCEPT_NAME) "CONCEPT_NAME",
      SOURCE,
      TRIM(INTERFACE_NAME) "INTERFACE_NAME",
      trim(COUNTRY) "COUNTRY",
      TYPE,
      SEPARATOR,
      DELAYED,
      upper(trim(TYPE_VALIDATION)) TYPE_VALIDATION
  FROM MTDT_INTERFACE_SUMMARY
  WHERE SOURCE <> 'SA';
  --where CONCEPT_NAME = 'CICLO';

  CURSOR dtd_interfaz_summary_history
  IS
    SELECT 
      TRIM(CONCEPT_NAME) "CONCEPT_NAME",
      SOURCE,
      INTERFACE_NAME,
      TYPE,
      SEPARATOR,
      DELAYED,
      HISTORY
    FROM MTDT_INTERFACE_SUMMARY
    where HISTORY is not null;
  
  CURSOR dtd_interfaz_detail (concep_name_in IN VARCHAR2, source_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(CONCEPT_NAME) "CONCEPT_NAME",
      SOURCE,
      COLUMNA,
      KEY,
      TYPE,
      LENGTH,
      NULABLE,
      PARTITIONED,
      POSITION
    FROM
      MTDT_INTERFACE_DETAIL
    WHERE
      trim(CONCEPT_NAME) = trim(concep_name_in) and
      SOURCE = source_in
      order by POSITION;
      
      

      reg_summary dtd_interfaz_summary%rowtype;

      reg_summary_history dtd_interfaz_summary_history%rowtype;

      reg_datail dtd_interfaz_detail%rowtype;
      
      primera_col INTEGER;
      v_nombre_particion VARCHAR2(30);
      TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
      TYPE list_columns_partitioned  IS TABLE OF VARCHAR(30);
      TYPE list_tablas_RE IS TABLE OF VARCHAR(30);
      TYPE list_posiciones  IS TABLE OF reg_datail.POSITION%type;

      
      lista_pk                                      list_columns_primary := list_columns_primary ();
      lista_pos                                    list_posiciones := list_posiciones (); 
      
      tipo_col                                      VARCHAR(70);
      lista_par                                     list_columns_partitioned := list_columns_partitioned();
      v_lista_tablas_RE                        list_tablas_RE := list_tablas_RE();
      lista_campos_particion            VARCHAR(250);
      no_encontrado                          VARCHAR(1);
      subset                                         VARCHAR(1);
      OWNER_SA                             VARCHAR2(60);
      OWNER_T                                VARCHAR2(60);
      OWNER_DM                            VARCHAR2(60);
      OWNER_MTDT                       VARCHAR2(60);
      TABLESPACE_SA                  VARCHAR2(60);
      NAME_DM                            VARCHAR(60);

      nombre_tabla_reducido VARCHAR2(30);
      v_existe_tablas_RE integer:=0;
      v_encontrado VARCHAR2(1):='N';
      nombre_interface_a_cargar   VARCHAR2(150);
      pos_ini_pais                            PLS_integer;
      pos_fin_pais                            PLS_integer;
      pos_ini_fecha                           PLS_integer;
      pos_fin_fecha                           PLS_integer;
      pos_ini_hora                              PLS_integer;
      pos_fin_hora                              PLS_integer;
      num_column PLS_INTEGER;
      v_ulti_pos                        PLS_integer;

      
      
    
      


BEGIN
  /* (20150119) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO TABLESPACE_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'TABLESPACE_SA';
  SELECT VALOR INTO NAME_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM';  
  /* (20150119) FIN*/
  
  
  OPEN dtd_interfaz_summary;
  LOOP
    FETCH dtd_interfaz_summary
    INTO reg_summary;
    EXIT WHEN dtd_interfaz_summary%NOTFOUND;
    /* (20160523) Angel Ruiz. NF: Funcionalidad para la creacion de Tablas Externas */
    if (reg_summary.TYPE_VALIDATION = 'T' or reg_summary.TYPE_VALIDATION = 'I' or reg_summary.TYPE_VALIDATION is null) then
      /* (20160523) Se trata de la creacion de una Tabla de Staging NORMAL */
        --DBMS_OUTPUT.put_line('CREATE TABLE IF NOT EXISTS ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME);
        DBMS_OUTPUT.put_line('CREATE TABLE IF NOT EXISTS ' || NAME_DM || '.SA_' || reg_summary.CONCEPT_NAME);
    else
      /* Se trata de una tabla externa */
        --DBMS_OUTPUT.put_line('CREATE EXTERNAL TABLE IF NOT EXISTS ' || OWNER_SA || '.SA_' || reg_summary.CONCEPT_NAME);
        DBMS_OUTPUT.put_line('CREATE EXTERNAL TABLE IF NOT EXISTS ' || NAME_DM || '.SA_' || reg_summary.CONCEPT_NAME);
    end if;
    DBMS_OUTPUT.put_line('(');
    OPEN dtd_interfaz_detail (reg_summary.CONCEPT_NAME, reg_summary.SOURCE);
    primera_col := 1;
    LOOP
      FETCH dtd_interfaz_detail
      INTO reg_datail;
      EXIT WHEN dtd_interfaz_detail%NOTFOUND;
      IF primera_col = 1 THEN /* Si es primera columna */
        CASE 
        WHEN reg_datail.TYPE = 'AN' THEN
          --tipo_col := 'VARCHAR2 (' || reg_datail.LENGTH || ')';
          tipo_col := 'STRING';
        WHEN reg_datail.TYPE = 'NU' THEN
          tipo_col := 'DECIMAL (' || reg_datail.LENGTH || ')';
        WHEN reg_datail.TYPE = 'DE' THEN
          tipo_col := 'DECIMAL (' || reg_datail.LENGTH || ')';
        WHEN reg_datail.TYPE = 'FE' THEN
          tipo_col := 'DATE';
        WHEN reg_datail.TYPE = 'IM' THEN
          tipo_col := 'DECIMAL (' || reg_datail.LENGTH || ')';
          --tipo_col := 'NUMBER (15, 3)';
        WHEN reg_datail.TYPE = 'TI' THEN
          --tipo_col := 'VARCHAR (8)';
          tipo_col := 'STRING';
        END CASE;
        DBMS_OUTPUT.put_line(reg_datail.COLUMNA || '          ' || tipo_col);
        primera_col := 0;
      ELSE  /* si no es primera columna */
        CASE 
        WHEN reg_datail.TYPE = 'AN' THEN
          --tipo_col := 'VARCHAR2 (' || reg_datail.LENGTH || ')';
          tipo_col := 'STRING';
        WHEN reg_datail.TYPE = 'NU' THEN
          tipo_col := 'DECIMAL (' || reg_datail.LENGTH || ')';
        WHEN reg_datail.TYPE = 'DE' THEN
          tipo_col := 'DECIMAL (' || reg_datail.LENGTH || ')';
        WHEN reg_datail.TYPE = 'FE' THEN
          tipo_col := 'DATE';
        WHEN reg_datail.TYPE = 'IM' THEN
          tipo_col := 'DECIMAL (' || reg_datail.LENGTH || ')';
          --tipo_col := 'NUMBER (15, 3)';
        WHEN reg_datail.TYPE = 'TI' THEN
          --tipo_col := 'VARCHAR2 (8)';
          tipo_col := 'STRING';
        END CASE;
          DBMS_OUTPUT.put_line(', ' || reg_datail.COLUMNA || '          '  || tipo_col);
      END IF;
      IF reg_datail.PARTITIONED = 'S' then
        lista_par.EXTEND;
        lista_par(lista_par.LAST) := ', ' || reg_datail.COLUMNA || ' ' || tipo_col;
      END IF;
    END LOOP;
    CLOSE dtd_interfaz_detail;
    IF (lista_par.COUNT = 0) THEN
      /* (20160811) No hay particionado explicito aunque si que esta el particionado */
      /* que toda tabla lleva */
      DBMS_OUTPUT.put_line(')');
      DBMS_OUTPUT.put_line('PARTITIONED BY (FCH_CARGA STRING)');
      DBMS_OUTPUT.put_line('ROW FORMAT DELIMITED');
      DBMS_OUTPUT.put_line('FIELDS TERMINATED BY ''' || reg_summary.SEPARATOR || '''');
      DBMS_OUTPUT.put_line('STORED AS TEXTFILE;');
    END IF;
    /* tomamos el campo por el que va a estar particionada la tabla */
    if (lista_par.COUNT > 0) then
      FOR indx IN lista_par.FIRST .. lista_par.LAST
      LOOP
        IF indx = lista_par.FIRST THEN
          lista_campos_particion:= lista_par (indx);
        ELSE
          lista_campos_particion:=lista_campos_particion || lista_par (indx);
        END IF;
      END LOOP;
      DBMS_OUTPUT.put_line('PARTITIONED BY');
      DBMS_OUTPUT.put_line('(FCH_CARGA STRING' || lista_campos_particion || ')');
      DBMS_OUTPUT.put_line('ROW FORMAT DELIMITED');
      DBMS_OUTPUT.put_line('FIELDS TERMINATED BY ''' || reg_summary.SEPARATOR || '''');
      DBMS_OUTPUT.put_line('STORED AS TEXTFILE;');
      DBMS_OUTPUT.put_line('');
      lista_par.DELETE;
      /* (20151118) Angel Ruiz. NF: Creacion de tablas para inyeccion SAD */
      /* (20151118) Angel Ruiz. FIN NF. Tablas para inyeccion SAD_ */
    end if;
  END LOOP;
  CLOSE dtd_interfaz_summary;

  DBMS_OUTPUT.put_line('!quit');
END;

