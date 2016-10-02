DECLARE
  /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR */
  /*CURSOR c_mtdt_modelo_logico_TABLA
  IS
    SELECT 
      DISTINCT
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLESPACE) "TABLESPACE",
      TRIM(CI) "CI"
    FROM MTDT_MODELO_LOGICO
    WHERE CI <> 'P';    *//* Las que poseen un valor "P" en esta columna son las tablas de PERMITED_VALUES, por lo que no hya que generar su modelo */
/*
  CURSOR c_mtdt_modelo_logico_COLUMNA (table_name_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(COLUMN_NAME) "COLUMN_NAME",
      DATA_TYPE,
      PK,
      CI,
      TRIM(VDEFAULT) "VDEFAULT",
      TRIM(TABLESPACE) "TABLESPACE"
    FROM MTDT_MODELO_LOGICO
    WHERE
      TRIM(TABLE_NAME) = table_name_in;
      */
/* (20150907) Angel Ruiz . NF: Se crea una tabla de metadato MTDT_MODELO_SUMMARY y otra MTDT_MODELO_DETAIL */
  CURSOR c_mtdt_modelo_logico_TABLA
  IS
    SELECT 
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLESPACE) "TABLESPACE",
      TRIM(CI) "CI"
    FROM MTDT_MODELO_SUMMARY
    WHERE TRIM(CI) <> 'P';    /* Las que poseen un valor "P" en esta columna son las tablas de PERMITED_VALUES, por lo que no hya que generar su modelo */
    
 CURSOR c_mtdt_modelo_logico_COLUMNA (table_name_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(COLUMN_NAME) "COLUMN_NAME",
      DATA_TYPE,
      PK,
      TRIM(VDEFAULT) "VDEFAULT"
    FROM MTDT_MODELO_DETAIL
    WHERE
      TRIM(TABLE_NAME) = table_name_in;

/* (20150907) Angel Ruiz . FIN NF: Se crea una tabla de metadato MTDT_MODELO_SUMMARY y otra MTDT_MODELO_DETAIL */
  r_mtdt_modelo_logico_TABLA                                          c_mtdt_modelo_logico_TABLA%rowtype;
  r_mtdt_modelo_logico_COLUMNA                                    c_mtdt_modelo_logico_COLUMNA%rowtype;
  
  TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
  lista_pk                                      list_columns_primary := list_columns_primary (); 
  num_filas INTEGER; /* ALMACENAREMOS EL NUMERO DE FILAS DE LA TABLA MTDT_PERMITED_VALUES  */
  longitud_campo INTEGER;
  clave_foranea INTEGER;  /* 0 Si la tabla no tiene clave foranea. 1 si la tiene  */
  primera_col INTEGER;
  cadena_values VARCHAR2(500);
  concept_name VARCHAR2 (30);
  nombre_tabla_reducido VARCHAR2(30);
  pos_abre_paren PLS_integer;
  pos_cierra_paren PLS_integer;
  longitud_des varchar2(5);
  longitud_des_numerico PLS_integer;
  
  OWNER_SA                             VARCHAR2(60);
  OWNER_T                                VARCHAR2(60);
  OWNER_DM                            VARCHAR2(60);
  OWNER_MTDT                       VARCHAR2(60);
  TABLESPACE_DIM                VARCHAR2(60);
  
BEGIN

  /* (20141219) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  SELECT VALOR INTO TABLESPACE_DIM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'TABLESPACE_DIM';
  
  /* (20141219) FIN*/

  SELECT COUNT(*) INTO num_filas FROM MTDT_MODELO_SUMMARY;
  /* COMPROBAMOS QUE TENEMOS FILAS EN NUESTRA TABLA MTDT_MODELO_LOGICO  */
  IF num_filas > 0 THEN
    /* hay filas en la tabla y por lo tanto el proceso tiene cosas que hacer  */
    --DBMS_OUTPUT.put_line('set echo on;');
    --DBMS_OUTPUT.put_line('whenever sqlerror exit 1;');
    --DBMS_OUTPUT.put_line('');
    OPEN c_mtdt_modelo_logico_TABLA;
    LOOP
      /* COMENZAMOS EL BUCLE QUE GENERARA LOS CREATES PARA CADA UNA DE LAS TABLAS */
      FETCH c_mtdt_modelo_logico_TABLA
      INTO r_mtdt_modelo_logico_TABLA;
      EXIT WHEN c_mtdt_modelo_logico_TABLA%NOTFOUND;
      nombre_tabla_reducido := substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 5); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
      DBMS_OUTPUT.put_line('DROP TABLE IF EXISTS ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME || ' PURGE;');
      --DBMS_OUTPUT.put_line('');
      /***************************/
      /* Ahora creamos la tabla TEMPORAL pero solo para aquellas que no se van a cargar como carga inicial */
      if (r_mtdt_modelo_logico_TABLA.CI = 'N') then
        /* Aquellas que no tienen ningún tipo de carga inicial */
        DBMS_OUTPUT.put_line('DROP TABLE IF EXISTS ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ' PURGE;');
      end if;      
      --DBMS_OUTPUT.put_line('');
      
      /****************************************************************************************************/
      /* Viene la parte donde se generan los INSERTS por defecto y la SECUENCIA */
      /****************************************************************************************************/
      --if (r_mtdt_modelo_logico_TABLA.CI = 'N' or r_mtdt_modelo_logico_TABLA.CI = 'I') then
        /* Generamos los inserts para aquellas tablas que no son de carga inicial */
        --if (regexp_count(r_mtdt_modelo_logico_TABLA.TABLE_NAME,'^??D_',1,'i') >0 or regexp_count(r_mtdt_modelo_logico_TABLA.TABLE_NAME,'^DMT_',1,'i') >0
        --or regexp_count(r_mtdt_modelo_logico_TABLA.TABLE_NAME,'^DWD_',1,'i') >0) then
          /* Solo si se trata de una dimension generamos los inserts por defecto y la secuencia */
          --if (r_mtdt_modelo_logico_TABLA.CI = 'N') then
            --DBMS_OUTPUT.put_line('DROP SEQUENCE ' || OWNER_DM || '.SEQ_' || SUBSTR(r_mtdt_modelo_logico_TABLA.TABLE_NAME,5) || ';');
          --end if;
        --end if;
      --end if;
      /**********************/
      /**********************/
      
      
    END LOOP;
    CLOSE c_mtdt_modelo_logico_TABLA;
  END IF;
  DBMS_OUTPUT.put_line('');
  --DBMS_OUTPUT.put_line('set echo off;');
  --DBMS_OUTPUT.put_line('exit SUCCESS;');
  DBMS_OUTPUT.put_line('!quit');
  DBMS_OUTPUT.put_line('');
END;

