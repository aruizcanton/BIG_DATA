DECLARE
  /* (20150907) Angel Ruiz . NF: Se crea una tabla de metadato MTDT_MODELO_SUMMARY y otra MTDT_MODELO_DETAIL */

    /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR */
  CURSOR c_mtdt_modelo_logico_TABLA
  IS
    SELECT 
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(TABLESPACE) "TABLESPACE",
      TRIM(CI) "CI",
      TRIM(PARTICIONADO) "PARTICIONADO"
    FROM MTDT_MODELO_SUMMARY
    WHERE TRIM(CI) <> 'P';    /* Las que poseen un valor "P" en esta columna son las tablas de PERMITED_VALUES, por lo que no hya que generar su modelo */
    
  CURSOR c_mtdt_modelo_logico_COLUMNA (table_name_in IN VARCHAR2)
  IS
    SELECT 
      TRIM(TABLE_NAME) "TABLE_NAME",
      TRIM(COLUMN_NAME) "COLUMN_NAME",
      DATA_TYPE,
      PK,
      TRIM(NULABLE) "NULABLE",
      TRIM(VDEFAULT) "VDEFAULT",
      TRIM(INDICE) "INDICE"
    FROM MTDT_MODELO_DETAIL
    WHERE
      TABLE_NAME = table_name_in
    ORDER BY POSITION ASC;
  /* (20150907) Angel Ruiz . FIN NF: Se crea una tabla de metadato MTDT_MODELO_SUMMARY y otra MTDT_MODELO_DETAIL */

  r_mtdt_modelo_logico_TABLA                                          c_mtdt_modelo_logico_TABLA%rowtype;
  r_mtdt_modelo_logico_COLUMNA                                    c_mtdt_modelo_logico_COLUMNA%rowtype;
  
  TYPE list_columns_primary  IS TABLE OF VARCHAR(30);
  TYPE list_columns_indice  IS TABLE OF VARCHAR(30);
  TYPE list_columns_par  IS TABLE OF VARCHAR(30);
  lista_pk                                      list_columns_primary := list_columns_primary (); 
  lista_ind                                      list_columns_indice := list_columns_indice (); 
  lista_par                                      list_columns_par := list_columns_par (); 
  num_filas INTEGER; /* ALMACENAREMOS EL NUMERO DE FILAS DE LA TABLA MTDT_PERMITED_VALUES  */
  longitud_campo INTEGER;
  clave_foranea INTEGER;  /* 0 Si la tabla no tiene clave foranea. 1 si la tiene  */
  primera_col INTEGER;
  cadena_values VARCHAR2(2000);
  concept_name VARCHAR2 (30);
  nombre_tabla_reducido VARCHAR2(30);
  v_nombre_particion VARCHAR2(30);
  pos_abre_paren PLS_integer;
  pos_cierra_paren PLS_integer;
  longitud_des varchar2(5);
  longitud_des_numerico PLS_integer;
  v_tipo_particionado VARCHAR2(10);
  subset                                         VARCHAR(1);
  no_encontrado                          VARCHAR(1);
  longitud_tipo                           varchar2(10);
  

  
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
    OPEN c_mtdt_modelo_logico_TABLA;
    LOOP
      /* COMENZAMOS EL BUCLE QUE GENERARA LOS CREATES PARA CADA UNA DE LAS TABLAS */
      FETCH c_mtdt_modelo_logico_TABLA
      INTO r_mtdt_modelo_logico_TABLA;
      EXIT WHEN c_mtdt_modelo_logico_TABLA%NOTFOUND;
      nombre_tabla_reducido := substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 5); /* Le quito al nombre de la tabla los caracteres DMD_ o DMF_ */
      --DBMS_OUTPUT.put_line('DROP TABLE ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME || ' CASCADE CONSTRAINTS;');
      DBMS_OUTPUT.put_line('CREATE TABLE IF NOT EXISTS ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
      DBMS_OUTPUT.put_line('(');
      concept_name := substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 5);
      OPEN c_mtdt_modelo_logico_COLUMNA (r_mtdt_modelo_logico_TABLA.TABLE_NAME);
      primera_col := 1;
      v_tipo_particionado := 'S';  /* (20150821) Angel Ruiz. Por defecto la tabla no estara particionada */
      LOOP
        FETCH c_mtdt_modelo_logico_COLUMNA
        INTO r_mtdt_modelo_logico_COLUMNA;
        EXIT WHEN c_mtdt_modelo_logico_COLUMNA%NOTFOUND;
        /* COMENZAMOS EL BUCLE QUE GENERARA LAS COLUMNAS */
        /* (20160916) Angel Ruiz. En HIVE cuando una columna es por la que se particiona, */
        /* esta columna no puede formar aprte del create de los camos */
        if ((upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) <> 'CVE_DIA' and
        upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) <> 'CVE_MES') or
        (regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, 4), '??F_',1,'i') =0 and
        regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, 4), '??A_',1,'i') =0)) then
          IF primera_col = 1 THEN /* Si es primera columna */
            CASE
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) THEN
                /* (20160916) Angel Ruiz. Determino la longitud. */
                longitud_tipo := trim(substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '('), instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, ')') - instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')+1));
                DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL ' || longitud_tipo);
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'VARCHAR') > 0) THEN
                DBMS_OUTPUT.put_line (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'STRING');
              ELSE  /* se trata de Fecha  */
                DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE);
            END CASE;
            primera_col := 0;
          ELSE  /* si no es primera columna */
            CASE 
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) THEN
                /* (20160916) Angel Ruiz. Determino la longitud. */
                longitud_tipo := trim(substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '('), instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, ')') - instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')+1));
                DBMS_OUTPUT.put_line (', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL ' || longitud_tipo);
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'VARCHAR') > 0) THEN
                DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'STRING');
              ELSE  /* se trata de Fecha  */
                DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE);
            END CASE;
          END IF;
          IF upper(trim(r_mtdt_modelo_logico_COLUMNA.PK)) = 'S' then
            lista_pk.EXTEND;
            lista_pk(lista_pk.LAST) := r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
          END IF;
        end if;
        /* (20160916) FIN */
        /* (20150821) ANGEL RUIZ. FUNCIONALIDAD PARA PARTICIONADO */
        if (regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, 4), '??F_',1,'i') >0 AND
        upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) = 'CVE_DIA') then 
          /* SE TRATA DE UNA TABLA DE HECHOS CON COLUMNA CVE_DIA ==> PARTICIONADO DIARIO */
          v_tipo_particionado := 'D';   /* Particionado Diario */
          /* (20160324) Angel Ruiz. NF: Indices en las tablas del modelo*/
          lista_par.extend;
          lista_par(lista_par.last) := r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
          /* (20160324) Angel Ruiz. Fin NF: Indices en las tablas del modelo*/
        end if;
        /* Gestionamos el posible particionado de la tabla */
        if (regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, 4) ,'??F_',1,'i') >0 AND
        upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) = 'CVE_MES') then 
          /* SE TRATA DE UNA TABLA DE HECHOS CON COLUMNA CVE_DIA ==> PARTICIONADO MENSUAL */
          if (r_mtdt_modelo_logico_TABLA.PARTICIONADO = 'M24') then
            /* (20150918) Angel Ruiz. NF: Se trata del particionado para BSC. Mensual pero 24 Particiones fijas.*/
            /* La filosofia cambia */
              v_tipo_particionado := 'M24';   /* Particionado Mensual */
          else
            v_tipo_particionado := 'M';   /* Particionado Mensual, aunque para una tabla de Agregados*/
          end if;
          /* (20160324) Angel Ruiz. NF: Indices en las tablas del modelo*/
          lista_par.extend;
          lista_par(lista_par.last) := r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
          /* (20160324) Angel Ruiz. Fin NF: Indices en las tablas del modelo*/
        end if;
        if (regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.TABLE_NAME, 1, 4), '??A_',1,'i') >0 AND
        upper(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME) = 'CVE_MES') then
          /* SE TRATA DE UNA TABLA DE AGREGADOS CON PARTICIONAMIENTO POR MES */
          v_tipo_particionado := 'M';   /* Particionado Mensual, aunque para una tabla de Agregados*/
          /* (20160324) Angel Ruiz. NF: Indices en las tablas del modelo*/
          lista_par.extend;
          lista_par(lista_par.last) := r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
          /* (20160324) Angel Ruiz. Fin NF: Indices en las tablas del modelo*/
        end if;
        /* (20150821) ANGEL RUIZ. FIN FUNCIONALIDAD PARA PARTICIONADO */
      END LOOP; 
      CLOSE c_mtdt_modelo_logico_COLUMNA;
      DBMS_OUTPUT.put_line(')');  /* Parentesis final del create*/
      --DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_COLUMNA.TABLESPACE);
      if (regexp_count(substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 1, 4) ,'??F_',1,'i') >0)  then  /* Se trata de una tabla de HECHOS  */
        --  /* Hay que particonarla */
        if (v_tipo_particionado = 'D') then
          /* Se trata de un particionado diario */
          DBMS_OUTPUT.put_line('PARTITIONED BY (CVE_DIA INT)');
          --DBMS_OUTPUT.put_line(');');
        elsif (v_tipo_particionado = 'M') then
          /* Se trata de un particionado Mensual */
          DBMS_OUTPUT.put_line('PARTITION BY (CVE_MES INT)');
          --DBMS_OUTPUT.put_line('(');
        elsif (v_tipo_particionado = 'M24') then
          /* (20150918) Angel Ruiz. N.F.: Se trata de implementar el particionado para BSC donde hay 24 particiones siempre */
          /* Las particiones se crean una vez y asi permanecen ya que el espacio de analisis se extiende 24 meses */
          DBMS_OUTPUT.put_line('PARTITION BY (CVE_MES INT)');
          /* (20150918) Angel Ruiz. Fin N.F*/
        end if;
      elsif (regexp_count(substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 1, 4), '??A_',1,'i') >0)  then  /* Se trata de una tabla de HECHOS AGREGADOS  */
        if (v_tipo_particionado = 'M') then
          --  /* Hay que particonarla */
          DBMS_OUTPUT.put_line('PARTITION BY (CVE_MES INT)');
        end if;
      end if;
      IF lista_pk.COUNT > 0 THEN
        DBMS_OUTPUT.put_line('CLUSTERED BY (');
        FOR indx IN lista_pk.FIRST .. lista_pk.LAST
        LOOP
          IF indx = lista_pk.LAST THEN
            DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
          ELSE
            DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
          END IF;
        END LOOP;
        DBMS_OUTPUT.put_line('INTO 1 BUCKETS');
        DBMS_OUTPUT.put_line('STORED AS ORC TBLPROPERTIES (''transactional''=''true'', ''orc.compress''=''ZLIB'', ''orc.create.index''=''true'')');
      ELSE
        DBMS_OUTPUT.put_line('STORED AS ORC TBLPROPERTIES (''orc.compress''=''ZLIB'', ''orc.create.index''=''true'')');
      END IF;
      DBMS_OUTPUT.put_line (';');
      
      lista_pk.DELETE;      /* Borramos los elementos de la lista */
      lista_ind.delete;
      lista_par.delete;
      DBMS_OUTPUT.put_line('');
      /***************************/
      /* AHORA CREAMOS LA TABLA TEMPORAL PERO SOLO PARA AQUELLAS QUE NO SE VAN A CARGAR COMO CARGA INICIAL */
      if (r_mtdt_modelo_logico_TABLA.CI = 'N') then
        /* Aquellas que no tienen ningún tipo de carga inicial */
        --DBMS_OUTPUT.put_line('DROP TABLE ' || OWNER_DM || '.T_' || nombre_tabla_reducido || ' CASCADE CONSTRAINTS;');
        DBMS_OUTPUT.put_line('CREATE TABLE IF NOT EXISTS ' || OWNER_DM || '.T_' || nombre_tabla_reducido);
        DBMS_OUTPUT.put_line('(');
        concept_name := substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 5);
        OPEN c_mtdt_modelo_logico_COLUMNA (r_mtdt_modelo_logico_TABLA.TABLE_NAME);
        primera_col := 1;
        LOOP
          FETCH c_mtdt_modelo_logico_COLUMNA
          INTO r_mtdt_modelo_logico_COLUMNA;
          EXIT WHEN c_mtdt_modelo_logico_COLUMNA%NOTFOUND;
          /* COMENZAMOS EL BUCLE QUE GENERARA LAS COLUMNAS */
          IF primera_col = 1 THEN /* Si es primera columna */
            CASE
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) THEN
                /* (20160916) Angel Ruiz. Determino la longitud. */
                longitud_tipo := trim(substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '('), instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, ')') - instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')+1));
                DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL ' || longitud_tipo);
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'VARCHAR') > 0) THEN
                DBMS_OUTPUT.put_line (r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'STRING');
              ELSE  /* se trata de Fecha  */
                DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE);
            END CASE;
            primera_col := 0;
          ELSE  /* si no es primera columna */
            CASE 
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) THEN
                /* (20160916) Angel Ruiz. Determino la longitud. */
                longitud_tipo := trim(substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '('), instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, ')') - instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, '(')+1));
                DBMS_OUTPUT.put_line (', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'DECIMAL ' || longitud_tipo);
              WHEN (INSTR(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'VARCHAR') > 0) THEN
                DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || 'STRING');
              ELSE  /* se trata de Fecha  */
                DBMS_OUTPUT.put_line(', ' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME || '          ' || r_mtdt_modelo_logico_COLUMNA.DATA_TYPE);
              END CASE;
          END IF;
          IF upper(trim(r_mtdt_modelo_logico_COLUMNA.PK)) = 'S' then
            lista_pk.EXTEND;
            lista_pk(lista_pk.LAST) := r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
          END IF;
          /* (20160324) Angel Ruiz. NF: INdices en el modelo */
          --IF upper(trim(r_mtdt_modelo_logico_COLUMNA.INDICE)) = 'S' then
            --lista_ind.EXTEND;
            --lista_ind(lista_ind.LAST) := r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME;
          --END IF;
        END LOOP; 
        CLOSE c_mtdt_modelo_logico_COLUMNA;
        DBMS_OUTPUT.put_line(')');  /* Parentesis final del create */
        IF lista_pk.COUNT > 0 THEN
          DBMS_OUTPUT.put_line('CLUSTERED BY (');
          FOR indx IN lista_pk.FIRST .. lista_pk.LAST
          LOOP
            IF indx = lista_pk.LAST THEN
              DBMS_OUTPUT.put_line(lista_pk (indx) || ') ');
            ELSE
              DBMS_OUTPUT.put_line(lista_pk (indx) || ',');
            END IF;
          END LOOP;
          DBMS_OUTPUT.put_line('INTO 1 BUCKETS');
          DBMS_OUTPUT.put_line('STORED AS ORC TBLPROPERTIES (''transactional''=''true'', ''orc.compress''=''ZLIB'', ''orc.create.index''=''true'')');
        ELSE
          DBMS_OUTPUT.put_line('STORED AS ORC TBLPROPERTIES (''orc.compress''=''ZLIB'', ''orc.create.index''=''true'')');
        END IF;
        DBMS_OUTPUT.put_line (';');
        --if (r_mtdt_modelo_logico_TABLA.TABLESPACE is not null) then
          --DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_TABLA.TABLESPACE || ';');
        --else
          --DBMS_OUTPUT.put_line(';');
        --end if;
        /* (20160324) Angel Ruiz. NF: Indices en las tablas del modelo */
        --if (lista_ind.COUNT > 0) then
          --DBMS_OUTPUT.put_line('CREATE INDEX T_' || nombre_tabla_reducido || '_I ON ' || OWNER_DM || '.T_' || nombre_tabla_reducido);
          --DBMS_OUTPUT.put_line('(');
          --FOR indx IN lista_ind.FIRST .. lista_ind.LAST
          --LOOP
            --IF indx = lista_ind.LAST THEN
              --DBMS_OUTPUT.put_line(lista_ind (indx) || ') ');
            --ELSE
              --DBMS_OUTPUT.put_line(lista_ind (indx) || ',');
            --END IF;
          --END LOOP;
          --DBMS_OUTPUT.put_line('NOLOGGING;');
        --end if;
        /* (20160324) Angel Ruiz. Fin NF:Indices en las tablas del modelo */
        
        --if (r_mtdt_modelo_logico_TABLA.TABLE_NAME='DMF_PARQUE_MVNO') then
        --  DBMS_OUTPUT.put_line('TABLESPACE ' || 'DWTBSP_D_MVNO_PARQUE;');
        --elsif (r_mtdt_modelo_logico_TABLA.TABLE_NAME='DMF_RECARGAS_MVNO') then
        --  DBMS_OUTPUT.put_line('TABLESPACE ' || 'DWTBSP_D_MVNO_AJUSTES;');
        --elsif (r_mtdt_modelo_logico_TABLA.TABLE_NAME='DMF_TRAFD_CU_MVNO;') then
        --  DBMS_OUTPUT.put_line('TABLESPACE ' || 'DWTBSP_D_MVNO_TRAFD;');
        --elsif (r_mtdt_modelo_logico_TABLA.TABLE_NAME='DMF_TRAFE_CU_MVNO') then
        --  DBMS_OUTPUT.put_line('TABLESPACE ' || 'DWTBSP_D_MVNO_TRAFE;');
        --elsif (r_mtdt_modelo_logico_TABLA.TABLE_NAME='DMF_TRAFV_CU_MVNO;') then
        --  DBMS_OUTPUT.put_line('TABLESPACE ' || 'DWTBSP_D_MVNO_TRAFV;');
        --else
        --  DBMS_OUTPUT.put_line('TABLESPACE ' || 'DWTBSP_D_MVNO_DIM' || ';');
        --end if;
      end if;      
      --DBMS_OUTPUT.put_line('TABLESPACE ' || r_mtdt_modelo_logico_COLUMNA.TABLESPACE || ';'); 
      lista_pk.DELETE;      /* Borramos los elementos de la lista */
      lista_ind.DELETE;
      DBMS_OUTPUT.put_line('');
      
      /****************************************************************************************************/
      /* Viene la parte donde se generan los INSERTS por defecto y la SECUENCIA */
      /****************************************************************************************************/
      /* (20150826) ANGEL RUIZ. Cambio la creacion de la secuencia para que se cree secuencia para todas las tablas DIMENSIONES o HECHOS */
      --if (r_mtdt_modelo_logico_TABLA.CI = 'N') then
        --DBMS_OUTPUT.put_line('DROP SEQUENCE ' || OWNER_DM || '.SEQ_' || SUBSTR(r_mtdt_modelo_logico_TABLA.TABLE_NAME,5) || ';');
        --DBMS_OUTPUT.put_line('CREATE SEQUENCE ' || OWNER_DM || '.SEQ_' || SUBSTR(r_mtdt_modelo_logico_TABLA.TABLE_NAME,5));
        --DBMS_OUTPUT.put_line('MINVALUE 1 START WITH 1 INCREMENT BY 1;');
        --DBMS_OUTPUT.put_line('');        
      --end if;
      
      if (r_mtdt_modelo_logico_TABLA.CI = 'N' or r_mtdt_modelo_logico_TABLA.CI = 'I') then
        /* Generamos los inserts para aquellas tablas que no son de carga inicial */
        if (regexp_count(substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 1, 4) ,'??D_',1,'i') >0 or regexp_count(substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 1, 4), 'DMT_',1,'i') >0 
        or regexp_count(substr(r_mtdt_modelo_logico_TABLA.TABLE_NAME, 1, 4), 'DWD_',1,'i') >0) then
          /* Solo si se trata de una dimension generamos los inserts por defecto y la secuencia */
          --if (r_mtdt_modelo_logico_TABLA.CI = 'N') then
            --DBMS_OUTPUT.put_line('CREATE SEQUENCE ' || OWNER_DM || '.SEQ_' || SUBSTR(r_mtdt_modelo_logico_TABLA.TABLE_NAME,5));
            --DBMS_OUTPUT.put_line('MINVALUE 1 START WITH 1 INCREMENT BY 1;');
          --end if;
          DBMS_OUTPUT.put_line('');        
          /* Primero el INSERT "NO APLICA" */
          DBMS_OUTPUT.put_line('INSERT INTO ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          --DBMS_OUTPUT.put_line('(');
          OPEN c_mtdt_modelo_logico_COLUMNA (r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          primera_col := 1;
          cadena_values := '';
          LOOP
            FETCH c_mtdt_modelo_logico_COLUMNA
            INTO r_mtdt_modelo_logico_COLUMNA;
            EXIT WHEN c_mtdt_modelo_logico_COLUMNA%NOTFOUND;
    
            IF primera_col = 1 THEN /* Si es primera columna */
                --DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
                CASE
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'CVE_',1,'i') >0 THEN
                    cadena_values := '-1';
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 3), 'ID_',1,'i') >0 THEN
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                      cadena_values := '-1';
                    else
                      if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0) then
                          cadena_values := 'N';
                        elsif (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                          cadena_values := 'NA';
                        else
                          cadena_values := 'NULL';
                        end if;
                      else
                        cadena_values := '''NA#''';
                      end if;
                    end if;
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'DES_',1,'i') >0 THEN
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 8) then
                      cadena_values := '''NO APLICA''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := '''NA#''';
                    else
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                        case 
                          when longitud_des_numerico = 2 then
                            cadena_values := '''NA''';
                          when longitud_des_numerico = 1 then
                            cadena_values := '''N''';
                        end case;
                      else
                        cadena_values := 'NULL';
                      end if;
                    end if;
                  WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'FCH_',1,'i') >0 THEN
                      cadena_values := 'current_date';
                  ELSE
                    /* (20150118) Angel Ruiz. BUG: Se han de incluir valores en lugar de NULL */
                  --if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                    if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                      cadena_values := '-1';
                    elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                      cadena_values := 'current_date';
                    else
                      /* VARCHAR */
                      pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                      pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                      longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                      longitud_des_numerico := to_number(longitud_des);
                      if (longitud_des_numerico > 8) then
                        cadena_values := '''NO APLICA''';
                      elsif (longitud_des_numerico > 2) then
                        cadena_values := '''NA#''';
                      else
                        cadena_values := '''N''';
                      end if;
                    end if;
                  --else
                    --cadena_values := 'NULL';
                  --end if;
                END CASE;  
                primera_col := 0;
            ELSE  /* si no es primera columna */
              --DBMS_OUTPUT.put_line(',' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
              CASE
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'CVE_',1,'i') >0 THEN
                  cadena_values := cadena_values || ', -1';
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 3),'ID_',1,'i') >0 THEN
                  if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0 ) then
                    cadena_values := cadena_values || ', -1';
                  else
                      if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0) then
                          cadena_values := cadena_values || ', ''N''';
                        elsif (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                          cadena_values := cadena_values || ', ''NA''';
                        else
                          cadena_values := cadena_values || ', NULL';
                        end if;
                      else
                        cadena_values := cadena_values || ', ''NA#''';
                      end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'DES_',1,'i') >0 THEN
                  pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                  pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                  longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                  longitud_des_numerico := to_number(longitud_des);
                  if (longitud_des_numerico > 8) then
                    cadena_values := cadena_values || ', ''NO APLICA''';
                  elsif (longitud_des_numerico > 2) then
                    cadena_values := cadena_values || ', ''NA#''';
                  else
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      case 
                        when longitud_des_numerico = 2 then
                          cadena_values := cadena_values || ', ''NA''';
                        when longitud_des_numerico = 1 then
                          cadena_values := cadena_values || ', ''N''';
                      end case;
                    else
                      cadena_values := cadena_values || ', NULL';
                    end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'FCH_',1,'i') >0 THEN
                    cadena_values := cadena_values || ', current_date';
                ELSE
                  /* (20150118) Angel Ruiz. BUG: Se han de incluir valores en lugar de NULL */
                  --if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                    cadena_values := cadena_values || ', -1';
                  elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                    cadena_values := cadena_values || ', current_date';
                  else
                    /* VARCHAR */
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 8) then
                      cadena_values := cadena_values || ', ''NO APLICA''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := cadena_values || ', ''NA#''';
                    else
                      cadena_values := cadena_values || ', ''N''';
                    end if;
                  end if;
                  --else
                  --cadena_values := cadena_values || ', NULL';
                  --end if;
              END CASE;  
            END IF;
          END LOOP; 
          --DBMS_OUTPUT.put_line(')');
          --DBMS_OUTPUT.put_line('VALUES');
          DBMS_OUTPUT.put_line('select');
          --DBMS_OUTPUT.put_line('(' || cadena_values || ');');
          DBMS_OUTPUT.put_line(cadena_values || ' from ' || OWNER_MTDT ||'.dual;');
          CLOSE c_mtdt_modelo_logico_COLUMNA;
          /* Siguiente insert "GENERICO" */
          DBMS_OUTPUT.put_line('INSERT INTO ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          --DBMS_OUTPUT.put_line('(');
          OPEN c_mtdt_modelo_logico_COLUMNA (r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          primera_col := 1;
          cadena_values := '';
          LOOP
            FETCH c_mtdt_modelo_logico_COLUMNA
            INTO r_mtdt_modelo_logico_COLUMNA;
            EXIT WHEN c_mtdt_modelo_logico_COLUMNA%NOTFOUND;
    
            IF primera_col = 1 THEN /* Si es primera columna */
              --DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
              CASE
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4),'CVE_',1,'i') >0 THEN
                  cadena_values := '-2';
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 3), 'ID_',1,'i') >0 THEN
                  if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                    cadena_values := '-2';
                  else
                      if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0) then
                          cadena_values := '''G''';
                        elsif (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                          cadena_values := '''GE''';
                        else
                          cadena_values := 'NULL';
                        end if;
                      else
                        cadena_values := '''GE#''';
                      end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4),'DES_',1,'i') >0 THEN
                  pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                  pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                  longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                  longitud_des_numerico := to_number(longitud_des);
                  if (longitud_des_numerico > 7) then
                    cadena_values := '''GENERICO''';
                  elsif (longitud_des_numerico > 2) then
                    cadena_values := '''GE#''';
                  else
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      case 
                        when longitud_des_numerico = 2 then
                          cadena_values := '''GE''';
                        when longitud_des_numerico = 1 then
                          cadena_values := '''G''';
                      end case;
                    else
                      cadena_values := 'NULL';
                    end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'FCH_',1,'i') >0 THEN
                    cadena_values := 'current_date';
                ELSE
                  /* (20150118) Angel Ruiz. BUG: Se han de incluir valores en lugar de NULL */
                --if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                    cadena_values := '-2';
                  elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                    cadena_values := 'current_date';
                  else
                    /* VARCHAR */
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 7) then
                      cadena_values := '''GENERICO''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := '''GE#''';
                    else
                      cadena_values := '''G''';
                    end if;
                  end if;
                  --else
                    --cadena_values := 'NULL';
                  --end if;
              END CASE;  
              primera_col := 0;
            ELSE  /* si no es primera columna */
              --DBMS_OUTPUT.put_line(',' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
              CASE
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'CVE_',1,'i') >0 THEN
                  cadena_values := cadena_values || ', -2';
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 3), 'ID_',1,'i') >0 THEN
                  if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                    cadena_values := cadena_values || ', -2';
                  else
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0) then
                        cadena_values := cadena_values || ', ''G''';
                      elsif (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        cadena_values := cadena_values || ', ''GE''';
                      else
                        cadena_values := cadena_values || ', NULL';
                      end if;
                    else
                      cadena_values := cadena_values || ', ''GE#''';
                    end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'DES_',1,'i') >0 THEN
                  pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                  pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                  longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                  longitud_des_numerico := to_number(longitud_des);
                  if (longitud_des_numerico > 7) then
                    cadena_values := cadena_values || ', ''GENERICO''';
                  elsif (longitud_des_numerico > 2) then
                    cadena_values := cadena_values || ', ''GE#''';
                  else
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      case 
                        when longitud_des_numerico = 2 then
                          cadena_values := cadena_values || ', ''GE''';
                        when longitud_des_numerico = 1 then
                          cadena_values := cadena_values || ', ''G''';
                      end case;
                    else
                      cadena_values := cadena_values || ', NULL';
                    end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'FCH_',1,'i') >0 THEN
                    cadena_values := cadena_values || ', current_date';
                ELSE
                  /* (20160118) ANGEL RUIZ. BUG. Deben aparecer siempre valores en lugar de NULL */
                  --if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                    cadena_values := cadena_values || ', -2';
                  elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                    cadena_values := cadena_values || ', current_date';
                  else
                    /* VARCHAR */
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 7) then
                      cadena_values := cadena_values || ', ''GENERICO''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := cadena_values || ', ''GE#''';
                    else
                      cadena_values := cadena_values || ', ''G''';
                    end if;
                  end if;
                  --else
                  --  cadena_values := cadena_values || ', NULL';
                  --end if;
              END CASE;  
            END IF;
          END LOOP; 
          --DBMS_OUTPUT.put_line(')');
          --DBMS_OUTPUT.put_line('VALUES');
          DBMS_OUTPUT.put_line('select');
          --DBMS_OUTPUT.put_line('(' || cadena_values || ');');
          DBMS_OUTPUT.put_line(cadena_values || ' from ' || OWNER_MTDT || '.dual;');
          CLOSE c_mtdt_modelo_logico_COLUMNA;
          /* Siguiente INSERT "NO INFORMADO" */
          DBMS_OUTPUT.put_line('INSERT INTO ' || OWNER_DM || '.' || r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          --DBMS_OUTPUT.put_line('(');
          OPEN c_mtdt_modelo_logico_COLUMNA (r_mtdt_modelo_logico_TABLA.TABLE_NAME);
          primera_col := 1;
          cadena_values := '';
          LOOP
            FETCH c_mtdt_modelo_logico_COLUMNA
            INTO r_mtdt_modelo_logico_COLUMNA;
            EXIT WHEN c_mtdt_modelo_logico_COLUMNA%NOTFOUND;
    
            IF primera_col = 1 THEN /* Si es primera columna */
              --DBMS_OUTPUT.put_line(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
              CASE
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'CVE_',1,'i') >0 THEN
                  cadena_values := '-3';
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 3), 'ID_',1,'i') >0 THEN
                  if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                    cadena_values :=  '-3';
                  else
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0) then
                        cadena_values := '''N''';
                      elsif (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        cadena_values := '''NI''';
                      else
                        cadena_values := 'NULL';
                      end if;
                    else
                      cadena_values := '''NI#''';
                    end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'DES_',1,'i') >0 THEN
                  pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                  pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                  longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                  longitud_des_numerico := to_number(longitud_des);
                  if (longitud_des_numerico > 11) then
                    cadena_values := '''NO INFORMADO''';
                  elsif (longitud_des_numerico > 2) then
                    cadena_values := '''NI#''';
                  else
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      case 
                        when longitud_des_numerico = 2 then
                          cadena_values := '''NI''';
                        when longitud_des_numerico = 1 then
                          cadena_values := '''N''';
                      end case;
                    else
                      cadena_values := 'NULL';
                    end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'FCH_',1,'i') >0 THEN
                    cadena_values := 'current_date';
                ELSE
                  /* (20160118) ANGEL RUIZ. BUG. Deben aparecer siempre valores en lugar de NULL */
                  --if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                    cadena_values := '-3';
                  elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                    cadena_values := 'current_date';
                  else
                    /* VARCHAR */
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 11) then
                      cadena_values := '''NO INFORMADO''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := '''NI#''';
                    else
                      cadena_values := '''N''';
                    end if;
                  end if;
                  --else
                    --cadena_values := 'NULL';
                  --end if;
              END CASE;  
              primera_col := 0;
            ELSE  /* si no es primera columna */
              --DBMS_OUTPUT.put_line(',' || r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME);
              CASE
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'CVE_',1,'i') >0 THEN
                  cadena_values := cadena_values || ', -3';
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 3), 'ID_',1,'i') >0 THEN
                  if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'NUMBER') > 0) then
                    cadena_values := cadena_values || ', -3';
                  else
                    if (instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0 or instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                      if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(1)') > 0) then
                        cadena_values := cadena_values || ', ''N''';
                      elsif (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N' and instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(2)') > 0) then
                        cadena_values := cadena_values || ', ''NI''';
                      else
                        cadena_values := cadena_values || ', NULL';
                      end if;
                    else                  
                      cadena_values := cadena_values || ', ''NI#''';
                    end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4), 'DES_',1,'i') >0 THEN
                  pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                  pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                  longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                  longitud_des_numerico := to_number(longitud_des);
                  if (longitud_des_numerico > 11) then
                    cadena_values := cadena_values || ', ''NO INFORMADO''';
                  elsif (longitud_des_numerico > 2) then
                    cadena_values := cadena_values || ', ''NI#''';
                  else
                    if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                      case 
                        when longitud_des_numerico = 2 then
                          cadena_values := cadena_values || ', ''NA''';
                        when longitud_des_numerico = 1 then
                          cadena_values := cadena_values || ', ''N''';
                      end case;
                    else
                      cadena_values := cadena_values || ', NULL';
                    end if;
                  end if;
                WHEN regexp_count(substr(r_mtdt_modelo_logico_COLUMNA.COLUMN_NAME, 1, 4),'FCH_',1,'i') >0 THEN
                    cadena_values := cadena_values || ', current_date';
                ELSE
                  /* (20160118) ANGEL RUIZ. BUG. Deben aparecer siempre valores en lugar de NULL */
                  --if (r_mtdt_modelo_logico_COLUMNA.NULABLE = 'N') then
                  if (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'NUMBER') > 0) then
                    cadena_values := cadena_values || ', -3';
                  elsif (regexp_count(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, 'DATE') > 0) then
                    cadena_values := cadena_values || ', current_date';
                  else
                    /* VARCHAR */
                    pos_abre_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,'(');
                    pos_cierra_paren := instr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE,')');
                    longitud_des := substr(r_mtdt_modelo_logico_COLUMNA.DATA_TYPE, pos_abre_paren+1, (pos_cierra_paren - pos_abre_paren)-1);
                    longitud_des_numerico := to_number(longitud_des);
                    if (longitud_des_numerico > 11) then
                      cadena_values := cadena_values || ', ''NO INFORMADO''';
                    elsif (longitud_des_numerico > 2) then
                      cadena_values := cadena_values || ', ''NI#''';
                    else
                      cadena_values := cadena_values || ', ''N''';
                    end if;
                  end if;
                  --else
                  --cadena_values := cadena_values || ', NULL';
                  --end if;
              END CASE;  
            END IF;
          END LOOP; 
          --DBMS_OUTPUT.put_line(')');
          --DBMS_OUTPUT.put_line('VALUES');
          DBMS_OUTPUT.put_line('select');
          --DBMS_OUTPUT.put_line('(' || cadena_values || ');');
          DBMS_OUTPUT.put_line(cadena_values || ' from ' || OWNER_MTDT || '.dual;');
          --DBMS_OUTPUT.put_line('commit;');
          DBMS_OUTPUT.put_line('');
          CLOSE c_mtdt_modelo_logico_COLUMNA;
        end if;
      end if;
      /**********************/
      /**********************/
      
      
    END LOOP;
    CLOSE c_mtdt_modelo_logico_TABLA;
  END IF;
  --DBMS_OUTPUT.put_line('set echo off;');
  --DBMS_OUTPUT.put_line('exit SUCCESS;');
  DBMS_OUTPUT.put_line('!quit');
END;

