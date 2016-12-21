DECLARE
  /* CURSOR QUE NOS DARA TODAS LAS TABLAS QUE HAY QUE CREAR */
  CURSOR dtd_permited_values
  IS
    SELECT 
      trim(ITEM_NAME) "ITEM_NAME",
      trim(ID_LIST) "ID_LIST",
      trim(AGREGATION) "AGREGATION",
      MAX(LENGTH(VALUE)) LONGITUD,
      MAX(LENGTH(DESCRIPTION)) LONGITUD_DES
    FROM MTDT_PERMITED_VALUES
    --WHERE ITEM_NAME not in ('ALMACEN')
    WHERE ITEM_NAME NOT IN      /*(20151125) Angel Ruiz. Para que no se generen los creates que ya se generan en modelo logico */
    (select trim(substr(table_name,5)) from mtdt_modelo_summary where CI = 'I')
    GROUP BY 
      trim(ITEM_NAME),
      trim(ID_LIST),
      trim(AGREGATION)
      ORDER BY trim(ID_LIST);
  reg_per_val dtd_permited_values%rowtype;
  num_filas INTEGER; /* ALMACENAREMOS EL NUMERO DE FILAS DE LA TABLA MTDT_PERMITED_VALUES  */
  longitud_campo INTEGER;
  longitud_campo_des INTEGER;
  clave_foranea INTEGER;  /* 0 Si la tabla no tiene clave foranea. 1 si la tiene  */
  
  OWNER_SA                             VARCHAR2(60);
  OWNER_T                                VARCHAR2(60);
  OWNER_DM                            VARCHAR2(60);
  OWNER_MTDT                       VARCHAR2(60);
  TABLESPACE_DIM                VARCHAR2(60);
  PREFIJO_DM                            VARCHAR2(60);
  
BEGIN
  /* (20141219) ANGEL RUIZ*/
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  SELECT VALOR INTO TABLESPACE_DIM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'TABLESPACE_DIM';
  SELECT VALOR INTO PREFIJO_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'PREFIJO_DM';
  /* (20141219) FIN*/

  SELECT COUNT(*) INTO num_filas FROM MTDT_PERMITED_VALUES;
  /* COMPROBAMOS QUE TENEMOS FILAS EN NUESTRA TABLA MTDT_PERMITED_VALUES  */
  IF num_filas > 0 THEN
    /* hay filas en la tabla y por lo tanto el proceso tiene cosas que hacer  */
    DBMS_OUTPUT.put_line('set echo on;');
    DBMS_OUTPUT.put_line('whenever sqlerror exit 1;');
    OPEN dtd_permited_values;
    LOOP
      /* COMENZAMOS EL BUCLE QUE GENERARA LOS CREATES PARA CADA UNA DE LAS TABLAS */
      FETCH dtd_permited_values
      INTO reg_per_val;
      EXIT WHEN dtd_permited_values%NOTFOUND;
      clave_foranea :=0;
      --DBMS_OUTPUT.put_line('');
      --DBMS_OUTPUT.put_line('DROP TABLE ' || OWNER_DM || '.' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || ' CASCADE CONSTRAINTS;');
      DBMS_OUTPUT.put_line('');
      --DBMS_OUTPUT.put_line('CREATE TABLE IF NOT EXISTS ' || OWNER_DM || '.' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME);
      DBMS_OUTPUT.put_line('INSERT INTO MTDT_CAMPOS_DETAIL (TABLE_NAME, COLUMN_NAME, TYPE, LENGTH, NULABLE) VALUES ');
      DBMS_OUTPUT.put_line('(');
      DBMS_OUTPUT.put_line('''' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || ''',');
      DBMS_OUTPUT.put_line('''' || 'CVE_' || reg_per_val.ITEM_NAME || ''',');
      DBMS_OUTPUT.put_line('''' || 'NUMBER' || ''',');
      DBMS_OUTPUT.put_line('''' || '10' || ''',');
      DBMS_OUTPUT.put_line('''' || 'N' || '''');
      DBMS_OUTPUT.put_line(');');
      --DBMS_OUTPUT.put_line('  CVE_' || reg_per_val.ITEM_NAME || '          DECIMAL(10),');
      if (reg_per_val.ITEM_NAME = 'PAIS_TM' or reg_per_val.ITEM_NAME = 'MONEDA') then
        longitud_campo:=5;
      elsif (reg_per_val.LONGITUD<10) THEN
        longitud_campo:=10;
      else
        longitud_campo:=reg_per_val.LONGITUD;
      end if;
      if (reg_per_val.LONGITUD_DES < 48) then
        longitud_campo_des:=50;
      else
        longitud_campo_des:= reg_per_val.LONGITUD_DES+2;
      end if;
        
      --DBMS_OUTPUT.put_line('  ID_' || reg_per_val.ITEM_NAME || '          VARCHAR2(' || longitud_campo || '),');
      DBMS_OUTPUT.put_line('INSERT INTO MTDT_CAMPOS_DETAIL (TABLE_NAME, COLUMN_NAME, TYPE, LENGTH, NULABLE) VALUES ');
      DBMS_OUTPUT.put_line('(');
      DBMS_OUTPUT.put_line('''' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || ''',');
      DBMS_OUTPUT.put_line('''' || 'ID_' || reg_per_val.ITEM_NAME || ''',');
      DBMS_OUTPUT.put_line('''' || 'VARCHAR2' || ''',');
      DBMS_OUTPUT.put_line('''' || longitud_campo || ''',');
      DBMS_OUTPUT.put_line('''' || 'N' || '''');
      DBMS_OUTPUT.put_line(');');
      --DBMS_OUTPUT.put_line('  DES_' || reg_per_val.ITEM_NAME || '          VARCHAR2(' || longitud_campo_des || '),');
      DBMS_OUTPUT.put_line('INSERT INTO MTDT_CAMPOS_DETAIL (TABLE_NAME, COLUMN_NAME, TYPE, LENGTH, NULABLE) VALUES ');
      DBMS_OUTPUT.put_line('(');
      DBMS_OUTPUT.put_line('''' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || ''',');
      DBMS_OUTPUT.put_line('''' || 'DES_' || reg_per_val.ITEM_NAME || ''',');
      DBMS_OUTPUT.put_line('''' || 'VARCHAR2' || ''',');
      DBMS_OUTPUT.put_line('''' || longitud_campo_des || ''',');
      DBMS_OUTPUT.put_line('''' || 'N' || '''');
      DBMS_OUTPUT.put_line(');');
      --DBMS_OUTPUT.put_line('  ID_LIST' || '          VARCHAR2(5),');
      DBMS_OUTPUT.put_line('INSERT INTO MTDT_CAMPOS_DETAIL (TABLE_NAME, COLUMN_NAME, TYPE, LENGTH, NULABLE) VALUES ');
      DBMS_OUTPUT.put_line('(');
      DBMS_OUTPUT.put_line('''' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || ''',');
      DBMS_OUTPUT.put_line('''' || 'ID_LIST' || ''',');
      DBMS_OUTPUT.put_line('''' || 'VARCHAR2' || ''',');
      DBMS_OUTPUT.put_line('''' || '5' || ''',');
      DBMS_OUTPUT.put_line('''' || 'N' || '''');
      DBMS_OUTPUT.put_line(');');
      if (reg_per_val.ITEM_NAME <> 'FUENTE') then  /* Esto lo pongo a posteriori porque ha aparecido un ITEM que se llama precisamente fuente. Para no repetir campos*/
        /* En el caso de que el ITEM sea FUENTE este campo no se incluye ya que ya estaria arriba */
        --DBMS_OUTPUT.put_line('  ID_FUENTE' || '          VARCHAR2(10),');
        DBMS_OUTPUT.put_line('INSERT INTO MTDT_CAMPOS_DETAIL (TABLE_NAME, COLUMN_NAME, TYPE, LENGTH, NULABLE) VALUES ');
        DBMS_OUTPUT.put_line('(');
        DBMS_OUTPUT.put_line('''' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || ''',');
        DBMS_OUTPUT.put_line('''' || 'ID_FUENTE' || ''',');
        DBMS_OUTPUT.put_line('''' || 'VARCHAR2' || ''',');
        DBMS_OUTPUT.put_line('''' || '10' || ''',');
        DBMS_OUTPUT.put_line('''' || 'N' || '''');
        DBMS_OUTPUT.put_line(');');
        --DBMS_OUTPUT.put_line('  ID_FUENTE' || '          STRING,');
      end if;
      --if (reg_per_val.ITEM_NAME = 'TIPO_ENVIO') then  /* Esto lo pongo a posteriori porque ha aparecido un ITEM que se llama precisamente fuente. Para no repetir campos*/
      -- /* En el caso de que el ITEM sea FUENTE este campo no se incluye ya que ya estaria arriba */
      --  DBMS_OUTPUT.put_line('  BAN_ON_NET' || '          VARCHAR2(2),');
      --  DBMS_OUTPUT.put_line('  BAN_TEMM' || '          VARCHAR2(2),');
      --  DBMS_OUTPUT.put_line('  BAN_RED' || '          VARCHAR2(5),');
      --end if;
      /* HAY VALOR EN LA COLUMNA AGREGATION  */
      IF (regexp_count(reg_per_val.AGREGATION,'^CVE_',1,'i') >0) THEN
        /* posee agregacion con clave foranea a otra dimension */
        clave_foranea := 1;
        DBMS_OUTPUT.put_line('INSERT INTO MTDT_CAMPOS_DETAIL (TABLE_NAME, COLUMN_NAME, TYPE, LENGTH, NULABLE) VALUES ');
        DBMS_OUTPUT.put_line('(');
        DBMS_OUTPUT.put_line('''' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || ''',');
        DBMS_OUTPUT.put_line('''CVE_' || substr(reg_per_val. AGREGATION,5) || ''',');
        DBMS_OUTPUT.put_line('''' || 'NUMBER' || ''',');
        DBMS_OUTPUT.put_line('''' || '10' || ''',');
        DBMS_OUTPUT.put_line('''' || 'N' || '''');
        DBMS_OUTPUT.put_line(');');
        --DBMS_OUTPUT.put_line('  CVE_' ||  substr(reg_per_val. AGREGATION,5) || ' DECIMAL(10),');       
      ELSIF (regexp_count(reg_per_val.AGREGATION,'^BAN_',1,'i') >0)          
      THEN
        /* se crea el campo nomas */            
        --DBMS_OUTPUT.put_line(reg_per_val. AGREGATION || '          VARCHAR2(1),');
        DBMS_OUTPUT.put_line('INSERT INTO MTDT_CAMPOS_DETAIL (TABLE_NAME, COLUMN_NAME, TYPE, LENGTH, NULABLE) VALUES ');
        DBMS_OUTPUT.put_line('(');
        DBMS_OUTPUT.put_line('''' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || ''',');
        DBMS_OUTPUT.put_line('''CVE_' || substr(reg_per_val. AGREGATION,5) || ''',');
        DBMS_OUTPUT.put_line('''' || 'VARCHAR2' || ''',');
        DBMS_OUTPUT.put_line('''' || '1' || ''',');
        DBMS_OUTPUT.put_line('''' || 'N' || '''');
        DBMS_OUTPUT.put_line(');');
        --DBMS_OUTPUT.put_line(reg_per_val. AGREGATION || '          STRING,');
      END IF;
      /*FIN TRATAMIENTO VALOR COLUMNA AGREGATION*/
      /* Cambio efectuado el 21/10/2014 */
      IF (regexp_count(reg_per_val.ITEM_NAME,'^RANGO',1,'i') > 0) THEN
      /* Se trata de una tabla del tipo DMD_RANGO_ */
      /* por lo que ha de tener dos campos mas  */
        --DBMS_OUTPUT.put_line('  MIN_' || reg_per_val.ITEM_NAME || '          NUMBER(' || 5 || '),');
        --DBMS_OUTPUT.put_line('  MAX_' || reg_per_val.ITEM_NAME || '          NUMBER(' || 5 || '),');
        DBMS_OUTPUT.put_line('INSERT INTO MTDT_CAMPOS_DETAIL (TABLE_NAME, COLUMN_NAME, TYPE, LENGTH, NULABLE) VALUES ');
        DBMS_OUTPUT.put_line('(');
        DBMS_OUTPUT.put_line('''' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || ''',');
        DBMS_OUTPUT.put_line('''MIN_' || reg_per_val.ITEM_NAME || ''',');
        DBMS_OUTPUT.put_line('''' || 'NUMBER' || ''',');
        DBMS_OUTPUT.put_line('''' || '5' || ''',');
        DBMS_OUTPUT.put_line('''' || 'N' || '''');
        DBMS_OUTPUT.put_line(');');
        DBMS_OUTPUT.put_line('INSERT INTO MTDT_CAMPOS_DETAIL (TABLE_NAME, COLUMN_NAME, TYPE, LENGTH, NULABLE) VALUES ');
        DBMS_OUTPUT.put_line('(');
        DBMS_OUTPUT.put_line('''' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || ''',');
        DBMS_OUTPUT.put_line('''MAX_' || reg_per_val.ITEM_NAME || ''',');
        DBMS_OUTPUT.put_line('''' || 'NUMBER' || ''',');
        DBMS_OUTPUT.put_line('''' || '5' || ''',');
        DBMS_OUTPUT.put_line('''' || 'N' || '''');
        DBMS_OUTPUT.put_line(');');
        --DBMS_OUTPUT.put_line('  MIN_' || reg_per_val.ITEM_NAME || '          DECIMAL(' || 5 || '),');
        --DBMS_OUTPUT.put_line('  MAX_' || reg_per_val.ITEM_NAME || '          DECIMAL(' || 5 || '),');
      END IF;
      /* Fin cambio */
        DBMS_OUTPUT.put_line('INSERT INTO MTDT_CAMPOS_DETAIL (TABLE_NAME, COLUMN_NAME, TYPE, LENGTH, NULABLE) VALUES ');
        DBMS_OUTPUT.put_line('(');
        DBMS_OUTPUT.put_line('''' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || ''',');
        DBMS_OUTPUT.put_line('''' || 'FCH_REGISTRO' || ''',');
        DBMS_OUTPUT.put_line('''' || 'DATE' || ''',');
        DBMS_OUTPUT.put_line('''' || '8' || ''',');
        DBMS_OUTPUT.put_line('''' || 'N' || '''');
        DBMS_OUTPUT.put_line(');');
      --DBMS_OUTPUT.put_line('  FCH_REGISTRO' || '          DATE,');
        DBMS_OUTPUT.put_line('INSERT INTO MTDT_CAMPOS_DETAIL (TABLE_NAME, COLUMN_NAME, TYPE, LENGTH, NULABLE) VALUES ');
        DBMS_OUTPUT.put_line('(');
        DBMS_OUTPUT.put_line('''' || PREFIJO_DM || 'D_' || reg_per_val.ITEM_NAME || ''',');
        DBMS_OUTPUT.put_line('''' || 'FCH_MODIFICACION' || ''',');
        DBMS_OUTPUT.put_line('''' || 'DATE' || ''',');
        DBMS_OUTPUT.put_line('''' || '8' || ''',');
        DBMS_OUTPUT.put_line('''' || 'N' || '''');
        DBMS_OUTPUT.put_line(');');
      --DBMS_OUTPUT.put_line('  FCH_MODIFICACION' || '          DATE');
      --DBMS_OUTPUT.put_line('CONSTRAINT "' || reg_per_val.ITEM_NAME || '_PK"' || ' PRIMARY KEY ("CVE_' || reg_per_val.ITEM_NAME || '")');
      --IF (clave_foranea=1)
      --THEN
        --DBMS_OUTPUT.put_line(', CONSTRAINT "' || reg_per_val. AGREGATION || '_FK"' || ' FOREIGN KEY ("CVE_' || substr(reg_per_val. AGREGATION,5) || '")');
        --DBMS_OUTPUT.put_line('REFERENCES ' || OWNER_DM || '.' || PREFIJO_DM || 'D_' || substr(reg_per_val. AGREGATION,5) || ' (' || 'CVE_' || substr(reg_per_val. AGREGATION,5) || ')');
      --END IF;
      --DBMS_OUTPUT.put_line(')');
      --DBMS_OUTPUT.put_line('CLUSTERED BY (');
      --DBMS_OUTPUT.put_line('CVE_' || reg_per_val.ITEM_NAME);
      --DBMS_OUTPUT.put_line(')');
      --DBMS_OUTPUT.put_line('INTO 1 BUCKETS');
      --DBMS_OUTPUT.put_line('STORED AS ORC TBLPROPERTIES (''transactional''=''true'', ''orc.compress''=''ZLIB'', ''orc.create.index''=''true'')');
      --DBMS_OUTPUT.put_line(';');
      --DBMS_OUTPUT.put_line('TABLESPACE ' || TABLESPACE_DIM || ';'); /* Parentesis final del create*/      
     
      DBMS_OUTPUT.put_line('commit;');
    END LOOP;
    CLOSE dtd_permited_values;
    DBMS_OUTPUT.put_line('set echo off;');
    DBMS_OUTPUT.put_line('exit SUCCESS;');
    --DBMS_OUTPUT.put_line('!quit');
  END IF;
END;
