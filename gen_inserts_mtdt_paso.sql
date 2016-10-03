DECLARE
/* Creo el curdor que nos va a recorrer todos los procesos existentes */
CURSOR
  cursor_mtdt_paso
IS
  SELECT
  CVE_PROCESO,
  CVE_PASO,
  FCH_ALTA,
  ESTADO,
  FCH_ESTADO,
  FCH_REGISTRO
  FROM MTDT_PASO
  ORDER BY CVE_PROCESO, CVE_PASO;
  
  reg cursor_mtdt_paso%rowtype;
  
  OWNER_SA                             VARCHAR2(60);
  OWNER_T                                VARCHAR2(60);
  OWNER_DM                            VARCHAR2(60);
  OWNER_MTDT                       VARCHAR2(60);
  NAME_DM                                VARCHAR(60);
  
BEGIN
  /* ANTES DE NADA LEEMOS LAS VAR. DE ENTORNO PARA TIEMPO DE GENERACION*/
  SELECT VALOR INTO OWNER_SA FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_SA';
  SELECT VALOR INTO OWNER_T FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_T';
  SELECT VALOR INTO OWNER_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_DM';
  SELECT VALOR INTO OWNER_MTDT FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'OWNER_MTDT';
  SELECT VALOR INTO NAME_DM FROM MTDT_VAR_ENTORNO WHERE NOMBRE_VAR = 'NAME_DM';
  /* (20141219) FIN*/
  OPEN cursor_mtdt_paso;
  LOOP
    FETCH cursor_mtdt_paso
    INTO reg;
    EXIT WHEN cursor_mtdt_paso%NOTFOUND;
    DBMS_OUTPUT.put_line('INSERT INTO ' || OWNER_MTDT || '.MTDT_PASO');
    DBMS_OUTPUT.put_line('SELECT ' || reg.CVE_PROCESO || ', ' || reg.CVE_PASO || ', ''' || to_char(reg.FCH_ALTA, 'YYYY-MM-DD') || ''', ''' || reg.ESTADO || ''', ''' || to_char(reg.FCH_ESTADO, 'YYYY-MM-DD') || ''', ''' || to_char(reg.FCH_REGISTRO, 'YYYY-MM-DD') || ''' from ' || OWNER_MTDT || '.dual;');
  END LOOP;
  DBMS_OUTPUT.put_line('!quit');
END;