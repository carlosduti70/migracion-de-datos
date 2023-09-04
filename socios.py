import pandas as pd
import psycopg2
import os

# Configuración de la conexión a PostgreSQL
db_config = {
    'host': '192.168.1.71',
    'user': 'postgres',
    'password': 'dba',
    'dbname': 'postgres'
}

conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

# ruta del documento
socios = 'C:\\migrar\\socios.xlsx'
sheet_name = 'Hoja1'  # Cambia esto al nombre de la hoja que deseas importar

df = pd.read_excel(socios, sheet_name=sheet_name)


for index, row in df.iterrows():
    if row['codigo del socio'] == None:
            print("Error en campo numero" + row['codigo del socio'])
    insert_query = f"INSERT INTO sgf_socio (cod_socio, cod_tipo_socio, fec_ingreso, cod_tipo_persona, cod_tipo_id, num_id, cod_parroquia, cod_canton, cod_provincia, cod_pais, nom_socio, ape_socio, cod_act_economica, cod_instruccion, nom_juridico, sts_sexo, fec_nacimiento, sts_civil, sts_socio, nom_representante_legal, ape_representante_legal, cod_tipo_id_rep_legal, num_id_rel_legal, nom_conyuge, ape_conyuge, fec_nac_con, cod_tipo_id_con, num_id_con, dir_dom, dir2_dom, tel_dom, tel_trabajo, tel_celular, sts_operador_cel, dir_correo, img_foto, txt_link, fec_usrmod, cod_usrmod, nom_beneficiario, ape_beneficiario, cod_tipo_id_ben, num_id_ben, dir_trabajo, dir2_trabajo, cod_tipo_sangre, val_ingreso_mensual, fec_solicitud, cod_origen_ingresos, val_activo, val_pasivo, val_patrimonio, val_gastos_mensuales, cod_causal_vinculacion, fec_causal, cod_tipo_vivienda, val_vivienda, num_tiempo_trabajo, num_cargas_familiares, cod_socio_conyuge, cod_socio_vinculado, cod_relacion, txt_observacion_relacion, cod_oficina, txt_lugar_trabajo, cod_nivel_estudios, sts_rep_asamblea, cod_parroquia_nac, cod_canton_nac, cod_provincia_nac, cod_pais_nac, cod_pais_dom, cod_barrio, sts_pep, sts_fuente_ingresos, sts_actualiza_web, fec_reingreso, sts_consejo_administracion, sts_consejo_vigilancia, sts_asamblea_gen_repres, sts_edu_financiera, cod_etnia, sts_representante_legal) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    # transformar fechas
    fec_ingreso = row['fec_ingreso'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_ingreso']) else None
    fec_nacimiento = row['Fecha de nacimiento'].strftime('%Y-%m-%d') if not pd.isnull(row['Fecha de nacimiento']) else None
    fec_nac_con = row['fec_nac_con'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_nac_con']) else None
    fec_usrmod = row['fec_usrmod'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_usrmod']) else None
    fec_solicitud = row['fec_solicitud'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_solicitud']) else None
    fec_causal = row['fec_causal'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_causal']) else None
    fec_reingreso = row['fec_reingreso'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_reingreso']) else None

    data = (row['codigo del socio'], row['tipo de socio S Socio C Cliente'], fec_ingreso, row['Tipo de persona N Natural J Juridica'], row['Tipo de identificacion C Cedula R RUC'], row['Numero de identificacion o cedula'], row['cod_parroquia'], row['cod_canton'], row['cod_provincia'], row['cod_pais'], row['nombre del socio'], row['apellidos del socio'], row['cod_act_economica'], row['cod_instruccion'], row['nom_juridico'], row['Genero M Masculino, F Femenino'], fec_nacimiento, row['Estado Civil C Casado S Solvero D Divorciado U Union Libre V Viudo'], row['sts_socio'], row['nom_representante_legal'], row['ape_representante_legal'], row['cod_tipo_id_rep_legal'], row['num_id_rel_legal'], row['nombre_conyuge'], row['ape_conyuge'], fec_nac_con, row['cod_tipo_id_con'], row['num_id_con'], row['direccion'], row['dir2_dom'], row['Telefono'], row['TelefonoTrab'], row['Celular'], row['sts_operador_cel'], row['dir_correo'], row['img_foto'], row['txt_link'], fec_usrmod, row['cod_usrmod'], row['nom_beneficiario'], row['ape_beneficiario'], row['cod_tipo_id_ben'], row['num_id_ben'], row['dir_trabajo'], row['dir2_trabajo'], row['cod_tipo_sangre'], row['val_ingreso_mensual'], fec_solicitud, row['cod_origen_ingresos'], row['val_activo'], row['val_pasivo'], row['val_patrimonio'], row['val_gastos_mensuales'], row['cod_casual_vinculacion'], fec_causal, row['cod_tipo_vivienda'], row['val_vivienda'], row['num_tiempo_trabajo'], row['num_cargas_familiares'], row['cod_socio_conyuge'], row['cod_socio_vinculado'], row['cod_relacion'], row['txt_observacion_relacion'], row['cod_oficina'], row['txt_lugar_trabajo'], row['cod_nivel_estudios'], row['sts_rep_asamblea'], row['cod_parroquia_nac'], row['cod_canton_nac'], row['cod_provincia_nac'], row['cod_pais_nac'], row['cod_pais_dom'], row['cod_barrio'], row['sts_pep'], row['sts_fuente_ingresos'], row['sts_actualiza_web'], fec_reingreso, row['sts_consejo_administracion'], row['sts_consejo_vigilancia'], row['sts_asamblea_gen_repres'], row['sts_edu_financiera'], row['cod_etnia'], row['sts_representante_legal'])

    cursor.execute(insert_query, data)
    conn.commit()



cursor.close()
conn.close()

print("Datos insertados correctamente en la base de datos.")