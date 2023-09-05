import pandas as pd
import psycopg2
import os

# Configuración de la conexión a PostgreSQL
db_config = {
    'host': '192.168.1.18',
    'user': 'postgres',
    'password': 'dba',
    'dbname': 'migracion'
}

conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

# ruta del documento
socios = 'C:\\migrar\\cuentas.xlsx'
sheet_name = 'Hoja1'  # Cambia esto al nombre de la hoja que deseas importar

df = pd.read_excel(socios, sheet_name=sheet_name)

for index, row in df.iterrows():
    insert_query = f"INSERT INTO sgf_cuenta (cod_producto, cod_cuenta, cod_socio, num_cuenta_ref, fec_apertura, fec_ult_movimiento, sts_cuenta, fec_sts_cuenta, txt_referencia, val_saldo, val_efectivo, val_cheques, val_bloqueado, cod_oficina, cod_usrmod, fec_usrmod, cod_oficial_cuenta, nom_cuenta, cod_forma_envio_correspondencia, nom_contacto_corresp, sts_tipo_vivienda_corresp, dir_corresp, val_inicial_apertura, sts_condiciones_especiales, cod_frecuencia, num_libreta, cod_forma_pago_capital, cod_forma_pago_interes, cod_cuenta_pago_interes, val_garantia, val_encaje, val_otro, txt_comentario, cod_cuenta_rel, num_linea_imp_libreta, val_fondo, val_gastos, val_certificado, val_ahorro, val_edificio, cod_producto_rel, val_interes, val_promedio30, val_promedio60, val_promedio90, val_saldo_minimo, val_tasa_interes, fec_vigente, num_plazo, sts_bloquea, cod_promotor) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    # transformar fechas
    fec_apertura = row['fec_apertura'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_apertura']) else None
    fec_ult_movimiento = row['fec_ult_movimiento'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_ult_movimiento']) else None
    fec_sts_cuenta = row['fec_sts_cuenta'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_sts_cuenta']) else None
    fec_usrmod = row['fec_usrmod'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_usrmod']) else None
    fec_vigente = row['fec_vigente'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_vigente']) else None


    data = (row['cod_producto'], row['cod_cuenta'], row['cod_socio'], row['num_cuenta_ref'], fec_apertura, fec_ult_movimiento, row['sts_cuenta'], fec_sts_cuenta, row['txt_referencia'], row['val_saldo'], row['val_efectivo'], row['val_cheques'], row['val_bloqueado'], row['cod_oficina'], row['cod_usrmod'], fec_usrmod, row['cod_oficial_cuenta'], row['nom_cuenta'], row['cod_forma_envio_correspondencia'], row['nom_contacto_corresp'], row['sts_tipo_vivienda_corresp'], row['dir_corresp'], row['val_inicial_apertura'], row['sts_condiciones_especiales'], row['cod_frecuencia'], row['num_libreta'], row['cod_forma_pago_capital'], row['cod_forma_pago_interes'], row['cod_cuenta_pago_interes'], row['val_garantia'], row['val_encaje'], row['val_otro'], row['txt_comentario'], row['cod_cuenta_rel'], row['num_linea_imp_libreta'], row['val_fondo'], row['val_gastos'], row['val_certificado'], row['val_ahorro'], row['val_edificio'], row['cod_producto_rel'], row['val_interes'], row['val_promedio30'], row['val_promedio60'], row['val_promedio90'], row['val_saldo_minimo'], row['val_tasa_interes'], fec_vigente, row['num_plazo'], row['sts_bloquea'], row['cod_promotor'])
    cursor.execute(insert_query, data)
    conn.commit()