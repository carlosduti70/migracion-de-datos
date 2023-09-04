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
dep_plazo_tabla = 'C:\\migrar\\plant_deposi_plazo.xlsx'
sheet_name = 'Hoja1'  # Cambia esto al nombre de la hoja que deseas importar

df = pd.read_excel(dep_plazo_tabla, sheet_name=sheet_name)


for index, row in df.iterrows():
    insert_query = f"INSERT INTO sgf_dep_plazo (cod_producto, cod_cuenta, cod_socio, num_documento, val_deposito, val_efectivo, val_cheques, val_tasa_interes, val_impuesto, sts_deposito, fec_deposito, num_plazo, num_cuotas_pago_interes, fec_vencimiento, txt_referencia, nom_beneficiario, ape_beneficiario, cod_tipo_id_ben, num_id_ben, cod_usrmod, fec_usrmod, val_interes, val_tasa_impuesto, cod_oficina, cod_cuenta_contable, cod_producto_socio, cod_cuenta_socio, sts_forma_pago_interes, val_saldo_impuesto, val_saldo_deposito, val_saldo_interes, val_tir, val_tea) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    # transformar fechas
    fec_deposito = row['fec_deposito'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_deposito']) else None
    fec_vencimiento = row['fec_vencimiento'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_vencimiento']) else None
    fec_usrmod = row['fec_usrmod'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_usrmod']) else None


    data = (row['cod_producto'], row['cod_cuenta'], row['cod_socio'], row['num_documento'], row['val_deposito'], row['val_efectivo'], row['val_cheques'], row['val_tasa_interes'], row['val_impuesto'], row['sts_deposito'], fec_deposito, row['num_plazo'], row['num_cuotas_pago_interes'], fec_vencimiento, row['txt_referencia'], row['nom_beneficiario'], row['ape_beneficiario'], row['cod_tipo_id_ben'], row['num_id_ben'], row['cod_usrmod'],fec_usrmod, row['val_interes'], row['val_tasa_impuesto'], row['cod_oficina'], row['cod_cuenta_contable'], row['cod_producto_socio'], row['cod_cuenta_socio'], row['sts_forma_pago_interes'], row['val_saldo_impuesto'], row['val_saldo_deposito'], row['val_saldo_interes'], row['val_tir'], row['val_tea'])

    cursor.execute(insert_query, data)
    conn.commit()

cursor.close()
conn.close()

print("Datos insertados correctamente en la base de datos.")