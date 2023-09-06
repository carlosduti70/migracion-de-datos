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
socios = 'C:\\migrar\\credito_tabla.xlsx'
sheet_name = 'Hoja1'  # Cambia esto al nombre de la hoja que deseas importar

df = pd.read_excel(socios, sheet_name=sheet_name)

for index, row in df.iterrows():
    insert_query = f"INSERT INTO sgf_credito_tabla (cod_producto, cod_cuenta, num_cuota, val_capital, val_interes, val_tasa_interes, val_gastos, val_gestion_cobro, val_ahorro, val_certificado, val_otros, val_impuesto, sts_credito_tabla, fec_inicio, fec_vencimiento, txt_referencia, fec_usrmod, val_seguro, val_notificacion, val_multa, val_saldo_capital, val_saldo_interes, val_saldo_gestion_cobro, val_saldo_ahorro,  val_saldo_certificado, val_saldo_gastos, val_saldo_otros, val_saldo_impuesto, val_saldo_seguro, val_saldo_notificacion, fec_ult_pago, val_saldo_multa, val_capital_mora, num_dias_mora, val_saldo_mora, val_edificio, val_saldo_edificio, val_fondo, val_saldo_fondo, cod_usrmod, val_capital_vencido, cod_cuenta_contable) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    # transformar fechas
    fec_inicio = row['fec_inicio'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_inicio']) else None
    fec_vencimiento = row['fec_vencimiento'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_vencimiento']) else None
    fec_usrmod = row['fec_usrmod'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_usrmod']) else None
    fec_ult_pago = row['fec_ult_pago'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_ult_pago']) else None

    sts_credito_tabla = str(row['sts_credito_tabla'])


    data = (row['cod_producto'], row['cod_cuenta'], row['num_cuota'], row['val_capital'], row['val_interes'], row['val_tasa_interes'], row['val_gastos'], row['val_gestion_cobro'], row['val_ahorro'], row['val_certificado'], row['val_otros'], row['val_impuesto'], sts_credito_tabla[0], fec_inicio, fec_vencimiento, row['txt_referencia'], fec_usrmod, row['val_seguro'], row['val_notificacion'], row['val_multa'], row['val_saldo_capital'], row['val_interes'], row['val_saldo_gestion_cobro'], row['val_saldo_ahorro'], row['val_saldo_certificado'], row['val_saldo_gastos'], row['val_saldo_otros'], row['val_saldo_impuesto'], row['val_saldo_seguro'], row['val_saldo_notificacion'], fec_ult_pago, row['val_saldo_multa'], row['val_capital_mora'], row['num_dias_mora'], row['val_saldo_mora'], row['val_edificio'], row['val_saldo_edificio'], row['val_fondo'], row['val_saldo_fondo'], row['cod_usrmod'], row['val_capital_vencido'], row['cod_cuenta_contable'])
    
    cursor.execute(insert_query, data)
    conn.commit()

print("Datos insertados correctamente :)...")