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
socios = 'C:\\migrar\\credito.xlsx'
sheet_name = 'Hoja1'  # Cambia esto al nombre de la hoja que deseas importar

df = pd.read_excel(socios, sheet_name=sheet_name)


for index, row in df.iterrows():
    insert_query = f"INSERT INTO sgf_credito (cod_producto, cod_cuenta, cod_producto_socio, cod_cuenta_socio, cod_socio, num_credito, num_solicitud, val_capital, val_desembolso, val_saldo, val_tasa_interes, val_interes, val_gastos, val_gestion_cobro, val_ahorro, val_certificado, val_otros, val_impuesto, num_plazo, num_dias_amortizacion, num_cuotas, cod_destino, fec_prestamo, fec_vencimiento, fec_ult_pago, sts_prestamo, sts_tipo_cuota, sts_tipo_tasa, sts_forma_pago, cod_oficial_credito, cod_oficina, txt_referencia, cod_usrmod, fec_usrmod, cod_producto_encaje, cod_cuenta_encaje, val_cupo, sts_encaje, val_encaje, cod_producto_certificado, cod_cuenta_certificado, fec_primer_pago, sts_certificado, sts_ahorro, num_cuotas_pagadas, val_mora, num_notificacion, fec_ult_notificacion, val_notificacion, val_seguro, num_dia_pago, sts_seguro, val_capital_mora, num_dias_mora, val_capital_vencido, val_multa, val_edificio, val_fondo, cod_notificacion, sts_tipo_credito, val_demanda, val_castigo, cod_tipo_credito, cod_cuenta_contable, cod_clasif_credito, val_tir, val_tea, val_prima_descuento, val_provision_inc_constituida, val_provision_inc_reducida, val_provision_int, fec_provision_int, fec_demanda, fec_castigo, cod_tipo_operacion, cod_obj_fideicomiso, fec_provision_inc, cod_cuenta_anterior, fec_novacion, cod_linea_credito, num_frecuencia_revision, cod_clase_credito, sts_operacion, cod_situacion_operacion, cod_destino_financiero, cod_act_eco_receptora, cod_provincia_destino, cod_canton_destino, cod_instruccion_esperada, num_participantes_solidario, cod_forma_cancelacion, fec_cancelacion, cod_parroquia_destino) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    # transformar fechas
    fec_prestamo = row['fec_prestamo'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_prestamo']) else None
    fec_vencimiento = row['fec_vencimiento'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_vencimiento']) else None
    fec_ult_pago = row['fec_ult_pago'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_ult_pago']) else None
    fec_usrmod = row['fec_usrmod'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_usrmod']) else None
    fec_primer_pago = row['fec_primer_pago'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_primer_pago']) else None
    fec_ult_notificacion = row['fec_ult_notificacion'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_ult_notificacion']) else None

    fec_provision_int = row['fec_provision_int'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_provision_int']) else None
    fec_demanda = row['fec_demanda'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_demanda']) else None
    fec_castigo = row['fec_castigo'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_castigo']) else None
    fec_provision_inc = row['fec_provision_inc'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_provision_inc']) else None
    fec_novacion = row['fec_novacion'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_novacion']) else None
    fec_cancelacion = row['fec_cancelacion'].strftime('%Y-%m-%d') if not pd.isnull(row['fec_cancelacion']) else None

    data = (row['cod_producto'], row['cod_cuenta'], row['cod_producto_socio'], row['cod_cuenta_socio'], row['cod_socio'], row['num_credito'], row['num_solicitud'], row['val_capital'], row['val_desembolso'], row['val_saldo'], row['val_tasa_interes'], row['val_interes'], row['val_gastos'], row['val_gestion_cobro'], row['val_ahorro'], row['val_certificado'], row['val_otros'], row['val_impuesto'], row['num_plazo'], row['num_dias_amortizacion'], row['num_cuotas'], row['cod_destino'], fec_prestamo, fec_vencimiento, fec_ult_pago, row['sts_prestamo'], row['sts_tipo_cuota'], row['sts_tipo_tasa'], row['sts_forma_pago'], row['cod_oficial_credito'], row['cod_oficina'], row['txt_referencia'], row['cod_usrmod'], fec_usrmod, row['cod_producto_encaje'], row['cod_cuenta_encaje'], row['val_cupo'], row['sts_encaje'], row['val_encaje'], row['cod_producto_certificado'], row['cod_cuenta_certificado'], fec_primer_pago, row['sts_certificado'], row['sts_ahorro'], row['num_cuotas_pagadas'], row['val_mora'], row['num_notificacion'], fec_ult_notificacion, row['val_notificacion'], row['val_seguro'], row['num_dia_pago'], row['sts_seguro'], row['val_capital_mora'], row['num_dias_mora'], row['val_capital_vencido'], row['val_multa'], row['val_edificio'], row['val_fondo'], row['cod_notificacion'], row['sts_tipo_credito'], row['val_demanda'], row['val_castigo'], row['cod_tipo_credito'], row['cod_cuenta_contable'], row['cod_clasif_credito'], row['val_tir'], row['val_tea'], row['val_prima_descuento'], row['val_provision_inc_constituida'], row['val_provision_inc_reducida'], row['val_provision_int'], fec_provision_int, fec_demanda, fec_castigo, row['cod_tipo_operacion'], row['cod_obj_fideicomiso'], fec_provision_inc, row['cod_cuenta_anterior'], fec_novacion, row['cod_linea_credito'], row['num_frecuencia_revision'], row['cod_clase_credito'], row['sts_operacion'], row['cod_situacion_operacion'], row['cod_destino_financiero'], row['cod_act_eco_receptora'], row['cod_provincia_destino'], row['cod_canton_destino'], row['cod_instruccion_esperada'], row['num_participantes_solidario'], row['cod_forma_cancelacion'], fec_cancelacion, row['cod_parroquia_destino'])

    cursor.execute(insert_query, data)
    conn.commit()

cursor.close()
conn.close()

print("Datos insertados correctamente en la base de datos.")