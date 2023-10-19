import psycopg2


# Datos de conexión a PostgreSQL
db_connection = psycopg2.connect(
    host="190.123.34.157",
    database="postgres",
    user="postgres",
    password="dba"
)

# Abre el archivo de Bloc de notas
with open("C:\\CREDITO_SALDO\\credito_saldo\\prueba.txt") as file:
    # Lee el archivo línea por línea
    lines = file.readlines()


# Crea un cursor para ejecutar consultas
cursor = db_connection.cursor()


# Itera a través de las líneas del archivo y ejecuta consultas INSERT
for line in lines:
    data = line.strip().split('	')  # Divide la línea en valores separados por un espacio
    if len(data) <= 94:
        # Reemplaza los valores vacíos con None
        data = [value if value != "" else None for value in data]
        data = [None if item == "NULL" else item for item in data]
        
        try:
            cursor.execute("INSERT INTO sgf_credito_saldo (fec_saldo, cod_producto, cod_cuenta, cod_producto_socio, cod_cuenta_socio, cod_socio, num_credito, num_solicitud, val_capital, val_desembolso, val_saldo, val_tasa_interes, val_interes, val_gastos, val_gestion_cobro, val_ahorro, val_certificado, val_otros, val_impuesto, num_plazo, num_dias_amortizacion, num_cuotas, cod_destino, fec_prestamo, fec_vencimiento, fec_ult_pago, sts_prestamo, sts_tipo_cuota, sts_tipo_tasa, sts_forma_pago, cod_oficial_credito, cod_oficina, txt_referencia, cod_usrmod, fec_usrmod, cod_producto_encaje, cod_cuenta_encaje, val_cupo, sts_encaje, val_encaje, cod_producto_certificado, cod_cuenta_certificado, fec_primer_pago, sts_certificado, sts_ahorro, num_cuotas_pagadas, val_mora, num_notificacion, fec_ult_notificacion, val_notificacion, val_seguro, num_dia_pago, sts_seguro, val_capital_mora, num_dias_mora, val_capital_vencido, val_multa, val_edificio, val_fondo, cod_notificacion, sts_tipo_credito, val_demanda, val_castigo, cod_tipo_credito, cod_cuenta_contable, cod_clasif_credito, val_tir, val_tea, val_prima_descuento, val_provision_inc_constituida, val_provision_inc_reducida, val_provision_int, fec_provision_int, fec_demanda, fec_castigo, cod_tipo_operacion, cod_obj_fideicomiso, fec_provision_inc, cod_cuenta_anterior, fec_novacion, cod_linea_credito, num_frecuencia_revision, cod_clase_credito, sts_operacion, cod_situacion_operacion, cod_destino_financiero, cod_act_eco_receptora, cod_provincia_destino, cod_canton_destino, cod_instruccion_esperada, num_participantes_solidario, cod_forma_cancelacion, fec_cancelacion) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15], data[16], data[17], data[18], data[19], data[20], data[21], data[22], data[23], data[24], data[25], data[26], data[27], data[28], data[29], data[30], data[31], data[32], data[33], data[34], data[35], data[36], data[37], data[38], data[39], data[40], data[41], data[42], data[43], data[44], data[45], data[46], data[47], data[48], data[49], data[50], data[51], data[52], data[53], data[54], data[55], data[56], data[57], data[58], data[59], data[60], data[61], data[62], data[63], data[64], data[65], data[66], data[67], data[68], data[69], data[70], data[71], data[72], data[73], data[74], data[75], data[76], data[77], data[78], data[79], data[80], data[81], data[82], data[83], data[84], data[85], data[86], data[87], data[88], data[89], data[90], data[91], data[92], data[93]))
        except psycopg2.Error as error:
            print(f"Error al insertar datos en la línea {line}: {error}")
    else:
        print(f"Advertencia: Línea incorrecta en la línea {line} con {len(data)} valores")
# Confirma los cambios en la base de datos y cierra la conexión
db_connection.commit()
cursor.close()
db_connection.close()

print("Los datos se han insertado en la base de datos con éxito :) ...")