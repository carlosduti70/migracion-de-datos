import psycopg2


# Datos de conexión a PostgreSQL
db_connection = psycopg2.connect(
    host="190.123.34.157",
    database="postgres",
    user="postgres",
    password="dba"
)

# Abre el archivo de Bloc de notas
with open("C:\\CREDITO_SALDO\\credito_saldo_tabla\\sgf_credito_saldo_tabla_2016_Diciembre_31.txt") as file:
    # Lee el archivo línea por línea
    lines = file.readlines()


# Crea un cursor para ejecutar consultas
cursor = db_connection.cursor()


# Itera a través de las líneas del archivo y ejecuta consultas INSERT
for line in lines:
    data = line.strip().split(' ')  # Divide la línea en valores separados por comas
    cursor.execute("INSERT INTO sgf_credito_saldo_tabla (fec_saldo, cod_producto, cod_cuenta, num_cuota, val_capital, val_interes, val_tasa_interes, val_gastos, val_gestion_cobro, val_ahorro, val_certificado, val_otros, val_impuesto, sts_credito_tabla, fec_inicio, fec_vencimiento, txt_referencia, fec_usrmod, val_seguro, val_notificacion, val_multa, val_saldo_capital, val_saldo_interes, val_saldo_gestion_cobro, val_saldo_ahorro,  val_saldo_certificado, val_saldo_gastos, val_saldo_otros, val_saldo_impuesto, val_saldo_seguro, val_saldo_notificacion, fec_ult_pago, val_saldo_multa, val_capital_mora, num_dias_mora, val_saldo_mora, val_edificio, val_saldo_edificio, val_fondo, val_saldo_fondo, cod_usrmod, val_capital_vencido, cod_cuenta_contable) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                   (data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9]))

# Confirma los cambios en la base de datos y cierra la conexión
db_connection.commit()
cursor.close()
db_connection.close()

print("Los datos se han insertado en la base de datos con éxito :) ...")
