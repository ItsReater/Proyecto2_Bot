import json 
import pandas as pd
from Models.plc import PLC
from Models.gateway import Gateway
from Models.server import Server
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    CallbackContext
)

## El programa esta hecho en función de los datos del CSV, sabiendo que las columnas son: [PLC_id, time, temperature, humidity]
# He hecho este tratamiento, basandome en los proyectos siguientes, sabiendo que solo tendremos sensores de temperatura y humedad.

# Hemos convertido el codigo de la actividad 1 en una funcion.
def create_data_csv():
    for plcs in range(len(config_dict["plcs"])):
        # Generamos la clase para cada PLC.
        plc = PLC(config_dict["plcs"][plcs]["id"], config_dict["plcs"][plcs]["sensors"], config_dict["plcs"][plcs]["gateway_id"])
        # Inicializamos la transmisión de datos PLC - Gateway - Servidor 
        plc.generate_data()
        plc_data = plc.send_data()
        gateway.plc_gateway_connection(plc_data)
        gtw_data = gateway.gateway_server_connection()
        server.store_data(gtw_data) 

# Funcion para filtrar el dataframe, con los sensor_id y PLC solicitados.
def filter_dataframe(df, dict):

    result = pd.DataFrame()
    warnings = []

    for plc_id, sensors in dict.items():
        plc_id_int = int(plc_id)
        # Aseguramos que los sensores existan en las columnas
        valid_sensors = [sensor for sensor in sensors if sensor in df.columns]
        invalid_sensors = [sensor for sensor in sensors if sensor not in df.columns]

        # Agregar advertencia si hay sensores no válidos
        if invalid_sensors:
            warnings.append(f"PLC {plc_id}: sensor(s) not valid: {', '.join(invalid_sensors)}")

        if not valid_sensors:
            continue

        # Filtramos por plc_id y seleccionamos columnas
        filtered = df[df["plc_id"] == plc_id_int][["time"] + valid_sensors]
        filtered.insert(0, "plc_id", plc_id_int)  # Asegurar columna plc_id si se necesita
        result = pd.concat([result, filtered])
    return result, warnings

# Clase del bot.
class SensorBot:
    def __init__(self, token: str):
        self.token = token
        self.app = ApplicationBuilder().token(self.token).build()
        self.admin_chat_id = None
        self.group_id = None
        self.last_data = None
        self.err = 0
        self.user_input = None
        self.subscription_dict = None
        self.get_data_dict = None
        self.create_alert_dict = None

        # Registramos los comandos
        self.app.add_handler(CommandHandler("start", self.start))
        self.app.add_handler(CommandHandler("setgroup", self.set_group))
        self.app.add_handler(CommandHandler("subscribe", self.subscribe))
        self.app.add_handler(CommandHandler("get_data", self.get_data))
        self.app.add_handler(CommandHandler("create_alert", self.create_alert))
        self.app.add_handler(CommandHandler("unsubscribe", self.unsubscribe))

    # Comando start.
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        print(f"User started the bot. Chat ID: {chat_id}")

        if self.admin_chat_id is None:
            self.admin_chat_id = chat_id
            await update.message.reply_text("You are now set as the admin.")
        else:
            await update.message.reply_text("Bot is already running.")

    # Comando set_group.
    async def set_group(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_chat.id != self.admin_chat_id:
            await update.message.reply_text("You are not authorized to use this command.")
            return

        if len(context.args) != 1:
            await update.message.reply_text("Usage: /setgroup <group_id>")
            return

        try:
            self.group_id = int(context.args[0])
            await update.message.reply_text(f"group_id set to {self.group_id}")
            print(f"group_id set to {self.group_id} by admin {self.admin_chat_id}")
        except ValueError:
            await update.message.reply_text("Invalid group_id. Must be a number.")



    # Funcion que será llamada cada 15 segundos cuando el usuario este suscrito.
    async def send_periodic_message(self, context: CallbackContext):
        self.last_data = pd.read_csv('./data/iot_data.csv')
        filtered_data, warnings = filter_dataframe(self.last_data, self.subscription_dict)
        chat_id = context.job.chat_id

        await context.bot.send_message(chat_id=chat_id, text=f"{filtered_data.tail(1)}")

    # Funcion que será llamada cada 10 segundos para ir creando datos en el CSV.
    async def generate_data(self, context: CallbackContext):
        create_data_csv()
    
    # Funcion para analizar si se ha producido algún outlier.
    # Como se rellena el CSV cada 10 segundos, pondre que analize cada 5 segundos. 
    async def outlier_respond(self, context: CallbackContext):
        
        chat_id = context.job.chat_id

        try:
            self.last_data = pd.read_csv('./data/iot_data.csv')
        except:
            context.bot.send_message(chat_id=chat_id, text="CSV not found.")

        # Eliminamos las unidades de los sensores y convertimos el valor en float.
        # Porque luego a la hora de calcular los cuantiles necesitamos que estos sean valores numericos.
        self.last_data['temperature'] = self.last_data['temperature'].str.replace(' C', '', regex=False).astype(float)
        self.last_data['humidity'] = self.last_data['humidity'].str.replace(' %', '', regex=False).astype(float)

        filtered_data, warnings = filter_dataframe(self.last_data, self.create_alert_dict)

        ## Función para detectar los outliers. Metodo IQR
        # Detectamos los nombres de todos los sensores que existen como columna en el CSV.
        column_names = [col for col in filtered_data.columns if col not in ["plc_id","time"]]

        # Separamos la última fila, de las demás.
        # El 
        last_row = filtered_data.iloc[-1:]

        df_historic = self.last_data.iloc[:-1]

        timestamp = last_row["time"].values[0]

        # Calculamos IQR.
        for col in column_names:
            Q1 = df_historic[col].quantile(0.25)
            Q3 = df_historic[col].quantile(0.75)
            IQR = Q3 - Q1

            # El factor que multiplica el IQR determinará el filtro por el cual aceptamos como normal un valor, y cual no.
            # Para que se estabilicen los limites, a unos limites aceptables, debo utilizar un factor muy pequeño 0.1/0.2 aprox.
            # Ja que si el CSV que se utiliza contiene pocos datos, los limites se disparan pudiendo sobrepasar el rango 1 - 100,
            # dando limites inferiores por debajo del 1 o limites superiores por encima del 100. Esto no pasaria si hubiera una gran cantidad de muestras,
            # haciendo que la diferencia entre Q1 y Q3 sea más pequeña, disminuyendo el IQR.
            # Si el CSV contiene un buen numero de datos, habria que modificar este factor, ja que sino, el IQR sera tan pequeño,
            # que augmentarán considerablemente los valores considerados outliers, derivando a una constante activación de la alerta y
            # consecuentemente, indicando falsos valores atípicos que podrían ser considerados perfectamente como normales.

            lim_inf = Q1 - 0.2 * IQR
            lim_sup = Q3 + 0.2 * IQR

            print(lim_inf)
            print(f"\n{lim_sup}")
            if col in filtered_data.columns:
                last_value = last_row[col].values[0]
                print(f"\n{last_value}")

                if last_value < lim_inf or last_value > lim_sup:
                    await context.bot.send_message(chat_id=chat_id, text=f"""Se ha detectado un outlier en {col} || Value = {last_value} || {timestamp}""")

        

    #Comando de la subscripcion, para empezar a enviar datos.
    async def subscribe(self, update: Update, context: ContextTypes.DEFAULT_TYPE):

        self.user_input = " ".join(context.args)
        self.user_input = self.user_input.replace("“", '"').replace("”", '"')
        self.subscription_dict = json.loads(self.user_input)

        if update.effective_chat.id != self.group_id:
            await update.message.reply_text("You can only subscribe from the registered group.")
            return
        
        await update.message.reply_text("You are now subscribed.")

        # Para que self.last_data no de error, cuando el csv esta vacio.
        create_data_csv()

        # Comprobamos si hay algun sensor, que no existe en el CSV, para enviar los mensajes de warning.
        self.last_data = pd.read_csv('./data/iot_data.csv')
        filtered_data, warnings = filter_dataframe(self.last_data, self.subscription_dict)

        if warnings:
            await update.message.reply_text(f"{warnings}")
        
        # En el caso que no exista ningún sensor valido, devolvemos mensaje por chat y finalizamos el proceso.
        if filtered_data.empty:
            # Si hay warnings, entonces el PLC_id existe en el CSV.
            if warnings:
                await update.message.reply_text("Valid data not found.")
                return
            else:
                await update.message.reply_text(f"PLC_id: {list(self.subscription_dict.keys())[0]}, not found.")
                return

        # Creamos la función para generar datos de los sensores cada 10 seg.
        context.job_queue.run_repeating(
            self.generate_data,
            interval=10,
            first=0,
            chat_id=update.effective_chat.id
        )

        #Creamos la función enviar mensajes cada 15 seg con la información.
        context.job_queue.run_repeating(
            self.send_periodic_message,
            interval=15,
            first=0,
            chat_id=update.effective_chat.id,
            name="Periodic_msg"
        )

    #Dejar de recibir datos.
    async def unsubscribe(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        jobs = context.job_queue.get_jobs_by_name('my_repeating_job')
        
        for job in jobs:
            job.schedule_removal()
        
        await update.message.reply_text("You unsubscribed from the service, you will no longer receive any data.")

    #Comando get_data, para enviar los datos del último scan.
    async def get_data(self,update: Update, context: ContextTypes.DEFAULT_TYPE):

        self.user_input = " ".join(context.args)
        self.user_input = self.user_input.replace("“", '"').replace("”", '"')
        self.get_data_dict = json.loads(self.user_input)

        try:
            self.last_data = pd.read_csv('./data/iot_data.csv')
        except:
            await update.message.reply_text("Unable to reach CSV file.")
            self.err = 1

        # Comprobamos si hay algun sensor, que no existe en el CSV, para enviar los mensajes de warning.
        filtered_data, warnings = filter_dataframe(self.last_data, self.get_data_dict)
        
        if warnings:
            await update.message.reply_text(f"{warnings}")
        
        # En el caso que no exista ningún sensor valido, devolvemos mensaje por chat y finalizamos el proceso.
        if filtered_data.empty:
            # Si hay warnings, entonces el PLC_id existe en el CSV.
            if warnings:
                await update.message.reply_text("Valid data not found.")
                return
            else:
                await update.message.reply_text(f"PLC_id: {list(self.get_data_dict.keys())[0]}, not found.")
                return
        
        if self.err == 0:
            await update.message.reply_text(f"{filtered_data.tail(1)}")

    # Comando alerta de outliers.
    async def create_alert(self,update: Update, context: ContextTypes.DEFAULT_TYPE):
        
        self.user_input = " ".join(context.args)
        self.user_input = self.user_input.replace("“", '"').replace("”", '"')
        self.create_alert_dict = json.loads(self.user_input)

        self.last_data = pd.read_csv('./data/iot_data.csv')

        # Comprobamos si hay algun sensor, que no existe en el CSV, para enviar los mensajes de warning.
        filtered_data, warnings = filter_dataframe(self.last_data, self.create_alert_dict)
        
        if warnings:
            await update.message.reply_text(f"{warnings}")
        
        # En el caso que no exista ningún sensor valido, devolvemos mensaje por chat y finalizamos el proceso.
        if filtered_data.empty:
            # Si hay warnings, entonces el PLC_id existe en el CSV.
            if warnings:
                await update.message.reply_text("Valid data not found.")
                return
            else:
                await update.message.reply_text(f"PLC_id: {list(self.get_data_dict.keys())[0]}, not found.")
                return
        
        #Creamos la función enviar mensajes cada 15 seg con la información.
        context.job_queue.run_repeating(
            self.outlier_respond,
            interval=15,
            first=0,
            chat_id=update.effective_chat.id,
            name="Outlier_alert"
        )


    def run(self):
        print("Bot is running...")
        self.app.run_polling()

if __name__ == "__main__":
    ## Apertura y guardado de datos del json.
    try:
        with open("config.json", "r") as json_file:
            config_dict = json.load(json_file)
    
    except:
        print("ERROR: config.json file not found!")
        exit()

    # Definir clase Gateway, y actualizar sus propiedades.
    gateway = Gateway()
    gateway.gateway_properties(config_dict["gateways"])

    # Definir clase Server, y actualizar sus propiedades.
    server = Server()
    server.server_properties(config_dict["servers"])

    # Inicializamos el bot.
    TOKEN = "8059527014:AAGSllyiszU0B4V_X8Z0uA0Ehvt7xMlMDaM"
    bot = SensorBot(TOKEN)
    bot.run()

