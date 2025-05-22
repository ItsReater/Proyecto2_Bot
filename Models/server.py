import pandas as pd
from datetime import datetime

# Nos guardamos la dirección del archivo csv, para luego utilizarlo.
path = "C:\\Users\\sergi\\PiC2\\Bot2025_Practica\\Data\\iot_data.csv"

class Server:
    def __init__(self):
        self.ip = []
        self.csvdict = {}

 
 # Tambien añadir la unidad a los sensores: temperatura [ºC], humedad [%]
    def server_properties(self, servers):
        for x_servers in range(len(servers)):
            self.ip.append(servers[x_servers]["ip"])

# Función que recibe los datos del gateway y los pasa a un diccionario adaptado para enviarlo con pandas al CSV.
    def store_data(self, gtw_data):
        # Identificamos la id del servidor al cual se quiere transferir la información.
        for server_id in range(len(self.ip)):

            if gtw_data["server_ip"] == self.ip[server_id]:

                temp = gtw_data["plc_data"]["sensor_readings"]["temperature"]
                hum = gtw_data["plc_data"]["sensor_readings"]["humidity"]

                # Columnas que se crearan ID, timestamp, temperature [ºC], humidity[%].

                self.csvdict["plc_id"] = gtw_data["plc_data"]["plc_id"]
                self.csvdict["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.csvdict["temperature"] = f"{temp[0]} {temp[1]}"
                self.csvdict["humidity"] = f"{hum[0]} {hum[1]}"
                
                df = pd.DataFrame([self.csvdict])


                try:
                    df.to_csv(path, mode= "a", index= False, header= False)
                    self.first_push = 0
                    print("Archivo guardado correctamente.")
                except Exception as e:
                    print("Ocurrió un error al guardar el archivo:", e)