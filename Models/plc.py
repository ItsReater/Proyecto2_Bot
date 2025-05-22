import random

class PLC:
    def __init__(self, plc_id, sensors, gateway_id):
        self.plc_id = plc_id
        self.sensors = sensors
        self.gateway_id = gateway_id
        self.plc_dict = {}
        self.sensor_dict = {}
        self.gt_data = {"gateway_id" : gateway_id, "plc_data" : {}}

    def hold_data(self):
        self.plc_dict["plc_id"] = self.plc_id
        self.plc_dict["sensor_readings"] = self.sensor_dict

    def send_data(self):
        print(f"PLC {self.plc_id} sending data: {self.gt_data['plc_data']}")
        return self.gt_data

    def generate_data(self):
        for sensor in range(len(self.sensors)):
            rand_num = round(random.uniform(1,100), 2)
            self.sensor_dict[self.sensors[sensor]["type"]] = [rand_num, self.sensors[sensor]["unit"]]
            self.hold_data()
        self.gt_data["plc_data"] = self.plc_dict

