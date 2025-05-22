class Gateway:
    def __init__(self):
        self.id = []
        self.protocol = []
        self.server_ip = []
        self.server_data = {"server_ip" : "", "protocol" : "", "plc_data" : {}}

    def gateway_properties(self, gateways):
        for x_gtw in range(len(gateways)):
            self.id.append(gateways[x_gtw]["id"])
            self.protocol.append(gateways[x_gtw]["protocol"])
            self.server_ip.append(gateways[x_gtw]["server_ip"])
    
    def plc_gateway_connection(self, plc_data):
        for gtw_id in range(len(self.id)):
            if self.id[gtw_id] == plc_data["gateway_id"]:
                self.server_data["server_ip"] = self.server_ip[gtw_id]
                self.server_data["protocol"] = self.protocol[gtw_id]
                self.server_data["plc_data"] = plc_data["plc_data"]
                print(f"Gateway {self.id[gtw_id]} received data: {plc_data['plc_data']}")

    def gateway_server_connection(self):
        return self.server_data