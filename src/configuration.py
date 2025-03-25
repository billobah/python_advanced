class Configuration:

    def __init__(self, input_config):
        self.input_config = input_config

    def methode_1(self):
        return self.input_config

configuration_1 = Configuration("valeur_1")
configuration_2 = Configuration("valeur_2")

print("configuration_1.input_config", configuration_1.input_config)
print("configuration_1.methode_1()", configuration_1.methode_1())

print("configuration_2.input_config", configuration_2.input_config)
print("configuration_2.methode_1()", configuration_2.methode_1())