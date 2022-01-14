

class ServiceParameters:
    name: "Fdsa"
    inputs: [Services]
    outputs: "ServiceName"

def executeService(serviceName, parameters=None):
    cached_data = registry.get(serviceName, parameters)
    if cached_data:
        return cached_data
    else:
        self._compute(serviceName, parameters)

def _compute(self, serviceName, parameters):
    data = callDocker(registry.image(serviceName), parameters)
    registry.put(data)
    ...
    return data

def registerService(serviceRegister):
    registry.add()
