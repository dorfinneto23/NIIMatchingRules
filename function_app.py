import azure.functions as func
import logging

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="niimatchingrules",
                               connection="medicalanalysis_SERVICEBUS") 
def NIIMatchingRules(azservicebus: func.ServiceBusMessage):
    logging.info('Python ServiceBus Queuee trigger processed a message: %s',
                azservicebus.get_body().decode('utf-8'))
