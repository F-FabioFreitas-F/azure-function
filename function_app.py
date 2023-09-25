import azure.functions as func
import logging

app = func.FunctionApp()


@app.schedule(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def execute_time(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed')